"""Paper Portfolio Tracker — active MTM tracking for accepted shadow trades.

Adds a separate paper portfolio layer beside shadow_trades.
- shadow_trades remains the source of accepted paper-intent events.
- paper_positions tracks active mark-to-market state.
- paper_position_marks stores 15-minute mark history for drawdown/reporting.

This module does not modify thresholds, quarantine, v2 gating, or live sizing.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_CLOB_MIDPOINT_URL = "https://clob.polymarket.com/midpoint?token_id={}"
_CLOB_BOOK_URL = "https://clob.polymarket.com/book?token_id={}"
_DATA_API_TRADES_URL = "https://data-api.polymarket.com/trades?{}"

# ── Schema ─────────────────────────────────────────────────────────────────────

_PAPER_POSITION_COLUMNS = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("shadow_trade_id", "INTEGER UNIQUE"),
    ("signal_id", "TEXT"),
    ("snapshot_id", "INTEGER"),
    ("market_id", "TEXT"),
    ("condition_id", "TEXT"),
    ("instrument_id", "TEXT"),
    ("outcome_token", "TEXT"),
    ("source", "TEXT"),
    ("category", "TEXT"),
    ("whale_name", "TEXT"),
    ("whale_address", "TEXT"),
    ("whale_cluster", "TEXT"),
    ("market_title", "TEXT"),
    ("entry_price", "REAL"),
    ("simulated_size", "REAL"),
    ("entry_timestamp", "TEXT"),
    ("current_price", "REAL"),
    ("last_price_timestamp", "TEXT"),
    ("price_status", "TEXT"),
    ("price_source", "TEXT"),
    ("unrealized_pnl", "REAL DEFAULT 0.0"),
    ("realized_pnl", "REAL DEFAULT 0.0"),
    ("max_favorable_excursion", "REAL DEFAULT 0.0"),
    ("max_adverse_excursion", "REAL DEFAULT 0.0"),
    ("resolved", "INTEGER DEFAULT 0"),
    ("won", "INTEGER"),
    ("side", "TEXT"),
    ("category_action_v2", "TEXT"),
    ("experiment_tag", "TEXT DEFAULT 'v6.6-paper-portfolio'"),
    ("created_at", "TEXT DEFAULT CURRENT_TIMESTAMP"),
    ("updated_at", "TEXT DEFAULT CURRENT_TIMESTAMP"),
]

_PAPER_MARK_COLUMNS = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("position_id", "INTEGER NOT NULL"),
    ("mark_timestamp", "TEXT NOT NULL"),
    ("current_price", "REAL"),
    ("unrealized_pnl", "REAL"),
    ("realized_pnl", "REAL"),
    ("total_pnl", "REAL"),
    ("price_status", "TEXT"),
    ("price_source", "TEXT"),
]


def ensure_paper_portfolio_tables(db_path: str | None = None) -> None:
    """Create paper_positions and paper_position_marks tables if missing."""
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    col_defs = ", ".join(f"{name} {defn}" for name, defn in _PAPER_POSITION_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS paper_positions ({col_defs})")

    mark_defs = ", ".join(f"{name} {defn}" for name, defn in _PAPER_MARK_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS paper_position_marks ({mark_defs})")

    # Indexes
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pp_shadow_trade_id ON paper_positions(shadow_trade_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pp_resolved ON paper_positions(resolved)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pp_price_status ON paper_positions(price_status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pp_marks_position_id ON paper_position_marks(position_id)"
    )
    # Migrations for columns added in later versions
    try:
        conn.execute("ALTER TABLE paper_positions ADD COLUMN price_source TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE paper_position_marks ADD COLUMN price_source TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def _parse_outcome_token(metadata_json: str, instrument_id: str | None) -> str | None:
    """Extract outcome_token from metadata_json first, then instrument_id."""
    if metadata_json:
        try:
            meta = json.loads(metadata_json)
            if isinstance(meta, dict):
                for key in ("outcome_token", "token_id", "clob_token_id", "our_token_id"):
                    val = meta.get(key)
                    if val:
                        return str(val)
                tokens = meta.get("tokens")
                if isinstance(tokens, list) and tokens:
                    tok = tokens[0].get("token_id") if isinstance(tokens[0], dict) else None
                    if tok:
                        return str(tok)
        except Exception:
            pass
    if instrument_id:
        raw = instrument_id.replace(".POLYMARKET", "")
        if "-" in raw:
            parts = raw.rsplit("-", 1)
            if len(parts) == 2 and parts[1] and len(parts[1]) >= 10:
                return parts[1]
    return None


def _resolve_outcome_token(st_outcome_token: str | None, metadata_json: str, instrument_id: str | None) -> str | None:
    """Resolve outcome token with priority:
    1. shadow_trades.outcome_token column
    2. metadata_json $.outcome_token
    3. metadata_json $.token_id
    4. valid instrument_id token segment
    Never fall back to condition_id.
    """
    if st_outcome_token and st_outcome_token.strip():
        return st_outcome_token.strip()
    return _parse_outcome_token(metadata_json, instrument_id)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_side_basis(entry_price: float, side: str | None) -> tuple[float, str]:
    """Return cost basis and normalized side."""
    side_upper = (side or "BUY").upper()
    if side_upper in ("BUY", "YES", "LONG"):
        basis = entry_price
    elif side_upper in ("SELL", "NO", "SHORT"):
        basis = 1.0 - entry_price
    else:
        basis = entry_price
    return basis, side_upper


def _compute_unrealized_pnl(
    current_price: float,
    entry_price: float,
    simulated_size: float,
    side: str | None,
) -> tuple[float | None, str]:
    """Compute unrealized PnL; return (pnl, status)."""
    if simulated_size <= 0 or entry_price <= 0:
        return None, "invalid_basis"
    basis, side_upper = _get_side_basis(entry_price, side)
    if basis <= 0:
        return None, "invalid_basis"
    if side_upper in ("BUY", "YES", "LONG"):
        pnl = (current_price - entry_price) * simulated_size / basis
    else:
        # SELL/NO: long NO token; NO price = 1 - YES price
        current_no_price = 1.0 - current_price
        entry_no_price = basis
        pnl = (current_no_price - entry_no_price) * simulated_size / basis
    return round(pnl, 4), "ok"


def create_or_update_from_shadow_trade(shadow_trade_id: int, db_path: str) -> int | None:
    """Create or update a paper_positions row from an accepted shadow_trade.

    Returns the paper position id, or None on failure.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        row = conn.execute(
            """
            SELECT st.id, st.signal_id, st.snapshot_id, st.condition_id,
                   st.instrument_id, st.side, st.entry_price, st.position_size_usd,
                   st.whale_name, st.whale_address, st.market_title, st.category,
                   st.edge_score, st.confidence, st.signal_type, st.entry_timestamp,
                   st.metadata_json, st.block_reason, st.outcome_token,
                   ds.source, ds.category_action_v2
            FROM shadow_trades st
            LEFT JOIN decision_snapshots ds ON ds.id = st.snapshot_id
            WHERE st.id = ?
            """,
            (shadow_trade_id,),
        ).fetchone()

        if row is None:
            log.warning("Paper portfolio: shadow_trade %s not found", shadow_trade_id)
            return None

        (
            _st_id, signal_id, snapshot_id, condition_id, instrument_id, side,
            entry_price, position_size_usd, whale_name, whale_address, market_title,
            category, _edge_score, _confidence, _signal_type, entry_timestamp,
            metadata_json, _block_reason, st_outcome_token, ds_source, ds_category_action_v2,
        ) = row

        outcome_token = _resolve_outcome_token(st_outcome_token, metadata_json or "", instrument_id)
        source = ds_source or ""
        category_action_v2 = ds_category_action_v2 or ""
        cat = category or ""

        if not outcome_token:
            price_status = "missing_outcome_token"
        else:
            price_status = "pending"

        now = _now_iso()

        # Upsert: if row exists for this shadow_trade_id, update; else insert
        existing = conn.execute(
            "SELECT id FROM paper_positions WHERE shadow_trade_id = ?",
            (shadow_trade_id,),
        ).fetchone()

        if existing:
            pos_id = existing[0]
            conn.execute(
                """
                UPDATE paper_positions SET
                    signal_id = ?, snapshot_id = ?, market_id = ?, condition_id = ?,
                    instrument_id = ?, outcome_token = ?, source = ?, category = ?,
                    whale_name = ?, whale_address = ?, market_title = ?, entry_price = ?,
                    simulated_size = ?, entry_timestamp = ?, price_status = ?,
                    side = ?, category_action_v2 = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    signal_id or "", snapshot_id, condition_id, condition_id,
                    instrument_id or "", outcome_token or "", source, cat,
                    whale_name or "", whale_address or "", market_title or "",
                    entry_price, position_size_usd, entry_timestamp or now,
                    price_status, side or "BUY", category_action_v2, now, pos_id,
                ),
            )
        else:
            cursor = conn.execute(
                """
                INSERT INTO paper_positions (
                    shadow_trade_id, signal_id, snapshot_id, market_id, condition_id,
                    instrument_id, outcome_token, source, category, whale_name,
                    whale_address, market_title, entry_price, simulated_size,
                    entry_timestamp, current_price, last_price_timestamp, price_status, price_source,
                    unrealized_pnl, realized_pnl, max_favorable_excursion,
                    max_adverse_excursion, resolved, won, side, category_action_v2,
                    experiment_tag, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shadow_trade_id, signal_id or "", snapshot_id, condition_id,
                    condition_id, instrument_id or "", outcome_token or "", source,
                    cat, whale_name or "", whale_address or "", market_title or "",
                    entry_price, position_size_usd, entry_timestamp or now,
                    None, None, price_status, "",
                    0.0, 0.0, 0.0, 0.0, 0, None, side or "BUY", category_action_v2,
                    "v6.6-paper-portfolio", now, now,
                ),
            )
            pos_id = cursor.lastrowid

        conn.commit()
        log.info(
            "PAPER_PORTFOLIO | created/updated position id=%s from shadow_trade=%s | status=%s",
            pos_id, shadow_trade_id, price_status,
        )
        return pos_id
    except Exception as e:
        log.error("PAPER_PORTFOLIO | create_or_update_from_shadow_trade failed: %s", e)
        return None
    finally:
        conn.close()


def create_paper_position_from_snapshot(snapshot_id: int, db_path: str) -> int | None:
    """Explicit helper to create a paper position from a decision_snapshot
    (including rejected snapshots for counterfactual analysis).

    Returns the paper position id, or None on failure.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        row = conn.execute(
            """
            SELECT id, signal_id, condition_id, side,
                   entry_price, position_size_usd, whale_name, whale_address,
                   market_title, category, metadata_json, source,
                   category_action_v2, timestamp
            FROM decision_snapshots
            WHERE id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        if row is None:
            log.warning("Paper portfolio: snapshot %s not found", snapshot_id)
            return None

        (
            snap_id, signal_id, condition_id, side,
            entry_price, position_size_usd, whale_name, whale_address,
            market_title, category, metadata_json, source,
            category_action_v2, timestamp,
        ) = row

        outcome_token = _parse_outcome_token(metadata_json or "", None)
        cat = category or ""
        now = _now_iso()
        price_status = "missing_outcome_token" if not outcome_token else "pending"

        cursor = conn.execute(
            """
            INSERT INTO paper_positions (
                shadow_trade_id, signal_id, snapshot_id, market_id, condition_id,
                instrument_id, outcome_token, source, category, whale_name,
                whale_address, market_title, entry_price, simulated_size,
                entry_timestamp, current_price, last_price_timestamp, price_status,
                unrealized_pnl, realized_pnl, max_favorable_excursion,
                max_adverse_excursion, resolved, won, side, category_action_v2,
                experiment_tag, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None, signal_id or "", snap_id, condition_id, condition_id,
                "", outcome_token or "", source or "", cat,
                whale_name or "", whale_address or "", market_title or "",
                entry_price, position_size_usd, timestamp or now,
                None, None, price_status,
                0.0, 0.0, 0.0, 0.0, 0, None, side or "BUY", category_action_v2 or "",
                "v6.6-paper-portfolio", now, now,
            ),
        )
        pos_id = cursor.lastrowid
        conn.commit()
        log.info(
            "PAPER_PORTFOLIO | explicit position id=%s from snapshot=%s | status=%s",
            pos_id, snapshot_id, price_status,
        )
        return pos_id
    except Exception as e:
        log.error("PAPER_PORTFOLIO | create_paper_position_from_snapshot failed: %s", e)
        return None
    finally:
        conn.close()


def _request_json(url: str, timeout: int = 15) -> tuple[Any | None, str]:
    """Return JSON payload plus status for a best-effort market-data request."""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "nautilus-paper-portfolio/1.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read()), "ok"
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None, "missing_price"
        return None, "api_error"
    except ssl.SSLError:
        return None, "api_error"
    except OSError:
        return None, "api_error"
    except Exception:
        return None, "api_error"


def _coerce_price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if 0.0 <= price <= 1.0:
        return price
    return None


def _price_from_midpoint(outcome_token: str) -> tuple[float | None, str, str]:
    data, status = _request_json(_CLOB_MIDPOINT_URL.format(outcome_token))
    if status != "ok":
        return None, status, "clob_midpoint"
    if not isinstance(data, dict):
        return None, "missing_price", "clob_midpoint"
    price = _coerce_price(data.get("mid") or data.get("midpoint") or data.get("price"))
    if price is None:
        return None, "missing_price", "clob_midpoint"
    return price, "ok", "clob_midpoint"


def _price_from_book(outcome_token: str) -> tuple[float | None, str, str]:
    data, status = _request_json(_CLOB_BOOK_URL.format(outcome_token))
    if status != "ok":
        return None, status, "clob_book"
    if not isinstance(data, dict):
        return None, "missing_price", "clob_book"

    bid_prices = [
        price for price in (_coerce_price(level.get("price")) for level in data.get("bids", []))
        if price is not None
    ]
    ask_prices = [
        price for price in (_coerce_price(level.get("price")) for level in data.get("asks", []))
        if price is not None
    ]
    best_bid = max(bid_prices) if bid_prices else None
    best_ask = min(ask_prices) if ask_prices else None
    if best_bid is not None and best_ask is not None:
        return round((best_bid + best_ask) / 2, 6), "ok", "clob_book"
    if best_bid is not None:
        return best_bid, "ok", "clob_book"
    if best_ask is not None:
        return best_ask, "ok", "clob_book"
    last_trade = _coerce_price(data.get("last_trade_price"))
    if last_trade is not None:
        return last_trade, "ok", "clob_book"
    return None, "missing_price", "clob_book"


def _price_from_data_api_last_trade(outcome_token: str, condition_id: str | None) -> tuple[float | None, str, str]:
    if not condition_id:
        return None, "missing_price", "data_api_last_trade"
    query = urllib.parse.urlencode({"market": condition_id, "limit": 20})
    data, status = _request_json(_DATA_API_TRADES_URL.format(query), timeout=12)
    if status != "ok":
        return None, status, "data_api_last_trade"
    if not isinstance(data, list) or not data:
        return None, "missing_price", "data_api_last_trade"

    exact_prices: list[float] = []
    complement_prices: list[float] = []
    for trade in data:
        if not isinstance(trade, dict):
            continue
        price = _coerce_price(trade.get("price"))
        if price is None:
            continue
        asset = str(trade.get("asset") or trade.get("token_id") or "")
        if asset == outcome_token:
            exact_prices.append(price)
        elif asset:
            complement_prices.append(round(1.0 - price, 6))

    if exact_prices:
        return exact_prices[0], "ok", "data_api_last_trade"
    if complement_prices:
        return complement_prices[0], "ok", "data_api_complement"
    return None, "missing_price", "data_api_last_trade"


def fetch_current_price(outcome_token: str, condition_id: str | None = None) -> tuple[float | None, str, str]:
    """Fetch a paper MTM price for a token.

    Returns (price, status, source) where status is one of:
      ok, missing_outcome_token, missing_price, api_error
    and source is the price source used.
    """
    if not outcome_token:
        return None, "missing_outcome_token", "missing_outcome_token"

    statuses: list[str] = []
    for fetcher in (
        lambda: _price_from_midpoint(outcome_token),
        lambda: _price_from_book(outcome_token),
        lambda: _price_from_data_api_last_trade(outcome_token, condition_id),
    ):
        price, status, source = fetcher()
        statuses.append(status)
        if status == "ok" and price is not None:
            return price, "ok", source

    if "api_error" in statuses:
        return None, "api_error", "api_error"
    return None, "missing_price", "missing_price"


def mark_to_market_position(position_id: int, db_path: str) -> dict[str, Any]:
    """Fetch current price and update a single paper position.

    Returns a summary dict with keys: updated (bool), price_status, unrealized_pnl.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        row = conn.execute(
            """
            SELECT entry_price, simulated_size, side, outcome_token, condition_id, unrealized_pnl,
                   realized_pnl, max_favorable_excursion, max_adverse_excursion,
                   resolved
            FROM paper_positions
            WHERE id = ?
            """,
            (position_id,),
        ).fetchone()
        if row is None:
            return {"updated": False, "price_status": "not_found", "unrealized_pnl": None}

        (
            entry_price, simulated_size, side, outcome_token, condition_id, prev_unrealized,
            realized_pnl, max_fav, max_adv, resolved,
        ) = row

        if resolved:
            return {"updated": False, "price_status": "resolved", "unrealized_pnl": prev_unrealized}

        if not outcome_token:
            # Check if this is already a legacy row - preserve that status
            current_status_row = conn.execute(
                "SELECT price_status FROM paper_positions WHERE id = ?",
                (position_id,)
            ).fetchone()
            
            current_status = current_status_row[0] if current_status_row else None
            new_status = current_status if current_status == "legacy_unpriceable_missing_token" else "missing_outcome_token"
            
            conn.execute(
                "UPDATE paper_positions SET price_status = ? WHERE id = ?",
                (new_status, position_id),
            )
            conn.commit()
            return {"updated": False, "price_status": new_status, "unrealized_pnl": prev_unrealized}

        current_price, price_status, price_source = fetch_current_price(outcome_token, condition_id)

        if price_status != "ok" or current_price is None:
            conn.execute(
                "UPDATE paper_positions SET price_status = ?, price_source = ?, last_price_timestamp = ? WHERE id = ?",
                (price_status, price_source, _now_iso(), position_id),
            )
            conn.commit()
            return {"updated": False, "price_status": price_status, "unrealized_pnl": prev_unrealized}

        unrealized_pnl, basis_status = _compute_unrealized_pnl(
            current_price, entry_price or 0.0, simulated_size or 0.0, side,
        )

        if basis_status == "invalid_basis":
            conn.execute(
                """
                UPDATE paper_positions SET
                    current_price = ?, last_price_timestamp = ?, price_status = ?
                WHERE id = ?
                """,
                (current_price, _now_iso(), "invalid_basis", position_id),
            )
            conn.commit()
            return {"updated": False, "price_status": "invalid_basis", "unrealized_pnl": unrealized_pnl}

        # Update MFE / MAE
        if unrealized_pnl is not None:
            new_max_fav = max(max_fav or 0.0, unrealized_pnl)
            new_max_adv = min(max_adv or 0.0, unrealized_pnl)
        else:
            new_max_fav = max_fav or 0.0
            new_max_adv = max_adv or 0.0

        now = _now_iso()
        conn.execute(
            """
            UPDATE paper_positions SET
                current_price = ?, last_price_timestamp = ?, price_status = ?, price_source = ?,
                unrealized_pnl = ?, max_favorable_excursion = ?,
                max_adverse_excursion = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                current_price, now, "ok", price_source, unrealized_pnl,
                new_max_fav, new_max_adv, now, position_id,
            ),
        )

        # Append mark
        total_pnl = (realized_pnl or 0.0) + (unrealized_pnl or 0.0)
        conn.execute(
            """
            INSERT INTO paper_position_marks (
                position_id, mark_timestamp, current_price, unrealized_pnl,
                realized_pnl, total_pnl, price_status, price_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (position_id, now, current_price, unrealized_pnl, realized_pnl or 0.0, total_pnl, "ok", price_source),
        )
        conn.commit()
        return {"updated": True, "price_status": "ok", "unrealized_pnl": unrealized_pnl}
    except Exception as e:
        log.error("PAPER_PORTFOLIO | mark_to_market failed for position %s: %s", position_id, e)
        return {"updated": False, "price_status": "error", "unrealized_pnl": None}
    finally:
        conn.close()


def resolve_paper_position_from_shadow_trade(shadow_trade_id: int, db_path: str) -> bool:
    """Resolve a paper position when its shadow trade resolves.

    Sets resolved=1, realized_pnl from shadow_trades.actual_pnl,
    unrealized_pnl=0, and appends a final mark.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        row = conn.execute(
            """
            SELECT pp.id, pp.unrealized_pnl, pp.realized_pnl, pp.outcome_token,
                   st.actual_pnl, st.won, st.winning_outcome
            FROM paper_positions pp
            JOIN shadow_trades st ON st.id = pp.shadow_trade_id
            WHERE pp.shadow_trade_id = ?
            """,
            (shadow_trade_id,),
        ).fetchone()

        if row is None:
            log.debug("No paper position for shadow_trade %s", shadow_trade_id)
            return False

        pos_id, prev_unrealized, prev_realized, outcome_token, actual_pnl, won, winning_outcome = row

        realized = actual_pnl if actual_pnl is not None else (prev_realized or 0.0)
        # When token outcome is knowable, set current_price to 1.0 if won else 0.0
        if won == 1:
            current_price = 1.0
        elif won == 0:
            current_price = 0.0
        else:
            current_price = None

        now = _now_iso()
        conn.execute(
            """
            UPDATE paper_positions SET
                resolved = 1, won = ?, realized_pnl = ?, unrealized_pnl = 0,
                current_price = ?, last_price_timestamp = ?, price_status = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (won, realized, current_price, now, "resolved", now, pos_id),
        )

        total_pnl = realized
        conn.execute(
            """
            INSERT INTO paper_position_marks (
                position_id, mark_timestamp, current_price, unrealized_pnl,
                realized_pnl, total_pnl, price_status, price_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (pos_id, now, current_price, 0.0, realized, total_pnl, "resolved", "resolved"),
        )
        conn.commit()
        log.info(
            "PAPER_PORTFOLIO | resolved position id=%s shadow_trade=%s won=%s realized=%s",
            pos_id, shadow_trade_id, won, realized,
        )
        return True
    except Exception as e:
        log.error("PAPER_PORTFOLIO | resolve failed for shadow_trade %s: %s", shadow_trade_id, e)
        return False
    finally:
        conn.close()


def mark_all_unresolved(db_path: str, limit: int | None = None) -> dict[str, Any]:
    """Run mark-to-market on all unresolved paper positions.

    Returns summary dict with counts.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        rows = conn.execute(
            """
            SELECT id FROM paper_positions
            WHERE resolved = 0
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit or -1,),
        ).fetchall()
    except Exception:
        conn.close()
        return {"updated": 0, "missing_price": 0, "stale": 0, "resolved": 0, "errors": 0, "total": 0}

    conn.close()

    updated = 0
    missing_price = 0
    stale = 0
    resolved_skipped = 0
    errors = 0
    now = datetime.now(timezone.utc)

    for (pos_id,) in rows:
        result = mark_to_market_position(pos_id, db_path)
        status = result.get("price_status")
        if status == "ok" and result.get("updated"):
            updated += 1
        elif status == "missing_price":
            missing_price += 1
        elif status == "resolved":
            resolved_skipped += 1
        elif status in ("api_error", "error"):
            errors += 1
        else:
            missing_price += 1

    # Stale check: only count tokenized rows whose last mark is > 30 min old
    conn2 = sqlite3.connect(str(db_path))
    conn2.execute("PRAGMA busy_timeout=5000")
    try:
        stale_mark = conn2.execute(
            """
            SELECT COUNT(*) FROM paper_positions
            WHERE resolved = 0
              AND outcome_token IS NOT NULL AND outcome_token != ''
              AND last_price_timestamp IS NOT NULL
              AND datetime(last_price_timestamp) < datetime(?)
            """,
            ((now - __import__("datetime").timedelta(minutes=30)).isoformat(),),
        ).fetchone()[0] or 0
        unpriceable_missing_token = conn2.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE resolved=0 AND (outcome_token IS NULL OR outcome_token='') AND price_status != 'legacy_unpriceable_missing_token'"
        ).fetchone()[0] or 0
        legacy_unpriceable_token = conn2.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE resolved=0 AND price_status = 'legacy_unpriceable_missing_token'"
        ).fetchone()[0] or 0
        unpriceable_no_market = conn2.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE resolved=0 AND outcome_token IS NOT NULL AND outcome_token!='' AND price_status IN ('missing_price','api_error')"
        ).fetchone()[0] or 0
    except Exception:
        stale_mark = 0
        unpriceable_missing_token = 0
        unpriceable_no_market = 0
    finally:
        conn2.close()

    return {
        "updated": updated,
        "missing_price": missing_price,
        "stale": stale_mark,
        "stale_mark": stale_mark,
        "unpriceable_missing_token": unpriceable_missing_token,
        "unpriceable_no_market": unpriceable_no_market,
        "legacy_unpriceable_token": legacy_unpriceable_token,
        "resolved": resolved_skipped,
        "errors": errors,
        "total": len(rows),
    }


def sync_resolved_from_shadow_trades(db_path: str, limit: int | None = None) -> int:
    """Find newly resolved shadow_trades and resolve their paper positions.

    Returns number of paper positions resolved.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        rows = conn.execute(
            """
            SELECT st.id FROM shadow_trades st
            JOIN paper_positions pp ON pp.shadow_trade_id = st.id
            WHERE st.resolved = 1 AND pp.resolved = 0
            ORDER BY st.id ASC
            LIMIT ?
            """,
            (limit or -1,),
        ).fetchall()
    except Exception:
        conn.close()
        return 0
    conn.close()

    resolved_count = 0
    for (shadow_id,) in rows:
        if resolve_paper_position_from_shadow_trade(shadow_id, db_path):
            resolved_count += 1
    return resolved_count


def sync_missing_from_shadow_trades(
    db_path: str, limit: int | None = None, dry_run: bool = False
) -> dict[str, Any]:
    """Find accepted shadow_trades without paper_positions and create them.

    Accepted shadow trades are those with block_reason indicating they passed
    the pipeline but were blocked by shadow mode (``shadow_mode_block``) or
    have no block_reason (legacy/empty). Rejected trades (e.g.
    ``sports_telemetry``, ``sports_quarantine``, ``circuit_breaker``) are
    skipped so they do not pollute the paper portfolio.

    Args:
        db_path: Path to the trades database.
        limit: Max shadow trades to process in this run.
        dry_run: When True, report what would be synced without writing.

    Returns:
        Summary dict with keys:
        - ``would_sync``: number of missing accepted shadow trades found.
        - ``synced``: number actually created/updated (0 in dry_run).
        - ``errors``: number of failures during creation.
        - ``ids``: list of shadow_trade_ids that were found.
    """
    ensure_paper_portfolio_tables(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        rows = conn.execute(
            """
            SELECT st.id FROM shadow_trades st
            LEFT JOIN paper_positions pp ON pp.shadow_trade_id = st.id
            WHERE pp.id IS NULL
              AND (
                  st.block_reason IS NULL
                  OR st.block_reason = ''
                  OR st.block_reason = 'shadow_mode_block'
              )
            ORDER BY st.id ASC
            LIMIT ?
            """,
            (limit or -1,),
        ).fetchall()
    except Exception:
        conn.close()
        return {"would_sync": 0, "synced": 0, "errors": 0, "ids": []}
    conn.close()

    synced = 0
    errors = 0
    ids: list[int] = []
    for (shadow_id,) in rows:
        ids.append(shadow_id)
        if dry_run:
            log.info(
                "PAPER_PORTFOLIO | dry-run: would sync shadow_trade=%s into paper_positions",
                shadow_id,
            )
            continue
        try:
            pos_id = create_or_update_from_shadow_trade(shadow_id, db_path)
            if pos_id is not None:
                synced += 1
            else:
                errors += 1
        except Exception as e:
            log.error(
                "PAPER_PORTFOLIO | sync_missing failed for shadow_trade %s: %s",
                shadow_id,
                e,
            )
            errors += 1

    if dry_run:
        log.info(
            "PAPER_PORTFOLIO | dry-run: would sync %d missing paper positions from shadow_trades",
            len(ids),
        )
    else:
        log.info(
            "PAPER_PORTFOLIO | synced %d missing paper positions from shadow_trades (errors=%d)",
            synced,
            errors,
        )

    return {
        "would_sync": len(ids),
        "synced": synced,
        "errors": errors,
        "ids": ids,
    }
