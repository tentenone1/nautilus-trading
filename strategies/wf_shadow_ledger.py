"""Shadow Trade Ledger — tracks would-have-traded signals and their hypothetical P&L.

Every signal rejected with reject_reason IN ('sports_telemetry', 'shadow_mode_block')
creates a shadow_trades row. We poll the Polymarket Gamma API for market resolution
and compute what the P&L would have been.

Schema lives in trades.db (shadow_trades table). See end of module for schema docs.
"""

from __future__ import annotations

import json
import logging
import socket
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _get_config_version() -> str:
    try:
        from strategies.wf_constants import ACTIVE_CONFIG_VERSION
        return ACTIVE_CONFIG_VERSION
    except Exception:
        return "unknown"


def _get_shadow_mode() -> bool:
    try:
        from strategies.wf_constants import SHADOW_MODE
        return SHADOW_MODE
    except Exception:
        return False


def _get_poll_batch_size() -> int:
    try:
        from strategies.wf_constants import SHADOW_TRADE_POLL_BATCH_SIZE
        return SHADOW_TRADE_POLL_BATCH_SIZE
    except Exception:
        return 50

log = logging.getLogger(__name__)

# ── DB path ────────────────────────────────────────────────────────────────────

def _get_db_path() -> Path:
    return Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")


# ── Core insert ───────────────────────────────────────────────────────────────

def insert_shadow_trade(
    signal_id: str,
    snapshot_id: int | None,
    condition_id: str,
    instrument_id: str | None,
    side: str,
    entry_price: float,
    position_size_usd: float,
    whale_name: str,
    whale_address: str,
    market_title: str,
    category: str,
    edge_score: float,
    confidence: float,
    signal_type: str,
    entry_timestamp: str | None = None,
    config_version: str | None = None,
    is_sports: int = 0,
    block_reason: str = "",
    handler_step: str = "",
    metadata_json: str = "",
) -> int | None:
    """Insert a shadow_trades row. Returns the row ID, or None on failure.

    For signals with position_size_usd=0 (e.g. SPORTS_TELEMETRY signals that
    were rejected before sizing was computed), we record size=0 and mark them
    as 'size unknown' in metadata. Hypothetical P&L will be computed as a
    percentage return per dollar when the market resolves.
    """
    import sqlite3

    db = _get_db_path()
    now = datetime.now(timezone.utc).isoformat()
    ts = entry_timestamp or now

    # Derive is_sports from category/market_title if not explicitly set
    if is_sports == 0:
        combined = f"{market_title}|{category}".lower()
        _SPORTS_KEYWORDS = (
            "nba", "nfl", "mlb", "nhl", "ncaaf", "ncaab", "ufc", "boxing",
            "tennis", "soccer", "football", "basketball", "baseball", "hockey",
            "sports", "game ", "championship", "finals", "playoffs", "season",
            "knicks", "cavaliers", "celtics", "lakers", "warriors", "bulls",
            "spread:", "point spread", "over/under", "moneyline", "totals",
            "nuggets", "mavericks", "heat", "spurs", "nets", "bucks", "raptors",
            "eagles", "chiefs", "49ers", "cowboys", "packers", "patriots",
            "raiders", "yankees", "red sox", "dodgers", "cubs", "giants",
            "astros", "braves", "rangers", "oilers", "penguins", "maple leafs",
            "devils", "avalanche", "diamondbacks", "guardians", "phillies",
            "mariners", "twins", "orioles",
        )
        is_sports = int(any(k in combined for k in _SPORTS_KEYWORDS))

    meta: dict[str, Any] = {}
    if metadata_json:
        try:
            meta = json.loads(metadata_json)
        except Exception:
            pass
    if position_size_usd <= 0:
        meta["size_unknown"] = True

    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        cursor = conn.execute(
            """
            INSERT INTO shadow_trades (
                signal_id, snapshot_id, condition_id, instrument_id, side,
                entry_price, position_size_usd, whale_name, whale_address,
                market_title, category, edge_score, confidence, signal_type,
                entry_timestamp, config_version, is_sports, block_reason,
                handler_step, metadata_json, resolved, hypothetical_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
            """,
            (
                signal_id, snapshot_id, condition_id, instrument_id, side,
                entry_price, position_size_usd, whale_name, whale_address,
                market_title, category, edge_score, confidence, signal_type,
                ts, config_version, is_sports, block_reason, handler_step,
                json.dumps(meta) if meta else metadata_json,
            ),
        )
        conn.commit()
        row_id = cursor.lastrowid
        log.info(
            f"SHADOW_LEDGER | inserted shadow_trade id={row_id} | "
            f"condition={condition_id[:20]} | block={block_reason} | size=${position_size_usd:.2f}"
        )
        return row_id
    except sqlite3.Error as e:
        log.error(f"SHADOW_LEDGER | insert failed: {e}")
        return None
    finally:
        conn.close()


# ── Gamma API polling ─────────────────────────────────────────────────────────

def poll_market_resolution(condition_id: str) -> dict[str, Any] | None:
    """Poll Polymarket Gamma API for market resolution.

    Returns a dict with keys: resolved (bool), outcome (str 'YES'/'NO'/None),
    closing_price (float), end_date_iso (str), or None on error.
    """
    import urllib.request

    url = f"https://gamma-api.polymarket.com/markets?condition_id={condition_id}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "nautilus-shadow-ledger/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        if not isinstance(data, list) or not data:
            return None

        m = data[0]
        is_resolved = bool(m.get("closed", False) or m.get("resolved", False))
        outcome = m.get("outcome", "") or ""
        # Polymarket sometimes stores the winning outcome in 'question' or as a list
        outcomes_raw = m.get("outcomes", "")
        try:
            outcomes_list = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        except Exception:
            outcomes_list = []

        # Determine winning outcome
        if is_resolved and outcome:
            winning = outcome.strip()
        elif is_resolved and outcomes_list:
            # If outcome field is empty but we have outcomes list,
            # try to determine winner from prices
            prices_raw = m.get("outcomePrices", "[]")
            try:
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else []
            except Exception:
                prices = []
            if prices and len(prices) >= 2:
                # Winner is the outcome with price closest to 1.0 (or > 0.5)
                try:
                    max_price_idx = max(range(len(prices)), key=lambda i: float(prices[i]))
                    winning = outcomes_list[max_price_idx] if max_price_idx < len(outcomes_list) else ""
                except Exception:
                    winning = outcomes_list[0] if outcomes_list else ""
            else:
                winning = outcomes_list[0] if outcomes_list else ""
        else:
            winning = ""

        normalized = winning.upper() if winning else ""
        if normalized not in ("YES", "NO"):
            normalized = None

        # closing_price: parse outcomePrices array, use max price as the "closing" price
        prices_raw = m.get("outcomePrices", "[]")
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
            closing_price = max((float(p) for p in prices if p), default=0.0)
        except Exception:
            closing_price = 0.0

        return {
            "resolved": is_resolved,
            "outcome": normalized,
            "closing_price": closing_price,
            "end_date_iso": m.get("endDateIso", ""),
            "question": m.get("question", ""),
            "market_title": m.get("title", ""),
        }

    # P2 FIX: Distinguish transient SSL/network errors from permanent failures.
    # Transient errors (SSL handshake failures, connection resets, timeouts) mean
    # the market is still open — don't log as a warning, just return None so the
    # batch poller treats it as "still pending" rather than "failed".
    # Permanent errors (HTTP 4xx, JSON parse, etc.) are real failures and should
    # still warn.
    except ssl.SSLError as e:
        # e.g. "Connection reset by peer", CERTIFICATE_VERIFY_FAILED
        log.debug(f"SHADOW_LEDGER | SSL transient for {condition_id[:20]}: {e}")
        return None
    except OSError as e:
        # e.g. ConnectionResetError, timeout, network unreachable
        log.debug(f"SHADOW_LEDGER | network transient for {condition_id[:20]}: {e}")
        return None
    except Exception as e:
        log.warning(f"SHADOW_LEDGER | poll failed for {condition_id[:20]}: {e}")
        return None


# ── Hypothetical P&L computation ──────────────────────────────────────────────

def compute_hypothetical_pnl(
    side: str,
    entry_price: float,
    position_size_usd: float,
    outcome: str,
) -> float | None:
    """Compute hypothetical P&L for a shadow trade.

    Polymarket YES tokens pay $1.00 at resolution if the outcome occurs,
    otherwise $0. A YES position of $X at price P_yes pays:
      - If YES wins: $X * (1/P_yes)   (return on investment)
      - If YES loses: -$X             (total loss)

    Equivalently: pnl = size * ((1/price) - 1) for winning YES,
                  pnl = -size         for losing YES

    NO tokens are the inverse: a NO position pays when the outcome does NOT occur.

    Args:
        side: 'BUY' means long YES; 'SELL' means short YES (long NO)
        entry_price: the price paid for the token (0.0 to 1.0)
        position_size_usd: dollar amount of the position
        outcome: 'YES' or 'NO'

    Returns:
        Hypothetical P&L in USD, or None if insufficient data.
    """
    if entry_price <= 0 or entry_price >= 1:
        return None
    if position_size_usd <= 0:
        return None
    if outcome not in ("YES", "NO"):
        return None

    side_upper = side.upper()
    price = entry_price

    if side_upper == "BUY":
        # Long YES
        if outcome == "YES":
            return position_size_usd * (1.0 / price - 1.0)
        else:
            return -position_size_usd
    elif side_upper in ("SELL", "NO"):
        # Long NO = short YES. When YES wins, we lose; when NO wins, we gain.
        # A NO token at price P_yes pays $1 at resolution, cost basis = P_yes.
        # Return if NO wins = (1 - P_yes) / P_yes = 1/P_yes - 1
        # But since we paid (1-P_yes) for the NO token (NO price = 1 - YES price):
        no_price = 1.0 - price
        if no_price <= 0:
            return None
        if outcome == "NO":
            return position_size_usd * (1.0 / no_price - 1.0)
        else:
            return -position_size_usd
    return None


# ── Resolve a single shadow trade ─────────────────────────────────────────────

def resolve_shadow_trade(shadow_trade_id: int, condition_id: str) -> bool:
    """Poll market resolution and compute hypothetical_pnl for one shadow trade.

    Returns True if the trade was resolved (market closed with known outcome).
    """
    import sqlite3

    result = poll_market_resolution(condition_id)
    if result is None:
        return False

    market_resolved = int(result["resolved"])
    outcome = result["outcome"]

    if not market_resolved or outcome is None:
        # Market not yet resolved — update poll timestamp only
        # DEBUG: this is expected for long-duration sports futures; added for visibility
        log.debug(
            f"SHADOW_LEDGER | market still active: id={shadow_trade_id} "
            f"cond={condition_id[:40]} resolved={market_resolved} outcome={outcome}"
        )
        now = datetime.now(timezone.utc).isoformat()
        db = _get_db_path()
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute(
            "UPDATE shadow_trades SET resolution_polled_at = ? WHERE id = ?",
            (now, shadow_trade_id),
        )
        conn.commit()
        conn.close()
        return False

    # Fetch the shadow trade to get entry price, side, size
    db = _get_db_path()
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA busy_timeout = 5000")
    row = conn.execute(
        "SELECT side, entry_price, position_size_usd FROM shadow_trades WHERE id = ?",
        (shadow_trade_id,),
    ).fetchone()
    if row is None:
        conn.close()
        return False

    side, entry_price, position_size_usd = row
    conn.close()

    hypothetical_pnl = compute_hypothetical_pnl(side, entry_price, position_size_usd, outcome)

    now = datetime.now(timezone.utc).isoformat()
    conn2 = sqlite3.connect(str(db))
    conn2.execute("PRAGMA busy_timeout = 5000")
    conn2.execute(
        """
        UPDATE shadow_trades SET
            resolved = 1,
            resolution_timestamp = ?,
            winning_outcome = ?,
            hypothetical_pnl = ?,
            resolution_polled_at = ?,
            resolved_integer = 1
        WHERE id = ?
        """,
        (now, outcome, hypothetical_pnl, now, shadow_trade_id),
    )
    conn2.commit()
    conn2.close()

    log.info(
        f"SHADOW_LEDGER | resolved id={shadow_trade_id} | outcome={outcome} | "
        f"hypothetical_pnl=${hypothetical_pnl:.2f if hypothetical_pnl is not None else 'N/A'}"
    )
    return True


# ── Batch resolution polling ───────────────────────────────────────────────────

def poll_pending_shadow_trades(limit: int | None = None) -> dict[str, Any]:
    """Poll all unresolved shadow_trades (resolved=0), up to `limit`.

    Returns a summary dict.
    """
    import sqlite3

    db = _get_db_path()
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA busy_timeout = 5000")

    rows = conn.execute(
        "SELECT id, condition_id FROM shadow_trades WHERE resolved = 0 ORDER BY id LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()

    if not rows:
        return {"polled": 0, "resolved": 0, "pending": 0, "total_hypothetical_pnl": 0.0}

    resolved_count = 0
    pending_count = 0
    total_pnl = 0.0
    errors = 0

    for shadow_id, condition_id in rows:
        try:
            done = resolve_shadow_trade(shadow_id, condition_id)
            if done:
                resolved_count += 1
                # Fetch the computed pnl
                conn2 = sqlite3.connect(str(db))
                conn2.execute("PRAGMA busy_timeout = 5000")
                pnl_row = conn2.execute(
                    "SELECT hypothetical_pnl FROM shadow_trades WHERE id = ?",
                    (shadow_id,),
                ).fetchone()
                conn2.close()
                if pnl_row and pnl_row[0] is not None:
                    total_pnl += pnl_row[0]
            else:
                pending_count += 1
        except Exception as e:
            log.warning(f"SHADOW_LEDGER | resolve error id={shadow_id}: {e}")
            errors += 1

    log.info(
        f"SHADOW_LEDGER poll summary | polled={len(rows)} resolved={resolved_count} "
        f"pending={pending_count} errors={errors} total_hypothetical_pnl=${total_pnl:.2f}"
    )

    return {
        "polled": len(rows),
        "resolved": resolved_count,
        "pending": pending_count,
        "errors": errors,
        "total_hypothetical_pnl": total_pnl,
    }


# ── Backfill existing sports_telemetry signals ─────────────────────────────────

def backfill_sports_telemetry_signals() -> int:
    """Insert shadow_trade rows for all existing 'sports_telemetry' decision_snapshots.

    These were logged to decision_snapshots but not yet tracked in shadow_trades.
    Returns the number of rows inserted.
    """
    import sqlite3

    db = _get_db_path()
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA busy_timeout = 5000")

    # Check which signal_ids are already in shadow_trades
    existing = set(
        r[0] for r in conn.execute("SELECT signal_id FROM shadow_trades WHERE signal_id IS NOT NULL")
    )

    rows = conn.execute(
        """
        SELECT id, signal_id, condition_id, market_title, category, whale_name,
               side, edge_score, confidence, position_size_usd, signal_type,
               timestamp, shadow_mode, metadata_json
        FROM decision_snapshots
        WHERE reject_reason = 'sports_telemetry'
          AND signal_id NOT IN (SELECT signal_id FROM shadow_trades WHERE signal_id IS NOT NULL)
        LIMIT 200
        """,
    ).fetchall()
    conn.close()

    if not rows:
        log.info("SHADOW_LEDGER | backfill: no new sports_telemetry signals to backfill")
        return 0

    inserted = 0
    for row in rows:
        (snap_id, signal_id, condition_id, market_title, category, whale_name,
         side, edge_score, confidence, position_size_usd, signal_type,
         timestamp, shadow_mode, metadata_json) = row

        if signal_id in existing:
            continue

        block_reason = "sports_telemetry"
        handler_step = "step1_pipeline_sports_telemetry_mode"

        sid = insert_shadow_trade(
            signal_id=signal_id or "",
            snapshot_id=snap_id,
            condition_id=condition_id or "",
            instrument_id=None,
            side=side or "BUY",
            entry_price=0.0,  # Sizing wasn't computed when snapshot was written
            position_size_usd=position_size_usd or 0.0,
            whale_name=whale_name or "",
            whale_address="",
            market_title=market_title or "",
            category=category or "",
            edge_score=edge_score or 0.0,
            confidence=confidence or 0.0,
            signal_type=signal_type or "COPY",
            entry_timestamp=timestamp,
            config_version=_get_config_version(),
            is_sports=1,
            block_reason=block_reason,
            handler_step=handler_step,
            metadata_json=metadata_json or "",
        )
        if sid:
            inserted += 1
            existing.add(signal_id)

    log.info(f"SHADOW_LEDGER | backfill complete: {inserted} rows inserted")
    return inserted


# ── Schema reference ──────────────────────────────────────────────────────────
"""
CREATE TABLE shadow_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER,           -- FK to decision_snapshots.id
    condition_id TEXT NOT NULL,    -- Polymarket condition ID
    instrument_id TEXT,             -- Nautilus instrument ID
    side TEXT,                     -- BUY (long YES) / SELL (long NO)
    entry_price REAL,             -- Target price at signal time
    position_size_usd REAL,        -- Position size; 0 = unknown (pre-sizing reject)
    whale_name TEXT,
    whale_address TEXT,
    market_title TEXT,
    category TEXT,
    edge_score REAL,
    confidence REAL,
    signal_type TEXT,              -- COPY / FADE
    entry_timestamp TEXT,          -- ISO8601
    config_version TEXT,
    resolution_polled_at TEXT,     -- Last poll attempt
    resolved INTEGER DEFAULT 0,    -- 1 = market resolved
    resolution_timestamp TEXT,     -- When we resolved it
    winning_outcome TEXT,          -- YES / NO
    winning_token_id TEXT,
    losing_outcome TEXT,
    losing_token_id TEXT,
    actual_pnl REAL,               -- NULL for shadow trades (they don't execute)
    actual_return REAL,
    won INTEGER,                   -- 1/0/NULL
    resolution_source TEXT,
    last_error TEXT,
    -- Added by v6.2:
    signal_id TEXT,                -- Links to decision_snapshots.signal_id
    is_sports INTEGER DEFAULT 0,  -- 1 = sports signal
    block_reason TEXT,             -- sports_telemetry / shadow_mode_block / etc.
    handler_step TEXT,             -- step1_pipeline / step2_handler / j1_position_manager
    hypothetical_pnl REAL,          -- Computed once market resolves
    metadata_json TEXT,            -- JSON blob for extensible metadata
);
"""
