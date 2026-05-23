"""Whale Follower — Database operations.

Standalone functions for logging trades and recovering open positions
from the trades database. No class coupling — all state is passed as parameters.
"""

from __future__ import annotations

import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import sys

# Add project root for bitable_writer import
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from components.bitable_writer import write_trading_signal


_DEFAULT_DB_PATH = Path(__file__).parent.parent / "research" / "trades.db"


# Phase 1 validation columns to add to trades table
_PHASE1_COLUMNS = [
    ("detection_delay_ms", "INTEGER DEFAULT 0"),
    ("execution_delay_ms", "INTEGER DEFAULT 0"),
    ("fill_delay_ms", "INTEGER DEFAULT 0"),
    ("total_latency_ms", "INTEGER DEFAULT 0"),
    ("intended_entry_price", "REAL"),
    ("actual_fill_price", "REAL"),
    ("slippage_bps", "REAL DEFAULT 0"),
    ("fill_completion_pct", "REAL DEFAULT 100"),
    ("snapshot_id", "TEXT"),
    ("market_slug", "TEXT"),
    ("token_index", "INTEGER"),
    ("token0_outcome", "TEXT"),
    ("token1_outcome", "TEXT"),
]


def _ensure_db_schema(conn: sqlite3.Connection) -> None:
    """Create the trades table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            whale_name TEXT,
            whale_address TEXT,
            category TEXT NOT NULL,
            market_title TEXT,
            condition_id TEXT,
            token_id TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            position_size_usd REAL,
            kelly_fraction REAL,
            confidence REAL,
            edge_score REAL,
            signal_source TEXT,
            entry_reason TEXT,
            exit_reason TEXT,
            realized_pnl REAL,
            realized_return REAL,
            duration_seconds REAL,
            resolution_outcome TEXT,
            dispute_flag INTEGER DEFAULT 0,
            notes TEXT,
            instrument_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            detection_delay_ms INTEGER DEFAULT 0,
            execution_delay_ms INTEGER DEFAULT 0,
            fill_delay_ms INTEGER DEFAULT 0,
            total_latency_ms INTEGER DEFAULT 0,
            intended_entry_price REAL,
            actual_fill_price REAL,
            slippage_bps REAL DEFAULT 0,
            fill_completion_pct REAL DEFAULT 100,
            snapshot_id TEXT,
            paper_trade INTEGER DEFAULT 0,
            market_slug TEXT,
            token_index INTEGER,
            token0_outcome TEXT,
            token1_outcome TEXT
        )
    """)

    # Add paper_trade column to existing tables (no-op if column already exists)
    try:
        conn.execute("ALTER TABLE trades ADD COLUMN paper_trade INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # column already exists

    # Remove the UNIQUE INDEX on (whale_name, condition_id) — trade_id is already
    # the PRIMARY KEY and is inherently unique. Whale re-trading the same market
    # is a valid scenario (scaling in/out) that should NOT be blocked by a
    # unique constraint. Keeping only the non-unique index on whale_address.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trades_whale_condition "
        "ON trades(whale_name, condition_id)"
    )


    # Non-unique index: same whale can re-enter same market after exiting (different trade_id each time)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_whale_condition ON trades(whale_name, condition_id)")

def migrate_trades_db(db_path: Optional[Path] = None) -> bool:
    """Migrate trades database to add Phase 1 validation columns.

    Checks if columns exist and adds missing columns with ALTER TABLE.
    Safe to run multiple times.

    Args:
        db_path: Path to trades.db. Defaults to research/trades.db.

    Returns:
        True if migration succeeded, False on failure.
    """
    db = db_path if db_path else _DEFAULT_DB_PATH

    if not db.exists():
        # No database to migrate
        return True

    conn = None
    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")

        # Get existing columns
        cursor = conn.execute("PRAGMA table_info(trades)")
        existing_columns = {row[1] for row in cursor.fetchall()}

        # Add missing columns
        for col_name, col_def in _PHASE1_COLUMNS:
            if col_name not in existing_columns:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_def}")

        return True

    except Exception:
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def log_trade_to_db(
    *,
    trade_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    whale_name: str,
    whale_address: str,
    market_title: str,
    side: str,
    entry_price: float,
    position_size_usd: float,
    category: str,
    signal_source: str = "whale_tracker",
    edge_score: float = 0.0,
    confidence: float = 0.0,
    kelly_fraction: float = 0.0,
    entry_reason: str = "",
    instrument_id: str = "",
    condition_id: str = "",
    detection_delay_ms: int = 0,
    execution_delay_ms: int = 0,
    fill_delay_ms: int = 0,
    total_latency_ms: int = 0,
    intended_entry_price: Optional[float] = None,
    actual_fill_price: Optional[float] = None,
    slippage_bps: float = 0.0,
    fill_completion_pct: float = 100.0,
    snapshot_id: str = "",
    paper_trade: int = 0,
    db_path: Optional[str] = None,
    log_func: Optional[Callable[[str], None]] = None,
) -> Optional[str]:
    """Insert a new trade record into the trades database.

    Uses a transaction with explicit BEGIN/COMMIT and rollback on failure.

    Args:
        trade_id: UUID for the trade row. Auto-generated if None.
        timestamp: ISO-8601 timestamp string. Defaults to now (UTC).
        whale_name: Whale wallet name or identifier.
        whale_address: On-chain wallet address.
        market_title: Human-readable market title.
        side: "BUY" or "SELL".
        entry_price: Fill price per share.
        position_size_usd: Notional size in USD.
        category: Market category (e.g. "sports", "politics").
        signal_source: Source of the trading signal.
        edge_score: Calibrated edge score.
        confidence: Signal confidence (0–1).
        kelly_fraction: Applied Kelly fraction.
        entry_reason: Reason for entering the trade.
        instrument_id: Full instrument ID string.
        condition_id: Condition ID portion of the instrument ID.
        detection_delay_ms: Time from signal detection to order submission (ms).
        execution_delay_ms: Time from order submission to first fill (ms).
        fill_delay_ms: Time from first fill to complete fill (ms).
        total_latency_ms: Total time from signal to complete fill (ms).
        intended_entry_price: Price we intended to enter at.
        actual_fill_price: Actual average fill price.
        slippage_bps: Slippage in basis points.
        fill_completion_pct: Percentage of order filled (0-100).
        snapshot_id: ID of the validation snapshot.
        db_path: Path to trades.db. Defaults to research/trades.db.
        log_func: Optional logging callable for errors.

    Returns:
        The trade_id string on success, or None on failure.
    """
    conn = None
    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)

    if trade_id is None:
        trade_id = str(uuid.uuid4())
    if timestamp is None:
        timestamp = str(datetime.now(timezone.utc))

    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        _ensure_db_schema(conn)

        # ── Look up market_slug, token_index, token outcomes from slug mapping DB ──
        _market_slug = None
        _token_index = None
        _token0_outcome = None
        _token1_outcome = None
        try:
            _slug_conn = sqlite3.connect("/tmp/cid_slug_mapping.db")
            _row = _slug_conn.execute(
                "SELECT market_slug, token0_id, token0_outcome, token1_id, token1_outcome "
                "FROM slug_mapping WHERE condition_id = ?",
                (condition_id,),
            ).fetchone()
            _slug_conn.close()
            if _row:
                _market_slug, _token0_id, _token0_outcome, _token1_id, _token1_outcome = _row
                # Derive token_id from instrument_id (format: condition_id-token_id)
                _token_id = instrument_id.split("-")[1].split(".")[0] if "-" in (instrument_id or "") else ""
                _token_index = 0 if _token_id == _token0_id else 1
        except Exception:
            pass  # mapping DB unavailable — columns remain None

        conn.execute("BEGIN TRANSACTION")
        conn.execute("""
            INSERT OR IGNORE INTO trades (
                trade_id, timestamp, whale_name, whale_address,
                market_title, side, entry_price, position_size_usd,
                category, signal_source, edge_score, confidence,
                kelly_fraction, entry_reason, instrument_id, condition_id,
                detection_delay_ms, execution_delay_ms, fill_delay_ms,
                total_latency_ms, intended_entry_price, actual_fill_price,
                slippage_bps, fill_completion_pct, snapshot_id, paper_trade,
                market_slug, token_index, token0_outcome, token1_outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id,
            timestamp,
            whale_name,
            whale_address or whale_name,  # fallback to name if address empty
            market_title,
            side,
            entry_price,
            position_size_usd,
            category,
            signal_source,
            edge_score,
            confidence,
            kelly_fraction,
            entry_reason,
            instrument_id,
            condition_id,
            detection_delay_ms,
            execution_delay_ms,
            fill_delay_ms,
            total_latency_ms,
            intended_entry_price,
            actual_fill_price,
            slippage_bps,
            fill_completion_pct,
            snapshot_id,
            paper_trade,
            _market_slug,
            _token_index,
            _token0_outcome,
            _token1_outcome,
        ))
        conn.execute("COMMIT")
        conn.close()
        conn = None

        # Write to Trading Hub Bitable — AFTER commit so it can't roll back the trade record
        try:
            signal_type = "Bullish" if side == "BUY" else "Bearish"
            signal_entry = {
                "多行文本": f"{whale_name} | {market_title[:60]} | ${position_size_usd:.0f} | entry={entry_price:.4f}",
                "Confidence": int(confidence * 100) if confidence else 50,
                "Signal Type": signal_type,
                "Source": f"{signal_source} ({category})",
            }
            write_trading_signal(signal_entry)
        except Exception as bitable_error:
            if log_func:
                log_func(f"[Bitable] Write failed: {bitable_error}")

        if log_func:
            log_func(
                f"[DB] Logged trade: {whale_name} | {category} | "
                f"{market_title[:40]} | ${position_size_usd:.0f}"
            )
        return trade_id

    except Exception as db_error:
        if conn:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        if log_func:
            log_func(
                f"[DB] Transaction failed, rolled back: {db_error} | "
                f"trade_id={trade_id} | whale={whale_name} | "
                f"market={market_title[:40]} | size=${position_size_usd:.0f} | "
                f"entry={entry_price:.4f}"
            )
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def update_trade_latency_fields(
    trade_id: str,
    detection_delay_ms: int,
    execution_delay_ms: int,
    fill_delay_ms: int,
    total_latency_ms: int,
    slippage_bps: float,
    fill_completion_pct: float,
    db_path: str | None = None,
) -> bool:
    """Update latency and slippage fields for an existing trade.

    Called after compute_latencies() and compute_slippage() run in
    on_order_filled(), so the values are available even though
    log_trade_to_db() was called earlier with zero defaults.

    Args:
        trade_id: UUID of the trade to update.
        detection_delay_ms: Time from whale detection to order submission.
        execution_delay_ms: Time from submission to first fill.
        fill_delay_ms: Time from first fill to complete fill.
        total_latency_ms: Total whale detection to complete fill.
        slippage_bps: Slippage in basis points.
        fill_completion_pct: Fill completion percentage (0-100).
        db_path: Optional override for trades.db path.

    Returns:
        True on success, False on failure.
    """
    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    if not db.exists():
        return False
    conn = None
    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "UPDATE trades SET "
            "detection_delay_ms=?, execution_delay_ms=?, fill_delay_ms=?, "
            "total_latency_ms=?, slippage_bps=?, fill_completion_pct=? "
            "WHERE trade_id=?",
            (
                detection_delay_ms,
                execution_delay_ms,
                fill_delay_ms,
                total_latency_ms,
                slippage_bps,
                fill_completion_pct,
                trade_id,
            ),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def recover_open_positions(
    db_path: Optional[str] = None,
    log_func: Optional[Callable[[str], None]] = None,
    max_recovery_age_hours: float = 4.0,
) -> List[Dict[str, Any]]:
    """Reload unfinished positions from the trades DB.

    Reads rows that have no exit_reason (i.e. still open) and reconstructs
    a list of position info dicts suitable for populating an _open_positions
    registry.

    Only recovers trades newer than ``max_recovery_age_hours``. Older orphans
    (from crashed runs) are skipped — they will be cleaned from the DB on the
    next successful exit or by admin maintenance. This prevents stale orphans
    from filling ``_open_positions`` and blocking ``max_open_positions``.

    Args:
        db_path: Path to trades.db. Defaults to research/trades.db.
        log_func: Optional logging callable.
        max_recovery_age_hours: Skip orphans older than this many hours.
            Default 4.0 (matches ``WhaleFollowerConfig.max_hold_hours``).

    Returns:
        List of position dicts, each with keys: inst_key, whale_name,
        market_title, category, side, entry_price, size, entry_time,
        trade_id, condition_id, venue_position_id, edge_score.
    """
    from datetime import datetime, timezone

    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    if not db.exists():
        if log_func:
            log_func("[RECOVER] No trades DB found, skipping recovery")
        return []

    conn = None
    try:
        conn = sqlite3.connect(str(db))
        rows = conn.execute(
            "SELECT instrument_id, trade_id, whale_name, market_title, category, "
            "side, entry_price, position_size_usd, condition_id, edge_score, timestamp "
            "FROM trades WHERE exit_reason IS NULL "
            "AND instrument_id IS NOT NULL "
            "ORDER BY timestamp"
        ).fetchall()

        if not rows:
            if log_func:
                log_func("[RECOVER] No orphan positions to recover")
            return []

        now = datetime.now(timezone.utc)
        cutoff_seconds = max_recovery_age_hours * 3600
        recovered: List[Dict[str, Any]] = []
        skipped = 0

        for row in rows:
            inst_id, trade_id, whale_name, market_title, category, side, entry_price, size, cond_id, edge_score, ts_str = row

            # ── Skip stale orphans (crashed sandbox runs) ──────────────
            try:
                ts = datetime.fromisoformat(ts_str)
                age_seconds = (now - ts).total_seconds()
                if age_seconds > cutoff_seconds:
                    if log_func:
                        log_func(
                            f"[RECOVER] SKIP stale orphan ({age_seconds/3600:.1f}h old): "
                            f"{whale_name or '?'} | {market_title[:40] if market_title else inst_id[:40]}"
                        )
                    skipped += 1
                    continue
            except (ValueError, TypeError):
                pass  # can't parse timestamp → include anyway (safer to recover)

            # Try to parse as InstrumentId; fall back to raw string
            try:
                from nautilus_trader.model.identifiers import InstrumentId
                inst_key = str(InstrumentId.from_str(inst_id))
            except Exception:
                inst_key = inst_id

            recovered.append({
                "inst_key": inst_key,
                "whale_name": whale_name or "unknown",
                "market_title": market_title or inst_id[:80],
                "category": category or "Unknown",
                "side": side or "BUY",
                "entry_price": entry_price or 0.5,
                "size": size or 0.0,
                "entry_time": time.time(),  # use current time so exit timer can age-check properly
                "trade_id": trade_id,
                "condition_id": cond_id or "",
                "venue_position_id": "",
                "edge_score": edge_score or 0.0,
            })

        if log_func:
            log_func(
                f"[RECOVER] Recovered {len(recovered)} open positions from DB"
            )
        return recovered

    except Exception as e:
        if log_func:
            log_func(f"[RECOVER] Failed to recover open positions: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_category_pnl(
    db_path: Optional[str] = None,
) -> dict[str, float]:
    """Load cumulative realized P&L per category from the trades DB.

    Used at startup to initialize ``WhaleFollower._category_pnl`` so that
    category-level fade decisions are active immediately, not just after
    the first restart trade.

    Returns:
        Dict mapping category name -> cumulative realized P&L.
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "trades.db")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT category, SUM(realized_pnl) FROM trades "
            "WHERE category IS NOT NULL AND category != '' "
            "GROUP BY category"
        )
        return {row[0]: float(row[1]) for row in cur.fetchall()}
    except Exception:
        return {}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
