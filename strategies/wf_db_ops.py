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

# Column definitions for the decision_snapshots table used in the
# signal-funnel observability layer (Phase 0 diagnostics).
_DECISION_SNAPSHOT_COLUMNS = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("timestamp", "TEXT NOT NULL"),
    ("signal_id", "TEXT"),
    ("source", "TEXT"),
    ("category", "TEXT"),
    # P0 FIX: raw/normalized/category_confidence — distinguish between category
    # as received from source vs after fallback inference, with confidence flag.
    # raw_category: what the signal source sent (may be empty string for sybil).
    # normalized_category: category after fallback inference from market_title.
    # category_confidence: 1.0 = from source, 0.5 = inferred from market_title.
    ("raw_category", "TEXT"),
    ("normalized_category", "TEXT"),
    ("category_confidence", "REAL DEFAULT 1.0"),
    ("market_title", "TEXT"),
    ("condition_id", "TEXT"),
    ("whale_name", "TEXT"),
    ("whale_address", "TEXT"),
    ("signal_type", "TEXT"),           # "COPY" or "FADE"
    ("edge_score", "REAL"),
    ("whale_wr", "REAL"),
    ("whale_sample_size", "INTEGER"),
    ("confidence", "REAL"),
    ("side", "TEXT"),                   # "BUY" or "SELL"
    # Gate results (1=passed, 0=rejected, -1=not evaluated)
    ("passed_category_filter", "INTEGER DEFAULT -1"),
    ("passed_quarantine", "INTEGER DEFAULT -1"),
    ("passed_blacklist", "INTEGER DEFAULT -1"),
    ("passed_edge_threshold", "INTEGER DEFAULT -1"),
    ("passed_fade_eligibility", "INTEGER DEFAULT -1"),
    ("passed_risk_manager", "INTEGER DEFAULT -1"),
    ("passed_execution_checks", "INTEGER DEFAULT -1"),
    ("passed_position_limits", "INTEGER DEFAULT -1"),
    ("passed_pnl_gate", "INTEGER DEFAULT -1"),
    ("passed_correlation_gate", "INTEGER DEFAULT -1"),
    ("passed_capital_pool", "INTEGER DEFAULT -1"),
    # Decision
    ("final_decision", "TEXT"),         # "TRADE", "REJECT", "SHADOW_TRADE"
    ("reject_reason", "TEXT"),
    ("position_size_usd", "REAL"),
    ("config_version", "TEXT"),
    ("shadow_mode", "INTEGER DEFAULT 0"),
    ("metadata_json", "TEXT"),
    # Phase 2: whale_category_classifier — per-category action for this signal
    ("category_action", "TEXT"),                    # FOLLOW | FADE | NEUTRAL | INSUFFICIENT_DATA
    ("category_action_confidence", "REAL"),         # 0.0–1.0
]

# Column definitions for the shadow_trades table.
# Records every SHADOW_TRADE signal for later resolution scoring.
# Populated proactively when SHADOW_MODE blocks execution (position_manager.py),
# and updated when Polymarket resolves the market.
_SHADOW_TRADE_COLUMNS = [
    ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    ("snapshot_id", "INTEGER"),           # FK to decision_snapshots.id
    ("condition_id", "TEXT NOT NULL"),
    ("instrument_id", "TEXT"),           # Full instrument ID
    ("side", "TEXT"),                   # "BUY" or "SELL"
    ("entry_price", "REAL"),             # Simulated entry price (current mid at signal time)
    ("position_size_usd", "REAL"),      # Kelly-computed size
    ("whale_name", "TEXT"),
    ("whale_address", "TEXT"),
    ("market_title", "TEXT"),
    ("category", "TEXT"),
    ("edge_score", "REAL"),
    ("confidence", "REAL"),
    ("signal_type", "TEXT"),            # "COPY" or "FADE"
    ("entry_timestamp", "TEXT"),         # When signal was received (from decision_snapshots.timestamp)
    ("config_version", "TEXT"),
    # Resolution fields (updated by wf_shadow_ledger.py)
    ("resolution_polled_at", "TEXT"),
    ("resolved", "INTEGER DEFAULT 0"),  # 1=resolved, 0=pending
    ("resolution_timestamp", "TEXT"),    # When market closed on Polymarket
    ("winning_outcome", "TEXT"),
    ("winning_token_id", "TEXT"),
    ("losing_outcome", "TEXT"),
    ("losing_token_id", "TEXT"),
    # Scoring
    ("actual_pnl", "REAL"),            # Calculated P&L based on resolution
    ("actual_return", "REAL"),          # Return %
    ("won", "INTEGER"),                 # 1=won, 0=lost, NULL=pending
    ("resolution_source", "TEXT"),      # "clob_api", "gamma_api", or NULL
    ("last_error", "TEXT"),             # Last polling error message
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

        # Ensure decision_snapshots table exists (Phase 0 observability)
        ensure_decision_snapshots_table(str(db))
        return True

    except Exception:
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Shadow Trade Ledger ─────────────────────────────────────────────────────────

def ensure_shadow_trades_table(db_path: str | None = None) -> None:
    """Create the shadow_trades table if it does not exist.

    Also creates indexes on condition_id and resolved to support fast
    polling queries.

    Args:
        db_path: Path to trades.db. Defaults to data/trades.db.
    """
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA journal_mode=WAL")
    col_defs = ", ".join(f"{name} {defn}" for name, defn in _SHADOW_TRADE_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS shadow_trades ({col_defs})")
    for idx_col in ("condition_id", "resolved", "entry_timestamp", "config_version"):
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_st_{idx_col} "
            f"ON shadow_trades({idx_col})"
        )
    conn.commit()
    conn.close()


def insert_shadow_trade(
    *,
    snapshot_id: int | None = None,
    condition_id: str,
    instrument_id: str = "",
    side: str = "",
    entry_price: float = 0.0,
    position_size_usd: float = 0.0,
    whale_name: str = "",
    whale_address: str = "",
    market_title: str = "",
    category: str = "",
    edge_score: float = 0.0,
    confidence: float = 0.0,
    signal_type: str = "",
    entry_timestamp: str = "",
    config_version: str = "",
    db_path: str | None = None,
) -> bool:
    """Insert a shadow trade record for a SHADOW_TRADE signal.

    Called proactively from position_manager.py when SHADOW_MODE blocks execution,
    and from wf_shadow_ledger.py on startup to backfill existing SHADOW_TRADE
    decision snapshots that don't yet have a shadow_trades row.

    Args:
        snapshot_id: FK to decision_snapshots.id.
        condition_id: Market condition ID (required).
        instrument_id: Full instrument ID string.
        side: "BUY" or "SELL".
        entry_price: Simulated entry price.
        position_size_usd: Kelly-computed position size.
        whale_name: Whale name.
        whale_address: Whale on-chain address.
        market_title: Human-readable market title.
        category: Market category.
        edge_score: Calibrated edge score.
        confidence: Signal confidence (0–1).
        signal_type: "COPY" or "FADE".
        entry_timestamp: When signal was received (from decision_snapshots.timestamp).
        config_version: Config version tag at signal time.
        db_path: Path to trades.db.

    Returns:
        True on success, False on failure.
    """
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = None
    try:
        ensure_shadow_trades_table(str(db))
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("BEGIN TRANSACTION")
        conn.execute(
            """INSERT INTO shadow_trades (
                snapshot_id, condition_id, instrument_id, side,
                entry_price, position_size_usd, whale_name, whale_address,
                market_title, category, edge_score, confidence, signal_type,
                entry_timestamp, config_version
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?
            )""",
            (
                snapshot_id, condition_id, instrument_id, side,
                entry_price, position_size_usd, whale_name, whale_address,
                market_title, category, edge_score, confidence, signal_type,
                entry_timestamp, config_version,
            ),
        )
        conn.execute("COMMIT")

        return True
    except Exception:
        if conn:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def update_shadow_trade_resolution(
    *,
    condition_id: str,
    resolved: bool,
    resolution_timestamp: str = "",
    winning_outcome: str = "",
    winning_token_id: str = "",
    losing_outcome: str = "",
    losing_token_id: str = "",
    actual_pnl: float | None = None,
    actual_return: float | None = None,
    won: bool | None = None,
    resolution_source: str = "",
    last_error: str = "",
    db_path: str | None = None,
) -> bool:
    """Update resolution fields for all pending shadow_trades rows matching condition_id.

    Called by wf_shadow_ledger.py after polling the Polymarket CLOB API.

    Args:
        condition_id: Market condition ID to update.
        resolved: Whether market is resolved.
        resolution_timestamp: When market resolved (ISO-8600).
        winning_outcome: Name of winning outcome.
        winning_token_id: Token ID of winning outcome.
        losing_outcome: Name of losing outcome.
        losing_token_id: Token ID of losing outcome.
        actual_pnl: Calculated P&L.
        actual_return: Calculated return %.
        won: 1=won, 0=lost.
        resolution_source: "clob_api" or "gamma_api".
        last_error: Last polling error message.
        db_path: Path to trades.db.

    Returns:
        True on success, False on failure.
    """
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    if not db.exists():
        return False
    conn = None
    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN TRANSACTION")
        conn.execute(
            """UPDATE shadow_trades SET
                resolved = ?,
                resolution_timestamp = ?,
                winning_outcome = ?,
                winning_token_id = ?,
                losing_outcome = ?,
                losing_token_id = ?,
                actual_pnl = ?,
                actual_return = ?,
                won = ?,
                resolution_source = ?,
                last_error = ?,
                resolution_polled_at = ?
            WHERE condition_id = ? AND resolved = 0""",
            (
                1 if resolved else 0,
                resolution_timestamp,
                winning_outcome,
                winning_token_id,
                losing_outcome,
                losing_token_id,
                actual_pnl,
                actual_return,
                1 if won is True else (0 if won is False else None),
                resolution_source,
                last_error,
                str(datetime.now(timezone.utc)),
                condition_id,
            ),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        if conn:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_pending_shadow_trades(
    db_path: str | None = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Fetch pending (unresolved) shadow_trades rows ordered by entry_timestamp ASC.

    Args:
        db_path: Path to trades.db.
        limit: Max rows to return (default 200).

    Returns:
        List of dicts with all shadow_trades columns.
    """
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    if not db.exists():
        return []
    conn = None
    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute(
            """SELECT * FROM shadow_trades
               WHERE resolved = 0
               ORDER BY entry_timestamp ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        cols = [desc[0] for desc in conn.execute("SELECT * FROM shadow_trades LIMIT 0").description]
        conn.close()
        return [dict(zip(cols, row)) for row in rows]
    except Exception:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return []


def backfill_shadow_trades_from_snapshots(db_path: str | None = None) -> int:
    """Backfill shadow_trades rows for any SHADOW_TRADE decision_snapshots that
    don't yet have a shadow_trades row.

    Called at wf_shadow_ledger startup to ensure completeness.

    Args:
        db_path: Path to trades.db.

    Returns:
        Number of rows inserted.
    """
    db = Path(db_path) if db_path else Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    if not db.exists():
        return 0
    conn = None
    try:
        ensure_shadow_trades_table(str(db))
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        # Find SHADOW_TRADE snapshots without a shadow_trades row
        rows = conn.execute(
            """SELECT ds.id, ds.condition_id, ds.side, ds.entry_price,
                      ds.position_size_usd, ds.whale_name, ds.whale_address,
                      ds.market_title, ds.category, ds.edge_score, ds.confidence,
                      ds.signal_type, ds.timestamp, ds.config_version
               FROM decision_snapshots ds
               LEFT JOIN shadow_trades st ON st.snapshot_id = ds.id
               WHERE ds.final_decision = 'SHADOW_TRADE'
                 AND st.id IS NULL
                 AND ds.condition_id IS NOT NULL
                 AND ds.condition_id != ''"""
        ).fetchall()
        if not rows:
            return 0
        inserted = 0
        for row in rows:
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute(
                    """INSERT INTO shadow_trades (
                        snapshot_id, condition_id, side, entry_price,
                        position_size_usd, whale_name, whale_address,
                        market_title, category, edge_score, confidence,
                        signal_type, entry_timestamp, config_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    row[1:] + (row[0],)[:0] + row,  # skip ds.id (row[0]), add at end
                )
                # Actually insert with snapshot_id at position 0
                conn.execute(
                    """INSERT INTO shadow_trades (
                        snapshot_id, condition_id, side, entry_price,
                        position_size_usd, whale_name, whale_address,
                        market_title, category, edge_score, confidence,
                        signal_type, entry_timestamp, config_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]),
                )
                conn.execute("COMMIT")
                inserted += 1
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
        conn.close()
        return inserted
    except Exception:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return 0



def ensure_decision_snapshots_table(db_path: str | None = None) -> None:
    """Create the decision_snapshots table if it does not exist.

    Also creates indexes on timestamp, source, category, reject_reason,
    and final_decision to support fast funnel-analysis queries.

    Args:
        db_path: Path to trades.db. Defaults to research/trades.db.
    """
    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA journal_mode=WAL")
    col_defs = ", ".join(f"{name} {defn}" for name, defn in _DECISION_SNAPSHOT_COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS decision_snapshots ({col_defs})")
    # P0 FIX: Migrate existing tables — add raw_category, normalized_category,
    # category_confidence columns if they don't exist yet (ALTER TABLE is safe
    # to re-run; SQLite ignores duplicate ADD COLUMN errors).
    _NEW_DECISION_SNAPSHOT_COLS = {
        "raw_category": "TEXT",
        "normalized_category": "TEXT",
        "category_confidence": "REAL DEFAULT 1.0",
        # Phase 2: whale_category_classifier columns
        "category_action": "TEXT",
        "category_action_confidence": "REAL",
    }
    for col_name, col_def in _NEW_DECISION_SNAPSHOT_COLS.items():
        try:
            conn.execute(f"ALTER TABLE decision_snapshots ADD COLUMN {col_name} {col_def}")
        except sqlite3.OperationalError:
            pass  # column already exists — no-op
    # Indexes for funnel analysis
    for idx_col in ("timestamp", "source", "category", "reject_reason", "final_decision"):
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_ds_{idx_col} "
            f"ON decision_snapshots({idx_col})"
        )
    conn.commit()
    conn.close()


def insert_decision_snapshot(
    *,
    signal_id: str = "",
    source: str = "",
    category: str = "",
    market_title: str = "",
    condition_id: str = "",
    whale_name: str = "",
    whale_address: str = "",
    signal_type: str = "",
    edge_score: float = 0.0,
    whale_wr: float = 0.0,
    whale_sample_size: int = 0,
    confidence: float = 0.0,
    side: str = "",
    passed_category_filter: int = -1,
    passed_quarantine: int = -1,
    passed_blacklist: int = -1,
    passed_edge_threshold: int = -1,
    passed_fade_eligibility: int = -1,
    passed_risk_manager: int = -1,
    passed_execution_checks: int = -1,
    passed_position_limits: int = -1,
    passed_pnl_gate: int = -1,
    passed_correlation_gate: int = -1,
    passed_capital_pool: int = -1,
    final_decision: str = "REJECT",
    reject_reason: str = "",
    position_size_usd: float = 0.0,
    config_version: str = "",
    shadow_mode: int = 0,
    metadata_json: str = "",
    # P0 FIX: raw_category, normalized_category, category_confidence fields
    # populated by wf_signal_handler._snap to track category provenance.
    raw_category: str = "",
    normalized_category: str = "",
    category_confidence: float = 1.0,
    # Phase 2: whale_category_classifier — FOLLOW / FADE / NEUTRAL / INSUFFICIENT_DATA
    category_action: str = "INSUFFICIENT_DATA",
    category_action_confidence: float = 0.0,
    # trace_id: accepted but not stored separately — already embedded in metadata_json
    trace_id: str = "",
    db_path: str | None = None,
) -> bool:
    """Insert a decision snapshot record into the decision_snapshots table.

    Records the full signal metadata and per-gate pass/fail results so the
    signal funnel can be reconstructed and analysed after the fact.

    Gate field semantics:
        1  = passed
        0  = rejected
       -1  = not evaluated (gate skipped)

    Args:
        signal_id: Unique identifier for this signal event.
        source: Signal source (e.g. "whale_tracker").
        category: Market category.
        market_title: Human-readable market title.
        condition_id: Condition ID of the market.
        whale_name: Whale wallet name or label.
        whale_address: Whale on-chain address.
        signal_type: "COPY" or "FADE".
        edge_score: Calibrated edge score.
        whale_wr: Whale win-rate estimate.
        whale_sample_size: Number of trades used for whale_wr.
        confidence: Signal confidence (0–1).
        side: "BUY" or "SELL".
        passed_category_filter: Category allowlist gate result.
        passed_quarantine: Quarantine expiry gate result.
        passed_blacklist: Blacklist/whitelist gate result.
        passed_edge_threshold: Minimum edge score gate result.
        passed_fade_eligibility: Fade-eligibility gate result.
        passed_risk_manager: Risk-manager review result.
        passed_execution_checks: Execution feasibility result.
        passed_position_limits: Per-whale / per-market position limit result.
        passed_pnl_gate: Category P&L gate result.
        passed_correlation_gate: Correlation filter result.
        passed_capital_pool: Capital availability gate result.
        final_decision: "TRADE", "REJECT", or "SHADOW_TRADE".
        reject_reason: Human-readable rejection description.
        position_size_usd: Position size computed by the sizer.
        config_version: Config version tag at time of decision.
        shadow_mode: 1 if shadow/simulation mode, 0 otherwise.
        metadata_json: Optional arbitrary JSON metadata blob.
        db_path: Path to trades.db. Defaults to research/trades.db.

    Returns:
        True on success, False on failure.
    """
    import json

    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)

    timestamp = str(datetime.now(timezone.utc))

    conn = None
    try:
        ensure_decision_snapshots_table(str(db))
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")

        conn.execute("BEGIN TRANSACTION")
        conn.execute(
            f"""INSERT INTO decision_snapshots (
                timestamp, signal_id, source, category, raw_category,
                normalized_category, category_confidence, market_title, condition_id,
                whale_name, whale_address, signal_type, edge_score, whale_wr,
                whale_sample_size, confidence, side,
                passed_category_filter, passed_quarantine, passed_blacklist,
                passed_edge_threshold, passed_fade_eligibility, passed_risk_manager,
                passed_execution_checks, passed_position_limits, passed_pnl_gate,
                passed_correlation_gate, passed_capital_pool,
                final_decision, reject_reason, position_size_usd,
                config_version, shadow_mode, metadata_json,
                category_action, category_action_confidence
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?,
            )""",
            (
                timestamp, signal_id, source, category, raw_category,
                normalized_category, category_confidence, market_title, condition_id,
                whale_name, whale_address, signal_type, edge_score, whale_wr,
                whale_sample_size, confidence, side,
                passed_category_filter, passed_quarantine, passed_blacklist,
                passed_edge_threshold, passed_fade_eligibility, passed_risk_manager,
                passed_execution_checks, passed_position_limits, passed_pnl_gate,
                passed_correlation_gate, passed_capital_pool,
                final_decision, reject_reason, position_size_usd,
                config_version, shadow_mode, metadata_json,
                category_action, category_action_confidence,
            ),
        )
        conn.execute("COMMIT")

        # ── Shadow trade ledger ──────────────────────────────────────────────
        # When a SHADOW_TRADE snapshot is recorded, also create a shadow_trades row
        # so we can track hypothetical P&L via Polymarket market resolution polling.
        if final_decision == "SHADOW_TRADE":
            try:
                from strategies.wf_shadow_ledger import insert_shadow_trade
                combined = f"{market_title}|{category}".lower()
                _SPORTS_KW = (
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
                is_sports = int(any(k in combined for k in _SPORTS_KW))
                step = "step2_handler" if "handler_step2" in (metadata_json or "") else "step1_pipeline"
                insert_shadow_trade(
                    signal_id=signal_id,
                    snapshot_id=None,
                    condition_id=condition_id,
                    instrument_id=None,
                    side=side or "BUY",
                    entry_price=0.0,
                    position_size_usd=position_size_usd,
                    whale_name=whale_name,
                    whale_address=whale_address,
                    market_title=market_title,
                    category=category,
                    edge_score=edge_score,
                    confidence=confidence,
                    signal_type=signal_type or "COPY",
                    entry_timestamp=timestamp,
                    config_version=config_version,
                    is_sports=is_sports,
                    block_reason=reject_reason,
                    handler_step=step,
                    metadata_json=metadata_json,
                )
            except Exception:
                pass  # Don't let shadow ledger errors affect the primary path

        return True

    except Exception:
        if conn:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
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
    config_version: str = "",
    whale_type: str = "",
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

        # ── Guard: slippage_bps is meaningless without an actual fill price ──
        # Store SQL NULL so historical queries never confuse a missing fill
        # with a 0-bps or sentinel value (-10000).
        _slippage_bps: float | None = None if actual_fill_price is None else slippage_bps

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
                market_slug, token_index, token0_outcome, token1_outcome, config_version, whale_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            _slippage_bps,
            fill_completion_pct,
            snapshot_id,
            paper_trade,
            _market_slug,
            _token_index,
            _token0_outcome,
            _token1_outcome,
            config_version,
            whale_type,
        ))
        conn.execute("COMMIT")

        # ── Slippage sanity check (v5.6) ──────────────────────────────────────
        # Warn if slippage_bps exceeds ±500 bps — this should already be clamped
        # in state_manager.py, but flag it as a data quality signal.
        if actual_fill_price is not None and slippage_bps is not None:
            if abs(slippage_bps) > 500:
                _warn_msg = (
                    f"SUSPECT_SLIPPAGE | trade_id={trade_id} | "
                    f"slippage_bps={slippage_bps:.1f} exceeds ±500 | clamped upstream"
                )
                if log_func:
                    log_func(f"[DB WARNING] {_warn_msg}")

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
    actual_fill_price: float | None = None,
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
        actual_fill_price: Actual average fill price from the market.
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
        # ── Guard: slippage_bps is only meaningful with an actual fill price ──────
        _slippage_bps: float | None = slippage_bps if actual_fill_price is not None else None

        if actual_fill_price is not None:
            conn.execute(
                "UPDATE trades SET "
                "detection_delay_ms=?, execution_delay_ms=?, fill_delay_ms=?, "
                "total_latency_ms=?, slippage_bps=?, fill_completion_pct=?, "
                "actual_fill_price=? "
                "WHERE trade_id=?",
                (
                    detection_delay_ms,
                    execution_delay_ms,
                    fill_delay_ms,
                    total_latency_ms,
                    _slippage_bps,
                    fill_completion_pct,
                    actual_fill_price,
                    trade_id,
                ),
            )
        else:
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
                    _slippage_bps,
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


def verify_config_version_integrity(
    db_path: str | None = None,
    hours: int = 24,
    log_func: Callable[[str], None] | None = None,
) -> int:
    """Alert if any trades in the last N hours have missing/unknown config_version.

    Called once at startup and every 4 hours to detect schema drift where
    new code writes 'unknown' config_version due to a deployment or migration issue.

    Args:
        db_path: Path to trades.db. Defaults to research/trades.db.
        hours: Check trades in the last N hours. Default 24.
        log_func: Optional logging callable for warnings.

    Returns:
        Number of trades with missing/unknown config_version.
    """
    db = Path(db_path) if db_path else _DEFAULT_DB_PATH
    if not db.exists():
        return 0
    conn = None
    try:
        conn = sqlite3.connect(str(db))
        conn.execute("PRAGMA busy_timeout=5000")
        cursor = conn.execute("""
            SELECT COUNT(*) FROM trades
            WHERE (config_version IS NULL OR config_version = '' OR config_version = 'unknown')
              AND timestamp > datetime('now', '-? hours')
        """, (hours,))
        row = cursor.fetchone()
        count = row[0] if row else 0
        conn.close()
        if count > 0:
            msg = (
                f"CONFIG_VERSION_INTEGRITY | {count} trades in last {hours}h "
                f"with missing/unknown config_version"
            )
            if log_func:
                log_func(f"[DB WARNING] {msg}")
        return count
    except Exception as e:
        if log_func:
            log_func(f"[DB WARNING] CONFIG_VERSION_INTEGRITY check failed: {e}")
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
