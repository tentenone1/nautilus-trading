"""Tests for canonical_perf view — unified performance source."""
import os
import sys
import sqlite3
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.wf_db_ops import (
    ensure_canonical_perf_view,
    ensure_shadow_trades_table,
    insert_decision_snapshot,
)


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE trades (
            trade_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            condition_id TEXT, instrument_id TEXT, side TEXT,
            entry_price REAL, position_size_usd REAL,
            whale_name TEXT, whale_address TEXT, market_title TEXT,
            category TEXT, edge_score REAL, confidence REAL,
            signal_source TEXT, config_version TEXT,
            realized_pnl REAL, realized_return REAL
        )
    """)
    conn.execute("""
        CREATE TABLE shadow_trades (
            id INTEGER PRIMARY KEY,
            condition_id TEXT, instrument_id TEXT, side TEXT,
            entry_price REAL, position_size_usd REAL,
            whale_name TEXT, whale_address TEXT, market_title TEXT,
            category TEXT, edge_score REAL, confidence REAL,
            signal_type TEXT, entry_timestamp TEXT, config_version TEXT,
            resolved INTEGER DEFAULT 0,
            hypothetical_pnl REAL, winning_outcome TEXT,
            actual_pnl REAL, actual_return REAL, won INTEGER
        )
    """)
    conn.commit()


def _seed_data(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO trades (trade_id, timestamp, whale_name, realized_pnl, realized_return, signal_source) VALUES (?, ?, ?, ?, ?, ?)",
        ("live-1", "2026-05-01T00:00:00+00:00", "LiveWhale", 100.0, 0.50, "known_whale"),
    )
    conn.execute(
        "INSERT INTO trades (trade_id, timestamp, whale_name, realized_pnl, realized_return, signal_source) VALUES (?, ?, ?, ?, ?, ?)",
        ("live-2", "2026-05-01T01:00:00+00:00", "LiveWhale", -50.0, -0.25, "known_whale"),
    )
    conn.execute(
        "INSERT INTO shadow_trades (id, whale_name, resolved, hypothetical_pnl, actual_pnl, actual_return, winning_outcome, won) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (1, "ShadowWhale", 1, 75.0, 75.0, 0.75, "YES", 1),
    )
    conn.execute(
        "INSERT INTO shadow_trades (id, whale_name, resolved, hypothetical_pnl, actual_pnl, actual_return, winning_outcome, won) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (2, "ShadowWhale", 1, -25.0, -25.0, -0.25, "NO", 0),
    )
    conn.execute(
        "INSERT INTO shadow_trades (id, whale_name, resolved, hypothetical_pnl, winning_outcome, won) VALUES (?, ?, ?, ?, ?, ?)",
        (3, "UnresolvedWhale", 0, None, None, None),
    )
    conn.commit()


def test_canonical_perf_has_actual_aliases() -> None:
    """Verify canonical_perf exposes both actual_pnl/actual_return and realized_pnl/realized_return."""
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db)
        _create_tables(conn)
        _seed_data(conn)
        conn.close()

        ensure_canonical_perf_view(db_path=db)

        conn = sqlite3.connect(db)
        # Check shadow rows have both aliases
        shadow = conn.execute(
            "SELECT realized_pnl, realized_return, actual_pnl, actual_return, source FROM canonical_perf WHERE source = 'shadow' ORDER BY id"
        ).fetchone()
        conn.close()

        assert shadow[0] == shadow[2], f"shadow realized_pnl={shadow[0]} != actual_pnl={shadow[2]}"
        assert shadow[1] == shadow[3], f"shadow realized_return={shadow[1]} != actual_return={shadow[3]}"


def test_canonical_perf_includes_both_sources() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db)
        _create_tables(conn)
        _seed_data(conn)
        conn.close()

        ensure_canonical_perf_view(db_path=db)

        conn = sqlite3.connect(db)
        rows = conn.execute(
            "SELECT whale_name, realized_pnl, realized_return, won, source FROM canonical_perf ORDER BY source, id"
        ).fetchall()
        conn.close()

        assert len(rows) == 4, f"Expected 4 rows (2 live + 2 resolved shadow), got {len(rows)}"
        assert rows[0] == ("LiveWhale", 100.0, 0.50, 1, "live")
        assert rows[1] == ("LiveWhale", -50.0, -0.25, 0, "live")
        assert rows[2] == ("ShadowWhale", 75.0, 0.75, 1, "shadow"), f"Got {rows[2]}"
        assert rows[3] == ("ShadowWhale", -25.0, -0.25, 0, "shadow"), f"Got {rows[3]}"


def test_canonical_perf_excludes_unresolved_shadow() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db)
        _create_tables(conn)
        _seed_data(conn)
        conn.close()

        ensure_canonical_perf_view(db_path=db)

        conn = sqlite3.connect(db)
        rows = conn.execute(
            "SELECT whale_name FROM canonical_perf WHERE whale_name = 'UnresolvedWhale'"
        ).fetchall()
        conn.close()

        assert len(rows) == 0, "Unresolved shadow trades should be excluded from canonical_perf"


def test_canonical_perf_count_used_for_bootstrap() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db)
        _create_tables(conn)
        conn.execute(
            "INSERT INTO shadow_trades (id, whale_name, resolved, hypothetical_pnl) VALUES (1, 'A', 1, 10.0)"
        )
        conn.commit()
        conn.close()

        ensure_canonical_perf_view(db_path=db)

        conn = sqlite3.connect(db)
        count = conn.execute("SELECT COUNT(*) FROM canonical_perf").fetchone()[0]
        conn.close()

        assert count == 1, f"Expected 1 resolved shadow in canonical count, got {count}"
        # Verify wf_constants.BOOTSTRAP_MODE would see this
        assert count >= 1


def test_ensure_shadow_trades_table_creates_queryable_canonical_perf() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE trades (
                trade_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                whale_name TEXT,
                whale_address TEXT,
                category TEXT,
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
                config_version TEXT
            )
        """)
        conn.commit()
        conn.close()

        ensure_shadow_trades_table(db_path=db)

        conn = sqlite3.connect(db)
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='canonical_perf'"
        ).fetchone()
        assert conn.execute("SELECT COUNT(*) FROM canonical_perf").fetchone()[0] == 0
        conn.close()


def test_shadow_trade_row_links_to_inserted_decision_snapshot(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        ensure_shadow_trades_table(db_path=db)

        import strategies.wf_shadow_ledger as ledger

        monkeypatch.setattr(ledger, "_get_db_path", lambda: os.path.abspath(db))

        ok = insert_decision_snapshot(
            signal_id="sig-1",
            source="known_whale",
            category="general",
            market_title="Will test pass?",
            condition_id="0xabc",
            whale_name="TestWhale",
            whale_address="0xwallet",
            side="BUY",
            final_decision="SHADOW_TRADE",
            reject_reason="shadow_mode_block",
            position_size_usd=10.0,
            db_path=db,
        )

        assert ok
        conn = sqlite3.connect(db)
        snapshot_id = conn.execute(
            "SELECT id FROM decision_snapshots WHERE signal_id = 'sig-1'"
        ).fetchone()[0]
        shadow_snapshot_id = conn.execute(
            "SELECT snapshot_id FROM shadow_trades WHERE signal_id = 'sig-1'"
        ).fetchone()[0]
        conn.close()

        assert shadow_snapshot_id == snapshot_id
