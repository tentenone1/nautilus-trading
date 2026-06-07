"""Tests for v6.6 paper portfolio missing-shadow-sync.

These tests verify the minimal backlog sync: accepted shadow_trades that do not
yet have paper_positions are safely synced, while rejected ones are skipped.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pytest

import strategies.wf_paper_portfolio as pp
import strategies.wf_db_ops as db_ops
from strategies.wf_constants import SHADOW_MODE


def _make_db() -> str:
    """Return a path to a fresh temporary SQLite database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _cleanup_db(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _setup_tables(db: str) -> None:
    """Create the minimal schema needed for sync tests."""
    # decision_snapshots (needed for LEFT JOIN in create_or_update_from_shadow_trade)
    db_ops.ensure_decision_snapshots_table(db)
    # shadow_trades
    db_ops.ensure_shadow_trades_table(db)
    # paper portfolio tables
    pp.ensure_paper_portfolio_tables(db)
    # trades table (for safety test) — create directly on same file
    conn = sqlite3.connect(db)
    db_ops._ensure_db_schema(conn)
    conn.close()


def _insert_shadow_trade(
    db: str,
    *,
    signal_id: str = "sig-1",
    snapshot_id: int | None = None,
    condition_id: str = "0xcond",
    instrument_id: str = "0xcond-tok.POLYMARKET",
    side: str = "BUY",
    entry_price: float = 0.50,
    position_size_usd: float = 100.0,
    whale_name: str = "TestWhale",
    market_title: str = "Test Market",
    category: str = "general",
    entry_timestamp: str = "",
    block_reason: str = "",
    outcome_token: str = "tok123",
    metadata_json: str = "",
    resolved: int = 0,
) -> int:
    """Insert a shadow_trades row directly and return its id."""
    conn = sqlite3.connect(db)
    cursor = conn.execute(
        """
        INSERT INTO shadow_trades (
            signal_id, snapshot_id, condition_id, instrument_id, side,
            entry_price, position_size_usd, whale_name, market_title, category,
            entry_timestamp, block_reason, outcome_token, metadata_json, resolved
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_id, snapshot_id, condition_id, instrument_id, side,
            entry_price, position_size_usd, whale_name, market_title, category,
            entry_timestamp or pp._now_iso(), block_reason, outcome_token,
            metadata_json, resolved,
        ),
    )
    row_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return row_id


class TestSyncMissingFromShadowTrades:
    def test_accepted_shadow_trade_missing_paper_row_creates_paper_position(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 1
            assert result["synced"] == 1
            assert result["errors"] == 0
            assert result["ids"] == [st_id]

            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT shadow_trade_id FROM paper_positions WHERE shadow_trade_id = ?",
                (st_id,),
            ).fetchone()
            conn.close()
            assert row is not None
            assert row[0] == st_id
        finally:
            _cleanup_db(db)

    def test_rejected_shadow_trade_does_not_create_paper_position(self):
        db = _make_db()
        try:
            _setup_tables(db)
            # Sports telemetry (rejected before sizing / non-accepted)
            st_id = _insert_shadow_trade(db, block_reason="sports_telemetry")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 0
            assert result["synced"] == 0

            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT 1 FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()
            conn.close()
            assert row is None
        finally:
            _cleanup_db(db)

    def test_sports_quarantine_blocked_trade_does_not_create_paper_position(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="sports_quarantine")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 0
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT 1 FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()
            conn.close()
            assert row is None
        finally:
            _cleanup_db(db)

    def test_circuit_breaker_blocked_trade_does_not_create_paper_position(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="circuit_breaker")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 0
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT 1 FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()
            conn.close()
            assert row is None
        finally:
            _cleanup_db(db)

    def test_shadow_mode_block_accepted_and_creates_paper_position(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="shadow_mode_block")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 1
            assert result["synced"] == 1
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT 1 FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()
            conn.close()
            assert row is not None
        finally:
            _cleanup_db(db)

    def test_existing_paper_position_is_not_duplicated(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="")

            # First sync creates the position
            pp.sync_missing_from_shadow_trades(db, dry_run=False)

            # Second sync must not duplicate
            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 0
            assert result["synced"] == 0

            conn = sqlite3.connect(db)
            count = conn.execute(
                "SELECT COUNT(*) FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()[0]
            conn.close()
            assert count == 1
        finally:
            _cleanup_db(db)

    def test_missing_outcome_token_handled_safely(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(
                db, block_reason="", outcome_token="", metadata_json="{}"
            )

            result = pp.sync_missing_from_shadow_trades(db, dry_run=False)

            assert result["would_sync"] == 1
            assert result["synced"] == 1
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT price_status FROM paper_positions WHERE shadow_trade_id = ?",
                (st_id,),
            ).fetchone()
            conn.close()
            assert row is not None
            assert row[0] == "missing_outcome_token"
        finally:
            _cleanup_db(db)

    def test_dry_run_performs_no_writes(self):
        db = _make_db()
        try:
            _setup_tables(db)
            st_id = _insert_shadow_trade(db, block_reason="")

            result = pp.sync_missing_from_shadow_trades(db, dry_run=True)

            assert result["would_sync"] == 1
            assert result["synced"] == 0
            assert result["errors"] == 0
            assert result["ids"] == [st_id]

            conn = sqlite3.connect(db)
            count = conn.execute(
                "SELECT COUNT(*) FROM paper_positions WHERE shadow_trade_id = ?", (st_id,)
            ).fetchone()[0]
            conn.close()
            assert count == 0
        finally:
            _cleanup_db(db)

    def test_limit_caps_number_of_syncs(self):
        db = _make_db()
        try:
            _setup_tables(db)
            for _ in range(5):
                _insert_shadow_trade(db, block_reason="")

            result = pp.sync_missing_from_shadow_trades(db, limit=2, dry_run=False)

            assert result["would_sync"] == 2
            assert result["synced"] == 2
            conn = sqlite3.connect(db)
            total = conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
            conn.close()
            assert total == 2
        finally:
            _cleanup_db(db)

    def test_trades_table_remains_untouched(self):
        db = _make_db()
        try:
            _setup_tables(db)
            conn = sqlite3.connect(db)
            conn.execute(
                """INSERT INTO trades (trade_id, timestamp, category)
                VALUES ('t1', '2026-01-01T00:00:00', 'general')"""
            )
            conn.commit()
            before = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            conn.close()

            st_id = _insert_shadow_trade(db, block_reason="")
            pp.sync_missing_from_shadow_trades(db, dry_run=False)

            conn = sqlite3.connect(db)
            after = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            conn.close()
            assert before == after == 1
        finally:
            _cleanup_db(db)

    def test_shadow_mode_constant_is_true(self):
        assert SHADOW_MODE is True


class TestUpdaterScript:
    def test_updater_calls_missing_shadow_sync_before_mtm(self):
        """Verify that update_v66_paper_portfolio.py calls sync_missing_from_shadow_trades
        before mark_all_unresolved, and that resolution sync is in between."""
        import scripts.update_v66_paper_portfolio as updater

        call_order: list[str] = []

        def _fake_missing_sync(db_path, limit=None, dry_run=False):
            call_order.append("missing_sync")
            return {"would_sync": 0, "synced": 0, "errors": 0, "ids": []}

        def _fake_resolved_sync(db_path, limit=None):
            call_order.append("resolved_sync")
            return 0

        def _fake_mtm(db_path, limit=None):
            call_order.append("mtm")
            return {
                "updated": 0,
                "missing_price": 0,
                "stale_mark": 0,
                "unpriceable_missing_token": 0,
                "unpriceable_no_market": 0,
                "resolved": 0,
                "errors": 0,
                "total": 0,
            }

        with patch.object(updater, "sync_missing_from_shadow_trades", _fake_missing_sync), \
             patch.object(updater, "sync_resolved_from_shadow_trades", _fake_resolved_sync), \
             patch.object(updater, "mark_all_unresolved", _fake_mtm), \
             patch.object(sys, "argv", ["updater"]):
            rc = updater.main()

        assert rc == 0
        assert call_order == ["missing_sync", "resolved_sync", "mtm"]

    def test_updater_dry_run_returns_zero_without_writes(self):
        """Dry-run must call missing sync with dry_run=True and skip resolved/MTM."""
        import scripts.update_v66_paper_portfolio as updater

        call_log: list[tuple[str, dict]] = []

        def _fake_missing_sync(db_path, limit=None, dry_run=False):
            call_log.append(("missing_sync", {"dry_run": dry_run}))
            return {"would_sync": 3, "synced": 0, "errors": 0, "ids": [1, 2, 3]}

        def _fake_resolved_sync(db_path, limit=None):
            call_log.append(("resolved_sync", {}))
            return 0

        def _fake_mtm(db_path, limit=None):
            call_log.append(("mtm", {}))
            return {
                "updated": 0,
                "missing_price": 0,
                "stale_mark": 0,
                "unpriceable_missing_token": 0,
                "unpriceable_no_market": 0,
                "resolved": 0,
                "errors": 0,
                "total": 0,
            }

        with patch.object(updater, "sync_missing_from_shadow_trades", _fake_missing_sync), \
             patch.object(updater, "sync_resolved_from_shadow_trades", _fake_resolved_sync), \
             patch.object(updater, "mark_all_unresolved", _fake_mtm), \
             patch.object(sys, "argv", ["updater", "--dry-run"]):
            rc = updater.main()

        assert rc == 0
        assert call_log == [("missing_sync", {"dry_run": True})]
        # Resolved sync and MTM must NOT be called in dry-run
        assert not any(name == "resolved_sync" for name, _ in call_log)
        assert not any(name == "mtm" for name, _ in call_log)

    def test_updater_dry_run_reports_would_sync_count(self):
        """Dry-run must report the number of missing trades it found."""
        import scripts.update_v66_paper_portfolio as updater

        captured: dict = {}

        def _fake_missing_sync(db_path, limit=None, dry_run=False):
            captured["dry_run"] = dry_run
            captured["would_sync"] = 5
            return {"would_sync": 5, "synced": 0, "errors": 0, "ids": [1, 2, 3, 4, 5]}

        with patch.object(updater, "sync_missing_from_shadow_trades", _fake_missing_sync), \
             patch.object(updater, "sync_resolved_from_shadow_trades"), \
             patch.object(updater, "mark_all_unresolved"), \
             patch.object(sys, "argv", ["updater", "--dry-run"]):
            rc = updater.main()

        assert rc == 0
        assert captured["dry_run"] is True
        assert captured.get("would_sync") == 5

    def test_updater_normal_mode_still_calls_all_three(self):
        """Normal (non-dry-run) mode must call missing, resolved, and MTM."""
        import scripts.update_v66_paper_portfolio as updater

        call_log: list[str] = []

        def _fake_missing_sync(db_path, limit=None, dry_run=False):
            call_log.append("missing_sync")
            return {"would_sync": 0, "synced": 0, "errors": 0, "ids": []}

        def _fake_resolved_sync(db_path, limit=None):
            call_log.append("resolved_sync")
            return 0

        def _fake_mtm(db_path, limit=None):
            call_log.append("mtm")
            return {
                "updated": 0,
                "missing_price": 0,
                "stale_mark": 0,
                "unpriceable_missing_token": 0,
                "unpriceable_no_market": 0,
                "resolved": 0,
                "errors": 0,
                "total": 0,
            }

        with patch.object(updater, "sync_missing_from_shadow_trades", _fake_missing_sync), \
             patch.object(updater, "sync_resolved_from_shadow_trades", _fake_resolved_sync), \
             patch.object(updater, "mark_all_unresolved", _fake_mtm), \
             patch.object(sys, "argv", ["updater"]):
            rc = updater.main()

        assert rc == 0
        assert call_log == ["missing_sync", "resolved_sync", "mtm"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
