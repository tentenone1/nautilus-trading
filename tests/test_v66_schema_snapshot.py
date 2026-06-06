"""
Schema snapshot tests for active v6.6 tables.

Validates that all schema creation functions produce tables with the
expected columns. Uses an in-memory temp database — never connects
to production data/trades.db.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _required_columns(cursor, table: str) -> set[str]:
    """Return set of column names for a table."""
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


class TestTradesSchema(unittest.TestCase):
    """Schema snapshot for trades table."""

    @classmethod
    def setUpClass(cls):
        from strategies.wf_db_ops import _ensure_db_schema
        cls.conn = sqlite3.connect(":memory:")
        _ensure_db_schema(cls.conn)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_trades_table_exists(self):
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertIn(("trades",), tables)

    def test_trades_has_key_columns(self):
        cols = _required_columns(self.conn, "trades")
        for col in ("trade_id", "timestamp", "whale_name", "category",
                     "market_title", "condition_id", "side", "entry_price",
                     "position_size_usd", "realized_pnl", "signal_source",
                     "snapshot_id", "paper_trade", "instrument_id"):
            self.assertIn(col, cols, f"trades missing column: {col}")


class TestShadowTradesSchema(unittest.TestCase):
    """Schema snapshot for shadow_trades table."""

    @classmethod
    def setUpClass(cls):
        from strategies.wf_db_ops import ensure_shadow_trades_table
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        ensure_shadow_trades_table(cls.tmp.name)
        cls.conn = sqlite3.connect(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.unlink(cls.tmp.name)

    def test_shadow_trades_table_exists(self):
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertIn(("shadow_trades",), tables)

    def test_shadow_trades_has_key_columns(self):
        cols = _required_columns(self.conn, "shadow_trades")
        for col in ("id", "snapshot_id", "condition_id", "side",
                     "entry_price", "position_size_usd", "whale_name",
                     "market_title", "category", "entry_timestamp",
                     "resolved", "won", "actual_pnl", "signal_id",
                     "outcome_token", "block_reason", "handler_step"):
            self.assertIn(col, cols, f"shadow_trades missing column: {col}")


class TestDecisionSnapshotsSchema(unittest.TestCase):
    """Schema snapshot for decision_snapshots table."""

    @classmethod
    def setUpClass(cls):
        from strategies.wf_db_ops import ensure_decision_snapshots_table
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        ensure_decision_snapshots_table(cls.tmp.name)
        cls.conn = sqlite3.connect(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.unlink(cls.tmp.name)

    def test_decision_snapshots_table_exists(self):
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertIn(("decision_snapshots",), tables)

    def test_decision_snapshots_has_key_columns(self):
        cols = _required_columns(self.conn, "decision_snapshots")
        for col in ("id", "timestamp", "signal_id", "source", "category",
                     "market_title", "condition_id", "whale_name",
                     "final_decision", "reject_reason", "position_size_usd",
                     "shadow_mode", "category_action", "category_action_v2",
                     "passed_execution_checks"):
            self.assertIn(col, cols, f"decision_snapshots missing column: {col}")


class TestPaperPositionsSchema(unittest.TestCase):
    """Schema snapshot for paper_positions table."""

    @classmethod
    def setUpClass(cls):
        from strategies.wf_paper_portfolio import ensure_paper_portfolio_tables
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        ensure_paper_portfolio_tables(cls.tmp.name)
        cls.conn = sqlite3.connect(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.unlink(cls.tmp.name)

    def test_paper_positions_table_exists(self):
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertIn(("paper_positions",), tables)

    def test_paper_positions_has_required_columns(self):
        cols = _required_columns(self.conn, "paper_positions")
        required = (
            "signal_id", "snapshot_id", "market_id", "condition_id",
            "outcome_token", "source", "category", "whale_name",
            "market_title", "entry_price", "simulated_size",
            "entry_timestamp", "current_price", "unrealized_pnl",
            "realized_pnl", "max_favorable_excursion",
            "max_adverse_excursion", "resolved", "won",
            "category_action_v2", "experiment_tag",
        )
        for col in required:
            self.assertIn(col, cols, f"paper_positions missing column: {col}")

    def test_paper_positions_has_price_source(self):
        cols = _required_columns(self.conn, "paper_positions")
        self.assertIn("price_source", cols)


class TestPaperPositionMarksSchema(unittest.TestCase):
    """Schema snapshot for paper_position_marks table."""

    @classmethod
    def setUpClass(cls):
        from strategies.wf_paper_portfolio import ensure_paper_portfolio_tables
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        ensure_paper_portfolio_tables(cls.tmp.name)
        cls.conn = sqlite3.connect(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        os.unlink(cls.tmp.name)

    def test_paper_position_marks_table_exists(self):
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        self.assertIn(("paper_position_marks",), tables)

    def test_paper_position_marks_has_required_columns(self):
        cols = _required_columns(self.conn, "paper_position_marks")
        required = (
            "position_id", "mark_timestamp", "current_price",
            "price_status", "price_source",
        )
        for col in required:
            self.assertIn(col, cols, f"paper_position_marks missing column: {col}")

    def test_paper_position_marks_has_pnl_columns(self):
        cols = _required_columns(self.conn, "paper_position_marks")
        for col in ("unrealized_pnl", "realized_pnl", "total_pnl"):
            self.assertIn(col, cols, f"paper_position_marks missing column: {col}")


if __name__ == "__main__":
    unittest.main()
