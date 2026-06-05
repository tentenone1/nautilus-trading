"""Focused tests for shadow ledger resolution integrity."""
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.wf_db_ops import ensure_shadow_trades_table
from strategies import wf_shadow_ledger as ledger


def test_resolve_shadow_trade_uses_existing_schema_columns(monkeypatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        ensure_shadow_trades_table(db_path=db)
        conn = sqlite3.connect(db)
        conn.execute(
            """
            INSERT INTO shadow_trades (
                id, signal_id, condition_id, side, entry_price, position_size_usd,
                whale_name, market_title, category, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "sig-1", "0xabc", "BUY", 0.25, 100.0, "TestWhale", "Will test resolve?", "general", 0),
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(ledger, "_get_db_path", lambda: os.path.abspath(db))
        monkeypatch.setattr(
            ledger,
            "poll_market_resolution",
            lambda condition_id: {"resolved": True, "outcome": "YES"},
        )

        assert ledger.resolve_shadow_trade(1, "0xabc") is True

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT resolved, winning_outcome, hypothetical_pnl, actual_pnl, actual_return, won FROM shadow_trades WHERE id = 1"
        ).fetchone()
        conn.close()

        assert row == (1, "YES", 300.0, 300.0, 3.0, 1)
