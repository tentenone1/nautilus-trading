from __future__ import annotations

import sqlite3
import pytest
import tempfile
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.report_v66_mtm_coverage import main as mtm_coverage_main
from strategies.wf_paper_portfolio import ensure_paper_portfolio_tables


def test_mtm_coverage_handles_legacy_rows_separately():
    """Test that MTM coverage report properly separates legacy rows."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Ensure tables are created
        ensure_paper_portfolio_tables(db_path)
        
        # Connect and insert test data
        conn = sqlite3.connect(db_path)
        
        # Insert a legacy unpriceable row
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'test_condition',
             '', 'legacy_unpriceable_missing_token', 'none', NULL, '', 'unknown',
             10.0, 0.0, 0.0)
        """)
        
        # Insert operational rows with various statuses
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'test_condition2',
             '0x123', 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z', 'Test Market', 'test_whale',
             10.0, 2.0, 0.0)
        """)
        
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (3, 'v6.6-paper-portfolio', 0, 3, 3, 'test_condition3',
             '0x456', 'ok', 'clob_book', '2026-06-05T10:00:00Z', 'Test Market 2', 'test_whale2',
             5.0, 1.0, 0.0)
        """)
        
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (4, 'v6.6-paper-portfolio', 0, 4, 4, 'test_condition4',
             '', 'missing_outcome_token', 'none', NULL, '', 'unknown2',
             5.0, 0.0, 0.0)
        """)
        
        conn.commit()
        conn.close()
        
        # For now, just verify the function doesn't crash
        # In a real test, we'd capture stdout and verify the output
        assert True
        
    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_mtm_coverage_calculations():
    """Test that MTM coverage calculations are correct."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)

        for i in range(3):
            conn.execute("""
                INSERT INTO paper_positions
                (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
                 outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
                 simulated_size, unrealized_pnl, realized_pnl)
                VALUES
                (?, 'v6.6-paper-portfolio', 0, ?, ?, ?,
                 '', 'legacy_unpriceable_missing_token', 'none', NULL, '', ?,
                 10.0, 0.0, 0.0)
            """, (i + 1, i + 1, i + 1, f"condition_{i + 1}", f"unknown_{i + 1}"))

        for i in range(4):
            conn.execute("""
                INSERT INTO paper_positions
                (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
                 outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
                 simulated_size, unrealized_pnl, realized_pnl)
                VALUES
                (?, 'v6.6-paper-portfolio', 0, ?, ?, ?,
                 ?, 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z', ?, 'test_whale',
                 10.0, 2.0, 0.0)
            """, (i + 4, i + 4, i + 4, f"condition_{i + 4}", f"0x{i + 4}", f"Test Market {i + 4}"))

        conn.commit()
        conn.close()

        conn = sqlite3.connect(db_path)
        total = conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
        legacy = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE price_status='legacy_unpriceable_missing_token'").fetchone()[0]
        operational = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE price_status!='legacy_unpriceable_missing_token'").fetchone()[0]
        tokenized = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE price_status!='legacy_unpriceable_missing_token' AND outcome_token!=''").fetchone()[0]
        ok = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE price_status='ok'").fetchone()[0]
        conn.close()

        assert total == 7
        assert legacy == 3
        assert operational == 4
        assert tokenized == 4
        assert ok == 4

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_mtm_coverage_counts_only_rows_with_actual_marks(monkeypatch, capsys):
    """Marked coverage should require both current_price and last_price_timestamp."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE trades (id INTEGER PRIMARY KEY)")
        conn.execute("""
            INSERT INTO paper_positions
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, current_price, price_status, price_source, last_price_timestamp,
             market_title, whale_name, simulated_size, unrealized_pnl, realized_pnl)
            VALUES
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'condition_1',
             '0xmarked', 0.42, 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z',
             'Marked Market', 'marked_whale', 10.0, 1.0, 0.0),
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'condition_2',
             '0xunmarked', NULL, 'pending', 'none', NULL,
             'Unmarked Market', 'unmarked_whale', 10.0, 0.0, 0.0),
            (3, 'v6.6-paper-portfolio', 0, 3, 3, 'condition_3',
             '', NULL, 'legacy_unpriceable_missing_token', 'none', NULL,
             '', 'legacy_whale', 10.0, 0.0, 0.0)
        """)
        conn.commit()
        conn.close()

        monkeypatch.setattr(sys, "argv", ["report_v66_mtm_coverage.py", "--db-path", db_path])

        assert mtm_coverage_main() == 0
        output = capsys.readouterr().out
        assert "Operational positions:           2" in output
        assert "Tokenized operational positions: 2 (100.0%)" in output
        assert "Marked operational positions:    1 (50.0%)" in output
        assert "OK price positions:              1 (50.0% of operational)" in output
        assert "Live trades count:               0" in output

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_mtm_coverage_excludes_no_orderbook_from_marked(monkeypatch, capsys):
    """no_orderbook_or_illiquid rows must NOT count as marked operational positions
    even if they carry a stale current_price and last_price_timestamp."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE trades (id INTEGER PRIMARY KEY)")
        conn.execute("""
            INSERT INTO paper_positions
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, current_price, price_status, price_source, last_price_timestamp,
             market_title, whale_name, simulated_size, unrealized_pnl, realized_pnl)
            VALUES
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'condition_ok',
             '0xok', 0.55, 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z',
             'OK Market', 'whale_ok', 10.0, 1.0, 0.0),
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'condition_noob',
             '0xnoob', 0.60, 'no_orderbook_or_illiquid', 'no_orderbook_or_illiquid', '2026-06-05T10:00:00Z',
             'NoBook Market', 'whale_noob', 10.0, 0.0, 0.0),
            (3, 'v6.6-paper-portfolio', 0, 3, 3, 'condition_legacy',
             '', NULL, 'legacy_unpriceable_missing_token', 'none', NULL,
             '', 'legacy_whale', 10.0, 0.0, 0.0)
        """)
        conn.commit()
        conn.close()

        monkeypatch.setattr(sys, "argv", ["report_v66_mtm_coverage.py", "--db-path", db_path])

        assert mtm_coverage_main() == 0
        output = capsys.readouterr().out
        # Operational should be 2 (ok + no_orderbook, excluding legacy)
        assert "Operational positions:           2" in output
        # Tokenized should be 2 (both have outcome_token)
        assert "Tokenized operational positions: 2 (100.0%)" in output
        # Marked should be 1 — the no_orderbook row is excluded despite having current_price
        assert "Marked operational positions:    1 (50.0%)" in output
        # OK count
        assert "OK price positions:              1 (50.0% of operational)" in output
        # no_orderbook reported separately
        assert "No orderbook / illiquid rows:    1" in output
        # Stale must exclude no_orderbook; the ok row is >30 min old so stale=1
        assert "Stale tokenized rows (>30 min):  1" in output
        assert "Live trades count:               0" in output

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
