from __future__ import annotations

import sqlite3
import pytest
import tempfile
import os
from datetime import datetime, timezone, timedelta

from scripts.report_v66_paper_portfolio import generate_report
from strategies.wf_paper_portfolio import ensure_paper_portfolio_tables


def test_report_excludes_missing_token_from_stale() -> None:
    """Test that missing token rows are not counted as stale in the report."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Ensure tables are created
        ensure_paper_portfolio_tables(db_path)
        
        # Connect and insert test data
        conn = sqlite3.connect(db_path)
        
        # Insert a missing token row that would otherwise appear stale
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id, 
             price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'test_condition',
             'missing_outcome_token', 'none', NULL, '', 'unknown',
             10.0, 0.0, 0.0)
        """)
        
        # Insert a normal row that should appear stale (no timestamp)
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'test_condition2',
             '0x123', 'ok', 'clob_midpoint', NULL, 'Test Market', 'test_whale',
             10.0, 0.0, 0.0)
        """)
        
        conn.commit()
        conn.close()
        
        # Generate report
        report = generate_report(db_path)
        
        # Missing token rows should not appear in stale prices
        assert len(report["stale_prices"]) == 1  # Only the normal row
        assert report["stale_prices"][0]["id"] == 2  # The normal row, not the missing token row
        
    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_report_handles_empty_titles_with_condition_id_fallback() -> None:
    """Test that empty market titles fall back to condition_id in concentration report."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Ensure tables are created
        ensure_paper_portfolio_tables(db_path)
        
        # Connect and insert test data
        conn = sqlite3.connect(db_path)
        
        # Insert a row with empty market title
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (1, 'v6.6-paper-portfolio', 0, 1, 1, '0x123456789abcdef0123456789abcdef',
             '0x123', 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z', '', 'test_whale',
             10.0, 5.0, 0.0)
        """)
        
        conn.commit()
        conn.close()
        
        # Generate report
        report = generate_report(db_path)
        
        # Check that concentration by market uses condition_id as fallback
        market_concentration = report["concentration_by_market"]
        assert len(market_concentration) == 1
        # Should show first 12 characters of condition_id
        assert market_concentration[0]["market_title"] == "0x123456789a"
        
    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_report_classifies_unpriceable_missing_token_correctly() -> None:
    """Test that unpriceable missing token rows are properly classified."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Ensure tables are created
        ensure_paper_portfolio_tables(db_path)
        
        # Connect and insert test data
        conn = sqlite3.connect(db_path)
        
        # Insert a unpriceable missing token row
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'test_condition',
             'unpriceable_missing_outcome_token', 'none', NULL, '', 'unknown',
             10.0, 0.0, 0.0)
        """)
        
        # Insert a normal row that should appear stale (no timestamp)
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'test_condition2',
             '0x123', 'ok', 'clob_midpoint', NULL, 'Test Market', 'test_whale',
             10.0, 0.0, 0.0)
        """)
        
        conn.commit()
        conn.close()
        
        # Generate report
        report = generate_report(db_path)
        
        # Unpriceable missing token rows should not appear in stale prices
        assert len(report["stale_prices"]) == 1  # Only the normal row
        assert report["stale_prices"][0]["id"] == 2  # The normal row, not the unpriceable missing token row
        
    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_report_handles_legacy_unpriceable_rows_separately() -> None:
    """Test that legacy unpriceable rows are handled separately in reports."""
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
             price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'test_condition',
             'legacy_unpriceable_missing_token', 'none', NULL, '', 'unknown',
             10.0, 5.0, 0.0)
        """)
        
        # Insert a normal operational row with recent timestamp
        recent_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (2, 'v6.6-paper-portfolio', 0, 2, 2, 'test_condition2',
             '0x123', 'ok', 'clob_midpoint', ?, 'Test Market', 'test_whale',
             10.0, 2.0, 0.0)
        """, (recent_time,))
        
        # Insert another legacy row
        conn.execute("""
            INSERT INTO paper_positions 
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES 
            (3, 'v6.6-paper-portfolio', 0, 3, 3, 'test_condition3',
             'legacy_unpriceable_missing_token', 'none', NULL, '', 'unknown2',
             5.0, 1.0, 0.0)
        """)
        
        conn.commit()
        conn.close()
        
        # Generate report
        report = generate_report(db_path)
        
        # Check that legacy rows are counted separately
        assert report["legacy_unpriceable_rows"]["count"] == 2
        assert report["open_positions"]["count"] == 1  # Only operational row
        
        # Check that PnL calculations exclude legacy rows
        assert report["open_positions"]["total_unrealized_pnl"] == 2.0  # Only operational row
        assert report["open_positions"]["total_pnl"] == 2.0
        
        # Check that legacy rows don't appear in missing prices or stale
        assert len(report["missing_prices"]) == 0
        assert len(report["stale_prices"]) == 0
        
        # Check that legacy rows don't appear in concentration
        market_concentration = report["concentration_by_market"]
        assert len(market_concentration) == 1
        assert market_concentration[0]["market_title"] == "Test Market"  # Only operational row
        
    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)



def test_top_winners_and_losers_exclude_legacy_and_use_title_fallback() -> None:
    """Top lists should report only operational rows and never render blank titles."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)

        conn.execute("""
            INSERT INTO paper_positions
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES
            (1, 'v6.6-paper-portfolio', 0, 1, 1, 'legacy_condition_should_not_show',
             '', 'legacy_unpriceable_missing_token', 'none', NULL, '', 'legacy_whale',
             10.0, -999.0, 0.0)
        """)
        conn.execute("""
            INSERT INTO paper_positions
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES
            (2, 'v6.6-paper-portfolio', 0, 2, 2, '0xwinnerabcdef123456',
             'winner_token', 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z', '', 'winner_whale',
             10.0, 12.0, 0.0)
        """)
        conn.execute("""
            INSERT INTO paper_positions
            (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
             outcome_token, price_status, price_source, last_price_timestamp, market_title, whale_name,
             simulated_size, unrealized_pnl, realized_pnl)
            VALUES
            (3, 'v6.6-paper-portfolio', 0, 3, 3, '0xloserabcdef123456',
             'loser_token', 'ok', 'clob_midpoint', '2026-06-05T10:00:00Z', '', 'loser_whale',
             10.0, -2.0, 0.0)
        """)
        conn.commit()
        conn.close()

        report = generate_report(db_path)

        assert [row["id"] for row in report["top_winners"]] == [2, 3]
        assert [row["id"] for row in report["top_losers"]] == [3, 2]
        assert report["top_winners"][0]["market_title"] == "0xwinnerabcd"
        assert report["top_losers"][0]["market_title"] == "0xloserabcde"

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__])