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


def test_report_adds_concentration_alerts_and_hypothetical_flags() -> None:
    """Concentration diagnostics should be report-only labels, not gates."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)
        recent_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()

        rows = [
            (1, 'cond_a', 'token_a', 'Market A', 'whale_a', '', 'known_whale', 'general', 'NEUTRAL', 50.0),
            (2, 'cond_b', 'token_b', 'Market B', 'whale_b', '', 'known_whale', 'politics', 'INSUFFICIENT_DATA', 30.0),
            (3, 'cond_c', 'token_c', 'Market C', 'unknown', '', 'model_insider', 'general', 'NEUTRAL', 20.0),
        ]
        for row in rows:
            conn.execute("""
                INSERT INTO paper_positions
                (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
                 outcome_token, price_status, price_source, last_price_timestamp, market_title,
                 whale_name, whale_cluster, source, category, category_action_v2,
                 simulated_size, unrealized_pnl, realized_pnl)
                VALUES
                (?, 'v6.6-paper-portfolio', 0, ?, ?, ?, ?, 'ok', 'clob_midpoint', ?, ?,
                 ?, ?, ?, ?, ?, ?, 0.0, 0.0)
            """, (
                row[0], row[0], row[0], row[1], row[2], recent_time, row[3],
                row[4], row[5], row[6], row[7], row[8], row[9],
            ))
        conn.commit()
        conn.close()

        report = generate_report(db_path)

        assert report["concentration_risk"]["total_open_exposure"] == 100.0
        assert report["concentration_risk"]["unknown_whale_exposure_pct"] == 20.0
        assert "single_whale_cluster_gt_40" in report["concentration_risk"]["alert_labels"]
        assert "single_market_gt_35" in report["concentration_risk"]["alert_labels"]
        assert "unknown_whale_gt_50" not in report["concentration_risk"]["alert_labels"]
        assert report["concentration_risk"]["hypothetical_flags"]["would_exceed_whale_cap"] is True
        assert report["concentration_risk"]["hypothetical_flags"]["would_exceed_market_cap"] is True
        assert report["concentration_risk"]["hypothetical_flags"]["would_exceed_unknown_cap"] is False
        assert report["concentration_by_source"][0]["source"] == "known_whale"
        assert report["concentration_by_category_action_v2"][0]["category_action_v2"] == "NEUTRAL"

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_report_alerts_when_mtm_coverage_is_below_threshold() -> None:
    """MTM coverage below 80% should render as an observation alert only."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name

    try:
        ensure_paper_portfolio_tables(db_path)
        conn = sqlite3.connect(db_path)
        recent_time = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()

        for idx in range(1, 6):
            status = 'ok' if idx <= 3 else 'pending'
            ts = recent_time if idx <= 3 else None
            conn.execute("""
                INSERT INTO paper_positions
                (id, experiment_tag, resolved, shadow_trade_id, snapshot_id, condition_id,
                 outcome_token, price_status, price_source, last_price_timestamp, market_title,
                 whale_name, source, category, category_action_v2,
                 simulated_size, unrealized_pnl, realized_pnl)
                VALUES
                (?, 'v6.6-paper-portfolio', 0, ?, ?, ?, ?, ?, 'clob_midpoint', ?, ?,
                 'test_whale', 'known_whale', 'general', 'NEUTRAL', 10.0, 0.0, 0.0)
            """, (idx, idx, idx, f'cond_{idx}', f'token_{idx}', status, ts, f'Market {idx}'))
        conn.commit()
        conn.close()

        report = generate_report(db_path)

        assert report["mtm_coverage"]["coverage_pct"] == 60.0
        assert "mtm_coverage_lt_80" in report["concentration_risk"]["alert_labels"]

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_report_alerts_when_last_updater_run_had_errors() -> None:
    """Updater errors should be read from the report log and labeled only."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as tmp_log:
        log_path = tmp_log.name
        tmp_log.write(
            "2026-06-06T10:00:01 INFO update | MTM complete | "
            "total=5 updated=4 missing_price=0 stale_mark=0 "
            "unpriceable_token=0 unpriceable_data=0 resolved=0 errors=1\n"
        )

    try:
        ensure_paper_portfolio_tables(db_path)

        report = generate_report(db_path, update_log_path=log_path)

        assert report["updater_health"]["last_errors"] == 1
        assert "updater_errors_gt_0" in report["concentration_risk"]["alert_labels"]

    finally:
        for path in (db_path, log_path):
            if os.path.exists(path):
                os.unlink(path)


if __name__ == "__main__":
    pytest.main([__file__])
