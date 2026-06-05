import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone

from strategies.wf_db_ops import ensure_decision_snapshots_table, ensure_shadow_trades_table, insert_decision_snapshot


def test_duration_metadata_populated_when_expiry_exists():
    from strategies.wf_observation_v66 import build_duration_metadata

    meta = build_duration_metadata(
        signal=type("S", (), {"market_expires_at": "2026-06-06T05:00:00+00:00"})(),
        now=datetime(2026, 6, 5, 5, 0, tzinfo=timezone.utc),
    )

    assert meta["market_expires_at"] == "2026-06-06T05:00:00+00:00"
    assert meta["expected_resolution_hours"] == 24.0
    assert meta["duration_bucket"] == "short"
    assert meta["resolution_priority"] == 2
    assert meta["duration_source"] == "signal.market_expires_at"
    assert "duration_missing_reason" not in meta


def test_missing_expiry_records_unknown_reason():
    from strategies.wf_observation_v66 import build_duration_metadata

    meta = build_duration_metadata(
        signal=type("S", (), {"market_title": "Will something happen someday?"})(),
        now=datetime(2026, 6, 5, 5, 0, tzinfo=timezone.utc),
    )

    assert meta["market_expires_at"] is None
    assert meta["expected_resolution_hours"] is None
    assert meta["duration_bucket"] == "unknown"
    assert meta["resolution_priority"] == 99
    assert meta["duration_missing_reason"] == "no_expiry_source_found"


def test_v2_insufficient_data_includes_reason_and_sample_size(tmp_path):
    from strategies.wf_category_action import get_category_action_v2

    db = tmp_path / "trades.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE shadow_trades (id INTEGER, whale_name TEXT, category TEXT, resolved INTEGER, actual_pnl REAL, actual_return REAL, won INTEGER, entry_timestamp TEXT)")
    conn.execute("CREATE TABLE trades (trade_id TEXT, whale_name TEXT, category TEXT, timestamp TEXT, realized_pnl REAL, realized_return REAL, signal_source TEXT)")
    conn.execute("CREATE VIEW canonical_perf AS SELECT id, whale_name, category, entry_timestamp, NULL AS exit_timestamp, actual_pnl AS realized_pnl, actual_return AS realized_return, actual_pnl, actual_return, won, 'shadow' AS source, NULL AS config_version FROM shadow_trades WHERE resolved=1")
    conn.execute("INSERT INTO shadow_trades VALUES (1, 'WhaleA', 'general', 1, 5.0, 0.1, 1, '2026-06-01')")
    conn.commit(); conn.close()

    result = get_category_action_v2("WhaleA", "general", db_path=db)

    assert result["action"] == "INSUFFICIENT_DATA"
    assert result["stats"]["total_trades"] == 1
    assert result["reason"] == "sample_size_below_minimum"
    assert result["lookup_key"] == "WhaleA|general"
    assert result["match_status"] == "matched_exact"


def test_observation_cohorts_do_not_change_final_decision():
    from strategies.wf_observation_v66 import enrich_snapshot_metadata

    snap = {
        "source": "sybil",
        "normalized_category": "sports",
        "whale_name": "sybil_meta_group_1",
        "final_decision": "REJECT",
        "reject_reason": "sports_confidence_below_min",
        "metadata_json": json.dumps({"trace_id": "abc"}),
        "category_action_v2": "FOLLOW",
        "category_sample_size_v2": 25,
        "category_avg_pnl_v2": 12.5,
        "category_win_rate_v2": 0.6,
    }
    signal = type("S", (), {"market_title": "Golden Knights vs. Hurricanes", "market_expires_at": "2026-06-06T05:00:00+00:00"})()

    enriched = enrich_snapshot_metadata(snap, signal, now=datetime(2026, 6, 5, 5, 0, tzinfo=timezone.utc))
    meta = json.loads(enriched["metadata_json"])

    assert enriched["final_decision"] == "REJECT"
    assert meta["v66_cohorts"]["v2_follow_candidate"] is True
    assert meta["v66_cohorts"]["sports_telemetry_candidate"] is True
    assert meta["v66_cohorts"]["sybil_cluster_candidate"] is True


def test_insert_shadow_trade_persists_duration_metadata(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "test.db")
        ensure_shadow_trades_table(db_path=db)
        import strategies.wf_shadow_ledger as ledger
        monkeypatch.setattr(ledger, "_get_db_path", lambda: os.path.abspath(db))

        ok = insert_decision_snapshot(
            signal_id="sig-v66",
            source="known_whale",
            category="general",
            market_title="Will test resolve tomorrow?",
            condition_id="0xabc",
            whale_name="TestWhale",
            side="BUY",
            final_decision="SHADOW_TRADE",
            reject_reason="shadow_mode_block",
            position_size_usd=10.0,
            entry_price=0.42,
            metadata_json=json.dumps({
                "v66_duration": {
                    "market_expires_at": "2026-06-06T05:00:00+00:00",
                    "expected_resolution_hours": 24.0,
                    "duration_bucket": "short",
                    "resolution_priority": 2,
                }
            }),
            db_path=db,
        )

        assert ok
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT market_expires_at, expected_resolution_hours, duration_bucket, resolution_priority FROM shadow_trades WHERE signal_id='sig-v66'").fetchone()
        conn.close()
        assert row == ("2026-06-06T05:00:00+00:00", 24.0, "short", 2)


def test_report_script_runs_read_only(tmp_path):
    from scripts.report_v66_strategy_validation import generate_report

    db = tmp_path / "trades.db"
    ensure_shadow_trades_table(db_path=str(db))
    ensure_decision_snapshots_table(db_path=str(db))
    before = db.stat().st_mtime_ns

    report = generate_report(str(db), start="2026-06-01 05:00:00+00:00")
    after = db.stat().st_mtime_ns

    assert report["meta"]["start"] == "2026-06-01 05:00:00+00:00"
    assert "accepted_by_source_category_whale" in report
    assert before == after
