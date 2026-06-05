#!/usr/bin/env python3
"""Read-only v6.6 strategy validation report."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB = "/home/elon-1/workspace/nautilus-trading/data/trades.db"
DEFAULT_START = "2026-06-01 05:00:00+00:00"


def _connect_ro(db_path: str) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    ).fetchone() is not None


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _has_table(conn, table):
        return set()
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _json_extract_counts(conn: sqlite3.Connection, start: str, expr: str) -> list[dict[str, Any]]:
    try:
        return _rows(conn, f"""
            SELECT COALESCE({expr}, 'unknown') AS key, COUNT(*) AS n
            FROM decision_snapshots
            WHERE datetime(timestamp) >= datetime(?)
            GROUP BY key ORDER BY n DESC
        """, (start,))
    except sqlite3.Error:
        return []


def generate_report(db_path: str = DEFAULT_DB, start: str = DEFAULT_START) -> dict[str, Any]:
    conn = _connect_ro(db_path)
    try:
        ds_cols = _cols(conn, "decision_snapshots")
        st_cols = _cols(conn, "shadow_trades")
        report: dict[str, Any] = {
            "meta": {"db_path": str(Path(db_path).resolve()), "start": start},
            "schema": {
                "decision_snapshots_has_v2_lookup": "category_lookup_key_v2" in ds_cols,
                "shadow_trades_has_duration": "duration_bucket" in st_cols,
            },
        }

        if _has_table(conn, "shadow_trades"):
            shadow_time_col = "s.created_at" if "created_at" in st_cols else "s.entry_timestamp"
            shadow_time_plain = "created_at" if "created_at" in st_cols else "entry_timestamp"
            report["accepted_by_source_category_whale"] = _rows(conn, f"""
                SELECT COALESCE(d.source,'') AS source,
                       COALESCE(d.normalized_category,d.category,s.category,'') AS category,
                       COALESCE(d.whale_name,s.whale_name,'') AS whale_name,
                       COALESCE(s.market_title,'') AS market_title,
                       COALESCE(d.category_action_v2,'') AS category_action_v2,
                       COUNT(*) AS n
                FROM shadow_trades s
                LEFT JOIN decision_snapshots d ON d.id=s.snapshot_id
                WHERE datetime({shadow_time_col}) >= datetime(?)
                GROUP BY 1,2,3,4,5
                ORDER BY n DESC LIMIT 100
            """, (start,))
            report["duration_coverage_accepted"] = _rows(conn, f"""
                SELECT COALESCE(duration_bucket,'unknown') AS duration_bucket,
                       COUNT(*) AS n,
                       SUM(CASE WHEN market_expires_at IS NULL OR market_expires_at='' THEN 1 ELSE 0 END) AS missing_expiry
                FROM shadow_trades
                WHERE datetime({shadow_time_plain}) >= datetime(?)
                GROUP BY duration_bucket ORDER BY n DESC
            """, (start,))
            report["unresolved_backlog_by_duration"] = _rows(conn, """
                SELECT COALESCE(duration_bucket,'unknown') AS duration_bucket, COUNT(*) AS n
                FROM shadow_trades
                WHERE resolved=0
                GROUP BY duration_bucket ORDER BY n DESC
            """)
            report["resolved_pnl"] = _rows(conn, f"""
                SELECT COALESCE(duration_bucket,'unknown') AS duration_bucket,
                       COUNT(*) AS n,
                       ROUND(SUM(COALESCE(actual_pnl,0)),2) AS pnl,
                       ROUND(AVG(actual_return),4) AS avg_return,
                       ROUND(AVG(won),4) AS win_rate
                FROM shadow_trades
                WHERE datetime({shadow_time_plain}) >= datetime(?) AND resolved=1
                GROUP BY duration_bucket ORDER BY n DESC
            """, (start,))

        if _has_table(conn, "decision_snapshots"):
            v2_reason_expr = "category_action_reason_v2"
            if "category_action_v2_reason" in ds_cols:
                v2_reason_expr = "COALESCE(category_action_reason_v2, category_action_v2_reason)"
            match_expr = "category_match_status_v2" if "category_match_status_v2" in ds_cols else "'unknown'"
            report["rejects_by_source_category_reason_v2"] = _rows(conn, """
                SELECT COALESCE(source,'') AS source,
                       COALESCE(normalized_category,category,'') AS category,
                       COALESCE(reject_reason,'') AS reject_reason,
                       COALESCE(category_action_v2,'') AS category_action_v2,
                       COUNT(*) AS n
                FROM decision_snapshots
                WHERE datetime(timestamp) >= datetime(?) AND final_decision='REJECT'
                GROUP BY 1,2,3,4
                ORDER BY n DESC LIMIT 100
            """, (start,))
            report["old_pipeline_vs_v2"] = _rows(conn, """
                SELECT final_decision,
                       COALESCE(category_action_v2,'') AS category_action_v2,
                       COUNT(*) AS n
                FROM decision_snapshots
                WHERE datetime(timestamp) >= datetime(?)
                GROUP BY final_decision, category_action_v2
                ORDER BY n DESC
            """, (start,))
            report["v2_abstention_reasons"] = _rows(conn, f"""
                SELECT COALESCE({v2_reason_expr}, 'unknown') AS reason,
                       COALESCE({match_expr}, 'unknown') AS match_status,
                       COUNT(*) AS n
                FROM decision_snapshots
                WHERE datetime(timestamp) >= datetime(?)
                  AND COALESCE(category_action_v2,'')='INSUFFICIENT_DATA'
                GROUP BY reason, match_status ORDER BY n DESC
            """, (start,))
            report["duration_coverage_rejected"] = _json_extract_counts(
                conn, start, "json_extract(metadata_json, '$.v66_duration.duration_bucket')"
            )
            report["cohort_counts"] = {
                "old_pipeline_accepted": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.old_pipeline_accepted')"),
                "v2_follow_candidate": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.v2_follow_candidate')"),
                "high_quality_discovery_whale": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.high_quality_discovery_whale')"),
                "short_duration_candidate": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.short_duration_candidate')"),
                "sports_telemetry_candidate": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.sports_telemetry_candidate')"),
                "sybil_cluster_candidate": _json_extract_counts(conn, start, "json_extract(metadata_json, '$.v66_cohorts.sybil_cluster_candidate')"),
            }
            report["top_market_concentration"] = _rows(conn, """
                SELECT COALESCE(market_title,'') AS market_title, COUNT(*) AS n
                FROM decision_snapshots
                WHERE datetime(timestamp) >= datetime(?)
                GROUP BY market_title ORDER BY n DESC LIMIT 30
            """, (start,))
            report["top_whale_concentration"] = _rows(conn, """
                SELECT COALESCE(whale_name,'') AS whale_name, COUNT(*) AS n
                FROM decision_snapshots
                WHERE datetime(timestamp) >= datetime(?)
                GROUP BY whale_name ORDER BY n DESC LIMIT 30
            """, (start,))
        return report
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    report = generate_report(args.db, args.start)
    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
