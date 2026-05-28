#!/usr/bin/env python3
"""
Classification Effectiveness Dashboard

Queries decision_snapshots to measure whether the whale category classifier
is actually driving better gate outcomes:

1. Per-whale, per-category-action rejection breakdown
2. Pass-through rate by category_action (FOLLOW/FADE/NEUTRAL/INSUFFICIENT_DATA)
3. Top blockers for each action type
4. Signal volume trend

Run:
    python3 scripts/classification_dashboard.py [--days 7]

The dashboard requires category_action to be populated in decision_snapshots.
This happens automatically once wf_signal_handler.py Phase 2 (T4 fix) is deployed
and the paper service restarts. Prior to that, all rows have NULL category_action.

Sample output (after Phase 2 deployment):
    CLASSIFIER EFFECTIVENESS DASHBOARD (last 7 days)
    Total signals: 12,847 | SHADOW_TRADE: 5,562 | REJECT: 7,285
    ── Per-action pass-through rates ──────────────────────────────────────────
    FOLLOW        1,234 signals  | passed:   892 (72.3%) | top block: edge_below_tier
    FADE              89 signals  | passed:    12 (13.5%) | top block: tier_confidence<25%
    NEUTRAL        3,102 signals  | passed:   891 (28.7%) | top block: sports_confidence_below_min
    INSUFFICIENT_DATA  8,422 signals | passed:   203 ( 2.4%) | top block: insufficient_data_whale
    ── Top whales by signal volume ───────────────────────────────────────────
    swisstony         886 signals  | action=NEUTRAL | block: tier_confidence<25% (598)
    mooseborzoi       349 signals  | action=FOLLOW  | block: sports_confidence_below_min (324)
"""

from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
WIDTH = 80


def query_dashboard(conn: sqlite3.Connection, days: int) -> dict:
    """Return all dashboard data from decision_snapshots."""
    cutoff = f"datetime('now', '-{days} days')"

    # Overall summary
    cur = conn.execute(f"""
        SELECT
            final_decision,
            COUNT(*) as cnt
        FROM decision_snapshots
        WHERE timestamp > {cutoff}
        GROUP BY final_decision
    """)
    summary = dict(cur.fetchall())

    # Per-action rejection breakdown
    cur = conn.execute(f"""
        SELECT
            COALESCE(NULLIF(category_action, ''), '(null)') as cat_action,
            reject_reason,
            final_decision,
            COUNT(*) as cnt
        FROM decision_snapshots
        WHERE timestamp > {cutoff}
        GROUP BY cat_action, reject_reason, final_decision
        ORDER BY cat_action, cnt DESC
    """)
    action_breakdown = cur.fetchall()

    # Top whales by signal volume
    cur = conn.execute(f"""
        SELECT
            whale_name,
            COALESCE(NULLIF(category_action, ''), '(null)') as cat_action,
            final_decision,
            reject_reason,
            COUNT(*) as cnt
        FROM decision_snapshots
        WHERE timestamp > {cutoff}
          AND whale_name IS NOT NULL
          AND whale_name != ''
        GROUP BY whale_name, cat_action, final_decision, reject_reason
        ORDER BY whale_name, cnt DESC
    """)
    whale_signals = cur.fetchall()

    return {
        "summary": summary,
        "action_breakdown": action_breakdown,
        "whale_signals": whale_signals,
    }


def format_dashboard(data: dict, days: int) -> str:
    lines = []
    lines.append("=" * WIDTH)
    lines.append(f"CLASSIFIER EFFECTIVENESS DASHBOARD (last {days} days)")
    lines.append("=" * WIDTH)

    summary = data["summary"]
    total = sum(summary.values())
    shadow = summary.get("SHADOW_TRADE", 0)
    reject = summary.get("REJECT", 0)
    accept = summary.get("ACCEPT", 0) + summary.get("FILL", 0)
    lines.append(f"Total signals: {total:,} | SHADOW_TRADE: {shadow:,} | REJECT: {reject:,} | ACCEPT: {accept:,}")
    lines.append("")

    # Per-action pass-through
    SEP1 = "─" * 20
    lines.append(f"─ Per-action breakdown {SEP1}")
    action_stats: dict = {}
    for cat_action, reject_reason, final_decision, cnt in data["action_breakdown"]:
        if cat_action not in action_stats:
            action_stats[cat_action] = {"total": 0, "reasons": {}}
        action_stats[cat_action]["total"] += cnt
        key = f"{final_decision}:{reject_reason}" if reject_reason else final_decision
        action_stats[cat_action]["reasons"][key] = (
            action_stats[cat_action]["reasons"].get(key, 0) + cnt
        )

    for action in ["FOLLOW", "FADE", "NEUTRAL", "INSUFFICIENT_DATA", "(null)"]:
        if action not in action_stats:
            continue
        stats = action_stats[action]
        total = stats["total"]
        passed = sum(c for k, c in stats["reasons"].items() if k.startswith("SHADOW") or k.startswith("ACCEPT") or k.startswith("FILL"))
        rate = passed / total * 100 if total > 0 else 0
        top_block = sorted(stats["reasons"].items(), key=lambda x: -x[1])
        top_block_str = top_block[0][0] if top_block else "none"
        lines.append(
            f"  {action:20s} {total:6,} signals | "
            f"passed: {passed:5,} ({rate:5.1f}%) | "
            f"top block: {top_block_str[:WIDTH-70]}"
        )

    lines.append("")
    SEP2 = "─" * 26
    lines.append(f"─ Top whales by volume {SEP2}")

    # Aggregate by whale
    whale_agg: dict = {}
    for whale, cat_action, final_decision, reject_reason, cnt in data["whale_signals"]:
        if whale not in whale_agg:
            whale_agg[whale] = {"total": 0, "cat_action": cat_action, "reasons": {}}
        whale_agg[whale]["total"] += cnt
        key = f"{final_decision}:{reject_reason}" if reject_reason else final_decision
        whale_agg[whale]["reasons"][key] = whale_agg[whale]["reasons"].get(key, 0) + cnt

    top_whales = sorted(whale_agg.items(), key=lambda x: -x[1]["total"])[:10]
    for whale, info in top_whales:
        total = info["total"]
        top_block = sorted(info["reasons"].items(), key=lambda x: -x[1])
        top_block_str = f"{top_block[0][0]} ({top_block[0][1]})" if top_block else "none"
        lines.append(
            f"  {whale:30s} {total:5,} signals | "
            f"action={info['cat_action']:20s} | "
            f"block: {top_block_str[:25]}"
        )

    lines.append("=" * WIDTH)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Classification Effectiveness Dashboard")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back (default: 7)")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: DB not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA busy_timeout = 5000")

    try:
        data = query_dashboard(conn, args.days)
    except Exception as e:
        print(f"ERROR querying dashboard: {e}")
        return 1
    finally:
        conn.close()

    output = format_dashboard(data, args.days)
    print(output)

    # Emit a Feishu-ready summary line
    summary = data["summary"]
    total = sum(summary.values())
    null_cnt = sum(
        cnt for cat, reject, decision, cnt in data["action_breakdown"]
        if cat == "(null)"
    )
    if null_cnt == total:
        print(
            f"\n[NOTE] category_action is NULL for all {total:,} signals in the last {args.days} days. "
            f"This means Phase 2 (whale_category_classifier) has not run yet in the deployed code. "
            f"Restart the paper service to activate the classifier."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
