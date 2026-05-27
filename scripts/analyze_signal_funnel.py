#!/usr/bin/env python3
"""
analyze_signal_funnel.py — Phase 0 Signal Funnel Analysis

Usage:
    python analyze_signal_funnel.py [--hours HOURS] [--db DB_PATH]

Queries the decision_snapshots table and prints gate-level pass/fail statistics
for all signals processed in the given time window.
"""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path


def get_db_path(db_arg: str | None) -> Path:
    if db_arg:
        return Path(db_arg)
    # Default to the standard workspace location
    default = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
    if default.exists():
        return default
    # Fallback: relative to script location
    return Path(__file__).parent.parent / "data" / "trades.db"


def analyze_funnel(
    db_path: Path,
    hours: int = 24,
    verbose: bool = False,
) -> int:
    """Run the funnel analysis. Returns total signals found."""
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 0

    conn = sqlite3.connect(str(db_path))

    # Verify table exists
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='decision_snapshots'"
    ).fetchall()
    if not tables:
        print("NO DATA: decision_snapshots table does not exist yet.")
        print("  → The service needs to process signals first.")
        print("  → Check that SHADOW_MODE=True and the service is running.")
        conn.close()
        return 0

    cutoff_ts = conn.execute(
        "SELECT datetime('now', ? || ' hours')",
        (f"-{hours}",),
    ).fetchone()[0]

    rows = conn.execute(
        """
        SELECT
            source,
            category,
            final_decision,
            reject_reason,
            side,
            confidence,
            edge_score,
            passed_category_filter,
            passed_quarantine,
            passed_blacklist,
            passed_edge_threshold,
            passed_fade_eligibility,
            passed_risk_manager,
            passed_execution_checks,
            passed_position_limits,
            passed_pnl_gate,
            passed_correlation_gate,
            passed_capital_pool,
            position_size_usd,
            signal_type,
            whale_name,
            market_title,
            timestamp
        FROM decision_snapshots
        WHERE timestamp >= ?
        ORDER BY timestamp DESC
        """,
        (cutoff_ts,),
    ).fetchall()

    conn.close()

    total = len(rows)
    if total == 0:
        print(f"\nNO DATA: No signals found in the last {hours} hours.")
        print("  → Check that the service is running and processing signals.")
        return 0

    print(f"\n{'='*70}")
    print(f"  SIGNAL FUNNEL ANALYSIS — Last {hours}h")
    print(f"  Database: {db_path}")
    print(f"  Time window: {cutoff_ts} → now")
    print(f"{'='*70}")

    # ── Overall counts ────────────────────────────────────────────────────
    decisions = defaultdict(int)
    for r in rows:
        decisions[r[2]] += 1

    print(f"\n[OVERALL] Total signals: {total}")
    print(f"  Decisions: ", end="")
    print(", ".join(f"{k}={v}" for k, v in sorted(decisions.items())))

    # ── Gate statistics ──────────────────────────────────────────────────
    gate_cols = [
        ("passed_category_filter",  "Category Filter"),
        ("passed_quarantine",     "Quarantine"),
        ("passed_blacklist",     "Blacklist"),
        ("passed_edge_threshold", "Edge Threshold"),
        ("passed_fade_eligibility","Fade Eligibility"),
        ("passed_risk_manager",   "Risk Manager"),
        ("passed_execution_checks","Execution Checks"),
        ("passed_position_limits","Position Limits"),
        ("passed_pnl_gate",       "48h P&L Gate"),
        ("passed_correlation_gate","Correlation Gate"),
        ("passed_capital_pool",  "Capital Pool"),
    ]

    print(f"\n[GATE STATISTICS]")
    print(f"  {'Gate':<22} {'Passed':>7} {'Rejected':>8} {'Skipped':>8} {'N/A':>5}")
    print(f"  {'-'*22} {'-'*7} {'-'*8} {'-'*8} {'-'*5}")

    for col_name, label in gate_cols:
        col_idx = 7 + [c[0] for c in gate_cols].index(col_name)
        passed   = sum(1 for r in rows if r[col_idx] == 1)
        rejected = sum(1 for r in rows if r[col_idx] == 0)
        na       = sum(1 for r in rows if r[col_idx] == -1)
        total_g  = passed + rejected + na
        skip_pct = (na / total_g * 100) if total_g else 0
        print(
            f"  {label:<22} {passed:>7} {rejected:>8} "
            f"{na:>8} ({skip_pct:>5.1f}% N/A)"
        )

    # ── Top rejection reasons ──────────────────────────────────────────────
    rejects = [r for r in rows if r[2] in ("REJECT", "SHADOW_TRADE")]
    reject_reasons: dict[str, int] = defaultdict(int)
    for r in rejects:
        reason = r[3] or "(no reason)"
        reject_reasons[reason] += 1

    if reject_reasons:
        print(f"\n[TOP REJECTION REASONS] (n={len(rejects)})")
        for reason, count in sorted(reject_reasons.items(), key=lambda x: -x[1])[:10]:
            pct = count / len(rejects) * 100
            bar = "█" * int(pct / 2)
            print(f"  {count:>4} ({pct:>5.1f}%) {bar} {reason}")

    # ── Source breakdown ───────────────────────────────────────────────────
    sources: dict[str, int] = defaultdict(int)
    for r in rows:
        sources[r[0] or "unknown"] += 1

    print(f"\n[SOURCE BREAKDOWN]")
    for src, count in sorted(sources.items(), key=lambda x: -x[1]):
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        print(f"  {count:>4} ({pct:>5.1f}%) {bar} {src}")

    # ── Category breakdown ────────────────────────────────────────────────
    categories: dict[str, int] = defaultdict(int)
    for r in rows:
        categories[r[1] or "(none)"] += 1

    print(f"\n[CATEGORY BREAKDOWN]")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1])[:15]:
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        print(f"  {count:>4} ({pct:>5.1f}%) {bar} {cat}")

    # ── Recent REJECTs (last 5) ────────────────────────────────────────────
    recent_rejects = [
        r for r in rows if r[2] in ("REJECT", "SHADOW_TRADE")
    ][:5]

    if recent_rejects:
        print(f"\n[RECENT REJECTIONS] (last 5)")
        print(
            f"  {'Time':<22} {'Decision':<14} {'Reason':<30} "
            f"{'Category':<12} {'Side'}"
        )
        print(f"  {'-'*22} {'-'*14} {'-'*30} {'-'*12} {'-'*4}")
        for r in recent_rejects:
            ts       = r[22] or ""
            decision = r[2] or ""
            reason   = (r[3] or "")[:30]
            cat      = (r[1] or "")[:12]
            side     = r[4] or ""
            print(f"  {ts:<22} {decision:<14} {reason:<30} {cat:<12} {side}")

    print(f"\n{'='*70}")
    print(f"  Total signals: {total} | Rejects: {len(rejects)} | Pass rate: {(total-len(rejects))/total*100:.1f}%")
    print(f"{'='*70}\n")
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyze signal funnel from decision_snapshots table."
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Time window in hours (default: 24)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to trades.db (default: auto-detect)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print verbose output",
    )
    args = parser.parse_args()

    db_path = get_db_path(args.db)
    total = analyze_funnel(db_path, hours=args.hours, verbose=args.verbose)
    raise SystemExit(0 if total > 0 else 1)
