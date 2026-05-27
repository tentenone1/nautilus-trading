#!/usr/bin/env python3
"""
Category Alpha Discovery Report
==============================
Queries trades.db (decision_snapshots) and dynamic_whale_state.json to produce:

1. Signal Generation Heatmap — by category: markets evaluated, signals generated,
   avg confidence, avg edge, and a breakdown of reject reasons.
2. Historical Profitability by Category — checks trades.db (trades table) for
   any historical trade data (likely 0 rows at this stage, but always check).
3. Whale Concentration Analysis — each whale's category distribution and volume
   (from decision_snapshots + dynamic_whale_state.json).

Run standalone:  python scripts/category_alpha_report.py
No arguments required. Output goes to stdout.
"""

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Paths ────────────────────────────────────────────────────────────────────
BASE = Path("/home/elon-1/workspace/nautilus-trading")
DB_PATH = BASE / "data" / "trades.db"
WHALE_STATE_PATH = BASE / "data" / "dynamic_whale_state.json"

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v:.1%}"


def fmt_float(v: Optional[float], decimals: int = 3) -> str:
    if v is None:
        return "N/A"
    return f"{v:.{decimals}f}"


def get_decision_snapshots() -> list[dict]:
    """Pull all rows from decision_snapshots."""
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA busy_timeout=5000")
    rows = conn.execute(
        "SELECT "
        "  category, whale_name, whale_address, source, "
        "  confidence, edge_score, side, reject_reason, "
        "  passed_category_filter, passed_quarantine, passed_blacklist, "
        "  passed_edge_threshold, final_decision, timestamp, market_title "
        "FROM decision_snapshots ORDER BY timestamp DESC"
    ).fetchall()
    conn.close()
    cols = [
        "category", "whale_name", "whale_address", "source",
        "confidence", "edge_score", "side", "reject_reason",
        "passed_category_filter", "passed_quarantine", "passed_blacklist",
        "passed_edge_threshold", "final_decision", "timestamp", "market_title",
    ]
    return [dict(zip(cols, r)) for r in rows]


def get_trades() -> list[dict]:
    """Pull all rows from trades table (historical P&L)."""
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA busy_timeout=5000")
    rows = conn.execute(
        "SELECT whale_name, category, side, confidence, edge_score, "
        "  realized_pnl, realized_return, position_size_usd "
        "FROM trades"
    ).fetchall()
    conn.close()
    cols = [
        "whale_name", "category", "side", "confidence", "edge_score",
        "realized_pnl", "realized_return", "position_size_usd",
    ]
    return [dict(zip(cols, r)) for r in rows]


def get_whale_state() -> dict:
    """Load dynamic_whale_state.json."""
    if not WHALE_STATE_PATH.exists():
        return {}
    return json.loads(WHALE_STATE_PATH.read_text())


# ────────────────────────────────────────────────────────────────────────────
# 1. Signal Generation Heatmap
# ────────────────────────────────────────────────────────────────────────────
def build_signal_heatmap(snapshots: list[dict]) -> None:
    """Signal Generation Heatmap — by category."""
    # Aggregate per category
    cat_stats: dict = defaultdict(lambda: {
        "total": 0,
        "signals": 0,         # rows with reject_reason = '' or final_decision = 'trade'
        "avg_confidence": [],
        "avg_edge": [],
        "reasons": defaultdict(int),
        "gates": defaultdict(int),  # passed_edge_threshold=1 count
    })

    for row in snapshots:
        cat = (row["category"] or "unknown").strip().lower()
        s = cat_stats[cat]
        s["total"] += 1
        conf = row["confidence"]
        edge = row["edge_score"]
        if conf is not None:
            s["avg_confidence"].append(float(conf))
        if edge is not None:
            s["avg_edge"].append(float(edge))

        reason = row["reject_reason"] or ""
        if reason:
            s["reasons"][reason] += 1
        else:
            s["signals"] += 1

        if row.get("passed_edge_threshold") == 1:
            s["gates"]["passed_edge_threshold"] += 1

    print()
    print("=" * 90)
    print(f"  SIGNAL GENERATION HEATMAP  —  as of {TODAY}")
    print("=" * 90)
    print()
    print(f"  {'Category':<12} {'Snapshots':>9}  {'Signals':>7}  {'Avg Conf':>8}  {'Avg Edge':>8}  {'% Pass Edge':>10}")
    print(f"  {'-'*12} {'-'*9}  {'-'*7}  {'-'*8}  {'-'*8}  {'-'*10}")

    for cat in sorted(cat_stats.keys(), key=lambda c: cat_stats[c]["total"], reverse=True):
        s = cat_stats[cat]
        avg_conf = sum(s["avg_confidence"]) / len(s["avg_confidence"]) if s["avg_confidence"] else 0
        avg_edge = sum(s["avg_edge"]) / len(s["avg_edge"]) if s["avg_edge"] else 0
        pct_edge = s["gates"]["passed_edge_threshold"] / s["total"] if s["total"] else 0
        print(
            f"  {cat:<12} {s['total']:>9}  {s['signals']:>7}  "
            f"{fmt_pct(avg_conf):>8}  {fmt_float(avg_edge):>8}  {fmt_pct(pct_edge):>10}"
        )

    # Reject reason breakdown
    print()
    print(f"  {'─'*90}")
    print(f"  REJECTION REASON BREAKDOWN (all categories)")
    print(f"  {'─'*90}")
    print()
    all_reasons: dict = defaultdict(int)
    for row in snapshots:
        reason = row["reject_reason"] or "(accepted)"
        all_reasons[reason] += 1
    for reason, cnt in sorted(all_reasons.items(), key=lambda x: x[1], reverse=True):
        pct = cnt / len(snapshots) * 100 if snapshots else 0
        print(f"    {cnt:>4}  {pct:>5.1f}%  {reason}")

    # Per-category reason breakdown
    print()
    print(f"  {'─'*90}")
    print(f"  REJECTION REASONS BY CATEGORY")
    print(f"  {'─'*90}")
    for cat in sorted(cat_stats.keys(), key=lambda c: cat_stats[c]["total"], reverse=True):
        s = cat_stats[cat]
        if not s["reasons"]:
            continue
        print(f"\n  [{cat.upper()}]")
        for reason, cnt in sorted(s["reasons"].items(), key=lambda x: x[1], reverse=True):
            pct = cnt / s["total"] * 100
            print(f"    {cnt:>4}  {pct:>5.1f}%  {reason}")


# ────────────────────────────────────────────────────────────────────────────
# 2. Historical Profitability by Category
# ────────────────────────────────────────────────────────────────────────────
def build_profitability_report(trades: list[dict]) -> None:
    """Historical Profitability by Category — from trades table."""
    print()
    print("=" * 90)
    print(f"  HISTORICAL PROFITABILITY BY CATEGORY  —  as of {TODAY}")
    print("=" * 90)

    if not trades:
        print()
        print("  trades table is EMPTY — no historical P&L data available.")
        print("  All observation data currently comes from decision_snapshots.")
        print("  Historical data will appear here once live/paper trades execute.")
        return

    cat_pnl: dict = defaultdict(lambda: {
        "count": 0,
        "total_pnl": 0.0,
        "total_size": 0.0,
        "wins": 0,
        "losses": 0,
    })
    for t in trades:
        cat = (t["category"] or "unknown").strip().lower()
        s = cat_pnl[cat]
        s["count"] += 1
        pnl = t["realized_pnl"] or 0
        size = t["position_size_usd"] or 0
        s["total_pnl"] += pnl
        s["total_size"] += size
        if pnl > 0:
            s["wins"] += 1
        elif pnl < 0:
            s["losses"] += 1

    print()
    print(f"  {'Category':<12} {'Trades':>6}  {'Win Rate':>8}  {'Avg P&L':>10}  {'Total P&L':>12}  {'Total Size':>12}")
    print(f"  {'-'*12} {'-'*6}  {'-'*8}  {'-'*10}  {'-'*12}  {'-'*12}")
    for cat in sorted(cat_pnl.keys(), key=lambda c: cat_pnl[c]["total_pnl"], reverse=True):
        s = cat_pnl[cat]
        wr = s["wins"] / s["count"] if s["count"] else 0
        avg_pnl = s["total_pnl"] / s["count"] if s["count"] else 0
        print(
            f"  {cat:<12} {s['count']:>6}  {fmt_pct(wr):>8}  "
            f"{avg_pnl:>10.2f}  {s['total_pnl']:>12.2f}  {s['total_size']:>12.2f}"
        )


# ────────────────────────────────────────────────────────────────────────────
# 3. Whale Concentration Analysis
# ────────────────────────────────────────────────────────────────────────────
def build_whale_concentration(snapshots: list[dict]) -> None:
    """Whale Concentration Analysis — each whale's category distribution and volume."""
    # Group by whale_name
    whale_cats: dict = defaultdict(lambda: defaultdict(int))
    whale_conf: dict = defaultdict(list)
    whale_edge: dict = defaultdict(list)
    for row in snapshots:
        wn = (row["whale_name"] or "unknown").strip()
        cat = (row["category"] or "unknown").strip().lower()
        whale_cats[wn][cat] += 1
        if row["confidence"] is not None:
            whale_conf[wn].append(float(row["confidence"]))
        if row["edge_score"] is not None:
            whale_edge[wn].append(float(row["edge_score"]))

    # Sort by total volume
    whale_totals = {wn: sum(cats.values()) for wn, cats in whale_cats.items()}
    sorted_whales = sorted(whale_totals.keys(), key=lambda w: whale_totals[w], reverse=True)

    print()
    print("=" * 90)
    print(f"  WHALE CONCENTRATION ANALYSIS  —  as of {TODAY}")
    print("=" * 90)
    print()
    print(f"  {'Whale':<40} {'Total':>6}  Categories (count)")
    print(f"  {'-'*40} {'-'*6}  {'-'*30}")
    for wn in sorted_whales[:30]:   # Top 30
        cats = whale_cats[wn]
        total = whale_totals[wn]
        avg_conf = sum(whale_conf[wn]) / len(whale_conf[wn]) if whale_conf[wn] else 0
        avg_edge = sum(whale_edge[wn]) / len(whale_edge[wn]) if whale_edge[wn] else 0
        cat_str = " | ".join(f"{c}={n}" for c, n in sorted(cats.items(), key=lambda x: x[1], reverse=True))
        print(f"  {wn:<40} {total:>6}  {cat_str}")
        print(f"    {'  Avg Conf':>42} {fmt_pct(avg_conf)}   {'Avg Edge':>9} {fmt_float(avg_edge)}")

    # Unknown whale breakdown
    print()
    print(f"  {'─'*90}")
    print(f"  UNKNOWN WHALE DETAIL")
    print(f"  {'─'*90}")
    unknown_rows = [r for r in snapshots if (r["whale_name"] or "").lower() in ("unknown", "", "unknown whale")]
    if unknown_rows:
        unknown_cats = defaultdict(int)
        for r in unknown_rows:
            unknown_cats[(r["category"] or "unknown").strip().lower()] += 1
        print()
        print(f"  {'Category':<12} {'Count':>6}  {'Avg Conf':>10}  {'Avg Edge':>10}")
        print(f"  {'-'*12} {'-'*6}  {'-'*10}  {'-'*10}")
        for cat, cnt in sorted(unknown_cats.items(), key=lambda x: x[1], reverse=True):
            conf_vals = [r["confidence"] for r in unknown_rows if (r["category"] or "").strip().lower() == cat and r["confidence"] is not None]
            edge_vals = [r["edge_score"] for r in unknown_rows if (r["category"] or "").strip().lower() == cat and r["edge_score"] is not None]
            avg_conf = sum(conf_vals) / len(conf_vals) if conf_vals else 0
            avg_edge = sum(edge_vals) / len(edge_vals) if edge_vals else 0
            print(f"  {cat:<12} {cnt:>6}  {fmt_pct(avg_conf):>10}  {fmt_float(avg_edge):>10}")
    else:
        print()
        print("  No unknown whale signals in decision_snapshots.")


# ────────────────────────────────────────────────────────────────────────────
# 4. Sports Telemetry Preview
# ────────────────────────────────────────────────────────────────────────────
def build_sports_telemetry_preview(snapshots: list[dict]) -> None:
    """Show what the sports signals would look like if the pipeline evaluated them fully."""
    sports_rows = [r for r in snapshots if (r["category"] or "").strip().lower() == "sports"]
    if not sports_rows:
        print()
        print("  No sports signals in decision_snapshots yet.")
        return

    print()
    print("=" * 90)
    print(f"  SPORTS TELEMETRY PREVIEW  —  as of {TODAY}")
    print(f"  (signals blocked by sports_quarantine — would they pass other gates?)")
    print("=" * 90)

    quarantine_rows = [r for r in sports_rows if r["reject_reason"] == "sports_quarantine"]
    non_quarantine = [r for r in sports_rows if r["reject_reason"] != "sports_quarantine"]

    print()
    print(f"  Sports snapshots: {len(sports_rows)} total")
    print(f"    Blocked by sports_quarantine: {len(quarantine_rows)}")
    print(f"    Blocked for other reasons:     {len(non_quarantine)}")

    if quarantine_rows:
        conf_vals = [r["confidence"] for r in quarantine_rows if r["confidence"] is not None]
        edge_vals = [r["edge_score"] for r in quarantine_rows if r["edge_score"] is not None]
        avg_conf = sum(conf_vals) / len(conf_vals) if conf_vals else 0
        avg_edge = sum(edge_vals) / len(edge_vals) if edge_vals else 0
        print()
        print(f"  sports_quarantine blocked signals:")
        print(f"    Count:    {len(quarantine_rows)}")
        print(f"    Avg Conf: {fmt_pct(avg_conf)}")
        print(f"    Avg Edge: {fmt_float(avg_edge)}")

    # Confidence distribution for sports signals
    print()
    print(f"  Sports signal confidence distribution:")
    buckets = {"<0.30": 0, "0.30-0.45": 0, "0.45-0.55": 0, "0.55-0.70": 0, ">=0.70": 0}
    for r in sports_rows:
        c = r["confidence"]
        if c is None:
            continue
        if c < 0.30:
            buckets["<0.30"] += 1
        elif c < 0.45:
            buckets["0.30-0.45"] += 1
        elif c < 0.55:
            buckets["0.45-0.55"] += 1
        elif c < 0.70:
            buckets["0.55-0.70"] += 1
        else:
            buckets[">=0.70"] += 1
    for k, v in buckets.items():
        bar = "█" * min(v // 5, 20)
        pct = v / len(sports_rows) * 100 if sports_rows else 0
        print(f"    {k:<12} {v:>5}  {pct:>5.1f}%  {bar}")


# ────────────────────────────────────────────────────────────────────────────
def main():
    print()
    print(f"╔{'═'*88}╗")
    print(f"║  CATEGORY ALPHA DISCOVERY REPORT  —  {TODAY:<35}║")
    print(f"╚{'═'*88}╝")

    # Load data
    snapshots = get_decision_snapshots()
    trades = get_trades()
    whale_state = get_whale_state()

    print(f"\n  Data: {len(snapshots)} decision_snapshots, {len(trades)} trades, "
          f"{len(whale_state.get('whales', {}))} whales in state")

    if not snapshots:
        print()
        print("  ⚠ decision_snapshots is empty — run the pipeline first to collect data.")
        return

    # 1. Signal Generation Heatmap
    build_signal_heatmap(snapshots)

    # 2. Historical Profitability
    build_profitability_report(trades)

    # 3. Whale Concentration
    build_whale_concentration(snapshots)

    # 4. Sports Telemetry Preview
    build_sports_telemetry_preview(snapshots)

    print()
    print(f"  Report generated: {datetime.now(timezone.utc).isoformat()}")
    print()


if __name__ == "__main__":
    main()
