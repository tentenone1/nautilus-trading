#!/usr/bin/env python3
"""
monte_carlo_simulation.py — Statistical Monte Carlo simulation for Hermes Trading System.

Shuffles the sequence of closed trades 10,000 times to build a distribution of
possible equity curves. Answers the question: is the observed P&L a statistical
edge, or was it lucky sequencing?

Usage:
    python3 scripts/monte_carlo_simulation.py [--trades N] [--sims SIMS] [--seed SEED]
    python3 scripts/monte_carlo_simulation.py --dry-run   # quick sanity check
"""

import argparse
import random
import statistics
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DB = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
LOG = Path("/home/elon-1/.hermes/logs/monte-carlo.log")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    line = f"[{ts}] [monte-carlo] {msg}"
    print(line)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def load_clean_trades():
    """Load post-May-14 clean trades with realized PnL, ordered chronologically."""
    if not DB.exists():
        log(f"ERROR: trades.db not found at {DB}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute("""
        SELECT realized_pnl
        FROM trades
        WHERE data_quality = 'clean'
          AND realized_pnl IS NOT NULL
        ORDER BY timestamp ASC
    """)
    pnls = [row[0] for row in cur.fetchall()]
    conn.close()

    if len(pnls) < 30:
        log(f"WARNING: only {len(pnls)} clean trades — results may be unreliable")

    log(f"Loaded {len(pnls)} clean trades from {DB}")
    return pnls


def equity_curve(pnls):
    """Return running equity curve (list of cumulative sums)."""
    equity = []
    total = 0.0
    for p in pnls:
        total += p
        equity.append(total)
    return equity


def max_drawdown(equity):
    """Return the maximum drawdown (positive number = depth)."""
    peak = -999999
    max_dd = 0.0
    for val in equity:
        if val > peak:
            peak = val
        dd = peak - val
        if dd > max_dd:
            max_dd = dd
    return max_dd


def simulate(pnls, n_sims, seed=None):
    """
    Run N simulations of shuffled trade sequences.
    Returns dict of arrays: equity_curves, final_pnls, max_drawdowns, dd_starts.
    """
    if seed is not None:
        random.seed(seed)

    n = len(pnls)
    final_pnls = []
    max_dds = []
    dd_depths_at_50pct = []   # drawdown at 50% of the equity curve

    # We only store every 10th equity curve to save memory
    sample_curves = []

    for i in range(n_sims):
        sim_pnls = pnls[:]  # copy
        random.shuffle(sim_pnls)
        eq = equity_curve(sim_pnls)
        final_pnls.append(eq[-1])
        dd = max_drawdown(eq)
        max_dds.append(dd)

        # Store at 50% mark
        mid = n // 2
        dd_at_half = max(0, max(equity_curve(pnls[:mid])) - min(eq[:mid])) if mid > 0 else 0
        dd_depths_at_50pct.append(dd_at_half)

        if i % 1000 == 0:
            log(f"  simulation {i}/{n_sims}")

        if i < 100:  # store first 100 for percentile reference
            sample_curves.append(eq)

    return {
        "final_pnls": final_pnls,
        "max_drawdowns": max_dds,
        "dd_at_50pct": dd_depths_at_50pct,
        "sample_curves": sample_curves,
    }


def percentile(sorted_vals, p):
    """Return the p-th percentile (0-100) of a sorted list."""
    if not sorted_vals:
        return 0.0
    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def summarize(results, pnls, n_sims):
    """Print a full statistical report."""
    final_pnls = results["final_pnls"]
    max_dds = results["max_drawdowns"]

    # Sort for percentile calculations
    sorted_final = sorted(final_pnls)
    sorted_dds = sorted(max_dds)

    # Real (non-shuffled) equity curve
    real_eq = equity_curve(pnls)
    real_final = real_eq[-1]
    real_max_dd = max_drawdown(real_eq)

    n = len(pnls)
    mean_final = statistics.mean(final_pnls)
    median_final = statistics.median(final_pnls)
    mean_dd = statistics.mean(max_dds)
    median_dd = statistics.median(max_dds)

    # What fraction of simulations produced less than the observed P&L
    pct_under_perf = sum(1 for x in final_pnls if x < real_final) / len(final_pnls) * 100

    # What fraction of simulations had a worse max drawdown than observed
    pct_worse_dd = sum(1 for x in max_dds if x > real_max_dd) / len(max_dds) * 100

    # Probability of >50% drawdown (drawdown > real_final since we start near 0)
    # Use max dd exceeding 50% of equity peak
    threshold_50pct_dd = real_max_dd * 0.5
    prob_50pct_dd = sum(1 for x in max_dds if x > threshold_50pct_dd) / len(max_dds) * 100

    # Probability of losing money overall
    prob_loss = sum(1 for x in final_pnls if x < 0) / len(final_pnls) * 100

    # Position sizing recommendation: reduce until 95th pctile DD < 15% of bankroll
    # For now, compute suggested Kelly fraction from win rate
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    if wins and losses:
        win_rate = len(wins) / n
        avg_win = statistics.mean(wins)
        avg_loss = abs(statistics.mean(losses))
        if avg_loss > 0:
            W = win_rate
            R = avg_win / avg_loss
            kelly = (W * R - (1 - W)) / R
            kelly_frac = max(0.0, min(1.0, kelly))
        else:
            kelly_frac = 0.0
    else:
        kelly_frac = 0.0

    print()
    print("=" * 70)
    print("  HERMES TRADING SYSTEM — MONTE CARLO SIMULATION REPORT")
    print("=" * 70)
    print(f"  Generated:     {datetime.now().isoformat()}")
    print(f"  Simulations:   {n_sims:,}")
    print(f"  Clean trades: {n}")
    print(f"  Date range:   May 14–May 25, 2026")
    print()
    print("─" * 70)
    print("  OBSERVED (REAL) PERFORMANCE")
    print("─" * 70)
    print(f"  Final P&L:           ${real_final:.2f}")
    print(f"  Max drawdown:        ${real_max_dd:.2f}")
    print(f"  Win rate:            {len(wins)/n:.2%}  ({len(wins)} wins / {len(losses)} losses)")
    if wins and losses:
        print(f"  Avg win:             ${statistics.mean(wins):.4f}")
        print(f"  Avg loss:            ${abs(statistics.mean(losses)):.4f}")
        print(f"  Profit factor:       {statistics.mean(wins)/abs(statistics.mean(losses)):.2f}")
    print()
    print("─" * 70)
    print("  MONTE CARLO DISTRIBUTION")
    print("─" * 70)
    print(f"  Simulated final P&L — mean:   ${mean_final:.2f}")
    print(f"  Simulated final P&L — median: ${median_final:.2f}")
    print(f"  Simulated final P&L — p5:     ${percentile(sorted_final, 5):.2f}")
    print(f"  Simulated final P&L — p25:    ${percentile(sorted_final, 25):.2f}")
    print(f"  Simulated final P&L — p50:    ${percentile(sorted_final, 50):.2f}")
    print(f"  Simulated final P&L — p75:    ${percentile(sorted_final, 75):.2f}")
    print(f"  Simulated final P&L — p95:   ${percentile(sorted_final, 95):.2f}")
    print()
    print(f"  Simulated max DD — mean:      ${mean_dd:.2f}")
    print(f"  Simulated max DD — median:    ${median_dd:.2f}")
    print(f"  Simulated max DD — p5:       ${percentile(sorted_dds, 5):.2f}")
    print(f"  Simulated max DD — p95:      ${percentile(sorted_dds, 95):.2f}")
    print()
    print("─" * 70)
    print("  EDGE SIGNIFICANCE (is this luck or skill?)")
    print("─" * 70)
    print(f"  P(real P&L ≤ simulated):   {pct_under_perf:.1f}%  ← higher = stronger edge")
    if pct_under_perf >= 95:
        print(f"  ★ EDGE CONFIRMED — observed P&L in top 5% of all simulations")
    elif pct_under_perf >= 90:
        print(f"  ● EDGE LIKELY — observed P&L in top 10% of all simulations")
    else:
        print(f"  ⚠ EDGE UNCLEAR — results may be within random variation")
    print()
    print(f"  P(DD > observed DD):        {pct_worse_dd:.1f}%  ← lower = more robust DD")
    print(f"  P(final P&L < 0):           {prob_loss:.1f}%  ← lower = more consistent profitability")
    print(f"  P(DD > 50% of observed DD): {prob_50pct_dd:.1f}%")
    print()
    print("─" * 70)
    print("  POSITION SIZING RECOMMENDATION")
    print("─" * 70)
    print(f"  Implied Kelly fraction:      {kelly_frac:.1%}")
    print(f"  Conservative (¼ Kelly):     {kelly_frac*0.25:.1%}")
    print(f"  Suggested max risk/trade:   {kelly_frac*0.25 * 100:.1f}% of bankroll")
    print(f"  P95 simulated max DD:        ${percentile(sorted_dds, 95):.2f}")
    print(f"  Risk of ruin (P95 DD):       {'LOW' if prob_loss < 5 else 'MODERATE' if prob_loss < 20 else 'HIGH'}")
    print()
    print("=" * 70)

    # Return verdict
    return {
        "real_final": real_final,
        "real_max_dd": real_max_dd,
        "pct_under_perf": pct_under_perf,
        "prob_loss": prob_loss,
        "pct_worse_dd": pct_worse_dd,
        "edge_confirmed": pct_under_perf >= 95,
        "kelly_frac": kelly_frac,
        "p95_max_dd": percentile(sorted_dds, 95),
    }


def main():
    parser = argparse.ArgumentParser(description="Monte Carlo trade simulation")
    parser.add_argument("--trades", type=int, default=0,
                        help="Use only the last N clean trades (0 = all)")
    parser.add_argument("--sims", type=int, default=10000,
                        help="Number of simulations (default: 10000)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42 — set to 0 for random)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run 100 simulations for quick sanity check")
    args = parser.parse_args()

    if args.dry_run:
        args.sims = 100

    LOG.parent.mkdir(parents=True, exist_ok=True)
    log(f"Starting Monte Carlo — {args.sims} simulations")

    pnls = load_clean_trades()
    if args.trades > 0:
        pnls = pnls[-args.trades:]
        log(f"Using last {args.trades} trades")

    if not pnls:
        log("ERROR: no trades loaded")
        sys.exit(1)

    results = simulate(pnls, args.sims, seed=args.seed if args.seed > 0 else None)
    verdict = summarize(results, pnls, args.sims)

    log(f"Done. Edge confirmed: {verdict['edge_confirmed']}")
    return 0 if verdict["edge_confirmed"] else 1


if __name__ == "__main__":
    sys.exit(main())
