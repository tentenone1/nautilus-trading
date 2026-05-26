#!/usr/bin/env python3
"""
walk_forward_validation.py — Walk-forward out-of-sample validation for Hermes Trading System.

Splits post-May-14 clean trades into rolling train/test windows:
  - Train: 7 days
  - Test:  3 days
  - Step:  3 days

For each window, computes test-set P&L, profit factor, and win rate.
If edge degrades significantly in OOS windows, the system is likely curve-fit.

Usage:
    python3 scripts/walk_forward_validation.py
    python3 scripts/walk_forward_validation.py --train-days 14 --test-days 7
"""

import argparse
import sqlite3
import statistics
import sys
from datetime import datetime, timedelta
from pathlib import Path

DB = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
LOG = Path("/home/elon-1/.hermes/logs/walk-forward.log")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    line = f"[{ts}] [walk-fwd] {msg}"
    print(line)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def load_clean_trades():
    """Load all clean trades as list of dicts."""
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_id, timestamp, realized_pnl, realized_return,
               position_size_usd, category, config_version
        FROM trades
        WHERE data_quality = 'clean'
          AND realized_pnl IS NOT NULL
        ORDER BY timestamp ASC
    """)
    rows = cur.fetchall()
    conn.close()

    trades = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(str(r[1]).replace("Z", "+00:00"))
        except Exception:
            continue
        trades.append({
            "trade_id": r[0],
            "timestamp": ts,
            "realized_pnl": r[2],
            "realized_return": r[3],
            "position_size_usd": r[4],
            "category": r[5],
            "config_version": r[6],
        })
    return trades


def window_stats(trades):
    """Compute stats for a list of trades."""
    if not trades:
        return None
    pnls = [t["realized_pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    n = len(pnls)
    win_rate = len(wins) / n if n > 0 else 0
    total_pnl = sum(pnls)
    avg_win = statistics.mean(wins) if wins else 0
    avg_loss = abs(statistics.mean(losses)) if losses else 0
    profit_factor = (avg_win / avg_loss) if avg_loss > 0 else float("inf") if avg_win > 0 else 0
    return {
        "n": n,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": profit_factor,
        "wins": len(wins),
        "losses": len(losses),
    }


def run_walk_forward(trades, train_days=7, test_days=3, step_days=3):
    """
    Rolling window walk-forward.
    Returns list of window results.
    """
    if not trades:
        return []

    # Determine date range
    start_dt = trades[0]["timestamp"]
    end_dt = trades[-1]["timestamp"]
    total_days = (end_dt - start_dt).days + 1

    log(f"Trade range: {start_dt.date()} → {end_dt.date()} ({total_days} days)")
    log(f"Windows: train={train_days}d, test={test_days}d, step={step_days}d")

    windows = []
    cursor = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)  # naive midnight

    # Use date() for comparisons to avoid timezone-aware vs naive comparison issues
    cursor_date = cursor.date()

    while True:
        train_end = cursor + timedelta(days=train_days - 1, hours=23, minutes=59, seconds=59)
        test_start = train_end + timedelta(seconds=1)
        test_end = test_start + timedelta(days=test_days - 1, hours=23, minutes=59, seconds=59)

        if test_start.date() > end_dt.date():
            break

        train_trades = [t for t in trades if cursor_date <= t["timestamp"].date() <= train_end.date()]
        test_trades = [t for t in trades if test_start.date() <= t["timestamp"].date() <= test_end.date()]

        train_stats = window_stats(train_trades)
        test_stats = window_stats(test_trades)

        # Compute OOS Sharpe approximation (assuming equal-weighted daily returns)
        # Simple: mean(daily_pnl) / stdev(daily_pnl) * sqrt252
        daily_pnls = {}
        for t in test_trades:
            day = t["timestamp"].date()
            daily_pnls[day] = daily_pnls.get(day, 0) + t["realized_pnl"]

        if len(daily_pnls) >= 2:
            dvals = list(daily_pnls.values())
            dmean = statistics.mean(dvals)
            dstdev = statistics.stdev(dvals) if len(dvals) > 1 else 0
            oos_sharpe = (dmean / dstdev * (252 ** 0.5)) if dstdev > 0 else (float("inf") if dmean > 0 else float("-inf"))
        else:
            oos_sharpe = None

        window = {
            "train_start": cursor.date(),
            "train_end": train_end.date(),
            "test_start": test_start.date(),
            "test_end": test_end.date(),
            "train_n": len(train_trades),
            "test_n": len(test_trades),
            "train_stats": train_stats,
            "test_stats": test_stats,
            "oos_sharpe": oos_sharpe,
        }
        windows.append(window)

        cursor += timedelta(days=step_days)
        cursor_date = cursor.date()

    return windows


def summarize(windows):
    """Print full report."""
    if not windows:
        print("No windows generated — check date range and window sizes.")
        return

    print()
    print("=" * 80)
    print("  HERMES — WALK-FORWARD OUT-OF-SAMPLE VALIDATION REPORT")
    print("=" * 80)
    print(f"  Generated: {datetime.now().isoformat()}")
    print(f"  Windows:   {len(windows)}")
    print()

    # Table header
    print(f"  {'Window':<25} {'Train':>7} {'Train':>8} {'Train':>8} "
          f"{'Test':>6} {'Test':>8} {'Test':>7} {'Test':>8} {'OOS':>7}")
    print(f"  {'Train dates':<25} {'n':>7} {'Win%':>8} {'PF':>8} "
          f"{'n':>6} {'Win%':>8} {'P&L':>7} {'PF':>8} {'Sharpe':>7}")
    print("  " + "─" * 78)

    oos_pnls = []
    oos_pfs = []
    oos_wrs = []
    oos_sharpes = []

    for w in windows:
        ts = w["train_stats"]
        tes = w["test_stats"]

        train_n = w["train_n"]
        train_wr = f"{ts['win_rate']:.0%}" if ts else "N/A"
        train_pf = f"{ts['profit_factor']:.2f}" if ts and ts["profit_factor"] != float("inf") else "∞"
        test_n = w["test_n"]
        test_wr = f"{tes['win_rate']:.0%}" if tes else "N/A"
        test_pnl = f"${tes['total_pnl']:.2f}" if tes else "N/A"
        test_pf = f"{tes['profit_factor']:.2f}" if tes and tes["profit_factor"] != float("inf") else "∞"
        oos_sh = f"{w['oos_sharpe']:.2f}" if w["oos_sharpe"] is not None else "N/A"

        window_label = f"{w['train_start']} – {w['train_end']}"
        test_label = f"{w['test_start']} – {w['test_end']}"

        print(f"  {window_label:<25} {train_n:>7} {train_wr:>8} {train_pf:>8} "
              f"{test_n:>6} {test_wr:>8} {test_pnl:>7} {test_pf:>8} {oos_sh:>7}")

        if tes:
            oos_pnls.append(tes["total_pnl"])
            oos_pfs.append(tes["profit_factor"])
            oos_wrs.append(tes["win_rate"])
        if w["oos_sharpe"] is not None:
            oos_sharpes.append(w["oos_sharpe"])

    print()
    print("─" * 80)
    print("  AGGREGATE OUT-OF-SAMPLE STATISTICS")
    print("─" * 80)

    if oos_pnls:
        print(f"  Total OOS P&L:          ${sum(oos_pnls):.2f}  "
              f"({'+' if sum(oos_pnls) > 0 else ''}{sum(oos_pnls):.2f})")
        print(f"  Avg OOS P&L/window:    ${statistics.mean(oos_pnls):.2f}")
        print(f"  OOS windows positive:  {sum(1 for p in oos_pnls if p > 0)}/{len(oos_pnls)}  "
              f"({sum(1 for p in oos_pnls if p > 0)/len(oos_pnls):.0%})")
    if oos_pfs:
        print(f"  Avg OOS profit factor:  {statistics.mean(oos_pfs):.2f}")
        print(f"  Min OOS profit factor: {min(oos_pfs):.2f}")
    if oos_wrs:
        print(f"  Avg OOS win rate:       {statistics.mean(oos_wrs):.0%}")
    if oos_sharpes:
        valid_sharpes = [s for s in oos_sharpes if abs(s) != float("inf")]
        if valid_sharpes:
            print(f"  Avg OOS Sharpe:        {statistics.mean(valid_sharpes):.2f}")
            print(f"  Min OOS Sharpe:        {min(valid_sharpes):.2f}")
        else:
            print(f"  OOS Sharpe:             ∞ (all returns in same direction)")

    print()
    print("─" * 80)
    print("  VERDICT")
    print("─" * 80)

    # Criteria
    pf_pass = statistics.mean(oos_pfs) >= 1.0 if oos_pfs else False
    wr_pass = statistics.mean(oos_wrs) >= 0.35 if oos_wrs else False
    pnl_pass = sum(oos_pnls) > 0 if oos_pnls else False
    sharpe_pass = statistics.mean(valid_sharpes) > 0.5 if valid_sharpes else None

    print(f"  {'Profit factor ≥ 1.0':<35} {'PASS' if pf_pass else 'FAIL':>8}  "
          f"(avg OOS PF: {statistics.mean(oos_pfs):.2f})" if oos_pfs else "  PF: N/A")
    print(f"  {'Win rate ≥ 35%':<35} {'PASS' if wr_pass else 'FAIL':>8}  "
          f"(avg OOS WR: {statistics.mean(oos_wrs):.0%})" if oos_wrs else "  WR: N/A")
    print(f"  {'Total OOS P&L > 0':<35} {'PASS' if pnl_pass else 'FAIL':>8}  "
          f"(${sum(oos_pnls):.2f})" if oos_pnls else "  P&L: N/A")
    if sharpe_pass is not None:
        print(f"  {'Sharpe > 0.5 (OOS)':<35} {'PASS' if sharpe_pass else 'FAIL':>8}  "
              f"(avg OOS Sharpe: {statistics.mean(valid_sharpes):.2f})")
    elif valid_sharpes:
        print(f"  {'Sharpe > 0.5 (OOS)':<35} {'N/A (insufficient data)':>30}")

    overall_pass = pf_pass and wr_pass and pnl_pass
    print()
    if overall_pass:
        print(f"  ★ WALK-FORWARD EDGE CONFIRMED — system is NOT curve-fit")
    else:
        print(f"  ⚠ EDGE DEGRADED IN OOS — system may be curve-fit to training data")

    print()
    print("=" * 80)

    return {
        "windows": len(windows),
        "oos_pf_avg": statistics.mean(oos_pfs) if oos_pfs else None,
        "oos_wr_avg": statistics.mean(oos_wrs) if oos_wrs else None,
        "oos_pnl_total": sum(oos_pnls) if oos_pnls else 0,
        "pass": overall_pass,
    }


def main():
    parser = argparse.ArgumentParser(description="Walk-forward out-of-sample validation")
    parser.add_argument("--train-days", type=int, default=7,
                        help="Training window in days (default: 7)")
    parser.add_argument("--test-days", type=int, default=3,
                        help="Test window in days (default: 3)")
    parser.add_argument("--step-days", type=int, default=3,
                        help="Step between windows in days (default: 3)")
    args = parser.parse_args()

    LOG.parent.mkdir(parents=True, exist_ok=True)
    log(f"Starting walk-forward validation")

    trades = load_clean_trades()
    if not trades:
        log("ERROR: no clean trades found")
        sys.exit(1)

    log(f"Loaded {len(trades)} clean trades")

    windows = run_walk_forward(trades,
                               train_days=args.train_days,
                               test_days=args.test_days,
                               step_days=args.step_days)

    result = summarize(windows)
    log(f"Done. Overall pass: {result['pass']}")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
