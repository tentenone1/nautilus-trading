#!/usr/bin/env python3
"""Clean Fade Backtest — Corrected slippage model.

Key fix: Calculate raw P&L from entry/exit prices, then compare against 
simulated P&L with realistic costs. This avoids comparing apples-to-oranges 
with the DB's realized_pnl which already includes the paper system's slippage.

Also properly handles voided trades and edge cases.
"""

import sqlite3
import sys
import json
import random
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass

sys.path.insert(0, str(Path(__file__).parent.parent))

PROJECT_DIR = Path(__file__).parent.parent
TRADES_DB = PROJECT_DIR / "data" / "trades.db"
BACKTEST_DB = PROJECT_DIR / "data" / "backtest_results.db"
CLEAN_DATA_FROM = "2026-05-14"
TRAIN_CUTOFF = "2026-05-21"

# Realistic slippage parameters
BASE_SLIPPAGE = 0.015       # 1.5% base
SPREAD_HALF = 0.005         # 0.5% half-spread (always present)
STOP_FILL_PROB = 0.70       # 70% fill at stop
STOP_WORSE_SLIP = 0.03      # 3% worse if stop misses

VOIDED_REASONS = {"legacy_voided", "orphan_cleanup", "orphaned_position_cleanup", 
                  "sybil_cleanup", "pending"}

CATEGORY_SLIP = {
    "crypto": 1.5, "economics": 1.2, "geopolitics": 1.2,
    "general": 1.0, "politics": 1.0, "technology": 1.0, "sports": 0.8,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s BACKTEST | %(message)s")
log = logging.getLogger(__name__)


def calc_raw_pnl(entry_price, exit_price, side, size_usd):
    """Calculate raw P&L from prices and position size."""
    if entry_price <= 0 or exit_price <= 0:
        return 0.0
    shares = size_usd / entry_price
    if side == "BUY":
        return (exit_price - entry_price) * shares
    else:
        return (entry_price - exit_price) * shares


def apply_slip(price, side, category, is_exit=False, is_stop_miss=False):
    """Apply realistic slippage to a price."""
    slip = BASE_SLIPPAGE * CATEGORY_SLIP.get(category, 1.0) + SPREAD_HALF
    
    # Low-priced contracts have higher relative friction
    if price < 0.10:
        slip *= 1.5
    elif price < 0.20:
        slip *= 1.2
    
    if is_exit:
        slip *= 1.2  # Exits are harder
    
    if is_stop_miss:
        slip += STOP_WORSE_SLIP
    
    if side in ("BUY",):
        return min(price * (1 + slip), 0.99)
    else:
        return max(price * (1 - slip), 0.01)


def calc_sim_pnl(entry_price, exit_price, side, size_usd, category, is_stop):
    """Calculate simulated P&L with realistic slippage."""
    sim_entry = apply_slip(entry_price, side, category)
    
    # Determine exit side
    exit_side = "SELL" if side == "BUY" else "BUY"
    
    # Model stop fills
    if is_stop:
        random.seed(hash(f"{entry_price}{exit_price}{side}{category}"))
        if random.random() < STOP_FILL_PROB:
            sim_exit = apply_slip(exit_price, exit_side, category, is_exit=True)
        else:
            sim_exit = apply_slip(exit_price, exit_side, category, is_exit=True, is_stop_miss=True)
    else:
        sim_exit = apply_slip(exit_price, exit_side, category, is_exit=True)
    
    shares = size_usd / sim_entry
    if side == "BUY":
        return (sim_exit - sim_entry) * shares
    else:
        return (sim_entry - sim_exit) * shares


def run_backtest(dataset="all"):
    conn = sqlite3.connect(str(TRADES_DB))
    cur = conn.cursor()
    
    date_filter = f"timestamp >= '{CLEAN_DATA_FROM}'"
    if dataset == "train":
        date_filter += f" AND timestamp < '{TRAIN_CUTOFF}'"
    elif dataset == "test":
        date_filter += f" AND timestamp >= '{TRAIN_CUTOFF}'"
    
    voided = "', '".join(VOIDED_REASONS)
    
    cur.execute(f"""
        SELECT trade_id, whale_name, category, side, entry_price, exit_price,
               position_size_usd, realized_pnl, market_title, timestamp, exit_reason
        FROM trades 
        WHERE exit_reason IS NOT NULL 
          AND exit_reason NOT IN ('{voided}')
          AND entry_price > 0 AND exit_price > 0
          AND position_size_usd > 0
          AND {date_filter}
        ORDER BY timestamp
    """)
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        log.warning("No trades for dataset=%s", dataset)
        return None
    
    # Calculate raw and simulated P&L for each trade
    total_raw = 0
    total_sim = 0
    raw_wins = 0
    sim_wins = 0
    raw_profit = 0
    raw_loss = 0
    sim_profit = 0
    sim_loss = 0
    cat_stats = {}
    
    for row in rows:
        (trade_id, whale_name, category, side, entry_price, exit_price,
         size_usd, db_pnl, market_title, timestamp, exit_reason) = row
        
        is_stop = exit_reason and "stop" in exit_reason.lower()
        
        raw_pnl = calc_raw_pnl(entry_price, exit_price, side, size_usd)
        sim_pnl = calc_sim_pnl(entry_price, exit_price, side, size_usd, category, is_stop)
        
        total_raw += raw_pnl
        total_sim += sim_pnl
        
        if raw_pnl > 0:
            raw_wins += 1
            raw_profit += raw_pnl
        else:
            raw_loss += abs(raw_pnl)
        
        if sim_pnl > 0:
            sim_wins += 1
            sim_profit += sim_pnl
        else:
            sim_loss += abs(sim_pnl)
        
        if category not in cat_stats:
            cat_stats[category] = {"raw": 0, "sim": 0, "n": 0, "raw_w": 0, "sim_w": 0}
        cat_stats[category]["raw"] += raw_pnl
        cat_stats[category]["sim"] += sim_pnl
        cat_stats[category]["n"] += 1
        if raw_pnl > 0:
            cat_stats[category]["raw_w"] += 1
        if sim_pnl > 0:
            cat_stats[category]["sim_w"] += 1
    
    n = len(rows)
    raw_wr = raw_wins / n * 100
    sim_wr = sim_wins / n * 100
    raw_pf = raw_profit / raw_loss if raw_loss > 0 else 0
    sim_pf = sim_profit / sim_loss if sim_loss > 0 else 0
    cost_pct = (1 - total_sim / total_raw) * 100 if total_raw != 0 else 0
    
    return {
        "n": n, "raw_pnl": total_raw, "sim_pnl": total_sim,
        "raw_wr": raw_wr, "sim_wr": sim_wr, "raw_pf": raw_pf, "sim_pf": sim_pf,
        "cost_pct": cost_pct, "cats": cat_stats,
    }


def print_results(label, r):
    if not r:
        return
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Trades: {r['n']}")
    print(f"  Raw P&L:     ${r['raw_pnl']:>10,.2f}  WR={r['raw_wr']:.1f}%  PF={r['raw_pf']:.2f}")
    print(f"  Sim P&L:     ${r['sim_pnl']:>10,.2f}  WR={r['sim_wr']:.1f}%  PF={r['sim_pf']:.2f}")
    print(f"  Cost impact: {r['cost_pct']:.1f}% reduction from realistic execution")
    print(f"\n  Per-category (sim P&L):")
    for cat, s in sorted(r['cats'].items(), key=lambda x: -x[1]['sim']):
        raw_wr = s['raw_w'] / s['n'] * 100
        sim_wr = s['sim_w'] / s['n'] * 100
        print(f"    {cat:15s} | {s['n']:4d} trades | raw=${s['raw']:>8.2f} ({raw_wr:.0f}%) | sim=${s['sim']:>8.2f} ({sim_wr:.0f}%)")


def main():
    print("=" * 60)
    print("  CLEAN FADE BACKTEST — Realistic Execution Model")
    print("  Base slippage: 1.5% | Spread: 0.5% | Stop fill: 70%")
    print(f"  Train: May 14-21 | Test: post-May-21")
    print("=" * 60)
    
    r_all = run_backtest("all")
    r_train = run_backtest("train")
    r_test = run_backtest("test")
    
    print_results("ALL CLEAN DATA (post-May-14)", r_all)
    print_results("TRAIN SET (May 14-21)", r_train)
    print_results("TEST SET — OUT-OF-SAMPLE (post-May-21)", r_test)
    
    if r_train and r_test:
        print(f"\n{'='*60}")
        print("  OUT-OF-SAMPLE COMPARISON")
        print(f"{'='*60}")
        print(f"  Train: sim P&L=${r_train['sim_pnl']:,.2f} | PF={r_train['sim_pf']:.2f} | WR={r_train['sim_wr']:.1f}%")
        print(f"  Test:  sim P&L=${r_test['sim_pnl']:,.2f} | PF={r_test['sim_pf']:.2f} | WR={r_test['sim_wr']:.1f}%")
        
        if r_test['sim_pf'] > 1.2:
            print(f"\n  VERDICT: PASS — Edge survives realistic costs (PF={r_test['sim_pf']:.2f})")
        elif r_test['sim_pf'] > 1.0:
            print(f"\n  VERDICT: MARGINAL — Edge barely positive (PF={r_test['sim_pf']:.2f})")
        else:
            print(f"\n  VERDICT: FAIL — No edge after realistic costs (PF={r_test['sim_pf']:.2f})")
            print(f"  The system loses ${abs(r_test['sim_pnl']):,.2f} on {r_test['n']} out-of-sample trades")
        
        # Key finding: general category (autoresearch) performance
        gen_train = r_train['cats'].get('general', {})
        gen_test = r_test['cats'].get('general', {})
        if gen_train and gen_test:
            print(f"\n  AUTOBACKTEST (general category only):")
            gen_train_pf = gen_train['sim'] / abs(min(-1, gen_train.get('sim', 0))) if gen_train['sim'] > 0 else 0
            gen_test_sim = gen_test.get('sim', 0)
            print(f"    Train: {gen_train['n']} trades, sim=${gen_train['sim']:.2f}")
            print(f"    Test:  {gen_test['n']} trades, sim=${gen_test_sim:.2f}")


if __name__ == "__main__":
    main()
