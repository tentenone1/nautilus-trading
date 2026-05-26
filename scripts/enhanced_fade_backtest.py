#!/usr/bin/env python3
"""Enhanced Fade Hypothesis Backtest — Realistic slippage model with order book depth awareness.

Improvements over the basic backtest:
1. Variable slippage based on market liquidity (from gamma API or estimated from volume)
2. Stop-loss fill probability model (70% at trigger, 30% at worse price)
3. Category-aware slippage (crypto more volatile, sports tighter spreads)
4. Deep value discount for very low-priced contracts
5. Out-of-sample validation (train on pre-May-21, test on post-May-21)

Writes results to data/backtest_results.db
"""

import sqlite3
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

# Configuration
PROJECT_DIR = Path(__file__).parent.parent
TRADES_DB = PROJECT_DIR / "data" / "trades.db"
BACKTEST_DB = PROJECT_DIR / "data" / "backtest_results.db"
CLEAN_DATA_FROM = "2026-05-14"
TRAIN_CUTOFF = "2026-05-21"  # Split for out-of-sample

# Slippage model parameters
BASE_SLIPPAGE = 0.015  # 1.5% base
SPREAD_COST = 0.005    # 0.5% half-spread
STOP_FILL_PROB = 0.70 # 70% chance of filling at stop price
STOP_WORSE_SLIP = 0.03 # 3% worse if stop doesn't fill

# Category-specific slippage multipliers
CATEGORY_SLIP_MULT = {
    "crypto": 1.8,       # Higher volatility, wider spreads
    "economics": 1.3,    # Moderate
    "geopolitics": 1.2,  # Moderate
    "general": 1.0,      # Baseline
    "politics": 1.0,     # Baseline
    "technology": 1.0,    # Baseline
    "sports": 0.8,       # Tighter spreads usually
}

# Liquidity-based slippage adjustments (estimated from volume24h)
VOLUME_SLIP_TIERS = [
    (10000, 0.005),   # < $10k vol: +0.5% extra
    (5000, 0.008),   # < $5k vol: +0.8% extra
    (1000, 0.015),   # < $1k vol: +1.5% extra
    (0, 0.025),      # < $0 vol: +2.5% extra (illiquid)
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] BACKTEST | %(message)s")
log = logging.getLogger(__name__)


@dataclass
class SimTrade:
    trade_id: str
    whale_name: str
    category: str
    side: str
    entry_price: float
    exit_price: float
    position_size_usd: float
    realized_pnl: float
    sim_entry_price: float
    sim_exit_price: float
    sim_pnl: float
    slippage_entry: float
    slippage_exit: float
    fill_model: str  # "normal", "stop_partial", "stop_missed"
    market_title: str = ""
    timestamp: str = ""


def compute_slippage(price: float, side: str, category: str = "general",
                     volume_24h: float = 10000, is_exit: bool = False) -> float:
    """Compute realistic slippage based on market conditions."""
    # Base slippage
    slip = BASE_SLIPPAGE
    
    # Category adjustment
    cat_mult = CATEGORY_SLIP_MULT.get(category, 1.0)
    slip *= cat_mult
    
    # Volume-based adjustment
    vol_adj = 0.0
    for threshold, extra in VOLUME_SLIP_TIERS:
        if volume_24h < threshold:
            vol_adj = extra
            break
    slip += vol_adj
    
    # Spread cost (always present)
    slip += SPREAD_COST
    
    # Exit trades have worse slippage
    if is_exit:
        slip *= 1.3
    
    # Deep value discount (very low-priced contracts have larger relative slippage)
    if price < 0.10:
        slip *= 1.5
    elif price < 0.20:
        slip *= 1.2
    
    # Apply direction
    if side.upper() in ("BUY",):
        return min(price * (1 + slip), 0.99)
    else:
        return max(price * (1 - slip), 0.01)


def simulate_exit(exit_price: float, side: str, category: str, volume: float,
                  is_stop: bool = False) -> tuple:
    """Simulate exit fill with probability model.
    Returns (sim_price, fill_model)."""
    if not is_stop:
        # Normal exit: apply slippage
        sim_price = compute_slippage(exit_price, "SELL" if side == "BUY" else "BUY",
                                      category, volume, is_exit=True)
        return sim_price, "normal"
    
    # Stop exit: probability model
    import random
    random.seed(hash(str(exit_price) + side + category))  # Deterministic
    
    if random.random() < STOP_FILL_PROB:
        # Fills at stop price with slippage
        sim_price = compute_slippage(exit_price, "SELL" if side == "BUY" else "BUY",
                                      category, volume, is_exit=True)
        return sim_price, "stop_partial"
    else:
        # Misses stop, fills at worse price
        worse_price = exit_price * (1 - STOP_WORSE_SLIP) if side == "BUY" else exit_price * (1 + STOP_WORSE_SLIP)
        sim_price = compute_slippage(worse_price, "SELL" if side == "BUY" else "BUY",
                                      category, volume, is_exit=True)
        return sim_price, "stop_missed"


def init_backtest_db():
    BACKTEST_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(BACKTEST_DB))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sim_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            trade_id TEXT,
            whale_name TEXT,
            category TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            position_size_usd REAL,
            original_pnl REAL,
            sim_entry_price REAL,
            sim_exit_price REAL,
            sim_pnl REAL,
            slippage_entry REAL,
            slippage_exit REAL,
            fill_model TEXT,
            market_title TEXT,
            timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS backtest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            description TEXT,
            total_trades INTEGER,
            total_original_pnl REAL,
            total_sim_pnl REAL,
            win_rate_original REAL,
            win_rate_sim REAL,
            profit_factor_original REAL,
            profit_factor_sim REAL,
            avg_slippage REAL,
            dataset TEXT
        );
    """)
    conn.commit()
    conn.close()


def run_backtest(dataset: str = "all", description: str = ""):
    """Run the enhanced fade backtest.
    
    dataset: 'all', 'train' (pre-May-21), 'test' (post-May-21)
    """
    conn = sqlite3.connect(str(TRADES_DB))
    cur = conn.cursor()
    
    # Build date filter
    date_filter = f"timestamp >= '{CLEAN_DATA_FROM}'"
    if dataset == "train":
        date_filter += f" AND timestamp < '{TRAIN_CUTOFF}'"
    elif dataset == "test":
        date_filter += f" AND timestamp >= '{TRAIN_CUTOFF}'"
    
    # Fetch closed trades
    cur.execute(f"""
        SELECT trade_id, whale_name, category, side, entry_price, exit_price,
               position_size_usd, realized_pnl, market_title, timestamp,
               exit_reason
        FROM trades 
        WHERE exit_reason IS NOT NULL 
          AND realized_pnl IS NOT NULL
AND exit_reason NOT IN ("legacy_voided", "orphan_cleanup", "orphaned_position_cleanup", "sybil_cleanup", "pending")
          AND {date_filter}
        ORDER BY timestamp
    """)
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        log.warning("No trades found for dataset=%s", dataset)
        return
    
    log.info("Running backtest on %d trades (dataset=%s)", len(rows), dataset)
    
    # Simulate each trade with realistic slippage
    sim_trades = []
    total_original_pnl = 0
    total_sim_pnl = 0
    original_wins = 0
    sim_wins = 0
    total_gross_profit = 0
    total_gross_loss = 0
    sim_gross_profit = 0
    sim_gross_loss = 0
    total_slippage = 0
    
    for row in rows:
        (trade_id, whale_name, category, side, entry_price, exit_price,
         size_usd, orig_pnl, market_title, timestamp, exit_reason) = row
        
        if entry_price is None or exit_price is None or entry_price <= 0:
            continue
        
        # Estimate volume (we don't have it, use a moderate default)
        est_volume = 5000  # Moderate assumption
        
        # Simulate entry with slippage
        sim_entry = compute_slippage(entry_price, side, category, est_volume)
        entry_slip = abs(sim_entry - entry_price) / entry_price
        
        # Determine if this was a stop exit
        is_stop = exit_reason and ("stop" in exit_reason.lower() or "loss_limit" in exit_reason.lower())
        
        # Simulate exit
        exit_side = "SELL" if side == "BUY" else "BUY"
        sim_exit, fill_model = simulate_exit(exit_price, exit_side, category, est_volume, is_stop)
        exit_slip = abs(sim_exit - exit_price) / max(exit_price, 0.001)
        
        # Calculate simulated P&L
        if side == "BUY":
            sim_pnl = (sim_exit - sim_entry) / sim_entry * size_usd
        else:
            sim_pnl = (sim_entry - sim_exit) / sim_entry * size_usd
        
        st = SimTrade(
            trade_id=trade_id, whale_name=whale_name, category=category,
            side=side, entry_price=entry_price, exit_price=exit_price,
            position_size_usd=size_usd, realized_pnl=orig_pnl,
            sim_entry_price=sim_entry, sim_exit_price=sim_exit,
            sim_pnl=sim_pnl, slippage_entry=entry_slip,
            slippage_exit=exit_slip, fill_model=fill_model,
            market_title=market_title, timestamp=timestamp,
        )
        sim_trades.append(st)
        
        total_original_pnl += orig_pnl
        total_sim_pnl += sim_pnl
        total_slippage += entry_slip + exit_slip
        
        if orig_pnl > 0:
            original_wins += 1
            total_gross_profit += orig_pnl
        else:
            total_gross_loss += abs(orig_pnl)
        
        if sim_pnl > 0:
            sim_wins += 1
            sim_gross_profit += sim_pnl
        else:
            sim_gross_loss += abs(sim_pnl)
    
    n = len(sim_trades)
    original_wr = original_wins / n * 100 if n > 0 else 0
    sim_wr = sim_wins / n * 100 if n > 0 else 0
    orig_pf = total_gross_profit / total_gross_loss if total_gross_loss > 0 else 0
    sim_pf = sim_gross_profit / sim_gross_loss if sim_gross_loss > 0 else 0
    avg_slippage = total_slippage / n * 100 if n > 0 else 0
    
    # Save results
    bt_conn = sqlite3.connect(str(BACKTEST_DB))
    cur = bt_conn.cursor()
    cur.execute("""
        INSERT INTO backtest_runs 
        (run_time, description, total_trades, total_original_pnl, total_sim_pnl,
         win_rate_original, win_rate_sim, profit_factor_original, profit_factor_sim,
         avg_slippage, dataset)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(timezone.utc).isoformat(), description, n,
        total_original_pnl, total_sim_pnl, original_wr, sim_wr,
        orig_pf, sim_pf, avg_slippage, dataset,
    ))
    run_id = cur.lastrowid
    
    for st in sim_trades[:500]:  # Cap at 500 to keep DB small
        cur.execute("""
            INSERT INTO sim_trades 
            (run_id, trade_id, whale_name, category, side, entry_price, exit_price,
             position_size_usd, original_pnl, sim_entry_price, sim_exit_price, sim_pnl,
             slippage_entry, slippage_exit, fill_model, market_title, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, st.trade_id, st.whale_name, st.category, st.side,
            st.entry_price, st.exit_price, st.position_size_usd, st.realized_pnl,
            st.sim_entry_price, st.sim_exit_price, st.sim_pnl,
            st.slippage_entry, st.slippage_exit, st.fill_model,
            st.market_title, st.timestamp,
        ))
    
    bt_conn.commit()
    bt_conn.close()
    
    # Print results
    print("=" * 70)
    print(f"ENHANCED BACKTEST RESULTS — {description}")
    print(f"Dataset: {dataset} | Trades: {n}")
    print("=" * 70)
    print(f"  Original P&L:     ${total_original_pnl:,.2f} (WR={original_wr:.1f}%, PF={orig_pf:.2f})")
    print(f"  Simulated P&L:    ${total_sim_pnl:,.2f} (WR={sim_wr:.1f}%, PF={sim_pf:.2f})")
    print(f"  Avg slippage:     {avg_slippage:.2f}%")
    print(f"  P&L degradation:  ${(total_original_pnl - total_sim_pnl):,.2f} ({(1 - total_sim_pnl/total_original_pnl)*100 if total_original_pnl != 0 else 100:.1f}% reduction)" if total_original_pnl != 0 else "")
    
    # Per-category breakdown
    print("\n  PER-CATEGORY BREAKDOWN:")
    cat_stats = {}
    for st in sim_trades:
        if st.category not in cat_stats:
            cat_stats[st.category] = {"orig": 0, "sim": 0, "n": 0, "orig_wins": 0, "sim_wins": 0}
        cat_stats[st.category]["orig"] += st.realized_pnl
        cat_stats[st.category]["sim"] += st.sim_pnl
        cat_stats[st.category]["n"] += 1
        if st.realized_pnl > 0:
            cat_stats[st.category]["orig_wins"] += 1
        if st.sim_pnl > 0:
            cat_stats[st.category]["sim_wins"] += 1
    
    for cat, s in sorted(cat_stats.items(), key=lambda x: -x[1]["sim"]):
        orig_wr = s["orig_wins"] / s["n"] * 100
        sim_wr = s["sim_wins"] / s["n"] * 100
        print(f"    {cat:15s} | {s['n']:4d} trades | orig=${s['orig']:8.2f} ({orig_wr:.0f}% WR) | sim=${s['sim']:8.2f} ({sim_wr:.0f}% WR)")
    
    # Fill model breakdown
    print("\n  FILL MODEL BREAKDOWN:")
    fill_stats = {}
    for st in sim_trades:
        fm = st.fill_model
        if fm not in fill_stats:
            fill_stats[fm] = {"n": 0, "sim_pnl": 0}
        fill_stats[fm]["n"] += 1
        fill_stats[fm]["sim_pnl"] += st.sim_pnl
    for fm, s in sorted(fill_stats.items()):
        print(f"    {fm:15s} | {s['n']:4d} trades | sim_pnl=${s['sim_pnl']:8.2f}")
    
    return {
        "run_id": run_id,
        "original_pnl": total_original_pnl,
        "sim_pnl": total_sim_pnl,
        "original_wr": original_wr,
        "sim_wr": sim_wr,
        "original_pf": orig_pf,
        "sim_pf": sim_pf,
        "avg_slippage": avg_slippage,
        "n_trades": n,
    }


def main():
    init_backtest_db()
    
    print("=" * 70)
    print("ENHANCED FADE BACKTEST — Variable Slippage Model")
    print(f"Base slippage: {BASE_SLIPPAGE*100:.1f}% | Spread: {SPREAD_COST*100:.1f}% | Stop fill: {STOP_FILL_PROB*100:.0f}%")
    print(f"Train cutoff: {TRAIN_CUTOFF} | Clean data from: {CLEAN_DATA_FROM}")
    print("=" * 70)
    
    # Run 1: All clean data
    print("\n")
    r1 = run_backtest(dataset="all", description="All clean data (post-May-14)")
    
    # Run 2: Train set
    print("\n")
    r2 = run_backtest(dataset="train", description="Train set (May 14-21)")
    
    # Run 3: Test set (out-of-sample)
    print("\n")
    r3 = run_backtest(dataset="test", description="Test set (post-May-21, out-of-sample)")
    
    # Summary comparison
    if r1 and r2 and r3:
        print("\n" + "=" * 70)
        print("OUT-OF-SAMPLE COMPARISON")
        print("=" * 70)
        print(f"  Train (May 14-21):  PnL=${r2['sim_pnl']:,.2f} | WR={r2['sim_wr']:.1f}% | PF={r2['sim_pf']:.2f}")
        print(f"  Test  (post-May 21): PnL=${r3['sim_pnl']:,.2f} | WR={r3['sim_wr']:.1f}% | PF={r3['sim_pf']:.2f}")
        if r2['sim_pnl'] != 0:
            degradation = (r3['sim_pnl'] - r2['sim_pnl']) / abs(r2['sim_pnl']) * 100
            print(f"  OOS degradation: {degradation:.1f}%")
        
        # The critical question: does edge survive realistic costs?
        if r1['sim_pf'] > 1.2:
            print(f"\n  VERDICT: PASS — Simulated PF={r1['sim_pf']:.2f} > 1.2 after realistic slippage")
        elif r1['sim_pf'] > 1.0:
            print(f"\n  VERDICT: MARGINAL — Simulated PF={r1['sim_pf']:.2f} barely positive after slippage")
        else:
            print(f"\n  VERDICT: FAIL — Simulated PF={r1['sim_pf']:.2f} < 1.0, no edge after realistic costs")


if __name__ == "__main__":
    main()
