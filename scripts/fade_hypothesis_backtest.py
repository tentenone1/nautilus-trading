#!/usr/bin/env python3
"""Fade Hypothesis Backtest — Validates whether fading blacklisted whales yields positive expectancy.

Strategy: Analyze historical trade data to determine:
1. Which whales have genuine negative edge (should be faded)
2. Which whales have positive edge (should be followed)
3. What the overall fade vs follow performance looks like

Uses only post-May-14 clean data from trades.db.
Applies realistic slippage (2% adverse) to all simulated trades.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Configuration ──────────────────────────────────────────────────────────
TRADES_DB = Path(__file__).parent.parent / "data" / "trades.db"
WHALE_DB = Path(__file__).parent.parent / "data" / "whale_discovery.db"
CLEAN_DATA_FROM = "2026-05-14"
SLIPPAGE_PCT = 0.025  # 2.5% total cost (slippage + spread)
MIN_TRADES = 3  # Minimum trades for statistical significance
FADE_WR_THRESHOLD = 0.30  # Fade whales with WR below this


@dataclass
class WhaleResult:
    name: str
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    action: str  # "follow", "fade", "ignore"
    confidence: str  # "high", "medium", "low"


def apply_slippage(price: float, side: str) -> float:
    """Apply realistic slippage to simulate actual execution cost."""
    if side.upper() == "BUY":
        return min(price * (1 + SLIPPAGE_PCT), 0.99)
    else:
        return max(price * (1 - SLIPPAGE_PCT), 0.01)


def compute_pnl(entry_price: float, exit_price: float, side: str, size_usd: float = 1.0) -> float:
    """Compute P&L for a trade with given entry/exit prices and side."""
    if side.upper() == "BUY":
        return (exit_price - entry_price) / entry_price * size_usd
    else:  # SELL
        return (entry_price - exit_price) / entry_price * size_usd


def analyze_whale_follow_performance(cur: sqlite3.Connection) -> list[WhaleResult]:
    """Analyze how each whale performed when we FOLLOWED their signals (same direction)."""
    cur.execute("""
        SELECT whale_name, side, entry_price, exit_price, realized_pnl, 
               realized_return, category, market_title, exit_reason
        FROM trades 
        WHERE timestamp >= ? AND realized_pnl IS NOT NULL
        ORDER BY whale_name, timestamp
    """, (CLEAN_DATA_FROM,))
    
    # Group trades by whale
    whale_trades = {}
    for row in cur.fetchall():
        name, side, entry_p, exit_p, pnl, ret, cat, title, exit_r = row
        if not name:
            continue
        whale_trades.setdefault(name, []).append({
            "side": side, "entry_price": entry_p, "exit_price": exit_p,
            "pnl": pnl, "return": ret, "category": cat, "exit_reason": exit_r,
        })
    
    results = []
    for name, trades in whale_trades.items():
        if len(trades) < MIN_TRADES:
            continue
        
        wins = sum(1 for t in trades if t["pnl"] and t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] and t["pnl"] < 0)
        total_pnl = sum(t["pnl"] or 0 for t in trades)
        wr = wins / len(trades) if trades else 0
        
        # Determine action
        if wr < FADE_WR_THRESHOLD and len(trades) >= 5:
            action = "fade"
            confidence = "high" if len(trades) >= 10 else "medium"
        elif wr >= FADE_WR_THRESHOLD and wr < 0.50 and len(trades) >= 5:
            action = "fade"  # Still fade but with less confidence
            confidence = "low"
        elif wr >= 0.50:
            action = "follow"
            confidence = "high" if wr >= 0.70 else "medium" if wr >= 0.60 else "low"
        else:
            action = "ignore"
            confidence = "low"
        
        results.append(WhaleResult(
            name=name, total_trades=len(trades), wins=wins, losses=losses,
            win_rate=wr, total_pnl=round(total_pnl, 2), avg_pnl=round(total_pnl/len(trades), 2),
            action=action, confidence=confidence,
        ))
    
    return sorted(results, key=lambda r: r.total_pnl)


def simulate_fade_performance(cur: sqlite3.Connection, fade_whales: set[str]) -> dict:
    """Simulate what would happen if we faded the specified whales instead of following them.
    
    For each trade where we followed a whale in the fade set:
    - Flip the side (BUY → SELL, SELL → BUY)
    - Apply slippage
    - Compute hypothetical P&L
    """
    cur.execute("""
        SELECT whale_name, side, entry_price, exit_price, realized_pnl,
               realized_return, category, market_title, exit_reason
        FROM trades 
        WHERE timestamp >= ? AND realized_pnl IS NOT NULL
          AND whale_name IS NOT NULL
        ORDER BY timestamp
    """, (CLEAN_DATA_FROM,))
    
    original_pnl = 0.0
    faded_pnl = 0.0
    follow_pnl = 0.0
    n_faded = 0
    n_followed = 0
    
    fade_details = []
    
    for row in cur.fetchall():
        name, side, entry_p, exit_p, pnl, ret, cat, title, exit_r = row
        if not name or not entry_p or not exit_p or entry_p <= 0 or exit_p <= 0:
            continue
        
        original_pnl += pnl or 0
        
        if name in fade_whales:
            n_faded += 1
            # Simulate FADE: flip side, apply slippage
            fade_side = "SELL" if side.upper() == "BUY" else "BUY"
            fade_entry = apply_slippage(entry_p, fade_side)
            fade_pnl = compute_pnl(fade_entry, exit_p, fade_side, size_usd=abs(pnl) / abs(ret) if ret and abs(ret) > 0.01 else 1.0)
            faded_pnl += fade_pnl
            fade_details.append({
                "whale": name, "original_side": side, "fade_side": fade_side,
                "original_pnl": round(pnl or 0, 4), "fade_pnl": round(fade_pnl, 4),
                "entry": entry_p, "exit": exit_p, "market": title[:60] if title else "?",
            })
        else:
            n_followed += 1
            follow_pnl += pnl or 0
    
    return {
        "original_total_pnl": round(original_pnl, 2),
        "follow_pnl": round(follow_pnl, 2),
        "fade_pnl": round(faded_pnl, 2),
        "n_followed_trades": n_followed,
        "n_faded_trades": n_faded,
        "fade_detail_count": len(fade_details),
        "fade_details_sample": fade_details[:20],
    }


def identify_true_losers(cur: sqlite3.Connection) -> list[dict]:
    """Find whales that consistently lose (0% WR) — best fade candidates."""
    cur.execute("""
        SELECT whale_name, 
            COUNT(*) as total,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
            ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as wr_pct,
            ROUND(SUM(realized_pnl), 2) as total_pnl
        FROM trades 
        WHERE timestamp >= ? AND realized_pnl IS NOT NULL
          AND whale_name IS NOT NULL
        GROUP BY whale_name
        HAVING COUNT(*) >= 3
        ORDER BY wr_pct ASC, total_pnl ASC
        LIMIT 30
    """, (CLEAN_DATA_FROM,))
    
    losers = []
    for row in cur.fetchall():
        name, total, wins, losses, wr, pnl = row
        losers.append({
            "name": name, "total_trades": total, "wins": wins, "losses": losses,
            "win_rate_pct": wr, "total_pnl": pnl,
        })
    return losers


def main():
    print("=" * 70)
    print("FADE HYPOTHESIS BACKTEST — Post-May 14 Clean Data")
    print(f"Slippage model: {SLIPPAGE_PCT*100:.1f}% adverse + spread")
    print(f"Clean data from: {CLEAN_DATA_FROM}")
    print("=" * 70)
    
    if not TRADES_DB.exists():
        print(f"ERROR: trades.db not found at {TRADES_DB}")
        return
    
    conn = sqlite3.connect(str(TRADES_DB))
    cur = conn.cursor()
    
    # 1. Count total clean trades
    cur.execute("SELECT COUNT(*) FROM trades WHERE timestamp >= ? AND realized_pnl IS NOT NULL", (CLEAN_DATA_FROM,))
    total_clean = cur.fetchone()[0]
    print(f"\nTotal clean trades (post {CLEAN_DATA_FROM}): {total_clean}")
    
    # 2. Identify true losers (best fade candidates)
    print("\n" + "-" * 70)
    print("TRUE LOSERS — Whales with 0% WR (best fade candidates)")
    print("-" * 70)
    losers = identify_true_losers(cur)
    zero_wr = [l for l in losers if l["win_rate_pct"] == 0.0]
    low_wr = [l for l in losers if 0 < l["win_rate_pct"] < 30]
    
    for l in zero_wr[:10]:
        print(f"  {l['name'][:30]:30s} | {l['total_trades']:3d} trades | 0% WR | PnL: ${l['total_pnl']:.2f}")
    if low_wr:
        print(f"\n  Low WR (<30%) candidates:")
        for l in low_wr[:5]:
            print(f"  {l['name'][:30]:30s} | {l['total_trades']:3d} trades | {l['win_rate_pct']:.0f}% WR | PnL: ${l['total_pnl']:.2f}")
    
    # 3. Analyze whale follow performance
    print("\n" + "-" * 70)
    print("WHALE PERFORMANCE (when we FOLLOWED their direction)")
    print("-" * 70)
    results = analyze_whale_follow_performance(cur)
    
    fade_candidates = [r for r in results if r.action == "fade"]
    follow_candidates = [r for r in results if r.action == "follow"]
    
    print(f"\n  Follow candidates (WR >= 50%): {len(follow_candidates)}")
    for r in follow_candidates[:10]:
        print(f"    {r.name[:30]:30s} | {r.total_trades:3d} trades | {r.win_rate:.0%} WR | PnL: ${r.total_pnl:.2f} | conf: {r.confidence}")
    
    print(f"\n  Fade candidates (WR < 30%): {len(fade_candidates)}")
    for r in fade_candidates[:10]:
        print(f"    {r.name[:30]:30s} | {r.total_trades:3d} trades | {r.win_rate:.0%} WR | PnL: ${r.total_pnl:.2f} | conf: {r.confidence}")
    
    # 4. Simulate fade performance for true losers
    print("\n" + "-" * 70)
    print("FADE SIMULATION — What if we faded the 0% WR whales?")
    print("-" * 70)
    zero_wr_names = {l["name"] for l in zero_wr}
    sim = simulate_fade_performance(cur, zero_wr_names)
    print(f"  Original total PnL:      ${sim['original_total_pnl']:.2f}")
    print(f"  Follow (non-faded) PnL:  ${sim['follow_pnl']:.2f} ({sim['n_followed_trades']} trades)")
    print(f"  Fade (flipped) PnL:      ${sim['fade_pnl']:.2f} ({sim['n_faded_trades']} trades)")
    print(f"  Combined (follow+fade):  ${sim['follow_pnl'] + sim['fade_pnl']:.2f}")
    
    if sim['fade_detail_count'] > 0:
        print(f"\n  Sample fade trades (first 10):")
        for d in sim['fade_details_sample'][:10]:
            print(f"    {d['whale'][:20]:20s} | {d['original_side']}→{d['fade_side']} | "
                  f"orig: ${d['original_pnl']:.2f} → fade: ${d['fade_pnl']:.2f} | "
                  f"{d['market'][:40]}")
    
    # 5. Current blacklist analysis
    print("\n" + "-" * 70)
    print("CURRENT BLACKLIST ANALYSIS")
    print("-" * 70)
    from strategies.wf_constants import WHALE_BLACKLIST
    blacklist_results = []
    for name in sorted(WHALE_BLACKLIST):
        cur.execute("""
            SELECT COUNT(*), 
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END),
                ROUND(SUM(realized_pnl), 2)
            FROM trades WHERE whale_name = ? AND realized_pnl IS NOT NULL
        """, (name,))
        row = cur.fetchone()
        if row and row[0] > 0:
            total, wins, losses, pnl = row
            wr = (wins / total * 100) if total > 0 else 0
            blacklist_results.append({
                "name": name, "trades": total, "wins": wins, "losses": losses,
                "wr": wr, "pnl": pnl or 0,
            })
            print(f"  {name[:30]:30s} | {total:3d} trades | {wr:.0f}% WR | PnL: ${pnl or 0:.2f}")
        else:
            print(f"  {name[:30]:30s} | no trades found")
    
    # 6. Recommendations
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS")
    print("=" * 70)
    
    # Whales to REMOVE from blacklist (they're actually profitable or 50/50)
    remove_from_blacklist = []
    for r in blacklist_results:
        if r["wr"] >= 50:
            remove_from_blacklist.append(r["name"])
    
    if remove_from_blacklist:
        print(f"\n  REMOVE from blacklist (WR >= 50%, fading them is losing):")
        for name in remove_from_blacklist:
            print(f"    - {name}")
    
    # Whales to ADD to blacklist (true losers with 0% WR)
    add_to_blacklist = []
    for l in zero_wr:
        if l["name"] not in WHALE_BLACKLIST and l["total_trades"] >= 5:
            add_to_blacklist.append(l)
    
    if add_to_blacklist:
        print(f"\n  ADD to blacklist (0% WR, genuine fade candidates):")
        for l in add_to_blacklist:
            print(f"    + {l['name']} ({l['total_trades']} trades, ${l['total_pnl']:.2f} loss)")
    
    # Profitable whales to FOLLOW more aggressively
    print(f"\n  PROFITABLE WHALES to follow more aggressively:")
    for r in sorted(follow_candidates, key=lambda x: x.total_pnl, reverse=True)[:5]:
        print(f"    ★ {r.name} ({r.total_trades} trades, {r.win_rate:.0%} WR, ${r.total_pnl:.2f})")
    
    conn.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
