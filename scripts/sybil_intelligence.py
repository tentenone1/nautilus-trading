#!/usr/bin/env python3
"""Sybil Intelligence Tracker — monitors sybil wallet groups and generates trading signals.

Instead of blacklisting sybil wallets (losing their data), this module:
1. Tracks aggregate positions per sybil group ("meta-whales")
2. Analyzes historical trading patterns (accuracy, bias, manipulation signals)
3. Generates fade/follow signals based on group behavior
4. Saves intelligence to research/sybil_intelligence.json

Usage:
    python3 scripts/sybil_intelligence.py [--full-history] [--output FILE]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from scripts.sybil_config import get_config

config = get_config()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── Constants (from centralized config) ─────────────────────────────────────

DATA_API = config.api.data_api_base
TRADES_URL_FMT = f"{DATA_API}{config.api.trades_endpoint}?user={{user}}&limit={{limit}}&after={{after_ts}}"
POSITIONS_URL_FMT = f"{DATA_API}{config.api.positions_endpoint}?user={{user}}"
REQUEST_TIMEOUT = config.api.request_timeout
TRADES_LIMIT = config.api.trades_limit

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
RESEARCH_DIR = config.paths.research_path(BASE_DIR)
DEFAULT_OUTPUT = BASE_DIR / config.paths.research_dir / config.paths.intelligence_file
TRADE_SCAN_STATE_FILE = RESEARCH_DIR / "sybil_trade_scan_state.json"


# ─── Data Classes ────────────────────────────────────────────────────────────

@dataclass
class WalletTrade:
    """Single trade record from a wallet."""
    wallet: str
    side: str
    outcome: str
    price: float
    size: float
    condition_id: str
    title: str
    timestamp: int
    pseudonym: str = ""


@dataclass
class MarketExposure:
    """Aggregated exposure for a single market across sybil wallets."""
    condition_id: str
    market_title: str
    total_yes_shares: float = 0.0
    total_no_shares: float = 0.0
    net_exposure: float = 0.0  # positive = net YES, negative = net NO
    wallet_count: int = 0
    wallets: list[str] = field(default_factory=list)
    avg_entry_price: float = 0.0
    total_volume: float = 0.0
    latest_trade_ts: int = 0


@dataclass
class GroupStats:
    """Statistics for a sybil group."""
    group_id: str
    label: str
    priority: str
    wallet_count: int
    active_wallets: int = 0
    total_trades_analyzed: int = 0
    total_volume: float = 0.0
    yes_ratio: float = 0.0  # fraction of trades that are YES
    avg_trade_size: float = 0.0
    unique_markets: int = 0
    markets_exposed: dict = field(default_factory=dict)  # condition_id -> MarketExposure
    recent_trades: list[dict] = field(default_factory=list)
    scan_timestamp: str = ""
    error_wallets: list[str] = field(default_factory=list)


# ─── API Functions ───────────────────────────────────────────────────────────

def fetch_trades(wallet: str, limit: int = TRADES_LIMIT, after_ts: int = 0) -> list[dict]:
    """Fetch recent trades for a wallet from Polymarket data API."""
    url = TRADES_URL_FMT.format(user=wallet, limit=limit, after_ts=after_ts)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "data" in data:
            return data["data"]
        return []
    except requests.RequestException as e:
        logger.warning("Failed to fetch trades for %s: %s", wallet[:20], e)
        return []


def fetch_positions(wallet: str) -> list[dict]:
    """Fetch open positions for a wallet."""
    url = POSITIONS_URL_FMT.format(user=wallet)
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except requests.RequestException as e:
        logger.warning("Failed to fetch positions for %s: %s", wallet[:20], e)
        return []


# ─── Trade Scan State ────────────────────────────────────────────────────────

def load_trade_scan_state() -> dict[str, int]:
    """Load trade scan state from disk.
    
    Returns:
        Mapping of wallet -> last_trade_ts.
        Empty dict on first run or if file is missing/corrupt.
    """
    if not TRADE_SCAN_STATE_FILE.exists():
        return {}
    try:
        with open(TRADE_SCAN_STATE_FILE, "r") as f:
            raw = json.load(f)
        return {
            wallet: info["last_trade_ts"]
            for wallet, info in raw.get("wallets", {}).items()
        }
    except (json.JSONDecodeError, KeyError, IOError) as e:
        logger.warning("Failed to load trade scan state: %s", e)
        return {}


def save_trade_scan_state(state: dict[str, int]) -> None:
    """Persist trade scan state to disk.
    
    Args:
        state: Mapping of wallet -> last_trade_ts.
    """
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_global_scan": datetime.now(timezone.utc).isoformat(),
        "wallets": {
            wallet: {"last_trade_ts": ts}
            for wallet, ts in state.items()
        },
    }
    with open(TRADE_SCAN_STATE_FILE, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info("Trade scan state saved to %s", TRADE_SCAN_STATE_FILE)


# ─── Analysis Functions ──────────────────────────────────────────────────────

def parse_trade(raw: dict) -> Optional[WalletTrade]:
    """Parse a raw trade dict into a WalletTrade."""
    try:
        return WalletTrade(
            wallet=raw.get("proxyWallet", raw.get("user", "")),
            side=raw.get("side", "UNKNOWN"),
            outcome=raw.get("outcome", ""),
            price=float(raw.get("price", 0)),
            size=float(raw.get("size", 0)),
            condition_id=raw.get("conditionId", ""),
            title=raw.get("title", ""),
            timestamp=int(raw.get("timestamp", 0)),
            pseudonym=raw.get("pseudonym", raw.get("name", "")),
        )
    except (ValueError, TypeError) as e:
        logger.debug("Failed to parse trade: %s", e)
        return None


def aggregate_group_trades(
    group_id: str,
    wallets: list[str],
    trades_limit: int = TRADES_LIMIT,
) -> GroupStats:
    """Fetch and aggregate trades for all wallets in a sybil group.

    Incremental: only fetches trades since last scan timestamp per wallet.
    """
    group_def = config.groups[group_id]
    stats = GroupStats(
        group_id=group_id,
        label=group_def.label,
        priority=group_def.priority,
        wallet_count=len(wallets),
        scan_timestamp=datetime.now(timezone.utc).isoformat(),
    )

    # Load incremental scan state
    scan_state = load_trade_scan_state()
    new_state: dict[str, int] = {}

    all_trades: list[WalletTrade] = []
    markets: dict[str, MarketExposure] = {}

    for wallet in wallets:
        after_ts = scan_state.get(wallet, 0)
        raw_trades = fetch_trades(wallet, limit=trades_limit, after_ts=after_ts)
        if not raw_trades:
            stats.error_wallets.append(wallet)
            # Preserve existing state even if no new trades
            if wallet in scan_state:
                new_state[wallet] = scan_state[wallet]
            continue

        stats.active_wallets += 1
        wallet_max_ts = 0
        for raw in raw_trades:
            trade = parse_trade(raw)
            if trade:
                all_trades.append(trade)
                if trade.timestamp > wallet_max_ts:
                    wallet_max_ts = trade.timestamp

                # Aggregate market exposure
                cid = trade.condition_id
                if cid not in markets:
                    markets[cid] = MarketExposure(
                        condition_id=cid,
                        market_title=trade.title,
                    )

                exp = markets[cid]
                if trade.side == "BUY":
                    if trade.outcome == "Yes" or trade.outcome.lower() == "yes":
                        exp.total_yes_shares += trade.size
                        exp.net_exposure += trade.size
                    else:
                        exp.total_no_shares += trade.size
                        exp.net_exposure -= trade.size
                elif trade.side == "SELL":
                    if trade.outcome == "Yes" or trade.outcome.lower() == "yes":
                        exp.total_yes_shares -= trade.size
                        exp.net_exposure -= trade.size
                    else:
                        exp.total_no_shares -= trade.size
                        exp.net_exposure += trade.size

                exp.total_volume += trade.size
                if wallet not in exp.wallets:
                    exp.wallets.append(wallet)
                exp.wallet_count = len(exp.wallets)
                if trade.timestamp > exp.latest_trade_ts:
                    exp.latest_trade_ts = trade.timestamp
                # Volume-weighted average price (not wallet-count weighted)
                prev_volume = exp.total_volume - trade.size  # volume before this trade
                exp.avg_entry_price = (
                    (exp.avg_entry_price * prev_volume + trade.price * trade.size)
                    / exp.total_volume
                )

        # Update state with max timestamp from this wallet's trades
        if wallet_max_ts > 0:
            new_state[wallet] = wallet_max_ts
        elif wallet in scan_state:
            new_state[wallet] = scan_state[wallet]

    # Persist updated scan state
    save_trade_scan_state(new_state)

    # Compute group-level stats
    stats.total_trades_analyzed = len(all_trades)
    stats.total_volume = sum(t.size for t in all_trades)
    stats.unique_markets = len(markets)

    if all_trades:
        yes_trades = sum(
            1 for t in all_trades
            if t.outcome.lower() == "yes" and t.side == "BUY"
        )
        stats.yes_ratio = yes_trades / len(all_trades)
        stats.avg_trade_size = stats.total_volume / len(all_trades)

    # Keep top 20 markets by exposure for the report
    top_markets = sorted(
        markets.values(),
        key=lambda m: abs(m.net_exposure),
        reverse=True,
    )[:20]

    stats.markets_exposed = {
        m.condition_id: asdict(m) for m in top_markets
    }

    # Keep recent trade summary (last 50)
    all_trades.sort(key=lambda t: t.timestamp, reverse=True)
    stats.recent_trades = [
        {
            "wallet": t.wallet[:30],
            "side": t.side,
            "outcome": t.outcome,
            "price": t.price,
            "size": t.size,
            "title": t.title[:80],
            "timestamp": t.timestamp,
        }
        for t in all_trades[:50]
    ]

    return stats


def detect_manipulation_signals(stats: GroupStats) -> list[dict]:
    """Detect potential manipulation patterns in a sybil group's trades."""
    signals = []

    for cid, market in stats.markets_exposed.items():
        wallets = market.get("wallets", [])
        volume = market.get("total_volume", 0)
        net = market.get("net_exposure", 0)

        # Pattern 1: Many wallets, small bets on same market → sentiment manipulation
        if len(wallets) >= 3 and volume > 0:
            avg_bet = volume / max(len(wallets), 1)
            if avg_bet < 500:  # Small average bet across many wallets
                signals.append({
                    "type": "sentiment_manipulation",
                    "group_id": stats.group_id,
                    "condition_id": cid,
                    "market_title": market["market_title"][:80],
                    "wallet_count": len(wallets),
                    "avg_bet_size": round(avg_bet, 2),
                    "total_volume": round(volume, 2),
                    "signal": f"{len(wallets)} wallets, avg ${avg_bet:.0f} bets — possible sentiment manipulation",
                })

        # Pattern 2: Large net exposure across sybils → real conviction
        if abs(net) > 5000:
            direction = "YES" if net > 0 else "NO"
            signals.append({
                "type": "aggregate_conviction",
                "group_id": stats.group_id,
                "condition_id": cid,
                "market_title": market["market_title"][:80],
                "net_exposure": round(net, 2),
                "direction": direction,
                "wallet_count": len(wallets),
                "signal": f"Net ${abs(net):,.0f} {direction} across {len(wallets)} sybil wallets",
            })

    return signals


def generate_llm_prompt(all_stats: list[GroupStats], all_signals: list[dict]) -> str:
    """Generate a prompt for the local LLM to analyze sybil data and suggest strategies."""
    prompt = """You are a quantitative trading analyst. Analyze the following sybil wallet group data from Polymarket and recommend trading strategies.

## Context
These are wallet groups that belong to the same entity (sybils). They control multiple wallets to either:
1. Hide their true position size
2. Manipulate market sentiment with coordinated small bets
3. Execute large positions across multiple wallets

## What we know
"""
    for stats in all_stats:
        prompt += f"\n### {stats.group_id} ({stats.label})\n"
        prompt += f"- Wallets: {stats.wallet_count} total, {stats.active_wallets} active\n"
        prompt += f"- Trades analyzed: {stats.total_trades_analyzed}\n"
        prompt += f"- Total volume: ${stats.total_volume:,.0f}\n"
        prompt += f"- YES bias: {stats.yes_ratio:.1%}\n"
        prompt += f"- Avg trade size: ${stats.avg_trade_size:,.0f}\n"
        prompt += f"- Unique markets: {stats.unique_markets}\n"

        if stats.markets_exposed:
            prompt += "\n**Top market exposures:**\n"
            for cid, m in list(stats.markets_exposed.items())[:5]:
                prompt += f"- {m['market_title'][:60]}: net ${m['net_exposure']:,.0f}, {m['wallet_count']} wallets\n"

    if all_signals:
        prompt += "\n## Manipulation/Conviction Signals\n"
        for sig in all_signals[:15]:
            prompt += f"- [{sig['type']}] {sig['signal']}\n"

    prompt += """
## Your Task
Analyze this data and answer:

1. **Pattern Analysis**: What behavioral patterns do you see in each group? Are they systematic? Do they favor certain market types?

2. **Accuracy Assessment**: Based on the trade patterns, are these groups likely to be informed traders or manipulators? What evidence supports this?

3. **Trading Strategies**: Propose 3-5 specific trading strategies that exploit this sybil data. For each:
   - Strategy name
   - Logic (why it works)
   - Entry/exit conditions
   - Risk level
   - Which sybil group(s) it applies to

4. **Immediate Actions**: Are there any current markets where the sybil data suggests an immediate trade opportunity?

Respond in JSON format with this structure:
{
  "pattern_analysis": {"group_id": "analysis text"},
  "accuracy_assessment": "overall assessment",
  "strategies": [
    {"name": "...", "logic": "...", "entry": "...", "exit": "...", "risk": "low/medium/high", "applies_to": "group_id"}
  ],
  "immediate_opportunities": [
    {"market": "...", "action": "BUY/SELL", "outcome": "YES/NO", "reason": "...", "confidence": "low/medium/high"}
  ]
}
"""
    return prompt


# ─── Main ────────────────────────────────────────────────────────────────────

def main(args=None) -> None:
    parser = argparse.ArgumentParser(description="Sybil Intelligence Tracker")
    parser.add_argument("--full-history", action="store_true", help="Fetch more trades per wallet (500 vs 100)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT), help="Output file path")
    parser.add_argument("--skip-llm", action="store_true", help="Skip LLM strategy generation")
    if args is None:
        args = parser.parse_args()

    trades_limit = 500 if args.full_history else TRADES_LIMIT
    logger.info("Starting sybil intelligence scan (limit=%d per wallet)", trades_limit)

    all_stats: list[GroupStats] = []
    all_signals: list[dict] = []

    for group_id, group_def in config.groups.items():
        wallet_addrs = [w.address for w in group_def.wallets]
        logger.info("Scanning %s (%d wallets)...", group_id, len(wallet_addrs))
        stats = aggregate_group_trades(group_id, wallet_addrs, trades_limit)
        all_stats.append(stats)

        signals = detect_manipulation_signals(stats)
        all_signals.extend(signals)

        logger.info(
            "  %s: %d trades, %d markets, %d signals, $%.0f volume",
            group_id,
            stats.total_trades_analyzed,
            stats.unique_markets,
            len(signals),
            stats.total_volume,
        )

    # Build output
    output = {
        "scan_timestamp": datetime.now(timezone.utc).isoformat(),
        "groups": [asdict(s) for s in all_stats],
        "signals": all_signals,
        "summary": {
            "total_groups": len(all_stats),
            "total_wallets": sum(s.wallet_count for s in all_stats),
            "active_wallets": sum(s.active_wallets for s in all_stats),
            "total_trades": sum(s.total_trades_analyzed for s in all_stats),
            "total_signals": len(all_signals),
        },
    }

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    logger.info("Results written to %s", output_path)

    # LLM strategy generation
    if not args.skip_llm:
        logger.info("Generating LLM strategy analysis...")
        prompt = generate_llm_prompt(all_stats, all_signals)

        # Save prompt for LLM input
        prompt_path = RESEARCH_DIR / "sybil_llm_prompt.txt"
        with open(prompt_path, "w") as f:
            f.write(prompt)
        logger.info("LLM prompt saved to %s (%d chars)", prompt_path, len(prompt))

    # Print summary
    print("\n" + "=" * 60)
    print("SYBIL INTELLIGENCE SUMMARY")
    print("=" * 60)
    for stats in all_stats:
        print(f"\n{stats.group_id} ({stats.label}):")
        print(f"  Active wallets: {stats.active_wallets}/{stats.wallet_count}")
        print(f"  Trades: {stats.total_trades_analyzed}, Volume: ${stats.total_volume:,.0f}")
        print(f"  YES bias: {stats.yes_ratio:.1%}, Avg size: ${stats.avg_trade_size:,.0f}")
        print(f"  Markets exposed: {stats.unique_markets}")
        print(f"  Signals: {len([s for s in all_signals if s['group_id'] == stats.group_id])}")

    print(f"\nTotal signals: {len(all_signals)}")
    print(f"Output: {output_path}")
    if not args.skip_llm:
        print(f"LLM prompt: {prompt_path}")


if __name__ == "__main__":
    main()
