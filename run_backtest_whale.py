#!/usr/bin/env python3
"""Whale Follower Backtest using PMXT historical data.

Uses the prediction-market-backtesting tool's PMXT data loader to
download/cache historical Polymarket book data, then feeds it into a
NautilusTrader BacktestEngine running the whale follower strategy.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

# ── Path setup ──
# nautilus-trading LAST (ends up at sys.path[0]) so its strategies/ package is found first
NAUTILUS_ROOT = os.path.expanduser("/home/elon-1/workspace/nautilus-trading")
TOOL_ROOT = os.path.expanduser("/home/elon-1/projects/prediction-market-backtesting")
sys.path.insert(0, TOOL_ROOT)
sys.path.insert(0, NAUTILUS_ROOT)

# ── Nautilus imports ──
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import Money
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.identifiers import TraderId, Venue

# ── PMXT data loader from the tool ──
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting.data_sources import (
    resolve_replay_adapter,
    Polymarket, Book, PMXT,
)
from prediction_market_extensions.adapters.prediction_market.replay import ReplayLoadRequest

# ── Whale follower strategy ──
from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig


async def load_pmxt_data(market_slug, start_time, end_time, token_index=0):
    """Load Polymarket order book data via PMXT cache."""
    print(f"Loading PMXT data: {market_slug} [{start_time} > {end_time}]")
    adapter = resolve_replay_adapter(
        platform=Polymarket, data_type=Book, vendor=PMXT,
    )
    replay = BookReplay(
        market_slug=market_slug, token_index=token_index,
        start_time=start_time, end_time=end_time,
    )
    request = ReplayLoadRequest(
        min_record_count=500, min_price_range=0.005,
        default_lookback_hours=72,
        default_start_time=start_time, default_end_time=end_time,
    )
    with adapter.configure_sources(sources=("archive:r2v2.pmxt.dev",)) as data_source:
        loaded = await adapter.load_replay(replay, request=request)
    if loaded is None:
        raise RuntimeError(f"Failed to load data for {market_slug}")
    print(f"  Loaded: {loaded.count} {loaded.count_key}")
    print(f"  Instrument: {loaded.instrument}")
    print(f"  Records: {len(loaded.records)} order book events")
    return loaded


def run_backtest(loaded_replay, bankroll=10000.0, kelly_fraction=0.25):
    """Run whale follower backtest with pre-loaded PMXT data."""
    instrument = loaded_replay.instrument
    records = loaded_replay.records

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("WHALE-BT-001"),
        logging=LoggingConfig(log_level="INFO"),
    ))

    from nautilus_trader.model.enums import OmsType, AccountType
    engine.add_venue(
        venue=Venue("POLYMARKET"), oms_type=OmsType.HEDGING,
        account_type=AccountType.CASH, base_currency=None,
        starting_balances=[Money(bankroll, USD)],
    )
    engine.add_instrument(instrument)
    print(f"  Instrument: {instrument.id}")

    strategy_config = WhaleFollowerConfig(
        instrument_ids=[instrument.id],
        bankroll=bankroll,
        kelly_fraction=kelly_fraction,
    )
    strategy = WhaleFollower(config=strategy_config)
    engine.add_strategy(strategy)

    # Feed all order book data at once
    engine.add_data(list(records))
    print(f"  Added {len(records)} data records")

    print("\nRunning backtest...")
    engine.run()
    print("Backtest complete.")

    # Results summary
    print(f"\n=== RESULTS ===")
    for actor in engine.actors:
        if hasattr(actor, 'account'):
            try:
                print(f"  Account balance: {actor.account.balance(USD)}")
            except:
                pass

    return {"engine": engine, "strategy": strategy}


def main():
    parser = argparse.ArgumentParser(description="Whale follower PMXT backtest")
    parser.add_argument("--market-slug", default="will-ludvig-aberg-win-the-2026-masters-tournament")
    parser.add_argument("--start", default="2026-04-05T00:00:00Z")
    parser.add_argument("--end", default="2026-04-07T23:59:59Z")
    parser.add_argument("--token-index", type=int, default=0)
    parser.add_argument("--bankroll", type=float, default=10000.0)
    parser.add_argument("--kelly", type=float, default=0.25)
    args = parser.parse_args()

    loaded = asyncio.run(load_pmxt_data(
        args.market_slug, args.start, args.end, args.token_index,
    ))

    result = run_backtest(loaded, args.bankroll, args.kelly)

    # Print any trades the strategy made
    strategy = result["strategy"]
    if hasattr(strategy, 'trade_count'):
        print(f"  Trades executed: {strategy.trade_count}")
    if hasattr(strategy, 'pnl'):
        print(f"  PnL: {strategy.pnl}")

    print(f"\nDone. Analyzed {loaded.count} book events.")


if __name__ == "__main__":
    main()
