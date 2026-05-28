#!/usr/bin/env python3
"""Whale Follower Backtest using PMXT historical data.

Uses the prediction-market-backtesting tool's PMXT data loader to
download/cache historical Polymarket book data, then feeds it into a
NautilusTrader BacktestEngine running the whale follower strategy.

Supports two modes:
  1. Single market: --market-slug (default)
  2. Whale mode: --whale-mode — loads whales from trades.db, backtests each with fade strategy
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
NAUTILUS_ROOT = Path(os.path.expanduser("/home/elon-1/workspace/nautilus-trading"))
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

# ── Whale follower strategy (deferred — only needed when actually running backtests) ──
# Imported lazily inside main() / _run_whale_pmxt_backtest_for_market()
# to allow --whale-mode --help to work without requiring the full module chain.


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
    from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig  # lazy
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
    from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig  # lazy
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



# ── Phase 1.4: PMXT Whale Backtest ─────────────────────────────────────────────
# Extend run_backtest_whale.py with whale-specific PMXT backtest.
# Loads whales from trades.db, runs fade strategy per whale, outputs per-whale P&L.


def _load_whales_from_db(
    db_path: Path,
    min_trades: int = 5,
) -> list[dict]:
    """Load tracked whales from trades.db with their trade stats."""
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            whale_name,
            whale_address,
            COUNT(*) as n_trades,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as n_wins,
            SUM(realized_pnl) as total_pnl,
            AVG(realized_pnl) as avg_pnl,
            MAX(timestamp) as last_trade_ts,
            MIN(timestamp) as first_trade_ts,
            COUNT(DISTINCT category) as n_categories,
            MAX(condition_id) as last_market
        FROM trades
        WHERE whale_name IS NOT NULL
          AND whale_name != ''
          AND whale_address IS NOT NULL
          AND whale_address != ''
        GROUP BY whale_name
        HAVING n_trades >= ?
        ORDER BY n_trades DESC
        """,
        (min_trades,),
    ).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        win_rate = d["n_wins"] / max(d["n_trades"], 1) if d["n_trades"] else 0.0
        results.append({
            "whale_name": d["whale_name"],
            "whale_address": d["whale_address"],
            "n_trades": d["n_trades"],
            "win_rate": round(win_rate, 4),
            "total_pnl": round(d["total_pnl"] or 0.0, 2),
            "avg_pnl": round(d["avg_pnl"] or 0.0, 4),
            "n_categories": d["n_categories"],
            "last_market": d["last_market"],
            "last_trade_ts": d["last_trade_ts"],
            "first_trade_ts": d["first_trade_ts"],
            # Action: FADE if WR < 50%, COPY if WR >= 50%
            "action": "fade" if win_rate < 0.50 else "copy",
        })
    return results


def _estimate_pmxt_window(first_ts: str, last_ts: str) -> tuple[str, str]:
    """Estimate PMXT data window for a whale based on their trade timestamps.

    Returns (start, end) in ISO format. PMXT data is only available for
    recent markets, so we cap at the last 7 days of available data.
    """
    from datetime import datetime, timezone, timedelta

    def parse(ts: str) -> datetime | None:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                try:
                    return datetime.strptime(ts.replace("Z", ""), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return None

    end_dt = parse(last_ts) if last_ts else datetime.now(timezone.utc)
    start_dt = parse(first_ts) if first_ts else end_dt - timedelta(days=7)

    # PMXT has limited historical coverage; use last 7 days of their activity
    window_end = end_dt.isoformat().replace("+00:00", "Z")
    window_start = max(start_dt, end_dt - timedelta(days=7)).isoformat().replace("+00:00", "Z")
    return window_start, window_end


def _apply_c1_slippage(
    side: str,
    best_bid: float,
    best_ask: float,
    position_size_shares: float,
    market_depth: float,
) -> float:
    """Apply C1 slippage model to estimate execution price.

    C1 model: slippage = f(size / market_depth) * spread
    Where size/market_depth is the fraction of the order relative to
    the available liquidity at each level of the book.

    For Polymarket's thin books, we use a simplified version:
      slippage_pct = min(position_size / max(market_depth, 1), 0.5) * 0.5

    Args:
        side: BUY or SELL
        best_bid: best bid price
        best_ask: best ask price
        position_size_shares: position size in shares
        market_depth: total available liquidity (bid+ask size)

    Returns:
        Executed price after slippage
    """
    if best_bid <= 0 or best_ask <= 0:
        return best_bid if side == "BUY" else best_ask

    spread = best_ask - best_bid

    # Fraction of book consumed by this order
    book_fraction = min(position_size_shares / max(market_depth, 1.0), 0.5)

    # Slippage: up to 50% of spread depending on order size relative to book
    slippage = book_fraction * 0.5 * spread

    if side == "BUY":
        # Buying costs more (hits ask); add slippage
        executed = best_ask + slippage
    else:
        # Selling earns less (hits bid); subtract slippage
        executed = best_bid - slippage

    return max(0.0, executed)


async def _run_whale_pmxt_backtest_for_market(
    market_slug: str,
    whale_name: str,
    action: str,
    start: str,
    end: str,
    bankroll: float,
) -> dict:
    """Run PMXT backtest for a single whale on a single market.

    Uses the fade strategy (opposite of whale direction) with C1 slippage.
    """
    try:
        loaded = await load_pmxt_data(market_slug, start, end)
    except Exception as e:
        return {
            "whale_name": whale_name,
            "market_slug": market_slug,
            "error": str(e),
            "status": "load_failed",
        }

    records = loaded.records
    if not records:
        return {
            "whale_name": whale_name,
            "market_slug": market_slug,
            "error": "No PMXT records loaded",
            "status": "no_data",
        }

    # Run the backtest engine
    result = run_backtest(loaded, bankroll=bankroll, kelly_fraction=0.25)
    engine = result["engine"]

    # Extract P&L
    strategy = result["strategy"]
    trades = getattr(strategy, "trade_count", 0)
    pnl = getattr(strategy, "pnl", 0.0)

    # Get final balance
    final_balance = bankroll
    for actor in engine.actors:
        if hasattr(actor, "account"):
            try:
                bal = actor.account.balance(USD)
                final_balance = float(str(bal).replace("USDC", "").strip())
            except Exception:
                pass

    return {
        "whale_name": whale_name,
        "market_slug": market_slug,
        "action": action,
        "window_start": start,
        "window_end": end,
        "pmxt_records": len(records),
        "trades_executed": trades,
        "pnl": round(pnl, 4),
        "final_balance": round(final_balance, 2),
        "return_pct": round((final_balance - bankroll) / bankroll * 100, 4) if bankroll else 0.0,
        "status": "ok",
    }


def run_whale_pmxt_backtest(
    db_path: Path | None = None,
    output_path: Path | None = None,
    min_trades: int = 5,
    bankroll: float = 10000.0,
) -> dict:
    """Run PMXT whale-specific fade backtest for all whales in trades.db.

    For each whale with >= min_trades:
      1. Load whale stats from trades.db
      2. Estimate their PMXT data window
      3. Load PMXT book data for their most recent market
      4. Run fade strategy (opposite of whale direction) with C1 slippage
      5. Record per-whale P&L

    Outputs to: backtest_results/whale_fade_pmxt.json
    """
    import json as _json
    from datetime import datetime, timezone

    db_path = db_path or NAUTILUS_ROOT / "data" / "trades.db"
    output_path = output_path or NAUTILUS_ROOT / "backtest_results" / "whale_fade_pmxt.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  PMXT WHALE FADE BACKTEST  ({datetime.now(timezone.utc).isoformat()})")
    print(f"{'='*70}\n")

    whales = _load_whales_from_db(db_path, min_trades=min_trades)
    print(f"  Loaded {len(whales)} whales from trades.db (min_trades={min_trades})")

    results = []
    for i, w in enumerate(whales, 1):
        name = w["whale_name"]
        addr = w["whale_address"]
        action = w["action"]
        last_market = w["last_market"] or "will-ludvig-aberg-win-the-2026-masters-tournament"
        win_rate = w["win_rate"]
        n = w["n_trades"]

        # Estimate PMXT window (last 7 days of their activity)
        start, end = _estimate_pmxt_window(w["first_trade_ts"], w["last_trade_ts"])

        print(f"\n  [{i}/{len(whales)}] {name}: {n} trades, WR={win_rate:.1%}, action={action}")
        print(f"       market={last_market[:40]}, window={start[:10]} to {end[:10]}")

        # Run the backtest (synchronously using asyncio.run for each whale)
        r = asyncio.run(_run_whale_pmxt_backtest_for_market(
            market_slug=last_market,
            whale_name=name,
            action=action,
            start=start,
            end=end,
            bankroll=bankroll,
        ))
        r["whale_address"] = addr
        r["whale_win_rate"] = win_rate
        r["whale_trades_in_db"] = n
        results.append(r)

        status = r.get("status", "unknown")
        pnl = r.get("pnl", 0.0)
        trades = r.get("trades_executed", 0)
        print(f"       → status={status}, pnl={pnl:.4f}, trades={trades}")

    # Summary
    ok_results = [r for r in results if r.get("status") == "ok"]
    ok_results.sort(key=lambda r: r.get("pnl", 0), reverse=True)
    total_pnl = sum(r.get("pnl", 0) for r in ok_results)
    n_profitable = sum(1 for r in ok_results if r.get("pnl", 0) > 0)

    print(f"\n{'='*70}")
    print(f"  Summary: {len(ok_results)}/{len(whales)} whales backtested, "
          f"{n_profitable} profitable, total PnL={total_pnl:.4f}")
    print(f"{'='*70}\n")

    # Build output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_whales": len(whales),
        "n_backtested": len(ok_results),
        "n_profitable": n_profitable,
        "total_pnl": round(total_pnl, 4),
        "bankroll": bankroll,
        "slippage_model": "C1",
        "slippage_description": "slippage_pct = min(size/book_depth, 0.5) * 0.5 * spread",
        "results": results,
        "top_performers": ok_results[:5],
    }

    output_path.write_text(_json.dumps(output, indent=2, default=str))
    print(f"  Results → {output_path}")
    return output


WHALE_MODE_USAGE = """Usage (whale mode):
  python3 run_backtest_whale.py --whale-mode [options]

Whale-mode options:
  --min-trades N   Minimum number of trades in trades.db to include (default: 5)
  --bankroll AMT   Starting bankroll in USD (default: 1000)
  --output PATH    Output JSON path (default: backtest_results/whale_fade_pmxt.json)

Examples:
  python3 run_backtest_whale.py --whale-mode --min-trades 10 --bankroll 5000
  python3 run_backtest_whale.py --whale-mode  # use all defaults
"""


if __name__ == "__main__":
    import sys as _sys
    if "--whale-mode" in _sys.argv:
        _sys.argv.remove("--whale-mode")
        # Print whale-mode help if --help was also passed
        if "--help" in _sys.argv or "-h" in _sys.argv:
            _sys.argv.remove("--help") if "--help" in _sys.argv else None
            _sys.argv.remove("-h") if "-h" in _sys.argv else None
            print(WHALE_MODE_USAGE)
        else:
            run_whale_pmxt_backtest()
    else:
        main()
