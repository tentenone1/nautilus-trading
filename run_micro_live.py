"""Polymarket Whale Follower — Micro Live Trading Node.

Minimal live runner for $2000 real-money trading via Polymarket CLOB API.
- Reads credentials from .env (same location as run_live.py)
# Subscribes to TOP 15 active whale markets (scaled from 5, PHASE 3.3)
- Real Polymarket CLOB execution (not sandbox)
- Reconciliation enabled on startup
- Graceful SIGTERM/INT shutdown

Usage:
    cd ~/workspace/nautilus-trading
    source venv/bin/activate
    python run_micro_live.py

Credentials (env vars, loaded from .env):
    POLYMARKET_PK          - Polygon wallet private key
    POLYMARKET_API_KEY     - Polymarket API key
    POLYMARKET_API_SECRET  - Polymarket API secret
    POLYMARKET_PASSPHRASE  - Polymarket API passphrase
"""

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── SHADOW_MODE hard stop ─────────────────────────────────────────────
from strategies.wf_constants import SHADOW_MODE
if SHADOW_MODE:
    print("FATAL: SHADOW_MODE=True — run_micro_live.py is blocked.")
    print("Set SHADOW_MODE=False in strategies/wf_constants.py to enable live trading.")
    sys.exit(1)

# Line-buffered stdout so crash output isn't silently lost
sys.stdout.reconfigure(line_buffering=True)

from decimal import Decimal

# Fix: Follow HTTP redirects
import httpx as _httpx
import py_clob_client.http_helpers.helpers as _clob_helpers
_clob_helpers._http_client = _httpx.Client(http2=True, follow_redirects=True)

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket import PolymarketExecClientConfig
from nautilus_trader.adapters.polymarket import PolymarketLiveDataClientFactory
from nautilus_trader.adapters.polymarket import PolymarketLiveExecClientFactory
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig, LoggingConfig, TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import TraderId

from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig

# ── Load .env file ─────────────────────────────────────────────────────
def load_dotenv(path: str = None) -> None:
    """Load environment variables from .env file."""
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        print(f"ERROR: .env not found at {path}")
        print("Copy .env.example to .env and fill in credentials.")
        sys.exit(1)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key not in os.environ:
                    os.environ[key] = value

load_dotenv()

# ── Load top 5 whale markets ──────────────────────────────────────────
def load_top_whale_markets(limit: int = 5) -> list[dict]:
    """Fetch top whale markets from discovery DB + Polymarket positions API."""
    db_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "pipeline", "data", "whale_discovery.db"
    )
    addresses = []
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT address FROM whales WHERE alpha_score >= 70 ORDER BY alpha_score DESC LIMIT 10"
        ).fetchall()
        conn.close()
        addresses = [r[0] for r in rows]

    if not addresses:
        print("WARNING: No whale addresses in DB. Using fallback markets.")
        return []

    market_conds = {}
    for addr in addresses:
        try:
            result = subprocess.run(
                ["curl", "-s", "-m", "15",
                 f"https://data-api.polymarket.com/positions?user={addr}&limit=30"],
                capture_output=True, text=True, timeout=20
            )
            if result.returncode == 0 and result.stdout.strip():
                positions = json.loads(result.stdout)
                for pos in positions:
                    cond = pos.get("conditionId", "")
                    if not cond:
                        continue
                    if cond not in market_conds:
                        market_conds[cond] = {
                            "condition_id": cond,
                            "title": pos.get("title", ""),
                            "whale_count": 0,
                        }
                    market_conds[cond]["whale_count"] += 1
        except Exception as e:
            print(f"  API error for {addr[:12]}...: {e}")
            continue

    markets_list = sorted(
        market_conds.values(), key=lambda x: x["whale_count"], reverse=True
    )[:limit]
    for m in markets_list:
        print(f"  [{m['whale_count']} whales] {m['title'][:60]}")
    return markets_list


# ── Fetch instrument definitions ──────────────────────────────────────
def load_instruments(whale_markets: list[dict]) -> list:
    """Fetch instrument definitions from Polymarket CLOB."""
    print(f"\nFetching {len(whale_markets)} market definitions from CLOB API...")
    clob = ClobClient(host="https://clob.polymarket.com", chain_id=POLYGON)

    all_instruments = []  # (instrument, market_title, condition_id)
    seen_conditions = set()

    for m in whale_markets:
        cond = m["condition_id"]
        if cond in seen_conditions:
            continue
        seen_conditions.add(cond)

        try:
            market_info = clob.get_market(condition_id=cond)
            if not market_info.get("active", False):
                print(f"  SKIP (inactive): {m['title'][:50]}")
                continue

            tokens = market_info.get("tokens", [])
            for t in tokens:
                instrument = parse_polymarket_instrument(
                    market_info=market_info,
                    token_id=t["token_id"],
                    outcome=t["outcome"],
                )
                all_instruments.append((instrument, m["title"], cond))
        except Exception as e:
            print(f"  FAIL: {m['title'][:50]} | {e}")

    print(f"\nLoaded {len(all_instruments)} instruments from {len(seen_conditions)} markets")
    return all_instruments


# ── Validate credentials ──────────────────────────────────────────────
def validate_credentials() -> None:
    """Ensure all required env vars are set."""
    required = ["POLYMARKET_PK", "POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_PASSPHRASE"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ERROR: Missing credentials: {', '.join(missing)}")
        print("Set them in .env file. See .env.example for template.")
        sys.exit(1)
    pk = os.getenv("POLYMARKET_PK", "")
    if pk in ("your_polygon_private_key", ""):
        print("ERROR: POLYMARKET_PK not configured (still has placeholder value)")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────
def main():
    validate_credentials()

    print(f"Scanning current whale positions for top 15 active markets...")
    whale_markets = load_top_whale_markets(limit=15)

    if not whale_markets:
        print("ERROR: No whale markets found. Check the discovery DB.")
        sys.exit(1)

    all_instruments = load_instruments(whale_markets)
    if not all_instruments:
        print("ERROR: No active instruments loaded")
        sys.exit(1)

    instrument_ids = [inst.id for inst, _, _ in all_instruments]
    reconciliation_ids = [inst.id for inst, _, _ in all_instruments]

    # ── Instrument provider config ────────────────────────────────────
    instrument_config = PolymarketInstrumentProviderConfig(
        load_ids=frozenset(str(i) for i in instrument_ids),
    )

    # ── Node config (micro: $250 bankroll) ────────────────────────────
    bankroll = float(os.getenv("MICRO_BANKROLL", "250"))
    config_node = TradingNodeConfig(
        trader_id=TraderId("WHALE-FOLLOWER-MICRO"),
        logging=LoggingConfig(log_level="INFO", use_pyo3=True),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,
            reconciliation_instrument_ids=reconciliation_ids,
            open_check_interval_secs=10.0,
            graceful_shutdown_on_exception=True,
        ),
        data_clients={
            POLYMARKET: PolymarketDataClientConfig(
                instrument_config=instrument_config,
            ),
        },
        exec_clients={
            POLYMARKET: PolymarketExecClientConfig(
                instrument_config=instrument_config,
            ),
        },
        timeout_connection=30.0,
        timeout_reconciliation=15.0,
        timeout_portfolio=15.0,
        timeout_disconnection=15.0,
        timeout_post_stop=10.0,
    )

    # ── Strategy config ───────────────────────────────────────────────
    config_strategy = WhaleFollowerConfig(
        instrument_ids=instrument_ids,
        bankroll=bankroll,
        kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.25")),
        stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "0.15")),
        take_profit_pct=float(os.getenv("TAKE_PROFIT_PCT", "0.20")),
        max_position_pct=float(os.getenv("MICRO_MAX_POS_PCT", "0.05")),
        min_confidence=float(os.getenv("MICRO_MIN_CONFIDENCE", "0.55")),
        auto_trade=os.getenv("MICRO_AUTO_TRADE", "true").lower() == "true",
        use_dynamic_kelly=True,
        max_trades_per_scan=int(os.getenv("MICRO_MAX_TRADES_PER_SCAN", "2")),
        max_hold_hours=float(os.getenv("MICRO_MAX_HOLD_HOURS", "6.0")),
        test_mode=False,
    )

    # ── Build node ────────────────────────────────────────────────────
    node = TradingNode(config=config_node)
    strategy = WhaleFollower(config=config_strategy)
    node.trader.add_strategy(strategy)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)

    # ── Graceful shutdown on SIGTERM/SIGINT ───────────────────────────
    shutdown_event = asyncio.Event()

    def handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        print(f"\nReceived {sig_name}. Initiating graceful shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Preload instruments into cache
    print(f"\nPreloading {len(all_instruments)} instruments into cache...")
    for instrument, title, cond in all_instruments:
        node._builder._cache.add_instrument(instrument)
    print(f"  Cached {len(all_instruments)} instruments across {len(whale_markets)} markets")

    node.build()

    # ── Run ───────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  POLYMARKET WHALE FOLLOWER — MICRO LIVE TRADING")
    print("=" * 60)
    print(f"  Markets:     {len(whale_markets)} active whale markets")
    print(f"  Instruments: {len(all_instruments)} (YES+NO tokens)")
    print(f"  Venue:       POLYMARKET (REAL MONEY)")
    print(f"  Bankroll:    ${bankroll:,.0f}")
    print(f"  Kelly:       {config_strategy.kelly_fraction}x")
    print(f"  Stop Loss:   {config_strategy.stop_loss_pct:.0%}")
    print(f"  Take Profit: {config_strategy.take_profit_pct:.0%}")
    print(f"  Max Pos:     {config_strategy.max_position_pct:.0%} of bankroll")
    print(f"  Min Conf:    {config_strategy.min_confidence:.0%}")
    print(f"  Max Trades:  {config_strategy.max_trades_per_scan} per scan")
    print(f"  Max Hold:    {config_strategy.max_hold_hours}h")
    print()
    print("  Data:   LIVE Polymarket WebSocket (authenticated)")
    print("  Exec:   LIVE Polymarket CLOB API (REAL MONEY)")
    print(f"  Reconciliation: ENABLED ({len(reconciliation_ids)} instruments)")
    print("  Risk:   HIGH — real money at risk")
    print("=" * 60)
    print()

    try:
        node.run()
    finally:
        print("\nShutting down node...")
        node.dispose()
        print("Node disposed. Micro live runner stopped.")


if __name__ == "__main__":
    main()
