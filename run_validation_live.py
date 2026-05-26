"""Polymarket Whale Follower — Validation Live Trading Node ($100 Capital).

Phase 2 validation runner for $100 capital testing with $100k safety standards.
- Reads credentials from .env (same location as run_live.py)
- Subscribes to TOP 10 active whale markets (reduced from 15 for validation)
- Real Polymarket CLOB execution (not sandbox)
- Reconciliation enabled on startup
- Graceful SIGTERM/INT shutdown
- Phase 2 whitelist filters enforced BEFORE position sizing
- Kill switch from Phase 1 preserved

Usage:
    cd ~/workspace/nautilus-trading
    source venv/bin/activate
    # Create guard file first (required for service to start)
    touch .guard/validation-live.ok
    python run_validation_live.py

Credentials (env vars, loaded from .env):
    POLYMARKET_PK          - Polygon wallet private key
    POLYMARKET_API_KEY     - Polymarket API key
    POLYMARKET_API_SECRET  - Polymarket API secret
    POLYMARKET_PASSPHRASE  - Polymarket API passphrase

CRITICAL: $100 capital treated with $100k safety standards.
          - Max $2 per position (2% of capital)
          - Max 5 concurrent positions
          - Daily loss limit $10 (10% of capital)
          - Only politics/geopolitics/general categories
          - Only skilled_human/sacrificial_account/degenerate_human whales
"""

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Line-buffered stdout so crash output isn't silently lost
sys.stdout.reconfigure(line_buffering=True)

from decimal import Decimal

# Set trade mode to live for Phase 2 validation
os.environ["TRADE_MODE"] = "live"

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
from strategies.wf_constants import (
    VALIDATION_CAPITAL,
    VALIDATION_DAILY_LOSS_LIMIT,
    VALIDATION_MAX_POSITION_USD,
    VALIDATION_MAX_CONCURRENT,
    VALIDATION_KELLY_FRACTION,
)

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


# ── Load top whale markets ──────────────────────────────────────────────
def load_top_whale_markets(limit: int = 10) -> list[dict]:
    """Fetch top whale markets from discovery DB + Polymarket positions API.
    
    Reduced from 15 to 10 for validation mode (smaller capital = fewer markets).
    """
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


# ── Fetch instrument definitions ──────────────────────────────────────────
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


# ── Validate credentials ──────────────────────────────────────────────────
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


# ── Guard file check ──────────────────────────────────────────────────────
def check_guard_file() -> None:
    """Ensure guard file exists before starting live trading."""
    guard_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        ".guard", "validation-live.ok"
    )
    if not os.path.exists(guard_path):
        print(f"ERROR: Guard file not found at {guard_path}")
        print("Create guard file to enable validation live trading:")
        print("  touch .guard/validation-live.ok")
        print("")
        print("This guard prevents accidental real-money execution.")
        sys.exit(1)
    print(f"Guard file found: {guard_path}")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    validate_credentials()
    check_guard_file()

    # Phase 2 validation parameters
    bankroll = VALIDATION_CAPITAL  # $100
    kelly_fraction = VALIDATION_KELLY_FRACTION  # 0.10
    max_position_pct = 0.02  # 2% of $100 = $2 max per position
    max_open_positions = VALIDATION_MAX_CONCURRENT  # 5
    daily_loss_limit = VALIDATION_DAILY_LOSS_LIMIT  # $10

    print(f"\nScanning current whale positions for top 10 active markets...")
    whale_markets = load_top_whale_markets(limit=10)

    if not whale_markets:
        print("ERROR: No whale markets found. Check the discovery DB.")
        sys.exit(1)

    all_instruments = load_instruments(whale_markets)
    if not all_instruments:
        print("ERROR: No active instruments loaded")
        sys.exit(1)

    instrument_ids = [inst.id for inst, _, _ in all_instruments]
    reconciliation_ids = [inst.id for inst, _, _ in all_instruments]

    # ── Instrument provider config ──────────────────────────────────────
    instrument_config = PolymarketInstrumentProviderConfig(
        load_ids=frozenset(str(i) for i in instrument_ids),
    )

    # ── Node config (validation: $100 bankroll) ──────────────────────────
    config_node = TradingNodeConfig(
        trader_id=TraderId("WHALE-FOLLOWER-VALIDATION"),
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

    # ── Strategy config (Phase 2 validation) ─────────────────────────────
    config_strategy = WhaleFollowerConfig(
        instrument_ids=instrument_ids,
        bankroll=bankroll,
        kelly_fraction=kelly_fraction,
        stop_loss_pct=0.15,
        take_profit_pct=0.20,
        max_position_pct=max_position_pct,
        min_confidence=0.60,  # Higher confidence threshold for validation
        auto_trade=True,
        use_dynamic_kelly=True,
        max_trades_per_scan=2,  # Reduced for validation
        max_hold_hours=4.0,
        max_open_positions=max_open_positions,
        daily_loss_limit=daily_loss_limit,
        validation_capital_base=bankroll,
        test_mode=False,
    )

    # ── Build node ──────────────────────────────────────────────────────
    node = TradingNode(config=config_node)
    strategy = WhaleFollower(config=config_strategy)
    node.trader.add_strategy(strategy)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)

    # ── Graceful shutdown on SIGTERM/SIGINT ─────────────────────────────
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

    # ── Run ─────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  POLYMARKET WHALE FOLLOWER — VALIDATION LIVE ($100)")
    print("=" * 60)
    print(f"  Mode:        VALIDATION (Phase 2 Whitelist Filters)")
    print(f"  Markets:     {len(whale_markets)} active whale markets")
    print(f"  Instruments: {len(all_instruments)} (YES+NO tokens)")
    print(f"  Venue:       POLYMARKET (REAL MONEY)")
    print(f"  Bankroll:    ${bankroll:,.0f}")
    print(f"  Kelly:       {config_strategy.kelly_fraction}x")
    print(f"  Max Pos USD: ${VALIDATION_MAX_POSITION_USD:.0f} (2% of capital)")
    print(f"  Max Positions: {max_open_positions} concurrent")
    print(f"  Daily Loss Limit: ${daily_loss_limit:.0f}")
    print(f"  Whitelist Categories: politics, geopolitics, general")
    print(f"  Whitelist Whale Types: skilled_human, sacrificial_account, degenerate_human")
    print()
    print("  Data:   LIVE Polymarket WebSocket (authenticated)")
    print("  Exec:   LIVE Polymarket CLOB API (REAL MONEY)")
    print(f"  Reconciliation: ENABLED ({len(reconciliation_ids)} instruments)")
    print("  Risk:   CONTROLLED — $100 with $100k safety standards")
    print("  Kill Switch: ENABLED (Phase 1 preserved)")
    print("=" * 60)
    print()

    try:
        node.run()
    finally:
        print("\nShutting down node...")
        node.dispose()
        print("Node disposed. Validation live runner stopped.")


if __name__ == "__main__":
    main()