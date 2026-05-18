"""Polymarket Whale Follower — Live Trading Node.

Connects to Polymarket via nautilus_trader and runs the WhaleFollower strategy.

Credentials (env vars):
    POLYMARKET_PK          - Polygon wallet private key
    POLYMARKET_API_KEY     - Polymarket API key
    POLYMARKET_API_SECRET  - Polymarket API secret
    POLYMARKET_PASSPHRASE  - Polymarket API passphrase

Usage:
    export POLYMARKET_PK="..."
    export POLYMARKET_API_KEY="..."
    export POLYMARKET_API_SECRET="..."
    export POLYMARKET_PASSPHRASE="..."
    python run_live.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket import PolymarketExecClientConfig
from nautilus_trader.adapters.polymarket import PolymarketLiveDataClientFactory
from nautilus_trader.adapters.polymarket import PolymarketLiveExecClientFactory
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_instrument_id
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig, LoggingConfig, TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId

from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig

# ── Market ──────────────────────────────────────────────────────────────
# Default: GTA VI released before June 2026
# Find markets: python nautilus_trader/adapters/polymarket/scripts/active_markets.py
CONDITION_ID = os.getenv(
    "POLYMARKET_CONDITION_ID",
    "0xcccb7e7613a087c132b69cbf3a02bece3fdcb824c1da54ae79acc8d4a562d902",
)
TOKEN_ID = os.getenv(
    "POLYMARKET_TOKEN_ID",
    "8441400852834915183759801017793514978104486628517653995211751018945988243154",
)
INSTRUMENT_ID = get_polymarket_instrument_id(CONDITION_ID, TOKEN_ID)

# ── Instrument provider ────────────────────────────────────────────────
instrument_config = PolymarketInstrumentProviderConfig(
    load_ids=frozenset([str(INSTRUMENT_ID)]),
)

# ── Node ───────────────────────────────────────────────────────────────
config_node = TradingNodeConfig(
    trader_id=TraderId("WHALE-FOLLOWER-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    exec_engine=LiveExecEngineConfig(
        reconciliation=True,
        reconciliation_instrument_ids=[INSTRUMENT_ID],
        open_check_interval_secs=5.0,
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
    timeout_connection=20.0,
    timeout_reconciliation=10.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=5.0,
)

# ── Strategy ───────────────────────────────────────────────────────────
config_strategy = WhaleFollowerConfig(
    instrument_ids=[INSTRUMENT_ID],
    bankroll=float(os.getenv("BANKROLL", "10000")),
    kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.25")),
    
    stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "0.15")),
    max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.10")),
)

# ── Build ──────────────────────────────────────────────────────────────
node = TradingNode(config=config_node)
strategy = WhaleFollower(config=config_strategy)
node.trader.add_strategy(strategy)
node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
node.build()

# ── Run ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Market:       {INSTRUMENT_ID}")
    print(f"Bankroll:     ${config_strategy.bankroll:,.0f}")
    print(f"Kelly:        {config_strategy.kelly_fraction}x")
    print(f"Win Rate:     {config_strategy.whale_win_rate:.0%}")
    print(f"Stop Loss:    {config_strategy.stop_loss_pct:.0%}")
    creds = "SET" if os.getenv("POLYMARKET_PK") else "NOT SET (data-only mode)"
    print(f"Credentials:  {creds}")
    print()
    try:
        node.run()
    finally:
        node.dispose()
