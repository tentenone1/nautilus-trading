"""Polymarket Whale Follower — Paper Trading (Sandbox) Node.

Multi-market: subscribes to top active whale markets across sports, politics, crypto.
Uses LIVE Polymarket data + SANDBOX execution (simulated fills).
NO real money. NO real API keys needed.

Usage:
    cd ~/workspace/nautilus-trading
    venv/bin/python run_paper.py
"""

import asyncio
import faulthandler
import json
import os
import signal
import sqlite3
import sys
import threading
import time as time_module
from http.server import HTTPServer, BaseHTTPRequestHandler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Enable faulthandler FIRST — catch C-level crashes and segfaults with Python traceback
faulthandler.enable()

# Fix: Line-buffered stdout so crash output isn't silently lost
sys.stdout.reconfigure(line_buffering=True)

# ── SIGABRT handler — write crash trace before dying so we catch the root cause ──
def _sigabrt_handler(signum, frame):
    import traceback
    crash_log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "sigabrt_crash.log")
    with open(crash_log, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"SIGABRT received at {time_module.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"PID: {os.getpid()}\n")
        f.write(f"Thread: {threading.current_thread().name}\n")
        f.write("Full stack trace:\n")
        traceback.print_stack(frame, file=f)
        # Print all thread stacks
        f.write(f"\nAll threads at crash time:\n")
        for tid, frame in sys._current_frames().items():
            f.write(f"\n--- Thread {tid} ---\n")
            traceback.print_stack(frame, file=f)
    os.kill(os.getpid(), signal.SIGABRT)  # re-raise after logging

# ── PID file lock — prevent duplicate processes (systemd User= double-fork workaround) ──
import atexit
PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".run_paper.pid")

def _check_pid_lock():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                old_pid = int(f.read().strip())
            # Check if process with that PID is still running
            os.kill(old_pid, 0)  # signal 0 = existence check
            print(f"Another instance already running (PID {old_pid}). Exiting (code 1).")
            sys.exit(0)  # exit 0 = clean exit — systemd Restart=on-failure won't retry
        except (ValueError, OSError, ProcessLookupError):
            # PID file stale or process dead — we can start
            pass

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    def _cleanup_pid():
        try:
            if os.path.exists(PID_FILE):
                with open(PID_FILE) as f:
                    stored = f.read().strip()
                if stored == str(os.getpid()):
                    os.remove(PID_FILE)
        except OSError:
            pass

    def _sigterm_handler(signum, frame):
        _cleanup_pid()
        sys.exit(0)

    atexit.register(_cleanup_pid)
    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGABRT, _sigabrt_handler)

_check_pid_lock()

# ── TRADING MODE GUARD ──────────────────────────────────────────────────
assert os.getenv("TRADING_MODE") == "paper", (
    "FATAL: TRADING_MODE must be 'paper'. "
    "This is NOT live trading. Set: export TRADING_MODE=paper"
)
assert os.getenv("PAPER_TRADING") in ("true", "1", "yes"), (
    "FATAL: PAPER_TRADING must be 'true'. "
    "This system executes SANDBOX trades only. No real money."
)
print("""
╔══════════════════════════════════════════════════════════════╗
║  PAPER TRADING MODE — NO REAL MONEY                        ║
║  TRADING_MODE=paper  |  PAPER_TRADING=true                 ║
║  SandboxExecutionClient — simulated fills at real prices     ║
╚══════════════════════════════════════════════════════════════╝
""")

from decimal import Decimal

# --- Fix 1: Follow HTTP redirects ---
# py_clob_client raises PolyApiException(301) on redirects.
# Monkey-patch the module-level httpx client to follow them.
import httpx as _httpx
import py_clob_client.http_helpers.helpers as _clob_helpers
_clob_helpers._http_client = _httpx.Client(http2=True, follow_redirects=True)
# -------------------------------------

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.adapters.polymarket.common.symbol import get_polymarket_instrument_id
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
from nautilus_trader.config import LiveExecEngineConfig, LoggingConfig, TradingNodeConfig, RoutingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import TraderId, Venue

from strategies.whale_follower import WhaleFollower, WhaleFollowerConfig

from components.position_reconciler import PositionReconciler

from strategies.wf_circuit_breaker import get_whale_api_breaker, get_clob_breaker, CircuitBreakerOpen

# ── Load top whale markets from CURRENT whale positions (Polymarket data API) ──
def load_whale_markets_from_api(limit: int = 20) -> list[dict]:
    """Fetch markets whales are actively holding positions in — live data, not stale DB.
    
    Rate limit: Polymarket allows ~100 requests/min for unauthenticated.
    We use 0.7s between calls = ~85 requests/min (safe margin).
    Retry logic: 3 retries with exponential backoff on empty responses.
    """
    import time
    
    db_path = "/home/elon-1/workspace/nautilus-trading/data/whale_discovery.db"
    addresses = []
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT address FROM whales WHERE alpha_score >= 70 ORDER BY alpha_score DESC LIMIT 15"
        ).fetchall()
        conn.close()
        addresses = [r[0] for r in rows]

    if not addresses:
        print("No whale addresses in DB, using fallback")
        return []

    import subprocess, json as _json
    market_conds = {}
    failed_count = 0
    
    for i, addr in enumerate(addresses):
        # Rate limit: 0.7s between requests
        if i > 0:
            time.sleep(0.7)
        
        success = False
        for retry in range(3):
            try:
                def _do_curl():
                    return subprocess.run(
                        ["curl", "-s", "-m", "15",
                         f"https://data-api.polymarket.com/positions?user={addr}&limit=50"],
                        capture_output=True, text=True, timeout=20
                    )
                
                result = get_whale_api_breaker().call(_do_curl)
                
                if result.returncode == 0 and result.stdout.strip():
                    positions = _json.loads(result.stdout)
                    if positions:
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
                        print(f"  OK [{i+1}/{len(addresses)}]: {addr[:12]}... ({len(positions)} pos)")
                        success = True
                        break
                    else:
                        print(f"  EMPTY [{i+1}/{len(addresses)}]: {addr[:12]}...")
                        success = True
                        break
                else:
                    if retry < 2:
                        wait = 2 ** retry
                        print(f"  RETRY {retry+1}: {addr[:12]}... (wait {wait}s)")
                        time.sleep(wait)
                    else:
                        print(f"  SKIP [{i+1}/{len(addresses)}]: {addr[:12]}... (rate limited)")
                        failed_count += 1
                        
            except CircuitBreakerOpen:
                print(f"  SKIP [{i+1}/{len(addresses)}]: {addr[:12]}... (circuit breaker open)")
                failed_count += 1
                break
            except subprocess.TimeoutExpired:
                if retry < 2:
                    wait = 2 ** retry
                    print(f"  TIMEOUT retry {retry+1}: {addr[:12]}...")
                    time.sleep(wait)
                else:
                    print(f"  TIMEOUT [{i+1}/{len(addresses)}]: {addr[:12]}...")
                    failed_count += 1
            except Exception as e:
                print(f"  ERROR [{i+1}/{len(addresses)}]: {addr[:12]}...: {e}")
                failed_count += 1
                break
    
    if failed_count > 0:
        print(f"  Failed: {failed_count}/{len(addresses)} addresses")

    # Sort by whale count and return top N
    markets_list = sorted(market_conds.values(), key=lambda x: x["whale_count"], reverse=True)[:limit]
    return markets_list
print("Scanning current whale positions for active markets...")
whale_markets = load_whale_markets_from_api(limit=80)
print(f"Found {len(whale_markets)} active whale markets")
for i, m in enumerate(whale_markets):
    print(f"  [{m['whale_count']:2d}] {m['title'][:55]}")

if not whale_markets:
    print("ERROR: No whale markets found. Check the discovery DB.")
    sys.exit(1)  # exit 1 = failure — systemd Restart=on-failure won't retry

# ── Fetch instrument definitions from Polymarket (anonymous) ─────────────
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
        def _do_get_market():
            return clob.get_market(condition_id=cond)
        
        market_info = get_clob_breaker().call(_do_get_market)
        if market_info is None:
            print(f"  SKIP (circuit open): {m['title'][:50]}")
            continue
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
            if len(all_instruments) == 1:
                print(f"  OK: {m['title'][:50]} | {instrument.id}")

    except CircuitBreakerOpen:
        print(f"  SKIP (circuit breaker open): {m['title'][:50]}")
        continue
    except Exception as e:
        print(f"  FAIL: {m['title'][:50]} | {e}")

print(f"\nLoaded {len(all_instruments)} instruments from {len(seen_conditions)} markets")

if not all_instruments:
    print("ERROR: No active instruments loaded")
    sys.exit(1)  # exit 1 = failure — systemd Restart=on-failure won't retry

# Extract instrument IDs
instrument_ids = [inst.id for inst, _, _ in all_instruments]

# ── Sandbox execution (simulated fills) ────────────────────────────────
SANDBOX_VENUE = Venue("POLYMARKET")
instrument_config = PolymarketInstrumentProviderConfig()

sandbox_config = SandboxExecutionClientConfig(
    instrument_provider=instrument_config,
    venue=str(SANDBOX_VENUE),
    account_type="CASH",
    oms_type="NETTING",
    starting_balances=["500 USDC.e", "10000 pUSD"],
    default_leverage=Decimal(1),
)

# ── Node ───────────────────────────────────────────────────────────────
config_node = TradingNodeConfig(
    trader_id=TraderId("WHALE-FOLLOWER-PAPER"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    exec_engine=LiveExecEngineConfig(
        reconciliation=False,
        open_check_interval_secs=5.0,
    ),
    data_clients={
        POLYMARKET: PolymarketDataClientConfig(
            instrument_provider=instrument_config,
        ),
    },
    exec_clients={
        str(SANDBOX_VENUE): sandbox_config,
    },
    timeout_connection=30.0,
    timeout_reconciliation=5.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=5.0,
)

# ── Strategy ───────────────────────────────────────────────────────────
config_strategy = WhaleFollowerConfig(
    instrument_ids=instrument_ids,
    bankroll=float(os.getenv("BANKROLL", "500")),
    kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.25")),
    stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "0.25")),
    take_profit_pct=float(os.getenv("TAKE_PROFIT_PCT", "0.50")),
    max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.10")),
    max_single_position_pct=float(os.getenv("MAX_SINGLE_PCT", "0.05")),  # 5% = $25 max on $500
    min_confidence=float(os.getenv("MIN_CONFIDENCE", "0.25")),  # Lower threshold = more trades
    minimaxi_api_key=os.getenv("MINIMAX_API_KEY", ""),  # LLM scoring — falls back to score=5 if missing
    auto_trade=os.getenv("AUTO_TRADE", "true").lower() == "true",
    test_mode=False,  # Real mode: actual whale signals → real trades
    test_signal_interval_secs=float(os.getenv("TEST_SIGNAL_INTERVAL", "60")),
)

# ── Build ──────────────────────────────────────────────────────────────
node = TradingNode(config=config_node)
strategy = WhaleFollower(config=config_strategy)
node.trader.add_strategy(strategy)

# ── Preload ALL instruments into cache BEFORE building data client ───────
print(f"\nPreloading {len(all_instruments)} instruments into cache...")
for instrument, title, cond in all_instruments:
    node._builder._cache.add_instrument(instrument)

print(f"  Cached {len(all_instruments)} instruments across {len(seen_conditions)} markets")

# ── Anonymous data factory ─────────────────────────────────────────────
from nautilus_trader.adapters.polymarket.data import PolymarketDataClient
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProvider
from nautilus_trader.live.factories import LiveDataClientFactory


class AnonymousPolymarketDataFactory(LiveDataClientFactory):
    """Creates Polymarket data client using anonymous (read-only) access."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: PolymarketDataClientConfig,
        msgbus,
        cache,
        clock,
    ):
        http_client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=POLYGON,
        )
        provider = PolymarketInstrumentProvider(
            client=http_client,
            clock=clock,
            config=config.instrument_provider,
        )
        client = PolymarketDataClient(
            loop=loop,
            http_client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )

        # ── Import WS message types for method overrides ──────────
        from nautilus_trader.adapters.polymarket.data import (
            PolymarketQuotes, PolymarketBookSnapshot,
            PolymarketTrade, PolymarketTickSizeChange,
        )

        # ── Suppress "Cannot find instrument" WARN spam ────────────
        # The WebSocket sends data for ALL tokens in a market (Yes/No
        # outcomes + derived tokens). Some tokens aren't in cache because
        # markets resolved between API fetch and WebSocket data arrival.
        # This floods logs at 600MB+/day. The Logger class is Cython/read-only
        # so we override the Python methods that produce the warnings instead.
        # See: ~/wiki/nautilus-stale-instrument-warn-spam.md
        #
        # The first 100 warnings are still logged, then suppressed silently.

        import itertools
        _warn_counter = itertools.count()

        def _log_stale_warn(self, instrument_id):
            """Log first 100 stale instrument warnings, then suppress."""
            count = next(_warn_counter)
            if count < 100:
                self._log.warning(f"Cannot find instrument for {instrument_id} (stale, suppressed)")
            elif count == 100:
                self._log.warning(
                    "[SUPPRESSED] Further 'Cannot find instrument' "
                    "warnings suppressed — stale instruments from resolved markets."
                )

        # Override _handle_quotes (highest volume: ~4M/day)
        def _suppressed_handle_quotes(self, ws_message):
            for price_change in ws_message.price_changes:
                instrument_id = get_polymarket_instrument_id(
                    ws_message.market, price_change.asset_id
                )
                instrument = self._cache.instrument(instrument_id)
                if instrument is None:
                    _log_stale_warn(self, instrument_id)
                    continue
                self._handle_quote(
                    instrument=instrument,
                    ws_message=ws_message,
                    price_change=price_change,
                )

        # Override _handle_ws_message (book snapshots, trades, tick changes)
        def _suppressed_handle_ws_message(self, msg):
            if isinstance(msg, PolymarketQuotes):
                self._handle_quotes(ws_message=msg)
            elif isinstance(msg, PolymarketBookSnapshot):
                instrument_id = get_polymarket_instrument_id(msg.market, msg.asset_id)
                instrument = self._cache.instrument(instrument_id)
                if instrument is None:
                    _log_stale_warn(self, instrument_id)
                    return
                self._handle_book_snapshot(instrument=instrument, ws_message=msg)
            elif isinstance(msg, PolymarketTrade):
                instrument_id = get_polymarket_instrument_id(msg.market, msg.asset_id)
                instrument = self._cache.instrument(instrument_id)
                if instrument is None:
                    _log_stale_warn(self, instrument_id)
                    return
                self._handle_trade(instrument=instrument, ws_message=msg)
            elif isinstance(msg, PolymarketTickSizeChange):
                instrument_id = get_polymarket_instrument_id(msg.market, msg.asset_id)
                instrument = self._cache.instrument(instrument_id)
                if instrument is None:
                    _log_stale_warn(self, instrument_id)
                    return
                self._handle_instrument_update(instrument=instrument, ws_message=msg)
            else:
                self._log.error(f"Unknown websocket message topic: {msg}")

        # Bind and replace methods on this instance
        from types import MethodType

        client._handle_quotes = MethodType(_suppressed_handle_quotes, client)
        client._handle_ws_message = MethodType(_suppressed_handle_ws_message, client)
        # Note: _request_instrument is not overridden because it fires
        # rarely (requested by nautilus engine) vs every WebSocket message.
        # ──────────────────────────────────────────────────────────

        return client


# ── Custom paper execution (monkey-patch SandboxExecutionClient) ───────
# We patch submit_order BEFORE the factory creates the client.
# Our replacement generates OrderFilled events directly (bypassing Cython)
# so our computed real-market fill price reaches the trade record.
from nautilus_trader.adapters.sandbox.execution import SandboxExecutionClient
from components.paper_execution import PaperExecClient

# Replace Cython submit_order with our own — generates fill events at real prices
SandboxExecutionClient.submit_order = PaperExecClient.submit_order
print("  Patched SandboxExecutionClient.submit_order with direct-fill event generation")

node.add_data_client_factory(POLYMARKET, AnonymousPolymarketDataFactory)
node.add_exec_client_factory(str(SANDBOX_VENUE), SandboxLiveExecClientFactory)
node.build()

# ── WS connection state tracker ─────────────────────────────────────────
_WS_CONNECTED = {"value": False}


def set_ws_connected(v: bool) -> None:
    """Called by patched PolymarketDataClient when WS connects/disconnects."""
    _WS_CONNECTED["value"] = v


def is_ws_connected() -> bool:
    return _WS_CONNECTED["value"]


# ── Patch PolymarketDataClient._set_connected to track WS state ──────────
from nautilus_trader.adapters.polymarket.data import PolymarketDataClient

_orig_set_connected = PolymarketDataClient._set_connected


def _patched_set_connected(self, connected: bool) -> None:
    _WS_CONNECTED["value"] = connected
    return _orig_set_connected(self, connected)


PolymarketDataClient._set_connected = _patched_set_connected

# ── Health Check HTTP Server ──────────────────────────────────────────────
START_TIME = time_module.time()

def _check_readiness():
    """Can the system accept trades?"""
    checks = {
        "strategy_loaded": strategy is not None,
        "node_built": node is not None,
        "instruments_loaded": len(all_instruments) > 0,
        "ws_connected": _WS_CONNECTED["value"],
    }
    ready = all(checks.values())
    return {
        "status": "ready" if ready else "not_ready",
        "checks": checks,
    }


def _full_health_check():
    """Comprehensive health check with metrics."""
    from components.metrics import get_metrics

    metrics = get_metrics().get_all()

    checks = {
        "open_positions": len(strategy._open_positions) if strategy else 0,
        "daily_pnl": metrics.get("daily_pnl", 0.0),
        "killswitch_active": getattr(strategy, "_kill_switch_breached", False) if strategy else False,
        "daily_loss_breached": getattr(strategy, "_daily_loss_breached", False) if strategy else False,
        "ws_connected": _WS_CONNECTED["value"],
    }

    if checks["killswitch_active"] or checks["daily_loss_breached"]:
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "uptime_secs": round(time_module.time() - START_TIME, 2),
        "checks": checks,
        "metrics": metrics,
    }


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for /health, /health/ready, /health/live, /health/ws."""

    def do_GET(self):
        try:
            if self.path == "/health/live":
                self._json_response({"status": "ok", "uptime_secs": round(time_module.time() - START_TIME, 2)})
            elif self.path == "/health/ready":
                self._json_response(_check_readiness())
            elif self.path == "/health/ws":
                self._json_response({"ws_connected": _WS_CONNECTED["value"]})
            elif self.path == "/health":
                self._json_response(_full_health_check())
            else:
                self.send_error(404)
        except Exception as e:
            self.send_error(500, str(e))

    def _json_response(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass  # Suppress default access logs


def start_health_server(port=8090):
    server = HTTPServer(("127.0.0.1", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ── Run ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print()
    print("=" * 60)
    print("  POLYMARKET WHALE FOLLOWER — PAPER TRADING")
    print("=" * 60)
    print(f"  Markets:     {len(seen_conditions)} active whale markets")
    print(f"  Instruments: {len(all_instruments)} (YES+NO tokens)")
    print(f"  Venue:       {SANDBOX_VENUE} (SIMULATED)")
    print(f"  Bankroll:    ${config_strategy.bankroll:,.0f}")
    print(f"  Kelly:       {config_strategy.kelly_fraction}x")
    print(f"  Stop Loss:   {config_strategy.stop_loss_pct:.0%}")
    print(f"  Take Profit: {config_strategy.take_profit_pct:.0%}")
    print(f"  Max Pos:     {config_strategy.max_position_pct:.0%} of bankroll")
    print(f"  Min Conf:    {config_strategy.min_confidence:.0%}")
    print(f"  Auto Trade:  {config_strategy.auto_trade}")
    print(f"  TEST MODE:   {'YES (synthetic signals every ' + str(int(config_strategy.test_signal_interval_secs)) + 's)' if config_strategy.test_mode else 'NO'}")
    print()
    print("  Data:   LIVE Polymarket WebSocket (anonymous)")
    print("  Exec:   SANDBOX (simulated fills)")
    print("  Risk:   ZERO — no real money")
    print("=" * 60)
    print()

    # ── P1-3: Position Reconciliation (startup + periodic) ─────────────
    reconciler = PositionReconciler()
    print("  Running startup position reconciliation...")
    report = reconciler.reconcile_all()
    print(f"  Startup recon: {report.matched}/{report.total_paper_positions} positions matched"
          f" | {len(report.mismatches)} issues | {len(report.unmatched_paper)} unmatched paper")
    if not report.ok:
        print(f"  ⚠️  RECONCILIATION ISSUES: {len(report.mismatches)} mismatches found")
        for m in report.mismatches[:5]:
            for issue in m.issues:
                print(f"       {issue}")
    # Start periodic reconciliation (every 5 minutes)
    reconciler.start_periodic(interval_secs=300.0)
    print("  Periodic reconciliation started: every 300s")
    print()

    # ── Health check server ──────────────────────────────────────────────
    health_port = int(os.getenv("HEALTH_PORT", "8090"))
    start_health_server(port=health_port)
    print(f"  Health endpoints: http://localhost:{health_port}/health [/live] [/ready] [/ws]")
    print()

    # ── WS watchdog: pause strategy if data feed disconnects ────────────
    _ws_watchdog_running = True

    def _ws_watchdog():
        """Poll WS state every 30s. Log warning and update health on disconnect."""
        consecutive_down = 0
        while _ws_watchdog_running:
            time_module.sleep(30)
            if not _WS_CONNECTED["value"]:
                consecutive_down += 1
                if consecutive_down == 1:
                    print(f"  ⚠️  WS disconnected — pausing strategy signals (check #{consecutive_down})")
                if consecutive_down >= 3:
                    print(f"  ⚠️  ⚠️  WS still down after {consecutive_down * 30}s — check network/exchange")
            else:
                if consecutive_down > 0:
                    print(f"  ✓  WS reconnected after {consecutive_down * 30}s")
                consecutive_down = 0

    _ws_thread = threading.Thread(target=_ws_watchdog, daemon=True)
    _ws_thread.start()
    print("  WS watchdog started: checking every 30s")
    print()

    try:
        node.run()
    finally:
        _ws_watchdog_running = False
        reconciler.stop_periodic()
        node.dispose()
