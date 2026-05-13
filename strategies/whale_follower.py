"""Whale Follower Strategy — Combined Detection.

Combines three signal sources:
1. Known whale wallet tracking (CemeterySun, CarlosMC, benwyatt)
2. WebSocket large trade detection (real-time >$5k trades)
3. Uncensored model analysis for insider edge detection

Uses Nautilus framework for execution, position tracking, and risk management.
"""

from __future__ import annotations

import re
import time
import os
import uuid
import json
import pandas as pd
import requests
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.trading.strategy import Strategy

from py_clob_client.client import ClobClient
from components.resolution_poller import ResolutionPoller
from py_clob_client.constants import POLYGON
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument

# P2: Sybil monitoring integration
try:
    from scripts.sybil_monitor_wrapper import run_sybil_monitoring
except ImportError:
    run_sybil_monitoring = None

from strategies.whale_tiering import WhaleTiering, WhaleIntelligence
from strategies.whale_tracker_new import (
    WhaleIdentity,
    WhaleSignal,
    WhaleSignalType,
    SignalSource,
    WhaleTracker,
)
from strategies.whale_insider_analyzer import WhaleInsiderAnalyzer

# ── Phase 1 Validation Integration ──────────────────────────────────────────────
# Import validation modules with backward compatibility (graceful degradation)
try:
    from components.validation.event_logger import EventType, log_event
    from components.validation.trade_context import TradeContext, get_trade_context
    from components.validation.snapshot_store import freeze_snapshot
    from components.validation.db_router import get_current_mode
    _validation_available = True
except ImportError as e:
    # Validation modules not available - strategy will work without event logging
    _validation_available = False
    EventType = None
    log_event = None
    TradeContext = None
    get_trade_context = None
    freeze_snapshot = None
    get_current_mode = lambda: "paper"

from strategies.wf_constants import (
    TRADE_BUFFER_SIZE_THRESHOLD,
    TRADE_BUFFER_FLUSH_COUNT,
    EXIT_TIMER_INTERVAL_SECS,
    RECYCLE_INTERVAL_SECS,
    RE_ENTRY_COOLDOWN_SECS,
    LOW_CASH_ALERT_PCT,
    WHALE_BLACKLIST,
    SPORTS_WHALE_BLACKLIST,
    CERTAINTY_WIN_THRESHOLD,
    CERTAINTY_LOSS_THRESHOLD,
    MAX_SANE_RETURN,
    MEMORY_PRESSURE_MB,
    STALE_SUBSCRIPTION_TTL_SECS,
    RESOLUTION_EXIT_HOURS,
    SPORTS_EXIT_HOURS_BEFORE_EVENT,
    LIQUIDITY_TIER4_THRESHOLD,
    LIQUIDITY_TIER3_THRESHOLD,
    LIQUIDITY_TIER4_MULTIPLIER,
    LIQUIDITY_TIER3_MULTIPLIER,
    LIQUIDITY_TIER2_MULTIPLIER,
    SPORTS_KELLY_MULTIPLIER,
    SPORTS_DAILY_LOSS_LIMIT,
    SPORTS_WHITELIST_PATTERNS,
    SPORTS_OU_BLACKLIST_PATTERNS,
    SPORTS_VS_BLACKLIST_PATTERNS,
    SINGLE_TEAM_PATTERNS,
    MIN_ENTRY_PRICE,
    MIN_CONFIDENCE,
)
from strategies.wf_sports import is_sports_market, get_market_event_time, should_exit_for_sports
from strategies.wf_db_ops import log_trade_to_db, recover_open_positions
from strategies.wf_position_persistence import save_open_positions, load_open_positions
from strategies.wf_signal_proc import on_signal, scan_whale_positions, process_trade_buffer, llm_score_signal
from strategies.wf_position_checks import check_all_positions, check_daily_loss_limit
from strategies.wf_position_checks import (
    check_position_limits,
    trigger_kill_switch,
    get_current_total_exposure,
    get_market_exposure,
)

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument


# ── Module-level Constants ─────────────────────────────────────────────────────

# Trade buffer thresholds
TRADE_BUFFER_SIZE_THRESHOLD = 200  # Minimum USD to buffer a trade
TRADE_BUFFER_FLUSH_COUNT = 5  # Number of trades to trigger buffer flush

# Exit timer configuration
EXIT_TIMER_INTERVAL_SECS = 30.0  # How often to check all positions for exits
RECYCLE_INTERVAL_SECS = 1800.0  # Unsubscribe/resubscribe interval to flush stale order books

# Sybil signal bridge configuration
SYBIL_SIGNAL_TTL_SECS = 6 * 3600  # 6 hours: discard signals older than this
SYBIL_SPORTS_SIGNAL_TTL_SECS = 3 * 3600  # 3 hours: sports markets decay faster
SYBIL_DEDUP_TTL_SECS = 6 * 3600  # 6 hours: dedup window for processed signals
SYBIL_CONFIDENCE_MIN = 0.40  # Reject signals below this confidence
SYBIL_CONFIDENCE_BASELINE = 0.50  # Reference point for confidence scaling
SYBIL_SIZE_MIN_MULTIPLIER = 0.5  # Floor: never go below 50% of base size
SYBIL_SIZE_MAX_MULTIPLIER = 2.0  # Ceiling: never go above 2x base size
SYBIL_MAX_PRICE_SLIPPAGE = 0.15  # Skip if price moved >15% against signal direction
SYBIL_BASE_SIZE_PCT = {
    "no_bias_fade": 0.10,
    "concentrated_follow": 0.20,
    "manipulation_fade": 0.05,
}

# Position management
RE_ENTRY_COOLDOWN_SECS = 300  # Don't re-enter same instrument within 5 minutes of exit
LOW_CASH_ALERT_PCT = 0.20  # Warn when free balance drops below 20% of bankroll

# Whale blacklist (auto-reject proven losers, data from trades.db)
# Sybil groups added 2026-05-08 from entity_clustering.py daily run (ebd130cd1643)
# Group 1 (15 wallets, timing_corr=14, 1047 trades, 252 markets)
# Group 2 (3 wallets, SMCAOMCRL cluster)
# Group 3 (3 wallets, LaBradfordSmith22 cluster)
WHALE_BLACKLIST = frozenset({
    "0x492442EaB586F242B53bDa933fD5dE859c8A3782-1766317541188",  # sybil G1 coordinator
    "0x5d1d9cfd66ee3068c2a8a57dedf1e1b006dcafd2",  # sybil G2 coordinator
    "0xcF609D3256f0f37f0595E5D",
    "AppleTime67",      # sybil G1
    "Dvitaminbets",     # sybil G1
    "FTWUTB",
    "Herdonia",         # sybil G1
    "JPMorgan101",
    "LaBradfordSmith22",  # sybil G3
    "NewTeamSosed4",    # sybil G1
    "Pajamapants",      # sybil G1
    "SMCAOMCRL",        # sybil G2
    "Sassy-Bucket",
    "Talvez10",         # sybil G1
    "TEST-WHALE-3",
    "TheVeryGoodCow",
    "Wannac",           # sybil G1
    "alwaysfade",
    "asdfjh",
    "beetlepimp",       # sybil G1
    "benwyatt",         # sybil G1
    "bossoskil1",       # sybil G1
    "easypredict",
    "iDARKenjoyer",
    "joblessfinalboss",  # sybil G3
    "johnny234",
    "loitterer",        # sybil G1
    "lu1zzz",
    "matanovik",        # sybil G3
    "meifei123",        # sybil G2
    "mooseborzoi",      # sybil G1
    "pilotlady",        # sybil G1
    "sbsigner",
    "therighteousdog",
    "timezonewarrior",
    "trade-via-Gravia",  # sybil G1
    "TTEST2",
})
SPORTS_WHALE_BLACKLIST = frozenset({
    "0xcF609D3256f0f37f0595E5D",
    "FTWUTB",
    "JPMorgan101",
    "LaBradfordSmith22",
    "SMCAOMCRL",
    "Sassy-Bucket",
    "TEST-WHALE-3",
    "TheVeryGoodCow",
    "alwaysfade",
    "asdfjh",
    "beetlepimp",
    "benwyatt",
    "easypredict",
    "iDARKenjoyer",
    "joblessfinalboss",
    "johnny234",
    "lu1zzz",
    "sbsigner",
    "therighteousdog",
    "timezonewarrior",
    "trade-via-Gravia",
})

# Certainty exit thresholds for binary prediction markets
CERTAINTY_WIN_THRESHOLD = 0.95  # Price above this = very likely to win
CERTAINTY_LOSS_THRESHOLD = 0.05  # Price below this = very likely to lose

# P&L sanity cap
MAX_SANE_RETURN = 2.0  # Cap P&L returns at ±200% to prevent sandbox artifacts

# Memory management
MEMORY_PRESSURE_MB = 2500  # RSS threshold in MB to trigger graceful shutdown

# Subscription cleanup
STALE_SUBSCRIPTION_TTL_SECS = 3600  # Clean up dynamic subscriptions older than 1 hour

# Resolution timing
RESOLUTION_EXIT_HOURS = 6  # Exit if market resolves within this many hours

# Sports market timing
SPORTS_EXIT_HOURS_BEFORE_EVENT = 1  # Exit sports positions this many hours before game

# Liquidity tier thresholds (volume + liquidity in USD)
LIQUIDITY_TIER4_THRESHOLD = 100_000  # Illiquid: reduce to 25% of Kelly
LIQUIDITY_TIER3_THRESHOLD = 1_000_000  # Moderate: reduce to 50% of Kelly

# Liquidity sizing multipliers
LIQUIDITY_TIER4_MULTIPLIER = 0.25
LIQUIDITY_TIER3_MULTIPLIER = 0.50
LIQUIDITY_TIER2_MULTIPLIER = 0.75


class WhaleFollowerConfig(StrategyConfig, frozen=True):
    """Configuration for WhaleFollower."""

    instrument_ids: list[InstrumentId]
    bankroll: float = 10000.0
    kelly_fraction: float = 0.25
    stop_loss_pct: float = 0.15
    take_profit_pct: float = 0.30
    max_position_pct: float = 0.10
    max_open_positions: int = 50
    # Max total gross exposure as % of bankroll (hard cap on aggregate position size)
    max_total_exposure_pct: float = 5.0  # Total open positions capped at 500% of bankroll
    # Phase 1 risk control limits (required by wf_position_checks.py)
    max_single_position_pct: float = 0.02  # Max 2% of capital per position
    max_market_exposure_pct: float = 0.05  # Max 5% of capital per market
    validation_capital_base: float = 1000.0  # Fixed capital base for validation mode
    # Daily loss limit: stop trading if daily loss exceeds this
    daily_loss_limit: float = 10000.0
    # Sports-specific daily loss limit: stop sports trading if sports daily loss exceeds this
    sports_daily_loss_limit: float = 2000.0
    min_confidence: float = 0.55
    scan_interval_secs: float = 30.0
    auto_trade: bool = True
    # Dynamic Kelly: use whale's actual win rate instead of fixed estimate
    use_dynamic_kelly: bool = True
    # Seen position TTL: re-scan positions older than this (seconds)
    seen_position_ttl: float = 3600.0  # 1 hour (was 4h — reduce to allow more frequent re-trades)
    # Max hold time for open positions (hours) — longer than this triggers auto-exit
    max_hold_hours: float = 4.0  # close positions held > 4h (was 24h — 6.2% WR on >1h positions)

    # Asymmetrical SL/TP: TP = TP_MULTIPLIER x SL threshold (winners run longer)
    tp_multiplier: float = 2.5  # TP width = 2.5x SL width

    # Trailing stop - activates after TP threshold is reached
    trailing_stop: bool = True
    trailing_stop_retrace_pct: float = 0.40  # Exit if price retraces 40% from peak gain
    # Max trades per scan cycle (prevents balance exhaustion on restart)
    max_trades_per_scan: int = 5
    # Trade buffer flush interval (seconds)
    trade_buffer_flush_secs: float = 30.0
    # Test mode: inject synthetic signals to exercise pipeline
    test_mode: bool = False
    test_signal_interval_secs: float = 300.0  # 5 min between synthetic signals

    # Backward compat: allow single instrument_id
    @property
    def instrument_id(self) -> InstrumentId:
        return self.instrument_ids[0] if self.instrument_ids else None


class WhaleFollower(Strategy):
    """Follow whale wallets with Kelly-sized positions."""

    def __init__(self, config: WhaleFollowerConfig) -> None:
        super().__init__(config)
        self._tracker: WhaleTracker | None = None
        self._clob = ClobClient(host="https://clob.polymarket.com", chain_id=POLYGON)
        self._dynamic_subscriptions: dict[str, float] = {}  # Maps instrument_id_str → timestamp of subscription
        self._entry_prices: dict[str, float] = {}
        self._last_scan: float = 0
        self._trade_buffer: list[dict] = []
        self._last_trade_flush: float = 0
        self._trade_count: int = 0  # Track received trade ticks
        self._trades_this_scan: int = 0  # Track trades per scan cycle
        self._exit_timer_last: float = 0
        self._exit_timer_interval: float = EXIT_TIMER_INTERVAL_SECS
        self._last_recycle: float = 0
        self._recycle_interval: float = RECYCLE_INTERVAL_SECS
        self._daily_pnl: float = 0.0
        self._daily_pnl_date: str = ""
        self._daily_loss_limit: float = self.config.daily_loss_limit  # Stop trading if daily loss exceeds this
        self._daily_loss_breached: bool = False  # Permanently stops trading until next day
        self._kill_switch_breached: bool = False  # Phase 1: stops trading when position limits breached
        self._sports_daily_pnl: float = 0.0
        self._sports_daily_pnl_date: str = ""
        self._sports_daily_loss_breached: bool = False
        self._pending_whales: dict[str, dict] = {}  # client_order_id -> {whale_name, market_title, category}
        self._last_exit_time: dict[str, float] = {}  # inst_id -> timestamp (re-entry cooldown)
        self._last_resolution_check: dict[str, float] = {}  # inst_id -> timestamp (rate-limit API calls)
        self._open_positions: dict[str, dict] = {}  # str(inst_id) -> {whale_name, market_title, category, side, entry_price, size, entry_time, trade_id, condition_id}
        self._exited_positions: set[str] = set()  # Track exited instrument IDs to prevent duplicate exits
        self._whale_tiering: WhaleTiering | None = None
        self._whale_intel: WhaleIntelligence | None = None
        # Fade tracking
        self._fade_positions: set[str] = set()  # Track active fade positions for concurrency limiting
        self._fade_max_concurrent: int = 3  # Max concurrent fade trades
        self._sybil_price_cache: dict[str, tuple[float, float]] = {}  # condition_id -> (midpoint, timestamp)
        
        # ── Phase 1 Validation Context ──────────────────────────────────────────────
        # Trade correlation tracker for latency/slippage metrics
        self._validation_context: TradeContext | None = None
        self._validation_run_id: str = ""
        self._signal_timestamps: dict[str, int] = {}  # signal_id -> monotonic_ns when whale detected
        if _validation_available:
            try:
                self._validation_context = get_trade_context()
                self._validation_run_id = str(uuid.uuid4())
                self.log.info(f"Validation context initialized (run_id={self._validation_run_id[:8]}...)")
            except Exception as e:
                self.log.warning(f"Failed to initialize validation context: {e}")
                self._validation_context = None

    def on_start(self) -> None:
        self._sports_daily_pnl: float = 0.0
        if not self.config.instrument_ids:
            self.log.error("No instrument_ids configured")
            self.stop()
            return

        # Subscribe to ALL instruments
        for inst_id in self.config.instrument_ids:
            instrument = self.cache.instrument(inst_id)
            if instrument is None:
                self.log.error(f"Could not find instrument: {inst_id}")
                self.stop()
                return
            self.subscribe_quote_ticks(inst_id)
            self.subscribe_trade_ticks(inst_id)
            self.subscribe_order_fills(inst_id)

        # Initialize tracker
        self._tracker = WhaleTracker()

        # Initialize whale tiering
        self._whale_tiering = WhaleTiering()

        # Load whale intelligence data
        self._whale_intel = WhaleIntelligence()
        self.log.info(f"Loaded {len(self._whale_intel._intel)} whale intelligence profiles")

        # Cache all classified whales into dual-axis tier matrix
        cached_count = self._whale_intel.bulk_cache_tiers(self._whale_tiering)
        self.log.info(f"Tier cache: {cached_count} whales assigned to dual-axis capital×precision matrix")

        # Augment blacklist from whale intelligence: should_fade + trust <= 2
        self._intel_blacklist: set[str] = set()
        if self._whale_intel:
            self._intel_blacklist = {
                name for name in self._whale_intel._intel
                if self._whale_intel._intel[name]["should_fade"]
                and self._whale_intel._intel[name]["trust_score"] <= 2
            }
            if self._intel_blacklist:
                self.log.info(f"Intel blacklist: {len(self._intel_blacklist)} whales from intelligence data")

        # Initialize resolution poller for real P&L tracking
        self._resolution_poller = ResolutionPoller()
        self._last_resolution_poll: float = 0
        self._resolution_poll_interval: float = 120.0  # Check resolutions every 2 minutes

        # Initialize insider analyzer
        self._analyzer = WhaleInsiderAnalyzer()

        self._last_analysis: float = 0
        self._analysis_interval: float = 300.0  # 5 minutes between analyses

        self.log.info(
            f"WhaleFollower started | {len(self.config.instrument_ids)} markets | "
            f"Bankroll=${self.config.bankroll:,.0f} | Kelly={self.config.kelly_fraction}x | "
            f"SL={self.config.stop_loss_pct:.0%} | TP={self.config.take_profit_pct:.0%} | "
            f"Min Conf={self.config.min_confidence:.0%} | "
            f"Auto Trade={self.config.auto_trade} | "
            f"Dynamic Kelly={self.config.use_dynamic_kelly} | "
            f"Seen TTL={self.config.seen_position_ttl/3600:.0f}h | "
            f"Tracking {len(self._tracker.whales)} whales | Dynamic subs: ON"
        )

        # Set up independent exit timer (fires regardless of quote ticks)
        # This fixes the design flaw where exit checks depended on quote tick flow
        self._exit_timer_id = "exit_check"
        self.clock.set_timer(
            name=self._exit_timer_id,
            interval=pd.Timedelta(seconds=EXIT_TIMER_INTERVAL_SECS),
            callback=self._on_exit_timer,
        )
        self.log.info("Exit timer registered: every 30s (independent of quote ticks)")

        # Recover orphan positions from DB (crash recovery)
        open_positions_list = recover_open_positions(log_func=self.log.info)
        for pos in open_positions_list:
            inst_key = pos['inst_key']
            if inst_key not in self._open_positions:
                self._open_positions[inst_key] = {k: v for k, v in pos.items() if k != 'inst_key'}

        # Recover open positions from JSON file (restart persistence)
        json_positions = load_open_positions()
        for inst_key, pos_info in json_positions.items():
            if inst_key not in self._open_positions:
                self._open_positions[inst_key] = pos_info
        if json_positions:
            self.log.info(f"[RECOVER] Loaded {len(json_positions)} positions from JSON file")

        # Sync recovered positions to metrics (so /health shows accurate counts)
        try:
            from components.metrics import get_metrics
            metrics = get_metrics()
            metrics.set_open_positions(len(self._open_positions))
            self.log.info(f"[METRICS] Synced open_positions={len(self._open_positions)} to metrics")
        except Exception as e:
            self.log.warning(f"[METRICS] Failed to sync recovered positions: {e}")

        self._last_scan = time.time()

        # Test mode: inject synthetic signals to exercise pipeline
        if self.config.test_mode:
            effective_conf = max(self.config.min_confidence, 0.30)
            self.log.warning(
                f"TEST MODE ACTIVE | Synthetic signals every {self.config.test_signal_interval_secs:.0f}s | "
                f"Effective confidence threshold: {effective_conf:.0%}"
            )
            self._test_signal_count = 0
            self.clock.set_timer(
                name="test_signal_inject",
                interval=pd.Timedelta(seconds=self.config.test_signal_interval_secs),
                callback=self._on_test_signal_timer,
            )

    def on_test_signal_timer(self) -> None:
        """Inject synthetic whale signals to test execution pipeline."""
        self._on_test_signal_timer()

    def _on_test_signal_timer(self, timer_name: str = None) -> None:
        """Generate synthetic whale signal and attempt execution."""
        if not self.config.test_mode:
            self.log.warning("Test signal timer skipped (test_mode=False)")
            return

        self._test_signal_count += 1
        test_instrument = self.config.instrument_ids[self._test_signal_count % len(self.config.instrument_ids)]
        instrument = self.cache.instrument(test_instrument)
        if not instrument:
            self.log.warning("Test signal instrument not found in cache")
            return

        # Synthetic signal: random side, reasonable confidence
        import random
        side = random.choice(["YES", "NO"])
        confidence = random.uniform(0.40, 0.80)
        whale_name = f"TEST-WHALE-{self._test_signal_count % 5 + 1}"
        
        # Extract condition_id and token_id from instrument
        inst_str = str(test_instrument)
        parts = inst_str.split("-")
        cond_id = parts[0] if len(parts) > 0 else "test"
        token_id = parts[1].split(".")[0] if len(parts) > 1 else "test"

        signal = WhaleSignal(
            whale_name=whale_name,
            signal_type=WhaleSignalType.LARGE_POSITION,
            condition_id=cond_id,
            token_id=token_id,
            outcome="YES" if "YES" in inst_str.upper() else "NO",
            side="buy" if side == "YES" else "sell",
            confidence=confidence,
            target_price=0.50,
            suggested_size_usd=random.uniform(500, 2000),
            whale_roi=0.25,
            timestamp=time.time(),
            reason=f"Synthetic test signal #{self._test_signal_count}",
            market_title=inst_str,
        )

        self.log.info(
            f"[TEST #{self._test_signal_count}] Injected: {whale_name} | "
            f"{instrument.id.value} | {side} | conf={confidence:.0%} | size=${signal.suggested_size_usd:.0f}"
        )

        # Process through normal pipeline
        self._on_signal(signal)

    def on_stop(self) -> None:
        self.log.info("WhaleFollower stopped")
        if self._tracker:
            summary = self._tracker.get_whale_summary()
            self.log.info(f"Summary: {summary}")
        if self._trade_buffer:
            self._process_trade_buffer()

    @staticmethod
    def _categorize_instrument(inst_id: str) -> str:
        """Fallback categorizer from instrument ID when signal lacks market_title."""
        if not inst_id:
            return "general"
        parts = inst_id.split("-")
        if len(parts) > 1:
            raw = parts[1].replace(".POLYMARKET", "").replace("_", " ").replace("-", " ")
            # Skip numeric-only strings (condition IDs) — not categorizable
            if raw and raw[0].isdigit() and raw.replace(".", "").replace("_", "").isalnum():
                return "general"
            from strategies.whale_tracker_new import _categorize_market
            result = _categorize_market(raw)
            return result if result != "general" or raw else "general"
        return "general"

    def on_quote_tick(self, tick: QuoteTick) -> None:
        bid = tick.bid_price.as_double()
        ask = tick.ask_price.as_double()
        mid = (bid + ask) / 2
        
        # NOTE: stop-loss & take-profit for ALL positions handled by
        # _check_all_positions() below (runs every 30s).
        # Single-instrument _check_stop_loss / _check_take_profit are deprecated.
        
        # Periodic exit checks (every 30s, independent of quote flow)
        now = time.time()
        if now - self._exit_timer_last >= self._exit_timer_interval:
            check_all_positions(config=self.config, cache=self.cache, log=self.log, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob)
            result = check_daily_loss_limit(config=self.config, log=self.log, daily_pnl=self._daily_pnl, daily_pnl_date=self._daily_pnl_date, daily_loss_breached=self._daily_loss_breached, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob, cache=self.cache)
            self._daily_pnl, self._daily_pnl_date, self._daily_loss_breached = result
            self._exit_timer_last = now

        # DISABLED: Blocks event loop with synchronous HTTP requests -> OOM
        # if now - self._last_analysis >= self._analysis_interval:
        #     self._analyze_insider_patterns()
        #     self._last_analysis = now

    def on_trade_tick(self, tick: TradeTick) -> None:
        size = tick.size.as_double()
        price = tick.price.as_double()
        usd = size * price
        self._trade_count += 1
        
        # Buffer trades >= TRADE_BUFFER_SIZE_THRESHOLD (lowered from $1000 for better responsiveness)
        if usd >= TRADE_BUFFER_SIZE_THRESHOLD:
            self._trade_buffer.append({
                "size": size,
                "price": price,
                "side": tick.aggressor_side.name,
                "timestamp": time.time(),
            })
            # Process buffer every TRADE_BUFFER_FLUSH_COUNT trades (was 10)
            if len(self._trade_buffer) >= TRADE_BUFFER_FLUSH_COUNT:
                self._process_trade_buffer()
        
        # Timer-based flush: process buffer every N seconds even if not full
        now = time.time()
        if now - self._last_trade_flush >= self.config.trade_buffer_flush_secs:
            if self._trade_buffer:
                self.log.info(
                    f"Trade buffer flush: {len(self._trade_buffer)} trades, "
                    f"total received: {self._trade_count}"
                )
                self._process_trade_buffer()
            self._last_trade_flush = now

    def on_order_filled(self, event: OrderFilled) -> None:
        """Log filled orders to the trades database."""
        conn = None
        try:
            import sqlite3
            from pathlib import Path
            
            db_path = Path(__file__).parent.parent / "research" / "trades.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    whale_name TEXT,
                    whale_address TEXT,
                    category TEXT NOT NULL,
                    market_title TEXT,
                    condition_id TEXT,
                    token_id TEXT,
                    side TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    position_size_usd REAL,
                    kelly_fraction REAL,
                    confidence REAL,
                    edge_score REAL,
                    signal_source TEXT,
                    entry_reason TEXT,
                    exit_reason TEXT,
                    realized_pnl REAL,
                    realized_return REAL,
                    duration_seconds REAL,
                    resolution_outcome TEXT,
                    dispute_flag INTEGER DEFAULT 0,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Extract trade details from the fill event
            inst_id = str(event.instrument_id)
            self.log.info(f"[DEBUG] FILL event: client_order_id={event.client_order_id} type={type(event.client_order_id).__name__} pending_keys={list(self._pending_whales.keys())}")

            # Look up whale metadata from pending dict (MUST be before any pending usage)
            pending = self._pending_whales.pop(str(event.client_order_id), {})

            # If no pending metadata, try to recover from _open_positions (restart recovery)
            if not pending:
                inst_key = str(event.instrument_id)
                recovered = self._open_positions.get(inst_key, {})
                if recovered:
                    raw_name = recovered.get("whale_name", "unknown")
                    if not raw_name or raw_name.lower() in ("", "unknown", "unknown whale"):
                        import logging as _lg
                        inst_label = str(event.instrument_id)[:30]
                        _lg.getLogger("whale_follower").warning(
                            f"Recovery: empty whale_name for {inst_label}..., "
                            f"marking as 'unknown'"
                        )
                    pending = {
                        "whale_name": raw_name,
                        "market_title": recovered.get("market_title", ""),
                        "category": recovered.get("category", ""),
                        "whale_address": "",
                        "edge_score": recovered.get("edge_score", 0.0),
                        "confidence": recovered.get("confidence", 0.0),
                        "entry_reason": "recovered_after_restart",
                        "kelly_fraction": self.config.kelly_fraction,
                        "entry_price": recovered.get("entry_price", 0.5),
                        "signal_source": "whale_tracker",
                    }
                    self.log.info(f"[RECOVER] Recovered metadata from _open_positions for {inst_key[:50]}...")

            # Still empty → not an entry fill (e.g. exit order, auto-managed fill) → skip
            if not pending:
                self.log.debug("No pending orders in fill handler, skipping")
                return

            raw_entry = pending.get('entry_price', None)
            if raw_entry is None or (isinstance(raw_entry, (int, float)) and raw_entry == 0.0):
                entry_price = event.last_px.as_double() if hasattr(event, 'last_px') and event.last_px else 0.5
            else:
                entry_price = raw_entry
            self.log.info(f"[DEBUG] PENDING DICT: {pending}")

            qty = event.last_qty.as_double() if hasattr(event, 'last_qty') and event.last_qty else 125
            size_usd = qty * entry_price

            # Use pending already populated above
            whale_name_raw = pending.get("whale_name", "unknown")
            if not whale_name_raw or whale_name_raw.lower() in ("", "unknown", "unknown whale"):
                import logging as _lg
                wallet = pending.get("whale_address", "")
                if wallet:
                    fallback = f"whale_0x{wallet[:6].lower()}"
                    _lg.getLogger("whale_follower").warning(
                        f"Fallback naming: {whale_name_raw!r} -> {fallback} "
                        f"(wallet={wallet[:10]}...)"
                    )
                    whale_name = fallback
                else:
                    _lg.getLogger("whale_follower").warning(
                        f"Empty whale_name with no wallet address for "
                        f"{pending.get('market_title', '?')[:40]}"
                    )
                    whale_name = "unknown"
            else:
                whale_name = whale_name_raw
            market_title = pending.get("market_title", "")
            category = pending.get("category", "") or self._categorize_instrument(inst_id)
            whale_address = pending.get("whale_address", "")
            edge_score = pending.get("edge_score", 0.0) or 0.0
            confidence = pending.get("confidence", 0.0) or 0.0
            entry_reason = pending.get("entry_reason", "")
            kelly_fraction = pending.get("kelly_fraction", 0.0)

            # Fallback: extract market title from instrument ID if not from signal
            if not market_title:
                parts = inst_id.split('-')
                raw_title = parts[1] if len(parts) > 1 else inst_id
                # Don't store numeric condition IDs as titles
                if raw_title and raw_title[0].isdigit():
                    market_title = ""  # leave empty rather than storing numeric ID
                else:
                    market_title = raw_title[:80]

            # Final category fallback — never leave as Unknown or empty
            if not category or category == "Unknown":
                category = "general"

            import uuid
            trade_id = str(uuid.uuid4())
            
            # ── DB OPS: Delegate to wf_db_ops module ──
            result = log_trade_to_db(
                trade_id=trade_id,
                timestamp=str(datetime.now(timezone.utc)),
                whale_name=whale_name,
                whale_address=whale_address,
                market_title=market_title,
                side=event.order_side.name if hasattr(event, 'order_side') else 'BUY',
                entry_price=entry_price,
                position_size_usd=size_usd,
                category=category,
                signal_source=pending.get("signal_source", "whale_tracker"),
                edge_score=edge_score,
                confidence=confidence,
                kelly_fraction=kelly_fraction,
                entry_reason=entry_reason,
                instrument_id=inst_id,
                condition_id=inst_id.split("-")[0] if "-" in inst_id else inst_id,
                log_func=self.log.info,
            )
            if result is None:
                self.log.error(f"[DB] Failed to log trade, skipping position registration")
                return

            conn.close()
            conn = None
            
            self.log.info(f"[DB] Logged trade: {whale_name} | {category} | {market_title[:40]} | ${size_usd:.0f}")
            
            # Register position for tracking
            cond_id = inst_id.split("-")[0] if "-" in inst_id else inst_id
            self._open_positions[str(event.instrument_id)] = {
                "whale_name": whale_name,
                "market_title": market_title,
                "category": category,
                "side": event.order_side.name if hasattr(event, 'order_side') else 'BUY',
                "entry_price": entry_price,
                "size": size_usd,
                "entry_time": time.time(),
                "trade_id": trade_id,
                "condition_id": cond_id,
                "venue_position_id": str(getattr(event, 'venue_position_id', '')),
                "edge_score": edge_score,
                "confidence": confidence,
                "kelly_fraction": kelly_fraction,
            }
            save_open_positions(self._open_positions)

            # ── Update metrics on entry ──────────────────────────────────
            # Use set_open_positions (NOT increment_trade_entered) because position
            # may not yet be in Nautilus cache — metrics must reflect confirmed state
            try:
                from components.metrics import get_metrics
                metrics = get_metrics()
                metrics.set_open_positions(len(self._open_positions))
            except Exception:
                pass

            # ── Phase 1: TRADE_FILLED event + latency/slippage metrics ──────────────────────────────
            # Emit event after fill received, compute latency and slippage
            filled_ts = time.monotonic_ns()
            client_order_id = str(event.client_order_id)
            validation_signal_id = pending.get("_validation_signal_id", "")
            validation_snapshot_id = pending.get("_validation_snapshot_id", "")
            
            if _validation_available and log_event and EventType and validation_signal_id:
                try:
                    # Register fill in trade context
                    if self._validation_context:
                        try:
                            self._validation_context.register_fill(
                                client_order_id=client_order_id,
                                filled_ts=filled_ts,
                                actual_price=float(entry_price),
                                filled_size=float(size_usd),
                            )
                        except Exception as ctx_err:
                            self.log.warning(f"Trade context fill registration failed: {ctx_err}")
                    
                    # Compute latency metrics
                    latencies = {"detection_delay_ms": 0, "execution_delay_ms": 0, "fill_delay_ms": 0, "total_latency_ms": 0}
                    if self._validation_context:
                        try:
                            latencies = self._validation_context.compute_latencies(client_order_id)
                        except Exception:
                            pass  # Graceful fallback to zeros
                    
                    # Compute slippage metrics
                    slippage = {"slippage_bps": 0.0, "fill_completion_pct": 100.0}
                    if self._validation_context:
                        try:
                            slippage = self._validation_context.compute_slippage(client_order_id)
                        except Exception:
                            pass  # Graceful fallback to zeros
                    
                    # Emit TRADE_FILLED event
                    log_event(
                        event_type=EventType.TRADE_FILLED,
                        payload={
                            "signal_id": validation_signal_id,
                            "snapshot_id": validation_snapshot_id,
                            "trade_id": trade_id,
                            "client_order_id": client_order_id,
                            "whale_name": whale_name,
                            "market_title": market_title[:80],
                            "category": category,
                            "side": event.order_side.name if hasattr(event, 'order_side') else 'BUY',
                            "actual_fill_price": float(entry_price),
                            "filled_size_usd": float(size_usd),
                            "quantity": float(qty),
                            "instrument_id": str(event.instrument_id)[:80],
                            "detection_delay_ms": latencies["detection_delay_ms"],
                            "execution_delay_ms": latencies["execution_delay_ms"],
                            "fill_delay_ms": latencies["fill_delay_ms"],
                            "total_latency_ms": latencies["total_latency_ms"],
                            "slippage_bps": slippage["slippage_bps"],
                            "fill_completion_pct": slippage["fill_completion_pct"],
                            "ts_mono_ns": filled_ts,
                        },
                        correlation_id=validation_signal_id,
                        mode=get_current_mode(),
                        strategy_id="whale_follower",
                        run_id=self._validation_run_id,
                    )
                    self.log.debug(
                        f"Validation: TRADE_FILLED {trade_id[:8]}... latency={latencies['total_latency_ms']}ms "
                        f"slippage={slippage['slippage_bps']:.1f}bps"
                    )
                except Exception as e:
                    self.log.warning(f"Validation event emission failed: {e}")

            # Track fade positions for concurrency limiting
            if pending.get("is_fade", False):
                inst_key = str(event.instrument_id)
                self._fade_positions.add(inst_key)
                self.log.info(f"FADE position opened: {whale_name} | {inst_key[:50]}... ({len(self._fade_positions)}/{self._fade_max_concurrent})")

            # ── Price Pump Tracking Hook ────────────────────────────────────────
            # Subscribe this market to price pump monitoring so that price
            # movements after the whale's entry can be tracked.
            try:
                from components.price_tracker import subscribe as _pt_subscribe

                _pt_subscribe(
                    market_id=cond_id,
                    signal_id=trade_id,
                    entry_price=entry_price,
                    whale_address=whale_address,
                    whale_name=whale_name,
                    market_title=market_title,
                )
            except ImportError:
                pass  # price_tracker not available yet
            except Exception as _pt_err:
                self.log.warning("Price tracker hook failed: %s", _pt_err)
        except Exception as e:
            self.log.error("[DB] Failed to log trade", extra={"error": str(e)})
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _scan_whale_positions(self) -> None:
        """Poll known whale positions with rate limiting."""
        if not self._tracker or not self.config.auto_trade or self._daily_loss_breached:
            self.log.warning(
                "Whale scan skipped: tracker=%s auto_trade=%s daily_loss=%s",
                bool(self._tracker), self.config.auto_trade, self._daily_loss_breached
            )
            return
        
        # Reset per-scan trade counter
        self._trades_this_scan = 0
        
        # Clear expired dedup entries (TTL-based re-scan)
        now = time.time()
        ttl = self.config.seen_position_ttl
        if self._tracker.seen_positions:
            expired = [
                k for k, v in self._tracker.seen_positions.items()
                if now - (v if isinstance(v, float) else v.get("timestamp", now)) > ttl
            ]
            if expired:
                for k in expired:
                    del self._tracker.seen_positions[k]
                self.log.info(f"Cleared {len(expired)} expired dedup entries (TTL={ttl/3600:.0f}h)")
        
        try:
            signals = self._tracker.scan_known_whales()
            
            if signals:
                self.log.info(
                    f"Whale scan complete: {len(signals)} new signals detected "
                    f"from {len(self._tracker.whales)} tracked whales"
                )
        
            for signal in signals:
                if self._trades_this_scan >= self.config.max_trades_per_scan:
                    self.log.info(
                        f"Scan trade limit reached ({self.config.max_trades_per_scan}), "
                        f"skipping {len(signals) - self._trades_this_scan} remaining signals"
                    )
                    break
                self._on_signal(signal)
                self._trades_this_scan += 1  # Track trades processed
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.log.error(f"Whale scan error: {e}\n{tb}")

    def _analyze_insider_patterns(self) -> None:
        """Run insider edge analysis on tracked whales."""
        if not self._tracker or not self._analyzer:
            self.log.debug("Insider analysis skipped: no tracker or analyzer")
            return

        try:
            for wallet, whale in self._tracker.whales.items():
                positions = self._tracker._fetch_positions(wallet)
                if not positions:
                    continue

                analysis = self._analyzer.analyze_wallet(
                    wallet=wallet,
                    positions=positions[:10],
                    trades=[],
                    wallet_name=whale.name,
                )

                if analysis:
                    self.log.info(
                        f"INSIDER ANALYSIS: {analysis.wallet_name} | "
                        f"Edge Score: {analysis.edge_score:.2f} | "
                        f"Type: {analysis.edge_type} | "
                        f"Action: {analysis.suggested_action}"
                    )

                    # If high priority, add to tracking with adjusted confidence
                    if analysis.suggested_action == "high_priority":
                        self.log.info(
                            f"  Reasoning: {analysis.reasoning[:100]}..."
                        )
                        # Could add new whales here if discovered
        except Exception as e:
            self.log.error(f"Insider analysis error: {e}")

    def _process_trade_buffer(self) -> None:
        """Process buffered large trades."""
        if not self._tracker or not self._trade_buffer:
            self.log.debug("Trade buffer processing skipped: no tracker or buffer empty")
            return

        try:
            signals = self._tracker.detect_large_trades(self._trade_buffer)
            self._trade_buffer.clear()
            for signal in signals:
                # ── Phase 1: WHALE_TRADE_DETECTED event ──────────────────────────────────────
                # Emit event when whale action is detected from trade buffer
                signal_id = str(uuid.uuid4())
                whale_trade_ts = time.monotonic_ns()
                self._signal_timestamps[signal_id] = whale_trade_ts
                
                if _validation_available and log_event and EventType:
                    try:
                        log_event(
                            event_type=EventType.WHALE_TRADE_DETECTED,
                            payload={
                                "signal_id": signal_id,
                                "whale_name": signal.whale_name,
                                "whale_address": getattr(signal, 'whale_address', ''),
                                "market_title": getattr(signal, 'market_title', '')[:80],
                                "market_category": getattr(signal, 'market_category', ''),
                                "side": signal.side,
                                "target_price": float(getattr(signal, 'target_price', 0.5)),
                                "suggested_size_usd": float(getattr(signal, 'suggested_size_usd', 0)),
                                "confidence": float(getattr(signal, 'confidence', 0)),
                                "edge_score": float(getattr(signal, 'edge_score', 0)),
                                "condition_id": signal.condition_id[:50],
                                "signal_source": signal.source.value if hasattr(signal.source, 'value') else str(signal.source),
                                "ts_mono_ns": whale_trade_ts,
                            },
                            correlation_id=signal_id,
                            mode=get_current_mode(),
                            strategy_id="whale_follower",
                            run_id=self._validation_run_id,
                        )
                        self.log.debug(f"Validation: WHALE_TRADE_DETECTED {signal_id[:8]}... ({signal.whale_name})")
                    except Exception as e:
                        self.log.warning(f"Validation event emission failed: {e}")
                
                # Pass signal_id to _on_signal for correlation
                signal._validation_signal_id = signal_id
                self._on_signal(signal)
        except Exception as e:
            self.log.error(f"Trade processing error: {e}")

    def _llm_score_signal(self, signal: WhaleSignal) -> int:
        """Score a whale signal using MiniMax cloud LLM.
        
        Circuit breaker: whale_api (protects MiniMax API calls from cascade failures).
        """
        import urllib.request
        import urllib.error
        import re
        from strategies.wf_circuit_breaker import get_whale_api_breaker, CircuitBreakerOpen
        market = getattr(signal, "market_title", "") or ""
        whale = signal.whale_name or "unknown"
        side = getattr(signal, "side", "?") or "?"
        price = getattr(signal, "target_price", 0.5) or 0.5
        category = getattr(signal, "market_category", "") or ""
        prompt = (
            f"Score this Polymarket signal 1-10. "
            f"Market: {market[:80]}. Whale: {whale[:30]}. "
            f"Side: {side} at {price:.3f}. Category: {category}."
        )
        if self._whale_intel:
            intel = self._whale_intel.get(signal.whale_name)
            if intel:
                prompt += f" Classification: {intel['classification']}, Trust: {intel['trust_score']}/10."
        if whale in ("unknown", "unknown whale", ""):
            prompt += " Unknown whale, be skeptical."
        system_prompt = "You are a scoring bot. Reply ONLY with a single digit 1-10. Nothing else."
        payload = {
            "model": "MiniMax-M2.7-highspeed",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 500,
            "temperature": 0.01
        }
        def _make_llm_request():
            req = urllib.request.Request(
                "https://api.minimaxi.com/v1/chat/completions",
                data=json.dumps(payload).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer sk-cp-...ATCc"
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                raw = data["choices"][0]["message"]["content"]
                # Extract score after last </think> (MiniMax thinking tags)
                last_close = raw.rfind("\n\n</Close>\n")
                if last_close != -1:
                    text = raw[last_close+6:].strip()
                else:
                    text = raw.strip()
                nums = re.findall(r'\d+', text)
                score = int(nums[0]) if nums else 5
                return max(1, min(10, score))

        try:
            breaker = get_whale_api_breaker()
            score = breaker.call(_make_llm_request)
            return score
        except CircuitBreakerOpen:
            self.log.warning("Whale API circuit breaker OPEN — skipping LLM scoring")
            return 5
        except Exception as e:
            self.log.warning(f"LLM score failed: {e}")
            return 5

    def _on_signal(self, signal: WhaleSignal) -> None:
        """Handle a whale signal from ANY subscribed market."""
        self.log.info(f"[DEBUG] _on_signal called for {signal.condition_id[:20]}... cond={signal.confidence:.2f}")

        # ── Whale Tiering Integration ────────────────────────────────────
        alpha_score = getattr(signal, 'alpha_score', 50.0) or 50.0
        whale_tags = getattr(signal, 'tags', '[]')
        try:
            tags_list = json.loads(whale_tags) if isinstance(whale_tags, str) else (whale_tags or [])
        except (json.JSONDecodeError, TypeError):
            tags_list = []

        # Prefer dual-axis tier cache; fall back to alpha-score single-axis
        dual_config = self._whale_tiering.get_cached_tier(signal.whale_name) if self._whale_tiering else {}
        if dual_config.get("max_position_usd", 0) > 0 and dual_config["max_position_usd"] != 100:
            cached = self._whale_tiering.get_raw_cache(signal.whale_name)
            cap = cached.get("capital_tier", "?") if cached else "?"
            prec = cached.get("precision_tier", "?") if cached else "?"
            tier = f"{cap}+{prec}"
            tier_config = dual_config
        else:
            tier = self._whale_tiering.get_tier(alpha_score) if self._whale_tiering else "unknown"
            tier_config = self._whale_tiering.get_tier_config(alpha_score) if self._whale_tiering else {}

        # Apply tier confidence threshold (overrides base config)
        if self._whale_tiering and not self._whale_tiering.validate_confidence(signal.confidence, alpha_score, tags_list):
            min_conf = tier_config.get("min_confidence", self.config.min_confidence)
            self.log.info(
                f"Signal below tier confidence threshold ({tier}): {signal.whale_name} "
                f"(conf {signal.confidence:.0%} < {min_conf:.0%})"
            )
            return

        # Apply tier edge_score threshold
        edge_val = getattr(signal, 'edge_score', 0.0) or 0.0
        if self._whale_tiering and not self._whale_tiering.validate_edge_score(edge_val, alpha_score):
            min_edge = tier_config.get("min_edge_score", 0.15)
            self.log.info(
                f"Signal below tier edge_score threshold ({tier}): {signal.whale_name} "
                f"(edge {edge_val:.2f} < {min_edge:.2f})"
            )
            return

        # REJECT: intelligence-flagged sacrificial accounts
        if self._whale_intel and self._whale_intel.should_hard_reject(signal.whale_name):
            intel = self._whale_intel.get(signal.whale_name)
            self.log.info(
                f"REJECT intelligence-flagged sacrificial account: {signal.whale_name} "
                f"(trust={intel['trust_score']}/10)"
            )
            return

        # CHECK: blacklisted whales - allow fade engine to process if eligible
        if signal.whale_name in WHALE_BLACKLIST:
            # Check if fade engine wants to fade this blacklisted whale
            if self._whale_intel and self._whale_intel.should_fade(signal.whale_name):
                self.log.info(f"BLACKLISTED whale eligible for FADE: {signal.whale_name}")
                # Continue to let fade detection at line 1024 handle it
            else:
                self.log.info(f"REJECT blacklisted whale: {signal.whale_name}")
                return
        mc = getattr(signal, "market_category", "") or ""
        if signal.whale_name in SPORTS_WHALE_BLACKLIST and mc.lower() == "sports":
            # Check if fade engine wants to fade this sports-blacklisted whale
            if self._whale_intel and self._whale_intel.should_fade(signal.whale_name):
                self.log.info(f"SPORTS-BLACKLISTED whale eligible for FADE: {signal.whale_name}")
                # Continue to let fade detection at line 1024 handle it
            else:
                self.log.info(f"REJECT sports-blacklisted whale: {signal.whale_name}")
                return

        # REJECT: unknown whale signals with zero edge score (noise trades)
        # Historical data shows 518 such trades lost -$2,532 total.
        if edge_val == 0.0 and (not signal.whale_name or signal.whale_name.lower() in ("unknown", "unknown whale", "")):
            wallet = getattr(signal, 'whale_address', '') or ''
            wallet_info = f" wallet={wallet[:10]}..." if wallet else ""
            self.log.info(
                f"REJECT unknown whale zero edge: {signal.whale_name}{wallet_info} | "
                f'market={getattr(signal, "market_title", "")[:40]} | '
                f"conf={signal.confidence:.0%}"
            )
            return

        # REJECT: non-whitelisted sports markets (whitelist Spread bets only)
        mc = getattr(signal, 'market_category', '') or ''
        if mc.lower() == 'sports':
            title = getattr(signal, 'market_title', '') or ''
            if any(re.search(p, title, re.IGNORECASE) for p in SPORTS_WHITELIST_PATTERNS):
                self.log.info(f"ALLOW Spread: {title[:60]}")
            elif any(re.search(p, title, re.IGNORECASE) for p in SPORTS_OU_BLACKLIST_PATTERNS):
                self.log.info(f"REJECT O/U: {title[:60]}")
                return
            elif any(re.search(p, title, re.IGNORECASE) for p in SPORTS_VS_BLACKLIST_PATTERNS):
                self.log.info(f"REJECT vs: {title[:60]}")
                return
            elif any(re.search(p, title, re.IGNORECASE) for p in SINGLE_TEAM_PATTERNS):
                self.log.info(f"REJECT single-team: {title[:60]}")
                return

        # Apply tier-based position sizing
        if self._whale_tiering:
            tier_kelly = self._whale_tiering.apply_overrides(
                tier_config, tags_list
            ).get("kelly_multiplier", 1.0)
            signal.suggested_size_usd = round(signal.suggested_size_usd * tier_kelly, 2)

        # Whale intelligence Kelly adjustment
        if self._whale_intel:
            original_size = signal.suggested_size_usd
            new_size, intel = self._whale_intel.adjust_size(signal.whale_name, signal.suggested_size_usd)
            if intel:
                signal.suggested_size_usd = new_size
                self.log.info(
                    f"Intel Kelly adjustment: {intel['classification']} "
                    f"x{self._whale_intel.kelly_multiplier(intel['classification'])} "
                    f"(${original_size:.0f} -> ${new_size:.0f})"
                )

        # LLM signal quality scoring (1700 Qwen3.5-9B, ~0.3s)
        llm_score = self._llm_score_signal(signal)
        if llm_score < 4:
            self.log.info(f"REJECT LLM score={llm_score}/10: {signal.whale_name}")
            return
        self.log.info(f"LLM score={llm_score}/10: {signal.whale_name} | market={getattr(signal, 'market_title', '')[:40]}")

        # Log signal with tier info
        self.log.info(
            f"SIGNAL [{signal.source.value}] [{tier.upper()}]: {signal.reason} | "
            f"Confidence: {signal.confidence:.0%} | "
            f"Suggested: ${signal.suggested_size_usd:,.0f}",
            color=LogColor.YELLOW if signal.source == SignalSource.KNOWN_WHALE else LogColor.CYAN,
        )

        if not self.config.auto_trade:
            self.log.debug("Auto-trade disabled, skipping signal execution")
            return
        if self._daily_loss_breached:
            self.log.warning(
                "Daily loss limit breached ($%.2f), skipping signal execution",
                self._daily_pnl
            )
            return

        # Sports daily loss breach check
        mc = getattr(signal, 'market_category', '') or ''
        if mc.lower() == 'sports' and self._sports_daily_loss_breached:
            self.log.warning(
                "Sports daily loss limit breached, skipping sports signal execution"
            )
            return

        # Sports-specific daily loss limit
        if mc.lower() == 'sports' and hasattr(self, '_sports_daily_pnl'):
            if self._sports_daily_pnl <= -SPORTS_DAILY_LOSS_LIMIT:
                self.log.info(f"Sports daily loss limit breached (-${SPORTS_DAILY_LOSS_LIMIT}), skipping sports signal")
                return

        # ── FADE DETECTION: Should this signal be inverted? ──
        is_fade = False
        if self._whale_intel:
            fade_intel = self._whale_intel.should_fade(signal.whale_name)
            if fade_intel:
                # Check fade concurrency limit
                if len(self._fade_positions) >= self._fade_max_concurrent:
                    self.log.info(f"FADE concurrency limit reached ({len(self._fade_positions)}/{self._fade_max_concurrent}), skipping: {signal.whale_name}")
                    return
                # Invert the signal: flip YES↔NO, buy↔sell
                original_side = signal.side
                original_outcome = signal.outcome
                signal.side = "sell" if signal.side == "buy" else "buy"
                signal.outcome = "NO" if signal.outcome == "YES" else "YES"
                # Reduce size by 0.5x fade multiplier
                original_size = signal.suggested_size_usd
                signal.suggested_size_usd = round(original_size * 0.5, 2)
                is_fade = True
                self.log.info(
                    f"FADE mode: {signal.whale_name} ({fade_intel['classification']}, trust={fade_intel['trust_score']}/10) "
                    f"inverted {original_side}→{signal.side}, size ${original_size:.0f}→${signal.suggested_size_usd:.0f}"
                )

        # Dynamic subscription: every signal is processed regardless of pre-subscribed markets.
        # The sandbox execution client has been patched to auto-fill any instrument,
        # bypassing the exchange's matching engine limitation.
        target_inst = self._ensure_instrument_for_signal(
            signal.condition_id, signal.token_id, signal.outcome
        )
        if target_inst is None:
            self.log.warning(f"Could not get instrument for {signal.market_title[:40]} | condition_id={signal.condition_id}, skipping")
            return

        # Determine side
        side = OrderSide.BUY if signal.side == "buy" else OrderSide.SELL

        # Get whale's actual win rate for dynamic Kelly sizing
        whale_wr = None
        if self.config.use_dynamic_kelly and self._tracker:
            # Look up whale by name in whales dict (has WhaleIdentity objects)
            for w in self._tracker.whales.values():
                if w.name == signal.whale_name:
                    whale_wr = w.win_rate
                    break
            if whale_wr is None:
                self.log.debug(f"Whale '{signal.whale_name}' not found in tracker, using default Kelly")

        # ── Phase 1: SIGNAL_GENERATED event + snapshot freeze ──────────────────────────────────────
        # Emit event and freeze decision inputs AFTER validation passes, BEFORE any future data leaks in
        signal_generated_ts = time.monotonic_ns()
        snapshot_id = ""
        validation_signal_id = getattr(signal, '_validation_signal_id', str(uuid.uuid4()))
        
        if _validation_available and log_event and EventType:
            try:
                # Freeze snapshot BEFORE order submission (critical for replay validation)
                if freeze_snapshot:
                    try:
                        # Gather market state from orderbook if available
                        market_state = {
                            "price": float(signal.target_price),
                            "side": signal.side,
                            "confidence": float(signal.confidence),
                            "edge_score": float(getattr(signal, 'edge_score', 0)),
                        }
                        # Minimal orderbook snapshot (top level only)
                        orderbook = {"bid": float(signal.target_price), "ask": float(signal.target_price)}
                        whale_metrics = {
                            "whale_name": signal.whale_name,
                            "suggested_size_usd": float(signal.suggested_size_usd),
                            "classification": getattr(signal, 'classification', 'unknown'),
                        }
                        
                        snapshot = freeze_snapshot(
                            signal_id=validation_signal_id,
                            market_state=market_state,
                            orderbook=orderbook,
                            whale_metrics=whale_metrics,
                            classification=getattr(signal, 'classification', 'unknown'),
                            confidence=float(signal.confidence),
                            market_regime="neutral",
                            strategy_version="v1.0",
                        )
                        snapshot_id = snapshot.snapshot_id
                        self.log.debug(f"Validation: Snapshot frozen {snapshot_id[:8]}...")
                    except Exception as snap_err:
                        self.log.warning(f"Snapshot freeze failed: {snap_err}")
                
                # Register signal in trade context for correlation
                whale_trade_ts = self._signal_timestamps.get(validation_signal_id, signal_generated_ts)
                if self._validation_context:
                    try:
                        self._validation_context.register_signal(
                            signal_id=validation_signal_id,
                            whale_trade_ts=whale_trade_ts,
                            signal_detected_ts=signal_generated_ts,  # Use signal_generated as proxy
                            signal_generated_ts=signal_generated_ts,
                            snapshot_id=snapshot_id,
                            side=signal.side.upper(),
                        )
                    except Exception as ctx_err:
                        self.log.warning(f"Trade context registration failed: {ctx_err}")
                
                # Emit SIGNAL_GENERATED event
                log_event(
                    event_type=EventType.SIGNAL_GENERATED,
                    payload={
                        "signal_id": validation_signal_id,
                        "snapshot_id": snapshot_id,
                        "whale_name": signal.whale_name,
                        "market_title": getattr(signal, 'market_title', '')[:80],
                        "market_category": getattr(signal, 'market_category', ''),
                        "side": signal.side,
                        "target_price": float(signal.target_price),
                        "suggested_size_usd": float(signal.suggested_size_usd),
                        "confidence": float(signal.confidence),
                        "edge_score": float(getattr(signal, 'edge_score', 0)),
                        "llm_score": llm_score,
                        "tier": tier,
                        "is_fade": is_fade,
                        "whale_win_rate": float(whale_wr or 0.55),
                        "ts_mono_ns": signal_generated_ts,
                    },
                    correlation_id=validation_signal_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=self._validation_run_id,
                )
                self.log.debug(f"Validation: SIGNAL_GENERATED {validation_signal_id[:8]}... snapshot={snapshot_id[:8] if snapshot_id else 'none'}")
            except Exception as e:
                self.log.warning(f"Validation event emission failed: {e}")
        
        # Pass signal_id and snapshot_id to enter_position for correlation
        signal._validation_signal_id = validation_signal_id
        signal._validation_snapshot_id = snapshot_id

        self.enter_position(
            side, signal.target_price, signal.suggested_size_usd,
            instrument_id=target_inst, whale_win_rate=whale_wr,
            whale_name=signal.whale_name,
            market_title=signal.market_title,
            market_category=getattr(signal, 'market_category', 'Unknown'),
            whale_address=getattr(signal, 'whale_address', '') or '',
            edge_score=getattr(signal, 'edge_score', 0.0) or 0.0,
            confidence=signal.confidence or 0.0,
            entry_reason=signal.reason or "",
            is_fade=is_fade,
            _validation_signal_id=validation_signal_id,
            _validation_snapshot_id=snapshot_id,
        )

    def _find_instrument(self, condition_id: str) -> InstrumentId | None:
        """Find the subscribed instrument matching a condition_id."""
        for inst_id in self.config.instrument_ids:
            if str(inst_id).split("-")[0] == condition_id:
                return inst_id
        return None

    def _ensure_instrument_for_signal(self, condition_id: str, token_id: str, outcome: str) -> InstrumentId | None:
        """Fetch market metadata and create instrument for a signal's market."""
        inst_id = InstrumentId.from_str(f"{condition_id}-{token_id}.POLYMARKET")
        existing = self.cache.instrument(inst_id)
        if existing is not None:
            return inst_id
        try:
            market_info = self._clob.get_market(condition_id=condition_id)
            if not market_info or not market_info.get("active", False):
                self.log.info(f"Signal market inactive: {condition_id[:20]}...")
                return None
            tokens = market_info.get("tokens", [])
            token_data = None
            for t in tokens:
                if t.get("token_id") == token_id:
                    token_data = t
                    break
            if not token_data:
                return None
            instrument = parse_polymarket_instrument(
                market_info=market_info,
                token_id=token_data["token_id"],
                outcome=token_data["outcome"],
            )
            self.cache.add_instrument(instrument)
            # Subscribe to quote ticks so dynamic instruments are checked by
            # _check_all_positions() Phase 2 stop-loss/take-profit/resolution logic
            self.subscribe_quote_ticks(inst_id)
            self.log.info(f"Registered dynamic instrument: {instrument.id.value[:50]} ...")
            return inst_id
        except Exception as e:
            self.log.error(f"Failed to register instrument for {condition_id[:20]}...: {e}")
            return None

    def _current_gross_exposure(self) -> float:
        """Calculate total notional exposure of all open positions as max loss amount."""
        total = 0.0
        for inst_id in self.config.instrument_ids:
            positions = self.cache.positions_open(instrument_id=inst_id)
            if positions:
                for pos in positions:
                    # For BinaryOption instruments: max loss = cost basis = quantity * entry_price
                    qty = pos.quantity.as_double() if hasattr(pos.quantity, 'as_double') else float(pos.quantity)
                    avg_open = pos.avg_px_open.as_double() if hasattr(pos.avg_px_open, 'as_double') else 0.0
                    total += qty * avg_open
        return total

    def enter_position(
        self, side: OrderSide, price: float, whale_amount: float = 0,
        instrument_id: InstrumentId = None, whale_win_rate: float | None = None,
        whale_name: str = None, market_title: str = "", market_category: str = "",
        whale_address: str = "", edge_score: float = 0.0, confidence: float = 0.0,
        entry_reason: str = "", is_fade: bool = False,
        _validation_signal_id: str = "", _validation_snapshot_id: str = "",
    ) -> None:
        """Enter Kelly-sized position."""
        # ── Phase 1 Risk Control: Kill Switch Check ─────────────────────────────
        if self._kill_switch_breached:
            self.log.warning(f"KILL_SWITCH active - rejecting signal for {market_title[:40]}")
            return

        inst_id = instrument_id or self.config.instrument_id
        
        # Whale name will be stored after order creation (using order.client_order_id)
        
        instrument = self.cache.instrument(inst_id)
        if instrument is None:
            return

        # ── Minimum price filter — reject near-zero EV long shots ─────────────────
        if price < MIN_ENTRY_PRICE:
            self.log.info(
                f"MIN_PRICE_REJECTED | {market_title[:50]} | "
                f"price=${price:.4f} < ${MIN_ENTRY_PRICE} | whale={whale_name}"
            )
            return

        # ── Confidence filter — reject low-confidence signals ────────────────
        if confidence < 0.15:
            self.log.info(
                f"REJECT confidence={confidence:.2f} < 0.15 | {inst_id}"
            )
            return

        # Check existing position via cache (pre-subscribed instruments)
        open_positions = self.cache.positions_open(instrument_id=inst_id)
        if open_positions and open_positions[0].quantity.as_double() != 0:
            self.log.info(f"Already have position in {inst_id}, skipping")
            return
        
        # Dedup check against our own position registry (covers dynamic instruments)
        inst_key = str(inst_id)
        if inst_key in self._open_positions:
            existing = self._open_positions[inst_key]
            self.log.info(
                f"Position already tracked: {existing['whale_name']} | "
                f"{inst_key[:50]}... | held {time.time()-existing['entry_time']:.0f}s, skipping"
            )
            return

        # Re-entry cooldown — don't re-enter same instrument within 5 minutes of exit
        last_exit = self._last_exit_time.get(str(inst_id), 0)
        if time.time() - last_exit < RE_ENTRY_COOLDOWN_SECS:
            self.log.info(f"Re-entry cooldown for {inst_id}: {time.time() - last_exit:.0f}s < {RE_ENTRY_COOLDOWN_SECS}s, skipping")
            return

        # Hard balance guard — check available USDC.e funds before sizing
        USDC_e = Currency.from_str("USDC.e")
        if instrument.venue:
            account = self.portfolio.account(instrument.venue)
        else:
            account = self.portfolio.account()
        if account is None:
            self.log.warning("Cash account not found – skipping order")
            return
        available = account.balance_free(USDC_e).as_double()

        # Kelly sizing with dynamic whale win rate (edge_score calibrated)
        # Pass available_balance so effective bankroll = min(config, available)
        # This prevents AccountBalanceNegative by auto-shrinking position sizes
        size_usd = self._kelly_size(price, whale_win_rate=whale_win_rate, edge_score=edge_score, available_balance=available, market_category=market_category)
        if size_usd <= 0:
            wr_note = f" (whale_wr={whale_win_rate:.0%})" if whale_win_rate else " (fixed_wr=55%)"
            self.log.info(f"No Kelly edge{wr_note}, skipping")
            return

        # Liquidity-based size adjustment (Track A)
        size_usd = self._adjust_size_for_liquidity(size_usd, inst_id)

        # ── HARD CAP: enforce max_single_position_pct AFTER liquidity adjustment
        # Must use validation_capital_base (like check_position_limits does) to ensure
        # the cap matches the actual limit. Using bankroll would give a $200 cap
        # while the limit is $20 (2% of $1000 validation capital).
        max_single_pct = getattr(self.config, "max_single_position_pct", 0.02)
        capital = (
            self.config.validation_capital_base
            if self.config.validation_capital_base > 0
            else self.config.bankroll
        )
        hard_cap = capital * max_single_pct
        if size_usd > hard_cap:
            size_usd = hard_cap

        # Brief guard: if even the computed size exceeds available, skip
        if size_usd > available:
            self.log.info(
                f"Size ${size_usd:,.2f} exceeds available ${available:,.2f}, skipping"
            )
            return

        # ── Phase 1 Risk Control: Position/Exposure Limits ───────────────────────
        # Check MAX_SINGLE_POSITION, MAX_TOTAL_EXPOSURE, MAX_MARKET_EXPOSURE
        allowed, reason = check_position_limits(
            config=self.config,
            cache=self.cache,
            instrument_id=inst_id,
            proposed_size_usd=size_usd,
            open_positions=self._open_positions,
            log=self.log,
            run_id=self._validation_run_id,
            mode=get_current_mode(),
        )
        if not allowed:
            # Position limits breached - trigger kill switch
            trigger_kill_switch(
                config=self.config,
                cache=self.cache,
                log=self.log,
                reason=reason,
                run_id=self._validation_run_id,
                mode=get_current_mode(),
                strategy_id="whale_follower",
                cancel_orders_func=self.cancel_all_open_orders,
            )
            self._kill_switch_breached = True
            return

        # Max open positions check
        open_count = len(self._open_positions)
        max_positions = self.config.max_open_positions
        if open_count >= max_positions:
            self.log.info(
                f"Max positions reached ({open_count}/{max_positions}), skipping"
            )
            return
        # Low‑cash alert: warn if free balance drops below 20 % of bankroll
        if available < LOW_CASH_ALERT_PCT * self.config.bankroll:
            self.log.warning(
                f"Low cash alert: free USDC.e ${available:,.2f} < {LOW_CASH_ALERT_PCT:.0%} of bankroll (${self.config.bankroll:,.2f})"
            )

        qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)
        if qty.as_decimal() <= 0:
            self.log.debug("Calculated quantity is zero, skipping order entry")
            return

        order = self.order_factory.market(
            instrument_id=inst_id,
            order_side=side,
            quantity=qty,
            time_in_force=TimeInForce.GTC,
        )

        # Store whale metadata keyed by the unique client_order_id for later lookup
        if whale_name:
            pass
        else:
            import logging as _lg
            _lg.getLogger("whale_follower").warning(
                f"enter_position called with empty whale_name for {market_title[:40]} "
                f"(inst={inst_id[:50]}...) - trade will be stored as 'unknown'"
            )
        if whale_name:
            self._pending_whales[str(order.client_order_id)] = {
                "whale_name": whale_name,
                "market_title": market_title,
                "category": market_category,
                "whale_address": whale_address,
                "edge_score": edge_score,
                "confidence": confidence,
                "entry_reason": entry_reason,
                "kelly_fraction": self.config.kelly_fraction,
                "entry_price": price,
                "is_fade": is_fade,
                "_validation_signal_id": _validation_signal_id,
                "_validation_snapshot_id": _validation_snapshot_id,
            }
        # Register intended price for PaperExecClient to use at fill time
        from components.paper_execution import set_fill_price
        set_fill_price(str(inst_id), price)
        whale_note = f" (following ${whale_amount:,.0f} whale)" if whale_amount else ""
        self.log.info(
            f"ENTER {side.name}: {qty.as_decimal():.0f} shares @ {price:.4f} "
            f"= ${size_usd:,.2f}{whale_note} | {inst_id}"
        )
        self.submit_order(order)
        
        # ── Phase 1: TRADE_SUBMITTED event ──────────────────────────────────────
        # Emit event after order submission for latency tracking
        submitted_ts = time.monotonic_ns()
        
        if _validation_available and log_event and EventType and _validation_signal_id:
            try:
                # Register submission in trade context
                if self._validation_context:
                    try:
                        self._validation_context.register_submission(
                            client_order_id=str(order.client_order_id),
                            signal_id=_validation_signal_id,
                            submitted_ts=submitted_ts,
                            intended_price=float(price),
                            intended_size=float(size_usd),
                        )
                    except Exception as ctx_err:
                        self.log.warning(f"Trade context submission registration failed: {ctx_err}")
                
                # Emit TRADE_SUBMITTED event
                log_event(
                    event_type=EventType.TRADE_SUBMITTED,
                    payload={
                        "signal_id": _validation_signal_id,
                        "snapshot_id": _validation_snapshot_id,
                        "client_order_id": str(order.client_order_id),
                        "whale_name": whale_name,
                        "market_title": market_title[:80],
                        "side": side.name,
                        "intended_price": float(price),
                        "intended_size_usd": float(size_usd),
                        "quantity": float(qty.as_decimal()),
                        "instrument_id": str(inst_id)[:80],
                        "ts_mono_ns": submitted_ts,
                    },
                    correlation_id=_validation_signal_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=self._validation_run_id,
                )
                self.log.debug(f"Validation: TRADE_SUBMITTED {str(order.client_order_id)[:12]}... signal={_validation_signal_id[:8]}")
            except Exception as e:
                self.log.warning(f"Validation event emission failed: {e}")
        
        self._trades_this_scan += 1

    def _fetch_real_midpoint(self, inst_key: str) -> float | None:
        """Fetch the real market midpoint price from Polymarket CLOB API.
        Returns the midpoint price or None if API fails.
        """
        from strategies.wf_market_data import fetch_real_midpoint
        return fetch_real_midpoint(inst_key)

    def _resolve_exit_price(self, pos_info: dict) -> float:
        """Determine exit price using REAL market data (no random walk).

        Priority:
        1. Market resolved -> resolution price ($1.00 if won, $0.00 if lost)
        2. CLOB API midpoint -> actual trading price
        3. Fallback: deterministic estimate (edge-based drift, no Gaussian noise)
        """
        from strategies.wf_market_data import resolve_exit_price
        from components.resolution_poller import get_market_resolution, calculate_actual_pnl
        return resolve_exit_price(
            pos_info=pos_info,
            instrument_id_str=pos_info.get("inst_key", ""),
            get_market_resolution=get_market_resolution,
            calculate_actual_pnl=calculate_actual_pnl,
            log_func=self.log.info,
        )

    def exit_position(self, instrument_id: InstrumentId = None, exit_reason: str = "manual") -> None:
        """Close current position with P&L tracking and DB update."""
        import sqlite3, uuid
        from pathlib import Path
        from datetime import datetime, timezone
        
        inst_id = instrument_id or self.config.instrument_id
        inst_key = str(inst_id)
        
        # ── POSITION CACHE DEDUP: Skip if already exited this position ──
        if inst_key in self._exited_positions:
            self.log.debug(f"Position already exited, skipping: {inst_key[:50]}...")
            return
        
        open_positions = self.cache.positions_open(instrument_id=inst_id)
        if not open_positions or open_positions[0].quantity.as_double() == 0:
            return
        pos = open_positions[0]
        qty = pos.quantity.as_double()
        
        # Look up position info from our registry
        pos_info = self._open_positions.pop(inst_key, {})
        pos_info["inst_key"] = inst_key
        save_open_positions(self._open_positions)
        
        # Simulate exit price
        entry_price = pos_info.get("entry_price", 0.50)
        entry_time = pos_info.get("entry_time", time.time())
        duration = time.time() - entry_time
        exit_price = self._resolve_exit_price(pos_info)
        
        # Calculate P&L
        side = pos_info.get("side", "BUY")
        if side == "BUY":
            realized_pnl = qty * (exit_price - entry_price)
        else:
            realized_pnl = qty * (entry_price - exit_price)  # SELL = short
        realized_return = (exit_price - entry_price) / entry_price if side == "BUY" else (entry_price - exit_price) / entry_price
        
        # Sanity cap: P&L return exceeding ±200% is almost certainly a sandbox pricing artifact
        # (e.g., entry fills at $0.005 on a $0.50 market → 5,000%+ returns)
        # Cap at ±200% and log warning so dashboard metrics stay realistic
        if abs(realized_return) > MAX_SANE_RETURN:
            self.log.warning(
                f"[SANITY CAP] {inst_key[:50]}... return={realized_return:+.2%} exceeds ±{MAX_SANE_RETURN:.0%} — "
                f"capping from ${realized_pnl:+.2f} to capped value. "
                f"entry=${entry_price:.4f} exit=${exit_price:.4f} qty={qty:.0f} side={side}"
            )
            # Scale P&L to return ±200% while preserving direction
            # For both BUY and SELL: capped_pnl = qty * entry * MAX_SANE_RETURN (directional)
            realized_pnl = qty * entry_price * MAX_SANE_RETURN * (1 if realized_pnl >= 0 else -1)
            realized_return = MAX_SANE_RETURN if realized_pnl >= 0 else -MAX_SANE_RETURN
            self.log.info(f"[SANITY CAP] Capped P&L: ${realized_pnl:+.2f} ({realized_return:+.2%})")
        
        # Update DB row with exit details
        trade_id = pos_info.get("trade_id", "")
        if trade_id:
            try:
                db_path = Path(__file__).parent.parent / "research" / "trades.db"
                conn = sqlite3.connect(str(db_path))
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute("""
                    UPDATE trades SET
                        exit_price = ?,
                        realized_pnl = ?,
                        realized_return = ?,
                        exit_reason = ?,
                        duration_seconds = ?
                    WHERE trade_id = ?
                """, (exit_price, realized_pnl, realized_return, exit_reason, duration, trade_id))
                conn.commit()
                try:
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
                conn.close()
            except Exception as e:
                self.log.error(f"[DB] Failed to update exit P&L: {e}")
        
        # ── Phase 1: TRADE_CLOSED event ──────────────────────────────────────
        # Emit event after position exit for complete trade lifecycle tracking
        closed_ts = time.monotonic_ns()
        
        if _validation_available and log_event and EventType and trade_id:
            try:
                # Emit TRADE_CLOSED event
                log_event(
                    event_type=EventType.TRADE_CLOSED,
                    payload={
                        "trade_id": trade_id,
                        "whale_name": pos_info.get("whale_name", ""),
                        "market_title": (pos_info.get("market_title", "") or "")[:80],
                        "category": category,
                        "side": side,
                        "entry_price": float(entry_price),
                        "exit_price": float(exit_price),
                        "quantity": float(qty),
                        "realized_pnl": float(realized_pnl),
                        "realized_return": float(realized_return),
                        "duration_seconds": float(duration),
                        "exit_reason": exit_reason,
                        "instrument_id": inst_key[:80],
                        "ts_mono_ns": closed_ts,
                    },
                    correlation_id=trade_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=self._validation_run_id,
                )
                self.log.debug(f"Validation: TRADE_CLOSED {trade_id[:8]}... PnL=${realized_pnl:+.2f}")
                
                # Clear trade context for this position
                if self._validation_context:
                    try:
                        # Find client_order_id from signal_id (reverse lookup)
                        # We don't have direct mapping, so we skip context clearing
                        # Context will be cleared on next signal or remain for analysis
                        pass
                    except Exception:
                        pass
            except Exception as e:
                self.log.warning(f"Validation event emission failed: {e}")
        
        # Nautilus close
        self.close_position(pos)
        self._last_exit_time[inst_key] = time.time()
        
        # ── POSITION CACHE DEDUP: Mark as exited ──
        self._exited_positions.add(inst_key)
        
        # Update daily P&L
        self._daily_pnl += realized_pnl
        
        # Update sports daily P&L if sports position
        category = pos_info.get('category', '') or ''
        if category.lower() == 'sports':
            self._sports_daily_pnl += realized_pnl
        
        pnl_sign = "+" if realized_pnl >= 0 else ""
        self.log.info(
            f"EXIT {exit_reason}: {qty:.0f} shrs @ ${exit_price:.4f} | "
            f"PnL: ${pnl_sign}{realized_pnl:.2f} ({realized_return:+.2%}) | "
            f"held {duration:.0f}s | daily_pnl=${self._daily_pnl:+.2f} | "
            f"{inst_key[:40]}..."
        )

    def exit_all_positions(self) -> None:
        """Close ALL open positions (emergency stop or daily loss limit)."""
        for inst_id in self.config.instrument_ids:
            open_positions = self.cache.positions_open(instrument_id=inst_id)
            if open_positions and open_positions[0].quantity.as_double() != 0:
                self.exit_position(inst_id, exit_reason="emergency_exit_all")

    def cancel_all_open_orders(self) -> None:
        """Cancel ALL pending open orders (kill switch).

        Phase 1 risk control: when position limits are breached,
        cancel all pending orders to stop trading immediately.
        """
        canceled_count = 0
        for order in self.cache.orders_open():
            try:
                self.cancel_order(order)
                canceled_count += 1
                self.log.info(f"Canceled order {order.client_order_id}")
            except Exception as e:
                self.log.error(f"Failed to cancel order {order.client_order_id}: {e}")
        self.log.info(f"KILL_SWITCH: canceled {canceled_count} open orders")


    def _recover_open_positions(self) -> None:
        """Reload unfinished positions from DB on restart.
        
        On crash recovery: reads trades without exit_reason from the trades DB,
        reconstructs the _open_positions dict so the exit timer can check them.
        """
        try:
            import sqlite3
            from pathlib import Path
            
            db_path = Path(__file__).parent.parent / "research" / "trades.db"
            if not db_path.exists():
                self.log.info("[RECOVER] No trades DB found, skipping recovery")
                return
            
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                "SELECT instrument_id, trade_id, whale_name, market_title, category, "
                "side, entry_price, position_size_usd, condition_id, edge_score "
                "FROM trades WHERE exit_reason IS NULL "
                "AND instrument_id IS NOT NULL "
                "ORDER BY timestamp"
            ).fetchall()
            conn.close()
            
            if not rows:
                self.log.info("[RECOVER] No orphan positions to recover")
                return
            
            recovered = 0
            for row in rows:
                inst_id, trade_id, whale_name, market_title, category, side, entry_price, size, cond_id, edge_score = row
                try:
                    from nautilus_trader.model.identifiers import InstrumentId
                    inst_key = str(InstrumentId.from_str(inst_id))
                except Exception:
                    inst_key = inst_id  # fallback: use raw string
                
                if inst_key not in self._open_positions:
                    self._open_positions[inst_key] = {
                        "whale_name": whale_name or "unknown",
                        "market_title": market_title or inst_id[:80],
                        "category": category or "Unknown",
                        "side": side or "BUY",
                        "entry_price": entry_price or 0.5,
                        "size": size or 0.0,
                        "entry_time": 0.0,  # unknown, let exit timer decide
                        "trade_id": trade_id,
                        "condition_id": cond_id or "",
                        "venue_position_id": "",
                        "edge_score": edge_score or 0.0,
                    }
                    recovered += 1
            
            self.log.info(
                f"[RECOVER] Recovered {recovered} open positions from DB "
                f"(total tracked: {len(self._open_positions)})"
            )
        except Exception as e:
            self.log.error(f"[RECOVER] Failed to recover open positions: {e}")

    def _check_all_positions(self) -> None:
        """Check stop-loss, take-profit, resolution, and duration exits for ALL open positions."""
        now = time.time()
        
        # Phase 1: Duration-based exit — close positions held past max_hold_hours
        max_hold = self.config.max_hold_hours
        expired = [k for k, v in self._open_positions.items() if now - v.get("entry_time", 0) > max_hold * 3600]
        for inst_key in expired:
            try:
                inst_id = InstrumentId.from_str(inst_key)
                self.exit_position(inst_id, exit_reason="max_hold")
            except Exception as e:
                self.log.error(f"Error exiting expired position {inst_key[:50]}...: {e}")
                # Clean up stale entry even on error
                if inst_key in self._open_positions:
                    del self._open_positions[inst_key]
                    save_open_positions(self._open_positions)
        
        # Phase 2: Check ALL open positions for stop-loss, take-profit, resolution exits
        # FIX: iterate self._open_positions (includes dynamic instruments) instead of
        # self.config.instrument_ids (pre-subscribed only). Dynamic instruments without
        # quote ticks use _resolve_exit_price as fallback current price.
        for inst_key in list(self._open_positions.keys()):
            # ── ERROR ISOLATION: Wrap each position in try/except ──
            try:
                try:
                    inst_id = InstrumentId.from_str(inst_key)
                except Exception as parse_err:
                    self.log.error(f"Failed to parse instrument ID '{inst_key[:50]}...': {parse_err}")
                    continue

                open_positions = self.cache.positions_open(instrument_id=inst_id)
                if not open_positions or open_positions[0].quantity.as_double() == 0:
                    continue

                pos = open_positions[0]
                # avg_px_open can be a Price object OR a raw float depending on Nautilus version
                raw_entry = pos.avg_px_open
                entry = raw_entry.as_double() if hasattr(raw_entry, 'as_double') else float(raw_entry)
                if entry <= 0:
                    continue

                # Get position info
                pos_info = self._open_positions.get(inst_key, {})
                
                # Get current price from cache (last quote)
                quote = self.cache.quote_tick(inst_id)
                if quote is None:
                    # Dynamic instrument without quote subscription — use simulated price
                    if pos_info:
                        mid = self._resolve_exit_price(pos_info)
                        self.log.info(f"SIMULATED PRICE for {inst_id}:  (no quote ticks)")
                    else:
                        continue
                else:
                    mid = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2

                # ── Resolution-aware exit for binary prediction markets ──
                # Price-based SL/TP on binary outcome markets captures mid-point
                # opinion, not resolution truth. This caused the $54K P&L divergence:
                #   - TP exits showed +$34,737 sim but -$2,090 actual
                #   - SL exits showed +$7,056 sim but -$11,431 actual
                # Instead, we hold to resolution and only exit on:
                #   1. Certainty: price > 0.95 (very likely to win) or < 0.05 (very likely to lose)
                #   2. Whale abandonment of the same market (future enhancement)
                #   3. ResolutionPoller handles final exit when market resolves
                # Note: edge-score calibration is preserved for position sizing (kelly.py),
                #       not for exit triggers.
                position_edge = pos_info.get("edge_score", 0.0) or 0.0

                # Get position side from stored info (default BUY for safety)
                side = pos_info.get("side", "BUY")

                # Certainty exit: if price strongly indicates the outcome
                if side == "BUY":
                    is_certain_win = mid > CERTAINTY_WIN_THRESHOLD
                    is_certain_loss = mid < CERTAINTY_LOSS_THRESHOLD
                else:
                    is_certain_win = mid < CERTAINTY_LOSS_THRESHOLD
                    is_certain_loss = mid > CERTAINTY_WIN_THRESHOLD

                if is_certain_win:
                    self.log.info(
                        f"CERTAINTY EXIT (WIN) {inst_id}: mid={mid:.4f}, "
                        f"entry={entry:.4f}, edge={position_edge:.2f}, "
                        f"condition_id={pos_info.get('condition_id', '?')[:20]}..."
                    )
                    self.exit_position(inst_id, exit_reason="certainty_win")
                    continue
                elif is_certain_loss:
                    self.log.info(
                        f"CERTAINTY LOSS BLOCKED (Phase A): {inst_id}: mid={mid:.4f}, "
                        f"entry={entry:.4f} - holding to resolution instead"
                    )
                    continue
                else:
                    # ── WHITELIST: Only Spread bets get sports exit signals ──
                    # Check resolution/sports exit for positions not certainty win/loss
                    # Only sports markets whitelisted as "Spread:" get the sports event exit signal
                    mc = pos_info.get("market_category", "")
                    if mc.lower() == "sports":
                        title = pos_info.get("market_title", "") or ""
                        from strategies.wf_constants import SPORTS_WHITELIST_PATTERNS
                        is_whitelisted = any(
                            re.search(p, title, re.IGNORECASE)
                            for p in SPORTS_WHITELIST_PATTERNS
                        )
                        
                        if is_whitelisted:
                            # Sports event exit (game imminent) - only for whitelisted Spread bets
                            if self._should_exit_for_sports(inst_id):
                                self.log.info(f"SPORTS EVENT EXIT {inst_id}: Spread bet, game imminent")
                                self.exit_position(inst_id, exit_reason="sports_event")
                                continue
                        else:
                            # Non-whitelisted sports: no sports exit signal
                            self.log.info(
                                f"SKIP sports exit (non-whitelisted): {title[:50]} | "
                                f"entry={entry:.4f}, mid={mid:.4f}"
                            )
                    
                    # Resolution exit check — exit if market resolves within 6 hours
                    if self._should_exit_for_resolution(inst_id):
                        self.log.info(f"RESOLUTION EXIT {inst_id}: market resolving soon")
                        self.exit_position(inst_id, exit_reason="resolution")
                        continue
                    
                    # Log holding state for transparency
                    self.log.info(
                        f"HOLDING {inst_id}: entry={entry:.4f}, mid={mid:.4f}, "
                        f"edge={position_edge:.2f} — holding to resolution"
                    )
            except Exception as pos_error:
                # Log error and continue to next position (error isolation)
                self.log.error(
                    f"Error checking position {inst_key[:50]}...: {pos_error} | "
                    f"entry={pos_info.get('entry_price', '?') if 'pos_info' in dir() else '?'} | "
                    f"continuing to next position"
                )
                continue

    def _should_exit_for_resolution(self, instrument_id: InstrumentId) -> bool:
        """Check if the market for this instrument resolves within RESOLUTION_EXIT_HOURS hours."""
        try:
            # Extract condition ID from instrument
            cond_id = str(instrument_id).split("-")[0]
            # Check Polymarket data-api for resolution time
            import requests
            resp = requests.get(
                f"https://data-api.polymarket.com/markets/{cond_id}",
                timeout=10,
            )
            if resp.status_code == 200:
                market = resp.json()
                end_date = market.get("end_date_iso")
                if end_date:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    if 0 < hours_left < RESOLUTION_EXIT_HOURS:
                        return True
                    # Exit if market has already ended (hours_left <= 0 = resolved/expired)
                    if hours_left <= 0:
                        self.log.info(
                            f"Market has already ended ({abs(hours_left):.1f}h ago) — "
                            f"{cond_id[:16]}..., exiting stale position"
                        )
                        return True
                    return False
        except Exception:
            pass  # API failure — don't exit on error
        return False

    def _check_daily_loss_limit(self) -> None:
        """Check if daily loss limit has been breached."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._daily_pnl_date:
            # New day, reset
            self._daily_pnl = 0.0
            self._daily_pnl_date = today
            self._daily_loss_breached = False  # Reset breach flag for new day
            self._sports_daily_pnl = 0.0
            self._sports_daily_pnl_date = today
            self._sports_daily_loss_breached = False
            return

        if self._daily_loss_breached:
            return  # Already breached — no need to re-log every 30s

        if self._daily_pnl <= -self._daily_loss_limit:
            self.log.error(
                f"DAILY LOSS LIMIT BREACHED: ${self._daily_pnl:,.2f} / -${self._daily_loss_limit:,.2f}. "
                f"Closing all positions and stopping auto-trade."
            )
            self._daily_loss_breached = True
            self.exit_all_positions()

        if self._sports_daily_loss_breached:
            return

        if self._sports_daily_pnl <= -self.config.sports_daily_loss_limit:
            self.log.error(
                f"SPORTS DAILY LOSS LIMIT BREACHED: ${self._sports_daily_pnl:,.2f} / -${self.config.sports_daily_loss_limit:,.2f}. "
                f"Closing all positions and stopping auto-trade."
            )
            self._sports_daily_loss_breached = True
            self.exit_all_positions()

    def _on_exit_timer(self, timer_name: str = None) -> None:
        """Timer callback — fires every 30s independently of quote ticks.
        
        This fixes the critical design flaw where exit checks only ran
        during quote tick processing. If quotes stop (frozen sports markets,
        WebSocket drops), exits were never checked.
        """
        check_all_positions(config=self.config, cache=self.cache, log=self.log, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob)
        result = check_daily_loss_limit(config=self.config, log=self.log, daily_pnl=self._daily_pnl, daily_pnl_date=self._daily_pnl_date, daily_loss_breached=self._daily_loss_breached, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob, cache=self.cache); self._daily_pnl, self._daily_pnl_date, self._daily_loss_breached = result
        # Heartbeat log for health monitor — throttle to once per minute
        now = time.time()
        if not hasattr(self, '_last_heartbeat') or (now - self._last_heartbeat) > 60:
            # Use self._open_positions (JSON/DB recovered) + Nautilus cache for full count
            nautilus_open = len(self.cache.positions_open(venue=Venue("POLYMARKET")))
            total_open = len(self._open_positions)
            self.log.info(f"Exit timer heartbeat — {total_open} positions (self._open_positions={total_open}, nautilus_cache={nautilus_open})")
            self._last_heartbeat = now
        
        # Whale position scanning (moved here from on_quote_tick so it runs
        # independently of WebSocket data flow — critical for reliability)
        if now - self._last_scan >= self.config.scan_interval_secs:
            self._scan_whale_positions()
            self._last_scan = now
        
        # Autoresearch LLM signal bridge — check for model-generated trade recommendations
        self._check_autoresearch_signals()
        
        # Sybil meta-whale signal bridge — check for sybil group fade/follow signals
        self._check_sybil_signals()
        
        # Memory pressure check - graceful restart before OOM
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        rss_mb = rss_kb / 1024
                        if rss_mb > MEMORY_PRESSURE_MB:
                            self.log.warning(f"MEMORY PRESSURE: {rss_mb:.0f}MB RSS - initiating graceful shutdown")
                            self.stop()
                        break
        except Exception:
            pass
        
        # System-level memory warning — log if total used > 85%
        try:
            with open("/proc/meminfo") as f:
                meminfo = f.read()
            total_match = re.search(r"MemTotal:\s+(\d+)", meminfo)
            avail_match = re.search(r"MemAvailable:\s+(\d+)", meminfo)
            if total_match and avail_match:
                total_kb = int(total_match.group(1))
                avail_kb = int(avail_match.group(1))
                used_pct = 100 - (avail_kb * 100 / total_kb)
                if used_pct > 85:
                    self.log.warning(f"HIGH SYSTEM MEMORY: {used_pct:.0f}% used ({avail_kb//1024}MB free)")
        except Exception:
            pass
        
        # Resolution polling — check if tracked open positions' markets have resolved
        # Updates trades.db with actual P&L when markets resolve
        if now - self._last_resolution_poll >= self._resolution_poll_interval:
            try:
                events = self._resolution_poller.poll_open_positions(self._open_positions)
                if events:
                    for ev in events:
                        self.log.info(
                            f"[RESOLUTION] {ev.get('question', '')[:50]} | "
                            f"Winner: {ev.get('winning_outcome', '?')} | "
                            f"Actual P&L: ${ev.get('total_actual_pnl', 0):+.2f} "
                            f"({ev.get('trades_count', 0)} trades)"
                        )
                        # If position resolved and we're still holding, exit at resolution price
                        for trade in ev.get("trades", []):
                            inst_key = trade.get("inst_key", "")
                            if inst_key and inst_key in self._open_positions:
                                try:
                                    inst_id = InstrumentId.from_str(inst_key)
                                    self.exit_position(inst_id, exit_reason="market_resolved")
                                    self.log.info(
                                        f"[RESOLUTION] Exited resolved position: {inst_key[:50]}..."
                                    )
                                except Exception as e:
                                    self.log.error(
                                        f"[RESOLUTION] Failed to exit resolved position: {e}"
                                    )
            except Exception as e:
                self.log.error(f"Resolution poll error: {e}")
            self._last_resolution_poll = now


        # P2: Sybil intelligence monitoring (every timer tick)
        if run_sybil_monitoring:
            try:
                sybil_report = run_sybil_monitoring()
                if sybil_report and not sybil_report.get("error"):
                    groups_active = len(sybil_report.get("meta_whale_groups", {}).get("groups", []))
                    if groups_active > 0:
                        self.log.info(f"[SYBIL] {groups_active} meta-whale groups tracked")
            except Exception as e:
                self.log.error(f"Sybil monitoring failed: {e}")

        # Instrument recycle: unsubscribe/resubscribe every 30min to flush stale order books
        # This prevents the memory leak from unbounded order book cache growth in the framework
        if now - self._last_recycle >= self._recycle_interval:
            recycle_count = len(self.config.instrument_ids)
            self.log.info(f"RECYCLE: Unsubscribing {recycle_count} instruments to flush order book cache")
            for inst_id in self.config.instrument_ids:
                self.unsubscribe_quote_ticks(inst_id)
                self.unsubscribe_trade_ticks(inst_id)
                self.unsubscribe_order_fills(inst_id)
            for inst_id in self.config.instrument_ids:
                self.subscribe_quote_ticks(inst_id)
                self.subscribe_trade_ticks(inst_id)
                self.subscribe_order_fills(inst_id)
            self._last_recycle = now
            self.log.info(f"RECYCLE: Resubscribed {recycle_count} instruments - order book cache flushed")
        
        # Cleanup stale subscriptions older than 1 hour
        if self._dynamic_subscriptions:
            one_hour_ago = now - STALE_SUBSCRIPTION_TTL_SECS
            stale_keys = [k for k, t in self._dynamic_subscriptions.items() if t < one_hour_ago]
            for key in stale_keys:
                del self._dynamic_subscriptions[key]
            if stale_keys:
                self.log.info(f"Cleaned up {len(stale_keys)} stale dynamic subscriptions")

    def _check_autoresearch_signals(self) -> None:
        """Poll autoresearch signal queue for model-generated trade recommendations."""
        queue_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "autoresearch_signal_queue.json")
        if not os.path.exists(queue_path):
            return
        try:
            with open(queue_path) as f:
                signals = json.load(f)
            if not signals or not isinstance(signals, list):
                return
            # Clear the queue immediately to prevent re-processing on crash
            with open(queue_path, "w") as f:
                json.dump([], f)
            processed = 0
            for s in signals:
                signal_obj = WhaleSignal(
                    signal_type=WhaleSignalType.LARGE_POSITION,
                    condition_id=s.get("condition_id", ""),
                    token_id=s.get("token_id", ""),
                    outcome=s.get("outcome", "Yes"),
                    side=s.get("side", "buy"),
                    confidence=s.get("confidence", 0.5),
                    target_price=s.get("entry_price", 0.5),
                    suggested_size_usd=s.get("suggested_size_usd", 0.0),
                    whale_name=s.get("whale_name", "autoresearch_llm"),
                    whale_roi=s.get("whale_roi", 0.0),
                    timestamp=s.get("timestamp", time.time()),
                    reason=s.get("reason", "Autoresearch LLM signal"),
                    market_title=s.get("market_title", ""),
                    market_category=s.get("market_category", ""),
                    whale_address=s.get("whale_address", ""),
                    edge_score=s.get("edge_score", 0.0),
                )
                self._on_signal(signal_obj)
                processed += 1
            if processed:
                self.log.info(f"Autoresearch signals: {processed} queued recommendations processed")
        except Exception as e:
            self.log.error(f"Autoresearch signal check failed: {e}")

    def _check_sybil_signals(self) -> None:
        """Poll sybil signal queue for meta-whale fade/follow recommendations.
        
        Full pipeline: Decay filter → Dedup filter → Price validation → 
        Confidence scaling → _on_signal(). Clears queue after processing.
        """
        queue_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "sybil_signal_queue.json")
        if not os.path.exists(queue_path):
            return
        try:
            with open(queue_path) as f:
                data = json.load(f)
            signals = data.get("signals", []) if isinstance(data, dict) else []
            if not signals:
                return
            
            now = time.time()
            total_in = len(signals)
            
            # Step 1: Signal Decay — filter out expired signals by age
            active_signals = []
            decayed = 0
            for s in signals:
                gen_at_str = s.get("generated_at", "")
                if not gen_at_str:
                    decayed += 1
                    continue
                try:
                    gen_ts = datetime.fromisoformat(gen_at_str.replace("Z", "+00:00")).timestamp()
                except (ValueError, TypeError):
                    decayed += 1
                    continue
                age = now - gen_ts
                title = s.get("market_title", "").lower()
                is_sports = any(kw in title for kw in ["nfl", "nba", "mlb", "nhl", "ncaa", "soccer", "ufc", "f1"])
                ttl = SYBIL_SPORTS_SIGNAL_TTL_SECS if is_sports else SYBIL_SIGNAL_TTL_SECS
                if age > ttl:
                    decayed += 1
                    continue
                active_signals.append(s)
            
            if not active_signals:
                with open(queue_path, "w") as f:
                    json.dump({"generated_at": "", "signal_count": 0, "signals": []}, f)
                if decayed:
                    self.log.info(f"Sybil signals: {decayed} expired, none remaining")
                return
            
            # Step 2: Dedup — file-based state tracking
            dedup_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "sybil_signal_dedup.json")
            dedup_state: dict[str, float] = {}
            try:
                if os.path.exists(dedup_path):
                    with open(dedup_path) as f:
                        dedup_state = json.load(f)
                    cutoff = now - SYBIL_DEDUP_TTL_SECS
                    dedup_state = {k: v for k, v in dedup_state.items() if isinstance(v, (int, float)) and v >= cutoff}
            except Exception:
                dedup_state = {}
            
            deduped = 0
            new_dedup: dict[str, float] = {}
            for s in active_signals:
                dk = f"{s.get('condition_id', '')}|{s.get('signal_type', '')}|{s.get('group_id', '')}"
                if dk in dedup_state:
                    deduped += 1
                    continue
                new_dedup[dk] = now
            
            if not new_dedup:
                with open(queue_path, "w") as f:
                    json.dump({"generated_at": "", "signal_count": 0, "signals": []}, f)
                if deduped:
                    self.log.info(f"Sybil signals: {deduped} deduped, none new")
                return
            
            # Persist dedup state
            dedup_state.update(new_dedup)
            try:
                with open(dedup_path, "w") as f:
                    json.dump(dedup_state, f)
            except Exception as e:
                self.log.error(f"Sybil dedup state write failed: {e}")
            
            # Clear queue — done before processing to prevent crash duplication
            with open(queue_path, "w") as f:
                json.dump({"generated_at": "", "signal_count": 0, "signals": []}, f)
            
            # Steps 3-4: Process each surviving signal with price + confidence checks
            processed = 0
            price_rejected = 0
            low_conf_rejected = 0
            
            for s in active_signals:
                dk = f"{s.get('condition_id', '')}|{s.get('signal_type', '')}|{s.get('group_id', '')}"
                if dk not in new_dedup:
                    continue  # Was in dedup_state from prior run
                
                confidence = s.get("confidence", 0.5)
                if confidence < SYBIL_CONFIDENCE_MIN:
                    low_conf_rejected += 1
                    continue
                
                # Price validation
                price_valid, price_reason = self._validate_sybil_signal_price(s)
                if not price_valid:
                    price_rejected += 1
                    self.log.info(f"Sybil price skip: {s.get('market_title', '')[:50]} — {price_reason}")
                    continue


                # F3: Filter sybil groups containing blacklisted whales (before signal generation)
                positions_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "sybil_positions.json")
                try:
                    with open(positions_path) as pf:
                        positions_data = json.load(pf)
                    group_markets = positions_data.get("groups", {}).get(s.get("group_id", ""), {}).get("markets", [])
                    for m in group_markets:
                        if m.get("condition_id") == s.get("condition_id", ""):
                            wallet_labels = [w.get("label", "") for w in m.get("wallets", [])]
                            blacklisted = [w for w in wallet_labels if w in WHALE_BLACKLIST]
                            if blacklisted:
                                self.log.debug(f"Skipping sybil signal (blacklisted whales in group): {blacklisted} for {s.get("market_title", "")[:50]}")
                                continue  # Skip this signal
                            break
                except Exception:
                    pass  # If positions file not available, proceed without filtering

                # F4: Filter long-dated markets (end_date > 30 days from now)
                end_date_str = s.get("end_date", "")
                if end_date_str:
                    try:
                        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                        days_until = (end_dt - datetime.now(timezone.utc)).total_seconds() / 86400
                        if days_until > 30:
                            self.log.debug(f"Skipping long-dated market: {s.get("market_title", "")[:50]} (end_date: {end_date_str})")
                            continue
                    except (ValueError, TypeError):
                        pass

                
                # Map side
                sybil_side = s.get("side", "BUY YES")
                if "BUY YES" in sybil_side.upper():
                    side = "buy"
                    outcome = "Yes"
                else:
                    side = "buy"
                    outcome = "No"
                
                # Confidence-based size scaling
                exposure = s.get("total_exposure_usd", 0)
                signal_type = s.get("signal_type", "")
                base_pct = SYBIL_BASE_SIZE_PCT.get(signal_type, 0.10)
                multiplier = confidence / SYBIL_CONFIDENCE_BASELINE
                multiplier = max(SYBIL_SIZE_MIN_MULTIPLIER, min(SYBIL_SIZE_MAX_MULTIPLIER, multiplier))
                suggested_size = min(exposure * base_pct * multiplier, 5000)
                
                group_id = s.get("group_id", "unknown")
                signal_obj = WhaleSignal(
                    signal_type=WhaleSignalType.LARGE_POSITION,
                    condition_id=s.get("condition_id", ""),
                    token_id="",
                    outcome=outcome,
                    side=side,
                    confidence=confidence,
                    target_price=0.5,
                    suggested_size_usd=suggested_size,
                    whale_name=f"sybil_meta_{group_id}",
                    whale_roi=0.0,
                    timestamp=time.time(),
                    reason=s.get("reason", f"Sybil {group_id} signal"),
                    market_title=s.get("market_title", ""),
                    market_category="",
                    whale_address="",
                    edge_score=confidence * 10,
                )
                self._on_signal(signal_obj)
                processed += 1
            
            if processed or decayed or deduped or price_rejected or low_conf_rejected:
                self.log.info(
                    f"Sybil signals: {total_in} in → {processed} executed, "
                    f"{decayed} decayed, {deduped} deduped, "
                    f"{price_rejected} price-rejected, {low_conf_rejected} low-conf"
                )
        except Exception as e:
            self.log.error(f"Sybil signal check failed: {e}")
    
    def _validate_sybil_signal_price(self, signal: dict) -> tuple[bool, str]:
        """Check current market midpoint hasn't moved against the signal direction.
        
        Args:
            signal: A sybil signal dict from the queue.
        
        Returns:
            (True, reason) if price is favorable for entry,
            (False, reason) if price has moved too far.
        """
        condition_id = signal.get("condition_id", "")
        if not condition_id:
            return True, "no_condition_id"
        
        now = time.time()
        cached = self._sybil_price_cache.get(condition_id)
        if cached and (now - cached[1]) < 30:
            midpoint = cached[0]
        else:
            try:
                import urllib.request
                url = f"https://clob.polymarket.com/midpoint?condition_id={condition_id}"
                req = urllib.request.Request(url, headers={"User-Agent": "nautilus-sybil/1.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.loads(resp.read().decode())
                midpoint_str = data.get("midpoint") or data.get("price")
                if midpoint_str is None:
                    return True, "no_midpoint"
                midpoint = float(midpoint_str)
                self._sybil_price_cache[condition_id] = (midpoint, now)
            except Exception as e:
                self.log.debug(f"Sybil price check failed for {condition_id[:20]}: {e}")
                return True, "api_failed"  # Fail-open on API error
        
        sybil_side = signal.get("side", "BUY YES")
        if "BUY YES" in sybil_side.upper():
            max_entry = 0.5 + SYBIL_MAX_PRICE_SLIPPAGE
            if midpoint > 0.90:
                return False, f"YES price {midpoint:.3f} near certainty"
            if midpoint > max_entry:
                return False, f"YES price {midpoint:.3f} > max entry {max_entry:.3f}"
            return True, f"YES at {midpoint:.3f}"
        else:
            no_price = 1.0 - midpoint
            max_entry = 0.5 + SYBIL_MAX_PRICE_SLIPPAGE
            if no_price > max_entry:
                return False, f"NO price {no_price:.3f} > max entry {max_entry:.3f}"
            return True, f"NO at {no_price:.3f} (YES={midpoint:.3f})"

    # ── Sports Market Detection (Track A) ─────────────────────────────────────

    _SPORTS_KEYWORDS = [
        "nfl", "nba", "mlb", "nhl", "ncaa", "college football", "college basketball",
        "soccer", "football", "basketball", "baseball", "hockey", "tennis", "golf",
        "boxing", "mma", "ufc", "wwe", "f1", "formula 1", "nascar",
        "super bowl", "world cup", "champions league", "premier league",
        "playoffs", "stanley cup", "world series", "final four", "march madness",
        "vs.", " vs ", "eagles", "49ers", "chiefs", "lakers", "celtics",
        "warriors", "yankees", "dodgers", "red sox", "patriots",
        "trail blazers", "spurs", "penguins", "stars", "wild",
        "bucks", "thunder", "nuggets", "timberwolves", "knicks",
    ]

    def _is_sports_market(self, instrument_id) -> tuple[bool, str]:
        """Check if an instrument is a sports market. Returns (is_sports, sport_type)."""
        title = str(instrument_id).lower()
        sport_types = {
            "nba": ["nba", "lakers", "celtics", "warriors", "bucks", "thunder", "nuggets", "knicks", "trail blazers", "spurs", "timberwolves"],
            "nfl": ["nfl", "eagles", "49ers", "chiefs", "patriots", "cowboys", "commanders"],
            "nhl": ["nhl", "penguins", "stars", "wild", "hurricanes", "golden knights", "avalanche", "oilers", "canucks"],
            "mlb": ["mlb", "yankees", "dodgers", "red sox"],
            "soccer": ["soccer", "champions league", "premier league", "world cup"],
            "ncaa": ["ncaa", "college football", "college basketball", "march madness", "final four"],
        }
        
        for sport_type, keywords in sport_types.items():
            for kw in keywords:
                if kw in title:
                    return True, sport_type
        
        # General sports check
        for kw in self._SPORTS_KEYWORDS:
            if kw in title:
                return True, "other_sports"
        
        return False, ""

    def _get_market_event_time(self, instrument_id) -> dict:
        """Fetch event timing for a market from Polymarket API."""
        cond_id = str(instrument_id).split("-")[0]
        try:
            # Use cached metadata if available
            metadata_file = Path.home() / "workspace" / "metadata" / "markets_latest.json"
            if metadata_file.exists():
                import json
                with open(metadata_file) as f:
                    markets = json.load(f)
                for m in markets:
                    if m.get("condition_id") == cond_id:
                        return {
                            "hours_until_event": m.get("hours_until_event"),
                            "is_imminent": m.get("is_imminent", False),
                            "is_in_play": m.get("is_in_play", False),
                            "is_past": m.get("is_past", False),
                            "event_date_iso": m.get("end_date_iso"),
                            "liquidity_tier": m.get("liquidity_tier", "tier3"),
                            "volume": m.get("volume", 0),
                            "liquidity": m.get("liquidity", 0),
                        }
        except Exception:
            pass
        
        # Fallback: fetch from API
        try:
            resp = requests.get(
                f"https://gamma-api.polymarket.com/markets/{cond_id}",
                timeout=10,
            )
            if resp.status_code == 200:
                m = resp.json()
                end_date = m.get("endDateIso", m.get("endDate"))
                if end_date:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    return {
                        "hours_until_event": round(hours_left, 1),
                        "is_imminent": 0 < hours_left < 6,
                        "is_in_play": hours_left < 6 and hours_left > 0,
                        "is_past": hours_left < 0,
                        "event_date_iso": end_date,
                        "liquidity_tier": "tier3",
                        "volume": float(m.get("volumeNum", 0)),
                        "liquidity": float(m.get("liquidityNum", 0)),
                    }
        except Exception:
            pass
        
        return {
            "hours_until_event": None,
            "is_imminent": False,
            "is_in_play": False,
            "is_past": False,
            "event_date_iso": None,
            "liquidity_tier": "tier3",
            "volume": 0,
            "liquidity": 0,
        }

    def _should_exit_for_sports(self, instrument_id) -> bool:
        """Check if a sports position should be exited (game imminent or in-play)."""
        is_sports, sport_type = self._is_sports_market(instrument_id)
        if not is_sports:
            return False
        
        timing = self._get_market_event_time(instrument_id)
        
        # Exit if game is within SPORTS_EXIT_HOURS_BEFORE_EVENT hour (prices will freeze during play)
        if timing["hours_until_event"] is not None and 0 < timing["hours_until_event"] < SPORTS_EXIT_HOURS_BEFORE_EVENT:
            self.log.info(
                f"Sports exit: {sport_type} market resolving in {timing['hours_until_event']:.1f}h"
            )
            return True
        
        # Exit if market is in-play (prices frozen, can't manage risk)
        if timing["is_in_play"]:
            self.log.info(f"Sports exit: {sport_type} market is in-play (prices frozen)")
            return True
        
        return False

    def _adjust_size_for_liquidity(self, size_usd: float, instrument_id) -> float:
        from strategies.wf_kelly import adjust_size_for_liquidity
        return adjust_size_for_liquidity(
            size_usd=size_usd,
            instrument_id_str=str(instrument_id),
            get_market_event_time_func=self._get_market_event_time,
            log_func=self.log.info,
        )

    def _kelly_size(self, price: float, whale_win_rate: float | None = None, edge_score: float = 0.0, available_balance: float | None = None, market_category: str = '') -> float:
        from strategies.wf_kelly import kelly_size
        return kelly_size(
            bankroll=self.config.bankroll,
            kelly_fraction=self.config.kelly_fraction,
            max_position_pct=self.config.max_position_pct,
            price=price,
            whale_win_rate=whale_win_rate,
            edge_score=edge_score,
            available_balance=available_balance,
            market_category=market_category,
            max_single_position_pct=self.config.max_single_position_pct,
            whale_tiering=self._whale_tiering,
        )

    # ── DEPRECATED: Use _check_all_positions() instead ──
    # _check_all_positions() handles stop-loss/take-profit for ALL positions
    # (including dynamic instruments), is side-aware (LONG/SHORT), and uses
    # proper exit_reason strings. These legacy methods only check the first
    # pre-subscribed instrument (self.config.instrument_id → instrument_ids[0]).
    # They are kept as no-op stubs for backward compat and removed in next major.
    def _check_stop_loss(self, current_price: float) -> None:
        """DEPRECATED: Use _check_all_positions()."""
        pass

    def _check_take_profit(self, current_price: float) -> None:
        """DEPRECATED: Use _check_all_positions()."""
        pass
