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
import queue
import threading
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
    import sys
    print(f"WARNING: Validation modules not available: {e}. Event logging disabled.", file=sys.stderr)
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
    SPORTS_WHITELIST_PATTERNS,
    SPORTS_OU_BLACKLIST_PATTERNS,
    SPORTS_VS_BLACKLIST_PATTERNS,
    SINGLE_TEAM_PATTERNS,
    MIN_ENTRY_PRICE,
    MIN_CONFIDENCE,
    BLOCKED_CATEGORIES,
    LIVE_ENTRY_PRICE_CAPS,
    BLOCKED_WHALE_ADDRESSES,
)
from strategies.wf_position_persistence import (
    save_open_positions,
    load_open_positions,
    load_daily_state,
    save_daily_state,
)
from strategies.wf_sports import is_sports_market, get_market_event_time, should_exit_for_sports
from strategies.wf_db_ops import log_trade_to_db, recover_open_positions, update_trade_latency_fields, ensure_decision_snapshots_table
from strategies.wf_signal_proc import scan_whale_positions
from strategies.llm_scorer import llm_score_signal
from strategies.signal_pipeline import SignalPipeline
from strategies.risk_manager import RiskManager, RiskState
from strategies.position_manager import PositionManager
from strategies.state_manager import StateManager
from strategies.signal_bridge import SignalBridge
from strategies.wf_signal_handler import SignalHandler
from strategies.state_manager import update_gap_state
from strategies.wf_position_checks import check_all_positions, check_daily_loss_limit
from strategies.wf_position_checks import (
    check_position_limits,
    trigger_kill_switch,
    get_current_total_exposure,
    get_market_exposure,
)
from strategies.capital_pool import CapitalPool
from strategies.base_strategy import get_strategy, BaseWhaleFollowerStrategy



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

# Sports market timing — SPORTS_EXIT_HOURS_BEFORE_EVENT imported from wf_constants

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
    sports_daily_loss_limit: float = 2000.0  # DEPRECATED: sports are paper-only, no longer tracked
    min_confidence: float = 0.55
    scan_interval_secs: float = 30.0
    auto_trade: bool = True
    # Dynamic Kelly: use whale's actual win rate instead of fixed estimate
    use_dynamic_kelly: bool = True
    # Seen position TTL: re-scan positions older than this (seconds)
    seen_position_ttl: float = 14400.0  # 4 hours (was 24h — 542 orphan_cleanup_sandbox trades avg'd 35h)
    # Max hold time for open positions (hours) — longer than this triggers auto-exit
    max_hold_hours: float = 4.0  # close positions held > 4h (was 24h — 6.2% WR on >1h positions)

    # Asymmetrical SL/TP: TP = TP_MULTIPLIER x SL threshold (winners run longer)
    tp_multiplier: float = 2.5  # TP width = 2.5x SL width

    # Trailing stop - activates after TP threshold is reached
    trailing_stop: bool = True
    trailing_stop_retrace_pct: float = 0.40  # Exit if price retraces 40% from peak gain
    # Max trades per scan cycle (prevents balance exhaustion on restart)
    max_trades_per_scan: int = 20
    # Trade buffer flush interval (seconds)
    trade_buffer_flush_secs: float = 30.0
    # Test mode: inject synthetic signals to exercise pipeline
    test_mode: bool = False
    test_signal_interval_secs: float = 300.0  # 5 min between synthetic signals

    # MiniMax API key for LLM scoring (llm_score_signal). Load from env if not set.
    minimaxi_api_key: str = ""

    # Market exclusion list: market IDs in this list are skipped by whale_follower
    # and by the autoresearch signal pipeline. Populated at runtime or via config.
    ignored_markets: list[str] = []

    # Backward compat: allow single instrument_id
    @property
    def instrument_id(self) -> InstrumentId:
        return self.instrument_ids[0] if self.instrument_ids else None


def _background_resolution_poll(
    open_positions: dict,
    poller,
    resolved_queue,
    strategy,
) -> None:
    """Run resolution polling in a background thread.

    Puts resolved instrument keys into resolved_queue (queue.Queue) for the main
    thread to process. This avoids all shared-state mutation from the background thread.
    """
    try:
        events = poller.poll_open_positions(open_positions)
        if not events:
            return
        for ev in events:
            strategy.log.info(
                f"[RESOLUTION] {ev.get('question', '')[:50]} | "
                f"Winner: {ev.get('winning_outcome', '?')} | "
                f"Actual P&L: ${ev.get('total_actual_pnl', 0):+.2f} "
                f"({ev.get('trades_count', 0)} trades)"
            )
            for trade in ev.get("trades", []):
                inst_key = trade.get("inst_key", "")
                if inst_key:
                    resolved_queue.put_nowait(inst_key)
    except Exception as e:
        strategy.log.error(f"[RESOLUTION] Background poll failed: {e}")


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
        self._kill_switch_time: float = 0.0  # Timestamp when kill switch was triggered
        self._pending_whales: dict[str, dict] = {}  # client_order_id -> {whale_name, market_title, category}
        self._last_exit_time: dict[str, float] = {}  # inst_id -> timestamp (re-entry cooldown)
        self._last_resolution_check: dict[str, float] = {}  # inst_id -> timestamp (rate-limit API calls)
        self._open_positions: dict[str, dict] = {}  # str(inst_id) -> {whale_name, market_title, category, side, entry_price, size, entry_time, trade_id, condition_id}
        self._exited_positions: set[str] = set()  # Track exited instrument IDs to prevent duplicate exits
        self._whale_tiering: WhaleTiering | None = None
        self._whale_intel: WhaleIntelligence | None = None
        # Fade tracking
        self._fade_positions: set[str] = set()  # Track active fade positions for concurrency limiting
        self._fade_max_concurrent: int = 6  # v5.1: raised from 3 to 6 — sybil fades were hitting cap  # Max concurrent fade trades
        self._sybil_price_cache: dict[str, tuple[float, float]] = {}  # condition_id -> (midpoint, timestamp)
        self._last_whale_type: str = ""  # Whale classification for DB logging

        # ── Signal Pipeline + Risk Manager (decomposed from inline logic) ──
        self._pipeline: SignalPipeline | None = None  # Initialized in on_start
        self._risk_state: RiskState | None = None
        self._risk_manager: RiskManager | None = None
        self._position_mgr: PositionManager | None = None  # Initialized in on_start
        self._state_mgr: StateManager | None = None  # Initialized in on_start
        self._signal_bridge: SignalBridge | None = None  # Initialized in on_start
        self._signal_handler: SignalHandler | None = None  # Initialized in on_start
        
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

        # ── Phase 4: Capital Pool + Per-Category Strategies ──────────────
        self._capital_pool: CapitalPool | None = None
        self._strategies: dict[str, BaseWhaleFollowerStrategy] = {}

    def on_start(self) -> None:
        # Load daily P&L state from disk so kill switches survive restarts.
        # If the stored date is stale (yesterday), returns clean defaults.
        ds = load_daily_state()
        self._daily_pnl = ds["daily_pnl"]
        self._daily_pnl_date = ds["daily_pnl_date"]
        self._daily_loss_breached = ds["daily_loss_breached"]
        self.log.info(
            f"Daily state loaded: daily_pnl=${self._daily_pnl:+.2f}, "
            f"breached={self._daily_loss_breached}"
        )

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

        # Ensure decision_snapshots table exists before any signal processing
        try:
            ensure_decision_snapshots_table()
            self.log.info("DecisionSnapshot table ensured (Phase 0 observability)")
        except Exception as e:
            self.log.warning(f"Could not ensure decision_snapshots table: {e}")

        # ── Initialize Signal Pipeline + Risk Manager ──────────────────
        self._pipeline = SignalPipeline(
            whale_tiering=self._whale_tiering,
            whale_intel=self._whale_intel,
            min_confidence=self.config.min_confidence,
            min_edge=0.10,
            auto_trade=self.config.auto_trade,
            daily_loss_breached=self._daily_loss_breached,
        )
        self._risk_state = RiskState(
            daily_pnl=self._daily_pnl,
            daily_pnl_date=self._daily_pnl_date,
            daily_loss_breached=self._daily_loss_breached,
        )
        self._risk_manager = RiskManager(config=self.config)
        self._position_mgr = PositionManager(strategy=self)
        self._state_mgr = StateManager(strategy=self)
        self._signal_bridge = SignalBridge(strategy=self)
        self._signal_handler = SignalHandler(strategy=self)
        self.log.info("SignalPipeline + RiskManager + PositionManager + StateManager initialized")

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

        # ── Phase 4: Initialize Capital Pool + Per-Category Strategies ──
        self._capital_pool = CapitalPool(total_bankroll=self.config.bankroll)
        self._strategies = {}
        for cat_name in CapitalPool.CATEGORIES:
            try:
                strat = get_strategy(cat_name)
                strat.config = self.config
                strat.tracker = self._tracker
                strat.whale_tiering = self._whale_tiering
                strat.capital_pool = self._capital_pool
                strat.log = self.log
                self._strategies[cat_name] = strat
            except ValueError:
                self.log.warning(f"No strategy class for category: {cat_name}")
        if self._strategies:
            names = ", ".join(sorted(self._strategies.keys()))
            self.log.info(
                f"Phase 4: CapitalPool=${self.config.bankroll:,.0f}, "
                f"strategies=[{names}]"
            )

        # Initialize resolution poller for real P&L tracking
        self._resolution_poller = ResolutionPoller(strategy=self)
        self._resolved_positions_queue: queue.Queue = queue.Queue()
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
        # Issue 2 fix: verify each position exists in Nautilus cache before adding
        # to avoid creating orphans when cache is empty on restart.
        json_positions = load_open_positions()
        loaded_from_json = 0
        for inst_key, pos_info in json_positions.items():
            if inst_key not in self._open_positions:
                # Verify the position actually exists in Nautilus cache
                try:
                    inst_id = InstrumentId.from_str(inst_key)
                    cache_positions = self.cache.positions_open(instrument_id=inst_id)
                    if cache_positions and cache_positions[0].quantity.as_double() != 0:
                        self._open_positions[inst_key] = pos_info
                        loaded_from_json += 1
                    else:
                        self.log.warning(
                            f"[RECOVER] Skipping orphan {inst_key[:50]}... — not in Nautilus cache"
                        )
                except Exception as e:
                    self.log.warning(f"[RECOVER] Skipping {inst_key[:50]}... — cache check failed: {e}")
        if loaded_from_json > 0:
            self.log.info(f"[RECOVER] Loaded {loaded_from_json} verified positions from JSON file")

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
        """Delegate to wf_constants."""
        from strategies.wf_constants import categorize_instrument
        return categorize_instrument(inst_id)

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
            check_all_positions(config=self.config, cache=self.cache, log=self.log, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob, strategy=self)
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
            # Extract condition_id and token_id from instrument_id (format: {condition_id}-{token_id}.POLYMARKET)
            inst_str = str(tick.instrument_id)
            parts = inst_str.split("-")
            condition_id = parts[0] if len(parts) > 0 else ""
            token_id = parts[1].split(".")[0] if len(parts) > 1 else ""
            self._trade_buffer.append({
                "size": size,
                "price": price,
                "side": tick.aggressor_side.name,
                "timestamp": time.time(),
                "conditionId": condition_id,
                "token_id": token_id,
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
        """Delegate fill handling to StateManager."""
        if self._state_mgr is not None:
            self._state_mgr.on_order_filled(event)
        else:
            # Fallback: log warning if state manager not initialized
            self.log.warning("StateManager not initialized, skipping fill handling")
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
        
            for sig in signals:
                if self._trades_this_scan >= self.config.max_trades_per_scan:
                    self.log.info(
                        f"Scan trade limit reached ({self.config.max_trades_per_scan}), "
                        f"skipping {len(signals) - self._trades_this_scan} remaining signals"
                    )
                    break
                self._on_signal(sig)
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
            for sig in signals:
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
                                "whale_name": sig.whale_name,
                                "whale_address": getattr(sig, 'whale_address', ''),
                                "market_title": getattr(sig, 'market_title', '')[:80],
                                "market_category": getattr(sig, 'market_category', ''),
                                "side": sig.side,
                                "target_price": float(getattr(sig, 'target_price', 0.5)),
                                "suggested_size_usd": float(getattr(sig, 'suggested_size_usd', 0)),
                                "confidence": float(getattr(sig, 'confidence', 0)),
                                "edge_score": float(getattr(sig, 'edge_score', 0)),
                                "condition_id": sig.condition_id[:50],
                                "signal_source": sig.source.value if hasattr(sig.source, 'value') else str(sig.source),
                                "ts_mono_ns": whale_trade_ts,
                            },
                            correlation_id=signal_id,
                            mode=get_current_mode(),
                            strategy_id="whale_follower",
                            run_id=self._validation_run_id,
                        )
                        self.log.debug(f"Validation: WHALE_TRADE_DETECTED {signal_id[:8]}... ({sig.whale_name})")
                    except Exception as e:
                        self.log.warning(f"Validation event emission failed: {e}")

                # Pass signal_id to _on_signal for correlation
                sig._validation_signal_id = signal_id
                self._on_signal(sig)
        except Exception as e:
            self.log.error(f"Trade processing error: {e}")

    def _llm_score_signal(self, signal: WhaleSignal) -> int:
        """Delegate LLM signal scoring to llm_scorer module."""
        return llm_score_signal(
            signal,
            whale_intel=self._whale_intel,
            api_key=self.config.minimaxi_api_key if hasattr(self.config, "minimaxi_api_key") else None,
            log_func=self.log.warning,
        )

    def _on_signal(self, signal: WhaleSignal) -> None:
        """Delegate signal handling to SignalHandler."""
        if self._signal_handler is not None:
            self._signal_handler.handle_signal(signal)

    def _find_instrument(self, condition_id: str) -> InstrumentId | None:
        """Delegate instrument lookup to SignalHandler."""
        if self._signal_handler is not None:
            return self._signal_handler._find_instrument(condition_id)
        return None

    def _ensure_instrument_for_signal(self, condition_id: str, token_id: str, outcome: str) -> InstrumentId | None:
        """Delegate instrument resolution to SignalHandler."""
        if self._signal_handler is not None:
            return self._signal_handler._ensure_instrument_for_signal(condition_id, token_id, outcome)
        return None

    def _current_gross_exposure(self) -> float:
        """Delegate to PositionManager."""
        if self._position_mgr is not None:
            return self._position_mgr._current_gross_exposure()
        return 0.0

    def enter_position(
        self, side: OrderSide, price: float, whale_amount: float = 0,
        instrument_id: InstrumentId = None, whale_win_rate: float | None = None,
        whale_name: str = None, market_title: str = "", market_category: str = "",
        whale_address: str = "", edge_score: float = 0.0, confidence: float = 0.0,
        entry_reason: str = "", is_fade: bool = False,
        signal_source: str = "known_whale",
        _validation_signal_id: str = "", _validation_snapshot_id: str = "",
        _decision_snapshot: dict | None = None,
        _pipeline_passed: bool = False,
    ) -> None:
        """Delegate position entry to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.enter_position(
                side=side, price=price, whale_amount=whale_amount,
                instrument_id=instrument_id, whale_win_rate=whale_win_rate,
                whale_name=whale_name, market_title=market_title,
                market_category=market_category, whale_address=whale_address,
                edge_score=edge_score, confidence=confidence,
                entry_reason=entry_reason, is_fade=is_fade,
                signal_source=signal_source,
                _validation_signal_id=_validation_signal_id,
                _validation_snapshot_id=_validation_snapshot_id,
                _decision_snapshot=_decision_snapshot,
                _pipeline_passed=_pipeline_passed,
            )
        else:
            self.log.warning("PositionManager not initialized, skipping entry")

    def _fetch_real_midpoint(self, inst_key: str) -> float | None:
        """Delegate to PositionManager."""
        if self._position_mgr is not None:
            return self._position_mgr._fetch_real_midpoint(inst_key)
        return None

    def _resolve_exit_price(self, pos_info: dict) -> float:
        """Delegate to PositionManager."""
        if self._position_mgr is not None:
            return self._position_mgr._resolve_exit_price(pos_info)
        return pos_info.get("entry_price", 0.5)

    def exit_position(self, instrument_id: InstrumentId = None, exit_reason: str = "manual") -> None:
        """Delegate position exit to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.exit_position(instrument_id=instrument_id, exit_reason=exit_reason)
        else:
            self.log.warning("PositionManager not initialized, skipping exit")

    def exit_all_positions(self) -> None:
        """Delegate emergency exit to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.exit_all_positions()
        else:
            self.log.warning("PositionManager not initialized, skipping emergency exit")

    def cancel_all_open_orders(self) -> None:
        """Cancel all pending open orders (kill switch)."""
        if self._position_mgr is not None:
            self._position_mgr.cancel_all_open_orders()

    def _recover_open_positions(self) -> None:
        """Delegate position recovery to StateManager."""
        if self._state_mgr is not None:
            self._state_mgr.recover_open_positions()
        else:
            self.log.warning("StateManager not initialized, skipping recovery")

    def _check_all_positions(self) -> None:
        """Delegate position checking to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.check_all_positions()

    def _should_exit_for_resolution(self, instrument_id: InstrumentId, pnl_pct: float = 0.0, market_category: str = "") -> bool:
        """Delegate resolution exit check to PositionManager."""
        if self._position_mgr is not None:
            return self._position_mgr._should_exit_for_resolution(instrument_id, pnl_pct=pnl_pct, market_category=market_category)
        return False

    def _check_daily_loss_limit(self) -> None:
        """Delegate daily loss check to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.check_daily_loss()

    def add_resolution_pnl(self, pnl: float) -> None:
        """Delegate resolution P&L tracking to PositionManager."""
        if self._position_mgr is not None:
            self._position_mgr.add_resolution_pnl(pnl)

    def _update_gap_state(self, signal) -> None:
        """Delegate gap state update to gap_state module."""
        from strategies.state_manager import update_gap_state
        update_gap_state(signal)

    def _on_exit_timer(self, timer_name: str = None) -> None:
        """Timer callback — fires every 30s independently of quote ticks.
        
        This fixes the critical design flaw where exit checks only ran
        during quote tick processing. If quotes stop (frozen sports markets,
        WebSocket drops), exits were never checked.
        """
        check_all_positions(config=self.config, cache=self.cache, log=self.log, open_positions=self._open_positions, exited_positions=self._exited_positions, last_exit_time=self._last_exit_time, resolution_poller=self._resolution_poller, clob_client=self._clob, strategy=self)
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
        
        # Sybil meta-whale signal bridge — conservative integration
        # Applies 65-72% confidence filter + $100 max position
        self._check_sybil_signals()
        
        # Memory pressure check - graceful restart before OOM
        # Cross-platform: resource.getrusage works on macOS and Linux
        try:
            import resource
            rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            import sys
            if sys.platform == "darwin":
                rss_mb = rss_bytes / (1024 * 1024)
            else:
                # Linux: ru_maxrss is in KB
                rss_mb = rss_bytes / 1024

            if rss_mb > MEMORY_PRESSURE_MB:
                self.log.warning(f"MEMORY PRESSURE: {rss_mb:.0f}MB RSS - closing all positions before shutdown")
                self.exit_all_positions()
                self.stop()
        except Exception:
            pass

        # System-level memory warning — Linux only (/proc/meminfo)
        if os.path.exists("/proc/meminfo"):
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
        # else: non-Linux host — memory monitoring not available, skip
        
        # Resolution polling — check if tracked open positions' markets have resolved
        if now - self._last_resolution_poll >= self._resolution_poll_interval:
            self._last_resolution_poll = now
            # Poll in background thread, but only enqueue resolved keys (no shared-state mutation)
            t = threading.Thread(
                target=_background_resolution_poll,
                args=(self._open_positions, self._resolution_poller, self._resolved_positions_queue, self),
                daemon=True,
            )
            t.start()
            # Drain queue from main thread (safe — queue.Queue is thread-safe and we own the event loop)
            while not self._resolved_positions_queue.empty():
                try:
                    resolved_key = self._resolved_positions_queue.get_nowait()
                    inst_id = InstrumentId.from_str(resolved_key)
                    self.exit_position(inst_id, exit_reason="market_resolved")
                except Exception as e:
                    self.log.error(f"[RESOLUTION] Failed to exit queued position: {e}")


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
        """Delegate autoresearch signal checking to SignalBridge."""
        if self._signal_bridge is not None:
            self._signal_bridge.check_autoresearch_signals()

    def _check_sybil_signals(self) -> None:
        """Delegate sybil signal checking to SignalBridge AND scan DB for new signals (J2)."""
        # Bridge: check queue-based sybil signals
        if self._signal_bridge is not None:
            self._signal_bridge.check_sybil_signals()

        # J2 Fix: Also scan sybil_signals DB table for non-sports signals missed by the bridge.
        # The sybil detector writes to sybil_signals table (950+ rows) but nothing was feeding
        # them into the pipeline. scan_sybil_signals() bridges that gap.
        if self._tracker is not None:
            try:
                sybil_signals = self._tracker.scan_sybil_signals(max_age_hours=4)
                if sybil_signals:
                    self.log.info(
                        f"SYBIL_DB_SCAN: {len(sybil_signals)} non-sports sybil signals "
                        f"found, routing to pipeline"
                    )
                    for sig in sybil_signals:
                        self._on_signal(sig)
            except Exception as e:
                self.log.warning(f"_check_sybil_signals: tracker scan failed: {e}")

    def _validate_sybil_signal_price(self, signal: dict) -> tuple[bool, str]:
        """Delegate sybil price validation to SignalBridge."""
        if self._signal_bridge is not None:
            return self._signal_bridge.validate_sybil_signal_price(signal)
        return True, "no_bridge"

    def _is_sports_market(self, instrument_id) -> tuple[bool, str]:
        """Delegate to wf_sports module."""
        return is_sports_market(str(instrument_id))

    def _get_market_event_time(self, instrument_id) -> dict:
        """Delegate to wf_sports module."""
        return get_market_event_time(str(instrument_id))

    def _should_exit_for_sports(self, instrument_id) -> bool:
        """Delegate to wf_sports module."""
        return should_exit_for_sports(str(instrument_id), log_func=self.log.info)

    def _adjust_size_for_liquidity(self, size_usd: float, instrument_id) -> float:
        from strategies.wf_kelly import adjust_size_for_liquidity
        return adjust_size_for_liquidity(
            size_usd=size_usd,
            instrument_id_str=str(instrument_id),
            get_market_event_time_func=self._get_market_event_time,
            log_func=self.log.info,
        )

    def _kelly_size(self, price: float, whale_win_rate: float | None = None, edge_score: float = 0.0, available_balance: float | None = None, market_category: str = '', is_fade: bool = False) -> float:
        from strategies.wf_kelly import kelly_size
        # ── Phase 4: Per-category params from strategy ──────────────────
        strategy = self._strategies.get(market_category.lower())
        if strategy is not None:
            kelly_fraction = strategy.params.kelly_fraction
            max_position_pct = strategy.params.max_single_position_pct
            max_single_pct = strategy.params.max_single_position_pct
        else:
            kelly_fraction = self.config.kelly_fraction
            max_position_pct = self.config.max_position_pct
            max_single_pct = self.config.max_single_position_pct
        return kelly_size(
            bankroll=self.config.bankroll,
            kelly_fraction=kelly_fraction,
            max_position_pct=max_position_pct,
            price=price,
            whale_win_rate=whale_win_rate,
            edge_score=edge_score,
            available_balance=available_balance,
            market_category=market_category,
            max_single_position_pct=max_single_pct,
            whale_tiering=self._whale_tiering,
            is_fade=is_fade,
        )


