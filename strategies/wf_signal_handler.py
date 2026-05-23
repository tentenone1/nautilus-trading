"""Signal Handler — Orchestrates signal processing, risk checks, sizing, and entry.

Extracted from WhaleFollower._on_signal to decompose the god class. This module
handles the full signal lifecycle from pipeline filtering through position entry:

  1. Pipeline signal filtering (edge scoring, fade logic, blacklist)
  2. Risk checks (daily loss, kill switch, auto-trade)
  3. Fade concurrency check
  4. Kelly sizing (tier-based + whale intel)
  5. LLM quality scoring
  6. Category routing (live vs paper)
  7. Instrument resolution
  8. Validation event emission
  9. Position entry delegation

Uses the strategy reference pattern (self._s) to access the WhaleFollower instance.
"""

from __future__ import annotations

import json
import time
import uuid

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId

from strategies.signal_pipeline import PipelineResult
from strategies.wf_constants import (
    LIVE_ENTRY_PRICE_CAPS,
    BLOCKED_CATEGORIES,
)

# Validation integration (graceful degradation)
try:
    from components.validation.event_logger import EventType, log_event
    from components.validation.trade_context import TradeContext, get_trade_context
    from components.validation.snapshot_store import freeze_snapshot
    from components.validation.db_router import get_current_mode
    _validation_available = True
except ImportError:
    _validation_available = False
    EventType = None
    log_event = None
    freeze_snapshot = None
    get_current_mode = lambda: "paper"


class SignalHandler:
    """Handles the full signal processing pipeline from filtering to position entry."""

    def __init__(self, strategy):
        self._s = strategy

    # ── Properties that delegate to the strategy ──────────────────────────

    @property
    def log(self):
        return self._s.log

    @property
    def config(self):
        return self._s.config

    @property
    def cache(self):
        return self._s.cache

    @property
    def pipeline(self):
        return self._s._pipeline

    @property
    def risk_manager(self):
        return self._s._risk_manager

    @property
    def risk_state(self):
        return self._s._risk_state

    @property
    def whale_tiering(self):
        return self._s._whale_tiering

    @property
    def whale_intel(self):
        return self._s._whale_intel

    @property
    def fade_positions(self):
        return self._s._fade_positions

    @property
    def fade_max_concurrent(self):
        return self._s._fade_max_concurrent

    @property
    def signal_timestamps(self):
        return self._s._signal_timestamps

    @property
    def validation_run_id(self):
        return self._s._validation_run_id

    @property
    def validation_context(self):
        return self._s._validation_context

    # ── Main entry point ──────────────────────────────────────────────────

    def handle_signal(self, signal) -> None:
        """Handle a whale signal from ANY subscribed market.

        Signal filtering is delegated to SignalPipeline. This method handles:
        1. Pipeline processing (edge scoring, fade logic, blacklist, etc.)
        2. Risk checks (daily loss, kill switch, auto-trade)
        3. Kelly sizing (tier-based, whale intel)
        4. LLM quality scoring
        5. Fade concurrency check
        6. Category routing (live vs paper)
        7. Validation event emission
        8. Position entry
        """
        self.log.info(f"[DEBUG] _on_signal called for {signal.condition_id[:20]}... cond={signal.confidence:.2f}")

        # ── Step 1: Pipeline signal filtering ─────────────────────────────────
        if self.pipeline is not None:
            pipeline_result = self.pipeline.process(signal, log=self.log)

            if not pipeline_result.should_trade:
                return

            edge_val = pipeline_result.edge_score
            is_fade = pipeline_result.is_fade
            if pipeline_result.side_flip:
                signal.side = pipeline_result.side
            tier = pipeline_result.tier
            tier_config = self.whale_tiering.get_tier_config(
                getattr(signal, 'alpha_score', 50.0) or 50.0
            ) if self.whale_tiering else {}
        else:
            pipeline_result = PipelineResult(should_trade=True, side=signal.side or "BUY")
            edge_val = getattr(signal, 'edge_score', 0.0) or 0.0
            is_fade = False
            alpha_score = getattr(signal, 'alpha_score', 50.0) or 50.0
            tier = self.whale_tiering.get_tier(alpha_score) if self.whale_tiering else "unknown"
            tier_config = self.whale_tiering.get_tier_config(alpha_score) if self.whale_tiering else {}

        # ── Step 2: Risk checks (RiskManager if available) ────────────────────
        if not self.config.auto_trade:
            self.log.debug("Auto-trade disabled, skipping signal execution")
            return

        mc = getattr(signal, 'market_category', '') or ''
        if self.risk_manager is not None and self.risk_state is not None:
            self.risk_state.daily_pnl = self._s._daily_pnl
            self.risk_state.daily_loss_breached = self._s._daily_loss_breached
            self.risk_state.kill_switch_breached = self._s._kill_switch_breached
            if not self.risk_manager.can_trade(self.risk_state, category=mc, log=self.log):
                return
        else:
            if self._s._daily_loss_breached:
                self.log.warning(
                    "Daily loss limit breached ($%.2f), skipping signal execution",
                    self._s._daily_pnl
                )
                return
            if self._s._kill_switch_breached:
                self.log.warning("KILL_SWITCH active - rejecting signal")
                return

        # ── Step 3: Fade concurrency check ────────────────────────────────────
        if is_fade and len(self.fade_positions) >= self.fade_max_concurrent:
            self.log.info(
                f"FADE concurrency limit reached ({len(self.fade_positions)}/{self.fade_max_concurrent}), "
                f"skipping: {signal.whale_name}"
            )
            return

        # ── Step 4: Kelly sizing (tier-based + whale intel) ────────────────────
        alpha_score = getattr(signal, 'alpha_score', 50.0) or 50.0
        whale_tags = getattr(signal, 'tags', '[]')
        try:
            tags_list = json.loads(whale_tags) if isinstance(whale_tags, str) else (whale_tags or [])
        except (json.JSONDecodeError, TypeError):
            tags_list = []

        dual_config = self.whale_tiering.get_cached_tier(signal.whale_name) if self.whale_tiering else {}
        if dual_config.get("max_position_usd", 0) > 0 and dual_config["max_position_usd"] != 100:
            cached = self.whale_tiering.get_raw_cache(signal.whale_name)
            cap = cached.get("capital_tier", "?") if cached else "?"
            prec = cached.get("precision_tier", "?") if cached else "?"
            tier = f"{cap}+{prec}"
            tier_config = dual_config

        if self.whale_tiering:
            tier_kelly = self.whale_tiering.apply_overrides(
                tier_config, tags_list
            ).get("kelly_multiplier", 1.0)
            signal.suggested_size_usd = round(signal.suggested_size_usd * tier_kelly, 2)

        if self.whale_intel:
            original_size = signal.suggested_size_usd
            new_size, intel_note = self.whale_intel.apply_size_modifier(
                signal.whale_name, original_size, mc
            )
            if new_size != original_size:
                signal.suggested_size_usd = new_size
                self.log.info(
                    f"INTEL SIZE: {signal.whale_name} ${original_size:.2f} -> ${new_size:.2f} ({intel_note})"
                )

        # ── Step 5: LLM quality scoring ────────────────────────────────────────
        llm_score = 0
        try:
            from strategies.llm_scorer import llm_score_signal as _llm_score
            llm_score = _llm_score(
                signal,
                whale_intel=self.whale_intel,
                api_key=getattr(self.config, "minimaxi_api_key", None),
                log_func=self.log.warning,
            )
        except Exception as e:
            self.log.debug(f"LLM scoring skipped: {e}")

        # ── Step 6: Category routing (live vs paper) ──────────────────────────
        mc_lower = mc.lower()
        trading_mode = get_current_mode() if _validation_available else "paper"
        price_cap = LIVE_ENTRY_PRICE_CAPS.get(mc_lower)

        if mc_lower in BLOCKED_CATEGORIES:
            self.log.info(f"BLOCKED category={mc_lower} | {signal.condition_id[:50]}")
            return

        if mc_lower in LIVE_ENTRY_PRICE_CAPS or mc_lower in ("politics", "crypto", "sports", "entertainment"):
            if price_cap is None and mc_lower in ("politics", "crypto", "sports", "entertainment"):
                # No explicit cap but category is live-eligible
                pass

        # Log category routing
        if mc_lower not in LIVE_ENTRY_PRICE_CAPS and mc_lower not in ("politics", "crypto", "sports", "entertainment"):
            # Unknown/unclassified category — paper-only
            if trading_mode == "live":
                self.log.info(
                    f"PAPER_ONLY category={mc_lower} | {signal.condition_id[:50]} - "
                    f"live trading blocked for this category"
                )
                return
            self.log.info(
                f"PAPER_ONLY category={mc_lower} | {signal.condition_id[:50]} - sandbox fill"
            )
        elif price_cap is not None:
            entry_price = getattr(signal, 'target_price', 0.0) or 0.0
            if entry_price > price_cap:
                if trading_mode == "live":
                    self.log.info(
                        f"PAPER_GATE category={mc_lower} price=${entry_price:.4f} > cap=${price_cap:.2f} | "
                        f"{signal.condition_id[:50]} - live blocked, paper fill"
                    )
                else:
                    self.log.info(
                        f"PAPER_GATE category={mc_lower} price=${entry_price:.4f} > cap=${price_cap:.2f} | "
                        f"{signal.condition_id[:50]} - sandbox fill"
                    )
            else:
                self.log.info(
                    f"LIVE_ELIGIBLE category={mc_lower} price=${entry_price:.4f} <= cap=${price_cap:.2f} | "
                    f"{signal.condition_id[:50]}"
                )
        else:
            self.log.info(
                f"LIVE_ELIGIBLE category={mc_lower} | {signal.condition_id[:50]}"
            )

        # ── Step 7: Ensure instrument & determine side ────────────────────────
        target_inst = self._ensure_instrument_for_signal(
            signal.condition_id, signal.token_id, signal.outcome
        )
        if target_inst is None:
            self.log.info(f"Could not get instrument for {signal.market_title[:40]}, skipping")
            return

        side = OrderSide.BUY if signal.side == "buy" else OrderSide.SELL

        # ── Step 8: Whale win rate lookup for Kelly sizing ──────────────────
        whale_wr = None
        if self.config.use_dynamic_kelly and self._s._tracker:
            for w in self._s._tracker.whales.values():
                if w.name == signal.whale_name:
                    whale_wr = w.win_rate
                    break
            if whale_wr is None:
                self.log.debug(f"Whale '{signal.whale_name}' not found in tracker, using default Kelly")

        # ── Step 9: Validation event emission ────────────────────────────────
        signal_generated_ts = time.monotonic_ns()
        snapshot_id = ""
        validation_signal_id = getattr(signal, '_validation_signal_id', str(uuid.uuid4()))

        if _validation_available and log_event and EventType:
            try:
                if freeze_snapshot:
                    try:
                        market_state = {
                            "price": float(signal.target_price),
                            "side": signal.side,
                            "confidence": float(signal.confidence),
                            "edge_score": float(edge_val),
                        }
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

                whale_trade_ts = self.signal_timestamps.get(validation_signal_id, signal_generated_ts)
                if self.validation_context:
                    try:
                        self.validation_context.register_signal(
                            signal_id=validation_signal_id,
                            whale_trade_ts=whale_trade_ts,
                            signal_detected_ts=signal_generated_ts,
                            signal_generated_ts=signal_generated_ts,
                            snapshot_id=snapshot_id,
                            side=signal.side.upper(),
                        )
                    except Exception as ctx_err:
                        self.log.warning(f"Trade context registration failed: {ctx_err}")

                log_event(
                    event_type=EventType.SIGNAL_GENERATED,
                    data={
                        "signal_id": validation_signal_id,
                        "whale_name": signal.whale_name,
                        "condition_id": signal.condition_id,
                        "side": signal.side,
                        "confidence": float(signal.confidence),
                        "edge_score": float(edge_val),
                        "llm_score": llm_score,
                        "tier": tier,
                        "is_fade": is_fade,
                        "whale_win_rate": float(whale_wr or 0.55),
                        "ts_mono_ns": signal_generated_ts,
                    },
                    correlation_id=validation_signal_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=self.validation_run_id,
                )
                self.log.debug(f"Validation: SIGNAL_GENERATED {validation_signal_id[:8]}... snapshot={snapshot_id[:8] if snapshot_id else 'none'}")
            except Exception as e:
                self.log.warning(f"Validation event emission failed: {e}")

        signal._validation_signal_id = validation_signal_id
        signal._validation_snapshot_id = snapshot_id

        # ── Step 10: Enter position ──────────────────────────────────────────
        self._s.enter_position(
            side, signal.target_price, signal.suggested_size_usd,
            instrument_id=target_inst, whale_win_rate=whale_wr,
            whale_name=signal.whale_name,
            market_title=signal.market_title,
            market_category=getattr(signal, 'market_category', 'Unknown'),
            whale_address=getattr(signal, 'whale_address', '') or '',
            edge_score=edge_val,
            confidence=signal.confidence or 0.0,
            entry_reason=signal.reason or "",
            is_fade=is_fade,
            _validation_signal_id=validation_signal_id,
            _validation_snapshot_id=snapshot_id,
        )

        self._s._update_gap_state(signal)

    # ── Instrument resolution ──────────────────────────────────────────────

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
            market_info = self._s._clob.get_market(condition_id=condition_id)
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
            from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
            instrument = parse_polymarket_instrument(
                market_info=market_info,
                token_id=token_data["token_id"],
                outcome=token_data["outcome"],
            )
            self.cache.add_instrument(instrument)
            # Subscribe to quote ticks so dynamic instruments are checked by
            # _check_all_positions() Phase 2 stop-loss/take-profit/resolution logic
            self._s.subscribe_quote_ticks(inst_id)
            self.log.info(f"Registered dynamic instrument: {instrument.id.value[:50]} ...")
            return inst_id
        except Exception as e:
            self.log.error(f"Failed to register instrument for {condition_id[:20]}...: {e}")
            return None
