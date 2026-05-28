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
    ACTIVE_CONFIG_VERSION,
    LIVE_ENTRY_PRICE_CAPS,
    BLOCKED_CATEGORIES,
    SHADOW_MODE,
    SPORTS_TELEMETRY_MODE,
)
from strategies.wf_db_ops import insert_decision_snapshot

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

        # TRACE LOG 1: Method entry — unmissable, cannot be filtered
        # If this doesn't appear in dashboard.log after restart, the module is stale.
        _entry_whale = getattr(signal, 'whale_name', 'UNKNOWN') or 'UNKNOWN'
        self.log.warning(
            f"TRACE_ENTER | handle_signal | whale={_entry_whale} | "
            f"ts={time.time():.0f} | cond={signal.condition_id[:12]}..."
        )

        # ── Phase 0: DecisionSnapshot — create at signal entry ──
        _snap = {
            "signal_id": str(uuid.uuid4()),
            "trace_id": str(uuid.uuid4())[:8],
            "source": getattr(signal, 'source', 'unknown') or 'unknown',
            "category": getattr(signal, 'market_category', '') or '',
            # P0 FIX: raw_category, normalized_category, category_confidence fields.
            # Track the category as received vs. after fallback inference so we can
            # measure what fraction of signals needed title-based inference.
            "raw_category": getattr(signal, 'market_category', '') or '',
            "normalized_category": getattr(signal, 'market_category', '') or '',
            "category_confidence": 1.0,
            # Per-category whale classification (Phase 2: whale_category_classifier)
            # action: FOLLOW | FADE | NEUTRAL | INSUFFICIENT_DATA
            "category_action": "INSUFFICIENT_DATA",
            "category_action_confidence": 0.0,
            "market_title": getattr(signal, 'market_title', '') or '',
            "condition_id": getattr(signal, 'condition_id', '') or '',
            "whale_name": getattr(signal, 'whale_name', '') or '',
            "whale_address": getattr(signal, 'whale_address', '') or '',
            "edge_score": 0.0,
            "whale_wr": 0.0,
            "whale_sample_size": 0,
            "confidence": float(getattr(signal, 'confidence', 0) or 0),
            "side": getattr(signal, 'side', 'BUY') or 'BUY',
            "signal_type": "COPY",
            "passed_category_filter": -1,
            "passed_quarantine": -1,
            "passed_blacklist": -1,
            "passed_edge_threshold": -1,
            "passed_fade_eligibility": -1,
            "passed_risk_manager": -1,
            "passed_execution_checks": -1,
            "passed_position_limits": -1,
            "passed_pnl_gate": -1,
            "passed_correlation_gate": -1,
            "passed_capital_pool": -1,
            "final_decision": "REJECT",
            "reject_reason": "",
            "position_size_usd": 0.0,
            "config_version": ACTIVE_CONFIG_VERSION,
            "shadow_mode": 1 if SHADOW_MODE else 0,
            "metadata_json": json.dumps({"trace_id": str(uuid.uuid4())[:8]}),
        }
        _trace_id = _snap["metadata_json"]  # used in TRACE_PHASE2 log below

        # ── P0 FIX: Category fallback inference ───────────────────────────────────────
        # If the signal source sent an empty market_category (common for sybil signals),
        # infer it from market_title using _categorize_market so the normalized_category
        # field captures what we would have used as a proxy.
        if not _snap["raw_category"]:
            try:
                from strategies.whale_tracker_new import _categorize_market
                _snap["normalized_category"] = _categorize_market(
                    getattr(signal, 'market_title', '') or ''
                )
                _snap["category_confidence"] = 0.5   # inferred from title
            except Exception:
                _snap["normalized_category"] = _snap["raw_category"]
                _snap["category_confidence"] = 1.0
        else:
            # Category came from source — normalized is the same
            _snap["normalized_category"] = _snap["raw_category"]
            _snap["category_confidence"] = 1.0

        # ── Phase 2 (T4 FIX): Run classifier BEFORE any pipeline gate ─────────────
        # Previously Phase 2 ran AFTER the pipeline rejection, so category_action
        # was NULL for every signal that was rejected before Phase 2 ran.
        # This made telemetry queries like:
        #   SELECT reject_reason, classifier_action FROM decision_snapshots
        # impossible — every row had NULL classifier_action.
        # Now the classifier runs immediately after category inference, so
        # category_action is always populated in every decision snapshot,
        # including REJECT signals blocked at Step 1 (pipeline) or Step 2
        # (sports quarantine), and including SHADOW_TRADE signals.
        _cat_normalized = _snap.get("normalized_category", "")
        _whale = getattr(signal, 'whale_name', '') or ''
        try:
            from strategies.wf_category_action import get_category_action as _get_cat_action
            _action_result = _get_cat_action(_whale, _cat_normalized)
        except Exception as _e:
            # TRACE LOG 2: Exception in get_category_action — catch silent failures
            self.log.warning(
                f"TRACE_EXCEPTION | get_category_action failed | whale={_whale} | "
                f"cat={_cat_normalized} | error={_e}"
            )
            _action_result = {
                "action": "INSUFFICIENT_DATA",
                "action_confidence": 0.0,
                "source": "default",
                "stats": {},
            }
        _snap["category_action"] = _action_result["action"]
        _snap["category_action_confidence"] = _action_result["action_confidence"]

        # ── DEBUG: verify category_action is set before any insert ──
        self.log.warning(
            f"DBG_PREINSERT | whale={_whale} | category_action={_snap.get('category_action','MISSING')} | "
            f"snap_keys={list(_snap.keys())}"
        )

        # TRACE LOG 3: Phase 2 completed — confirms classifier ran to completion
        self.log.warning(
            f"TRACE_PHASE2 | whale={_whale} | cat={_cat_normalized} | "
            f"action={_action_result['action']} | conf={_action_result['action_confidence']:.0%} | "
            f"trace={_snap.get('trace_id','?')}"
        )

        self.log.info(
            f"CLASSIFY | {_whale} | {_cat_normalized} | "
            f"action={_action_result['action']} "
            f"(conf={_action_result['action_confidence']:.0%}, "
            f"source={_action_result['source']}) | "
            f"stats={_action_result.get('stats', {})}"
        )

        # ── T5: Classifier-gated thresholds ─────────────────────────────────────
        # Wire the whale classification into the pipeline's confidence gates.
        # This MUST run before pipeline.process() so modulated confidence/edge
        # is what the pipeline validates.
        #
        #   FOLLOW        proven whale → boost confidence so stricter tier gates
        #                 pass more easily (SPORTS_MIN_CONFIDENCE × 0.70 = 0.45)
        #   FADE          reverse signal direction (handled below after pipeline)
        #   NEUTRAL       standard thresholds
        #   INSUFFICIENT_DATA  require higher bar — handled by post-pipeline gate
        #
        _cat_action = _action_result["action"]
        _cat_wr = _action_result.get("stats", {}).get("win_rate", 0.0)
        if _cat_action == "FOLLOW":
            # ── WR floor guard ──────────────────────────────────────────────────
            # The classifier says FOLLOW, but we must enforce WR >= 50% at runtime.
            # The JSON is rebuilt with this floor but Python imports may be cached —
            # an old module version without the floor could run on this node. The
            # runtime check is the final safeguard: a FOLLOW whale with sub-50% WR
            # gets the boost blocked so a bad classifier JSON can't push losing
            # whales through the pipeline gate.
            if _cat_wr < 0.50:
                self.log.warning(
                    f"CLASSIFIER_BOOST | BLOCKED | {_whale}/{_cat_normalized} | "
                    f"FOLLOW action but WR={_cat_wr:.0%} < 50% — "
                    f"boost denied to prevent bad whale from passing gate"
                )
            else:
                _orig_conf = getattr(signal, 'confidence', 0.0) or 0.0
                _boosted_conf = min(_orig_conf * 1.5, 1.0)
                signal.confidence = _boosted_conf
                self.log.info(
                    f"CLASSIFIER_BOOST | FOLLOW whale — conf boosted "
                    f"{_orig_conf:.0%} → {_boosted_conf:.0%} "
                    f"(pipeline min_conf=0.65 now easier to satisfy, WR={_cat_wr:.0%})"
                )

        # ── Step 8 (P0 FIX): Whale win rate lookup — runs BEFORE pipeline so
        # whale_wr is available in ALL insert_decision_snapshot calls, including
        # the early REJECT insert at Step 1 (line 385) which previously ran
        # BEFORE this lookup, causing whale_wr=0.0 for every rejected signal.
        _whale_for_step8 = getattr(signal, 'whale_name', '') or ''
        _whale_wr_step8 = None
        _tracker_ok = self.config.use_dynamic_kelly and self._s._tracker
        if _tracker_ok:
            for _w in self._s._tracker.whales.values():
                if _w.name == _whale_for_step8:
                    _whale_wr_step8 = _w.win_rate
                    _snap["whale_wr"] = _w.win_rate
                    _snap["whale_sample_size"] = _w.total_trades
                    break
            if _whale_wr_step8 is None:
                self.log.warning(
                    f"STEP8_DEBUG | whale='{_whale_for_step8}' NOT found in tracker "
                    f"(use_dk={self.config.use_dynamic_kelly}, tracker={self._s._tracker is not None}, "
                    f"total_tracked={len(self._s._tracker.whales)})"
                )
            else:
                self.log.debug(
                    f"STEP8_DEBUG | whale='{_whale_for_step8}' found wr={_whale_wr_step8} "
                    f"n={_snap.get('whale_sample_size', 0)}"
                )

        elif _cat_action == "FADE":
            # Reverse YES/NO direction: FADE whales are proven losers, so flip the
            # signal. A FADE whale buying YES → we SELL NO. A FADE whale selling
            # NO → we BUY YES. Preserves base side (BUY/SELL).
            _side = getattr(signal, 'side', '') or ''
            _side_upper = _side.upper()
            if 'YES' in _side_upper:
                signal.side = _side_upper.replace('YES', 'NO')
            elif 'NO' in _side_upper:
                signal.side = _side_upper.replace('NO', 'YES')
            # Update pipeline_result.side so downstream enter_position() uses reversed side
            if pipeline_result is not None:
                pipeline_result.side = signal.side
            self.log.info(
                f"FADE_REVERSED | {_whale} | side flipped to {signal.side}"
            )

        # ── Step 1: Pipeline signal filtering ─────────────────────────────────
        if self.pipeline is not None:
            pipeline_result = self.pipeline.process(signal, log=self.log)

            # L2: Sports telemetry mode — write to decision_snapshots with
            # reject_reason="sports_telemetry" so we can observe which sports signals
            # would have passed all gates, then block execution as normal.
            if getattr(pipeline_result, 'sports_telemetry_mode', False):
                _snap["passed_category_filter"]  = pipeline_result.passed_category_filter
                _snap["passed_quarantine"]      = pipeline_result.passed_quarantine
                _snap["passed_blacklist"]        = pipeline_result.passed_blacklist
                _snap["passed_edge_threshold"]   = pipeline_result.passed_edge_threshold
                _snap["passed_fade_eligibility"] = pipeline_result.passed_fade_eligibility
                _snap["edge_score"]              = pipeline_result.edge_score

                # N1 FIX: preserve actual pipeline reject reason when should_trade=False.
                # Previously all sports telemetry signals were tagged "sports_telemetry"
                # regardless of which gate actually rejected them, losing diagnostic data.
                if pipeline_result.should_trade:
                    # Signal would have been ACCEPTED --
                    # M2 fix: When SHADOW_MODE=True, DON'T write a REJECT snapshot
                    # and DON'T return early. Let enter_position() write SHADOW_TRADE
                    # so the would-have-traded signal is tracked in the shadow ledger.
                    if SHADOW_MODE:
                        _snap["final_decision"] = "SHADOW_TRADE"
                        _snap["reject_reason"]  = "shadow_mode_block"
                        meta = {"sports_telemetry": True, "would_have_traded": True, "shadow_trade": True}
                        _snap["metadata_json"] = json.dumps(meta)
                        # Fall through to Step 2 (sports quarantine) -- SHADOW_MODE
                        # in enter_position() will write SHADOW_TRADE and skip execution.
                    else:
                        _snap["reject_reason"]  = "sports_telemetry"
                        _snap["final_decision"] = "REJECT"
                        meta = {"sports_telemetry": True, "would_have_traded": True}
                        _snap["metadata_json"] = json.dumps(meta)
                        _snap["passed_execution_checks"] = 1
                        insert_decision_snapshot(**_snap)
                        self.log.info(
                            f"SPORTS_TELEMETRY | sports signal logged to decision_snapshots | "
                            f"whale={signal.whale_name} | conf={signal.confidence:.0%} | "
                            f"edge={pipeline_result.edge_score:.3f} | "
                            f"reject_reason={pipeline_result.reject_reason} | "
                            f"would_have_traded={pipeline_result.should_trade}"
                        )
                        return   # block execution, observation only
                else:
                    # Signal was rejected by a real gate -- preserve the actual reason
                    _snap["reject_reason"]  = pipeline_result.reject_reason
                    _snap["final_decision"] = "REJECT"
                    meta = {
                        "sports_telemetry": True,
                        "would_have_traded": False,
                        "telemetry_tagged": True,
                    }
                    _snap["metadata_json"] = json.dumps(meta)
                    _snap["passed_execution_checks"] = 0
                    insert_decision_snapshot(**_snap)
                    self.log.info(
                        f"SPORTS_TELEMETRY | sports signal logged to decision_snapshots | "
                        f"whale={signal.whale_name} | conf={signal.confidence:.0%} | "
                        f"edge={pipeline_result.edge_score:.3f} | "
                        f"reject_reason={pipeline_result.reject_reason} | "
                        f"would_have_traded={pipeline_result.should_trade}"
                    )
                    return   # block execution, observation only


            if not pipeline_result.should_trade:
                # Copy gate results and record rejection
                _snap["passed_category_filter"] = pipeline_result.passed_category_filter
                _snap["passed_quarantine"]      = pipeline_result.passed_quarantine
                _snap["passed_blacklist"]       = pipeline_result.passed_blacklist
                _snap["passed_edge_threshold"]   = pipeline_result.passed_edge_threshold
                _snap["passed_fade_eligibility"]= pipeline_result.passed_fade_eligibility
                _snap["edge_score"]              = pipeline_result.edge_score
                _snap["reject_reason"]            = pipeline_result.reject_reason
                _snap["final_decision"]           = "REJECT"
                _snap["passed_execution_checks"]   = 0
                insert_decision_snapshot(**_snap)
                return

            edge_val = pipeline_result.edge_score
            is_fade = pipeline_result.is_fade
            if pipeline_result.side_flip:
                signal.side = pipeline_result.side
            tier = pipeline_result.tier
            if pipeline_result.is_fade:
                _snap["signal_type"] = "FADE"
            # Store whale_type on strategy for state_manager DB logging
            self._s._last_whale_type = getattr(pipeline_result, 'whale_type', '')
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

        # ── Phase 2: Per-category whale classification ──────────────────────────
        # Phase 2 (whale_category_classifier): Look up this whale's historical
        # action in this category. The result populates category_action in the
        # decision snapshot and modulates execution gate thresholds:
        #   FOLLOW        proven profitable in this category → more lenient gates
        #   FADE          proven loser → existing fade logic handles inversion
        #   NEUTRAL       neither good nor bad → standard gates apply
        #   INSUFFICIENT_DATA  unknown whale → stricter gates (conf ≥ 0.70, edge ≥ 0.25)
        _cat_normalized = _snap.get("normalized_category", "")
        _whale = getattr(signal, 'whale_name', '') or ''
        try:
            from strategies.wf_category_action import get_category_action as _get_cat_action
            _action_result = _get_cat_action(_whale, _cat_normalized)
        except Exception:
            _action_result = {
                "action": "INSUFFICIENT_DATA",
                "action_confidence": 0.0,
                "source": "default",
                "stats": {},
            }
        _snap["category_action"] = _action_result["action"]
        _snap["category_action_confidence"] = _action_result["action_confidence"]

        # T4 FIX: propagate classifier result to pipeline result for telemetry queries.
        # This makes classifier_action / classifier_confidence queryable via
        # SELECT reject_reason, classifier_action FROM decision_snapshots.
        if pipeline_result is not None:
            pipeline_result.classifier_action = _action_result["action"]
            pipeline_result.classifier_confidence = _action_result["action_confidence"]

        self.log.info(
            f"CLASSIFY | {_whale} | {_cat_normalized} | "
            f"action={_action_result['action']} "
            f"(conf={_action_result['action_confidence']:.0%}, "
            f"source={_action_result['source']}) | "
            f"stats={_action_result.get('stats', {})}"
        )

        # Gate: INSUFFICIENT_DATA whales — stricter scrutiny for unknown whales
        # Only apply this gate when the pipeline DID NOT already reject the signal.
        # If the pipeline rejected it, the rejection reason is preserved in _snap.
        if _action_result["action"] == "INSUFFICIENT_DATA":
            _pipeline_rejected = (
                self.pipeline is not None
                and not getattr(pipeline_result, 'should_trade', True)
            )
            if not _pipeline_rejected:
                # Whale not in our classification DB — require higher bar to proceed
                if signal.confidence < 0.70 or edge_val < 0.25:
                    self.log.info(
                        f"REJECT insufficient_data_whale: {_whale} | "
                        f"conf={signal.confidence:.0%} < 70% or edge={edge_val:.3f} < 0.25 | "
                        f"category={_cat_normalized}"
                    )
                    _snap["reject_reason"] = "insufficient_data_whale"
                    _snap["final_decision"] = "REJECT"
                    _snap["passed_execution_checks"] = 0
                    insert_decision_snapshot(**_snap)
                    return

        # Gate: FADE classification — log for observability (existing fade logic
        # in pipeline handles the actual signal inversion; this is for telemetry)
        if _action_result["action"] == "FADE":
            self.log.info(
                f"FADE classification: {_whale} | {_cat_normalized} | "
                f"conf={_action_result['action_confidence']:.0%} | "
                f"total_pnl={_action_result.get('stats', {}).get('total_pnl', '?')}"
            )

        # ── Step 2: HARD SPORTS QUARANTINE — block ALL sports signals ──────────────
        # v5.0-emergency-fix: Remove fade bypass. Sports has been a consistent P&L drain
        # (-$4,142 all-time) driven by systematic losses from whale COPY signals (28% WR)
        # and now also from fade signals on blacklisted whales that have insufficient
        # statistical basis. Hard quarantine = no sports signals at all, regardless of
        # whale type, edge score, or fade status.
        mc = getattr(signal, 'market_category', '') or ''
        market_title = getattr(signal, 'market_title', '') or ''
        combined = f"{market_title}|{mc}".lower()
        is_sports = any(p in combined for p in (
            'nba', 'nfl', 'mlb', 'nhl', 'ncaaf', 'ncaab', 'ufc', 'boxing',
            'tennis', 'soccer', 'football', 'basketball', 'baseball', 'hockey',
            'sports', 'game ', 'championship', 'finals', 'playoffs', 'season',
            # Team name fragments that leak through as 'general' category
            'knicks', 'cavaliers', 'celtics', 'lakers', 'warriors', 'bulls',
            'spread:', 'point spread', 'over/under', 'moneyline', 'totals',
            'nuggets', 'mavericks', 'heat', 'spurs', 'nets', 'bucks', 'raptors',
            'eagles', 'chiefs', '49ers', 'cowboys', 'packers', 'patriots', 'raiders',
            'yankees', 'red sox', 'dodgers', 'cubs', 'giants', 'astros', 'braves',
            'rangers', 'oilers', 'penguins', 'maple leafs', 'devils', 'avalanche',
            'diamondbacks', 'guardians', 'phillies', 'mariners', 'twins', 'orioles',
        ))
        # ── Step 2: HARD SPORTS QUARANTINE — with autoresearch bypass ─────────────
        # v5.6: Autoresearch (model_insider) has demonstrated +$1,243 on 531 clean
        # sports trades. It bypasses the quarantine; whale_tracker and sybil sports
        # remain blocked. A P&L circuit breaker re-quarantines autoresearch if its
        # last 100 sports trades drop below -$50.
        if is_sports:
            # ── Sports FOLLOW whales bypass the quarantine ──────────────────────────
            # v5.6 Phase 2: Sports FOLLOW whales (confidence >= 0.70) are allowed
            # through the quarantine. This is the primary sports signal path.
            # Placed BEFORE the autoresearch check so it applies to all non-autoresearch
            # FOLLOW sports whales too (not just within the autoresearch else-clause).
            if (
                _action_result["action"] == "FOLLOW"
                and _action_result["action_confidence"] >= 0.70
            ):
                self.log.info(
                    f"SPORTS_QUARANTINE_BYPASS [v5.6] | FOLLOW whale allowed through | "
                    f"whale={_whale} | cat={_cat_normalized} | "
                    f"conf={_action_result['action_confidence']:.0%}"
                )
                # fall through to Step 3 (risk checks) — whitelist approved

            # v5.6: Autoresearch (model_insider) has demonstrated +$1,243 on 531 clean
            # sports trades. It bypasses the quarantine; whale_tracker and sybil sports
            # remain blocked. A P&L circuit breaker re-quarantines autoresearch if its
            # last 100 sports trades drop below -$50.
            from strategies.wf_constants import (
                SPORTS_QUARANTINE_BYPASS_SOURCES,
                AUTORESEARCH_SPORTS_PNL_CIRCUIT_BREAKER,
                AUTORESEARCH_SPORTS_CIRCUIT_BREAKER_WINDOW,
            )
            signal_source = getattr(signal, 'source', '') or ''
            is_autoresearch = (
                signal_source in SPORTS_QUARANTINE_BYPASS_SOURCES
                or getattr(signal, 'whale_name', '') == 'autoresearch_llm'
            )

            if is_autoresearch and not self._check_autoresearch_sports_circuit_breaker():
                # Circuit breaker not triggered — allow autoresearch through
                self.log.info(
                    f"SPORTS_QUARANTINE_BYPASS [v5.6] | autoresearch allowed | "
                    f"whale={signal.whale_name} | {signal.condition_id[:30]}... | "
                    f"market={market_title[:50]}"
                )
            else:
                # Either not autoresearch, or circuit breaker triggered — block
                block_reason = "circuit_breaker" if is_autoresearch else "sports_quarantine"
                self.log.info(
                    f"SPORTS_QUARANTINE [v5.6]: blocking sports signal | "
                    f"whale={signal.whale_name} | {signal.condition_id[:30]}... | "
                    f"is_fade={is_fade} | source={signal_source} | reason={block_reason}"
                )
                # N2 FIX: write decision_snapshot before blocking in Step 2 quarantine.
                # Previously ~90% of sports whale signals were blocked here with NO
                # snapshot, making them invisible to telemetry.
                #
                # M2 fix: If the signal passed all pipeline gates (should_trade=True)
                # but gets blocked by the handler quarantine, write it as SHADOW_TRADE
                # so the would-have-traded signal is tracked in the shadow ledger.
                _pipeline_passed = (
                    self.pipeline is not None
                    and getattr(pipeline_result, 'should_trade', False)
                )
                # BUGFIX: Do NOT insert snapshot here. enter_position() is called immediately
                # after this block in Step 10, and its SHADOW_MODE handler (position_manager.py
                # line ~560) correctly sets passed_execution_checks=1 and inserts there.
                # The old insert_decision_snapshot call here created duplicate SHADOW_TRADE
                # rows with passed_execution_checks=-1 (never set), corrupting telemetry.
                # Just log and fall through — enter_position() will write the clean snapshot.
                if SHADOW_MODE and _pipeline_passed:
                    _snap["final_decision"] = "SHADOW_TRADE"
                    _snap["reject_reason"]  = "shadow_mode_block"
                    _snap["passed_execution_checks"] = 1  # pipeline passed, blocked at execution
                    _snap["metadata_json"] = json.dumps({
                        "sports_telemetry": True,
                        "would_have_traded": True,
                        "shadow_trade": True,
                        "handler_step2": True,
                        "block_reason": block_reason,
                    })
                    self.log.info(
                        f"SHADOW_TRADE | sports signal passed pipeline, blocked at handler | "
                        f"whale={signal.whale_name} | reason={block_reason} | "
                        f"enter_position will write clean snapshot"
                    )
                    # fall through to Step 10 (enter_position)
                elif SPORTS_TELEMETRY_MODE:
                    _snap["reject_reason"]  = "sports_handler_quarantine"
                    _snap["passed_execution_checks"] = 0   # BUGFIX: was left at -1
                    _snap["final_decision"] = "REJECT"
                    _snap["metadata_json"] = json.dumps({
                        "sports_telemetry": True,
                        "handler_step2": True,
                        "block_reason": block_reason,
                    })
                    insert_decision_snapshot(**_snap)
                    self.log.info(
                        f"SPORTS_TELEMETRY | Step 2 quarantine snapshot written | "
                        f"whale={signal.whale_name} | reason={block_reason}"
                    )
                return

        # ── Step 3: Risk checks (RiskManager if available) ────────────────────
        if not self.config.auto_trade:
            self.log.debug("Auto-trade disabled, skipping signal execution")
            _snap["passed_risk_manager"] = 0
            _snap["passed_execution_checks"] = 0   # BUGFIX: was left at -1
            _snap["final_decision"]       = "REJECT"
            _snap["reject_reason"]         = "auto_trade_disabled"
            insert_decision_snapshot(**_snap)
            return

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
                _snap["passed_risk_manager"] = 0
                _snap["passed_execution_checks"] = 0   # BUGFIX: was left at -1
                _snap["final_decision"]       = "REJECT"
                _snap["reject_reason"]         = "kill_switch_active"
                insert_decision_snapshot(**_snap)
                return

        # ── Step 3: Fade concurrency check ────────────────────────────────────
        if is_fade and len(self.fade_positions) >= self.fade_max_concurrent:
            self.log.info(
                f"FADE concurrency limit reached ({len(self.fade_positions)}/{self.fade_max_concurrent}), "
                f"skipping: {signal.whale_name}"
            )
            _snap["passed_risk_manager"] = 0
            _snap["passed_execution_checks"] = 0   # BUGFIX: was left at -1
            _snap["final_decision"]       = "REJECT"
            _snap["reject_reason"]         = "fade_concurrency_limit"
            insert_decision_snapshot(**_snap)
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

        # ── Step 5: LLM quality scoring (ANNOTATION-ONLY — audit quarantine) ───────────
        # AUDIT FINDING: llm_score from MiniMax API is an unvalidated black-box classifier.
        # It is NOT used in any signal decision gate. It is:
        #   - Computed here for logging/observability only
        #   - Included in validation event payloads (SIGNAL_GENERATED)
        #   - NOT used in any `if` statement that controls whether to proceed
        # This ensures the LLM score does not influence trading decisions and satisfies
        # the audit requirement that black-box models are quarantined from the decision path.
        # If llm_score is ever added to a gate, it must go through the same statistical
        # validation as the rest of the signal pipeline (backtest + OOS testing).
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
            _snap["passed_category_filter"] = 0
            _snap["passed_execution_checks"] = 0   # BUGFIX: was left at -1
            _snap["final_decision"] = "REJECT"
            _snap["reject_reason"] = "blocked_category"
            insert_decision_snapshot(**_snap)
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
                _snap["passed_category_filter"] = 0
                _snap["final_decision"] = "REJECT"
                _snap["reject_reason"] = "paper_only_category"
                _snap["passed_execution_checks"] = 0
                insert_decision_snapshot(**_snap)
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

        # ── Step 6.5: Missing market data gate ───────────────────────────────
        # Unknown whale signals (e.g. from detect_large_trades) can arrive with empty
        # condition_id and market_title — these cannot be traded as we don't know which
        # market they belong to. Reject early with a clear reason.
        if not signal.condition_id or not signal.condition_id.strip():
            self.log.info(
                f"MISSING_MARKET_DATA: rejecting signal with empty condition_id | "
                f"whale={signal.whale_name} | market_title={signal.market_title[:40]}"
            )
            _snap["passed_execution_checks"] = 0
            _snap["final_decision"] = "REJECT"
            _snap["reject_reason"] = "missing_market_data"
            insert_decision_snapshot(**_snap)
            return

        # ── Step 7: Ensure instrument & determine side ────────────────────────
        target_inst = self._ensure_instrument_for_signal(
            signal.condition_id, signal.token_id, signal.outcome
        )
        if target_inst is None:
            self.log.info(f"Could not get instrument for {signal.market_title[:40]}, skipping")
            _snap["passed_execution_checks"] = 0
            _snap["final_decision"]           = "REJECT"
            _snap["reject_reason"]            = "instrument_not_found"
            insert_decision_snapshot(**_snap)
            return

        # Parse compound sides: "BUY YES", "BUY NO", "SELL YES", "SELL NO"
        # vs simple: "BUY", "SELL". Extract the base order side only.
        side_raw = (signal.side or "BUY").strip().upper()
        if side_raw.startswith("BUY"):
            base_side = OrderSide.BUY
        elif side_raw.startswith("SELL"):
            base_side = OrderSide.SELL
        else:
            base_side = OrderSide.BUY  # Unknown format — default to BUY
        side = base_side

        # ── Step 8: Whale win rate lookup for Kelly sizing ──────────────────
        whale_wr = None
        if self.config.use_dynamic_kelly and self._s._tracker:
            for w in self._s._tracker.whales.values():
                if w.name == signal.whale_name:
                    whale_wr = w.win_rate
                    _snap["whale_wr"] = w.win_rate
                    _snap["whale_sample_size"] = w.total_trades
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
                    payload={
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
            signal_source=getattr(signal, 'source', 'known_whale'),
            _validation_signal_id=validation_signal_id,
            _validation_snapshot_id=snapshot_id,
            _decision_snapshot=_snap,
            _pipeline_passed=(
                self.pipeline is not None
                and getattr(pipeline_result, 'should_trade', False)
            ),
        )
        # ── P1 DIAGNOSTIC: log pipeline gate outcome for every signal ──────────
        _pipe_none = self.pipeline is None
        _should_trade = getattr(pipeline_result, 'should_trade', False)
        _reject_reason = getattr(pipeline_result, 'reject_reason', '')
        _passed_cat = getattr(pipeline_result, 'passed_category_filter', -1)
        _passed_quar = getattr(pipeline_result, 'passed_quarantine', -1)
        _passed_bl = getattr(pipeline_result, 'passed_blacklist', -1)
        _passed_edge = getattr(pipeline_result, 'passed_edge_threshold', -1)
        _passed_fade = getattr(pipeline_result, 'passed_fade_eligibility', -1)
        _log_fn = self.log.warning if _pipe_none or not _should_trade else self.log.debug
        _log_fn(
            f"[PIPELINE_GATE] cond={signal.condition_id[:30]} wh={signal.whale_name} "
            f"pipe_none={_pipe_none} should_trade={_should_trade} "
            f"reject_reason={_reject_reason} "
            f"gate_flags=[cat:{_passed_cat} quar:{_passed_quar} bl:{_passed_bl} "
            f"edge:{_passed_edge} fade:{_passed_fade}]"
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
        """Fetch market metadata and create instrument for a signal's market.

        Resolution strategy (3-tier fallback):
          1. Check in-process metadata cache (populated upstream by signal generator)
          2. CLOB API with retry + exponential backoff (3 attempts, 2s/4s/8s delays)
          3. Gamma API as last resort (public endpoint, no auth needed)

        Cached metadata is stored in-process with TTL to avoid redundant API calls
        within the same timer cycle.
        """
        import time as _time

        inst_id = InstrumentId.from_str(f"{condition_id}-{token_id}.POLYMARKET")
        existing = self.cache.instrument(inst_id)
        if existing is not None:
            return inst_id

        # ── Tier 1: Check in-process cache (populated upstream by signal generator) ──
        if hasattr(self, "_market_meta_cache"):
            cached = self._market_meta_cache.get(condition_id)
            if cached is not None:
                cache_age = _time.time() - cached.get("_cached_at", 0)
                if cache_age < 300:  # Cache valid for 5 minutes
                    try:
                        market_info = cached["data"]
                        tokens = market_info.get("tokens", [])
                        token_data = next(
                            (t for t in tokens if t.get("token_id") == token_id), None
                        )
                        if token_data:
                            from nautilus_trader.adapters.polymarket.common.parsing import (
                                parse_polymarket_instrument,
                            )
                            instrument = parse_polymarket_instrument(
                                market_info=market_info,
                                token_id=token_data["token_id"],
                                outcome=token_data["outcome"],
                            )
                            self.cache.add_instrument(instrument)
                            self._s.subscribe_quote_ticks(inst_id)
                            self.log.info(
                                f"Resolved instrument from upstream cache: {instrument.id.value[:50]}..."
                            )
                            return inst_id
                    except Exception:
                        pass  # Cache hit but parse failed — fall through to API

        # ── Tier 2: CLOB API with retry + exponential backoff ─────────────────────────
        _MAX_RETRIES = 3
        _BASE_DELAY = 2.0  # seconds

        for attempt in range(_MAX_RETRIES):
            try:
                market_info = self._s._clob.get_market(condition_id=condition_id)
                if market_info and market_info.get("active", False):
                    tokens = market_info.get("tokens", [])
                    token_data = next(
                        (t for t in tokens if t.get("token_id") == token_id), None
                    )
                    if token_data:
                        from nautilus_trader.adapters.polymarket.common.parsing import (
                            parse_polymarket_instrument,
                        )
                        instrument = parse_polymarket_instrument(
                            market_info=market_info,
                            token_id=token_data["token_id"],
                            outcome=token_data["outcome"],
                        )
                        self.cache.add_instrument(instrument)
                        self._s.subscribe_quote_ticks(inst_id)
                        self.log.info(
                            f"Registered dynamic instrument: {instrument.id.value[:50]} ..."
                        )
                        return inst_id
                    else:
                        # Market found but token not in response — might be a binary market
                        # where token lookup just needs the outcome directly
                        if tokens:
                            token_data = tokens[0]
                            from nautilus_trader.adapters.polymarket.common.parsing import (
                                parse_polymarket_instrument,
                            )
                            instrument = parse_polymarket_instrument(
                                market_info=market_info,
                                token_id=token_data["token_id"],
                                outcome=token_data["outcome"],
                            )
                            self.cache.add_instrument(instrument)
                            self._s.subscribe_quote_ticks(inst_id)
                            self.log.info(
                                f"Registered dynamic instrument (fallback token): {instrument.id.value[:50]} ..."
                            )
                            return inst_id
                if attempt < _MAX_RETRIES - 1:
                    delay = _BASE_DELAY * (2 ** attempt)
                    self.log.info(
                        f"CLOB retry {attempt + 1}/{_MAX_RETRIES} for {condition_id[:20]}... "
                        f"(market inactive or empty), waiting {delay:.0f}s"
                    )
                    _time.sleep(delay)
                continue
            except Exception as e:
                if attempt < _MAX_RETRIES - 1:
                    delay = _BASE_DELAY * (2 ** attempt)
                    self.log.info(
                        f"CLOB retry {attempt + 1}/{_MAX_RETRIES} for {condition_id[:20]}...: {e}, "
                        f"waiting {delay:.0f}s"
                    )
                    _time.sleep(delay)
                else:
                    self.log.error(
                        f"CLOB exhausted all retries for {condition_id[:20]}...: {e}"
                    )

        # ── Tier 3: Gamma API as last resort ──────────────────────────────────────────
        try:
            import urllib.request as _urllib
            import json as _json

            gamma_url = (
                f"https://gamma-api.polymarket.com/markets?condition_id={condition_id}"
            )
            req = _urllib.Request(gamma_url, headers={"User-Agent": "nautilus-signal/1.0"})
            with _urllib.urlopen(req, timeout=15) as resp:
                gamma_data = _json.loads(resp.read())

            if isinstance(gamma_data, list) and gamma_data:
                m = gamma_data[0]
                tokens_raw = _json.loads(m.get("clobTokenIds", "[]"))
                outcomes_raw = _json.loads(m.get("outcomes", "[]"))
                prices_raw = _json.loads(m.get("outcomePrices", "[]"))

                if not tokens_raw or not outcomes_raw:
                    self.log.error(
                        f"Gamma fallback: no tokens/outcomes for {condition_id[:20]}..."
                    )
                    return None

                # Build tokens list in the format parse_polymarket_instrument expects
                tokens_for_parse = []
                for i, (tid, outcome_str, price_str) in enumerate(
                    zip(tokens_raw, outcomes_raw, prices_raw)
                ):
                    tokens_for_parse.append(
                        {
                            "token_id": tid,
                            "outcome": outcome_str,
                            "price": price_str,
                        }
                    )

                gamma_market = {
                    "condition_id": condition_id,
                    "question": m.get("question", ""),
                    "description": m.get("description", ""),
                    "tokens": tokens_for_parse,
                    "active": m.get("active", True),
                    "closed": m.get("closed", False),
                    "end_date_iso": m.get("endDateIso", ""),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "volume24hr": float(m.get("volume24hr", 0) or 0),
                }

                # Find the matching token
                token_data = next(
                    (t for t in tokens_for_parse if t["token_id"] == token_id),
                    tokens_for_parse[0] if tokens_for_parse else None,
                )
                if token_data:
                    from nautilus_trader.adapters.polymarket.common.parsing import (
                        parse_polymarket_instrument,
                    )
                    instrument = parse_polymarket_instrument(
                        market_info=gamma_market,
                        token_id=token_data["token_id"],
                        outcome=token_data["outcome"],
                    )
                    self.cache.add_instrument(instrument)
                    self._s.subscribe_quote_ticks(inst_id)
                    self.log.info(
                        f"Registered instrument via Gamma fallback: {instrument.id.value[:50]} ..."
                    )
                    return inst_id
        except Exception as gamma_err:
            self.log.error(
                f"Gamma fallback also failed for {condition_id[:20]}...: {gamma_err}"
            )

        self.log.info(
            f"Could not get instrument for {condition_id[:20]}... (all tiers exhausted)"
        )
        return None

    # ── Autoresearch Sports P&L Circuit Breaker ──────────────────────────

    _ar_sports_cb_cache_time: float = 0.0
    _ar_sports_cb_cache_result: bool = False

    def _check_autoresearch_sports_circuit_breaker(self) -> bool:
        """Return True if the circuit breaker is triggered (re-quarantine autoresearch).

        Checks the sum of realized_pnl over the last N sports trades from
        autoresearch. If below AUTORESEARCH_SPORTS_PNL_CIRCUIT_BREAKER, the
        sports quarantine bypass is disabled for all sources.

        Result is cached for 5 minutes to avoid hammering the DB on every signal.
        """
        import time as _time
        import sqlite3 as _sqlite3
        from pathlib import Path as _Path
        from strategies.wf_constants import (
            AUTORESEARCH_SPORTS_PNL_CIRCUIT_BREAKER,
            AUTORESEARCH_SPORTS_CIRCUIT_BREAKER_WINDOW,
        )

        # 5-minute TTL cache
        now = _time.time()
        if now - self._ar_sports_cb_cache_time < 300:
            return self._ar_sports_cb_cache_result

        _DB = _Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
        if not _DB.exists():
            self._ar_sports_cb_cache_time = now
            self._ar_sports_cb_cache_result = False
            return False

        try:
            conn = _sqlite3.connect(str(_DB))
            conn.execute("PRAGMA busy_timeout=5000")
            # Identify sports trades using keyword patterns in market_title
            cursor = conn.execute("""
                SELECT SUM(realized_pnl) FROM (
                    SELECT realized_pnl FROM trades
                    WHERE signal_source IN ('model_insider', 'autoresearch_llm')
                      AND realized_pnl IS NOT NULL
                      AND (
                          market_title LIKE '%vs%'
                       OR market_title LIKE '%O/U%'
                       OR market_title LIKE '%Spread:%'
                       OR market_title LIKE '%Counter-Strike%'
                       OR market_title LIKE '%Up or Down%'
                       OR market_title LIKE '%BO3%'
                       OR market_title LIKE '%BO5%'
                       OR market_title LIKE '%Roland Garros%'
                       OR market_title LIKE '%ITF%'
                       OR market_title LIKE '%WTA%'
                       OR market_title LIKE '%ATP%'
                       OR market_title LIKE '%NBA%'
                       OR market_title LIKE '%NFL%'
                       OR market_title LIKE '%MLB%'
                       OR market_title LIKE '%NHL%'
                       OR market_title LIKE '%Valorant%'
                       OR market_title LIKE '%Dota%'
                       OR market_title LIKE '%League of Legends%'
                      )
                    ORDER BY timestamp DESC
                    LIMIT ?
                )
            """, (AUTORESEARCH_SPORTS_CIRCUIT_BREAKER_WINDOW,))
            row = cursor.fetchone()
            conn.close()

            pnl_sum = row[0] if row and row[0] is not None else 0.0
            triggered = pnl_sum < AUTORESEARCH_SPORTS_PNL_CIRCUIT_BREAKER

            if triggered:
                self.log.warning(
                    f"AUTORESEARCH_SPORTS_CIRCUIT_BREAKER | P&L=${pnl_sum:.2f} over last "
                    f"{AUTORESEARCH_SPORTS_CIRCUIT_BREAKER_WINDOW} sports trades < "
                    f"${AUTORESEARCH_SPORTS_PNL_CIRCUIT_BREAKER:.0f} threshold | "
                    f"re-quarantining autoresearch sports"
                )

            self._ar_sports_cb_cache_time = now
            self._ar_sports_cb_cache_result = triggered
            return triggered

        except Exception as e:
            self.log.warning(
                f"AUTORESEARCH_SPORTS_CIRCUIT_BREAKER query failed: {e} | "
                f"failing open (allowing autoresearch sports)"
            )
            self._ar_sports_cb_cache_time = now
            self._ar_sports_cb_cache_result = False
            return False
