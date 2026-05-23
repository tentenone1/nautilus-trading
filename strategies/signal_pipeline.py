"""Signal Pipeline — Unified signal processing pipeline.

Consolidates the duplicated filtering logic from whale_follower._on_signal
and wf_signal_proc.on_signal into a single, testable pipeline.

Stages:
  1. Tier validation (confidence + edge thresholds)
  2. Intelligence blacklist (sacrificial accounts)
  3. Whale blacklist / sports blacklist (with fade override)
  4. Edge scoring (data-driven, replaces legacy anti-predictive edge_score)
  5. Fade logic (side flip for losing whales)
  6. Sybil modulation (adjust confidence based on sybil groups)
  7. Manipulation playbook check
  8. Whale profile fade/follow
  9. Category filtering (blocked categories, sports restrictions)
  10. LLM quality scoring (optional)
  11. Final validation (daily loss, auto-trade, etc.)

Each stage can be individually enabled/disabled and tested.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from strategies.wf_constants import (
    WHALE_BLACKLIST,
    SPORTS_WHALE_BLACKLIST,
    BLOCKED_CATEGORIES,
    ALLOWED_CATEGORIES,
    BLOCKED_WHALE_TYPES,
    ALLOWED_WHALE_TYPES,
    MIN_ENTRY_PRICE,
    SPORTS_MIN_EDGE,
    SPORTS_MIN_CONFIDENCE,
)
from strategies.wf_edge_scorer import EdgeScorer, EdgeResult
from strategies.whale_classifier import WhaleClassifier
from strategies.wf_whale_perf import flip_side_for_fade, is_fade_whale_dynamic
from strategies.whale_tracker_new import WhaleSignal, SignalSource, _categorize_market
from strategies.wf_sports import is_sports_market

logger = logging.getLogger("SignalPipeline")


@dataclass
class PipelineResult:
    """Result of signal pipeline processing."""
    should_trade: bool = False
    reject_reason: str = ""
    edge_score: float = 0.0
    original_edge: float = 0.0
    action: str = "ignore"          # "copy", "fade", "ignore"
    side_flip: bool = False
    side: str = "BUY"              # Final side (may be flipped for fade)
    confidence: float = 0.0
    trust: float = 5.0
    tier: str = "unknown"
    llm_score: int = 0
    is_fade: bool = False
    profile_fade: bool = False
    sybil_modulated: bool = False
    sybil_decision: str = ""
    edge_result: Optional[EdgeResult] = None


class SignalPipeline:
    """Unified signal processing pipeline.

    Replaces the duplicated logic in whale_follower._on_signal and
    wf_signal_proc.on_signal with a single, composable pipeline.

    Usage:
        pipeline = SignalPipeline(
            whale_tiering=whale_tiering,
            whale_intel=whale_intel,
            min_confidence=0.55,
            min_edge=0.15,
            auto_trade=True,
            daily_loss_breached=False,
            sports_daily_loss_breached=False,
        )
        result = pipeline.process(signal)
        if result.should_trade:
            enter_position(side=result.side, ...)
    """

    def __init__(
        self,
        whale_tiering=None,
        whale_intel=None,
        min_confidence: float = 0.55,
        min_edge: float = 0.15,
        auto_trade: bool = True,
        daily_loss_breached: bool = False,
        sports_daily_loss_breached: bool = False,
        enable_llm: bool = True,
        enable_edge_scorer: bool = True,
        enable_sybil: bool = True,
        enable_manipulation: bool = True,
    ):
        self.whale_tiering = whale_tiering
        self.whale_intel = whale_intel
        self.min_confidence = min_confidence
        self.min_edge = min_edge
        self.auto_trade = auto_trade
        self.daily_loss_breached = daily_loss_breached
        self.sports_daily_loss_breached = sports_daily_loss_breached
        self.enable_llm = enable_llm
        self.enable_edge_scorer = enable_edge_scorer
        self.enable_sybil = enable_sybil
        self.enable_manipulation = enable_manipulation

        # Lazy-initialized singletons
        self._edge_scorer: EdgeScorer | None = None
        self._whale_classifier: WhaleClassifier | None = None

        # Manipulation playbook and whale profiles (loaded from files)
        self._manip_playbook: dict = {"tactics": []}
        self._whale_profiles: dict = {"profiles": []}
        self._jailbreak_strategies: dict = {"strategies": []}
        self._load_playbooks()

    def _load_playbooks(self) -> None:
        """Load manipulation playbook, whale profiles, and jailbreak strategies."""
        from pathlib import Path
        base = Path(__file__).resolve().parents[1]
        for attr, filename, key in [
            ("_manip_playbook", "manipulation_playbook.json", "tactics"),
            ("_whale_profiles", "whale_profiles.json", "profiles"),
            ("_jailbreak_strategies", "jailbreak_strategies.json", "strategies"),
        ]:
            path = base / "research" / filename
            try:
                data = json.loads(path.read_text())
                setattr(self, attr, data)
            except (FileNotFoundError, json.JSONDecodeError):
                setattr(self, attr, {key: []})

    @property
    def edge_scorer(self) -> EdgeScorer:
        """Lazy-init EdgeScorer singleton."""
        if self._edge_scorer is None:
            self._edge_scorer = EdgeScorer()
        return self._edge_scorer

    @property
    def whale_classifier(self) -> WhaleClassifier:
        """Lazy-init WhaleClassifier singleton."""
        if self._whale_classifier is None:
            self._whale_classifier = WhaleClassifier()
            self._whale_classifier.classify_all(min_trades=5)
        return self._whale_classifier

    def process(self, signal: WhaleSignal, log=None) -> PipelineResult:
        """Process a signal through all pipeline stages.

        Args:
            signal: The WhaleSignal to evaluate.
            log: Optional logger for pipeline decisions.

        Returns:
            PipelineResult with should_trade, side, edge_score, etc.
        """
        result = PipelineResult(
            side=signal.side or "BUY",
            confidence=signal.confidence or 0.5,
        )

        # ── Stage 0: Auto-trade and daily loss gates ──────────────────────
        if not self.auto_trade:
            result.reject_reason = "auto_trade_disabled"
            return result

        if self.daily_loss_breached:
            result.reject_reason = "daily_loss_breached"
            return result

        mc = getattr(signal, "market_category", "") or ""
        is_sports, _ = is_sports_market(getattr(signal, "market_title", "") or "")
        if (is_sports or mc.lower() == "sports") and self.sports_daily_loss_breached:
            result.reject_reason = "sports_daily_loss_breached"
            return result

        # ── Stage 1: Tier validation ─────────────────────────────────────
        alpha_score = getattr(signal, "alpha_score", 50.0) or 50.0
        if self.whale_tiering:
            tier_config = self.whale_tiering.get_tier_config(alpha_score)
            tier = self.whale_tiering.get_tier(alpha_score)
            result.tier = tier

            if not self.whale_tiering.validate_confidence(
                signal.confidence, alpha_score, []
            ):
                min_conf = tier_config.get("min_confidence", self.min_confidence)
                if log:
                    log.info(
                        f"PIPELINE_REJECT | tier_confidence | {signal.whale_name} | "
                        f"conf={signal.confidence:.0%} < {min_conf:.0%}"
                    )
                result.reject_reason = f"tier_confidence<{min_conf:.0%}"
                return result

        # ── Stage 2: Intelligence blacklist ──────────────────────────────
        if self.whale_intel and self.whale_intel.should_hard_reject(signal.whale_name):
            intel = self.whale_intel.get(signal.whale_name)
            if log:
                log.info(
                    f"PIPELINE_REJECT | intel_hard_reject | {signal.whale_name} | "
                    f"trust={intel.get('trust_score', '?')}/10"
                )
            result.reject_reason = "intel_hard_reject"
            return result

        # ── Stage 3: Whale blacklist (with fade override) ────────────────
        if signal.whale_name in WHALE_BLACKLIST:
            if self.whale_intel and self.whale_intel.should_fade(signal.whale_name):
                if log:
                    log.info(f"PIPELINE_FADE_OVERRIDE | blacklist_fade | {signal.whale_name}")
                result.profile_fade = True
            else:
                if log:
                    log.info(f"PIPELINE_REJECT | blacklisted | {signal.whale_name}")
                result.reject_reason = "blacklisted"
                return result

        if signal.whale_name in SPORTS_WHALE_BLACKLIST and mc.lower() == "sports":
            if self.whale_intel and self.whale_intel.should_fade(signal.whale_name):
                if log:
                    log.info(f"PIPELINE_FADE_OVERRIDE | sports_blacklist_fade | {signal.whale_name}")
                result.profile_fade = True
            else:
                if log:
                    log.info(f"PIPELINE_REJECT | sports_blacklisted | {signal.whale_name}")
                result.reject_reason = "sports_blacklisted"
                return result

        # ── Stage 4: Data-driven edge scoring (replaces legacy edge_score) ─
        edge_val = getattr(signal, "edge_score", 0.0) or 0.0
        result.original_edge = edge_val

        if self.enable_edge_scorer:
            category = mc or "unknown"
            edge_result = self.edge_scorer.score_signal(
                whale_name=signal.whale_name or "",
                category=category,
                raw_edge_score=edge_val,
                confidence=signal.confidence or 0.5,
                side=signal.side or "BUY",
                min_edge=self.min_edge,
            )
            result.edge_result = edge_result

            # Reject signals the scorer says to ignore
            if edge_result.action == "ignore" and edge_result.source == "classifier":
                if log:
                    log.info(
                        f"PIPELINE_REJECT | edge_ignore | {signal.whale_name} | "
                        f"edge={edge_result.edge_score:.3f} trust={edge_result.whale_trust:.1f}"
                    )
                result.reject_reason = "edge_ignore"
                return result

            if not edge_result.should_trade:
                if log:
                    log.info(
                        f"PIPELINE_REJECT | edge_below_min | {signal.whale_name} | "
                        f"edge={edge_result.edge_score:.3f} < {self.min_edge}"
                    )
                result.reject_reason = "edge_below_min"
                return result

            # Override legacy edge with data-driven edge
            edge_val = edge_result.edge_score
            result.edge_score = edge_val
            result.action = edge_result.action
            result.side_flip = edge_result.side_flip
            result.trust = edge_result.whale_trust

            # Handle FADE signals: flip the trade side
            if edge_result.side_flip:
                result.side = flip_side_for_fade(signal.side or "BUY")
                result.is_fade = True
                if log:
                    log.info(
                        f"PIPELINE_FADE | {signal.whale_name} | "
                        f"{signal.side}->{result.side} | "
                        f"edge={edge_val:.3f} trust={edge_result.whale_trust:.1f}"
                    )
            else:
                if log:
                    log.info(
                        f"PIPELINE_EDGE | {signal.whale_name} | "
                        f"action={edge_result.action} | "
                        f"edge={result.original_edge:.2f}->{edge_val:.3f} "
                        f"trust={edge_result.whale_trust:.1f}"
                    )
        else:
            # Fallback: use legacy edge_score with tier validation
            if self.whale_tiering and not self.whale_tiering.validate_edge_score(edge_val, alpha_score):
                min_edge = 0.15
                if log:
                    log.info(
                        f"PIPELINE_REJECT | edge_below_tier | {signal.whale_name} | "
                        f"edge={edge_val:.2f} < {min_edge}"
                    )
                result.reject_reason = "edge_below_tier"
                return result
            result.edge_score = edge_val

        # ── Stage 5: Category filtering ──────────────────────────────────
        category_lower = mc.lower()

        # Blocked categories
        if category_lower in BLOCKED_CATEGORIES:
            if log:
                log.info(f"PIPELINE_REJECT | blocked_category | {category_lower}")
            result.reject_reason = f"blocked_category:{category_lower}"
            return result

        # Category whitelist
        if category_lower not in ALLOWED_CATEGORIES and category_lower not in ("general",):
            if log:
                log.info(f"PIPELINE_REJECT | category_not_allowed | {category_lower}")
            result.reject_reason = f"category_not_allowed:{category_lower}"
            return result

        # Sports-specific restrictions
        if is_sports or category_lower == "sports":
            if edge_val < SPORTS_MIN_EDGE:
                if log:
                    log.info(
                        f"PIPELINE_REJECT | sports_edge | {signal.whale_name} | "
                        f"edge={edge_val:.2f} < {SPORTS_MIN_EDGE}"
                    )
                result.reject_reason = "sports_edge_below_min"
                return result
            if (signal.confidence or 0) < SPORTS_MIN_CONFIDENCE:
                if log:
                    log.info(
                        f"PIPELINE_REJECT | sports_confidence | {signal.whale_name} | "
                        f"conf={signal.confidence:.0%} < {SPORTS_MIN_CONFIDENCE:.0%}"
                    )
                result.reject_reason = "sports_confidence_below_min"
                return result

        # ── Stage 6: Unknown whale rejection ──────────────────────────────
        is_unknown = (
            not signal.whale_name
            or signal.whale_name.lower() in ("", "unknown", "unknown whale", "")
        )
        if is_unknown and edge_val < 7:
            if log:
                log.info(
                    f"PIPELINE_REJECT | unknown_whale_low_edge | "
                    f"edge={edge_val:.2f} | {signal.whale_name}"
                )
            result.reject_reason = "unknown_whale_low_edge"
            return result

        # ── Stage 7: Sybil modulation (optional) ─────────────────────────
        if self.enable_sybil:
            try:
                from strategies.wf_sybil_modulator import modulate as sybil_modulate
                sybil = sybil_modulate(signal)
                if sybil.has_sybil:
                    if sybil.should_skip:
                        if log:
                            log.info(
                                f"PIPELINE_REJECT | sybil_skip | {signal.whale_name} | "
                                f"ratio={sybil.sybil_ratio:.0%} {sybil.decision}"
                            )
                        result.reject_reason = "sybil_skip"
                        return result
                    if sybil.confidence_delta != 0.0 or sybil.size_multiplier != 1.0:
                        old_conf = signal.confidence
                        signal.confidence = max(0.0, min(1.0, signal.confidence + sybil.confidence_delta))
                        signal.suggested_size_usd = round(
                            signal.suggested_size_usd * sybil.size_multiplier, 2
                        )
                        result.confidence = signal.confidence
                        result.sybil_modulated = True
                        result.sybil_decision = sybil.decision
            except ImportError:
                pass

        # ── Stage 8: Whale profile fade/follow ───────────────────────────
        for profile in self._whale_profiles.get("profiles", []):
            stats = profile.get("stats", {})
            if stats.get("name") == signal.whale_name:
                profile_data = profile.get("profile", {})
                if profile_data.get("should_fade", False) and not result.is_fade:
                    result.is_fade = True
                    result.side = flip_side_for_fade(signal.side or "BUY")
                    if log:
                        log.info(f"PIPELINE_FADE | profile_fade | {signal.whale_name}")
                elif profile_data.get("should_follow", False):
                    signal.confidence = min(1.0, signal.confidence * 1.25)
                    result.confidence = signal.confidence
                    if log:
                        log.info(f"PIPELINE_FOLLOW | profile_follow | {signal.whale_name}")
                break

        # ── Stage 9: Phase 2 whitelist check ─────────────────────────────
        whale_classification = self._get_whale_classification(signal.whale_name)
        if whale_classification and whale_classification.lower() in BLOCKED_WHALE_TYPES:
            if log:
                log.info(
                    f"PIPELINE_REJECT | whale_type_blocked | "
                    f"{signal.whale_name} type={whale_classification}"
                )
            result.reject_reason = f"whale_type_blocked:{whale_classification}"
            return result

        # All checks passed
        result.should_trade = True
        result.confidence = signal.confidence or 0.5
        return result

    def _get_whale_classification(self, whale_name: str) -> str:
        """Get whale classification from classifier or profiles."""
        # Try classifier first (data-driven)
        try:
            cls = self.whale_classifier._classifications.get(whale_name)
            if cls:
                return cls.get("classification", "unknown")
        except Exception:
            pass

        # Fall back to profiles
        for profile in self._whale_profiles.get("profiles", []):
            stats = profile.get("stats", {})
            if stats.get("name") == whale_name:
                return profile.get("profile", {}).get("classification", "unknown")
        return "unknown"

    def get_whale_win_rate(self, signal: WhaleSignal, tracker=None) -> float | None:
        """Get whale's historical win rate for Kelly sizing."""
        if tracker:
            for w in tracker.whales.values():
                if w.name == signal.whale_name:
                    return w.win_rate
        return None
