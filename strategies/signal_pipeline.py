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
    SPORTS_WHITELIST_PATTERNS,
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


# ── Proven Whale Fast Lane (v5.2) ──────────────────────────────────────────
# Whales with >=20 trades AND >=60% WR in a specific category (non-sports only)
# bypass tier_confidence entirely — their track record speaks for itself.
FAST_LANE_MIN_TRADES = 20
FAST_LANE_MIN_WIN_RATE = 0.60

# ── Graduated tier confidence (v5.2) ────────────────────────────────────────
# Lower thresholds for proven categories; 40% remains default for unknowns.
CATEGORY_MIN_CONFIDENCE = {
    "general": 0.30,
    "geopolitics": 0.35,
    "politics": 0.35,
    "crypto": 0.40,
    "economics": 0.35,
    "technology": 0.40,
}


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
    whale_type: str = ""  # Whale classification for data segmentation
    skip_edge_scorer: bool = False  # v5.6: bypass edge scorer for premium signal sources


class SignalPipeline:
    """Unified signal processing pipeline.

    Replaces the duplicated logic in whale_follower._on_signal and
    wf_signal_proc.on_signal with a single, composable pipeline.

    Usage:
        pipeline = SignalPipeline(
            whale_tiering=whale_tiering,
            whale_intel=whale_intel,
            min_confidence=0.55,
            min_edge=0.10,
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
        min_edge: float = 0.10,
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

        # Load poly_data historical profiles
        try:
            from pathlib import Path as _P
            _poly_path = _P("/home/elon-1/workspace/nautilus-trading/data/poly_historical_profiles.json")
            if _poly_path.exists():
                self._poly_profiles = json.loads(_poly_path.read_text())
                if self._poly_profiles:
                    logger.info("Loaded poly_data profiles for classification: %d", len(self._poly_profiles))
        except Exception:
            pass


        # Load polymarket_analyzer snapshot for market liquidity data
        self._analyzer_snapshot: dict = {}
        try:
            from pathlib import Path as _AP
            _analyzer_path = _AP("/home/elon-1/workspace/nautilus-trading/data/polymarket_analyzer_snapshot.json")
            if _analyzer_path.exists():
                self._analyzer_snapshot = json.loads(_analyzer_path.read_text())
                _mkts = self._analyzer_snapshot.get("markets", [])
                if _mkts:
                    logger.info("Loaded analyzer snapshot: %d markets", len(_mkts))
        except Exception:
            pass

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


        # ── Stage 0.5: Proven whale fast lane (v5.2 + v5.6 fix) ─────
        # Whales with >=20 trades AND >=60% WR in a specific category (non-sports)
        # bypass tier_confidence entirely. Their track record speaks for itself.
        # v5.6 FIX: Also bypass edge scorer for autoreseartch_llm — the edge scorer's
        # fallback returns "ignore" for unknown whales, killing our best BUY signal source.
        is_fast_lane = False
        if not (is_sports or mc.lower() == "sports") and signal.whale_name and signal.whale_name != "unknown":
            is_fast_lane = self._check_fast_lane(signal.whale_name, mc, log=log)
            if is_fast_lane and log:
                log.info(
                    f"PIPELINE_FAST_LANE | {signal.whale_name} | category={mc} | "
                    f"bypassing tier_confidence"
                )
            if is_fast_lane and self.whale_tiering:
                alpha_score_fast = getattr(signal, "alpha_score", 50.0) or 50.0
                result.tier = self.whale_tiering.get_tier(alpha_score_fast)
            # autoreseartch_llm fast lane also skips the edge scorer — its signals
            # bypass the tier check but then hit the edge scorer which "ignores" them
            # because it's not in whale_classifications.json
            if signal.whale_name == "autoresearch_llm":
                result.skip_edge_scorer = True
                if log:
                    log.info(
                        f"PIPELINE_FAST_LANE | autoresearch_llm | "
                        f"bypassing edge_scorer (v5.6 fix)"
                    )

        # ── Stage 1: Tier validation ─────────────────────────────────────
        # Fade signals bypass tier validation — fading works regardless of whale tier
        is_whitelist_fade = signal.whale_name in WHALE_BLACKLIST
        alpha_score = getattr(signal, "alpha_score", 50.0) or 50.0
        if self.whale_tiering and not is_whitelist_fade and not is_fast_lane:
            tier_config = self.whale_tiering.get_tier_config(alpha_score)
            tier = self.whale_tiering.get_tier(alpha_score)
            result.tier = tier

            # Use the LOWER of tier-specified min and category-graduated min (v5.6).
            # Previously this was only applied to the log message after validate_confidence
            # failed — but validate_confidence enforces the tier floor (0.40 for emerging)
            # which is stricter than the category floor (0.25 for general). The tier floor
            # blocks non-sports signals that should pass via the category floor.
            effective_min_conf = CATEGORY_MIN_CONFIDENCE.get(
                mc.lower(), self.min_confidence
            )
            tier_min_conf = tier_config.get("min_confidence", effective_min_conf)
            min_conf = min(tier_min_conf, effective_min_conf)

            if signal.confidence < min_conf:
                if log:
                    log.info(
                        f"PIPELINE_REJECT | tier_confidence | {signal.whale_name} | "
                        f"conf={signal.confidence:.0%} < {min_conf:.0%}"
                    )
                result.reject_reason = f"tier_confidence<{min_conf:.0%}"
                return result
        elif self.whale_tiering:
            tier = self.whale_tiering.get_tier(alpha_score)
            result.tier = tier

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

        # ── Stage 3: Whale blacklist (fade by default, hard-reject only sacrificial) ─
        # v5.0-emergency-fix: All blacklist fades now require statistical confirmation:
        # >=10 trades in that category AND win rate <25%. Prevents fading on insufficient data.

        if signal.whale_name in WHALE_BLACKLIST:
            if self.whale_intel and self.whale_intel.should_hard_reject(signal.whale_name):
                if log:
                    log.info(f"PIPELINE_REJECT | blacklist_hard_reject | {signal.whale_name}")
                result.reject_reason = "blacklist_hard_reject"
                return result
            else:
                # Blacklisted whales DEFAULT to fade — they are on the list because they lose
                # BUT only if statistical threshold is met (>=10 trades, <25% WR in category)
                if self._check_fade_eligibility(signal.whale_name, mc, log=log):
                    if log:
                        log.info(f"PIPELINE_FADE | blacklist_fade | {signal.whale_name}")
                    result.profile_fade = True
                    result.is_fade = True
                    result.side = flip_side_for_fade(signal.side or "BUY")
                else:
                    if log:
                        log.info(
                            f"PIPELINE_IGNORE | fade_insufficient_data | {signal.whale_name} | "
                            f"category={mc} — skipping fade (<10 trades or WR >=25%)"
                        )
                    result.reject_reason = "fade_insufficient_data"

        if signal.whale_name in SPORTS_WHALE_BLACKLIST and mc.lower() == "sports":
            if self.whale_intel and self.whale_intel.should_hard_reject(signal.whale_name):
                if log:
                    log.info(f"PIPELINE_REJECT | sports_hard_reject | {signal.whale_name}")
                result.reject_reason = "sports_hard_reject"
                return result
            else:
                if self._check_fade_eligibility(signal.whale_name, mc, log=log):
                    if log:
                        log.info(f"PIPELINE_FADE | sports_blacklist_fade | {signal.whale_name}")
                    result.profile_fade = True
                    result.is_fade = True
                    result.side = flip_side_for_fade(signal.side or "BUY")
                else:
                    if log:
                        log.info(
                            f"PIPELINE_IGNORE | sports_fade_insufficient_data | {signal.whale_name} | "
                            f"category={mc} — skipping sports fade (<10 trades or WR >=25%)"
                        )
                    result.reject_reason = "sports_fade_insufficient_data"

        # ── Stage 4: Data-driven edge scoring (replaces legacy edge_score) ─
        edge_val = getattr(signal, "edge_score", 0.0) or 0.0
        result.original_edge = edge_val

        if self.enable_edge_scorer and not result.skip_edge_scorer:
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

            # For pre-existing fade signals (from Stage 3 blacklist), edge scorer is advisory only
            # It should NOT reject or override the fade decision
            already_fading = result.is_fade

            if not already_fading:
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
            result.trust = edge_result.whale_trust

            if already_fading:
                # Stage 3 already set the fade side — keep it, but use edge score for sizing
                if log:
                    log.info(
                        f"PIPELINE_FADE | {signal.whale_name} | blacklist_fade_confirmed | "
                        f"edge={edge_val:.3f} trust={edge_result.whale_trust:.1f}"
                    )
            else:
                result.action = edge_result.action
                result.side_flip = edge_result.side_flip

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
                min_edge = 0.10
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
        if category_lower in BLOCKED_CATEGORIES and not (result.profile_fade or result.is_fade):
            if log:
                log.info(f"PIPELINE_REJECT | blocked_category | {category_lower}")
            result.reject_reason = f"blocked_category:{category_lower}"
            return result

        # Category whitelist (fade signals bypass — fading works in any category)
        if not (result.profile_fade or result.is_fade) and category_lower not in ALLOWED_CATEGORIES and category_lower not in ("general",):
            if log:
                log.info(f"PIPELINE_REJECT | category_not_allowed | {category_lower}")
            result.reject_reason = f"category_not_allowed:{category_lower}"
            return result

        # ── Phase A2 Sports Quarantine — with autoresearch bypass ─────────────────
        # v5.6: Autoresearch (model_insider) bypasses the sports quarantine.
        # Whale_tracker and sybil sports signals remain quarantined.
        if is_sports or category_lower == "sports":
            signal_source = getattr(signal, 'source', '') or ''
            from strategies.wf_constants import SPORTS_QUARANTINE_BYPASS_SOURCES
            is_autoresearch = (
                signal_source in SPORTS_QUARANTINE_BYPASS_SOURCES
                or getattr(signal, 'whale_name', '') == 'autoresearch_llm'
            )
            if is_autoresearch:
                # Autoresearch allowed through — fall through to remaining pipeline checks
                if log:
                    log.info(
                        f"PIPELINE_SPORTS_BYPASS | autoresearch allowed | "
                        f"whale={signal.whale_name} | market={getattr(signal, 'market_title', '')[:50]}"
                    )
            else:
                # Non-autoresearch: apply existing whitelist/quarantine logic
                market_title = getattr(signal, "market_title", "") or ""
                whitelist_hit = any(
                    re.search(p, market_title, re.IGNORECASE)
                    for p in SPORTS_WHITELIST_PATTERNS
                )
                if whitelist_hit:
                    if log:
                        log.info(
                            f"PIPELINE_SPORTS_WHITELIST_HIT | {signal.whale_name} | "
                            f"market={market_title[:60]}"
                        )
                else:
                    if log:
                        log.info(
                            f"PIPELINE_REJECT | sports_quarantine | {signal.whale_name} | "
                            f"market={market_title[:40]}"
                        )
                    result.reject_reason = "sports_quarantine"
                    return result

        # Sports-specific restrictions (skip for fade signals — fading losing whales)
        # NOTE: this block is now dead code for sports markets since the quarantine
        # above rejects ALL sports unconditionally. Kept for documentation.
        if (is_sports or category_lower == "sports") and not (result.profile_fade or result.is_fade):
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
        # Fade signals bypass whale type blocking — fading works on any whale type
        if not result.is_fade:
            whale_classification = self._get_whale_classification(signal.whale_name)
            result.whale_type = whale_classification or "unknown"
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


    def get_market_liquidity(self, condition_id: str) -> dict:
        # Get market liquidity from analyzer snapshot
        for m in self._analyzer_snapshot.get("markets", []):
            if m.get("conditionId", "").lower() == condition_id.lower():
                return {
                    "volume_24h": m.get("volume24hr", 0),
                    "best_bid": m.get("bestBid", 0),
                    "best_ask": m.get("bestAsk", 0),
                    "spread": m.get("bestAsk", 1) - m.get("bestBid", 0),
                    "slug": m.get("slug", ""),
                }
        return {}

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
        # Fall back to poly_data historical profiles (7976 addresses)
        try:
            for p in self._poly_profiles:
                if p.get("address") == whale_name or p.get("name") == whale_name:
                    return p.get("classification", "unknown")
        except Exception:
            pass
        return "unknown"

    # ── Fade eligibility threshold (v5.0-emergency-fix) ─────────────────────────
    # Only allow fade signals when the whale has statistically significant evidence:
    # >=10 trades in that category AND win rate <25%.
    # This prevents fading on insufficient data.

    FADE_MIN_TRADES = 10       # Minimum trades in category before fade is allowed
    FADE_MAX_WIN_RATE = 0.25     # Win rate must be below this for fade to be allowed

    def _check_fade_eligibility(
        self,
        whale_name: str,
        category: str,
        log=None,
    ) -> bool:
        """Check if a whale qualifies for a statistical fade in a given category.

        Returns True only if the whale has >=10 trades in that category
        with win rate <25%. Falls back to True if the DB query fails
        (fail-open — allow the fade, log a warning).

        Args:
            whale_name: Name of the whale to check.
            category: Market category to check (e.g. "sports", "general").
            log: Optional logger.

        Returns:
            True if fade is statistically justified, False otherwise.
        """
        import sqlite3
        from pathlib import Path

        db_path = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
        if not db_path.exists():
            if log:
                log.warning(
                    f"FADE_ELIGIBILITY: trades.db not found, allowing fade by default | {whale_name}"
                )
            return True  # Fail open — allow the fade, log a warning

        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA busy_timeout=5000")
            row = conn.execute(
                """
                SELECT COUNT(*), SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
                FROM trades
                WHERE whale_name = ?
                  AND LOWER(category) = LOWER(?)
                  AND realized_pnl IS NOT NULL
                """,
                (whale_name, category),
            ).fetchone()
            conn.close()

            if row is None or row[0] is None:
                if log:
                    log.warning(
                        f"FADE_ELIGIBILITY: no data for {whale_name} in {category}, allowing fade | "
                        f"fail-open behavior"
                    )
                return True  # Fail open

            trade_count = row[0]
            wins = row[1] or 0
            if trade_count < self.FADE_MIN_TRADES:
                if log:
                    log.info(
                        f"FADE_ELIGIBILITY: {whale_name} category={category} — "
                        f"{trade_count} trades < {self.FADE_MIN_TRADES} required | NOT eligible for fade"
                    )
                return False

            win_rate = wins / trade_count
            eligible = win_rate < self.FADE_MAX_WIN_RATE
            if log:
                if eligible:
                    log.info(
                        f"FADE_ELIGIBILITY: {whale_name} category={category} — "
                        f"{trade_count} trades, WR={win_rate:.1%} < {self.FADE_MAX_WIN_RATE:.0%} | ELIGIBLE"
                    )
                else:
                    log.info(
                        f"FADE_ELIGIBILITY: {whale_name} category={category} — "
                        f"{trade_count} trades, WR={win_rate:.1%} >= {self.FADE_MAX_WIN_RATE:.0%} | NOT eligible"
                    )
            return eligible

        except Exception as e:
            if log:
                log.warning(
                    f"FADE_ELIGIBILITY: DB query failed for {whale_name}/{category}: {e} | "
                    f"allowing fade (fail-open)"
                )
            return True  # Fail open — DB errors should not block fades


    # ── Proven Whale Fast Lane (v5.2) ──────────────────────────────────────────

    def _check_fast_lane(
        self,
        whale_name: str,
        category: str,
        log=None,
    ) -> bool:
        """Check if a whale qualifies for the proven whale fast lane.

        Fast lane whales bypass tier_confidence because their track record
        is strong enough that confidence thresholds are unnecessary noise.

        Criteria: >=20 trades AND >=60% WR in this specific category (non-sports).
        Also fast-lanes autoresearch_llm unconditionally (our best signal source).

        Returns True if the whale qualifies for the fast lane.
        """
        # autoresearch_llm is our best signal source — always fast lane it
        if whale_name == "autoresearch_llm":
            return True

        import sqlite3
        from pathlib import Path

        db_path = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
        if not db_path.exists():
            return False

        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA busy_timeout=5000")
            row = conn.execute(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
                FROM trades
                WHERE whale_name = ?
                  AND LOWER(category) = LOWER(?)
                  AND realized_pnl IS NOT NULL
                """,
                (whale_name, category),
            ).fetchone()
            conn.close()

            if row is None or row[0] is None or row[0] < FAST_LANE_MIN_TRADES:
                return False

            trade_count = row[0]
            wins = row[1] or 0
            win_rate = wins / trade_count
            qualifies = win_rate >= FAST_LANE_MIN_WIN_RATE

            if log and qualifies:
                log.info(
                    f"FAST_LANE | {whale_name} category={category} — "
                    f"{trade_count} trades, WR={win_rate:.1%} >= {FAST_LANE_MIN_WIN_RATE:.0%} | QUALIFIES"
                )
            return qualifies

        except Exception as e:
            if log:
                log.warning(f"FAST_LANE: DB query failed for {whale_name}/{category}: {e}")
            return False

    def get_whale_win_rate(self, signal: WhaleSignal, tracker=None) -> float | None:
        """Get whale's historical win rate for Kelly sizing."""
        if tracker:
            for w in tracker.whales.values():
                if w.name == signal.whale_name:
                    return w.win_rate
        return None
