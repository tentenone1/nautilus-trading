"""Signal router utilities for whale follower strategy.
Provides fade/follow detection, classification, and market filtering.
All functions are pure and receive required state via arguments.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Set

from strategies.wf_constants import (
    WHALE_BLACKLIST,
    SPORTS_WHALE_BLACKLIST,
    SPORTS_WHITELIST_PATTERNS,
    SPORTS_OU_BLACKLIST_PATTERNS,
    SPORTS_VS_BLACKLIST_PATTERNS,
    SINGLE_TEAM_PATTERNS,
    ALLOWED_CATEGORIES,
    BLOCKED_CATEGORIES,
)
from strategies.wf_whale_classifier import _is_fade_whale, _is_follow_whale


# ── Fade Detection ───────────────────────────────────────────────────────


def should_fade_signal(*, whale_name: str, whale_intel: Optional[Dict[str, Any]] = None, log) -> tuple[bool, Dict[str, Any]]:
    """Check fade eligibility via profile, intel or blacklist override."""
    fade_info: Dict[str, Any] = {}

    if _is_fade_whale(whale_name):
        fade_info.update(source="whale_profiles", classification="fade_profile")
        log.info(f"FADE profile: {whale_name}", extra={"whale_name": whale_name})
        return True, fade_info
    if whale_intel and whale_intel.get("should_fade", False):
        ts = whale_intel.get("trust_score", 10)
        if ts <= 2:
            fade_info.update(source="whale_intel", classification=whale_intel.get("classification", "unknown"), trust_score=ts)
            log.info(f"FADE intel: {whale_name} trust={ts}", extra={"whale_name": whale_name})
            return True, fade_info
    if whale_name in WHALE_BLACKLIST and whale_intel and whale_intel.get("should_fade", False):
        log.info(f"FADE blacklist override: {whale_name}", extra={"whale_name": whale_name})
        fade_info["source"] = "blacklist_override"
        return True, fade_info
    return False, fade_info


def should_follow_signal(*, whale_name: str, whale_intel: Optional[Dict[str, Any]] = None, log) -> tuple[bool, Dict[str, Any]]:
    """Check follow eligibility via profile or intel."""
    info: Dict[str, Any] = {}
    if _is_follow_whale(whale_name):
        info.update(source="whale_profiles", classification="hidden_partner")
        log.info(f"FOLLOW profile: {whale_name}", extra={"whale_name": whale_name})
        return True, info
    if whale_intel:
        cls = whale_intel.get("classification", "")
        ts = whale_intel.get("trust_score", 0)
        if cls == "skilled_human" and ts >= 7:
            info.update(source="whale_intel", classification=cls, trust_score=ts)
            log.info(f"FOLLOW intel: {whale_name} trust={ts}", extra={"whale_name": whale_name})
            return True, info
    return False, info


# ── Signal Filtering ───────────────────────────────────────────────────────


def is_blacklisted_whale(*, whale_name: str, blacklist: Set[str] = WHALE_BLACKLIST, sports_blacklist: Set[str] = SPORTS_WHALE_BLACKLIST, market_category: str = "", log) -> tuple[bool, str]:
    """Return (True, reason) if whale is blacklisted; empty reason otherwise."""
    if whale_name in blacklist:
        log.info(f"BLACKLISTED: {whale_name}", extra={"whale_name": whale_name})
        return True, "blacklisted_general"
    if whale_name in sports_blacklist and market_category.lower() == "sports":
        log.info(f"SPORTS BL: {whale_name}", extra={"whale_name": whale_name})
        return True, "blacklisted_sports"
    return False, ""


def is_sports_market_allowed(
    *,
    market_title: str,
    log,
) -> tuple[bool, str]:
    """Check if sports market passes whitelist/blacklist filters.

    Whitelist: Spread bets, major leagues.
    Blacklist: O/U markets, vs markets, single-team winners.

    Args:
        market_title: Market title to check.
        log: Logger instance.

    Returns:
        (allowed, reason) tuple.
    """
    title = market_title or ""

    # Whitelist check
    is_whitelisted = any(
        re.search(p, title, re.IGNORECASE)
        for p in SPORTS_WHITELIST_PATTERNS
    )
    if is_whitelisted:
        log.info(
            f"ALLOW Spread/major league: {title[:60]}",
            extra={"title": title[:80], "filter": "whitelist"},
        )
        return True, "whitelisted"

    # O/U blacklist
    if any(re.search(p, title, re.IGNORECASE) for p in SPORTS_OU_BLACKLIST_PATTERNS):
        log.info(
            f"REJECT O/U: {title[:60]}",
            extra={"title": title[:80], "filter": "ou_blacklist"},
        )
        return False, "ou_blacklist"

    # VS blacklist
    if any(re.search(p, title, re.IGNORECASE) for p in SPORTS_VS_BLACKLIST_PATTERNS):
        log.info(
            f"REJECT vs: {title[:60]}",
            extra={"title": title[:80], "filter": "vs_blacklist"},
        )
        return False, "vs_blacklist"

    # Single-team blacklist
    if any(re.search(p, title, re.IGNORECASE) for p in SINGLE_TEAM_PATTERNS):
        log.info(
            f"REJECT single-team: {title[:60]}",
            extra={"title": title[:80], "filter": "single_team"},
        )
        return False, "single_team"

    return True, ""


def is_category_allowed(
    *,
    market_category: str,
    log,
) -> tuple[bool, str]:
    """Check if market category is allowed (Phase 2 whitelist).

    Args:
        market_category: Market category string.
        log: Logger instance.

    Returns:
        (allowed, reason) tuple.
    """
    mc_lower = (market_category or "general").lower()

    # Blocked categories
    if mc_lower in BLOCKED_CATEGORIES:
        log.info(
            f"REJECT category: {mc_lower}",
            extra={"category": mc_lower, "filter": "blocked"},
        )
        return False, "blocked_category"

    # Allowed categories
    if mc_lower in ALLOWED_CATEGORIES:
        return True, "allowed"

    # Unknown category — default reject
    log.info(
        f"REJECT unknown category: {mc_lower}",
        extra={"category": mc_lower, "filter": "not_whitelisted"},
    )
    return False, "category_not_whitelisted"


# ── Whale Classification ───────────────────────────────────────────────────


def classify_whale(
    *,
    whale_name: str,
    whale_intel: Optional[Dict[str, Any]] = None,
    whale_tiering: Optional[Any] = None,
    alpha_score: float = 50.0,
) -> Dict[str, Any]:
    """Get whale classification from intelligence data or tiering.

    Args:
        whale_name: Whale wallet name.
        whale_intel: Optional whale intelligence dict.
        whale_tiering: Optional WhaleTiering instance.
        alpha_score: Whale alpha score for tiering.

    Returns:
        Dict with classification, trust_score, tier, etc.
    """
    result: Dict[str, Any] = {
        "whale_name": whale_name,
        "classification": "unknown",
        "trust_score": 0,
        "tier": "unknown",
        "should_fade": False,
        "should_follow": False,
    }

    # Intelligence data
    if whale_intel:
        result["classification"] = whale_intel.get("classification", "unknown")
        result["trust_score"] = whale_intel.get("trust_score", 0)
        result["should_fade"] = whale_intel.get("should_fade", False)

    # Tiering
    if whale_tiering:
        tier = whale_tiering.get_tier(alpha_score)
        tier_config = whale_tiering.get_tier_config(alpha_score)
        result["tier"] = tier
        result["tier_config"] = tier_config

        # Check cached tier
        cached_config = whale_tiering.get_cached_tier(whale_name)
        if cached_config.get("max_position_usd", 0) > 0:
            result["cached_tier"] = cached_config

    return result


def apply_fade_transformation(
    *,
    signal_side: str,
    signal_outcome: str,
    signal_size_usd: float,
    fade_multiplier: float = 0.5,
    log,
) -> tuple[str, str, float]:
    """Transform a signal for fade mode (invert side, reduce size).

    Args:
        signal_side: Original side ("buy" or "sell").
        signal_outcome: Original outcome ("YES" or "NO").
        signal_size_usd: Original suggested size.
        fade_multiplier: Size reduction multiplier.
        log: Logger instance.

    Returns:
        (new_side, new_outcome, new_size) tuple.
    """
    # Invert side
    new_side = "sell" if signal_side == "buy" else "buy"

    # Invert outcome
    new_outcome = "NO" if signal_outcome == "YES" else "YES"

    # Reduce size
    new_size = round(signal_size_usd * fade_multiplier, 2)

    log.info(
        f"FADE transformation: {signal_side}→{new_side} | "
        f"{signal_outcome}→{new_outcome} | "
        f"${signal_size_usd:.0f}→${new_size:.0f}",
        extra={
            "original_side": signal_side,
            "new_side": new_side,
            "original_outcome": signal_outcome,
            "new_outcome": new_outcome,
            "original_size": signal_size_usd,
            "new_size": new_size,
        },
    )

    return new_side, new_outcome, new_size


def apply_follow_boost(
    *,
    signal_confidence: float,
    confidence_multiplier: float = 1.25,
    max_confidence: float = 1.0,
    log,
) -> float:
    """Boost signal confidence for follow mode (hidden partner).

    Args:
        signal_confidence: Original confidence.
        confidence_multiplier: Boost multiplier.
        max_confidence: Maximum allowed confidence.
        log: Logger instance.

    Returns:
        Boosted confidence (clamped to max).
    """
    boosted = min(signal_confidence * confidence_multiplier, max_confidence)
    log.info(
        f"FOLLOW boost: confidence {signal_confidence:.0%}→{boosted:.0%}",
        extra={
            "original_confidence": signal_confidence,
            "boosted_confidence": boosted,
            "multiplier": confidence_multiplier,
        },
    )
    return boosted


# ── Signal Router Main ────────────────────────────────────────────────────


def route_whale_signal(
    *,
    signal,
    config,
    whale_intel: Optional[Dict[str, Any]] = None,
    whale_tiering: Optional[Any] = None,
    fade_positions: Set[str],
    fade_max_concurrent: int,
    log,
) -> tuple[bool, str, Optional[Dict[str, Any]]]:
    """Route a whale signal through all filters and transformations.

    Main routing pipeline:
    1. Tier validation (confidence + edge_score thresholds)
    2. Blacklist rejection (hard reject or fade-eligible)
    3. Intelligence hard reject (trust_score 0)
    4. Category whitelist validation
    5. Sports whitelist/blacklist validation
    5. Fade/follow detection
    6. Apply transformations if needed

    Args:
        signal: WhaleSignal object.
        config: WhaleFollowerConfig.
        whale_intel: Optional whale intelligence dict.
        whale_tiering: Optional WhaleTiering instance.
        fade_positions: Active fade positions for concurrency check.
        fade_max_concurrent: Max concurrent fade trades.
        log: Logger instance.

    Returns:
        (should_process, reason, transformed_signal) tuple.
        transformed_signal is None if rejected, contains modifications
        if fade/follow applied.
    """
    whale_name = signal.whale_name or "unknown"
    market_category = getattr(signal, "market_category", "") or ""
    market_title = getattr(signal, "market_title", "") or ""
    alpha_score = getattr(signal, "alpha_score", 50.0) or 50.0
    confidence = signal.confidence or 0.5
    edge_score = getattr(signal, "edge_score", 0.0) or 0.0

    transformed: Dict[str, Any] = {
        "is_fade": False,
        "is_follow": False,
        "confidence_boost": 0.0,
        "size_multiplier": 1.0,
    }

    # ── 1. Tier validation ──
    if whale_tiering:
        min_conf = whale_tiering.get_tier_config(alpha_score).get(
            "min_confidence", config.min_confidence
        )
        if not whale_tiering.validate_confidence(confidence, alpha_score, []):
            log.info(
                f"REJECT tier confidence: {whale_name} | "
                f"conf={confidence:.0%} < {min_conf:.0%}",
                extra={"whale_name": whale_name, "confidence": confidence, "min_conf": min_conf},
            )
            return False, "tier_confidence_below_threshold", None

        min_edge = whale_tiering.get_tier_config(alpha_score).get("min_edge_score", 0.15)
        if not whale_tiering.validate_edge_score(edge_score, alpha_score):
            log.info(
                f"REJECT tier edge: {whale_name} | edge={edge_score:.2f} < {min_edge:.2f}",
                extra={"whale_name": whale_name, "edge_score": edge_score, "min_edge": min_edge},
            )
            return False, "tier_edge_below_threshold", None

    # ── 2. Intelligence hard reject ──
    if whale_intel and whale_intel.get("trust_score", 10) == 0:
        log.info(
            f"REJECT intel trust=0: {whale_name}",
            extra={"whale_name": whale_name, "trust_score": 0},
        )
        return False, "intelligence_hard_reject", None

    # ── 3. Blacklist check (hard reject or fade-eligible) ──
    is_blacklisted, blacklist_reason = is_blacklisted_whale(
        whale_name=whale_name,
        market_category=market_category,
        log=log,
    )
    if is_blacklisted:
        # Check if intel marks as fade-eligible
        if whale_intel and whale_intel.get("should_fade", False):
            log.info(
                f"BLACKLISTED whale FADE eligible: {whale_name}",
                extra={"whale_name": whale_name, "reason": blacklist_reason},
            )
            # Continue to fade processing below
        else:
            return False, blacklist_reason, None

    # ── 4. Category whitelist ──
    allowed, cat_reason = is_category_allowed(market_category=market_category, log=log)
    if not allowed:
        return False, cat_reason, None

    # ── 5. Sports market whitelist ──
    if market_category.lower() == "sports":
        allowed, sports_reason = is_sports_market_allowed(
            market_title=market_title, log=log
        )
        if not allowed:
            return False, sports_reason, None

    # ── 6. Fade detection ──
    should_fade, fade_info = should_fade_signal(
        whale_name=whale_name,
        whale_intel=whale_intel,
        log=log,
    )
    if should_fade:
        # Check concurrency limit
        if len(fade_positions) >= fade_max_concurrent:
            log.info(
                f"FADE concurrency limit: {len(fade_positions)}/{fade_max_concurrent}",
                extra={"whale_name": whale_name, "current_fades": len(fade_positions)},
            )
            return False, "fade_concurrency_limit", None

        # Apply fade transformation
        new_side, new_outcome, new_size = apply_fade_transformation(
            signal_side=signal.side,
            signal_outcome=signal.outcome,
            signal_size_usd=signal.suggested_size_usd,
            log=log,
        )
        transformed["is_fade"] = True
        transformed["new_side"] = new_side
        transformed["new_outcome"] = new_outcome
        transformed["new_size"] = new_size

    # ── 7. Follow boost ──
    should_follow, follow_info = should_follow_signal(
        whale_name=whale_name,
        whale_intel=whale_intel,
        log=log,
    )
    if should_follow and not should_fade:
        boosted_confidence = apply_follow_boost(
            signal_confidence=confidence,
            log=log,
        )
        transformed["is_follow"] = True
        transformed["confidence_boost"] = boosted_confidence - confidence

    return True, "signal_approved", transformed