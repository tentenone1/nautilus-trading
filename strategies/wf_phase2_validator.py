"""Whale Follower — Phase 2 signal validation.

Validates signals against category and whale-type whitelist filters
for the $100 validation mode.
"""

from __future__ import annotations

from strategies.whale_tracker_new import WhaleSignal
from strategies.wf_constants import (
    ALLOWED_CATEGORIES,
    BLOCKED_CATEGORIES,
    ALLOWED_WHALE_TYPES,
    BLOCKED_WHALE_TYPES,
)


def validate_phase2_signal(
    *,
    signal: WhaleSignal,
    whale_classification: str = "",
    log,
) -> bool:
    """Validate a signal against Phase 2 whitelist filters.

    This function enforces strict category and whale type whitelists for
    the $100 validation mode. Every signal must pass both checks before
    entering the position sizing pipeline.

    CRITICAL: This check runs BEFORE position sizing to prevent any
    exposure to blocked categories or whale types.

    Args:
        signal: The WhaleSignal to validate.
        whale_classification: Whale type classification string
            (e.g., "skilled_human", "degenerate_human"). If empty,
            defaults to "unknown" which is blocked.
        log: Logger instance.

    Returns:
        True if signal passes whitelist checks, False if blocked.
        Logs the rejection reason for every blocked signal.
    """
    # Get market category from signal
    market_category = getattr(signal, "market_category", "") or ""
    if not market_category:
        # Fallback to general if not categorized
        market_category = "general"

    # Normalize category to lowercase for matching
    category_lower = market_category.lower()

    # Get whale name for logging
    whale_name = signal.whale_name or "unknown"
    market_title = (getattr(signal, "market_title", "") or "")[:50]

    # ── Category Whitelist Check ────────────────────────────────────────
    # First check BLOCKED_CATEGORIES (hard rejection)
    if category_lower in BLOCKED_CATEGORIES:
        log.info(
            "P2_BLOCK",
            extra={
                "category": category_lower,
                "whale": whale_name,
                "market": market_title,
            },
        )
        return False

    # Then check ALLOWED_CATEGORIES (must be in whitelist)
    if category_lower not in ALLOWED_CATEGORIES:
        log.info(
            "P2_BLOCK",
            extra={
                "category": category_lower,
                "reason": "not whitelisted",
                "whale": whale_name,
                "market": market_title,
            },
        )
        return False

    # ── Whale Type Whitelist Check ──────────────────────────────────────
    if not whale_classification:
        log.info(
            "P2_PASS",
            extra={
                "category": category_lower,
                "whale_type": "none",
                "whale": whale_name,
                "market": market_title,
            },
        )
        return True

    whale_type = whale_classification.lower()

    if whale_type in BLOCKED_WHALE_TYPES:
        log.info(
            "P2_BLOCK",
            extra={
                "whale_type": whale_type,
                "whale": whale_name,
                "market": market_title,
            },
        )
        return False

    if whale_type not in ALLOWED_WHALE_TYPES:
        log.info(
            "P2_BLOCK",
            extra={
                "whale_type": whale_type,
                "reason": "not whitelisted",
                "whale": whale_name,
                "market": market_title,
            },
        )
        return False

    log.info(
        "P2_PASS",
        extra={
            "category": category_lower,
            "whale_type": whale_type,
            "whale": whale_name,
            "market": market_title,
        },
    )
    return True
