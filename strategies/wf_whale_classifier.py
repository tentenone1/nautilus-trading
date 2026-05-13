"""Whale Follower — Whale classification helpers.

Fade/follow classification, strategy confidence scoring,
and manipulation signal detection.
"""

from __future__ import annotations

from strategies.wf_signal_config import (
    _MANIPULATION_PLAYBOOK,
    _WHALE_PROFILES,
    _JAILBREAK_STRATEGIES,
    _load_manip_playbook,
    _load_whale_profiles,
)


def _is_manipulation_signal(signal_data: dict) -> bool:
    """Check if signal matches manipulation playbook pattern.

    Hot-reloads the playbook before every check so edits take effect immediately.

    Args:
        signal_data: Signal dictionary with whale_sig or whale_name.

    Returns:
        True if the signal matches a known manipulation pattern.
    """
    _load_manip_playbook()
    whale_sig = signal_data.get("whale_sig", "") or signal_data.get("whale_name", "")
    if not whale_sig:
        return False
    for tactic in _MANIPULATION_PLAYBOOK.get("tactics", []):
        pattern = tactic.get("whale_sig", "")
        if pattern and pattern.lower() in whale_sig.lower():
            return True
    return False


def _is_fade_whale(whale_name: str) -> bool:
    """Check if whale has should_fade=True in profiles (hot-reloads before check).

    Args:
        whale_name: Name of the whale to check.

    Returns:
        True if the whale profile has should_fade set to True.
    """
    _load_whale_profiles()
    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            return bool(profile_data.get("should_fade", False))
    return False


def _is_follow_whale(whale_name: str) -> bool:
    """Check if whale has should_follow=True in profiles (hot-reloads before check).

    Args:
        whale_name: Name of the whale to check.

    Returns:
        True if the whale profile has should_follow set to True.
    """
    _load_whale_profiles()
    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            return bool(profile_data.get("should_follow", False))
    return False


def _get_strategy_confidence(strategy_name: str) -> float | None:
    """Get confidence score for a jailbreak strategy.

    Args:
        strategy_name: Name of the jailbreak strategy.

    Returns:
        Confidence score as a float, or None if strategy not found.
    """
    for strat in _JAILBREAK_STRATEGIES.get("strategies", []):
        if strat.get("name") == strategy_name:
            return float(strat.get("confidence", 0))
    return None


def get_whale_classification(whale_name: str) -> str:
    """Get whale classification from whale profiles data.

    Args:
        whale_name: The whale name to look up.

    Returns:
        Classification string (e.g., "skilled_human", "degenerate_human")
        or "unknown" if not found.
    """
    if not whale_name:
        return "unknown"

    _load_whale_profiles()
    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            classification = profile_data.get("classification", "")
            if classification:
                return classification.lower()
            if profile_data.get("should_fade", False):
                return "degenerate_human"
            if profile_data.get("should_follow", False):
                return "skilled_human"

    return "unknown"
