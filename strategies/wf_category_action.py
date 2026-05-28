"""Per-category whale classification action lookups.

Provides get_category_action() — the single integration point for the signal
pipeline. Reads data/whale_category_classifications.json (built by
scripts/build_category_classifier.py from the historical trade DB).

Lookup hierarchy:
    1. whale + specific category  → category-specific action (highest priority)
    2. whale + no category data   → global action fallback
    3. whale not in file          → INSUFFICIENT_DATA (default)

Actions: FOLLOW | FADE | NEUTRAL | INSUFFICIENT_DATA
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

# Cache TTL in seconds — avoids re-loading JSON on every signal
_CACHE_TTL_SECONDS = 60.0

# Project data directory
_DATA_DIR = Path(__file__).parent.parent / "data"
_CLASSIFICATIONS_PATH = _DATA_DIR / "whale_category_classifications.json"

# Module-level cache
_cached_data: dict[str, Any] | None = None
_cache_load_time: float = 0.0


def _load_classifications() -> dict[str, Any]:
    """Load the classifications JSON, respecting the cache TTL."""
    global _cached_data, _cache_load_time
    now = time.monotonic()
    if _cached_data is not None and (now - _cache_load_time) < _CACHE_TTL_SECONDS:
        return _cached_data
    if not _CLASSIFICATIONS_PATH.exists():
        return {}
    _cached_data = json.loads(_CLASSIFICATIONS_PATH.read_text())
    _cache_load_time = now
    return _cached_data


def invalidate_cache() -> None:
    """Force the next call to re-load from disk."""
    global _cached_data, _cache_load_time
    _cached_data = None
    _cache_load_time = 0.0


def get_category_action(
    whale_name: str,
    category: str,
    *,
    classifications_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Returns the action for a (whale, category) pair.

    Args:
        whale_name:           Whale identifier (e.g. "SMCAOMCRL")
        category:             Market category (e.g. "sports", "crypto")
        classifications_path: Optional override path (default: data/whale_category_classifications.json)

    Returns:
        {
            "action":             "FOLLOW" | "FADE" | "NEUTRAL" | "INSUFFICIENT_DATA",
            "action_confidence":  float 0.0–1.0,
            "source":             "category_specific" | "global_fallback" | "default",
            "stats": {
                "total_trades":   int,
                "win_rate":       float,
                "avg_pnl":        float,
                "total_pnl":      float,
            }
        }
    """
    if not whale_name:
        return {
            "action": "INSUFFICIENT_DATA",
            "action_confidence": 0.0,
            "source": "default",
            "stats": {},
        }

    path = Path(classifications_path) if classifications_path else _CLASSIFICATIONS_PATH
    data = _load_classifications()

    if whale_name in data:
        entry = data[whale_name]

        # 1. Category-specific lookup
        if category and category in entry.get("categories", {}):
            cat_entry = entry["categories"][category]
            return {
                "action": cat_entry.get("action", "INSUFFICIENT_DATA"),
                "action_confidence": cat_entry.get("action_confidence", 0.3),
                "source": "category_specific",
                "stats": {
                    "total_trades": cat_entry.get("total_trades", 0),
                    "win_rate": cat_entry.get("win_rate", 0.0),
                    "avg_pnl": cat_entry.get("avg_pnl", 0.0),
                    "total_pnl": cat_entry.get("total_pnl", 0.0),
                },
            }

        # 2. Global fallback
        global_data = entry.get("global", {})
        if global_data:
            return {
                "action": global_data.get("global_action", "INSUFFICIENT_DATA"),
                "action_confidence": global_data.get("global_action_confidence", 0.3),
                "source": "global_fallback",
                "stats": {
                    "total_trades": global_data.get("total_trades", 0),
                    "win_rate": global_data.get("win_rate", 0.0),
                    "avg_pnl": global_data.get("avg_pnl", 0.0),
                    "total_pnl": global_data.get("total_pnl", 0.0),
                },
            }

    # 3. Unknown whale — default
    return {
        "action": "INSUFFICIENT_DATA",
        "action_confidence": 0.3,
        "source": "default",
        "stats": {},
    }


def get_follow_whales(category: str | None = None) -> list[str]:
    """Return all whale names classified as FOLLOW in the given category (or globally)."""
    data = _load_classifications()
    result = []
    for whale_name, entry in data.items():
        if whale_name in ("updated_at", "db_source", "db_row_count", "version"):
            continue
        if category:
            cat_entry = entry.get("categories", {}).get(category, {})
            if cat_entry.get("action") == "FOLLOW":
                result.append(whale_name)
        else:
            if entry.get("global", {}).get("global_action") == "FOLLOW":
                result.append(whale_name)
    return result


def get_fade_whales(category: str | None = None) -> list[str]:
    """Return all whale names classified as FADE in the given category (or globally)."""
    data = _load_classifications()
    result = []
    for whale_name, entry in data.items():
        if whale_name in ("updated_at", "db_source", "db_row_count", "version"):
            continue
        if category:
            cat_entry = entry.get("categories", {}).get(category, {})
            if cat_entry.get("action") == "FADE":
                result.append(whale_name)
        else:
            if entry.get("global", {}).get("global_action") == "FADE":
                result.append(whale_name)
    return result
