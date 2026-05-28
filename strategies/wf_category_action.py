"""Per-category whale classification action lookups.

Provides get_category_action() — the single integration point for the signal
pipeline. Reads data/whale_category_classifications.json (built by
scripts/build_category_classifier.py from the historical trade DB).

Lookup hierarchy (v6.0):
    1. whale + specific category  → category-specific action (highest priority)
    2. whale + no category data → global action fallback
    3. whale in pending_whales  → INSUFFICIENT_DATA (probation — see below)
    4. whale not in file       → INSUFFICIENT_DATA (default)

Probation system (v6.0):
    Whales discovered by discover_whales_cron.py that are not yet in the
    classifier JSON are placed in pending_whales with status='probation'.
    They remain INSUFFICIENT_DATA until they accumulate >= 10 observed trades.
    The classifier is rebuilt by build_category_classifier.py (run periodically)
    which promotes eligible pending whales into the JSON.

Actions: FOLLOW | FADE | NEUTRAL | INSUFFICIENT_DATA
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

# Cache TTL in seconds — avoids re-loading JSON on every signal
_CACHE_TTL_SECONDS = 60.0

# Project data directory
_DATA_DIR = Path(__file__).parent.parent / "data"
_CLASSIFICATIONS_PATH = _DATA_DIR / "whale_category_classifications.json"
_DISCOVERY_DB_PATH   = _DATA_DIR / "whale_discovery.db"

# Probation threshold: whales need >= 10 observed trades before classification
_PROBATION_MIN_TRADES = 10

# Module-level caches
_cached_data: dict[str, Any] | None = None
_cache_load_time: float = 0.0
_pending_cache: dict[str, dict] | None = None
_pending_cache_time: float = 0.0
_PENDING_CACHE_TTL: float = 30.0   # shorter TTL — pending whales change more frequently


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


def _load_pending_whales() -> dict[str, dict]:
    """Load pending_whales from the discovery DB.

    Returns a dict keyed by whale_name → {status, observed_trades, ...}.
    Only probation whales (status='probation') are returned.
    """
    global _pending_cache, _pending_cache_time
    now = time.monotonic()
    if _pending_cache is not None and (now - _pending_cache_time) < _PENDING_CACHE_TTL:
        return _pending_cache
    _pending_cache = {}
    if not _DISCOVERY_DB_PATH.exists():
        return {}
    try:
        conn = sqlite3.connect(str(_DISCOVERY_DB_PATH))
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute("""
            SELECT whale_name, status, observed_trades,
                   win_rate, avg_pnl, total_pnl, updated_at
            FROM pending_whales
            WHERE status = 'probation'
        """).fetchall()
        conn.close()
        for (name, status, trades, wr, avg_pnl, total_pnl, updated_at) in rows:
            if name:
                _pending_cache[str(name)] = {
                    "status": status,
                    "observed_trades": trades,
                    "win_rate": wr or 0.0,
                    "avg_pnl": avg_pnl or 0.0,
                    "total_pnl": total_pnl or 0.0,
                    "updated_at": updated_at,
                }
    except Exception:
        pass
    _pending_cache_time = now
    return _pending_cache


def invalidate_cache() -> None:
    """Force the next call to re-load from disk (both classifier JSON and pending DB)."""
    global _cached_data, _cache_load_time, _pending_cache, _pending_cache_time
    _cached_data = None
    _cache_load_time = 0.0
    _pending_cache = None
    _pending_cache_time = 0.0


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
            "source":             "category_specific" | "global_fallback" | "probation" | "default",
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

    # 3. Check pending_whales — newly discovered whales in probation
    # (not yet in the classifier JSON, but tracked in the DB)
    pending = _load_pending_whales()
    if whale_name in pending:
        pd = pending[whale_name]
        trades = pd["observed_trades"]
        return {
            "action": "INSUFFICIENT_DATA",
            "action_confidence": round(min(trades / _PROBATION_MIN_TRADES, 1.0) * 0.5, 3),
            # Confidence grows as whale accumulates trades toward the 10-trade minimum
            # 0 trades → 0.0, 10 trades → 0.5 (still insufficient but building evidence)
            "source": "probation",
            "stats": {
                "total_trades": trades,
                "win_rate": pd["win_rate"],
                "avg_pnl": pd["avg_pnl"],
                "total_pnl": pd["total_pnl"],
            },
        }

    # 4. Unknown whale — default
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
