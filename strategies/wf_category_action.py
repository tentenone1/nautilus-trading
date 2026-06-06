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
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Cache TTL in seconds — avoids re-loading JSON on every signal
_CACHE_TTL_SECONDS = 60.0

# Project data directory
_DATA_DIR = Path(__file__).parent.parent / "data"
_CLASSIFICATIONS_PATH = _DATA_DIR / "whale_category_classifications.json"
_DISCOVERY_DB_PATH   = _DATA_DIR / "whale_discovery.db"

# Probation threshold: whales need >= 10 observed trades before classification
_PROBATION_MIN_TRADES = 10

# Activity gate thresholds
_ACTIVITY_SIGNAL_HOURS = 48        # whales must have signal in last 48h
_ACTIVITY_DISCOVERY_DAYS = 7        # whales must be in discovery DB within 7 days

# Module-level caches
_cached_data: dict[str, Any] | None = None
_cache_load_time: float = 0.0
_pending_cache: dict[str, dict] | None = None
_pending_cache_time: float = 0.0
_PENDING_CACHE_TTL: float = 30.0   # shorter TTL — pending whales change more frequently

# Active whale cache: names seen in decision_snapshots in last 48h
_active_whale_cache: set[str] | None = None
_active_whale_cache_time: float = 0.0
_ACTIVE_CACHE_TTL: float = 300.0   # refresh every 5 min

# Inactive override cache: whales that are INACTIVE (not seen recently)
# Value: reason string
_inactive_cache: dict[str, str] = {}
_inactive_cache_time: float = 0.0
_INACTIVE_CACHE_TTL = 60.0


def _sanitize_action(action_value: Any, context: str = "") -> str:
    """Return a safe action string, falling back to INSUFFICIENT_DATA for bad data.

    Hardens against None, empty string, non-string, or unexpected values
    in the classification JSON. Logs a warning for every fallback so the
    operator can spot bad classifier output.
    """
    if isinstance(action_value, str) and action_value.strip():
        return action_value.strip()
    logger.warning(
        "invalid_classification_action | context=%s | value=%r",
        context,
        action_value,
    )
    # None, empty, blank, or non-string → safe fallback
    return "INSUFFICIENT_DATA"


def _load_classifications() -> dict[str, Any]:
    """Load the classifications JSON, respecting the cache TTL.

    Hardened: returns empty dict on file errors or JSON parse failures
    instead of propagating exceptions. Logs the failure for visibility.
    """
    global _cached_data, _cache_load_time
    now = time.monotonic()
    if _cached_data is not None and (now - _cache_load_time) < _CACHE_TTL_SECONDS:
        return _cached_data
    if not _CLASSIFICATIONS_PATH.exists():
        return {}
    try:
        _cached_data = json.loads(_CLASSIFICATIONS_PATH.read_text())
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning(
            "classification_load_failed: %s",
            _CLASSIFICATIONS_PATH,
            exc_info=True,
        )
        _cached_data = {}
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
    global _active_whale_cache, _active_whale_cache_time
    _cached_data = None
    _cache_load_time = 0.0
    _pending_cache = None
    _pending_cache_time = 0.0
    _active_whale_cache = None
    _active_whale_cache_time = 0.0


def is_whale_active(whale_name: str, classifications_path: str | Path | None = None) -> tuple[bool, str]:
    """
    Check if a whale is currently "active" — either recently signaling or recently
    rediscovered in the discovery DB.

    Returns (is_active: bool, reason: str)
    - is_active=True  → whale is generating signals or was recently rediscovered
    - is_active=False → whale is INACTIVE (ghost), reason explains why
    """
    global _active_whale_cache, _active_whale_cache_time
    global _inactive_cache, _inactive_cache_time
    now = time.monotonic()

    # Fast path: already cached as inactive
    if whale_name in _inactive_cache:
        _checked_at = _inactive_cache.get(whale_name, ("", 0.0))
        if isinstance(_checked_at, tuple):
            reason, checked_at = _checked_at
        else:
            reason, checked_at = _checked_at, 0.0
        if now - checked_at < _INACTIVE_CACHE_TTL:
            return False, reason
        # TTL expired, re-check

    # Refresh active whale cache if stale
    if _active_whale_cache is None or (now - _active_whale_cache_time) > _ACTIVE_CACHE_TTL:
        _active_whale_cache = _load_recent_signal_whales()
        _active_whale_cache_time = now

    if whale_name in _active_whale_cache:
        # Whale was active in last 48h — clear any stale inactive cache entry
        _inactive_cache.pop(whale_name, None)
        return True, "active_recent_signal"

    # Not in recent signals. Check discovery DB freshness.
    path = Path(classifications_path) if classifications_path else _DISCOVERY_DB_PATH
    discovery_fresh = _is_discovery_fresh(whale_name)
    if discovery_fresh:
        _inactive_cache.pop(whale_name, None)
        return True, "active_recent_discovery"

    # Neither condition met — INACTIVE
    reason = (
        f"ghost_whale: no signal in 48h and not rediscovered in "
        f"discovery DB in {_ACTIVITY_DISCOVERY_DAYS}d"
    )
    _inactive_cache[whale_name] = reason
    _inactive_cache_time = now
    return False, reason


def _load_recent_signal_whales() -> set[str]:
    """Query decision_snapshots for all whale names seen in the last 48 hours."""
    try:
        conn = sqlite3.connect(str(_DATA_DIR / "trades.db"))
        conn.execute("PRAGMA busy_timeout=5000")
        cutoff = f"datetime('now', '-{_ACTIVITY_SIGNAL_HOURS} hours')"
        rows = conn.execute(f"""
            SELECT DISTINCT whale_name FROM decision_snapshots
            WHERE timestamp >= {cutoff}
              AND whale_name IS NOT NULL
              AND whale_name != ''
        """).fetchall()
        conn.close()
        return {str(r[0]) for r in rows if r[0]}
    except Exception:
        return set()


def _is_discovery_fresh(whale_name: str) -> bool:
    """Check if whale was in discovery DB updated within ACTIVITY_DISCOVERY_DAYS.

    Treats updated_at=NULL as NOT fresh (those entries haven't been
    refreshed by the discovery cron and should not pass the activity gate).
    """
    try:
        conn = sqlite3.connect(str(_DISCOVERY_DB_PATH))
        conn.execute("PRAGMA busy_timeout=5000")
        cutoff = f"datetime('now', '-{_ACTIVITY_DISCOVERY_DAYS} days')"
        # COALESCE ensures NULL updated_at fails the comparison
        row = conn.execute(f"""
            SELECT 1 FROM whales
            WHERE name = ?
              AND COALESCE(updated_at, '1970-01-01') >= {cutoff}
        """, (whale_name,)).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


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
                "action": _sanitize_action(
                    cat_entry.get("action"),  # None if key missing → logged by sanitizer
                    f"category={category},whale={whale_name}",
                ),
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
            result = {
                "action": _sanitize_action(
                    global_data.get("global_action"),  # None if key missing → logged by sanitizer
                    f"global,whale={whale_name}",
                ),
                "action_confidence": global_data.get("global_action_confidence", 0.3),
                "source": "global_fallback",
                "stats": {
                    "total_trades": global_data.get("total_trades", 0),
                    "win_rate": global_data.get("win_rate", 0.0),
                    "avg_pnl": global_data.get("avg_pnl", 0.0),
                    "total_pnl": global_data.get("total_pnl", 0.0),
                },
            }
            # Activity gate: ghost whales get INACTIVE regardless of classification
            active, _ = is_whale_active(whale_name, classifications_path)
            if not active:
                result["action"] = "INACTIVE"
                result["action_confidence"] = 0.05
                result["source"] = "ghost_inactive"
            return result

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


def get_category_action_v2(
    whale_name: str,
    category: str,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Query canonical_perf for per-whale per-category real performance stats.

    v6.6: this is observation-only. It returns explicit lookup/reason fields so
    abstentions can be audited, but callers must not use it as an execution gate.
    """
    lookup_category = category or ""
    lookup_key = f"{whale_name or ''}|{lookup_category}"

    def _result(action: str, conf: float, reason: str, stats: dict[str, Any] | None = None, match_status: str = "unknown") -> dict[str, Any]:
        return {
            "action": action or "INSUFFICIENT_DATA",
            "action_confidence": round(float(conf or 0.0), 3),
            "source": "canonical_perf",
            "reason": reason,
            "lookup_key": lookup_key,
            "match_status": match_status,
            "stats": stats or {"total_trades": 0, "win_rate": 0.0, "avg_pnl": 0.0, "total_pnl": 0.0},
        }

    if not whale_name:
        return _result("INSUFFICIENT_DATA", 0.0, "missing_whale_name", match_status="no_lookup")

    path = Path(db_path) if db_path else _DATA_DIR / "trades.db"
    if not path.exists():
        return _result("INSUFFICIENT_DATA", 0.0, "canonical_perf_db_missing", match_status="db_missing")

    try:
        conn = sqlite3.connect(str(path))
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            """SELECT
                   COUNT(*),
                   COALESCE(AVG(actual_pnl), 0.0),
                   CASE WHEN COUNT(*) > 0
                        THEN CAST(SUM(won) AS REAL) / COUNT(*)
                        ELSE 0.0 END
               FROM canonical_perf
               WHERE whale_name = ? AND category = ?
                 AND actual_pnl IS NOT NULL""",
            (whale_name, lookup_category),
        ).fetchone()
        whale_row = conn.execute(
            """SELECT COUNT(*), GROUP_CONCAT(DISTINCT COALESCE(category, ''))
               FROM canonical_perf
               WHERE whale_name = ? AND actual_pnl IS NOT NULL""",
            (whale_name,),
        ).fetchone()
        conn.close()

        total_trades = row[0] or 0
        avg_pnl = row[1] or 0.0
        win_rate = row[2] or 0.0
        stats = {
            "total_trades": total_trades,
            "win_rate": round(win_rate, 4),
            "avg_pnl": round(avg_pnl, 2),
            "total_pnl": round(avg_pnl * total_trades, 2),
        }

        if total_trades == 0:
            whale_total = whale_row[0] if whale_row else 0
            if whale_total:
                stats["whale_total_other_categories"] = whale_total
                stats["available_categories"] = (whale_row[1] or "") if whale_row else ""
                return _result(
                    "INSUFFICIENT_DATA", 0.0, "no_rows_for_lookup_key",
                    stats, match_status="whale_matched_category_mismatch",
                )
            return _result("INSUFFICIENT_DATA", 0.0, "no_canonical_perf_rows", stats, match_status="no_match")

        if total_trades < 10:
            conf = min(total_trades / 10, 1.0) * 0.5
            return _result(
                "INSUFFICIENT_DATA", conf, "sample_size_below_minimum",
                stats, match_status="matched_exact",
            )

        edge_hit = avg_pnl >= 25.0 and win_rate >= 0.05 and total_trades >= 50
        primary_hit = avg_pnl >= 10.0 and win_rate >= 0.55
        fade_hit = avg_pnl < -5.0 and win_rate <= 0.45
        conf = min(0.5 + total_trades / 100 * 0.45, 0.95)

        if edge_hit or primary_hit:
            return _result("FOLLOW", conf, "positive_canonical_perf", stats, match_status="matched_exact")
        if fade_hit:
            return _result("FADE", conf, "negative_canonical_perf", stats, match_status="matched_exact")
        return _result("NEUTRAL", conf, "canonical_perf_neutral", stats, match_status="matched_exact")
    except Exception as exc:
        return _result("INSUFFICIENT_DATA", 0.0, f"canonical_perf_error:{type(exc).__name__}", match_status="error")

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
