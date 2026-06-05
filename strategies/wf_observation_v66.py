"""v6.6 observation-only metadata helpers.

These helpers enrich telemetry only. They must not change should_trade,
final_decision, sizing, quarantine, or live-trading behavior.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _get_attr(obj: Any, names: tuple[str, ...]) -> tuple[Any, str | None]:
    if obj is None:
        return None, None
    for name in names:
        value = getattr(obj, name, None)
        if value:
            return value, f"signal.{name}"
    return None, None


def _load_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _title_expiry(title: str, now: datetime) -> tuple[datetime | None, str | None]:
    if not title:
        return None, None
    pat = re.compile(
        r"(?:by|on|before|through|until)?\s*"
        r"(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"\s+(\d{1,2})(?:,\s*(\d{4}))?",
        re.IGNORECASE,
    )
    match = pat.search(title)
    if not match:
        return None, None
    month = _MONTHS[match.group(1).lower()]
    day = int(match.group(2))
    year = int(match.group(3) or now.year)
    try:
        dt = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
        if dt < now and not match.group(3):
            dt = datetime(year + 1, month, day, 23, 59, 59, tzinfo=timezone.utc)
        return dt, "market_title_fallback"
    except ValueError:
        return None, None


def _bucket(hours: float | None) -> tuple[str, int]:
    if hours is None:
        return "unknown", 99
    if hours <= 0:
        return "expired", 0
    if hours <= 12:
        return "short", 1
    if hours <= 72:
        return "short", 2
    if hours <= 24 * 14:
        return "medium", 5
    return "long", 9


def build_duration_metadata(
    signal: Any = None,
    *,
    metadata: dict[str, Any] | None = None,
    market_title: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return normalized v6.6 duration metadata for telemetry."""
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    meta = metadata or {}

    existing = meta.get("v66_duration") if isinstance(meta.get("v66_duration"), dict) else {}
    if existing and existing.get("duration_bucket") and "expected_resolution_hours" in existing:
        return dict(existing)
    expiry_value = existing.get("market_expires_at")
    source = existing.get("duration_source")

    if not expiry_value:
        expiry_value, source = _get_attr(signal, (
            "market_expires_at", "expires_at", "end_date_iso", "endDateIso", "endDate", "market_end_date",
        ))

    if not expiry_value:
        for key in ("market_expires_at", "expires_at", "end_date_iso", "endDateIso", "endDate", "market_end_date"):
            if meta.get(key):
                expiry_value = meta[key]
                source = f"metadata_json.{key}"
                break

    title = market_title or getattr(signal, "market_title", "") or meta.get("market_title", "") or ""
    expiry_dt = _parse_dt(expiry_value)
    if expiry_dt is None:
        expiry_dt, source = _title_expiry(title, now_dt)

    if expiry_dt is None:
        return {
            "market_expires_at": None,
            "expected_resolution_hours": None,
            "duration_bucket": "unknown",
            "resolution_priority": 99,
            "duration_source": "unknown",
            "duration_missing_reason": "no_expiry_source_found",
        }

    hours = round((expiry_dt - now_dt).total_seconds() / 3600.0, 3)
    bucket, priority = _bucket(hours)
    return {
        "market_expires_at": expiry_dt.isoformat(),
        "expected_resolution_hours": hours,
        "duration_bucket": bucket,
        "resolution_priority": priority,
        "duration_source": source or "unknown",
    }


def build_observation_cohorts(snapshot: dict[str, Any], duration: dict[str, Any]) -> dict[str, bool]:
    source = (snapshot.get("source") or "").lower()
    whale = (snapshot.get("whale_name") or "").lower()
    category = (snapshot.get("normalized_category") or snapshot.get("category") or "").lower()
    reject_reason = (snapshot.get("reject_reason") or "").lower()
    v2_action = (snapshot.get("category_action_v2") or "").upper()
    sample = int(snapshot.get("category_sample_size_v2") or 0)
    avg_pnl = float(snapshot.get("category_avg_pnl_v2") or 0.0)
    win_rate = float(snapshot.get("category_win_rate_v2") or 0.0)
    final_decision = snapshot.get("final_decision") or ""

    high_quality = sample >= 10 and (avg_pnl > 0 or win_rate >= 0.55)
    sports_like = category == "sports" or "sports" in reject_reason or bool(snapshot.get("sports_telemetry"))
    return {
        "old_pipeline_accepted": final_decision == "SHADOW_TRADE",
        "v2_follow_candidate": v2_action == "FOLLOW",
        "high_quality_discovery_whale": high_quality,
        "short_duration_candidate": duration.get("duration_bucket") in {"expired", "short"},
        "sports_telemetry_candidate": sports_like,
        "sybil_cluster_candidate": source == "sybil" or whale.startswith("sybil_") or "sybil" in whale,
    }


def enrich_snapshot_metadata(
    snapshot: dict[str, Any],
    signal: Any = None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Attach v6.6 telemetry metadata without altering decision fields."""
    enriched = dict(snapshot)
    meta = _load_json(enriched.get("metadata_json"))
    duration = build_duration_metadata(
        signal,
        metadata=meta,
        market_title=enriched.get("market_title", ""),
        now=now,
    )
    meta["v66_duration"] = duration
    meta["v66_cohorts"] = build_observation_cohorts(enriched, duration)
    meta.setdefault("v66_observation_only", True)
    enriched["metadata_json"] = json.dumps(meta, sort_keys=True)
    return enriched


def duration_from_metadata_json(metadata_json: str | None) -> dict[str, Any]:
    meta = _load_json(metadata_json)
    duration = meta.get("v66_duration")
    return duration if isinstance(duration, dict) else {}
