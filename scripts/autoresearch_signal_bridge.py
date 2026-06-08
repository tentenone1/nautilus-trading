#!/usr/bin/env python3
"""Autoresearch Signal Bridge — feeds trade_recommendations.json into whale_follower.

Pipeline:
1. Reads research/trade_recommendations.json (output from autoresearch_bridge.py)
2. Filters for BUY decisions not yet processed
3. Resolves condition_id → token_id/outcome via Polymarket CLOB API
4. Writes enriched signals to research/autoresearch_signal_queue.json
5. The running whale_follower polls this queue and executes via _on_signal()

State tracking: research/autoresearch_bridge_state.json prevents re-processing.
from nrs_guardian import enforce_singleton
enforce_singleton("signal_bridge")

TTL / Expiry Mode (manual CLI):
--report   : show which recommendation keys would expire, without writing
--execute  : actually remove expired recommendation keys from state
--ttl-days : override default TTL (default: 7 days)
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RECS_FILE = os.path.join(BASE_DIR, "research", "trade_recommendations.json")
QUEUE_FILE = os.path.join(BASE_DIR, "research", "autoresearch_signal_queue.json")
STATE_FILE = os.path.join(BASE_DIR, "research", "autoresearch_bridge_state.json")
LOG_FILE = os.path.join(BASE_DIR, "logs", "autoresearch-signal-bridge.log")

DEFAULT_TTL_DAYS = 7
META_KEYS = frozenset({"last_run", "whales_updated", "snapshot"})


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[autoresearch-bridge] {ts} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


# ── Ignored Markets ─────────────────────────────────────────────────────────────

IGNORED_MARKETS_FILE = os.path.join(BASE_DIR, "research", "ignored_markets.json")


def _load_ignored_markets() -> list[str]:
    """Load ignored markets list from JSON file. Returns empty list on error."""
    if not os.path.exists(IGNORED_MARKETS_FILE):
        return []
    try:
        with open(IGNORED_MARKETS_FILE) as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data.get("ignored_markets", [])
        if isinstance(data, list):
            return data
        return []
    except (json.JSONDecodeError, OSError):
        return []


def is_excluded_market(market_id: str) -> bool:
    """Returns True if market_id is in the ignored markets list, False otherwise.

    The ignored markets list is persisted in research/ignored_markets.json and
    synchronized with WhaleFollowerConfig.ignored_markets by the strategy at runtime.
    """
    return market_id in _load_ignored_markets()


CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{}"

# ─── Quality Gates ─────────────────────────────────────────────────────────────
CONFIDENCE_GATE = 0.50      # Minimum confidence for BUY to pass to execution (raised from 0.65)
KELLY_MIN = 0.01            # 1% floor
KELLY_MAX = 0.125           # 12.5% ceiling (matches whale_tiers.json max_position_pct)

# Sports keywords — used to filter out sports markets before wasting API calls
# on resolve_yes_token(). Sports signals will be quarantined at the handler anyway.
BRIDGE_SPORTS_KEYWORDS = [
    "nfl", "nba", "mlb", "nhl", "ncaa", "college football", "college basketball",
    "soccer", "football", "basketball", "baseball", "hockey", "tennis", "golf",
    "boxing", "mma", "ufc", "wwe", "f1", "formula 1", "nascar",
    "super bowl", "world cup", "champions league", "premier league",
    "playoffs", "stanley cup", "world series", "final four", "march madness",
    " vs ", " vs.", "eagles", "49ers", "chiefs", "lakers", "celtics",
    "warriors", "yankees", "dodgers", "red sox", "patriots",
    "trail blazers", "spurs", "penguins", "stars", "wild",
    "bucks", "thunder", "nuggets", "timberwolves", "knicks",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def fetch_market(condition_id: str) -> dict | None:
    """Fetch market info from Polymarket CLOB API by condition_id."""
    import urllib.request

    url = CLOB_MARKET_URL.format(condition_id)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "nautilus-trading/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data and data.get("active", False):
            return data
        return None
    except Exception:
        return None


def resolve_yes_token(condition_id: str) -> tuple[str, str, str] | None:
    """Resolve condition_id to (token_id, outcome, category) for YES outcome.

    Returns None if market can't be found or is inactive.
    Falls back to "Yes" outcome, or the first token if no "Yes" found.
    Category is extracted from the market response and defaults to "general".
    """
    market = fetch_market(condition_id)
    if not market:
        return None

    tokens = market.get("tokens", [])
    if not tokens:
        return None

    market_category = (market.get("category") or "general").strip()

    # Prefer "Yes" outcome (case-insensitive)
    for t in tokens:
        outcome = (t.get("outcome") or "").strip().lower()
        if outcome == "yes":
            return str(t["token_id"]), t["outcome"], market_category

    # Fallback to first token
    t = tokens[0]
    return str(t["token_id"]), t["outcome"], market_category


# ── State TTL / Expiry utilities ────────────────────────────────────────────────


def _is_meta_key(key: str) -> bool:
    return key in META_KEYS


def _is_recommendation_key(key: str) -> bool:
    """Return True for keys shaped like condition_id|timestamp."""
    # Must contain a pipe separator and look like a compound key
    if "|" not in key:
        return False
    # Exclude known metadata keys
    if _is_meta_key(key):
        return False
    # Must start with "0x" hexish condition_id
    parts = key.split("|", 1)
    return len(parts) == 2 and parts[0].startswith("0x")


def classify_state_keys(state: dict[str, Any]) -> dict[str, list[str]]:
    """Classify state keys into metadata, recent recommendations, expired recommendations."""
    meta = []
    rec_recent = []
    rec_expired = []
    for key, val in state.items():
        if _is_meta_key(key):
            meta.append(key)
            continue
        if isinstance(val, (int, float)) and _is_recommendation_key(key):
            rec_recent.append(key)
        else:
            # Non-numeric or non-recommendation values are kept as metadata
            meta.append(key)
    return {
        "meta": meta,
        "rec_recent": rec_recent,
        "rec_expired": rec_expired,
    }


def expire_old_recommendation_keys(state: dict[str, Any], ttl_secs: float) -> tuple[dict[str, Any], int]:
    """Return a new state dict with expired recommendation keys removed, and count of removed keys.

    Preserves:
    - metadata keys (last_run, whales_updated, snapshot, etc.)
    - non-numeric values
    - recommendation keys with numeric timestamps within TTL
    """
    now = time.time()
    cleaned: dict[str, Any] = {}
    expired_count = 0
    for key, val in state.items():
        if _is_meta_key(key):
            cleaned[key] = val
            continue
        if not isinstance(val, (int, float)):
            cleaned[key] = val
            continue
        if _is_recommendation_key(key):
            age = now - float(val)
            if age <= ttl_secs:
                cleaned[key] = val
            else:
                expired_count += 1
        else:
            cleaned[key] = val
    return cleaned, expired_count


def report_ttl(state: dict[str, Any], ttl_secs: float, recommendations: list[dict]) -> dict[str, Any]:
    """Report what would happen with TTL expiry, without mutating state."""
    now = time.time()
    total_keys = len(state)
    meta_keys = [k for k in state if _is_meta_key(k)]
    meta_keys_count = len(meta_keys)

    rec_total = 0
    rec_expired = 0
    rec_retained = 0
    sample_expired: list[str] = []
    sample_retained: list[str] = []

    for key, val in state.items():
        if _is_recommendation_key(key) and isinstance(val, (int, float)):
            rec_total += 1
            age = now - float(val)
            if age > ttl_secs:
                rec_expired += 1
                if len(sample_expired) < 3:
                    sample_expired.append(f"{key[:60]} (age={age/86400:.1f}d)")
            else:
                rec_retained += 1
                if len(sample_retained) < 3:
                    sample_retained.append(f"{key[:60]} (age={age/86400:.1f}d)")

    # Would-requeue BUYs
    buy_recs = [r for r in recommendations if r.get("decision") == "BUY" and r.get("confidence", 0) >= CONFIDENCE_GATE]
    new_recs = []
    for r in buy_recs:
        key = make_signal_key(r)
        if key in state and isinstance(state.get(key), (int, float)):
            age = now - float(state[key])
            if age > ttl_secs:
                new_recs.append(r)
        else:
            new_recs.append(r)

    return {
        "total_keys": total_keys,
        "meta_keys_count": meta_keys_count,
        "meta_keys": meta_keys,
        "rec_total": rec_total,
        "rec_expired": rec_expired,
        "rec_retained": rec_retained,
        "sample_expired": sample_expired,
        "sample_retained": sample_retained,
        "buy_total": len(buy_recs),
        "would_requeue": len(new_recs),
    }


def run_ttl_report(ttl_days: int = DEFAULT_TTL_DAYS) -> int:
    """Dry-run TTL report. Returns 0."""
    state = load_state()
    recommendations: list[dict] = []
    if os.path.exists(RECS_FILE):
        try:
            with open(RECS_FILE) as f:
                recommendations = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    ttl_secs = ttl_days * 86400
    report = report_ttl(state, ttl_secs, recommendations)
    now = datetime.now(timezone.utc).isoformat()

    print(f"== Autoresearch Bridge State TTL Report ({now}) ==")
    print(f"TTL: {ttl_days} days ({ttl_secs} seconds)")
    print(f"State file: {STATE_FILE}")
    print()
    print(f"Total keys:              {report['total_keys']}")
    print(f"  Metadata keys:         {report['meta_keys_count']}")
    for mk in report["meta_keys"]:
        print(f"    - {mk}")
    print(f"  Recommendation keys:  {report['rec_total']}")
    print(f"    Expired:             {report['rec_expired']}")
    print(f"    Retained:            {report['rec_retained']}")
    print()
    if report["sample_expired"]:
        print("Sample expired keys:")
        for s in report["sample_expired"]:
            print(f"    {s}")
    if report["sample_retained"]:
        print("Sample retained keys:")
        for s in report["sample_retained"]:
            print(f"    {s}")
    print()
    print(f"BUY recommendations:     {report['buy_total']}")
    print(f"Would requeue after TTL: {report['would_requeue']}")
    print()
    print("DRY RUN — no state changes written.")
    print("To execute, pass --execute after review.")
    return 0


def run_ttl_execute(ttl_days: int = DEFAULT_TTL_DAYS) -> int:
    """Execute TTL expiry on state file. Returns 0."""
    state = load_state()
    ttl_secs = ttl_days * 86400
    cleaned, expired_count = expire_old_recommendation_keys(state, ttl_secs)
    removed = len(state) - len(cleaned)
    if removed > 0:
        save_state(cleaned)
        log(f"TTL expiry: removed {removed} recommendation keys (TTL={ttl_days} days)")
    else:
        log(f"TTL expiry: no recommendation keys expired (TTL={ttl_days} days)")
    return 0


def load_state() -> dict[str, float]:
    """Load processed recommendation timestamps."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_state(state: dict[str, float]) -> None:
    """Save processed recommendation timestamps."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def load_queue() -> list[dict[str, Any]]:
    """Load existing signal queue (unprocessed signals from previous runs)."""
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
        except (json.JSONDecodeError, OSError):
            return []
    return []


def save_queue(signals: list[dict[str, Any]]) -> None:
    """Write signal queue file."""
    os.makedirs(os.path.dirname(QUEUE_FILE), exist_ok=True)
    with open(QUEUE_FILE, "w") as f:
        json.dump(signals, f, indent=2)


def make_signal_key(rec: dict) -> str:
    """Generate unique key for a recommendation (condition_id + timestamp)."""
    cid = rec.get("condition_id", "") or ""
    ts = rec.get("timestamp", "") or ""
    return f"{cid}|{ts}"


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> int:
    log(f"Starting at {datetime.now(timezone.utc).isoformat()}")

    # Load state and recommendations
    state = load_state()
    if not os.path.exists(RECS_FILE):
        log(f"No recommendations file at {RECS_FILE}")
        return 0

    with open(RECS_FILE) as f:
        recommendations: list[dict] = json.load(f)

    # Filter for BUY recommendations not yet processed (with confidence gate)
    buy_recs = [
        r for r in recommendations
        if r.get("decision") == "BUY"
        and r.get("confidence", 0) >= CONFIDENCE_GATE
    ]
    new_recs = [r for r in buy_recs if make_signal_key(r) not in state]

    if not new_recs:
        log("No new BUY recommendations to process")
        return 0

    log(f"{len(new_recs)} new BUY recommendations of {len(buy_recs)} total")

    # Load existing queue
    queue = load_queue()

    # Process each recommendation
    processed = 0
    skipped = 0
    for rec in new_recs:
        cid = rec.get("condition_id", "") or ""
        market = rec.get("market", "Unknown") or "Unknown"
        entry_price = rec.get("entry_price", 0.5)
        confidence = rec.get("confidence", 0.5)
        kelly = rec.get("kelly_fraction", 0.15)
        kelly = max(KELLY_MIN, min(KELLY_MAX, kelly))  # Clamp to bounds
        reason = rec.get("reason", "") or ""
        hold_hours = rec.get("hold_hours", 24)

        if not cid:
            log(f"Skipping '{market[:50]}...' — no condition_id")
            skipped += 1
            state[make_signal_key(rec)] = time.time()
            continue

        # Skip excluded markets (populated by whale_follower at runtime)
        if is_excluded_market(cid):
            log(f"Skipping excluded market: '{market[:50]}...'")
            skipped += 1
            state[make_signal_key(rec)] = time.time()
            continue

        # Skip sports markets — they'll be quarantined at the handler anyway.
        # Filtering here saves an API call to resolve_yes_token().
        market_lower = market.lower()
        if any(kw in market_lower for kw in BRIDGE_SPORTS_KEYWORDS):
            log(f"Skipping sports market: '{market[:50]}...'")
            skipped += 1
            state[make_signal_key(rec)] = time.time()
            continue

        # Resolve YES token from Polymarket API
        token_info = resolve_yes_token(cid)
        if token_info is None:
            log(f"Skipping '{market[:50]}...' — could not resolve token (inactive?)")
            skipped += 1
            state[make_signal_key(rec)] = time.time()
            continue

        token_id, outcome, market_category = token_info

        signal = {
            "condition_id": cid,
            "token_id": token_id,
            "outcome": outcome,
            "market_category": market_category,  # Resolved from Polymarket CLOB API
            "side": "buy",
            "entry_price": entry_price,
            "confidence": confidence,
            "kelly_fraction": kelly,
            "suggested_size_usd": 0.0,  # Filled by Kelly sizing in strategy
            "market_title": market,
            "reason": reason,
            "hold_hours": hold_hours,
            "whale_name": "autoresearch_llm",
            "whale_roi": 0.0,
            "edge_score": confidence * 0.8,  # Scale confidence into edge score
            "source": "model_insider",
            "timestamp": time.time(),
        }

        queue.append(signal)
        state[make_signal_key(rec)] = time.time()
        processed += 1

        log(f"✅ {market[:50]:50s} | ${entry_price:.2f} | conf={confidence:.0%}")

    # Write updated queue
    # Keep only last 100 signals to prevent unbounded growth
    queue = queue[-100:]
    save_queue(queue)
    save_state(state)

    log(f"Done. {processed} queued, {skipped} skipped, {len(queue)} total in queue")

    return processed


def run_loop() -> None:
    """Continuous polling loop."""
    INTERVAL_SECS = 60
    while True:
        try:
            result = main()
            if result < 0:
                # Negative return = fatal error, break
                break
        except KeyboardInterrupt:
            break
        except Exception as e:
            log(f"Unexpected error: {e}")
        time.sleep(INTERVAL_SECS)


def cli(argv: list[str] | None = None) -> int:
    """Entry point that preserves daemon behavior when called with no args.

    No arguments (or only unrecognized ones via run_loop) -> start daemon loop.
    --report -> dry-run TTL report.
    --execute -> TTL expiry with state mutation.
    """
    # If no sys.argv provided and no explicit cli flags were passed, keep daemon behavior
    if argv is None:
        argv = sys.argv[1:]

    # Detect intent: if --report or --execute is explicitly requested, run TTL mode
    # Otherwise default to daemon/run_loop to preserve existing systemd/cron behavior
    if "--report" in argv or "--execute" in argv:
        parser = argparse.ArgumentParser(description="Autoresearch Signal Bridge TTL Manager")
        parser.add_argument(
            "--report", action="store_true",
            help="Dry-run: report state TTL expiry without writing."
        )
        parser.add_argument(
            "--execute", action="store_true",
            help="Execute: remove expired recommendation keys from state file."
        )
        parser.add_argument(
            "--ttl-days", type=int, default=DEFAULT_TTL_DAYS,
            help=f"TTL in days for recommendation keys (default: {DEFAULT_TTL_DAYS})"
        )
        args = parser.parse_args(argv)

        if args.execute:
            return run_ttl_execute(args.ttl_days)
        return run_ttl_report(args.ttl_days)

    # Default: run the daemon loop (existing behavior)
    run_loop()
    return 0


if __name__ == "__main__":
    cli()
