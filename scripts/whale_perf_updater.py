#!/usr/bin/env python3
"""Whale Performance Updater — closes the feedback loop between trade outcomes and tier assignments.

Pipeline position: called by autoresearch_bridge.py after each bridge run.
Reads resolved trades from trades.db, computes per-whale stats, and updates
tier_assignments.json with fresh precision tiers and Kelly multipliers.

Only whales with MIN_TRADES resolved trades get updated (avoids noise from small samples).
Uses a rolling window (default 30 days) so scores stay fresh.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("whale_perf_updater")

# ── Tunables ──────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "trades.db"
TIER_ASSIGNMENTS_PATH = PROJECT_ROOT / "config" / "tier_assignments.json"
STATE_FILE = PROJECT_ROOT / "research" / "autoresearch_bridge_state.json"

MIN_TRADES = 3          # Minimum resolved trades before updating a whale
ROLLING_WINDOW_DAYS = 30  # Only count trades within this window
MIN_VOLUME_USD = 1_000  # Minimum volume to even consider a whale

# Kelly multiplier bounds (don't let the model go crazy)
KELLY_MIN = 0.15
KELLY_MAX = 1.25

# Precision tier thresholds based on win rate
WR_HIGH = 0.65
WR_MEDIUM = 0.45

# ── Whale Name Normalization ──────────────────────────────────────────────────

_WALLET_RE = re.compile(r"(0x[a-fA-F0-9]{1,40})", re.IGNORECASE)
_FULL_WALLET_RE = re.compile(r"(0x[a-fA-F0-9]{40})", re.IGNORECASE)


def extract_address(name: str) -> str | None:
    """Extract first full (40-char) 0x address from a whale name string, if present."""
    m = _FULL_WALLET_RE.search(name)
    return m.group(1).lower() if m else None


def build_name_to_addr_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Build a bidirectional name→address map from resolved trades.

    Covers three patterns:
      1. Both name and address present: use address as canonical key
      2. Name only but contains address: extract it
      3. Name-only (plain text handle): no address available
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT whale_name, whale_address
        FROM trades
        WHERE whale_name IS NOT NULL
    """)
    name_to_addr: dict[str, str] = {}
    for name, addr in cur.fetchall():
        if addr:
            name_to_addr[name.lower()] = addr.lower()
        else:
            extracted = extract_address(name)
            if extracted:
                name_to_addr[name.lower()] = extracted
    return name_to_addr


def normalize_to_addr(name: str, name_to_addr: dict[str, str]) -> str:
    """Map a whale name (or address) to its canonical lowercase address."""
    return name_to_addr.get(name.lower(), name.lower())


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_tier_assignments() -> tuple[dict[str, Any], dict[str, str]]:
    """Load tier assignments and build an address prefix → TA key index.

    TA keys store truncated addresses (8 chars): "p1-0xb6d6e9"
    trades.db/whale_discovery have full 40-char addresses.
    We match by checking if a full address STARTS WITH the TA key's truncated address.
    """
    if not TIER_ASSIGNMENTS_PATH.exists():
        logger.warning("No tier_assignments.json found — starting empty")
        return {}, {}

    with open(TIER_ASSIGNMENTS_PATH) as f:
        data = json.load(f)

    addr_index: dict[str, str] = {}  # address_or_name → TA key

    # Build index: truncated address (from TA key) → TA key
    for ta_key in data:
        # Direct lowercase match
        addr_index[ta_key.lower()] = ta_key
        # Extract any address fragment from TA key
        for m in _WALLET_RE.finditer(ta_key):
            frag = m.group(1).lower()
            # Index by full address if it's full (40 chars), or truncated fragment
            addr_index[frag] = ta_key

    # For whale_discovery: build full-address → TA key via truncated prefix match
    # The TA key truncated address must match the START of the whale_discovery full address
    wd_path = PROJECT_ROOT / "data" / "whale_discovery.db"
    if wd_path.exists():
        try:
            conn = sqlite3.connect(wd_path)
            cur = conn.cursor()
            cur.execute("SELECT name, address FROM whales")
            for wname, addr in cur.fetchall():
                if not addr or wname not in data:
                    continue
                addr_lower = addr.lower()
                # Check if any TA key's address fragment matches start of this full address
                for ta_key in data:
                    for m in _WALLET_RE.finditer(ta_key):
                        frag = m.group(1).lower()
                        if addr_lower.startswith(frag) and len(frag) >= 6:
                            addr_index[addr_lower] = ta_key
            conn.close()
        except Exception as e:
            logger.warning("Could not load whale_discovery.db: %s", e)

    return data, addr_index


def _find_ta_key(canonical_key: str, addr_index: dict[str, str]) -> str | None:
    """Find the tier_assignments key that matches a whale by canonical key (address or name).

    Matches by:
    1. Exact lowercase match in addr_index
    2. Address fragment prefix match (truncated TA key fragment matches start of full address)
    3. Extracted full address from name → prefix match
    """
    # 1. Exact match
    if canonical_key in addr_index:
        return addr_index[canonical_key]

    # 2. Full address → check if any truncated fragment in index is a prefix
    # (handles case where stats key is full 40-char address but TA index has 8-char fragment)
    if canonical_key.startswith("0x"):
        # Try progressively shorter prefixes down to 8 chars
        for i in range(40, 7, -1):
            prefix = canonical_key[:i]
            if prefix in addr_index:
                return addr_index[prefix]

    # 3. Extract address fragments from the key itself and look up
    for m in _WALLET_RE.finditer(canonical_key):
        frag = m.group(1).lower()
        if frag in addr_index:
            return addr_index[frag]
        # Also try prefix match for extracted fragments
        if len(frag) == 40:
            for i in range(40, 7, -1):
                prefix = frag[:i]
                if prefix in addr_index:
                    return addr_index[prefix]

    return None


def save_tier_assignments(data: dict[str, Any]) -> None:
    """Write updated tier assignments back to disk."""
    TIER_ASSIGNMENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TIER_ASSIGNMENTS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(TIER_ASSIGNMENTS_PATH)
    logger.info("Wrote %s", TIER_ASSIGNMENTS_PATH)



def compute_whale_stats(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Compute per-whale stats from resolved trades within the rolling window.

    Uses whale_address as canonical identifier when available, falling back to
    whale_name for named handles without associated addresses.

    Returns dict keyed by canonical identifier (address or name).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=ROLLING_WINDOW_DAYS)
    cutoff_str = cutoff.isoformat()

    # Build name→address map first so we can use it during aggregation
    name_to_addr = build_name_to_addr_map(conn)

    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.whale_name,
            t.whale_address,
            COUNT(*)                                   AS trade_count,
            SUM(CASE WHEN t.actual_pnl > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate,
            SUM(t.actual_pnl)                          AS total_pnl,
            AVG(t.actual_pnl)                          AS avg_pnl,
            SUM(t.position_size_usd)                   AS total_volume,
            COUNT(DISTINCT t.category)                  AS category_spread,
            -- Consistency: variance of per-trade P&L (lower = more consistent)
            CASE WHEN COUNT(*) > 1
                 THEN AVG(t.actual_pnl * t.actual_pnl) - AVG(t.actual_pnl) * AVG(t.actual_pnl)
                 ELSE 0
            END                                        AS pnl_variance
        FROM trades t
        WHERE t.whale_name     IS NOT NULL
          AND t.actual_pnl     IS NOT NULL
          AND t.resolution_outcome IS NOT NULL
          AND t.timestamp      >= ?
        GROUP BY t.whale_name, t.whale_address
        HAVING SUM(t.position_size_usd) >= ?
    """, (cutoff_str, MIN_VOLUME_USD))

    stats: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        raw_name, raw_addr, n, wr, pnl, avg_pnl, volume, cat_spread, variance = row

        # Canonical key: address if available, else name
        key = raw_addr.lower() if raw_addr else raw_name.lower()

        std_dev = variance ** 0.5 if variance else 0.0

        # Consistency score: -1 (always losing) to +1 (always winning)
        if avg_pnl != 0 and std_dev > 0:
            consistency = max(-1.0, min(1.0, avg_pnl / (2 * std_dev)))
        else:
            consistency = 0.0

        # Kelly multiplier based on win rate and consistency
        raw_kelly = wr * (0.5 + 0.5 * consistency)
        kelly = max(KELLY_MIN, min(KELLY_MAX, raw_kelly))

        stats[key] = {
            "trade_count": n,
            "win_rate": round(wr, 4),
            "total_pnl": round(pnl, 2),
            "avg_pnl": round(avg_pnl, 2),
            "total_volume": round(volume, 2),
            "category_spread": cat_spread,
            "consistency": round(consistency, 3),
            "kelly_multiplier": round(kelly, 3),
            # Keep human-readable name for logging
            "display_name": raw_name,
        }

    return stats


def classify_precision(win_rate: float) -> str:
    """Classify precision tier from win rate."""
    if win_rate >= WR_HIGH:
        return "HIGH"
    if win_rate >= WR_MEDIUM:
        return "MEDIUM"
    return "LOW"


def update_tier_assignments(
    current: dict[str, Any],
    addr_index: dict[str, str],
    stats: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Merge computed stats into tier assignments, preserving capital tier.

    Matches whales from stats to TA entries using address + name lookup.
    """
    updated = 0
    created = 0

    for key, s in stats.items():
        if s["trade_count"] < MIN_TRADES:
            continue

        display = s.get("display_name", key)
        ta_key = _find_ta_key(key, addr_index)

        if ta_key is None:
            # New whale — create entry using key (address or name)
            ta_key = key
            prev_entry = {}
        else:
            prev_entry = current.get(ta_key, {})

        precision_tier = classify_precision(s["win_rate"])
        kelly_mult = s["kelly_multiplier"]

        # Capital tier: use existing or compute from volume
        prev_cap = prev_entry.get("capital_tier", "E")
        new_cap = _maybe_upgrade_capital(
            prev_entry.get("volume", 0),
            s["total_volume"],
            prev_cap,
        )

        should_copy = precision_tier == "HIGH" and kelly_mult >= 0.6
        should_fade = precision_tier == "LOW" and kelly_mult <= 0.3

        updated_entry = {
            "capital_tier": new_cap,
            "precision_tier": precision_tier,
            "kelly_multiplier": kelly_mult,
            "win_rate": s["win_rate"],
            "volume": s["total_volume"],
            "classification": prev_entry.get("classification", "unknown"),
            "should_copy": should_copy,
            "should_fade": should_fade,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "trade_count_30d": s["trade_count"],
            "total_pnl_30d": s["total_pnl"],
            "consistency": s["consistency"],
            # Keep original TA key for reference
            "_ta_key": ta_key,
            "_display_name": display,
        }
        current[ta_key] = updated_entry
        if prev_entry:
            updated += 1
        else:
            created += 1

        logger.debug(
            "%s (key=%s) → tier=%s kelly=%.3f WR=%.2f PnL=$%.2f",
            display, ta_key[:20], precision_tier, kelly_mult, s["win_rate"], s["total_pnl"],
        )

    logger.info(
        "Updated %d whales, created %d new entries "
        "(skipped %d below min trades threshold)",
        updated, created,
        sum(1 for s in stats.values() if s["trade_count"] < MIN_TRADES),
    )
    return current


def _maybe_upgrade_capital(prev_volume: float, curr_volume: float, prev_tier: str) -> str:
    """Upgrade capital tier if cumulative volume warrants it. Never downgrades."""
    # Use the higher of previous and current volume for tier classification
    vol = max(prev_volume, curr_volume)
    cap_order = ["E", "D", "C", "B", "A"]
    for tier in reversed(cap_order):
        if vol >= {"A": 200_000, "B": 50_000, "C": 10_000, "D": 1_000, "E": 0}[tier]:
            # Only upgrade, never downgrade
            cur_idx = cap_order.index(prev_tier)
            new_idx = cap_order.index(tier)
            return cap_order[min(cur_idx, new_idx)]
    return prev_tier


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Update whale tier assignments from resolved trades")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Extra debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not DB_PATH.exists():
        logger.error("trades.db not found at %s", DB_PATH)
        sys.exit(1)

    logger.info(
        "Computing whale stats from last %d days (min %d trades, min $%s volume)",
        ROLLING_WINDOW_DAYS, MIN_TRADES, f"{MIN_VOLUME_USD:,}",
    )

    conn = sqlite3.connect(DB_PATH)
    stats = compute_whale_stats(conn)
    conn.close()

    if not stats:
        logger.info("No whales met the minimum thresholds — nothing to update")
        sys.exit(0)

    logger.info("Computed stats for %d whales", len(stats))

    current, addr_index = load_tier_assignments()
    updated = update_tier_assignments(current, addr_index, stats)

    if args.dry_run:
        # Show diff
        for name, entry in updated.items():
            prev = current.get(name, {})
            changes = {
                k: (prev.get(k), v)
                for k, v in entry.items()
                if prev.get(k) != v and k != "last_updated"
            }
            if changes:
                logger.info("WOULD UPDATE %s: %s", name, changes)
    else:
        save_tier_assignments(updated)
        logger.info("Done — tier_assignments.json updated")

        # Save a snapshot for the bridge state — MERGE with existing to preserve
        # signal bridge tracking keys (condition_id|timestamp entries).
        # See "Shared State Warning" in autoresearch-bridge skill for details.
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if STATE_FILE.exists():
            try:
                existing = json.loads(STATE_FILE.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        existing.update({
            "last_run": datetime.now(timezone.utc).isoformat(),
            "whales_updated": len(stats),
            "snapshot": {name: {k: v for k, v in s.items() if k in (
                "win_rate", "kelly_multiplier", "precision_tier", "trade_count_30d", "total_pnl_30d"
            )} for name, s in stats.items()}
        })
        STATE_FILE.write_text(json.dumps(existing, indent=2, sort_keys=False))


if __name__ == "__main__":
    main()
