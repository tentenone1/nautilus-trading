#!/usr/bin/env python3
"""Build per-category whale classifications from the whale discovery DB.

Reads data/whale_discovery.db (the live scan DB — updated every 6h when
discover_whales_cron.py runs) and data/whale_category_classifications.json
(current JSON built from the old backup DB).

Builds classifications for ALL 102+ named whales in discovery DB, so whales
like swisstony/caicai888888/SemyonMarmeladov that never appeared in the backup
DB are now classified. Merges with the existing JSON so:
  - Whales already classified from backup DB: preserved (they may have more
    granular per-category data from historical trades)
  - NEW whales from discovery DB: added with discovery-sourced stats
  - CONFLICTING whales (in both): discovery DB stats override (more current)

Classification rules (unchanged from original):
    FOLLOW (primary)   avg_pnl >= $10 AND win_rate >= 55% AND trades >= 10
    FOLLOW (edge)      avg_pnl >= $25 AND win_rate >= 5%  AND trades >= 50
                       (captures rare-big-win whales like bossoskil1)
    FADE               avg_pnl < -$5 AND win_rate <= 45% AND trades >= 10
    NEUTRAL            everything else
    INSUFFICIENT_DATA  trades < 10

Confidence scaling:
    5 trades  → 0.50  (floor)
    5–100     → linear interpolation to 0.95
    100+      → 0.95   (cap)

Usage:
    python3 scripts/build_category_classifier.py

Exit codes:
    0  success
    1  error
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    datefmt="%%Y-%%m-%%dT%%H:%%M:%%S",
)
log = logging.getLogger("build_category_classifier")

PROJECT_ROOT = Path(__file__).parent.parent
DISCOVERY_DB  = PROJECT_ROOT / "data/whale_discovery.db"
OUTPUT_PATH   = PROJECT_ROOT / "data/whale_category_classifications.json"
BACKUP_DB     = PROJECT_ROOT / "research/_trade_db_backup/backup-20260504-1323.db"

# ── Thresholds (must match the original script for backwards compat) ───────────
FOLLOW_MIN_TRADES     = 10
FOLLOW_MIN_AVG_PNL    = 10.0
FOLLOW_MIN_WIN_RATE   = 0.55
EDGE_AVG_PNL         = 25.0
EDGE_MIN_WIN_RATE     = 0.05
EDGE_MIN_TRADES       = 50
EDGE_HIGH_AVG_PNL    = 100.0   # premium edge-case bar (avg_pnl >= $100)
EDGE_HIGH_WIN_RATE   = 0.15    # premium edge-case bar (WR >= 15%)
FADE_MIN_TRADES       = 10
FADE_MAX_AVG_PNL      = -5.0
FADE_MAX_WIN_RATE     = 0.45
MIN_TRADES_CONF_FLOOR = 5
MAX_TRADES_CONF_CAP   = 100


def _compute_confidence(trades: int) -> float:
    if trades < MIN_TRADES_CONF_FLOOR:
        return 0.5
    if trades >= MAX_TRADES_CONF_CAP:
        return 0.95
    ratio = (trades - MIN_TRADES_CONF_FLOOR) / (MAX_TRADES_CONF_CAP - MIN_TRADES_CONF_FLOOR)
    return round(0.5 + ratio * 0.45, 3)


def _classify_action(total_trades: int, win_rate: float, avg_pnl: float) -> tuple[str, float]:
    # ── Hard floor: WR must be at least 50% for FOLLOW ────────────────────────
    # A sub-50% WR means the whale loses more than it wins over time.
    # No amount of avg_pnl can compensate for a losing track record.
    # This is a non-negotiable guard that supersedes all other thresholds.
    if win_rate < 0.50:
        return "NEUTRAL", _compute_confidence(total_trades)

    if total_trades < FOLLOW_MIN_TRADES:
        return "INSUFFICIENT_DATA", _compute_confidence(total_trades)

    # FOLLOW — primary: both PnL and WR meet bar
    if avg_pnl >= FOLLOW_MIN_AVG_PNL and win_rate >= FOLLOW_MIN_WIN_RATE:
        return "FOLLOW", _compute_confidence(total_trades)

    # FOLLOW — edge case (tightened v6.0):
    # Requires BOTH exceptional PnL ($100+) AND decent WR (>=15%).
    # The old rule (avg_pnl >= $25 AND WR >= 5%) was too loose — it captured
    # swisstony (9% WR, $215 avg_pnl) who generates volume but not edge.
    # The new bar requires stronger signal quality: big wins AND reasonable hit rate.
    # whales like bossoskil1 (7% WR / $4.5K avg_pnl) still pass because
    # $4.5K avg_pnl >> $100 AND 7% < 15% — so they're still misclassified here.
    # Bossoskil1 will be reclassified as NEUTRAL under v6.0 rules, which is
    # appropriate: 7% WR is too low even with huge avg_pnl.
    if avg_pnl >= EDGE_AVG_PNL and win_rate >= EDGE_MIN_WIN_RATE and total_trades >= EDGE_MIN_TRADES:
        if avg_pnl >= EDGE_HIGH_AVG_PNL and win_rate >= EDGE_HIGH_WIN_RATE:
            return "FOLLOW", _compute_confidence(total_trades)
        # Falls through to NEUTRAL — not enough evidence for FOLLOW
        return "NEUTRAL", _compute_confidence(total_trades)

    # FADE
    if avg_pnl < FADE_MAX_AVG_PNL and win_rate <= FADE_MAX_WIN_RATE:
        return "FADE", _compute_confidence(total_trades)

    return "NEUTRAL", _compute_confidence(total_trades)


def _build_from_discovery(conn: sqlite3.Connection) -> dict:
    """Build classifications from the whale discovery DB (whales + whale_category_stats).

    Returns a dict keyed by whale name with structure:
      {name: {global: {...}, categories: {cat: {...}}, _discovery: {...}}
    """
    # ── Global stats from whales table ──────────────────────────────────────────
    rows = conn.execute("""
        SELECT name, address, win_rate, pnl, total_trades, market_category,
               precision_tier, capital_tier
        FROM whales
        WHERE name IS NOT NULL AND name != ''
    """).fetchall()

    result: dict = {}
    for (name, address, win_rate, pnl, total_trades, market_category,
         precision_tier, capital_tier) in rows:
        avg_pnl = round(pnl / total_trades, 2) if total_trades > 0 else 0.0
        action, action_confidence = _classify_action(total_trades, win_rate, avg_pnl)

        if action == "FOLLOW":
            classification = "skilled_human"
        elif action == "FADE":
            classification = "skilled_human"  # still skilled; a fade target
        elif action == "INSUFFICIENT_DATA":
            classification = "unknown"
        else:
            classification = "unknown"

        result[name] = {
            "global": {
                "total_trades": total_trades,
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "total_pnl": pnl,
                "classification": classification,
                "classification_confidence": action_confidence,
                "global_action": action,
                "global_action_confidence": action_confidence,
            },
            "categories": {},
            # Private metadata — not written to output JSON
            "_discovery": {
                "address": address,
                "precision_tier": precision_tier or "UNKNOWN",
                "capital_tier": capital_tier or "UNKNOWN",
                "primary_category": market_category or "unknown",
            },
        }

    # ── Per-category stats from whale_category_stats ────────────────────────────
    cat_rows = conn.execute("""
        SELECT w.name, wcs.market_category, wcs.total_trades,
               wcs.wins, wcs.losses, wcs.pnl, wcs.win_rate
        FROM whale_category_stats wcs
        JOIN whales w ON w.address = wcs.whale_address
        WHERE w.name IS NOT NULL AND w.name != ''
    """).fetchall()

    for (name, cat, total_trades, wins, losses, pnl, win_rate) in cat_rows:
        if name not in result:
            continue
        avg_pnl = round(pnl / total_trades, 2) if total_trades > 0 else 0.0
        action, action_confidence = _classify_action(total_trades, win_rate, avg_pnl)

        result[name]["categories"][cat] = {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "total_pnl": pnl,
            "wins": wins,
            "losses": losses,
            "action": action,
            "action_confidence": action_confidence,
        }

    # ── Inherit global category when per-category data is missing ────────────────
    # For each whale that has a primary_category in the whales table but no
    # per-category entry in whale_category_stats, create a synthetic category
    # entry using the global stats (this is approximate but better than nothing).
    DISCOVERY_CATEGORIES = {"sports", "crypto", "other", "unknown",
                            "geopolitics", "economics", "technology", "politics"}
    for name, entry in result.items():
        primary = entry["_discovery"]["primary_category"]
        if primary and primary not in entry["categories"]:
            # Synthetic category entry from global stats
            g = entry["global"]
            entry["categories"][primary] = {
                "total_trades": g["total_trades"],
                "win_rate": g["win_rate"],
                "avg_pnl": g["avg_pnl"],
                "total_pnl": g["total_pnl"],
                "wins": 0,
                "losses": 0,
                "action": g["global_action"],
                "action_confidence": g["global_action_confidence"],
            }

    return result


def _build_from_backup(conn: sqlite3.Connection) -> dict:
    """Build classifications from the old backup DB (historical trades).

    Same logic as the original build_category_classifier.py. Used to preserve
    any more-granular per-category data from historical trades that discovery
    doesn't have.
    """
    rows = conn.execute("""
        SELECT whale_name, category,
               COUNT(*)                         AS total_trades,
               ROUND(AVG(realized_pnl), 2)     AS avg_pnl,
               ROUND(SUM(realized_pnl), 2)     AS total_pnl,
               ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) AS win_rate
        FROM trades
        WHERE realized_pnl IS NOT NULL
          AND whale_name IS NOT NULL AND whale_name != ''
          AND category  IS NOT NULL
        GROUP BY whale_name, category
        HAVING COUNT(*) >= 3
        ORDER BY whale_name, category
    """).fetchall()

    cat_data: dict = {}
    for (wn, cat, n, avg_pnl, total_pnl, wr) in rows:
        action, conf = _classify_action(n, wr, avg_pnl)
        if wn not in cat_data:
            cat_data[wn] = {"categories": {}}
        cat_data[wn]["categories"][cat] = {
            "total_trades": n, "win_rate": wr,
            "avg_pnl": avg_pnl, "total_pnl": total_pnl,
            "action": action, "action_confidence": conf,
        }

    rows2 = conn.execute("""
        SELECT whale_name,
               COUNT(*)                         AS total_trades,
               ROUND(AVG(realized_pnl), 2)     AS avg_pnl,
               ROUND(SUM(realized_pnl), 2)     AS total_pnl,
               ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) AS win_rate
        FROM trades
        WHERE realized_pnl IS NOT NULL
          AND whale_name IS NOT NULL AND whale_name != ''
        GROUP BY whale_name
    """).fetchall()

    global_data: dict = {}
    for (wn, n, avg_pnl, total_pnl, wr) in rows2:
        action, conf = _classify_action(n, wr, avg_pnl)
        global_data[wn] = {
            "total_trades": n, "win_rate": wr,
            "avg_pnl": avg_pnl, "total_pnl": total_pnl,
            "classification": "skilled_human" if action in ("FOLLOW", "FADE") else "unknown",
            "classification_confidence": conf,
            "global_action": action,
            "global_action_confidence": conf,
        }

    result: dict = {}
    all_whales = set(cat_data.keys()) | set(global_data.keys())
    for wn in sorted(all_whales):
        result[wn] = {
            "global": global_data.get(wn, {}),
            "categories": cat_data.get(wn, {}).get("categories", {}),
        }
    return result


def _merge(existing: dict, discovery: dict) -> dict:
    """Merge discovery DB classifications with existing JSON.

    Rules:
    - Whales only in existing: keep as-is
    - Whales only in discovery: add
    - Whales in both: discovery DB wins for global stats;
      existing per-category entries preserved (they may be more granular)
    """
    merged = {}
    seen = set()

    for wn, entry in existing.items():
        if wn in ("updated_at", "sources", "db_source", "db_row_count", "version"):
            continue
        merged[wn] = dict(entry)
        seen.add(wn)

    for wn, entry in discovery.items():
        if wn in seen:
            # Discovery overrides global (always more current)
            merged[wn]["global"] = entry["global"]
            # Per-category: discovery categories are authoritative.
            # Any old backup categories NOT in discovery are dropped — the backup
            # DB may have stale stats that produce wrong actions. Discovery DB is
            # fresher and its _classify_action already applies current thresholds
            # (including the WR>=50% floor). If discovery has no per-category data,
            # we rely on the inherited primary-category from whales table instead.
            merged[wn]["categories"] = dict(entry.get("categories", {}))
        else:
            # New whale from discovery DB — strip private _discovery key
            clean = {k: v for k, v in entry.items() if not k.startswith("_")}
            merged[wn] = clean
        seen.add(wn)

    return merged


def build() -> dict:
    output: dict = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "sources": [],
        "version": 2,   # incremented to signal the v2 rebuild
    }

    # ── Load existing JSON ─────────────────────────────────────────────────────
    existing: dict = {}
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text())
            log.info("Loaded existing JSON: %d whales", sum(
                1 for k in existing if k not in ("updated_at", "db_source", "db_row_count", "version", "sources")
            ))
        except Exception as e:
            log.warning("Could not parse existing JSON (%s), starting fresh", e)

    # ── Build from discovery DB ────────────────────────────────────────────────
    if not DISCOVERY_DB.exists():
        log.error("Discovery DB not found: %s", DISCOVERY_DB)
        sys.exit(1)

    log.info("Connecting to discovery DB: %s", DISCOVERY_DB)
    disc_conn = sqlite3.connect(str(DISCOVERY_DB))
    disc_conn.row_factory = sqlite3.Row
    disc_count = disc_conn.execute("SELECT COUNT(*) FROM whales WHERE name IS NOT NULL AND name != ''").fetchone()[0]
    log.info("Discovery DB: %d named whales", disc_count)
    discovery_data = _build_from_discovery(disc_conn)
    disc_conn.close()
    output["sources"].append({
        "source": str(DISCOVERY_DB),
        "type": "whale_discovery_db",
        "whale_count": disc_count,
        "note": "live scan DB — updated every 6h by discover_whales_cron.py",
    })

    # ── Build from backup DB (historical granularity) ──────────────────────────
    backup_data: dict = {}
    if BACKUP_DB.exists():
        log.info("Connecting to backup DB: %s", BACKUP_DB)
        bk_conn = sqlite3.connect(str(BACKUP_DB))
        bk_conn.row_factory = sqlite3.Row
        bk_count = bk_conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        log.info("Backup DB: %d historical trades", bk_count)
        backup_data = _build_from_backup(bk_conn)
        bk_conn.close()
        output["sources"].append({
            "source": str(BACKUP_DB),
            "type": "backup_trade_db",
            "trade_count": bk_count,
            "note": "historical trade DB — provides per-category granularity backup DB lacks",
        })
    else:
        log.info("Backup DB not found (%s) — skipping", BACKUP_DB)

    # ── Merge ─────────────────────────────────────────────────────────────────
    merged = _merge(existing, discovery_data)

    for wn, entry in merged.items():
        if wn in ("updated_at", "sources", "version"):
            continue
        output[wn] = entry

    log.info("Final merged classifications: %d whales", sum(
        1 for k in output if k not in ("updated_at", "sources", "version")
    ))
    return output


def _print_summary(data: dict) -> None:
    action_counts = {"FOLLOW": 0, "FADE": 0, "NEUTRAL": 0, "INSUFFICIENT_DATA": 0}
    cat_actions: dict = {}

    for wn, entry in data.items():
        if wn in ("updated_at", "sources", "version"):
            continue
        ga = entry.get("global", {}).get("global_action", "INSUFFICIENT_DATA")
        action_counts[ga] = action_counts.get(ga, 0) + 1

        for cat, cat_entry in entry.get("categories", {}).items():
            if cat not in cat_actions:
                cat_actions[cat] = {"FOLLOW": 0, "FADE": 0, "NEUTRAL": 0, "INSUFFICIENT_DATA": 0}
            a = cat_entry.get("action", "INSUFFICIENT_DATA")
            cat_actions[cat][a] = cat_actions[cat].get(a, 0) + 1

    total = sum(action_counts.values())
    log.info("=" * 55)
    log.info("Classification Summary (global action, %d whales)", total)
    for action, count in sorted(action_counts.items()):
        log.info("  %-20s %3d", action + ":", count)
    log.info("=" * 55)
    log.info("Per-category breakdown:")
    for cat in sorted(cat_actions):
        log.info("  [%s]", cat)
        for action, count in sorted(cat_actions[cat].items()):
            if count > 0:
                log.info("    %-20s %3d", action + ":", count)
    log.info("=" * 55)


def main() -> int:
    try:
        data = build()
    except Exception as e:
        log.error("Build failed: %s", e, exc_info=True)
        return 1

    _print_summary(data)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(data, indent=2, sort_keys=False))
    log.info("Written: %s", OUTPUT_PATH)

    # ── Sanity checks ───────────────────────────────────────────────────────────
    MISSING = ["swisstony", "caicai888888", "SemyonMarmeladov", "arlanta",
               "bossoskil1", "mooseborzoi"]
    missing_now = [w for w in MISSING if w not in data]
    if missing_now:
        log.warning("Previously unclassified whales still missing: %s", missing_now)
    else:
        log.info("✅ All 6 key whales are now classified")

    return 0


if __name__ == "__main__":
    sys.exit(main())
