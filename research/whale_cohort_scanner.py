#!/usr/bin/env python3
"""Whale Cohort Scanner — Weekly scan for new fade-worthy whale candidates.

Scans poly_whale_stats for wallets matching fade criteria and emits a JSON report.
Run weekly via cron: scripts/whale_cohort_cron.sh
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WhaleCohortScanner")

NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
DB_PATH = NAUTILUS_ROOT / "data" / "trades.db"
KNOWN_WHALE_PATH = NAUTILUS_ROOT / "config" / "known_whale_wallets.json"
CACHE_PATH = NAUTILUS_ROOT / "research" / ".whale_cohort_cache.json"
REPORT_DIR = NAUTILUS_ROOT / "research"


def load_known_whales() -> set[str]:
    """Load addresses from known_whale_wallets.json."""
    known = set()
    if not KNOWN_WHALE_PATH.exists():
        logger.warning("known_whale_wallets.json not found at %s", KNOWN_WHALE_PATH)
        return known
    try:
        data = json.loads(KNOWN_WHALE_PATH.read_text())
        for name, addr in data.items():
            if name.startswith("_"):
                continue
            if addr and isinstance(addr, str):
                known.add(addr.lower())
        logger.info("Loaded %d known whale addresses", len(known))
    except Exception as e:
        logger.error("Failed to load known whales: %s", e)
    return known


def load_cache() -> list[dict]:
    """Load pre-cached whale stats if available."""
    if not CACHE_PATH.exists():
        return []
    try:
        data = json.loads(CACHE_PATH.read_text())
        logger.info("Loaded %d cached whale stats", len(data.get("whales", [])))
        return data.get("whales", [])
    except Exception as e:
        logger.warning("Failed to load cache: %s", e)
        return []


def get_top_markets(db: sqlite3.Connection, address: str, limit: int = 5) -> list[str]:
    """Get top markets for a given address from poly_address_map or trades table."""
    try:
        rows = db.execute(
            """SELECT DISTINCT condition_id FROM trades
               WHERE LOWER(whale_address) = ?
               ORDER BY volume_usd DESC LIMIT ?""",
            (address.lower(), limit),
        ).fetchall()
        if rows:
            return [r[0] for r in rows if r[0]]
    except Exception:
        pass
    return []


def compute_fade_worthiness(row: dict) -> float:
    """Compute fade_worthiness score (0-100), normalized across all classifications.

    Higher for bots with high avg_trade_size — these are good fade targets.
    All classifications are normalized to 0-100 so the >=75 threshold is meaningful.
    """
    classification = row.get("classification", "")
    avg_size = row.get("avg_trade_size_usd", 0)
    total_trades = row.get("total_trades", 0)
    volume = row.get("total_volume_usd", 0)

    if classification == "trading_bot":
        size_score = min(avg_size / 1000 * 40, 40)
        freq_score = min(total_trades / 500 * 30, 30)
        vol_score = min(volume / 50000 * 30, 30)
        return round(size_score + freq_score + vol_score, 1)

    elif classification in ("skilled_human", "degenerate_human", "market_maker"):
        size_max = 250 if classification == "degenerate_human" else 150
        vol_max = 100
        raw = min(avg_size / 500 * 15, size_max) + min(volume / 100000 * 10, vol_max)
        return round(min(raw, size_max + vol_max) * (100 / (size_max + vol_max)), 1)

    else:
        return round(min(volume / 100000 * 10, 100), 1)


def scan_whale_cohort() -> dict:
    """Scan poly_whale_stats for new fade-worthy whale candidates."""
    known_whales = load_known_whales()
    cached_whales = load_cache()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    query = """
        SELECT
            address,
            classification,
            total_trades,
            total_volume_usd,
            avg_trade_size_usd,
            buy_count,
            sell_count,
            buy_sell_ratio,
            unique_markets,
            nautilus_whale_name
        FROM poly_whale_stats
        WHERE (
            (classification = 'trading_bot' AND total_trades > 50 AND avg_trade_size_usd > 500)
            OR (classification = 'skilled_human' AND buy_sell_ratio > 0.6 AND total_trades > 100)
            OR (total_volume_usd > 100000)
        )
        ORDER BY total_volume_usd DESC
    """

    rows = conn.execute(query).fetchall()
    conn.close()

    candidates = []
    for row in rows:
        r = dict(row)
        r["address"] = r["address"].lower()
        r["top_markets"] = get_top_markets(conn, r["address"]) if conn else []
        r["fade_worthiness_score"] = compute_fade_worthiness(r)
        candidates.append(r)

    new_candidates = [c for c in candidates if c["address"] not in known_whales]
    existing_candidates = [c for c in candidates if c["address"] in known_whales]

    bots = [c for c in candidates if c["classification"] == "trading_bot"]
    humans_to_avoid = [c for c in candidates if c["classification"] == "skilled_human"]

    bots.sort(key=lambda x: x["fade_worthiness_score"], reverse=True)
    humans_to_avoid.sort(key=lambda x: x["fade_worthiness_score"], reverse=True)

    report = {
        "scan_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "total_scanned": len(candidates),
        "new_candidates": new_candidates[:20],
        "existing_candidates": existing_candidates[:20],
        "top_bots": bots[:5],
        "top_humans_to_avoid": humans_to_avoid[:5],
    }

    return report


def emit_report(report: dict) -> Path:
    """Write JSON report to research directory."""
    scan_date = report["scan_date"]
    filename = f"whale_cohort_report_{scan_date}.json"
    path = REPORT_DIR / filename
    path.write_text(json.dumps(report, indent=2, default=str))
    logger.info("Emitted report to %s", path)
    return path


def main():
    logger.info("=== Whale Cohort Scanner Starting ===")

    report = scan_whale_cohort()
    path = emit_report(report)

    logger.info("Scan complete: %d total, %d new, %d existing",
                report["total_scanned"],
                len(report["new_candidates"]),
                len(report["existing_candidates"]))

    print(f"\nReport written to: {path}")
    print(f"Total scanned: {report['total_scanned']}")
    print(f"New candidates: {len(report['new_candidates'])}")
    print(f"Existing candidates: {len(report['existing_candidates'])}")
    print(f"Top bots: {len(report['top_bots'])}")
    print(f"Top humans to avoid: {len(report['top_humans_to_avoid'])}")

    logger.info("=== Whale Cohort Scanner Complete ===")


if __name__ == "__main__":
    main()
