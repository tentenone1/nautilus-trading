#!/usr/bin/env python3
"""Poly Data Loader — Historical whale behavior enrichment from poly_data.

Loads 282K processed trades + 322K goldsky fills from the poly_data repository
and computes behavioral statistics per address. These stats enrich:

1. whale_classifier.py — historical behavioral patterns for classification
2. edge_scorer.py — historical volume/frequency/consistency signals
3. whale_profiles.py — LLM-driven profiling with historical context
4. signal_pipeline.py — domain-specific trust scores from historical data

Output tables in trades.db:
  - poly_whale_stats: per-address behavioral statistics
  - poly_market_stats: per-market historical statistics
  - poly_address_map: address → nautilus whale_name mapping (where overlap exists)

Output file: data/poly_historical_profiles.json (for LLM consumption)
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("PolyDataLoader")

# ── Paths ─────────────────────────────────────────────────────────────────────
NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
DB_PATH = NAUTILUS_ROOT / "data" / "trades.db"
OUTPUT_PATH = NAUTILUS_ROOT / "data" / "poly_historical_profiles.json"
COHORT_CACHE_PATH = NAUTILUS_ROOT / "research" / ".whale_cohort_cache.json"

POLY_DATA_ROOT = Path("/home/elon-1/projects/poly_data")
PROCESSED_TRADES = POLY_DATA_ROOT / "processed" / "trades.csv"
MARKETS_CSV = POLY_DATA_ROOT / "markets.csv"
GOLDSKY_FILLS = POLY_DATA_ROOT / "goldsky" / "orderFilled.csv"

# ── Classification thresholds (aligned with whale_classifier.py) ───────────────
SKILLED_WIN_RATE = 0.50
DEGEN_WIN_RATE = 0.30
SACRIFICIAL_WIN_RATE = 0.15
MM_TWO_SIDED_RATIO = 0.30
MM_MIN_TRADES = 50
BOT_ROUND_NUMBER_RATIO = 0.40  # % of round-number sizes → bot signal
BOT_247_SPREAD = 0.50  # trades spread across many hours → bot


def _ensure_tables(db: sqlite3.Connection) -> None:
    """Create poly_data tables if they don't exist."""
    db.executescript("""
        CREATE TABLE IF NOT EXISTS poly_whale_stats (
            address TEXT PRIMARY KEY,
            total_trades INTEGER DEFAULT 0,
            total_volume_usd REAL DEFAULT 0,
            buy_count INTEGER DEFAULT 0,
            sell_count INTEGER DEFAULT 0,
            buy_sell_ratio REAL DEFAULT 0,
            unique_markets INTEGER DEFAULT 0,
            avg_trade_size_usd REAL DEFAULT 0,
            max_trade_size_usd REAL DEFAULT 0,
            min_trade_size_usd REAL DEFAULT 0,
            median_trade_size_usd REAL DEFAULT 0,
            price_range_low REAL DEFAULT 0,
            price_range_high REAL DEFAULT 0,
            avg_price REAL DEFAULT 0,
            first_trade_ts TEXT DEFAULT '',
            last_trade_ts TEXT DEFAULT '',
            active_days INTEGER DEFAULT 0,
            trades_per_day REAL DEFAULT 0,
            hour_entropy REAL DEFAULT 0,
            round_number_ratio REAL DEFAULT 0,
            classification TEXT DEFAULT 'unknown',
            classification_confidence REAL DEFAULT 0,
            action TEXT DEFAULT 'ignore',
            action_confidence REAL DEFAULT 0,
            nautilus_whale_name TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS poly_market_stats (
            condition_id TEXT PRIMARY KEY,
            question TEXT DEFAULT '',
            slug TEXT DEFAULT '',
            total_trades INTEGER DEFAULT 0,
            total_volume_usd REAL DEFAULT 0,
            unique_traders INTEGER DEFAULT 0,
            avg_price REAL DEFAULT 0,
            first_trade_ts TEXT DEFAULT '',
            last_trade_ts TEXT DEFAULT '',
            updated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS poly_address_map (
            address TEXT,
            nautilus_whale_name TEXT,
            nautilus_source TEXT DEFAULT '',
            match_type TEXT DEFAULT '',
            PRIMARY KEY (address, nautilus_whale_name)
        );

        CREATE INDEX IF NOT EXISTS idx_poly_whale_class ON poly_whale_stats(classification);
        CREATE INDEX IF NOT EXISTS idx_poly_whale_action ON poly_whale_stats(action);
        CREATE INDEX IF NOT EXISTS idx_poly_whale_name ON poly_whale_stats(nautilus_whale_name);
        CREATE INDEX IF NOT EXISTS idx_poly_market_cid ON poly_market_stats(condition_id);
    """)
    db.commit()


def _compute_hour_entropy(hours: list[int]) -> float:
    """Shannon entropy of trading hour distribution. High entropy = 24/7 (bot)."""
    if not hours:
        return 0.0
    from math import log2
    counts = defaultdict(int)
    for h in hours:
        counts[h] += 1
    total = sum(counts.values())
    if total == 0:
        return 0.0
    entropy = 0.0
    for c in counts.values():
        p = c / total
        if p > 0:
            entropy -= p * log2(p)
    return round(entropy, 3)


def _is_round_number(val: float) -> bool:
    """Check if a USD amount is suspiciously round (bot indicator)."""
    if val <= 0:
        return False
    if val >= 100 and val == int(val):
        return True
    if val >= 10 and val * 10 == int(val * 10):
        return True
    return False


def _classify_whale(stats: dict) -> tuple[str, float, str, float]:
    """Classify a whale based on its behavioral stats.

    Returns: (classification, confidence, action, action_confidence)
    """
    n = stats.get("total_trades", 0)
    if n < 5:
        return "unknown", 0.1, "ignore", 0.0

    buy_sell_ratio = stats.get("buy_sell_ratio", 0)
    vol = stats.get("total_volume_usd", 0)
    avg_size = stats.get("avg_trade_size_usd", 0)
    hour_entropy = stats.get("hour_entropy", 0)
    round_ratio = stats.get("round_number_ratio", 0)
    markets = stats.get("unique_markets", 0)

    # Market maker: high two-sided activity, many trades, many markets
    if n >= MM_MIN_TRADES and buy_sell_ratio > 0.4 and buy_sell_ratio < 2.5 and markets > 5:
        two_sided = min(stats.get("buy_count", 0), stats.get("sell_count", 0))
        two_sided_ratio = two_sided / max(n, 1) if n > 0 else 0
        if two_sided_ratio >= MM_TWO_SIDED_RATIO:
            conf = min(0.5 + two_sided_ratio, 0.95)
            return "market_maker", conf, "fade", conf * 0.8

    # Bot: round numbers, 24/7 activity, high frequency
    is_bot = False
    bot_signals = 0
    if round_ratio >= BOT_ROUND_NUMBER_RATIO:
        bot_signals += 1
    if hour_entropy >= 3.5:  # Near-uniform hour distribution
        bot_signals += 1
    if n > 1000 and stats.get("trades_per_day", 0) > 20:
        bot_signals += 1
    if bot_signals >= 2:
        is_bot = True
        conf = 0.6 + 0.15 * min(bot_signals, 3)
        return "trading_bot", min(conf, 0.95), "fade", conf * 0.6

    # Skilled human: good volume, moderate frequency, selective markets
    if n >= 5 and n < 1000 and markets < 100 and hour_entropy < 3.0:
        conf = 0.5 + min(markets / 20, 0.3)
        return "skilled_human", min(conf, 0.85), "copy", conf * 0.7

    # Degenerate: high volume, many markets, erratic
    if n > 100 and markets > 50 and hour_entropy > 2.5:
        return "degenerate_human", 0.6, "fade", 0.5

    # Mixed entity: doesn't fit other categories
    return "mixed_entity", 0.3, "ignore", 0.2


def load_processed_trades() -> dict:
    """Load poly_data processed trades and compute per-address stats."""
    logger.info("Loading processed trades from %s", PROCESSED_TRADES)
    if not PROCESSED_TRADES.exists():
        logger.error("Processed trades file not found: %s", PROCESSED_TRADES)
        return {}

    address_data = defaultdict(lambda: {
        "total_trades": 0, "total_usd": 0.0,
        "buy_count": 0, "sell_count": 0,
        "prices": [], "sizes": [],
        "hours": [], "markets": set(),
        "timestamps": [],
    })

    count = 0
    with open(PROCESSED_TRADES, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            for addr_field in ("maker", "taker"):
                addr = row.get(addr_field, "").strip().lower()
                if not addr:
                    continue
                d = address_data[addr]
                d["total_trades"] += 1
                try:
                    usd = float(row.get("usd_amount", 0) or 0)
                    d["total_usd"] += usd
                    d["sizes"].append(usd)
                except (ValueError, TypeError):
                    pass
                try:
                    d["prices"].append(float(row.get("price", 0) or 0))
                except (ValueError, TypeError):
                    pass
                ts = row.get("timestamp", "")
                if ts:
                    d["timestamps"].append(ts)
                    try:
                        hour = int(ts.split("T")[1][:2]) if "T" in ts else 12
                        d["hours"].append(hour)
                    except (ValueError, IndexError):
                        pass
                mid = row.get("market_id", "").strip()
                if mid:
                    d["markets"].add(mid)

            # Taker direction
            taker = row.get("taker", "").strip().lower()
            if taker and taker in address_data:
                direction = row.get("taker_direction", "").upper()
                if direction == "BUY":
                    address_data[taker]["buy_count"] += 1
                elif direction == "SELL":
                    address_data[taker]["sell_count"] += 1

    logger.info("Loaded %d trades, %d unique addresses", count, len(address_data))

    # Compute derived stats and classify
    results = {}
    for addr, d in address_data.items():
        n = d["total_trades"]
        sizes = d["sizes"]
        prices = d["prices"]
        buy_count = d["buy_count"]
        sell_count = d["sell_count"]

        stats = {
            "address": addr,
            "total_trades": n,
            "total_volume_usd": round(d["total_usd"], 2),
            "buy_count": buy_count,
            "sell_count": sell_count,
            "buy_sell_ratio": round(buy_count / max(sell_count, 1), 3),
            "unique_markets": len(d["markets"]),
            "avg_trade_size_usd": round(d["total_usd"] / max(n, 1), 2),
            "max_trade_size_usd": round(max(sizes), 2) if sizes else 0,
            "min_trade_size_usd": round(min(sizes), 2) if sizes else 0,
            "median_trade_size_usd": round(sorted(sizes)[len(sizes)//2], 2) if sizes else 0,
            "price_range_low": round(min(prices), 4) if prices else 0,
            "price_range_high": round(max(prices), 4) if prices else 0,
            "avg_price": round(sum(prices) / max(len(prices), 1), 4) if prices else 0,
            "first_trade_ts": min(d["timestamps"]) if d["timestamps"] else "",
            "last_trade_ts": max(d["timestamps"]) if d["timestamps"] else "",
            "hour_entropy": _compute_hour_entropy(d["hours"]),
            "round_number_ratio": round(
                sum(1 for s in sizes if _is_round_number(s)) / max(len(sizes), 1), 3
            ),
        }

        # Active days
        if d["timestamps"]:
            dates = set(ts[:10] for ts in d["timestamps"] if ts)
            stats["active_days"] = len(dates)
            stats["trades_per_day"] = round(n / max(len(dates), 1), 2)
        else:
            stats["active_days"] = 0
            stats["trades_per_day"] = 0

        # Classify
        cls, conf, action, action_conf = _classify_whale(stats)
        stats["classification"] = cls
        stats["classification_confidence"] = conf
        stats["action"] = action
        stats["action_confidence"] = action_conf

        results[addr] = stats

    return results


def load_goldsky_fills() -> dict:
    """Load goldsky orderbook fills for additional volume/timing data."""
    logger.info("Loading goldsky fills from %s", GOLDSKY_FILLS)
    if not GOLDSKY_FILLS.exists():
        logger.warning("Goldsky fills not found, skipping")
        return {}

    address_data = defaultdict(lambda: {
        "fill_count": 0, "total_filled_usd": 0.0,
        "as_maker": 0, "as_taker": 0,
    })

    count = 0
    with open(GOLDSKY_FILLS, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            for role, prefix in [("maker", "maker"), ("taker", "taker")]:
                addr = row.get(f"{prefix}", "").strip().lower()
                if not addr:
                    continue
                d = address_data[addr]
                d["fill_count"] += 1
                if role == "maker":
                    d["as_maker"] += 1
                else:
                    d["as_taker"] += 1
                try:
                    amount = float(row.get(f"{prefix}AmountFilled", 0) or 0)
                    d["total_filled_usd"] += amount / 1e6  # USDC has 6 decimals
                except (ValueError, TypeError):
                    pass

    logger.info("Loaded %d goldsky fills, %d unique addresses", count, len(address_data))
    return dict(address_data)


def _load_wallet_registry() -> dict[str, str]:
    """Load address→name mapping from known_whale_wallets.json."""
    registry_path = Path("/home/elon-1/workspace/nautilus-trading/config/known_whale_wallets.json")
    if not registry_path.exists():
        logger.warning("known_whale_wallets.json not found at %s", registry_path)
        return {}
    try:
        raw = json.loads(registry_path.read_text())
        # Skip metadata keys (those starting with _)
        result = {}
        for name, addr in raw.items():
            if name.startswith("_"):
                continue
            if not addr or not isinstance(addr, str):
                continue
            # Store both full address and last 8 chars (substring match for short-format)
            full = addr.lower()
            result[full] = name
            if len(full) >= 8:
                result[full[-8:]] = name  # short-format suffix match
        return result
    except Exception as e:
        logger.error("Failed to load wallet registry: %s", e)
        return {}


def _build_whale_name_to_addr_map(db: sqlite3.Connection) -> dict[str, str]:
    """Build whale_name → whale_address mapping from trades.db.

    The trades table stores whale_name and whale_address for each trade.
    This gives us a direct mapping from our tracked whale names to their addresses.
    """
    name_to_addr: dict[str, str] = {}
    try:
        rows = db.execute(
            "SELECT whale_name, whale_address FROM trades "
            "WHERE whale_name IS NOT NULL AND whale_name != '' "
            "  AND whale_address IS NOT NULL AND whale_address != '' "
            "GROUP BY whale_name"
        ).fetchall()
        for name, addr in rows:
            if addr:
                name_to_addr[str(name)] = str(addr).lower()
        logger.info("Loaded %d whale_name→address mappings from trades.db", len(name_to_addr))
    except Exception as e:
        logger.warning("Could not load whale_name→address map from trades.db: %s", e)
    return name_to_addr


def link_nautilus_whales(db: sqlite3.Connection, poly_stats: dict) -> None:
    """Link poly_data addresses to nautilus whale names via known_whale_wallets.json + trades table.

    Enhanced matching strategy:
      1. Exact address match against known_whale_wallets.json (full 0x address)
      2. Substring match against known_whale_wallets.json (last 8 chars of address)
      3. Whale-name cross-reference: if a nautilus whale has traded, find their address
         in poly_stats via the name→address map
      4. Fallback: check if any poly_stats address ends with any known whale suffix

    The core problem: poly_data tracks 7,976 unique addresses across 282K trades,
    while Nautilus tracks 22 specific whales. These sets may have minimal overlap
    because poly_data's scope is broader. We try multiple strategies to maximize matches.
    """
    # Source 1: known_whale_wallets.json (primary — all 26 tracked whales with addresses)
    registry = _load_wallet_registry()
    # registry maps: full_address → whale_name OR short_suffix → whale_name
    full_addrs = {k: v for k, v in registry.items() if k.startswith("0x")}
    short_addrs = {k: v for k, v in registry.items() if not k.startswith("0x")}
    logger.info(
        "Loaded %d whales from known_whale_wallets.json (%d full, %d short)",
        len(registry), len(full_addrs), len(short_addrs),
    )

    # Source 2: whale_name → address from trades.db
    name_to_addr = _build_whale_name_to_addr_map(db)

    # Source 3: poly_stats keys (all addresses that appear in processed trades)
    poly_addrs = set(poly_stats.keys())
    logger.info("poly_stats has %d unique addresses", len(poly_addrs))

    # ── Debug: check for any overlap at all ──────────────────────────────────
    if full_addrs:
        full_overlap = set(full_addrs.keys()) & poly_addrs
        logger.info("Exact address overlap: %d/%d", len(full_overlap), len(full_addrs))
        if full_overlap:
            for a in list(full_overlap)[:3]:
                logger.info("  Found: %s → %s", a, full_addrs[a])

    # ── Match strategies ──────────────────────────────────────────────────────
    linked = 0
    match_types = {"exact": 0, "short_suffix": 0, "name_crossref": 0}

    for addr, stats in poly_stats.items():
        addr_lc = addr.lower() if isinstance(addr, str) else addr
        matched_name: str | None = None
        match_type = ""

        # Strategy 1: exact address match (full 0x address)
        if addr_lc in full_addrs:
            matched_name = full_addrs[addr_lc]
            match_type = "exact"

        # Strategy 2: short suffix match (last 8 chars)
        elif len(str(addr_lc)) >= 8:
            suffix = str(addr_lc)[-8:]
            if suffix in short_addrs:
                matched_name = short_addrs[suffix]
                match_type = "short_suffix"

        # Strategy 3: name cross-reference via trades.db
        # For each tracked whale name, check if their address appears in poly_stats
        elif name_to_addr:
            # Try all tracked whale addresses against this poly address
            for name, tracked_addr in name_to_addr.items():
                if tracked_addr == addr_lc or addr_lc.endswith(tracked_addr[-8:]):
                    matched_name = name
                    match_type = "name_crossref"
                    break

        if matched_name:
            stats["nautilus_whale_name"] = matched_name
            db.execute(
                "INSERT OR REPLACE INTO poly_address_map "
                "(address, nautilus_whale_name, nautilus_source, match_type) "
                "VALUES (?, ?, ?, ?)",
                (addr, matched_name, "wallet_registry" if match_type in ("exact", "short_suffix") else "trades_db", match_type),
            )
            match_types[match_type] = match_types.get(match_type, 0) + 1
            linked += 1

    logger.info(
        "Linked %d addresses to nautilus whale names "
        "(exact=%d, short_suffix=%d, name_crossref=%d)",
        linked,
        match_types.get("exact", 0),
        match_types.get("short_suffix", 0),
        match_types.get("name_crossref", 0),
    )


def update_whale_cohort_cache(db: sqlite3.Connection) -> None:
    """Pre-populate JSON cache with top 50 most active wallets for scanner."""
    rows = db.execute(
        """SELECT address, classification, total_trades, total_volume_usd,
                  avg_trade_size_usd, buy_sell_ratio, nautilus_whale_name
           FROM poly_whale_stats
           ORDER BY total_volume_usd DESC
           LIMIT 50"""
    ).fetchall()

    whales = [
        {
            "address": r[0].lower(),
            "classification": r[1],
            "total_trades": r[2],
            "total_volume_usd": r[3],
            "avg_trade_size_usd": r[4],
            "buy_sell_ratio": r[5],
            "nautilus_whale_name": r[6] or "",
        }
        for r in rows
    ]

    cache_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(whales),
        "whales": whales,
    }

    COHORT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COHORT_CACHE_PATH.write_text(json.dumps(cache_data, indent=2, default=str))
    logger.info("Wrote %d whales to cohort cache at %s", len(whales), COHORT_CACHE_PATH)


def load_market_stats() -> dict:
    """Load market metadata from poly_data markets.csv."""
    logger.info("Loading market stats from %s", MARKETS_CSV)
    if not MARKETS_CSV.exists():
        logger.warning("Markets CSV not found, skipping")
        return {}

    markets = {}
    with open(MARKETS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("condition_id", "").strip()
            if not cid:
                continue
            markets[cid] = {
                "condition_id": cid,
                "question": row.get("question", ""),
                "slug": row.get("market_slug", ""),
                "volume": row.get("volume", ""),
                "closed_time": row.get("closedTime", ""),
            }
    logger.info("Loaded %d markets", len(markets))
    return markets


def build_historical_profiles(poly_stats: dict, goldsky_data: dict = None) -> list[dict]:
    """Build LLM-consumable profiles for top historical whales."""
    # Sort by volume, take top 500
    top_whales = sorted(
        poly_stats.values(),
        key=lambda x: x.get("total_volume_usd", 0),
        reverse=True,
    )[:500]

    profiles = []
    for stats in top_whales:
        profile = {
            "address": stats["address"][:16] + "...",
            "classification": stats["classification"],
            "classification_confidence": stats["classification_confidence"],
            "action": stats["action"],
            "action_confidence": stats["action_confidence"],
            "total_trades": stats["total_trades"],
            "total_volume_usd": stats["total_volume_usd"],
            "buy_sell_ratio": stats["buy_sell_ratio"],
            "unique_markets": stats["unique_markets"],
            "avg_trade_size": stats["avg_trade_size_usd"],
            "active_days": stats["active_days"],
            "trades_per_day": stats["trades_per_day"],
            "hour_entropy": stats["hour_entropy"],
            "round_number_ratio": stats["round_number_ratio"],
            "price_range": f"{stats['price_range_low']:.2f}-{stats['price_range_high']:.2f}",
            "nautilus_whale_name": stats.get("nautilus_whale_name", ""),
        }

        # Merge goldsky data if available
        addr = stats["address"]
        if goldsky_data and addr in goldsky_data:
            gs = goldsky_data[addr]
            profile["goldsky_fills"] = gs["fill_count"]
            profile["goldsky_maker_ratio"] = round(
                gs["as_maker"] / max(gs["fill_count"], 1), 3
            )

        profiles.append(profile)

    return profiles


def write_to_db(db: sqlite3.Connection, poly_stats: dict, market_stats: dict) -> None:
    """Write computed stats to SQLite tables."""
    now = datetime.now(timezone.utc).isoformat()

    for addr, stats in poly_stats.items():
        db.execute(
            """INSERT OR REPLACE INTO poly_whale_stats (
                address, total_trades, total_volume_usd, buy_count, sell_count,
                buy_sell_ratio, unique_markets, avg_trade_size_usd, max_trade_size_usd,
                min_trade_size_usd, median_trade_size_usd, price_range_low, price_range_high,
                avg_price, first_trade_ts, last_trade_ts, active_days, trades_per_day,
                hour_entropy, round_number_ratio, classification, classification_confidence,
                action, action_confidence, nautilus_whale_name, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                addr, stats["total_trades"], stats["total_volume_usd"],
                stats["buy_count"], stats["sell_count"], stats["buy_sell_ratio"],
                stats["unique_markets"], stats["avg_trade_size_usd"],
                stats["max_trade_size_usd"], stats["min_trade_size_usd"],
                stats["median_trade_size_usd"], stats["price_range_low"],
                stats["price_range_high"], stats["avg_price"],
                stats["first_trade_ts"], stats["last_trade_ts"],
                stats["active_days"], stats["trades_per_day"],
                stats["hour_entropy"], stats["round_number_ratio"],
                stats["classification"], stats["classification_confidence"],
                stats["action"], stats["action_confidence"],
                stats.get("nautilus_whale_name", ""), now,
            ),
        )

    for cid, m in market_stats.items():
        db.execute(
            """INSERT OR REPLACE INTO poly_market_stats (
                condition_id, question, slug, updated_at
            ) VALUES (?, ?, ?, ?)""",
            (cid, m.get("question", ""), m.get("slug", ""), now),
        )

    db.commit()
    logger.info("Wrote %d whale stats and %d market stats to DB", len(poly_stats), len(market_stats))


def enrich_edge_scorer_query(db: sqlite3.Connection, whale_address: str, category: str = "") -> Optional[dict]:
    """Query poly_data stats for a given whale address to enrich edge scoring.

    Returns historical behavioral data: volume, classification, action, etc.
    """
    addr = whale_address.lower().strip()
    row = db.execute(
        "SELECT total_trades, total_volume_usd, buy_sell_ratio, unique_markets, "
        "avg_trade_size_usd, active_days, trades_per_day, hour_entropy, round_number_ratio, "
        "classification, classification_confidence, action, action_confidence "
        "FROM poly_whale_stats WHERE address = ?",
        (addr,),
    ).fetchone()

    if not row:
        return None

    return {
        "poly_total_trades": row[0],
        "poly_total_volume": row[1],
        "poly_buy_sell_ratio": row[2],
        "poly_unique_markets": row[3],
        "poly_avg_size": row[4],
        "poly_active_days": row[5],
        "poly_trades_per_day": row[6],
        "poly_hour_entropy": row[7],
        "poly_round_number_ratio": row[8],
        "poly_classification": row[9],
        "poly_classification_confidence": row[10],
        "poly_action": row[11],
        "poly_action_confidence": row[12],
    }


def get_historical_classification(db: sqlite3.Connection, whale_address: str) -> str:
    """Get the historical classification for a whale from poly_data.

    Returns one of: skilled_human, trading_bot, degenerate_human, sacrificial_account,
    market_maker, mixed_entity, or 'unknown' if no data.
    """
    row = db.execute(
        "SELECT classification FROM poly_whale_stats WHERE address = ?",
        (whale_address.lower().strip(),),
    ).fetchone()
    return row[0] if row else "unknown"


def get_category_top_whales(db: sqlite3.Connection, classification: str, limit: int = 20) -> list[dict]:
    """Get top historical whales by classification for pattern matching."""
    rows = db.execute(
        "SELECT address, total_volume_usd, total_trades, unique_markets, "
        "buy_sell_ratio, active_days, classification_confidence, action, action_confidence "
        "FROM poly_whale_stats WHERE classification = ? "
        "ORDER BY total_volume_usd DESC LIMIT ?",
        (classification, limit),
    ).fetchall()
    return [
        {
            "address": r[0], "volume": r[1], "trades": r[2], "markets": r[3],
            "buy_sell_ratio": r[4], "active_days": r[5], "confidence": r[6],
            "action": r[7], "action_confidence": r[8],
        }
        for r in rows
    ]


def main():
    """Run the full poly_data pipeline."""
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logger.info("=== Poly Data Loader Starting ===")

    # Load processed trades
    poly_stats = load_processed_trades()
    if not poly_stats:
        logger.error("No poly_data trades loaded, aborting")
        sys.exit(1)

    # Load goldsky fills
    goldsky_data = load_goldsky_fills()

    # Load market stats
    market_stats = load_market_stats()

    # Connect to nautilus DB
    db = sqlite3.connect(str(DB_PATH))
    _ensure_tables(db)

    # Link to nautilus whales
    link_nautilus_whales(db, poly_stats)

    # Pre-populate whale cohort cache for scanner
    update_whale_cohort_cache(db)

    # Write to DB
    write_to_db(db, poly_stats, market_stats)

    # Build and save historical profiles
    profiles = build_historical_profiles(poly_stats, goldsky_data)
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_addresses": len(poly_stats),
            "total_profiles": len(profiles),
            "classification_counts": defaultdict(
                int,
                {s["classification"]: sum(1 for v in poly_stats.values() if v["classification"] == s["classification"])
                 for s in poly_stats.values()},
            ),
            "profiles": profiles,
        }, f, indent=2, default=str)
    logger.info("Saved %d profiles to %s", len(profiles), OUTPUT_PATH)

    # Summary stats
    class_counts = defaultdict(int)
    for s in poly_stats.values():
        class_counts[s["classification"]] += 1

    logger.info("=== Classification Summary ===")
    for cls, cnt in sorted(class_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {cls}: {cnt}")
    logger.info(f"  Total: {len(poly_stats)}")
    logger.info(f"  Linked to nautilus: %d", sum(1 for s in poly_stats.values() if s.get("nautilus_whale_name")))

    db.close()
    logger.info("=== Poly Data Loader Complete ===")


if __name__ == "__main__":
    main()
