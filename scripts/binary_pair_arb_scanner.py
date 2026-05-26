#!/usr/bin/env python3
"""Market Intelligence & Arbitrage Scanner - Standalone, zero cross-contamination.

Scans Polymarket for:
1. Multi-outcome event arbitrage (neg_risk events where total prob > 1.0 + fees)
2. Deep value opportunities (markets near resolution but mispriced)
3. Market health tracking (spreads, liquidity, volume)

Uses gamma-api.polymarket.com (reliable) instead of CLOB midpoint API (broken).
Writes to its own database (data/arb_trades.db), never touches trades.db.
"""

import sqlite3
import time
import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field

import requests

# Configuration
PROJECT_DIR = Path(__file__).parent.parent
ARB_DB = PROJECT_DIR / "data" / "arb_trades.db"
LOG_DIR = PROJECT_DIR / "logs"

GAMMA_API = "https://gamma-api.polymarket.com"
POLY_FEE = 0.02  # 2% total fees (1% per side)
ARB_THRESHOLD = 0.02  # 2% net after fees
DEEP_VALUE_THRESHOLD = 0.15  # Market priced <0.15 but likely to resolve YES
SCAN_INTERVAL_SECS = 300  # 5 minutes
REQUEST_TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ARB_SCANNER | %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_DIR / "arb_scanner.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


@dataclass
class EventArb:
    event_title: str
    event_slug: str
    total_probability: float
    net_arb_pct: float
    num_markets: int
    markets: list = field(default_factory=list)
    volume_24h: float = 0.0
    liquidity: float = 0.0


@dataclass
class DeepValue:
    question: str
    condition_id: str
    yes_price: float
    volume_24h: float
    liquidity: float
    spread: float
    days_to_expiry: int
    slug: str


def init_db():
    ARB_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(ARB_DB))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS event_arbs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER,
            event_title TEXT,
            event_slug TEXT,
            total_probability REAL,
            net_arb_pct REAL,
            num_markets INTEGER,
            volume_24h REAL DEFAULT 0,
            liquidity REAL DEFAULT 0,
            timestamp TEXT,
            status TEXT DEFAULT 'detected'
        );
        CREATE TABLE IF NOT EXISTS deep_value_opps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER,
            question TEXT,
            condition_id TEXT,
            yes_price REAL,
            volume_24h REAL DEFAULT 0,
            liquidity REAL DEFAULT 0,
            spread REAL DEFAULT 0,
            days_to_expiry INTEGER,
            slug TEXT,
            timestamp TEXT,
            status TEXT DEFAULT 'detected'
        );
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_time TEXT,
            events_scanned INTEGER,
            neg_risk_events INTEGER,
            arb_count INTEGER,
            deep_value_count INTEGER,
            duration_secs REAL
        );
    """)
    conn.commit()
    conn.close()
    log.info("ARB database initialized at %s", ARB_DB)


def fetch_json(url, params=None, timeout=REQUEST_TIMEOUT, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = min(2 ** attempt * 2, 30)
                log.warning("Rate limited, waiting %ds", wait)
                time.sleep(wait)
            else:
                log.warning("HTTP %d from %s", r.status_code, url[:60])
                time.sleep(2)
        except Exception as e:
            wait = min(2 ** attempt * 2, 30)
            log.warning("Fetch error: %s, retry %d/%d", type(e).__name__, attempt+1, retries)
            time.sleep(wait)
    return None


def scan_event_arbs():
    """Scan for multi-outcome event arbitrage opportunities."""
    log.info("Scanning for event arbitrage...")
    arbs = []
    page = 0
    total_events = 0
    neg_risk_count = 0

    while page < 10:
        params = {"limit": 50, "active": "true", "closed": "false"}
        if page > 0:
            params["offset"] = page * 50
        data = fetch_json(f"{GAMMA_API}/events", params=params)
        if not data:
            break
        events = data if isinstance(data, list) else []
        if not events:
            break

        total_events += len(events)
        for e in events:
            markets = e.get("markets", [])
            if len(markets) < 2:
                continue

            # Calculate total probability across all outcomes
            total_prob = 0.0
            market_details = []
            vol_24h = 0.0
            liq = 0.0

            for m in markets:
                prices_str = m.get("outcomePrices", "[]")
                try:
                    prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                    if prices and len(prices) >= 1:
                        yes_p = float(prices[0])
                        total_prob += yes_p
                        market_details.append({
                            "question": m.get("question", "???")[:80],
                            "yes_price": yes_p,
                            "volume_24h": float(m.get("volume24hr", 0) or 0),
                            "liquidity": float(m.get("liquidity", 0) or 0),
                        })
                        vol_24h += float(m.get("volume24hr", 0) or 0)
                        liq += float(m.get("liquidity", 0) or 0)
                except (ValueError, TypeError, json.JSONDecodeError):
                    continue

            if not market_details:
                continue

            # Check if this is a neg_risk event (mutually exclusive)
            is_neg_risk = any(m.get("negRisk", False) for m in markets)
            if is_neg_risk:
                neg_risk_count += 1

            net_arb = (total_prob - 1.0) - POLY_FEE if total_prob > 1.0 else 0
            net_arb_pct = net_arb * 100

            # Filter: require minimum per-market liquidity to avoid illusory long-tail arbs
            min_per_market_liq = 500
            markets_with_liq = sum(1 for m in market_details if float(m.get('liquidity', 0)) >= min_per_market_liq)
            if markets_with_liq < len(markets) * 0.5:
                log.info("ARB_FILTER: skipping | only %d/%d markets with >$500 liq", markets_with_liq, len(markets))
                continue

            # Only flag if truly mutually exclusive AND profitable after fees
            if is_neg_risk and net_arb_pct > ARB_THRESHOLD * 100:
                arb = EventArb(
                    event_title=e.get("title", "???")[:200],
                    event_slug=e.get("slug", ""),
                    total_probability=total_prob,
                    net_arb_pct=net_arb_pct,
                    num_markets=len(markets),
                    markets=market_details,
                    volume_24h=vol_24h,
                    liquidity=liq,
                )
                arbs.append(arb)
                log.info("ARB: %s | total=%.3f | net=%.2f%% | %d markets",
                         arb.event_title[:50], total_prob, net_arb_pct, len(markets))

        page += 1
        time.sleep(0.5)

    log.info("Event scan: %d events, %d neg_risk, %d arb opportunities",
             total_events, neg_risk_count, len(arbs))
    return arbs, total_events, neg_risk_count


def scan_deep_value():
    """Find markets priced low that might be mispriced (deep value opportunities)."""
    log.info("Scanning for deep value opportunities...")
    opportunities = []
    page = 0

    while page < 5:
        params = {"limit": 50, "active": "true", "closed": "false", "order": "volume24hr", "ascending": "false"}
        if page > 0:
            params["offset"] = page * 50
        data = fetch_json(f"{GAMMA_API}/markets", params=params)
        if not data:
            break
        markets = data if isinstance(data, list) else []
        if not markets:
            break

        for m in markets:
            prices_str = m.get("outcomePrices", "[]")
            try:
                prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                if not prices or len(prices) < 1:
                    continue
                yes_price = float(prices[0])
            except:
                continue

            # Deep value: YES price < threshold and market has decent volume
            if yes_price > DEEP_VALUE_THRESHOLD:
                continue

            vol_24h = float(m.get("volume24hr", 0) or 0)
            liq = float(m.get("liquidity", 0) or 0)
            spread = float(m.get("spread", 0) or 0)

            # Need some volume and liquidity to be tradeable
            if liq < 100 or vol_24h < 10:
                continue

            # Calculate days to expiry
            end_date = m.get("endDate", "")
            days_to_expiry = 999
            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    days_to_expiry = max(0, (end_dt - datetime.now(timezone.utc)).days)
                except:
                    pass

            dv = DeepValue(
                question=m.get("question", "???")[:200],
                condition_id=m.get("conditionId", ""),
                yes_price=yes_price,
                volume_24h=vol_24h,
                liquidity=liq,
                spread=spread,
                days_to_expiry=days_to_expiry,
                slug=m.get("slug", ""),
            )
            opportunities.append(dv)

        page += 1
        time.sleep(0.5)

    # Sort by liquidity (most tradeable first)
    opportunities.sort(key=lambda x: x.liquidity, reverse=True)
    log.info("Found %d deep value opportunities", len(opportunities))
    return opportunities


def save_results(arbs, deep_values, events_scanned, neg_risk_count, duration):
    conn = sqlite3.connect(str(ARB_DB))
    now = datetime.now(timezone.utc).isoformat()

    # Insert scan log
    cur = conn.execute("""
        INSERT INTO scan_log (scan_time, events_scanned, neg_risk_events, arb_count, deep_value_count, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (now, events_scanned, neg_risk_count, len(arbs), len(deep_values), duration))
    scan_id = cur.lastrowid

    # Insert event arbs
    for arb in arbs:
        conn.execute("""
            INSERT INTO event_arbs (scan_id, event_title, event_slug, total_probability, net_arb_pct, num_markets, volume_24h, liquidity, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'detected')
        """, (scan_id, arb.event_title, arb.event_slug, arb.total_probability,
              arb.net_arb_pct, arb.num_markets, arb.volume_24h, arb.liquidity, now))

    # Insert deep value opportunities
    for dv in deep_values[:50]:  # Cap at top 50
        conn.execute("""
            INSERT INTO deep_value_opps (scan_id, question, condition_id, yes_price, volume_24h, liquidity, spread, days_to_expiry, slug, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'detected')
        """, (scan_id, dv.question, dv.condition_id, dv.yes_price,
              dv.volume_24h, dv.liquidity, dv.spread, dv.days_to_expiry, dv.slug, now))

    conn.commit()
    conn.close()
    log.info("Saved scan results: %d arbs, %d deep value, scan_id=%d", len(arbs), len(deep_values), scan_id)


def run_scan():
    start = time.time()
    log.info("=== Starting market intelligence scan ===")

    arbs, events_scanned, neg_risk_count = scan_event_arbs()
    deep_values = scan_deep_value()

    duration = time.time() - start

    # Print summary
    log.info("=== Scan complete in %.1fs ===", duration)
    log.info("Events: %d (%d neg_risk) | ARBs: %d | Deep Value: %d",
             events_scanned, neg_risk_count, len(arbs), len(deep_values))

    if arbs:
        log.info("TOP ARB OPPORTUNITIES:")
        for arb in sorted(arbs, key=lambda x: -x.net_arb_pct)[:5]:
            log.info("  %s | total=%.3f | net=%.2f%% | liq=$%.0f | %d markets",
                     arb.event_title[:50], arb.total_probability, arb.net_arb_pct,
                     arb.liquidity, arb.num_markets)

    if deep_values:
        log.info("TOP DEEP VALUE (low-priced, high-liquidity):")
        for dv in deep_values[:10]:
            log.info("  %s | YES=$%.3f | liq=$%.0f | spread=%.3f | %d days",
                     dv.question[:50], dv.yes_price, dv.liquidity, dv.spread, dv.days_to_expiry)

    save_results(arbs, deep_values, events_scanned, neg_risk_count, duration)
    return arbs, deep_values


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Market Intelligence & Arb Scanner")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=SCAN_INTERVAL_SECS, help="Scan interval")
    args = parser.parse_args()

    init_db()

    if args.once:
        run_scan()
    elif args.daemon:
        log.info("Starting daemon mode, interval=%ds", args.interval)
        while True:
            try:
                run_scan()
            except Exception as e:
                log.error("Scan failed: %s", e)
            log.info("Sleeping %ds until next scan", args.interval)
            time.sleep(args.interval)
    else:
        run_scan()


if __name__ == "__main__":
    main()
