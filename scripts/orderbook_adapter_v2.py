#!/usr/bin/env python3
"""Orderbook Adapter v2 — CLOB + Data API + Substreams hybrid.

Data sources:
  1. Gamma API — market metadata (active markets, volume, liquidity, token IDs)
  2. CLOB API — orderbook snapshots (bids/asks/spread/depth)
  3. Data API — public trade fills (whale wallet tracking, prices, sizes)
  4. Substreams — on-chain fills (background, PostgreSQL sink)

Writes to PostgreSQL for the nautilus trading system to query.

Usage:
    python3 scripts/orderbook_adapter_v2.py [--once] [--poll-interval 60]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orderbook_adapter_v2")

# ── Configuration ─────────────────────────────────────────────────────────

PG_DSN = "postgresql://polymarket:polymarket@localhost:5432/polymarket_orderbook"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
DEFAULT_POLL_INTERVAL = 60
STATE_FILE = Path("/home/elon-1/workspace/nautilus-trading/data/orderbook_adapter_v2_state.json")
ENV_FILE = Path("/home/elon-1/workspace/nautilus-trading/.env")
SUBSTREAMS_JWT_ENV = "SUBSTREAMS_API_TOKEN"

# ── Environment ────────────────────────────────────────────────────────────

def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().split("#")[0].strip()
                if key not in os.environ:
                    os.environ[key] = val

load_env()

# ── PostgreSQL ──────────────────────────────────────────────────────────────

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

def get_pg_connection():
    if not HAS_PSYCOPG2:
        return None
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"PG connection failed: {e}")
        return None

# ── Schema ────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- CLOB orderbook snapshots (polled every cycle)
CREATE TABLE IF NOT EXISTS clob_orderbook_snapshots (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR NOT NULL,
    token_id VARCHAR NOT NULL,
    outcome VARCHAR NOT NULL,
    best_bid NUMERIC(38, 18),
    best_ask NUMERIC(38, 18),
    midpoint NUMERIC(38, 18),
    spread NUMERIC(38, 18),
    bid_depth NUMERIC(78, 0) DEFAULT 0,
    ask_depth NUMERIC(78, 0) DEFAULT 0,
    bid_liquidity NUMERIC(78, 0) DEFAULT 0,
    ask_liquidity NUMERIC(78, 0) DEFAULT 0,
    num_bids INTEGER DEFAULT 0,
    num_asks INTEGER DEFAULT 0,
    last_trade_price NUMERIC(38, 18),
    snapshot_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_clob_snap_condition ON clob_orderbook_snapshots(condition_id);
CREATE INDEX IF NOT EXISTS idx_clob_snap_ts ON clob_orderbook_snapshots(snapshot_ts DESC);
CREATE INDEX IF NOT EXISTS idx_clob_snap_condition_ts ON clob_orderbook_snapshots(condition_id, snapshot_ts DESC);

-- Market metadata from Gamma API
CREATE TABLE IF NOT EXISTS gamma_markets (
    condition_id VARCHAR PRIMARY KEY,
    question TEXT,
    slug VARCHAR,
    active BOOLEAN DEFAULT true,
    closed BOOLEAN DEFAULT false,
    accepting_orders BOOLEAN DEFAULT false,
    neg_risk BOOLEAN DEFAULT false,
    volume24hr NUMERIC(38, 18) DEFAULT 0,
    volume_total NUMERIC(38, 18) DEFAULT 0,
    liquidity NUMERIC(38, 18) DEFAULT 0,
    end_date_iso VARCHAR,
    tokens_json JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_gamma_active ON gamma_markets(active) WHERE active = true;
CREATE INDEX IF NOT EXISTS idx_gamma_volume ON gamma_markets(volume24hr DESC);

-- Public trade data from data-api.polymarket.com
CREATE TABLE IF NOT EXISTS data_api_trades (
    id VARCHAR PRIMARY KEY,
    condition_id VARCHAR NOT NULL,
    token_id VARCHAR,
    side VARCHAR NOT NULL,
    price NUMERIC(38, 18) NOT NULL,
    size NUMERIC(38, 18) NOT NULL,
    proxy_wallet VARCHAR(42),
    transaction_hash VARCHAR(66),
    outcome VARCHAR,
    outcome_index INTEGER,
    title TEXT,
    slug VARCHAR,
    trade_ts TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_data_trades_condition ON data_api_trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_data_trades_wallet ON data_api_trades(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_data_trades_ts ON data_api_trades(trade_ts DESC);
CREATE INDEX IF NOT EXISTS idx_data_trades_wallet_ts ON data_api_trades(proxy_wallet, trade_ts DESC);

-- Adapter run log
CREATE TABLE IF NOT EXISTS adapter_runs (
    id SERIAL PRIMARY KEY,
    adapter_type VARCHAR NOT NULL,
    markets_processed INTEGER DEFAULT 0,
    snapshots_taken INTEGER DEFAULT 0,
    trades_processed INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    duration_sec NUMERIC(10, 2),
    run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    cur.close()
    logger.info("Schema ensured")

# ── HTTP Helpers ────────────────────────────────────────────────────────────

def http_get(url: str, timeout: int = 20) -> Optional[dict | list]:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Hermes-OB/2.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        logger.debug(f"HTTP {e.code} for {url[:80]}")
        return None
    except Exception as e:
        logger.debug(f"HTTP error for {url[:80]}: {e}")
        return None

# ── Market Metadata (Gamma API) ────────────────────────────────────────────

def fetch_active_markets(limit: int = 500) -> list[dict]:
    """Fetch active, non-closed markets from Gamma API."""
    all_markets = []
    offset = 0
    batch = 100
    while offset < limit:
        url = f"{GAMMA_API}/markets?closed=false&active=true&limit={batch}&offset={offset}&order=volume24hr&ascending=false"
        data = http_get(url)
        if not data or not isinstance(data, list):
            break
        all_markets.extend(data)
        if len(data) < batch:
            break
        offset += batch
        time.sleep(0.2)
    return all_markets

def upsert_market_metadata(conn, market: dict):
    cur = conn.cursor()
    # Handle both camelCase (Gamma API) and snake_case (CLOB API)
    cid = market.get("conditionId") or market.get("condition_id") or market.get("condition_id", "")
    if not cid:
        return
    cid = cid.lower()
    
    # Parse token IDs from clobTokenIds (Gamma) or tokens (CLOB)
    tokens = market.get("tokens", [])
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = []
    
    # Gamma API has clobTokenIds as a JSON string
    clob_token_ids = market.get("clobTokenIds", "")
    if isinstance(clob_token_ids, str) and clob_token_ids:
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except Exception:
            clob_token_ids = []
    
    # Build token list with IDs
    enriched_tokens = []
    if isinstance(tokens, list) and tokens:
        for i, t in enumerate(tokens):
            if isinstance(t, dict):
                tid = t.get("token_id", "")
                if not tid and clob_token_ids and i < len(clob_token_ids):
                    tid = clob_token_ids[i]
                enriched_tokens.append({
                    "token_id": tid,
                    "outcome": t.get("outcome", ""),
                    "price": t.get("price", 0),
                    "winner": t.get("winner", False),
                })
            elif isinstance(t, str):
                enriched_tokens.append({"token_id": t})
    
    if not enriched_tokens and clob_token_ids:
        outcomes = market.get("outcomes", "")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = ["Yes", "No"]
        for i, tid in enumerate(clob_token_ids):
            outcome = outcomes[i] if i < len(outcomes) else ""
            enriched_tokens.append({"token_id": tid, "outcome": outcome})
    
    cur.execute("""
        INSERT INTO gamma_markets (condition_id, question, slug, active, closed,
            accepting_orders, neg_risk, volume24hr, volume_total, liquidity,
            end_date_iso, tokens_json, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (condition_id) DO UPDATE SET
            question = EXCLUDED.question,
            slug = EXCLUDED.slug,
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            accepting_orders = EXCLUDED.accepting_orders,
            neg_risk = EXCLUDED.neg_risk,
            volume24hr = EXCLUDED.volume24hr,
            volume_total = EXCLUDED.volume_total,
            liquidity = EXCLUDED.liquidity,
            end_date_iso = EXCLUDED.end_date_iso,
            tokens_json = EXCLUDED.tokens_json,
            updated_at = NOW()
    """, (
        cid,
        market.get("question", market.get("title", "")),
        market.get("slug", ""),
        market.get("active", True),
        market.get("closed", False),
        market.get("acceptingOrders", market.get("accepting_orders", False)),
        market.get("negRisk", market.get("neg_risk", False)),
        market.get("volume24hr", 0) or 0,
        market.get("volume", market.get("volumeTotal", market.get("volume_total", 0))) or 0,
        market.get("liquidityNum", market.get("liquidity", 0)) or 0,
        market.get("endDateIso", market.get("end_date_iso", market.get("endDate", ""))),
        json.dumps(enriched_tokens),
    ))
    cur.close()

# ── Orderbook Snapshots (CLOB API) ────────────────────────────────────────

def fetch_orderbook_snapshot(token_id: str) -> Optional[dict]:
    url = f"{CLOB_API}/book?token_id={token_id}"
    return http_get(url, timeout=10)

def insert_orderbook_snapshot(conn, condition_id: str, token_id: str, outcome: str, ob_data: dict):
    cur = conn.cursor()
    bids = ob_data.get("bids", [])
    asks = ob_data.get("asks", [])
    
    # CLOB API returns bids ascending, asks descending — sort for best prices
    sorted_bids = sorted(bids, key=lambda x: float(x.get("price", 0)), reverse=True)
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 0)), reverse=False)
    best_bid = float(sorted_bids[0].get("price", 0)) if sorted_bids else 0
    best_ask = float(sorted_asks[0].get("price", 0)) if sorted_asks else 0
    midpoint = (best_bid + best_ask) / 2 if best_bid and best_ask else (best_bid or best_ask)
    spread = best_ask - best_bid if best_bid and best_ask else 0
    
    bid_liquidity = sum(float(b.get("size", 0)) for b in bids[:10])
    ask_liquidity = sum(float(a.get("size", 0)) for a in asks[:10])
    bid_depth = sum(float(b.get("size", 0)) for b in bids)
    ask_depth = sum(float(a.get("size", 0)) for a in asks)
    
    last_price = float(ob_data.get("last_trade_price", 0)) if ob_data.get("last_trade_price") else 0
    
    cur.execute("""
        INSERT INTO clob_orderbook_snapshots 
            (condition_id, token_id, outcome, best_bid, best_ask, midpoint, spread,
             bid_depth, ask_depth, bid_liquidity, ask_liquidity, num_bids, num_asks,
             last_trade_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        condition_id, token_id, outcome,
        best_bid, best_ask, midpoint, spread,
        bid_depth, ask_depth, bid_liquidity, ask_liquidity,
        len(bids), len(asks), last_price
    ))
    cur.close()

# ── Trade Data (Data API) ────────────────────────────────────────────────

def fetch_recent_trades(limit: int = 500) -> list[dict]:
    """Fetch recent trades from data-api.polymarket.com."""
    url = f"{DATA_API}/trades?limit={limit}"
    data = http_get(url)
    if isinstance(data, list):
        return data
    return []

def fetch_trades_for_market(condition_id: str, limit: int = 100) -> list[dict]:
    """Fetch recent trades for a specific market."""
    url = f"{DATA_API}/trades?conditionId={condition_id}&limit={limit}"
    data = http_get(url)
    if isinstance(data, list):
        return data
    return []

def fetch_whale_trades(min_size: float = 5000, limit: int = 200) -> list[dict]:
    """Fetch large trades (whale activity)."""
    url = f"{DATA_API}/trades?limit={limit}"
    data = http_get(url)
    if isinstance(data, list):
        # Filter for large trades
        return [t for t in data if float(t.get("size", 0) or 0) >= min_size]
    return []

def insert_trades(conn, trades: list[dict]) -> int:
    """Insert trades into data_api_trades, returns new count."""
    cur = conn.cursor()
    inserted = 0
    for t in trades:
        tx_hash = t.get("transactionHash", "")
        if not tx_hash:
            continue
        oidx = t.get("outcomeIndex", 0)
        unique_id = f"{tx_hash}-{oidx}"
        try:
            cur.execute("""
                INSERT INTO data_api_trades 
                    (id, condition_id, token_id, side, price, size,
                     proxy_wallet, transaction_hash, outcome, outcome_index,
                     title, slug, trade_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                unique_id,
                t.get("conditionId", t.get("condition_id", "")).lower(),
                t.get("asset", t.get("token_id", "")),
                t.get("side", ""),
                t.get("price", 0),
                t.get("size", 0),
                t.get("proxyWallet", t.get("proxy_wallet", "")).lower(),
                tx_hash,
                t.get("outcome", ""),
                oidx,
                t.get("title", ""),
                t.get("slug", ""),
                datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc) if t.get("timestamp") else None,
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.debug(f"Trade insert skip: {e}")
    cur.close()
    return inserted

# ── Main Collection Cycle ──────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_sync": 0, "total_snapshots": 0, "total_trades": 0, "cycles": 0}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def run_cycle(conn, max_markets: int = 50) -> dict:
    """Run one full collection cycle."""
    stats = {"markets": 0, "snapshots": 0, "trades": 0, "errors": 0}
    start_time = time.time()
    
    # 1. Market metadata from Gamma API
    logger.info("Fetching market metadata from Gamma API...")
    markets = fetch_active_markets(limit=500)
    stats["markets"] = len(markets)
    logger.info(f"Found {len(markets)} active markets")
    
    for m in markets:
        try:
            upsert_market_metadata(conn, m)
        except Exception as e:
            stats["errors"] += 1
    
    # 2. Orderbook snapshots for top markets
    top_markets = sorted(markets, key=lambda m: float(m.get("volume24hr", 0) or 0), reverse=True)[:max_markets]
    logger.info(f"Taking orderbook snapshots for top {len(top_markets)} markets")
    
    for i, m in enumerate(top_markets):
        cid = (m.get("conditionId") or m.get("condition_id") or m.get("condition_id", "")).lower()
        if not cid:
            continue
        
        # Get token IDs from clobTokenIds or tokens
        clob_token_ids = m.get("clobTokenIds", "")
        if isinstance(clob_token_ids, str) and clob_token_ids:
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except Exception:
                clob_token_ids = []
        
        outcomes = m.get("outcomes", "")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = ["Yes", "No"]
        
        if not clob_token_ids:
            # Try CLOB API to get tokens
            mkt_data = http_get(f"{CLOB_API}/markets/{cid}")
            if mkt_data and isinstance(mkt_data, dict):
                tokens = mkt_data.get("tokens", [])
                clob_token_ids = [t.get("token_id", "") for t in tokens if isinstance(t, dict)]
                if not outcomes or outcomes == ["Yes", "No"]:
                    outcomes = [t.get("outcome", "") for t in tokens if isinstance(t, dict)]
            time.sleep(0.05)
        
        for j, tid in enumerate(clob_token_ids[:2]):
            if not tid:
                continue
            outcome = outcomes[j] if j < len(outcomes) else ""
            
            ob_data = fetch_orderbook_snapshot(tid)
            if ob_data and (ob_data.get("bids") or ob_data.get("asks")):
                try:
                    insert_orderbook_snapshot(conn, cid, tid, outcome, ob_data)
                    stats["snapshots"] += 1
                except Exception as e:
                    stats["errors"] += 1
        
        time.sleep(0.1)
        
        if (i + 1) % 25 == 0:
            logger.info(f"  Snapshots: {i+1}/{len(top_markets)}, {stats['snapshots']} ok, {stats['errors']} err")
    
    # 3. Recent trades from Data API
    logger.info("Fetching recent trades from Data API...")
    recent_trades = fetch_recent_trades(limit=500)
    if recent_trades:
        inserted = insert_trades(conn, recent_trades)
        stats["trades"] += inserted
        logger.info(f"Inserted {inserted}/{len(recent_trades)} recent trades")
    
    # 4. Whale-specific trades for top markets
    whale_markets = top_markets[:10]
    for m in whale_markets:
        cid = (m.get("conditionId") or m.get("condition_id") or m.get("condition_id", ""))
        if not cid:
            continue
        mkt_trades = fetch_trades_for_market(cid, limit=100)
        if mkt_trades:
            inserted = insert_trades(conn, mkt_trades)
            stats["trades"] += inserted
        time.sleep(0.3)
    
    duration = time.time() - start_time
    logger.info(f"Cycle complete in {duration:.1f}s: {stats['markets']} markets, "
                f"{stats['snapshots']} snapshots, {stats['trades']} trades, {stats['errors']} errors")
    
    # Log run
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO adapter_runs (adapter_type, markets_processed, snapshots_taken, trades_processed, errors, duration_sec)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, ("clob_gamma_data", stats["markets"], stats["snapshots"], stats["trades"], stats["errors"], duration))
    cur.close()
    
    # Update state
    state = load_state()
    state["last_sync"] = int(time.time())
    state["total_snapshots"] = state.get("total_snapshots", 0) + stats["snapshots"]
    state["total_trades"] = state.get("total_trades", 0) + stats["trades"]
    state["cycles"] = state.get("cycles", 0) + 1
    save_state(state)
    
    return stats

# ── Substreams Manager ──────────────────────────────────────────────────────

def start_substreams_sink(jwt_token: str) -> bool:
    cmd = (
        f"SUBSTREAMS_API_TOKEN='{jwt_token}' "
        f"nohup substreams-sink-sql run "
        f"--endpoint polygon.substreams.pinax.network:443 "
        f"--start-block -100000 "
        f"--on-module-hash-mismatch warn "
        f"'postgres://polymarket:polymarket@localhost:5432/polymarket_orderbook?sslmode=disable' "
        f"polymarket-orderbook-substreams@v0.4.0 "
        f">>/home/elon-1/workspace/nautilus-trading/logs/substreams_sink.log 2>&1 &"
    )
    try:
        subprocess.Popen(cmd, shell=True, executable="/bin/bash")
        logger.info("Started substreams-sink-sql")
        return True
    except Exception as e:
        logger.error(f"Failed to start substreams: {e}")
        return False

def check_substreams_running() -> bool:
    try:
        result = subprocess.run(["pgrep", "-f", "substreams-sink-sql"], capture_output=True, text=True)
        return result.returncode == 0
    except Exception:
        return False

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Orderbook Adapter v2 (CLOB + Data API + Substreams)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--no-substreams", action="store_true", help="Don't start substreams sink")
    parser.add_argument("--max-markets", type=int, default=50, help="Max markets per snapshot cycle")
    args = parser.parse_args()

    logger.info("=== Orderbook Adapter v2 (CLOB + Data API + Substreams) ===")
    
    if not HAS_PSYCOPG2:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)
    
    conn = get_pg_connection()
    if conn is None:
        logger.error("Cannot connect to PostgreSQL")
        sys.exit(1)
    logger.info("Connected to PostgreSQL")
    
    ensure_schema(conn)
    
    # Start substreams sink if not running
    if not args.no_substreams and not check_substreams_running():
        jwt = os.environ.get(SUBSTREAMS_JWT_ENV, "")
        if jwt:
            start_substreams_sink(jwt)
        else:
            logger.warning("No SUBSTREAMS_API_TOKEN set; skipping substreams sink")
    
    if args.once:
        run_cycle(conn, max_markets=args.max_markets)
    else:
        logger.info(f"Starting daemon (poll: {args.poll_interval}s, max_markets: {args.max_markets})")
        while True:
            try:
                run_cycle(conn, max_markets=args.max_markets)
            except Exception as e:
                logger.error(f"Cycle error: {e}")
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
            except Exception:
                logger.warning("Reconnecting to PostgreSQL...")
                try:
                    conn = get_pg_connection()
                except Exception:
                    logger.error("PG reconnect failed")
            time.sleep(args.poll_interval)

if __name__ == "__main__":
    main()
