"""Orderbook Query Module — Bridge between PostgreSQL orderbook data and the signal pipeline.

Provides three key functions for the trading system:
  1. Liquidity check — is there enough depth to enter/exit a trade?
  2. Slippage estimation — how much will our trade move the price?
  3. Whale wallet activity — what are known whale wallets doing recently?

All queries go to the polymarket_orderbook PostgreSQL database.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger("OrderbookQuery")

PG_DSN = "postgresql://polymarket:polymarket@localhost:5432/polymarket_orderbook"

# ── Lazy connection ────────────────────────────────────────────────────────

_pg_conn = None

def _get_conn():
    global _pg_conn
    if _pg_conn is not None:
        try:
            cur = _pg_conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return _pg_conn
        except Exception:
            try:
                _pg_conn.close()
            except Exception:
                pass
            _pg_conn = None
    try:
        import psycopg2
        _pg_conn = psycopg2.connect(PG_DSN)
        _pg_conn.autocommit = True
        return _pg_conn
    except Exception as e:
        logger.warning(f"Cannot connect to orderbook DB: {e}")
        return None


# ── Data Classes ───────────────────────────────────────────────────────────

@dataclass
class LiquidityInfo:
    """Liquidity assessment for a market outcome."""
    condition_id: str = ""
    token_id: str = ""
    outcome: str = ""
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread: float = 0.0
    midpoint: float = 0.0
    bid_liquidity: float = 0.0   # Top-10 bid depth in units
    ask_liquidity: float = 0.0   # Top-10 ask depth in units
    bid_depth: float = 0.0       # Total bid depth
    ask_depth: float = 0.0       # Total ask depth
    spread_pct: float = 0.0      # spread / midpoint * 100
    liquidity_score: float = 0.0  # 0-1 composite score
    is_thin: bool = False         # True if liquidity is dangerously low
    snapshot_age_sec: float = 0.0
    available: bool = False


@dataclass
class SlippageEstimate:
    """Estimated slippage for a given trade size."""
    condition_id: str = ""
    side: str = "BUY"
    size_usd: float = 100.0
    estimated_fill_price: float = 0.0
    midpoint: float = 0.0
    slippage_pct: float = 0.0      # (fill - midpoint) / midpoint * 100
    can_fill: bool = False           # Is there enough depth?
    fill_depth_usd: float = 0.0     # Available depth on the side we're buying
    recommended_size_usd: float = 0.0 # Safe size (50% of fill depth)


@dataclass
class WhaleActivity:
    """Recent activity summary for a whale wallet."""
    wallet: str = ""
    trade_count_24h: int = 0
    total_volume_24h: float = 0.0
    avg_size_24h: float = 0.0
    markets_traded_24h: int = 0
    last_side: str = ""
    last_price: float = 0.0
    last_trade_ts: Optional[datetime] = None
    recent_trades: list = field(default_factory=list)


# ── Query Functions ─────────────────────────────────────────────────────────

def get_liquidity(condition_id: str, token_id: str = "", outcome: str = "") -> LiquidityInfo:
    """Get the latest liquidity snapshot for a market outcome.
    
    If token_id is not provided, fetches from gamma_markets first.
    Returns a LiquidityInfo with a composite liquidity_score (0-1).
    """
    info = LiquidityInfo(condition_id=condition_id)
    conn = _get_conn()
    if conn is None:
        return info

    try:
        cur = conn.cursor()
        
        # If no token_id, look it up from gamma_markets
        if not token_id:
            cur.execute("""
                SELECT tokens_json FROM gamma_markets 
                WHERE condition_id = %s AND active = true
                LIMIT 1
            """, (condition_id,))
            row = cur.fetchone()
            if row and row[0]:
                import json
                tokens = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                if tokens and len(tokens) > 0:
                    token_id = tokens[0].get("token_id", "")
                    outcome = tokens[0].get("outcome", "Yes")
        
        if not token_id:
            cur.close()
            return info

        info.token_id = token_id
        info.outcome = outcome

        # Get latest orderbook snapshot
        cur.execute("""
            SELECT best_bid, best_ask, midpoint, spread, 
                   bid_depth, ask_depth, bid_liquidity, ask_liquidity,
                   num_bids, num_asks, last_trade_price, snapshot_ts
            FROM clob_orderbook_snapshots
            WHERE condition_id = %s AND token_id = %s
            ORDER BY snapshot_ts DESC
            LIMIT 1
        """, (condition_id, token_id))
        row = cur.fetchone()
        cur.close()

        if row is None:
            return info

        info.best_bid = float(row[0]) if row[0] else 0.0
        info.best_ask = float(row[1]) if row[1] else 0.0
        info.midpoint = float(row[2]) if row[2] else 0.0
        info.spread = float(row[3]) if row[3] else 0.0
        info.bid_depth = float(row[4]) if row[4] else 0.0
        info.ask_depth = float(row[5]) if row[5] else 0.0
        info.bid_liquidity = float(row[6]) if row[6] else 0.0
        info.ask_liquidity = float(row[7]) if row[7] else 0.0
        info.available = True

        # Calculate derived metrics
        if info.midpoint > 0:
            info.spread_pct = (info.spread / info.midpoint) * 100
        
        # Snapshot age
        if row[11]:
            age = datetime.now(timezone.utc) - row[11].replace(tzinfo=timezone.utc)
            info.snapshot_age_sec = age.total_seconds()

        # Composite liquidity score (0-1)
        # Factors: spread tightness, depth on both sides
        spread_score = max(0, 1 - info.spread_pct / 20)  # 0% spread = 1.0, 20%+ = 0
        depth_score = min(1.0, (info.bid_liquidity + info.ask_liquidity) / 10000)  # 10k+ units = 1.0
        info.liquidity_score = (spread_score * 0.4 + depth_score * 0.6)
        
        # Thin market detection
        info.is_thin = (
            info.liquidity_score < 0.3
            or info.bid_liquidity < 500
            or info.ask_liquidity < 500
            or info.spread_pct > 10
        )

    except Exception as e:
        logger.warning(f"Liquidity query failed: {e}")
    
    return info


def estimate_slippage(condition_id: str, side: str, size_usd: float, token_id: str = "") -> SlippageEstimate:
    """Estimate slippage for a trade of given size and side.
    
    Uses the orderbook depth to model how much the price would move
    if we placed an order of `size_usd`.
    
    For BUY: walks up the asks from best_ask
    For SELL: walks down the bids from best_bid
    """
    liq = get_liquidity(condition_id, token_id)
    est = SlippageEstimate(
        condition_id=condition_id,
        side=side,
        size_usd=size_usd,
    )

    if not liq.available or liq.midpoint <= 0:
        return est

    est.midpoint = liq.midpoint

    # Available depth on the side we need
    if side.upper() == "BUY":
        fill_depth = liq.ask_liquidity  # We buy from asks
        est.fill_depth_usd = fill_depth * liq.best_ask if liq.best_ask > 0 else 0
    else:
        fill_depth = liq.bid_liquidity  # We sell into bids
        est.fill_depth_usd = fill_depth * liq.best_bid if liq.best_bid > 0 else 0

    # Can we fill?
    est.can_fill = est.fill_depth_usd >= size_usd
    est.recommended_size_usd = min(size_usd, est.fill_depth_usd * 0.5)  # Never take more than 50% of depth

    # Simple slippage model: proportional to size/depth ratio
    if est.fill_depth_usd > 0:
        impact_ratio = min(1.0, size_usd / est.fill_depth_usd)
        # Spread is our base slippage, impact_ratio scales it
        est.slippage_pct = liq.spread_pct + (impact_ratio * liq.spread_pct * 2)
        
        # Estimated fill price
        if side.upper() == "BUY":
            est.estimated_fill_price = liq.best_ask * (1 + est.slippage_pct / 100)
        else:
            est.estimated_fill_price = liq.best_bid * (1 - est.slippage_pct / 100)
    else:
        est.slippage_pct = 100.0  # No liquidity at all

    return est


def get_whale_activity(wallet: str, hours: int = 24) -> WhaleActivity:
    """Get recent trading activity for a whale wallet from data_api_trades.
    
    Args:
        wallet: The proxy wallet address (lowercase, with 0x prefix)
        hours: Lookback window in hours
    """
    activity = WhaleActivity(wallet=wallet)
    conn = _get_conn()
    if conn is None:
        return activity

    try:
        cur = conn.cursor()
        wallet_lower = wallet.lower()
        
        cur.execute("""
            SELECT side, price, size, title, condition_id, trade_ts
            FROM data_api_trades
            WHERE proxy_wallet = %s AND trade_ts >= NOW() - INTERVAL '%s hours'
            ORDER BY trade_ts DESC
            LIMIT 50
        """, (wallet_lower, hours))
        
        rows = cur.fetchall()
        cur.close()

        if not rows:
            return activity

        total_volume = 0.0
        markets = set()
        sides = {}

        for row in rows:
            side, price, size, title, cond_id, ts = row
            trade_value = float(price) * float(size) if price and size else 0
            total_volume += trade_value
            markets.add(cond_id)
            sides[side] = sides.get(side, 0) + 1

            activity.recent_trades.append({
                "side": side,
                "price": float(price) if price else 0,
                "size": float(size) if size else 0,
                "title": title,
                "condition_id": cond_id,
                "trade_ts": ts.isoformat() if ts else None,
            })

        activity.trade_count_24h = len(rows)
        activity.total_volume_24h = total_volume
        activity.avg_size_24h = total_volume / len(rows) if rows else 0
        activity.markets_traded_24h = len(markets)
        activity.last_side = rows[0][0] if rows else ""
        activity.last_price = float(rows[0][1]) if rows and rows[0][1] else 0
        activity.last_trade_ts = rows[0][5] if rows else None

    except Exception as e:
        logger.warning(f"Whale activity query failed: {e}")

    return activity


def get_market_info(condition_id: str) -> Optional[dict]:
    """Get market metadata from gamma_markets."""
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT condition_id, question, slug, active, closed, accepting_orders,
                   neg_risk, volume24hr, volume_total, liquidity, end_date_iso, tokens_json
            FROM gamma_markets
            WHERE condition_id = %s
            LIMIT 1
        """, (condition_id,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            return None
        import json
        return {
            "condition_id": row[0],
            "question": row[1],
            "slug": row[2],
            "active": row[3],
            "closed": row[4],
            "accepting_orders": row[5],
            "neg_risk": row[6],
            "volume_24hr": float(row[7]) if row[7] else 0,
            "volume_total": float(row[8]) if row[8] else 0,
            "liquidity": float(row[9]) if row[9] else 0,
            "end_date": row[10],
            "tokens": json.loads(row[11]) if row[11] else [],
        }
    except Exception as e:
        logger.warning(f"Market info query failed: {e}")
        return None


def get_recent_whale_trades(min_size: float = 5000, hours: int = 24, limit: int = 100) -> list:
    """Get recent large trades (whale-sized) from the data API.
    
    Useful for cross-referencing against known whale wallets.
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, condition_id, token_id, side, price, size, 
                   proxy_wallet, outcome, title, trade_ts
            FROM data_api_trades
            WHERE size >= %s AND trade_ts >= NOW() - INTERVAL '%s hours'
            ORDER BY size DESC, trade_ts DESC
            LIMIT %s
        """, (min_size, hours, limit))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "condition_id": r[1],
                "token_id": r[2],
                "side": r[3],
                "price": float(r[4]) if r[4] else 0,
                "size": float(r[5]) if r[5] else 0,
                "proxy_wallet": r[6],
                "outcome": r[7],
                "title": r[8],
                "trade_ts": r[9].isoformat() if r[9] else None,
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning(f"Whale trades query failed: {e}")
        return []


# ── CLI Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== Orderbook Query Module Smoke Test ===\n")
    
    # Test 1: Get a recent market
    print("--- Recent Markets (top 3 by volume) ---")
    conn = _get_conn()
    if conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT condition_id, question, volume24hr, liquidity
            FROM gamma_markets WHERE active = true
            ORDER BY volume24hr DESC NULLS LAST LIMIT 3
        """)
        for row in cur.fetchall():
            print(f"  {row[1][:60]}... vol24h=${float(row[2]):,.0f} liq=${float(row[3]):,.0f}")
            cid = row[0]
            liq = get_liquidity(cid)
            if liq.available:
                print(f"    Liquidity: spread={liq.spread_pct:.1f}% score={liq.liquidity_score:.2f} "
                      f"bid_liq={liq.bid_liquidity:.0f} ask_liq={liq.ask_liquidity:.0f} thin={liq.is_thin}")
                slip = estimate_slippage(cid, "BUY", 500)
                print(f"    Slippage($500): {slip.slippage_pct:.1f}% can_fill={slip.can_fill} "
                      f"rec_size=${slip.recommended_size_usd:.0f}")
            else:
                print("    No orderbook snapshot yet")
        cur.close()
    
    # Test 2: Whale trades
    print("\n--- Recent Large Trades (top 5) ---")
    whales = get_recent_whale_trades(min_size=1000, hours=48, limit=5)
    for w in whales:
        print(f"  {w['proxy_wallet'][:12]}... {w['side']} {w['size']:.0f} @ {w['price']:.3f} "
              f"on {w['title'][:40]}...")
    
    # Test 3: Unique wallets
    if conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(DISTINCT proxy_wallet), count(*), COALESCE(sum(size::numeric), 0)
            FROM data_api_trades WHERE trade_ts >= NOW() - INTERVAL '24 hours'
        """)
        row = cur.fetchone()
        print(f"\n--- Data API Stats (24h) ---")
        print(f"  Unique wallets: {row[0]}, Trades: {row[1]}, Volume: ${float(row[2]):,.0f}")
        cur.close()
    
    print("\n=== Smoke test complete ===")
