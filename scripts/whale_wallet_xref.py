"""Whale Wallet Cross-Reference — Links known whale wallets from trades.db
with orderbook activity from data_api_trades in PostgreSQL.

This module:
1. Builds a wallet -> whale_name mapping from trades.db
2. Matches those wallets against data_api_trades proxy_wallet addresses
3. Provides functions to enrich whale signals with orderbook activity data
4. Updates data_api_trades with whale classifications when matches are found

Used by the signal pipeline to check if an unknown wallet in data_api_trades
is actually a known whale, and by the whale_tracker to enrich whale data.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("WhaleWalletXref")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
CLASSIFICATIONS_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_classifications.json")
PG_DSN = "postgresql://polymarket:polymarket@localhost:5432/polymarket_orderbook"

# ── Cache ──────────────────────────────────────────────────────────────────
_wallet_map: dict[str, dict] = {}  # wallet_lower -> {name, classification, action, ...}
_last_loaded: datetime = datetime.min
_CACHE_TTL = timedelta(minutes=30)


def _load_wallet_map(force: bool = False) -> dict:
    """Load and cache the wallet -> whale mapping from trades.db and classifications."""
    global _wallet_map, _last_loaded

    if not force and _wallet_map and datetime.now(timezone.utc) - _last_loaded < _CACHE_TTL:
        return _wallet_map

    _wallet_map = {}

    # Load from trades.db
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        cur.execute("""
            SELECT whale_name, whale_address
            FROM trades
            WHERE whale_address IS NOT NULL AND LENGTH(whale_address) > 10
            GROUP BY whale_name
        """)
        for name, addr in cur.fetchall():
            if addr and len(addr) >= 20:
                key = addr.lower()
                if len(addr) == 42:
                    key = addr.lower()
                _wallet_map[key] = {
                    "name": name,
                    "address": addr,
                    "source": "trades_db",
                }
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to load from trades.db: {e}")

    # Load from whale_classifications.json
    try:
        if CLASSIFICATIONS_PATH.exists():
            data = json.loads(CLASSIFICATIONS_PATH.read_text())
            for whale_name, cls_data in data.get("classifications", {}).items():
                # Check if the classification has an address field
                addr = cls_data.get("address", cls_data.get("whale_address", ""))
                if addr and len(addr) >= 20:
                    key = addr.lower()
                    if key in _wallet_map:
                        _wallet_map[key]["classification"] = cls_data.get("classification", "")
                        _wallet_map[key]["action"] = cls_data.get("action", "")
                        _wallet_map[key]["win_rate"] = cls_data.get("win_rate", 0)
                    else:
                        _wallet_map[key] = {
                            "name": whale_name,
                            "address": addr,
                            "classification": cls_data.get("classification", ""),
                            "action": cls_data.get("action", ""),
                            "win_rate": cls_data.get("win_rate", 0),
                            "source": "classifications",
                        }
    except Exception as e:
        logger.warning(f"Failed to load classifications: {e}")

    _last_loaded = datetime.now(timezone.utc)
    logger.info(f"Loaded {len(_wallet_map)} known whale wallets")
    return _wallet_map


def lookup_wallet(address: str) -> Optional[dict]:
    """Look up a wallet address in the known whale mapping.
    
    Args:
        address: Ethereum wallet address (0x...)
    
    Returns:
        Dict with whale info if found, None otherwise.
    """
    if not address or len(address) < 10:
        return None
    wallet_map = _load_wallet_map()
    return wallet_map.get(address.lower())


def get_whale_orderbook_activity(whale_name: str, hours: int = 24) -> Optional[dict]:
    """Get orderbook trading activity for a known whale.
    
    Uses the whale's known wallet address(es) to find recent trades
    in the data_api_trades table.
    
    Args:
        whale_name: The whale's name from the tracker
        hours: Lookback window in hours
    
    Returns:
        Dict with activity data or None if not found.
    """
    wallet_map = _load_wallet_map()
    
    # Find all wallet addresses for this whale
    addresses = []
    for addr, info in wallet_map.items():
        if info["name"] == whale_name:
            addresses.append(addr)
    
    if not addresses:
        return None
    
    # Query PostgreSQL for activity
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        placeholders = ",".join(["%s"] * len(addresses))
        cur.execute(f"""
            SELECT COUNT(*), SUM(size::numeric), AVG(price::numeric),
                   COUNT(DISTINCT condition_id),
                   MAX(trade_ts)
            FROM data_api_trades
            WHERE proxy_wallet IN ({placeholders})
              AND trade_ts >= NOW() - INTERVAL '%s hours'
        """, (*addresses, hours))
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row and row[0] > 0:
            return {
                "whale_name": whale_name,
                "addresses": addresses,
                "trade_count": int(row[0]),
                "total_volume": float(row[1]) if row[1] else 0,
                "avg_price": float(row[2]) if row[2] else 0,
                "markets_traded": int(row[3]),
                "last_trade_ts": row[4].isoformat() if row[4] else None,
            }
    except Exception as e:
        logger.warning(f"PostgreSQL query failed for whale activity: {e}")
    
    return None


def enrich_unknown_wallets_from_orderbook(limit: int = 100) -> list[dict]:
    """Find unknown wallets in data_api_trades that match known whales.
    
    This cross-references wallets in the orderbook data against our known
    whale classifications to discover which anonymous traders are actually
    known whales operating under proxy wallets.
    
    Args:
        limit: Maximum number of unknown wallets to check
    
    Returns:
        List of dicts with matched wallet info.
    """
    wallet_map = _load_wallet_map()
    known_addresses = set(wallet_map.keys())
    
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Get unknown wallets with significant activity
        cur.execute("""
            SELECT proxy_wallet, COUNT(*) as trade_count,
                   SUM(size::numeric) as total_volume,
                   COUNT(DISTINCT condition_id) as market_count
            FROM data_api_trades
            GROUP BY proxy_wallet
            HAVING COUNT(*) >= 3
            ORDER BY total_volume DESC
            LIMIT %s
        """, (limit,))
        
        unknown_wallets = cur.fetchall()
        matches = []
        
        for wallet, count, volume, markets in unknown_wallets:
            wallet_lower = wallet.lower()
            if wallet_lower in known_addresses:
                info = wallet_map[wallet_lower]
                matches.append({
                    "proxy_wallet": wallet,
                    "known_whale": info["name"],
                    "classification": info.get("classification", "unknown"),
                    "action": info.get("action", "unknown"),
                    "trade_count": int(count),
                    "total_volume": float(volume) if volume else 0,
                    "market_count": int(markets),
                })
        
        cur.close()
        conn.close()
        return matches
        
    except Exception as e:
        logger.warning(f"Failed to query unknown wallets: {e}")
        return []


# ── CLI Test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== Whale Wallet Cross-Reference ===\n")
    
    # Load wallet map
    wallet_map = _load_wallet_map(force=True)
    print(f"Known whale wallets: {len(wallet_map)}")
    
    # Show a few examples
    for i, (addr, info) in enumerate(list(wallet_map.items())[:5]):
        print(f"  {addr[:20]}... -> {info['name']} ({info.get('classification', 'unknown')})")
    
    # Check overlap with orderbook data
    matches = enrich_unknown_wallets_from_orderbook(limit=500)
    print(f"\nKnown whale wallets found in orderbook data: {len(matches)}")
    for m in matches[:10]:
        print(f"  {m['proxy_wallet'][:16]}... -> {m['known_whale']} ({m['classification']}) "
              f"trades={m['trade_count']} vol=${m['total_volume']:.0f}")
    
    # Test activity lookup
    if wallet_map:
        first_whale = list(wallet_map.values())[0]["name"]
        activity = get_whale_orderbook_activity(first_whale, hours=168)
        print(f"\nOrderbook activity for {first_whale}: {activity}")
    
    print("\n=== Cross-reference complete ===")
