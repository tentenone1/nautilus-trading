"""Orderbook Liquidity Check — Signal pipeline integration.

Provides a simple function to check market liquidity before entering a trade.
Returns a LiquidityResult that the signal pipeline uses to gate trades in
thin-liquidity markets and adjust position sizing.

This module uses scripts/orderbook_query.py for PostgreSQL queries.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("OrderbookCheck")

# Import from orderbook_query — lazy import to avoid startup issues
_obq = None

def _get_obq():
    global _obq
    if _obq is None:
        try:
            from scripts.orderbook_query import get_liquidity, estimate_slippage, get_whale_activity
            _obq = type("OBQ", (), {
                "get_liquidity": staticmethod(get_liquidity),
                "estimate_slippage": staticmethod(estimate_slippage),
                "get_whale_activity": staticmethod(get_whale_activity),
            })()
        except Exception as e:
            logger.warning("Cannot import orderbook_query: %s", e)
    return _obq


@dataclass
class LiquidityResult:
    """Result of a liquidity check for a signal."""
    available: bool = False          # Orderbook data available?
    is_thin: bool = False            # Thin liquidity market?
    liquidity_score: float = 0.0     # 0-1 composite score
    spread_pct: float = 0.0          # Bid-ask spread as % of midpoint
    slippage_pct: float = 0.0        # Estimated slippage for our trade
    can_fill: bool = True            # Enough depth to fill?
    recommended_size_usd: float = 0.0  # Safe position size
    bid_liquidity: float = 0.0       # Top-10 bid depth
    ask_liquidity: float = 0.0       # Top-10 ask depth
    snapshot_age_sec: float = 0.0    # How old is the snapshot?


def check_liquidity(
    condition_id: str,
    token_id: str = "",
    side: str = "BUY",
    size_usd: float = 100.0,
    reject_thin: bool = True,
    thin_threshold: float = 0.25,
    max_spread_pct: float = 15.0,
    max_slippage_pct: float = 20.0,
) -> LiquidityResult:
    """Check market liquidity before entering a trade.
    
    Args:
        condition_id: Market condition ID (from signal)
        token_id: Token ID (optional, looked up from gamma_markets)
        side: Trade side ("BUY" or "SELL")
        size_usd: Intended position size in USD
        reject_thin: Whether to reject thin-liquidity markets
        thin_threshold: Minimum liquidity_score to not be thin (0-1)
        max_spread_pct: Maximum acceptable spread %
        max_slippage_pct: Maximum acceptable slippage %
    
    Returns:
        LiquidityResult with liquidity assessment and recommendation.
    """
    result = LiquidityResult()
    obq = _get_obq()
    
    if obq is None:
        # Orderbook data not available — pass through with warning
        result.available = False
        result.can_fill = True  # Don't block trades if data unavailable
        logger.debug("Orderbook data unavailable, passing through")
        return result
    
    # Get liquidity info
    liq = obq.get_liquidity(condition_id, token_id)
    result.available = liq.available
    result.is_thin = liq.is_thin
    result.liquidity_score = liq.liquidity_score
    result.spread_pct = liq.spread_pct
    result.bid_liquidity = liq.bid_liquidity
    result.ask_liquidity = liq.ask_liquidity
    result.snapshot_age_sec = liq.snapshot_age_sec
    
    if not liq.available:
        # No orderbook snapshot for this market — don't block
        result.can_fill = True
        return result
    
    # Estimate slippage
    slip = obq.estimate_slippage(condition_id, side, size_usd, token_id)
    result.slippage_pct = slip.slippage_pct
    result.can_fill = slip.can_fill
    result.recommended_size_usd = slip.recommended_size_usd
    
    # Check thresholds
    if reject_thin and liq.is_thin and liq.liquidity_score < thin_threshold:
        result.can_fill = False
        logger.info(
            "THIN_MARKET | %s... | score=%.2f < %.2f | spread=%.1f%% | rejecting",
            condition_id[:16], liq.liquidity_score, thin_threshold, liq.spread_pct
        )
    
    if liq.spread_pct > max_spread_pct:
        result.can_fill = False
        logger.info(
            "WIDE_SPREAD | %s... | spread=%.1f%% > %.1f%%",
            condition_id[:16], liq.spread_pct, max_spread_pct
        )
    
    if slip.slippage_pct > max_slippage_pct:
        # Don't reject, but reduce size
        result.recommended_size_usd = min(result.recommended_size_usd, size_usd * 0.25)
        logger.info(
            "HIGH_SLIPPAGE | %s... | slip=%.1f%% > %.1f%% | reducing size",
            condition_id[:16], slip.slippage_pct, max_slippage_pct
        )
    
    # If snapshot is very old (>10 min), reduce confidence
    if liq.snapshot_age_sec > 600:
        result.recommended_size_usd *= 0.5
        logger.debug(
            "STALE_SNAPSHOT | %s... | age=%.0fs",
            condition_id[:16], liq.snapshot_age_sec
        )
    
    return result


def get_whale_wallet_activity(wallet: str, hours: int = 24) -> Optional[dict]:
    """Get recent activity for a whale wallet from orderbook data.
    
    Returns a dict with trade_count, volume, markets, and last trade info.
    Returns None if orderbook data is unavailable.
    """
    obq = _get_obq()
    if obq is None:
        return None
    
    activity = obq.get_whale_activity(wallet, hours)
    if not activity.trade_count_24h:
        return None
    
    return {
        "wallet": activity.wallet,
        "trade_count_24h": activity.trade_count_24h,
        "total_volume_24h": activity.total_volume_24h,
        "avg_size_24h": activity.avg_size_24h,
        "markets_traded_24h": activity.markets_traded_24h,
        "last_side": activity.last_side,
        "last_price": activity.last_price,
    }
