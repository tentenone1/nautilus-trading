"""
Slippage and spread simulation for paper trading.

Models realistic fill prices by adjusting mid-price based on:
1. Order size relative to visible liquidity
2. Market spread
3. Fixed basis point buffer
"""

from typing import Literal


def compute_slippage(
    side: str,          # "BUY" or "SELL"
    price: float,      # mid-price at time of fill
    order_size_usd: float,
    visible_volume_usd: float,
    model: str = "bounded_adverse",
    size_threshold_bps: float = 100,
    max_slippage_bps: float = 20,
) -> float:
    """
    Compute fill price with slippage adjustment.
    
    Args:
        side: "BUY" or "SELL" 
        price: current mid-price
        order_size_usd: size of our order in USD
        visible_volume_usd: visible order book volume (for comparison)
        model: slippage model type
        size_threshold_bps: size threshold in basis points
        max_slippage_bps: maximum slippage in basis points
    
    Returns:
        Fill price (adjusted for slippage).
        For BUY: slightly higher than mid (adverse)
        For SELL: slightly lower than mid (adverse)
    """
    if model == "none":
        return price
    
    if model == "fixed_bps":
        bps = 5  # 5 bps fixed
        slip = price * bps / 10000
        return price + slip if side == "BUY" else price - slip
    
    if model == "bounded_adverse":
        # Size relative to visible volume in basis points
        if visible_volume_usd <= 0:
            # No visible volume data — use fixed 5 bps fallback
            slippage_bps = 5.0
            slip = price * slippage_bps / 10000
            return price + slip if side == "BUY" else price - slip
        else:
            size_bps = (order_size_usd / visible_volume_usd) * 10000
        
        # Only apply slippage if order exceeds threshold
        if size_bps <= size_threshold_bps:
            return price  # no slippage for small orders
        
        # Slippage scales with size: more volume = more adverse
        # Capped at max_slippage_bps
        excess_bps = min(size_bps - size_threshold_bps, max_slippage_bps * 2)
        slippage_bps = min(excess_bps / 2, max_slippage_bps)
        
        slip = price * slippage_bps / 10000
        return price + slip if side == "BUY" else price - slip
    
    return price  # fallback: no slippage


def compute_spread_adjustment(
    side: str,
    price: float,
    visible_spread_bps: float = 5,
    model: str = "proportional",
) -> float:
    """
    Adjust fill price for bid-ask spread.
    
    When BUYING, you hit the ask (higher).
    When SELLING, you hit the bid (lower).
    """
    if model == "fixed":
        spread = price * visible_spread_bps / 10000
    else:  # proportional: spread is a fraction of price
        spread = price * min(visible_spread_bps, 20) / 10000
    
    if side == "BUY":
        return price + spread / 2
    else:
        return price - spread / 2


def compute_fill_price(
    side: str,
    mid_price: float,
    order_size_usd: float,
    visible_volume_usd: float = 0,
    slippage_model: str = "bounded_adverse",
) -> float:
    """
    Compute realistic fill price combining slippage and spread.
    
    Order of operations:
    1. Start with mid price
    2. Apply spread (move toward relevant side)
    3. Apply slippage (additional adverse move for large orders)
    """
    # Step 1: mid price
    fill = mid_price
    
    # Step 2: spread adjustment
    fill = compute_spread_adjustment(side, fill)
    
    # Step 3: slippage for large orders
    fill = compute_slippage(
        side=side,
        price=fill,
        order_size_usd=order_size_usd,
        visible_volume_usd=visible_volume_usd,
        model=slippage_model,
    )
    
    # Ensure price stays in [0, 1] for binary options
    return max(0.001, min(0.999, fill))
