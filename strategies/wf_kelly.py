"""Whale Follower — Kelly criterion sizing, liquidity adjustment, and exposure.

Standalone functions for position sizing with no class coupling.
All state is passed as explicit parameters.
"""

from __future__ import annotations

from strategies.wf_constants import (
    SPORTS_KELLY_MULTIPLIER,
    LIQUIDITY_TIER4_THRESHOLD,
    LIQUIDITY_TIER3_THRESHOLD,
    LIQUIDITY_TIER4_MULTIPLIER,
    LIQUIDITY_TIER3_MULTIPLIER,
    LIQUIDITY_TIER2_MULTIPLIER,
)


def kelly_size(
    *,
    bankroll: float,
    kelly_fraction: float,
    max_position_pct: float,
    price: float,
    whale_win_rate: float | None = None,
    edge_score: float = 0.0,
    available_balance: float | None = None,
    market_category: str = "",
    max_single_position_pct: float = 0.02,  # hard cap (same as check_position_limits)
    whale_tiering=None,
) -> float:
    """Kelly criterion position sizing with edge_score calibration.

    Uses whale's actual historical win rate if available,
    otherwise falls back to fixed estimate (55 %).

    Edge_score calibrates the base Kelly fraction using empirically
    derived mapping from 1228-trade analysis (2026-05-03).
    Applies sanity checks: max % of portfolio, min % of portfolio.

    When available_balance is provided, the effective bankroll is
    min(config.bankroll, available_balance) — preventing position
    oversizing as the sandbox balance depletes.

    Args:
        bankroll: Configured bankroll from WhaleFollowerConfig.
        kelly_fraction: Fractional Kelly multiplier (e.g. 0.25).
        max_position_pct: Maximum position size as fraction of bankroll.
        price: Current market price (probability).
        whale_win_rate: Whale's historical win rate, or None to use 55 %.
        edge_score: Calibrated edge score (0.0–1.0).
        available_balance: Live available USDC.e balance, or None.
        whale_tiering: Optional WhaleTiering instance for edge-based sizing.

    Returns:
        Size in USD, rounded to 2 decimals. Returns 0.0 if no edge.
    """
    if price <= 0.001 or price >= 0.999:
        return 0.0

    # Use actual available balance as effective bankroll (prevents
    # AccountBalanceNegative death spiral when sandbox balance drops)
    effective_bankroll = (
        min(bankroll, available_balance)
        if available_balance is not None
        else bankroll
    )
    p = whale_win_rate if whale_win_rate else 0.55
    q = 1 - p
    b = (1 - price) / price

    kelly = (b * p - q) / b
    if kelly <= 0:
        return 0.0

    # Sports markets: apply halved Kelly multiplier (38.6% WR vs 55% breakeven)
    if market_category.lower() == "sports":
        kelly_fraction *= SPORTS_KELLY_MULTIPLIER
    # Crypto: cap Kelly at 15% (high volatility, 39% market-resolved loss rate)
    elif market_category.lower() == "crypto":
        kelly_fraction = min(kelly_fraction, 0.15)
    # Geopolitics: cap Kelly at 10% (small sample, best avg win but low volume)
    elif market_category.lower() == "geopolitics":
        kelly_fraction = min(kelly_fraction, 0.10)

    # Base fractional Kelly from config
    base_size = effective_bankroll * kelly * kelly_fraction

    # Edge-score calibrated Kelly fraction (overrides base for finer granularity)
    if whale_tiering is not None:
        edge_kelly = whale_tiering.get_edge_kelly(edge_score)
        # Blend: use the MORE CONSERVATIVE of base fraction and edge-calibrated
        effective_kelly = min(kelly_fraction, edge_kelly)
        # edge_kelly replaces base when edge is well-understood (>=0.35)
        if edge_score >= 0.35:
            effective_kelly = edge_kelly
        size = effective_bankroll * kelly * effective_kelly
    else:
        size = base_size

    # Apply sanity checks
    if whale_tiering is not None:
        sanity = whale_tiering.get_sanity_checks()
        if sanity.get("enabled", True):
            max_pct = sanity.get("max_position_pct", max_position_pct)
            min_pct = sanity.get("min_position_pct", 0.01)
        else:
            max_pct = max_position_pct
            min_pct = 0.0
    else:
        max_pct = max_position_pct
        min_pct = 0.0

    cap = effective_bankroll * max_pct
    floor = effective_bankroll * min_pct

    # ── HARD CAP: enforce max_single_position_pct (2% by default) ──
    # Align with check_position_limits() to prevent kill switch triggers
    # on routine Kelly-sized positions.
    hard_cap = effective_bankroll * max_single_position_pct
    if cap > hard_cap:
        cap = hard_cap

    # Clamp: floor <= size <= cap
    if size < floor:
        size = floor
    return round(min(size, cap), 2)


def adjust_size_for_liquidity(
    size_usd: float,
    instrument_id_str: str,
    get_market_event_time_func=None,
    log_func=None,
) -> float:
    """Adjust position size based on market liquidity tier.

    Args:
        size_usd: Original Kelly-sized amount in USD.
        instrument_id_str: Full instrument ID string.
        get_market_event_time_func: Callable returning event timing dict
            (defaults to strategies.wf_sports.get_market_event_time).
        log_func: Optional logging callable for adjustment messages.

    Returns:
        Adjusted size in USD, reduced for illiquid markets.
    """
    if get_market_event_time_func is None:
        from strategies.wf_sports import get_market_event_time
        get_market_event_time_func = get_market_event_time

    timing = get_market_event_time_func(instrument_id_str)
    liq_tier = timing.get("liquidity_tier", "tier3")
    volume = timing.get("volume", 0)
    liquidity = timing.get("liquidity", 0)

    if liq_tier == "tier4" or (volume + liquidity) < LIQUIDITY_TIER4_THRESHOLD:
        # Illiquid: reduce to 25% of Kelly size
        adjusted = size_usd * LIQUIDITY_TIER4_MULTIPLIER
        if log_func:
            log_func(f"Liquidity adjustment (tier4): ${size_usd:,.0f} → ${adjusted:,.0f}")
        return adjusted
    elif liq_tier == "tier3" or (volume + liquidity) < LIQUIDITY_TIER3_THRESHOLD:
        # Moderate: reduce to 50% of Kelly size
        adjusted = size_usd * LIQUIDITY_TIER3_MULTIPLIER
        if log_func:
            log_func(f"Liquidity adjustment (tier3): ${size_usd:,.0f} → ${adjusted:,.0f}")
        return adjusted
    elif liq_tier == "tier2":
        # Good: reduce to 75% of Kelly size
        adjusted = size_usd * LIQUIDITY_TIER2_MULTIPLIER
        if log_func:
            log_func(f"Liquidity adjustment (tier2): ${size_usd:,.0f} → ${adjusted:,.0f}")
        return adjusted

    return size_usd  # tier1: full Kelly size


def current_gross_exposure(
    open_positions: dict,
) -> float:
    """Calculate total notional exposure from open positions.

    For binary-option positions, max loss = cost basis = quantity * entry_price.

    This is the standalone equivalent of the strategy's _current_gross_exposure
    method, but operates on a plain dict instead of the Nautilus cache.

    NOTE: If you need Nautilus cache–based exposure, call the original
    method on the strategy instance. This function is for when you already
    have a snapshot of positions as a dict.

    Args:
        open_positions: Dict mapping instrument_id_str to position info dicts
            (same shape as WhaleFollower._open_positions). Each entry should
            contain at least "size" (the position_size_usd).

    Returns:
        Total exposure in USD.
    """
    total = 0.0
    for pos_info in open_positions.values():
        total += pos_info.get("size", 0.0)
    return total
