"""Whale Follower — Risk Management.

Standalone functions for risk control: kill switch, daily loss limits,
position count limits, and category exposure tracking. No class coupling —
all state is passed as parameters.

Responsibilities:
- Kill switch activation/deactivation
- Daily loss limit checks (general + sports)
- Position count limits
- Category exposure limits

Usage:
    from strategies.wf_risk import (
        check_daily_loss_limit,
        is_kill_switch_active,
        trigger_kill_switch,
        check_position_count,
    )
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Set

from strategies.wf_constants import (
    SPORTS_DAILY_LOSS_LIMIT,
    MAX_SINGLE_POSITION_PCT,
    MAX_TOTAL_EXPOSURE_PCT,
    MAX_MARKET_EXPOSURE_PCT,
)


# ── Kill Switch ───────────────────────────────────────────────────────────


def is_kill_switch_active(
    *,
    kill_switch_breached: bool,
) -> bool:
    """Check if kill switch is active (trading blocked).

    Args:
        kill_switch_breached: The _kill_switch_breached flag.

    Returns:
        True if trading should be blocked, False otherwise.
    """
    return kill_switch_breached


def activate_kill_switch(
    *,
    kill_switch_breached: bool,
    log,
    reason: str = "",
) -> bool:
    """Activate the kill switch (permanently blocks new trading).

    Once activated, requires manual reset or process restart.

    Args:
        kill_switch_breached: The _kill_switch_breached flag (will be set True).
        log: Logger instance.
        reason: Reason for activation.

    Returns:
        True (always, since this activates the switch).
    """
    log.error(
        f"KILL_SWITCH ACTIVATED: {reason}",
        extra={"reason": reason, "timestamp": time.time()},
    )
    return True


def deactivate_kill_switch(
    *,
    log,
) -> bool:
    """Deactivate the kill switch (allow trading again).

    WARNING: This should only be called after verifying that the
    condition that triggered the kill switch has been resolved.

    Args:
        log: Logger instance.

    Returns:
        False (always, since this deactivates the switch).
    """
    log.warning("KILL_SWITCH DEACTIVATED — trading resumed", extra={"timestamp": time.time()})
    return False


# ── Daily Loss Limit ─────────────────────────────────────────────────────


def check_daily_loss_limit(
    *,
    config,
    daily_pnl: float,
    daily_pnl_date: str,
    daily_loss_breached: bool,
    sports_daily_pnl: float,
    sports_daily_pnl_date: str,
    sports_daily_loss_breached: bool,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    exit_all_positions_func: Callable,
    log,
) -> tuple[float, str, bool, float, str, bool]:
    """Check if daily loss limits have been breached.

    General daily loss limit and sports-specific limit. If breached,
    closes all positions and blocks new trading for the day.

    Args:
        config: WhaleFollowerConfig with daily_loss_limit.
        daily_pnl: Current general daily P&L.
        daily_pnl_date: Date string for general daily P&L.
        daily_loss_breached: General daily loss breach flag.
        sports_daily_pnl: Current sports daily P&L.
        sports_daily_pnl_date: Date string for sports daily P&L.
        sports_daily_loss_breached: Sports daily loss breach flag.
        open_positions: The _open_positions registry.
        exited_positions: The _exited_positions dedup set.
        last_exit_time: Re-entry cooldown dict.
        exit_all_positions_func: Callback to close all positions.
        log: Logger instance.

    Returns:
        Tuple: (daily_pnl, daily_pnl_date, daily_loss_breached,
                sports_daily_pnl, sports_daily_pnl_date, sports_daily_loss_breached)
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ── New day reset ──
    if today != daily_pnl_date:
        log.info(
            f"New trading day — resetting daily P&L counters",
            extra={"new_date": today, "prev_daily_pnl": daily_pnl},
        )
        daily_pnl = 0.0
        daily_pnl_date = today
        daily_loss_breached = False

    if today != sports_daily_pnl_date:
        sports_daily_pnl = 0.0
        sports_daily_pnl_date = today
        sports_daily_loss_breached = False
        log.info(
            f"New sports trading day — resetting sports daily P&L",
            extra={"new_date": today, "prev_sports_pnl": sports_daily_pnl},
        )

    # ── Already breached — skip logging ──
    if daily_loss_breached:
        return (daily_pnl, daily_pnl_date, daily_loss_breached,
                sports_daily_pnl, sports_daily_pnl_date, sports_daily_loss_breached)

    if sports_daily_loss_breached:
        return (daily_pnl, daily_pnl_date, daily_loss_breached,
                sports_daily_pnl, sports_daily_pnl_date, sports_daily_loss_breached)

    # ── General daily loss check ──
    daily_limit = getattr(config, "daily_loss_limit", 500.0)
    if daily_pnl <= -daily_limit:
        log.error(
            f"DAILY LOSS LIMIT BREACHED: ${daily_pnl:,.2f} / -${daily_limit:,.2f}. "
            f"Closing all positions and stopping auto-trade.",
            extra={
                "daily_pnl": daily_pnl,
                "daily_limit": daily_limit,
                "open_positions_count": len(open_positions),
            },
        )
        daily_loss_breached = True
        exit_all_positions_func()

    # ── Sports daily loss check ──
    sports_limit = getattr(config, "sports_daily_loss_limit", SPORTS_DAILY_LOSS_LIMIT)
    if sports_daily_pnl <= -sports_limit:
        log.error(
            f"SPORTS DAILY LOSS LIMIT BREACHED: ${sports_daily_pnl:,.2f} / -${sports_limit:,.2f}. "
            f"Closing all positions and stopping sports auto-trade.",
            extra={
                "sports_daily_pnl": sports_daily_pnl,
                "sports_limit": sports_limit,
            },
        )
        sports_daily_loss_breached = True
        exit_all_positions_func()

    return (daily_pnl, daily_pnl_date, daily_loss_breached,
            sports_daily_pnl, sports_daily_pnl_date, sports_daily_loss_breached)


# ── Position Count Limits ────────────────────────────────────────────────


def check_position_count_limit(
    *,
    config,
    open_positions: Dict[str, Dict],
    log,
) -> tuple[bool, str]:
    """Check if position count limit has been reached.

    Args:
        config: WhaleFollowerConfig with max_open_positions.
        open_positions: The _open_positions registry.
        log: Logger instance.

    Returns:
        (allowed, reason) — True if under limit, False if blocked.
    """
    max_positions = getattr(config, "max_open_positions", 50)
    current_count = len(open_positions)

    if current_count >= max_positions:
        log.info(
            f"Max positions reached ({current_count}/{max_positions})",
            extra={
                "current_count": current_count,
                "max_positions": max_positions,
            },
        )
        return False, f"max_positions={max_positions}"

    return True, ""


def get_remaining_position_capacity(
    *,
    config,
    open_positions: Dict[str, Dict],
) -> int:
    """Get how many more positions can be opened.

    Args:
        config: WhaleFollowerConfig with max_open_positions.
        open_positions: The _open_positions registry.

    Returns:
        Number of positions that can still be opened.
    """
    max_positions = getattr(config, "max_open_positions", 50)
    current_count = len(open_positions)
    return max(0, max_positions - current_count)


# ── Exposure Limits ───────────────────────────────────────────────────────


def calculate_total_exposure(
    *,
    open_positions: Dict[str, Dict],
) -> float:
    """Calculate total gross exposure across all open positions.

    For binary prediction markets: max_loss = size (each share costs $0-$1).

    Args:
        open_positions: The _open_positions registry.

    Returns:
        Total exposure in USD.
    """
    total = 0.0
    for pos_info in open_positions.values():
        size = pos_info.get("size", 0.0) or 0.0
        # entry_price kept for future precision: exposure = size * entry_price
        exposure = size  # size is already in USD
        total += exposure
    return total


def check_single_position_limit(
    *,
    config,
    proposed_size_usd: float,
    capital_base: Optional[float] = None,
    log,
) -> tuple[bool, str]:
    """Check if proposed position size exceeds single position limit.

    Args:
        config: WhaleFollowerConfig with max_single_position_pct.
        proposed_size_usd: Proposed position size in USD.
        capital_base: Override capital base (defaults to config).
        log: Logger instance.

    Returns:
        (allowed, reason) — True if under limit, False if blocked.
    """
    max_pct = getattr(config, "max_single_position_pct", MAX_SINGLE_POSITION_PCT)
    capital = capital_base if capital_base is not None else getattr(
        config, "validation_capital_base", config.bankroll
    )
    max_size = capital * max_pct

    if proposed_size_usd > max_size:
        log.info(
            f"Single position limit exceeded: ${proposed_size_usd:,.2f} > ${max_size:,.2f}",
            extra={
                "proposed_size": proposed_size_usd,
                "max_size": max_size,
                "max_pct": max_pct,
                "capital": capital,
            },
        )
        return False, f"single_position_max=${max_size:,.2f}"

    return True, ""


def check_total_exposure_limit(
    *,
    config,
    open_positions: Dict[str, Dict],
    proposed_size_usd: float,
    capital_base: Optional[float] = None,
    log,
) -> tuple[bool, str]:
    """Check if adding proposed position would exceed total exposure limit.

    Args:
        config: WhaleFollowerConfig with max_total_exposure_pct.
        open_positions: The _open_positions registry.
        proposed_size_usd: Proposed additional position size.
        capital_base: Override capital base (defaults to config).
        log: Logger instance.

    Returns:
        (allowed, reason) — True if under limit, False if blocked.
    """
    max_pct = getattr(config, "max_total_exposure_pct", MAX_TOTAL_EXPOSURE_PCT)
    capital = capital_base if capital_base is not None else getattr(
        config, "validation_capital_base", config.bankroll
    )
    max_exposure = capital * max_pct

    current_exposure = calculate_total_exposure(open_positions=open_positions)
    new_exposure = current_exposure + proposed_size_usd

    if new_exposure > max_exposure:
        log.info(
            f"Total exposure limit exceeded: ${new_exposure:,.2f} > ${max_exposure:,.2f}",
            extra={
                "current_exposure": current_exposure,
                "proposed_size": proposed_size_usd,
                "new_exposure": new_exposure,
                "max_exposure": max_exposure,
                "max_pct": max_pct,
                "capital": capital,
            },
        )
        return False, f"total_exposure_max=${max_exposure:,.2f}"

    return True, ""


# ── Category Exposure ─────────────────────────────────────────────────────


def get_category_exposure(
    *,
    open_positions: Dict[str, Dict],
    category: str,
) -> float:
    """Get total exposure for a specific category.

    Args:
        open_positions: The _open_positions registry.
        category: Market category to filter by.

    Returns:
        Total exposure in USD for that category.
    """
    total = 0.0
    for pos_info in open_positions.values():
        pos_category = (pos_info.get("category", "") or "").lower()
        if pos_category == category.lower():
            size = pos_info.get("size", 0.0) or 0.0
            total += size
    return total


def check_category_exposure_limit(
    *,
    config,
    open_positions: Dict[str, Dict],
    category: str,
    proposed_size_usd: float,
    capital_base: Optional[float] = None,
    category_limit_pct: Optional[float] = None,
    log,
) -> tuple[bool, str]:
    """Check if adding position would exceed category exposure limit.

    Args:
        config: WhaleFollowerConfig.
        open_positions: The _open_positions registry.
        category: Market category to check.
        proposed_size_usd: Proposed additional position size.
        capital_base: Override capital base.
        category_limit_pct: Override category limit pct.
        log: Logger instance.

    Returns:
        (allowed, reason) — True if under limit, False if blocked.
    """
    max_pct = category_limit_pct if category_limit_pct is not None else getattr(
        config, "max_market_exposure_pct", MAX_MARKET_EXPOSURE_PCT
    )
    capital = capital_base if capital_base is not None else getattr(
        config, "validation_capital_base", config.bankroll
    )
    max_exposure = capital * max_pct

    current_exposure = get_category_exposure(
        open_positions=open_positions, category=category
    )
    new_exposure = current_exposure + proposed_size_usd

    if new_exposure > max_exposure:
        log.info(
            f"Category exposure limit exceeded ({category}): "
            f"${new_exposure:,.2f} > ${max_exposure:,.2f}",
            extra={
                "category": category,
                "current_exposure": current_exposure,
                "proposed_size": proposed_size_usd,
                "new_exposure": new_exposure,
                "max_exposure": max_exposure,
            },
        )
        return False, f"category_{category}_max=${max_exposure:,.2f}"

    return True, ""


# ── Risk Summary ────────────────────────────────────────────────────────


def get_risk_summary(
    *,
    config,
    open_positions: Dict[str, Dict],
    daily_pnl: float,
    sports_daily_pnl: float,
    kill_switch_breached: bool,
    daily_loss_breached: bool,
    sports_daily_loss_breached: bool,
) -> Dict[str, Any]:
    """Get a summary of current risk state.

    Args:
        config: WhaleFollowerConfig.
        open_positions: The _open_positions registry.
        daily_pnl: Current daily P&L.
        sports_daily_pnl: Current sports daily P&L.
        kill_switch_breached: Kill switch flag.
        daily_loss_breached: Daily loss breach flag.
        sports_daily_loss_breached: Sports daily loss breach flag.

    Returns:
        Dict with risk metrics and status.
    """
    max_positions = getattr(config, "max_open_positions", 50)
    daily_limit = getattr(config, "daily_loss_limit", 500.0)
    sports_limit = getattr(config, "sports_daily_loss_limit", SPORTS_DAILY_LOSS_LIMIT)

    return {
        "position_count": len(open_positions),
        "max_positions": max_positions,
        "positions_remaining": max_positions - len(open_positions),
        "total_exposure": calculate_total_exposure(open_positions=open_positions),
        "daily_pnl": daily_pnl,
        "daily_limit": daily_limit,
        "daily_remaining": daily_limit + daily_pnl,
        "sports_daily_pnl": sports_daily_pnl,
        "sports_limit": sports_limit,
        "sports_remaining": sports_limit + sports_daily_pnl,
        "kill_switch_active": kill_switch_breached,
        "daily_loss_breached": daily_loss_breached,
        "sports_daily_loss_breached": sports_daily_loss_breached,
        "trading_allowed": (
            not kill_switch_breached
            and not daily_loss_breached
            and len(open_positions) < max_positions
        ),
    }