"""Whale Follower — Order Management.

Standalone functions for order submission, entry, exit, cancel,
and flatten. No class coupling — all state is passed as parameters.

Responsibilities:
- Market order submission
- Position entry with Kelly sizing
- Position exit with P&L tracking
- Order cancellation (kill switch)
- Emergency flatten

Usage:
    from strategies.wf_order_mgmt import (
        submit_market_order,
        enter_position,
        exit_position,
        cancel_all_orders,
        flatten_all_positions,
    )
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity

from strategies.wf_constants import (
    MAX_SANE_RETURN,
    RE_ENTRY_COOLDOWN_SECS,
    MIN_ENTRY_PRICE,
)
from strategies.wf_state import (
    has_active_position,
    remove_position,
    mark_exited,
    is_exited,
    is_in_cooldown,
    record_exit_time,
)


# ── Order Submission ─────────────────────────────────────────────────────


def submit_market_order(
    *,
    strategy,
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: Quantity,
    time_in_force: TimeInForce = TimeInForce.GTC,
) -> Any:
    """Submit a market order via the strategy's order factory.

    Args:
        strategy: Nautilus Strategy instance with order_factory.
        instrument_id: Target instrument.
        side: BUY or SELL.
        quantity: Order quantity.
        time_in_force: Time in force (default GTC).

    Returns:
        The submitted Order object.
    """
    order = strategy.order_factory.market(
        instrument_id=instrument_id,
        order_side=side,
        quantity=quantity,
        time_in_force=time_in_force,
    )
    strategy.submit_order(order)
    return order


# ── Position Entry ───────────────────────────────────────────────────────


def can_enter_position(
    *,
    config,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    instrument_id: InstrumentId,
    inst_key: str,
    kill_switch_breached: bool,
    daily_loss_breached: bool,
    sports_daily_loss_breached: bool,
    market_category: str,
    price: float,
    confidence: float,
    log,
) -> tuple[bool, str]:
    """Check if a new position entry is allowed.

    Checks:
    1. Kill switch not active
    2. Daily loss limit not breached
    3. Sports daily loss not breached (if sports)
    4. Position count limit
    5. Re-entry cooldown
    6. Already have position
    7. Minimum price threshold
    8. Minimum confidence threshold

    Args:
        config: WhaleFollowerConfig.
        open_positions: The _open_positions registry.
        exited_positions: The _exited_positions dedup set.
        last_exit_time: Re-entry cooldown dict.
        instrument_id: Instrument ID.
        inst_key: Instrument ID string.
        kill_switch_breached: Kill switch flag.
        daily_loss_breached: Daily loss breach flag.
        sports_daily_loss_breached: Sports daily loss breach flag.
        market_category: Market category (for sports check).
        price: Entry price.
        confidence: Signal confidence.
        log: Logger instance.

    Returns:
        (allowed, reason) tuple.
    """
    # 1. Kill switch
    if kill_switch_breached:
        log.warning(
            f"KILL_SWITCH active — rejecting entry",
            extra={"inst_key": inst_key[:50], "reason": "kill_switch"},
        )
        return False, "kill_switch"

    # 2. Daily loss limit
    if daily_loss_breached:
        log.warning(
            f"Daily loss breached — rejecting entry",
            extra={"inst_key": inst_key[:50], "reason": "daily_loss"},
        )
        return False, "daily_loss_breached"

    # 3. Sports daily loss
    if market_category.lower() == "sports" and sports_daily_loss_breached:
        log.warning(
            f"Sports daily loss breached — rejecting sports entry",
            extra={"inst_key": inst_key[:50], "reason": "sports_daily_loss"},
        )
        return False, "sports_daily_loss_breached"

    # 4. Position count limit
    max_positions = getattr(config, "max_open_positions", 50)
    if len(open_positions) >= max_positions:
        log.info(
            f"Max positions ({max_positions}) reached",
            extra={"inst_key": inst_key[:50], "reason": "max_positions"},
        )
        return False, "max_positions"

    # 5. Re-entry cooldown
    if is_in_cooldown(
        last_exit_time=last_exit_time,
        inst_key=inst_key,
        cooldown_secs=RE_ENTRY_COOLDOWN_SECS,
    ):
        last_exit = last_exit_time.get(inst_key, 0)
        cooldown_remaining = RE_ENTRY_COOLDOWN_SECS - (time.time() - last_exit)
        log.info(
            f"Re-entry cooldown active ({cooldown_remaining:.0f}s remaining)",
            extra={"inst_key": inst_key[:50], "cooldown_remaining": cooldown_remaining},
        )
        return False, "re_entry_cooldown"

    # 6. Already have position
    if has_active_position(open_positions=open_positions, inst_key=inst_key):
        log.info(
            f"Already have position",
            extra={"inst_key": inst_key[:50]},
        )
        return False, "already_positioned"

    # 7. Minimum price
    if price < MIN_ENTRY_PRICE:
        log.info(
            f"Price below minimum (${price:.4f} < ${MIN_ENTRY_PRICE})",
            extra={"inst_key": inst_key[:50], "price": price},
        )
        return False, "min_price"

    # 8. Minimum confidence
    if confidence < 0.15:
        log.info(
            f"Confidence too low ({confidence:.2f} < 0.15)",
            extra={"inst_key": inst_key[:50], "confidence": confidence},
        )
        return False, "min_confidence"

    return True, ""


# ── Position Exit ────────────────────────────────────────────────────────


def exit_position(
    *,
    strategy,
    config,
    cache,
    log,
    instrument_id: InstrumentId,
    inst_key: str,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    exit_reason: str,
    resolve_exit_price_func: Callable,
    daily_pnl: float,
    sports_daily_pnl: float,
    update_pnl_func: Optional[Callable] = None,
) -> tuple[float, float, float]:
    """Exit a position with P&L tracking and DB update.

    Args:
        strategy: Nautilus Strategy instance.
        config: WhaleFollowerConfig.
        cache: Nautilus cache.
        log: Logger instance.
        instrument_id: Instrument ID to exit.
        inst_key: Instrument ID string.
        open_positions: The _open_positions registry (mutated).
        exited_positions: The _exited_positions dedup set (mutated).
        last_exit_time: Re-entry cooldown dict (mutated).
        exit_reason: Reason for exit.
        resolve_exit_price_func: Callback to get simulated exit price.
        daily_pnl: Current daily P&L (mutated via return).
        sports_daily_pnl: Current sports daily P&L (mutated via return).
        update_pnl_func: Optional callback to update P&L.

    Returns:
        (realized_pnl, new_daily_pnl, new_sports_daily_pnl) tuple.
    """
    # Dedup check
    if is_exited(exited_positions=exited_positions, inst_key=inst_key):
        log.debug(f"Position already exited: {inst_key[:50]}")
        return 0.0, daily_pnl, sports_daily_pnl

    # Get Nautilus position
    open_positions_list = cache.positions_open(instrument_id=instrument_id)
    if not open_positions_list or open_positions_list[0].quantity.as_double() == 0:
        return 0.0, daily_pnl, sports_daily_pnl

    pos = open_positions_list[0]
    qty = pos.quantity.as_double()

    # Get position info from registry
    pos_info = remove_position(
        open_positions=open_positions,
        exited_positions=exited_positions,
        inst_key=inst_key,
    )
    if not pos_info:
        pos_info = {}

    pos_info["inst_key"] = inst_key

    # Resolve exit price
    entry_price = pos_info.get("entry_price", 0.50) or 0.50
    entry_time = pos_info.get("entry_time", time.time()) or time.time()
    duration = time.time() - entry_time
    exit_price = resolve_exit_price_func(pos_info)

    # Calculate P&L
    side = pos_info.get("side", "BUY") or "BUY"
    if side == "BUY":
        realized_pnl = qty * (exit_price - entry_price)
    else:
        realized_pnl = qty * (entry_price - exit_price)

    realized_return = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0

    # Sanity cap
    if abs(realized_return) > MAX_SANE_RETURN:
        log.warning(
            f"SANITY CAP: return={realized_return:+.2%} > ±{MAX_SANE_RETURN:.0%}",
            extra={
                "inst_key": inst_key[:60],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": qty,
                "side": side,
            },
        )
        realized_pnl = qty * entry_price * MAX_SANE_RETURN * (1 if realized_pnl >= 0 else -1)
        realized_return = MAX_SANE_RETURN if realized_pnl >= 0 else -MAX_SANE_RETURN

    # Update DB
    trade_id = pos_info.get("trade_id", "")
    if trade_id:
        try:
            db_path = Path(__file__).parent.parent / "research" / "trades.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                UPDATE trades SET
                    exit_price = ?,
                    realized_pnl = ?,
                    realized_return = ?,
                    exit_reason = ?,
                    duration_seconds = ?
                WHERE trade_id = ?
                """,
                (exit_price, realized_pnl, realized_return, exit_reason, duration, trade_id),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"DB update failed: {e}", extra={"trade_id": trade_id})

    # Mark exited
    mark_exited(exited_positions=exited_positions, inst_key=inst_key)
    record_exit_time(last_exit_time=last_exit_time, inst_key=inst_key)

    # Update P&L
    new_daily_pnl = daily_pnl + realized_pnl
    category = (pos_info.get("category", "") or "").lower()
    new_sports_daily_pnl = sports_daily_pnl
    if category == "sports":
        new_sports_daily_pnl = sports_daily_pnl + realized_pnl

    # Close Nautilus position
    strategy.close_position(pos)

    # Log
    pnl_sign = "+" if realized_pnl >= 0 else ""
    log.info(
        f"EXIT {exit_reason}: {qty:.0f} shrs @ ${exit_price:.4f} | "
        f"PnL: ${pnl_sign}{realized_pnl:.2f} ({realized_return:+.2%}) | "
        f"held {duration:.0f}s | daily=${new_daily_pnl:+.2f}",
        extra={
            "inst_key": inst_key[:60],
            "exit_reason": exit_reason,
            "exit_price": exit_price,
            "entry_price": entry_price,
            "qty": qty,
            "realized_pnl": realized_pnl,
            "realized_return": realized_return,
            "duration": duration,
            "daily_pnl": new_daily_pnl,
        },
    )

    return realized_pnl, new_daily_pnl, new_sports_daily_pnl


# ── Emergency Functions ──────────────────────────────────────────────────


def cancel_all_open_orders(
    *,
    strategy,
    cache,
    log,
) -> int:
    """Cancel ALL pending open orders (kill switch).

    Args:
        strategy: Nautilus Strategy instance.
        cache: Nautilus cache.
        log: Logger instance.

    Returns:
        Count of orders canceled.
    """
    canceled_count = 0
    for order in cache.orders_open():
        try:
            strategy.cancel_order(order)
            canceled_count += 1
            log.info(f"Canceled order {order.client_order_id}")
        except Exception as e:
            log.error(
                f"Failed to cancel order {order.client_order_id}: {e}",
                extra={"order_id": str(order.client_order_id)},
            )
    log.info(f"KILL_SWITCH: canceled {canceled_count} open orders")
    return canceled_count


def flatten_all_positions(
    *,
    strategy,
    config,
    cache,
    log,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    resolve_exit_price_func: Callable,
    daily_pnl: float,
    sports_daily_pnl: float,
    kill_switch_func: Optional[Callable] = None,
) -> int:
    """Emergency flatten: close ALL open positions across ALL instruments.

    Args:
        strategy: Nautilus Strategy instance.
        config: WhaleFollowerConfig.
        cache: Nautilus cache.
        log: Logger instance.
        open_positions: The _open_positions registry (mutated).
        exited_positions: The _exited_positions dedup set (mutated).
        last_exit_time: Re-entry cooldown dict (mutated).
        resolve_exit_price_func: Callback for simulated exit price.
        daily_pnl: Current daily P&L.
        sports_daily_pnl: Current sports daily P&L.
        kill_switch_func: Optional callback to activate kill switch.

    Returns:
        Count of positions closed.
    """
    log.error("FLATTEN: Emergency flatten initiated — closing ALL positions")

    # Activate kill switch
    if kill_switch_func:
        kill_switch_func()

    # Cancel pending orders first
    cancel_all_open_orders(strategy=strategy, cache=cache, log=log)

    # Snapshot keys to avoid mutation during iteration
    inst_keys = list(open_positions.keys())

    closed = 0
    failed = 0

    for inst_key in inst_keys:
        try:
            inst_id = InstrumentId.from_str(inst_key)
            pnl, _, _ = exit_position(
                strategy=strategy,
                config=config,
                cache=cache,
                log=log,
                instrument_id=inst_id,
                inst_key=inst_key,
                open_positions=open_positions,
                exited_positions=exited_positions,
                last_exit_time=last_exit_time,
                exit_reason="emergency_flatten",
                resolve_exit_price_func=resolve_exit_price_func,
                daily_pnl=daily_pnl,
                sports_daily_pnl=sports_daily_pnl,
            )
            closed += 1
            log.info(f"FLATTEN: Closed position {inst_key[:60]}")
        except Exception as e:
            log.error(
                f"FLATTEN: Failed to close {inst_key[:60]}: {e}",
                extra={"inst_key": inst_key[:80], "error": str(e)},
            )
            failed += 1

    log.error(f"FLATTEN: Complete — closed={closed}, failed={failed}")
    return closed


def exit_all_positions(
    *,
    strategy,
    config,
    cache,
    log,
    instrument_ids: List[InstrumentId],
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    resolve_exit_price_func: Callable,
    daily_pnl: float,
    sports_daily_pnl: float,
) -> int:
    """Close ALL positions in pre-subscribed instruments (non-emergency).

    Note: Only iterates config.instrument_ids. Use flatten_all_positions
    for full coverage including dynamic instruments.

    Args:
        strategy: Nautilus Strategy instance.
        config: WhaleFollowerConfig.
        cache: Nautilus cache.
        log: Logger instance.
        instrument_ids: Pre-subscribed instrument IDs.
        open_positions: The _open_positions registry.
        exited_positions: The _exited_positions dedup set.
        last_exit_time: Re-entry cooldown dict.
        resolve_exit_price_func: Callback for exit price.
        daily_pnl: Current daily P&L.
        sports_daily_pnl: Current sports daily P&L.

    Returns:
        Count of positions closed.
    """
    closed = 0
    for inst_id in instrument_ids:
        inst_key = str(inst_id)
        open_positions_list = cache.positions_open(instrument_id=inst_id)
        if open_positions_list and open_positions_list[0].quantity.as_double() != 0:
            exit_position(
                strategy=strategy,
                config=config,
                cache=cache,
                log=log,
                instrument_id=inst_id,
                inst_key=inst_key,
                open_positions=open_positions,
                exited_positions=exited_positions,
                last_exit_time=last_exit_time,
                exit_reason="exit_all",
                resolve_exit_price_func=resolve_exit_price_func,
                daily_pnl=daily_pnl,
                sports_daily_pnl=sports_daily_pnl,
            )
            closed += 1
    return closed