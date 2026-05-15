"""Whale Follower — Position checking and daily loss limit.

Standalone functions extracted from wf_exits.py for modularity.
All state passed as explicit parameters.

Phase 1 risk control: position/exposure limits and kill switch mechanism.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from nautilus_trader.model.identifiers import InstrumentId

from strategies.wf_constants import (
    CERTAINTY_LOSS_THRESHOLD,
    CERTAINTY_WIN_THRESHOLD,
    MAX_SANE_RETURN,
    SPORTS_AUTO_EXIT_LOSS,
    MAX_SINGLE_POSITION_PCT,
    MAX_TOTAL_EXPOSURE_PCT,
    MAX_MARKET_EXPOSURE_PCT,
    VALIDATION_CAPITAL_BASE,
    RESOLUTION_EXIT_HOURS,
)
from strategies.wf_market_data import should_exit_for_resolution

# ── Category-based exit thresholds ─────────────────────────────────────────────
# Each category defines entry-to-exit percentage triggers.
# Positive = take profit at that gain; Negative = cut loss at that drawdown.
# Used in check_all_positions Phase 2 to fix the 'win small, lose big' pattern.
CATEGORY_EXIT_THRESHOLDS = {
    "crypto": {"take_profit_pct": 0.50, "stop_loss_pct": -0.30},
    "sports": {"take_profit_pct": 0.80, "stop_loss_pct": -0.25},
    "general": {"take_profit_pct": 0.50, "stop_loss_pct": -0.40},
    "politics": {"take_profit_pct": 0.50, "stop_loss_pct": -0.50},
    "geopolitics": {"take_profit_pct": 0.50, "stop_loss_pct": -0.50},
}
_DEFAULT_EXIT_THRESHOLDS = {"take_profit_pct": 0.50, "stop_loss_pct": -0.50}

# ── Phase 1 Validation Integration ──────────────────────────────────────────────
# Import validation modules with backward compatibility (graceful degradation)
try:
    from components.validation.event_logger import EventType, log_event

    _validation_available = True
except ImportError:
    _validation_available = False
    EventType = None
    log_event = None

from strategies.wf_exits import (
    _resolve_exit_price_with_deps,
    exit_all_positions,
    exit_position,
)


def check_all_positions(
    *,
    config,
    cache,
    log,
    open_positions: dict,
    exited_positions: set,
    last_exit_time: dict,
    resolution_poller=None,
    clob_client=None,
    strategy=None,  # WhaleFollower instance — passed through to exit_position for sports P&L tracking
) -> None:
    """Check exit conditions for ALL open positions.

    Phase 0: Stale position resolution check — if a position is held > max_hold/2
        AND the market is already resolved, exit immediately.
    Phase 1: Duration-based exit — close positions held past max_hold_hours.
    Phase 2: Certainty exits — exit when price > 0.95 or < 0.05.
    Phase 3: Sports stop-loss.
    Phase 4: Resolution exit — exit if market already resolved or resolving soon.

    Args:
        config: WhaleFollowerConfig.
        cache: Nautilus Cache.
        log: Logger.
        open_positions: dict of inst_key -> position info (mutated).
        exited_positions: set of exited inst_keys (mutated).
        last_exit_time: dict of inst_key -> timestamp (mutated).
        resolution_poller: Optional ResolutionPoller.
        clob_client: Optional ClobClient.
        strategy: Optional WhaleFollower instance for sports P&L tracking.
    """
    now = time.time()

    # Phase 0: Stale position resolution check — check markets that might already be resolved
    # Even if position is young, if the market already ended, we should exit
    stale_threshold = config.max_hold_hours * 3600 // 2  # half of max_hold
    stale_candidates = [
        k for k, v in open_positions.items()
        if now - v.get("entry_time", 0) > stale_threshold
    ]
    for inst_key in stale_candidates:
        if inst_key in exited_positions:
            continue
        try:
            # Check if market is already resolved — exit immediately
            if should_exit_for_resolution(inst_key, log_func=log.warning):
                inst_id = InstrumentId.from_str(inst_key)
                pos_info = open_positions.get(inst_key, {})
                log.info(
                    f"STALE RESOLUTION EXIT {inst_key[:50]}...: "
                    f"market already ended, exiting without waiting for max_hold"
                )
                exit_position(
                    config=config,
                    cache=cache,
                    log=log,
                    open_positions=open_positions,
                    exited_positions=exited_positions,
                    last_exit_time=last_exit_time,
                    resolution_poller=resolution_poller,
                    clob_client=clob_client,
                    instrument_id=inst_id,
                    exit_reason="stale_resolution",
                    market_category=pos_info.get("category", "Unknown"),
                    strategy=strategy,
                )
        except Exception as e:
            log.debug(f"Phase-0 resolution check error for {inst_key[:50]}...: {e}")

    # Phase 1: Duration-based exit
    max_hold = config.max_hold_hours
    expired = [
        k
        for k, v in open_positions.items()
        if now - v.get("entry_time", 0) > max_hold * 3600
    ]
    for inst_key in expired:
        try:
            inst_id = InstrumentId.from_str(inst_key)
            pos_info = open_positions.get(inst_key, {})
            exit_position(
                config=config,
                cache=cache,
                log=log,
                open_positions=open_positions,
                exited_positions=exited_positions,
                last_exit_time=last_exit_time,
                resolution_poller=resolution_poller,
                clob_client=clob_client,
                instrument_id=inst_id,
                exit_reason="max_hold",
                market_category=pos_info.get("category", "Unknown"),
                strategy=strategy,
            )
        except Exception as e:
            log.error(f"Error exiting expired position {inst_key[:50]}...: {e}")
            if inst_key in open_positions:
                exit_position(
                    config=config,
                    cache=cache,
                    log=log,
                    open_positions=open_positions,
                    exited_positions=exited_positions,
                    last_exit_time=last_exit_time,
                    resolution_poller=resolution_poller,
                    clob_client=clob_client,
                    instrument_id=inst_id,
                    exit_reason="error_cleanup",
                    market_category=pos_info.get("category", "Unknown"),
                    strategy=strategy,
                )

    # Phase 2: Check ALL open positions for certainty exits
    for inst_key in list(open_positions.keys()):
        try:
            try:
                inst_id = InstrumentId.from_str(inst_key)
            except Exception as parse_err:
                log.error(
                    f"Failed to parse instrument ID '{inst_key[:50]}...': {parse_err}"
                )
                continue

            open_pos_list = cache.positions_open(instrument_id=inst_id)
            if not open_pos_list or open_pos_list[0].quantity.as_double() == 0:
                # Clean up stale orphan positions not in Nautilus cache
                pos_info = open_positions.get(inst_key, {})
                et = pos_info.get("entry_time")
                if et is None or et <= 0:
                    stale_age = float("inf")
                else:
                    stale_age = now - et
                if stale_age > max_hold * 3600:
                    log.info(
                        f"CLEANUP stale orphan {inst_key[:50]}...: "
                        f"age={stale_age / 3600:.1f}h > max_hold={max_hold}h, "
                        f"not in Nautilus cache"
                    )
                    exit_position(
                        config=config,
                        cache=cache,
                        log=log,
                        open_positions=open_positions,
                        exited_positions=exited_positions,
                        last_exit_time=last_exit_time,
                        resolution_poller=resolution_poller,
                        clob_client=clob_client,
                        instrument_id=inst_id,
                        exit_reason="stale_orphan_cleanup",
                        market_category=pos_info.get("category", "Unknown"),
                        strategy=strategy,
                    )
                continue

            pos = open_pos_list[0]
            raw_entry = pos.avg_px_open
            entry = (
                raw_entry.as_double()
                if hasattr(raw_entry, "as_double")
                else float(raw_entry)
            )
            if entry <= 0:
                continue

            pos_info = open_positions.get(inst_key, {})
            quote = cache.quote_tick(inst_id)
            if quote is None:
                if pos_info:
                    mid = _resolve_exit_price_with_deps(
                        pos_info=pos_info,
                        instrument_id_str=inst_key,
                        resolution_poller=resolution_poller,
                        clob_client=clob_client,
                        log=log,
                    )
                    log.info(
                        f"SIMULATED PRICE for {inst_id}: {mid:.4f} (no quote ticks)"
                    )
                else:
                    continue
            else:
                mid = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2

            position_edge = pos_info.get("edge_score", 0.0) or 0.0
            side = pos_info.get("side", "BUY")

            # ── Category-based stop-loss / take-profit ──
            # Computes current return as percentage change from entry price.
            # Exits immediately if category thresholds are breached.
            # This fixes the 'win small, lose big' pattern by preventing
            # catastrophic losses before they compound.
            category = (pos_info.get("category") or "").lower()
            thresholds = CATEGORY_EXIT_THRESHOLDS.get(category, _DEFAULT_EXIT_THRESHOLDS)
            if side == "BUY":
                current_return = (mid - entry) / entry
            else:
                current_return = (entry - mid) / entry

            if current_return <= thresholds["stop_loss_pct"]:
                log.info(
                    f"CATEGORY STOP-LOSS {inst_id}: category={category}, "
                    f"return={current_return:+.2%}, threshold={thresholds['stop_loss_pct']:+.0%}, "
                    f"entry={entry:.4f}, mid={mid:.4f}, "
                    f"condition_id={pos_info.get('condition_id', '?')[:20]}..."
                )
                exit_position(
                    config=config,
                    cache=cache,
                    log=log,
                    open_positions=open_positions,
                    exited_positions=exited_positions,
                    last_exit_time=last_exit_time,
                    resolution_poller=resolution_poller,
                    clob_client=clob_client,
                    instrument_id=inst_id,
                    exit_reason="category_stop_loss",
                    market_category=category,
                    strategy=strategy,
                )
                continue
            elif current_return >= thresholds["take_profit_pct"]:
                log.info(
                    f"CATEGORY TAKE-PROFIT {inst_id}: category={category}, "
                    f"return={current_return:+.2%}, threshold={thresholds['take_profit_pct']:+.0%}, "
                    f"entry={entry:.4f}, mid={mid:.4f}, "
                    f"condition_id={pos_info.get('condition_id', '?')[:20]}..."
                )
                exit_position(
                    config=config,
                    cache=cache,
                    log=log,
                    open_positions=open_positions,
                    exited_positions=exited_positions,
                    last_exit_time=last_exit_time,
                    resolution_poller=resolution_poller,
                    clob_client=clob_client,
                    instrument_id=inst_id,
                    exit_reason="category_take_profit",
                    market_category=category,
                    strategy=strategy,
                )
                continue

            if side == "BUY":
                is_certain_win = mid > CERTAINTY_WIN_THRESHOLD
                is_certain_loss = mid < CERTAINTY_LOSS_THRESHOLD
            else:
                is_certain_win = mid < CERTAINTY_LOSS_THRESHOLD
                is_certain_loss = mid > CERTAINTY_WIN_THRESHOLD

            if is_certain_win:
                log.info(
                    f"CERTAINTY EXIT (WIN) {inst_id}: mid={mid:.4f}, "
                    f"entry={entry:.4f}, edge={position_edge:.2f}, "
                    f"condition_id={pos_info.get('condition_id', '?')[:20]}..."
                )
                exit_position(
                    config=config,
                    cache=cache,
                    log=log,
                    open_positions=open_positions,
                    exited_positions=exited_positions,
                    last_exit_time=last_exit_time,
                    resolution_poller=resolution_poller,
                    clob_client=clob_client,
                    instrument_id=inst_id,
                    exit_reason="certainty_win",
                    market_category=pos_info.get("category", "Unknown"),
                    strategy=strategy,
                )
                continue
            elif is_certain_loss:
                log.info(
                    f"CERTAINTY LOSS BLOCKED (Phase A): {inst_id}: mid={mid:.4f}, "
                    f"entry={entry:.4f}, edge={position_edge:.2f}, "
                    f"condition_id={pos_info.get('condition_id', '?')[:20]}... "
                    f"holding to resolution instead"
                )
                continue

            # Phase 3: Sports stop-loss check
            # FIX: positions stored with key "category" not "market_category"
            market_category = pos_info.get("category", "Unknown")
            if market_category == "sports":
                qty = pos.quantity.as_double()
                if side == "BUY":
                    unrealized_pnl = qty * (mid - entry)
                else:
                    unrealized_pnl = qty * (entry - mid)

                if unrealized_pnl <= -SPORTS_AUTO_EXIT_LOSS:
                    log.info(
                        f"SPORTS STOP LOSS {inst_id}: mid={mid:.4f}, "
                        f"entry={entry:.4f}, qty={qty:.2f}, "
                        f"unrealized_pnl={unrealized_pnl:.2f}, "
                        f"threshold=-{SPORTS_AUTO_EXIT_LOSS}"
                    )
                    exit_position(
                        config=config,
                        cache=cache,
                        log=log,
                        open_positions=open_positions,
                        exited_positions=exited_positions,
                        last_exit_time=last_exit_time,
                        resolution_poller=resolution_poller,
                        clob_client=clob_client,
                        instrument_id=inst_id,
                        exit_reason="sports_stop_loss",
                        market_category=market_category,
                        strategy=strategy,
                    )
                    continue

            # Phase 4: Resolution exit — exit if market already resolved or resolving soon
            # This is critical: sports markets stop getting quote ticks after the event ends.
            # Without this, the quote-tick-based resolution check never fires for dormant markets.
            try:
                if should_exit_for_resolution(inst_key, log_func=log.warning):
                    log.info(
                        f"RESOLUTION EXIT {inst_id}: market resolved or resolving within "
                        f"{RESOLUTION_EXIT_HOURS}h — exiting now"
                    )
                    exit_position(
                        config=config,
                        cache=cache,
                        log=log,
                        open_positions=open_positions,
                        exited_positions=exited_positions,
                        last_exit_time=last_exit_time,
                        resolution_poller=resolution_poller,
                        clob_client=clob_client,
                        instrument_id=inst_id,
                        exit_reason="resolution_exit",
                        market_category=market_category,
                        strategy=strategy,
                    )
                    continue
            except Exception as res_err:
                log.debug(f"Resolution check error for {inst_key[:50]}...: {res_err}")

            log.info(
                f"HOLDING {inst_id}: entry={entry:.4f}, mid={mid:.4f}, "
                f"edge={position_edge:.2f} - holding to resolution"
            )
            continue

        except Exception as pos_error:
            log.error(
                f"Error checking position {inst_key[:50]}...: {pos_error} | "
                f"continuing to next position"
            )
            continue


def check_daily_loss_limit(
    *,
    config,
    log,
    daily_pnl: float,
    daily_pnl_date: str,
    daily_loss_breached: bool,
    open_positions: dict,
    exited_positions: set,
    last_exit_time: dict,
    resolution_poller=None,
    clob_client=None,
    cache=None,
) -> tuple[float, str, bool]:
    """Check if daily loss limit has been breached.

    Args:
        config: WhaleFollowerConfig (for daily_loss_limit).
        log: Logger.
        daily_pnl: Current daily P&L accumulator.
        daily_pnl_date: Date string of current daily tracking.
        daily_loss_breached: Whether limit was already breached today.
        open_positions: dict of inst_key -> position info.
        exited_positions: set of exited inst_keys.
        last_exit_time: dict of inst_key -> timestamp.
        resolution_poller: Optional ResolutionPoller.
        clob_client: Optional ClobClient.
        cache: Nautilus Cache (for exit_all_positions).

    Returns:
        Tuple of (new_daily_pnl, new_daily_pnl_date, new_daily_loss_breached).
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != daily_pnl_date:
        return 0.0, today, False

    if daily_loss_breached:
        return daily_pnl, daily_pnl_date, True

    if daily_pnl <= -config.daily_loss_limit:
        log.error(
            f"DAILY LOSS LIMIT BREACHED: ${daily_pnl:,.2f} / "
            f"-${config.daily_loss_limit:,.2f}. "
            f"Closing all positions and stopping auto-trade."
        )
        if cache is not None:
            exit_all_positions(
                config=config,
                cache=cache,
                log=log,
                open_positions=open_positions,
                exited_positions=exited_positions,
                last_exit_time=last_exit_time,
                resolution_poller=resolution_poller,
                clob_client=clob_client,
            )
        return daily_pnl, daily_pnl_date, True

    return daily_pnl, daily_pnl_date, False


# ── Phase 1 Risk Control: Position/Exposure Limits ──────────────────────────────────


def get_current_total_exposure(
    *,
    cache,
    open_positions: dict,
) -> float:
    """Calculate total notional exposure of all open positions.

    Args:
        cache: Nautilus Cache.
        open_positions: dict of inst_key -> position info.

    Returns:
        Total exposure in USD (sum of all position notional values).
    """
    total = 0.0
    for inst_key, pos_info in open_positions.items():
        try:
            inst_id = InstrumentId.from_str(inst_key)
            positions = cache.positions_open(instrument_id=inst_id)
            if positions:
                for pos in positions:
                    qty = (
                        pos.quantity.as_double()
                        if hasattr(pos.quantity, "as_double")
                        else float(pos.quantity)
                    )
                    avg_open = (
                        pos.avg_px_open.as_double()
                        if hasattr(pos.avg_px_open, "as_double")
                        else 0.0
                    )
                    total += qty * avg_open
        except Exception:
            # Fallback to stored position info
            size = pos_info.get("size", 0.0)
            entry_price = pos_info.get("entry_price", 0.0)
            total += size * entry_price
    return total


def get_market_exposure(
    *,
    cache,
    instrument_id,
    open_positions: dict,
) -> float:
    """Calculate exposure for a specific market/instrument.

    Args:
        cache: Nautilus Cache.
        instrument_id: InstrumentId to check.
        open_positions: dict of inst_key -> position info.

    Returns:
        Exposure in USD for this specific instrument.
    """
    inst_key = str(instrument_id)
    exposure = 0.0

    # Check Nautilus cache
    positions = cache.positions_open(instrument_id=instrument_id)
    if positions:
        for pos in positions:
            qty = (
                pos.quantity.as_double()
                if hasattr(pos.quantity, "as_double")
                else float(pos.quantity)
            )
            avg_open = (
                pos.avg_px_open.as_double()
                if hasattr(pos.avg_px_open, "as_double")
                else 0.0
            )
            exposure += qty * avg_open

    # Check internal registry
    if inst_key in open_positions:
        pos_info = open_positions[inst_key]
        size = pos_info.get("size", 0.0)
        entry_price = pos_info.get("entry_price", 0.0)
        exposure = max(exposure, size * entry_price)

    return exposure


def check_position_limits(
    *,
    config,
    cache,
    instrument_id,
    proposed_size_usd: float,
    open_positions: dict,
    log,
    run_id: str = "",
    mode: str = "paper",
) -> tuple[bool, str]:
    """Check all Phase 1 position/exposure limits before entering.

    Args:
        config: WhaleFollowerConfig.
        cache: Nautilus Cache.
        instrument_id: InstrumentId for proposed position.
        proposed_size_usd: Proposed position size in USD.
        open_positions: dict of inst_key -> position info.
        log: Logger.
        run_id: Validation run ID for event logging.
        mode: Execution mode (paper/live).

    Returns:
        Tuple of (allowed: bool, reason: str).
        If not allowed, reason describes which limit was breached.
    """
    # Determine capital base (use validation capital or config bankroll)
    capital = (
        config.validation_capital_base
        if config.validation_capital_base > 0
        else config.bankroll
    )

    # 1. Check MAX_SINGLE_POSITION (2% of capital)
    max_single = capital * config.max_single_position_pct
    if proposed_size_usd > max_single:
        reason = (
            f"MAX_SINGLE_POSITION breached: ${proposed_size_usd:,.2f} > "
            f"${max_single:,.2f} ({config.max_single_position_pct:.0%} of ${capital:,.0f})"
        )
        log.warning(reason)
        return False, reason

    # 2. Check MAX_TOTAL_EXPOSURE (20% of capital)
    current_total = get_current_total_exposure(
        cache=cache, open_positions=open_positions
    )
    max_total = capital * config.max_total_exposure_pct
    if current_total + proposed_size_usd > max_total:
        reason = (
            f"MAX_TOTAL_EXPOSURE breached: ${current_total:,.2f} + ${proposed_size_usd:,.2f} > "
            f"${max_total:,.2f} ({config.max_total_exposure_pct:.0%} of ${capital:,.0f})"
        )
        log.warning(reason)
        return False, reason

    # 3. Check MAX_MARKET_EXPOSURE (5% of capital per market)
    current_market = get_market_exposure(
        cache=cache, instrument_id=instrument_id, open_positions=open_positions
    )
    max_market = capital * config.max_market_exposure_pct
    if current_market + proposed_size_usd > max_market:
        reason = (
            f"MAX_MARKET_EXPOSURE breached: ${current_market:,.2f} + ${proposed_size_usd:,.2f} > "
            f"${max_market:,.2f} ({config.max_market_exposure_pct:.0%} of ${capital:,.0f}) for {instrument_id}"
        )
        log.warning(reason)
        return False, reason

    # All checks passed
    return True, ""


def trigger_kill_switch(
    *,
    config,
    cache,
    log,
    reason: str,
    run_id: str = "",
    mode: str = "paper",
    strategy_id: str = "whale_follower",
    cancel_orders_func=None,
) -> bool:
    """Trigger kill switch: stop trading, cancel orders, emit event.

    Args:
        config: WhaleFollowerConfig (will set _kill_switch_breached=True).
        cache: Nautilus Cache.
        log: Logger.
        reason: Reason for kill switch activation.
        run_id: Validation run ID for event logging.
        mode: Execution mode (paper/live).
        strategy_id: Strategy identifier.
        cancel_orders_func: Optional callable to cancel open orders.

    Returns:
        True if kill switch was triggered successfully.
    """
    # Note: _kill_switch_breached is a strategy instance attribute (self._kill_switch_breached),
    # not a config field. Callers must set it on the strategy instance after this returns.

    # Log incident
    log.error(
        f"KILL_SWITCH_TRIGGERED: {reason}. "
        f"Stopping all trading and canceling open orders."
    )

    # Cancel all open orders if function provided
    if cancel_orders_func:
        try:
            cancel_orders_func()
            log.info("All open orders canceled due to kill switch")
        except Exception as e:
            log.error(f"Failed to cancel orders: {e}")

    # Emit KILL_SWITCH_TRIGGERED event (graceful degradation)
    if _validation_available and log_event and EventType:
        try:
            log_event(
                event_type=EventType.KILL_SWITCH_TRIGGERED,
                run_id=run_id,
                mode=mode,
                strategy_id=strategy_id,
                payload={
                    "reason": reason,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            log.info("KILL_SWITCH_TRIGGERED event logged")
        except Exception as e:
            log.warning(f"Failed to emit KILL_SWITCH_TRIGGERED event: {e}")

    return True
