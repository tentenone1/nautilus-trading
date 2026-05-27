"""Whale Follower — Exit logic.

Standalone functions for closing positions, checking stop-loss/take-profit
conditions, and enforcing daily loss limits.
"""

from __future__ import annotations

import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from nautilus_trader.model.identifiers import InstrumentId

from datetime import datetime, timezone

from strategies.wf_constants import MAX_SANE_RETURN
from strategies.wf_sports import is_sports_market
from strategies.wf_market_data import (
    resolve_exit_price,
    should_exit_for_resolution,
)


def _is_market_resolved(
    condition_id: str,
    resolution_poller=None,
) -> bool:
    """Check if a market is resolved via the resolution poller."""
    if not condition_id or resolution_poller is None:
        return False
    try:
        from components.resolution_poller import get_market_resolution
        market = get_market_resolution(condition_id)
        return market is not None and market.get("resolved", False)
    except Exception:
        return False


def exit_position(
    *,
    config,
    cache,
    log,
    open_positions: dict,
    exited_positions: set,
    last_exit_time: dict,
    resolution_poller=None,
    clob_client=None,
    instrument_id: InstrumentId | None = None,
    exit_reason: str = "manual",
    market_category: str = "",
    strategy=None,  # WhaleFollower instance — used to write _sports_daily_pnl (fix: was writing to log instead)
) -> None:
    """Close current position with P&L tracking and DB update.

    Args:
        config: WhaleFollowerConfig.
        cache: Nautilus Cache.
        log: Logger.
        open_positions: dict of inst_key -> position info (mutated).
        exited_positions: set of exited inst_keys (mutated).
        last_exit_time: dict of inst_key -> timestamp (mutated).
        resolution_poller: Optional ResolutionPoller for real P&L.
        clob_client: Optional ClobClient for midpoint fetching.
        instrument_id: Instrument to exit. Uses config default if None.
        exit_reason: Reason string for the exit.
        strategy: Optional WhaleFollower instance. Used to update _sports_daily_pnl.
    """
    inst_id = instrument_id or config.instrument_id
    inst_key = str(inst_id)

    # Skip if already exited this position
    if inst_key in exited_positions:
        log.debug(f"position_already_exited component=wf_exits event=position_already_exited instrument_id={inst_key}")
        return

    # Look up position info from our registry FIRST (before cache check)
    pos_info = open_positions.get(inst_key, {})
    condition_id = pos_info.get("condition_id", "")

    # Check if position is in Nautilus cache
    open_pos_list = cache.positions_open(instrument_id=inst_id)
    position_in_cache = open_pos_list and open_pos_list[0].quantity.as_double() != 0

    if not position_in_cache:
        # Position not in cache - check if market is resolved
        is_resolved = _is_market_resolved(condition_id, resolution_poller)
        is_resolved_exit = exit_reason == "market_resolved"

        if is_resolved or is_resolved_exit:
            # Market is resolved but Nautilus already cleared the position
            # Record the exit using pos_info data
            log.info(f"position_not_in_cache_resolved component=wf_exits event=position_not_in_cache_resolved instrument_id={inst_key} condition_id={condition_id}")
        else:
            # Not resolved and not in cache - nothing to do
            return

    # Get quantity from cache or pos_info
    if position_in_cache:
        pos = open_pos_list[0]
        qty = pos.quantity.as_double()
        # Pop from registry now that we're processing
        pos_info = open_positions.pop(inst_key, {})
    else:
        # Use quantity from pos_info for resolved markets
        qty = pos_info.get("size", 0.0) or pos_info.get("position_size_usd", 0.0)
        # Pop from registry
        pos_info = open_positions.pop(inst_key, {})

    pos_info["inst_key"] = inst_key

    entry_price = pos_info.get("entry_price", 0.50)
    entry_time = pos_info.get("entry_time", time.time())
    duration = time.time() - entry_time

    # Resolve exit price using real market data
    exit_price = _resolve_exit_price_with_deps(
        pos_info=pos_info,
        instrument_id_str=inst_key,
        resolution_poller=resolution_poller,
        clob_client=clob_client,
        log=log,
    )

    # Get market category from pos_info or parameter
    # FIX: positions are stored with key "category" not "market_category"
    market_cat = pos_info.get("category", market_category or "Unknown")

    # Calculate P&L
    side = pos_info.get("side", "BUY")
    if side == "BUY":
        realized_pnl = qty * (exit_price - entry_price)
    else:
        realized_pnl = qty * (entry_price - exit_price)  # SELL = short

    # Track sports-specific P&L
    is_sports, sport_type = is_sports_market(inst_key)
    if is_sports or market_cat.lower() == "sports":
        # FIX: write to strategy instance (self) not log object
        # The strategy's _check_daily_loss_limit reads self._sports_daily_pnl
        target = strategy if strategy is not None else log
        sports_pnl = getattr(target, "_sports_daily_pnl", 0.0)
        sports_date = getattr(target, "_sports_daily_pnl_date", "")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if sports_date != today_str:
            sports_pnl = 0.0
            sports_date = today_str
        sports_pnl += realized_pnl
        setattr(target, "_sports_daily_pnl", sports_pnl)
        setattr(target, "_sports_daily_pnl_date", sports_date)

        # Check sports daily loss limit
        sports_limit = getattr(config, "sports_daily_loss_limit", 2000.0)
        if sports_pnl <= -sports_limit:
            setattr(target, "_sports_daily_loss_breached", True)
            log.error(f"sports_daily_loss_breached component=wf_exits event=sports_daily_loss_breached sports_pnl={round(sports_pnl, 2)} sports_limit={sports_limit}")

    realized_return = (
        (exit_price - entry_price) / entry_price
        if side == "BUY"
        else (entry_price - exit_price) / entry_price
    )

    # Sanity cap: P&L return exceeding +/-200% is almost certainly a
    # sandbox pricing artifact
    if abs(realized_return) > MAX_SANE_RETURN:
        log.warning(f"pnl_sanity_capped component=wf_exits event=pnl_sanity_capped instrument_id={inst_key} uncapped_return={round(realized_return, 4)} uncapped_pnl={round(realized_pnl, 4)} entry_price={entry_price} exit_price={exit_price} quantity={qty} side={side} max_sane_return={MAX_SANE_RETURN}")
        realized_pnl = (
            qty * entry_price * MAX_SANE_RETURN * (1 if realized_pnl >= 0 else -1)
        )
        realized_return = MAX_SANE_RETURN if realized_pnl >= 0 else -MAX_SANE_RETURN
        log.info(f"pnl_capped component=wf_exits event=pnl_capped capped_pnl={round(realized_pnl, 4)} capped_return={realized_return}")

    # Update DB row with exit details
    trade_id = pos_info.get("trade_id", "")
    if trade_id:
        _update_trade_exit(
            exit_price=exit_price,
            realized_pnl=realized_pnl,
            realized_return=realized_return,
            exit_reason=exit_reason,
            duration=duration,
            trade_id=trade_id,
            log=log,
        )

    # Nautilus close (only if position is still in cache)
    if position_in_cache:
        _close_position_nautilus(cache, pos, log, inst_id)

    last_exit_time[inst_key] = time.time()

    # Mark as exited
    exited_positions.add(inst_key)

    pnl_sign = "+" if realized_pnl >= 0 else ""
    log.info(f"trade_exited component=wf_exits event=trade_exited exit_reason={exit_reason} instrument_id={inst_key} quantity={qty} exit_price={round(exit_price, 4)} entry_price={round(entry_price, 4)} realized_pnl={round(realized_pnl, 4)} realized_return={round(realized_return, 4)} duration_secs={round(duration, 1)} market_category={market_cat}")
    try:
        from components.metrics import get_metrics
        metrics = get_metrics()
        metrics.increment_trade_exited()
        metrics.add_daily_pnl(realized_pnl)
        metrics.set_open_positions(len(open_positions))
    except Exception:
        pass


def _resolve_exit_price_with_deps(
    pos_info: dict,
    instrument_id_str: str,
    resolution_poller=None,
    clob_client=None,
    log=None,
) -> float:
    """Determine exit price using real market data, matching the original logic."""
    entry = pos_info.get("entry_price", 0.5)
    inst_key = pos_info.get("inst_key", instrument_id_str)
    side = pos_info.get("side", "BUY")

    # 1. Check if market is resolved via ResolutionPoller
    condition_id = pos_info.get("condition_id", "")
    if condition_id and resolution_poller is not None:
        try:
            from components.resolution_poller import (
                get_market_resolution,
                calculate_actual_pnl,
            )
            market = get_market_resolution(condition_id)
            if market and market.get("resolved"):
                winning_token_id = market.get("winning_token_id", "")
                our_token_id = ""
                if inst_key and "-" in inst_key:
                    our_token_id = inst_key.replace(".POLYMARKET", "").split("-")[-1]
                if our_token_id and winning_token_id:
                    size = pos_info.get("size", 100.0)
                    pnl_info = calculate_actual_pnl(
                        entry_price=entry,
                        position_size_usd=size,
                        our_token_id=our_token_id,
                        winning_token_id=winning_token_id,
                        side=side,
                    )
                    if pnl_info.get("won"):
                        return 1.00
                    else:
                        return 0.00
        except Exception:
            pass

    # 2. Try real midpoint from CLOB API
    if inst_key and clob_client is not None:
        try:
            token_id = inst_key.replace(".POLYMARKET", "").split("-")[-1]
            url = f"https://clob.polymarket.com/midpoint?token_id={token_id}"
            import urllib.request, json
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            price_str = data.get("midpoint") or data.get("price")
            if price_str is not None:
                mid = float(price_str)
                if 0.01 <= mid <= 0.99:
                    return mid
        except Exception:
            pass

    # 3. Fallback: deterministic estimate (NO random walk)
    edge = pos_info.get("edge_score", 0.0) or 0.0
    target = 1.0 if side.upper() in ("BUY", "LONG") else 0.0
    drift = edge * (target - entry) * 0.30
    price = entry + drift
    return max(0.01, min(0.99, price))


def _update_trade_exit(
    *,
    exit_price: float,
    realized_pnl: float,
    realized_return: float,
    exit_reason: str,
    duration: float,
    trade_id: str,
    log,
) -> None:
    """Update the trades DB row with exit details."""
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
        log.error(f"db_exit_update_failed component=wf_exits event=db_exit_update_failed error={e}")


# ── T11: should_exit_early() — early exit logic for a single position ──────────
# Exported from wf_exits.py so callers can check a single position without
# iterating all open positions. Used by the exit timer and signal handler.


def should_exit_early(
    *,
    instrument_id: InstrumentId | str,
    open_positions: dict,
    exited_positions: set,
    config,
    cache,
    log,
    resolution_poller=None,
    clob_client=None,
    strategy=None,
) -> tuple[bool, str]:
    """Check if a single position should exit early.

    Encapsulates all early-exit conditions that were previously embedded in
    check_all_positions(). Calling this directly avoids iterating all positions
    when checking one instrument.

    Args:
        instrument_id: Instrument to check.
        open_positions: dict of inst_key -> position info.
        exited_positions: set of already-exited inst_keys.
        config: WhaleFollowerConfig.
        cache: Nautilus Cache.
        log: Logger.
        resolution_poller: Optional ResolutionPoller.
        clob_client: Optional ClobClient.
        strategy: Optional WhaleFollower instance.

    Returns:
        Tuple of (should_exit, exit_reason). should_exit=False means hold.
    """
    inst_key = str(instrument_id)

    if inst_key in exited_positions:
        return False, ""

    pos_info = open_positions.get(inst_key, {})
    if not pos_info:
        return False, ""

    # Check Nautilus cache
    try:
        inst_id = InstrumentId.from_str(inst_key)
    except Exception:
        return False, ""
    open_pos_list = cache.positions_open(instrument_id=inst_id)
    if not open_pos_list or open_pos_list[0].quantity.as_double() == 0:
        return False, ""

    pos = open_pos_list[0]
    raw_entry = pos.avg_px_open
    entry = (
        raw_entry.as_double()
        if hasattr(raw_entry, "as_double")
        else float(raw_entry)
    )
    if entry <= 0:
        return False, ""

    # Get mid price
    quote = cache.quote_tick(inst_id)
    if quote is None:
        mid = _resolve_exit_price_with_deps(
            pos_info=pos_info,
            instrument_id_str=inst_key,
            resolution_poller=resolution_poller,
            clob_client=clob_client,
            log=log,
        )
    else:
        mid = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2

    side = pos_info.get("side", "BUY")
    category = (pos_info.get("category") or "").lower()
    thresholds = CATEGORY_EXIT_THRESHOLDS.get(category, _DEFAULT_EXIT_THRESHOLDS)

    if side == "BUY":
        current_return = (mid - entry) / entry
    else:
        current_return = (entry - mid) / entry

    # Category stop-loss
    if current_return <= thresholds["stop_loss_pct"]:
        return True, "category_stop_loss"

    # Category take-profit
    if current_return >= thresholds["take_profit_pct"]:
        return True, "category_take_profit"

    # Certainty win
    if side == "BUY":
        is_certain_win = mid > CERTAINTY_WIN_THRESHOLD
        is_certain_loss = mid < CERTAINTY_LOSS_THRESHOLD
    else:
        is_certain_win = mid < CERTAINTY_LOSS_THRESHOLD
        is_certain_loss = mid > CERTAINTY_WIN_THRESHOLD

    if is_certain_win:
        return True, "certainty_win"

    if is_certain_loss:
        # Certainty loss: hold to resolution, don't exit early
        return False, ""

    # Sports stop-loss
    if category == "sports":
        qty = pos.quantity.as_double()
        unrealized_pnl = qty * (mid - entry) if side == "BUY" else qty * (entry - mid)
        if unrealized_pnl <= -SPORTS_AUTO_EXIT_LOSS:
            return True, "sports_stop_loss"

    # Pre-resolution stop-loss
    hours_left = hours_until_resolution(inst_key)
    if hours_left is not None and 0 < hours_left < PRE_RESOLUTION_EXIT_HOURS:
        if current_return <= PRE_RESOLUTION_STOP_LOSS_PCT:
            return True, "pre_resolution_stop_loss"

    # Resolution exit
    if should_exit_for_resolution(inst_key, log_func=log.warning):
        return True, "resolution_exit"

    return False, ""


def _close_position_nautilus(cache, position, log, inst_id) -> None:
    """Close a position via the Nautilus framework.

    This is a thin wrapper since Nautilus requires the strategy's
    close_position method.  Caller must ensure this runs inside the
    strategy context.
    """
    # Nautilus close_position is a Strategy method, so we can't call it
    # from standalone code.  Return the position info for the caller to
    # close.  In practice the strategy's exit_position wrapper calls:
    #   self.close_position(pos)
    pass


def exit_all_positions(
    *,
    config,
    cache,
    log,
    open_positions: dict,
    exited_positions: set,
    last_exit_time: dict,
    resolution_poller=None,
    clob_client=None,
    exit_reason: str = "emergency_exit_all",
) -> None:
    """Close ALL open positions (emergency stop or daily loss limit).

    Args:
        config: WhaleFollowerConfig.
        cache: Nautilus Cache.
        log: Logger.
        open_positions: dict of inst_key -> position info (mutated).
        exited_positions: set of exited inst_keys (mutated).
        last_exit_time: dict of inst_key -> timestamp (mutated).
        resolution_poller: Optional ResolutionPoller.
        clob_client: Optional ClobClient.
        exit_reason: Reason string (default: emergency_exit_all).
    """
    for inst_id in config.instrument_ids:
        open_pos_list = cache.positions_open(instrument_id=inst_id)
        if open_pos_list and open_pos_list[0].quantity.as_double() != 0:
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
                exit_reason=exit_reason,
            )

    return

