"""Whale Follower — Position entry logic.

Standalone function for entering positions with all pre-flight checks:
Kelly sizing, re-entry cooldown, position dedup, exposure caps,
order creation, and position tracking.

The caller (strategy) is responsible for calling submit_order() on the
created order.  The order is stored on ``config`` as ``_pending_order``
so the caller can retrieve it after a successful return.
"""

from __future__ import annotations

import time
import logging
from decimal import Decimal

from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.objects import Currency

from strategies.wf_constants import (
    RE_ENTRY_COOLDOWN_SECS,
    LOW_CASH_ALERT_PCT,
)
from strategies.wf_kelly import kelly_size, adjust_size_for_liquidity
from strategies.wf_position_checks import (
    check_position_limits,
    trigger_kill_switch,
)


def enter_position(
    *,
    config,
    cache,
    portfolio,
    order_factory,
    log_func,
    side: OrderSide,
    price: float,
    whale_amount: float,
    instrument_id,
    whale_win_rate,
    whale_name,
    market_title,
    market_category,
    whale_address,
    edge_score,
    confidence,
    entry_reason,
    open_positions: dict,
    exited_positions: set,
    last_exit_time: dict,
    whale_tiering,
    clob_client,
    # Phase 1 validation parameters
    run_id: str = "",
    mode: str = "paper",
    cancel_orders_func=None,
) -> bool:
    """Enter a Kelly-sized position.

    All position-sizing logic (Kelly, liquidity adjustment, exposure caps,
    cash checks) is delegated to helpers.  On success the order is created
    and stored on *config* as ``_pending_order``; whale metadata is
    registered in ``config._pending_whales`` for later fill correlation.

    The caller must invoke ``submit_order(config._pending_order)`` after
    this function returns ``True``.

    Parameters
    ----------
    config : WhaleFollowerConfig
        Strategy configuration dataclass.
    cache : nautilus_trader.common.cache.Cache
        Nautilus cache for instrument/position lookups.
    portfolio : nautilus_trader.portfolio.portfolio.Portfolio
        Nautilus portfolio for account queries.
    order_factory : nautilus_trader.execution.messages.OrderFactory
        Nautilus order factory for creating market orders.
    log_func : callable
        Logging callable (e.g. ``self.log.info``).  Must also support
        ``.warning()`` for high-exposure / low-cash alerts.
    side : OrderSide
        BUY or SELL.
    price : float
        Target entry price (market probability).
    whale_amount : float
        Whale's notional size in USD (informational, logged).
    instrument_id : InstrumentId
        Target instrument to trade.
    whale_win_rate : float | None
        Whale's historical win rate for dynamic Kelly, or None for 55%.
    whale_name : str
        Whale identifier (stored in pending metadata).
    market_title : str
        Human-readable market title.
    market_category : str
        Market category string.
    whale_address : str
        On-chain wallet address.
    edge_score : float
        Calibrated edge score (0.0–1.0).
    confidence : float
        Signal confidence (0–1).
    entry_reason : str
        Reason for entering the position.
    open_positions : dict
        Mutable dict of inst_key → position info (position registry).
    exited_positions : set
        Mutable set of exited inst_keys (dedup cache).
    last_exit_time : dict
        Mutable dict of inst_key → epoch timestamp (re-entry cooldown).
    whale_tiering : WhaleTiering | None
        WhaleTiering instance for edge-based sizing, or None.
    clob_client : ClobClient
        py_clob_client ClobClient (passed through; reserved for future use).

    Returns
    -------
    bool
        ``True`` if position was entered (order created and stored on config),
        ``False`` if skipped for any reason.
    """

    inst_id = instrument_id
    inst_key = str(inst_id)

    # ── Instrument validation ─────────────────────────────────────────────
    instrument = cache.instrument(inst_id)
    if instrument is None:
        log_func(f"Instrument not found in cache: {inst_id}")
        return False

    # ── Position dedup: cache (pre-subscribed instruments) ────────────────
    open_pos_list = cache.positions_open(instrument_id=inst_id)
    if open_pos_list and open_pos_list[0].quantity.as_double() != 0:
        log_func(f"Already have position in {inst_id}, skipping")
        return False

    # ── Position dedup: internal registry (covers dynamic instruments) ────
    if inst_key in open_positions:
        existing = open_positions[inst_key]
        log_func(
            f"Position already tracked: {existing['whale_name']} | "
            f"{inst_key[:50]}... | held {time.time() - existing['entry_time']:.0f}s, skipping"
        )
        return False

    # ── Re-entry cooldown ─────────────────────────────────────────────────
    last_exit = last_exit_time.get(inst_key, 0)
    if time.time() - last_exit < RE_ENTRY_COOLDOWN_SECS:
        log_func(
            f"Re-entry cooldown for {inst_id}: "
            f"{time.time() - last_exit:.0f}s < {RE_ENTRY_COOLDOWN_SECS}s, skipping"
        )
        return False

    # ── Hard balance guard ────────────────────────────────────────────────
    USDC_e = Currency.from_str("USDC.e")
    if instrument.venue:
        account = portfolio.account(instrument.venue)
    else:
        account = portfolio.account()
    if account is None:
        log_func.warning("Cash account not found – skipping order")
        return False
    available = account.balance_free(USDC_e).as_double()

    # ── Kelly sizing (uses available_balance for effective bankroll) ──────
    size_usd = kelly_size(
        bankroll=config.bankroll,
        kelly_fraction=config.kelly_fraction,
        max_position_pct=config.max_position_pct,
        price=price,
        whale_win_rate=whale_win_rate,
        edge_score=edge_score,
        available_balance=available,
        market_category=market_category,
        max_single_position_pct=config.max_single_position_pct,  # hard cap
        whale_tiering=whale_tiering,
    )
    if size_usd <= 0:
        wr_note = f" (whale_wr={whale_win_rate:.0%})" if whale_win_rate else " (fixed_wr=55%)"
        log_func(f"No Kelly edge{wr_note}, skipping")
        return False

    # ── Liquidity-based size adjustment ───────────────────────────────────
    size_usd = adjust_size_for_liquidity(size_usd, inst_key, log_func)

    # ── Size vs available ─────────────────────────────────────────────────
    if size_usd > available:
        log_func(
            f"Size ${size_usd:,.2f} exceeds available ${available:,.2f}, skipping"
        )
        return False

    # ── Phase 1 Risk Control: Position/Exposure Limits ─────────────────────
    # Check MAX_SINGLE_POSITION, MAX_TOTAL_EXPOSURE, MAX_MARKET_EXPOSURE
    allowed, reason = check_position_limits(
        config=config,
        cache=cache,
        instrument_id=inst_id,
        proposed_size_usd=size_usd,
        open_positions=open_positions,
        log=log_func,
        run_id=run_id,
        mode=mode,
    )
    if not allowed:
        # Position limits breached - trigger kill switch
        trigger_kill_switch(
            config=config,
            cache=cache,
            log=log_func,
            reason=reason,
            run_id=run_id,
            mode=mode,
            cancel_orders_func=cancel_orders_func,
        )
        return False

    # ── Max open positions ────────────────────────────────────────────────
    open_count = sum(
        1 for iid in config.instrument_ids
        if cache.positions_open(instrument_id=iid)
        and cache.positions_open(instrument_id=iid)[0].quantity.as_double() != 0
    )
    if open_count >= config.max_open_positions:
        log_func(
            f"Max positions reached ({open_count}/{config.max_open_positions}), skipping"
        )
        return False

    # ── Low-cash alert ────────────────────────────────────────────────────
    if available < LOW_CASH_ALERT_PCT * config.bankroll:
        log_func.warning(
            f"Low cash alert: free USDC.e ${available:,.2f} < "
            f"{LOW_CASH_ALERT_PCT:.0%} of bankroll (${config.bankroll:,.2f})"
        )

    # ── Build quantity ────────────────────────────────────────────────────
    qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)
    if qty.as_decimal() <= 0:
        log_func("Calculated quantity is zero, skipping order entry")
        return False

    # ── Create order ──────────────────────────────────────────────────────
    order = order_factory.market(
        instrument_id=inst_id,
        order_side=side,
        quantity=qty,
        time_in_force=TimeInForce.GTC,
    )

    # ── Whale name warning ────────────────────────────────────────────────
    if not whale_name:
        logging.getLogger("whale_follower").warning(
            f"enter_position called with empty whale_name for {market_title[:40]} "
            f"(inst={inst_key[:50]}...) - trade will be stored as 'unknown'"
        )

    # ── Store pending whale metadata (keyed by client_order_id) ───────────
    if whale_name:
        if not hasattr(config, "_pending_whales"):
            config._pending_whales = {}
        config._pending_whales[str(order.client_order_id)] = {
            "whale_name": whale_name,
            "market_title": market_title,
            "category": market_category,
            "whale_address": whale_address,
            "edge_score": edge_score,
            "confidence": confidence,
            "entry_reason": entry_reason,
            "kelly_fraction": config.kelly_fraction,
            "entry_price": price,
        }

    # ── Register intended fill price for PaperExecClient ──────────────────
    try:
        from components.paper_execution import set_fill_price
        set_fill_price(str(inst_id), price)
    except ImportError:
        pass

    # ── Log ───────────────────────────────────────────────────────────────
    whale_note = f" (following ${whale_amount:,.0f} whale)" if whale_amount else ""
    log_func(
        f"ENTER {side.name}: {qty.as_decimal():.0f} shares @ {price:.4f} "
        f"= ${size_usd:,.2f}{whale_note} | {inst_id}"
    )

    # ── Store order on config for caller to submit ────────────────────────
    config._pending_order = order

    return True


# ── Helpers ──────────────────────────────────────────────────────────────────

def _current_gross_exposure(cache, instrument_ids: list) -> float:
    """Calculate total notional exposure of all open positions as max loss amount."""
    total = 0.0
    for inst_id in instrument_ids:
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
    return total
