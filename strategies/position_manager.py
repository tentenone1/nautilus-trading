"""Position Manager — Entry, exit, and lifecycle management for whale follower positions.

Extracted from WhaleFollower to decompose the god class. Handles:
  - Position entry with risk checks and Kelly sizing
  - Position exit with P&L tracking and DB updates
  - Periodic position checking (stop-loss, take-profit, resolution, duration)
  - Kill switch position limit enforcement
  - Capital pool management (copy vs fade)

The strategy (WhaleFollower) delegates to PositionManager while retaining
the Nautilus Strategy lifecycle hooks (on_start, on_stop, on_quote_tick, etc.).
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.model.objects import Currency

from strategies.wf_constants import (
    MIN_ENTRY_PRICE,
    MAX_SANE_RETURN,
    RE_ENTRY_COOLDOWN_SECS,
    LOW_CASH_ALERT_PCT,
    RESOLUTION_EXIT_HOURS,
    LIVE_ENTRY_PRICE_CAPS,
    CERTAINTY_WIN_THRESHOLD,
    CERTAINTY_LOSS_THRESHOLD,
    SPORTS_WHITELIST_PATTERNS,
)
from strategies.wf_position_checks import check_position_limits, trigger_kill_switch
from strategies.wf_position_persistence import save_open_positions, load_daily_state, save_daily_state

# Validation integration (graceful degradation)
try:
    from components.validation.event_logger import EventType, log_event
    from components.validation.trade_context import TradeContext, get_trade_context
    from components.validation.snapshot_store import freeze_snapshot
    from components.validation.db_router import get_current_mode
    _validation_available = True
except ImportError:
    _validation_available = False
    EventType = None
    log_event = None
    TradeContext = None
    get_trade_context = None
    freeze_snapshot = None
    get_current_mode = lambda: "paper"

from strategies.wf_market_data import should_exit_for_resolution, fetch_real_midpoint, resolve_exit_price
from strategies.wf_sports import is_sports_market, get_market_event_time, should_exit_for_sports
from strategies.wf_db_ops import log_trade_to_db, update_trade_latency_fields


class PositionManager:
    """Manages position entry, exit, and lifecycle for the whale follower strategy.

    Receives a reference to the strategy for Nautilus-specific operations
    (cache, order submission, position close) and manages position state
    through the strategy's state dicts.
    """

    def __init__(self, strategy):
        """Initialize with a reference to the WhaleFollower strategy instance.

        Args:
            strategy: The WhaleFollower strategy instance. Provides access to
                cache, log, config, portfolio, order_factory, and Nautilus methods.
        """
        self._s = strategy

    # ── Properties for convenient access ────────────────────────────────────

    @property
    def cache(self):
        return self._s.cache

    @property
    def log(self):
        return self._s.log

    @property
    def config(self):
        return self._s.config

    @property
    def portfolio(self):
        return self._s.portfolio

    @property
    def order_factory(self):
        return self._s.order_factory

    @property
    def strategies(self):
        return self._s._strategies

    @property
    def open_positions(self):
        return self._s._open_positions

    @property
    def pending_whales(self):
        return self._s._pending_whales

    @property
    def exited_positions(self):
        return self._s._exited_positions

    @property
    def last_exit_time(self):
        return self._s._last_exit_time

    @property
    def fade_positions(self):
        return self._s._fade_positions

    @property
    def kill_switch_breached(self):
        return self._s._kill_switch_breached

    @property
    def daily_pnl(self):
        return self._s._daily_pnl

    @property
    def daily_pnl_date(self):
        return self._s._daily_pnl_date

    @property
    def daily_loss_limit(self):
        return self._s.config.daily_loss_limit

    @property
    def whale_tiering(self):
        return self._s._whale_tiering

    @property
    def validation_run_id(self):
        return self._s._validation_run_id

    @property
    def validation_context(self):
        return self._s._validation_context

    # ── Position Entry ──────────────────────────────────────────────────────

    def enter_position(
        self,
        side: OrderSide,
        price: float,
        whale_amount: float = 0,
        instrument_id: InstrumentId = None,
        whale_win_rate: float | None = None,
        whale_name: str = None,
        market_title: str = "",
        market_category: str = "",
        whale_address: str = "",
        edge_score: float = 0.0,
        confidence: float = 0.0,
        entry_reason: str = "",
        is_fade: bool = False,
        _validation_signal_id: str = "",
        _validation_snapshot_id: str = "",
    ) -> None:
        """Enter Kelly-sized position with risk checks and capital management."""
        s = self._s

        # Kill switch check (with auto-release)
        if s._kill_switch_breached:
            elapsed = time.time() - getattr(s, '_kill_switch_time', 0)
            if elapsed > 300:
                s.log.info(f"KILL_SWITCH auto-released after {elapsed:.0f}s, resuming trading")
                s._kill_switch_breached = False
            else:
                s.log.warning(f"KILL_SWITCH active ({elapsed:.0f}s/300s) - rejecting signal for {market_title[:40]}")
                return

        inst_id = instrument_id or s.config.instrument_id
        instrument = s.cache.instrument(inst_id)
        if instrument is None:
            return

        if price < MIN_ENTRY_PRICE:
            s.log.info(f"MIN_PRICE_REJECTED | {market_title[:50]} | price=${price:.4f} < ${MIN_ENTRY_PRICE} | whale={whale_name}")
            return

        if confidence < 0.15:
            s.log.info(f"REJECT confidence={confidence:.2f} < 0.15 | {inst_id}")
            return

        open_positions = s.cache.positions_open(instrument_id=inst_id)
        if open_positions and open_positions[0].quantity.as_double() != 0:
            s.log.info(f"Already have position in {inst_id}, skipping")
            return

        inst_key = str(inst_id)
        if inst_key in s._open_positions:
            existing = s._open_positions[inst_key]
            s.log.info(f"Position already tracked: {existing['whale_name']} | {inst_key[:50]}... | held {time.time()-existing['entry_time']:.0f}s, skipping")
            return

        last_exit = s._last_exit_time.get(str(inst_id), 0)
        if time.time() - last_exit < RE_ENTRY_COOLDOWN_SECS:
            s.log.info(f"Re-entry cooldown for {inst_id}: {time.time() - last_exit:.0f}s < {RE_ENTRY_COOLDOWN_SECS}s, skipping")
            return

        USDC_e = Currency.from_str("USDC.e")
        if instrument.venue:
            account = s.portfolio.account(instrument.venue)
        else:
            account = s.portfolio.account()
        if account is None:
            s.log.warning("Cash account not found - skipping order")
            return
        available = account.balance_free(USDC_e).as_double()

        # Kelly sizing
        size_usd = s._kelly_size(price, whale_win_rate=whale_win_rate, edge_score=edge_score, available_balance=available, market_category=market_category)
        if size_usd <= 0:
            wr_note = f" (whale_wr={whale_win_rate:.0%})" if whale_win_rate else " (fixed_wr=55%)"
            s.log.info(f"No Kelly edge{wr_note}, skipping")
            return

        strategy = s._strategies.get(market_category.lower())
        if strategy is not None and not strategy.can_accept_position():
            s.log.info(f"({market_category}) can_accept_position=False - cap reached, skipping: {whale_name} | {inst_key[:50]}...")
            return

        size_usd = s._adjust_size_for_liquidity(size_usd, inst_id)

        max_single_pct = getattr(s.config, "max_single_position_pct", 0.02)
        capital = s.config.validation_capital_base if s.config.validation_capital_base > 0 else s.config.bankroll
        hard_cap = capital * max_single_pct
        if size_usd > hard_cap:
            size_usd = hard_cap

        # Capital pool: fade positions use dedicated fade bucket
        capital_requested = False
        if strategy is not None:
            if is_fade:
                granted = strategy.request_fade_capital(size_usd)
                if granted <= 0:
                    s.log.info(f"({market_category}) FADE pool exhausted - request_fade_capital(${size_usd:.0f}) returned ${granted:.0f}, skipping: {whale_name}")
                    return
            else:
                granted = strategy.request_capital(size_usd)
                if granted <= 0:
                    s.log.info(f"({market_category}) pool exhausted - request_capital(${size_usd:.0f}) returned ${granted:.0f}, skipping: {whale_name}")
                    return
            if granted < size_usd:
                s.log.info(f"({market_category}) partial capital grant: ${granted:.0f} < desired ${size_usd:.0f}, adjusting from ${size_usd:.0f} to ${granted:.0f}")
                size_usd = granted
            capital_requested = True

        if size_usd > available:
            s.log.info(f"Size ${size_usd:,.2f} exceeds available ${available:,.2f}, skipping")
            return

        allowed, reason = check_position_limits(
            config=s.config, cache=s.cache, instrument_id=inst_id,
            proposed_size_usd=size_usd, open_positions=s._open_positions,
            log=s.log, run_id=s._validation_run_id, mode=get_current_mode(),
        )
        if not allowed:
            if capital_requested and strategy is not None:
                strategy.release_capital(0.0, size_usd, is_fade=is_fade)
            trigger_kill_switch(
                config=s.config, cache=s.cache, log=s.log, reason=reason,
                run_id=s._validation_run_id, mode=get_current_mode(),
                strategy_id="whale_follower", cancel_orders_func=s.cancel_all_open_orders,
            )
            s._kill_switch_breached = True
            s._kill_switch_time = time.time()
            return

        if strategy is None:
            open_count = len(s._open_positions)
            max_positions = s.config.max_open_positions
            if open_count >= max_positions:
                s.log.info(f"Max positions reached ({open_count}/{max_positions}), skipping")
                return

        if available < LOW_CASH_ALERT_PCT * s.config.bankroll:
            s.log.warning(f"Low cash alert: free USDC.e ${available:,.2f} < {LOW_CASH_ALERT_PCT:.0%} of bankroll (${s.config.bankroll:,.2f})")

        qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)
        if qty.as_decimal() <= 0:
            s.log.debug("Calculated quantity is zero, skipping order entry")
            return

        order = s.order_factory.market(
            instrument_id=inst_id, order_side=side, quantity=qty,
            time_in_force=TimeInForce.GTC,
        )

        if whale_name:
            pass
        else:
            import logging as _lg
            _lg.getLogger("whale_follower").warning(f"enter_position called with empty whale_name for {market_title[:40]} (inst={str(inst_id)[:50]}...) - trade will be stored as 'unknown'")

        pending_name = whale_name if whale_name else f"unknown_whale_{uuid.uuid4().hex[:8]}"
        s._pending_whales[str(order.client_order_id)] = {
            "whale_name": pending_name,
            "market_title": market_title,
            "category": market_category,
            "whale_address": whale_address,
            "edge_score": edge_score,
            "confidence": confidence,
            "entry_reason": entry_reason,
            "kelly_fraction": s.config.kelly_fraction,
            "entry_price": price,
            "is_fade": is_fade,
            "_validation_signal_id": _validation_signal_id,
            "_validation_snapshot_id": _validation_snapshot_id,
        }

        from components.paper_execution import set_fill_price
        set_fill_price(str(inst_id), price)

        whale_note = f" (following ${whale_amount:,.0f} whale)" if whale_amount else ""
        s.log.info(f"ENTER {side.name}: {qty.as_decimal():.0f} shares @ {price:.4f} = ${size_usd:,.2f}{whale_note} | {inst_id}")
        s.submit_order(order)
        s._trades_this_scan += 1

        # Validation: TRADE_SUBMITTED event
        submitted_ts = time.monotonic_ns()
        if _validation_available and log_event and EventType and _validation_signal_id:
            try:
                if s._validation_context:
                    try:
                        s._validation_context.register_submission(
                            client_order_id=str(order.client_order_id),
                            signal_id=_validation_signal_id,
                            submitted_ts=submitted_ts,
                            intended_price=float(price),
                            intended_size=float(size_usd),
                        )
                    except Exception as ctx_err:
                        s.log.warning(f"Trade context submission registration failed: {ctx_err}")

                log_event(
                    event_type=EventType.TRADE_SUBMITTED,
                    payload={
                        "signal_id": _validation_signal_id,
                        "snapshot_id": _validation_snapshot_id,
                        "client_order_id": str(order.client_order_id),
                        "whale_name": whale_name,
                        "market_title": market_title[:80],
                        "side": side.name,
                        "intended_price": float(price),
                        "intended_size_usd": float(size_usd),
                        "quantity": float(qty.as_decimal()),
                        "instrument_id": str(inst_id)[:80],
                        "ts_mono_ns": submitted_ts,
                    },
                    correlation_id=_validation_signal_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=s._validation_run_id,
                )
                s.log.debug(f"Validation: TRADE_SUBMITTED {str(order.client_order_id)[:12]}... signal={_validation_signal_id[:8]}")
            except Exception as e:
                s.log.warning(f"Validation event emission failed: {e}")

    # ── Position Exit ────────────────────────────────────────────────────────

    def exit_position(self, instrument_id: InstrumentId = None, exit_reason: str = "manual") -> None:
        """Close current position with P&L tracking and DB update."""
        s = self._s
        import sqlite3, uuid as _uuid

        inst_id = instrument_id or s.config.instrument_id
        inst_key = str(inst_id)

        if inst_key in s._exited_positions:
            s.log.debug(f"Position already exited, skipping: {inst_key[:50]}...")
            return

        open_positions = s.cache.positions_open(instrument_id=inst_id)
        if not open_positions or open_positions[0].quantity.as_double() == 0:
            return
        pos = open_positions[0]
        qty = pos.quantity.as_double()

        pos_info = s._open_positions.pop(inst_key, {})
        pos_info["inst_key"] = inst_key
        save_open_positions(s._open_positions)

        entry_price = pos_info.get("entry_price", 0.50)
        entry_time = pos_info.get("entry_time", time.time())
        duration = time.time() - entry_time
        exit_price = self._resolve_exit_price(pos_info)

        side = pos_info.get("side", "BUY")
        if side == "buy" or side == "BUY":
            realized_pnl = qty * (exit_price - entry_price)
            realized_return = (exit_price - entry_price) / entry_price if entry_price > 0 else 0.0
        else:
            realized_pnl = qty * (entry_price - exit_price)
            realized_return = (entry_price - exit_price) / entry_price if entry_price > 0 else 0.0

        if abs(realized_return) > MAX_SANE_RETURN:
            s.log.warning(
                f"[SANITY CAP] {inst_key[:50]}... return={realized_return:+.2%} exceeds +/-{MAX_SANE_RETURN:.0%} - "
                f"capping from ${realized_pnl:+.2f} to capped value. "
                f"entry=${entry_price:.4f} exit=${exit_price:.4f} qty={qty:.0f} side={side}"
            )
            realized_pnl = qty * entry_price * MAX_SANE_RETURN * (1 if realized_pnl >= 0 else -1)
            realized_return = MAX_SANE_RETURN if realized_pnl >= 0 else -MAX_SANE_RETURN
            s.log.info(f"[SANITY CAP] Capped P&L: ${realized_pnl:+.2f} ({realized_return:+.2%})")

        # DB update
        trade_id = pos_info.get("trade_id", "")
        if trade_id:
            try:
                db_path = Path(__file__).parent.parent / "research" / "trades.db"
                conn = sqlite3.connect(str(db_path))
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute("""
                    UPDATE trades SET
                        exit_price = ?, realized_pnl = ?, realized_return = ?,
                        exit_reason = ?, duration_seconds = ?
                    WHERE trade_id = ?
                """, (exit_price, realized_pnl, realized_return, exit_reason, duration, trade_id))
                conn.commit()
                try:
                    conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
                conn.close()
            except Exception as e:
                s.log.error(f"[DB] Failed to update exit P&L: {e}")

        # Validation: TRADE_CLOSED event
        closed_ts = time.monotonic_ns()
        category = pos_info.get('category', '') or ''

        if _validation_available and log_event and EventType and trade_id:
            try:
                log_event(
                    event_type=EventType.TRADE_CLOSED,
                    payload={
                        "trade_id": trade_id,
                        "whale_name": pos_info.get("whale_name", ""),
                        "market_title": (pos_info.get("market_title", "") or "")[:80],
                        "category": category,
                        "side": side,
                        "entry_price": float(entry_price),
                        "exit_price": float(exit_price),
                        "quantity": float(qty),
                        "realized_pnl": float(realized_pnl),
                        "realized_return": float(realized_return),
                        "duration_seconds": float(duration),
                        "exit_reason": exit_reason,
                        "instrument_id": inst_key[:80],
                        "ts_mono_ns": closed_ts,
                    },
                    correlation_id=trade_id,
                    mode=get_current_mode(),
                    strategy_id="whale_follower",
                    run_id=s._validation_run_id,
                )
                s.log.debug(f"Validation: TRADE_CLOSED {trade_id[:8]}... PnL=${realized_pnl:+.2f}")
            except Exception as e:
                s.log.warning(f"Validation event emission failed: {e}")

        # Nautilus close
        s.close_position(pos)
        s._last_exit_time[inst_key] = time.time()
        s._exited_positions.add(inst_key)

        # Capital release and daily P&L
        cat_lower = category.lower()
        strat = s._strategies.get(cat_lower)
        if strat is not None:
            position_size = pos_info.get("size", qty * entry_price)
            strat.release_capital(realized_pnl, position_size, is_fade=pos_info.get("is_fade", False))

        # Remove from fade positions tracking
        if inst_key in s._fade_positions:
            s._fade_positions.discard(inst_key)
            s.log.info(f"FADE position closed: {inst_key[:50]}... ({len(s._fade_positions)}/{s._fade_max_concurrent} remaining)")

        price_cap = LIVE_ENTRY_PRICE_CAPS.get(cat_lower, None)
        is_paper = (price_cap == 0.0) or (price_cap is not None and price_cap > 0.0 and entry_price > price_cap)
        if not is_paper:
            s._daily_pnl += realized_pnl
            if s._daily_pnl <= -s.config.daily_loss_limit:
                s._daily_loss_breached = True
                s.log.warning(f"Daily loss limit breached: ${s._daily_pnl:.2f} <= -${s.config.daily_loss_limit:.2f}")
        else:
            s.log.info(f"PAPER P&L excluded from daily limit: category={cat_lower} | price=${entry_price:.4f} | pnl=${realized_pnl:+.2f}")

        save_daily_state(daily_pnl=s._daily_pnl, daily_pnl_date=s._daily_pnl_date, daily_loss_breached=s._daily_loss_breached)

        pnl_sign = "+" if realized_pnl >= 0 else ""
        s.log.info(
            f"EXIT {exit_reason}: {qty:.0f} shrs @ ${exit_price:.4f} | "
            f"PnL: ${pnl_sign}{realized_pnl:.2f} ({realized_return:+.2%}) | "
            f"held {duration:.0f}s | daily_pnl=${s._daily_pnl:+.2f} | "
            f"{inst_key[:40]}..."
        )

    def exit_all_positions(self) -> None:
        """Close ALL open positions (emergency stop or daily loss limit)."""
        s = self._s
        exited = 0
        for inst_id in s.config.instrument_ids:
            open_positions = s.cache.positions_open(instrument_id=inst_id)
            if open_positions and open_positions[0].quantity.as_double() != 0:
                self.exit_position(inst_id, exit_reason="emergency_exit_all")
                exited += 1

        if hasattr(s, '_dynamic_subscriptions'):
            for inst_id_str in list(s._dynamic_subscriptions.keys()):
                inst_id = InstrumentId.from_str(inst_id_str)
                pos = s.cache.position(inst_id)
                if pos and pos.is_open:
                    self.exit_position(inst_id, exit_reason="emergency_exit_all")
                    exited += 1

        s.log.warning(f"Emergency exit complete: {exited} positions closed")

    # ── Helper Methods ───────────────────────────────────────────────────────

    def _resolve_exit_price(self, pos_info: dict) -> float:
        """Resolve exit price from position info or market data."""
        resolve_exit_price(pos_info)

    def _fetch_real_midpoint(self, inst_key: str) -> float | None:
        """Fetch real market midpoint from CLOB API."""
        fetch_real_midpoint(inst_key)

    def _current_gross_exposure(self) -> float:
        """Calculate total notional exposure of all open positions."""
        s = self._s
        total = 0.0
        for inst_id in s.config.instrument_ids:
            positions = s.cache.positions_open(instrument_id=inst_id)
            if positions:
                for pos in positions:
                    qty = pos.quantity.as_double() if hasattr(pos.quantity, 'as_double') else float(pos.quantity)
                    avg_open = pos.avg_px_open.as_double() if hasattr(pos.avg_px_open, 'as_double') else 0.0
                    total += qty * avg_open
        return total

    # ── Position Checking ────────────────────────────────────────────────────

    def check_all_positions(self) -> None:
        """Check stop-loss, take-profit, resolution, and duration exits for ALL open positions."""
        import re as _re
        s = self._s
        now = time.time()

        max_hold = s.config.max_hold_hours
        max_hold_secs = max_hold * 3600
        force_close_threshold = max_hold_secs * 2
        expired = [
            k for k, v in s._open_positions.items()
            if now - v.get("entry_time", 0) > max_hold_secs
        ]
        for inst_key in expired:
            try:
                inst_id = InstrumentId.from_str(inst_key)
                market_resolved = should_exit_for_resolution(inst_key, log_func=s.log.warning)
                age = now - s._open_positions[inst_key].get("entry_time", 0)
                significantly_exceeded = age > force_close_threshold
                if market_resolved or significantly_exceeded:
                    self.exit_position(inst_id, exit_reason="max_hold")
                else:
                    s.log.info(
                        f"HOLD {inst_key[:50]}...: age={age/3600:.1f}h > max_hold={max_hold}h "
                        f"but market not resolved and not significantly exceeded (2x), "
                        f"waiting for resolution poller"
                    )
            except Exception as e:
                s.log.error(f"Error exiting expired position {inst_key[:50]}...: {e}")
                if inst_key in s._open_positions:
                    del s._open_positions[inst_key]
                    save_open_positions(s._open_positions)

        for inst_key in list(s._open_positions.keys()):
            try:
                try:
                    inst_id = InstrumentId.from_str(inst_key)
                except Exception as parse_err:
                    s.log.error(f"Failed to parse instrument ID '{inst_key[:50]}...': {parse_err}")
                    continue

                open_positions = s.cache.positions_open(instrument_id=inst_id)
                if not open_positions or open_positions[0].quantity.as_double() == 0:
                    continue

                pos = open_positions[0]
                raw_entry = pos.avg_px_open
                entry = raw_entry.as_double() if hasattr(raw_entry, "as_double") else float(raw_entry)
                if entry <= 0:
                    continue

                pos_info = s._open_positions.get(inst_key, {})

                quote = s.cache.quote_tick(inst_id)
                if quote is None:
                    if pos_info:
                        mid = self._resolve_exit_price(pos_info)
                        s.log.info(f"SIMULATED PRICE for {inst_id}:  (no quote ticks)")
                    else:
                        continue
                else:
                    mid = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2

                position_edge = pos_info.get("edge_score", 0.0)
                side = pos_info.get("side", "BUY")
                if side == "BUY":
                    pnl_pct = (mid - entry) / entry if entry > 0 else 0.0
                else:
                    pnl_pct = (entry - mid) / entry if entry > 0 else 0.0

                market_category = pos_info.get("market_category", "") or pos_info.get("category", "")

                if mid > CERTAINTY_WIN_THRESHOLD:
                    s.log.info(
                        f"CERTAINTY WIN EXIT {inst_id}: mid={mid:.4f} > "
                        f"{CERTAINTY_WIN_THRESHOLD} | entry={entry:.4f} | "
                        f"edge={position_edge:.2f}, condition_id={pos_info.get('condition_id', '?')[:20]}..."
                    )
                    self.exit_position(inst_id, exit_reason="certainty_win")
                    continue
                elif mid < CERTAINTY_LOSS_THRESHOLD:
                    s.log.info(
                        f"CERTAINTY LOSS EXIT {inst_id}: mid={mid:.4f} < "
                        f"{CERTAINTY_LOSS_THRESHOLD} | entry={entry:.4f} | "
                        f"edge={position_edge:.2f}, condition_id={pos_info.get('condition_id', '?')[:20]}..."
                    )
                    self.exit_position(inst_id, exit_reason="certainty_loss")
                    continue
                else:
                    mc = pos_info.get("market_category", "")
                    if mc.lower() == "sports":
                        title = pos_info.get("market_title", "") or ""
                        is_whitelisted = any(
                            _re.search(p, title, _re.IGNORECASE)
                            for p in SPORTS_WHITELIST_PATTERNS
                        )
                        if is_whitelisted:
                            if should_exit_for_sports(inst_id, s.log.info):
                                s.log.info(f"SPORTS EVENT EXIT {inst_id}: Spread bet, game imminent")
                                self.exit_position(inst_id, exit_reason="sports_event")
                                continue
                        else:
                            s.log.info(
                                f"SKIP sports exit (non-whitelisted): {title[:50]} | "
                                f"entry={entry:.4f}, mid={mid:.4f}"
                            )

                    if self._should_exit_for_resolution(inst_id, pnl_pct=pnl_pct, market_category=market_category):
                        s.log.info(f"RESOLUTION EXIT {inst_id}: market resolving soon")
                        self.exit_position(inst_id, exit_reason="resolution")
                        continue

                    s.log.info(
                        f"HOLDING {inst_id}: entry={entry:.4f}, mid={mid:.4f}, "
                        f"edge={position_edge:.2f} -- holding to resolution"
                    )
            except Exception as pos_error:
                s.log.error(
                    f"Error checking position {inst_key[:50]}...: {pos_error} | "
                    f"continuing to next position"
                )
                continue

    def _should_exit_for_resolution(self, instrument_id: InstrumentId, pnl_pct: float = 0.0, market_category: str = "") -> bool:
        """Check if market resolves within RESOLUTION_EXIT_HOURS and apply P&L check."""
        import requests
        s = self._s
        try:
            cond_id = str(instrument_id).split("-")[0]
            resp = requests.get(
                f"https://data-api.polymarket.com/markets/{cond_id}",
                timeout=10,
            )
            if resp.status_code == 200:
                market = resp.json()
                end_date = market.get("end_date_iso")
                if end_date:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    if 0 < hours_left < RESOLUTION_EXIT_HOURS:
                        if pnl_pct < -0.20:
                            s.log.info(f"PRE-RESOLUTION STOP-LOSS: {cond_id[:16]}... pnl={pnl_pct:.1%}, exiting early")
                            return True
                        return True
                    if hours_left <= 0:
                        s.log.info(
                            f"Market has already ended ({abs(hours_left):.1f}h ago) -- "
                            f"{cond_id[:16]}..., exiting stale position"
                        )
                        return True
                    return False
        except Exception:
            pass
        return False

    def check_daily_loss(self) -> None:
        """Check if daily loss limit has been breached.

        Delegates to RiskManager when available, falls back to inline logic.
        Also syncs _risk_state so can_trade() stays up to date.
        """
        from strategies.wf_position_persistence import save_daily_state
        from datetime import datetime, timezone
        s = self._s
        # Sync state into RiskState
        if s._risk_state is not None:
            s._risk_state.daily_pnl = s._daily_pnl
            s._risk_state.daily_pnl_date = s._daily_pnl_date
            s._risk_state.daily_loss_breached = s._daily_loss_breached
            s._risk_state = s._risk_manager.check_daily_loss(s._risk_state, log=s.log)
            # Sync back to strategy attributes
            s._daily_pnl = s._risk_state.daily_pnl
            s._daily_pnl_date = s._risk_state.daily_pnl_date
            s._daily_loss_breached = s._risk_state.daily_loss_breached
            if s._daily_loss_breached:
                self.exit_all_positions()
        else:
            # Fallback: inline logic
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if today != s._daily_pnl_date:
                s._daily_pnl = 0.0
                s._daily_pnl_date = today
                s._daily_loss_breached = False
                return

            if s._daily_loss_breached:
                return

            if s._daily_pnl <= -s.config.daily_loss_limit:
                s.log.error(
                    f"DAILY LOSS LIMIT BREACHED: ${s._daily_pnl:,.2f} / -${s.config.daily_loss_limit:,.2f}. "
                    f"Closing all positions and stopping auto-trade."
                )
                s._daily_loss_breached = True
                save_daily_state(
                    daily_pnl=s._daily_pnl,
                    daily_pnl_date=s._daily_pnl_date,
                    daily_loss_breached=s._daily_loss_breached,
                )
                self.exit_all_positions()

    def cancel_all_open_orders(self) -> None:
        """Cancel ALL pending open orders (kill switch).

        Phase 1 risk control: when position limits are breached,
        cancel all pending orders to stop trading immediately.
        """
        s = self._s
        canceled_count = 0
        for order in s.cache.orders_open():
            try:
                s.cancel_order(order)
                canceled_count += 1
                s.log.info(f"Canceled order {order.client_order_id}")
            except Exception as e:
                s.log.error(f"Failed to cancel order {order.client_order_id}: {e}")
        s.log.info(f"KILL_SWITCH: canceled {canceled_count} open orders")


    def add_resolution_pnl(self, pnl: float) -> None:
        """Called by ResolutionPoller when a market resolves with real P&L.

        Feeds actual (resolution-based) P&L into the daily kill switch tracker.
        """
        s = self._s
        if pnl == 0:
            return
        s._daily_pnl += pnl
        save_daily_state(
            daily_pnl=s._daily_pnl,
            daily_pnl_date=s._daily_pnl_date,
            daily_loss_breached=s._daily_loss_breached,
        )
        self.check_daily_loss()
