"""Whale Follower — Exit Engine.

Standalone functions for position exit checks, stop-loss/take-profit,
duration limits, and resolution-based exits. No class coupling — all state
is passed as parameters.

Responsibilities:
- Exit timer callback (runs every 30s)
- Stop-loss, take-profit checks (certainty thresholds)
- Duration-based exit (max_hold_hours)
- Resolution-aware exit
- Sports event exit

Usage:
    from strategies.wf_exit_engine import (
        check_all_positions,
        should_exit_for_resolution,
        should_exit_for_sports,
    )
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

from nautilus_trader.model.identifiers import InstrumentId

from strategies.wf_constants import (
    CERTAINTY_WIN_THRESHOLD,
    CERTAINTY_LOSS_THRESHOLD,
    RESOLUTION_EXIT_HOURS,
    SPORTS_EXIT_HOURS_BEFORE_EVENT,
    SPORTS_WHITELIST_PATTERNS,
)
from strategies.wf_state import (
    get_position_info,
)


# ── Constants ────────────────────────────────────────────────────────

# Exit timer fires every 30 seconds
EXIT_TIMER_INTERVAL_SECS = 30.0

# Exit reason codes
EXIT_REASON_STOP_LOSS = "stop_loss"
EXIT_REASON_TAKE_PROFIT = "take_profit"
EXIT_REASON_MAX_HOLD = "max_hold"
EXIT_REASON_RESOLUTION = "resolution"
EXIT_REASON_SPORTS_EVENT = "sports_event"
EXIT_REASON_CERTAINTY_WIN = "certainty_win"
EXIT_REASON_CERTAINTY_LOSS = "certainty_loss"
EXIT_REASON_MANUAL = "manual"
EXIT_REASON_EMERGENCY = "emergency_exit_all"
EXIT_REASON_EMERGENCY_FLATTEN = "emergency_flatten"


# ── Exit Checks ───────────────────────────────────────────────────────


def check_all_positions(
    *,
    config,
    cache,
    log,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    exit_position_func: Callable,
    resolve_exit_price_func: Callable,
    current_time: Optional[float] = None,
) -> List[str]:
    """Check ALL open positions for exit conditions.

    Iterates through all tracked positions and checks:
    1. Duration-based exit (max_hold_hours)
    2. Certainty thresholds (win/loss)
    3. Sports event exit
    4. Resolution exit

    Args:
        config: WhaleFollowerConfig with max_hold_hours etc.
        cache: Nautilus cache for position lookup.
        log: Logger instance.
        open_positions: The _open_positions registry.
        exited_positions: The _exited_positions dedup set.
        last_exit_time: Re-entry cooldown dict.
        exit_position_func: Callback to close a position (inst_id, reason).
        resolve_exit_price_func: Callback to get simulated exit price.
        current_time: Optional timestamp (defaults to now).

    Returns:
        List of instrument keys that were exited.
    """
    now = current_time if current_time is not None else time.time()
    exited_keys: List[str] = []

    # Phase 1: Duration-based exit — close positions held past max_hold_hours
    max_hold = getattr(config, "max_hold_hours", 4.0)
    expired_keys = [
        k for k, v in open_positions.items()
        if now - v.get("entry_time", 0) > max_hold * 3600
    ]

    for inst_key in expired_keys:
        try:
            inst_id = InstrumentId.from_str(inst_key)
            exit_position_func(inst_id, exit_reason=EXIT_REASON_MAX_HOLD)
            exited_keys.append(inst_key)
        except Exception as e:
            log.error(
                f"Error exiting expired position {inst_key[:50]}...: {e}",
                extra={"inst_key": inst_key[:80], "error": str(e)},
            )
            # Clean up stale entry even on error
            if inst_key in open_positions:
                del open_positions[inst_key]

    # Phase 2: Check ALL open positions for certainty/resolution/sports exits
    for inst_key in list(open_positions.keys()):
        try:
            # ── ERROR ISOLATION: Wrap each position in try/except ──
            try:
                inst_id = InstrumentId.from_str(inst_key)
            except Exception as parse_err:
                log.error(
                    f"Failed to parse instrument ID '{inst_key[:50]}...': {parse_err}",
                    extra={"inst_key": inst_key[:80], "error": str(parse_err)},
                )
                continue

            # Get Nautilus position
            nautilus_positions = cache.positions_open(instrument_id=inst_id)
            if not nautilus_positions or nautilus_positions[0].quantity.as_double() == 0:
                continue

            pos = nautilus_positions[0]
            raw_entry = pos.avg_px_open
            entry = raw_entry.as_double() if hasattr(raw_entry, "as_double") else float(raw_entry)
            if entry <= 0:
                continue

            # Get position info from registry
            pos_info = get_position_info(open_positions=open_positions, inst_key=inst_key)
            if not pos_info:
                continue

            # Get current price from cache (last quote)
            quote = cache.quote_tick(inst_id)
            if quote is None:
                # Dynamic instrument without quote subscription — use simulated price
                mid = resolve_exit_price_func(pos_info)
                log.info(
                    f"SIMULATED PRICE for {inst_id}: mid={mid:.4f}",
                    extra={"inst_key": inst_key[:60], "simulated_price": mid},
                )
            else:
                mid = (quote.bid_price.as_double() + quote.ask_price.as_double()) / 2

            # ── Resolution-aware exit for binary prediction markets ──
            # Price-based SL/TP on binary outcome markets captures mid-point
            # opinion, not resolution truth. Instead, we hold to resolution
            # and only exit on certainty thresholds.
            position_edge = pos_info.get("edge_score", 0.0) or 0.0
            side = pos_info.get("side", "BUY")

            # Certainty exit: if price strongly indicates the outcome
            is_certain_win = False
            is_certain_loss = False

            if side == "BUY":
                is_certain_win = mid > CERTAINTY_WIN_THRESHOLD
                is_certain_loss = mid < CERTAINTY_LOSS_THRESHOLD
            else:  # SELL (short)
                is_certain_win = mid < CERTAINTY_LOSS_THRESHOLD
                is_certain_loss = mid > CERTAINTY_WIN_THRESHOLD

            if is_certain_win:
                log.info(
                    f"CERTAINTY EXIT (WIN) {inst_id}: mid={mid:.4f}, entry={entry:.4f}",
                    extra={
                        "inst_key": inst_key[:60],
                        "mid": mid,
                        "entry": entry,
                        "side": side,
                    },
                )
                exit_position_func(inst_id, exit_reason=EXIT_REASON_CERTAINTY_WIN)
                exited_keys.append(inst_key)
                continue
            elif is_certain_loss:
                log.info(
                    f"CERTAINTY LOSS BLOCKED (Phase A): {inst_id}: mid={mid:.4f}, entry={entry:.4f}",
                    extra={"inst_key": inst_key[:60], "mid": mid, "entry": entry},
                )
                continue

            # ── Sports exit (whitelisted Spread bets only) ──
            mc = pos_info.get("market_category", "")
            if mc.lower() == "sports":
                title = pos_info.get("market_title", "") or ""
                is_whitelisted = any(
                    re.search(p, title, re.IGNORECASE)
                    for p in SPORTS_WHITELIST_PATTERNS
                )

                if is_whitelisted:
                    # Sports event exit (game imminent)
                    should_exit, reason = should_exit_for_sports(
                        inst_key=inst_key,
                        title=title,
                        log=log,
                    )
                    if should_exit:
                        log.info(
                            f"SPORTS EVENT EXIT {inst_id}: Spread bet, game imminent",
                            extra={"inst_key": inst_key[:60], "reason": reason},
                        )
                        exit_position_func(inst_id, exit_reason=EXIT_REASON_SPORTS_EVENT)
                        exited_keys.append(inst_key)
                        continue
                else:
                    log.info(
                        f"SKIP sports exit (non-whitelisted): {title[:50]}",
                        extra={"title": title[:80], "entry": entry, "mid": mid},
                    )

            # ── Resolution exit check ──
            should_exit, reason = should_exit_for_resolution(inst_key=inst_key, log=log)
            if should_exit:
                log.info(
                    f"RESOLUTION EXIT {inst_id}: market resolving soon",
                    extra={"inst_key": inst_key[:60], "reason": reason},
                )
                exit_position_func(inst_id, exit_reason=EXIT_REASON_RESOLUTION)
                exited_keys.append(inst_key)
                continue

            # Log holding state for transparency
            log.info(
                f"HOLDING {inst_id}: entry={entry:.4f}, mid={mid:.4f}, edge={position_edge:.2f}",
                extra={
                    "inst_key": inst_key[:60],
                    "entry": entry,
                    "mid": mid,
                    "edge": position_edge,
                },
            )

        except Exception as pos_error:
            log.error(
                f"Error checking position {inst_key[:50]}...: {pos_error}",
                extra={"inst_key": inst_key[:80], "error": str(pos_error)},
            )
            continue

    return exited_keys


def should_exit_for_resolution(
    *,
    inst_key: str,
    log,
    requests_module=None,
) -> tuple[bool, str]:
    """Check if the market resolves within RESOLUTION_EXIT_HOURS hours.

    Args:
        inst_key: Instrument ID string.
        log: Logger instance.
        requests_module: Optional requests module override.

    Returns:
        (should_exit, reason) tuple.
    """
    import requests as _requests
    requests = requests_module or _requests

    cond_id = inst_key.split("-")[0]

    try:
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
                    return True, f"resolves in {hours_left:.1f}h"
                if hours_left <= 0:
                    log.info(
                        f"Market has already ended ({abs(hours_left):.1f}h ago)",
                        extra={"cond_id": cond_id[:20]},
                    )
                    return True, "market_ended"
    except Exception as e:
        log.debug(f"Resolution check failed: {e}", extra={"cond_id": cond_id[:20]})

    return False, ""


def should_exit_for_sports(
    *,
    inst_key: str,
    title: str,
    log,
    requests_module=None,
) -> tuple[bool, str]:
    """Check if a sports position should be exited (game imminent).

    Args:
        inst_key: Instrument ID string.
        title: Market title for keyword matching.
        log: Logger instance.
        requests_module: Optional requests module override.

    Returns:
        (should_exit, reason) tuple.
    """
    import requests as _requests
    requests = requests_module or _requests

    # Extract condition ID
    cond_id = inst_key.split("-")[0]

    # Check gamma API for event timing
    try:
        resp = requests.get(
            f"https://gamma-api.polymarket.com/markets/{cond_id}",
            timeout=10,
        )
        if resp.status_code == 200:
            m = resp.json()
            end_date = m.get("endDateIso") or m.get("endDate")
            if end_date:
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600

                # Exit if game within SPORTS_EXIT_HOURS_BEFORE_EVENT
                if hours_left is not None and 0 < hours_left < SPORTS_EXIT_HOURS_BEFORE_EVENT:
                    return True, f"game in {hours_left:.1f}h"

                # Exit if market is in-play (prices frozen)
                # In-play detection: hours_left < 6 and event started
                if hours_left < 6 and hours_left > 0:
                    return True, "in_play"
    except Exception as e:
        log.debug(f"Sports timing check failed: {e}", extra={"cond_id": cond_id[:20]})

    return False, ""


# ── Exit Timer Callback ───────────────────────────────────────────────


def on_exit_timer(
    *,
    config,
    cache,
    log,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    last_exit_time: Dict[str, float],
    exit_position_func: Callable,
    resolve_exit_price_func: Callable,
    scan_whale_positions_func: Optional[Callable] = None,
    check_daily_loss_func: Optional[Callable] = None,
    check_autoresearch_func: Optional[Callable] = None,
    check_sybil_func: Optional[Callable] = None,
    resolution_poller: Optional[Any] = None,
    last_scan: float = 0.0,
    last_resolution_poll: float = 0.0,
    resolution_poll_interval: float = 120.0,
    last_heartbeat: Optional[float] = None,
) -> Dict[str, Any]:
    """Timer callback — fires every 30s independently of quote ticks.

    This fixes the design flaw where exit checks only ran during quote
    tick processing. If quotes stop, exits were never checked.

    Args:
        config: WhaleFollowerConfig.
        cache: Nautilus cache.
        log: Logger instance.
        open_positions: The _open_positions registry.
        exited_positions: The _exited_positions dedup set.
        last_exit_time: Re-entry cooldown dict.
        exit_position_func: Callback to close a position.
        resolve_exit_price_func: Callback to get simulated exit price.
        scan_whale_positions_func: Optional whale scan callback.
        check_daily_loss_func: Optional daily loss check callback.
        check_autoresearch_func: Optional autoresearch signal check.
        check_sybil_func: Optional sybil signal check.
        resolution_poller: Optional ResolutionPoller instance.
        last_scan: Last whale scan timestamp.
        last_resolution_poll: Last resolution poll timestamp.
        resolution_poll_interval: Resolution poll interval in seconds.
        last_heartbeat: Last heartbeat log timestamp.

    Returns:
        Dict with updated timestamps: last_scan, last_resolution_poll, last_heartbeat.
    """
    now = time.time()

    # ── Position Exit Checks ──
    check_all_positions(
        config=config,
        cache=cache,
        log=log,
        open_positions=open_positions,
        exited_positions=exited_positions,
        last_exit_time=last_exit_time,
        exit_position_func=exit_position_func,
        resolve_exit_price_func=resolve_exit_price_func,
        current_time=now,
    )

    # ── Daily Loss Limit Check ──
    if check_daily_loss_func:
        check_daily_loss_func()

    # ── Heartbeat Log (throttle to once per minute) ──
    heartbeat_interval = 60.0
    if last_heartbeat is None or (now - last_heartbeat) > heartbeat_interval:
        nautilus_open = len(cache.positions_open())
        total_open = len(open_positions)
        log.info(
            f"Exit timer heartbeat — {total_open} positions tracked",
            extra={
                "open_positions": total_open,
                "nautilus_cache": nautilus_open,
            },
        )
        last_heartbeat = now

    # ── Whale Position Scanning ──
    scan_interval = getattr(config, "scan_interval_secs", 30.0)
    if scan_whale_positions_func and (now - last_scan) >= scan_interval:
        scan_whale_positions_func()
        last_scan = now

    # ── Autoresearch LLM Signal Bridge ──
    if check_autoresearch_func:
        check_autoresearch_func()

    # ── Sybil Signal Bridge ──
    if check_sybil_func:
        check_sybil_func()

    # ── Resolution Polling ──
    if resolution_poller and (now - last_resolution_poll) >= resolution_poll_interval:
        try:
            events = resolution_poller.poll_open_positions(open_positions)
            if events:
                for ev in events:
                    log.info(
                        f"[RESOLUTION] {ev.get('question', '')[:50]} | "
                        f"Winner: {ev.get('winning_outcome', '?')} | "
                        f"Actual P&L: ${ev.get('total_actual_pnl', 0):+.2f}",
                        extra={
                            "question": ev.get("question", "")[:80],
                            "winning_outcome": ev.get("winning_outcome", "?"),
                            "total_actual_pnl": ev.get("total_actual_pnl", 0),
                        },
                    )
                    # Exit positions whose markets have resolved
                    for trade in ev.get("trades", []):
                        resolved_inst_key = trade.get("inst_key", "")
                        if resolved_inst_key and resolved_inst_key in open_positions:
                            try:
                                resolved_inst_id = InstrumentId.from_str(resolved_inst_key)
                                exit_position_func(resolved_inst_id, exit_reason="market_resolved")
                            except Exception as e:
                                log.error(
                                    f"Failed to exit resolved position: {e}",
                                    extra={"inst_key": resolved_inst_key[:60]},
                                )
        except Exception as e:
            log.error(f"Resolution poll error: {e}")
        last_resolution_poll = now

    return {
        "last_scan": last_scan,
        "last_resolution_poll": last_resolution_poll,
        "last_heartbeat": last_heartbeat,
    }