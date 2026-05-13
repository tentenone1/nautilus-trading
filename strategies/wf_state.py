"""Whale Follower — Position State Machine.

Standalone functions for managing position state, idempotency tracking,
and recovery from database. No class coupling — all state is passed as parameters.

Responsibilities:
- _open_positions registry management
- _exited_positions dedup tracking
- _filled_orders idempotency
- DB recovery of orphan positions

Usage:
    from strategies.wf_state import (
        init_state,
        has_active_position,
        remove_position,
        recover_open_positions,
    )
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Set

from strategies.wf_db_ops import recover_open_positions as _db_recover_open_positions


# ── Constants ────────────────────────────────────────────────────────

MAX_FILLED_ORDERS_SET_SIZE = 10000  # Prune filled_orders set if exceeds this
PRUNE_KEEP_COUNT = 5000  # Keep this many entries after pruning


# ── Position State Management ─────────────────────────────────────────


def init_state() -> Dict[str, Any]:
    """Initialize the position state containers.

    Returns a dict with empty containers for position tracking:
        - open_positions: dict[str, dict] — position info by instrument_id
        - exited_positions: set[str] — exited instrument IDs (dedup)
        - filled_orders: set[str] — processed client_order_ids (idempotency)

    Returns:
        Dict with empty state containers.
    """
    return {
        "open_positions": {},
        "exited_positions": set(),
        "filled_orders": set(),
    }


def has_active_position(
    *,
    open_positions: Dict[str, Dict],
    inst_key: str,
) -> bool:
    """Check if an instrument has an active tracked position.

    Args:
        open_positions: The _open_positions registry.
        inst_key: Instrument ID string to check.

    Returns:
        True if position exists in registry, False otherwise.
    """
    return inst_key in open_positions


def get_position_info(
    *,
    open_positions: Dict[str, Dict],
    inst_key: str,
) -> Optional[Dict[str, Any]]:
    """Get position info for an instrument.

    Args:
        open_positions: The _open_positions registry.
        inst_key: Instrument ID string to look up.

    Returns:
        Position info dict if exists, None otherwise.
    """
    return open_positions.get(inst_key)


def register_position(
    *,
    open_positions: Dict[str, Dict],
    inst_key: str,
    whale_name: str,
    market_title: str,
    category: str,
    side: str,
    entry_price: float,
    size: float,
    trade_id: str,
    condition_id: str,
    venue_position_id: str = "",
    edge_score: float = 0.0,
    confidence: float = 0.0,
    kelly_fraction: float = 0.0,
    entry_time: Optional[float] = None,
) -> None:
    """Register a new position in the tracking registry.

    Args:
        open_positions: The _open_positions registry (mutated).
        inst_key: Instrument ID string.
        whale_name: Whale wallet name.
        market_title: Human-readable market title.
        category: Market category.
        side: Position side (BUY/SELL).
        entry_price: Entry price per share.
        size: Position size in USD.
        trade_id: UUID for the trade.
        condition_id: Polymarket condition ID.
        venue_position_id: Venue-assigned position ID.
        edge_score: Signal edge score.
        confidence: Signal confidence.
        kelly_fraction: Applied Kelly fraction.
        entry_time: Entry timestamp (defaults to now).
    """
    open_positions[inst_key] = {
        "whale_name": whale_name,
        "market_title": market_title,
        "category": category,
        "side": side,
        "entry_price": entry_price,
        "size": size,
        "entry_time": entry_time if entry_time is not None else time.time(),
        "trade_id": trade_id,
        "condition_id": condition_id,
        "venue_position_id": venue_position_id,
        "edge_score": edge_score,
        "confidence": confidence,
        "kelly_fraction": kelly_fraction,
    }


def remove_position(
    *,
    open_positions: Dict[str, Dict],
    exited_positions: Set[str],
    inst_key: str,
) -> Optional[Dict[str, Any]]:
    """Remove a position from the registry and mark as exited.

    Args:
        open_positions: The _open_positions registry (mutated).
        exited_positions: The _exited_positions set (mutated).
        inst_key: Instrument ID string to remove.

    Returns:
        The removed position info dict if existed, None otherwise.
    """
    exited_positions.add(inst_key)
    return open_positions.pop(inst_key, None)


def mark_exited(
    *,
    exited_positions: Set[str],
    inst_key: str,
) -> None:
    """Mark an instrument as exited (dedup tracking).

    Args:
        exited_positions: The _exited_positions set (mutated).
        inst_key: Instrument ID string to mark.
    """
    exited_positions.add(inst_key)


def is_exited(
    *,
    exited_positions: Set[str],
    inst_key: str,
) -> bool:
    """Check if an instrument has already been exited.

    Args:
        exited_positions: The _exited_positions set.
        inst_key: Instrument ID string to check.

    Returns:
        True if already exited, False otherwise.
    """
    return inst_key in exited_positions


# ── Order Idempotency ────────────────────────────────────────────────


def is_order_processed(
    *,
    filled_orders: Set[str],
    client_order_id: str,
) -> bool:
    """Check if an order fill was already processed.

    Args:
        filled_orders: The _filled_orders idempotency set.
        client_order_id: The client order ID to check.

    Returns:
        True if already processed, False otherwise.
    """
    return client_order_id in filled_orders


def mark_order_processed(
    *,
    filled_orders: Set[str],
    client_order_id: str,
) -> bool:
    """Mark an order as processed (idempotency tracking).

    Returns True if this was a NEW order (first time processed).
    Returns False if already processed (duplicate).

    Args:
        filled_orders: The _filled_orders idempotency set (mutated).
        client_order_id: The client order ID to mark.

    Returns:
        True if new (first processing), False if duplicate.
    """
    if client_order_id in filled_orders:
        return False  # Duplicate
    filled_orders.add(client_order_id)
    return True  # New


def prune_filled_orders(
    *,
    filled_orders: Set[str],
    max_size: int = MAX_FILLED_ORDERS_SET_SIZE,
    keep_count: int = PRUNE_KEEP_COUNT,
) -> int:
    """Prune filled_orders set if too large (memory guard).

    Args:
        filled_orders: The _filled_orders idempotency set (mutated).
        max_size: Threshold to trigger pruning.
        keep_count: Number of recent entries to keep.

    Returns:
        Number of entries removed.
    """
    if len(filled_orders) <= max_size:
        return 0

    # Keep most recent entries (set iteration order is insertion order in Python 3.7+)
    as_list = list(filled_orders)
    removed_count = len(as_list) - keep_count

    # Clear and re-add only the recent ones
    filled_orders.clear()
    for entry in as_list[-keep_count:]:
        filled_orders.add(entry)

    return removed_count


# ── Position Recovery ─────────────────────────────────────────────────


def recover_positions_from_db(
    *,
    open_positions: Dict[str, Dict],
    log_func: Optional[Callable[[str], None]] = None,
    max_recovery_age_hours: float = 4.0,
) -> int:
    """Reload unfinished positions from trades database.

    Reads trades without exit_reason and reconstructs the _open_positions
    registry. Only recovers trades newer than max_recovery_age_hours.

    Args:
        open_positions: The _open_positions registry (mutated).
        log_func: Optional logging callable.
        max_recovery_age_hours: Skip orphans older than this.

    Returns:
        Count of recovered positions.
    """
    recovered_list = _db_recover_open_positions(
        log_func=log_func,
        max_recovery_age_hours=max_recovery_age_hours,
    )

    recovered_count = 0
    for pos in recovered_list:
        inst_key = pos.get("inst_key", "")
        if not inst_key:
            continue
        if inst_key not in open_positions:
            # Copy all fields except inst_key into registry
            open_positions[inst_key] = {
                k: v for k, v in pos.items() if k != "inst_key"
            }
            recovered_count += 1

    return recovered_count


def get_open_position_count(
    *,
    open_positions: Dict[str, Dict],
) -> int:
    """Get count of tracked open positions.

    Args:
        open_positions: The _open_positions registry.

    Returns:
        Number of tracked positions.
    """
    return len(open_positions)


def get_all_instrument_keys(
    *,
    open_positions: Dict[str, Dict],
) -> List[str]:
    """Get all instrument keys currently tracked.

    Args:
        open_positions: The _open_positions registry.

    Returns:
        List of instrument ID strings.
    """
    return list(open_positions.keys())


# ── Re-entry Cooldown ────────────────────────────────────────────────


def is_in_cooldown(
    *,
    last_exit_time: Dict[str, float],
    inst_key: str,
    cooldown_secs: float,
) -> bool:
    """Check if re-entry cooldown is active for an instrument.

    Args:
        last_exit_time: Dict of inst_key -> last exit timestamp.
        inst_key: Instrument ID string to check.
        cooldown_secs: Cooldown duration in seconds.

    Returns:
        True if in cooldown, False if allowed to re-enter.
    """
    last_exit = last_exit_time.get(inst_key, 0.0)
    return (time.time() - last_exit) < cooldown_secs


def record_exit_time(
    *,
    last_exit_time: Dict[str, float],
    inst_key: str,
) -> None:
    """Record exit timestamp for cooldown tracking.

    Args:
        last_exit_time: Dict of inst_key -> last exit timestamp (mutated).
        inst_key: Instrument ID string that was exited.
    """
    last_exit_time[inst_key] = time.time()