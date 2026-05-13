"""Whale Follower — Market exposure calculation.

Provides position exposure analytics across markets and overall.
"""

from __future__ import annotations

from nautilus_trader.model.identifiers import InstrumentId


def get_current_total_exposure(*, cache, open_positions: dict) -> float:
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


def get_market_exposure(*, cache, instrument_id, open_positions: dict) -> float:
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
