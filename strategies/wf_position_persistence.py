"""
Open position persistence.

Saves open positions to a JSON file so they survive restarts.
Format: {instrument_id_str: {size, entry_price, side, market_title, trade_id, ...}}
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

POSITIONS_FILE = Path(__file__).parent.parent / "open_positions.json"


def save_open_positions(open_positions: dict) -> None:
    """Persist open positions to JSON file."""
    # Convert any non-serializable values
    serializable = {}
    for inst_id, info in open_positions.items():
        serializable[str(inst_id)] = {k: v for k, v in info.items() if k != "_pending"}
    
    with open(POSITIONS_FILE, "w") as f:
        json.dump(serializable, f, indent=2, default=str)


def load_open_positions() -> dict:
    """Load open positions from JSON file. Returns {} if no file exists."""
    if not POSITIONS_FILE.exists():
        return {}
    
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
        return data
    except (json.JSONDecodeError, IOError):
        return {}


def clear_open_positions() -> None:
    """Clear persisted positions (called after full resolution)."""
    if POSITIONS_FILE.exists():
        os.remove(POSITIONS_FILE)


# ── Daily P&L state (for kill-switch persistence across restarts) ──────────────

DAILY_STATE_FILE = Path(__file__).parent.parent / "daily_state.json"


def load_daily_state() -> dict:
    """Load daily P&L state from disk.

    Returns defaults if no file exists or date is stale (yesterday).
    The caller is responsible for checking date freshness.
    """
    defaults = {
        "daily_pnl": 0.0,
        "daily_pnl_date": "",
        "daily_loss_breached": False,
        "sports_daily_pnl": 0.0,
        "sports_daily_pnl_date": "",
        "sports_daily_loss_breached": False,
    }
    if not DAILY_STATE_FILE.exists():
        return defaults

    try:
        with open(DAILY_STATE_FILE) as f:
            data = json.load(f)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        loaded_date = data.get("daily_pnl_date", "")
        # Reset if date is stale (not today and not yesterday)
        if loaded_date not in (today, yesterday):
            data = {k: defaults[k] for k in defaults}
        return {**defaults, **data}
    except (json.JSONDecodeError, IOError):
        return defaults


def save_daily_state(
    daily_pnl: float,
    daily_pnl_date: str,
    daily_loss_breached: bool,
    sports_daily_pnl: float = 0.0,
    sports_daily_pnl_date: str = "",
    sports_daily_loss_breached: bool = False,
) -> None:
    """Persist daily P&L state to disk so kill switches survive restarts."""
    data = {
        "daily_pnl": daily_pnl,
        "daily_pnl_date": daily_pnl_date,
        "daily_loss_breached": daily_loss_breached,
        "sports_daily_pnl": sports_daily_pnl,
        "sports_daily_pnl_date": sports_daily_pnl_date,
        "sports_daily_loss_breached": sports_daily_loss_breached,
    }
    try:
        with open(DAILY_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except IOError:
        pass  # Non-fatal — logging system may not be up during early init