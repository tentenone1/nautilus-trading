"""Whale Follower — Market data fetching and resolution checks.

Standalone functions for fetching real-time midpoint prices and
determining whether to exit based on market resolution status.
"""

from __future__ import annotations

import requests
from datetime import datetime, timezone

from strategies.wf_constants import RESOLUTION_EXIT_HOURS


def fetch_real_midpoint(condition_id: str) -> float | None:
    """Fetch the real market midpoint price from Polymarket CLOB API.

    Args:
        condition_id: The condition ID extracted from the instrument ID.

    Returns:
        Midpoint price as a float, or None if the API call fails.
    """
    try:
        import urllib.request
        import json

        url = f"https://clob.polymarket.com/midpoint?token_id={condition_id}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        price_str = data.get("midpoint") or data.get("price")
        if price_str is not None:
            return float(price_str)
    except Exception:
        pass
    return None


def resolve_exit_price(
    pos_info: dict,
    instrument_id_str: str,
    get_market_resolution=None,
    calculate_actual_pnl=None,
    log_func=None,
) -> float:
    """Determine exit price using real market data (no random walk).

    Priority:
        1. Market resolved -> resolution price ($1.00 if won, $0.00 if lost)
        2. CLOB API midpoint -> actual trading price
        3. Fallback: deterministic estimate (edge-based drift, no Gaussian noise)

    Args:
        pos_info: Position info dict with keys: entry_price, side, condition_id,
            inst_key, edge_score, size.
        instrument_id_str: Full instrument ID string (e.g. "cond-token.POLYMARKET").
        get_market_resolution: Optional callable to get market resolution status.
            Defaults to components.resolution_poller.get_market_resolution.
        calculate_actual_pnl: Optional callable to calculate P&L.
            Defaults to components.resolution_poller.calculate_actual_pnl.
        log_func: Optional logging callable.

    Returns:
        Exit price as a float.
    """
    entry = pos_info.get("entry_price", 0.5)
    side = pos_info.get("side", "BUY")

    # 1. Check if market is resolved
    condition_id = pos_info.get("condition_id", "")
    if condition_id:
        if get_market_resolution is None:
            from components.resolution_poller import (
                get_market_resolution,
                calculate_actual_pnl,
            )
        try:
            market = get_market_resolution(condition_id)
            if market and market.get("resolved"):
                winning_token_id = market.get("winning_token_id", "")
                our_token_id = ""
                if instrument_id_str and "-" in instrument_id_str:
                    our_token_id = instrument_id_str.replace(".POLYMARKET", "").split("-")[-1]
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
    if instrument_id_str:
        mid = fetch_real_midpoint(instrument_id_str)
        if mid is not None and 0.01 <= mid <= 0.99:
            return mid

    # 3. Fallback: deterministic estimate (NO random walk)
    edge = pos_info.get("edge_score", 0.0) or 0.0
    target = 1.0 if side.upper() in ("BUY", "LONG") else 0.0
    drift = edge * (target - entry) * 0.30
    price = entry + drift
    return max(0.01, min(0.99, price))


def should_exit_for_resolution(
    instrument_id_str: str,
    log_func=None,
) -> bool:
    """Check if the market for this instrument resolves within RESOLUTION_EXIT_HOURS hours.

    Args:
        instrument_id_str: Full instrument ID string (e.g. "cond-token.POLYMARKET").
        log_func: Optional logging callable.

    Returns:
        True if the position should be exited due to imminent resolution.
    """
    try:
        cond_id = instrument_id_str.split("-")[0]
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
                    return True
                # Exit if market has already ended (hours_left <= 0 = resolved/expired)
                if hours_left <= 0:
                    if log_func:
                        log_func(
                            f"Market has already ended ({abs(hours_left):.1f}h ago) — "
                            f"{cond_id[:16]}..., exiting stale position"
                        )
                    return True
                return False
    except Exception:
        pass  # API failure — don't exit on error
    return False
