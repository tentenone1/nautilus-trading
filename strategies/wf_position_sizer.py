"""Whale Follower — Unified Position Sizer (T17).

Standalone module providing a single entry point for position sizing.
Orchestrates the full sizing pipeline:

  1. Kelly sizing          → wf_kelly.kelly_size()
  2. Liquidity adjustment  → wf_kelly.adjust_size_for_liquidity()
  3. Hard-cap enforcement  → 2 % of capital per position
  4. Position-limit check  → wf_position_checks.check_position_limits()

All state is passed as explicit parameters — no class coupling.

Unlike the WhaleFollower._kelly_size() / _adjust_size_for_liquidity() wrapper
methods (which mirror wf_kelly.py 1:1), this module provides a single
compute_position_size() call that returns the final recommended size AND a
metadata dict containing every intermediate value, enabling full funnel
observability for the DecisionSnapshot pipeline.

Usage
-----
    from strategies.wf_position_sizer import compute_position_size

    final_size, meta = compute_position_size(
        strategy=strategy_instance,   # WhaleFollower
        price=0.55,
        whale_win_rate=0.62,
        edge_score=0.45,
        market_category="general",
        instrument_id=instrument_id,
        is_fade=False,
    )
    if final_size <= 0:
        print(f"Rejected: {meta['reject_reason']}")
    else:
        print(f"Size: ${final_size:.2f}  kelly={meta['kelly_size']:.2f}")

v6.0-observability FREEZE — 2026-05-27
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nautilus_trader.model.objects import Currency

if TYPE_CHECKING:
    from nautilus_trader.model.identifiers import InstrumentId
    from strategies.whale_follower import WhaleFollower

# Re-export for convenience
from strategies.wf_kelly import kelly_size, adjust_size_for_liquidity
from strategies.wf_position_checks import check_position_limits


def compute_position_size(
    *,
    strategy: "WhaleFollower",
    price: float,
    whale_win_rate: float | None,
    edge_score: float,
    market_category: str,
    instrument_id: "InstrumentId",
    is_fade: bool = False,
    available_balance: float | None = None,
    open_positions: dict | None = None,
) -> tuple[float, dict]:
    """Compute recommended position size through the full sizing pipeline.

    Pipeline
    --------
    1. Available-balance resolution
       - Uses provided ``available_balance`` if given
       - Otherwise fetches free USDC.e balance from Nautilus portfolio account

    2. Kelly sizing
       - Calls ``wf_kelly.kelly_size()`` with strategy config parameters
       - Per-category Kelly fraction from ``strategy._strategies[category]`` if set
       - Falls back to ``strategy.config`` Kelly fraction
       - Applies per-category caps (crypto: 15 %, geopolitics: 10 %, sports: halved)

    3. Liquidity adjustment
       - Calls ``wf_kelly.adjust_size_for_liquidity()``
       - Reduces size for tier-4 (≤25 %), tier-3 (≤50 %), tier-2 (≤75 %) markets

    4. Hard-cap enforcement
       - Caps at 2 % of capital (same as ``check_position_limits`` Phase 1)

    5. Position-limit check
       - Calls ``wf_position_checks.check_position_limits()``
       - Checks: MAX_SINGLE_POSITION, MAX_TOTAL_EXPOSURE, MAX_MARKET_EXPOSURE

    Parameters
    ----------
    strategy : WhaleFollower
        Strategy instance (provides config, portfolio, open_positions).
    price : float
        Current market price (probability, 0–1).
    whale_win_rate : float | None
        Whale's historical win rate (None → 55 % default).
    edge_score : float
        Edge-score from the signal (0.0–1.0).
    market_category : str
        Market category string (e.g. "general", "sports", "crypto").
    instrument_id : InstrumentId
        Nautilus InstrumentId for the proposed position.
    is_fade : bool
        Whether this is a fade signal (inverts win rate in Kelly calc).
    available_balance : float | None
        Pre-fetched available USDC.e balance. If None, fetches from portfolio.
    open_positions : dict | None
        Snapshot of open positions. If None, uses ``strategy._open_positions``.

    Returns
    -------
    tuple[float, dict]
        ``(final_size, meta)``

        *final_size* is the recommended position size in USD, or ``0.0`` if
        the signal was rejected at any pipeline stage.

        *meta* is a dict with the following keys:

        ========================  ==============================================
        key                       value
        ========================  ==============================================
        ``kelly_size``            Raw Kelly-computed size (before liquidity)
        ``liquidity_adjusted``    Size after liquidity adjustment
        ``hard_cap``              2 %-of-capital cap that was applied
        ``available_balance``     Resolved available balance
        ``capital``               Capital base used (validation or config bankroll)
        ``position_limit_allowed`` Whether position-limit check passed (bool)
        ``position_limit_reason``  Rejection reason if not allowed
        ``current_exposure``       Current total exposure at time of check
        ``market_exposure``        Current market (condition) exposure
        ``reject_reason``          Top-level rejection reason, or ``""``
        ``reject_stage``          Pipeline stage that rejected (``"kelly"``,
                                  ``"position_limit"``, ``""``)
        ========================  ==============================================
    """
    # ── 0. Defaults ──────────────────────────────────────────────────────────
    inst_key = str(instrument_id)
    meta: dict = {
        "kelly_size": 0.0,
        "liquidity_adjusted": 0.0,
        "hard_cap": 0.0,
        "available_balance": 0.0,
        "capital": 0.0,
        "position_limit_allowed": True,
        "position_limit_reason": "",
        "current_exposure": 0.0,
        "market_exposure": 0.0,
        "reject_reason": "",
        "reject_stage": "",
    }

    # ── 1. Resolve available balance ─────────────────────────────────────────
    if available_balance is None:
        try:
            USDC_E = Currency.from_str("USDC.e")
            account = strategy.portfolio.account()
            if account is not None:
                available_balance = account.balance_free(USDC_E).as_double()
            else:
                available_balance = 0.0
        except Exception:
            available_balance = 0.0

    meta["available_balance"] = available_balance

    # ── 2. Capital base ───────────────────────────────────────────────────────
    capital = (
        strategy.config.validation_capital_base
        if strategy.config.validation_capital_base > 0
        else strategy.config.bankroll
    )
    meta["capital"] = capital

    # ── 3. Kelly sizing ───────────────────────────────────────────────────────
    cat_strategy = strategy._strategies.get(market_category.lower())
    if cat_strategy is not None:
        kelly_fraction = cat_strategy.params.kelly_fraction
        max_single_pct = cat_strategy.params.max_single_position_pct
        max_position_pct = cat_strategy.params.max_single_position_pct
    else:
        kelly_fraction = strategy.config.kelly_fraction
        max_single_pct = strategy.config.max_single_position_pct
        max_position_pct = strategy.config.max_position_pct

    kelly_sz = kelly_size(
        bankroll=capital,
        kelly_fraction=kelly_fraction,
        max_position_pct=max_position_pct,
        price=price,
        whale_win_rate=whale_win_rate,
        edge_score=edge_score,
        available_balance=available_balance,
        market_category=market_category,
        max_single_position_pct=max_single_pct,
        whale_tiering=strategy._whale_tiering if hasattr(strategy, "_whale_tiering") else None,
        is_fade=is_fade,
    )
    meta["kelly_size"] = kelly_sz

    if kelly_sz <= 0:
        wr_note = f" (whale_wr={whale_win_rate:.0%})" if whale_win_rate else " (fixed_wr=55%)"
        meta["reject_reason"] = f"No Kelly edge{wr_note}, skipping"
        meta["reject_stage"] = "kelly"
        return 0.0, meta

    # ── 4. Liquidity adjustment ──────────────────────────────────────────────
    def _log_liq(msg: str) -> None:
        strategy.log.debug(msg)

    liquidity_adj_sz = adjust_size_for_liquidity(
        size_usd=kelly_sz,
        instrument_id_str=inst_key,
        get_market_event_time_func=strategy._get_market_event_time
        if hasattr(strategy, "_get_market_event_time")
        else None,
        log_func=_log_liq,
    )
    meta["liquidity_adjusted"] = liquidity_adj_sz

    # ── 5. Hard-cap enforcement (2 % of capital) ─────────────────────────────
    hard_cap = capital * max_single_pct
    meta["hard_cap"] = hard_cap

    sized = min(liquidity_adj_sz, hard_cap)

    # ── 6. Position-limit check ──────────────────────────────────────────────
    _open_positions = open_positions if open_positions is not None else getattr(
        strategy, "_open_positions", {}
    )

    limit_allowed, limit_reason = check_position_limits(
        config=strategy.config,
        cache=strategy.cache,
        instrument_id=instrument_id,
        proposed_size_usd=sized,
        open_positions=_open_positions,
        log=strategy.log,
    )
    meta["position_limit_allowed"] = limit_allowed
    meta["position_limit_reason"] = limit_reason

    if not limit_allowed:
        meta["reject_reason"] = limit_reason
        meta["reject_stage"] = "position_limit"
        return 0.0, meta

    # ── 7. Compute current exposures for metadata ─────────────────────────────
    try:
        from strategies.wf_position_checks import (
            get_current_total_exposure,
            get_market_exposure,
        )

        meta["current_exposure"] = get_current_total_exposure(
            cache=strategy.cache, open_positions=_open_positions
        )
        meta["market_exposure"] = get_market_exposure(
            cache=strategy.cache,
            instrument_id=instrument_id,
            open_positions=_open_positions,
        )
    except Exception:
        # Non-fatal: exposures are for observability only
        pass

    # ── 8. Available-balance check ───────────────────────────────────────────
    if sized > available_balance:
        meta["reject_reason"] = (
            f"Size ${sized:,.2f} exceeds available ${available_balance:,.2f}"
        )
        meta["reject_stage"] = "balance"
        return 0.0, meta

    return round(sized, 2), meta


def quick_size(
    *,
    bankroll: float,
    kelly_fraction: float,
    price: float,
    whale_win_rate: float | None = None,
    edge_score: float = 0.0,
    market_category: str = "",
    is_fade: bool = False,
) -> float:
    """Lightweight Kelly-size estimate without Nautilus dependency.

    Use this when you only have the raw parameters and don't have a live
    strategy instance (e.g. backtesting scripts, signal pre-screening).

    For live trading always prefer ``compute_position_size()`` which includes
    liquidity adjustment, hard-capping, and position-limit checks.

    Parameters
    ----------
    bankroll : float
        Capital base.
    kelly_fraction : float
        Kelly multiplier from config.
    price : float
        Market price (probability).
    whale_win_rate : float | None
        Historical win rate (None → 55 % default).
    edge_score : float
        Edge score (0.0–1.0).
    market_category : str
        Category for per-category caps.
    is_fade : bool
        Fade signal flag.

    Returns
    -------
    float
        Estimated Kelly size in USD (rounded to 2 dp). Returns 0.0 if no edge.
    """
    return kelly_size(
        bankroll=bankroll,
        kelly_fraction=kelly_fraction,
        max_position_pct=0.02,  # conservative default
        price=price,
        whale_win_rate=whale_win_rate,
        edge_score=edge_score,
        available_balance=None,
        market_category=market_category,
        max_single_position_pct=0.02,
        whale_tiering=None,
        is_fade=is_fade,
    )
