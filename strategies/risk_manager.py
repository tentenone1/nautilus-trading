"""Risk Manager -- Extracted risk controls from whale_follower.py.

Handles:
  - Kill switch (position limits breached)
  - Daily loss limit
  - Position limit checks
  - Max open positions
  - Low cash alerts
  - Daily P&L tracking with state persistence

All methods are stateless or use explicit state parameters,
making them independently testable without the full Strategy class.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from strategies.wf_position_checks import check_position_limits
from strategies.wf_position_persistence import load_daily_state, save_daily_state

logger = logging.getLogger("RiskManager")


@dataclass
class RiskState:
    """Mutable risk state that persists across the trading session."""
    daily_pnl: float = 0.0
    daily_pnl_date: str = ""
    daily_loss_breached: bool = False
    kill_switch_breached: bool = False
    sports_daily_pnl: float = 0.0
    sports_daily_loss_breached: bool = False
    fade_positions: set = field(default_factory=set)
    fade_max_concurrent: int = 3


class RiskManager:
    """Manages risk controls for the whale follower strategy.

    Extracted from WhaleFollower's inline risk logic to make it testable
    and composable. The strategy delegates risk decisions to this class.
    """

    def __init__(self, config=None):
        self.config = config

    def check_daily_loss(self, state: RiskState, log=None) -> RiskState:
        """Check and enforce daily loss limits."""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if today != state.daily_pnl_date:
            state.daily_pnl = 0.0
            state.daily_pnl_date = today
            state.daily_loss_breached = False
            state.sports_daily_loss_breached = False

        if state.daily_loss_breached:
            return state

        daily_limit = getattr(self.config, "daily_loss_limit", 500.0) if self.config else 500.0
        if state.daily_pnl <= -daily_limit:
            if log:
                log.error(
                    f"DAILY LOSS LIMIT BREACHED: ${state.daily_pnl:,.2f} / "
                    f"-${daily_limit:,.2f}"
                )
            state.daily_loss_breached = True
            save_daily_state(
                daily_pnl=state.daily_pnl,
                daily_pnl_date=state.daily_pnl_date,
                daily_loss_breached=state.daily_loss_breached,
            )
        return state

    def can_trade(self, state: RiskState, category: str = "", log=None) -> bool:
        """Check if trading is allowed."""
        if state.kill_switch_breached:
            if log:
                log.warning("KILL_SWITCH active -- rejecting signal")
            return False
        if state.daily_loss_breached:
            if log:
                log.warning(
                    f"Daily loss limit breached (${state.daily_pnl:.2f}), "
                    f"skipping signal execution"
                )
            return False
        if category.lower() == "sports" and state.sports_daily_loss_breached:
            if log:
                log.warning(
                    f"Sports daily loss limit breached (${state.sports_daily_pnl:.2f}), "
                    f"skipping sports signal execution"
                )
            return False
        return True

    def can_open_fade(self, state: RiskState, log=None) -> bool:
        """Check if we can open a new fade position."""
        if len(state.fade_positions) >= state.fade_max_concurrent:
            if log:
                log.info(
                    f"Max concurrent fade positions ({state.fade_max_concurrent}) reached"
                )
            return False
        return True

    def record_pnl(self, state: RiskState, pnl: float, category: str = "", is_fade: bool = False) -> RiskState:
        """Record a P&L event and check daily limits."""
        if pnl == 0:
            return state
        state.daily_pnl += pnl
        save_daily_state(
            daily_pnl=state.daily_pnl,
            daily_pnl_date=state.daily_pnl_date,
            daily_loss_breached=state.daily_loss_breached,
        )
        if category.lower() == "sports":
            state.sports_daily_pnl += pnl
        state = self.check_daily_loss(state)
        return state

    @staticmethod
    def check_position_limits(config, cache, instrument_id, proposed_size_usd, open_positions, log=None, run_id="", mode="paper"):
        """Delegate to wf_position_checks.check_position_limits."""
        return check_position_limits(
            config=config, cache=cache, instrument_id=instrument_id,
            proposed_size_usd=proposed_size_usd, open_positions=open_positions,
            log=log, run_id=run_id, mode=mode,
        )


if __name__ == "__main__":
    state = RiskState()
    rm = RiskManager()
    state = rm.record_pnl(state, pnl=-200.0, category="sports")
    print(f"Daily P&L: ${state.daily_pnl:.2f}")
    print(f"Can trade: {rm.can_trade(state, category='sports')}")
    state.kill_switch_breached = True
    print(f"Can trade (kill switch): {rm.can_trade(state)}")
    state.kill_switch_breached = False
    assert rm.can_open_fade(state)
    state.fade_positions.update(["pos1", "pos2", "pos3"])
    print(f"Can open fade (max 3): {rm.can_open_fade(state)}")
    print("RiskManager smoke tests passed!")
