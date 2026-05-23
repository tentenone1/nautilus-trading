"""Capital Pool — Shared capital allocator for per-category sub-strategies.

Manages a single shared bankroll split across 3 independent sub-strategies:
  Sports  (50%), Crypto (35%), Geopolitics (15%)

Each sub-strategy operates within its allocated slice. The pool enforces:
  - Per-category exposure caps (allocation % × total bankroll)
  - Per-category max concurrent positions
  - Global exposure cap (sum of all category exposures)

Usage:
    pool = CapitalPool(total_bankroll=10_000.0)

    # Before placing a trade
    allocated = pool.request_capital("crypto", desired_size=500.0)
    if allocated <= 0:
        reject("Crypto allocation exhausted")

    # After settlement
    pool.release_capital("crypto", pnl=+72.0)   # win
    pool.release_capital("crypto", pnl=-31.0)   # loss
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar


# Default allocation percentages per category
DEFAULT_ALLOCATIONS: dict[str, float] = {
    "sports": 0.50,
    "crypto": 0.35,
    "geopolitics": 0.15,
}

# Max concurrent positions per category
DEFAULT_MAX_POSITIONS: dict[str, int] = {
    "sports": 10,
    "crypto": 8,
    "geopolitics": 3,
}

# Fade position allocation: percentage of each category's allocation reserved for FADE trades.
# Fade trades (trading opposite to consistently losing whales) are an independent signal source,
# so they get their own slice within each category.
DEFAULT_FADE_ALLOCATION_PCT: float = 0.20  # 20% of each category's allocation goes to fade


@dataclass
class CategoryState:
    """Live state for one category slice."""

    allocation_pct: float = 0.0       # e.g. 0.50 for sports
    max_positions: int = 0            # e.g. 10 for sports
    current_exposure: float = 0.0     # sum of open position sizes (USD)
    open_position_count: int = 0      # number of open positions
    total_pnl: float = 0.0           # cumulative realized P&L for this category
    # Fade-specific tracking
    fade_allocation_pct: float = 0.0  # percentage of THIS category's allocation for fade
    fade_exposure: float = 0.0        # current exposure in fade positions
    fade_pnl: float = 0.0            # cumulative P&L from fade positions
    fade_open_count: int = 0         # number of open fade positions

    def available_capacity(self, total_bankroll: float) -> float:
        """How much more this category can deploy."""
        cap = total_bankroll * self.allocation_pct
        return max(0.0, cap - self.current_exposure)

    def fade_available(self, total_bankroll: float) -> float:
        """How much fade capacity remains within this category's fade bucket."""
        fade_cap = total_bankroll * self.allocation_pct * self.fade_allocation_pct
        return max(0.0, fade_cap - self.fade_exposure)

    def request(self, amount: float, total_bankroll: float) -> float:
        """Try to allocate `amount`. Returns what was actually granted (0 if exhausted)."""
        granted = min(amount, self.available_capacity(total_bankroll))
        if granted > 0:
            self.current_exposure += granted
        return granted

    def request_fade(self, amount: float, total_bankroll: float) -> float:
        """Try to allocate `amount` from the fade bucket. Returns what was granted."""
        granted = min(amount, self.fade_available(total_bankroll))
        if granted > 0:
            self.fade_exposure += granted
            self.current_exposure += granted
            self.fade_open_count += 1
        return granted

    def release(self, pnl: float, position_size: float, is_fade: bool = False) -> None:
        """Record settlement and return capital to the pool.

        Args:
            pnl: Realized P&L (positive = profit, negative = loss).
            position_size: Original position size - used to clear exposure, NOT pnl.
            is_fade: Whether this was a fade position (tracks P&L separately).
        """
        # Exposure is cleared by the full position size (capital returns to pool)
        self.current_exposure = max(0.0, self.current_exposure - position_size)
        # P&L is added separately (can be positive or negative)
        self.total_pnl += pnl
        # Track fade positions separately
        if is_fade:
            self.fade_exposure = max(0.0, self.fade_exposure - position_size)
            self.fade_pnl += pnl

    def close_position(self) -> None:
        """Mark one position closed (reduces count).
        Call release() separately to adjust exposure and P&L."""
        self.open_position_count = max(0, self.open_position_count - 1)


class CapitalPool:
    """Shared capital pool with per-category allocation and position enforcement.

    Attributes:
        total_bankroll: Total bankroll across all categories.
        allocations: Per-category allocation fractions. Must sum to 1.0.
        max_positions: Per-category max concurrent position count.
        global_exposure_cap: Global exposure cap as fraction of total bankroll (default 0.60).
    """

    # Known categories (lowercase keys throughout)
    CATEGORIES: ClassVar[list[str]] = ["sports", "crypto", "geopolitics"]

    def __init__(
        self,
        total_bankroll: float,
        allocations: dict[str, float] | None = None,
        max_positions: dict[str, int] | None = None,
        global_exposure_cap: float = 0.60,
        fade_allocation_pct: float = DEFAULT_FADE_ALLOCATION_PCT,
    ) -> None:
        if total_bankroll <= 0:
            raise ValueError(f"total_bankroll must be positive, got {total_bankroll}")

        self.total_bankroll = total_bankroll
        self.global_exposure_cap = global_exposure_cap
        self._allocations = allocations or dict(DEFAULT_ALLOCATIONS)
        self._max_positions = max_positions or dict(DEFAULT_MAX_POSITIONS)

        # Validate allocations sum to ~1.0
        total_alloc = sum(self._allocations.values())
        if abs(total_alloc - 1.0) > 0.001:
            raise ValueError(
                f"Allocations must sum to 1.0, got {total_alloc:.4f}: {self._allocations}"
            )

        self._fade_allocation_pct = fade_allocation_pct
        self._category_states: dict[str, CategoryState] = {
            cat: CategoryState(
                allocation_pct=self._allocations[cat],
                max_positions=self._max_positions.get(cat, 0),
                fade_allocation_pct=self._fade_allocation_pct,
            )
            for cat in self.CATEGORIES
        }

    # ── Public API ──────────────────────────────────────────────────────────

    def request_capital(
        self,
        category: str,
        desired_size: float,
        position_size: float | None = None,
    ) -> float:
        """Request capital for a new position in `category`.

        Args:
            category: One of sports, crypto, geopolitics.
            desired_size: The Kelly-sized position size in USD.
            position_size: If provided, used instead of desired_size for exposure
                tracking (allows requesting more than granted when cap is hit).

        Returns:
            The amount actually granted (0 if allocation exhausted or max positions reached).

        Raises:
            KeyError: If category is not a known category.
        """
        cat = category.lower()
        if cat not in self._category_states:
            raise KeyError(f"Unknown category: {category}. Known: {self.CATEGORIES}")

        state = self._category_states[cat]

        # Check max concurrent positions
        if state.open_position_count >= state.max_positions:
            return 0.0

        # Check global exposure cap
        total_exposure = sum(s.current_exposure for s in self._category_states.values())
        global_cap = self.total_bankroll * self.global_exposure_cap
        if total_exposure >= global_cap:
            return 0.0

        granted = state.request(desired_size, self.total_bankroll)

        if granted > 0:
            state.open_position_count += 1

        return granted

    def request_fade_capital(
        self,
        category: str,
        desired_size: float,
    ) -> float:
        """Request capital from the fade bucket within a category.

        Fade positions trade opposite to consistently losing whales.
        They use a dedicated portion of each category's allocation.

        Returns:
            Amount of capital actually granted (0 if fade bucket exhausted
            or category would exceed max positions).
        """
        cat = category.lower()
        if cat not in self._category_states:
            cat = "general" if "general" in self._category_states else self.CATEGORIES[0]

        state = self._category_states.get(cat)
        if state is None:
            return 0.0

        # Check max positions (fade positions count toward total)
        if state.open_position_count >= state.max_positions:
            return 0.0

        # Check global exposure cap
        total_exposure = sum(s.current_exposure for s in self._category_states.values())
        max_global = self.total_bankroll * self.global_exposure_cap
        if total_exposure >= max_global:
            return 0.0

        # Check fade bucket capacity
        granted = state.request_fade(
            min(desired_size, self.total_bankroll * 0.02),  # max 2% per fade position
            self.total_bankroll,
        )

        return granted

    def release_capital(
        self,
        category: str,
        pnl: float,
        position_size: float,
        is_fade: bool = False,
    ) -> None:
        """Release capital after position settlement.

        Args:
            category: Category the position was in.
            pnl: Realized P&L (positive = profit, negative = loss).
            position_size: Original position size (used to reduce exposure).
            is_fade: Whether this was a fade position (tracks P&L separately).
        """
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return
        state.release(pnl, position_size, is_fade=is_fade)
        state.close_position()

    def get_category_allocation(self, category: str) -> float:
        """Return the allocated capital for a category (total_bankroll × allocation_pct)."""
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return 0.0
        return self.total_bankroll * state.allocation_pct

    def get_category_available(self, category: str) -> float:
        """Return remaining deployable capital for a category."""
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return 0.0
        return state.available_capacity(self.total_bankroll)

    def get_category_exposure(self, category: str) -> float:
        """Return current total exposure for a category."""
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return 0.0
        return state.current_exposure

    def get_total_exposure(self) -> float:
        """Return sum of all category exposures."""
        return sum(s.current_exposure for s in self._category_states.values())

    def get_category_pnl(self, category: str) -> float:
        """Return cumulative realized P&L for a category."""
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return 0.0
        return state.total_pnl

    def update_bankroll(self, new_total: float) -> None:
        """Update total bankroll (called when account balance changes)."""
        if new_total <= 0:
            raise ValueError(f"new_total must be positive, got {new_total}")
        self.total_bankroll = new_total

    def get_state_snapshot(self) -> dict:
        """Return a full snapshot of pool state for logging/debugging."""
        return {
            "total_bankroll": self.total_bankroll,
            "total_exposure": self.get_total_exposure(),
            "global_exposure_pct": self.get_total_exposure() / self.total_bankroll
            if self.total_bankroll > 0
            else 0.0,
            "categories": {
                cat: {
                    "allocation_pct": state.allocation_pct,
                    "allocated": self.total_bankroll * state.allocation_pct,
                    "current_exposure": state.current_exposure,
                    "available": state.available_capacity(self.total_bankroll),
                    "fade_exposure": state.fade_exposure,
                    "fade_available": state.fade_available(self.total_bankroll),
                    "fade_pnl": state.fade_pnl,
                    "open_positions": state.open_position_count,
                    "fade_open_count": state.fade_open_count,
                    "max_positions": state.max_positions,
                    "total_pnl": state.total_pnl,
                }
                for cat, state in self._category_states.items()
            },
        }

    def can_open_position(self, category: str) -> bool:
        """Return True if category can accept another position."""
        cat = category.lower()
        state = self._category_states.get(cat)
        if state is None:
            return False
        return (
            state.open_position_count < state.max_positions
            and state.available_capacity(self.total_bankroll) > 0
        )


# ── Tests ─────────────────────────────────────────────────────────────────────

def _run_tests() -> None:
    import math

    pool = CapitalPool(total_bankroll=10_000.0)

    snap = pool.get_state_snapshot()
    assert abs(snap["categories"]["sports"]["allocated"] - 5000.0) < 0.01, "Sports 50% allocation"
    assert abs(snap["categories"]["crypto"]["allocated"] - 3500.0) < 0.01, "Crypto 35% allocation"
    assert abs(snap["categories"]["geopolitics"]["allocated"] - 1500.0) < 0.01, "Geo 15% allocation"
    print("  ✓ allocation percentages correct")

    # Test request capital
    granted = pool.request_capital("crypto", desired_size=500.0)
    assert granted == 500.0, f"Expected 500, got {granted}"
    assert pool.get_category_exposure("crypto") == 500.0
    assert pool.get_category_available("crypto") == 3000.0
    print("  ✓ request_capital works")

    # Test max positions: after first request (count=1), only 7 more fit (max=8)
    success_count = 0
    for i in range(8):
        g = pool.request_capital("crypto", desired_size=100.0)
        if g > 0:
            success_count += 1
    print(f"  {success_count} positions granted before rejection (expected 7)")
    assert success_count == 7, f"Expected 7 grants, got {success_count}"
    # Next one must be rejected
    rejected = pool.request_capital("crypto", desired_size=100.0)
    assert rejected == 0.0, f"Should be rejected, got {rejected}"
    print("  ✓ max positions enforced")

    # Release ALL crypto positions to reset for next test
    pool.release_capital("crypto", pnl=72.0, position_size=500.0)   # initial $500
    for _ in range(7):
        pool.release_capital("crypto", pnl=20.0, position_size=100.0)
    assert pool.get_category_exposure("crypto") == 0.0
    assert pool._category_states["crypto"].open_position_count == 0
    print("  ✓ release_capital works")

    # Test total exposure across categories
    pool.request_capital("sports", desired_size=1000.0)
    pool.request_capital("geopolitics", desired_size=500.0)
    assert abs(pool.get_total_exposure() - 1500.0) < 0.01
    print("  ✓ total exposure correct")

    # Test bankroll update
    pool.update_bankroll(15_000.0)
    assert pool.get_category_allocation("crypto") == 5250.0  # 35% × 15000
    print("  ✓ update_bankroll works")

    # Test can_open_position
    assert pool.can_open_position("geopolitics") == True
    for i in range(3):  # max 3 for geo
        pool.request_capital("geopolitics", desired_size=200.0)
    assert pool.can_open_position("geopolitics") == False
    print("  ✓ can_open_position works")

    print("\nAll CapitalPool tests passed ✓")


if __name__ == "__main__":
    _run_tests()
