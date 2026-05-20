"""Base Whale Follower Strategy — abstract base for per-category sub-strategies.

Each category (Sports, Crypto, Geopolitics) gets its own concrete subclass that
defines:
  - Kelly fraction and category-specific cap
  - Max concurrent positions
  - Allocation percentage of shared capital pool
  - Category-specific filters (edge, confidence, whale size, etc.)

The base class provides shared signal-processing utilities (wf_signal_proc),
Kelly sizing (wf_kelly), and order-execution primitives that subclasses inherit
without overriding.

Migration path:
  Phase 2 (this file): Extract ABC, parameterise existing logic, no behaviour change.
  Phase 3: Implement concrete Sport/Crypto/Geo strategy classes.
  Phase 4: Wire CapitalPool into _open_position().
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from strategies.capital_pool import CapitalPool

#: Type alias to avoid runtime import of CapitalPool (not in venv yet)
_CapitalPoolLike = "object"  # Actual type: CapitalPool


# -----------------------------------------------------------------------
# Per-category parameter schemas
# -----------------------------------------------------------------------

@dataclass(frozen=True)
class CategoryParams:
    """Immutable per-category trading parameters."""

    # Kelly sizing
    kelly_fraction: float           # e.g. 0.20 for Sports
    kelly_cap: float                # Hard cap applied in kelly_size() e.g. 0.15 for Crypto
    max_single_position_pct: float  # Hard cap on single position as % of bankroll

    # Exposure
    max_concurrent: int             # Max open positions for this category
    capital_allocation_pct: float  # % of shared bankroll allocated to this category

    # Signal filters
    min_edge_score: float           # Minimum edge_score to accept signal
    min_confidence: float           # Minimum confidence to accept signal
    min_whale_size_usd: float       # Minimum suggested_size_usd to accept signal
    min_liquidity_volume: float      # Minimum 24h volume to accept (0 = no filter)


# -----------------------------------------------------------------------
# Abstract base strategy
# -----------------------------------------------------------------------

class BaseWhaleFollowerStrategy(ABC):
    """Abstract base for per-category whale-following strategies.

    Subclasses MUST override `category_name` and `params`.

    Shared infrastructure (inherited, not overridden):
      - wf_signal_proc.process_whale_signal() for signal validation/scoring
      - wf_kelly.kelly_size() for Kelly sizing
      - Nautilus order execution (_open_position, _close_position)

    Subclasses override:
      - category_name, params  — per-category configuration
      - validate_category_signal() — category-specific pre-filter (optional)
      - apply_category_overrides() — adjust size/edge/confidence before sizing
    """

    # Override in subclass
    category_name: ClassVar[str] = ""          # "sports", "crypto", "geopolitics"
    params: ClassVar[CategoryParams]          # set via @classmethod or class attrs

    # Shared resources (set by WhaleFollower on __init__)
    config: "object" = field(default=None, init=False, repr=False)
    tracker: "object" = field(default=None, init=False, repr=False)
    whale_tiering: "object" = field(default=None, init=False, repr=False)
    capital_pool: "CapitalPool | None" = field(default=None, init=False, repr=False)
    log: "object" = field(default=None, init=False, repr=False)

    # ------------------------------------------------------------------
    # Public API (called by WhaleFollower._open_position)
    # ------------------------------------------------------------------

    def can_accept_position(self) -> bool:
        """Return True if this category can accept another position."""
        if self.capital_pool is not None:
            return self.capital_pool.can_open_position(self.category_name)
        # Fallback: capital pool unavailable — fail closed
        self.log.warning("CapitalPool unavailable — rejecting position for %s", self.category_name)
        return False

    def request_capital(self, desired_size: float) -> float:
        """Request capital from the shared pool for a new position.

        Returns the amount actually granted (may be less than desired if
        category allocation or global cap is hit).
        """
        if self.capital_pool is None:
            # Fail-closed: without CapitalPool we cannot safely allocate
            self.log.warning("CapitalPool unavailable — cannot allocate capital for %s", self.category_name)
            return 0.0
        return self.capital_pool.request_capital(self.category_name, desired_size)

    def release_capital(self, pnl: float, position_size: float) -> None:
        """Release capital back to the pool after position settlement."""
        if self.capital_pool is not None:
            self.capital_pool.release_capital(self.category_name, pnl, position_size)

    def get_category_name(self) -> str:
        return self.category_name

    def get_allocation_pct(self) -> float:
        return self.params.capital_allocation_pct

    # ------------------------------------------------------------------
    # Category-specific hooks (override in subclass as needed)
    # ------------------------------------------------------------------

    def validate_category_signal(self, signal) -> bool:
        """Category-specific pre-filter before general signal processing.

        Return False to reject the signal early.
        Override in subclass for category-specific rules.
        Default: accept all signals that pass the general filters.
        """
        mc = getattr(signal, "market_category", None)
        if mc is None:
            self.log.warning(
                "Signal missing market_category — rejecting for %s",
                self.category_name,
            )
            return False
        if mc.lower() != self.category_name:
            return False  # Not for this category
        return True

    def apply_category_overrides(
        self,
        signal,
        kelly_fraction: float,
        max_position_pct: float,
    ) -> tuple[float, float]:
        """Adjust Kelly fraction and max_position_pct based on category rules.

        Called from _open_position() after signal passes validation but before
        kelly_size() is called.

        Args:
            signal: The WhaleSignal being processed.
            kelly_fraction: Base kelly_fraction from config.
            max_position_pct: Base max_position_pct from config.

        Returns:
            (adjusted_kelly_fraction, adjusted_max_position_pct)
        """
        # Default: apply Kelly cap from params
        capped = min(kelly_fraction, self.params.kelly_cap)
        return capped, max_position_pct

    def get_max_concurrent(self) -> int:
        return self.params.max_concurrent

    def get_min_edge_score(self) -> float:
        return self.params.min_edge_score

    def get_min_confidence(self) -> float:
        return self.params.min_confidence


# -----------------------------------------------------------------------
# Concrete strategy implementations
# -----------------------------------------------------------------------

class SportsStrategy(BaseWhaleFollowerStrategy):
    """Sports markets: 50% allocation, 20% Kelly, 10 concurrent max."""

    category_name: ClassVar[str] = "sports"
    params: ClassVar[CategoryParams] = CategoryParams(
        kelly_fraction=0.20,
        kelly_cap=0.20,       # Hard cap — matches kelly_fraction; was 1.0 (unlimited)
        max_single_position_pct=0.02,
        max_concurrent=10,
        capital_allocation_pct=0.50,
        min_edge_score=0.0,      # Controlled by SPORTS_MIN_EDGE in config
        min_confidence=0.0,      # Controlled by SPORTS_MIN_CONFIDENCE in config
        min_whale_size_usd=0.0,
        min_liquidity_volume=0.0,
    )


class CryptoStrategy(BaseWhaleFollowerStrategy):
    """Crypto markets: 35% allocation, 15% Kelly cap, 8 concurrent max."""

    category_name: ClassVar[str] = "crypto"
    params: ClassVar[CategoryParams] = CategoryParams(
        kelly_fraction=0.15,
        kelly_cap=0.15,          # Hard cap — high vol, 39% market-resolved loss rate
        max_single_position_pct=0.02,
        max_concurrent=8,
        capital_allocation_pct=0.35,
        min_edge_score=0.0,
        min_confidence=0.0,
        min_whale_size_usd=0.0,
        min_liquidity_volume=50_000.0,  # Min $50K 24h volume
    )


class GeopoliticsStrategy(BaseWhaleFollowerStrategy):
    """Geopolitics markets: 15% allocation, 10% Kelly cap, 3 concurrent max."""

    category_name: ClassVar[str] = "geopolitics"
    params: ClassVar[CategoryParams] = CategoryParams(
        kelly_fraction=0.10,
        kelly_cap=0.10,          # Hard cap — small sample, best avg win
        max_single_position_pct=0.02,
        max_concurrent=3,
        capital_allocation_pct=0.15,
        min_edge_score=0.0,
        min_confidence=0.0,
        min_whale_size_usd=5_000.0,   # Only enter if whale position > $5,000
        min_liquidity_volume=0.0,
    )


# -----------------------------------------------------------------------
# Strategy registry
# -----------------------------------------------------------------------

STRATEGY_REGISTRY: dict[str, type[BaseWhaleFollowerStrategy]] = {
    "sports": SportsStrategy,
    "crypto": CryptoStrategy,
    "geopolitics": GeopoliticsStrategy,
}


def get_strategy(category: str) -> BaseWhaleFollowerStrategy:
    """Factory: return a new instance of the strategy for `category`."""
    cls = STRATEGY_REGISTRY.get(category.lower())
    if cls is None:
        raise ValueError(f"No strategy for category: {category}")
    return cls()
