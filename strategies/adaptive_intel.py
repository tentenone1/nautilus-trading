"""Adaptive Market Intelligence — Regime detection and dynamic category weighting.

Continuously learns which whale classes, market regimes, and categories deliver edge.
Adjusts Kelly multipliers, signal weights, and capital allocations based on:

1. Market regime detection (trending/neutral/volatile per category)
2. Category performance tracking (rolling WR and PnL by category)
3. Whale class performance by regime (which whales deliver edge when)
4. Dynamic category weight adjustment (boost winning categories, suppress losers)

The system assumes markets cycle through regimes and that edge is not static:
a strategy that works in trending markets may lose in volatile ones.

Usage:
    intel = AdaptiveIntel(db_path="data/trades.db")
    regime = intel.detect_regime("sports")
    weights = intel.get_category_weights()
    adjustments = intel.get_signal_adjustments(whale_name, category, regime)
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("AdaptiveIntel")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/adaptive_intel_state.json")

# ── Regime Definitions ────────────────────────────────────────────────────────

class Regime(Enum):
    TRENDING = "trending"       # Sustained directional moves, low whipsaw
    NEUTRAL = "neutral"         # Range-bound, no clear direction
    VOLATILE = "volatile"       # High variance, frequent reversals
    CRISIS = "crisis"           # Extreme dislocations, wide spreads


# ── Default Category Performance ──────────────────────────────────────────────
# Calibrated from 2,661 trades baseline
DEFAULT_CATEGORY_PERF: dict[str, dict] = {
    "general":      {"wr": 0.46, "pnl": 93632, "trades": 808,  "weight": 0.50},
    "sports":       {"wr": 0.37, "pnl": 2996,  "trades": 845,  "weight": 0.15},
    "crypto":       {"wr": 0.49, "pnl": 0,     "trades": 503,  "weight": 0.25},
    "geopolitics":  {"wr": 0.22, "pnl": 0,     "trades": 23,   "weight": 0.05},
    "politics":     {"wr": 0.60, "pnl": 0,     "trades": 20,   "weight": 0.05},
    "economics":    {"wr": 0.33, "pnl": 0,     "trades": 12,   "weight": 0.00},
    "weather":      {"wr": 0.00, "pnl": 0,     "trades": 1,    "weight": 0.00},
    "technology":   {"wr": 0.00, "pnl": 0,     "trades": 6,    "weight": 0.00},
}

# ── Regime-Aware Kelly Adjustments ───────────────────────────────────────────
# Kelly fraction multipliers by regime and category
KELLY_REGIME_ADJUSTMENTS: dict[str, dict[str, float]] = {
    "trending":  {"general": 1.0, "sports": 0.7, "crypto": 1.1, "geopolitics": 0.5, "politics": 1.2},
    "neutral":  {"general": 0.8, "sports": 0.5, "crypto": 0.6, "geopolitics": 0.3, "politics": 0.8},
    "volatile": {"general": 0.5, "sports": 0.3, "crypto": 0.4, "geopolitics": 0.2, "politics": 0.5},
    "crisis":   {"general": 0.3, "sports": 0.1, "crypto": 0.2, "geopolitics": 0.1, "politics": 0.3},
}

# ── Regime Signal Confidence Adjustments ─────────────────────────────────────
# Boost/suppress signal confidence based on regime
CONFIDENCE_REGIME_ADJUSTMENTS: dict[str, float] = {
    "trending": 1.1,   # Trending markets: signals more reliable
    "neutral":  0.9,   # Neutral: reduce confidence
    "volatile": 0.7,   # Volatile: significant confidence reduction
    "crisis":   0.5,   # Crisis: heavy discount
}

# Minimum recent trades to determine regime
MIN_REGIME_TRADES = 20
# Lookback window for regime detection (hours)
REGIME_LOOKBACK_HOURS = 168  # 7 days
# Weight of recent vs historical performance (0-1, higher = more recent-weighted)
RECENCY_WEIGHT = 0.6


@dataclass
class CategoryPerformance:
    """Rolling performance metrics for a category."""
    category: str
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    trade_count: int = 0
    recent_wr: float = 0.0       # Last 7 days WR
    recent_pnl: float = 0.0      # Last 7 days PnL
    recent_count: int = 0         # Last 7 days trade count
    regime: str = "neutral"
    weight: float = 0.0           # Dynamic weight (0-1)
    kelly_multiplier: float = 1.0
    confidence_adjust: float = 1.0


@dataclass
class WhaleRegimePerformance:
    """How a whale class performs in different regimes."""
    whale_name: str
    trending_wr: float = 0.0
    trending_pnl: float = 0.0
    trending_count: int = 0
    neutral_wr: float = 0.0
    neutral_pnl: float = 0.0
    neutral_count: int = 0
    volatile_wr: float = 0.0
    volatile_pnl: float = 0.0
    volatile_count: int = 0


class AdaptiveIntel:
    """Adaptive market intelligence for dynamic regime-aware trading.

    Detects market regimes from recent trade data, adjusts category weights
    and Kelly multipliers accordingly, and learns which whale classes deliver
    edge in which regimes.

    Designed to be called from the signal pipeline:
        intel = AdaptiveIntel()
        adjustments = intel.get_signal_adjustments(whale, category, regime)
        # Use adjustments.kelly_multiplier, adjustments.confidence_adjust, etc.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        state_path: str | Path | None = None,
        lookback_hours: int = REGIME_LOOKBACK_HOURS,
        min_trades: int = MIN_REGIME_TRADES,
        recency_weight: float = RECENCY_WEIGHT,
    ):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.state_path = Path(state_path) if state_path else STATE_PATH
        self.lookback_hours = lookback_hours
        self.min_trades = min_trades
        self.recency_weight = recency_weight

        # Live state
        self._category_perf: dict[str, CategoryPerformance] = {}
        self._whale_regime_perf: dict[str, WhaleRegimePerformance] = {}
        self._last_update: datetime | None = None

        # Initialize from defaults
        for cat, perf in DEFAULT_CATEGORY_PERF.items():
            self._category_perf[cat] = CategoryPerformance(
                category=cat,
                win_rate=perf["wr"],
                total_pnl=perf["pnl"],
                trade_count=perf["trades"],
                weight=perf["weight"],
            )

        # Load persisted state
        self._load_state()

    def refresh(self) -> None:
        """Refresh all metrics from the database. Called periodically (e.g. every 5 min)."""
        self._refresh_category_performance()
        self._detect_all_regimes()
        self._compute_whale_regime_performance()
        self._adjust_category_weights()
        self._last_update = datetime.now(timezone.utc)
        self._save_state()

    # ── Category Performance ───────────────────────────────────────────────

    def _refresh_category_performance(self) -> None:
        """Query trades.db for per-category performance, split recent vs historical."""
        if not self.db_path.exists():
            logger.warning("trades.db not found at %s", self.db_path)
            return

        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)).isoformat()

            # Historical performance
            rows = conn.execute("""
                SELECT category,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                       ROUND(SUM(realized_pnl), 2) as total_pnl,
                       ROUND(AVG(realized_pnl), 2) as avg_pnl
                FROM trades
                WHERE realized_pnl IS NOT NULL AND category IS NOT NULL
                GROUP BY category
            """).fetchall()

            # Recent performance (last 7 days)
            recent_rows = conn.execute("""
                SELECT category,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                       ROUND(SUM(realized_pnl), 2) as total_pnl
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND category IS NOT NULL
                  AND timestamp >= ?
                GROUP BY category
            """, (cutoff,)).fetchall()

            recent_map = {r["category"]: r for r in recent_rows}

            for row in rows:
                cat = row["category"]
                recent = recent_map.get(cat)
                perf = self._category_perf.get(cat, CategoryPerformance(category=cat))
                perf.win_rate = row["win_rate"] or 0.0
                perf.total_pnl = row["total_pnl"] or 0.0
                perf.avg_pnl = row["avg_pnl"] or 0.0
                perf.trade_count = row["trades"] or 0

                if recent:
                    perf.recent_wr = recent["win_rate"] or 0.0
                    perf.recent_pnl = recent["total_pnl"] or 0.0
                    perf.recent_count = recent["trades"] or 0
                else:
                    perf.recent_wr = perf.win_rate
                    perf.recent_pnl = 0.0
                    perf.recent_count = 0

                self._category_perf[cat] = perf

        finally:
            conn.close()

    # ── Regime Detection ──────────────────────────────────────────────────

    def _detect_all_regimes(self) -> None:
        """Detect current regime for each category based on recent trading."""
        for cat in list(self._category_perf.keys()):
            perf = self._category_perf[cat]
            perf.regime = self._detect_regime(cat).value

    def _detect_regime(self, category: str) -> Regime:
        """Detect market regime for a category from recent trade data.

        Uses WR variance and PnL direction to classify regime:
        - TRENDING: High WR with consistent positive PnL
        - VOLATILE: Low WR, wide PnL swings, many losses
        - CRISIS: Very low WR, large negative PnL
        - NEUTRAL: Default / insufficient data
        """
        perf = self._category_perf.get(category)
        if not perf or perf.trade_count < self.min_trades:
            return Regime.NEUTRAL

        # Blend recent and historical WR
        if perf.recent_count >= 5:
            blended_wr = (
                self.recency_weight * perf.recent_wr
                + (1 - self.recency_weight) * perf.win_rate
            )
            blended_pnl = (
                self.recency_weight * perf.recent_pnl
                + (1 - self.recency_weight) * perf.total_pnl
            )
        else:
            blended_wr = perf.win_rate
            blended_pnl = perf.total_pnl

        # Regime classification
        if blended_wr >= 0.50 and blended_pnl > 0:
            return Regime.TRENDING
        elif blended_wr <= 0.25 or blended_pnl < -5000:
            return Regime.CRISIS
        elif blended_wr < 0.35 or blended_pnl < -500:
            return Regime.VOLATILE
        else:
            return Regime.NEUTRAL

    # ── Whale-Regime Performance ──────────────────────────────────────────

    def _compute_whale_regime_performance(self) -> None:
        """Compute per-whale performance in each regime (requires regime labels on trades)."""
        if not self.db_path.exists():
            return

        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("""
                SELECT whale_name, category,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                       ROUND(SUM(realized_pnl), 2) as total_pnl
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                GROUP BY whale_name, category
                HAVING COUNT(*) >= 3
            """).fetchall()

            for row in rows:
                name = row["whale_name"]
                cat = row["category"]
                # Get the current regime for this category
                cat_regime = self._category_perf.get(cat, CategoryPerformance(category=cat)).regime

                if name not in self._whale_regime_perf:
                    self._whale_regime_perf[name] = WhaleRegimePerformance(whale_name=name)

                wp = self._whale_regime_perf[name]
                if cat_regime == "trending":
                    # Blend: weighted average of existing and new
                    n = wp.trending_count
                    wp.trending_wr = (wp.trending_wr * n + (row["win_rate"] or 0) * row["trades"]) / max(n + row["trades"], 1)
                    wp.trending_pnl += (row["total_pnl"] or 0)
                    wp.trending_count += row["trades"]
                elif cat_regime == "volatile":
                    n = wp.volatile_count
                    wp.volatile_wr = (wp.volatile_wr * n + (row["win_rate"] or 0) * row["trades"]) / max(n + row["trades"], 1)
                    wp.volatile_pnl += (row["total_pnl"] or 0)
                    wp.volatile_count += row["trades"]
                elif cat_regime == "crisis":
                    n = wp.neutral_count  # reuse neutral for crisis
                    wp.neutral_wr = (wp.neutral_wr * n + (row["win_rate"] or 0) * row["trades"]) / max(n + row["trades"], 1)
                    wp.neutral_pnl += (row["total_pnl"] or 0)
                    wp.neutral_count += row["trades"]
                else:
                    n = wp.neutral_count
                    wp.neutral_wr = (wp.neutral_wr * n + (row["win_rate"] or 0) * row["trades"]) / max(n + row["trades"], 1)
                    wp.neutral_pnl += (row["total_pnl"] or 0)
                    wp.neutral_count += row["trades"]
        finally:
            conn.close()

    # ── Dynamic Weight Adjustment ─────────────────────────────────────────

    def _adjust_category_weights(self) -> None:
        """Adjust category weights based on blended performance.

        Categories with positive edge (WR > breakeven, positive PnL) get boosted.
        Losing categories get suppressed. Weights are normalized to sum to 1.0
        across allowed categories.
        """
        allowed = {"general", "sports", "crypto", "geopolitics", "politics"}
        raw_weights: dict[str, float] = {}

        for cat, perf in self._category_perf.items():
            if cat not in allowed:
                continue

            # Blend recent and historical
            if perf.recent_count >= 5:
                blended_wr = self.recency_weight * perf.recent_wr + (1 - self.recency_weight) * perf.win_rate
                blended_pnl = self.recency_weight * perf.recent_pnl + (1 - self.recency_weight) * perf.total_pnl
            else:
                blended_wr = perf.win_rate
                blended_pnl = perf.total_pnl

            # Base weight from WR edge over 0.40 breakeven (prediction market)
            wr_edge = max(0, blended_wr - 0.40)
            pnl_signal = 1.0 if blended_pnl > 0 else 0.5 if blended_pnl > -500 else 0.2

            # Regime adjustments
            regime_mult = KELLY_REGIME_ADJUSTMENTS.get(perf.regime, {}).get(cat, 0.8)

            raw_weights[cat] = wr_edge * pnl_signal * regime_mult

        # Normalize
        total = sum(raw_weights.values()) or 1.0
        for cat in raw_weights:
            self._category_perf[cat].weight = round(raw_weights[cat] / total, 3)

        # Set Kelly multiplier and confidence adjust from regime
        for cat, perf in self._category_perf.items():
            regime = perf.regime
            perf.kelly_multiplier = KELLY_REGIME_ADJUSTMENTS.get(regime, {}).get(cat, 0.8)
            perf.confidence_adjust = CONFIDENCE_REGIME_ADJUSTMENTS.get(regime, 0.9)

    # ── Public API ─────────────────────────────────────────────────────────

    def detect_regime(self, category: str) -> Regime:
        """Get the current regime for a category."""
        perf = self._category_perf.get(category)
        if perf:
            return Regime(perf.regime)
        return Regime.NEUTRAL

    def get_category_weights(self) -> dict[str, float]:
        """Get current category weights (sum to ~1.0)."""
        return {cat: perf.weight for cat, perf in self._category_perf.items() if perf.weight > 0}

    def get_kelly_multiplier(self, category: str) -> float:
        """Get regime-aware Kelly multiplier for a category."""
        perf = self._category_perf.get(category)
        if perf:
            return perf.kelly_multiplier
        return 0.8  # Default conservative

    def get_confidence_adjust(self, category: str) -> float:
        """Get regime-aware confidence adjustment for a category."""
        perf = self._category_perf.get(category)
        if perf:
            return perf.confidence_adjust
        return 0.9  # Default slight reduction

    def get_signal_adjustments(self, whale_name: str, category: str, regime: str | None = None) -> dict:
        """Get all regime-aware adjustments for a signal.

        Returns dict with:
            kelly_multiplier: float (regime-adjusted Kelly multiplier)
            confidence_adjust: float (confidence scaling factor)
            weight: float (category weight)
            regime: str (detected regime)
            whale_regime_wr: float (whale's WR in this regime, 0 if unknown)
        """
        if regime is None:
            regime = self._category_perf.get(category, CategoryPerformance(category=category)).regime

        perf = self._category_perf.get(category, CategoryPerformance(category=category))

        # Whale-specific regime performance
        wp = self._whale_regime_perf.get(whale_name)
        whale_regime_wr = 0.0
        if wp:
            if regime == "trending":
                whale_regime_wr = wp.trending_wr
            elif regime == "volatile":
                whale_regime_wr = wp.volatile_wr
            else:
                whale_regime_wr = wp.neutral_wr

        return {
            "kelly_multiplier": perf.kelly_multiplier,
            "confidence_adjust": perf.confidence_adjust,
            "weight": perf.weight,
            "regime": regime,
            "whale_regime_wr": whale_regime_wr,
        }

    def get_performance_snapshot(self) -> dict:
        """Get a full snapshot of all adaptive intelligence state."""
        return {
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "categories": {
                cat: {
                    "win_rate": perf.win_rate,
                    "recent_wr": perf.recent_wr,
                    "total_pnl": perf.total_pnl,
                    "recent_pnl": perf.recent_pnl,
                    "trade_count": perf.trade_count,
                    "recent_count": perf.recent_count,
                    "regime": perf.regime,
                    "weight": perf.weight,
                    "kelly_multiplier": perf.kelly_multiplier,
                    "confidence_adjust": perf.confidence_adjust,
                }
                for cat, perf in self._category_perf.items()
            },
            "whale_regime_count": len(self._whale_regime_perf),
        }

    # ── Persistence ────────────────────────────────────────────────────────

    def _save_state(self) -> None:
        """Persist adaptive intelligence state to JSON."""
        data = {
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "category_perf": {
                cat: asdict(perf) for cat, perf in self._category_perf.items()
            },
            "whale_regime_perf": {
                name: asdict(wp) for name, wp in self._whale_regime_perf.items()
            },
        }
        self.state_path.write_text(json.dumps(data, indent=2, default=str))

    def _load_state(self) -> None:
        """Load persisted state from JSON."""
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text())
            for cat, perf_data in data.get("category_perf", {}).items():
                perf = CategoryPerformance(**perf_data)
                self._category_perf[cat] = perf
            for name, wp_data in data.get("whale_regime_perf", {}).items():
                wp = WhaleRegimePerformance(**wp_data)
                self._whale_regime_perf[name] = wp
            if data.get("last_update"):
                self._last_update = datetime.fromisoformat(data["last_update"])
        except Exception as e:
            logger.warning("Failed to load adaptive intel state: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    intel = AdaptiveIntel()
    intel.refresh()

    print("\n=== Adaptive Market Intelligence ===")
    print(f"Last update: {intel._last_update}")
    print()
    for cat, perf in sorted(intel._category_perf.items(), key=lambda x: -x[1].weight):
        print(f"  {cat:15s}: WR={perf.win_rate:.1%} recent_WR={perf.recent_wr:.1%} regime={perf.regime:10s} weight={perf.weight:.3f} kelly={perf.kelly_multiplier:.2f} conf={perf.confidence_adjust:.2f}")

    print("\n=== Signal Adjustments (sample) ===")
    for cat in ["general", "sports", "crypto"]:
        adj = intel.get_signal_adjustments("sample_whale", cat)
        print(f"  {cat:15s}: kelly={adj['kelly_multiplier']:.2f} conf={adj['confidence_adjust']:.2f} regime={adj['regime']}")
