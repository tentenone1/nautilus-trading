"""Edge Scorer — Data-driven edge estimation replacing the anti-predictive legacy edge_score.

The legacy edge_score is anti-predictive:
  - edge_score 0.8 → 47.2% WR, -$3,163 PnL
  - edge_score 0.5 → 38.2% WR, +$1,492 PnL

This module computes edge from three data-driven sources:
  1. Whale classification (copy/fade/ignore) and action confidence
  2. Per-whale-per-category trust scores (0-10)
  3. Historical category performance (actual WR and PnL by category)

Edge formula:
  base_edge = whale_action_confidence * trust_weight + category_wr * cat_weight
  For FADE signals, edge = (1 - whale_win_rate) * fade_multiplier
  For COPY signals, edge = whale_win_rate * copy_multiplier
  Trust scores modulate the final edge: high trust → boost, low trust → suppress

All queries go to trades.db + whale_classifications.json. No external deps.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("EdgeScorer")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
CLASSIFICATIONS_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_classifications.json")

# ── Category performance from historical data ─────────────────────────────────
# Source: SELECT category, COUNT(*), AVG(win), SUM(pnl) FROM trades
# These are calibrated from 2,661 trades. Updated by refresh_category_performance().
CATEGORY_WEIGHTS: dict[str, float] = {
    "general": 0.65,      # 46% WR, +$93,632 PnL — best category
    "sports": 0.30,       # 40% WR, +$2,996 PnL — marginal
    "crypto": 0.15,       # 39% WR, -$5,312 PnL — losing
    "geopolitics": 0.10,  # 15% WR — tiny sample
    "entertainment": 0.0,  # blocked
    "finance": 0.0,        # blocked
    "unknown": 0.05,       # minimal
}

# ── Edge scoring weights ─────────────────────────────────────────────────────
WHALE_ACTION_WEIGHT = 0.50    # 50% weight on whale action (copy/fade/ignore)
CATEGORY_PERF_WEIGHT = 0.25  # 25% weight on category historical performance
TRUST_WEIGHT = 0.25          # 25% weight on per-whale-per-category trust

# ── Action multipliers ───────────────────────────────────────────────────────
COPY_WIN_RATE_BOOST = 1.3     # Copying a profitable whale: 30% boost
FADE_WIN_RATE_BOOST = 1.5     # Fading a losing whale: 50% boost (stronger signal)
IGNORE_MULTIPLIER = 0.0       # Ignored whales get zero edge

# ── Trust score modulation ───────────────────────────────────────────────────
TRUST_HIGH_THRESHOLD = 7.0    # Trust >= 7: boost edge by 20%
TRUST_LOW_THRESHOLD = 3.0     # Trust < 3: suppress edge by 40%
TRUST_HIGH_BOOST = 1.2
TRUST_LOW_SUPPRESS = 0.6


@dataclass
class EdgeResult:
    """Result of edge scoring computation."""
    edge_score: float              # Final calibrated edge (0.0 - 1.0)
    raw_edge: float                # Pre-calibration raw edge
    action: str                    # copy, fade, or ignore
    action_confidence: float       # Confidence in the action (from classifier)
    whale_trust: float             # Trust score for this whale in this category
    category_weight: float         # Category weight used
    source: str                    # "classifier" | "fallback" | "static"
    should_trade: bool             # Whether to enter this trade
    side_flip: bool                # Whether to flip the signal side (for fade)


class EdgeScorer:
    """Data-driven edge scorer using whale classification, trust, and category performance.

    Replaces the legacy edge_score which was anti-predictive. This scorer:
    1. Loads whale classifications from whale_classifications.json
    2. Builds category performance from trades.db
    3. Computes per-signal edge using whale action + trust + category
    4. Returns an EdgeResult with calibrated edge and trading decision

    Usage:
        scorer = EdgeScorer()
        result = scorer.score_signal(
            whale_name="p37-0xe5efd6",
            category="sports",
            raw_edge_score=0.8,    # legacy edge (used as fallback only)
            confidence=0.75,
            side="BUY",
        )
        if result.should_trade:
            side = "SELL" if result.side_flip else "BUY"
            enter_position(edge_score=result.edge_score, side=side, ...)
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        classifications_path: str | Path | None = None,
    ):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.classifications_path = Path(classifications_path) if classifications_path else CLASSIFICATIONS_PATH
        self._classifications: dict[str, dict] = {}
        self._trust_scores: dict[str, dict[str, float]] = {}
        self._category_performance: dict[str, dict] = {}
        self._last_refresh: datetime | None = None
        self._refresh_interval_hours: float = 4.0  # refresh every 4 hours
        self._load_classifications()
        self._build_category_performance()

    # ── Data Loading ──────────────────────────────────────────────────────

    def _load_classifications(self) -> None:
        """Load whale classifications from JSON."""
        if not self.classifications_path.exists():
            logger.warning(f"No classifications file at {self.classifications_path}")
            return
        try:
            data = json.loads(self.classifications_path.read_text())
            self._classifications = data.get("classifications", {})
            # Extract trust scores
            for name, cls_data in self._classifications.items():
                if "category_performance" in cls_data:
                    cat_perf = cls_data["category_performance"]
                    if isinstance(cat_perf, dict) and cat_perf:
                        self._trust_scores[name] = cat_perf
            logger.info(f"Loaded {len(self._classifications)} whale classifications")
        except Exception as e:
            logger.error(f"Failed to load classifications: {e}")

    def _build_category_performance(self) -> None:
        """Build category WR and PnL from trades.db."""
        if not self.db_path.exists():
            logger.warning(f"No trades.db at {self.db_path}")
            return
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT
                    category,
                    COUNT(*) as total_trades,
                    ROUND(SUM(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                    ROUND(SUM(actual_pnl), 2) as total_pnl,
                    ROUND(AVG(actual_pnl), 2) as avg_pnl,
                    ROUND(AVG(edge_score), 3) as avg_edge,
                    ROUND(AVG(confidence), 3) as avg_confidence
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                GROUP BY category
            """).fetchall()
            conn.close()

            for row in rows:
                d = dict(row)
                cat = (d.get("category") or "unknown").lower()
                self._category_performance[cat] = {
                    "trades": d["total_trades"],
                    "win_rate": d["win_rate"] or 0.0,
                    "total_pnl": d["total_pnl"] or 0.0,
                    "avg_pnl": d["avg_pnl"] or 0.0,
                    "avg_edge": d["avg_edge"] or 0.0,
                    "avg_confidence": d["avg_confidence"] or 0.0,
                }
            self._last_refresh = datetime.now(timezone.utc)
            logger.info(f"Built category performance for {len(self._category_performance)} categories")
        except Exception as e:
            logger.error(f"Failed to build category performance: {e}")

    def refresh_if_stale(self) -> None:
        """Refresh classifications and category performance if stale."""
        if self._last_refresh is None:
            self._load_classifications()
            self._build_category_performance()
            return
        age_hours = (datetime.now(timezone.utc) - self._last_refresh).total_seconds() / 3600
        if age_hours >= self._refresh_interval_hours:
            self._load_classifications()
            self._build_category_performance()

    # ── Edge Computation ───────────────────────────────────────────────────

    def score_signal(
        self,
        whale_name: str,
        category: str,
        raw_edge_score: float,
        confidence: float,
        side: str,
        min_edge: float = 0.15,
    ) -> EdgeResult:
        """Compute data-driven edge score for a signal.

        Args:
            whale_name: Whale identifier (e.g., "p37-0xe5efd6").
            category: Market category (e.g., "sports", "general").
            raw_edge_score: Legacy edge score (used as fallback only).
            confidence: Signal confidence (0-1).
            side: Signal side ("BUY" or "SELL").
            min_edge: Minimum edge to consider trading (default 0.15).

        Returns:
            EdgeResult with calibrated edge, action, and trading decision.
        """
        self.refresh_if_stale()

        cat = (category or "unknown").lower()
        cls_data = self._classifications.get(whale_name, {})
        cat_perf = self._category_performance.get(cat, {})

        # ── Step 1: Determine action from classifier ──────────────────────
        action = cls_data.get("action", "ignore")
        action_confidence = cls_data.get("action_confidence", 0.0)

        # If no classification, fall back to static edge
        if not cls_data:
            # No classifier data — use category weight as a conservative estimate
            cat_wr = cat_perf.get("win_rate", 0.4)
            cat_weight = CATEGORY_WEIGHTS.get(cat, 0.05)
            fallback_edge = max(cat_wr * cat_weight, min_edge * 0.5)
            return EdgeResult(
                edge_score=round(min(fallback_edge, 0.5), 3),  # cap fallback at 0.5
                raw_edge=fallback_edge,
                action="ignore",
                action_confidence=0.0,
                whale_trust=0.0,
                category_weight=cat_weight,
                source="fallback",
                should_trade=fallback_edge >= min_edge,
                side_flip=False,
            )

        # ── Step 2: Compute whale action edge ─────────────────────────────
        whale_wr = cls_data.get("win_rate", 0.4)

        if action == "copy":
            # Copying a profitable whale: edge = whale_win_rate * boost
            action_edge = whale_wr * COPY_WIN_RATE_BOOST
            side_flip = False
        elif action == "fade":
            # Fading a losing whale: edge = (1 - whale_win_rate) * boost
            # If whale WR = 25%, fading gives 75% theoretical WR
            action_edge = (1.0 - whale_wr) * FADE_WIN_RATE_BOOST
            side_flip = True
        else:  # ignore
            action_edge = 0.0
            return EdgeResult(
                edge_score=0.0,
                raw_edge=0.0,
                action="ignore",
                action_confidence=0.0,
                whale_trust=0.0,
                category_weight=CATEGORY_WEIGHTS.get(cat, 0.05),
                source="classifier",
                should_trade=False,
                side_flip=False,
            )

        # ── Step 3: Get trust score for this whale in this category ────────
        trust = self._get_trust(whale_name, cat)
        cat_weight = CATEGORY_WEIGHTS.get(cat, 0.05)

        # ── Step 4: Get category WR ──────────────────────────────────────
        category_wr = cat_perf.get("win_rate", 0.4)

        # ── Step 5: Weighted combination ───────────────────────────────────
        raw_edge = (
            action_edge * WHALE_ACTION_WEIGHT
            + category_wr * CATEGORY_PERF_WEIGHT
            + (trust / 10.0) * TRUST_WEIGHT
        )

        # ── Step 6: Trust modulation ──────────────────────────────────────
        if trust >= TRUST_HIGH_THRESHOLD:
            raw_edge *= TRUST_HIGH_BOOST
        elif trust < TRUST_LOW_THRESHOLD:
            raw_edge *= TRUST_LOW_SUPPRESS

        # ── Step 7: Confidence modulation ──────────────────────────────────
        # Low-confidence signals should not get high edge
        confidence_multiplier = 0.5 + (confidence * 0.5)  # range 0.5-1.0
        raw_edge *= confidence_multiplier

        # ── Step 8: Clamp and calibrate ────────────────────────────────────
        # Map raw_edge (0-2ish) to calibrated edge (0-1)
        # Using sigmoid-like compression for extreme values
        calibrated_edge = raw_edge / (1.0 + raw_edge)  # maps (0,∞) → (0,1)
        calibrated_edge = max(0.0, min(1.0, calibrated_edge))

        should_trade = calibrated_edge >= min_edge

        return EdgeResult(
            edge_score=round(calibrated_edge, 3),
            raw_edge=round(raw_edge, 3),
            action=action,
            action_confidence=action_confidence,
            whale_trust=trust,
            category_weight=cat_weight,
            source="classifier",
            should_trade=should_trade,
            side_flip=side_flip,
        )

    def score_signal_simple(
        self,
        whale_name: str,
        category: str,
        confidence: float,
        side: str,
    ) -> EdgeResult:
        """Simplified edge scoring without legacy edge_score input.

        Uses only classifier + category performance. For use when
        legacy edge_score is unavailable or unreliable.
        """
        return self.score_signal(
            whale_name=whale_name,
            category=category,
            raw_edge_score=0.0,  # ignored by fallback path
            confidence=confidence,
            side=side,
        )

    # ── Trust Helpers ─────────────────────────────────────────────────────

    def _get_trust(self, whale_name: str, category: str) -> float:
        """Get trust score for a whale in a category.

        Falls back to overall classification confidence if no per-category trust.
        """
        cat_trusts = self._trust_scores.get(whale_name, {})
        if isinstance(cat_trusts, dict) and category in cat_trusts:
            val = cat_trusts[category]
            if isinstance(val, dict):
                return float(val.get("trust_score", val.get("win_rate", 5.0) * 10))
            return float(val)

        # Fall back to overall confidence from classification
        cls_data = self._classifications.get(whale_name, {})
        if cls_data:
            return cls_data.get("action_confidence", 0.5) * 10.0
        return 5.0  # neutral default

    # ── Batch Operations ──────────────────────────────────────────────────

    def get_all_edges(self, min_trades: int = 3) -> list[dict]:
        """Get edge scores for all classified whales across all categories.

        Useful for analysis and debugging. Returns list of dicts with
        whale_name, category, edge_score, action, trust, etc.
        """
        self.refresh_if_stale()
        results = []
        for whale_name, cls_data in self._classifications.items():
            categories = cls_data.get("categories", [])
            for cat in categories:
                result = self.score_signal(
                    whale_name=whale_name,
                    category=cat,
                    raw_edge_score=cls_data.get("signals", {}).get("win_rate", 0.4),
                    confidence=cls_data.get("action_confidence", 0.5),
                    side="BUY",  # placeholder
                )
                results.append({
                    "whale_name": whale_name,
                    "category": cat,
                    "classification": cls_data.get("classification", "unknown"),
                    "edge_score": result.edge_score,
                    "raw_edge": result.raw_edge,
                    "action": result.action,
                    "action_confidence": result.action_confidence,
                    "whale_trust": result.whale_trust,
                    "should_trade": result.should_trade,
                    "side_flip": result.side_flip,
                    "whale_wr": cls_data.get("win_rate", 0.0),
                    "total_pnl": cls_data.get("total_pnl", 0.0),
                })
        return results

    def validate_against_history(self) -> dict:
        """Validate edge scorer predictions against historical trade outcomes.

        Returns dict with validation metrics comparing new edge_score
        against actual win rate and PnL by edge_score bucket.
        """
        self.refresh_if_stale()
        if not self.db_path.exists():
            return {"error": "trades.db not found"}

        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT
                    whale_name,
                    category,
                    edge_score,
                    confidence,
                    side,
                    actual_pnl,
                    CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END as win
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
            """).fetchall()
            conn.close()

            # Score each historical trade with the new scorer
            buckets: dict[str, list] = {
                "0.0-0.15": [], "0.15-0.3": [], "0.3-0.5": [],
                "0.5-0.7": [], "0.7-1.0": [],
            }
            copy_pnl = 0.0
            fade_pnl = 0.0
            total_validated = 0

            for row in rows:
                d = dict(row)
                result = self.score_signal(
                    whale_name=d["whale_name"],
                    category=d["category"] or "unknown",
                    raw_edge_score=d["edge_score"] or 0.0,
                    confidence=d["confidence"] or 0.5,
                    side=d["side"] or "BUY",
                )
                # Determine bucket
                e = result.edge_score
                if e < 0.15:
                    bucket = "0.0-0.15"
                elif e < 0.3:
                    bucket = "0.15-0.3"
                elif e < 0.5:
                    bucket = "0.3-0.5"
                elif e < 0.7:
                    bucket = "0.5-0.7"
                else:
                    bucket = "0.7-1.0"

                pnl = d["actual_pnl"]
                # For fade signals, pnl is inverted (we profit when they lose)
                if result.side_flip:
                    pnl = -pnl
                buckets[bucket].append(pnl)

                if result.action == "copy":
                    copy_pnl += d["actual_pnl"]
                elif result.action == "fade":
                    fade_pnl += -d["actual_pnl"]
                total_validated += 1

            # Compute bucket stats
            bucket_stats = {}
            for bucket, pnls in buckets.items():
                if not pnls:
                    bucket_stats[bucket] = {"trades": 0, "wr": 0, "pnl": 0}
                    continue
                wr = sum(1 for p in pnls if p > 0) / len(pnls)
                bucket_stats[bucket] = {
                    "trades": len(pnls),
                    "wr": round(wr, 3),
                    "pnl": round(sum(pnls), 2),
                    "avg_pnl": round(sum(pnls) / len(pnls), 2),
                }

            return {
                "total_validated": total_validated,
                "copy_strategy_pnl": round(copy_pnl, 2),
                "fade_strategy_pnl": round(fade_pnl, 2),
                "bucket_stats": bucket_stats,
                "monotonic": self._check_monotonicity(bucket_stats),
            }
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"error": str(e)}

    @staticmethod
    def _check_monotonicity(bucket_stats: dict) -> bool:
        """Check that higher edge scores correlate with higher win rates.

        A good edge scorer should produce monotonically increasing WR
        across edge buckets (0.0→0.15→0.3→0.5→0.7→1.0).
        """
        ordered = ["0.0-0.15", "0.15-0.3", "0.3-0.5", "0.5-0.7", "0.7-1.0"]
        wrs = []
        for b in ordered:
            stats = bucket_stats.get(b, {})
            if stats.get("trades", 0) >= 5:
                wrs.append(stats.get("wr", 0))
        if len(wrs) < 2:
            return True  # Not enough data
        # Check that WR generally increases with edge
        increases = sum(1 for i in range(1, len(wrs)) if wrs[i] >= wrs[i-1])
        return increases >= len(wrs) - 1  # Allow 1 non-monotonic point


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scorer = EdgeScorer()

    # Validate against history
    print("\n=== Edge Scorer Validation ===")
    validation = scorer.validate_against_history()
    print(f"Total trades validated: {validation.get('total_validated', 0)}")
    print(f"Copy strategy PnL: ${validation.get('copy_strategy_pnl', 0):,.2f}")
    print(f"Fade strategy PnL: ${validation.get('fade_strategy_pnl', 0):,.2f}")
    print(f"Monotonicity: {validation.get('monotonic', 'unknown')}")
    print("\nEdge Bucket Stats:")
    for bucket, stats in validation.get("bucket_stats", {}).items():
        print(f"  {bucket}: {stats.get('trades', 0)} trades, WR={stats.get('wr', 0):.1%}, PnL=${stats.get('pnl', 0):,.2f}")

    # Show top copy and fade signals
    print("\n=== Top Copy Signals ===")
    edges = scorer.get_all_edges()
    copy_signals = [e for e in edges if e["action"] == "copy"]
    copy_signals.sort(key=lambda x: x["edge_score"], reverse=True)
    for e in copy_signals[:10]:
        print(f"  {e['whale_name']:30s} {e['category']:12s} edge={e['edge_score']:.3f} trust={e['whale_trust']:.1f} wr={e['whale_wr']:.1%}")

    print("\n=== Top Fade Signals ===")
    fade_signals = [e for e in edges if e["action"] == "fade"]
    fade_signals.sort(key=lambda x: x["edge_score"], reverse=True)
    for e in fade_signals[:10]:
        print(f"  {e['whale_name']:30s} {e['category']:12s} edge={e['edge_score']:.3f} trust={e['whale_trust']:.1f} wr={e['whale_wr']:.1%}")
