"""Self-Calibrating Feedback Engine — The system's learning loop.

Continuously learns from trade outcomes to calibrate:
1. Confidence thresholds (where does confidence actually predict wins?)
2. Edge score thresholds (where does edge actually predict PnL?)
3. Signal source weights (which signal sources deliver real edge?)
4. Whale sample gating (minimum trades before trusting a whale)
5. Exit timing (when should we exit vs waiting for resolution?)
6. Fade calibration (which whales are genuinely profitable to fade?)

The core insight: static thresholds are anti-predictive. The market changes,
whale behavior shifts, and what worked yesterday may not work tomorrow.
This module reads outcomes, recalibrates, and writes updated parameters.

Calibration cycle:
  - Every N trades or M minutes (whichever comes first)
  - Rebuild confidence/edge calibration curves from recent data
  - Adjust category weights based on rolling performance
  - Update signal source reliability scores
  - Persist calibrated parameters to data/feedback_state.json
"""

from __future__ import annotations

import json
import logging
import sqlite3
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from strategies.wf_constants import OVERCONFIDENCE_FLOOR
from typing import Optional

logger = logging.getLogger("FeedbackEngine")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/feedback_state.json")

# ── Calibration Parameters ───────────────────────────────────────────────────
MIN_CALIBRATION_TRADES = 50     # Minimum trades before calibrating
LOOKBACK_DAYS = 30              # Use last 30 days for calibration
CLEAN_DATA_START_DATE = '2026-05-14'  # Only use data from after the strategic overhaul
                                      # Pre-May-14 data contaminated by P&L bugs and broken filters
MIN_BUCKET_SIZE = 10            # Minimum trades per bucket for reliable stats
RECALIBRATION_INTERVAL_SECS = 300  # Re-calibrate every 5 minutes
WHALE_MIN_TRADES_FOR_COPY = 10     # Minimum trades before trusting a whale for copy
WHALE_MIN_TRADES_FOR_FADE = 5      # Minimum trades before trusting a whale for fade
WHALE_MIN_TRADES_FOR_CATEGORY = 3  # Minimum trades in a category before trusting category-specific WR
CATEGORY_BLOCK_PNL_THRESHOLD = -2000  # Auto-block category only if PnL is catastrophically negative
CATEGORY_BLOCK_WR_THRESHOLD = 0.30    # Auto-block category if WR < this (was 0.40, too aggressive)
CATEGORY_BLOCK_MIN_TRADES = 100      # Minimum trades before blocking a category (was 50, too eager)
SIGNAL_SOURCE_MIN_TRADES = 20

# Whales/sources excluded from feedback calibration — they skew category stats
# autoresearch_llm is an internal system, not a real whale; its trades contaminate category PnL
EXCLUDE_WHALES = {'autoresearch_llm'} 


@dataclass
class ConfidenceCalibration:
    """Maps raw confidence scores to calibrated (actual win-rate) scores."""
    # Bucket boundaries and their actual win rates
    buckets: dict = field(default_factory=dict)  # e.g., {"0.5-0.6": 0.62, "0.6-0.7": 0.58, ...}
    # Optimal threshold (minimum confidence that still has >50% WR)
    optimal_min_confidence: float = 0.55
    # Confidence above which WR actually drops (overconfidence zone)
    overconfidence_threshold: float = 0.90
    last_updated: str = ""


@dataclass
class EdgeCalibration:
    """Maps raw edge scores to calibrated (actual win-rate) scores."""
    buckets: dict = field(default_factory=dict)
    optimal_min_edge: float = 0.15
    optimal_max_edge: float = 0.70  # Above this, edge is anti-predictive
    last_updated: str = ""


@dataclass
class SignalSourceRating:
    """Rating for a signal source based on historical performance."""
    source: str = ""
    win_rate: float = 0.0
    avg_pnl: float = 0.0
    total_pnl: float = 0.0
    trade_count: int = 0
    weight: float = 1.0  # Multiplier applied to confidence from this source
    reliable: bool = False


@dataclass
class CategoryGuardState:
    """Auto-blocking state for a category."""
    category: str = ""
    blocked: bool = False
    block_reason: str = ""
    win_rate: float = 0.0
    total_pnl: float = 0.0
    trade_count: int = 0
    last_checked: str = ""


@dataclass
class FeedbackState:
    """Persisted state for the feedback engine."""
    confidence_calibration: ConfidenceCalibration = field(default_factory=ConfidenceCalibration)
    edge_calibration: EdgeCalibration = field(default_factory=EdgeCalibration)
    signal_sources: dict = field(default_factory=dict)  # source -> SignalSourceRating
    category_guards: dict = field(default_factory=dict)  # category -> CategoryGuardState
    whale_min_trades: dict = field(default_factory=dict)  # whale_name -> min_trades assessment
    total_trades_seen: int = 0
    last_recalibration: str = ""
    calibration_count: int = 0


class FeedbackEngine:
    """Self-calibrating feedback engine.

    Reads trade outcomes, recalibrates thresholds, and persists parameters.
    The strategy calls recalibrate() periodically (every 5 min) and uses
    the calibrated thresholds to filter signals.

    Usage:
        engine = FeedbackEngine()
        engine.recalibrate()  # Full recalibration from DB
        # In signal pipeline:
        if engine.should_trade(confidence=0.85, edge_score=0.6, category="sports"):
            ...
        # Calibrated confidence:
        cal_conf = engine.calibrate_confidence(0.85)
        # Signal source weight:
        weight = engine.get_signal_source_weight("autoresearch_llm")
        # Category guard:
        blocked = engine.is_category_blocked("sports")
    """

    def __init__(self, db_path: str | Path | None = None, state_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.state_path = Path(state_path) if state_path else STATE_PATH
        self.state = FeedbackState()
        self._load_state()

    # ── Persistence ────────────────────────────────────────────────────────

    def _load_state(self) -> None:
        """Load feedback state from disk."""
        if self.state_path.exists():
            try:
                data = json.loads(self.state_path.read_text())
                # Reconstruct nested dataclasses
                if "confidence_calibration" in data:
                    cc = data["confidence_calibration"]
                    self.state.confidence_calibration = ConfidenceCalibration(**cc)
                if "edge_calibration" in data:
                    ec = data["edge_calibration"]
                    self.state.edge_calibration = EdgeCalibration(**ec)
                if "signal_sources" in data:
                    self.state.signal_sources = {
                        k: SignalSourceRating(**v) for k, v in data["signal_sources"].items()
                    }
                if "category_guards" in data:
                    self.state.category_guards = {
                        k: CategoryGuardState(**v) for k, v in data["category_guards"].items()
                    }
                self.state.total_trades_seen = data.get("total_trades_seen", 0)
                self.state.last_recalibration = data.get("last_recalibration", "")
                self.state.calibration_count = data.get("calibration_count", 0)
                logger.info(f"Loaded feedback state: {self.state.calibration_count} calibrations, "
                           f"{len(self.state.signal_sources)} sources, "
                           f"{len(self.state.category_guards)} guards")
            except Exception as e:
                logger.warning(f"Failed to load feedback state: {e}, using defaults")

    def _save_state(self) -> None:
        """Persist feedback state to disk."""
        try:
            data = {
                "confidence_calibration": asdict(self.state.confidence_calibration),
                "edge_calibration": asdict(self.state.edge_calibration),
                "signal_sources": {k: asdict(v) for k, v in self.state.signal_sources.items()},
                "category_guards": {k: asdict(v) for k, v in self.state.category_guards.items()},
                "whale_min_trades": self.state.whale_min_trades,
                "total_trades_seen": self.state.total_trades_seen,
                "last_recalibration": self.state.last_recalibration,
                "calibration_count": self.state.calibration_count,
            }
            self.state_path.write_text(json.dumps(data, indent=2, default=str))
        except Exception as e:
            logger.error(f"Failed to save feedback state: {e}")

    # ── DB Queries ──────────────────────────────────────────────────────────

    def _query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Execute SQL query and return list of dicts."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"DB query failed: {e}")
            return []

    # ── Confidence Calibration ───────────────────────────────────────────────

    def _calibrate_confidence(self) -> ConfidenceCalibration:
        """Build confidence calibration curve from recent trades.

        Maps raw confidence to actual win rate per bucket.
        Identifies the overconfidence zone where higher confidence = worse outcomes.
        """
        rows = self._query("""
            SELECT confidence,
                   CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END as win,
                   realized_pnl,
                   realized_return
            FROM trades
            WHERE realized_return IS NOT NULL
              AND confidence IS NOT NULL
              AND whale_name NOT IN ('autoresearch_llm')
              AND timestamp > datetime('now', '-30 days')
              AND timestamp >= '2026-05-14' 
        """)

        if len(rows) < MIN_CALIBRATION_TRADES:
            # Fall back to all-time data
            rows = self._query("""
                SELECT confidence,
                       CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END as win,
                       realized_pnl,
                       realized_return
                FROM trades
                WHERE realized_return IS NOT NULL AND confidence IS NOT NULL
                  AND timestamp >= '2026-05-14' 
            """)

        if len(rows) < MIN_CALIBRATION_TRADES:
            logger.warning(f"Only {len(rows)} trades for confidence calibration, using defaults")
            return self.state.confidence_calibration

        # Bucket by confidence ranges
        buckets = {}
        bounds = [(0.0, 0.3), (0.3, 0.4), (0.4, 0.5), (0.5, 0.6),
                  (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01)]

        for lo, hi in bounds:
            key = f"{lo:.1f}-{hi:.1f}"
            bucket_rows = [r for r in rows if lo <= r["confidence"] < hi]
            if len(bucket_rows) >= MIN_BUCKET_SIZE:
                wr = sum(r["win"] for r in bucket_rows) / len(bucket_rows)
                pnl = sum(r["realized_pnl"] for r in bucket_rows)
                buckets[key] = {
                    "wr": round(wr, 3),
                    "pnl": round(pnl, 2),
                    "trades": len(bucket_rows),
                    "avg_pnl": round(pnl / len(bucket_rows), 2),
                }

        # Find optimal minimum confidence (lowest bucket with WR > 0.52)
        optimal_min = 0.55
        for lo, hi in bounds:
            key = f"{lo:.1f}-{hi:.1f}"
            if key in buckets and buckets[key]["wr"] > 0.52 and buckets[key]["pnl"] > 0:
                optimal_min = lo
                break

        # Find overconfidence threshold (where WR starts declining)
        overconfidence = 0.85  # Floor prevents starvation loop
        prev_wr = 0.0
        for lo, hi in bounds:
            key = f"{lo:.1f}-{hi:.1f}"
            if key in buckets:
                if buckets[key]["wr"] < prev_wr and buckets[key]["pnl"] < 0:
                    overconfidence = lo
                    break
                prev_wr = buckets[key]["wr"]

        cal = ConfidenceCalibration(
            buckets=buckets,
            optimal_min_confidence=optimal_min,
            overconfidence_threshold=max(overconfidence, OVERCONFIDENCE_FLOOR),
            last_updated=datetime.now(timezone.utc).isoformat(),
        )
        logger.info(f"Confidence calibration: optimal_min={optimal_min:.2f}, "
                    f"overconfidence={max(overconfidence, OVERCONFIDENCE_FLOOR):.2f}, buckets={len(buckets)}")
        return cal

    # ── Edge Score Calibration ──────────────────────────────────────────────

    def _calibrate_edge(self) -> EdgeCalibration:
        """Build edge score calibration curve from recent trades.

        Identifies the range where edge score is actually predictive.
        """
        rows = self._query("""
            SELECT edge_score,
                   CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END as win,
                   realized_pnl
            FROM trades
            WHERE realized_return IS NOT NULL
              AND edge_score IS NOT NULL
              AND whale_name NOT IN ('autoresearch_llm')
              AND timestamp >= '2026-05-14' 
        """)

        if len(rows) < MIN_CALIBRATION_TRADES:
            return self.state.edge_calibration

        buckets = {}
        bounds = [(0.0, 0.15), (0.15, 0.3), (0.3, 0.5), (0.5, 0.7), (0.7, 1.01)]

        for lo, hi in bounds:
            key = f"{lo:.2f}-{hi:.2f}"
            bucket_rows = [r for r in rows if lo <= r["edge_score"] < hi]
            if len(bucket_rows) >= MIN_BUCKET_SIZE:
                wr = sum(r["win"] for r in bucket_rows) / len(bucket_rows)
                pnl = sum(r["realized_pnl"] for r in bucket_rows)
                buckets[key] = {
                    "wr": round(wr, 3),
                    "pnl": round(pnl, 2),
                    "trades": len(bucket_rows),
                }

        # Find optimal edge range (highest WR and PnL)
        optimal_min = 0.15
        optimal_max = 0.70
        best_pnl = 0.0
        for lo, hi in bounds:
            key = f"{lo:.2f}-{hi:.2f}"
            if key in buckets and buckets[key]["pnl"] > best_pnl:
                best_pnl = buckets[key]["pnl"]
                optimal_min = lo

        # Find where edge becomes anti-predictive
        for lo, hi in bounds:
            key = f"{lo:.2f}-{hi:.2f}"
            if key in buckets and buckets[key]["pnl"] < 0 and lo > 0.5:
                optimal_max = lo
                break

        cal = EdgeCalibration(
            buckets=buckets,
            optimal_min_edge=optimal_min,
            optimal_max_edge=optimal_max,
            last_updated=datetime.now(timezone.utc).isoformat(),
        )
        logger.info(f"Edge calibration: optimal_range={optimal_min:.2f}-{optimal_max:.2f}")
        return cal

    # ── Signal Source Rating ─────────────────────────────────────────────────

    def _rate_signal_sources(self) -> dict[str, SignalSourceRating]:
        """Rate signal sources by their actual performance."""
        rows = self._query("""
            SELECT signal_source,
                   COUNT(*) as trades,
                   ROUND(AVG(CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END), 3) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as total_pnl,
                   ROUND(AVG(realized_pnl), 2) as avg_pnl
            FROM trades
            WHERE realized_return IS NOT NULL
              AND signal_source IS NOT NULL
              AND whale_name NOT IN ('autoresearch_llm')
              AND timestamp >= '2026-05-14'
            GROUP BY signal_source
        """)

        sources = {}
        for r in rows:
            src = r["signal_source"] or "unknown"
            trades = r["trades"]
            wr = r["win_rate"] or 0.0
            pnl = r["total_pnl"] or 0.0

            # Weight: boost profitable sources, suppress losing ones
            if trades >= SIGNAL_SOURCE_MIN_TRADES:
                if pnl > 100 and wr > 0.55:
                    weight = 1.2  # Boost strong sources
                elif pnl > 0 and wr > 0.50:
                    weight = 1.0  # Neutral
                elif pnl < -200 or wr < 0.45:
                    weight = 0.5  # Suppress weak sources
                else:
                    weight = 0.8  # Slight discount
                reliable = True
            else:
                weight = 0.7  # Discount low-sample sources
                reliable = False

            sources[src] = SignalSourceRating(
                source=src,
                win_rate=wr,
                avg_pnl=r["avg_pnl"] or 0.0,
                total_pnl=pnl,
                trade_count=trades,
                weight=weight,
                reliable=reliable,
            )

        return sources

    # ── Category Guard ───────────────────────────────────────────────────────

    def _check_category_guards(self) -> dict[str, CategoryGuardState]:
        """Auto-block categories that are losing money."""
        rows = self._query("""
            SELECT category,
                   COUNT(*) as trades,
                   ROUND(AVG(CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END), 3) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as total_pnl
            FROM trades
            WHERE realized_return IS NOT NULL
              AND whale_name NOT IN ('autoresearch_llm')
              AND timestamp >= '2026-05-14'
            GROUP BY category
        """)

        guards = {}
        for r in rows:
            cat = r["category"] or "unknown"
            trades = r["trades"]
            wr = r["win_rate"] or 0.0
            pnl = r["total_pnl"] or 0.0

            blocked = False
            block_reason = ""

            if trades >= CATEGORY_BLOCK_MIN_TRADES:
                if pnl < CATEGORY_BLOCK_PNL_THRESHOLD:
                    blocked = True
                    block_reason = f"pnl_below_threshold: ${pnl:.0f} < -${CATEGORY_BLOCK_PNL_THRESHOLD}"
                elif wr < CATEGORY_BLOCK_WR_THRESHOLD and trades >= 100:
                    blocked = True
                    block_reason = f"wr_below_threshold: {wr:.1%} < {CATEGORY_BLOCK_WR_THRESHOLD:.0%}"
                # Don't block categories that have positive PnL from profitable whales
                # even if overall PnL is negative - use multiplier instead
                # Hard block only for catastrophically bad categories

            guards[cat] = CategoryGuardState(
                category=cat,
                blocked=blocked,
                block_reason=block_reason,
                win_rate=wr,
                total_pnl=pnl,
                trade_count=trades,
                last_checked=datetime.now(timezone.utc).isoformat(),
            )

        return guards

    # ── Whale Sample Gating ─────────────────────────────────────────────────

    def _assess_whale_min_trades(self) -> dict[str, int]:
        """Determine minimum trade requirements per whale based on variance."""
        rows = self._query("""
            SELECT whale_name, COUNT(*) as trades,
                   ROUND(AVG(CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END), 3) as wr
            FROM trades
            WHERE realized_return IS NOT NULL
              AND whale_name NOT IN ('autoresearch_llm')
              AND timestamp >= '2026-05-14'
            GROUP BY whale_name
            HAVING COUNT(*) >= 3
        """)

        min_trades = {}
        for r in rows:
            whale = r["whale_name"]
            trades = r["trades"]
            wr = r["wr"] or 0.5

            # High-variance whales (extreme WR) need more trades to trust
            if wr > 0.8 or wr < 0.2:
                min_trades[whale] = WHALE_MIN_TRADES_FOR_COPY + 5  # 15 trades
            elif wr > 0.65 or wr < 0.35:
                min_trades[whale] = WHALE_MIN_TRADES_FOR_COPY  # 10 trades
            else:
                min_trades[whale] = WHALE_MIN_TRADES_FOR_FADE  # 5 trades

        return min_trades

    # ── Public API ───────────────────────────────────────────────────────────

    def recalibrate(self) -> None:
        """Full recalibration from trade database. Called periodically."""
        logger.info("Starting feedback recalibration...")

        self.state.confidence_calibration = self._calibrate_confidence()
        self.state.edge_calibration = self._calibrate_edge()
        self.state.signal_sources = self._rate_signal_sources()
        self.state.category_guards = self._check_category_guards()
        self.state.whale_min_trades = self._assess_whale_min_trades()

        # Count total trades
        count_rows = self._query("SELECT COUNT(*) as cnt FROM trades WHERE realized_return IS NOT NULL AND whale_name NOT IN ('autoresearch_llm') AND timestamp >= '2026-05-14'")
        self.state.total_trades_seen = count_rows[0]["cnt"] if count_rows else 0
        self.state.last_recalibration = datetime.now(timezone.utc).isoformat()
        self.state.calibration_count += 1

        self._save_state()

        # Log summary
        logger.info(f"Feedback recalibration #{self.state.calibration_count} complete:")
        logger.info(f"  Confidence: optimal_min={self.state.confidence_calibration.optimal_min_confidence:.2f}, "
                    f"overconfidence={self.state.confidence_calibration.overconfidence_threshold:.2f}")
        logger.info(f"  Edge: optimal_range={self.state.edge_calibration.optimal_min_edge:.2f}-"
                    f"{self.state.edge_calibration.optimal_max_edge:.2f}")
        logger.info(f"  Signal sources: {len(self.state.signal_sources)} rated")
        blocked_cats = [f"{k} ({v.block_reason})" for k, v in self.state.category_guards.items() if v.blocked]
        if blocked_cats:
            logger.info(f"  Blocked categories: {', '.join(blocked_cats)}")
        logger.info(f"  Total trades analyzed: {self.state.total_trades_seen}")

    def calibrate_confidence(self, raw_confidence: float, signal_source: str = "") -> float:
        """Convert raw confidence to calibrated confidence based on historical performance.

        Uses a blended approach: bucket WR provides a floor/dampener for losing buckets,
        but profitable buckets preserve more of the raw confidence. This prevents
        over-suppression of mid-range confidence signals that the edge scorer
        has already validated.
        """
        cal = self.state.confidence_calibration

        # Find the bucket for this confidence
        calibrated = raw_confidence
        bucket_found = False
        for key, stats in cal.buckets.items():
            parts = key.split("-")
            lo, hi = float(parts[0]), float(parts[1])
            if lo <= raw_confidence < hi:
                bucket_found = True
                trades = stats.get("trades", 0)
                wr = stats.get("wr", raw_confidence)
                pnl = stats.get("pnl", 0)
                avg_pnl = stats.get("avg_pnl", 0)

                if trades >= MIN_BUCKET_SIZE:
                    # Profitable bucket: preserve most raw confidence, use WR as floor only
                    # if the bucket is net profitable (avg_pnl > 0)
                    if avg_pnl > 0 or pnl > 0:
                        # Profitable range — dampen raw confidence toward WR but preserve most of it
                        # Blend: 70% raw + 30% WR to keep signal strength while acknowledging data
                        calibrated = raw_confidence * 0.70 + wr * 0.30
                    else:
                        # Losing bucket — use WR directly but apply a floor
                        # so we don't suppress below 0.30 (allows small positions)
                        calibrated = max(wr, 0.30)
                break

        # If no bucket found, use raw confidence with a slight discount
        if not bucket_found:
            calibrated = raw_confidence * 0.85

        # Overconfidence suppression: if raw confidence > threshold, cap it
        if raw_confidence >= cal.overconfidence_threshold:
            # Only suppress extreme overconfidence (0.9+)
            # Cap to optimal_min + 0.35 (allows up to 0.80 after calibration)
            calibrated = min(calibrated, cal.optimal_min_confidence + 0.35)

        # Signal source weight adjustment (small boost for reliable sources, small penalty for unreliable)
        # Changed from multiplicative to additive to prevent over-suppression
        if signal_source and signal_source in self.state.signal_sources:
            src = self.state.signal_sources[signal_source]
            if src.reliable:
                # Boost reliable sources by 10% of the gap between calibrated and 1.0
                calibrated += (1.0 - calibrated) * 0.10
            elif src.trade_count >= MIN_BUCKET_SIZE and src.weight < 0.5:
                # Penalize unreliable sources by 10%
                calibrated *= 0.90

        return max(0.0, min(1.0, calibrated))

    def should_trade(self, confidence: float, edge_score: float, category: str = "",
                     signal_source: str = "", whale_name: str = "") -> tuple[bool, str]:
        """Determine if a signal should be traded based on calibrated thresholds.

        Returns (should_trade, reason) tuple.
        """
        # Category guard check
        if category and category.lower() in self.state.category_guards:
            guard = self.state.category_guards[category.lower()]
            if guard.blocked:
                return False, f"category_blocked:{guard.block_reason}"

        # Confidence and edge calibration are advisory only.
        # The pipeline already has min_confidence and min_edge thresholds.
        # Calibrated confidence is used for position sizing (via calibrate_confidence()),
        # not as an additional hard gate. This prevents the feedback loop from
        # over-filtering when calibration data is sparse or skewed.

        return True, "passed_feedback_filter"

    def get_signal_source_weight(self, source: str) -> float:
        """Get reliability weight for a signal source."""
        if source in self.state.signal_sources:
            return self.state.signal_sources[source].weight
        return 0.7  # Default weight for unknown sources

    def is_category_blocked(self, category: str) -> bool:
        """Check if a category is auto-blocked."""
        cat = category.lower()
        if cat in self.state.category_guards:
            return self.state.category_guards[cat].blocked
        return False

    def get_category_multiplier(self, category: str) -> float:
        """Get position size multiplier for a category based on performance.

        Winning categories get boosted, losing categories get suppressed.
        """
        cat = category.lower()
        if cat in self.state.category_guards:
            guard = self.state.category_guards[cat]
            if guard.blocked:
                return 0.0
            # Scale multiplier based on WR and PnL
            if guard.trade_count >= 50:
                if guard.total_pnl > 1000 and guard.win_rate > 0.55:
                    return 1.2  # Boost winning categories
                elif guard.total_pnl > 0 and guard.win_rate > 0.50:
                    return 1.0  # Neutral
                elif guard.total_pnl < -200:
                    return 0.5  # Suppress losing categories
                else:
                    return 0.8  # Slight discount
        return 1.0  # Default

    def get_whale_min_trades(self, whale_name: str) -> int:
        """Get minimum trades required before trusting this whale."""
        return self.state.whale_min_trades.get(whale_name, WHALE_MIN_TRADES_FOR_COPY)

    def get_optimal_confidence_min(self) -> float:
        """Get calibrated minimum confidence threshold."""
        return self.state.confidence_calibration.optimal_min_confidence

    def get_optimal_edge_range(self) -> tuple[float, float]:
        """Get calibrated edge score range (min, max)."""
        return (
            self.state.edge_calibration.optimal_min_edge,
            self.state.edge_calibration.optimal_max_edge,
        )

    def get_diagnostic_summary(self) -> dict:
        """Get a diagnostic summary of calibration state."""
        return {
            "calibration_count": self.state.calibration_count,
            "total_trades_seen": self.state.total_trades_seen,
            "last_recalibration": self.state.last_recalibration,
            "confidence": {
                "optimal_min": self.state.confidence_calibration.optimal_min_confidence,
                "overconfidence_threshold": self.state.confidence_calibration.overconfidence_threshold,
                "buckets": self.state.confidence_calibration.buckets,
            },
            "edge": {
                "optimal_min": self.state.edge_calibration.optimal_min_edge,
                "optimal_max": self.state.edge_calibration.optimal_max_edge,
                "buckets": self.state.edge_calibration.buckets,
            },
            "signal_sources": {
                k: {"wr": v.win_rate, "pnl": v.total_pnl, "weight": v.weight, "reliable": v.reliable}
                for k, v in self.state.signal_sources.items()
            },
            "category_guards": {
                k: {"blocked": v.blocked, "wr": v.win_rate, "pnl": v.total_pnl, "trades": v.trade_count}
                for k, v in self.state.category_guards.items()
            },
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
    engine = FeedbackEngine()
    engine.recalibrate()

    summary = engine.get_diagnostic_summary()
    print("\n=== Feedback Engine Diagnostic Summary ===")
    print(f"Calibration #{summary['calibration_count']}, {summary['total_trades_seen']} trades analyzed")
    print(f"\nConfidence: optimal_min={summary['confidence']['optimal_min']:.2f}, "
          f"overconfidence={summary['confidence']['overconfidence_threshold']:.2f}")
    print("Confidence buckets:")
    for k, v in summary["confidence"]["buckets"].items():
        print(f"  {k}: WR={v['wr']:.1%}, PnL=${v['pnl']:.0f}, trades={v['trades']}")

    print(f"\nEdge: optimal_range={summary['edge']['optimal_min']:.2f}-{summary['edge']['optimal_max']:.2f}")
    print("Edge buckets:")
    for k, v in summary["edge"]["buckets"].items():
        print(f"  {k}: WR={v['wr']:.1%}, PnL=${v['pnl']:.0f}, trades={v['trades']}")

    print("\nSignal Sources:")
    for k, v in summary["signal_sources"].items():
        print(f"  {k}: WR={v['wr']:.1%}, PnL=${v['pnl']:.0f}, weight={v['weight']:.2f}, reliable={v['reliable']}")

    print("\nCategory Guards:")
    for k, v in summary["category_guards"].items():
        status = "BLOCKED" if v["blocked"] else "OK"
        print(f"  {k}: [{status}] WR={v['wr']:.1%}, PnL=${v['pnl']:.0f}, trades={v['trades']}")
