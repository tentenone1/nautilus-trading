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
# v5.5 calibration: profitable whales (~70% WR) scored 0.33-0.39 — below the 0.50
# discrimination target. Boosted multipliers so that 70% WR whales score >= 0.50.
# Formula: calibrated = raw/(1+raw), targets: 70% WR whale => >=0.50, <30% WR => <=0.30.
COPY_WIN_RATE_BOOST = 2.0      # Copying a profitable whale: 2x multiplier (was 1.3)
FADE_WIN_RATE_BOOST = 2.2      # Fading a losing whale: 2.2x multiplier (stronger signal, was 1.5)
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
    poly_enriched: bool = False    # Whether poly_data stats enriched this signal
    tournament_multiplier: float = 1.0  # Size multiplier from tournament advisory


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
            logger.info(f"Loaded {len(self._classifications)} whale classifications from JSON")
            # Also load from whale_intelligence DB table
            self._load_db_classifications()
        except Exception as e:
            logger.error(f"Failed to load classifications: {e}")

    def _load_db_classifications(self) -> None:
        """Load whale classifications from whale_discovery.db AND trades.db.

        Uses should_copy/should_fade flags from whale_intelligence,
        plus PnL data from trades.db to determine copy/fade/ignore.
        Supplements the JSON classifications file.
        """
        db_path = self.db_path.parent / "whale_discovery.db"
        if not db_path.exists():
            return
        try:
            import sqlite3

            # Step 1: Load PnL summary from trades.db
            pnl_data = {}
            if self.db_path.exists():
                try:
                    pnl_conn = sqlite3.connect(str(self.db_path), timeout=10.0)
                    pnl_rows = pnl_conn.execute(
                        "SELECT whale_name, COUNT(*) as trades, "
                        "ROUND(SUM(realized_pnl), 2) as pnl, "
                        "ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr "
                        "FROM trades WHERE whale_name IS NOT NULL AND realized_pnl IS NOT NULL "
                        "GROUP BY whale_name"
                    ).fetchall()
                    pnl_conn.close()
                    for r in pnl_rows:
                        pnl_data[r[0]] = {"trades": r[1], "pnl": r[2] or 0, "wr": r[3] or 0}
                except Exception:
                    pass

            # Step 2: Load from whale_intelligence
            conn = sqlite3.connect(str(db_path), timeout=10.0)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT name, classification, trust_score, should_copy, should_fade, "
                "win_rate, volume FROM whale_intelligence WHERE name IS NOT NULL"
            ).fetchall()
            conn.close()

            added = 0
            for row in rows:
                name = row["name"]
                if name in self._classifications:
                    continue  # JSON takes priority

                should_copy = bool(row["should_copy"])
                should_fade = bool(row["should_fade"])
                classification = row["classification"] or "unknown"
                trust = row["trust_score"] or 5.0
                wr = row["win_rate"] or 0.4
                volume = row["volume"] or 0

                # Get trades.db PnL data for this whale
                td = pnl_data.get(name, {})
                trades_count = td.get("trades", 0)
                pnl = td.get("pnl", 0)
                td_wr = td.get("wr", 0)

                # Use trades.db WR if available (more accurate)
                if trades_count >= 10:
                    wr = td_wr

                # Determine action: explicit flags first, then data-driven
                if should_copy:
                    action = "copy"
                elif should_fade:
                    action = "fade"
                elif pnl > 500 and trades_count >= 20:
                    # Whale with significant positive PnL = follow
                    action = "copy"
                elif pnl < -500 and trades_count >= 20:
                    # Whale with significant negative PnL = fade
                    action = "fade"
                elif wr >= 0.50 and trades_count >= 10:
                    action = "copy"
                elif wr < 0.35 and trades_count >= 10:
                    action = "fade"
                elif wr >= 0.48 and pnl > 0 and trades_count >= 50:
                    # Edge case: slightly below 50% WR but profitable (big wins)
                    action = "copy"
                else:
                    action = "ignore"

                self._classifications[name] = {
                    "whale_name": name,
                    "classification": classification,
                    "confidence": min(1.0, trust / 10.0),
                    "win_rate": wr,
                    "total_trades": trades_count,
                    "total_pnl": pnl,
                    "avg_pnl": pnl / max(trades_count, 1),
                    "categories": [],
                    "category_performance": {},
                    "signals": {},
                    "action": action,
                    "action_confidence": min(1.0, max(0.3, trust / 10.0)),
                }
                added += 1

            if added:
                logger.info(f"Loaded {added} additional whale classifications from DB")
        except Exception as e:
            logger.error(f"Failed to load DB classifications: {e}")

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
                    ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                    ROUND(SUM(realized_pnl), 2) as total_pnl,
                    ROUND(AVG(realized_pnl), 2) as avg_pnl,
                    ROUND(AVG(edge_score), 3) as avg_edge,
                    ROUND(AVG(confidence), 3) as avg_confidence
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name IS NOT NULL

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
            # Sybil/entity cluster signals get boosted minimum (coordinated activity = signal)
            is_sybil = "sybil" in whale_name.lower() or "entity_cluster" in whale_name.lower()
            min_fallback = min_edge * 0.8 if is_sybil else min_edge * 0.5
            fallback_edge = max(cat_wr * cat_weight, min_fallback)
            # Sybil/entity clusters use min_fallback as threshold (coordinated activity = signal)
            # Regular whales still need min_edge
            should_trade_threshold = min_fallback if is_sybil else min_edge
            return EdgeResult(
                edge_score=round(min(fallback_edge, 0.5), 3),  # cap fallback at 0.5
                raw_edge=fallback_edge,
                action="copy" if not is_sybil else "fade",
                action_confidence=0.1 if not is_sybil else 0.3,
                whale_trust=1.0 if not is_sybil else 3.0,
                category_weight=cat_weight,
                source="fallback" if not is_sybil else "fallback_sybil",
                should_trade=fallback_edge >= (min_edge if not is_sybil else min_fallback),
                side_flip=is_sybil,  # Fade sybil clusters by default
                poly_enriched=False,
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
                poly_enriched=False,
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

        # ── Step 7: Poly Data enrichment boost ─────────────────────────────────
        # Boost for whales that are linked to poly_data (high-volume traders)
        poly_stats, poly_enriched = self._get_poly_enrichment(whale_name)
        if poly_enriched:
            classification = poly_stats.get("classification", "unknown")
            total_volume = float(poly_stats.get("total_volume_usd", 0) or 0)
            trades_per_day = float(poly_stats.get("trades_per_day", 0) or 0)

            # Boost confidence for skilled humans, reduce for bots
            if classification == "skilled_human":
                action_confidence = min(1.0, action_confidence + 0.15)
                raw_edge *= 1.10  # +10% edge for verified skilled humans
                logger.debug(
                    "Poly boost: %s skilled_human vol=%.0f tpd=%.1f → boosted",
                    whale_name, total_volume, trades_per_day,
                )
            elif classification == "trading_bot":
                action_confidence = max(0.0, action_confidence - 0.10)
                raw_edge *= 0.85  # -15% edge for bots
                logger.debug(
                    "Poly boost: %s trading_bot → suppressed",
                    whale_name,
                )

            # Volume and activity bonuses
            if total_volume > 100_000:
                raw_edge *= 1.05  # +5% edge for whales with >$100K volume
            if trades_per_day > 5:
                raw_edge *= 1.03  # +3% for active traders (>5 trades/day)

        # ── Step 8: Confidence modulation ──────────────────────────────────
        # Low-confidence signals should not get high edge
        confidence_multiplier = 0.5 + (confidence * 0.5)  # range 0.5-1.0
        raw_edge *= confidence_multiplier

        # ── Step 9: Tournament size multiplier ──────────────────────────────
        # When tournament conditions are favorable, relax the edge threshold
        # (size_multiplier > 1 means position sizing is boosted → less edge needed)
        tournament_multiplier = 1.0
        try:
            from strategies.tournament_signal_bridge import TournamentSignalBridge
            advisory = TournamentSignalBridge().get_advisory()
            tournament_multiplier = advisory.get("size_multiplier", 1.0)
        except Exception:
            pass

        effective_min_edge = min_edge / max(tournament_multiplier, 0.5)

        # ── Step 10: Clamp and calibrate ────────────────────────────────────
        # Map raw_edge (0-2ish) to calibrated edge (0-1)
        # Using sigmoid-like compression for extreme values
        calibrated_edge = raw_edge / (1.0 + raw_edge)
        calibrated_edge = max(0.0, min(1.0, calibrated_edge))

        should_trade = calibrated_edge >= effective_min_edge

        return EdgeResult(
            edge_score=round(calibrated_edge, 3),
            raw_edge=round(raw_edge, 3),
            action=action,
            action_confidence=round(action_confidence, 3),
            whale_trust=trust,
            category_weight=cat_weight,
            source="classifier",
            should_trade=should_trade,
            side_flip=side_flip,
            poly_enriched=poly_enriched,
            tournament_multiplier=tournament_multiplier,
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

    # ── Poly Data Enrichment ────────────────────────────────────────────────

    def _get_poly_enrichment(self, whale_name: str) -> tuple[dict, bool]:
        """Query poly_data for this whale's stats from trades.db.

        Returns (stats_dict, enriched) where enriched=True if data found.
        Only returns data for whales that are linked via poly_address_map.
        """
        import sqlite3
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                """
                SELECT ps.*, pam.nautilus_whale_name
                FROM poly_whale_stats ps
                JOIN poly_address_map pam ON LOWER(pam.address) = LOWER(ps.address)
                WHERE pam.nautilus_whale_name = ?
                LIMIT 1
                """,
                (whale_name,),
            )
            row = cur.fetchone()
            conn.close()
            if row:
                return dict(row), True
            return {}, False
        except Exception:
            return {}, False

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
                    realized_pnl,
                    CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END as win
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name IS NOT NULL

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

                pnl = d["realized_pnl"]
                # For fade signals, pnl is inverted (we profit when they lose)
                if result.side_flip:
                    pnl = -pnl
                buckets[bucket].append(pnl)

                if result.action == "copy":
                    copy_pnl += d["realized_pnl"]
                elif result.action == "fade":
                    fade_pnl += -d["realized_pnl"]
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


# ── Phase C3: Edge Scorer Calibration ─────────────────────────────────────────

def calibrate_edge_scorer(
    db_path: Path | str | None = None,
    output_path: Path | str | None = None,
    min_trades_per_whale: int = 3,
) -> dict:
    """Calibrate edge scorer weights using historical realized_pnl data.

    Loads all closed trades with realized_pnl IS NOT NULL, computes per-whale
    aggregate stats (win rate, avg PnL, Sharpe), then checks whether the current
    scorer produces edge scores that satisfy the discrimination target:

      - Profitable whales (total PnL > 0):  edge_score >= 0.50
      - Unprofitable whales (total PnL < 0): edge_score <= 0.30

    If the current scorer fails these thresholds, the weight knobs are adjusted
    and the recommended calibration is written to the output JSON.

    Args:
        db_path: Path to trades.db.
        output_path: Path to write config/edge_scorer_calibration_v5.5.json.
        min_trades_per_whale: Minimum trades required for a whale to be included.

    Returns:
        Dict with calibration results, including per-whale stats, threshold
        checks, recommended_weight_changes, and final verdict.
    """
    import json as _json
    import math as _math

    _db = Path(db_path) if db_path else DB_PATH
    _out = Path(output_path) if output_path else (
        Path("/home/elon-1/workspace/nautilus-trading/config")
        / "edge_scorer_calibration_v5.5.json"
    )
    _out.parent.mkdir(parents=True, exist_ok=True)

    # ── 1. Load closed trades ─────────────────────────────────────────────────
    conn = sqlite3.connect(str(_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT whale_name, category, side, realized_pnl, exit_reason
        FROM trades
        WHERE realized_pnl IS NOT NULL
          AND exit_reason IN ('resolved', 'max_hold')
        ORDER BY whale_name, timestamp
        """
    ).fetchall()
    conn.close()

    # ── 2. Aggregate per-whale stats ─────────────────────────────────────────
    whale_data: dict = {}
    for row in rows:
        w = (row["whale_name"] or "unknown", row["category"] or "unknown")
        if w not in whale_data:
            whale_data[w] = {"pnls": [], "category": row["category"] or "unknown"}
        whale_data[w]["pnls"].append(row["realized_pnl"] or 0.0)

    whale_stats = {}
    for (wname, cat), data in whale_data.items():
        pnls = data["pnls"]
        n = len(pnls)
        if n < min_trades_per_whale:
            continue
        total = sum(pnls)
        wins = sum(1 for p in pnls if p > 0)
        mean = total / n
        variance = sum((p - mean) ** 2 for p in pnls) / max(n - 1, 1)
        std = _math.sqrt(variance) if variance > 0 else 0.0
        sharpe = (mean / std) * _math.sqrt(252) if std > 0 else 0.0
        whale_stats[wname] = {
            "category": cat,
            "n_trades": n,
            "total_pnl": round(total, 2),
            "avg_pnl": round(mean, 4),
            "win_rate": round(wins / n, 3),
            "sharpe": round(sharpe, 4),
            "profitable": total > 0,
        }

    # ── 3. Score each whale using the current scorer ─────────────────────────
    scorer = EdgeScorer(db_path=_db)
    scorer.refresh_if_stale()

    profitable_above_threshold = 0
    profitable_below_threshold = 0
    unprofitable_below_threshold = 0
    unprofitable_above_threshold = 0
    calibration_needed = False
    adjustments = []

    whale_score_details = {}
    for wname, stats in whale_stats.items():
        result = scorer.score_signal(
            whale_name=wname,
            category=stats["category"],
            raw_edge_score=0.5,
            confidence=0.5,
            side="BUY",
        )
        score = result.edge_score
        whale_score_details[wname] = {
            "category": stats["category"],
            "edge_score": score,
            "total_pnl": stats["total_pnl"],
            "win_rate": stats["win_rate"],
            "sharpe": stats["sharpe"],
            "n_trades": stats["n_trades"],
            "profitable": stats["profitable"],
        }

        if stats["profitable"]:
            if score >= 0.50:
                profitable_above_threshold += 1
            else:
                profitable_below_threshold += 1
                calibration_needed = True
                adjustments.append({
                    "whale": wname,
                    "current_score": score,
                    "target": ">= 0.50",
                    "reason": f"profitable whale ({stats['category']}) scored below 0.50",
                })
        else:
            if score <= 0.30:
                unprofitable_below_threshold += 1
            else:
                unprofitable_above_threshold += 1
                calibration_needed = True
                adjustments.append({
                    "whale": wname,
                    "current_score": score,
                    "target": "<= 0.30",
                    "reason": f"unprofitable whale ({stats['category']}) scored above 0.30",
                })

    # ── 4. Compute recommended weight changes ─────────────────────────────────
    total_profitable = profitable_above_threshold + profitable_below_threshold
    total_unprofitable = unprofitable_below_threshold + unprofitable_above_threshold

    # Check if weights need bumping up/down
    current_weights = {
        "WHALE_ACTION_WEIGHT": WHALE_ACTION_WEIGHT,
        "CATEGORY_PERF_WEIGHT": CATEGORY_PERF_WEIGHT,
        "TRUST_WEIGHT": TRUST_WEIGHT,
        "COPY_WIN_RATE_BOOST": COPY_WIN_RATE_BOOST,
        "FADE_WIN_RATE_BOOST": FADE_WIN_RATE_BOOST,
        "TRUST_HIGH_BOOST": TRUST_HIGH_BOOST,
        "TRUST_LOW_SUPPRESS": TRUST_LOW_SUPPRESS,
    }

    # Simple heuristic: if profitable whales are under-scored, boost the
    # action and win-rate weights; if unprofitable whales are over-scored,
    # suppress the action weight and boost fade WR multiplier.
    recommended = dict(current_weights)
    if calibration_needed:
        if profitable_below_threshold > 0:
            # Profitable whales scored too low → boost whale action weight
            delta = min(0.05 * profitable_below_threshold, 0.15)
            recommended["WHALE_ACTION_WEIGHT"] = round(
                min(recommended["WHALE_ACTION_WEIGHT"] + delta, 0.70), 2
            )
            recommended["COPY_WIN_RATE_BOOST"] = round(
                min(recommended["COPY_WIN_RATE_BOOST"] + 0.05, 1.6), 2
            )
        if unprofitable_above_threshold > 0:
            # Unprofitable whales scored too high → suppress action weight
            delta = min(0.05 * unprofitable_above_threshold, 0.15)
            recommended["WHALE_ACTION_WEIGHT"] = round(
                max(recommended["WHALE_ACTION_WEIGHT"] - delta, 0.30), 2
            )
            recommended["FADE_WIN_RATE_BOOST"] = round(
                min(recommended["FADE_WIN_RATE_BOOST"] + 0.05, 1.8), 2
            )

    verdict = "PASS" if not calibration_needed else "ADJUST_RECOMMENDED"
    if not calibration_needed:
        verdict = "PASS"
    elif total_profitable > 0 and profitable_below_threshold / total_profitable < 0.2:
        verdict = "MARGINAL"
    else:
        verdict = "ADJUST_RECOMMENDED"

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config_version": "v5.5",
        "verdict": verdict,
        "n_whales_calibrated": len(whale_stats),
        "n_adjustments": len(adjustments),
        "threshold_summary": {
            "profitable_above_0.50": profitable_above_threshold,
            "profitable_below_0.50": profitable_below_threshold,
            "unprofitable_below_0.30": unprofitable_below_threshold,
            "unprofitable_above_0.30": unprofitable_above_threshold,
        },
        "current_weights": current_weights,
        "recommended_weights": recommended if calibration_needed else current_weights,
        "weight_changes": (
            {k: v for k, v in recommended.items() if v != current_weights.get(k)}
        ) if calibration_needed else {},
        "adjustments": adjustments,
        "whale_scores": whale_score_details,
    }

    # ── 5. Write output ──────────────────────────────────────────────────────
    _out.parent.mkdir(parents=True, exist_ok=True)
    with open(_out, "w") as f:
        _json.dump(result, f, indent=2)
    logger.info("Edge scorer calibration written to %s", _out)

    # ── 6. Console summary ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("PHASE C3: EDGE SCORER CALIBRATION  (v5.5)")
    print("=" * 60)
    print(f"  Verdict:            {verdict}")
    print(f"  Whales calibrated:  {len(whale_stats)}")
    print(f"  Adjustments needed: {len(adjustments)}")
    print()
    print(f"  Threshold check:")
    print(f"    Profitable whales scoring >= 0.50: {profitable_above_threshold}/{total_profitable}")
    print(f"    Profitable whales scoring <  0.50: {profitable_below_threshold}/{total_profitable}")
    print(f"    Unprofitable whales scoring <= 0.30: {unprofitable_below_threshold}/{total_unprofitable}")
    print(f"    Unprofitable whales scoring >  0.30: {unprofitable_above_threshold}/{total_unprofitable}")
    if adjustments:
        print(f"\n  Adjustments ({len(adjustments)}):")
        for adj in adjustments[:5]:
            print(f"    {adj['whale']:25s} score={adj['current_score']:.3f} "
                  f"target={adj['target']} — {adj['reason']}")
    if calibration_needed:
        print(f"\n  Recommended weight changes:")
        for k, v in result["weight_changes"].items():
            print(f"    {k}: {current_weights[k]} -> {v}")
    print(f"\n  Saved: {_out}")
    print()

    return result


if __name__ == "__main__":
    calibrate_edge_scorer()


# ── T13/T16: Edge Confidence (standalone function) ────────────────────────────
# Imports are here (not top-level) to avoid circular imports.
# wf_constants may not yet have EDGE_CONFIDENCE_WEIGHTS during early boot.


def get_edge_confidence(
    *,
    edge_score: float = 0.0,
    whale_win_rate: float | None = None,
    whale_trade_count: int = 0,
    category: str = "general",
    source: str = "classifier",
) -> float:
    """Compute a calibrated confidence score from multiple signals.

    Uses EDGE_CONFIDENCE_WEIGHTS from wf_constants.py to combine:
      1. whale_win_rate  (45%) — whale's historical WR in this category
      2. edge_score      (30%) — pipeline edge_score (calibrated 0–1)
      3. trade_history   (15%) — recency/activity signal from whale_trade_count
      4. category_perf   (10%) — category-level WR from CATEGORY_WEIGHTS

    Returns a float in [0.0, 1.0]. Returns 0.0 for unknown/new whales
    with no history.

    Args:
        edge_score: Pipeline edge_score (0.0–1.0), already computed.
        whale_win_rate: Whale's historical win rate (0.0–1.0) in this category.
        whale_trade_count: Number of trades the whale has in this category.
        category: Market category (general, sports, crypto, geopolitics, etc.).
        source: Signal source (classifier | fallback | static).

    Returns:
        Calibrated confidence in [0.0, 1.0].
    """
    try:
        from strategies.wf_constants import (
            EDGE_CONFIDENCE_WEIGHTS,
            CATEGORY_WEIGHTS as _CAT_WEIGHTS,
        )
        weights = EDGE_CONFIDENCE_WEIGHTS
    except ImportError:
        # Graceful degradation if constants not yet loaded
        weights = {
            "whale_win_rate": 0.45,
            "edge_score": 0.30,
            "trade_history": 0.15,
            "category_perf": 0.10,
        }
        _CAT_WEIGHTS = {
            "general": 0.65,
            "sports": 0.30,
            "crypto": 0.15,
            "geopolitics": 0.10,
            "entertainment": 0.0,
            "finance": 0.0,
            "unknown": 0.05,
        }

    # ── Component 1: whale_win_rate (45%) ──────────────────────────────
    wr_signal = float(whale_win_rate) if whale_win_rate is not None else 0.0

    # ── Component 2: edge_score (30%) ─────────────────────────────────
    es_signal = float(max(0.0, min(1.0, edge_score)))

    # ── Component 3: trade_history (15%) ──────────────────────────────
    # More trades → higher confidence in the whale_win_rate signal.
    # Sigmoid: 0 trades=0, 5 trades≈0.38, 10 trades≈0.50, 20+ trades≈0.73
    if whale_trade_count <= 0:
        th_signal = 0.0
    else:
        import math
        th_signal = 1.0 - (1.0 / (1.0 + math.sqrt(max(0, whale_trade_count))))

    # ── Component 4: category_perf (10%) ───────────────────────────────
    cat_signal = _CAT_WEIGHTS.get(category, 0.05)

    confidence = (
        weights["whale_win_rate"] * wr_signal
        + weights["edge_score"] * es_signal
        + weights["trade_history"] * th_signal
        + weights["category_perf"] * cat_signal
    )

    # Unknown whale (no WR, no trade history): return edge_score only
    # with reduced weight — we have no independent confirmation.
    if whale_win_rate is None and whale_trade_count == 0:
        confidence = 0.60 * es_signal  # degrade to just edge_score * 0.60

    return round(max(0.0, min(1.0, confidence)), 4)
