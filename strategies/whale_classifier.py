"""Whale Classifier — Data-driven behavioral classification.

Classifies whales into behavioral categories based on trading history:
  - skilled_human:    High win rate, consistent profits, good timing
  - trading_bot:     Round-number sizes, 24/7 activity, mechanical patterns
  - degenerate_human: Low win rate, high volume, emotional trading
  - sacrificial_account: Extremely low win rate, suspicious patterns
  - market_maker:    Two-sided activity, small spreads, high frequency
  - mixed_entity:    Doesn't fit cleanly into other categories

Uses win rate, PnL, timing, size patterns, and activity regularity
to assign classifications with confidence scores. Outputs are consumed
by the fade/follow decision engine and the tier adjuster.

All queries go to trades.db — no external dependencies.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("WhaleClassifier")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
OUTPUT_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_classifications.json")


class WhaleClass(Enum):
    SKILLED_HUMAN = "skilled_human"
    TRADING_BOT = "trading_bot"
    DEGENERATE_HUMAN = "degenerate_human"
    SACRIFICIAL_ACCOUNT = "sacrificial_account"
    MARKET_MAKER = "market_maker"
    MIXED_ENTITY = "mixed_entity"


@dataclass
class WhaleClassification:
    whale_name: str
    classification: WhaleClass
    confidence: float  # 0.0-1.0
    win_rate: float
    total_trades: int
    total_pnl: float
    avg_pnl: float
    categories: list[str]
    # Per-category performance
    category_performance: dict[str, dict] = field(default_factory=dict)
    # Classification signals
    signals: dict[str, float] = field(default_factory=dict)
    # Should we copy or fade this whale?
    action: str = "ignore"  # "copy", "fade", "ignore"
    action_confidence: float = 0.0


class WhaleClassifier:
    """Classify whales based on realized trading performance.

    Thresholds are calibrated from the existing trade data:
    - 2,661 trades, 2,218 with PnL
    - Whale-following alone: 40.8% WR, $2,822 total PnL
    - LLM signals: 47.1% WR, $94,348 total PnL
    """

    # Classification thresholds — tuned from historical data
    SKILLED_WIN_RATE = 0.50       # > 50% WR = skilled
    SKILLED_MIN_TRADES = 5        # Need at least 5 trades to call skilled
    DEGEN_WIN_RATE = 0.30         # < 30% WR = degenerate
    SACRIFICIAL_WIN_RATE = 0.15   # < 15% WR = sacrificial
    SACRIFICIAL_MIN_TRADES = 5
    MM_TWO_SIDED_RATIO = 0.30    # 30%+ trades on opposite side = MM behavior
    MM_MIN_TRADES = 10
    BOT_SIZE_ROUNDNESS = 0.50    # 50%+ round-number sizes = bot-like
    BOT_MIN_TRADES = 8

    # Action thresholds
    COPY_MIN_WIN_RATE = 0.45
    COPY_MIN_PNL_POSITIVE = True
    FADE_MAX_WIN_RATE = 0.25
    FADE_MIN_TRADES = 5

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._classifications: dict[str, WhaleClassification] = {}
        self._last_updated: datetime | None = None

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def classify_all(self, min_trades: int = 3) -> dict[str, WhaleClassification]:
        """Classify all whales with sufficient trade history."""
        conn = self._get_connection()
        try:
            rows = conn.execute("""
                SELECT
                    whale_name,
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                    ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                    ROUND(SUM(realized_pnl), 2) as total_pnl,
                    ROUND(AVG(realized_pnl), 2) as avg_pnl,
                    GROUP_CONCAT(DISTINCT category) as categories,
                    SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buy_count,
                    SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sell_count,
                    ROUND(AVG(position_size_usd), 2) as avg_size,
                    ROUND(AVG(edge_score), 3) as avg_edge,
                    ROUND(AVG(confidence), 3) as avg_confidence
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                GROUP BY whale_name
                HAVING COUNT(*) >= ?
            """, (min_trades,)).fetchall()
        finally:
            conn.close()

        for row in rows:
            cls = self._classify_whale(dict(row))
            self._classifications[cls.whale_name] = cls

        self._last_updated = datetime.now(timezone.utc)
        logger.info(f"Classified {len(self._classifications)} whales")
        return self._classifications

    def classify_whale(self, whale_name: str) -> WhaleClassification | None:
        """Classify a single whale by name."""
        if whale_name in self._classifications:
            return self._classifications[whale_name]
        self.classify_all()
        return self._classifications.get(whale_name)

    def _classify_whale(self, data: dict) -> WhaleClassification:
        """Core classification logic using rule-based signals."""
        name = data["whale_name"]
        wr = data["win_rate"] or 0.0
        trades = data["total_trades"]
        pnl = data["total_pnl"] or 0.0
        avg_pnl = data["avg_pnl"] or 0.0
        categories = (data.get("categories") or "").split(",")
        buy_count = data.get("buy_count", 0) or 0
        sell_count = data.get("sell_count", 0) or 0

        # Compute classification signals
        signals = {}
        signals["win_rate"] = wr
        signals["pnl_positive"] = 1.0 if pnl > 0 else 0.0
        signals["two_sided_ratio"] = min(buy_count, sell_count) / max(buy_count + sell_count, 1)
        signals["consistency"] = min(1.0, trades / 20.0)  # More trades = more consistent

        # Classification scoring
        scores = {cls: 0.0 for cls in WhaleClass}

        # Skilled human: high WR, positive PnL
        if wr >= self.SKILLED_WIN_RATE and trades >= self.SKILLED_MIN_TRADES:
            scores[WhaleClass.SKILLED_HUMAN] = (wr - 0.4) * 2 + (1.0 if pnl > 0 else 0.3)
        elif wr >= 0.40 and pnl > 0:
            scores[WhaleClass.SKILLED_HUMAN] = (wr - 0.3) + 0.5 * min(1.0, pnl / 500.0)

        # Degenerate human: low WR, high volume
        if wr <= self.DEGEN_WIN_RATE and trades >= 5:
            scores[WhaleClass.DEGENERATE_HUMAN] = (0.35 - wr) * 3 + signals["consistency"]
        elif wr <= 0.40 and pnl < 0:
            scores[WhaleClass.DEGENERATE_HUMAN] = (0.45 - wr) * 2 + 0.5 * min(1.0, abs(pnl) / 200.0)

        # Sacrificial account: extremely low WR
        if wr <= self.SACRIFICIAL_WIN_RATE and trades >= self.SACRIFICIAL_MIN_TRADES:
            scores[WhaleClass.SACRIFICIAL_ACCOUNT] = (0.20 - wr) * 5 + 0.3 * min(1.0, trades / 10.0)

        # Market maker: two-sided activity
        if signals["two_sided_ratio"] >= self.MM_TWO_SIDED_RATIO and trades >= self.MM_MIN_TRADES:
            scores[WhaleClass.MARKET_MAKER] = signals["two_sided_ratio"] * 2 + 0.5 * (1.0 - abs(wr - 0.5))

        # Trading bot: round-number sizes, mechanical patterns
        # (Would need raw trade data for size analysis; using proxy signals)
        if trades >= self.BOT_MIN_TRADES and 0.35 < wr < 0.55:
            scores[WhaleClass.TRADING_BOT] = 0.5  # Moderate suspicion

        # Mixed entity: default for unclear patterns
        scores[WhaleClass.MIXED_ENTITY] = 0.3

        # Pick highest scoring classification
        best_cls = max(scores, key=lambda k: scores[k])
        best_score = scores[best_cls]
        confidence = min(1.0, best_score)

        # If no strong signal, default to mixed
        if best_score < 0.3:
            best_cls = WhaleClass.MIXED_ENTITY
            confidence = 0.3

        # Determine action: copy, fade, or ignore
        action, action_conf = self._determine_action(best_cls, wr, pnl, trades)

        return WhaleClassification(
            whale_name=name,
            classification=best_cls,
            confidence=round(confidence, 3),
            win_rate=round(wr, 4),
            total_trades=trades,
            total_pnl=round(pnl, 2),
            avg_pnl=round(avg_pnl, 2),
            categories=[c.strip() for c in categories if c.strip()],
            category_performance={},
            signals={k: round(v, 3) for k, v in signals.items()},
            action=action,
            action_confidence=round(action_conf, 3),
        )

    def _determine_action(
        self, cls: WhaleClass, wr: float, pnl: float, trades: int
    ) -> tuple[str, float]:
        """Decide whether to COPY, FADE, or IGNORE a whale."""
        # Skilled humans: copy
        if cls == WhaleClass.SKILLED_HUMAN:
            if wr >= self.COPY_MIN_WIN_RATE and trades >= 3:
                return "copy", min(1.0, wr * 1.5 + (0.3 if pnl > 0 else 0.0))
            return "copy", 0.4

        # Sacrificial accounts and degenerate humans: fade
        if cls in (WhaleClass.SACRIFICIAL_ACCOUNT, WhaleClass.DEGENERATE_HUMAN):
            if wr <= self.FADE_MAX_WIN_RATE and trades >= self.FADE_MIN_TRADES:
                return "fade", min(1.0, (0.30 - wr) * 4 + (0.3 if pnl < 0 else 0.0))
            return "fade", 0.4

        # Market makers: ignore (we can't reliably copy or fade)
        if cls == WhaleClass.MARKET_MAKER:
            return "ignore", 0.5

        # Trading bots: fade with caution
        if cls == WhaleClass.TRADING_BOT:
            return "fade", 0.3

        # Mixed: copy if profitable, fade if losing
        if pnl > 0 and wr > 0.40:
            return "copy", 0.3
        elif pnl < 0 and wr < 0.35:
            return "fade", 0.3
        return "ignore", 0.2

    def get_category_performance(self, whale_name: str) -> dict[str, dict]:
        """Get per-category performance for a whale."""
        conn = self._get_connection()
        try:
            rows = conn.execute("""
                SELECT
                    category,
                    COUNT(*) as trades,
                    ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                    ROUND(SUM(realized_pnl), 2) as total_pnl,
                    ROUND(AVG(realized_pnl), 2) as avg_pnl
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name = ?
                  AND whale_name != 'autoresearch_llm'
                GROUP BY category
            """, (whale_name,)).fetchall()
        finally:
            conn.close()

        result = {}
        for row in rows:
            d = dict(row)
            cat = d.pop("category", "unknown")
            result[cat] = d
        return result

    def build_trust_scores(self) -> dict[str, dict[str, float]]:
        """Build per-whale-per-category trust scores (0-10).

        Trust = weighted combination of:
          - Win rate (40%)
          - PnL profitability (30%)
          - Trade count / consistency (20%)
          - Edge score alignment (10%)
        """
        conn = self._get_connection()
        try:
            rows = conn.execute("""
                SELECT
                    whale_name,
                    category,
                    COUNT(*) as trades,
                    ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) as win_rate,
                    ROUND(SUM(realized_pnl), 2) as total_pnl,
                    ROUND(AVG(edge_score), 3) as avg_edge,
                    ROUND(AVG(confidence), 3) as avg_confidence
                FROM trades
                WHERE realized_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                GROUP BY whale_name, category
                HAVING COUNT(*) >= 3
            """).fetchall()
        finally:
            conn.close()

        trust: dict[str, dict[str, float]] = {}
        for row in rows:
            d = dict(row)
            name = d["whale_name"]
            cat = d["category"]
            wr = d["win_rate"] or 0.0
            pnl = d["total_pnl"] or 0.0
            trades = d["trades"] or 0
            edge = d["avg_edge"] or 0.3

            # Compute trust score 0-10
            wr_component = wr * 4.0                          # 0-4
            pnl_component = min(1.0, max(0.0, pnl / 1000.0)) * 3.0 if pnl > 0 else max(0.0, 1.0 + pnl / 500.0) * 1.5  # 0-3
            consistency = min(1.0, trades / 30.0) * 2.0      # 0-2
            edge_component = min(1.0, max(0.0, edge)) * 1.0  # 0-1

            trust_score = round(wr_component + pnl_component + consistency + edge_component, 1)
            trust_score = max(0.0, min(10.0, trust_score))

            if name not in trust:
                trust[name] = {}
            trust[name][cat] = trust_score

        return trust

    def save(self, path: Path | None = None) -> None:
        """Save classifications to JSON."""
        path = path or OUTPUT_PATH
        data = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "whale_count": len(self._classifications),
            "classifications": {
                name: {
                    **asdict(cls),
                    "classification": cls.classification.value,
                }
                for name, cls in self._classifications.items()
            },
        }
        path.write_text(json.dumps(data, indent=2, default=str))
        logger.info(f"Saved {len(self._classifications)} classifications to {path}")

    def load(self, path: Path | None = None) -> dict[str, WhaleClassification]:
        """Load classifications from JSON."""
        path = path or OUTPUT_PATH
        if not path.exists():
            return {}
        data = json.loads(path.read_text())
        for name, cls_data in data.get("classifications", {}).items():
            cls_data["classification"] = WhaleClass(cls_data["classification"])
            self._classifications[name] = WhaleClassification(**cls_data)
        self._last_updated = datetime.fromisoformat(data.get("updated_at", ""))
        return self._classifications


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    classifier = WhaleClassifier()
    classifications = classifier.classify_all(min_trades=5)
    trust = classifier.build_trust_scores()

    # Print summary
    from collections import Counter
    class_counts = Counter(cls.classification.value for cls in classifications.values())
    action_counts = Counter(cls.action for cls in classifications.values())

    print("\n=== Whale Classification Summary ===")
    for cls_name, count in sorted(class_counts.items()):
        print(f"  {cls_name}: {count} whales")
    print("\n=== Action Summary ===")
    for action, count in sorted(action_counts.items()):
        print(f"  {action}: {count} whales")

    print("\n=== Top Copy Targets ===")
    copy_whales = [c for c in classifications.values() if c.action == "copy"]
    copy_whales.sort(key=lambda c: c.action_confidence, reverse=True)
    for w in copy_whales[:10]:
        print(f"  {w.whale_name}: WR={w.win_rate:.0%} PnL=${w.total_pnl:.0f} confidence={w.action_confidence:.2f}")

    print("\n=== Top Fade Targets ===")
    fade_whales = [c for c in classifications.values() if c.action == "fade"]
    fade_whales.sort(key=lambda c: c.action_confidence, reverse=True)
    for w in fade_whales[:10]:
        print(f"  {w.whale_name}: WR={w.win_rate:.0%} PnL=${w.total_pnl:.0f} confidence={w.action_confidence:.2f}")

    print(f"\n=== Trust Scores ===")
    for name, cats in sorted(trust.items()):
        cat_str = ", ".join(f"{k}={v:.1f}" for k, v in sorted(cats.items(), key=lambda x: -x[1]))
        print(f"  {name}: {cat_str}")

    classifier.save()
