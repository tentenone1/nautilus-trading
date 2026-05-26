"""Dynamic Whale Intelligence -- Real-time trust score updates and classification recomputation.

Extends the static whale_classifier.py with real-time updates:
- After each trade resolves, update the whale's win_rate, pnl, category_performance
- Recompute classification and action (copy/fade/ignore) dynamically
- Maintain a live trust score that reflects recent performance, not just historical
- Integrate with adaptive_intel for regime-aware whale performance tracking

The core insight: static classifications become stale. A whale that was skilled
3 months ago may have degraded. This module keeps classifications fresh by
continuously updating from live trade results.

Usage:
    intel = DynamicWhaleIntel(db_path="data/trades.db")
    intel.refresh()  # Full refresh from DB
    # After a trade resolves:
    intel.update_on_trade_resolve(whale_name="p37-0xe5efd6", category="sports", pnl=-50.0, won=False)
    # Get updated classification:
    cls = intel.get_classification("p37-0xe5efd6")
    trust = intel.get_trust_score("p37-0xe5efd6", "sports")
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("DynamicWhaleIntel")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
CLASSIFICATIONS_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_classifications.json")
STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/dynamic_whale_state.json")

# Recency weighting: how much to weight recent vs historical
TRUST_RECENCY_WEIGHT = 0.65
# Minimum trades required for dynamic classification
MIN_DYNAMIC_TRADES = 10
# Lookback window for recent performance (days)
RECENT_WINDOW_DAYS = 30
# Trust score decay: how fast trust decays without new data (days)
TRUST_DECAY_HALF_LIFE = 14
# Classification stability: minimum confidence delta to change classification
CLASSIFICATION_STABILITY_THRESHOLD = 0.15


@dataclass
class DynamicWhaleState:
    """Live state for a single whale, updated from trade results."""
    whale_name: str
    classification: str = "unknown"
    action: str = "ignore"
    action_confidence: float = 0.0
    overall_trust: float = 5.0
    overall_wr: float = 0.0
    overall_pnl: float = 0.0
    total_trades: int = 0
    recent_wr: float = 0.0
    recent_pnl: float = 0.0
    recent_trades: int = 0
    category_performance: dict = field(default_factory=dict)
    classification_confidence: float = 0.0
    last_updated: str = ""
    last_trade_time: str = ""


class DynamicWhaleIntel:
    """Real-time whale intelligence engine.

    Maintains live trust scores and classifications that update as trades resolve.
    Blends historical and recent performance to detect regime changes in whale
    behavior (e.g., a skilled whale that has started losing).
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        classifications_path: str | Path | None = None,
        state_path: str | Path | None = None,
        recency_weight: float = TRUST_RECENCY_WEIGHT,
        min_trades: int = MIN_DYNAMIC_TRADES,
        recent_window_days: int = RECENT_WINDOW_DAYS,
    ):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.classifications_path = Path(classifications_path) if classifications_path else CLASSIFICATIONS_PATH
        self.state_path = Path(state_path) if state_path else STATE_PATH
        self.recency_weight = recency_weight
        self.min_trades = min_trades
        self.recent_window_days = recent_window_days

        self._whales: dict[str, DynamicWhaleState] = {}
        self._last_full_refresh: datetime | None = None
        self._load_state()

    def refresh(self) -> None:
        """Full refresh from DB. Recomputes all whale states from trades."""
        self._refresh_from_db()
        self._refresh_from_classifications()
        self._last_full_refresh = datetime.now(timezone.utc)
        self._save_state()
        logger.info("DynamicWhaleIntel refreshed: %d whales", len(self._whales))

    def _refresh_from_db(self) -> None:
        """Query trades.db for per-whale performance."""
        if not self.db_path.exists():
            logger.warning("trades.db not found at %s", self.db_path)
            return

        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=self.recent_window_days)).isoformat()

            rows = conn.execute("""
                SELECT whale_name,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                       ROUND(SUM(actual_pnl), 2) as total_pnl
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                GROUP BY whale_name
                HAVING COUNT(*) >= ?
            """, (self.min_trades,)).fetchall()

            for row in rows:
                name = row["whale_name"]
                if name not in self._whales:
                    self._whales[name] = DynamicWhaleState(whale_name=name)
                ws = self._whales[name]
                ws.overall_wr = row["win_rate"] or 0.0
                ws.overall_pnl = row["total_pnl"] or 0.0
                ws.total_trades = row["trades"] or 0

            recent_rows = conn.execute("""
                SELECT whale_name,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                       ROUND(SUM(actual_pnl), 2) as total_pnl
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                  AND timestamp >= ?
                GROUP BY whale_name
                HAVING COUNT(*) >= ?
            """, (cutoff, 2)).fetchall()

            for row in recent_rows:
                name = row["whale_name"]
                if name in self._whales:
                    self._whales[name].recent_wr = row["win_rate"] or 0.0
                    self._whales[name].recent_pnl = row["total_pnl"] or 0.0
                    self._whales[name].recent_trades = row["trades"] or 0

            cat_rows = conn.execute("""
                SELECT whale_name, category,
                       COUNT(*) as trades,
                       ROUND(SUM(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                       ROUND(SUM(actual_pnl), 2) as total_pnl
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND whale_name IS NOT NULL
                  AND whale_name != 'autoresearch_llm'
                  AND category IS NOT NULL
                GROUP BY whale_name, category
                HAVING COUNT(*) >= ?
            """, (2,)).fetchall()

            for row in cat_rows:
                name = row["whale_name"]
                cat = row["category"]
                if name in self._whales:
                    self._whales[name].category_performance[cat] = {
                        "wr": row["win_rate"] or 0.0,
                        "pnl": row["total_pnl"] or 0.0,
                        "trades": row["trades"] or 0,
                    }
        finally:
            conn.close()

    def _refresh_from_classifications(self) -> None:
        """Load static classifications as priors, then override with dynamic data."""
        if not self.classifications_path.exists():
            return
        try:
            data = json.loads(self.classifications_path.read_text())
            for name, cls_data in data.get("classifications", {}).items():
                if name not in self._whales:
                    self._whales[name] = DynamicWhaleState(whale_name=name)
                ws = self._whales[name]
                if ws.classification == "unknown":
                    ws.classification = cls_data.get("classification", "unknown")
                    ws.action = cls_data.get("action", "ignore")
                    ws.action_confidence = cls_data.get("action_confidence", 0.0)
                    ws.classification_confidence = cls_data.get("confidence", 0.0)
        except Exception as e:
            logger.warning("Failed to load classifications: %s", e)

        for name, ws in self._whales.items():
            self._recompute_classification(ws)

    def _recompute_classification(self, ws: DynamicWhaleState) -> None:
        """Recompute classification and action for a whale based on live data."""
        if ws.total_trades < self.min_trades:
            ws.classification = "untested"
            ws.action = "ignore"
            ws.action_confidence = 0.0
            ws.overall_trust = min(ws.overall_trust, 3.0)
            return

        if ws.recent_trades >= 3:
            blended_wr = self.recency_weight * ws.recent_wr + (1 - self.recency_weight) * ws.overall_wr
            blended_pnl = self.recency_weight * ws.recent_pnl + (1 - self.recency_weight) * ws.overall_pnl
        else:
            blended_wr = ws.overall_wr
            blended_pnl = ws.overall_pnl

        old_class = ws.classification
        old_action = ws.action

        if blended_wr >= 0.55 and ws.total_trades >= 10 and blended_pnl > 0:
            new_class = "skilled_human"
        elif blended_wr <= 0.15 and ws.total_trades >= 10:
            new_class = "sacrificial_account"
        elif blended_wr <= 0.30 and ws.total_trades >= 10:
            new_class = "degenerate_human"
        elif ws.total_trades >= 20 and blended_pnl < 0 and blended_wr < 0.40:
            new_class = "degenerate_human"
        elif ws.total_trades < 10:
            new_class = "untested"  # Not enough data to classify
        else:
            new_class = old_class if old_class != "unknown" else "mixed_entity"

        if blended_wr >= 0.55 and blended_pnl > 0 and ws.total_trades >= 10:
            new_action = "copy"
            action_conf = min(1.0, (blended_wr - 0.50) * 4 + (1 if blended_pnl > 500 else 0.5))
        elif blended_wr <= 0.25 and ws.total_trades >= 10 and blended_pnl < 0:
            new_action = "fade"
            action_conf = min(1.0, (0.30 - blended_wr) * 4)
        else:
            new_action = "ignore"
            action_conf = 0.3

        # Stability check
        if old_class != "unknown" and abs(ws.classification_confidence - 0.5) > CLASSIFICATION_STABILITY_THRESHOLD:
            if new_class != old_class and ws.classification_confidence > 0.7:
                new_class = old_class

        trust = self._compute_trust(ws, blended_wr, blended_pnl)

        ws.classification = new_class
        ws.action = new_action
        ws.action_confidence = round(action_conf, 3)
        ws.overall_trust = round(trust, 1)
        ws.classification_confidence = round(abs(blended_wr - 0.40) * 2, 3) if blended_wr > 0 else 0.1
        ws.last_updated = datetime.now(timezone.utc).isoformat()

    def _compute_trust(self, ws: DynamicWhaleState, blended_wr: float, blended_pnl: float) -> float:
        """Compute trust score (0-10) for a whale."""
        wr_score = blended_wr * 4.0
        pnl_score = min(1.0, max(0.0, blended_pnl / 1000.0)) * 3.0 if blended_pnl > 0 else max(0.0, 1.0 + blended_pnl / 500.0) * 1.5
        consistency = min(1.0, ws.total_trades / 30.0) * 2.0
        recency = min(1.0, ws.recent_trades / 10.0) * 1.0 if ws.recent_wr > 0.40 else 0.5

        trust = wr_score + pnl_score + consistency + recency
        return max(0.0, min(10.0, trust))

    def update_on_trade_resolve(self, whale_name: str, category: str, pnl: float, won: bool) -> DynamicWhaleState:
        """Update whale state after a trade resolves.

        Called from position_manager when a trade closes. Incrementally updates
        the whale's performance and recomputes classification.
        """
        if whale_name not in self._whales:
            self._whales[whale_name] = DynamicWhaleState(whale_name=whale_name)

        ws = self._whales[whale_name]
        ws.total_trades += 1
        total_wins = round(ws.overall_wr * (ws.total_trades - 1))
        if won:
            total_wins += 1
        ws.overall_wr = round(total_wins / ws.total_trades, 4)
        ws.overall_pnl = round(ws.overall_pnl + pnl, 2)

        ws.recent_trades += 1
        recent_wins = round(ws.recent_wr * (ws.recent_trades - 1))
        if won:
            recent_wins += 1
        ws.recent_wr = round(recent_wins / ws.recent_trades, 4)
        ws.recent_pnl = round(ws.recent_pnl + pnl, 2)

        if category not in ws.category_performance:
            ws.category_performance[category] = {"wr": 0.0, "pnl": 0.0, "trades": 0}
        cat = ws.category_performance[category]
        cat_wins = round(cat["wr"] * cat["trades"])
        if won:
            cat_wins += 1
        cat["trades"] += 1
        cat["wr"] = round(cat_wins / cat["trades"], 4)
        cat["pnl"] = round(cat["pnl"] + pnl, 2)

        self._recompute_classification(ws)
        ws.last_trade_time = datetime.now(timezone.utc).isoformat()
        self._save_state()

        logger.info(
            "WHALE_INTEL_UPDATE | %s | class=%s action=%s trust=%.1f WR=%.1f%% recent_WR=%.1f%% PnL=%.0f",
            whale_name, ws.classification, ws.action, ws.overall_trust,
            ws.overall_wr * 100, ws.recent_wr * 100, ws.overall_pnl,
        )
        return ws

    def get_classification(self, whale_name: str) -> DynamicWhaleState | None:
        """Get current dynamic classification for a whale."""
        return self._whales.get(whale_name)

    def get_trust_score(self, whale_name: str, category: str = "") -> float:
        """Get trust score for a whale, optionally category-specific."""
        ws = self._whales.get(whale_name)
        if not ws:
            return 5.0
        if category and category in ws.category_performance:
            cat = ws.category_performance[category]
            cat_trust = min(10.0, cat["wr"] * 5.0 + (1.0 if cat["pnl"] > 0 else 0.5) * 3.0 + min(cat["trades"] / 10.0, 1.0) * 2.0)
            return round(ws.overall_trust * 0.4 + cat_trust * 0.6, 1)
        return ws.overall_trust

    def get_action(self, whale_name: str, category: str = "") -> str:
        """Get recommended action (copy/fade/ignore) for a whale."""
        ws = self._whales.get(whale_name)
        if not ws:
            return "ignore"
        if category and category in ws.category_performance:
            cat = ws.category_performance[category]
            if cat["trades"] >= 3:
                if cat["wr"] >= 0.50 and cat["pnl"] > 0:
                    return "copy"
                elif cat["wr"] <= 0.25 and cat["trades"] >= 5:
                    return "fade"
        return ws.action

    def get_top_whales(self, category: str = "", action: str = "", limit: int = 20) -> list[DynamicWhaleState]:
        """Get top whales ranked by trust score."""
        whales = list(self._whales.values())
        if category:
            whales = [w for w in whales if category in w.category_performance]
        if action:
            whales = [w for w in whales if w.action == action]
        whales.sort(key=lambda w: w.overall_trust, reverse=True)
        return whales[:limit]

    def get_fade_targets(self, category: str = "", min_trades: int = 5, limit: int = 20) -> list[DynamicWhaleState]:
        """Get whales that should be faded (consistently losing)."""
        results = [
            w for w in self._whales.values()
            if w.action == "fade"
            and w.total_trades >= min_trades
            and (not category or category in w.category_performance)
        ]
        return results[:limit]

    def get_snapshot(self) -> dict:
        """Get a full snapshot of all whale states for dashboard consumption."""
        return {
            "last_full_refresh": self._last_full_refresh.isoformat() if self._last_full_refresh else None,
            "whale_count": len(self._whales),
            "copy_count": sum(1 for w in self._whales.values() if w.action == "copy"),
            "fade_count": sum(1 for w in self._whales.values() if w.action == "fade"),
            "ignore_count": sum(1 for w in self._whales.values() if w.action == "ignore"),
            "whales": {name: asdict(ws) for name, ws in self._whales.items()},
        }

    def apply_size_modifier(self, whale_name: str, base_size: float, category: str = "") -> tuple[float, str]:
        """Apply trust-based size modifier for a whale."""
        state = self.get_classification(whale_name)
        if state is None:
            return base_size, "no_intel_data"
        trust = state.trust if hasattr(state, 'trust') and state.trust else 5.0
        trust_mult = max(0.25, min(2.0, trust / 5.0))
        new_size = round(base_size * trust_mult, 2)
        note = f"trust={trust:.1f} mult={trust_mult:.2f}" if abs(trust_mult - 1.0) > 0.05 else ""
        return new_size, note

    def _save_state(self) -> None:
        """Persist dynamic whale state to JSON."""
        data = {
            "last_full_refresh": self._last_full_refresh.isoformat() if self._last_full_refresh else None,
            "whales": {name: asdict(ws) for name, ws in self._whales.items()},
        }
        self.state_path.write_text(json.dumps(data, indent=2, default=str))

    def _load_state(self) -> None:
        """Load persisted state from JSON."""
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text())
            for name, ws_data in data.get("whales", {}).items():
                cat_perf = ws_data.get("category_performance", {})
                ws = DynamicWhaleState(
                    whale_name=ws_data.get("whale_name", name),
                    classification=ws_data.get("classification", "unknown"),
                    action=ws_data.get("action", "ignore"),
                    action_confidence=ws_data.get("action_confidence", 0.0),
                    overall_trust=ws_data.get("overall_trust", 5.0),
                    overall_wr=ws_data.get("overall_wr", 0.0),
                    overall_pnl=ws_data.get("overall_pnl", 0.0),
                    total_trades=ws_data.get("total_trades", 0),
                    recent_wr=ws_data.get("recent_wr", 0.0),
                    recent_pnl=ws_data.get("recent_pnl", 0.0),
                    recent_trades=ws_data.get("recent_trades", 0),
                    category_performance=cat_perf,
                    classification_confidence=ws_data.get("classification_confidence", 0.0),
                    last_updated=ws_data.get("last_updated", ""),
                    last_trade_time=ws_data.get("last_trade_time", ""),
                )
                self._whales[name] = ws
            if data.get("last_full_refresh"):
                self._last_full_refresh = datetime.fromisoformat(data["last_full_refresh"])
        except Exception as e:
            logger.warning("Failed to load dynamic whale state: %s", e)
