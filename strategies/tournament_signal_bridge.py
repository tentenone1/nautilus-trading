"""Tournament Signal Bridge — reads weekly backtest tournament and adjusts WhaleFollower sizing."""

from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Optional

NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
RESULTS_DIR = NAUTILUS_ROOT / "backtest_results"
RESULTS_FILE = RESULTS_DIR / "weekly_tournament_latest.json"


class TournamentSignalBridge:
    """Reads weekly tournament results, provides sizing advisory to WhaleFollower."""

    def __init__(self, refresh_interval: float = 3600.0):
        """
        Args:
            refresh_interval: How often to reload the tournament file (seconds).
                              Set to 3600 (1h) so it refreshes daily during live trading.
        """
        self._refresh_interval = refresh_interval
        self._last_load: float = 0
        self._advisory: Optional[dict] = None
        self._load_advisory()

    def _load_advisory(self) -> None:
        """Reload tournament JSON if stale."""
        now = time.time()
        if self._advisory is not None and (now - self._last_load) < self._refresh_interval:
            return
        if not RESULTS_FILE.exists():
            self._advisory = self._default_advisory()
            return
        try:
            data = json.loads(RESULTS_FILE.read_text())
            self._advisory = self._compute_advisory(data)
        except Exception:
            self._advisory = self._default_advisory()
        self._last_load = now

    def _compute_advisory(self, data: dict) -> dict:
        """Compute advisory from tournament data."""
        strategies = data.get("strategies", [])
        if not strategies:
            return self._default_advisory()

        ranked = sorted(strategies, key=lambda s: s.get("sharpe", -999), reverse=True)

        whale_entry = next((s for s in ranked if s.get("strategy") == "whale_follower"), None)
        whale_rank = ranked.index(whale_entry) + 1 if whale_entry else len(ranked)

        top_strategy = ranked[0].get("strategy", "unknown") if ranked else "unknown"
        top_sharpe = ranked[0].get("sharpe", 0) if ranked else 0

        total_strategies = len(ranked)
        if whale_rank == 1:
            action = "increase_size"
            confidence = min(1.0, 0.5 + (top_sharpe * 0.1))
        elif whale_rank > total_strategies - 2:
            action = "reduce_size"
            confidence = 0.6
        else:
            action = "hold"
            confidence = 0.3

        size_multiplier = {
            "increase_size": 1.25,
            "reduce_size": 0.5,
            "hold": 1.0,
        }[action]

        return {
            "primary_strategy": top_strategy,
            "whale_follower_rank": whale_rank,
            "total_strategies": total_strategies,
            "top_sharpe": top_sharpe,
            "action": action,
            "confidence": confidence,
            "size_multiplier": size_multiplier,
            "generated_at": data.get("generated_at", ""),
        }

    def _default_advisory(self) -> dict:
        return {
            "primary_strategy": "unknown",
            "whale_follower_rank": -1,
            "total_strategies": 0,
            "top_sharpe": 0,
            "action": "hold",
            "confidence": 0,
            "size_multiplier": 1.0,
            "generated_at": "",
        }

    def get_advisory(self) -> dict:
        """Get current tournament advisory. Auto-refreshes if stale."""
        self._load_advisory()
        return self._advisory

    def get_size_multiplier(self) -> float:
        """Convenience: just get the size multiplier."""
        return self.get_advisory().get("size_multiplier", 1.0)