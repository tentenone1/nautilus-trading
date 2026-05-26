"""Whale Regime Detector — Detects behavioral shifts in whale activity.

The core problem: a whale that was losing may have changed strategy (loss leader
reversal, new approach, regime change). Fading a whale that has ALREADY started
winning is catastrophic. This module detects:

1. **Loss Leader Reversal**: A whale was losing, we started fading them, then
   they reversed strategy. We need to stop fading immediately.

2. **Regime Change**: A whale's WR pattern shifts significantly (e.g., from 30%
   to 60% over recent trades). This invalidates both copy and fade decisions
   based on stale data.

3. **Adversarial Detection**: A whale whose losing pattern coincides with
   unusually large position sizes on their winning side, suggesting they may
   be running a two-sided strategy (lose small on one side, win big on another).

4. **Fade Decay**: The edge from fading a whale diminishes over time as either
   (a) the whale adapts, or (b) the market adjusts. We track fade effectiveness
   and auto-expire stale fade classifications.

Usage:
    detector = RegimeDetector(db_path="data/trades.db")
    detector.refresh()
    
    # Check before acting on a fade signal:
    regime = detector.check_whale_regime("JewishNinja", "sports", "BUY")
    if regime.should_fade:
        # Safe to fade
    elif regime.regime_change_detected:
        # Stop fading, whale behavior shifted
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("RegimeDetector")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/regime_state.json")

# ── Detection Parameters ──────────────────────────────────────────────────────
REGIME_WINDOW_RECENT = 10      # Number of recent trades to check for regime change
REGIME_WINDOW_HISTORICAL = 30   # Number of historical trades to compare against
REGIME_WR_SHIFT_THRESHOLD = 0.20  # 20% WR shift = regime change
REGIME_PNL_SHIFT_THRESHOLD = 0.50  # 50% PnL direction shift = regime change
LOSS_LEADER_SIZE_RATIO = 2.0    # If winning-side positions are 3x larger than losing-side
FADE_DECAY_DAYS = 14             # Fade classifications decay after 14 days without confirmation
MIN_TRADES_FOR_REGIME = 5       # Minimum trades before detecting regime changes
ADVERSARIAL_WR_THRESHOLD = 0.10  # If WR jumps by >10% after fade starts, suspect adversarial


@dataclass
class WhaleRegimeResult:
    """Result of regime detection for a whale+category+side combination."""
    whale_name: str
    category: str
    side: str
    should_fade: bool = True       # Whether it's still safe to fade
    regime_change_detected: bool = False  # Whale behavior has shifted
    loss_leader_suspected: bool = False    # Whale may be running a loss leader strategy
    adversarial_suspected: bool = False    # Whale may be adapting to being faded
    recent_wr: float = 0.0        # Win rate in recent window
    historical_wr: float = 0.0     # Win rate in historical window
    wr_shift: float = 0.0         # Shift in WR (positive = improving)
    recent_pnl: float = 0.0      # PnL in recent window
    historical_pnl: float = 0.0   # PnL in historical window
    fade_age_days: float = 0.0    # How long since fade classification was confirmed
    fade_confidence: float = 0.0  # Confidence in fade decision (0-1)
    regime_type: str = ""         # "stable_losing", "improving", "reversed", "adversarial", "loss_leader"
    reason: str = ""


@dataclass
class WhaleRegimeState:
    """Persisted state for regime detection."""
    whale_regimes: dict = field(default_factory=dict)  # whale_name -> {category_side: WhaleRegimeResult}
    last_updated: str = ""
    total_checks: int = 0
    regime_changes_detected: int = 0
    loss_leaders_detected: int = 0


class RegimeDetector:
    """Detects behavioral shifts in whale activity that invalidate fade decisions.

    The key insight: fading is only profitable if the whale's losing pattern
    is STABLE. If the whale's behavior shifts (regime change), we need to
    stop fading immediately. This module:
    
    1. Compares recent performance to historical performance
    2. Detects whales that have started winning (loss leader reversal)
    3. Detects whales running two-sided strategies (adversarial)
    4. Decays fade classifications over time
    5. Tracks fade effectiveness to auto-expire stale fades
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        state_path: str | Path | None = None,
        regime_window_recent: int = REGIME_WINDOW_RECENT,
        regime_window_historical: int = REGIME_WINDOW_HISTORICAL,
        regime_wr_threshold: float = REGIME_WR_SHIFT_THRESHOLD,
    ):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.state_path = Path(state_path) if state_path else STATE_PATH
        self.regime_window_recent = regime_window_recent
        self.regime_window_historical = regime_window_historical
        self.regime_wr_threshold = regime_wr_threshold
        self._state = WhaleRegimeState()
        self._load_state()

    def refresh(self) -> None:
        """Full refresh of regime state from DB."""
        if not self.db_path.exists():
            logger.warning("RegimeDetector: DB not found at %s", self.db_path)
            return

        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            
            # Get all whales with enough trades for regime analysis
            rows = conn.execute("""
                SELECT whale_name, side, category, COUNT(*) as trades,
                    ROUND(AVG(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                    ROUND(SUM(actual_pnl), 2) as pnl
                FROM trades
                WHERE actual_pnl IS NOT NULL
                GROUP BY whale_name, side, category
                HAVING trades >= ?
            """, (MIN_TRADES_FOR_REGIME,)).fetchall()
            
            # Get per-whale overall stats for loss leader detection
            overall_rows = conn.execute("""
                SELECT whale_name, side,
                    COUNT(*) as trades,
                    ROUND(AVG(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                    ROUND(SUM(actual_pnl), 2) as pnl,
                    ROUND(AVG(position_size_usd), 2) as avg_size
                FROM trades
                WHERE actual_pnl IS NOT NULL
                GROUP BY whale_name, side
                HAVING trades >= ?
            """, (MIN_TRADES_FOR_REGIME,)).fetchall()
            
            # Get recent trades for each whale (for regime change detection)
            recent_rows = conn.execute("""
                SELECT whale_name, side, category,
                    COUNT(*) as trades,
                    ROUND(AVG(CASE WHEN actual_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                    ROUND(SUM(actual_pnl), 2) as pnl
                FROM trades
                WHERE actual_pnl IS NOT NULL
                  AND timestamp > ?
                GROUP BY whale_name, side, category
                HAVING trades >= 3
            """, ((datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),)).fetchall()
            
            conn.close()
            
            # Build recent performance lookup
            recent_lookup = {}
            for row in recent_rows:
                key = (row["whale_name"], row["side"], row["category"])
                recent_lookup[key] = {"wr": row["wr"], "pnl": row["pnl"], "trades": row["trades"]}
            
            # Build overall side stats for loss leader detection
            side_stats = {}
            for row in overall_rows:
                wn = row["whale_name"]
                if wn not in side_stats:
                    side_stats[wn] = {}
                side_stats[wn][row["side"]] = {
                    "wr": row["wr"], "pnl": row["pnl"], 
                    "trades": row["trades"], "avg_size": row["avg_size"] or 0,
                }
            
            # Analyze each whale+side+category
            regime_changes = 0
            loss_leaders = 0
            for row in rows:
                wn = row["whale_name"]
                side = row["side"]
                category = row["category"]
                overall_wr = row["wr"]
                overall_pnl = row["pnl"]
                overall_trades = row["trades"]
                
                key = (wn, side, category)
                recent = recent_lookup.get(key)
                
                result = self._analyze_regime(
                    whale_name=wn, side=side, category=category,
                    overall_wr=overall_wr, overall_pnl=overall_pnl,
                    overall_trades=overall_trades,
                    recent_wr=recent["wr"] if recent else None,
                    recent_pnl=recent["pnl"] if recent else None,
                    recent_trades=recent["trades"] if recent else 0,
                    side_stats=side_stats.get(wn, {}),
                )
                
                regime_key = f"{wn}:{category}:{side}"
                self._state.whale_regimes[regime_key] = asdict(result)
                
                if result.regime_change_detected:
                    regime_changes += 1
                if result.loss_leader_suspected:
                    loss_leaders += 1
            
            self._state.last_updated = datetime.now(timezone.utc).isoformat()
            self._state.regime_changes_detected = regime_changes
            self._state.loss_leaders_detected = loss_leaders
            self._save_state()
            
            logger.info(
                "RegimeDetector: refreshed %d whale regimes, %d regime changes, %d loss leaders",
                len(rows), regime_changes, loss_leaders,
            )
        except Exception as e:
            logger.error("RegimeDetector refresh failed: %s", e)

    def _analyze_regime(
        self,
        whale_name: str,
        side: str,
        category: str,
        overall_wr: float,
        overall_pnl: float,
        overall_trades: int,
        recent_wr: float | None,
        recent_pnl: float | None,
        recent_trades: int,
        side_stats: dict,
    ) -> WhaleRegimeResult:
        """Analyze a single whale+side+category for regime changes."""
        result = WhaleRegimeResult(
            whale_name=whale_name, category=category, side=side,
            historical_wr=overall_wr, historical_pnl=overall_pnl,
        )
        
        # Default: fade is safe if whale is historically losing
        # Also default to allowing fade if we have no strong evidence either way,
        # since the pipeline already classified this whale as a fade target
        is_losing = overall_wr < 0.40 and overall_pnl < 0
        is_marginal = 0.40 <= overall_wr < 0.50 and overall_pnl < 0
        result.should_fade = is_losing or is_marginal
        result.fade_confidence = max(0, min(1.0, (0.40 - overall_wr) * 3 + min(abs(overall_pnl) / 200, 0.5)))
        
        # ── 1. Regime change detection ──────────────────────────────
        if recent_wr is not None and recent_trades >= 3:
            result.recent_wr = recent_wr
            result.recent_pnl = recent_pnl or 0.0
            result.wr_shift = recent_wr - overall_wr
            
            # Whale was losing but is now winning — STOP FADING
            if overall_wr < 0.35 and recent_wr > 0.50:
                result.regime_change_detected = True
                result.should_fade = False
                result.regime_type = "reversed"
                result.reason = f"Whale reversed: overall WR={overall_wr:.0%} but recent WR={recent_wr:.0%}"
                result.fade_confidence = 0.0
                return result
            
            # Significant WR improvement — regime likely shifting
            if result.wr_shift > self.regime_wr_threshold:
                result.regime_change_detected = True
                result.regime_type = "improving"
                result.reason = f"Whale improving: WR shifted by {result.wr_shift:.0%} (historical={overall_wr:.0%}, recent={recent_wr:.0%})"
                # Reduce fade confidence but don't eliminate it yet
                result.fade_confidence *= 0.5
            
            # Significant WR degradation — fade becomes MORE attractive
            if result.wr_shift < -self.regime_wr_threshold and recent_wr < 0.30:
                result.should_fade = True
                result.fade_confidence = min(1.0, result.fade_confidence * 1.5)
                result.regime_type = "stable_losing"
                result.reason = f"Whale degrading: recent WR={recent_wr:.0%} (historical={overall_wr:.0%})"
        
        # ── 2. Loss leader / adversarial detection ───────────────────
        # A whale running a loss leader strategy will:
        #   - Lose consistently on one side (usually BUY) with small positions
        #   - Win on the opposite side with larger positions
        #   - The winning side may have better WR and much higher PnL
        opposite_side = "SELL" if side == "BUY" else "BUY"
        current_side = side_stats.get(side, {})
        opposite = side_stats.get(opposite_side, {})
        
        if current_side and opposite:
            current_wr = current_side.get("wr", 0.5)
            opposite_wr = opposite.get("wr", 0.5)
            current_pnl = current_side.get("pnl", 0)
            opposite_pnl = opposite.get("pnl", 0)
            current_avg_size = current_side.get("avg_size", 0)
            opposite_avg_size = opposite.get("avg_size", 0)
            
            # Loss leader pattern: losing on current side, winning on opposite
            # with bigger positions on the winning side
            current_losing = current_wr < 0.40 and current_pnl < 0
            opposite_winning = opposite_wr >= 0.45 and opposite_pnl > 0
            
            if current_losing and opposite_winning:
                result.loss_leader_suspected = True
                
                # Check if winning side has much larger positions (classic loss leader)
                size_ratio = opposite_avg_size / current_avg_size if current_avg_size > 0 else 0
                
                if size_ratio > LOSS_LEADER_SIZE_RATIO:
                    result.adversarial_suspected = True
                    result.should_fade = False
                    result.regime_type = "loss_leader"
                    result.reason = (
                        f"Loss leader suspected: {side} WR={current_wr:.0%}/${current_pnl:.0f} vs "
                        f"{opposite_side} WR={opposite_wr:.0%}/${opposite_pnl:.0f}, "
                        f"size ratio={size_ratio:.1f}x"
                    )
                    result.fade_confidence = 0.0
                    logger.warning(
                        "LOSS LEADER: %s in %s — %s losing (${%.0f}) but %s winning (${%.0f}), "
                        "size ratio=%.1fx. NOT FADING.",
                        whale_name, category, side, current_pnl, opposite_side, opposite_pnl, size_ratio,
                    )
                else:
                    # Whale is two-sided but positions are similar size
                    # This could still be adversarial, but we can fade the losing side
                    result.regime_type = "two_sided"
                    result.reason = (
                        f"Two-sided whale: {side} WR={current_wr:.0%}/${current_pnl:.0f} vs "
                        f"{opposite_side} WR={opposite_wr:.0%}/${opposite_pnl:.0f}"
                    )
                    # Reduce fade confidence but keep it if the losing side is consistent
                    result.fade_confidence *= 0.7
        
        # ── 3. Fade decay ──────────────────────────────────────────
        # Check when the fade classification was last confirmed
        # If it's been > FADE_DECAY_DAYS, reduce confidence
        regime_key = f"{whale_name}:{category}:{side}"
        prev = self._state.whale_regimes.get(regime_key)
        if prev and prev.get("last_confirmed"):
            last_confirmed = datetime.fromisoformat(prev["last_confirmed"])
            days_since = (datetime.now(timezone.utc) - last_confirmed).days
            result.fade_age_days = days_since
            if days_since > FADE_DECAY_DAYS:
                decay_factor = max(0.3, 1.0 - (days_since - FADE_DECAY_DAYS) / 30.0)
                result.fade_confidence *= decay_factor
                if result.fade_confidence < 0.1:
                    result.should_fade = False
                    result.reason = f"Fade classification expired (last confirmed {days_since} days ago)"
        
        # Store confirmation timestamp
        if result.should_fade:
            result.__dict__["last_confirmed"] = datetime.now(timezone.utc).isoformat()
        
        if not result.regime_type:
            if is_losing:
                result.regime_type = "stable_losing"
                result.reason = f"Stable losing whale: WR={overall_wr:.0%}, PnL=${overall_pnl:.0f}"
            else:
                result.regime_type = "stable"
                result.reason = f"Not a fade target: WR={overall_wr:.0%}, PnL=${overall_pnl:.0f}"
                result.should_fade = False
        
        return result

    def check_whale_regime(
        self,
        whale_name: str,
        category: str,
        side: str,
    ) -> WhaleRegimeResult:
        """Check if it's safe to fade a whale on a specific side+category.
        
        This should be called BEFORE executing any fade signal to ensure
        the whale hasn't changed behavior since the fade classification was made.
        """
        regime_key = f"{whale_name}:{category}:{side}"
        cached = self._state.whale_regimes.get(regime_key)
        
        if cached:
            result = WhaleRegimeResult(**{k: v for k, v in cached.items() 
                                           if k in WhaleRegimeResult.__dataclass_fields__})
            # Check if cache is stale (>1 hour old)
            last_updated = self._state.last_updated
            if last_updated:
                last_dt = datetime.fromisoformat(last_updated)
                age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
                if age_hours > 1:
                    # Stale cache, refresh
                    self.refresh()
                    cached = self._state.whale_regimes.get(regime_key)
                    if cached:
                        result = WhaleRegimeResult(**{k: v for k, v in cached.items()
                                                       if k in WhaleRegimeResult.__dataclass_fields__})
            return result
        
        # Not in cache, refresh and try again
        self.refresh()
        cached = self._state.whale_regimes.get(regime_key)
        if cached:
            return WhaleRegimeResult(**{k: v for k, v in cached.items()
                                          if k in WhaleRegimeResult.__dataclass_fields__})
        
        # No regime data for this whale+category+side combination.
        # The whale was already classified as a fade target by the pipeline
        # (whitelist/blacklist/edge scorer). We should NOT block the fade
        # just because we lack regime data. Only block if we have POSITIVE
        # evidence of a regime change or loss leader pattern.
        # Default: allow the fade, proceed with reduced confidence.
        return WhaleRegimeResult(
            whale_name=whale_name, category=category, side=side,
            should_fade=True, reason="No regime data, allowing fade with caution",
            fade_confidence=0.5,  # Moderate confidence when no data
        )

    def get_loss_leaders(self) -> list[dict]:
        """Get all whales suspected of running loss leader strategies."""
        return [
            regime for regime in self._state.whale_regimes.values()
            if regime.get("loss_leader_suspected") or regime.get("adversarial_suspected")
        ]

    def get_regime_changes(self) -> list[dict]:
        """Get all whales where regime changes were detected."""
        return [
            regime for regime in self._state.whale_regimes.values()
            if regime.get("regime_change_detected")
        ]

    def get_diagnostic_summary(self) -> dict:
        """Get a diagnostic summary of regime detection state."""
        total = len(self._state.whale_regimes)
        fading = sum(1 for r in self._state.whale_regimes.values() if r.get("should_fade"))
        reversed_whales = sum(1 for r in self._state.whale_regimes.values() if r.get("regime_type") == "reversed")
        loss_leaders = sum(1 for r in self._state.whale_regimes.values() if r.get("loss_leader_suspected"))
        adversarial = sum(1 for r in self._state.whale_regimes.values() if r.get("adversarial_suspected"))
        
        return {
            "total_whales_analyzed": total,
            "currently_fading": fading,
            "regime_changes_detected": self._state.regime_changes_detected,
            "reversed_whales": reversed_whales,
            "loss_leaders_suspected": loss_leaders,
            "adversarial_suspected": adversarial,
            "last_updated": self._state.last_updated,
        }

    def _save_state(self) -> None:
        """Persist regime state to JSON."""
        data = asdict(self._state)
        self.state_path.write_text(json.dumps(data, indent=2, default=str))

    def _load_state(self) -> None:
        """Load persisted regime state from JSON."""
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text())
            self._state = WhaleRegimeState(
                whale_regimes=data.get("whale_regimes", {}),
                last_updated=data.get("last_updated", ""),
                total_checks=data.get("total_checks", 0),
                regime_changes_detected=data.get("regime_changes_detected", 0),
                loss_leaders_detected=data.get("loss_leaders_detected", 0),
            )
        except Exception as e:
            logger.warning("RegimeDetector: failed to load state: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
    detector = RegimeDetector()
    detector.refresh()
    
    summary = detector.get_diagnostic_summary()
    print("\n=== Regime Detector Diagnostic Summary ===")
    print(f"Total whales analyzed: {summary['total_whales_analyzed']}")
    print(f"Currently safe to fade: {summary['currently_fading']}")
    print(f"Regime changes detected: {summary['regime_changes_detected']}")
    print(f"Reversed whales (STOP FADING): {summary['reversed_whales']}")
    print(f"Loss leaders suspected: {summary['loss_leaders_suspected']}")
    print(f"Adversarial suspected: {summary['adversarial_suspected']}")
    
    loss_leaders = detector.get_loss_leaders()
    if loss_leaders:
        print("\n=== Loss Leader Whales ===")
        for ll in loss_leaders:
            print(f"  {ll['whale_name']:30s} {ll['category']:12s} {ll['side']:5s} "
                  f"type={ll['regime_type']} should_fade={ll['should_fade']} "
                  f"reason={ll.get('reason', '')[:80]}")
    
    regime_changes = detector.get_regime_changes()
    if regime_changes:
        print("\n=== Regime Changes ===")
        for rc in regime_changes:
            print(f"  {rc['whale_name']:30s} {rc['category']:12s} {rc['side']:5s} "
                  f"type={rc['regime_type']} shift={rc['wr_shift']:.0%} "
                  f"reason={rc.get('reason', '')[:80]}")
    
    # Check specific whales
    test_whales = [
        ("JewishNinja", "sports", "BUY"),
        ("JewishNinja", "crypto", "SELL"),
        ("p37-0xe5efd6", "crypto", "BUY"),
        ("COMEONDUDE", "sports", "BUY"),
    ]
    print("\n=== Specific Whale Regime Checks ===")
    for wn, cat, side in test_whales:
        result = detector.check_whale_regime(wn, cat, side)
        print(f"  {wn:30s} {cat:12s} {side:5s} -> fade={result.should_fade} "
              f"type={result.regime_type} conf={result.fade_confidence:.2f} "
              f"loss_leader={result.loss_leader_suspected} adversarial={result.adversarial_suspected}")
