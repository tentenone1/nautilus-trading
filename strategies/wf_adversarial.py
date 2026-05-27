"""Adversarial Whale Detector — Identifies whales using manipulative strategies.

Detects whale archetypes that undermine naive copy/fade strategies:

1. **Market Makers**: High volume, ~50% WR, tiny positions, near-zero PnL.
   They profit from spread/fees, not predictions. Copying or fading them is noise.

2. **Loss Leaders**: Intentionally losing on one side (small positions) while
   winning on the opposite side (large positions). Already partially detected by
   RegimeDetector, but this module adds detection of the two-sided coordination.

3. **Sacrificial Accounts**: High confidence (95%), 0% WR, small positions.
   These are burner accounts used to test markets or create false signals.

4. **Cross-Platform Hedgers**: Consistently lose in one category but win in
   another, suggesting they're hedging positions taken on other platforms.

5. **MEV / Front-Runners**: Execute with zero latency, consistent small profits,
   appear to be profitable but are actually extracting value from other traders.

Usage:
    detector = AdversarialDetector(db_path="data/trades.db")
    result = detector.classify_whale("JewishNinja")
    if result.adversarial_type:
        print(f"WARNING: {result.adversarial_type} - {result.reason}")
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("AdversarialDetector")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/adversarial_state.json")

# ── Detection Thresholds ──────────────────────────────────────────────────────
MM_MIN_TRADES = 20
MM_WR_RANGE = (0.42, 0.58)
MM_MAX_PNL_RATIO = 0.05
MM_MAX_AVG_SIZE = 15.0

SAC_MIN_TRADES = 5
SAC_MAX_WR = 0.15
SAC_MIN_CONFIDENCE = 0.85
SAC_MAX_AVG_SIZE = 20.0

HEDGER_MIN_TRADES = 5
HEDGER_WR_DIFF = 0.30
HEDGER_LOSING_PNL = -100

MEV_MIN_TRADES = 10
MEV_MAX_LATENCY = 50
MEV_MIN_WR = 0.55
MEV_MAX_AVG_SIZE = 50.0


@dataclass
class AdversarialResult:
    whale_name: str
    is_adversarial: bool = False
    adversarial_type: str = ""
    confidence: float = 0.0
    reason: str = ""
    recommendation: str = ""
    metrics: dict = field(default_factory=dict)


@dataclass
class AdversarialState:
    whale_classifications: dict = field(default_factory=dict)
    last_updated: str = ""
    total_analyzed: int = 0
    market_makers: int = 0
    loss_leaders: int = 0
    sacrificial_accounts: int = 0
    hedgers: int = 0
    mev_accounts: int = 0


class AdversarialDetector:
    def __init__(self, db_path=None, state_path=None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.state_path = Path(state_path) if state_path else STATE_PATH
        self._state = AdversarialState()
        self._load_state()

    def classify_whale(self, whale_name):
        result = AdversarialResult(whale_name=whale_name)
        if not self.db_path.exists():
            return result
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            overall = conn.execute("""
                SELECT COUNT(*) as trades,
                    ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                    ROUND(SUM(realized_pnl), 2) as total_pnl,
                    ROUND(AVG(position_size_usd), 2) as avg_size,
                    ROUND(AVG(confidence), 4) as avg_conf,
                    ROUND(AVG(total_latency_ms), 1) as avg_latency,
                    COUNT(DISTINCT category) as categories,
                    COUNT(DISTINCT side) as sides
                FROM trades WHERE realized_pnl IS NOT NULL AND whale_name = ?
            """, (whale_name,)).fetchone()
            if not overall or overall["trades"] < 3:
                conn.close()
                return result
            result.metrics["overall_trades"] = overall["trades"]
            result.metrics["overall_wr"] = overall["wr"] or 0
            result.metrics["overall_pnl"] = overall["total_pnl"] or 0
            result.metrics["avg_size"] = overall["avg_size"] or 0
            result.metrics["avg_conf"] = overall["avg_conf"] or 0
            result.metrics["avg_latency"] = overall["avg_latency"] or 0

            if self._is_market_maker(overall):
                result.is_adversarial = True
                result.adversarial_type = "market_maker"
                result.confidence = 0.9
                result.reason = (
                    f"Market maker: {overall['trades']} trades, "
                    f"WR={overall['wr']:.0%}, PnL=${overall['total_pnl']:.2f}, "
                    f"avg_size=${overall['avg_size']:.2f}. Profits from spread, not predictions."
                )
                result.recommendation = "ignore"
                conn.close()
                return result

            if self._is_sacrificial(overall):
                result.is_adversarial = True
                result.adversarial_type = "sacrificial"
                result.confidence = 0.85
                result.reason = (
                    f"Sacrificial account: {overall['trades']} trades, "
                    f"WR={overall['wr']:.0%}, conf={overall['avg_conf']:.0%}, "
                    f"avg_size=${overall['avg_size']:.2f}. High confidence, near-zero WR = burner."
                )
                result.recommendation = "ignore"
                conn.close()
                return result

            loss_leader = self._is_loss_leader(conn, whale_name, overall)
            if loss_leader:
                result.is_adversarial = True
                result.adversarial_type = "loss_leader"
                result.confidence = loss_leader["confidence"]
                result.reason = loss_leader["reason"]
                result.recommendation = "no_fade"
                result.metrics["loss_leader_detail"] = loss_leader
                conn.close()
                return result

            hedger = self._is_cross_platform_hedger(conn, whale_name)
            if hedger:
                result.is_adversarial = True
                result.adversarial_type = "cross_platform_hedger"
                result.confidence = hedger["confidence"]
                result.reason = hedger["reason"]
                result.recommendation = "reduce_size"
                result.metrics["hedger_detail"] = hedger
                conn.close()
                return result

            if self._is_mev(overall):
                result.is_adversarial = True
                result.adversarial_type = "mev"
                result.confidence = 0.7
                result.reason = (
                    f"MEV/front-runner: {overall['trades']} trades, "
                    f"WR={overall['wr']:.0%}, latency={overall['avg_latency']:.0f}ms, "
                    f"avg_size=${overall['avg_size']:.2f}. Extracts value from timing."
                )
                result.recommendation = "ignore"
                conn.close()
                return result

            conn.close()
        except Exception as e:
            logger.error("AdversarialDetector error for %s: %s", whale_name, e)
        return result

    def _is_market_maker(self, overall):
        trades = overall["trades"] or 0
        wr = overall["wr"] or 0
        pnl = overall["total_pnl"] or 0
        avg_size = overall["avg_size"] or 0
        if trades < MM_MIN_TRADES:
            return False
        if not (MM_WR_RANGE[0] <= wr <= MM_WR_RANGE[1]):
            return False
        if avg_size > MM_MAX_AVG_SIZE:
            return False
        pnl_per_trade = abs(pnl) / max(trades, 1)
        return pnl_per_trade <= 1.0

    def _is_sacrificial(self, overall):
        trades = overall["trades"] or 0
        wr = overall["wr"] or 0
        avg_conf = overall["avg_conf"] or 0
        avg_size = overall["avg_size"] or 0
        return (trades >= SAC_MIN_TRADES and wr <= SAC_MAX_WR
                and avg_conf >= SAC_MIN_CONFIDENCE and avg_size <= SAC_MAX_AVG_SIZE)

    def _is_loss_leader(self, conn, whale_name, overall):
        sides = conn.execute("""
            SELECT side, COUNT(*) as trades,
                ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                ROUND(SUM(realized_pnl), 2) as total_pnl,
                ROUND(AVG(position_size_usd), 2) as avg_size
            FROM trades WHERE realized_pnl IS NOT NULL AND whale_name = ?
            GROUP BY side
        """, (whale_name,)).fetchall()
        if len(sides) < 2:
            return None
        buy_data = next((s for s in sides if s["side"] == "BUY"), None)
        sell_data = next((s for s in sides if s["side"] == "SELL"), None)
        if not buy_data or not sell_data:
            return None
        if buy_data["trades"] < 3 or sell_data["trades"] < 3:
            return None
        losing = buy_data if buy_data["total_pnl"] < sell_data["total_pnl"] else sell_data
        winning = sell_data if losing["side"] == "BUY" else buy_data
        l_wr = losing["wr"] or 0
        w_wr = winning["wr"] or 0
        l_pnl = losing["total_pnl"] or 0
        w_pnl = winning["total_pnl"] or 0
        l_size = losing["avg_size"] or 0
        w_size = winning["avg_size"] or 0
        if l_wr < 0.40 and l_pnl < -50 and w_wr > 0.40 and w_pnl > 100:
            size_ratio = w_size / l_size if l_size > 0 else 0
            wr_gap = w_wr - l_wr
            confidence = min(0.95, 0.5 + wr_gap * 0.5 + min(size_ratio / 5, 0.3))
            return {
                "losing_side": losing["side"], "losing_wr": l_wr, "losing_pnl": l_pnl,
                "losing_avg_size": l_size, "winning_side": winning["side"],
                "winning_wr": w_wr, "winning_pnl": w_pnl, "winning_avg_size": w_size,
                "size_ratio": size_ratio, "wr_gap": wr_gap, "confidence": confidence,
                "reason": (
                    f"Loss leader: {losing['side']} WR={l_wr:.0%}/${l_pnl:.0f} "
                    f"vs {winning['side']} WR={w_wr:.0%}/${w_pnl:.0f}, "
                    f"size ratio={size_ratio:.1f}x"
                ),
            }
        return None

    def _is_cross_platform_hedger(self, conn, whale_name):
        categories = conn.execute("""
            SELECT category, COUNT(*) as trades,
                ROUND(AVG(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END), 4) as wr,
                ROUND(SUM(realized_pnl), 2) as total_pnl,
                ROUND(AVG(position_size_usd), 2) as avg_size
            FROM trades WHERE realized_pnl IS NOT NULL AND whale_name = ?
            GROUP BY category HAVING trades >= ?
        """, (whale_name, HEDGER_MIN_TRADES)).fetchall()
        if len(categories) < 2:
            return None
        losing = [c for c in categories if c["total_pnl"] < HEDGER_LOSING_PNL]
        winning = [c for c in categories if c["total_pnl"] > 100 and c["wr"] > 0.45]
        if not losing or not winning:
            return None
        worst = min(categories, key=lambda c: c["total_pnl"])
        best = max(categories, key=lambda c: c["total_pnl"])
        wr_gap = best["wr"] - worst["wr"]
        if wr_gap < HEDGER_WR_DIFF:
            return None
        confidence = min(0.85, 0.4 + wr_gap * 0.5)
        return {
            "worst_category": worst["category"], "worst_wr": worst["wr"],
            "worst_pnl": worst["total_pnl"], "best_category": best["category"],
            "best_wr": best["wr"], "best_pnl": best["total_pnl"],
            "wr_gap": wr_gap, "confidence": confidence,
            "reason": (
                f"Cross-platform hedger: {worst['category']} WR={worst['wr']:.0%}/${worst['total_pnl']:.0f} "
                f"vs {best['category']} WR={best['wr']:.0%}/${best['total_pnl']:.0f}, "
                f"WR gap={wr_gap:.0%}"
            ),
        }

    def _is_mev(self, overall):
        trades = overall["trades"] or 0
        wr = overall["wr"] or 0
        avg_latency = overall["avg_latency"] or 0
        avg_size = overall["avg_size"] or 0
        return (trades >= MEV_MIN_TRADES and avg_latency <= MEV_MAX_LATENCY
                and wr >= MEV_MIN_WR and avg_size <= MEV_MAX_AVG_SIZE)

    def classify_all(self):
        if not self.db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            whales = conn.execute("""
                SELECT DISTINCT whale_name FROM trades
                WHERE realized_pnl IS NOT NULL AND whale_name IS NOT NULL
            """).fetchall()
            conn.close()
            results = {}
            mm = ll = sac = hed = mev = 0
            for (wn,) in whales:
                result = self.classify_whale(wn)
                results[wn] = result
                if result.adversarial_type == "market_maker": mm += 1
                elif result.adversarial_type == "loss_leader": ll += 1
                elif result.adversarial_type == "sacrificial": sac += 1
                elif result.adversarial_type == "cross_platform_hedger": hed += 1
                elif result.adversarial_type == "mev": mev += 1
            self._state.whale_classifications = {k: asdict(v) for k, v in results.items()}
            self._state.last_updated = datetime.now(timezone.utc).isoformat()
            self._state.total_analyzed = len(results)
            self._state.market_makers = mm
            self._state.loss_leaders = ll
            self._state.sacrificial_accounts = sac
            self._state.hedgers = hed
            self._state.mev_accounts = mev
            self._save_state()
            logger.info("AdversarialDetector: %d whales - %d MM, %d LL, %d SAC, %d HED, %d MEV",
                        len(results), mm, ll, sac, hed, mev)
            return results
        except Exception as e:
            logger.error("AdversarialDetector classify_all failed: %s", e)
            return {}

    def get_recommendation(self, whale_name):
        result = self.classify_whale(whale_name)
        return result.recommendation if result.is_adversarial else "copy"

    def get_summary(self):
        return asdict(self._state)

    def _save_state(self):
        self.state_path.write_text(json.dumps(asdict(self._state), indent=2, default=str))

    def _load_state(self):
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text())
            self._state = AdversarialState(
                whale_classifications=data.get("whale_classifications", {}),
                last_updated=data.get("last_updated", ""),
                total_analyzed=data.get("total_analyzed", 0),
                market_makers=data.get("market_makers", 0),
                loss_leaders=data.get("loss_leaders", 0),
                sacrificial_accounts=data.get("sacrificial_accounts", 0),
                hedgers=data.get("hedgers", 0),
                mev_accounts=data.get("mev_accounts", 0),
            )
        except Exception as e:
            logger.warning("AdversarialDetector: failed to load state: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
    detector = AdversarialDetector()
    results = detector.classify_all()
    print(f"\n=== Adversarial Whale Detection ===")
    print(f"Total analyzed: {detector._state.total_analyzed}")
    print(f"Market makers: {detector._state.market_makers}")
    print(f"Loss leaders: {detector._state.loss_leaders}")
    print(f"Sacrificial accounts: {detector._state.sacrificial_accounts}")
    print(f"Cross-platform hedgers: {detector._state.hedgers}")
    print(f"MEV accounts: {detector._state.mev_accounts}")
    adversarial = {k: v for k, v in results.items() if v.is_adversarial}
    print(f"\n=== Adversarial Whales ({len(adversarial)}) ===")
    for wn, result in sorted(adversarial.items(), key=lambda x: x[1].confidence, reverse=True):
        print(f"  {wn}: type={result.adversarial_type} conf={result.confidence:.0%}")
        print(f"    {result.reason[:120]}")
        print(f"    Recommendation: {result.recommendation}")
