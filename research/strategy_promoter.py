#!/usr/bin/env python3
"""Strategy Promoter — reads tournament results and writes candidate strategies.

Reads the weekly tournament JSON from:
    backtest_results/weekly_tournament_latest.json

If top strategies meet promotion thresholds:
    - Sharpe >= 1.0
    - Max Drawdown <= 15%
    - Has actual fills

Writes promotion candidates to:
    config/live_strategy_candidates.json

NEVER auto-deploys. Only writes the candidate file for human review.
Sends Feishu notification with top-3 summary if candidates exist.

Usage:
    python3 research/strategy_promoter.py
    python3 research/strategy_promoter.py --tournament /path/to/tournament.json
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("StrategyPromoter")

# ── Paths ─────────────────────────────────────────────────────────────────────
NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
RESULTS_DIR = NAUTILUS_ROOT / "backtest_results"
CANDIDATES_PATH = NAUTILUS_ROOT / "config" / "live_strategy_candidates.json"

# ── Promotion thresholds (must match strategy_tournament.py) ──────────────────
MIN_SHARPE = 1.0
MAX_DD_PCT = 0.15  # 15%
MIN_FILLS = 1

# ── Strategy → Nautilus config mapping ────────────────────────────────────────
# Maps PMB strategy names to NautilusStrategy config params for live deployment
STRATEGY_PARAMS: dict[str, dict] = {
    "breakout": {
        "strategy_class": "BreakoutStrategy",
        "module": "strategies.breakout",
        "params": {"breakout_threshold": 0.01, "lookback_bars": 20},
        "notes": "Captures momentum breaks from range consolidation",
    },
    "ema_crossover": {
        "strategy_class": "EMACrossoverStrategy",
        "module": "strategies.ema_crossover",
        "params": {"fast_ema": 9, "slow_ema": 21},
        "notes": "Classic trend-following via EMA crossover",
    },
    "mean_reversion": {
        "strategy_class": "MeanReversionStrategy",
        "module": "strategies.mean_reversion",
        "params": {"z_entry": 2.0, "z_exit": 0.5, "lookback": 20},
        "notes": "Fades extended prices back toward mean",
    },
    "microprice_imbalance": {
        "strategy_class": "MicropriceImbalanceStrategy",
        "module": "strategies.microprice_imbalance",
        "params": {"imbalance_threshold": 0.3, "volume_threshold": 0.6},
        "notes": "Books-weighted microprice vs mid-price divergence",
    },
    "vwap_reversion": {
        "strategy_class": "VWAPReversionStrategy",
        "module": "strategies.vwap_reversion",
        "params": {"vwap_deviation_entry": 0.02, "vwap_deviation_exit": 0.005},
        "notes": "Fades price deviations from VWAP",
    },
    "panic_fade": {
        "strategy_class": "PanicFadeStrategy",
        "module": "strategies.panic_fade",
        "params": {"panic_threshold": 0.05, "recovery_target": 0.01},
        "notes": "Fades market overreactions / panic bids",
    },
    "final_period_momentum": {
        "strategy_class": "FinalPeriodMomentumStrategy",
        "module": "strategies.final_period_momentum",
        "params": {"minutes_before_close": 15, "momentum_lookback": 5},
        "notes": "Exploits final-period directional momentum",
    },
    "late_favorite_limit_hold": {
        "strategy_class": "LateFavoriteLimitHoldStrategy",
        "module": "strategies.late_favorite_limit_hold",
        "params": {"favorite_threshold": 0.05, "hold_minutes": 10},
        "notes": "Holds limit orders on heavy favorites late in the market",
    },
    "threshold_momentum": {
        "strategy_class": "ThresholdMomentumStrategy",
        "module": "strategies.threshold_momentum",
        "params": {"volume_surge_mult": 2.0, "price_change_min": 0.01},
        "notes": "Triggers on volume surges + directional price moves",
    },
    "rsi_reversion": {
        "strategy_class": "RSIReversionStrategy",
        "module": "strategies.rsi_reversion",
        "params": {"rsi_oversold": 30, "rsi_overbought": 70, "lookback": 14},
        "notes": "Classic RSI mean reversion strategy",
    },
}


def _compute_confidence_score(ranked_strategies: list[dict]) -> dict[str, float]:
    """Compute a confidence score (0-1) for each strategy based on tournament rank.

    Top-ranked strategies with good metrics get higher confidence.
    """
    scores = {}
    n = len(ranked_strategies)
    for i, s in enumerate(ranked_strategies):
        name = s["strategy"]
        rank_frac = 1.0 - (i / max(n - 1, 1))  # 1st = 1.0, last = near 0
        sharpe = s["metrics"].get("sharpe_ratio")
        dd = s["metrics"].get("max_drawdown_pct")

        # Sharpe component: NaN or None → 0.0
        if sharpe is None or (isinstance(sharpe, float) and (sharpe != sharpe)):  # NaN check
            sharpe_comp = 0.0
        else:
            sharpe_comp = min(sharpe / 3.0, 1.0) if sharpe > 0 else 0.0

        # DD component: None = worst-case (0.0), 0% = 1.0, 50% = 0.0
        if dd is None:
            dd_comp = 0.0
        else:
            dd_comp = max(0.0, 1.0 - abs(dd) / 50.0)

        # Combined: rank * sharpe * DD components
        confidence = round(rank_frac * sharpe_comp * dd_comp, 4)
        scores[name] = confidence

    return scores


def _load_tournament(tournament_path: Path) -> dict:
    """Load the tournament JSON, with fallback to latest."""
    if not tournament_path.exists():
        latest = RESULTS_DIR / "weekly_tournament_latest.json"
        if latest.exists():
            tournament_path = latest
        else:
            raise FileNotFoundError(
                f"Tournament file not found: {tournament_path}\n"
                f"Run strategy_tournament.py first."
            )
    return json.loads(tournament_path.read_text())


def _build_candidates(tournament: dict) -> list[dict]:
    """Build promotion candidate list from tournament results."""
    ranked = tournament.get("ranked_strategies", [])
    if not ranked:
        return []

    confidence_scores = _compute_confidence_score(ranked)
    candidates = []

    for s in ranked:
        name = s["strategy"]
        m = s["metrics"]

        sharpe = m.get("sharpe_ratio")
        dd_raw = m.get("max_drawdown_pct")
        fills = m.get("total_fills", 0)
        pnl = m.get("pnl_total", 0.0)

        # Promotion gate: skip if sharpe is None/NaN or dd is None
        if sharpe is None or (isinstance(sharpe, float) and (sharpe != sharpe)):
            continue  # NaN or null sharpe = no data
        if dd_raw is None:
            continue  # No DD data = cannot verify DD threshold
        dd = abs(dd_raw)

        if sharpe < MIN_SHARPE:
            continue
        if dd > MAX_DD_PCT * 100:  # MAX_DD_PCT is 0.15 = 15%
            continue
        if fills < MIN_FILLS:
            continue

        params = STRATEGY_PARAMS.get(name, {})
        confidence = confidence_scores.get(name, 0.0)

        candidates.append({
            "strategy_name": name,
            "confidence_score": confidence,
            "market_slugs": [],  # Human fills in before deployment
            "parameters": params.get("params", {}),
            "strategy_class": params.get("strategy_class", ""),
            "module": params.get("module", ""),
            "notes": params.get("notes", ""),
            "metrics": {
                "sharpe_ratio": sharpe,
                "max_drawdown_pct": dd,
                "win_rate": m.get("win_rate", 0.0),
                "pnl_total": pnl,
                "total_fills": fills,
                "sortino_ratio": m.get("sortino_ratio", 0.0),
            },
            "promotion_date": datetime.now(timezone.utc).isoformat(),
            "tournament_date": tournament.get("generated_at", ""),
            "tournament_rank": s["rank"],
            "status": "pending_review",  # Human changes to "approved" or "rejected"
            "human_review_required": True,
        })

    # Sort by confidence score descending
    candidates.sort(key=lambda c: c["confidence_score"], reverse=True)
    return candidates


def _write_candidates(candidates: list[dict]) -> Path:
    """Write candidates JSON and update status."""
    CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_candidates": len(candidates),
        "thresholds": {
            "min_sharpe": MIN_SHARPE,
            "max_dd_pct": MAX_DD_PCT * 100,
            "min_fills": MIN_FILLS,
        },
        "candidates": candidates,
    }

    CANDIDATES_PATH.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Written %d candidates to %s", len(candidates), CANDIDATES_PATH)
    return CANDIDATES_PATH


def _send_feishu_notification(candidates: list[dict], tournament: dict) -> bool:
    """Send Feishu notification with top-3 summary. Returns True on success."""
    try:
        import urllib.request

        top3 = candidates[:3]
        if not top3:
            body = (
                f"Strategy Tournament Results\n"
                f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
                f"No strategies met promotion thresholds this week.\n"
                f"Top performer: {tournament.get('ranked_strategies', [{}])[0].get('strategy', 'N/A')}"
            )
        else:
            lines = [
                f"*Strategy Tournament Results*",
                f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                f"Candidates: {len(candidates)}",
                "",
            ]
            for c in top3:
                m = c["metrics"]
                lines.append(
                    f"★ #{c['tournament_rank']} *{c['strategy_name']}*"
                )
                _dd = m['max_drawdown_pct']
                _dd_str = f"{_dd:.1f}%" if _dd is not None else "None"
                _sh = m['sharpe_ratio']
                _sh_str = f"{_sh:.2f}" if _sh is not None else "nan"
                lines.append(f"   Sharpe={_sh_str}  DD={_dd_str}")
                lines.append(
                    f"   WR={m['win_rate']:.1%}  PnL={m['pnl_total']:.4f}  confidence={c['confidence_score']:.3f}"
                )
                lines.append("")

            lines.append("_live_strategy_candidates.json updated. Human review required before deployment._")

            body = "\n".join(lines)

        # Load Feishu webhook URL
        webhook_path = Path(os.path.expanduser("~/.claw.json"))
        webhook_url = None
        if webhook_path.exists():
            try:
                settings = json.loads(webhook_path.read_text())
                webhook_url = settings.get("feishu_webhook_url") or settings.get("feishu", {}).get("webhook")
            except Exception:
                pass

        if not webhook_url:
            # Try legacy path
            legacy = Path("/home/elon-1/.claw.json")
            if legacy.exists():
                try:
                    settings = json.loads(legacy.read_text())
                    webhook_url = settings.get("feishu_webhook_url")
                except Exception:
                    pass

        if not webhook_url:
            print("  Feishu webhook not configured — skipping notification")
            return False

        payload = json.dumps({"msg_type": "text", "content": {"text": body}}).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Feishu notification sent: %s", resp.read().decode())
        return True

    except Exception as e:
        logger.warning("Feishu notification failed: %s", e)
        return False


def run_promoter(tournament_path: Path | None = None) -> list[dict]:
    """Run the strategy promoter.

    Args:
        tournament_path: Path to tournament JSON. Defaults to weekly_tournament_latest.json.

    Returns:
        List of promotion candidates.
    """
    print(f"\n{'='*70}")
    print(f"  STRATEGY PROMOTER  ({datetime.now(timezone.utc).isoformat()})")
    print(f"{'='*70}\n")

    path = tournament_path or RESULTS_DIR / "weekly_tournament_latest.json"
    print(f"  Loading tournament: {path}")

    try:
        tournament = _load_tournament(path)
    except FileNotFoundError as e:
        print(f"  ERROR: {e}")
        return []

    ranked = tournament.get("ranked_strategies", [])
    n = len(ranked)
    print(f"  Tournament has {n} strategies ranked")

    candidates = _build_candidates(tournament)
    print(f"  Promotion candidates: {len(candidates)}")

    # Write candidates file
    if candidates:
        out_path = _write_candidates(candidates)
        print(f"  Candidates → {out_path}")

        # Show top candidates
        print("\n  ★ Promotion candidates:")
        print(f"  {'Rank':<5} {'Strategy':<28} {'Sharpe':>7} {'DD%':>6} {'WR':>6} {'Conf':>6}")
        print(f"  {'-'*60}")
        for c in candidates[:5]:
            m = c["metrics"]
            _sh = m.get('sharpe_ratio')
            _dd = m.get('max_drawdown_pct')
            _sh_str = f"{_sh:>7.2f}" if _sh is not None else "    nan"
            _dd_str = f"{_dd:>6.1f}" if _dd is not None else "  None"
            _wr = m.get('win_rate', 0)
            _wr_str = f"{_wr:>6.1%}"
            print(
                f"  {c['tournament_rank']:<5} {c['strategy_name']:<28} "
                f"{_sh_str} {_dd_str} {_wr_str} {c['confidence_score']:>6.3f}"
            )
    else:
        print("\n  No strategies met promotion thresholds.")
        print(f"  Thresholds: Sharpe >= {MIN_SHARPE}, DD <= {MAX_DD_PCT*100:.0f}%, fills >= {MIN_FILLS}")

        # Show the best non-qualifier for context
        if ranked:
            top = ranked[0]
            m = top["metrics"]
            print(f"\n  Closest non-qualifier: #{top['rank']} {top['strategy']}")
            _dd = m.get('max_drawdown_pct')
            _sh = m.get('sharpe_ratio')
            _dd_str = f"{_dd:.1f}%" if _dd is not None else "None"
            _sh_str = f"{_sh:.2f}" if _sh is not None else "nan"
            print(f"    Sharpe={_sh_str}  DD={_dd_str}")
            print(f"    (needs Sharpe >= {MIN_SHARPE} and DD <= {MAX_DD_PCT*100:.0f}%)")

    # Send Feishu notification
    print()
    feishu_ok = _send_feishu_notification(candidates, tournament)
    print(f"  Feishu notification: {'sent ✓' if feishu_ok else 'skipped / failed'}")

    print(f"\n{'='*70}\n")
    return candidates


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-20s %(levelname)-6s %(message)s",
    )

    parser = argparse.ArgumentParser(description="PMXT Strategy Promoter")
    parser.add_argument(
        "--tournament",
        type=Path,
        default=None,
        help="Path to tournament JSON (default: weekly_tournament_latest.json)",
    )
    args = parser.parse_args()

    run_promoter(tournament_path=args.tournament)
