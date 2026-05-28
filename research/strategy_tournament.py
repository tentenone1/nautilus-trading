#!/usr/bin/env python3
"""Strategy Tournament — weekly ranked backtest of all PMXT strategies.

Parses the prediction-market-backtesting batch runner output logs to extract
performance metrics (Sharpe, Sortino, MaxDD, Win Rate, PnL) for each strategy,
then ranks them and writes the results to:
    backtest_results/weekly_tournament_YYYY-MM-DD.json

This script can also optionally RE-RUN the batch runner to generate fresh results
against the latest cached PMXT data.

Usage:
    python3 research/strategy_tournament.py              # Parse existing results
    python3 research/strategy_tournament.py --rerun      # Re-run batch + parse
    python3 research/strategy_tournament.py --rerun --days 7  # Re-run last 7 days
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("StrategyTournament")

# ── Paths ─────────────────────────────────────────────────────────────────────
NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
PMB_ROOT = Path("/home/elon-1/projects/prediction-market-backtesting")
RESULTS_DIR = NAUTILUS_ROOT / "backtest_results"
BATCH_RESULTS_DIR = PMB_ROOT / "output" / "batch_results"
LOG_DIR = NAUTILUS_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

# ── Scoring thresholds for promotion ──────────────────────────────────────────
MIN_SHARPE_FOR_PROMOTION = 1.0
MAX_DD_FOR_PROMOTION = 0.15  # 15%
MIN_TRADES_TO_RANK = 3

# ── Strategy definitions (matches batch_runner.py STRATEGIES) ────────────────
SKIP_STRATEGIES = {
    "binary_pair_arbitrage": "requires 2+ paired markets (YES/NO) — cannot run single-market replay",
}


def _parse_number(s: str) -> Optional[float]:
    """Parse a number from a string, returning None on failure."""
    try:
        return float(s.strip())
    except (ValueError, TypeError):
        return None


def _parse_float(s: str) -> float:
    """Parse a float from a string.

    - Returns math.nan if the string is 'nan' (no trades → undefined stats).
    - Returns 0.0 only if the string is genuinely unparseable.
    """
    try:
        val = float(s.strip())
        # Distinguish "no trades" nan from parse failure
        if math.isnan(val):
            return math.nan
        return val
    except (ValueError, TypeError):
        return 0.0


def _parse_strategy_log(log_path: Path) -> dict:
    """Parse a batch runner strategy log for metrics.

    Looks for:
      - Portfolio return stats: Sharpe, Sortino, PF, etc.
      - Portfolio PnL stats: PnL total, PnL%, WR, Expectancy
      - Market rows: per-market breakdown
    """
    if not log_path.exists():
        return {"error": f"Log not found: {log_path}"}

    raw = log_path.read_text()

    result = {
        "strategy": log_path.stem,
        "log_path": str(log_path),
        "metrics": {},
        "market_count": 0,
        "has_fills": False,
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Portfolio return stats ────────────────────────────────────────────────
    portfolio_match = re.search(
        r"Portfolio return stats:\s*"
        r"Sharpe Ratio.*?:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Sortino Ratio.*?:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Profit Factor:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Risk Return Ratio:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Returns Volatility.*?:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Average.*?Return.*?:\s*([-\d.e]+|nan)",
        raw,
    )

    # When the batch runner finds no trades, it skips the return-stats line entirely.
    # In that case Sharpe/Sortino are undefined → use NaN so scoring treats them as 0.
    if portfolio_match:
        sharpe = _parse_float(portfolio_match.group(1))
        sortino = _parse_float(portfolio_match.group(2))
        profit_factor = _parse_float(portfolio_match.group(3))
        risk_return = _parse_float(portfolio_match.group(4))
        vol = _parse_float(portfolio_match.group(5))
        avg_return = _parse_float(portfolio_match.group(6))
    else:
        sharpe = sortino = profit_factor = risk_return = vol = avg_return = math.nan

    # ── Portfolio PnL stats ──────────────────────────────────────────────────
    pnl_match = re.search(
        r"Portfolio PnL stats.*?:\s*"
        r"PnL \(total\):\s*([-\d.]+|nan)\s*\|?\s*"
        r"PnL% \(total\):\s*([-\d.e]+|nan)\s*\|?\s*"
        r"Win Rate:\s*([-\d.]+|nan)\s*\|?\s*"
        r"Expectancy:\s*([-\d.e]+|nan)",
        raw,
    )

    # When pnl_match is None, PnL/WR are also undefined → NaN
    if pnl_match:
        pnl_total = _parse_float(pnl_match.group(1))
        pnl_pct = _parse_float(pnl_match.group(2))
        win_rate = _parse_float(pnl_match.group(3))
        expectancy = _parse_float(pnl_match.group(4))
    else:
        pnl_total = pnl_pct = win_rate = expectancy = math.nan

    result["metrics"] = {
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "profit_factor": profit_factor,
        "risk_return_ratio": risk_return,
        "returns_volatility": vol,
        "avg_return": avg_return,
        "pnl_total": pnl_total,
        "pnl_pct": pnl_pct,
        "win_rate": win_rate,
        "expectancy": expectancy,
    }

        # ── Extract fill / order count from Portfolio run stats line ──────────────
    # Format: "Portfolio run stats: ... | Events: 8 | Orders: 4 | Positions: 2 | ..."
    total_fills = 0
    portfolio_run_match = re.search(
        r"Portfolio run stats:[^|]*\|\s*Events:\s*(\d+)\s*\|",
        raw,
    )
    if portfolio_run_match:
        total_fills = int(portfolio_run_match.group(1))  # Events = fill events

    result["has_fills"] = total_fills > 0
    result["metrics"]["total_fills"] = total_fills

    # ── Max drawdown: None when circuit breaker not configured ───────────────
    # The batch runner emits a WARNING when no portfolio-level drawdown is tracked.
    # In that case max_drawdown_pct is unavailable (no ground truth).
    has_dd_warning = (
        "No portfolio-level drawdown" in raw
        or "No portfolio-level drawdown or daily-loss circuit breaker" in raw
    )
    if has_dd_warning:
        max_dd: float | None = None
    else:
        # Try extracting from Nautilus INFO lines
        dd_match = re.search(r"Max Drawdown[^:]*:\s*([-\d.]+)", raw)
        max_dd = float(dd_match.group(1)) if dd_match else None

    result["metrics"]["max_drawdown_pct"] = max_dd

# ── Determine validity ─────────────────────────────────────────────────────
    # A valid result needs either fills OR a non-zero portfolio stat
    is_valid = (total_fills > 0) or (abs(pnl_total) > 0) or (not math.isnan(sharpe) and sharpe != 0.0)
    result["valid"] = is_valid

    return result


def _score_strategy(result: dict) -> dict:
    """Compute composite score and promotion eligibility for a strategy result.

    Composite score formula (higher = better):
      score = sharpe_normalized * 0.35
              + sortino_normalized * 0.15
              + pnl_sign * min(|pnl| / 10, 1.0) * 0.25
              + (1 - |dd| / 50) * 0.15        # penalize large drawdowns
              + win_rate * 0.10  (zeroed when no fills)

    NaN / None handling:
      - sharpe/sortino = NaN → normalized to 0.0 (contributes nothing)
      - max_drawdown_pct = None → treated as worst-case (100%), score=0.0
      - win_rate weight set to 0 when has_fills=False (no trades = meaningless)

    Promotion eligibility:
      - sharpe_ratio >= MIN_SHARPE_FOR_PROMOTION (1.0)
      - max_drawdown_pct is not None AND abs(dd) <= MAX_DD_FOR_PROMOTION
      - has_fills == True
      - valid == True
    """
    m = result.get("metrics", {})
    sharpe = m.get("sharpe_ratio", 0.0)
    sortino = m.get("sortino_ratio", 0.0)
    pnl = m.get("pnl_total", 0.0)
    dd = m.get("max_drawdown_pct")          # may be None
    wr = m.get("win_rate", 0.0)
    has_fills = result.get("has_fills", False)

    # ── Normalize Sharpe (NaN → 0.0) ─────────────────────────────────────────
    if math.isnan(sharpe):
        sharpe_norm = 0.0
    else:
        sharpe_clamped = max(min(sharpe, 5.0), -5.0)
        sharpe_norm = (sharpe_clamped + 5.0) / 10.0  # maps -5→5 to 0→1

    # ── Normalize Sortino (NaN → 0.0) ────────────────────────────────────────
    if math.isnan(sortino):
        sortino_norm = 0.0
    else:
        sortino_clamped = max(min(sortino, 5.0), -5.0)
        sortino_norm = (sortino_clamped + 5.0) / 10.0

    # ── Normalize PnL ─────────────────────────────────────────────────────────
    pnl_sign = 1.0 if pnl >= 0 else -1.0
    pnl_norm = min(abs(pnl) / 10.0, 1.0) * pnl_sign  # -1 to 1

    # ── Normalize DD (None = worst-case 100% → score 0.0) ──────────────────
    if dd is None:
        dd_norm = 0.0   # worst-case: no data assumed = maximum risk
    else:
        dd_norm = max(0.0, 1.0 - abs(dd) / 50.0)

    # ── Win rate: zero out when no fills (WR=0 means no trades, not 0%) ──────
    wr_weight = wr * 0.10 if has_fills else 0.0

    composite = (
        sharpe_norm * 0.35
        + sortino_norm * 0.15
        + pnl_norm * 0.25
        + dd_norm * 0.15
        + wr_weight
    )

    # ── Promotion gate ────────────────────────────────────────────────────────
    sharpe_ok = (not math.isnan(sharpe)) and sharpe >= MIN_SHARPE_FOR_PROMOTION
    dd_ok = (dd is not None) and (abs(dd) <= MAX_DD_FOR_PROMOTION)
    promotion_eligible = (
        sharpe_ok
        and dd_ok
        and has_fills
        and result.get("valid", False)
    )

    return {
        "composite_score": round(composite, 4),
        "sharpe_ok": sharpe_ok,
        "dd_ok": dd_ok,
        "promotion_eligible": promotion_eligible,
    }


def run_batch_runner() -> bool:
    """Run the prediction-market-backtesting batch runner.

    Returns True if all strategies completed successfully.
    """
    print("=== Re-running PMB batch runner ===")
    runner = PMB_ROOT / "backtests" / "batch_runner.py"
    if not runner.exists():
        logger.error(f"batch_runner.py not found at {runner}")
        return False

    try:
        result = subprocess.run(
            [sys.executable, str(runner)],
            capture_output=True,
            text=True,
            cwd=str(PMB_ROOT),
            timeout=900,  # 15 min max
        )
        logger.info("Batch runner exit code: %d", result.returncode)
        if result.stdout:
            print(result.stdout[-2000:])  # last 2KB
        if result.returncode != 0 and result.stderr:
            logger.warning("Batch runner stderr: %s", result.stderr[-1000:])
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error("Batch runner timed out (>900s)")
        return False
    except Exception as e:
        logger.error("Batch runner failed: %s", e)
        return False


def _json_serializer(obj):
    """Serialize NaN / Infinity to JSON-compatible null / finite numbers."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def run_tournament(rerun: bool = False) -> dict:
    """Run the strategy tournament.

    Args:
        rerun: If True, run the batch runner first to refresh results.

    Returns:
        Tournament dict with ranked strategies and summary.
    """
    print(f"\n{'='*70}")
    print(f"  STRATEGY TOURNAMENT  ({datetime.now(timezone.utc).isoformat()})")
    print(f"{'='*70}\n")

    if rerun:
        success = run_batch_runner()
        if not success:
            print("WARNING: batch runner had errors — proceeding with existing results")
    else:
        print("Parsing existing batch results...")

    # ── Parse all strategy logs ────────────────────────────────────────────────
    strategies = []

    for log_file in sorted(BATCH_RESULTS_DIR.glob("*.log")):
        name = log_file.stem
        if name in SKIP_STRATEGIES:
            print(f"  SKIP [{name}]: {SKIP_STRATEGIES[name]}")
            continue

        parsed = _parse_strategy_log(log_file)
        scored = _score_strategy(parsed)
        parsed.update(scored)

        strategies.append(parsed)

        status = "✓" if parsed.get("valid") else "✗"
        m = parsed.get("metrics", {})
        fills = m.get("total_fills", 0)
        sharpe = m.get("sharpe_ratio", 0.0)
        dd = m.get("max_drawdown_pct", 0.0)
        pnl = m.get("pnl_total", 0.0)
        promo = "★ ELIGIBLE" if scored.get("promotion_eligible") else ""
        dd_str = f"{dd:7.2f}%" if dd is not None else "    None%"
        sharpe_str = f"{sharpe:7.2f}" if not (sharpe is not None and sharpe != sharpe) else "    nan"
        # Check for nan without importing math in this scope
        import math as _math
        sharpe_str = f"{sharpe:7.2f}" if not _math.isnan(sharpe) else "    nan"
        pnl_str = f"{pnl:8.4f}"
        print(
            f"  {status} {name:30s} fills={fills:5d}  sharpe={sharpe_str}  "
            f"dd={dd_str}  pnl={pnl_str}  score={scored['composite_score']:.3f} {promo}"
        )

    # ── Rank by composite score ────────────────────────────────────────────────
    strategies.sort(key=lambda s: s["composite_score"], reverse=True)

    ranked = []
    for i, s in enumerate(strategies, 1):
        ranked.append({
            "rank": i,
            "strategy": s["strategy"],
            "composite_score": s["composite_score"],
            "metrics": s["metrics"],
            "has_fills": s["has_fills"],
            "promotion_eligible": s.get("promotion_eligible", False),
            "log_path": s.get("log_path", ""),
        })

    # ── Build output dict ──────────────────────────────────────────────────────
    tournament = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rerun": rerun,
        "n_strategies": len(strategies),
        "n_eligible": sum(1 for s in ranked if s["promotion_eligible"]),
        "ranked_strategies": ranked,
        "thresholds": {
            "min_sharpe_for_promotion": MIN_SHARPE_FOR_PROMOTION,
            "max_dd_for_promotion": MAX_DD_FOR_PROMOTION,
        },
    }

    # ── Write JSON ─────────────────────────────────────────────────────────────
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_path = RESULTS_DIR / f"weekly_tournament_{today}.json"
    out_path.write_text(json.dumps(tournament, indent=2, default=_json_serializer))
    print(f"\n  Results → {out_path}")

    # ── Also write latest symlink ──────────────────────────────────────────────
    latest = RESULTS_DIR / "weekly_tournament_latest.json"
    latest.write_text(json.dumps(tournament, indent=2, default=_json_serializer))

    # ── Summary ────────────────────────────────────────────────────────────────
    eligible = [s for s in ranked if s["promotion_eligible"]]
    print(f"\n  Summary: {len(strategies)} strategies, {len(eligible)} promotion-eligible")
    if eligible:
        print("  ★ Eligible for promotion:")
        for s in eligible:
            m = s["metrics"]
            _dd = m['max_drawdown_pct']
            _sh = m['sharpe_ratio']
            _dd_str = f"{_dd:.2f}%" if _dd is not None else "None"
            _sh_str = f"{_sh:.2f}" if not math.isnan(_sh) else "nan"
            print(
                f"    #{s['rank']} {s['strategy']}: Sharpe={_sh_str}, "
                f"DD={_dd_str}, PnL={m['pnl_total']:.4f}"
            )
    else:
        print("  No strategies met promotion thresholds (Sharpe>1.0, DD<-15%, has fills)")
        # Show top scorer for reference
        if ranked:
            top = ranked[0]
            m = top["metrics"]
            _dd = m['max_drawdown_pct']
            _sh = m['sharpe_ratio']
            _dd_str = f"{_dd:.2f}%" if _dd is not None else "None"
            _sh_str = f"{_sh:.2f}" if not math.isnan(_sh) else "nan"
            print(
                f"  Top scorer: #{top['rank']} {top['strategy']}: "
                f"Sharpe={_sh_str}, DD={_dd_str}, "
                f"Score={top['composite_score']:.3f}"
            )
    print(f"\n{'='*70}\n")
    return tournament


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-20s %(levelname)-6s %(message)s",
    )

    parser = argparse.ArgumentParser(description="PMXT Strategy Tournament")
    parser.add_argument("--rerun", action="store_true", help="Re-run batch runner before parsing")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days (future use)")
    args = parser.parse_args()

    run_tournament(rerun=args.rerun)
