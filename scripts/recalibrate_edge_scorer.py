#!/usr/bin/env python3
"""T17: Recalibrate Edge Scorer — Walk-Forward Recalibration Script.

Standalone script that runs on a schedule (e.g., daily via cron) to:
  1. Pull the latest closed trades from trades.db
  2. Validate the current EdgeScorer against the freshest data
  3. Compare new edge_score buckets against actual win rates
  4. Recommend recalibration if discrimination target is breached
  5. Write updated calibration JSON to config/

Usage:
    python scripts/recalibrate_edge_scorer.py
    python scripts/recalibrate_edge_scorer.py --db-path /path/to/trades.db --output /path/to/calibration.json

Recommended cron (runs daily at 06:00 CST):
    0 6 * * * cd /home/elon-1/workspace/nautilus-trading && python scripts/recalibrate_edge_scorer.py >> logs/recalibrate.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Setup logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("RecalibrateEdgeScorer")


# ── Paths ─────────────────────────────────────────────────────────────────────
DEFAULT_DB = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
DEFAULT_OUT = Path("/home/elon-1/workspace/nautilus-trading/config/edge_scorer_calibration_latest.json")
DEFAULT_LOG = Path("/home/elon-1/workspace/nautilus-trading/logs/recalibrate.log")
DEFAULT_ARCHIVE = Path("/home/elon-1/workspace/nautilus-trading/config/edge_scorer_calibration_archive")


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.row_factory = sqlite3.Row
    return conn


def _annualized_sharpe(pnls: list[float]) -> float:
    """Compute annualized Sharpe ratio from a list of PnL values."""
    n = len(pnls)
    if n < 2:
        return 0.0
    mean = sum(pnls) / n
    variance = sum((p - mean) ** 2 for p in pnls) / max(n - 1, 1)
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (mean / std) * math.sqrt(252)


def load_trades(
    db_path: Path,
    *,
    min_exit_reasons: tuple[str, ...] | None = None,
    cutoff_days: int | None = None,
) -> list[sqlite3.Row]:
    """Load closed trades from trades.db.

    Args:
        db_path: Path to trades.db.
        min_exit_reasons: Only include trades with these exit_reasons.
            Defaults to valid final exits only.
        cutoff_days: Only include trades from the last `cutoff_days` days.
            None = all trades.

    Returns:
        List of sqlite3.Row objects.
    """
    if not db_path.exists():
        logger.error("trades.db not found at %s", db_path)
        return []

    conn = _connect(db_path)

    if min_exit_reasons is None:
        min_exit_reasons = ("resolved", "max_hold", "certainty_win",
                            "category_take_profit", "pre_resolution_stop_loss",
                            "stale_resolution")

    reason_clause = " OR ".join(f"exit_reason = ?" for _ in min_exit_reasons)
    query = f"""
        SELECT whale_name, category, side, realized_pnl, realized_return,
               exit_reason, edge_score, confidence, timestamp
        FROM trades
        WHERE realized_pnl IS NOT NULL
          AND ({reason_clause})
        ORDER BY timestamp DESC
    """
    params = list(min_exit_reasons)

    if cutoff_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
        query += " AND timestamp >= ?"
        params.append(cutoff.strftime("%Y-%m-%d"))

    rows = conn.execute(query, params).fetchall()
    conn.close()
    logger.info("Loaded %d closed trades from %s", len(rows), db_path)
    return rows


def compute_whale_stats(rows: list[sqlite3.Row], min_trades: int = 3) -> dict:
    """Aggregate per-whale stats from loaded trades.

    Returns:
        Dict[whale_name, dict] with n_trades, total_pnl, avg_pnl, win_rate, sharpe.
    """
    # Aggregate
    by_whale: dict = {}
    for r in rows:
        name = r["whale_name"] or "unknown"
        cat = r["category"] or "unknown"
        key = (name, cat)
        if key not in by_whale:
            by_whale[key] = {"pnls": [], "category": cat}
        by_whale[key]["pnls"].append(float(r["realized_pnl"] or 0))

    stats = {}
    for (name, cat), data in by_whale.items():
        pnls = data["pnls"]
        n = len(pnls)
        if n < min_trades:
            continue
        total = sum(pnls)
        wins = sum(1 for p in pnls if p > 0)
        mean = total / n
        variance = sum((p - mean) ** 2 for p in pnls) / max(n - 1, 1)
        std = math.sqrt(variance)
        sharpe = (mean / std) * math.sqrt(252) if std > 0 else 0.0
        stats[name] = {
            "category": cat,
            "n_trades": n,
            "total_pnl": round(total, 2),
            "avg_pnl": round(mean, 4),
            "win_rate": round(wins / n, 4),
            "sharpe": round(sharpe, 4),
            "profitable": total > 0,
        }
    return stats


def score_whales(
    whale_stats: dict,
    edge_scorer_class,
    db_path: Path,
) -> dict:
    """Score each whale using the current EdgeScorer and return per-whale results."""
    try:
        scorer = edge_scorer_class(db_path=db_path)
        scorer.refresh_if_stale()
    except Exception as e:
        logger.warning("Could not create EdgeScorer instance: %s", e)
        return {}

    results = {}
    for wname, stats in whale_stats.items():
        try:
            result = scorer.score_signal(
                whale_name=wname,
                category=stats["category"],
                raw_edge_score=0.5,
                confidence=0.5,
                side="BUY",
            )
            results[wname] = {
                "edge_score": result.edge_score,
                "action": result.action,
                "should_trade": result.should_trade,
                "source": result.source,
            }
        except Exception as e:
            logger.debug("Scoring failed for whale %s: %s", wname, e)

    return results


def bucket_validation(
    rows: list[sqlite3.Row],
    edge_scorer_class,
    db_path: Path,
) -> dict:
    """Validate that edge_score buckets correlate with actual win rates.

    Returns:
        Dict with per-bucket stats and overall monotonicity check.
    """
    buckets: dict[str, list[float]] = {b: [] for b in [
        "0.0-0.15", "0.15-0.3", "0.3-0.5", "0.5-0.7", "0.7-1.0",
    ]}

    try:
        scorer = edge_scorer_class(db_path=db_path)
        scorer.refresh_if_stale()
    except Exception:
        return {}

    for r in rows:
        try:
            result = scorer.score_signal(
                whale_name=r["whale_name"] or "unknown",
                category=r["category"] or "unknown",
                raw_edge_score=float(r["edge_score"] or 0.0),
                confidence=float(r["confidence"] or 0.5),
                side=r["side"] or "BUY",
            )
            e = result.edge_score
            if e < 0.15:
                bucket = "0.0-0.15"
            elif e < 0.30:
                bucket = "0.15-0.3"
            elif e < 0.50:
                bucket = "0.3-0.5"
            elif e < 0.70:
                bucket = "0.5-0.7"
            else:
                bucket = "0.7-1.0"

            pnl = float(r["realized_pnl"] or 0.0)
            if result.side_flip:
                pnl = -pnl
            buckets[bucket].append(pnl)
        except Exception:
            pass

    bucket_stats = {}
    for bucket, pnls in buckets.items():
        if not pnls:
            bucket_stats[bucket] = {"trades": 0, "wr": 0.0, "pnl": 0.0, "avg_pnl": 0.0}
            continue
        wr = sum(1 for p in pnls if p > 0) / len(pnls)
        bucket_stats[bucket] = {
            "trades": len(pnls),
            "wr": round(wr, 4),
            "pnl": round(sum(pnls), 2),
            "avg_pnl": round(sum(pnls) / len(pnls), 4),
        }

    # Check monotonicity: WR should rise with edge bucket
    ordered = ["0.0-0.15", "0.15-0.3", "0.3-0.5", "0.5-0.7", "0.7-1.0"]
    wrs = [bucket_stats[b]["wr"] for b in ordered if bucket_stats[b]["trades"] >= 5]
    monotonic = (
        sum(1 for i in range(1, len(wrs)) if wrs[i] >= wrs[i - 1]) >= len(wrs) - 1
        if len(wrs) >= 2 else True
    )

    return {"buckets": bucket_stats, "monotonic": monotonic, "wrs": wrs}


def check_discrimination_target(
    whale_scores: dict,
    whale_stats: dict,
) -> dict:
    """Check if current scorer satisfies the discrimination target:
      - Profitable whale (total PnL > 0): edge_score >= 0.50
      - Unprofitable whale (total PnL < 0): edge_score <= 0.30
    """
    above = below = unprof_above = unprof_below = 0
    adjustments = []

    for wname, score_data in whale_scores.items():
        stats = whale_stats.get(wname, {})
        if not stats:
            continue
        score = score_data["edge_score"]
        if stats["profitable"]:
            if score >= 0.50:
                above += 1
            else:
                below += 1
                adjustments.append({
                    "whale": wname,
                    "score": score,
                    "target": ">= 0.50",
                    "reason": f"profitable whale scored below 0.50 (n={stats['n_trades']})",
                })
        else:
            if score <= 0.30:
                unprof_below += 1
            else:
                unprof_above += 1
                adjustments.append({
                    "whale": wname,
                    "score": score,
                    "target": "<= 0.30",
                    "reason": f"unprofitable whale scored above 0.30 (n={stats['n_trades']})",
                })

    total_profitable = above + below
    total_unprofitable = unprof_below + unprof_above

    if below == 0 and unprof_above == 0:
        verdict = "PASS"
    elif total_profitable > 0 and (below / total_profitable) < 0.2:
        verdict = "MARGINAL"
    else:
        verdict = "ADJUST_RECOMMENDED"

    return {
        "verdict": verdict,
        "profitable_above": above,
        "profitable_below": below,
        "unprofitable_below": unprof_below,
        "unprofitable_above": unprof_above,
        "adjustments": adjustments[:20],  # Cap at 20 for readability
    }


def archive_calibration(output_path: Path, archive_dir: Path) -> Path | None:
    """Archive the current calibration JSON before overwriting."""
    if not output_path.exists():
        return None
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    dest = archive_dir / f"calibration_{stamp}.json"
    import shutil
    shutil.copy2(output_path, dest)
    logger.info("Archived calibration to %s", dest)
    return dest


def main():
    parser = argparse.ArgumentParser(description="Recalibrate Edge Scorer")
    parser.add_argument(
        "--db-path", type=Path, default=DEFAULT_DB,
        help=f"Path to trades.db (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUT,
        help=f"Output calibration JSON path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--cutoff-days", type=int, default=90,
        help="Only use trades from the last N days (default: 90)",
    )
    parser.add_argument(
        "--min-trades", type=int, default=3,
        help="Minimum trades per whale to include (default: 3)",
    )
    parser.add_argument(
        "--archive", action="store_true",
        help="Archive the existing calibration before writing",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress console output (log only)",
    )
    args = parser.parse_args()

    if args.quiet:
        logger.setLevel(logging.WARNING)

    logger.info("=== Edge Scorer Recalibration ===")
    logger.info("DB: %s | Cutoff: %sd | Min trades: %d",
                args.db_path, args.cutoff_days, args.min_trades)

    # ── Step 1: Load trades ────────────────────────────────────────────────
    rows = load_trades(args.db_path, cutoff_days=args.cutoff_days)
    if not rows:
        logger.error("No trades loaded — aborting.")
        sys.exit(1)

    # ── Step 2: Compute whale stats ─────────────────────────────────────────
    whale_stats = compute_whale_stats(rows, min_trades=args.min_trades)
    logger.info("Whales with >= %d trades: %d", args.min_trades, len(whale_stats))

    # ── Step 3: Score whales ────────────────────────────────────────────────
    # Import EdgeScorer lazily to avoid startup dependency
    try:
        from strategies.wf_edge_scorer import EdgeScorer
        edge_scorer_class = EdgeScorer
    except ImportError:
        logger.warning("Could not import EdgeScorer — skipping whale scoring")
        edge_scorer_class = None

    whale_scores = {}
    bucket_result = {}
    discrimination = {"verdict": "SKIPPED"}

    if edge_scorer_class is not None:
        whale_scores = score_whales(whale_stats, edge_scorer_class, args.db_path)
        logger.info("Whales scored: %d", len(whale_scores))

        # ── Step 4: Bucket validation ──────────────────────────────────────
        bucket_result = bucket_validation(rows, edge_scorer_class, args.db_path)
        monotonic = bucket_result.get("monotonic", None)
        logger.info(
            "Bucket monotonicity: %s | Buckets: %s",
            monotonic,
            {k: v["wr"] for k, v in bucket_result.get("buckets", {}).items() if v["trades"] > 0},
        )

        # ── Step 5: Discrimination target ─────────────────────────────────
        discrimination = check_discrimination_target(whale_scores, whale_stats)
        logger.info(
            "Discrimination verdict: %s | %d profitable, %d below threshold | "
            "%d unprofitable, %d above threshold",
            discrimination["verdict"],
            discrimination["profitable_above"],
            discrimination["profitable_below"],
            discrimination["unprofitable_below"],
            discrimination["unprofitable_above"],
        )

    # ── Step 6: Compute portfolio Sharpe ───────────────────────────────────
    all_pnls = [float(r["realized_pnl"] or 0) for r in rows]
    portfolio_sharpe = round(_annualized_sharpe(all_pnls), 4)
    logger.info("Portfolio Sharpe (annualized, last %dd): %s", args.cutoff_days, portfolio_sharpe)

    # ── Step 7: Compile result ──────────────────────────────────────────────
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config_version": "recalibration",
        "db_path": str(args.db_path),
        "cutoff_days": args.cutoff_days,
        "n_trades_loaded": len(rows),
        "n_whales": len(whale_stats),
        "portfolio_sharpe": portfolio_sharpe,
        "discrimination": discrimination,
        "bucket_validation": bucket_result,
        "whale_scores": whale_scores,
    }

    # ── Step 8: Write output ───────────────────────────────────────────────
    if args.archive:
        archive_calibration(args.output, DEFAULT_ARCHIVE)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    logger.info("Calibration written to %s", args.output)

    # ── Console summary ─────────────────────────────────────────────────────
    if not args.quiet:
        print("\n" + "=" * 60)
        print("EDGE SCORER RECALIBRATION")
        print("=" * 60)
        print(f"  Verdict:       {discrimination['verdict']}")
        print(f"  Trades:       {len(rows)}")
        print(f"  Whales:       {len(whale_stats)}")
        print(f"  Sharpe:       {portfolio_sharpe}")
        print(f"  Monotonic:    {bucket_result.get('monotonic', 'N/A')}")
        print()
        if discrimination["adjustments"]:
            print(f"  Adjustments ({len(discrimination['adjustments'])}):")
            for adj in discrimination["adjustments"][:5]:
                print(f"    {adj['whale'][:35]:35s} score={adj['score']:.3f}  {adj['reason']}")
        print()
        print(f"  Output: {args.output}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
