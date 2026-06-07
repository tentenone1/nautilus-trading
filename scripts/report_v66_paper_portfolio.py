#!/usr/bin/env python3
"""Read-only report script for v6.6 paper portfolio.

Outputs markdown by default; optional --json for machine-readable output.

Includes:
  - open paper positions
  - unrealized/realized/total PnL
  - PnL by source/category/whale or cluster/category_action_v2
  - top winners and losers
  - max drawdown from paper_position_marks
  - position concentration by market, whale/cluster, category
  - markets with missing prices
  - stale price rows (last_price_timestamp older than 30 minutes)
  - live trades count (must print 0)
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

DEFAULT_DB = "/home/elon-1/workspace/nautilus-trading/data/trades.db"
DEFAULT_UPDATE_LOG = "/home/elon-1/workspace/nautilus-trading/logs/v66_paper_portfolio_update.log"

WHALE_CLUSTER_ALERT_PCT = 40.0
MARKET_ALERT_PCT = 35.0
UNKNOWN_WHALE_ALERT_PCT = 50.0
MTM_COVERAGE_ALERT_PCT = 80.0


def _connect_ro(db_path: str) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    if not path.exists():
        # Graceful fallback: return empty in-memory DB so report still renders
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        return conn
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    try:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
            (name,),
        ).fetchone() is not None
    except Exception:
        return False


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _pct(part: float, total: float) -> float:
    if not total:
        return 0.0
    return round((part / total) * 100.0, 2)


def _latest_updater_health(update_log_path: str = DEFAULT_UPDATE_LOG) -> dict[str, Any]:
    path = Path(update_log_path)
    default = {
        "last_summary": None,
        "last_errors": 0,
        "log_path": str(path),
        "found": False,
    }
    if not path.exists():
        return default

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return default

    for line in reversed(lines):
        if "MTM complete |" not in line:
            continue
        pairs = {
            key: int(value)
            for key, value in re.findall(r"(\w+)=(\d+)", line)
        }
        return {
            "last_summary": line,
            "last_errors": pairs.get("errors", 0),
            "last_total": pairs.get("total", 0),
            "last_updated": pairs.get("updated", 0),
            "last_missing_price": pairs.get("missing_price", 0),
            "last_stale_mark": pairs.get("stale_mark", 0),
            "last_unpriceable_token": pairs.get("unpriceable_token", 0),
            "last_unpriceable_data": pairs.get("unpriceable_data", 0),
            "last_resolved": pairs.get("resolved", 0),
            "log_path": str(path),
            "found": True,
        }

    return default


def _empty_concentration_report(update_log_path: str = DEFAULT_UPDATE_LOG) -> dict[str, Any]:
    updater_health = _latest_updater_health(update_log_path)
    return {
        "mtm_coverage": {
            "operational_positions": 0,
            "tokenized_operational_positions": 0,
            "marked_operational_positions": 0,
            "coverage_pct": 100.0,
        },
        "updater_health": updater_health,
        "concentration_risk": {
            "total_open_exposure": 0.0,
            "unknown_whale_exposure": 0.0,
            "unknown_whale_exposure_pct": 0.0,
            "max_whale_cluster_exposure_pct": 0.0,
            "max_market_exposure_pct": 0.0,
            "alert_labels": ["updater_errors_gt_0"] if updater_health.get("last_errors", 0) > 0 else [],
            "hypothetical_flags": {
                "would_exceed_whale_cap": False,
                "would_exceed_market_cap": False,
                "would_exceed_unknown_cap": False,
            },
        },
        "concentration_by_source": [],
        "concentration_by_category_action_v2": [],
    }


def generate_report(db_path: str = DEFAULT_DB, update_log_path: str = DEFAULT_UPDATE_LOG) -> dict[str, Any]:
    conn = _connect_ro(db_path)
    try:
        report: dict[str, Any] = {}

        # live trades count
        live_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] if _has_table(conn, "trades") else 0
        report["live_trades_count"] = live_count

        if not _has_table(conn, "paper_positions"):
            report.update({
                "open_positions": {"count": 0, "total_unrealized_pnl": 0.0, "total_realized_pnl": 0.0, "total_pnl": 0.0},
                "all_time": {"realized_pnl": 0.0, "unrealized_pnl": 0.0, "total_pnl": 0.0},
                "pnl_by_source_category_whale": [],
                "top_winners": [],
                "top_losers": [],
                "max_drawdown": 0.0,
                "concentration_by_market": [],
                "concentration_by_whale": [],
                "concentration_by_category": [],
                "missing_prices": [],
                "stale_prices": [],
            })
            report.update(_empty_concentration_report(update_log_path))
            return report

        # ── open positions summary ──────────────────────────────────────
        open_summary = conn.execute(
            """
            SELECT COUNT(*) AS open_count,
                   COALESCE(SUM(unrealized_pnl), 0) AS total_unrealized,
                   COALESCE(SUM(realized_pnl), 0) AS total_realized,
                   COALESCE(SUM(unrealized_pnl + realized_pnl), 0) AS total_pnl
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            """
        ).fetchone()
        report["open_positions"] = {
            "count": open_summary["open_count"],
            "total_unrealized_pnl": open_summary["total_unrealized"],
            "total_realized_pnl": open_summary["total_realized"],
            "total_pnl": open_summary["total_pnl"],
        }

        # ── legacy unpriceable rows ─────────────────────────────────────
        legacy_count = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE price_status = 'legacy_unpriceable_missing_token'"
        ).fetchone()[0]
        report["legacy_unpriceable_rows"] = {
            "count": legacy_count,
            "reason": "missing outcome_token from pre-v6.6 capture window"
        }

        # ── all-time totals ─────────────────────────────────────────────
        totals = conn.execute(
            """
            SELECT COALESCE(SUM(realized_pnl), 0) AS all_realized,
                   COALESCE(SUM(unrealized_pnl), 0) AS all_unrealized,
                   COALESCE(SUM(realized_pnl + unrealized_pnl), 0) AS all_total
            FROM paper_positions
            WHERE COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            """
        ).fetchone()
        report["all_time"] = {
            "realized_pnl": totals["all_realized"],
            "unrealized_pnl": totals["all_unrealized"],
            "total_pnl": totals["all_total"],
        }

        # ── PnL by source/category/whale ────────────────────────────────
        report["pnl_by_source_category_whale"] = _rows(conn, """
            SELECT COALESCE(source, 'unknown') AS source,
                   COALESCE(category, 'unknown') AS category,
                   COALESCE(whale_name, 'unknown') AS whale_name,
                   COALESCE(whale_cluster, 'unknown') AS whale_cluster,
                   COALESCE(category_action_v2, 'unknown') AS category_action_v2,
                   COUNT(*) AS n,
                   ROUND(SUM(unrealized_pnl), 2) AS unrealized,
                   ROUND(SUM(realized_pnl), 2) AS realized,
                   ROUND(SUM(unrealized_pnl + realized_pnl), 2) AS total
            FROM paper_positions
            WHERE COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY source, category, whale_name, whale_cluster, category_action_v2
            ORDER BY total DESC
            LIMIT 50
        """)

        # ── top winners and losers (open + closed) ───────────────────────
        top_positions_sql = """
            SELECT id, shadow_trade_id, whale_name,
                   COALESCE(NULLIF(market_title, ''), SUBSTR(condition_id, 1, 12)) AS market_title,
                   ROUND(unrealized_pnl + realized_pnl, 2) AS total_pnl
            FROM paper_positions
            WHERE COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            ORDER BY total_pnl {direction}
            LIMIT 10
        """
        report["top_winners"] = _rows(conn, top_positions_sql.format(direction="DESC"))
        report["top_losers"] = _rows(conn, top_positions_sql.format(direction="ASC"))

        # ── max drawdown from marks ─────────────────────────────────────
        # Compute per-position drawdown as max(total_pnl) - min(total_pnl)
        dd = conn.execute(
            """
            SELECT COALESCE(MAX(drawdown), 0) AS max_drawdown
            FROM (
                SELECT MAX(total_pnl) - MIN(total_pnl) AS drawdown
                FROM paper_position_marks
                WHERE position_id IN (
                SELECT id FROM paper_positions 
                    WHERE COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
                )
                GROUP BY position_id
            )
            """
        ).fetchone()
        report["max_drawdown"] = dd["max_drawdown"] if dd else 0.0

        # ── concentration ────────────────────────────────────────────────
        total_open_exposure = conn.execute(
            """
            SELECT COALESCE(SUM(simulated_size), 0) AS total_size
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            """
        ).fetchone()["total_size"] or 0.0

        report["concentration_by_market"] = _rows(conn, """
            SELECT COALESCE(NULLIF(market_title, ''), SUBSTR(condition_id, 1, 12)) AS market_title,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size,
                   ROUND((SUM(simulated_size) * 100.0) / NULLIF(?, 0), 2) AS exposure_pct
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY COALESCE(NULLIF(market_title, ''), SUBSTR(condition_id, 1, 12))
            ORDER BY total_size DESC
            LIMIT 20
        """, (total_open_exposure,))
        report["concentration_by_whale"] = _rows(conn, """
            SELECT COALESCE(NULLIF(whale_name, ''), COALESCE(whale_cluster, 'unknown')) AS whale_name,
                   COALESCE(whale_cluster, 'unknown') AS whale_cluster,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size,
                   ROUND((SUM(simulated_size) * 100.0) / NULLIF(?, 0), 2) AS exposure_pct
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY COALESCE(NULLIF(whale_name, ''), COALESCE(whale_cluster, 'unknown')), whale_cluster
            ORDER BY total_size DESC
            LIMIT 20
        """, (total_open_exposure,))
        report["concentration_by_category"] = _rows(conn, """
            SELECT COALESCE(category, 'unknown') AS category,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size,
                   ROUND((SUM(simulated_size) * 100.0) / NULLIF(?, 0), 2) AS exposure_pct
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY category
            ORDER BY total_size DESC
            LIMIT 20
        """, (total_open_exposure,))
        report["concentration_by_source"] = _rows(conn, """
            SELECT COALESCE(source, 'unknown') AS source,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size,
                   ROUND((SUM(simulated_size) * 100.0) / NULLIF(?, 0), 2) AS exposure_pct
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY source
            ORDER BY total_size DESC
            LIMIT 20
        """, (total_open_exposure,))
        report["concentration_by_category_action_v2"] = _rows(conn, """
            SELECT COALESCE(category_action_v2, 'unknown') AS category_action_v2,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size,
                   ROUND((SUM(simulated_size) * 100.0) / NULLIF(?, 0), 2) AS exposure_pct
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            GROUP BY category_action_v2
            ORDER BY total_size DESC
            LIMIT 20
        """, (total_open_exposure,))

        mtm = conn.execute(
            """
            SELECT
                COUNT(*) AS operational_positions,
                SUM(CASE WHEN outcome_token IS NOT NULL AND outcome_token != '' THEN 1 ELSE 0 END) AS tokenized_positions,
                SUM(CASE WHEN outcome_token IS NOT NULL AND outcome_token != ''
                         AND price_status = 'ok'
                         AND last_price_timestamp IS NOT NULL THEN 1 ELSE 0 END) AS marked_positions
            FROM paper_positions
            WHERE resolved = 0 AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
            """
        ).fetchone()
        tokenized_positions = mtm["tokenized_positions"] or 0
        marked_positions = mtm["marked_positions"] or 0
        coverage_pct = 100.0 if tokenized_positions == 0 else _pct(marked_positions, tokenized_positions)
        report["mtm_coverage"] = {
            "operational_positions": mtm["operational_positions"] or 0,
            "tokenized_operational_positions": tokenized_positions,
            "marked_operational_positions": marked_positions,
            "coverage_pct": coverage_pct,
        }

        unknown_exposure = conn.execute(
            """
            SELECT COALESCE(SUM(simulated_size), 0) AS total_size
            FROM paper_positions
            WHERE resolved = 0
              AND COALESCE(price_status, '') != 'legacy_unpriceable_missing_token'
              AND LOWER(COALESCE(NULLIF(whale_name, ''), COALESCE(whale_cluster, 'unknown'))) = 'unknown'
            """
        ).fetchone()["total_size"] or 0.0
        max_whale_pct = report["concentration_by_whale"][0]["exposure_pct"] if report["concentration_by_whale"] else 0.0
        max_market_pct = report["concentration_by_market"][0]["exposure_pct"] if report["concentration_by_market"] else 0.0
        updater_health = _latest_updater_health(update_log_path)

        alert_labels: list[str] = []
        if max_whale_pct > WHALE_CLUSTER_ALERT_PCT:
            alert_labels.append("single_whale_cluster_gt_40")
        if max_market_pct > MARKET_ALERT_PCT:
            alert_labels.append("single_market_gt_35")
        if _pct(unknown_exposure, total_open_exposure) > UNKNOWN_WHALE_ALERT_PCT:
            alert_labels.append("unknown_whale_gt_50")
        if coverage_pct < MTM_COVERAGE_ALERT_PCT:
            alert_labels.append("mtm_coverage_lt_80")
        if updater_health.get("last_errors", 0) > 0:
            alert_labels.append("updater_errors_gt_0")

        report["updater_health"] = updater_health
        report["concentration_risk"] = {
            "total_open_exposure": round(total_open_exposure, 2),
            "unknown_whale_exposure": round(unknown_exposure, 2),
            "unknown_whale_exposure_pct": _pct(unknown_exposure, total_open_exposure),
            "max_whale_cluster_exposure_pct": max_whale_pct or 0.0,
            "max_market_exposure_pct": max_market_pct or 0.0,
            "alert_labels": alert_labels,
            "hypothetical_flags": {
                "would_exceed_whale_cap": (max_whale_pct or 0.0) > WHALE_CLUSTER_ALERT_PCT,
                "would_exceed_market_cap": (max_market_pct or 0.0) > MARKET_ALERT_PCT,
                "would_exceed_unknown_cap": _pct(unknown_exposure, total_open_exposure) > UNKNOWN_WHALE_ALERT_PCT,
            },
        }

        # ── missing prices ──────────────────────────────────────────────
        report["missing_prices"] = _rows(conn, """
            SELECT id, shadow_trade_id, whale_name, market_title, price_status
            FROM paper_positions
            WHERE resolved = 0
              AND price_status IN ('missing_outcome_token', 'missing_price')
              AND price_status != 'legacy_unpriceable_missing_token'
            ORDER BY id DESC
            LIMIT 50
        """)

        # ── stale prices (older than 30 minutes) ────────────────────────
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        report["stale_prices"] = _rows(conn, """
            SELECT id, shadow_trade_id, whale_name, market_title, last_price_timestamp
            FROM paper_positions
            WHERE resolved = 0
              AND price_status != 'missing_outcome_token'
              AND price_status != 'unpriceable_missing_outcome_token'
              AND price_status != 'legacy_unpriceable_missing_token'
              AND last_price_timestamp IS NOT NULL
              AND datetime(last_price_timestamp) < datetime(?)
            ORDER BY id DESC
            LIMIT 50
        """, (stale_cutoff,))

        return report
    finally:
        conn.close()


def _fmt_money(v: float) -> str:
    return f"${v:,.2f}"


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# v6.6 Paper Portfolio Report")
    lines.append("")

    lines.append(f"**Live trades count:** {report['live_trades_count']} (must be 0)")
    lines.append("")

    op = report["open_positions"]
    lines.append("## Open Positions")
    lines.append(f"- Count: {op['count']}")
    lines.append(f"- Unrealized PnL: {_fmt_money(op['total_unrealized_pnl'])}")
    lines.append(f"- Realized PnL:   {_fmt_money(op['total_realized_pnl'])}")
    lines.append(f"- Total PnL:      {_fmt_money(op['total_pnl'])}")
    lines.append("")

    at = report["all_time"]
    lines.append("## All-Time Totals")
    lines.append(f"- Realized PnL:   {_fmt_money(at['realized_pnl'])}")
    lines.append(f"- Unrealized PnL: {_fmt_money(at['unrealized_pnl'])}")
    lines.append(f"- Total PnL:      {_fmt_money(at['total_pnl'])}")
    lines.append("")

    # ── legacy unpriceable rows ─────────────────────────────────────
    legacy = report["legacy_unpriceable_rows"]
    lines.append("## Legacy Unpriceable Rows")
    lines.append(f"- Count: {legacy['count']}")
    lines.append(f"- Reason: {legacy['reason']}")
    lines.append("")

    lines.append("## PnL by Source / Category / Whale / Cluster / v2 Action")
    lines.append("| source | category | whale | cluster | v2 | n | unrealized | realized | total |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in report["pnl_by_source_category_whale"]:
        lines.append(
            f"| {row['source']} | {row['category']} | {row['whale_name']} | "
            f"{row['whale_cluster']} | {row['category_action_v2']} | "
            f"{row['n']} | {_fmt_money(row['unrealized'])} | {_fmt_money(row['realized'])} | {_fmt_money(row['total'])} |"
        )
    lines.append("")

    lines.append("## Top Winners")
    for row in report["top_winners"]:
        lines.append(f"- id={row['id']} | {row['whale_name']} | {row['market_title'][:50]} | PnL={_fmt_money(row['total_pnl'])}")
    lines.append("")

    lines.append("## Top Losers")
    for row in report["top_losers"]:
        lines.append(f"- id={row['id']} | {row['whale_name']} | {row['market_title'][:50]} | PnL={_fmt_money(row['total_pnl'])}")
    lines.append("")

    lines.append(f"## Max Drawdown (from marks)")
    lines.append(f"- {_fmt_money(report['max_drawdown'])}")
    lines.append("")

    risk = report["concentration_risk"]
    mtm = report["mtm_coverage"]
    updater = report["updater_health"]
    flags = risk["hypothetical_flags"]
    lines.append("## Concentration Risk Alerts")
    lines.append(f"- Total open exposure: {_fmt_money(risk['total_open_exposure'])}")
    lines.append(
        f"- MTM coverage: {mtm['marked_operational_positions']}/"
        f"{mtm['tokenized_operational_positions']} ({mtm['coverage_pct']:.2f}%)"
    )
    lines.append(f"- Unknown whale exposure: {_fmt_money(risk['unknown_whale_exposure'])} ({risk['unknown_whale_exposure_pct']:.2f}%)")
    lines.append(f"- Last updater errors: {updater.get('last_errors', 0)}")
    if risk["alert_labels"]:
        lines.append(f"- Alert labels: {', '.join(risk['alert_labels'])}")
    else:
        lines.append("- Alert labels: None")
    lines.append(f"- would_exceed_whale_cap: {flags['would_exceed_whale_cap']}")
    lines.append(f"- would_exceed_market_cap: {flags['would_exceed_market_cap']}")
    lines.append(f"- would_exceed_unknown_cap: {flags['would_exceed_unknown_cap']}")
    lines.append("")

    for name, key in [
        ("Market", "concentration_by_market"),
        ("Whale/Cluster", "concentration_by_whale"),
        ("Category", "concentration_by_category"),
        ("Source", "concentration_by_source"),
        ("Category Action v2", "concentration_by_category_action_v2"),
    ]:
        lines.append(f"## Concentration by {name}")
        for row in report[key]:
            label = (
                row.get('market_title')
                or row.get('whale_name')
                or row.get('category')
                or row.get('source')
                or row.get('category_action_v2')
            )
            pct = row.get("exposure_pct")
            pct_text = f" | exposure={pct:.2f}%" if pct is not None else ""
            lines.append(f"- {label} | n={row['n']} | size={_fmt_money(row['total_size'])}{pct_text}")
        lines.append("")

    lines.append("## Missing Prices")
    if report["missing_prices"]:
        for row in report["missing_prices"]:
            lines.append(f"- id={row['id']} | {row['whale_name']} | {row['market_title'][:50]} | status={row['price_status']}")
    else:
        lines.append("None")
    lines.append("")

    lines.append("## Stale Prices (>30 min)")
    if report["stale_prices"]:
        for row in report["stale_prices"]:
            ts = row.get("last_price_timestamp") or "never"
            lines.append(f"- id={row['id']} | {row['whale_name']} | {row['market_title'][:50]} | last={ts}")
    else:
        lines.append("None")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=DEFAULT_DB)
    parser.add_argument("--json", action="store_true", help="Output JSON instead of markdown")
    args = parser.parse_args()

    report = generate_report(args.db_path)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(render_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
