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
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB = "/home/elon-1/workspace/nautilus-trading/data/trades.db"


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


def generate_report(db_path: str = DEFAULT_DB) -> dict[str, Any]:
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
            return report

        # ── open positions summary ──────────────────────────────────────
        open_summary = conn.execute(
            """
            SELECT COUNT(*) AS open_count,
                   COALESCE(SUM(unrealized_pnl), 0) AS total_unrealized,
                   COALESCE(SUM(realized_pnl), 0) AS total_realized,
                   COALESCE(SUM(unrealized_pnl + realized_pnl), 0) AS total_pnl
            FROM paper_positions
            WHERE resolved = 0 AND price_status != 'legacy_unpriceable_missing_token'
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
            WHERE price_status != 'legacy_unpriceable_missing_token'
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
            WHERE price_status != 'legacy_unpriceable_missing_token'
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
            WHERE price_status != 'legacy_unpriceable_missing_token'
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
                    WHERE price_status != 'legacy_unpriceable_missing_token'
                )
                GROUP BY position_id
            )
            """
        ).fetchone()
        report["max_drawdown"] = dd["max_drawdown"] if dd else 0.0

        # ── concentration ────────────────────────────────────────────────
        report["concentration_by_market"] = _rows(conn, """
            SELECT COALESCE(NULLIF(market_title, ''), SUBSTR(condition_id, 1, 12)) AS market_title,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size
            FROM paper_positions
            WHERE resolved = 0 AND price_status != 'legacy_unpriceable_missing_token'
            GROUP BY COALESCE(NULLIF(market_title, ''), SUBSTR(condition_id, 1, 12))
            ORDER BY n DESC
            LIMIT 20
        """)
        report["concentration_by_whale"] = _rows(conn, """
            SELECT COALESCE(NULLIF(whale_name, ''), COALESCE(whale_cluster, 'unknown')) AS whale_name,
                   COALESCE(whale_cluster, 'unknown') AS whale_cluster,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size
            FROM paper_positions
            WHERE resolved = 0 AND price_status != 'legacy_unpriceable_missing_token'
            GROUP BY COALESCE(NULLIF(whale_name, ''), COALESCE(whale_cluster, 'unknown')), whale_cluster
            ORDER BY n DESC
            LIMIT 20
        """)
        report["concentration_by_category"] = _rows(conn, """
            SELECT COALESCE(category, 'unknown') AS category,
                   COUNT(*) AS n,
                   ROUND(SUM(simulated_size), 2) AS total_size
            FROM paper_positions
            WHERE resolved = 0 AND price_status != 'legacy_unpriceable_missing_token'
            GROUP BY category
            ORDER BY n DESC
            LIMIT 20
        """)

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
        from datetime import datetime, timezone, timedelta
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        report["stale_prices"] = _rows(conn, """
            SELECT id, shadow_trade_id, whale_name, market_title, last_price_timestamp
            FROM paper_positions
            WHERE resolved = 0
              AND price_status != 'missing_outcome_token'
              AND price_status != 'unpriceable_missing_outcome_token'
              AND price_status != 'legacy_unpriceable_missing_token'
              AND (last_price_timestamp IS NULL OR datetime(last_price_timestamp) < datetime(?))
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

    for name, key in [
        ("Market", "concentration_by_market"),
        ("Whale/Cluster", "concentration_by_whale"),
        ("Category", "concentration_by_category"),
    ]:
        lines.append(f"## Concentration by {name}")
        for row in report[key]:
            lines.append(f"- {row.get('market_title') or row.get('whale_name') or row.get('category')} | n={row['n']} | size={_fmt_money(row['total_size'])}")
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
