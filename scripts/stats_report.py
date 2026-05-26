#!/usr/bin/env python3
"""
A standalone stats report script for the Nautilus paper trading system.

This script replaces all ad‑hoc P&L reporting and produces a single authoritative
human‑readable report with the following sections:
1. Summary header – timestamp, total trades, resolved, open
2. P&L per category – using the canonical query
3. CapitalPool state – read from ``daily_state.json``
4. Open positions – count + brief list
5. Recent resolutions – last 5 resolved trades with outcome
6. Stall check – flag if no new trades in 4+ hours

Run it with the virtualenv interpreter:
```
cd /home/elon-1/workspace/nautilus-trading && ./venv/bin/python scripts/stats_report.py
```
"""

from __future__ import annotations

import datetime
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_PATH = Path("/home/elon-1/workspace/nautilus-trading")
DB_PATH = BASE_PATH / "data" / "trades.db"
STATE_JSON = BASE_PATH / "daily_state.json"
# Canonical P&L query – must stay exactly as specified in the task
PANDATA_QUERY = (
    """
    SELECT
        COALESCE(NULLIF(category, ''), 'Unknown') as category,
        COUNT(*) as total,
        SUM(CASE WHEN resolution_outcome LIKE 'WIN%%' THEN 1 ELSE 0 END) as wins,
        ROUND(1.0 * SUM(CASE WHEN resolution_outcome LIKE 'WIN%%' THEN 1 ELSE 0 END) / COUNT(*), 4) as accuracy,
        ROUND(SUM(actual_pnl), 2) as actual_pnl,
        ROUND(SUM(CASE WHEN exit_reason != 'certainty_win' THEN actual_pnl ELSE 0 END), 2) as pnl_ex_clwt
    FROM trades
    WHERE actual_pnl IS NOT NULL
        AND resolution_outcome NOT IN ('STALE_CLEANUP', 'PENDING')
        AND resolution_outcome NOT LIKE 'sybil%%'
        AND resolution_outcome NOT LIKE 'reconciled%%'
    GROUP BY category
    ORDER BY actual_pnl DESC
    """
)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_db_connection() -> sqlite3.Connection:
    """Return a new SQLite connection to the trades database."""
    return sqlite3.connect(DB_PATH)


def fetch_summary_stats(conn: sqlite3.Connection) -> Tuple[int, int, int]:
    """Return total trades, resolved, and open trade counts.

    ``resolved`` are trades where ``actual_pnl IS NOT NULL``.
    ``open`` are trades where ``exit_price IS NULL`` and ``exit_reason IS NULL`` and ``paper_trade=1``.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM trades")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM trades WHERE actual_pnl IS NOT NULL")
    resolved = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM trades WHERE exit_price IS NULL AND exit_reason IS NULL AND paper_trade=1")
    open_trades = cur.fetchone()[0]
    return total, resolved, open_trades


def fetch_pnl_per_category(conn: sqlite3.Connection) -> List[Tuple]:
    """Return rows of the canonical P&L query."""
    cur = conn.cursor()
    cur.execute(PANDATA_QUERY)
    return cur.fetchall()


def load_capital_state() -> Dict:
    """Load the daily_state.json file.

    Expected keys are not enforced – the script simply renders the JSON.
    """
    with STATE_JSON.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def fetch_open_positions(conn: sqlite3.Connection) -> List[Tuple[int, str]]:
    """Return a list of (trade_id, market_title) for open positions.

    Open = exit_price IS NULL AND exit_reason IS NULL (truly unresolved).
    Trades with an exit_reason but no exit_price are resolved-with-missing-price
    (e.g. orphan_cleanup) and should not appear as open.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_id, market_title FROM trades WHERE exit_price IS NULL AND exit_reason IS NULL AND paper_trade=1"
    )
    return cur.fetchall()


def fetch_recent_resolutions(conn: sqlite3.Connection, limit: int = 5) -> List[Tuple[int, str, str, str]]:
    """Return the most recent ``limit`` resolved trades.

    Columns: trade_id, market_title, resolution_outcome, timestamp.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_id, market_title, resolution_outcome, timestamp FROM trades WHERE actual_pnl IS NOT NULL ORDER BY timestamp DESC LIMIT ?",
        (limit,),
    )
    return cur.fetchall()


def check_stall(conn: sqlite3.Connection, threshold_hours: int = 4) -> bool:
    """Return True if no new trades in the past ``threshold_hours``.

    The function checks the timestamp of the most recent trade record.
    """
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM trades")
    max_ts = cur.fetchone()[0]
    if max_ts is None:
        return False
    most_recent = datetime.datetime.fromisoformat(max_ts).replace(tzinfo=None)
    return (datetime.datetime.now().replace(tzinfo=None) - most_recent) >= datetime.timedelta(hours=threshold_hours)


def format_table(headers: List[str], rows: List[List]) -> str:
    """Create a simple text table.

    Minimal implementation that aligns columns based on the longest cell.
    """
    padded_rows = [list(row) + [""] * (len(headers) - len(row)) for row in rows]
    col_sizes = [max(len(str(cell)) for cell in [header] + [row[i] for row in padded_rows]) for i, header in enumerate(headers)]
    sep = "|"
    line = "+" + "+".join("-" * (size + 2) for size in col_sizes) + "+"
    header_line = sep + sep.join(f" {header:{col_sizes[i]}} " for i, header in enumerate(headers)) + sep
    parts = [line, header_line, line]
    for row in padded_rows:
        parts.append(sep + sep.join(f" {str(cell):{col_sizes[i]}} " for i, cell in enumerate(row)) + sep)
    parts.append(line)
    return "\n".join(parts)

# ---------------------------------------------------------------------------
# Main report rendering
# ---------------------------------------------------------------------------

def main() -> None:
    conn = get_db_connection()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total, resolved, open_trades = fetch_summary_stats(conn)

    print(f"\n=== Trade Report – {now} ===\n")
    print(f"Total trades : {total}")
    print(f"Resolved    : {resolved}")
    print(f"Open        : {open_trades}\n")

    # P&L per category
    print("--- P&L per Category ---")
    rows = fetch_pnl_per_category(conn)
    if rows:
        headers = ["Category", "Total", "Wins", "Accuracy", "Actual P&L", "P&L Ex CLWT"]
        print(format_table(headers, [list(r) for r in rows]))
    else:
        print("No P&L data available.")
    print()

    # CapitalPool state
    print("--- CapitalPool State ---")
    state = load_capital_state()
    print(json.dumps(state, indent=2))
    print()

    # Open positions
    print("--- Open Positions ---")
    open_pos = fetch_open_positions(conn)
    print(f"Count: {len(open_pos)}")
    for trade_id, symbol in open_pos[:5]:
        print(f"  {trade_id}: {symbol}")
    if len(open_pos) > 5:
        print(f"  ... and {len(open_pos)-5} more")
    print()

    # Recent resolutions
    print("--- Recent Resolutions (5) ---")
    recent = fetch_recent_resolutions(conn, 5)
    if recent:
        headers = ["Trade ID", "Symbol", "Outcome", "Timestamp"]
        print(format_table(headers, [list(r) for r in recent]))
    else:
        print("No resolved trades found.")
    print()

    # Stall check
    print("--- Stall Check ---")
    if check_stall(conn):
        print("⚠️  No new trades in the last 4 hours – potential stall detected.")
    else:
        print("✅  Trade activity recent.")

    conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # pragma: no cover
        print(f"Error generating report: {e}", file=sys.stderr)
        sys.exit(1)
