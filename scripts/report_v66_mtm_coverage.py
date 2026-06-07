#!/usr/bin/env python3
"""v6.6 MTM Coverage Report — shows mark-to-market health for paper positions.

Reports:
  - total paper positions / tokenized / marked
  - mark coverage %
  - ok price %
  - fallback source counts
  - missing token rows
  - API/no-data rows
  - stale tokenized rows
  - live trades count
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = "/home/elon-1/workspace/nautilus-trading/data/trades.db"


def _connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path).resolve()
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    try:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?", (name,)
        ).fetchone() is not None
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=DEFAULT_DB)
    args = parser.parse_args()

    conn = _connect(args.db_path)
    has_pp = _has_table(conn, "paper_positions")

    # Live trades count
    live_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] if _has_table(conn, "trades") else 0

    # Paper position counts
    total = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio'").fetchone()[0] if has_pp else 0
    legacy_count = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='legacy_unpriceable_missing_token'").fetchone()[0] if has_pp else 0
    operational_count = total - legacy_count
    operational_tokenized = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND outcome_token IS NOT NULL AND outcome_token!='' AND price_status!='legacy_unpriceable_missing_token'").fetchone()[0] if has_pp else 0
    # Actually count marked positions (have price data) rather than assuming all tokenized are marked
    operational_marked = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND outcome_token IS NOT NULL AND outcome_token!='' AND price_status NOT IN ('legacy_unpriceable_missing_token','no_orderbook_or_illiquid') AND current_price IS NOT NULL AND last_price_timestamp IS NOT NULL").fetchone()[0] if has_pp else 0
    
    ok_count = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='ok'").fetchone()[0] if has_pp else 0
    operational_ok_count = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='ok' AND outcome_token IS NOT NULL AND outcome_token!='' AND price_status!='legacy_unpriceable_missing_token'").fetchone()[0] if has_pp else 0

    # Fallback source counts (exclude legacy rows)
    by_source = {}
    if has_pp:
        sources = conn.execute("SELECT price_source, COUNT(*) AS n FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status!='legacy_unpriceable_missing_token' GROUP BY price_source").fetchall()
        by_source = {row["price_source"] or "none": row["n"] for row in sources}

    # Unpriceable categories
    missing_token = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND (outcome_token IS NULL OR outcome_token='') AND price_status!='legacy_unpriceable_missing_token'").fetchone()[0] if has_pp else 0
    api_errors = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='api_error'").fetchone()[0] if has_pp else 0
    no_orderbook = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='no_orderbook_or_illiquid'").fetchone()[0] if has_pp else 0
    no_price = conn.execute("SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND price_status='missing_price'").fetchone()[0] if has_pp else 0

    # Stale tokenized rows (>30 min since last mark) - exclude legacy rows
    stale_tokenized = 0
    if has_pp:
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        stale_tokenized = conn.execute(
            "SELECT COUNT(*) FROM paper_positions WHERE experiment_tag='v6.6-paper-portfolio' AND resolved=0 AND outcome_token IS NOT NULL AND outcome_token!='' AND price_status NOT IN ('legacy_unpriceable_missing_token','no_orderbook_or_illiquid') AND last_price_timestamp IS NOT NULL AND datetime(last_price_timestamp) < datetime(?)",
            (cutoff,),
        ).fetchone()[0]

    conn.close()

    operational_mark_pct = round(operational_marked / operational_count * 100, 1) if operational_count else 0
    operational_ok_pct = round(operational_ok_count / operational_count * 100, 1) if operational_count else 0
    operational_token_pct = round(operational_tokenized / operational_count * 100, 1) if operational_count else 0

    print("v6.6 MTM Coverage Report")
    print("=" * 60)
    print(f"Total paper positions:           {total}")
    print(f"Legacy unpriceable rows:         {legacy_count}")
    print(f"Operational positions:           {operational_count}")
    print(f"Tokenized operational positions: {operational_tokenized} ({operational_token_pct}%)")
    print(f"Marked operational positions:    {operational_marked} ({operational_mark_pct}%)")
    print(f"OK price positions:              {ok_count} ({operational_ok_pct}% of operational)")
    print()
    print("Fallback source counts:")
    for src, n in sorted(by_source.items()):
        print(f"  {src:30s} {n}")
    print()
    print(f"Missing token rows:              {missing_token}")
    print(f"API error rows:                  {api_errors}")
    print(f"No orderbook / illiquid rows:    {no_orderbook}")
    print(f"No market data rows:             {no_price}")
    print(f"Stale tokenized rows (>30 min):  {stale_tokenized}")
    print()
    print(f"Live trades count:               {live_count}")
    if live_count != 0:
        print("  WARNING: live trades count is non-zero!")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
