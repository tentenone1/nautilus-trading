#!/usr/bin/env python3
"""
Polymarket Market Resolution Tracker

Polls Polymarket CLOB API for each unique condition_id in trades.db, 
determines actual market resolution outcomes, and records real P&L.

Usage:
    python scripts/polymarket_resolution.py                          # Run against all trades
    python scripts/polymarket_resolution.py --condition_id 0x...     # Single market check
    python scripts/polymarket_resolution.py --report-only            # Generate report without writing
    python scripts/polymarket_resolution.py --update-db              # Write results back to DB
"""

import argparse
import json
import sqlite3
import sys
import time
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Configuration ──────────────────────────────────────────────────────
TRADES_DB = Path("/home/elon-1/workspace/nautilus-trading/research/trades.db")
CLOB_API = "https://clob.polymarket.com"
RATE_LIMIT_SLEEP = 0.2          # seconds between API calls
REQUEST_TIMEOUT = 25             # seconds per request
MAX_RETRIES = 3                 # max retries per market on SSL/EOF errors
RETRY_BASE_DELAY = 2.0          # base delay seconds for exponential backoff
CHUNK_SIZE = 30                 # markets per batch before commit
CHECKPOINT_FILE = Path("/tmp/resolution_checkpoint.json")

# ── Schema ─────────────────────────────────────────────────────────────
# trades table has: condition_id, instrument_id, token_id, side,
#   entry_price, position_size_usd, exit_reason, realized_pnl,
#   realized_return, resolution_outcome, dispute_flag


def get_trade_db_connection() -> sqlite3.Connection:
    """Open connection to trades.db with row factory."""
    conn = sqlite3.connect(str(TRADES_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_market_resolution(condition_id: str) -> Optional[dict]:
    """
    Fetch market data from CLOB API and return resolution info.
    
    Retries up to MAX_RETRIES times on SSL/EOF/network errors with
    exponential backoff. Returns None only after all retries exhausted.
    
    Returns dict with keys:
        - resolved (bool): market has a definitive winner
        - winning_outcome (str): name of the winning outcome
        - winning_token_id (str): token_id of the winning outcome
        - losing_outcome (str): name of the losing outcome
        - losing_token_id (str): token_id of the losing outcome
        - closed (bool): market is closed to new orders
        - question (str): market question
        - all_tokens (list): full token list from API
    
    Returns None if API error.
    """
    url = f"{CLOB_API}/markets/{condition_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "Hermes/1.0"})
    
    last_error: str = ""
    data: Optional[dict] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                data = json.loads(resp.read().decode())
            break
        except (
            ssl.SSLEOFError,
            ssl.SSLError,
            ConnectionResetError,
            urllib.error.HTTPError,
            urllib.error.URLError,
            json.JSONDecodeError,
            OSError,
        ) as e:
            last_error = str(e)
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                print(f"  [RETRY] {condition_id[:30]}... attempt {attempt}/{MAX_RETRIES} "
                      f"failed ({e}), retrying in {delay:.1f}s...", file=sys.stderr)
                time.sleep(delay)
            else:
                print(f"  [WARN] API error for {condition_id[:30]}... (after {MAX_RETRIES} "
                      f"attempts): {e}", file=sys.stderr)
                return None
    
    if data is None:
        return None
    
    if not isinstance(data, dict):
        print(f"  [WARN] Unexpected response for {condition_id[:30]}...", file=sys.stderr)
        return None
    
    tokens = data.get("tokens", [])
    if not tokens:
        return {
            "resolved": False,
            "closed": data.get("closed", False),
            "question": data.get("question", ""),
            "winning_outcome": None,
            "winning_token_id": None,
            "losing_outcome": None,
            "losing_token_id": None,
            "all_tokens": [],
        }
    
    winners = [t for t in tokens if t.get("winner") is True]
    losers = [t for t in tokens if t.get("winner") is False]
    
    resolved = len(winners) == 1 and len(losers) >= 1
    winning_token_id = winners[0].get("token_id", "") if winners else None
    winning_outcome = winners[0].get("outcome", "") if winners else None
    losing_token_id = losers[0].get("token_id", "") if losers else None
    losing_outcome = losers[0].get("outcome", "") if losers else None
    
    return {
        "resolved": resolved,
        "closed": data.get("closed", False),
        "question": data.get("question", ""),
        "winning_outcome": winning_outcome,
        "winning_token_id": winning_token_id,
        "losing_outcome": losing_outcome,
        "losing_token_id": losing_token_id,
        "all_tokens": tokens,
    }


def parse_token_id_from_instrument(instrument_id: str) -> Optional[str]:
    """
    Extract token_id from instrument_id.
    
    Format: {condition_id}-{token_id}.POLYMARKET
    """
    if not instrument_id:
        return None
    symbol = instrument_id
    if ".POLYMARKET" in symbol:
        symbol = symbol.replace(".POLYMARKET", "")
    parts = symbol.split("-")
    if len(parts) >= 2:
        return parts[-1]
    return None


def calculate_actual_pnl(
    entry_price: float,
    position_size_usd: float,
    our_token_id: str,
    winning_token_id: str,
    side: str,
) -> dict:
    """
    Calculate actual P&L based on market resolution.
    
    position_size_usd = cost basis (shares * entry_price)
    shares = position_size_usd / entry_price
    
    If our token won:
        value = shares * $1 = position_size_usd / entry_price
        pnl = value - cost = position_size_usd / entry_price - position_size_usd
            = position_size_usd * (1/entry_price - 1)
    If our token lost:
        value = 0
        pnl = -position_size_usd
        
    For SELL side (sold YES token = effectively bought NO):
        Inverse logic applies.
    """
    if entry_price is None or entry_price <= 0 or entry_price > 1:
        return {"actual_pnl": None, "actual_return": None, "won": None}
    
    shares = position_size_usd / entry_price
    
    if side.upper() == "SELL":
        # Sell side: we sold YES at entry_price, so we want YES to lose
        # Actually, in Nautilus/Polymarket adapter:
        # BUY  = buying YES token (long event to happen)
        # SELL = selling YES token (short event to happen = long NO token)
        won = (our_token_id != winning_token_id)
    else:
        # BUY = buying YES token
        won = (our_token_id == winning_token_id)
    
    if won:
        actual_pnl = round(shares * 1.0 - position_size_usd, 2)
        actual_return = round((1.0 - entry_price) / entry_price * 100, 2)
    else:
        actual_pnl = round(-position_size_usd, 2)
        actual_return = -100.0
    
    return {
        "actual_pnl": actual_pnl,
        "actual_return": actual_return,
        "won": won,
    }


def fetch_all_condition_ids(conn: sqlite3.Connection) -> list[dict]:
    """Get all unique condition_ids from trades with sample metadata."""
    cursor = conn.execute("""
        SELECT DISTINCT 
            t.condition_id,
            t.instrument_id,
            t.side,
            t.entry_price,
            t.position_size_usd,
            t.market_title,
            t.whale_name,
            t.category
        FROM trades t
        WHERE t.condition_id IS NOT NULL 
          AND t.condition_id != ''
          AND t.instrument_id IS NOT NULL 
          AND t.instrument_id != ''
          AND t.condition_id IN (
              SELECT condition_id FROM trades
              WHERE condition_id IS NOT NULL AND condition_id != ''
              GROUP BY condition_id
              HAVING COUNT(CASE WHEN actual_pnl IS NULL THEN 1 END) > 0
          )
        ORDER BY t.category, t.condition_id
    """)
    return [dict(r) for r in cursor.fetchall()]


def fetch_trades_for_condition(conn: sqlite3.Connection, condition_id: str) -> list[dict]:
    """Get all trades for a specific condition_id."""
    cursor = conn.execute("""
        SELECT trade_id, instrument_id, side, entry_price, 
               position_size_usd, exit_reason, realized_pnl,
               resolution_outcome, dispute_flag
        FROM trades
        WHERE condition_id = ?
    """, (condition_id,))
    return [dict(r) for r in cursor.fetchall()]


def update_trade_with_resolution(
    conn: sqlite3.Connection,
    trade_id: str,
    actual_pnl: float,
    actual_return: float,
    resolution_outcome: str,
    dispute_flag: int = 0,
) -> bool:
    """Update a trade record with actual resolution data."""
    try:
        conn.execute("""
            UPDATE trades
            SET actual_pnl = ?,
                actual_return = ?,
                resolution_outcome = ?,
                dispute_flag = ?
            WHERE trade_id = ?
        """, (actual_pnl, actual_return, resolution_outcome, dispute_flag, trade_id))
        return True
    except sqlite3.Error as e:
        print(f"  [ERROR] DB update failed for {trade_id[:20]}...: {e}", file=sys.stderr)
        return False


def run_resolution_check(
    condition_id: str,
    all_trades: list[dict],
    update_db: bool = False,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """
    Check resolution for one condition_id across all its trades.
    
    Returns summary dict.
    """
    market_info = get_market_resolution(condition_id)
    if market_info is None:
        return {"condition_id": condition_id, "error": "API error", "trades_updated": 0, "resolved": False}
    
    if not market_info["resolved"]:
        return {
            "condition_id": condition_id,
            "question": market_info.get("question", ""),
            "resolved": False,
            "closed": market_info.get("closed", False),
            "trades_updated": 0,
        }
    
    winning_token_id = market_info["winning_token_id"]
    question = market_info.get("question", "")
    winning_outcome = market_info.get("winning_outcome", "")
    
    updated_count = 0
    trade_results = []
    
    update_errors = []
    
    for trade in all_trades:
        instrument_id = trade.get("instrument_id", "")
        our_token_id = parse_token_id_from_instrument(instrument_id)
        
        if not our_token_id:
            trade_results.append({
                "trade_id": trade["trade_id"],
                "status": "skipped_no_token",
            })
            continue
        
        pnl_info = calculate_actual_pnl(
            entry_price=trade["entry_price"],
            position_size_usd=trade["position_size_usd"],
            our_token_id=our_token_id,
            winning_token_id=winning_token_id,
            side=trade["side"],
        )
        
        if pnl_info["actual_pnl"] is None:
            trade_results.append({
                "trade_id": trade["trade_id"],
                "status": "skipped_bad_price",
            })
            continue
        
        won = pnl_info["won"]
        actual_pnl = pnl_info["actual_pnl"]
        actual_return = pnl_info["actual_return"]
        
        simulated_pnl = trade.get("realized_pnl")
        sim_vs_actual = None
        if simulated_pnl is not None:
            sim_vs_actual = round(actual_pnl - simulated_pnl, 2)
        
        resolution_note = (
            f"{'WIN' if won else 'LOSS'} | "
            f"Market: {winning_outcome} won | "
            f"Actual P&L: ${actual_pnl:+.2f}"
        )
        
        if update_db and conn:
            success = update_trade_with_resolution(
                conn=conn,
                trade_id=trade["trade_id"],
                actual_pnl=actual_pnl,
                actual_return=actual_return,
                resolution_outcome=resolution_note,
                dispute_flag=0,
            )
            if success:
                updated_count += 1
            else:
                update_errors.append(trade["trade_id"])
        
        trade_results.append({
            "trade_id": trade["trade_id"],
            "status": "updated" if update_db else "calculated",
            "won": won,
            "actual_pnl": actual_pnl,
            "actual_return": actual_return,
            "simulated_pnl": simulated_pnl,
            "sim_vs_actual": sim_vs_actual,
            "side": trade["side"],
            "entry_price": trade["entry_price"],
            "position_size_usd": trade["position_size_usd"],
            "our_token_id": our_token_id[:20],
        })
    
    return {
        "condition_id": condition_id,
        "question": question,
        "resolved": True,
        "winning_outcome": winning_outcome,
        "trades_count": len(all_trades),
        "trades_updated": updated_count,
        "update_errors": len(update_errors),
        "trade_results": trade_results,
    }


def get_statistics(conn: sqlite3.Connection) -> dict:
    """Get current stats about trades in the DB."""
    stats = {}
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total_trades,
            COUNT(DISTINCT condition_id) as unique_conditions,
            COUNT(CASE WHEN condition_id IS NOT NULL AND condition_id != '' 
                       AND instrument_id IS NOT NULL AND instrument_id != '' 
                  THEN 1 END) as trackable_conditions,
            SUM(CASE WHEN resolution_outcome IS NOT NULL THEN 1 ELSE 0 END) as already_resolved,
            SUM(realized_pnl) as total_realized_pnl
        FROM trades
    """)
    row = cursor.fetchone()
    if row:
        stats = dict(row)
    
    # By exit reason
    cursor = conn.execute("""
        SELECT exit_reason, COUNT(*) as count,
               ROUND(AVG(realized_pnl), 2) as avg_pnl,
               ROUND(SUM(realized_pnl), 2) as total_pnl
        FROM trades
        GROUP BY exit_reason
        ORDER BY count DESC
    """)
    stats["by_exit_reason"] = [dict(r) for r in cursor.fetchall()]
    
    # By category
    cursor = conn.execute("""
        SELECT category, COUNT(*) as count,
               ROUND(AVG(realized_pnl), 2) as avg_pnl,
               ROUND(SUM(realized_pnl), 2) as total_pnl
        FROM trades
        GROUP BY category
        ORDER BY count DESC
    """)
    stats["by_category"] = [dict(r) for r in cursor.fetchall()]
    
    return stats


def generate_report(results: list[dict], stats: dict) -> str:
    """Generate a human-readable report from resolution results."""
    lines = []
    lines.append("=" * 72)
    lines.append("POLYMARKET RESOLUTION TRACKING REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 72)
    lines.append("")
    
    # Market-level summary
    total_checked = len(results)
    resolved = [r for r in results if r.get("resolved")]
    errors = [r for r in results if r.get("error")]
    unresolved = [r for r in results if not r.get("resolved") and not r.get("error")]
    
    lines.append(f"Markets checked: {total_checked}")
    lines.append(f"  Resolved:     {len(resolved)}")
    lines.append(f"  Not resolved: {len(unresolved)}")
    lines.append(f"  Errors:       {len(errors)}")
    lines.append("")
    
    if stats:
        lines.append(f"Total trades in DB:     {stats.get('total_trades', '?')}")
        lines.append(f"Trackable conditions:   {stats.get('trackable_conditions', '?')}")
        lines.append(f"Already resolved:       {stats.get('already_resolved', 0)}")
        lines.append(f"Total simulated P&L:    ${stats.get('total_realized_pnl', 0):+,.2f}")
        lines.append("")
    
    # Resolved markets detail
    if resolved:
        lines.append("─" * 72)
        lines.append("RESOLVED MARKETS")
        lines.append("─" * 72)
        
        all_trade_results = []
        for r in resolved:
            for tr in r.get("trade_results", []):
                all_trade_results.append(tr)
        
        wins = [t for t in all_trade_results if t.get("won")]
        losses = [t for t in all_trade_results if not t.get("won")]
        
        lines.append(f"Trades matched: {len(all_trade_results)}")
        lines.append(f"  Won:  {len(wins)}  (${sum(t.get('actual_pnl',0) for t in wins):+,.2f})")
        lines.append(f"  Lost: {len(losses)} (${sum(t.get('actual_pnl',0) for t in losses):+,.2f})")
        
        total_actual_pnl = sum(t.get("actual_pnl", 0) for t in all_trade_results)
        total_sim_pnl = sum(t.get("simulated_pnl", 0) for t in all_trade_results if t.get("simulated_pnl") is not None)
        
        lines.append(f"  Total actual P&L: ${total_actual_pnl:+,.2f}")
        lines.append(f"  Total simulated:  ${total_sim_pnl:+,.2f}")
        lines.append(f"  Divergence:       ${total_actual_pnl - total_sim_pnl:+,.2f}")
        lines.append("")
        
        for r in resolved[:20]:
            lines.append(f"  Market: {r.get('question', '')[:60]}")
            lines.append(f"    Winner: {r.get('winning_outcome', '')}")
            lines.append(f"    Trades: {r.get('trades_count', 0)}")
            for tr in r.get("trade_results", []):
                sim = tr.get("simulated_pnl")
                sim_str = f" (sim: ${sim:+.2f})" if sim is not None else ""
                lines.append(f"      {'WIN' if tr.get('won') else 'LOSS'}: "
                           f"${tr.get('actual_pnl', 0):+.2f}{sim_str}")
            lines.append("")
    
    # Unresolved markets
    if unresolved:
        lines.append("─" * 72)
        lines.append(f"UNRESOLVED MARKETS ({len(unresolved)})")
        lines.append("─" * 72)
        for r in unresolved[:10]:
            lines.append(f"  {r.get('condition_id', '')[:20]}... | {r.get('question', '')[:50]}")
        if len(unresolved) > 10:
            lines.append(f"  ... and {len(unresolved) - 10} more")
        lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Polymarket Resolution Tracker")
    parser.add_argument("--condition-id", type=str, help="Check a single condition_id")
    parser.add_argument("--report-only", action="store_true", help="Generate report without writing to DB")
    parser.add_argument("--update-db", action="store_true", help="Write resolution results to DB")
    parser.add_argument("--stats", action="store_true", help="Show DB statistics only")
    args = parser.parse_args()
    
    if not TRADES_DB.exists():
        print(f"[ERROR] trades.db not found at {TRADES_DB}", file=sys.stderr)
        sys.exit(1)
    
    conn = get_trade_db_connection()
    
    # Stats only mode
    if args.stats:
        stats = get_statistics(conn)
        print(json.dumps(stats, indent=2, default=str))
        conn.close()
        return
    
    # Single condition check
    if args.condition_id:
        cid = args.condition_id
        print(f"\nChecking condition_id: {cid[:30]}...")
        trades = fetch_trades_for_condition(conn, cid)
        if not trades:
            print("  No trades found for this condition_id")
        else:
            print(f"  Found {len(trades)} trades")
        
        result = run_resolution_check(
            condition_id=cid,
            all_trades=trades,
            update_db=args.update_db,
            conn=conn,
        )
        
        if args.update_db:
            conn.commit()
            print(f"  Updated {result.get('trades_updated', 0)} trades")
        
        print(json.dumps(result, indent=2, default=str))
        conn.close()
        return
    
    # Full scan mode
    print("Fetching all unique condition_ids from trades.db...")
    all_conditions = fetch_all_condition_ids(conn)
    
    if not all_conditions:
        print("No trackable condition_ids found.")
        conn.close()
        return

    # Load checkpoint to resume from last completed chunk
    checkpoint: dict[str, int] = {}
    if CHECKPOINT_FILE.exists():
        try:
            checkpoint = json.loads(CHECKPOINT_FILE.read_text())
            resumed = checkpoint.get("completed_chunks", 0)
            print(f"[CHECKPOINT] Resuming from chunk {resumed}")
        except Exception:
            checkpoint = {}

    # Group by condition_id, keeping metadata
    condition_map: dict[str, dict] = {}
    for row in all_conditions:
        cid = row["condition_id"]
        if cid not in condition_map:
            condition_map[cid] = {
                "condition_id": cid,
                "market_title": row.get("market_title", ""),
                "category": row.get("category", ""),
            }

    total = len(condition_map)
    print(f"Found {total} unique market condition_ids to check")
    print()

    # Build ordered list of condition_ids
    cid_list = list(condition_map.keys())
    num_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    chunk_errors = 0
    chunk_resolved = 0
    chunk_unresolved = 0

    results = []
    processed = 0
    completed_chunks = checkpoint.get("completed_chunks", 0)

    for chunk_idx in range(num_chunks):
        chunk_start = chunk_idx * CHUNK_SIZE
        chunk_end = min(chunk_start + CHUNK_SIZE, total)
        chunk_cids = cid_list[chunk_start:chunk_end]
        is_first_chunk = chunk_idx == 0

        # Skip chunks already completed (chunk_idx < completed_chunks means fully done)
        # Always run chunk 0 (is_first_chunk) unless checkpoint covers beyond it
        if chunk_idx < completed_chunks:
            continue

        print(f"\n=== Chunk {chunk_idx + 1}/{num_chunks} ({len(chunk_cids)} markets) ===")

        for cid in chunk_cids:
            meta = condition_map[cid]
            processed += 1
            print(f"[{processed}/{total}] Checking {cid[:30]}... ({meta.get('category', '?')})")

            trades = fetch_trades_for_condition(conn, cid)
            result = run_resolution_check(
                condition_id=cid,
                all_trades=trades,
                update_db=args.update_db,
                conn=conn,
            )
            results.append(result)

            # Per-market result summary
            if result.get("error"):
                print(f"  ❌ Error: {result['error']}")
            elif result.get("resolved"):
                print(f"  ✅ RESOLVED: {result.get('winning_outcome', '?')} | "
                      f"{result.get('trades_updated', 0)} trades updated")
            else:
                print(f"  ⏳ Not resolved (closed={result.get('closed', '?')})")

            time.sleep(RATE_LIMIT_SLEEP)

        # Commit after each chunk
        if args.update_db:
            conn.commit()

        # Update checkpoint
        completed_chunks = chunk_idx + 1
        try:
            CHECKPOINT_FILE.write_text(json.dumps({"completed_chunks": completed_chunks}))
        except Exception as e:
            print(f"[WARN] Could not write checkpoint: {e}", file=sys.stderr)

        # Count chunk stats
        chunk_resolved = sum(1 for r in results[chunk_start:chunk_end] if r.get("resolved"))
        chunk_errors = sum(1 for r in results[chunk_start:chunk_end] if r.get("error"))
        chunk_unresolved = sum(
            1 for r in results[chunk_start:chunk_end]
            if not r.get("resolved") and not r.get("error")
        )
        print(f"Chunk {completed_chunks}/{num_chunks}: "
              f"{chunk_resolved} resolved, {chunk_errors} errors, "
              f"{chunk_unresolved} unresolved")

    # Clear checkpoint on successful completion
    if CHECKPOINT_FILE.exists():
        try:
            CHECKPOINT_FILE.unlink()
        except Exception:
            pass

    # Generate report
    stats = get_statistics(conn) if args.update_db else None
    report = generate_report(results, stats)

    report_path = TRADES_DB.parent / f"resolution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_path, "w") as f:
        f.write(report)

    print()
    print("─" * 72)
    print(report)
    print(f"\nReport saved to: {report_path}")

    conn.close()


if __name__ == "__main__":
    main()
