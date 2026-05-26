#!/usr/bin/env python3
"""Load sybil signals from JSON output into trades.db"""
import json
import sqlite3
from datetime import datetime, timezone

def load_signals(json_path: str, db_path: str):
    with open(json_path) as f:
        data = json.load(f)

    signals = data.get("signals", [])
    if not signals:
        print("No signals found in JSON")
        return 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    inserted = 0
    now = datetime.now(timezone.utc).isoformat()

    for sig in signals:
        direction = sig.get("direction", "NO").upper()
        side = f"BUY YES" if direction == "YES" else f"BUY NO"

        # Compute reasonable confidence from wallet count
        wallet_count = sig.get("wallet_count", 1)
        confidence = min(0.5 + wallet_count * 0.1, 0.99)

        # Parse exposure
        net = abs(sig.get("net_exposure", 0))
        yes_size = net if direction == "YES" else 0
        no_size = net if direction == "NO" else 0
        yes_ratio = 1.0 if direction == "YES" else 0.0
        avg_bet = net / wallet_count if wallet_count > 0 else net

        cur.execute("""
            INSERT INTO sybil_signals (
                generated_at, signal_type, group_id, market_title, condition_id,
                side, confidence, reason, total_exposure_usd, wallet_count,
                yes_size_usd, no_size_usd, yes_ratio, avg_bet_usd, inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("scan_timestamp", now),
            sig.get("type", "unknown"),
            sig.get("group_id", ""),
            sig.get("market_title", ""),
            sig.get("condition_id", ""),
            side,
            confidence,
            sig.get("signal", ""),
            net,
            wallet_count,
            yes_size,
            no_size,
            yes_ratio,
            avg_bet,
            now
        ))
        inserted += 1

    conn.commit()
    print(f"Inserted {inserted} signals into {db_path}")

    # Verify
    cur.execute("SELECT COUNT(*) FROM sybil_signals")
    print(f"Total sybil_signals rows: {cur.fetchone()[0]}")

    conn.close()
    return inserted

if __name__ == "__main__":
    import sys
    json_path = sys.argv[1] if len(sys.argv) > 1 else "research/sybil_intelligence.json"
    db_path = sys.argv[2] if len(sys.argv) > 2 else "data/trades.db"
    load_signals(json_path, db_path)
