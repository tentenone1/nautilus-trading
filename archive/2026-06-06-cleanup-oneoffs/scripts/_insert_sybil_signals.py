#!/usr/bin/env python3
"""
Insert sybil signals from sybil_intelligence.json into trades.db.

Handles two JSON formats:
  1. Standalone: {scan_timestamp, groups: [...], signals: [...], summary: {...}}
  2. Wrapper:    {timestamp, signals: {...}, positions: {...}, meta_whale_groups: {...}}
"""
import json
import sqlite3
from datetime import datetime, timezone

DB_PATH = "/home/elon-1/workspace/nautilus-trading/research/trades.db"
JSON_PATH = "/home/elon-1/workspace/nautilus-trading/research/sybil_intelligence.json"

with open(JSON_PATH) as f:
    data = json.load(f)

# Detect format
if "scan_timestamp" in data:
    # Standalone intelligence format
    scan_ts = data["scan_timestamp"]
    signals = data["signals"]
elif "meta_whale_groups" in data and isinstance(data.get("signals"), dict):
    # Wrapper format — signals are empty, no trade signals to insert
    print("Wrapper format detected (no trade-group signals). Skipping insert.")
    conn = sqlite3.connect(DB_PATH)
    cnt = conn.execute("SELECT COUNT(*) FROM sybil_signals").fetchone()[0]
    print(f"Total sybil_signals in DB: {cnt}")
    conn.close()
    exit(0)
else:
    # Unknown format
    print(f"Unknown JSON format. Keys: {list(data.keys())}")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Check if these signals already exist (same generated_at)
existing = cur.execute(
    "SELECT COUNT(*) FROM sybil_signals WHERE generated_at = ?", (scan_ts,)
).fetchone()[0]

if existing > 0:
    print(f"Signals with generated_at={scan_ts} already present ({existing} rows). Skipping insert.")
else:
    inserted = 0
    for s in signals:
        direction = s.get("direction", "")
        side = f"BUY {direction}" if direction else "BUY YES"

        net_exp = abs(s.get("net_exposure", 0)) if "net_exposure" in s else abs(s.get("total_volume", 0))
        wallet_cnt = s.get("wallet_count", s.get("walletCount", 0))
        reason = s.get("signal", "")
        avg_bet = s.get("avg_bet_size", None)

        cur.execute(
            """INSERT INTO sybil_signals
               (generated_at, signal_type, group_id, market_title, condition_id,
                side, confidence, reason, total_exposure_usd, wallet_count,
                yes_size_usd, no_size_usd, yes_ratio, avg_bet_usd)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                scan_ts,
                s["type"],
                s["group_id"],
                s["market_title"],
                s["condition_id"],
                side,
                None,  # confidence
                reason,
                net_exp,
                wallet_cnt,
                None,  # yes_size_usd
                None,  # no_size_usd
                None,  # yes_ratio
                avg_bet,
            ),
        )
        inserted += 1

    conn.commit()
    print(f"Inserted {inserted} new sybil signals from scan {scan_ts}")

# Summary
cnt = cur.execute("SELECT COUNT(*) FROM sybil_signals").fetchone()[0]
print(f"Total sybil_signals in DB: {cnt}")
conn.close()
