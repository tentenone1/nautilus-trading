#!/usr/bin/env python3
"""Autoresearch Bridge Logic for Nautilus Trading System."""

import sqlite3
import json
import time
from datetime import datetime
from pathlib import Path

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
BRIDGE_CONFIG = Path("/home/elon-1/workspace/nautilus-trading/autoresearch-bridge/bridge_config.yaml")
OUTPUT_DIR = Path("/home/elon-1/workspace/nautilus-trading/autoresearch-bridge/output")

def load_config():
    """Load bridge configuration."""
    import yaml
    with open(BRIDGE_CONFIG) as f:
        return yaml.safe_load(f)

def fetch_trades_last_5min():
    """Fetch trades from last 5 minutes."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT trade_id, timestamp, market_title, condition_id, side,
               entry_price, exit_price, position_size_usd, realized_pnl,
               realized_return, resolution_outcome, category, instrument_id
        FROM trades
        WHERE timestamp >= datetime('now', '-5 minutes')
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

def transform_for_autoresearch(trades):
    """Transform trades to Autoresearch format."""
    return [
        {
            "trade_id": t[0],
            "timestamp": t[1],
            "market_title": t[2],
            "condition_id": t[3],
            "side": t[4],
            "entry_price": t[5],
            "exit_price": t[6],
            "position_size_usd": t[7],
            "realized_pnl": t[8],
            "realized_return": t[9],
            "resolution_outcome": t[10],
            "category": t[11],
            "instrument_id": t[12],
        }
        for t in trades
    ]

def main():
    """Run Autoresearch bridge logic."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    config = load_config()
    
    trades = fetch_trades_last_5min()
    transformed = transform_for_autoresearch(trades)
    
    output_file = OUTPUT_DIR / f"bridge_data_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(transformed, f, indent=2)
    
    print(f"Autoresearch bridge complete: {len(transformed)} trades -> {output_file}")

if __name__ == "__main__":
    main()