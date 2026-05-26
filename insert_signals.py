#!/usr/bin/env python3
import json
import sqlite3
from datetime import datetime, timezone

def insert_signals():
    # Read the signal queue
    with open('/home/elon-1/workspace/nautilus-trading/research/sybil_signal_queue.json', 'r') as f:
        data = json.load(f)
    
    # Connect to the database
    conn = sqlite3.connect('/data/trades.db')
    cursor = conn.cursor()
    
    # Insert each signal
    for signal in data['signals']:
        signal_json = json.dumps(signal)
        cursor.execute(
            'INSERT INTO signals (signal_json) VALUES (?)',
            (signal_json,)
        )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    print(f"Inserted {len(data['signals'])} signals into /data/trades.db")

if __name__ == '__main__':
    insert_signals()