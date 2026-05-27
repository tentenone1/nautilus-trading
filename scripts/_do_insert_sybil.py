#!/usr/bin/env python3
"""Insert signals from fresh JSON into /data/trades.db"""
import sqlite3, json
from datetime import datetime, timezone

json_path = 'research/sybil_intelligence_fresh.json'
db_path = '/data/trades.db'

with open(json_path) as f:
    data = json.load(f)

scan_ts = data['scan_timestamp']
signals = data['signals']
print(f'Scan timestamp: {scan_ts}')
print(f'Signals to insert: {len(signals)}')

conn = sqlite3.connect(db_path, timeout=30)
cur = conn.cursor()

existing = cur.execute(
    'SELECT COUNT(*) FROM sybil_signals WHERE generated_at = ?', (scan_ts,)
).fetchone()[0]
print(f'Existing with same timestamp: {existing}')

inserted = 0
for s in signals:
    direction = s.get('direction', '')
    side = f'BUY {direction}' if direction else 'BUY YES'
    net_exp = abs(s.get('net_exposure', 0)) if 'net_exposure' in s else abs(s.get('total_volume', 0))
    wallet_cnt = s.get('wallet_count', 0)
    reason = s.get('signal', '')
    avg_bet = s.get('avg_bet_size', None)

    cur.execute(
        """INSERT INTO sybil_signals
           (generated_at, signal_type, group_id, market_title, condition_id,
            side, confidence, reason, total_exposure_usd, wallet_count,
            yes_size_usd, no_size_usd, yes_ratio, avg_bet_usd, inserted_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (scan_ts, s.get('type',''), s.get('group_id',''), s.get('market_title',''),
         s.get('condition_id',''), side, None, reason, net_exp, wallet_cnt,
         None, None, None, avg_bet, datetime.now(timezone.utc).isoformat())
    )
    inserted += 1

conn.commit()
total = cur.execute('SELECT COUNT(*) FROM sybil_signals').fetchone()[0]
print(f'Inserted: {inserted}, Total: {total}')
conn.close()