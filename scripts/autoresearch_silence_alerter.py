#!/usr/bin/env python3
"""
Autoresearch silence detector — fires alert if model_insider generates
zero trades in any rolling 4-hour window.

Cron: */15 * * * * cd /home/elon-1/workspace/nautilus-trading && ./venv/bin/python scripts/autoresearch_silence_alerter.py >> logs/autoresearch_silence_cron.log 2>&1
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
HEARTBEAT_PATH = Path("/home/elon-1/workspace/nautilus-trading/.heartbeat.json")
ALERT_LOG = Path("/home/elon-1/workspace/nautilus-trading/logs/autoresearch_silence.log")

THRESHOLD_HOURS = 4
MIN_TRADES_IN_WINDOW = 1  # must have at least 1 trade in 4 hours


def check_autoresearch_activity():
    """Returns (trade_count_in_last_4h, last_trade_timestamp, count_24h, pnl_24h) for model_insider."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*), MAX(timestamp) FROM trades
        WHERE signal_source IN ('model_insider', 'autoresearch_llm')
          AND timestamp > datetime('now', '-4 hours')
    """)
    count, last_ts = cursor.fetchone()

    # Also check last 24h for trend
    cursor.execute("""
        SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0) FROM trades
        WHERE signal_source IN ('model_insider', 'autoresearch_llm')
          AND timestamp > datetime('now', '-24 hours')
    """)
    count_24h, pnl_24h = cursor.fetchone()

    conn.close()
    return (count or 0), last_ts, (count_24h or 0), (pnl_24h or 0.0)


def update_heartbeat(trade_count_4h: int, trade_count_24h: int, pnl_24h: float, last_trade_ts: str | None) -> None:
    """Update heartbeat with autoresearch health metrics."""
    heartbeat = {}
    if HEARTBEAT_PATH.exists():
        try:
            heartbeat = json.loads(HEARTBEAT_PATH.read_text())
        except Exception:
            heartbeat = {}

    if 'autoresearch_health' not in heartbeat:
        heartbeat['autoresearch_health'] = {}

    status = 'CRITICAL' if trade_count_4h == 0 else ('WARNING' if trade_count_4h < 3 else 'HEALTHY')
    heartbeat['autoresearch_health'].update({
        'trade_count_4h': trade_count_4h,
        'trade_count_24h': trade_count_24h,
        'pnl_24h': round(pnl_24h, 2),
        'last_trade_ts': last_trade_ts,
        'status': status,
        'checked_at': datetime.utcnow().isoformat(),
    })
    HEARTBEAT_PATH.write_text(json.dumps(heartbeat, indent=2))


def main():
    count_4h, last_ts, count_24h, pnl_24h = check_autoresearch_activity()

    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

    if count_4h == 0:
        alert_msg = (
            f"[{timestamp}] AUTORESEARCH_SILENCE_ALERT | "
            f"0 model_insider trades in last 4 hours | "
            f"24h: {count_24h} trades, ${pnl_24h:.2f} PnL | "
            f"last_trade: {last_ts or 'NEVER'}"
        )
        ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(ALERT_LOG, 'a') as f:
            f.write(alert_msg + '\n')
        print(alert_msg)  # for cron output / monitoring
    else:
        print(
            f"[{timestamp}] AUTORESEARCH_OK | "
            f"4h: {count_4h} trades | "
            f"24h: {count_24h} trades, ${pnl_24h:.2f} PnL | "
            f"last_trade: {last_ts or 'NEVER'}"
        )

    update_heartbeat(count_4h, count_24h, pnl_24h, last_ts)


if __name__ == '__main__':
    main()
