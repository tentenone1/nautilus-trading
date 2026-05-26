#!/usr/bin/env python3
"""G5: Heartbeat Alert Cron — runs every 5 minutes to monitor system health.

Writes a one-line status summary to stdout and logs critical alerts to logs/heartbeat_alert.log.

Cron wiring:
    */5 * * * * cd ~/workspace/nautilus-trading && venv/bin/python scripts/heartbeat_alerter.py
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
NAUTILUS_ROOT = Path(__file__).parent.parent
HEARTBEAT_FILE = NAUTILUS_ROOT / ".heartbeat.json"
TRADES_DB = NAUTILUS_ROOT / "data" / "trades.db"
LOG_FILE = NAUTILUS_ROOT / "logs" / "heartbeat_alert.log"
OPEN_POSITIONS_FILE = NAUTILUS_ROOT / "data" / "open_positions.json"

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("heartbeat_alerter")


def _read_heartbeat() -> dict:
    """Read .heartbeat.json. Returns empty dict if missing or parse error."""
    if not HEARTBEAT_FILE.exists():
        return {}
    try:
        with open(HEARTBEAT_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _get_open_position_count() -> int:
    """Count open positions from persistence file."""
    try:
        if OPEN_POSITIONS_FILE.exists():
            with open(OPEN_POSITIONS_FILE) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return len(data.get("positions", data))
                return len(data)
    except Exception:
        pass
    return 0


def _get_48h_pnl() -> float:
    """Get 48h realized P&L from trades.db."""
    try:
        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM trades WHERE "
            "exit_time > datetime('now', '-48 hours') AND realized_pnl IS NOT NULL"
        ).fetchone()
        conn.close()
        return float(row[0]) if row else 0.0
    except Exception:
        return 0.0


def _get_last_trade_age_minutes() -> float | None:
    """Age of most recent trade in minutes, or None if no trades."""
    try:
        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT MAX(timestamp) FROM trades WHERE timestamp IS NOT NULL"
        ).fetchone()
        conn.close()
        if row and row[0]:
            trade_time = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - trade_time).total_seconds() / 60
            return round(age, 1)
        return None
    except Exception:
        return None


def _format_pnl(pnl: float) -> str:
    sign = "+" if pnl >= 0 else ""
    return f"{sign}${pnl:.2f}"


def main():
    heartbeat = _read_heartbeat()
    status = heartbeat.get("status", "unknown")
    message = heartbeat.get("message", "")
    open_count = _get_open_position_count()
    pnl_48h = _get_48h_pnl()
    last_trade_age = _get_last_trade_age_minutes()
    max_pos = 50  # from wf_constants MAX_OPEN_POSITIONS default

    # Build one-line summary
    age_str = f"{last_trade_age:.0f}m" if last_trade_age is not None else "none"
    summary = (
        f"HEARTBEAT: {status} | "
        f"open={open_count}/{max_pos} | "
        f"pnl_48h={_format_pnl(pnl_48h)} | "
        f"last_trade={age_str} | "
        f"msg={message[:60]!r}"
    )

    # Always print to stdout for cron capture
    print(summary)

    # Log critical alerts to heartbeat_alert.log
    if status == "critical":
        log.error(f"CRITICAL: {message}")
    elif status == "degraded":
        log.warning(f"DEGRADED: {message}")
    elif status == "unknown":
        log.info(f"UNKNOWN heartbeat state")

    # Additional monitoring: flag if 48h P&L is deeply negative
    if pnl_48h < -100:
        log.warning(f"NEGATIVE_48H_PNL: pnl_48h={_format_pnl(pnl_48h)}")

    # Flag if no trades in >2 hours
    if last_trade_age is not None and last_trade_age > 120:
        log.warning(f"STALE_TRADING: last trade {last_trade_age:.0f}m ago")

    # Flag if open positions approaching cap
    if open_count >= max_pos * 0.8:
        log.warning(f"POSITION_CAP_NEAR: {open_count}/{max_pos}")

    # ── Autoresearch health check (from heartbeat JSON written by silence alerter) ──
    ar_health = heartbeat.get('autoresearch_health', {})
    if ar_health:
        ar_status = ar_health.get('status', 'unknown')
        ar_trades_4h = ar_health.get('trade_count_4h', 0)
        ar_trades_24h = ar_health.get('trade_count_24h', 0)
        ar_pnl_24h = ar_health.get('pnl_24h', 0.0)
        ar_last_ts = ar_health.get('last_trade_ts', 'NEVER')
        ar_checked = ar_health.get('checked_at', '')

        if ar_status == 'CRITICAL':
            log.error(
                f"AUTORESEARCH_CRITICAL | 0 trades in 4h | "
                f"24h: {ar_trades_24h} trades ${ar_pnl_24h:.2f} | "
                f"last: {ar_last_ts} | checked: {ar_checked}"
            )
        elif ar_status == 'WARNING':
            log.warning(
                f"AUTORESEARCH_WARNING | {ar_trades_4h} trades in 4h | "
                f"24h: {ar_trades_24h} trades ${ar_pnl_24h:.2f} | "
                f"last: {ar_last_ts}"
            )
    else:
        # Silence alerter hasn't run yet or heartbeat missing
        log.warning("AUTORESEARCH_HEALTH_MISSING: no autoresearch_health in heartbeat")


if __name__ == "__main__":
    main()
