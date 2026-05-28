#!/usr/bin/env python3
"""Market Health Alerts — detect market degradation and send Feishu alerts.

Checks the latest analyzer snapshot against tracked state and fires alerts when:
  - spread > 0.05 for 3 consecutive snapshots
  - volume crashed >90% day-over-day
  - top_holder_pct > 85
  - health changed good → poor

Wired into polymarket_analyzer_bridge.py (runs every 10 minutes).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
import yaml

NAUTILUS_ROOT = Path("/home/elon-1/workspace/nautilus-trading")
STATE_PATH = NAUTILUS_ROOT / "research" / ".health_alert_state.json"
CONFIG_PATH = Path("/home/elon-1/.hermes/config.yaml")
CHAT_ID = "oc_9f3d634000ee97d8c71e0b81f55b1464"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("market_health")


def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    return {"markets": {}, "last_alert_at": None}


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, default=str))


def _get_latest_snapshot() -> Optional[dict]:
    paths = sorted((NAUTILUS_ROOT / "data").glob("polymarket_analyzer_snapshot*.json"), reverse=True)
    if not paths:
        return None
    try:
        return json.loads(paths[0].read_text())
    except Exception:
        return None


def _get_feishu_token() -> Optional[str]:
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        fs = cfg.get("feishu", {})
        app_id = fs.get("app_id", os.environ.get("FEISHU_APP_ID", ""))
        app_secret = fs.get("app_secret", os.environ.get("FEISHU_APP_SECRET", ""))
        if not app_id or not app_secret:
            return None
        r = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": app_secret},
            timeout=10,
        )
        return r.json().get("tenant_access_token")
    except Exception as e:
        logger.warning(f"Failed to get Feishu token: {e}")
        return None


def _send_feishu_alert(title: str, body: str) -> None:
    token = _get_feishu_token()
    if not token:
        logger.warning("No Feishu token — skipping alert")
        return
    card = {
        "config": {"wide_screen_mode": True},
        "elements": [{"tag": "markdown", "content": f"**{title}**\n\n{body}"}],
    }
    payload = {"receive_id": CHAT_ID, "msg_type": "interactive", "content": json.dumps(card)}
    try:
        r = requests.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=10,
        )
        logger.info(f"Feishu alert sent: {r.status_code}")
    except Exception as e:
        logger.warning(f"Failed to send Feishu alert: {e}")


def check_alert_conditions(snapshot_path: Optional[str] = None) -> list[str]:
    """Check latest snapshot against tracked state, fire alerts, update state.

    Called by polymarket_analyzer_bridge.py after each snapshot is written.
    Returns list of alert strings that were triggered.
    """
    snapshot = _get_latest_snapshot()
    if not snapshot:
        logger.info("No analyzer snapshot found — skipping")
        return []

    timestamp = snapshot.get("timestamp", "")
    markets = snapshot.get("market_snapshots", [])
    if not markets:
        logger.info("Snapshot has no markets — skipping")
        return []

    state = _load_state()
    triggered: list[str] = []

    for m in markets:
        cond_id = m.get("condition_id", "unknown")
        spread = float(m.get("spread") or 0)
        volume = float(m.get("volume24hr") or 0)
        holder_pct = float(m.get("top_holder_pct") or 0)
        health = m.get("health", "unknown")
        title = (m.get("market_title") or cond_id or "unknown")[:60]

        prev = state["markets"].setdefault(cond_id, {
            "spread_history": [],
            "prev_volume": None,
            "last_health": None,
            "title": title,
        })
        prev_title = prev.get("title", title)

        # Spread tracking
        spread_hist = prev.get("spread_history", [])
        spread_hist.append(spread)
        spread_hist = spread_hist[-5:]  # keep last 5
        prev["spread_history"] = spread_hist

        if len(spread_hist) >= 3 and all(s > 0.05 for s in spread_hist[-3:]):
            msg = f"Market `{title[:50]}` spread >5% for 3 consecutive snapshots — skip new positions"
            if msg not in triggered:
                triggered.append(msg)
                _send_feishu_alert("Spread Alert", msg)
                logger.warning(f"ALERT: {msg}")

        # Volume crash detection
        prev_vol = prev.get("prev_volume")
        if prev_vol is not None and prev_vol > 10000 and volume < prev_vol * 0.1:
            msg = f"Market `{title[:50]}` volume crashed {((1 - volume/max(prev_vol,1)) * 100):.0f}% (was ${prev_vol:,.0f}, now ${volume:,.0f})"
            if msg not in triggered:
                triggered.append(msg)
                _send_feishu_alert("Volume Crash Alert", msg)
                logger.warning(f"ALERT: {msg}")
        prev["prev_volume"] = volume

        # Holder concentration
        if holder_pct > 85:
            msg = f"Market `{title[:50]}` top holder {holder_pct:.0f}% — high manipulation risk"
            if msg not in triggered:
                triggered.append(msg)
                _send_feishu_alert("Concentration Alert", msg)
                logger.warning(f"ALERT: {msg}")

        # Health change: good → poor
        last_health = prev.get("last_health")
        if last_health == "good" and health == "poor":
            msg = f"Market `{title[:50]}` health changed good→poor — review positions"
            if msg not in triggered:
                triggered.append(msg)
                _send_feishu_alert("Health Degradation Alert", msg)
                logger.warning(f"ALERT: {msg}")

        prev["last_health"] = health

    state["markets"] = {k: v for k, v in state["markets"].items() if k in {m.get("condition_id") for m in markets}}
    state["last_check"] = timestamp
    _save_state(state)

    return triggered


def main() -> None:
    logger.info("=== Market Health Alert Check ===")
    snapshot = _get_latest_snapshot()
    if snapshot:
        logger.info(f"Using snapshot: {NAUTILUS_ROOT / 'data' / 'polymarket_analyzer_snapshot.json'}")
    alerts = check_alert_conditions()
    if alerts:
        logger.info(f"Triggered alerts: {alerts}")
    else:
        logger.info("No alerts triggered")
    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
