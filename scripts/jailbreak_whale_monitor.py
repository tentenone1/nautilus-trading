#!/usr/bin/env python3
"""Jailbreak Whale Monitor — tracks COPY/FADE whale trades from jailbreak analysis.

Dynamically loads whales from jailbreak_analysis.json signals, fetches recent trades
via async HTTP, and outputs actionable alerts with confidence-weighted priority.

Output: ~/workspace/nautilus-trading/research/jailbreak_whale_alerts.json
Schedule: every 30 minutes
"""

import asyncio
import json
import os
import httpx
from datetime import datetime, timezone
from typing import Any

# ── Configuration ────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(BASE_DIR, "research", "jailbreak_whale_state.json")
ALERTS_FILE = os.path.join(BASE_DIR, "research", "jailbreak_whale_alerts.json")
ANALYSIS_FILE = os.path.join(BASE_DIR, "research", "jailbreak_analysis.json")

API_BASE = "https://data-api.polymarket.com/v1"
API_TIMEOUT = 10.0
RATE_LIMIT_DELAY = 0.2
LOOKBACK_HOURS = 6
MIN_CONFIDENCE = 0.6

# ── Fallback Hardcoded Whales (used if analysis file missing) ────────────────
FALLBACK_COPY_WHALES = {
    "RJW1": "0x85f031d069de300055900c4055c1baeb6bde3f67",
    "surfandturf": "0x9f2fe025f84839ca81dd8e0338892605702d2ca8",
    "matanovik": "0x39d3c773be30fcc73161fc6768f46d563a779ef0",
    "p150-0xba389f": "0xba389f76b0119aed07c53c9029852664bd97e406",
    "pilotbaby": "0x6815040a7176c958e6ff8818bfe188e80dbd9edb",
    "Countryside": "0xbddf61af533ff524d27154e589d2d7a81510c684",
}

FALLBACK_FADE_WHALES = {
    "asdfjh": "0x0eb568f307e9a48af2c3e688ad6074236712c494",
    "SMCAOMCRL": "0x3b5c629f114098b0dee345fb78b7a3a013c7126e",
    "benwyatt": "0x1117eade222413335b7ec959e5b48c1d3dbc3532",
    "JPMorgan101": "0xb6d6e99d3bfe055874a04279f659f009fd57be17",
    "bossoskil1": "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a",
    "trade-via-Gravia": "0xe48109602719f95c247fec255ffb71bab3f985a3",
}


# ── Dynamic Whale Loading ──────────────────────────────────────────────────────
def get_wallet_for_whale(whale_name: str) -> str | None:
    """Lookup wallet address for a whale name.

    Sources (in order):
    1. Fallback COPY whales dict
    2. Fallback FADE whales dict
    3. trades.db (whale_name column)
    """
    # Check fallback dicts first
    if whale_name in FALLBACK_COPY_WHALES:
        return FALLBACK_COPY_WHALES[whale_name]
    if whale_name in FALLBACK_FADE_WHALES:
        return FALLBACK_FADE_WHALES[whale_name]

    # Fallback: query trades.db for wallet
    try:
        import sqlite3
        db_path = os.path.join(BASE_DIR, "research", "trades.db")
        if os.path.exists(db_path):
            db = sqlite3.connect(db_path)
            result = db.execute(
                "SELECT DISTINCT whale_address FROM trades WHERE whale_name = ? AND whale_address IS NOT NULL AND whale_address != '' LIMIT 1",
                (whale_name,)
            ).fetchone()
            db.close()
            if result and result[0]:
                return result[0]
    except Exception:
        pass

    return None


def load_signals_from_analysis() -> tuple[dict[str, str], dict[str, tuple[str, float]]]:
    """Load COPY/FADE whales from jailbreak_analysis.json signals.

    Returns:
        (copy_whales, fade_whales) where:
        - copy_whales: {name: wallet}
        - fade_whales: {name: (wallet, confidence)}
    """
    if not os.path.exists(ANALYSIS_FILE):
        return {}, {}

    with open(ANALYSIS_FILE) as f:
        data = json.load(f)

    copy_whales: dict[str, str] = {}
    fade_whales: dict[str, tuple[str, float]] = {}

    seen_whales: set[str] = set()  # Track seen whales to avoid duplicates

    for signal in data.get("signals", []):
        whale_name = signal.get("whale")
        action = signal.get("action")
        confidence = float(signal.get("confidence", 0))

        # Skip duplicates (LLM sometimes outputs same whale twice)
        if whale_name in seen_whales:
            continue

        # Lookup wallet
        wallet = get_wallet_for_whale(whale_name)

        if wallet and confidence >= MIN_CONFIDENCE:
            seen_whales.add(whale_name)
            if action == "COPY":
                copy_whales[whale_name] = wallet
            elif action == "FADE":
                fade_whales[whale_name] = (wallet, confidence)

    return copy_whales, fade_whales


def get_active_whales() -> tuple[dict[str, str], dict[str, tuple[str, float]]]:
    """Get active whales with fallback to hardcoded lists."""
    copy_whales, fade_whales = load_signals_from_analysis()

    # Fallback if no dynamic signals found
    if not copy_whales:
        copy_whales = FALLBACK_COPY_WHALES.copy()
        print("[jailbreak-monitor] Using fallback COPY whales (no analysis signals)", flush=True)

    if not fade_whales:
        # Fallback FADE whales get default confidence 0.7
        fade_whales = {k: (v, 0.7) for k, v in FALLBACK_FADE_WHALES.items()}
        print("[jailbreak-monitor] Using fallback FADE whales (no analysis signals)", flush=True)

    return copy_whales, fade_whales


# ── Async HTTP Fetching ────────────────────────────────────────────────────────
async def fetch_trades_async(client: httpx.AsyncClient, wallet: str, limit: int = 10) -> list[dict]:
    """Fetch trades asynchronously with timeout and error handling."""
    url = f"{API_BASE}/trades?user={wallet}&limit={limit}"
    try:
        resp = await client.get(url, timeout=API_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            return []  # Rate limited, retry next cycle
        return [{"error": f"HTTP {e.response.status_code}"}]
    except httpx.TimeoutException:
        return [{"error": "timeout"}]
    except Exception as e:
        return [{"error": str(e)}]


async def fetch_whale_trades(
    client: httpx.AsyncClient,
    whales: dict[str, Any],
    is_fade: bool = False,
) -> dict[str, list[dict]]:
    """Fetch all whale trades with rate limiting."""
    results: dict[str, list[dict]] = {}

    for name, wallet_data in whales.items():
        wallet = wallet_data if isinstance(wallet_data, str) else wallet_data[0]
        results[name] = await fetch_trades_async(client, wallet)
        await asyncio.sleep(RATE_LIMIT_DELAY)

    return results


# ── State Management ────────────────────────────────────────────────────────────
def load_state() -> dict[str, int]:
    """Load last-seen trade timestamps."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict[str, int]) -> None:
    """Persist last-seen timestamps."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def timestamp_to_iso(ts: int) -> str:
    """Convert Unix timestamp to ISO string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


# ── Alert Processing ────────────────────────────────────────────────────────────
def process_new_trades(
    trades: list[dict],
    last_seen: int,
    now_ts: float,
) -> list[dict]:
    """Extract new trades from API response."""
    new_trades: list[dict] = []

    for t in trades:
        if isinstance(t, dict) and "error" in t:
            continue

        ts = int(t.get("timestamp", 0))
        if ts > last_seen and (now_ts - ts) < LOOKBACK_HOURS * 3600:
            new_trades.append({
                "title": t.get("title", "Unknown"),
                "slug": t.get("slug", ""),
                "side": t.get("side", ""),
                "price": float(t.get("price", 0)),
                "size": float(t.get("size", 0)),
                "usd_value": round(float(t.get("size", 0)) * float(t.get("price", 0)), 2),
                "timestamp": timestamp_to_iso(ts),
                "ts_unix": ts,
            })

    return new_trades


def get_priority(confidence: float) -> str:
    """Get priority label from confidence."""
    if confidence >= 0.8:
        return "HIGH"
    elif confidence >= 0.6:
        return "MEDIUM"
    return "LOW"


# ── Main Execution ──────────────────────────────────────────────────────────────
async def main_async() -> dict[str, Any]:
    """Async main function."""
    copy_whales, fade_whales = get_active_whales()
    state = load_state()
    now_ts = asyncio.get_event_loop().time()

    # Use actual timestamp for lookback
    now_real = datetime.now(timezone.utc).timestamp()

    alerts = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "copy_signals": [],
        "fade_signals": [],
        "whales_tracked": {
            "copy": len(copy_whales),
            "fade": len(fade_whales),
        },
    }

    async with httpx.AsyncClient() as client:
        # Fetch COPY whale trades
        copy_results = await fetch_whale_trades(client, copy_whales, is_fade=False)

        for name, trades in copy_results.items():
            if trades and "error" not in trades[0]:
                last_seen = state.get(name, 0)
                new_trades = process_new_trades(trades, last_seen, now_real)

                if new_trades:
                    max_ts = max(t["ts_unix"] for t in new_trades)
                    state[name] = max_ts

                    # Get confidence (COPY whales use 0.85 default if not in fade dict)
                    confidence = fade_whales.get(name, (None, 0.85))[1] if name in fade_whales else 0.85

                    alerts["copy_signals"].append({
                        "whale": name,
                        "wallet": copy_whales[name],
                        "type": "COPY",
                        "confidence": confidence,
                        "priority": get_priority(confidence),
                        "trades": new_trades,
                    })

        # Fetch FADE whale trades
        fade_results = await fetch_whale_trades(client, fade_whales, is_fade=True)

        for name, trades in fade_results.items():
            if trades and "error" not in trades[0]:
                last_seen = state.get(name, 0)
                new_trades = process_new_trades(trades, last_seen, now_real)

                if new_trades:
                    max_ts = max(t["ts_unix"] for t in new_trades)
                    state[name] = max_ts

                    wallet, confidence = fade_whales[name]

                    alerts["fade_signals"].append({
                        "whale": name,
                        "wallet": wallet,
                        "type": "FADE",
                        "confidence": confidence,
                        "priority": get_priority(confidence),
                        "trades": new_trades,
                    })

    save_state(state)

    # Write alerts file
    os.makedirs(os.path.dirname(ALERTS_FILE), exist_ok=True)
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)

    return alerts


def main() -> dict[str, Any]:
    """Entry point."""
    alerts = asyncio.run(main_async())

    # Print summary
    copy_count = len(alerts["copy_signals"])
    fade_count = len(alerts["fade_signals"])
    total_new = sum(len(s["trades"]) for s in alerts["copy_signals"]) + \
                sum(len(s["trades"]) for s in alerts["fade_signals"])

    tracked = alerts.get("whales_tracked", {})
    print(f"[jailbreak-whale-monitor] {datetime.now(timezone.utc).strftime('%H:%M UTC')}")
    print(f"  COPY whales tracked: {tracked.get('copy', 0)}")
    print(f"  FADE whales tracked: {tracked.get('fade', 0)}")
    print(f"  COPY whales with new trades: {copy_count}")
    print(f"  FADE whales with new trades: {fade_count}")
    print(f"  Total new trades detected: {total_new}")

    for s in alerts["copy_signals"]:
        total_usd = sum(t["usd_value"] for t in s["trades"])
        priority_icon = "🔴" if s["priority"] == "HIGH" else ("🟡" if s["priority"] == "MEDIUM" else "⚪")
        print(f"  {priority_icon} COPY {s['whale']} (conf={s['confidence']:.0%}): ${total_usd:.0f} across {len(s['trades'])} trades")
        for t in s["trades"]:
            print(f"      {t['side']} ${t['usd_value']:.0f} {t['title'][:40]} @ ${t['price']:.4f}")

    for s in alerts["fade_signals"]:
        total_usd = sum(t["usd_value"] for t in s["trades"])
        priority_icon = "🔴" if s["priority"] == "HIGH" else ("🟡" if s["priority"] == "MEDIUM" else "⚪")
        print(f"  {priority_icon} FADE {s['whale']} (conf={s['confidence']:.0%}): ${total_usd:.0f} across {len(s['trades'])} trades")
        for t in s["trades"]:
            print(f"      {t['side']} ${t['usd_value']:.0f} {t['title'][:40]} @ ${t['price']:.4f}")

    return alerts


if __name__ == "__main__":
    main()