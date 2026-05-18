#!/usr/bin/env python3
"""Real-Time Signal Pattern Monitor — detects whale coordination patterns live.

Instead of tracking specific wallet addresses (which are not discoverable),
this monitors Polymarket for the BEHAVIORAL patterns we identified:
1. Early low-price entries (<$0.30) on new markets
2. Rapid price pumps (>50% in short time)
3. Multiple whales entering same market in succession

Runs every 5 minutes via cron.
"""

import json
import urllib.request
import time
import os
import sqlite3
from datetime import datetime, timezone

TRADES_API = "https://data-api.polymarket.com/trades?limit=100"
MIDPOINT_API = "https://clob.polymarket.com/midpoint?token_id={}"
MARKETS_API = "https://data-api.polymarket.com/markets?limit=100&closed=false&tag=sports"
STATE_FILE = "research/signal_monitor_state.json"
OUTPUT_FILE = "research/signal_detections.json"

# Known wallets (limited but verifiable)
KNOWN_WALLETS = {
    "0x6815040a7176c958e6ff8818bfe188e80dbd9edb": "pilotbaby",
    "0xd106952ebf30a3125affd8a23b6c1f30c35fc79c": "Herdonia",
}

# Signal patterns
LOW_PRICE_THRESHOLD = 0.30  # Entry below this = potential signal
PUMP_THRESHOLD = 0.50  # 50%+ price move from first entry
FOLLOW_WINDOW_SECS = 300  # 5 min between entries = follow pattern
NOISE_KEYWORDS = ["highest temperature", "bitcoin up or down", "ethereum up or down", "solana up or down", "weather"]


def fetch_json(url: str, timeout: int = 10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_scan": 0, "tracked_markets": {}, "detections": []}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def main():
    state = load_state()
    now = int(time.time())
    detections = []
    
    # 1. Scan recent trades for whale patterns
    print(f"[signal_monitor] Scanning trades at {datetime.now(timezone.utc).isoformat()}", flush=True)
    trades = fetch_json(TRADES_API)
    
    if trades:
        # Group trades by condition_id
        markets = {}
        for t in trades:
            cid = t.get("conditionId", "")
            if not cid:
                continue
            if cid not in markets:
                markets[cid] = {
                    "title": t.get("title", "Unknown"),
                    "trades": [],
                    "first_entry": None,
                    "lowest_price": 1.0,
                    "whales": set(),
                }
            markets[cid]["trades"].append(t)
            price = t.get("price", 0.5)
            if price < markets[cid]["lowest_price"]:
                markets[cid]["lowest_price"] = price
            if markets[cid]["first_entry"] is None or t.get("timestamp", 0) < markets[cid]["first_entry"]:
                markets[cid]["first_entry"] = t.get("timestamp", now)
            
            # Check for known wallets
            wallet = t.get("proxyWallet", "")
            if wallet in KNOWN_WALLETS:
                markets[cid]["whales"].add(KNOWN_WALLETS[wallet])
                print(f"  Known whale detected: {KNOWN_WALLETS[wallet]} on {t.get('title','?')[:40]}", flush=True)
        
        # 2. Detect signal patterns — filter out noise
        NOISE_PATTERNS = ["highest temperature", "bitcoin up or down", "ethereum up or down", "solana up or down", "will ", " win on "]
        for cid, m in markets.items():
            title_lower = m["title"].lower()
            if any(p in title_lower for p in NOISE_PATTERNS):
                continue
            if m["lowest_price"] < LOW_PRICE_THRESHOLD:
                # Low price entry detected — potential signal
                age = now - (m["first_entry"] or now)
                
                if age < 3600:  # Within last hour
                    det = {
                        "timestamp": now,
                        "market": m["title"],
                        "condition_id": cid,
                        "lowest_price": m["lowest_price"],
                        "trades_count": len(m["trades"]),
                        "age_seconds": age,
                        "type": "low_entry_signal" if m["lowest_price"] < 0.20 else "moderate_entry",
                        "known_whales": list(m["whales"]),
                    }
                    detections.append(det)
                    print(f"  SIGNAL: {m['title'][:50]} — lowest ${m['lowest_price']:.2f} ({len(m['trades'])} trades)", flush=True)
                    
                    # Track this market for pump detection
                    if cid not in state.get("tracked_markets", {}):
                        state.setdefault("tracked_markets", {})[cid] = {
                            "title": m["title"],
                            "first_seen": now,
                            "lowest_price": m["lowest_price"],
                            "highest_price": m["lowest_price"],
                            "whales": list(m["whales"]),
                        }
    
    # Save detections before pump check loop (early save prevents timeout data loss)
    if detections:
        existing = []
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE) as f:
                    existing = json.load(f)
            except Exception:
                pass
        existing.extend(detections)
        existing = existing[-100:]
        with open(OUTPUT_FILE, "w") as f:
            json.dump(existing, f, indent=2)

    # 3. Check tracked markets for price pumps (max 10 per run to prevent timeout)
    tracked_cids = list(state.get("tracked_markets", {}).keys())[:10]
    for cid in tracked_cids:
        tm = state["tracked_markets"][cid]
        age = now - tm["first_seen"]
        
        # Check midpoint price via CLOB
        # Need token_id — try to derive from condition_id
        # Actually, let's just re-check the trades API
        fresh_trades = fetch_json(f"https://data-api.polymarket.com/trades?conditionId={cid}&limit=50")
        if fresh_trades:
            prices = [t.get("price", 0.5) for t in fresh_trades if t.get("price")]
            if prices:
                current_high = max(prices)
                if current_high > tm.get("highest_price", 0):
                    tm["highest_price"] = current_high
                    pump_pct = (current_high / max(tm["lowest_price"], 0.001) - 1) * 100
                    print(f"  PUMP CHECK {tm['title'][:40]}: lowest=${tm['lowest_price']:.2f} → now=${current_high:.2f} ({pump_pct:+.0f}%)", flush=True)
                    
                    if pump_pct > 50:
                        det = {
                            "timestamp": now,
                            "market": tm["title"],
                            "condition_id": cid,
                            "type": "pump_detected",
                            "entry_price": tm["lowest_price"],
                            "current_price": current_high,
                            "pump_pct": round(pump_pct, 1),
                            "age_hours": round(age / 3600, 1),
                            "known_whales": tm.get("whales", []),
                        }
                        detections.append(det)
                        print(f"  🚨 PUMP DETECTED: {tm['title'][:40]} — {pump_pct:+.0f}% in {age/3600:.1f}h", flush=True)
        
        # Stop tracking after 24h
        if age > 86400:
            del state["tracked_markets"][cid]
    
    # 4. Save pump detections from section 3 (signal detections already saved in early save)
    pump_dets = [d for d in detections if d.get('type') == 'pump_detected']
    if pump_dets:
        existing = []
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE) as f:
                    existing = json.load(f)
            except Exception:
                pass
        existing.extend(pump_dets)
        # Keep last 100
        existing = existing[-100:]
        with open(OUTPUT_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    
    state["last_scan"] = now
    state["scan_count"] = state.get("scan_count", 0) + 1
    save_state(state)
    
    print(f"[signal_monitor] Scan complete. {len(detections)} new detections. Tracking {len(state.get('tracked_markets', {}))} markets.", flush=True)


if __name__ == "__main__":
    main()
