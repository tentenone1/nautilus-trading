#!/usr/bin/env python3
"""Polymarket Analyzer Bridge — Integrates polymarket-analyzer TUI data into nautilus.

This script runs the polymarket-analyzer in snapshot mode to capture current
market data for MULTIPLE markets (top-N by volume), then writes it to
nautilus-trading data files for use by the signal pipeline.

Output: data/polymarket_analyzer_snapshot.json
  - market_snapshots[]: top-N markets with full orderbook depth, spread, volume
  - target: >=5 markets, file size <500KB
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
NAUTILUS_ROOT = SCRIPT_DIR.parent
ANALYZER_ROOT = Path("/home/elon-1/projects/polymarket-analyzer")
OUTPUT_PATH = NAUTILUS_ROOT / "data" / "polymarket_analyzer_snapshot.json"

BUN_PATH = Path("/home/elon-1/.bun/bin/bun")

# Config
DEFAULT_LIMIT = 10  # Number of top markets to snapshot
MAX_FILE_SIZE_KB = 500
SNAPSHOT_TIMEOUT_SECS = 60
LIST_TIMEOUT_SECS = 30


def _run_bun(cmd: list[str], cwd: str, timeout: int) -> tuple[str, str, int]:
    """Run a bun command, return (stdout, stderr, returncode)."""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "timeout", -1
    except FileNotFoundError:
        return "", f"bun not found at {BUN_PATH}", -2


def list_markets(limit: int = 50) -> list[dict]:
    """List current Polymarket markets using the analyzer.

    Returns list of market dicts sorted by volume (highest first).

    Each dict: {condition_id, slug, question, volume24hr, best_bid, best_ask, spread}
    """
    cmd = [
        str(BUN_PATH), "run",
        str(ANALYZER_ROOT / "src/index.ts"),
        "--list-markets", "--limit", str(limit), "--json",
    ]
    stdout, stderr, rc = _run_bun(cmd, str(ANALYZER_ROOT), LIST_TIMEOUT_SECS)

    if rc != 0:
        print(f"List markets failed: {stderr[:300]}", file=sys.stderr)
        return []

    output = stdout.strip()
    if not output or output == "[]" or output == "{}":
        return []

    try:
        data = json.loads(output)
    except json.JSONDecodeError:
        return _parse_text_list(stdout)

    if isinstance(data, dict):
        # Single market dict
        return [_market_from_dict(data)]
    elif isinstance(data, list):
        markets = [_market_from_dict(m) for m in data if isinstance(m, dict)]
        # Sort by volume descending
        markets.sort(key=lambda m: m.get("volume24hr", 0) or 0, reverse=True)
        return markets
    return []


def _market_from_dict(d: dict) -> dict:
    """Normalize a market dict from analyzer output."""
    vol = d.get("volume24hr") or d.get("volume24HR") or d.get("volume") or 0
    try:
        vol = float(vol)
    except (ValueError, TypeError):
        vol = 0.0

    best_bid = d.get("bestBid") or d.get("best_bid") or 0
    best_ask = d.get("bestAsk") or d.get("best_ask") or 0
    try:
        best_bid = float(best_bid)
        best_ask = float(best_ask)
    except (ValueError, TypeError):
        best_bid = 0.0
        best_ask = 0.0

    spread = round(best_ask - best_bid, 4) if best_ask and best_bid else 0.0

    return {
        "condition_id": d.get("conditionId") or d.get("condition_id") or "",
        "slug": d.get("slug") or "",
        "question": d.get("question") or d.get("eventTitle") or "",
        "volume24hr": vol,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        # holders / depth — available in snapshot mode only
        "top_holder_pct": None,
        "orderbook_depth": None,
    }


def _parse_text_list(output: str) -> list[dict]:
    """Parse numbered list output format (fallback)."""
    markets = []
    for line in output.split("\n"):
        line = line.strip()
        if " | " in line:
            parts = line.split(" | ")
            if len(parts) >= 3:
                markets.append({
                    "condition_id": parts[-1].strip(),
                    "question": parts[1].strip(),
                    "volume24hr": 0.0,
                    "best_bid": 0.0,
                    "best_ask": 0.0,
                    "spread": 0.0,
                })
    return markets


def fetch_snapshot_for_market(market_slug: str, limit: int = 1) -> dict | None:
    """Fetch deep snapshot for a single market (condition_id or slug)."""
    cmd = [
        str(BUN_PATH), "run",
        str(ANALYZER_ROOT / "src/index.ts"),
        "--once", "--limit", str(limit),
    ]
    # Try slug first, then condition_id
    if market_slug:
        cmd.extend(["--slug", market_slug])

    stdout, stderr, rc = _run_bun(cmd, str(ANALYZER_ROOT), SNAPSHOT_TIMEOUT_SECS)

    if rc != 0:
        print(f"Snapshot for {market_slug} failed: {stderr[:200]}", file=sys.stderr)
        return None

    output = stdout.strip()
    if output.startswith("{") or output.startswith("["):
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            pass
    return None


def _fetch_depth_for_market(market: dict) -> dict:
    """Enhance a market dict with orderbook depth and holder data from snapshot.

    Returns market dict with added: top_holder_pct, orderbook_depth
    """
    slug = market.get("slug")
    if not slug:
        return market

    snapshot = fetch_snapshot_for_market(slug, limit=1)
    if not snapshot:
        return market

    # Extract snapshot_data
    snap_data = snapshot.get("snapshot_data") or {}
    if isinstance(snap_data, dict):
        # Holder concentration
        holders = snap_data.get("holders") or []
        if isinstance(holders, list) and holders:
            total_shares = sum(h.get("shares", 0) or 0 for h in holders[:10])
            # top-10 holder pct is a proxy — full calculation needs total supply
            market["top_holder_pct"] = None  # calculated downstream if data available

        # Orderbook depth
        ob = snap_data.get("orderbook") or {}
        if isinstance(ob, dict):
            bids = ob.get("bids", []) or []
            asks = ob.get("asks", []) or []
            depth = {
                "bid_levels": len(bids),
                "ask_levels": len(asks),
                "total_bid_size": sum(float(b.get("size", 0) or 0) for b in bids),
                "total_ask_size": sum(float(a.get("size", 0) or 0) for a in asks),
            }
            market["orderbook_depth"] = depth

    return market


def run_multi_snapshot(limit: int = DEFAULT_LIMIT) -> list[dict]:
    """Fetch top-N markets by volume and enrich each with snapshot depth."""
    print(f"Fetching top {limit} markets by volume...")
    markets = list_markets(limit=50)  # Get top 50 to sort, take top N

    if not markets:
        print("No markets found", file=sys.stderr)
        return []

    # Take top-N by volume
    top_markets = markets[:limit]
    print(f"Top {len(top_markets)} markets by volume:")
    for m in top_markets:
        print(f"  {m['slug'] or m['condition_id'][:20]:25s} vol={m['volume24hr']:>10,.0f}  spread={m['spread']:.4f}")

    # Enrich each with snapshot depth (skip for speed if too many)
    enriched = []
    for i, market in enumerate(top_markets):
        slug = market.get("slug")
        if slug:
            print(f"  [{i+1}/{len(top_markets)}] Fetching depth for {slug}...")
            market = _fetch_depth_for_market(market)
        enriched.append(market)

    return enriched


def save_snapshot(market_snapshots: list[dict]) -> None:
    """Save multi-market snapshot to JSON, enforcing size limit."""
    now = datetime.now(timezone.utc).isoformat()

    snapshot = {
        "timestamp": now,
        "market_snapshots": market_snapshots,
        "_meta": {
            "count": len(market_snapshots),
            "generated_by": "polymarket_analyzer_bridge.py",
            "max_file_size_kb": MAX_FILE_SIZE_KB,
        },
    }

    raw = json.dumps(snapshot, indent=2, default=str)
    size_kb = len(raw.encode()) / 1024

    if size_kb > MAX_FILE_SIZE_KB:
        # Trim to reduce size
        print(f"Snapshot {size_kb:.1f}KB exceeds limit, trimming depth fields...")
        for m in market_snapshots:
            m.pop("orderbook_depth", None)
            m.pop("top_holder_pct", None)
        raw = json.dumps(snapshot, indent=2, default=str)
        size_kb = len(raw.encode()) / 1024
        print(f"Trimmed snapshot: {size_kb:.1f}KB")

    OUTPUT_PATH.write_text(raw)
    print(f"Saved snapshot to {OUTPUT_PATH} ({size_kb:.1f}KB, {len(market_snapshots)} markets)")


def check_market_health(market: dict) -> str:
    """Assess market health: 'good', 'poor', or 'concentrated'.

    Rules:
      - spread > 0.05 (5 cents) → 'poor' (wide spread = illiquid)
      - volume_24h < 1000 → 'poor'
      - top_holder_pct > 70 → 'concentrated' (manipulation risk)
    """
    spread = market.get("spread", 0) or 0
    volume = market.get("volume24hr", 0) or 0
    holder_pct = market.get("top_holder_pct")

    if spread > 0.05:
        return "poor"
    if volume < 1000:
        return "poor"
    if holder_pct is not None and holder_pct > 70:
        return "concentrated"
    return "good"


def main() -> None:
    """Run the analyzer bridge — multi-market snapshot."""
    print(f"=== Polymarket Analyzer Bridge ({datetime.now(timezone.utc).isoformat()}) ===")

    market_snapshots = run_multi_snapshot(limit=DEFAULT_LIMIT)

    if not market_snapshots:
        print("No markets captured, writing empty snapshot")
        market_snapshots = []

    # Attach health assessment
    for m in market_snapshots:
        m["health"] = check_market_health(m)

    save_snapshot(market_snapshots)

    # Summary
    good = sum(1 for m in market_snapshots if m.get("health") == "good")
    poor = sum(1 for m in market_snapshots if m.get("health") == "poor")
    conc = sum(1 for m in market_snapshots if m.get("health") == "concentrated")
    print(f"\nHealth summary: {good} good, {poor} poor, {conc} concentrated")
    print("=== Done ===")


if __name__ == "__main__":
    main()
