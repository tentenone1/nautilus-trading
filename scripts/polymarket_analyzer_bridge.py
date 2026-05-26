#!/usr/bin/env python3
"""Polymarket Analyzer Bridge — Integrates polymarket-analyzer TUI data into nautilus.

This script runs the polymarket-analyzer in snapshot mode to capture current
market data, then writes it to nautilus-trading data files for use by the
signal pipeline and whale intelligence layer.

Output: data/polymarket_analyzer_snapshot.json (updated every cycle)
"""

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


def run_snapshot(market_slug: str = "", limit: int = 20) -> dict | None:
    """Run polymarket-analyzer in snapshot mode and parse the output.
    
    Args:
        market_slug: Optional market slug to focus on
        limit: Number of markets to include
    
    Returns:
        Parsed snapshot data or None on failure
    """
    cmd = [str(BUN_PATH), "run", str(ANALYZER_ROOT / "src/index.ts"), "--once"]
    if market_slug:
        cmd.extend(["--slug", market_slug])
    cmd.extend(["--limit", str(limit)])

    try:
        result = subprocess.run(
            cmd,
            cwd=str(ANALYZER_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            print(f"Analyzer failed: {result.stderr[:500]}", file=sys.stderr)
            return None
        
        # The --once mode outputs JSON to stdout
        # Parse it if possible
        output = result.stdout.strip()
        if output.startswith("{") or output.startswith("["):
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                pass
        
        # If not JSON, just store the raw output
        return {"raw_output": output, "timestamp": datetime.now(timezone.utc).isoformat()}
    
    except subprocess.TimeoutExpired:
        print("Analyzer timed out after 60s", file=sys.stderr)
        return None
    except FileNotFoundError:
        print(f"bun not found at {BUN_PATH}", file=sys.stderr)
        return None


def run_list_markets(limit: int = 50) -> list[dict] | None:
    """List current Polymarket markets using the analyzer.
    
    Returns list of market dicts with slug, condition_id, prices, etc.
    """
    cmd = [str(BUN_PATH), "run", str(ANALYZER_ROOT / "src/index.ts"), 
           "--list-markets", "--limit", str(limit), "--json"]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ANALYZER_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(f"List markets failed: {result.stderr[:500]}", file=sys.stderr)
            return None
        
        output = result.stdout.strip()
        if output.startswith("{") or output.startswith("["):
            try:
                data = json.loads(output)
                if isinstance(data, list):
                    return data
                return [data]
            except json.JSONDecodeError:
                pass
        
        # Parse line-by-line output (numbered list format)
        markets = []
        for line in output.split("\n"):
            line = line.strip()
            if " | " in line:
                parts = line.split(" | ")
                if len(parts) >= 3:
                    markets.append({
                        "question": parts[1].strip(),
                        "condition_id": parts[-1].strip(),
                        "side": parts[2].strip() if len(parts) > 2 else "",
                    })
        return markets if markets else None
    
    except subprocess.TimeoutExpired:
        print("List markets timed out", file=sys.stderr)
        return None
    except FileNotFoundError:
        print(f"bun not found at {BUN_PATH}", file=sys.stderr)
        return None


def save_snapshot(data: dict, markets: list | None = None) -> None:
    """Save snapshot data to JSON file for nautilus consumption."""
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "snapshot_data": data,
        "markets": markets or [],
    }
    OUTPUT_PATH.write_text(json.dumps(snapshot, indent=2, default=str))
    print(f"Saved snapshot to {OUTPUT_PATH}")


def main():
    """Run the analyzer bridge and save data."""
    print(f"=== Polymarket Analyzer Bridge ({datetime.now(timezone.utc).isoformat()}) ===")
    
    # Get current markets
    print("Listing current markets...")
    markets = run_list_markets(limit=50)
    if markets:
        print(f"  Found {len(markets)} active markets")
    else:
        print("  No markets data available")
    
    # Get snapshot data
    print("Running analyzer snapshot...")
    snapshot = run_snapshot(limit=20)
    
    # Save
    save_snapshot(snapshot or {}, markets)
    print("=== Done ===")


if __name__ == "__main__":
    main()
