"""Whale Discovery Pipeline — Main Orchestrator.

Runs as a background process:
1. Scans known whale positions every 60s
2. Discovers new whales every 6 hours
3. Writes signals to SQLite DB
4. Nautilus reads signals via shared DB
"""
import sys
import os
import time
from nrs_guardian import enforce_singleton
enforce_singleton("whale_pipeline")
import json
import signal as sig
from datetime import datetime, timezone

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (
    SCAN_INTERVAL, DISCOVERY_INTERVAL, KNOWN_WHALES, MIN_ALPHA_SCORE
)
from pipeline.db import init_db, log_scan, get_stats
from pipeline.wallet_scanner import WalletScanner
from pipeline.wallet_tracker import WalletTracker, get_active_whales

# Graceful shutdown
_shutdown = False

def handle_signal(signum, frame):
    global _shutdown
    _shutdown = True

sig.signal(sig.SIGTERM, handle_signal)
sig.signal(sig.SIGINT, handle_signal)


def run_discovery_scan(scanner: WalletScanner) -> int:
    """Run full whale discovery scan."""
    start = time.time()
    new_whales = 0
    errors = ""
    
    try:
        # Update known whales
        whales_with_positions = scanner.scan_known_whales(KNOWN_WHALES)
        
        # Discover new whales from top markets
        new_whales = scanner.discover_new_whales(top_n=50)
        
        duration = time.time() - start
        log_scan("discovery", whales_with_positions + new_whales, 0,
                errors, duration)
        
        return whales_with_positions + new_whales
    except Exception as e:
        errors = str(e)[:200]
        duration = time.time() - start
        log_scan("discovery", 0, 0, errors, duration)
        return 0


def run_position_scan() -> int:
    """Scan all active whales for new positions."""
    start = time.time()
    tracker = WalletTracker()
    
    whales = get_active_whales()
    if not whales:
        log_scan("position", 0, 0, "No active whales", 0)
        return 0
    
    signals = tracker.scan(whales)
    duration = time.time() - start
    
    if signals:
        print(f"  [SIGNALS] {len(signals)} new signals:")
        for s in signals:
            print(f"    {s['whale_name']} -> {s['market'][:50]}... "
                  f"(${s['usd_value']:,.0f}, conf={s['confidence']:.2f})")
    
    log_scan("position", len(whales), len(signals), "", duration)
    return len(signals)


def write_signal_file() -> None:
    """Write current signals to a JSON file for Nautilus to read."""
    from pipeline.db import get_unsignaled_signals, mark_signals_signaled
    
    signals = get_unsignaled_signals()
    if not signals:
        return
    
    signal_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "whale_signals.json"
    )
    
    with open(signal_file, "w") as f:
        json.dump({
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "signals": signals,
        }, f, indent=2)
    
    # Mark as consumed
    mark_signals_signaled([s["id"] for s in signals])
    print(f"  [FILE] Wrote {len(signals)} signals to whale_signals.json")


def main():
    global _shutdown
    
    print("=" * 60)
    print("  WHALE DISCOVERY PIPELINE")
    print("=" * 60)
    
    # Initialize DB
    init_db()
    
    scanner = WalletScanner()
    last_discovery = 0
    
    print(f"  Scan interval: {SCAN_INTERVAL}s")
    print(f"  Discovery interval: {DISCOVERY_INTERVAL}s")
    print(f"  Known whales: {len(KNOWN_WHALES)}")
    print()
    
    # Initial discovery
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running initial discovery...")
    run_discovery_scan(scanner)
    last_discovery = time.time()
    stats = get_stats()
    print(f"  Stats: {stats}")
    
    # Main loop
    while not _shutdown:
        try:
            now = time.time()
            
            # Position scan
            signals = run_position_scan()
            write_signal_file()
            
            # Discovery scan (every 6 hours)
            if now - last_discovery > DISCOVERY_INTERVAL:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Discovery scan...")
                run_discovery_scan(scanner)
                last_discovery = time.time()
                stats = get_stats()
                print(f"  Stats: {stats}")
            
            # Sleep
            for _ in range(SCAN_INTERVAL):
                if _shutdown:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[ERROR] Pipeline error: {e}")
            time.sleep(30)  # Back off on errors
    
    print("\n[SHUTDOWN] Whale pipeline stopped.")


if __name__ == "__main__":
    main()
