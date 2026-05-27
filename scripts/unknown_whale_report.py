#!/usr/bin/env python3
"""
Unknown Whale Tracking Report
=============================
Tracks whale-named and raw-address entries in decision_snapshots that are
unresolved / classified as "unknown". Reports:
- Unique unknown whale addresses seen
- Their category distribution
- Whether they're generating signals post-I2 edge-fix
- Avg confidence and edge scores

Run: python3 scripts/unknown_whale_report.py
"""
import json
import sys
import os
from collections import defaultdict
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NAUTILUS_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, ".."))
TRADES_DB = os.path.join(NAUTILUS_DIR, "data", "trades.db")
DWS_PATH = os.path.join(NAUTILUS_DIR, "data", "dynamic_whale_state.json")
FEEDBACK_PATH = os.path.join(NAUTILUS_DIR, "data", "feedback_state.json")

NAUTILUS_SCRIPTS_DIR = os.path.join(NAUTILUS_DIR, "scripts")
sys.path.insert(0, NAUTILUS_SCRIPTS_DIR)

try:
    import sqlite3
except ImportError:
    print("ERROR: sqlite3 not available")
    sys.exit(1)


def get_named_whale_addresses(dws_path):
    """Return set of all known whale addresses from dynamic_whale_state.json."""
    names = set()
    addrs = set()
    try:
        dws = json.load(open(dws_path))
        for addr, info in dws.get("whales", {}).items():
            addrs.add(addr.lower())
            # also collect explicit name field if present
            wname = info.get("whale_name", "")
            if wname:
                names.add(wname.lower())
    except Exception as e:
        print(f"WARNING: Could not load {dws_path}: {e}")
    return names, addrs


def get_decision_snapshots(db_path):
    """Yield rows from decision_snapshots as dicts."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, signal_id, source, category, market_title,
               whale_name, whale_address, confidence, edge_score,
               side, final_decision, reject_reason,
               passed_category_filter, passed_quarantine, passed_edge_threshold,
               passed_risk_manager, passed_pnl_gate, passed_correlation_gate,
               shadow_mode
        FROM decision_snapshots
        ORDER BY timestamp DESC
    """)
    for row in cur:
        yield dict(row)
    conn.close()


def is_unknown_address(whale_name, named_addrs):
    """Return True if whale_name looks like a raw address (not a known named whale)."""
    wn = whale_name.lower()
    if wn in named_addrs:
        return False
    # Raw addresses start with 0x and are > 20 chars
    if whale_name.startswith("0x") and len(whale_name) > 30:
        return True
    # Addresses with timestamp suffixes like ...-1759935795465
    if "0x" in whale_name and "-" in whale_name and whale_name.count("-") >= 1:
        return True
    return False


def run_report():
    print(f"\n{'='*70}")
    print(f"  UNKNOWN WHALE TRACKING REPORT")
    print(f"  Generated: {datetime.now().isoformat()}Z")
    print(f"{'='*70}")

    # ── 1. Load known named whales ──────────────────────────────────────────
    named_whales, known_addrs = get_named_whale_addresses(DWS_PATH)
    print(f"\n[Config] Known named whales: {len(named_whales)}")
    print(f"[Config] Known raw addresses: {len(known_addrs)}")

    # ── 2. Load feedback state ──────────────────────────────────────────────
    try:
        feedback = json.load(open(FEEDBACK_PATH))
        wmt = feedback.get("whale_min_trades", {})
        print(f"[Config] whale_min_trades entries: {len(wmt)}")
    except Exception as e:
        wmt = {}
        print(f"WARNING: Could not load {FEEDBACK_PATH}: {e}")

    # ── 3. Query all decision_snapshots ─────────────────────────────────────
    if not os.path.exists(TRADES_DB):
        print(f"\nERROR: trades.db not found at {TRADES_DB}")
        return

    rows = list(get_decision_snapshots(TRADES_DB))
    if not rows:
        print("\nNo rows in decision_snapshots yet.")
        return

    print(f"\n[Data] Total decision_snapshots rows: {len(rows)}")

    # ── 4. Segment: named vs unknown-address vs "unknown" label ─────────────
    named_rows    = []   # known whale names
    addr_rows     = []   # raw address not in known_addrs
    label_unknown = []  # whale_name == "unknown"

    for row in rows:
        wn = row["whale_name"]
        if wn == "unknown":
            label_unknown.append(row)
        elif is_unknown_address(wn, known_addrs):
            addr_rows.append(row)
        else:
            named_rows.append(row)

    # ── 5. I2 fix boundary ──────────────────────────────────────────────────
    # I2 fix: edge_score threshold was 0.30 → 0.20 for unknown whales
    # Approximate date of fix: when the code was last modified
    # Use 2026-05-27T00:00:00Z as a conservative boundary
    try:
        i2_boundary = datetime.fromisoformat("2026-05-27T00:00:00+00:00")
    except Exception:
        i2_boundary = datetime(2026, 5, 27, 0, 0, 0)

    def post_i2(row):
        try:
            ts = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            return ts >= i2_boundary
        except Exception:
            return False

    print(f"\n{'─'*70}")
    print(f"  SEGMENTATION OVERVIEW")
    print(f"{'─'*70}")
    print(f"  Named whales:              {len(named_rows):>6} rows")
    print(f"  Raw address (unresolved):  {len(addr_rows):>6} rows")
    print(f"  Label 'unknown':            {len(label_unknown):>6} rows")
    print(f"  ─")
    print(f"  Total:                     {len(rows):>6} rows")

    # ── 6. Label "unknown" whales ───────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  LABEL 'unknown' WHALES (category general, blocked by edge<I2)")
    print(f"{'─'*70}")

    if label_unknown:
        by_cat = defaultdict(list)
        for row in label_unknown:
            by_cat[row["category"]].append(row)

        print(f"\n  Category breakdown:")
        print(f"  {'Category':<12} {'Count':>6}  {'Avg Conf':>8}  {'Avg Edge':>8}  {'Latest':>20}")
        print(f"  {'-'*12} {'-'*6}  {'-'*8}  {'-'*8}  {'-'*20}")
        for cat, cat_rows in sorted(by_cat.items(), key=lambda x: -len(x[1])):
            avg_conf = sum(r["confidence"] for r in cat_rows) / len(cat_rows)
            avg_edge = sum(r["edge_score"] for r in cat_rows) / len(cat_rows)
            latest = max(r["timestamp"] for r in cat_rows)
            print(f"  {cat:<12} {len(cat_rows):>6}  {avg_conf:>8.3f}  {avg_edge:>8.3f}  {latest:>20}")

        # Post-I2 signals
        post_i2_unknown = [r for r in label_unknown if post_i2(r)]
        print(f"\n  Post-I2-fix signals: {len(post_i2_unknown)}")
        if post_i2_unknown:
            by_cat_i2 = defaultdict(list)
            for row in post_i2_unknown:
                by_cat_i2[row["category"]].append(row)
            print(f"  {'Category':<12} {'Count':>6}  {'Avg Conf':>8}  {'Avg Edge':>8}")
            print(f"  {'-'*12} {'-'*6}  {'-'*8}  {'-'*8}")
            for cat, cat_rows in sorted(by_cat_i2.items(), key=lambda x: -len(x[1])):
                avg_conf = sum(r["confidence"] for r in cat_rows) / len(cat_rows)
                avg_edge = sum(r["edge_score"] for r in cat_rows) / len(cat_rows)
                print(f"  {cat:<12} {len(cat_rows):>6}  {avg_conf:>8.3f}  {avg_edge:>8.3f}")
    else:
        print("\n  No 'unknown' label rows found.")

    # ── 7. Raw address whales ───────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  RAW ADDRESS WHALES (unresolved addresses in decision_snapshots)")
    print(f"{'─'*70}")

    if addr_rows:
        # Group by whale_name (address)
        by_addr = defaultdict(list)
        for row in addr_rows:
            by_addr[row["whale_name"]].append(row)

        print(f"\n  Unique addresses: {len(by_addr)}")
        print(f"\n  {'Address':<50} {'Count':>6}  {'Avg Conf':>8}  {'Avg Edge':>8}  {'Categories'}")
        print(f"  {'-'*50} {'-'*6}  {'-'*8}  {'-'*8}  {'-'*20}")
        for addr, addr_rows_list in sorted(by_addr.items(), key=lambda x: -len(x[1]))[:30]:
            avg_conf = sum(r["confidence"] for r in addr_rows_list) / len(addr_rows_list)
            avg_edge = sum(r["edge_score"] for r in addr_rows_list) / len(addr_rows_list)
            cats = sorted(set(r["category"] for r in addr_rows_list))
            cats_str = ",".join(cats)[:20]
            addr_short = addr[:48]
            print(f"  {addr_short:<50} {len(addr_rows_list):>6}  {avg_conf:>8.3f}  {avg_edge:>8.3f}  {cats_str}")
    else:
        print("\n  No raw address rows found.")

    # ── 8. Signal generation by unknown whales post-I2 ─────────────────────
    print(f"\n{'─'*70}")
    print(f"  SIGNAL GENERATION POST-I2 FIX (unknown whales)")
    print(f"{'─'*70}")
    all_unknown = label_unknown + addr_rows
    # final_decision is 'accept' or 'reject' (or NULL)
    post_i2_signals = [r for r in all_unknown if post_i2(r) and r.get("final_decision") == "accept"]
    post_i2_blocked = [r for r in all_unknown if post_i2(r) and r.get("final_decision") != "accept"]

    print(f"\n  Post-I2 total (unknown whales):  {len([r for r in all_unknown if post_i2(r)])}")
    print(f"  Signals accepted:               {len(post_i2_signals)}")
    print(f"  Blocked (final_decision!=accept): {len(post_i2_blocked)}")

    if post_i2_blocked:
        by_reject = defaultdict(int)
        for r in post_i2_blocked:
            by_reject[r["reject_reason"] or "none"] += 1
        print(f"\n  Block reasons:")
        for reason, cnt in sorted(by_reject.items(), key=lambda x: -x[1]):
            print(f"    {reason:<40} {cnt:>5}")

    # ── 9. Whale state summary for unresolved addresses ─────────────────────
    print(f"\n{'─'*70}")
    print(f"  WHALE STATE: UNRESOLVED ADDRESSES")
    print(f"{'─'*70}")

    try:
        dws = json.load(open(DWS_PATH))
        unresolved_in_state = {}
        for addr, info in dws.get("whales", {}).items():
            wname = info.get("whale_name", "")
            # An address is unresolved if it's in state but not in named_addrs
            # AND not a known named whale (i.e., it's a raw address)
            if is_unknown_address(wname, known_addrs) or is_unknown_address(addr, known_addrs):
                unresolved_in_state[addr] = info

        if unresolved_in_state:
            print(f"\n  Addresses in dynamic_whale_state.json without named classification:")
            print(f"\n  {'Address':<50} {'Class':<20} {'WR':>6}  {'PnL':>8}  {'Trades':>7}")
            print(f"  {'-'*50} {'-'*20} {'-'*6}  {'-'*8}  {'-'*7}")
            for addr, info in sorted(unresolved_in_state.items(), key=lambda x: -x[1].get("total_trades", 0))[:20]:
                print(f"  {addr[:48]:<50} {info.get('classification','?'):<20} "
                      f"{info.get('overall_wr', 0):>6.3f}  {info.get('overall_pnl', 0):>8.2f}  "
                      f"{info.get('total_trades', 0):>7}")
        else:
            print("\n  All whale addresses in state have named classifications.")
    except Exception as e:
        print(f"\n  Could not load whale state: {e}")

    print(f"\n{'='*70}")
    print(f"  END OF REPORT")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    run_report()
