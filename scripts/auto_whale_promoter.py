#!/usr/bin/env python3
"""
Auto-promote high fade-worthiness whales from weekly cohort report into tracked whale list.

Usage:
    python3 scripts/auto_whale_promoter.py          # Dry run (preview)
    python3 scripts/auto_whale_promoter.py --apply   # Actually write to DB
    python3 scripts/auto_whale_promoter.py --graduate # Check and graduate eligible probation whales
    python3 scripts/auto_whale_promoter.py --status  # Show current probation whales
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta
from glob import glob


BASE_DIR = "/home/elon-1/workspace/nautilus-trading"
DATA_DIR = os.path.join(BASE_DIR, "data")
RESEARCH_DIR = os.path.join(BASE_DIR, "research")
CONFIG_DIR = os.path.join(BASE_DIR, "config")

DB_PATH = os.path.join(DATA_DIR, "whale_discovery.db")
TRADES_DB_PATH = os.path.join(DATA_DIR, "trades.db")
KNOWN_WHALES_PATH = os.path.join(CONFIG_DIR, "known_whale_wallets.json")
COHORT_GLOB = os.path.join(RESEARCH_DIR, "whale_cohort_report_*.json")

FADE_THRESHOLD = 75.0
GRADUATION_POSITIVE_TRADES = 3
GRADUATION_MAX_AGE_DAYS = 14


def get_today():
    return datetime.now().strftime("%Y-%m-%d")


def get_latest_cohort_report():
    reports = glob(COHORT_GLOB)
    if not reports:
        return None
    latest = max(reports)
    with open(latest, "r") as f:
        return json.load(f), latest


def load_known_whales():
    if not os.path.exists(KNOWN_WHALES_PATH):
        return {}
    with open(KNOWN_WHALES_PATH, "r") as f:
        return json.load(f)


def save_known_whales(whales_dict):
    metadata_keys = {"_comment", "_source", "_removed", "_added", "_blacklist_note"}
    filtered = {k: v for k, v in whales_dict.items() if not k.startswith("_")}
    for key in metadata_keys:
        if key in whales_dict:
            filtered[key] = whales_dict[key]
    with open(KNOWN_WHALES_PATH, "w") as f:
        json.dump(filtered, f, indent=2)


def get_existing_addresses():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT address FROM whales")
    addresses = {row[0] for row in cursor}
    conn.close()
    return addresses


def get_probation_whales():
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT address, name, tags, discovered_at, last_seen, alpha_score, volume, total_trades, win_rate
        FROM whales
        WHERE tags LIKE '%probation%'
    """
    cursor = conn.execute(query)
    whales = []
    for row in cursor:
        whales.append({
            "address": row[0],
            "name": row[1],
            "tags": row[2],
            "discovered_at": row[3],
            "last_seen": row[4],
            "alpha_score": row[5],
            "volume": row[6],
            "total_trades": row[7],
            "win_rate": row[8],
        })
    conn.close()
    return whales


def count_positive_pnl_trades(whale_address):
    conn = sqlite3.connect(TRADES_DB_PATH)
    query = """
        SELECT COUNT(*) FROM trades
        WHERE whale_address = ? AND realized_pnl > 0
    """
    cursor = conn.execute(query, (whale_address,))
    count = cursor.fetchone()[0]
    conn.close()
    return count


def generate_name(address):
    hex_part = address[2:10]
    return f"probation_{hex_part}"


def insert_whale(candidate, dry_run=True):
    address = candidate["address"]
    name = generate_name(address)
    alpha_score = candidate["fade_worthiness_score"]
    volume = candidate["total_volume_usd"]
    total_trades = candidate["total_trades"]
    buy_count = candidate.get("buy_count", 0)
    sell_count = candidate.get("sell_count", 0)
    win_rate = buy_count / (buy_count + sell_count) if (buy_count + sell_count) > 0 else 0.0
    tags = json.dumps(["probation", "auto_promoted"])
    today = get_today()

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT INTO whales (
                address, name, alpha_score, pnl, volume, win_rate, total_trades,
                market_category, last_seen, tags, discovered_at, updated_at,
                capital_tier, precision_tier, fully_processed
            ) VALUES (?, ?, ?, 0, ?, ?, ?, 'unknown', ?, ?, ?, ?, 'unknown', 'unknown', 0)
        """, (address, name, alpha_score, volume, win_rate, total_trades, today, tags, today, today))
        conn.commit()
        if not dry_run:
            print(f"  INSERTED: {address} as {name}")
        else:
            print(f"  WOULD INSERT: {address} as {name}")
        return True
    except sqlite3.IntegrityError:
        print(f"  SKIPPED (exists): {address}")
        return False
    finally:
        conn.close()


def promote_candidates(dry_run=True):
    result = get_latest_cohort_report()
    if not result:
        print("No cohort report found")
        return []

    report, report_path = result
    print(f"Using cohort report: {report_path}")

    candidates = report.get("new_candidates", [])
    print(f"Total new_candidates: {len(candidates)}")

    existing_addresses = get_existing_addresses()
    promoted = []

    qualifying = [
        c for c in candidates
        if c.get("fade_worthiness_score", 0) >= FADE_THRESHOLD
        and c.get("classification") == "trading_bot"
        and c.get("address") not in existing_addresses
    ]

    print(f"Qualifying candidates (score>={FADE_THRESHOLD}, bot, not exists): {len(qualifying)}")

    if not dry_run:
        whales = load_known_whales()

    for candidate in qualifying:
        address = candidate["address"]
        print(f"\n  {address}")
        print(f"    fade_worthiness_score: {candidate.get('fade_worthiness_score')}")
        print(f"    total_volume_usd: {candidate.get('total_volume_usd')}")
        print(f"    total_trades: {candidate.get('total_trades')}")
        success = insert_whale(candidate, dry_run=dry_run)
        if success:
            promoted.append(candidate)
            if not dry_run:
                name = generate_name(address)
                whales[name] = address

    if not dry_run and promoted:
        save_known_whales(whales)

    log = {
        "date": get_today(),
        "cohort_report": report_path,
        "total_candidates": len(candidates),
        "qualifying_count": len(qualifying),
        "promoted_count": len(promoted),
        "promoted": promoted,
        "dry_run": dry_run,
    }
    log_path = os.path.join(RESEARCH_DIR, f"auto_promoter_log_{get_today()}.json")
    with open(log_path, "w") as f:
        json.dump(log, f, indent=2)
    print(f"\nLog written to: {log_path}")

    return promoted


def check_graduation_eligibility(whale):
    address = whale["address"]
    last_seen = whale.get("last_seen")
    if not last_seen:
        return False, "no last_seen"

    try:
        last_seen_date = datetime.strptime(last_seen.split(" ")[0], "%Y-%m-%d")
    except (ValueError, IndexError):
        return False, f"invalid last_seen format: {last_seen}"

    age_days = (datetime.now() - last_seen_date).days
    if age_days > GRADUATION_MAX_AGE_DAYS:
        return False, f"too old ({age_days} days)"

    positive_trades = count_positive_pnl_trades(address)
    if positive_trades < GRADUATION_POSITIVE_TRADES:
        return False, f"only {positive_trades} positive_pnl trades (need {GRADUATION_POSITIVE_TRADES}+)"

    return True, f"eligible ({positive_trades} positive trades, {age_days} days old)"


def graduate_probation_whales(dry_run=True):
    whales = get_probation_whales()
    print(f"Current probation whales: {len(whales)}")

    pending = []
    eligible = []

    for whale in whales:
        address = whale["address"]
        eligible_flag, reason = check_graduation_eligibility(whale)
        print(f"\n  {address} ({whale.get('name', 'unknown')})")
        print(f"    discovered_at: {whale.get('discovered_at')}")
        print(f"    last_seen: {whale.get('last_seen')}")
        print(f"    alpha_score: {whale.get('alpha_score')}")

        if eligible_flag:
            print(f"    => GRADUATION ELIGIBLE: {reason}")
            eligible.append({**whale, "graduation_reason": reason})
        else:
            print(f"    => Not eligible: {reason}")
            pending.append({**whale, "ineligibility_reason": reason})

    pending_path = os.path.join(RESEARCH_DIR, "auto_promoter_pending_graduations.json")
    with open(pending_path, "w") as f:
        json.dump({
            "date": get_today(),
            "total_probation": len(whales),
            "eligible_for_graduation": len(eligible),
            "pending": pending,
            "eligible": eligible,
        }, f, indent=2)
    print(f"\nPending graduations written to: {pending_path}")

    if not dry_run and eligible:
        conn = sqlite3.connect(DB_PATH)
        for whale in eligible:
            address = whale["address"]
            tags = json.loads(whale.get("tags", "[]"))
            tags = [t for t in tags if t != "probation"]
            if "graduated" not in tags:
                tags.append("graduated")
            tags_str = json.dumps(tags)
            conn.execute("UPDATE whales SET tags = ?, updated_at = ? WHERE address = ?",
                         (tags_str, get_today(), address))
        conn.commit()
        conn.close()
        print(f"Graduated {len(eligible)} whales")

    return eligible, pending


def show_status():
    whales = get_probation_whales()
    print(f"Current probation whales: {len(whales)}")

    if not whales:
        print("\nNo probation whales found.")
        return

    for whale in whales:
        address = whale["address"]
        eligible_flag, reason = check_graduation_eligibility(whale)
        status = "ELIGIBLE" if eligible_flag else "pending"
        print(f"\n  {address}")
        print(f"    name: {whale.get('name')}")
        print(f"    discovered_at: {whale.get('discovered_at')}")
        print(f"    last_seen: {whale.get('last_seen')}")
        print(f"    alpha_score: {whale.get('alpha_score')}")
        print(f"    volume: {whale.get('volume')}")
        print(f"    total_trades: {whale.get('total_trades')}")
        print(f"    win_rate: {whale.get('win_rate')}")
        print(f"    status: {status} ({reason})")


def main():
    parser = argparse.ArgumentParser(description="Auto-promote whales from cohort report")
    parser.add_argument("--apply", action="store_true", help="Actually write to DB (default is dry run)")
    parser.add_argument("--graduate", action="store_true", help="Check and graduate eligible probation whales")
    parser.add_argument("--status", action="store_true", help="Show current probation whales status")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    if args.graduate:
        graduate_probation_whales(dry_run=not args.apply)
        return

    if args.apply:
        print("=== APPLY MODE ===")
    else:
        print("=== DRY RUN MODE ===")

    promote_candidates(dry_run=not args.apply)


if __name__ == "__main__":
    main()
