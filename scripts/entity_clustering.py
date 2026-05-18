#!/usr/bin/env python3
"""Entity-Level Whale Clustering + Sybil Wallet Detection.

Groups wallets into entities based on:
1. Timing correlation — trades within same minute window
2. Bet sizing similarity — same Kelly-like sizing patterns
3. Market overlap — same markets traded
4. Side coordination — one wallet buys, another sells same market

Outputs: entity clusters + sybil group alerts

Schedule: daily
"""

import os
import sqlite3
import json
from datetime import datetime, timezone

# Resolve relative to the script's location (works on both Mac and 1700)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NAUTILUS_ROOT = os.path.dirname(SCRIPT_DIR)  # goes up from scripts/ to nautilus-trading/
DB_PATH = os.path.join(NAUTILUS_ROOT, "research", "trades.db")
BACKUP_DB_PATH = os.path.join(NAUTILUS_ROOT, "research", "trades.db.archive")
OUTPUT_PATH = os.path.join(NAUTILUS_ROOT, "research", "entity_clusters.json")
CORRELATION_WINDOW_SECS = 60  # Trades within 1 minute = correlated
MIN_SHARED_MARKETS = 3  # Minimum shared markets for correlation
MIN_TRADES = 5


def main():
    # Fall back to backup DB if primary is too fresh
    primary_count = 0
    backup_count = 0
    for path, count_var in [(DB_PATH, "primary"), (BACKUP_DB_PATH, "backup")]:
        if os.path.exists(path):
            try:
                conn = sqlite3.connect(path)
                cnt = conn.execute(
                    "SELECT COUNT(DISTINCT whale_name) FROM trades WHERE whale_name IS NOT NULL AND whale_name != '' AND whale_name NOT LIKE 'unknown%'"
                ).fetchone()[0]
                conn.close()
                if count_var == "primary":
                    primary_count = cnt
                else:
                    backup_count = cnt
            except Exception:
                pass
    
    db_path = BACKUP_DB_PATH if (backup_count > primary_count * 3 and backup_count >= 3) else DB_PATH
    db = sqlite3.connect(db_path)
    src = "backup" if db_path == BACKUP_DB_PATH else "primary"
    print(f"[entity_clustering] Using {src} DB ({db_path.split('/')[-1]}) — {backup_count} backup vs {primary_count} primary whales", flush=True)

    # Get all whales with enough data
    whales = db.execute("""
        SELECT 
            whale_name,
            whale_address,
            COUNT(*) as trades,
            COUNT(DISTINCT condition_id) as markets,
            GROUP_CONCAT(DISTINCT condition_id) as market_list,
            GROUP_CONCAT(printf('%.0f', unixepoch(timestamp))) as timestamps,
            GROUP_CONCAT(printf('%.2f', position_size_usd)) as sizes,
            GROUP_CONCAT(side) as sides
        FROM trades 
        WHERE whale_name IS NOT NULL AND whale_name != '' 
            AND whale_name NOT LIKE 'unknown%'
            AND whale_address IS NOT NULL AND whale_address != ''
        GROUP BY whale_name
        HAVING trades >= ?
    """, (MIN_TRADES,)).fetchall()

    whale_data = {}
    for w in whales:
        name, addr, trades, markets, market_list, timestamps, sizes, sides = w
        whale_data[name] = {
            "address": addr,
            "trades": trades,
            "markets": markets,
            "market_set": set(market_list.split(",")) if market_list else set(),
            "timestamps": [float(t) for t in timestamps.split(",")] if timestamps else [],
            "sizes": [float(s) for s in sizes.split(",")] if sizes else [],
            "sides": sides.split(",") if sides else [],
        }

    # ── Entity Clustering ─────────────────────────────────────────────

    names = list(whale_data.keys())
    clusters = []
    assigned = set()

    for i, name1 in enumerate(names):
        if name1 in assigned:
            continue
        cluster = [name1]
        d1 = whale_data[name1]

        for name2 in names[i + 1:]:
            if name2 in assigned:
                continue
            d2 = whale_data[name2]

            score = 0
            reasons = []

            # 1. Timing correlation
            shared_time = 0
            for t1 in d1["timestamps"]:
                for t2 in d2["timestamps"]:
                    if abs(t1 - t2) < CORRELATION_WINDOW_SECS:
                        shared_time += 1
                        break
            if shared_time >= 3:
                score += 2
                reasons.append(f"timing_corr={shared_time}")

            # 2. Market overlap
            shared_markets = d1["market_set"] & d2["market_set"]
            if len(shared_markets) >= MIN_SHARED_MARKETS:
                score += 2
                reasons.append(f"market_overlap={len(shared_markets)}")

            # 3. Side coordination (opposite sides on same market)
            if d1["sides"] and d2["sides"]:
                side_pairs = 0
                m_list = list(shared_markets)[:10]  # Check top 10 shared markets
                for m in m_list:
                    idx1 = [i for i, m1 in enumerate(d1.get("market_set", set())) if m1 == m]
                    idx2 = [i for i, m2 in enumerate(d2.get("market_set", set())) if m2 == m]
                    for i1 in idx1[:1]:
                        for i2 in idx2[:1]:
                            if i1 < len(d1["sides"]) and i2 < len(d2["sides"]):
                                if d1["sides"][i1] != d2["sides"][i2]:
                                    side_pairs += 1
                if side_pairs > 0:
                    score += 2
                    reasons.append(f"side_coord={side_pairs}")

            # 4. Sizing similarity (both use Kelly-like fractions)
            if d1["sizes"] and d2["sizes"]:
                avg1 = sum(d1["sizes"]) / len(d1["sizes"])
                avg2 = sum(d2["sizes"]) / len(d2["sizes"])
                if avg1 > 0 and avg2 > 0:
                    ratio = max(avg1, avg2) / min(avg1, avg2)
                    if ratio < 1.5:
                        score += 1
                        reasons.append(f"size_ratio={ratio:.2f}")

            if score >= 3:
                cluster.append(name2)
                assigned.add(name2)

        if len(cluster) >= 2:
            clusters.append({"entities": cluster, "score": score, "reasons": reasons})
            for n in cluster:
                assigned.add(n)

    # ── Sybil Detection ────────────────────────────────────────────────

    sybil_groups = []
    for cluster_data in clusters:
        entities = cluster_data["entities"]
        if len(entities) >= 3:
            # Check if cluster has coordinated betting pattern
            total_volume = sum(whale_data[n].get("trades", 0) for n in entities)
            unique_markets = set()
            for n in entities:
                unique_markets |= whale_data[n].get("market_set", set())

            sybil_groups.append({
                "wallets": entities,
                "total_trades": total_volume,
                "unique_markets": len(unique_markets),
                "market_concentration": len(unique_markets) / max(total_volume, 1),
                "alerts": [],
            })

    # ── Generate Alerts ────────────────────────────────────────────────

    for group in sybil_groups:
        if group["market_concentration"] < 0.3:
            group["alerts"].append("HIGH CONCENTRATION: Few markets, many trades — likely coordinated")
        if group["total_trades"] > 50:
            group["alerts"].append(f"HIGH VOLUME: {group['total_trades']} total trades across group")

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "whales_analyzed": len(whales),
        "clusters_found": len(clusters),
        "sybil_groups_found": len(sybil_groups),
        "clusters": clusters,
        "sybil_groups": sybil_groups,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Entity clustering: {len(whales)} whales → {OUTPUT_PATH}")
    print(f"  Clusters: {len(clusters)}")
    for c in clusters[:5]:
        print(f"    {', '.join(c['entities'][:4])}... (score={c['score']})")
    print(f"  Sybil groups: {len(sybil_groups)}")
    for g in sybil_groups[:3]:
        print(f"    {', '.join(g['wallets'][:3])}... ({g['total_trades']} trades)")

    db.close()


if __name__ == "__main__":
    main()
