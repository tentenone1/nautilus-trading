#!/usr/bin/env python3
"""
Whale Discovery Cron — runs every 6h to discover new whales on Polymarket.
Uses Polymarket data-api /v1/leaderboard + position scanning to find whales.
Writes to whale_discovery.db — Nautilus trader loads dynamically.
"""
import sqlite3
import time
import requests
from pathlib import Path
from collections import Counter
from nrs_guardian import enforce_singleton
enforce_singleton("discover_whales")
from strategies.whale_tiering import WhaleTiering

WHALE_TIERING = WhaleTiering()

BASE = Path(__file__).resolve().parents[1]
DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_discovery.db")

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
WHALE_API = "https://data-api.polymarket.com/v1/whales"
POSITIONS_URL = "https://data-api.polymarket.com/positions"
TRADES_URL = "https://data-api.polymarket.com/v1/trades?user={address}"


def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whales (
            address TEXT PRIMARY KEY,
            name TEXT,
            alpha_score REAL DEFAULT 0,
            pnl REAL DEFAULT 0,
            volume REAL DEFAULT 0,
            win_rate REAL DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            category TEXT DEFAULT 'unknown',
            capital_tier TEXT DEFAULT 'E',
            precision_tier TEXT DEFAULT 'LOW',
            last_seen TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


# ── Category keywords for market classification ──────────────────────────────
# Non-sports categories use first-match-wins (ordered by specificity).
# Sports uses a weighted tier system to reduce false positives from generic terms.

# Strong sports keywords — a single match is sufficient for sports classification.
# These are unmistakably sports: league names, major teams, unequivocal sports events.
SPORTS_STRONG_KEYWORDS: list[str] = [
    " nfl ", " nba ", " mlb ", " nhl ", " ncaa ",
    " ufc ", "mma", "boxing", "wwe", "esports", "fifa",
    "lakers", "celtics", "warriors", "knicks", "spurs", "thunder", "nuggets",
    "eagles", "49ers", "ravens", "steelers", "cowboys", "chiefs", "patriots",
    "yankees", "dodgers", "red sox", "bruins", "sabres", "canadiens",
    "super bowl", "world cup", "champions league", "premier league",
    "stanley cup", "world series", "final four", "march madness",
    "nascar", "pga", "atp", "wta",
    "dota", "league of legends", "valorant", "csgo",
]

# Weak sports keywords — generic terms that need multiple matches for confidence.
# These words appear in many non-sports contexts (e.g., "game theory", "final vote").
SPORTS_WEAK_KEYWORDS: list[str] = [
    "soccer", "basketball", "baseball", "hockey", "tennis", "golf",
    "f1", "formula 1", "grand prix",
    "football",
    " vs ", "vs.",
    "moneyline", "point spread", "spread", "o/u", "total points",
    "playoffs", "finals", "championship", "tournament",
    " win ", "goal", "match", "game", "cup", "final", "draft", "round",
]

# Negative keywords — if present alongside weak-only matches, block sports.
# These words indicate a non-sports context (legal, economic, political, etc.).
SPORTS_NEGATIVE_KEYWORDS: list[str] = [
    "court", "judicial", "lawsuit", "litigation",
    "inflation", "gdp", "recession", "interest rate",
    "president", "senator", "congressional",
    "patent", "fda approval", "clinical trial",
    "climate", "regulation", "regulatory",
    "budget", "tax", "funding", "subsidy",
]

# Non-sports categories use simple first-match-wins.
# Checked in order — most specific categories first.
CATEGORY_KEYWORDS = {
    "crypto": [
        "bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "defi",
        "xrp", "dogecoin", "doge", "shiba", "token", "blockchain", "nft",
        "binance", "coinbase", "uniswap", "usdc", "usdt", "altcoin", "mining",
        "web3", "stablecoin",
        "sui", "aptos", "near", "ton", "sei", "injective",
        "zksync", "arbitrum", "optimism", "avalanche", "polygon",
        "hyperliquid", "berachain", "monad", "base",
    ],
    "geopolitics": [
        "war", "ukraine", "russia", "iran", "israel", "gaza", "palestine",
        "ceasefire", "nato", "military", "conflict", "strait of hormuz",
        "nuclear", "surrender", "enriched uranium", "diplomatic", "invasion",
        "sanctions", "china", "taiwan", "korea", "putin", "zelenskyy",
        "hamas", "hezbollah", "middle east", "missile", "troops", "airstrike",
    ],
    "politics": [
        "trump", "biden", "harris", "election", "president", "congress",
        "senate", "governor", "vote", "primary", "caucus", "republican",
        "democrat", "supreme court", "impeachment", "midterm", "ballot",
        "referendum", "campaign", "political", "nominee", "cabinet",
        "scotus",
    ],
    "economics": [
        "gdp", "inflation", "fed", "interest rate", "recession", "cpi",
        "unemployment", "federal reserve", "treasury", "jobs report",
        "pce", "ppi", "retail sales", "housing starts", "mortgage",
        "oil", "crude", "wti", "natural gas", "copper", "commodities",
    ],
    "technology": [
        "ai", "chatgpt", "spacex", "nasa", "tesla", "nvidia", "semiconductor",
        "chip", "patent", "clinical trial", "fda", "approval", "launch",
        "quantum", "robot", "drone", "satellite", "mars", "moon",
    ],
}


def classify_market(title: str) -> str:
    """Classify a market title into a category using weighted keyword matching.

    Non-sports categories use first-match-wins (checked in order).
    Sports uses a weighted tier system to reduce false positives from
    generic terms like "game", "final", "draft", etc.

    Args:
        title: Market title string.

    Returns:
        Category name: 'sports', 'crypto', 'geopolitics', 'politics',
        'economics', 'technology', or 'other'.
    """
    if not title:
        return "other"
    t = " " + title.lower() + " "  # pad for boundary-safe matching

    # ── Stage 1: Check non-sports categories (first-match-wins) ────────
    # These are checked first because they have specific, non-ambiguous keywords.
    # If a market matches a crypto/politics/etc keyword first, it's NOT sports.
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in t:
                return category

    # ── Stage 2: Weighted sports classification ────────────────────────
    strong_matches = sum(1 for kw in SPORTS_STRONG_KEYWORDS if kw in t)
    weak_matches = sum(1 for kw in SPORTS_WEAK_KEYWORDS if kw in t)
    neg_matches = sum(1 for kw in SPORTS_NEGATIVE_KEYWORDS if kw in t)

    if strong_matches >= 1:
        return "sports"

    if weak_matches >= 3:
        return "sports"

    if weak_matches >= 2 and neg_matches == 0:
        return "sports"

    return "other"


def infer_whale_category(trades: list) -> str:
    """Infer a whale's primary market category from their recent trades."""
    cats = Counter()
    for t in trades:
        title = t.get("title", t.get("market_title", ""))
        cats[classify_market(title)] += 1
    
    if not cats:
        return "unknown"
    
    # If sports dominates, label as sports specialist
    total = sum(cats.values())
    top_cat, top_count = cats.most_common(1)[0]
    top_pct = top_count / total
    
    if top_pct >= 0.60:
        return top_cat
    elif top_cat == "sports" and top_pct >= 0.40:
        return "sports"
    else:
        # Mixed — pick the strongest non-other category
        non_other = [(c, n) for c, n in cats.most_common() if c != "other"]
        if non_other:
            return non_other[0][0]
        return top_cat


def scan_leaderboard():
    """Scan Polymarket leaderboard for top performers.
    
    Now categorizes whales by their market activity pattern.
    """
    found = []
    limit = 100  # max the API returns
    
    try:
        resp = requests.get(LEADERBOARD_URL, params={"limit": limit}, timeout=20)
        if resp.status_code != 200:
            print(f"[WARN] Leaderboard API returned {resp.status_code}")
            return found
        
        entries = resp.json()
        print(f"  Leaderboard: {len(entries)} entries returned")
        
        for entry in entries:
            addr = (entry.get("proxyWallet") or "").lower()
            if not addr:
                continue
            pnl = float(entry.get("pnl", 0) or 0)
            vol = float(entry.get("vol", 0) or 0)
            name = entry.get("userName") or entry.get("xUsername") or addr[:10]
            
            # Estimate win_rate and total_trades from PnL/volume ratio
            wr_est = 0.0
            trades_est = max(int(vol / 5000), 1)
            if vol > 0 and pnl > 0:
                roi = pnl / vol
                wr_est = min(roi * 2, 0.95)
            
            # Minimum bar: $5K PnL or $50K volume
            if pnl < 5000 and vol < 50000:
                continue
            
            alpha = min(pnl / 10000 * 10 + wr_est * 50, 100)
            
            # Try to fetch recent trades to infer category
            market_category = "unknown"
            try:
                trades_resp = requests.get(
                    TRADES_URL.format(address=addr),
                    params={"limit": 10},
                    timeout=10
                )
                if trades_resp.status_code == 200:
                    trades = trades_resp.json()
                    if isinstance(trades, list) and len(trades) > 0:
                        market_category = infer_whale_category(trades)
            except Exception:
                pass
            
            capital_tier = WHALE_TIERING.classify_capital(vol)
            precision_tier = WHALE_TIERING.classify_precision(wr_est)
            
            found.append({
                "address": addr,
                "name": name,
                "pnl": pnl,
                "volume": vol,
                "win_rate": round(wr_est, 2),
                "total_trades": trades_est,
                "alpha_score": round(alpha, 1),
                "market_category": market_category,
                "capital_tier": capital_tier,
                "precision_tier": precision_tier,
            })
    except Exception as e:
        print(f"[WARN] Leaderboard scan error: {type(e).__name__}: {e}")
    
    # Summary by category
    cat_counts = Counter(f["market_category"] for f in found)
    print(f"  Category breakdown: {dict(cat_counts)}")
    
    # Highlight non-sports whales
    non_sports = [f for f in found if f["market_category"] not in ("sports", "unknown", "other")]
    if non_sports:
        print(f"  Non-sports candidates: {len(non_sports)}")
        for ns in sorted(non_sports, key=lambda x: -x["pnl"])[:10]:
            print(f"    {ns['name'][:25]:25s} | PnL=${ns['pnl']:>8,.0f} | Cat={ns['market_category']}")
    
    return found


def write_to_db(conn, whales):
    """Write discovered whales to DB, skip existing. Updates category if changed."""
    added = 0
    updated = 0
    existing = {r[0]: r[1] for r in conn.execute("SELECT address, market_category FROM whales").fetchall()}
    
    for w in whales:
        addr = w["address"]
        if addr not in existing:
            conn.execute(
                "INSERT OR IGNORE INTO whales "
                "(address, name, alpha_score, pnl, volume, win_rate, total_trades, market_category, capital_tier, precision_tier, last_seen) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                (addr, w["name"], w["alpha_score"], w["pnl"],
                 w["volume"], w["win_rate"], w["total_trades"], w.get("market_category", "unknown"), w.get("capital_tier", "E"), w.get("precision_tier", "LOW"))
            )
            added += 1
        elif w.get("market_category") and w["market_category"] != "unknown" and existing[addr] != w["market_category"]:
            conn.execute(
                "UPDATE whales SET market_category = ?, capital_tier = ?, precision_tier = ?, updated_at = datetime('now') WHERE address = ?",
                (w["market_category"], w.get("capital_tier", "E"), w.get("precision_tier", "LOW"), addr)
            )
            updated += 1
    
    if added or updated:
        conn.commit()
    return added, updated


def main():
    start = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Whale discovery starting...")
    
    conn = ensure_db()
    whales = scan_leaderboard()
    added, updated = write_to_db(conn, whales)
    
    total = conn.execute("SELECT COUNT(*) FROM whales").fetchone()[0]
    high = conn.execute("SELECT COUNT(*) FROM whales WHERE alpha_score >= 60").fetchone()[0]
    
    # Show category distribution
    cat_dist = conn.execute(
        "SELECT market_category, COUNT(*) FROM whales WHERE market_category != 'unknown' GROUP BY market_category ORDER BY COUNT(*) DESC"
    ).fetchall()
    if cat_dist:
        print(f"  Category distribution: {dict(cat_dist)}")
    
    print("  Dual-axis breakdown: capital distribution and precision distribution")
    cap_dist = conn.execute("SELECT capital_tier, COUNT(*) FROM whales GROUP BY capital_tier ORDER BY capital_tier").fetchall()
    print(f"    Capital: {dict(cap_dist)}")
    prec_dist = conn.execute("SELECT precision_tier, COUNT(*) FROM whales GROUP BY precision_tier ORDER BY precision_tier").fetchall()
    print(f"    Precision: {dict(prec_dist)}")
    
    conn.close()
    
    elapsed = time.time() - start
    print(f"[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s — "
          f"{len(whales)} candidates, {added} new, {updated} updated, "
          f"{total} total whales ({high} with α≥60)")


if __name__ == "__main__":
    main()
