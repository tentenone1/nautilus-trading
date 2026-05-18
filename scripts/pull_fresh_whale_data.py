#!/usr/bin/env python3
"""Pull fresh Polymarket trade data for all tracked COPY/FADE whales + Tony's wallet."""
import urllib.request, json, time, sys

ALL_WALLETS = {
    "RJW1": "0x85f031d069de300055900c4055c1baeb6bde3f67",
    "surfandturf": "0x9f2fe025f84839ca81dd8e0338892605702d2ca8",
    "matanovik": "0x39d3c773be30fcc73161fc6768f46d563a779ef0",
    "p150-0xba389f": "0xba389f76b0119aed07c53c9029852664bd97e406",
    "pilotbaby": "0x6815040a7176c958e6ff8818bfe188e80dbd9edb",
    "Countryside": "0xbddf61af533ff524d27154e589d2d7a81510c684",
    "asdfjh": "0x0eb568f307e9a48af2c3e688ad6074236712c494",
    "SMCAOMCRL": "0x3b5c629f114098b0dee345fb78b7a3a013c7126e",
    "benwyatt": "0x1117eade222413335b7ec959e5b48c1d3dbc3532",
    "JPMorgan101": "0xb6d6e99d3bfe055874a04279f659f009fd57be17",
    "bossoskil1": "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a",
    "trade-via-Gravia": "0xe48109602719f95c247fec255ffb71bab3f985a3",
    "Tony (trading wallet)": "0x970807Acd56ecA1f0179599BeDE25EBeCDDdb86C",
}

def classify_trade(slug):
    slug = (slug or "").lower()
    if any(x in slug for x in ["nba", "spread", "ou-", "thunder", "lakers", "celtics", "knicks", "76ers", "pistons", "spurs", "cavaliers", "rockets", "warriors", "clippers", "magic", "raptors", "bulls", "heat", "hawks", "nets", "hornets", "pacers", "grizzlies", "pelicans", "suns", "kings", "nuggets", "jazz", "blazers", "wolves", "bucks", "mavericks"]):
        return "sports/nba"
    if any(x in slug for x in ["soccer", "fc ", "united", "chelsea", "bayern", "roma", "ac ", "inter", "juventus", "liverpool", "city", "arsenal", "tottenham", "psg", "barcelona", "real ", "atletico", "dortmund", "milan", "napoli", "leverkusen", "benfica", "porto", "ajax"]):
        return "sports/soccer"
    if any(x in slug for x in ["bitcoin", "btc", "crypto", "eth"]):
        return "crypto"
    if any(x in slug for x in ["cs-", "counter", "lol", "league", "valorant", "esports", "map", "cybershoke", "astral", "bo3"]):
        return "esports"
    if any(x in slug for x in ["world", "politics", "iran", "trump", "election", "congress", "senate"]):
        return "politics"
    if any(x in slug for x in ["ufc", "fight", "mlb", "nfl", "nhl", "tennis", "golf", "fifa", "world cup"]):
        return "sports/other"
    return "other"

results = {}
for name, wallet in ALL_WALLETS.items():
    print(f"Fetching {name}...", flush=True)
    url = f"https://data-api.polymarket.com/v1/trades?user={wallet}&limit=50"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
        
        buys = sum(1 for t in data if t.get("side") == "BUY")
        sells = sum(1 for t in data if t.get("side") == "SELL")
        total_vol = round(sum(t.get("size", 0) * t.get("price", 0) for t in data), 2)
        
        categories = {}
        for t in data:
            cat = classify_trade(t.get("slug", ""))
            categories[cat] = categories.get(cat, 0) + 1
        
        latest = data[0] if data else {}
        results[name] = {
            "wallet": wallet,
            "total_trades": len(data),
            "buys": buys,
            "sells": sells,
            "total_volume_usd": total_vol,
            "categories": categories,
            "latest_trade": latest.get("title", ""),
            "latest_price": latest.get("price", 0),
        }
        print(f"  -> {len(data)} trades, ${total_vol:.0f} vol", flush=True)
    except Exception as e:
        results[name] = {"wallet": wallet, "error": str(e)}
        print(f"  -> ERROR: {e}", flush=True)
    time.sleep(0.5)

outpath = "/home/elon-1/workspace/nautilus-trading/research/jailbreak_fresh_data.json"
with open(outpath, "w") as f:
    json.dump(results, f, indent=2)

print(f"\nData saved to {outpath}")
print(f"\n=== SUMMARY ===")
for name, d in results.items():
    if "error" in d:
        print(f"  {name:30s}: ERROR - {d['error']}")
    else:
        cats = ", ".join(f"{k}={v}" for k, v in d.get("categories", {}).items())
        print(f"  {name:30s}: {d['total_trades']:3d} trades, ${d['total_volume_usd']:>7,.0f} vol | {cats}")
