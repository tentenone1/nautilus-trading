#!/usr/bin/env python3
"""Whale Intelligence Pipeline — Classify all 420 wallets from whale_discovery.db.

Uses 5900x LM Studio (qwen3.6-35b-a3b-uncensored) for LLM-based classification.
Batches wallets for efficient processing, stores results in whale_intelligence_420.json
and a new DB table whale_intelligence.
"""

import json
import re
import sqlite3
import urllib.request
import os
import sys
import time
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────────
LLM_URL = "http://127.0.0.1:8080/v1/chat/completions"
LLM_MODEL = "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive"
BATCH_SIZE = 10          # Wallets per LLM call
TOKENS_PER_BATCH = 3000  # Max tokens per batch call
TIMEOUT_PER_CALL = 300   # 5 min per LLM call
TEMPERATURE = 0.15

DB_PATH = os.path.expanduser(
    "~/workspace/nautilus-trading/pipeline/data/whale_discovery.db"
)
OUTPUT_PATH = os.path.expanduser(
    "~/workspace/nautilus-trading/research/whale_intelligence_420.json"
)

# ── Classification categories ──────────────────────────────────────────────
CLASSIFICATION_TYPES = [
    "skilled_human", "degenerate_human", "trading_bot",
    "market_maker", "sacrificial_account", "mixed_entity", "unknown"
]

# ── System prompt ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a behavioral analyst for prediction markets. 
Output ONLY valid JSON. No thinking, no tags, no commentary.
Return an array of objects, one per whale: 
[{"name":"...","classification":"skilled_human|degenerate_human|trading_bot|market_maker|sacrificial_account|mixed_entity","trust_score":0-10,"should_copy":"yes/no","should_fade":"yes/no","reasoning":"max 15 words"}]
Each classification must be one of the specified types. trust_score: 0=completely untrustworthy, 10=elite trader."""


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_table(conn: sqlite3.Connection):
    """Create whale_intelligence table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS whale_intelligence (
            address TEXT PRIMARY KEY,
            name TEXT,
            alpha_score REAL,
            pnl REAL,
            volume REAL,
            win_rate REAL,
            total_trades INTEGER,
            classification TEXT,
            trust_score INTEGER,
            should_copy INTEGER,
            should_fade INTEGER,
            reasoning TEXT,
            batch_id INTEGER,
            analyzed_at TEXT
        )
    """)
    conn.commit()


def load_wallets(conn: sqlite3.Connection) -> list[dict]:
    """Load all wallets from whales table, ordered by alpha desc."""
    rows = conn.execute("""
        SELECT address, name, alpha_score, pnl, volume, win_rate, total_trades,
               tags, market_category
        FROM whales
        ORDER BY alpha_score DESC, pnl DESC
    """).fetchall()
    return [dict(r) for r in rows]


def build_batch_prompt(wallets: list[dict]) -> str:
    """Build a prompt for a batch of wallets."""
    lines = []
    for i, w in enumerate(wallets, 1):
        lines.append(f"{i}. {w['name']}: α={w['alpha_score']:.0f}, "
                     f"PnL=${w['pnl']:,.0f}, Vol=${w['volume']:,.0f}, "
                     f"WR={w['win_rate']:.0%}, Trades={w['total_trades']}, "
                     f"Tags={w['tags']}, Category={w['market_category']}")
    return "Analyze these whales for prediction market intelligence:\n\n" + \
           "\n".join(lines) + \
           "\n\nOutput a JSON array with one object per whale. Include name, classification, trust_score (0-10), should_copy (yes/no), should_fade (yes/no), reasoning (max 15 words). JSON array ONLY."


def call_llm(prompt: str) -> str | None:
    """Send batch prompt to 5900x LLM and return raw content."""
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": TOKENS_PER_BATCH,
        "temperature": TEMPERATURE,
    }).encode()

    try:
        req = urllib.request.Request(
            LLM_URL, data=payload,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT_PER_CALL) as resp:
            data = json.loads(resp.read())
        content = data["choices"][0]["message"]["content"]
        return content
    except Exception as e:
        print(f"  [ERROR] LLM call failed: {e}", flush=True)
        return None


def extract_json_array(text: str) -> list[dict] | None:
    """Extract a JSON array from model output, handling think tags and code blocks."""
    if not text:
        return None

    # Remove think tags
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    cleaned = re.sub(r'<think>.*', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'.*</think>', '', cleaned, flags=re.DOTALL)

    # Try to find JSON in code blocks first
    code_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', cleaned, re.DOTALL)
    if code_match:
        try:
            return json.loads(code_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try bare JSON array
    array_match = re.search(r'\[.*\]', cleaned, re.DOTALL)
    if array_match:
        try:
            return json.loads(array_match.group(0))
        except json.JSONDecodeError:
            pass

    # Try individual JSON objects
    objects = []
    for match in re.finditer(r'\{[^{}]*\}', cleaned):
        try:
            obj = json.loads(match.group(0))
            if "name" in obj and "classification" in obj:
                objects.append(obj)
        except json.JSONDecodeError:
            pass
    if objects:
        return objects

    return None


def normalize_result(res: dict, wallet: dict) -> dict:
    """Normalize a classification result, filling defaults for missing fields."""
    classification = res.get("classification", "unknown")
    if classification not in CLASSIFICATION_TYPES:
        classification = "unknown"

    trust_score = res.get("trust_score", 5)
    if trust_score is None:
        trust_score = 5
    trust_score = max(0, min(10, int(trust_score)))

    should_copy = str(res.get("should_copy", "no")).lower() in ("yes", "true")
    should_fade = str(res.get("should_fade", "no")).lower() in ("yes", "true")
    reasoning = str(res.get("reasoning", "") or "")[:200]

    return {
        "name": wallet["name"],
        "address": wallet["address"],
        "alpha_score": wallet["alpha_score"],
        "pnl": wallet["pnl"],
        "volume": wallet["volume"],
        "win_rate": wallet["win_rate"],
        "total_trades": wallet["total_trades"],
        "classification": classification,
        "trust_score": trust_score,
        "should_copy": should_copy,
        "should_fade": should_fade,
        "reasoning": reasoning,
    }


def store_results(conn: sqlite3.Connection, results: list[dict], batch_id: int):
    """Store results in both DB and in-memory."""
    now = datetime.now(timezone.utc).isoformat()
    for r in results:
        conn.execute("""
            INSERT OR REPLACE INTO whale_intelligence
            (address, name, alpha_score, pnl, volume, win_rate, total_trades,
             classification, trust_score, should_copy, should_fade, reasoning,
             batch_id, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["address"], r["name"], r["alpha_score"], r["pnl"],
            r["volume"], r["win_rate"], r["total_trades"],
            r["classification"], r["trust_score"],
            1 if r["should_copy"] else 0,
            1 if r["should_fade"] else 0,
            r["reasoning"], batch_id, now
        ))
    conn.commit()


def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🐋 Whale Intelligence Pipeline Starting", flush=True)
    print(f"  LLM: {LLM_MODEL} @ 1700 | Batch size: {BATCH_SIZE}", flush=True)
    print(f"  Output: {OUTPUT_PATH}", flush=True)

    conn = get_db()
    ensure_table(conn)

    # Load all wallets
    wallets = load_wallets(conn)
    total = len(wallets)
    print(f"  Loaded {total} wallets from whale_discovery.db", flush=True)

    # Stats
    top_tier = [w for w in wallets if w["alpha_score"] >= 80]
    high_signal = [w for w in wallets if w["alpha_score"] >= 70]
    print(f"  α≥80: {len(top_tier)} | α≥70: {len(high_signal)}", flush=True)
    print(f"  Bottom tier (α<60): {len([w for w in wallets if (w['alpha_score'] or 0) < 60])}", flush=True)

    # Batch processing
    batches = [wallets[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    print(f"  Batches: {len(batches)} (size {BATCH_SIZE})", flush=True)
    print()

    all_results = []
    total_completed = 0

    # Check what's already been analyzed
    existing = set()
    for r in conn.execute("SELECT address FROM whale_intelligence").fetchall():
        existing.add(r["address"])
    if existing:
        print(f"  Found {len(existing)} previously analyzed wallets — will skip & update", flush=True)

    for batch_idx, batch in enumerate(batches):
        batch_id = batch_idx + 1
        
        # Skip wallets already analyzed
        unanalyzed = [w for w in batch if w["address"] not in existing]
        if not unanalyzed:
            total_completed += len(batch)
            print(f"  [{batch_id}/{len(batches)}] Batch {batch_id}: 0 new wallets (already analyzed)", flush=True)
            continue
        
        print(f"  [{batch_id}/{len(batches)}] Analyzing {len(unanalyzed)}/{len(batch)} wallets...", flush=True)
        print(f"    Batch includes: {', '.join(w['name'][:20] for w in unanalyzed[:4])}{'...' if len(unanalyzed)>4 else ''}", flush=True)
        
        prompt = build_batch_prompt(unanalyzed)
        raw = call_llm(prompt)
        
        if raw is None:
            print(f"    ❌ LLM failed, retrying once...", flush=True)
            time.sleep(5)
            raw = call_llm(prompt)
            if raw is None:
                print(f"    ❌ LLM failed again. Moving on.", flush=True)
                continue
        
        # Extract JSON
        parsed = extract_json_array(raw)
        
        if parsed is None:
            print(f"    ⚠️  Could not parse JSON from response. Saving raw for debugging.", flush=True)
            # Save raw for debugging
            debug_path = f"/tmp/whale_batch_{batch_id}_raw.txt"
            with open(debug_path, "w") as f:
                f.write(raw[:5000])
            print(f"    Raw saved to {debug_path}", flush=True)
            continue

        # Match results to wallets
        results = []
        for p in parsed:
            # Find matching wallet
            name = p.get("name", "")
            matching = [w for w in unanalyzed if w["name"] == name]
            if matching:
                results.append(normalize_result(p, matching[0]))
            elif unanalyzed:
                # Try to match by index position
                idx = parsed.index(p)
                if idx < len(unanalyzed):
                    results.append(normalize_result(p, unanalyzed[idx]))
                else:
                    print(f"    ⚠️  Could not match result: {p.get('name', '?')}", flush=True)
        
        if results:
            store_results(conn, results, batch_id)
            all_results.extend(results)
            total_completed += len(results)
            
            # Stats for this batch
            classifications = {}
            for r in results:
                c = r["classification"]
                classifications[c] = classifications.get(c, 0) + 1
            cls_str = ", ".join(f"{c}={n}" for c, n in classifications.items())
            print(f"    ✅ {len(results)} analyzed: {cls_str}", flush=True)
        else:
            print(f"    ⚠️  No valid results extracted from batch", flush=True)

        # Small delay between batches
        time.sleep(2)
        
        # Progress
        pct = total_completed * 100 / total if total else 0
        print(f"    Progress: {total_completed}/{total} ({pct:.1f}%)", flush=True)

    # Final output
    now = datetime.now(timezone.utc)
    
    # Load all results from DB
    rows = conn.execute("""
        SELECT * FROM whale_intelligence
        ORDER BY alpha_score DESC, pnl DESC
    """).fetchall()
    all_from_db = [dict(r) for r in rows]
    
    output = {
        "generated": now.isoformat(),
        "total_wallets": total,
        "analyzed": len(all_from_db),
        "model": LLM_MODEL,
        "stats": {
            "by_classification": {},
            "by_alpha_tier": {},
        },
        "profiles": all_from_db,
    }
    
    # Stats
    for p in all_from_db:
        c = p["classification"]
        output["stats"]["by_classification"][c] = \
            output["stats"]["by_classification"].get(c, 0) + 1
        
        a = p["alpha_score"] or 0
        tier = "α≥80" if a >= 80 else ("α≥70" if a >= 70 else 
               ("α≥60" if a >= 60 else ("α≥50" if a >= 50 else "α<50")))
        output["stats"]["by_alpha_tier"][tier] = \
            output["stats"]["by_alpha_tier"].get(tier, 0) + 1
    
    # Top whales summary
    top_by_alpha = sorted(all_from_db, key=lambda x: x.get("alpha_score", 0) or 0, reverse=True)[:20]
    output["top_whales"] = [
        {
            "name": t["name"],
            "alpha_score": t["alpha_score"],
            "classification": t["classification"],
            "trust_score": t["trust_score"],
            "should_copy": t["should_copy"] == 1,
            "should_fade": t["should_fade"] == 1,
            "reasoning": t["reasoning"],
        }
        for t in top_by_alpha
    ]
    
    # Check for copy recommendations
    copy_whales = [t for t in all_from_db if t["should_copy"] == 1]
    output["copy_recommendations"] = [
        {
            "name": t["name"],
            "alpha_score": t["alpha_score"],
            "trust_score": t["trust_score"],
            "classification": t["classification"],
            "reasoning": t["reasoning"],
        }
        for t in copy_whales
    ]
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    conn.close()
    
    print()
    print("=" * 60)
    print(f"  🐋 WHALE INTELLIGENCE COMPLETE")
    print("=" * 60)
    print(f"  Total wallets: {total}")
    print(f"  Analyzed: {len(all_from_db)}")
    print(f"  Output: {OUTPUT_PATH}")
    print()
    print("  Classification Distribution:")
    for c, n in sorted(output["stats"]["by_classification"].items(),
                       key=lambda x: -x[1]):
        print(f"    {c}: {n}")
    print()
    print("  Alpha Tier Distribution:")
    for t in ["α≥80", "α≥70", "α≥60", "α≥50", "α<50"]:
        n = output["stats"]["by_alpha_tier"].get(t, 0)
        print(f"    {t}: {n}")
    print()
    print(f"  Copy Recommendations: {len(copy_whales)}")
    if copy_whales:
        for cw in output["copy_recommendations"][:5]:
            print(f"    {cw['name']:25s} α={cw['alpha_score']:3.0f} trust={cw['trust_score']}/10")
    print()
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
