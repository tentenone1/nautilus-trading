#!/usr/bin/env python3
"""Whale Intelligence Pipeline v2 — Classify all 420 wallets via individual LLM calls.

Optimized: individual calls, tight prompts, strict JSON extraction.
Uses 5900x LM Studio (qwen3.6-35b-a3b-uncensored).
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
TIMEOUT_PER_CALL = 120
TEMPERATURE = 0.15

DB_PATH = os.path.expanduser(
    "~/workspace/nautilus-trading/pipeline/data/whale_discovery.db"
)
OUTPUT_PATH = os.path.expanduser(
    "~/workspace/nautilus-trading/research/whale_intelligence_420.json"
)

CLASSIFICATION_TYPES = [
    "skilled_human", "degenerate_human", "trading_bot",
    "market_maker", "sacrificial_account", "mixed_entity", "unknown"
]


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_table(conn):
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


def load_wallets(conn):
    rows = conn.execute("""
        SELECT address, name, alpha_score, pnl, volume, win_rate, total_trades,
               tags, market_category
        FROM whales
        ORDER BY alpha_score DESC, pnl DESC
    """).fetchall()
    return [dict(r) for r in rows]


def build_prompt(w):
    """Build a tight one-shot prompt for a single wallet."""
    return f"Classify this Polymarket whale. Output ONLY a JSON object.\n\nName: {w['name']}\nAlpha: {w['alpha_score']:.0f}, PnL: ${w['pnl']:,.0f}, Volume: ${w['volume']:,.0f}, WinRate: {w['win_rate']:.0%}, Trades: {w['total_trades']}, Category: {w['market_category']}, Tags: {w['tags']}\n\n{{\"name\":\"{w['name']}\",\"classification\":\"skilled_human\",\"trust_score\":8,\"should_copy\":\"yes\",\"should_fade\":\"no\",\"reasoning\":\"brief reason\"}}"


def call_llm(prompt):
    """Send a single-wallet prompt to 5900x LLM."""
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "Output ONLY a JSON object. No thinking, no tags, no reasoning text outside the JSON."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 500,
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
        return f"__ERROR__: {e}"


def extract_json(text):
    """Extract a JSON object from model output."""
    if not text or text.startswith("__ERROR__"):
        return None
    
    # Remove think tags
    cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    cleaned = re.sub(r'<think>.*', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'.*</think>', '', cleaned, flags=re.DOTALL)
    
    # Find JSON object
    # Try code block first
    code_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', cleaned, re.DOTALL)
    if code_match:
        try:
            return json.loads(code_match.group(1))
        except:
            pass
    
    # Try bare JSON object
    match = re.search(r'\{[^{}]*\}', cleaned)
    if match:
        try:
            return json.loads(match.group(0))
        except:
            pass
    
    # Try nested braces
    depth = 0
    start = -1
    for i, c in enumerate(cleaned):
        if c == '{':
            if start == -1:
                start = i
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(cleaned[start:i+1])
                except:
                    start = -1
    
    return None


def normalize(parsed, wallet):
    """Normalize LLM output to standard format."""
    classification = str(parsed.get("classification", "unknown"))
    if classification not in CLASSIFICATION_TYPES:
        classification = "unknown"
    
    trust_score = parsed.get("trust_score", 5)
    if trust_score is None:
        trust_score = 5
    try:
        trust_score = max(0, min(10, int(float(trust_score))))
    except (ValueError, TypeError):
        trust_score = 5
    
    should_copy = str(parsed.get("should_copy", "no")).lower() in ("yes", "true", "1")
    should_fade = str(parsed.get("should_fade", "no")).lower() in ("yes", "true", "1")
    reasoning = str(parsed.get("reasoning", "") or "")[:200]
    
    return {
        "address": wallet["address"],
        "name": wallet["name"],
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


def store_result(conn, result, batch_id):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO whale_intelligence
        (address, name, alpha_score, pnl, volume, win_rate, total_trades,
         classification, trust_score, should_copy, should_fade, reasoning,
         batch_id, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["address"], result["name"], result["alpha_score"], result["pnl"],
        result["volume"], result["win_rate"], result["total_trades"],
        result["classification"], result["trust_score"],
        1 if result["should_copy"] else 0,
        1 if result["should_fade"] else 0,
        result["reasoning"], batch_id, now
    ))
    conn.commit()


def heuristic_skip(wallet):
    """Quick heuristic classification without LLM, returns result dict or None if LLM needed."""
    a = wallet["alpha_score"] or 0
    pnl = wallet["pnl"] or 0
    vol = wallet["volume"] or 0
    wr = wallet["win_rate"] or 0
    trades = wallet["total_trades"] or 0
    
    # No data at all
    if trades == 0 and pnl == 0 and vol == 0:
        return {
            "classification": "unknown",
            "trust_score": 1,
            "should_copy": False,
            "should_fade": False,
            "reasoning": "No trade data available"
        }
    
    # Top skilled humans
    if a >= 80 and wr >= 0.65 and trades >= 10 and pnl > 0:
        return {
            "classification": "skilled_human",
            "trust_score": 9,
            "should_copy": True,
            "should_fade": False,
            "reasoning": f"Elite: α={a:.0f} WR={wr:.0%} PnL=${pnl:,.0f}"
        }
    
    # Good performers
    if a >= 70 and wr >= 0.45 and trades >= 5 and pnl > 0:
        return {
            "classification": "skilled_human",
            "trust_score": 7,
            "should_copy": True,
            "should_fade": False,
            "reasoning": f"Strong: α={a:.0f} WR={wr:.0%} PnL=${pnl:,.0f}"
        }
    
    # Bad performers - sacrificial / degenerate
    if wr < 0.30 and trades >= 50 and pnl < 0:
        return {
            "classification": "sacrificial_account" if abs(pnl/vol) > 0.3 else "degenerate_human",
            "trust_score": 2,
            "should_copy": False,
            "should_fade": True,
            "reasoning": f"High loss: WR={wr:.0%} PnL=${pnl:,.0f} on {trades} trades"
        }
    
    # Heavy traders with near-random performance → bot
    if trades >= 200 and 0.35 <= wr <= 0.65 and abs(pnl) < 10000:
        return {
            "classification": "trading_bot",
            "trust_score": 4,
            "should_copy": False,
            "should_fade": True,
            "reasoning": f"High volume ({trades} trades) with near-random WR={wr:.0%}"
        }
    
    # High volume degenerate
    if vol >= 100000 and wr < 0.30:
        return {
            "classification": "degenerate_human",
            "trust_score": 3,
            "should_copy": False,
            "should_fade": True,
            "reasoning": f"High volume ${vol:,.0f} but only {wr:.0%} WR"
        }
    
    # Use LLM for ambiguous cases
    return None


def main():
    start_time = time.time()
    print(f"[{datetime.now(timezone.utc).isoformat()}] 🐋 Whale Intelligence Pipeline v2 Starting", flush=True)
    print(f"  Model: {LLM_MODEL} @ 1700", flush=True)
    print(f"  Output: {OUTPUT_PATH}", flush=True)
    sys.stdout.flush()

    conn = get_db()
    ensure_table(conn)
    
    wallets = load_wallets(conn)
    total = len(wallets)
    print(f"  Loaded {total} wallets", flush=True)
    sys.stdout.flush()

    # Check what's already analyzed
    existing = set()
    for r in conn.execute("SELECT address FROM whale_intelligence").fetchall():
        existing.add(r["address"])
    
    # Stats tracking
    stats = {"heuristic": 0, "llm": 0, "skipped_existing": 0, "errors": 0}
    classifications = {}
    copy_recommended = []
    llm_batch = []  # Wallets needing LLM
    
    # Phase 1: Heuristic classification
    print(f"\n  Phase 1: Heuristic classification...", flush=True)
    for w in wallets:
        if w["address"] in existing:
            stats["skipped_existing"] += 1
            continue
        
        result = heuristic_skip(w)
        if result is not None:
            normalized = {**result, "address": w["address"], "name": w["name"],
                         "alpha_score": w["alpha_score"], "pnl": w["pnl"],
                         "volume": w["volume"], "win_rate": w["win_rate"],
                         "total_trades": w["total_trades"]}
            store_result(conn, normalized, 0)
            stats["heuristic"] += 1
            c = result["classification"]
            classifications[c] = classifications.get(c, 0) + 1
            if result["should_copy"]:
                copy_recommended.append(w["name"])
        else:
            llm_batch.append(w)
    
    print(f"  Heuristic: {stats['heuristic']} classified, {len(llm_batch)} need LLM", flush=True)
    if stats["heuristic"] > 0:
        print(f"  Distribution: {', '.join(f'{c}={n}' for c,n in sorted(classifications.items(), key=lambda x:-x[1]))}", flush=True)
    sys.stdout.flush()
    
    # Phase 2: LLM classification (only for ambiguous wallets)
    print(f"\n  Phase 2: LLM classification of {len(llm_batch)} wallets...", flush=True)
    
    # Process with status tracking
    total_llm = len(llm_batch)
    for idx, w in enumerate(llm_batch):
        print(f"    [{idx+1}/{total_llm}] {w['name'][:25]:25s} α={w['alpha_score']:3.0f} PnL=${w['pnl']:>8,.0f} Vol=${w['volume']:>8,.0f}...", end=" ", flush=True)
        
        prompt = build_prompt(w)
        raw = call_llm(prompt)
        
        if raw and not raw.startswith("__ERROR__"):
            parsed = extract_json(raw)
            if parsed:
                result = normalize(parsed, w)
                store_result(conn, result, idx + 1)
                stats["llm"] += 1
                c = result["classification"]
                classifications[c] = classifications.get(c, 0) + 1
                if result["should_copy"]:
                    copy_recommended.append(w["name"])
                print(f"→ {c}", flush=True)
            else:
                print(f"→ PARSING_ERROR (len={len(raw)})", flush=True)
                stats["errors"] += 1
        else:
            err = raw.replace("__ERROR__: ", "") if raw else "no_response"
            print(f"→ ERROR: {err[:60]}", flush=True)
            stats["errors"] += 1
    
    # Summary
    elapsed = time.time() - start_time
    print(f"\n  {'='*40}", flush=True)
    print(f"  Pipeline Complete!", flush=True)
    print(f"  Time: {elapsed:.0f}s ({elapsed/60:.1f} min)", flush=True)
    print(f"  Total wallets: {total}", flush=True)
    print(f"  Heuristic: {stats['heuristic']}", flush=True)
    print(f"  LLM: {stats['llm']}", flush=True)
    print(f"  Existing (skipped): {stats['skipped_existing']}", flush=True)
    print(f"  Errors: {stats['errors']}", flush=True)
    print(f"  Copy recommendations: {len(copy_recommended)}", flush=True)
    if classifications:
        print(f"  Distribution:", flush=True)
        for c, n in sorted(classifications.items(), key=lambda x: -x[1]):
            print(f"    {c}: {n}", flush=True)
    sys.stdout.flush()
    
    # Write JSON output
    print(f"\n  Writing JSON output...", flush=True)
    all_results = [dict(r) for r in conn.execute("""
        SELECT * FROM whale_intelligence
        ORDER BY alpha_score DESC, pnl DESC
    """).fetchall()]
    
    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "total_wallets": total,
        "analyzed": len(all_results),
        "models_used": {
            "heuristic": stats["heuristic"],
            "llm": stats["llm"],
        },
        "stats": {
            "by_classification": classifications,
        },
        "copy_recommendations": [
            {
                "name": w["name"],
                "alpha_score": w["alpha_score"],
                "classification": w["classification"],
                "trust_score": w["trust_score"],
                "reasoning": w["reasoning"],
                "pnl": w["pnl"],
            }
            for w in all_results if w["should_copy"] == 1
        ],
        "profiles": all_results,
    }
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    conn.close()
    
    print(f"  Output written to {OUTPUT_PATH}", flush=True)
    print(f"  [{datetime.now(timezone.utc).isoformat()}] Done.", flush=True)


if __name__ == "__main__":
    main()
