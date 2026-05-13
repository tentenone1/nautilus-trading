#!/usr/bin/env python3
"""Whale Intelligence Pipeline — incremental batch processor for 5900X LM Studio.

Reads whales from whale_discovery.db, classifies via 5900X LLM,
saves results to whale_intelligence table after each whale.
"""

import json
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone

DB_PATH = "pipeline/data/whale_discovery.db"
PROFILES_PATH = "research/whale_profiles.json"
LLM_URL = "http://127.0.0.1:8080/v1/chat/completions"
LLM_MODEL = "Qwen3.6-35B-A3B-Uncensored-HauhauCS-Aggressive-IQ2_M"
BATCH_SIZE = 5  # processed per run
LLM_TIMEOUT = 120


def query_llm(prompt: str) -> str:
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You classify traders. Output a JSON object. No explanations."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 1200,
        "temperature": 0.15,
        "reasoning_format": "none",
    }).encode()
    try:
        req = urllib.request.Request(LLM_URL, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=LLM_TIMEOUT) as resp:
            data = json.loads(resp.read())
        msg = data["choices"][0]["message"]
        content = msg.get("content", "") or ""
        reasoning = msg.get("reasoning_content", "") or ""
        return content if content else reasoning
    except Exception as e:
        return f"ERROR: {e}"


def analyze_whale(whale: dict) -> dict:
    name = whale.get("name", "Unknown")
    alpha = whale.get("alpha_score", 0) or 0
    pnl = whale.get("pnl", 0) or 0
    volume = whale.get("volume", 0) or 0
    wr = whale.get("win_rate", 0) or 0
    trades = whale.get("total_trades", 0) or 0
    tags = whale.get("tags", "[]") or "[]"
    category = whale.get("market_category", "unknown") or "unknown"

    # Heuristic: no trade data → classify based on available data
    if trades == 0 or (pnl == 0 and volume == 0):
        if alpha >= 70 and abs(pnl) > 10000:
            # High alpha + high PnL but zero trades — likely data gap, still a real whale
            return {
                "classification": "skilled_human",
                "trust_score": 6,
                "should_copy": 1,
                "should_fade": 0,
                "reasoning": f"No trades tracked but alpha={alpha:.0f} and PnL=${pnl:,.0f} suggest real activity",
                "llm_raw": "heuristic_skip",
            }
        return {
            "classification": "sacrificial_account",
            "trust_score": 3,
            "should_copy": 0,
            "should_fade": 1,
            "reasoning": "No trade data available — zero trades or zero volume",
            "llm_raw": "heuristic_skip",
        }

    prompt = f"""Analyze this Polymarket whale.

WHALE: {name}
Alpha Score: {alpha:.0f}/100
PnL: ${pnl:,.0f}
Volume: ${volume:,.0f}
Win Rate: {wr:.0%}
Total Trades: {trades}
Category: {category}
Tags: {tags}

Pick ONE classification: skilled_human, degenerate_human, trading_bot, market_maker, sacrificial_account, mixed_entity
Pick trust_score 0-10, should_copy yes/no, should_fade yes/no.

JSON format:
{{"classification":"REPLACE_ME","trust_score":5,"should_copy":"no","should_fade":"no","why":"reason"}}"""

    result = query_llm(prompt)

    # Parse JSON from response — Qwen models prepend thinking, find JSON anywhere
    cleaned = result
    # Remove think tags
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<think>", "", cleaned)
    cleaned = re.sub(r"</think>", "", cleaned)

    # Find the LAST complete JSON object (balances braces)
    json_str = None
    # Find all potential JSON start positions
    for start_idx in [m.start() for m in re.finditer(r"\{", cleaned)]:
        # Try to find matching closing brace (handle nesting)
        depth = 0
        for end_idx in range(start_idx, len(cleaned)):
            if cleaned[end_idx] == "{":
                depth += 1
            elif cleaned[end_idx] == "}":
                depth -= 1
                if depth == 0:
                    candidate = cleaned[start_idx:end_idx+1]
                    if "REPLACE_ME" in candidate or "chosen_type" in candidate or "TYPE" in candidate:
                        # Template literal, not actual classification — skip this candidate
                        continue
                    if "classification" in candidate:
                        json_str = candidate
                    break

    if not json_str:
        # Fallback: find JSON-like block
        json_match = re.search(r"\{[^{}]*classification[^{}]*\}", cleaned, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)

    if json_str:
        try:
            parsed = json.loads(json_str)
            return {
                "classification": parsed.get("classification", "unknown"),
                "trust_score": int(parsed.get("trust_score", 5)),
                "should_copy": 1 if str(parsed.get("should_copy", "no")).lower() in ("yes", "true") else 0,
                "should_fade": 1 if str(parsed.get("should_fade", "no")).lower() in ("yes", "true") else 0,
                "reasoning": (parsed.get("why", "") or parsed.get("reasoning", "") or "")[:500],
                "llm_raw": result,
            }
        except (json.JSONDecodeError, ValueError):
            pass

    return {
        "classification": "unknown",
        "trust_score": 5,
        "should_copy": 0,
        "should_fade": 0,
        "reasoning": f"parse_failed: {result[:200]}",
        "llm_raw": result,
    }


def save_to_db(db: sqlite3.Connection, whale: dict, profile: dict, batch_id: int):
    db.execute("""INSERT OR REPLACE INTO whale_intelligence
        (address, name, alpha_score, pnl, volume, win_rate, total_trades,
         classification, trust_score, should_copy, should_fade, reasoning, batch_id, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
        whale["address"],
        whale["name"],
        whale.get("alpha_score", 0),
        whale.get("pnl", 0),
        whale.get("volume", 0),
        whale.get("win_rate", 0),
        whale.get("total_trades", 0),
        profile["classification"],
        profile["trust_score"],
        profile["should_copy"],
        profile["should_fade"],
        profile["reasoning"],
        batch_id,
        datetime.now(timezone.utc).isoformat(),
    ))
    db.commit()


def sync_profiles_json(db: sqlite3.Connection):
    """Sync the whale_profiles.json file from DB state."""
    rows = db.execute("""
        SELECT address, name, alpha_score, pnl, volume, win_rate, total_trades,
               classification, trust_score, should_copy, should_fade, reasoning, analyzed_at
        FROM whale_intelligence ORDER BY analyzed_at DESC
    """).fetchall()
    profiles = []
    for r in rows:
        stats = {
            "name": r[1],
            "alpha_score": r[2],
            "total_pnl": r[3],
            "total_volume": r[4],
            "win_rate": r[6],
            "total_trades": r[6],
        }
        profile_entry = {
            "whale": r[1],
            "classification": r[7],
            "trust_score": r[8],
            "should_copy": bool(r[9]),
            "should_fade": bool(r[10]),
            "reasoning": r[11],
        }
        profiles.append({"stats": stats, "profile": profile_entry})

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "profiles": profiles,
    }
    with open(PROFILES_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)


def main():
    start = time.time()
    db = sqlite3.connect(DB_PATH)
    total_processed = 0

    while True:
        # Get next batch_id
        last_batch = db.execute("SELECT COALESCE(MAX(batch_id), 0) FROM whale_intelligence").fetchone()[0]
        batch_id = last_batch + 1

        # Find unprocessed whales sorted by alpha_score DESC
        unprocessed = db.execute("""
            SELECT w.address, w.name, w.alpha_score, w.pnl, w.volume,
                   w.win_rate, w.total_trades, w.tags, w.market_category
            FROM whales w
            LEFT JOIN whale_intelligence wi ON w.address = wi.address
            WHERE wi.address IS NULL
            ORDER BY w.alpha_score DESC
            LIMIT ?
        """, (BATCH_SIZE,)).fetchall()

        if not unprocessed:
            total_done = db.execute("SELECT COUNT(*) FROM whale_intelligence").fetchone()[0]
            total_all = db.execute("SELECT COUNT(*) FROM whales").fetchone()[0]
            elapsed = time.time() - start
            print(f"\n[whale-intel] ALL DONE. {total_done}/{total_all} whales processed ({elapsed:.0f}s total)", flush=True)
            break

        cols = ["address", "name", "alpha_score", "pnl", "volume", "win_rate", "total_trades", "tags", "market_category"]
        whales = [dict(zip(cols, r)) for r in unprocessed]

        print(f"[whale-intel] Batch {batch_id}: Processing {len(whales)} whales", flush=True)

        for i, whale in enumerate(whales):
            print(f"  [{i+1}/{len(whales)}] {whale['name'][:30]}... ", end="", flush=True)
            profile = analyze_whale(whale)
            save_to_db(db, whale, profile, batch_id)
            total_processed += 1
            print(f"{profile['classification']} | trust={profile['trust_score']}/10 | "
                  f"copy={'Y' if profile['should_copy'] else 'N'} "
                  f"fade={'Y' if profile['should_fade'] else 'N'}", flush=True)

        # Sync profiles JSON after each batch
        sync_profiles_json(db)

        total_done = db.execute("SELECT COUNT(*) FROM whale_intelligence").fetchone()[0]
        total_all = db.execute("SELECT COUNT(*) FROM whales").fetchone()[0]
        elapsed = time.time() - start
        print(f"  → {total_done}/{total_all} whales done ({elapsed:.0f}s elapsed, {total_processed} in this run)", flush=True)

    db.close()


if __name__ == "__main__":
    main()
