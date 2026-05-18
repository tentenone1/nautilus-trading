#!/usr/bin/env python3
"""Jailbreak Deep Analysis Cron — uncensored whale behavior analysis.

Uses Qwen3.6-35B-A3B-Uncensored on localhost:8080 to analyze whale trading
patterns without censorship. Outputs edge signals for nautilus strategy.

Schedule: every 6 hours
Output: ~/workspace/nautilus-trading/research/jailbreak_analysis.json
"""

import json
import os
import sqlite3
import time
import urllib.request as ureq
from datetime import datetime, timezone

# Resolve relative to the script's location (works on both Mac and 1700)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NAUTILUS_ROOT = os.path.dirname(SCRIPT_DIR)  # goes up from scripts/ to nautilus-trading/
DB_PATH = os.path.join(NAUTILUS_ROOT, "research", "trades.db")
BACKUP_DB_PATH = os.path.join(NAUTILUS_ROOT, "research", "trades.db.archive")
OUTPUT_PATH = os.path.join(NAUTILUS_ROOT, "research", "jailbreak_analysis.json")
LLM_URL = "http://127.0.0.1:8080/v1/chat/completions"
LLM_MODEL = "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive"
MIN_TRADES = 5  # Minimum trades for analysis


def query_llm(prompt: str) -> str:
    """Query the uncensored model. Returns text response."""
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You are a gambling behavior analyst. Analyze whale betting patterns, detect strategy, identify weaknesses. Be direct and specific. Give concrete numbers and patterns. No moralizing."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 4000,
        "temperature": 0.3,
    }).encode()
    try:
        req = ureq.Request(LLM_URL, data=payload, headers={"Content-Type": "application/json"})
        with ureq.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read())
        msg = data["choices"][0]["message"]
        content = msg.get("content", "") or ""
        reasoning = msg.get("reasoning_content", "") or ""
        # Model is reasoning variant — content comes through reasoning_content
        return content if content else reasoning
    except Exception as e:
        return f"LLM error: {e}"


def main():
    # Try primary DB first, fall back to backup if insufficient data
    db_path = DB_PATH
    db = sqlite3.connect(db_path)

    # Check if backup has more data than primary
    primary_count = 0
    backup_count = 0
    try:
        primary = sqlite3.connect(DB_PATH)
        primary_count = primary.execute(
            "SELECT COUNT(DISTINCT whale_name) FROM trades WHERE whale_name IS NOT NULL AND whale_name != '' AND whale_name NOT LIKE 'unknown%'"
        ).fetchone()[0]
        primary.close()
    except Exception:
        pass
    try:
        backup = sqlite3.connect(BACKUP_DB_PATH)
        backup_count = backup.execute(
            "SELECT COUNT(DISTINCT whale_name) FROM trades WHERE whale_name IS NOT NULL AND whale_name != '' AND whale_name NOT LIKE 'unknown%'"
        ).fetchone()[0]
        backup.close()
    except Exception:
        pass

    # Use backup if it has significantly more named whales
    if backup_count > primary_count * 3 and backup_count >= 3:
        db_path = BACKUP_DB_PATH
        db = sqlite3.connect(db_path)
        print(f"[jailbreak] Primary DB has {primary_count} named whales, backup has {backup_count} — using backup", flush=True)
    else:
        print(f"[jailbreak] Using primary DB ({primary_count} named whales)", flush=True)

    # Get all whales with enough trade data
    whales = db.execute("""
        SELECT 
            whale_name,
            COUNT(*) as trades,
            SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buys,
            SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sells,
            COALESCE(AVG(position_size_usd), 0) as avg_bet,
            COALESCE(MAX(position_size_usd), 0) as max_bet,
            COALESCE(MIN(position_size_usd), 0) as min_bet,
            COUNT(DISTINCT condition_id) as unique_markets,
            COUNT(DISTINCT category) as categories,
            GROUP_CONCAT(DISTINCT category) as cat_list,
            COALESCE(SUM(realized_pnl), 0) as total_pnl,
            COUNT(CASE WHEN actual_pnl > 0 THEN 1 END) as wins,
            COUNT(CASE WHEN actual_pnl < 0 THEN 1 END) as losses
        FROM trades 
        WHERE whale_name IS NOT NULL AND whale_name != '' AND whale_name NOT LIKE 'unknown%'
        GROUP BY whale_name
        HAVING trades >= ?
        ORDER BY total_pnl ASC
    """, (MIN_TRADES,)).fetchall()

    results = []
    for w in whales:
        name, trades, buys, sells, avg_bet, max_bet, min_bet, markets, cats, cat_list, pnl, wins, losses = w
        total_closed = wins + losses
        wr = wins / total_closed if total_closed > 0 else 0
        side_bias = "BUY" if buys > sells * 1.5 else ("SELL" if sells > buys * 1.5 else "balanced")

        whale_data = {
            "name": name,
            "trades": trades,
            "win_rate": round(wr, 3),
            "total_pnl": round(pnl, 2),
            "avg_bet": round(avg_bet, 2),
            "max_bet": round(max_bet, 2),
            "side_bias": side_bias,
            "unique_markets": markets,
            "categories": cat_list[:100] if cat_list else "unknown",
        }

        # Batch whales into groups of 3 for LLM analysis to save tokens
        results.append(whale_data)

    # Analyze top losers and top winners with LLM
    top_losers = sorted(results, key=lambda x: x["total_pnl"])[:5]
    top_winners = sorted(results, key=lambda x: x["total_pnl"], reverse=True)[:5]

    analysis_text = "=== TOP LOSERS (most negative PnL) ===\n"
    for w in top_losers:
        analysis_text += f"- {w['name']}: {w['trades']} trades, {w['win_rate']:.0%} WR, ${w['total_pnl']:.0f} PnL, avg ${w['avg_bet']:.0f}/bet, bias={w['side_bias']}, markets={w['unique_markets']}\n"

    analysis_text += "\n=== TOP WINNERS (most positive PnL) ===\n"
    for w in top_winners:
        analysis_text += f"- {w['name']}: {w['trades']} trades, {w['win_rate']:.0%} WR, ${w['total_pnl']:.0f} PnL, avg ${w['avg_bet']:.0f}/bet, bias={w['side_bias']}, markets={w['unique_markets']}\n"

    prompt = f"""Analyze these Polymarket whale betting patterns. Today {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC.

{analysis_text}

For each whale, tell me:
1. Are they a skilled bettor or lucky gambler?
2. What strategy do they seem to use?
3. Should we COPY or FADE them?
4. Any unusual patterns or red flags?

Be specific. Give a concrete recommendation for each."""
    
    llm_output = query_llm(prompt)

    # Dedicated signal extraction call — ask LLM for pure JSON only
    signals = []
    extract_prompt = f"""From the whale data below, output ONLY a JSON array of COPY/FADE signals. No other text.

Whales:
{analysis_text}

Output format (JSON array only):
[{{"whale":"name","action":"COPY","confidence":0.0-1.0,"reason":"one sentence"}}]

Replace with actual values for each whale. Action must be COPY or FADE."""
    extract_result = query_llm(extract_prompt)
    signals = []
    # Find all JSON-like arrays in output by bracket depth matching
    i = 0
    while i < len(extract_result):
        if extract_result[i] == "[":
            depth = 0
            j = i
            while j < len(extract_result):
                if extract_result[j] == "[":
                    depth += 1
                elif extract_result[j] == "]":
                    depth -= 1
                    if depth == 0:
                        candidate = extract_result[i:j+1]
                        try:
                            parsed = json.loads(candidate)
                            if isinstance(parsed, list):
                                for s in parsed:
                                    if isinstance(s, dict) and s.get("whale") and s.get("action") in ("COPY", "FADE"):
                                        signals.append(s)
                        except json.JSONDecodeError:
                            pass
                        break
                j += 1
            i = j + 1
        else:
            i += 1
    if not signals:
        print("[jailbreak] No valid JSON signals from extract call", flush=True)

    output = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "whales_analyzed": len(results),
        "analysis": analysis_text,
        "llm_analysis": llm_output,
        "signals": signals,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Jailbreak analysis: {len(results)} whales analyzed → {OUTPUT_PATH}")
    print(f"Signals generated: {len(output['signals'])}")
    for s in output["signals"]:
        print(f"  {s['action']}: {s['whale']}")

    db.close()


if __name__ == "__main__":
    main()
