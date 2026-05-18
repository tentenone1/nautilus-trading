#!/usr/bin/env python3
"""Deep analysis of tracked whales via uncensored LLM on 5900X.

Pulls fresh data from jailbreak_fresh_data.json and sends to Qwen3.6 on 5900X
for detailed behavior analysis, COPY/FADE reclassification, and strategy synthesis.
"""

import json, urllib.request, sys, os

LLM_URL = "http://127.0.0.1:8080/v1/chat/completions"
LLM_MODEL = "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive"

BASE_DIR = "/home/elon-1/workspace/nautilus-trading"
DATA_PATH = os.path.join(BASE_DIR, "research", "jailbreak_fresh_data.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "research", "jailbreak_deep_analysis.json")

# Fix benwyatt and Tony data inline
BENWYATT_DATA = {"total_trades": 50, "buys": 50, "sells": 0, "total_volume_usd": 337065, "categories": {"sports/nba": 35, "other": 15}, "latest_trade": "Lakers vs. Rockets", "latest_price": 0.78}
TONY_DATA = {"total_trades": 0, "buys": 0, "sells": 0, "total_volume_usd": 0, "categories": {}, "latest_trade": "", "latest_price": 0}

def load_data():
    with open(DATA_PATH) as f:
        data = json.load(f)
    # Merge retry data
    if "benwyatt" in data and "error" in data["benwyatt"]:
        data["benwyatt"] = BENWYATT_DATA
    if "Tony (trading wallet)" in data and "error" in data["Tony (trading wallet)"]:
        data["Tony (trading wallet)"] = TONY_DATA
    # Add benwyatt wallet address
    data["benwyatt"]["wallet"] = "0x1117eade222413335b7ec959e5b48c1d3dbc3532"
    data["Tony (trading wallet)"]["wallet"] = "0x970807Acd56ecA1f0179599BeDE25EBeCDDdb86C"
    return data

def query_llm(prompt):
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You are a gambling behavior analyst specializing in Polymarket whale analysis. Analyze trading patterns with precision. Output structured JSON. No moralizing. Be direct."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 8000,
        "temperature": 0.3,
    }).encode()
    try:
        req = urllib.request.Request(LLM_URL, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            result = json.loads(resp.read())
        msg = result["choices"][0]["message"]
        content = msg.get("content", "") or ""
        reasoning = msg.get("reasoning_content", "") or ""
        return content if content else reasoning
    except Exception as e:
        return f"LLM ERROR: {e}"

def main():
    data = load_data()
    
    # Build the analysis prompt
    whale_summaries = []
    for name, info in data.items():
        if "error" in info:
            continue
        whale_summaries.append(f"""WHALE: {name}
  Wallet: {info.get('wallet', 'unknown')[:20]}...
  Trades (last 50): {info['total_trades']}
  Buy/Sell: {info['buys']}/{info['sells']}
  Volume: ${info['total_volume_usd']:,.0f}
  Categories: {info.get('categories', {})}
  Latest: {info.get('latest_trade', 'N/A')} @ ${info.get('latest_price', 0):.4f}""")

    prompt = f"""You are analyzing Polymarket prediction market traders. Below are the last 50 trades for each tracked wallet.

For EACH whale, analyze:
1. TRADING STYLE: What category/market type do they favor? One sentence.
2. SKILL vs LUCK: Is their volume+pattern consistent with skill or noise?
3. COPY or FADE: Should we follow their trades or bet against them?
4. CONFIDENCE: 0.0-1.0
5. KEY INSIGHT: One sentence on what to watch for

Then provide a CROSS-WHALE ANALYSIS:
6. Which 2 whales would make the best COPY pair (diversified)?
7. Which 2 whales to FADE most aggressively?
8. Any coordination/overlap patterns between wallets?
9. Is Tony's wallet active or dormant?

Whales to analyze:
{chr(10).join(whale_summaries)}

RESPOND WITH VALID JSON ONLY:
{{"whales": [
    {{"name": "RJW1", "style": "...", "skill": "skilled|noise|mixed", "action": "COPY|FADE|WATCH", "confidence": 0.0, "insight": "..."}},
    ...
],"cross_analysis": {{
    "best_copy_pair": ["name1", "name2"],
    "most_fade": ["name1", "name2"],
    "coordination_findings": "...",
    "tony_wallet_status": "active|dormant|unknown",
    "overall_recommendation": "..."
}}}}
"""
    
    print("Sending to 5900X Qwen for deep analysis... (this takes ~60s)")
    print(f"Prompt length: {len(prompt)} chars", flush=True)
    
    result = query_llm(prompt)
    
    # Try to parse JSON from response
    json_result = None
    try:
        # Find JSON block
        if "```json" in result:
            json_str = result.split("```json")[1].split("```")[0].strip()
            json_result = json.loads(json_str)
        elif "```" in result:
            json_str = result.split("```")[1].strip()
            json_result = json.loads(json_str)
        else:
            json_result = json.loads(result)
    except:
        pass
    
    output = {
        "generated": __import__('datetime').datetime.now().isoformat(),
        "model": LLM_MODEL,
        "data_source": DATA_PATH,
        "llm_response": result,
        "parsed_json": json_result,
    }
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nAnalysis saved to {OUTPUT_PATH}")
    if json_result:
        print(f"\n=== PARSED RESULTS ===")
        for w in json_result.get("whales", []):
            print(f"  {w['name']:25s} | {w['action']:5s} | conf={w['confidence']:.2f} | {w['style'][:50]}")
        ca = json_result.get("cross_analysis", {})
        print(f"\n  Best COPY pair: {ca.get('best_copy_pair', 'N/A')}")
        print(f"  Most FADE: {ca.get('most_fade', 'N/A')}")
        print(f"  Tony wallet: {ca.get('tony_wallet_status', 'N/A')}")
        print(f"  Coordination: {ca.get('coordination_findings', 'N/A')[:100]}")
    else:
        print(f"\nRaw LLM response (first 1000 chars):")
        print(result[:1000])

if __name__ == "__main__":
    main()
