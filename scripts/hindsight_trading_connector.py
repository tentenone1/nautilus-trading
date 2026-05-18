#!/usr/bin/env python3
"""
Hindsight Trading Connector
Bridges the AutoResearch Analyst with the Hindsight Memory Bank (Port 8888).
"""

import json
import urllib.request
import logging

HINDSIGHT_URL = "http://localhost:8888"

def store_research_fact(market_title, fact_description):
    """Save a research insight into the collective trading brain."""
    payload = json.dumps({
        "action": "add",
        "bank": "trading-wisdom",
        "content": f"MARKET: {market_title} | INSIGHT: {fact_description}",
        "tags": "autoresearch,polymarket"
    }).encode()
    try:
        req = urllib.request.Request(f"{HINDSIGHT_URL}/query", data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

def recall_market_wisdom(market_title):
    """Retrieve past research for a specific market or similar matches."""
    payload = json.dumps({
        "action": "search",
        "bank": "trading-wisdom",
        "query": market_title,
        "limit": 3
    }).encode()
    try:
        req = urllib.request.Request(f"{HINDSIGHT_URL}/query", data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return []

if __name__ == "__main__":
    # Test
    print(store_research_fact("Lakers vs Nuggets", "LeBron James confirmed STARTING despite ankle report."))
