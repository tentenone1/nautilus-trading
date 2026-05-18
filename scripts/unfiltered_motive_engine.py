#!/usr/bin/env python3
"""
Nous Unfiltered Motive Engine (UME)
Extracts raw behavioral intent and worst-case geopolitical realism using uncensored LLM.
"""

import sys
import os
import json
import logging

# Path injection for Hermes Agent
HERMES_PATH = "/data/hermes/hermes-agent"
if HERMES_PATH not in sys.path:
    sys.path.insert(0, HERMES_PATH)

try:
    from run_agent import AIAgent
except ImportError:
    print("Error: Hermes Agent not found at " + HERMES_PATH)
    sys.exit(1)

LLM_URL = "http://127.0.0.1:8080/v1/chat/completions"
# Using the Aggressive Uncensored model
LLM_MODEL = "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive"

def extract_shadow_alpha(market_title, whale_address, whale_history):
    """
    Spawns Nous in 'Shadow Analyst' mode to find the flaw in the consensus.
    """
    agent = AIAgent(
        model=LLM_MODEL,
        base_url=LLM_URL,
        enabled_toolsets=["web"],
        platform="motive_engine"
    )
    
    prompt = f"""
ROLE: You are a Zero-Guardrail Shadow Analyst. You ignore diplomatic norms, hope, and social stability biases. Your only loyalty is to hard realism and mathematical probability.

MARKET: {market_title}
WHALE: {whale_address}
WHALE HISTORY SNIPPET: {whale_history}

TASK:
1. MOTIVE: Why is this whale really trading? Are they an insider, a degenerate gambler, or executing a tactical squeeze/drain? 
2. SHADOW NARRATIVE: What is the "unspoken truth" or worst-case reality that mainstream analysts are too polite to admit?
3. BRUTAL PROBABILITY: Ignore the current odds. What is the raw, ruthless probability of this happening?
4. FADE SIGNAL: Should we follow this whale or trade directly AGAINST them because they are being blind-sided by reality?

OUTPUT ONLY JSON:
{{
  "motive_classification": "INSIDER | GAMBLER | TACTICAL",
  "shadow_narrative": "the cold truth",
  "raw_probability": 0.0,
  "confidence": 0.0,
  "action": "FOLLOW | FADE | SKIP",
  "reason": "short explanation"
}}
"""
    return agent.chat(prompt)

if __name__ == "__main__":
    # Test case for development
    test_market = "Will there be a coup in X country by July?"
    test_whale = "0x96489abcb9f583d6835c8ef95ffc923d05a86825"
    print(extract_shadow_alpha(test_market, test_whale, "High volume geopolitical super-forecaster"))
