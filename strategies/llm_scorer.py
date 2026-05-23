"""LLM Signal Scorer -- MiniMax cloud LLM scoring for whale signals.

Extracted from WhaleFollower to decompose the god class. Handles:
  - Building prompts from whale signal attributes
  - Calling MiniMax API with circuit breaker protection
  - Parsing LLM responses into 1-10 scores
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
from typing import Optional

from strategies.wf_circuit_breaker import get_whale_api_breaker, CircuitBreakerOpen


def llm_score_signal(
    signal,
    whale_intel: dict | None = None,
    api_key: str | None = None,
    log_func=None,
) -> int:
    """Score a whale signal using MiniMax cloud LLM.

    Circuit breaker: whale_api (protects MiniMax API calls from cascade failures).

    Args:
        signal: A WhaleSignal object with market/whale attributes.
        whale_intel: Optional dict of whale intelligence data.
        api_key: MiniMax API key. Falls back to MINIMAX_API_KEY env var.
        log_func: Optional logging callable.

    Returns:
        Integer score 1-10. Returns 5 (neutral) on any failure.
    """
    market = getattr(signal, "market_title", "") or ""
    whale = signal.whale_name or "unknown"
    side = getattr(signal, "side", "?") or "?"
    price = getattr(signal, "target_price", 0.5) or 0.5
    category = getattr(signal, "market_category", "") or ""

    prompt = (
        f"Score this Polymarket signal 1-10. "
        f"Market: {market[:80]}. Whale: {whale[:30]}. "
        f"Side: {side} at {price:.3f}. Category: {category}."
    )

    if whale_intel:
        intel = whale_intel.get(signal.whale_name)
        if intel:
            prompt += f" Classification: {intel['classification']}, Trust: {intel['trust_score']}/10."

    if whale in ("unknown", "unknown whale", ""):
        prompt += " Unknown whale, be skeptical."

    system_prompt = "You are a scoring bot. Reply ONLY with a single digit 1-10. Nothing else."

    payload = {
        "model": "MiniMax-M2.7-highspeed",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 500,
        "temperature": 0.01
    }

    key = api_key or os.environ.get("MINIMAX_API_KEY", "")

    def _make_llm_request():
        req = urllib.request.Request(
            "https://api.minimaxi.com/v1/chat/completions",
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + key,
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            raw = data["choices"][0]["message"]["content"]
            # Extract score after last closing tag (MiniMax thinking tags)
            last_close = raw.rfind("</think>")
            if last_close != -1:
                text = raw[last_close + 8:].strip()
            else:
                text = raw.strip()
            nums = re.findall(r"[0-9]+", text)
            score = int(nums[0]) if nums else 5
            return max(1, min(10, score))

    try:
        breaker = get_whale_api_breaker()
        score = breaker.call(_make_llm_request)
        return score
    except CircuitBreakerOpen:
        if log_func:
            log_func("Whale API circuit breaker OPEN -- skipping LLM scoring")
        return 5
    except Exception as e:
        if log_func:
            log_func(f"[LLM] Scoring failed for {whale}: {e} -- using neutral score")
        return 5
