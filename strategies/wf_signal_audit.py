"""Whale Follower — LLM signal scoring and paper exit audit logging.

Contains:
- llm_score_signal: Score whale signals using local LLM
- log_paper_exit_conditions: Audit trail for sports position exit rules
"""

from __future__ import annotations

import re

from strategies.whale_tracker_new import WhaleSignal
from strategies.wf_constants import (
    SPORTS_WHITELIST_PATTERNS,
    SPORTS_WHALE_BLACKLIST,
    SPORTS_EXIT_HOURS_BEFORE_EVENT,
    SPORTS_AUTO_EXIT_LOSS,
    SPORTS_DAILY_LOSS_LIMIT,
)
from strategies.wf_sports import is_sports_market, get_market_event_time


def llm_score_signal(
    *,
    signal: WhaleSignal,
    log,
) -> int:
    """Score a whale signal using a local LLM (Qwen3.5-9B).

    Args:
        signal: The WhaleSignal to score.
        log: Logger instance.

    Returns:
        Score from 1-10, or 5 on error.
    """
    try:
        import urllib.request as ureq
        import json

        prompt = (
            f"Rate signal quality 1-10 for whale {signal.whale_name} "
            f"on {getattr(signal, 'market_title', 'unknown')}. "
            f"Confidence: {getattr(signal, 'confidence', 0):.0%}. "
            f"Edge: {getattr(signal, 'edge_score', 0):.2f}. "
            f"Return ONLY a number."
        )
        req = ureq.Request(
            "http://localhost:8080/v1/chat/completions",
            data=json.dumps(
                {
                    "model": "qwen3.5-9b",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 8,
                    "temperature": 0.0,
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with ureq.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode())
            text = body["choices"][0]["message"]["content"]
            nums = re.findall(r"\d+", text.replace('"', "").replace(" ", ""))
            score = int(nums[0]) if nums else 5
            return max(1, min(10, score))
    except Exception as e:
        log.warning("LLM score failed", extra={"error": str(e)})
        return 5


def log_paper_exit_conditions(
    *,
    signal: WhaleSignal,
    instrument_id_str: str,
    log,
) -> None:
    """Log paper exit conditions for a sports signal.

    Records the exit rules that will apply to this sports position at the
    time of signal processing, creating an audit trail that prevents
    future divergence between expected and actual exit behavior.

    Args:
        signal: The WhaleSignal being processed.
        instrument_id_str: The instrument ID string for market lookup.
        log: Logger instance.
    """
    is_sports_flag, sport_type = is_sports_market(
        getattr(signal, "market_title", "") or ""
    )
    if not is_sports_flag:
        return

    timing = get_market_event_time(getattr(signal, "market_title", "") or "")
    hours_until_event = timing.get("hours_until_event")

    entry_price = getattr(signal, "target_price", 0.5) or 0.5
    whale_name = signal.whale_name or "unknown"
    market_title = (getattr(signal, "market_title", "") or "")[:60]

    # Check whitelist status for sports exit signals
    is_whitelisted = any(
        re.search(p, market_title, re.IGNORECASE) for p in SPORTS_WHITELIST_PATTERNS
    )

    # Check blacklist status
    on_blacklist = whale_name in SPORTS_WHALE_BLACKLIST

    # Construct exit condition log message
    blacklist_note = " | blacklisted" if on_blacklist else ""
    whitelist_note = " | whitelisted=Spread" if is_whitelisted else " | not-whitelisted"

    if hours_until_event is not None and hours_until_event > 0:
        exit_in_hours = max(0, hours_until_event - SPORTS_EXIT_HOURS_BEFORE_EVENT)
        log.info(
            "SPORTS_PAPER_EXIT",
            extra={
                "market_title": market_title,
                "whale": whale_name,
                "blacklist_note": blacklist_note,
                "whitelist_note": whitelist_note,
                "entry_price": entry_price,
                "hours_until_event": hours_until_event,
                "exit_in_hours": exit_in_hours,
                "auto_exit_loss": SPORTS_AUTO_EXIT_LOSS,
                "daily_loss_limit": SPORTS_DAILY_LOSS_LIMIT,
                "sport": sport_type,
            },
        )
    else:
        log.info(
            "SPORTS_PAPER_EXIT",
            extra={
                "market_title": market_title,
                "whale": whale_name,
                "blacklist_note": blacklist_note,
                "whitelist_note": whitelist_note,
                "entry_price": entry_price,
                "event_time": "N/A (no timing data)",
                "auto_exit_loss": SPORTS_AUTO_EXIT_LOSS,
                "daily_loss_limit": SPORTS_DAILY_LOSS_LIMIT,
                "sport": sport_type,
            },
        )

    # Log blacklist divergence warning if whale is blacklisted but we did not reject
    if on_blacklist:
        log.warning(
            "SPORTS_EXIT_DIVERGENCE",
            extra={
                "whale": whale_name,
                "note": "signal passed the blacklist check above (post-check divergence). Verify that SPORTS_WHALE_BLACKLIST contains the latest blacklisted whales.",
            },
        )

    # Register entry price for paper exit divergence tracking
    if instrument_id_str:
        log.info(
            "PAPER_ENTRY: sports",
            extra={
                "instrument_id": instrument_id_str[:30],
                "price": entry_price,
                "whale": whale_name,
            },
        )
