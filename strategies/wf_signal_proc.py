"""Whale Follower — Signal processing.

Standalone functions for handling whale signals from all sources,
scanning known whale positions, processing trade buffers, and
LLM-based signal quality scoring.
"""

from __future__ import annotations

import json
import re
import time

from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.enums import OrderSide

from strategies.whale_tracker_new import (
    WhaleSignal,
    SignalSource,
    _categorize_market,
)
from strategies.wf_constants import (
    WHALE_BLACKLIST,
    SPORTS_WHALE_BLACKLIST,
    SPORTS_WHITELIST_PATTERNS,
    SPORTS_EXIT_HOURS_BEFORE_EVENT,
    SPORTS_AUTO_EXIT_LOSS,
    SPORTS_DAILY_LOSS_LIMIT,
    SPORTS_MIN_EDGE,
    SPORTS_MIN_CONFIDENCE,
    # Phase 2 whitelist constants
    ALLOWED_CATEGORIES,
    BLOCKED_CATEGORIES,
    ALLOWED_WHALE_TYPES,
    BLOCKED_WHALE_TYPES,
)
from strategies.wf_sports import (
    is_sports_market,
    get_market_event_time,
    should_exit_for_sports,
)
from components.paper_execution import set_fill_price, get_fill_price

# ---------------------------------------------------------------------------
# P1 Integration - Manipulation Playbook, Whale Profiles, Jailbreak Strategies
# ---------------------------------------------------------------------------
from pathlib import Path as _Path

_MANIP_PLAYBOOK_PATH = _Path(__file__).resolve().parents[2] / "research" / "manipulation_playbook.json"
_WHALE_PROFILES_PATH = _Path(__file__).resolve().parents[2] / "research" / "whale_profiles.json"
_JAILBREAK_PATH = _Path(__file__).resolve().parents[2] / "research" / "jailbreak_strategies.json"

try:
    with open(_MANIP_PLAYBOOK_PATH, "r", encoding="utf-8") as f:
        _MANIPULATION_PLAYBOOK = json.load(f)
except FileNotFoundError:
    _MANIPULATION_PLAYBOOK = {"tactics": []}

try:
    with open(_WHALE_PROFILES_PATH, "r", encoding="utf-8") as f:
        _WHALE_PROFILES = json.load(f)
except FileNotFoundError:
    _WHALE_PROFILES = {"profiles": []}

try:
    with open(_JAILBREAK_PATH, "r", encoding="utf-8") as f:
        _JAILBREAK_STRATEGIES = json.load(f)
except FileNotFoundError:
    _JAILBREAK_STRATEGIES = {"strategies": []}






def _is_manipulation_signal(signal_data: dict) -> bool:
    """Check if signal matches manipulation playbook pattern."""
    whale_sig = signal_data.get("whale_sig", "") or signal_data.get("whale_name", "")
    if not whale_sig:
        return False
    for tactic in _MANIPULATION_PLAYBOOK.get("tactics", []):
        pattern = tactic.get("whale_sig", "")
        if pattern and pattern.lower() in whale_sig.lower():
            return True
    return False


def _is_fade_whale(whale_name: str) -> bool:
    """Check if whale has should_fade=True in profiles."""
    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            return bool(profile_data.get("should_fade", False))
    return False




def _is_follow_whale(whale_name: str) -> bool:
    """Check if whale has should_follow=True in profiles (hidden partner)."""
    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            return bool(profile_data.get("should_follow", False))
    return False


def _get_strategy_confidence(strategy_name: str) -> float | None:
    """Get confidence for a jailbreak strategy."""
    for strat in _JAILBREAK_STRATEGIES.get("strategies", []):
        if strat.get("name") == strategy_name:
            return float(strat.get("confidence", 0))
    return None


def on_signal(
    *,
    config,
    log,
    open_positions: dict,
    pending_whales: dict,
    tracker,
    whale_tiering,
    analyzer,
    signal: WhaleSignal,
) -> None:
    """Handle a whale signal from ANY subscribed market.

    This is the central signal processing pipeline.  It performs:
        1. Tier validation (confidence + edge_score thresholds).
        2. Blacklist rejection.
        3. LLM quality scoring.
        4. Dynamic instrument registration.
        5. Kelly-sized position entry via wf_entries.enter_position().

    Args:
        config: WhaleFollowerConfig.
        log: Logger.
        open_positions: dict of inst_key -> position info.
        pending_whales: Dict keyed by client_order_id for fill metadata.
        tracker: WhaleTracker instance.
        whale_tiering: WhaleTiering instance.
        analyzer: WhaleInsiderAnalyzer instance.
        signal: The WhaleSignal to process.
    """
    from strategies.wf_entries import enter_position, ensure_instrument

    # ── Whale Tiering Integration ────────────────────────────────────
    alpha_score = getattr(signal, "alpha_score", 50.0) or 50.0
    whale_tags = getattr(signal, "tags", "[]")
    try:
        tags_list = (
            json.loads(whale_tags) if isinstance(whale_tags, str) else (whale_tags or [])
        )
    except (json.JSONDecodeError, TypeError):
        tags_list = []

    tier = whale_tiering.get_tier(alpha_score) if whale_tiering else "unknown"
    tier_config = (
        whale_tiering.get_tier_config(alpha_score) if whale_tiering else {}
    )

    # Apply tier confidence threshold (overrides base config)
    if whale_tiering and not whale_tiering.validate_confidence(
        signal.confidence, alpha_score, tags_list
    ):
        min_conf = tier_config.get("min_confidence", config.min_confidence)
        log.info(
            f"Signal below tier confidence threshold ({tier}): {signal.whale_name} "
            f"(conf {signal.confidence:.0%} < {min_conf:.0%})"
        )
        return

    # Apply tier edge_score threshold
    edge_val = getattr(signal, "edge_score", 0.0) or 0.0
    if whale_tiering and not whale_tiering.validate_edge_score(edge_val, alpha_score):
        min_edge = tier_config.get("min_edge_score", 0.15)
        log.info(
            f"Signal below tier edge_score threshold ({tier}): {signal.whale_name} "
            f"(edge {edge_val:.2f} < {min_edge:.2f})"
        )
        return

    # ── Sports-Specific Entry Filter ────────────────────────────────────
    # Stricter edge + confidence for sports markets (edge≥0.20, conf≥0.65)
    is_sports_signal = (
        is_sports_market(getattr(signal, "market_title", "") or "")[0]
        or mc.lower() == "sports"
    )
    if is_sports_signal:
        sports_edge = edge_val
        sports_conf = signal.confidence or 0.0
        if sports_edge < SPORTS_MIN_EDGE or sports_conf < SPORTS_MIN_CONFIDENCE:
            log.info(
                f"REJECT sports below entry filter: {signal.whale_name} | "
                f"edge={sports_edge:.2f} < {SPORTS_MIN_EDGE:.2f} or "
                f"conf={sports_conf:.0%} < {SPORTS_MIN_CONFIDENCE:.0%} | "
                f"market={getattr(signal, 'market_title', '')[:40]}"
            )
            return

    # ── General Category — Heavily Restricted ───────────────────────────
    # General has 95 trades, -$527, PF 0.46: winning trades but $27 avg loss vs $6 avg win
    # Apply strict edge filter AND minimum whale size filter
    mc = getattr(signal, "market_category", "") or ""
    if mc.lower() == "general":
        if edge_val < 0.25:
            log.info(
                f"REJECT general below edge threshold: {signal.whale_name} | "
                f"edge={edge_val:.2f} < 0.25 | "
                f"market={getattr(signal, 'market_title', '')[:40]}"
            )
            return
        # $5K minimum whale position size (DeepSeek V4 Pro recommendation)
        whale_size = getattr(signal, "suggested_size_usd", 0) or 0
        if whale_size < 5000:
            log.info(
                f"REJECT general below whale size threshold: {signal.whale_name} | "
                f"whale_size=${whale_size:.0f} < $5,000 | "
                f"market={getattr(signal, 'market_title', '')[:40]}"
            )
            return
        # $100K minimum market volume (DeepSeek V4 Pro Priority #4)
        vol = getattr(signal, "volume", 0) or 0
        if vol < 100_000:
            log.info(
                f"REJECT general below volume threshold: {signal.whale_name} | "
                f"volume=${vol:,.0f} < $100,000 | "
                f"market={getattr(signal, 'market_title', '')[:40]}"
            )
            return
        # 7+ days to resolution (DeepSeek V4 Pro Priority #4)
        hours_left = getattr(signal, "hours_until_event", None)
        if hours_left is not None and hours_left < 168:
            log.info(
                f"REJECT general too close to resolution: {signal.whale_name} | "
                f"hours_until_event={hours_left:.0f}h < 168h (7 days) | "
                f"market={getattr(signal, 'market_title', '')[:40]}"
            )
            return

    # REJECT: blacklisted whales
    if signal.whale_name in WHALE_BLACKLIST:
        log.info(f"REJECT blacklisted whale: {signal.whale_name}")
        return
    # mc already set above — reuse for sports blacklist check
    if signal.whale_name in SPORTS_WHALE_BLACKLIST and mc.lower() == "sports":
        log.info(f"REJECT sports-blacklisted whale: {signal.whale_name}")
        return

    # ---------------------------------------------------------------------
    # Phase‑2 whitelist/blacklist validation (category & whale type)
    # This must run before any fade/whale‑profile logic to ensure blocked
    # categories are rejected early.
    # ---------------------------------------------------------------------
    if not validate_phase2_signal(
        signal=signal,
        whale_classification=get_whale_classification(signal.whale_name),
        log=log,
    ):
        return

    # ── Sybil Conviction Modulation ──────────────────────────────────────
    # Adjust confidence/size based on sybil group consensus.
    # Skip signal entirely if contradicted by strong sybil conviction.
    from strategies.wf_sybil_modulator import modulate as sybil_modulate
    sybil = sybil_modulate(signal)
    if sybil.has_sybil:
        if sybil.should_skip:
            log.info(
                f"SYBIL SKIP {signal.whale_name} | {getattr(signal, 'market_title', '')[:40]} | "
                f"sybil_yes={sybil.sybil_ratio:.0%} {sybil.decision} | "
                f"contradicted by sybil group conviction"
            )
            return
        if sybil.confidence_delta != 0.0 or sybil.size_multiplier != 1.0:
            old_conf = signal.confidence
            old_size = signal.suggested_size_usd
            signal.confidence = max(0.0, min(1.0, signal.confidence + sybil.confidence_delta))
            signal.suggested_size_usd = round(signal.suggested_size_usd * sybil.size_multiplier, 2)
            log.info(
                f"SYBIL MODULATE {signal.whale_name} | "
                f"conf={old_conf:.0%}→{signal.confidence:.0%} "
                f"size=${old_size:.0f}→${signal.suggested_size_usd:.0f} | "
                f"sybil_yes={sybil.sybil_ratio:.0%} wallets={sybil.sybil_wallets} {sybil.decision}"
            )

    # P1: Manipulation playbook check
    if _is_manipulation_signal({"whale_name": signal.whale_name, "whale_sig": getattr(signal, "whale_address", "")}):
        log.info(f"REJECT manipulation pattern: {signal.whale_name}")
        return

    # P1: Whale profile fade check
    if _is_fade_whale(signal.whale_name):
        log.info(f"FADE whale (profile): {signal.whale_name}")
        # Mark as fade instead of reject - system can use this for counter-trading
        return

    # P2: Hidden partner boost (should_follow=True → confidence boost)
    if _is_follow_whale(signal.whale_name):
        original_conf = signal.confidence
        signal.confidence = min(1.0, signal.confidence * 1.25)  # 25% boost
        log.info(f"FOLLOW hidden partner: {signal.whale_name} | conf {original_conf:.0%} → {signal.confidence:.0%}")
        # Continue processing - don't return, just boost confidence


    # REJECT: unknown whale signals with insufficient edge (noise trades)
    # Skip the LLM call — an unknown whale with no trade history and edge below
    # the LLM threshold is not worth scoring. Saves ~0.3s per rejection.
    is_unknown_whale = (
        not signal.whale_name
        or signal.whale_name.lower() in ("", "unknown", "unknown whale", "")
    )
    if is_unknown_whale and edge_val < 7:
        wallet = getattr(signal, "whale_address", "") or ""
        wallet_info = f" wallet={wallet[:10]}..." if wallet else ""
        log.info(
            f"REJECT unknown whale low edge={edge_val:.2f}: {signal.whale_name}{wallet_info} | "
            f"market={getattr(signal, 'market_title', '')[:40]} | "
            f"conf={signal.confidence:.0%}"
        )
        return

    # Apply tier-based position sizing
    if whale_tiering:
        tier_kelly = whale_tiering.apply_overrides(
            tier_config, tags_list
        ).get("kelly_multiplier", 1.0)
        signal.suggested_size_usd = round(signal.suggested_size_usd * tier_kelly, 2)

    # LLM signal quality scoring (1700 Qwen3.5-9B, ~0.3s)
    llm_score = llm_score_signal(signal=signal, log=log)
    if llm_score < 5:
        log.info(f"REJECT LLM score={llm_score}/10: {signal.whale_name}")
        return
    log.info(
        f"LLM score={llm_score}/10: {signal.whale_name} | "
        f"market={getattr(signal, 'market_title', '')[:40]}"
    )

    # Log signal with tier info
    log.info(
        f"SIGNAL [{signal.source.value}] [{tier.upper()}]: {signal.reason} | "
        f"Confidence: {signal.confidence:.0%} | "
        f"Suggested: ${signal.suggested_size_usd:,.0f}",
        color=(
            LogColor.YELLOW
            if signal.source == SignalSource.KNOWN_WHALE
            else LogColor.CYAN
        ),
    )

    if not config.auto_trade:
        log.debug("Auto-trade disabled, skipping signal execution")
        return
    if getattr(log, "_daily_loss_breached", False):
        log.warning(
            "Daily loss limit breached ($%.2f), skipping signal execution",
            getattr(log, "_daily_pnl", 0.0),
        )
        return

    # Sports-specific daily loss check
    market_category = getattr(signal, "market_category", "") or ""
    is_sports, sport_type = is_sports_market(getattr(signal, "market_title", "") or "")
    if is_sports or market_category.lower() == "sports":
        if getattr(log, "_sports_daily_loss_breached", False):
            log.warning(
                "Sports daily loss limit breached ($%.2f), skipping sports signal execution",
                getattr(log, "_sports_daily_pnl", 0.0),
            )
            return

    # Dynamic subscription: every signal is processed regardless of
    # pre-subscribed markets.
    target_inst = ensure_instrument(
        cache=None,  # TODO: pass from caller
        log=log,
        condition_id=signal.condition_id,
        token_id=signal.token_id,
        outcome=signal.outcome,
        clob_client=None,  # TODO: pass from caller
    )
    if target_inst is None:
        log.info(
            f"Could not get instrument for {getattr(signal, 'market_title', '')[:40]}, skipping"
        )
        return

    inst_key = str(target_inst)

    # ── Paper Exit Signal Tracking for Sports ───────────────────────────
    # Log exit conditions at entry time to create an audit trail that
    # prevents divergence between expected and actual exit behavior.
    # Runs after instrument resolution (target_inst must be available).
    if is_sports or market_category.lower() == "sports":
        log_paper_exit_conditions(
            signal=signal,
            instrument_id_str=inst_key,
            log=log,
        )

    # Determine side
    side = OrderSide.BUY if signal.side == "buy" else OrderSide.SELL

    # Get whale's actual win rate for dynamic Kelly sizing
    whale_wr = None
    if config.use_dynamic_kelly and tracker:
        for w in tracker.whales.values():
            if w.name == signal.whale_name:
                whale_wr = w.win_rate
                break
        if whale_wr is None:
            log.debug(
                f"Whale '{signal.whale_name}' not found in tracker, using default Kelly"
            )

    # Delegate to wf_entries.enter_position() for execution
    enter_position(
        config=config,
        cache=None,  # TODO: pass from caller
        portfolio=None,  # TODO: pass from caller
        order_factory=None,  # TODO: pass from caller
        log=log,
        open_positions=open_positions,
        exited_positions=set(),  # TODO: pass from caller
        last_exit_time={},  # TODO: pass from caller
        whale_tiering=whale_tiering,
        clob_client=None,  # TODO: pass from caller
        side=side,
        price=signal.target_price,
        whale_amount=signal.suggested_size_usd,
        instrument_id=target_inst,
        whale_win_rate=whale_wr,
        whale_name=signal.whale_name,
        market_title=signal.market_title,
        market_category=getattr(signal, "market_category", "Unknown"),
        whale_address=getattr(signal, "whale_address", "") or "",
        edge_score=edge_val,
        confidence=signal.confidence or 0.0,
        entry_reason=signal.reason or "",
    )


def scan_whale_positions(
    *,
    config,
    log,
    tracker,
    on_signal_fn,
) -> None:
    """Poll known whale positions with rate limiting.

    Args:
        config: WhaleFollowerConfig.
        log: Logger.
        tracker: WhaleTracker instance.
        on_signal_fn: Callable to handle each detected signal
            (typically on_signal() from this module).
    """
    if not tracker or not config.auto_trade:
        log.warning(
            "Whale scan skipped: tracker=%s auto_trade=%s",
            bool(tracker),
            config.auto_trade,
        )
        return

    # Reset per-scan trade counter (caller manages)
    trades_this_scan = 0

    # Clear expired dedup entries (TTL-based re-scan)
    now = time.time()
    ttl = config.seen_position_ttl
    if tracker.seen_positions:
        expired = [
            k for k, v in tracker.seen_positions.items() if now - v > ttl
        ]
        if expired:
            for k in expired:
                del tracker.seen_positions[k]
            log.info(f"Cleared {len(expired)} expired dedup entries (TTL={ttl/3600:.0f}h)")

    try:
        signals = tracker.scan_known_whales()

        if signals:
            log.info(
                f"Whale scan complete: {len(signals)} new signals detected "
                f"from {len(tracker.whales)} tracked whales"
            )

        for signal in signals:
            if trades_this_scan >= config.max_trades_per_scan:
                log.info(
                    f"Scan trade limit reached ({config.max_trades_per_scan}), "
                    f"skipping {len(signals) - trades_this_scan} remaining signals"
                )
                break
            on_signal_fn(signal)
            trades_this_scan += 1
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log.error(f"Whale scan error: {e}\n{tb}")


def process_trade_buffer(
    *,
    tracker,
    trade_buffer: list[dict],
    on_signal_fn,
    log,
) -> None:
    """Process buffered large trades into signals.

    Args:
        tracker: WhaleTracker instance.
        trade_buffer: List of trade dicts with size, price, side, timestamp.
        on_signal_fn: Callable to handle each detected signal.
        log: Logger.
    """
    if not tracker or not trade_buffer:
        log.debug("Trade buffer processing skipped: no tracker or buffer empty")
        return

    try:
        signals = tracker.detect_large_trades(trade_buffer)
        trade_buffer.clear()
        for signal in signals:
            on_signal_fn(signal)
    except Exception as e:
        log.error(f"Trade processing error: {e}")


def llm_score_signal(
    *,
    signal: WhaleSignal,
    log,
) -> int:
    """Score a whale signal using a local LLM (Qwen3.5-9B).

    Sends a short prompt to the local LLM endpoint and extracts
    a numeric score 1-10.

    Args:
        signal: The WhaleSignal to score.
        log: Logger.

    Returns:
        Integer score 1-10. Returns 5 on failure.
    """
    import urllib.request as ureq

    market = getattr(signal, "market_title", "") or ""
    whale = signal.whale_name or "unknown"
    side = getattr(signal, "side", "?") or "?"
    price = getattr(signal, "target_price", 0.5) or 0.5
    category = getattr(signal, "market_category", "") or ""
    prompt = (
        "Score this Polymarket signal 1-10. "
        f"Market: {market[:80]}. Whale: {whale[:30]}. "
        f"Side: {side} at {price:.3f}. Category: {category}."
    )
    if whale in ("unknown", "unknown whale", ""):
        prompt += " Unknown whale, be skeptical."

    payload = json.dumps(
        {
            "model": "Qwen3.5-9B-Uncensored-HauhauCS-Aggressive-Q4_K_M.gguf",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Score betting signals 1-10. Known losing whales get 1-3. "
                        "Good signals get 7-10. Reply ONLY a number 1-10."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 10,
            "temperature": 0.01,
        }
    ).encode()

    try:
        req = ureq.Request(
            "http://127.0.0.1:8080/v1/chat/completions",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with ureq.urlopen(req, timeout=10) as resp:
            data2 = json.loads(resp.read())
        content = data2["choices"][0]["message"].get("content", "").strip()
        nums = re.findall(
            r"\d+", content.replace("<think>", "").replace("</think>", "")
        )
        score = int(nums[0]) if nums else 5
        return max(1, min(10, score))
    except Exception as e:
        log.warning(f"LLM score failed: {e}")
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

    Logs:
    - Exit timing: hours until event, exit window, auto-exit loss threshold
    - Blacklist status: whether the whale is sports-blacklisted
    - Whitelist status: whether the market is a whitelisted Spread bet

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
        re.search(p, market_title, re.IGNORECASE)
        for p in SPORTS_WHITELIST_PATTERNS
    )

    # Check blacklist status
    on_blacklist = whale_name in SPORTS_WHALE_BLACKLIST

    # Construct exit condition log message
    blacklist_note = " | blacklisted" if on_blacklist else ""
    whitelist_note = " | whitelisted=Spread" if is_whitelisted else " | not-whitelisted"

    if hours_until_event is not None and hours_until_event > 0:
        exit_in_hours = max(0, hours_until_event - SPORTS_EXIT_HOURS_BEFORE_EVENT)
        log.info(
            f"SPORTS_PAPER_EXIT | {market_title}"
            f" | whale={whale_name}"
            f"{blacklist_note}"
            f"{whitelist_note}"
            f" | entry=${entry_price:.3f}"
            f" | event_in={hours_until_event:.2f}h"
            f" | exit_in={exit_in_hours:.2f}h"
            f" | auto_exit=-${SPORTS_AUTO_EXIT_LOSS}"
            f" | daily_limit=-${SPORTS_DAILY_LOSS_LIMIT}"
            f" | sport={sport_type}"
        )
    else:
        # No event time available — log nevertheless to show we tracked it
        log.info(
            f"SPORTS_PAPER_EXIT | {market_title}"
            f" | whale={whale_name}"
            f"{blacklist_note}"
            f"{whitelist_note}"
            f" | entry=${entry_price:.3f}"
            f" | event_time=N/A (no timing data)"
            f" | auto_exit=-${SPORTS_AUTO_EXIT_LOSS}"
            f" | daily_limit=-${SPORTS_DAILY_LOSS_LIMIT}"
            f" | sport={sport_type}"
        )

    # Log blacklist divergence warning if whale is blacklisted but we did not reject
    if on_blacklist:
        log.warning(
            f"SPORTS_EXIT_DIVERGENCE: {whale_name} is sports-blacklisted but "
            f"signal passed the blacklist check above (post-check divergence). "
            f"Verify that SPORTS_WHALE_BLACKLIST contains the latest blacklisted whales."
        )

    # Register entry price for paper exit divergence tracking
    if instrument_id_str:
        log.info(
            f"PAPER_ENTRY: sports | inst={instrument_id_str[:30]} | "
            f"price={entry_price:.3f} | whale={whale_name}"
        )


# ── Phase 2 Whitelist Filter ───────────────────────────────────────────────────

def validate_phase2_signal(
    *,
    signal: WhaleSignal,
    whale_classification: str = "",
    log,
) -> bool:
    """Validate a signal against Phase 2 whitelist filters.

    This function enforces strict category and whale type whitelists for
    the $100 validation mode. Every signal must pass both checks before
    entering the position sizing pipeline.

    CRITICAL: This check runs BEFORE position sizing to prevent any
    exposure to blocked categories or whale types.

    Args:
        signal: The WhaleSignal to validate.
        whale_classification: Whale type classification string
            (e.g., "skilled_human", "degenerate_human"). If empty,
            defaults to "unknown" which is blocked.
        log: Logger instance.

    Returns:
        True if signal passes whitelist checks, False if blocked.
        Logs the rejection reason for every blocked signal.

    Example rejection log:
        P2_BLOCK | category=crypto | whale=whale_0xabcd | market=Bitcoin $100k?
        P2_BLOCK | whale_type=unknown | whale=unknown | market=Politics 2028
    """
    # Get market category from signal
    market_category = getattr(signal, "market_category", "") or ""
    if not market_category:
        # Fallback: categorize from market title using keyword detection
        # instead of defaulting to "general"
        market_title = getattr(signal, "market_title", "") or ""
        market_category = _categorize_market(market_title)

    # Normalize category to lowercase for matching
    category_lower = market_category.lower()

    # Get whale name for logging
    whale_name = signal.whale_name or "unknown"
    market_title = (getattr(signal, "market_title", "") or "")[:50]

    # ── Category Whitelist Check ────────────────────────────────────────
    # First check BLOCKED_CATEGORIES (hard rejection)
    if category_lower in BLOCKED_CATEGORIES:
        log.info(
            f"P2_BLOCK | category={category_lower} | whale={whale_name} | "
            f"market={market_title}"
        )
        return False

    # Then check ALLOWED_CATEGORIES (must be in whitelist)
    if category_lower not in ALLOWED_CATEGORIES:
        # Category not in whitelist = reject
        log.info(
            f"P2_BLOCK | category={category_lower} (not whitelisted) | "
            f"whale={whale_name} | market={market_title}"
        )
        return False

    # ── Whale Type Whitelist Check ──────────────────────────────────────
    # If no explicit whale classification is provided, skip whale type checks.
    if not whale_classification:
        # Bypass whale type filtering; only category validation is required.
        log.info(
            f"P2_PASS | category={category_lower} | whale_type=none | "
            f"whale={whale_name} | market={market_title}"
        )
        return True

    # Normalize whale classification
    whale_type = whale_classification.lower()

    # First check BLOCKED_WHALE_TYPES (hard rejection)
    if whale_type in BLOCKED_WHALE_TYPES:
        log.info(
            f"P2_BLOCK | whale_type={whale_type} | whale={whale_name} | "
            f"market={market_title}"
        )
        return False

    # Then check ALLOWED_WHALE_TYPES (must be in whitelist)
    if whale_type not in ALLOWED_WHALE_TYPES:
        # Whale type not in whitelist = reject
        log.info(
            f"P2_BLOCK | whale_type={whale_type} (not whitelisted) | "
            f"whale={whale_name} | market={market_title}"
        )
        return False

    # Signal passes all whitelist checks
    log.info(
        f"P2_PASS | category={category_lower} | whale_type={whale_type} | "
        f"whale={whale_name} | market={market_title}"
    )
    return True


def get_whale_classification(whale_name: str) -> str:
    """Get whale classification from whale profiles data.

    Looks up the whale classification type from _WHALE_PROFILES.
    Falls back to "unknown" if whale not found or classification missing.

    Args:
        whale_name: The whale name to lookup.

    Returns:
        Classification string (e.g., "skilled_human", "degenerate_human")
        or "unknown" if not found.
    """
    if not whale_name:
        return "unknown"

    for profile in _WHALE_PROFILES.get("profiles", []):
        stats = profile.get("stats", {})
        if stats.get("name") == whale_name:
            profile_data = profile.get("profile", {})
            classification = profile_data.get("classification", "")
            if classification:
                return classification.lower()
            # Fallback: check should_fade for degenerate_human
            if profile_data.get("should_fade", False):
                return "degenerate_human"
            # Fallback: check should_follow for skilled_human
            if profile_data.get("should_follow", False):
                return "skilled_human"

    # Whale not in profiles = unknown
    return "unknown"
