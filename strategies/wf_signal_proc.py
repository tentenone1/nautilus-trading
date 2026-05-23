"""Whale Signal Processing — Scanner and position polling.

Stripped down to only the functions still in active use:
  - scan_whale_positions: Poll known whale positions with rate limiting
  - get_whale_classification: Get whale behavioral classification

The on_signal() pipeline, process_trade_buffer(), and llm_score_signal()
were moved to wf_signal_handler.py, whale_follower.py, and llm_scorer.py
respectively during the decomposition.
"""

from __future__ import annotations

import time
import json
import re

from strategies.whale_tracker_new import WhaleSignal


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
            (typically self._on_signal from WhaleFollower).
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


def get_whale_classification(whale_name: str) -> str:
    """Get whale classification from classifier or profiles.

    Tries data-driven classification first, falls back to profile-based.
    """
    try:
        from strategies.whale_classifier import WhaleClassifier
        classifier = WhaleClassifier()
        cls = classifier._classifications.get(whale_name)
        if cls:
            return cls.get("classification", "unknown")
    except Exception:
        pass

    # Fall back to profiles
    try:
        import json
        from pathlib import Path
        profiles_path = Path("data/whale_profiles.json")
        if profiles_path.exists():
            with open(profiles_path) as f:
                profiles = json.load(f)
            for profile in profiles.get("profiles", []):
                stats = profile.get("stats", {})
                if stats.get("name") == whale_name:
                    return profile.get("profile", {}).get("classification", "unknown")
    except Exception:
        pass

    return "unknown"
