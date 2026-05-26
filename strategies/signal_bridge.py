"""Signal Bridge -- Autoresearch and Sybil signal queue polling.

Extracted from WhaleFollower to decompose the god class. Handles:
  - Autoresearch LLM signal queue polling and processing
  - Sybil meta-whale signal queue polling, filtering, and processing
  - Sybil signal price validation against live market midpoint

The strategy (WhaleFollower) delegates to SignalBridge for queue polling,
and passes its _on_signal callback for signal injection.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from pathlib import Path
from typing import Callable, Optional

# SYBIL_MAX_PRICE_SLIPPAGE is defined in whale_follower.py module level
SYBIL_MAX_PRICE_SLIPPAGE = 0.15  # Skip if price moved >15% against signal direction



class SignalBridge:
    """Polls external signal queues and injects WhaleSignal objects into the strategy.

    Receives a reference to the strategy for logging and signal injection.
    The strategy's _on_signal method is passed as a callback.
    """

    def __init__(self, strategy):
        """Initialize with a reference to the WhaleFollower strategy instance.

        Args:
            strategy: The WhaleFollower strategy instance. Provides access to
                log, _on_signal callback, and _sybil_price_cache.
        """
        self._s = strategy

        # ── Load market metadata cache (Tier 1 instrument resolution) ─────────────
        # The signal generator pre-fetches market metadata and writes it here during
        # signal generation. Loading it here means _ensure_instrument_for_signal
        # can resolve instruments WITHOUT making a live CLOB API call (cache hit).
        self._market_meta_cache: dict = {}
        self._load_market_meta_cache()

        # Inject the cache into SignalHandler so _ensure_instrument_for_signal
        # can use it as Tier 1 (upstream cache) before falling through to CLOB/Gamma.
        if self._s._signal_handler is not None:
            self._s._signal_handler._market_meta_cache = self._market_meta_cache

    def _load_market_meta_cache(self) -> None:
        """Load market metadata cache from disk (written by signal generator).

        The cache maps condition_id -> market metadata dict. Entries older than
        5 minutes are filtered out on load.
        """
        import time as _time

        cache_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "research", "autoresearch_market_meta_cache.json"
        )
        if not os.path.exists(cache_path):
            self.log.debug("Market metadata cache not found (will use live API)")
            return
        try:
            with open(cache_path) as f:
                raw = json.load(f)
            now = _time.time()
            for cid, meta in raw.items():
                age = now - meta.get("_cached_at", 0)
                if age > 300:  # 5-minute TTL
                    continue
                self._market_meta_cache[cid] = meta
            max_age = 0
            if self._market_meta_cache:
                max_age = max((now - m.get("_cached_at", now) for m in self._market_meta_cache.values()), default=0)
            self.log.info(
                f"Market metadata cache loaded: {len(self._market_meta_cache)} entries "
                f"(5min TTL, oldest entry {max_age:.0f}s old)"
            )
        except (json.JSONDecodeError, OSError) as e:
            self.log.warning(f"Failed to load market metadata cache: {e}")

    @property
    def log(self):
        return self._s.log

    def check_autoresearch_signals(self) -> None:
        """Poll autoresearch signal queue for model-generated trade recommendations."""
        from strategies.whale_tracker_new import WhaleSignal, WhaleSignalType

        queue_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "research", "autoresearch_signal_queue.json"
        )
        if not os.path.exists(queue_path):
            return
        try:
            with open(queue_path) as f:
                signals = json.load(f)
            if not signals or not isinstance(signals, list):
                return
            # Clear the queue immediately to prevent re-processing on crash
            with open(queue_path, "w") as f:
                json.dump([], f)
            processed = 0
            for s in signals:
                signal_obj = WhaleSignal(
                    signal_type=WhaleSignalType.LARGE_POSITION,
                    condition_id=s.get("condition_id", ""),
                    token_id=s.get("token_id", ""),
                    outcome=s.get("outcome", "Yes"),
                    side=s.get("side", "buy"),
                    confidence=s.get("confidence", 0.5),
                    target_price=s.get("entry_price", 0.5),
                    suggested_size_usd=s.get("suggested_size_usd", 0.0),
                    whale_name=s.get("whale_name", "autoresearch_llm"),
                    whale_roi=s.get("whale_roi", 0.0),
                    timestamp=s.get("timestamp", time.time()),
                    reason=s.get("reason", "Autoresearch LLM signal"),
                    market_title=s.get("market_title", ""),
                    market_category=s.get("market_category", ""),
                    whale_address=s.get("whale_address", ""),
                    edge_score=s.get("edge_score", 0.0),
                )
                self._s._on_signal(signal_obj)
                processed += 1
            if processed:
                self.log.info(f"Autoresearch signals: {processed} queued recommendations processed")
        except Exception as e:
            self.log.error(f"Autoresearch signal check failed: {e}")

    def check_sybil_signals(self) -> None:
        """Poll sybil signal queue -- conservative integration.

        Filters: confidence 0.60-0.95, max $100 position.
        Clears queue after processing to prevent re-execution on crash.
        """
        from strategies.whale_tracker_new import WhaleSignal, WhaleSignalType

        queue_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "research", "sybil_signal_queue.json"
        )
        if not os.path.exists(queue_path):
            return
        try:
            with open(queue_path) as f:
                data = json.load(f)
            signals = data.get("signals", []) if isinstance(data, dict) else []
            if not signals:
                return
            # Clear queue immediately to prevent re-processing on crash
            with open(queue_path, "w") as f:
                json.dump({"generated_at": "", "signal_count": 0, "signals": []}, f)

            processed = 0
            for s in signals:
                confidence = s.get("confidence", 0.5)
                # Confidence filter: 0.60-0.95 (high conviction sybil calls above 0.90
                # are typically consensus clusters — these are our best signals)
                if confidence < 0.60 or confidence > 0.95:
                    mt = s.get("market_title", "")
                    self.log.info(f"Sybil signal skipped -- confidence {confidence:.2f} outside 0.60-0.95 zone | {mt}")
                    continue

                # Map side
                sybil_side = s.get("side", "BUY YES")
                if "BUY YES" in sybil_side.upper():
                    side, outcome = "buy", "Yes"
                elif "BUY NO" in sybil_side.upper():
                    side, outcome = "buy", "No"
                else:
                    side, outcome = "buy", "Yes"

                # Position sizing: max $100
                suggested_size = min(100.0, s.get("total_exposure_usd", 0) * 0.01)

                group_id = s.get("group_id", "unknown")
                # Resolve token_id from condition_id via Polymarket API
                sybil_token_id = s.get("token_id", "")
                sybil_condition_id = s.get("condition_id", "")
                if not sybil_token_id and sybil_condition_id:
                    try:
                        market_url = f"https://clob.polymarket.com/markets/{sybil_condition_id}"
                        req2 = urllib.request.Request(market_url, headers={"User-Agent": "nautilus-sybil/1.0"})
                        with urllib.request.urlopen(req2, timeout=5) as resp2:
                            market_data = json.loads(resp2.read().decode())
                        tokens = market_data.get("tokens", [])
                        if tokens:
                            if outcome.lower() == "no":
                                sybil_token_id = tokens[1].get("token_id", "") if len(tokens) > 1 else tokens[0].get("token_id", "")
                            else:
                                sybil_token_id = tokens[0].get("token_id", "")
                    except Exception as tok_err:
                        self.log.warning(f"Sybil token_id lookup failed: {tok_err}")

                signal_obj = WhaleSignal(
                    signal_type=WhaleSignalType.LARGE_POSITION,
                    condition_id=sybil_condition_id,
                    token_id=sybil_token_id,
                    outcome=outcome,
                    side=side,
                    confidence=confidence,
                    target_price=0.5,
                    suggested_size_usd=suggested_size,
                    whale_name=f"sybil_meta_{group_id}",
                    whale_roi=0.0,
                    timestamp=time.time(),
                    reason=s.get("reason", f"Sybil {group_id} signal"),
                    market_title=s.get("market_title", ""),
                    market_category=s.get("market_category", ""),
                    whale_address="",
                    edge_score=confidence * 10,
                )
                self._s._on_signal(signal_obj)
                processed += 1
            if processed:
                self.log.info(f"Sybil signals: {processed} queued signals processed (0.60-0.95 filter)")
        except Exception as e:
            self.log.error(f"Sybil signal check failed: {e}")

    def validate_sybil_signal_price(self, signal: dict) -> tuple[bool, str]:
        """Check current market midpoint has not moved against the signal direction.

        Args:
            signal: A sybil signal dict from the queue.

        Returns:
            (True, reason) if price is favorable for entry,
            (False, reason) if price has moved too far.
        """
        condition_id = signal.get("condition_id", "")
        if not condition_id:
            return True, "no_condition_id"

        now = time.time()
        cached = self._s._sybil_price_cache.get(condition_id)
        if cached and (now - cached[1]) < 30:
            midpoint = cached[0]
        else:
            try:
                url = f"https://clob.polymarket.com/midpoint?condition_id={condition_id}"
                req = urllib.request.Request(url, headers={"User-Agent": "nautilus-sybil/1.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.loads(resp.read().decode())
                midpoint_str = data.get("midpoint") or data.get("price")
                if midpoint_str is None:
                    return True, "no_midpoint"
                midpoint = float(midpoint_str)
                self._s._sybil_price_cache[condition_id] = (midpoint, now)
            except Exception as e:
                self.log.debug(f"Sybil price check failed for {condition_id[:20]}: {e}")
                return True, "api_failed"

        sybil_side = signal.get("side", "BUY YES")
        if "BUY YES" in sybil_side.upper():
            max_entry = 0.5 + SYBIL_MAX_PRICE_SLIPPAGE
            if midpoint > 0.90:
                return False, f"YES price {midpoint:.3f} near certainty"
            if midpoint > max_entry:
                return False, f"YES price {midpoint:.3f} > max entry {max_entry:.3f}"
            return True, f"YES at {midpoint:.3f}"
        else:
            no_price = 1.0 - midpoint
            max_entry = 0.5 + SYBIL_MAX_PRICE_SLIPPAGE
            if no_price > max_entry:
                return False, f"NO price {no_price:.3f} > max entry {max_entry:.3f}"
            return True, f"NO at {no_price:.3f} (YES={midpoint:.3f})"
