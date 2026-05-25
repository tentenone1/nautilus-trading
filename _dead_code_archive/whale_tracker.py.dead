"""Combined Whale Detection System for Polymarket.

Combines three signal sources:
1. Known whale wallet position tracking (CemeterySun, CarlosMC, benwyatt)
2. WebSocket large trade detection (real-time trades >$5k)
3. Uncensored model analysis for insider edge detection

Uses public data-api.polymarket.com and Nautilus WebSocket streams.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import requests


class SignalSource(Enum):
    """Where the signal came from."""

    KNOWN_WHALE = "known_whale"  # Tracked wallet position
    LARGE_TRADE = "large_trade"  # Unknown wallet, large trade
    MODEL_INSIDER = "model_insider"  # Detected by uncensored model


@dataclass
class WhaleIdentity:
    """Known whale wallet with performance metrics."""

    name: str
    proxy_wallet: str
    win_rate: float
    avg_trade_size: float
    style: str = ""
    notes: str = ""
    roi: float = 0.0


# Known profitable whales from V2 research
KNOWN_WHALES = [
    WhaleIdentity(
        name="CemeterySun",
        proxy_wallet="0x4bbe10ba5b7f6df147c0dae17b46c44a6e562cf3",
        win_rate=0.62,
        avg_trade_size=50000,
        style="event_driven",
        notes="Event-driven trader, high volume",
        roi=0.62,
    ),
    WhaleIdentity(
        name="CarlosMC",
        proxy_wallet="0x96489abcb9f583d6835c8ef95ffc923d05a86825",
        win_rate=0.58,
        avg_trade_size=65000,
        style="contrarian",
        notes="Contrarian style, large positions",
        roi=0.58,
    ),
    WhaleIdentity(
        name="benwyatt",
        proxy_wallet="0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        win_rate=0.60,
        avg_trade_size=60000,
        style="research_based",
        notes="Research-based trader",
        roi=0.60,
    ),
]


@dataclass
class WhaleSignal:
    """Trading signal from whale activity."""

    source: SignalSource
    condition_id: str
    outcome: str
    side: str  # "buy" or "sell"
    confidence: float  # 0-1
    target_price: float
    suggested_size_usd: float
    whale_name: str  # "CemeterySun", "Unknown Whale", "Model Detected"
    timestamp: float
    reason: str = ""
    market_title: str = ""


class WhaleTracker:
    """Combined whale detection system."""

    DATA_API = "https://data-api.polymarket.com"
    STATE_FILE = "/home/elon-1/workspace/nautilus-trading/data/whale_state.json"
    SCAN_INTERVAL = 30  # seconds
    LARGE_TRADE_THRESHOLD = 5000  # USD
    MIN_POSITION_SIZE = 1000  # USD

    def __init__(self):
        self.whales = {w.proxy_wallet: w for w in KNOWN_WHALES}
        self.seen_positions: dict[
            str, float
        ] = {}  # "wallet:conditionId:outcome" -> timestamp
        self.signal_history: list[WhaleSignal] = []
        self.last_scan_time: float = 0
        self._last_sizes: dict = {}
        self._load_state()

    def _log(self, msg: str) -> None:
        print(f"[WhaleTracker] {msg}")

    def _load_state(self) -> None:
        try:
            if os.path.exists(self.STATE_FILE):
                with open(self.STATE_FILE, "r") as f:
                    data = json.load(f)
                self.seen_positions = data.get("seen_positions", {})
                self.last_scan_time = data.get("last_scan_time", 0)
                self._log(f"Loaded state: {len(self.seen_positions)} seen positions")
        except Exception as e:
            self._log(f"Failed to load state: {e}")

    def _save_state(self) -> None:
        try:
            state = {
                "seen_positions": self.seen_positions,
                "last_scan_time": self.last_scan_time,
            }
            os.makedirs(os.path.dirname(self.STATE_FILE), exist_ok=True)
            with open(self.STATE_FILE, "w") as f:
                json.dump(state, f, indent=2, default=str)
        except Exception as e:
            self._log(f"Failed to save state: {e}")

    def register_whale(self, whale: WhaleIdentity) -> None:
        """Add new whale to tracking (e.g., from model analysis)."""
        self.whales[whale.proxy_wallet] = whale
        self._log(f"Added whale: {whale.name} ({whale.win_rate:.0%} WR)")

    def scan_known_whales(self) -> list[WhaleSignal]:
        """Poll positions for known whales."""
        now = time.time()
        if now - self.last_scan_time < self.SCAN_INTERVAL:
            return []

        signals = []
        for wallet, whale in self.whales.items():
            positions = self._fetch_positions(wallet)
            for pos in positions:
                signal = self._process_position(pos, whale, now)
                if signal:
                    signals.append(signal)
                    self.signal_history.append(signal)

        self.last_scan_time = now
        self._save_state()
        return signals

    def _fetch_positions(self, address: str) -> list[dict]:
        """Fetch wallet positions from data API."""
        url = f"{self.DATA_API}/positions?user={address}"
        try:
            resp = requests.get(url, timeout=15)
            return resp.json() if resp.status_code == 200 else []
        except Exception as e:
            self._log(f"API error for {address}: {e}")
            return []

    def _process_position(
        self, pos: dict, whale: WhaleIdentity, now: float
    ) -> Optional[WhaleSignal]:
        """Process a single position, return signal if new."""
        condition_id = pos.get("conditionId", "")
        outcome = pos.get("outcome", "")
        price = float(pos.get("price", 0))
        size = float(pos.get("size", 0))
        title = pos.get("title", "")

        if size < self.MIN_POSITION_SIZE or price <= 0.001:
            return None  # Skip tiny/expired

        pos_key = f"{whale.proxy_wallet}:{condition_id}:{outcome}"
        last_size = self._last_sizes.get(pos_key, 0)

        if last_size > 0 and size <= last_size * 1.10:
            return None

        self._last_sizes[pos_key] = size
        self.seen_positions[pos_key] = now

        # Signal generation
        side = "buy"
        signal_type = SignalSource.KNOWN_WHALE
        confidence = min(whale.win_rate + abs(price - 0.5) * 0.5, 0.95)
        suggested = size * 0.25  # 25% of whale's size

        return WhaleSignal(
            source=signal_type,
            condition_id=condition_id,
            outcome=outcome,
            side=side,
            confidence=confidence,
            target_price=price,
            suggested_size_usd=suggested,
            whale_name=whale.name,
            timestamp=now,
            reason=f"{whale.name} ({whale.win_rate:.0%} WR, {whale.style}) {side} {outcome} ${size:,.0f} @ {price:.3f}",
            market_title=title,
        )

    def detect_large_trades(self, trades: list[dict]) -> list[WhaleSignal]:
        """Process TradeTick stream data for large trades."""
        signals = []
        now = time.time()

        for trade in trades:
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            usd = size * price

            if usd < self.LARGE_TRADE_THRESHOLD:
                continue

            condition_id = trade.get("conditionId", "")
            outcome = trade.get("outcome", "")
            side_raw = trade.get("side", "BUY")
            side = "buy" if side_raw == "BUY" else "sell"
            proxy_wallet = trade.get("proxyWallet", "")
            title = trade.get("title", "")

            # Deduplicate
            trade_key = f"{proxy_wallet}:{condition_id}:{now:.0f}"
            if trade_key in self.seen_positions:
                continue
            self.seen_positions[trade_key] = now

            # Confidence based on trade size
            confidence = min(0.50 + (usd / 100000) * 0.2, 0.70)

            signals.append(
                WhaleSignal(
                    source=SignalSource.LARGE_TRADE,
                    condition_id=condition_id,
                    outcome=outcome,
                    side=side,
                    confidence=confidence,
                    target_price=price,
                    suggested_size_usd=usd * 0.25,
                    whale_name="Unknown Whale",
                    timestamp=now,
                    reason=f"Large trade {side} {outcome} ${usd:,.0f} @ {price:.3f}",
                    market_title=title,
                )
            )

        return signals

    def get_signals_for_market(self, condition_id: str) -> list[WhaleSignal]:
        """Get all signals for a specific market."""
        return [s for s in self.signal_history if s.condition_id == condition_id]

    def get_whale_summary(self) -> dict:
        return {
            "whales_tracked": len(self.whales),
            "signals_generated": len(self.signal_history),
            "seen_positions": len(self.seen_positions),
        }
