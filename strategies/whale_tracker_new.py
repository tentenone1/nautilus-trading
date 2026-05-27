"""Whale wallet tracking engine for Polymarket — Enhanced with persistent state.

Monitors specific high-performing wallets, detects their trades,
and generates trading signals based on their activity.

Uses the public data-api.polymarket.com endpoint with:
- Redis-based persistent state (seen trades, sequence numbers)
- Async API scanning with rate limiting
- Signal validation before publishing
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

import requests

from nautilus_trader.adapters.polymarket.common.symbol import (
    get_polymarket_instrument_id,
)

# Add components directory to path
from pathlib import Path

COMPONENTS_DIR = Path(__file__).parent.parent.parent / "components"
sys_path_base = str(Path(__file__).parent.parent.parent)
if sys_path_base not in __import__("sys").path:
    __import__("sys").path.insert(0, sys_path_base)

from components.state_manager import StateManager
from components.api_rate_limiter import APIRateLimiter
from components.signal_validator import SignalValidator


class WhaleSignalType(Enum):
    """Types of whale signals."""

    BUY_YES = "buy_yes"
    BUY_NO = "buy_no"
    SELL_YES = "sell_yes"
    SELL_NO = "sell_no"
    LARGE_POSITION = "large_position"


# Backward compat: whale_follower.py uses SignalSource
class SignalSource(Enum):
    """Where the signal came from (backward compat with whale_tracker.py)."""

    KNOWN_WHALE = "known_whale"
    LARGE_TRADE = "large_trade"
    MODEL_INSIDER = "model_insider"


# ── REST Price Cache (fallback for WebSocket drops) ───────────────────────────
_PRICE_CACHE = {}
_PRICE_CACHE_TIME = 0
_PRICE_CACHE_TTL = 60  # seconds


def get_market_prices(condition_id: str = None) -> dict:
    """Fetch current market prices from Polymarket data-api.

    Caches results for 60s to avoid rate limits. Returns {condition_id: {yes_price, no_price, volume}}.
    """
    global _PRICE_CACHE, _PRICE_CACHE_TIME
    now = time.time()
    if now - _PRICE_CACHE_TIME < _PRICE_CACHE_TTL and _PRICE_CACHE:
        return _PRICE_CACHE.get(condition_id, {}) if condition_id else _PRICE_CACHE
    try:
        resp = requests.get(
            "https://data-api.polymarket.com/markets",
            params={"limit": 100, "closed": "false"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        prices = {}
        for m in data:
            cond = m.get("condition_id", "")
            if cond:
                prices[cond] = {
                    "yes_price": float(m.get("yes_bid", 0) or m.get("yes_price", 0)),
                    "no_price": float(m.get("no_bid", 0) or m.get("no_price", 0)),
                    "volume": float(m.get("volume", 0)),
                }
        _PRICE_CACHE = prices
        _PRICE_CACHE_TIME = now
        return prices.get(condition_id, {}) if condition_id else prices
    except Exception as e:
        import logging

        logging.getLogger("whale_tracker").debug("REST price fetch failed: %s", e)
        return _PRICE_CACHE.get(condition_id, {}) if condition_id else _PRICE_CACHE


@dataclass
class WhaleIdentity:
    """Known whale wallet with performance metrics."""

    name: str
    proxy_wallet: str  # The on-chain proxy wallet address
    roi: float  # Historical ROI as decimal (0.62 = 62%)
    win_rate: float
    total_trades: int
    avg_trade_size: float  # USD
    style: str = ""
    notes: str = ""


_WHALE_DB_PATH = (
    Path(__file__).resolve().parents[1] / "pipeline" / "data" / "whale_discovery.db"
)

FALLBACK_WHALES = [
    WhaleIdentity(
        name="weflyhigh",
        proxy_wallet="0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        roi=0.86,
        win_rate=0.86,
        total_trades=500,
        avg_trade_size=50000,
        style="top_performer",
        notes="$863K PnL",
    ),
    WhaleIdentity(
        name="Anointed-Connect",
        proxy_wallet="0x8f037a2e4fd49d11267f4ab874ab7ba745ac64d6",
        roi=0.70,
        win_rate=0.70,
        total_trades=300,
        avg_trade_size=40000,
        style="top_performer",
        notes="$269K PnL",
    ),
    WhaleIdentity(
        name="How.Dare.You",
        proxy_wallet="0x4bbe10ba5b7f6df147c0dae17b46c44a6e562cf3",
        roi=0.90,
        win_rate=0.90,
        total_trades=100,
        avg_trade_size=15000,
        style="high_efficiency",
        notes="Alpha=90, $62K PnL",
    ),
    WhaleIdentity(
        name="redskinrick",
        proxy_wallet="0xe24838258b572f1771dffba3bcdde57a78def293",
        roi=0.80,
        win_rate=0.80,
        total_trades=80,
        avg_trade_size=10000,
        style="high_efficiency",
        notes="Alpha=80, $31K PnL",
    ),
]


def make_fallback_name(wallet_addr: str) -> str:
    """Generate a fallback whale name from a wallet address.

    Used when a whale wallet isn't in the known list. Creates an
    identifiable name from the first 6 chars of the address so
    we never store "unknown" or "Unknown Whale" in the DB.
    """
    if wallet_addr and len(wallet_addr) >= 6:
        short = wallet_addr[:6].lower()
        return f"whale_0x{short}"
    return "unknown"


def load_whales_from_db():
    if not _WHALE_DB_PATH.exists():
        print(
            f"DB not found at {_WHALE_DB_PATH}, using {len(FALLBACK_WHALES)} fallback whales"
        )
        return list(FALLBACK_WHALES)
    try:
        conn = sqlite3.connect(str(_WHALE_DB_PATH))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT address, name, alpha_score, pnl, volume, win_rate, total_trades FROM whales WHERE alpha_score >= 60 ORDER BY alpha_score DESC, pnl DESC"
        ).fetchall()
        conn.close()
        whales = []
        for r in rows:
            # Skip whales with zero trades and zero win rate — these are placeholder
            # entries with no real track record and should not be followed
            if (r["total_trades"] or 0) == 0 and (r["win_rate"] or 0.0) == 0.0:
                continue
            avg = (
                25000
                if r["total_trades"] < 5
                else (r["volume"] or 0) / max(r["total_trades"], 1)
            )
            whales.append(
                WhaleIdentity(
                    name=r["name"] or r["address"][:10],
                    proxy_wallet=r["address"],
                    roi=min(max(r["pnl"] / 100000 if r["pnl"] else 0, 0), 1.0),
                    win_rate=r["win_rate"] or 0.5,
                    total_trades=r["total_trades"] or 0,
                    avg_trade_size=avg,
                    style="discovered",
                    notes=f"Alpha={r['alpha_score']:.0f}, PnL=${r['pnl']:,.0f}",
                )
            )
        if not whales:
            return list(FALLBACK_WHALES)
        print(f"Loaded {len(whales)} whales from DB")
        return whales
    except Exception as e:
        print(f"DB error: {e}, using {len(FALLBACK_WHALES)} fallback whales")
        return list(FALLBACK_WHALES)


@dataclass
class WhaleTrade:
    """A single trade by a whale wallet."""

    whale_name: str
    whale_wallet: str
    condition_id: str
    token_id: str
    outcome: str  # "Yes" or "No"
    side: str  # "BUY" or "SELL"
    size: float  # Number of shares
    price: float  # Price per share (0-1)
    usd_value: float  # size * price
    timestamp: float
    whale_address: str = ""
    market_title: str = ""
    market_slug: str = ""


@dataclass
class WhaleSignal:
    """Trading signal generated from whale activity."""

    signal_type: WhaleSignalType
    condition_id: str
    token_id: str
    outcome: str
    side: str  # "buy" or "sell"
    confidence: float  # 0-1, based on whale's historical performance
    target_price: float  # Entry price
    suggested_size_usd: float
    whale_name: str
    whale_roi: float
    timestamp: float
    reason: str = ""
    market_title: str = ""
    market_category: str = ""
    whale_address: str = ""  # proxy wallet address — added 2026-05-02
    edge_score: float = 0.0  # edge score from tracker analysis
    source: str = "known_whale"  # Signal origin: "known_whale", "model_insider", "sybil"


def _categorize_market(title: str) -> str:
    """Simple keyword-based market categorizer."""
    if not title:
        return "general"
    t = title.lower()
    t = " " + t + " "  # pad with spaces for boundary-safe keyword matching
    if any(
        w in t
        for w in [
            "vs.",
            " vs ",
            "spread",
            "spread:",
            "run line",
            "puck line",
            " moneyline",
            "point",
            "over/under",
            "o/u",
            "goal",
            "touchdown",
            " nfl ",
            " nba ",
            " mlb ",
            " nhl ",
            " ufc ",
            "boxing",
            "championship",
            "game",
            "match",
            "player",
            "team",
            "draft",
            "medal",
            "gold",
            "soccer",
            "football",
            "basketball",
            "baseball",
            "hockey",
            "tennis",
            "fight",
            "inning",
            "recruit",
            "champions",
            "uefa",
            "premier",
            "la liga",
            "serie a",
            "bundesliga",
            "europa league",
            "final",
            "cup",
            "score",
            " win ",
            "race",
            "round",
            "series",
            " f1 ",
            "formula",
            "mma",
            "league",
            "olympic",
            "grand slam",
            "playoff",
            "semi",
            "qualif",
            "fc ",
            "fc\b",
            " united",
            "liverpool",
            "city ",
            "real ",
            "barça",
            "barcelona",
            "juventus",
            "bayern",
            "psg",
            " ncaa ",
            "college",
            "athletic",
            "athlete",
            "total",
            "over ",
            "under ",
            "handicap",
            "atp",
            "wta",
            "golf",
            "pga",
            "masters",
            "gp",
            "derby",
            "grand prix",
            "lol",
            "dota",
            "valorant",
            "csgo",
            "counter-strike",
            "esports",
            "lck ",
            "lpl ",
            "cblol",
            "lec ",
            "lcs ",
            "vct ",
            "blast",
            "iem ",
            "esl ",
            "pgl ",
            "world cup",
            "fifa",
            "atp ",
            "wta ",
            "nfl ",
            "mlb ",
            "nhl ",
            "fifa world cup",
        ]
    ):
        return "sports"
    if any(
        w in t
        for w in [
            "president",
            "election",
            "congress",
            "senate",
            "house",
            "governor",
            "senator",
            "democrat",
            "republican",
            "vote",
            "poll",
            "candidate",
            "trump",
            "biden",
            "harris",
            "aoc",
            "newsom",
            "midterm",
            "political",
            "impeach",
            "cabinet",
            "scotus",
            "supreme court",
        ]
    ):
        return "politics"
    if any(
        w in t
        for w in [
            "gdp",
            "inflation",
            "interest rate",
            "fed",
            "recession",
            "unemployment",
            "stock market",
            "tariff",
            "economy",
            "oil",
            "crude",
            "wti",
            "natural gas",
            "copper",
            "commodities",
            "gold price",
        ]
    ):
        return "economics"
    if any(
        w in t
        for w in [
            "bitcoin",
            "ethereum",
            "btc",
            "eth",
            "solana",
            "sol",
            "crypto",
            "token",
            "coin",
            "defi",
            "nft",
            "blockchain",
            "halving",
            "stablecoin",
            "usdc",
            "usdt",
            "price",
            " ath ",
            "up or down",
            "above",
            "below",
            "bitcoin etf",
            "eth etf",
            "layer",
            "arbitrum",
            "optimism",
            "polygon",
            "avalanche",
            "chainlink",
            "uniswap",
            "aave",
            "pump",
            "meme coin",
            "sui",
            "aptos",
            "near",
            "ton",
            "base ",
            "sei",
            "injective",
            "ordinals",
            "manta",
            "zksync",
            "hyperliquid",
            "berachain",
            "monad",
            "movement",
        ]
    ):
        return "crypto"
    if any(
        w in t
        for w in [
            "war",
            "conflict",
            "invasion",
            "sanction",
            "nato",
            "china",
            "russia",
            "ukraine",
            "taiwan",
            "iran",
            "israel",
            "gaza",
            "missile",
            "nuclear",
            "military",
            "ceasefire",
            "geopolitical",
        ]
    ):
        return "geopolitics"
    if any(
        w in t
        for w in [
            "oscar",
            "grammy",
            "emmy",
            "award",
            "movie",
            "film",
            "celebrity",
            "entertainment",
            "box office",
            "billboard",
            "album",
        ]
    ):
        return "entertainment"
    if any(
        w in t
        for w in [
            "ai",
            "artificial intelligence",
            "chatgpt",
            "gpt",
            "openai",
            "space",
            "nasa",
            "spacex",
            "starship",
            "launch",
            "rocket",
            "science",
            "technology",
            "tech",
            "robot",
        ]
    ):
        return "technology"
    if any(
        w in t
        for w in [
            "ipo",
            "merger",
            "acquisition",
            "earnings",
            "revenue",
            "ceo",
            "startup",
            "venture",
            "funding",
            "business",
        ]
    ):
        return "business"
    if any(
        w in t
        for w in [
            "hurricane",
            "earthquake",
            "weather",
            "temperature",
            "climate",
            "storm",
            "flood",
            "wildfire",
        ]
    ):
        return "weather"
    return "general"


class WhaleTracker:
    """Tracks whale wallet activity and generates trading signals.

    Enhanced with:
    - Persistent state (Redis + disk fallback)
    - Async API scanning with rate limiting
    - Signal validation before publishing
    """

    DATA_API = "https://data-api.polymarket.com"
    LARGE_TRADE_THRESHOLD = 5000.0  # USD threshold for large trade detection

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        db: int = 0,
        fallback_memory: bool = True,
        fallback_dir: Optional[str] = None,
        min_confidence: float = 0.60,
        min_trade_size: float = 5000.0,
        max_trade_size: float = 200000.0,
        scan_interval: float = 60.0,
    ):
        """Initialize whale tracker.

        Args:
            redis_url: Redis connection URL
            db: Redis database number
            fallback_memory: If True, also maintain in-memory state
            fallback_dir: Directory for disk backups
            min_confidence: Minimum confidence to accept signal
            min_trade_size: Minimum trade size in USD
            max_trade_size: Maximum trade size in USD
            scan_interval: Seconds between scans (if using time-based scan)
        """
        db_whales = load_whales_from_db()
        self.whales = {w.proxy_wallet: w for w in db_whales}
        self.whale_names = {w.name: w for w in db_whales}

        # Initialize state manager
        self._state_manager = StateManager(
            redis_url=redis_url,
            db=db,
            fallback_memory=fallback_memory,
            fallback_dir=fallback_dir,
        )

        # Initialize rate limiter
        self._rate_limiter = APIRateLimiter(
            base_url=self.DATA_API,
            default_timeout=10.0,
            default_limit=100,
            max_retries=5,
        )

        # Initialize signal validator
        self._validator = SignalValidator(
            min_confidence=min_confidence,
            min_trade_size=min_trade_size,
            max_trade_size=max_trade_size,
        )

        self.seen_trades: set = set()  # Fallback: in-memory dedup
        self.seen_positions: dict = {}  # Backward compat with whale_follower
        self._last_sizes: dict = {}  # track last known position sizes
        self.signal_history: list = []
        self.last_scan_time: float = 0.0
        self.last_scan_offset: int = 0  # For pagination
        self.scan_interval: float = scan_interval
        self.SCAN_INTERVAL: float = scan_interval  # Backward compat alias

        # Load discovered whales from pipeline DB
        self._load_discovered_whales()

    def _fetch_positions(self, address: str) -> list[dict]:
        """Backward compat: fetch wallet positions from data API."""
        try:
            url = f"{self.DATA_API}/positions?user={address}&limit=50"
            resp = requests.get(url, timeout=15)
            return resp.json() if resp.status_code == 200 else []
        except Exception:
            return []

    def _compute_pnl_edge(self, whale_name: str) -> float:
        """Query trades.db to compute PnL-derived edge score for a whale.

        The original edge_score formula (win_rate*0.8 + roi*0.2) was INVERTED:
        high scores correlated with losses, low scores with profits. This method
        replaces it with a score based on actual average PnL from trades.db.

        Formula: clamp(0.3 + (avg_pnl / 500) * 0.5, min=0.1, max=0.9)
        """
        try:
            import os as _os

            trades_db = _os.path.join(
                _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
                "research",
                "trades.db",
            )
            if not _os.path.exists(trades_db):
                return 0.5  # neutral default when no DB available

            conn = sqlite3.connect(trades_db)
            conn.execute("PRAGMA busy_timeout=3000")
            row = conn.execute(
                "SELECT AVG(COALESCE(realized_pnl, 0)) FROM trades "
                "WHERE whale_name = ?",
                (whale_name,),
            ).fetchone()
            conn.close()

            avg_pnl = row[0] if row and row[0] is not None else 0.0
            raw = 0.3 + (avg_pnl / 500.0) * 0.5
            return min(max(raw, 0.1), 0.9)
        except Exception:
            return 0.5  # safe default on error

    def _process_position(
        self, pos: dict, whale: WhaleIdentity, now: float
    ) -> Optional[WhaleSignal]:
        """Backward compat: process a single position, return signal if new."""
        condition_id = pos.get("conditionId", "")
        size = float(pos.get("size", 0))
        # data-api uses avgPrice/curPrice, not "price"
        price = float(pos.get("avgPrice", pos.get("curPrice", pos.get("price", 0))))
        if price <= 0:
            return None  # skip positions without price data
        title = pos.get("title", "")
        # data-api doesn't have "outcome" directly; infer from asset/token info
        outcome = pos.get("outcome", "")
        if not outcome:
            # Try to get from tokens array
            tokens = pos.get("tokens", [])
            if tokens:
                # The asset field usually maps to the token_id
                asset = pos.get("asset", "")
                for t in tokens:
                    if t.get("token_id") == asset:
                        outcome = t.get("outcome", "")
                        break
            if not outcome and tokens:
                outcome = tokens[0].get("outcome", "Yes")

        if size < 100 or price <= 0.001:
            return None

        pos_key = f"{whale.proxy_wallet}:{condition_id}:{outcome}"
        last_size = self._last_sizes.get(pos_key, 0)
        should_signal = False

        # Signal conditions: NEW position, SIZE INCREASE >10%, or DIRECTION FLIP
        if last_size == 0:
            should_signal = True  # NEW position - whale entering market
        elif size > last_size * 1.10:
            should_signal = True  # SIZE INCREASE > 10%
        else:
            # Check for direction flip (outcome changed)
            prev_data = self.seen_positions.get(pos_key, {})
            prev_outcome = prev_data.get("outcome", "")
            if prev_outcome and prev_outcome != outcome:
                should_signal = True  # DIRECTION FLIP

        if not should_signal:
            return None

        # Update tracking
        self.seen_positions[pos_key] = {"timestamp": now, "outcome": outcome}
        self._last_sizes[pos_key] = size

        # Signal generation
        confidence = min(whale.win_rate + abs(price - 0.5) * 0.5, 0.95)
        suggested = size * 0.25

        return WhaleSignal(
            signal_type=WhaleSignalType.BUY_YES
            if outcome.lower() == "yes"
            else WhaleSignalType.BUY_NO,
            condition_id=condition_id,
            token_id=pos.get("asset", pos.get("token_id", "")),
            outcome=outcome,
            side="buy",
            confidence=confidence,
            target_price=price,
            suggested_size_usd=suggested,
            whale_name=whale.name,
            whale_roi=whale.roi,
            timestamp=now,
            reason=f"{whale.name} ({whale.win_rate:.0%} WR, {whale.style}) buy {outcome} ${size:,.0f} @ {price:.3f}",
            market_title=title,
            market_category=_categorize_market(title),
            whale_address=whale.proxy_wallet,
            # Edge score: derived from actual PnL per whale in trades.db
            # Original win_rate*0.8+roi*0.2 formula was INVERTED — high scores lost money
            edge_score=self._compute_pnl_edge(whale.name),
        )

    def scan_known_whales(self) -> list:
        """Poll positions for known whales using a thread pool (non-blocking)."""
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        now = _time.time()
        if now - self.last_scan_time < self.SCAN_INTERVAL:
            return []

        signals = []
        wallets = list(self.whales.items())

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_map = {
                executor.submit(self._fetch_positions, wallet): wallet
                for wallet, _ in wallets
            }
            for future in as_completed(future_map):
                wallet = future_map[future]
                whale = self.whales.get(wallet)
                try:
                    positions = future.result()
                    for pos in positions:
                        signal = self._process_position(pos, whale, now)
                        if signal:
                            signals.append(signal)
                            self.signal_history.append(signal)
                except Exception:
                    pass  # individual wallet failure is non-fatal

        self.last_scan_time = now
        # Cap signals to prevent OOM from processing too many at once
        MAX_SIGNALS = 100
        if len(signals) > MAX_SIGNALS:
            signals = signals[:MAX_SIGNALS]
        # Cap signal_history to prevent unbounded memory growth
        if len(self.signal_history) > 1000:
            self.signal_history = self.signal_history[-500:]
        return signals

    def scan_sybil_signals(self, max_age_hours: int = 4) -> list[WhaleSignal]:
        """I3 Fix: Read pending sybil signals from trades.db and convert to WhaleSignal.

        The sybil detector writes signals to sybil_signals table (950 pending as of r1700)
        but nothing was reading them and feeding them into the pipeline. This method
        bridges that gap — it should be called by the main signal loop on each scan cycle.

        Args:
            max_age_hours: Only process signals newer than this. Default 4h to match
                          the autoresearch cron cadence.

        Returns:
            List of WhaleSignal objects ready for signal_pipeline.process().
        """
        import sqlite3
        import time as _time

        db_path = "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        cutoff_ts = _time.time() - (max_age_hours * 3600)
        cutoff_iso = _time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(cutoff_ts))

        try:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA busy_timeout=5000")
            rows = conn.execute(
                """
                SELECT generated_at, signal_type, group_id, market_title, condition_id,
                       side, confidence, reason, total_exposure_usd, wallet_count,
                       yes_size_usd, no_size_usd, yes_ratio, avg_bet_usd, inserted_at
                FROM sybil_signals
                WHERE inserted_at >= ?
                ORDER BY inserted_at DESC
                LIMIT 50
                """,
                (cutoff_iso,),
            ).fetchall()
            conn.close()
        except Exception as e:
            import logging as _lg
            _lg.getLogger("whale_tracker").warning(f"scan_sybil_signals: DB query failed: {e}")
            return []

        if not rows:
            return []

        signals = []
        for row in rows:
            (generated_at, signal_type, group_id, market_title, condition_id,
             side, confidence, reason, total_exposure_usd, wallet_count,
             yes_size_usd, no_size_usd, yes_ratio, avg_bet_usd, inserted_at) = row

            # Skip sports markets — sybil signals are not sanitized by the generator
            from strategies.wf_sports import is_sports_market
            if is_sports_market(market_title)[0]:
                continue

            # Guard: skip signals without a condition_id (unresolvable)
            if not condition_id or not condition_id.strip():
                continue

            # Parse side: "BUY YES" or "BUY NO" → WhaleSignal side
            side_upper = (side or "").upper()
            if "YES" in side_upper:
                ws_side = "buy"
                outcome = "yes"
            elif "NO" in side_upper:
                ws_side = "sell"
                outcome = "no"
            else:
                ws_side = "buy"
                outcome = "yes"

            # Compute edge_score: sybil conviction proxy — total_exposure is a strong signal.
            # Use log-scaled exposure to keep edge in [0, 1]. $500k+ exposure → edge≈0.5+
            exposure = float(total_exposure_usd or 0)
            edge_score = min(float(exposure) / 1_000_000, 1.0) if exposure > 0 else 0.10

            # confidence already computed by sybil_signal_loader (0.5 + wallet_count*0.1, capped 0.99)
            conf = float(confidence) if confidence else 0.60

            # suggested_size_usd: proportional to conviction but capped for risk management
            size_usd = min(float(avg_bet_usd or 0) * float(wallet_count or 1), 5000.0)

            ws = WhaleSignal(
                signal_type=WhaleSignalType.LARGE_POSITION,  # CONCENTRATED_FOLLOW not in enum; LARGE_POSITION is closest match
                condition_id=str(condition_id).strip(),
                token_id="",
                outcome=outcome,
                side=ws_side,
                confidence=conf,
                target_price=0.5,
                suggested_size_usd=round(size_usd, 2),
                whale_name=f"sybil_{group_id}" if group_id else "sybil_unknown",
                whale_roi=0.0,
                timestamp=_time.time(),
                reason=reason or "",
                market_title=market_title or "",
                market_category="general",
                whale_address="",
                edge_score=round(edge_score, 3),
                source="sybil",
            )
            signals.append(ws)

        return signals

    def _load_discovered_whales(self) -> None:
        """Load discovered whales from pipeline database."""
        try:
            import sqlite3
            import os

            pipeline_db = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "pipeline",
                "data",
                "whale_discovery.db",
            )
            if not os.path.exists(pipeline_db):
                return

            conn = sqlite3.connect(pipeline_db)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            rows = conn.execute(
                "SELECT address, name, alpha_score, pnl, volume, total_trades "
                "FROM whales WHERE alpha_score >= 70 ORDER BY alpha_score DESC"
            ).fetchall()
            conn.close()

            added = 0
            for row in rows:
                addr, name, alpha, pnl, vol, trades = row
                if addr not in self.whales:
                    whale = WhaleIdentity(
                        name=name,
                        proxy_wallet=addr,
                        roi=alpha / 100,
                        win_rate=alpha / 100,
                        total_trades=trades,
                        avg_trade_size=vol / max(trades, 1),
                        style="discovered",
                        notes=f"Pipeline discovered: alpha={alpha}, PnL=${pnl:,.0f}",
                    )
                    self.register_whale(whale)
                    added += 1

            self.seen_trades.add(f"__pipeline_loaded_{added}_whales__")
        except Exception as e:
            pass

    def register_whale(self, whale: WhaleIdentity) -> None:
        """Add a new whale to track."""
        self.whales[whale.proxy_wallet] = whale
        self.whale_names[whale.name] = whale.proxy_wallet

    def scan_whale_trades_sync(
        self,
        condition_ids: Optional[list[str]] = None,
    ) -> List[WhaleSignal]:
        """Scan for recent whale trades and generate signals.

        Enhanced with:
        - Rate limiting and retries
        - Persistent state (Redis + disk)
        - Signal validation

        Args:
            condition_ids: Specific markets to scan. None = scan all recent trades.

        Returns:
            List of new trading signals.
        """
        now = time.time()

        # Check scan interval (if using time-based scan)
        if now - self.last_scan_time < self.scan_interval:
            return []

        self.last_scan_time = now

        signals = []

        try:
            # Fetch recent trades from data API with rate limiting
            trades = self._rate_limiter.scan_trades(
                condition_ids=condition_ids,
                offset=self.last_scan_offset,
            )

            if not trades:
                return []

            # Process trades
            for trade_data in trades[:50]:  # Check last 50 trades
                trade = self._parse_trade(trade_data)
                if not trade:
                    continue

                # Check if this trade is from a known whale
                whale = self._match_whale(trade)
                if not whale:
                    continue

                # Generate sequence number
                sequence = self._state_manager.get_sequence()
                timestamp_ms = int(time.time() * 1000)

                # Check if seen before (persistent dedup)
                if self._state_manager.has_seen_trade(
                    trade.whale_wallet,
                    trade.condition_id,
                    timestamp_ms,
                    sequence,
                ):
                    continue

                # Mark as seen
                self._state_manager.mark_seen_trade(
                    trade.whale_wallet,
                    trade.condition_id,
                    timestamp_ms,
                    sequence,
                )

                # Generate signal if trade meets threshold
                if trade.usd_value >= whale.avg_trade_size * 0.1:  # At least 10% of avg
                    signal = self._generate_signal(trade, whale)
                    if signal:
                        # Validate signal
                        result = self._validator.validate_signal(
                            whale_name=whale.name,
                            whale_wallet=trade.whale_wallet,
                            condition_id=trade.condition_id,
                            token_id=trade.token_id,
                            side=trade.side,
                            outcome=trade.outcome,
                            size=trade.size,
                            price=trade.price,
                            usd_value=trade.usd_value,
                            timestamp=trade.timestamp,
                        )

                        if result.is_valid:
                            signals.append(signal)
                            self.signal_history.append(signal)
                            self._state_manager.save_state()

                        elif result.is_rejected:
                            self._state_manager.save_state()

            # Update offset for next scan
            if trades:
                self.last_scan_offset = self.last_scan_offset + 50

            # Rate limit: wait before next scan
            self._rate_limiter._maybe_throttle()

        except Exception as e:
            # Silently skip errors - whale tracking shouldn't crash the strategy
            self._state_manager.save_state()

        return signals

    def detect_large_trades(self, trades: list[dict]) -> list[WhaleSignal]:
        """Process TradeTick stream data for large trades.

        Backward compat: whale_follower.py calls this method to process
        buffered large trades (>= $1000) from TradeTick streams.

        Args:
            trades: List of raw trade data from TradeTick streams

        Returns:
            List of WhaleSignal for trades meeting the threshold
        """
        signals = []
        now = time.time()

        for trade in trades:
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            usd = size * price

            if usd < self.LARGE_TRADE_THRESHOLD:
                continue

            condition_id = trade.get("conditionId", "")
            # ── Guard: skip trades without a condition_id ─────────────────────
            # Trades from TradeTick streams can arrive with empty conditionId when
            # the stream delivers aggregated or cross-market data. These signals
            # are untradeable — we cannot resolve an instrument without a condition_id.
            # Reject early at the source instead of propagating bad data through the
            # pipeline where it wastes processing and produces confusing rejections.
            if not condition_id or not condition_id.strip():
                import logging as _lg
                _lg.getLogger("whale_tracker").debug(
                    f"detect_large_trades: skipping trade with empty conditionId | "
                    f"proxy={str(trade.get('proxyWallet',''))[:10]}... usd=${usd:,.0f}"
                )
                continue

            outcome = trade.get("outcome", "")
            side_raw = trade.get("side", "BUY")
            side = "buy" if side_raw == "BUY" else "sell"
            proxy_wallet = trade.get("proxyWallet", "")
            title = trade.get("title", "")

            # Deduplicate - use timestamp as part of key
            trade_key = f"{proxy_wallet}:{condition_id}:{now:.0f}"
            if trade_key in self.seen_positions:
                continue
            self.seen_positions[trade_key] = now

            # Confidence based on trade size (for unknown/large-trade whales)
            confidence = min(0.50 + (usd / 100000) * 0.2, 0.70)
            # Edge score: large trades ($50K+) are CONTRARIAN indicators with better PnL
            # Higher cap (0.85) allows edge scores proportional to trade impact
            large_trade_edge = min(0.40 + (usd / 50000) * 0.15, 0.85)

            token_id = trade.get("token_id", "unknown")
            signals.append(
                WhaleSignal(
                    signal_type=WhaleSignalType.LARGE_POSITION,
                    condition_id=condition_id,
                    token_id=token_id,  # Extracted from instrument_id in TradeTick buffer
                    outcome=outcome,
                    side=side,
                    confidence=confidence,
                    target_price=price,
                    suggested_size_usd=usd * 0.25,
                    whale_name=make_fallback_name(proxy_wallet),
                    whale_roi=0.50,  # Default ROI for unknown whales
                    timestamp=now,
                    reason=f"Large trade {side} {outcome} ${usd:,.0f} @ {price:.3f}",
                    market_title=title,
                    market_category=_categorize_market(title),
                    whale_address=proxy_wallet,
                    edge_score=large_trade_edge,
                )
            )

        return signals

    def scan_whale_trades_by_offset(
        self,
        offset: int = 0,
        limit: int = 100,
    ) -> List[WhaleSignal]:
        """Scan for whale trades starting from a specific offset.

        Args:
            offset: Pagination offset
            limit: Trades per page

        Returns:
            List of new trading signals
        """
        self.last_scan_offset = offset

        trades = self._rate_limiter.scan_trades(
            condition_ids=None,  # Scan all
            offset=offset,
            limit=limit,
        )

        if not trades:
            return []

        signals = []

        for trade_data in trades:
            trade = self._parse_trade(trade_data)
            if not trade:
                continue

            whale = self._match_whale(trade)
            if not whale:
                continue

            # Generate sequence number
            sequence = self._state_manager.get_sequence()
            timestamp_ms = int(time.time() * 1000)

            # Check if seen before
            if self._state_manager.has_seen_trade(
                trade.whale_wallet,
                trade.condition_id,
                timestamp_ms,
                sequence,
            ):
                continue

            # Mark as seen
            self._state_manager.mark_seen_trade(
                trade.whale_wallet,
                trade.condition_id,
                timestamp_ms,
                sequence,
            )

            # Generate and validate signal
            signal = self._generate_signal(trade, whale)
            if signal:
                result = self._validator.validate_signal(
                    whale_name=whale.name,
                    whale_wallet=trade.whale_wallet,
                    condition_id=trade.condition_id,
                    token_id=trade.token_id,
                    side=trade.side,
                    outcome=trade.outcome,
                    size=trade.size,
                    price=trade.price,
                    usd_value=trade.usd_value,
                    timestamp=trade.timestamp,
                )

                if result.is_valid:
                    signals.append(signal)
                    self.signal_history.append(signal)

            # Rate limit
            self._rate_limiter._maybe_throttle()

        return signals

    def _parse_trade(self, trade_data: dict) -> Optional[WhaleTrade]:
        """Parse raw trade data from data API into WhaleTrade."""
        try:
            proxy_wallet = trade_data.get("proxyWallet", "")
            condition_id = trade_data.get("conditionId", "")
            side = trade_data.get("side", "")
            size = float(trade_data.get("size", 0))
            price = float(trade_data.get("price", 0))
            timestamp = float(trade_data.get("timestamp", 0))
            outcome = trade_data.get("outcome", "Unknown")
            market_title = trade_data.get("title", "")
            market_slug = trade_data.get("slug", "")

            # Asset ID is the token ID
            token_id = trade_data.get("asset", "")

            if size <= 0 or price <= 0 or not proxy_wallet:
                return None

            # DEBUG: log when proxy_wallet is unknown
            fallback = make_fallback_name(proxy_wallet)
            import logging as _lg

            _lg.getLogger("whale_tracker").debug(
                f"No whale identity for wallet {proxy_wallet[:10]}... "
                f"assigning fallback name '{fallback}'"
            )
            return WhaleTrade(
                whale_name=fallback,
                whale_wallet=proxy_wallet,
                condition_id=condition_id,
                token_id=token_id,
                outcome=outcome,
                side=side,
                size=size,
                price=price,
                usd_value=size * price,
                timestamp=timestamp,
                market_title=market_title,
                market_slug=market_slug,
            )
        except (ValueError, KeyError, TypeError):
            return None

    def _match_whale(self, trade: WhaleTrade) -> Optional[WhaleIdentity]:
        """Check if a trade is from a known whale wallet."""
        if trade.whale_wallet not in self.whales:
            import logging as _lg

            _lg.getLogger("whale_tracker").debug(
                f"Wallet {trade.whale_wallet[:10]}... not in known whales "
                f"(have {len(self.whales)} known wallets)"
            )
        return self.whales.get(trade.whale_wallet)

    def _generate_signal(
        self,
        trade: WhaleTrade,
        whale: WhaleIdentity,
    ) -> Optional[WhaleSignal]:
        """Generate a trading signal from a whale trade."""
        try:
            # Map whale trade to our signal
            if trade.side == "BUY":
                signal_type = (
                    WhaleSignalType.BUY_YES
                    if trade.outcome.lower() == "yes"
                    else WhaleSignalType.BUY_NO
                )
                side = "buy"
            else:
                signal_type = (
                    WhaleSignalType.SELL_YES
                    if trade.outcome.lower() == "yes"
                    else WhaleSignalType.SELL_NO
                )
                side = "sell"

            # Confidence based on whale's historical performance
            confidence = min(whale.win_rate * 0.8 + 0.2, 0.95)

            # Suggested size based on Kelly fraction
            suggested_size = trade.usd_value * 0.25  # 25% of whale's size

            return WhaleSignal(
                signal_type=signal_type,
                condition_id=trade.condition_id,
                token_id=trade.token_id,
                outcome=trade.outcome,
                side=side,
                confidence=confidence,
                target_price=trade.price,
                suggested_size_usd=suggested_size,
                whale_name=whale.name,
                whale_roi=whale.roi,
                # Edge score: derived from actual PnL per whale in trades.db
                # Original win_rate*0.8+roi*0.2 formula was INVERTED — high scores lost money
                edge_score=self._compute_pnl_edge(whale.name),
                timestamp=trade.timestamp,
                reason=f"{whale.name} ({whale.roi:.0%} ROI, {whale.style}) {side} {trade.outcome} "
                f"${trade.usd_value:,.0f} @ {trade.price:.3f}",
                market_title=trade.market_title,
                market_category=_categorize_market(trade.market_title),
            )
        except Exception:
            return None

    def get_whale_summary(self) -> dict:
        """Get summary of tracked whales and their recent activity."""
        summary = {
            "whales_tracked": len(self.whales),
            "signals_generated": len(self.signal_history),
            "whales": {},
        }

        for wallet, whale in self.whales.items():
            whale_signals = [
                s for s in self.signal_history if s.whale_name == whale.name
            ]
            summary["whales"][whale.name] = {
                "roi": whale.roi,
                "win_rate": whale.win_rate,
                "style": whale.style,
                "recent_signals": len(whale_signals),
                "avg_signal_size": (
                    sum(s.suggested_size_usd for s in whale_signals)
                    / len(whale_signals)
                    if whale_signals
                    else 0
                ),
            }

        return summary

    def get_state_summary(self) -> dict:
        """Get state manager summary."""
        return self._state_manager.get_summary()

    def get_rate_limit_info(self) -> dict:
        """Get rate limiter info."""
        return self._rate_limiter.get_rate_limit_info()

    def reset(self) -> None:
        """Reset tracker state."""
        self._state_manager.reset_sequence()
        self._rate_limiter.reset()
        self.last_scan_time = 0.0
        self.last_scan_offset = 0
        self.seen_trades.clear()
        self.signal_history.clear()

    def cleanup_old_trades(self, max_age_days: int = 365) -> None:
        """Clean up old trade entries."""
        self._state_manager.cleanup_old_trades(max_age_days)

    def save_state(self) -> None:
        """Force save state."""
        self._state_manager.save_state()
