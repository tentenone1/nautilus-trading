"""Whale Performance Tracker — Dynamic fade/follow detection from live trade data.

Queries trades.db to compute per-whale, per-category win rate and P&L.
Whales with WR < FADE_WR_THRESHOLD over FADE_MIN_TRADES are marked as fade candidates.
Results are persisted to whale_performance.json so they survive restarts.

Paradigm: A whale that's consistently wrong is a signal to fade, not to block.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Fade thresholds — Phase A3 alignment:
# Fade eligible only when whale has statistically significant losing record:
#   >=10 trades in that category AND win rate <25%
# (supersedes legacy constants used by is_fade_whale_dynamic)
FADE_WR_THRESHOLD: float = 0.25   # <25% WR to qualify
FADE_MIN_TRADES: int = 10          # >=10 trades in category before fade is allowed
FADE_CONFIDENCE_MIN: float = 0.8
FADE_KELLY_MULTIPLIER: float = 1.5

_TRADES_DB_PATH = Path(__file__).parent.resolve().parent / "research" / "trades.db"
_PERF_FILE = Path(__file__).parent.resolve().parent / "research" / "whale_performance.json"

logger = logging.getLogger("whale_perf")


@dataclass(frozen=True)
class WhalePerfRecord:
    """Performance record for one whale in one category."""
    whale_address: str
    category: str
    trades: int
    wins: int
    losses: int
    win_rate: float
    pnl: float
    avg_pnl: float
    avg_confidence: float
    should_fade: bool
    last_updated: str


def _get_default_stats() -> dict:
    """Empty stats structure."""
    return {}


def load_whale_performance() -> dict:
    """Load cached whale performance from JSON, or return empty dict."""
    if not _PERF_FILE.exists():
        return _get_default_stats()
    try:
        with open(_PERF_FILE, "r", encoding="utf-8") as f:
            data: dict = json.load(f)
        result: dict = {}
        for addr, cats in data.items():
            result[addr] = {}
            for cat, rec in cats.items():
                result[addr][cat] = WhalePerfRecord(**rec)
        return result
    except Exception as e:
        logger.warning("Failed to load whale_performance.json: %s", e)
        return _get_default_stats()


def save_whale_performance(stats: dict) -> None:
    """Persist whale performance to JSON."""
    data = {}
    for addr, cats in stats.items():
        data[addr] = {}
        for cat, rec in cats.items():
            data[addr][cat] = asdict(rec)
    with open(_PERF_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)


def compute_whale_stats() -> dict:
    """Query trades.db and compute per-whale per-category win rate + fade flags."""
    if not _TRADES_DB_PATH.exists():
        logger.warning("trades.db not found at %s, skipping whale perf update", _TRADES_DB_PATH)
        return load_whale_performance()

    try:
        stats = load_whale_performance()

        import sqlite3
        conn = sqlite3.connect(str(_TRADES_DB_PATH))
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        now_str = now.isoformat()

        cur.execute("SELECT whale_address, category, COUNT(*), SUM(realized_pnl), AVG(realized_pnl), AVG(confidence) FROM trades GROUP BY whale_address, category")
        rows = cur.fetchall()

        for row in rows:
            addr, cat, trades_n, pnl_val, avg_pnl_val, avg_conf = row
            wins_n = 0
            losses_n = 0

            # Count wins/losses
            cur2 = conn.cursor()
            cur2.execute("SELECT realized_pnl FROM trades WHERE whale_address=? AND category=?", (addr, cat))
            for r in cur2.fetchall():
                if r[0] and r[0] > 0:
                    wins_n += 1
                elif r[0] and r[0] < 0:
                    losses_n += 1

            wr = wins_n / trades_n if trades_n > 0 else 0.0
            should_fade = (
                trades_n >= FADE_MIN_TRADES
                and wr < FADE_WR_THRESHOLD
                and (avg_conf or 0) >= FADE_CONFIDENCE_MIN
            )

            record = WhalePerfRecord(
                whale_address=addr,
                category=cat,
                trades=trades_n,
                wins=wins_n,
                losses=losses_n,
                win_rate=wr,
                pnl=pnl_val or 0.0,
                avg_pnl=avg_pnl_val or 0.0,
                avg_confidence=avg_conf or 0.0,
                should_fade=should_fade,
                last_updated=now_str,
            )
            stats.setdefault(addr, {})[cat] = record

        conn.close()
        save_whale_performance(stats)
        return stats

    except Exception as e:
        logger.warning("Failed to compute whale stats from trades.db: %s", e)
        return load_whale_performance()


def is_fade_whale_dynamic(
    whale_address: str,
    category: str,
    confidence: float,
) -> bool:
    """Check if whale should be faded for a given category and confidence level."""
    if confidence < FADE_CONFIDENCE_MIN:
        return False
    cats = load_whale_performance().get(whale_address, {})
    record = cats.get(category) or cats.get("general")
    if not record:
        return False
    return record.should_fade


def get_fade_kelly_multiplier(
    whale_address: str,
    category: str,
    stats: Optional[dict] = None,
) -> float:
    """Return Kelly multiplier for fading a whale. 1.0 = neutral, >1.0 = more aggressive."""
    cats = (stats or load_whale_performance()).get(whale_address, {})
    record = cats.get(category) or cats.get("general")
    if not record or not record.should_fade:
        return 1.0
    wr = record.win_rate
    wr_gap = FADE_WR_THRESHOLD - wr  # how wrong this whale is
    bonus = min(wr_gap * FADE_KELLY_MULTIPLIER, FADE_KELLY_MULTIPLIER - 1.0)
    return max(1.0, FADE_KELLY_MULTIPLIER - bonus)


def flip_side_for_fade(side: str) -> str:
    """Flip a side for fading: BUY -> SELL, SELL -> BUY."""
    return "sell" if side.lower() == "buy" else "buy"
