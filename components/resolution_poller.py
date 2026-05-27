"""Resolution Poller — Periodic market resolution tracking for the paper trader.

Integrates into WhaleFollower's _on_exit_timer loop to:
1. Poll Polymarket CLOB API for resolution status of tracked positions
2. Calculate actual P&L based on resolution outcome (vs simulated mark-to-market)
3. Store actual P&L in trades.db alongside realized (simulated) P&L
4. Log resolution events for dashboard consumption

Usage:
    from components.resolution_poller import ResolutionPoller
    
    poller = ResolutionPoller()
    poller.poll_open_positions(open_positions_dict)
"""

import json
import logging
import sqlite3
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("ResolutionPoller")

# ── Configuration ──────────────────────────────────────────────────────
CLOB_API = "https://clob.polymarket.com"
RATE_LIMIT_SLEEP = 0.3           # seconds between API calls
REQUEST_TIMEOUT = 15             # seconds
POLL_INTERVAL_SECS = 120         # default: check resolutions every 2 minutes
RESOLUTION_CACHE_TTL = 3600      # re-check unresolved markets after 1 hour


def parse_token_id_from_instrument(instrument_id: str) -> Optional[str]:
    """Extract token_id from instrument_id.

    Format: {condition_id}-{token_id}.POLYMARKET
    """
    if not instrument_id:
        return None
    symbol = instrument_id
    if ".POLYMARKET" in symbol:
        symbol = symbol.replace(".POLYMARKET", "")
    parts = symbol.split("-")
    if len(parts) >= 2:
        return parts[-1]
    return None


def get_market_resolution(condition_id: str) -> Optional[dict]:
    """Fetch market data from CLOB API and return resolution info.

    Returns dict with keys:
        - resolved (bool): market has a definitive winner
        - winning_outcome (str): name of the winning outcome
        - winning_token_id (str): token_id of the winning outcome
        - losing_outcome (str): name of the losing outcome
        - losing_token_id (str): token_id of the losing outcome
        - closed (bool): market is closed to new orders
        - question (str): market question

    Returns None if API error.
    """
    url = f"{CLOB_API}/markets/{condition_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "Hermes/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.HTTPError, urllib.error.URLError,
            json.JSONDecodeError, OSError) as e:
        logger.warning(f"API error for {condition_id[:30]}... : {e}")
        return None

    if not isinstance(data, dict):
        logger.warning(f"Unexpected response for {condition_id[:30]}...")
        return None

    tokens = data.get("tokens", [])
    if not tokens:
        return {
            "resolved": False,
            "closed": data.get("closed", False),
            "question": data.get("question", ""),
            "winning_outcome": None,
            "winning_token_id": None,
            "losing_outcome": None,
            "losing_token_id": None,
        }

    winners = [t for t in tokens if t.get("winner") is True]
    losers = [t for t in tokens if t.get("winner") is False]

    resolved = len(winners) == 1 and len(losers) >= 1
    winning_token_id = winners[0].get("token_id", "") if winners else None
    winning_outcome = winners[0].get("outcome", "") if winners else None
    losing_token_id = losers[0].get("token_id", "") if losers else None
    losing_outcome = losers[0].get("outcome", "") if losers else None

    return {
        "resolved": resolved,
        "closed": data.get("closed", False),
        "question": data.get("question", ""),
        "winning_outcome": winning_outcome,
        "winning_token_id": winning_token_id,
        "losing_outcome": losing_outcome,
        "losing_token_id": losing_token_id,
    }


def calculate_actual_pnl(
    entry_price: Optional[float],
    position_size_usd: float,
    our_token_id: str,
    winning_token_id: str,
    side: str,
) -> dict:
    """Calculate actual P&L based on market resolution.

    position_size_usd = cost basis (shares * entry_price)
    shares = position_size_usd / entry_price

    If our token won:
        value = shares * $1 = position_size_usd / entry_price
        pnl = value - cost = position_size_usd * (1/entry_price - 1)
    If our token lost:
        value = 0
        pnl = -position_size_usd

    For SELL side (sold YES token = effectively bought NO):
        Inverse logic applies.
    """
    if entry_price is None or entry_price <= 0 or entry_price > 1:
        return {"actual_pnl": None, "actual_return": None, "won": None}

    shares = position_size_usd / entry_price

    # Sanity cap: prevent absurdly large share counts from corrupting P&L
    MAX_SANE_SHARES = 10000
    if shares > MAX_SANE_SHARES:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(
            f"Capped shares from {shares:.0f} to {MAX_SANE_SHARES} "
            f"(entry_price={entry_price}, position=${position_size_usd})"
        )
        shares = MAX_SANE_SHARES

    if side.upper() == "SELL":
        # Sell side: we sold YES, so we want YES to lose (NO wins)
        won = (our_token_id != winning_token_id)
    else:
        # BUY = buying YES token
        won = (our_token_id == winning_token_id)

    if won:
        actual_pnl = round(shares * 1.0 - position_size_usd, 2)
        actual_return = round((1.0 - entry_price) / entry_price * 100, 2)
    else:
        actual_pnl = round(-position_size_usd, 2)
        actual_return = -100.0

    return {
        "actual_pnl": actual_pnl,
        "actual_return": actual_return,
        "won": won,
    }


class ResolutionPoller:
    """Periodic resolution checker that integrates into the paper trader loop.

    Tracks which condition_ids have already been checked to avoid redundant API calls.
    Thread-safe for concurrent DB writes.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        strategy = None,
    ) -> None:
        """
        Args:
            db_path: Path to trades.db. Defaults to data/trades.db in workspace.
            strategy: Optional WhaleFollower instance. When provided, calls
                strategy.add_resolution_pnl() after each resolved position
                to feed real P&L into the daily kill switch.
        """
        self._db_path = db_path or Path(__file__).parent.parent / "data" / "trades.db"
        self._checked: dict[str, float] = {}
        self._last_poll_time: float = 0
        self._strategy = strategy
        self._poll_count: int = 0
        self._resolved_count: int = 0
        # Stats for dashboard
        self.total_real_pnl: float = 0.0
        self.total_simulated_pnl: float = 0.0
        self.resolved_conditions: list[dict] = []

    @property
    def stats(self) -> dict:
        """Return current poller stats for the dashboard."""
        return {
            "poll_count": self._poll_count,
            "resolved_count": self._resolved_count,
            "total_real_pnl": round(self.total_real_pnl, 2),
            "total_simulated_pnl": round(self.total_simulated_pnl, 2),
            "divergence": round(self.total_real_pnl - self.total_simulated_pnl, 2),
            "conditions_checked": len(self._checked),
            "last_poll_time": self._last_poll_time,
            "recent_resolutions": self.resolved_conditions[-10:],
        }

    def poll_open_positions(self, open_positions: dict[str, dict]) -> list[dict]:
        """Poll resolution status for all open positions that haven't been checked recently.

        Args:
            open_positions: Dict from WhaleFollower._open_positions
                           {inst_key: {whale_name, market_title, category, side,
                                       entry_price, size, trade_id, condition_id, ...}}

        Returns:
            List of resolution events (resolved condition_ids with P&L impact)
        """
        now = time.time()
        self._last_poll_time = now

        # Collect unique condition_ids from open positions
        # Only re-check conditions older than RESOLUTION_CACHE_TTL
        conditions_to_check: dict[str, list[dict]] = {}
        for inst_key, pos in open_positions.items():
            cond_id = pos.get("condition_id", "")
            if not cond_id:
                # Try to extract from instrument key
                if "-" in inst_key:
                    cond_id = inst_key.split("-")[0]

            if not cond_id:
                continue

            # Check if we've looked at this recently
            last_check = self._checked.get(cond_id, 0)
            if now - last_check < RESOLUTION_CACHE_TTL:
                continue  # Already checked recently, skip

            if cond_id not in conditions_to_check:
                conditions_to_check[cond_id] = []
            conditions_to_check[cond_id].append({
                "inst_key": inst_key,
                "side": pos.get("side", "BUY"),
                "entry_price": pos.get("entry_price", 0.5),
                "size": pos.get("size", 0.0),
                "trade_id": pos.get("trade_id", ""),
            })

        # Also check unresolved trades in the DB that aren't in open positions
        db_conditions = self._get_unresolved_db_conditions(now)
        for cond_id, trades in db_conditions.items():
            if cond_id not in conditions_to_check:
                conditions_to_check[cond_id] = []
            for t in trades:
                if t not in conditions_to_check[cond_id]:
                    conditions_to_check[cond_id].append(t)

        if not conditions_to_check:
            return []

        self._poll_count += 1
        events = []

        for cond_id, trade_infos in conditions_to_check.items():
            # Rate limit
            time.sleep(RATE_LIMIT_SLEEP)

            market = get_market_resolution(cond_id)
            self._checked[cond_id] = now

            if market is None:
                continue  # API error, will retry next poll

            if not market["resolved"]:
                continue  # Not resolved yet

            # ── Market resolved! Calculate actual P&L for each trade ──
            winning_token_id = market["winning_token_id"]
            question = market.get("question", cond_id[:40])
            winning_outcome = market.get("winning_outcome", "?")

            condition_events = []
            condition_actual_pnl = 0.0
            condition_simulated_pnl = 0.0

            for trade_info in trade_infos:
                # Parse token_id from instrument if we have it
                inst_key = trade_info.get("inst_key", "")
                our_token_id = parse_token_id_from_instrument(inst_key)

                if not our_token_id:
                    logger.warning(
                        f"Cannot determine token_id for {inst_key[:40]}..., "
                        f"skipping resolution P&L"
                    )
                    continue

                pnl_data = calculate_actual_pnl(
                    entry_price=trade_info["entry_price"],
                    position_size_usd=trade_info["size"],
                    our_token_id=our_token_id,
                    winning_token_id=winning_token_id,
                    side=trade_info["side"],
                )

                if pnl_data["actual_pnl"] is None:
                    continue

                actual_pnl = pnl_data["actual_pnl"]
                won = pnl_data["won"]

                # Update the DB row with actual P&L
                trade_id = trade_info.get("trade_id", "")
                if trade_id:
                    self._update_trade_resolution(
                        trade_id=trade_id,
                        actual_pnl=actual_pnl,
                        actual_return=pnl_data["actual_return"],
                        resolution_outcome=f"{'WIN' if won else 'LOSS'} | {winning_outcome} won | Actual: ${actual_pnl:+.2f}",
                    )

                condition_actual_pnl += actual_pnl
                condition_events.append({
                    "inst_key": inst_key,
                    "trade_id": trade_id,
                    "won": won,
                    "actual_pnl": actual_pnl,
                    "entry_price": trade_info["entry_price"],
                    "size": trade_info["size"],
                    "side": trade_info["side"],
                })

                # Track running totals
                self.total_real_pnl += actual_pnl

                # Feed real P&L into the daily kill switch
                if self._strategy is not None and actual_pnl != 0:
                    try:
                        self._strategy.add_resolution_pnl(actual_pnl)
                    except Exception as e:
                        logger.warning(f"Failed to update strategy P&L: {e}")

            # Create a summary event for this resolved condition
            resolution_event = {
                "condition_id": cond_id,
                "question": question,
                "winning_outcome": winning_outcome,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "trades_count": len(condition_events),
                "total_actual_pnl": round(condition_actual_pnl, 2),
                "trades": condition_events,
            }
            events.append(resolution_event)
            self.resolved_conditions.append(resolution_event)
            self._resolved_count += 1

            # Log it
            logger.info(
                f"RESOLUTION: {question[:50]} | Winner: {winning_outcome} | "
                f"Actual P&L: ${condition_actual_pnl:+.2f} ({len(condition_events)} trades)"
            )

        return events

    def _get_unresolved_db_conditions(self, now: float) -> dict[str, list[dict]]:
        """Find condition_ids in DB that still need resolution tracking.

        Returns dict of condition_id -> list of trade info dicts for unresolved trades.
        """
        db_path = self._db_path
        if not db_path.exists():
            return {}

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=3000")

            rows = conn.execute("""
                SELECT DISTINCT
                    t.trade_id, t.condition_id, t.instrument_id,
                    t.side, t.entry_price, t.position_size_usd,
                    t.market_title
                FROM trades t
                WHERE t.realized_pnl IS NULL
                  AND t.condition_id IS NOT NULL
                  AND t.condition_id != ''
                  AND t.instrument_id IS NOT NULL
                  AND t.instrument_id != ''
                  AND t.entry_price > 0
                ORDER BY t.timestamp DESC
                LIMIT 200
            """).fetchall()
            conn.close()

            conditions: dict[str, list[dict]] = {}
            for r in rows:
                cond_id = r["condition_id"]
                # Only include conditions not already in checked (or stale)
                last_check = self._checked.get(cond_id, 0)
                if now - last_check < RESOLUTION_CACHE_TTL:
                    continue
                if cond_id not in conditions:
                    conditions[cond_id] = []
                conditions[cond_id].append({
                    "inst_key": r["instrument_id"] or "",
                    "side": r["side"] or "BUY",
                    "entry_price": r["entry_price"] or 0.5,
                    "size": r["position_size_usd"] or 0.0,
                    "trade_id": r["trade_id"] or "",
                })

            return conditions

        except sqlite3.Error as e:
            logger.error(f"DB error in _get_unresolved_db_conditions: {e}")
            return {}

    def _update_trade_resolution(
        self,
        trade_id: str,
        actual_pnl: float,
        actual_return: float,
        resolution_outcome: str,
    ) -> bool:
        """Update a trade record with actual resolution P&L data.

        Thread-safe with WAL mode and busy_timeout.
        """
        if not trade_id:
            return False
        try:
            conn = sqlite3.connect(str(self._db_path))
            conn.execute("PRAGMA busy_timeout=5000")
            # exit_price for binary options: 1.0 if won, 0.0 if lost
            exit_price = 1.0 if actual_pnl > 0 else 0.0
            conn.execute("""
                UPDATE trades
                SET actual_pnl = ?,
                    actual_return = ?,
                    resolution_outcome = ?,
                    exit_price = ?,
                    exit_reason = 'resolved'
                WHERE trade_id = ?
            """, (actual_pnl, actual_return, resolution_outcome, exit_price, trade_id))
            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            logger.error(f"DB update failed for trade {trade_id[:20]}...: {e}")
            return False

    def get_db_summary(self) -> dict:
        """Query the DB for aggregate P&L stats (real vs simulated)."""
        db_path = self._db_path
        if not db_path.exists():
            return {
                "total_trades": 0,
                "resolved_trades": 0,
                "total_realized_pnl": 0.0,
                "total_actual_pnl": 0.0,
                "divergence": 0.0,
                "open_positions": 0,
            }

        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA busy_timeout=3000")
            row = conn.execute("""
                SELECT
                    COUNT(*) as total_trades,
                    COUNT(CASE WHEN actual_pnl IS NOT NULL THEN 1 END) as resolved_trades,
                    COALESCE(SUM(COALESCE(realized_pnl, 0)), 0) as total_realized_pnl,
                    COALESCE(SUM(COALESCE(actual_pnl, 0)), 0) as total_actual_pnl,
                    COUNT(CASE WHEN exit_reason IS NULL THEN 1 END) as open_positions
                FROM trades
            """).fetchone()
            conn.close()

            if row:
                total_realized = row[2] or 0.0
                total_actual = row[3] or 0.0
                return {
                    "total_trades": row[0] or 0,
                    "resolved_trades": row[1] or 0,
                    "total_realized_pnl": round(total_realized, 2),
                    "total_actual_pnl": round(total_actual, 2),
                    "divergence": round(total_actual - total_realized, 2),
                    "open_positions": row[4] or 0,
                }
        except sqlite3.Error as e:
            logger.error(f"DB summary query error: {e}")

        return {
            "total_trades": 0,
            "resolved_trades": 0,
            "total_realized_pnl": 0.0,
            "total_actual_pnl": 0.0,
            "divergence": 0.0,
            "open_positions": 0,
        }

    def get_recent_resolutions(self, limit: int = 20) -> list[dict]:
        """Get recent resolution events from the DB for dashboard display."""
        db_path = self._db_path
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA busy_timeout=3000")
            rows = conn.execute("""
                SELECT
                    market_title, condition_id, side, entry_price,
                    position_size_usd, realized_pnl, actual_pnl,
                    actual_return, resolution_outcome, timestamp
                FROM trades
                WHERE actual_pnl IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,)).fetchall()
            conn.close()

            return [
                {
                    "market_title": r[0] or "N/A",
                    "condition_id": (r[1] or "")[:20] + "..." if r[1] else "N/A",
                    "side": r[2] or "?",
                    "entry_price": r[3] or 0,
                    "size": r[4] or 0,
                    "realized_pnl": r[5] or 0,
                    "actual_pnl": r[6] or 0,
                    "actual_return": r[7] or 0,
                    "resolution_outcome": r[8] or "",
                    "timestamp": r[9] or "",
                }
                for r in rows
            ]
        except sqlite3.Error:
            return []
