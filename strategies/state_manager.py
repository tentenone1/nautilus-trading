"""State Manager — Position tracking, fill handling, and position recovery.

Extracted from WhaleFollower to decompose the god class. Handles:
  - Order fill processing (DB logging, validation events, fade tracking)
  - Position state management (_open_positions, _pending_whales, etc.)
  - Position recovery from DB on restart
  - Daily state persistence

The strategy (WhaleFollower) delegates to StateManager while retaining
the Nautilus Strategy lifecycle hooks.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import InstrumentId, Venue

from strategies.wf_constants import (
    LIVE_ENTRY_PRICE_CAPS,
    ACTIVE_CONFIG_VERSION,
)
from strategies.wf_position_persistence import save_open_positions, save_daily_state, load_daily_state
from strategies.wf_db_ops import log_trade_to_db, update_trade_latency_fields
import json

# Validation integration (graceful degradation)
try:
    from components.validation.event_logger import EventType, log_event
    from components.validation.trade_context import TradeContext, get_trade_context
    from components.validation.snapshot_store import freeze_snapshot
    from components.validation.db_router import get_current_mode
    _validation_available = True
except ImportError:
    _validation_available = False
    EventType = None
    log_event = None
    TradeContext = None
    get_trade_context = None
    freeze_snapshot = None
    get_current_mode = lambda: "paper"


class StateManager:
    """Manages position state, fill handling, and recovery for the whale follower.

    Receives a reference to the strategy for Nautilus-specific operations
    and manages position tracking state through the strategy's state dicts.
    """

    def __init__(self, strategy):
        """Initialize with a reference to the WhaleFollower strategy instance."""
        self._s = strategy

    @property
    def log(self):
        return self._s.log

    @property
    def config(self):
        return self._s.config

    @property
    def cache(self):
        return self._s.cache

    # ── Fill Handler ──────────────────────────────────────────────────────────

    def on_order_filled(self, event: OrderFilled) -> None:
        """Process a filled order: log to DB, update state, emit validation events."""
        s = self._s
        conn = None
        try:
            db_path = Path(__file__).parent.parent / "research" / "trades.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(str(db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    whale_name TEXT,
                    whale_address TEXT,
                    category TEXT NOT NULL,
                    market_title TEXT,
                    condition_id TEXT,
                    token_id TEXT,
                    side TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    position_size_usd REAL,
                    kelly_fraction REAL,
                    confidence REAL,
                    edge_score REAL,
                    signal_source TEXT,
                    entry_reason TEXT,
                    exit_reason TEXT,
                    realized_pnl REAL,
                    realized_return REAL,
                    duration_seconds REAL,
                    resolution_outcome TEXT,
                    dispute_flag INTEGER DEFAULT 0,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            inst_id = str(event.instrument_id)
            s.log.info(f"[DEBUG] FILL event: client_order_id={event.client_order_id} type={type(event.client_order_id).__name__} pending_keys={list(s._pending_whales.keys())}")

            # Look up whale metadata from pending dict
            pending = s._pending_whales.pop(str(event.client_order_id), {})

            # Recovery from _open_positions if no pending metadata
            if not pending:
                inst_key = str(event.instrument_id)
                recovered = s._open_positions.get(inst_key, {})
                if recovered:
                    raw_name = recovered.get("whale_name", "unknown")
                    if not raw_name or raw_name.lower() in ("", "unknown", "unknown whale"):
                        import logging as _lg
                        inst_label = str(event.instrument_id)[:30]
                        _lg.getLogger("whale_follower").warning(
                            f"Recovery: empty whale_name for {inst_label}..., marking as 'unknown'"
                        )
                    pending = {
                        "whale_name": raw_name,
                        "market_title": recovered.get("market_title", ""),
                        "category": recovered.get("category", ""),
                        "whale_address": "",
                        "edge_score": recovered.get("edge_score", 0.0),
                        "confidence": recovered.get("confidence", 0.0),
                        "entry_reason": "recovered_after_restart",
                        "kelly_fraction": s.config.kelly_fraction,
                        "entry_price": recovered.get("entry_price", 0.5),
                        "signal_source": "whale_tracker",
                    }
                    s.log.info(f"[RECOVER] Recovered metadata from _open_positions for {inst_key[:50]}...")

            if not pending:
                s.log.debug("No pending orders in fill handler, skipping")
                return

            raw_entry = pending.get('entry_price', None)
            if raw_entry is None or (isinstance(raw_entry, (int, float)) and raw_entry == 0.0):
                entry_price = event.last_px.as_double() if hasattr(event, 'last_px') and event.last_px else 0.5
            else:
                entry_price = raw_entry

            # ── Extract actual fill price from OrderFilled event ───────────────────────
            # event.last_px is the actual modeled fill price from paper_execution.py
            # (includes slippage applied via _apply_slippage). This is what we
            # wire to the DB so slippage_bps is non-zero and reflects real costs.
            # entry_price (above) is the intended price — used for the trade record
            # and P&L comparison; actual_fill_price is used for execution analytics.
            _raw_actual = None
            if hasattr(event, 'last_px') and event.last_px:
                try:
                    _raw_actual = event.last_px.as_double()
                except Exception:
                    try:
                        _raw_actual = float(event.last_px)
                    except Exception:
                        pass
            actual_fill_price = _raw_actual if (_raw_actual is not None and _raw_actual > 0) else entry_price

            qty = event.last_qty.as_double() if hasattr(event, 'last_qty') and event.last_qty else 125
            size_usd = qty * entry_price

            whale_name_raw = pending.get("whale_name", "unknown")
            if not whale_name_raw or whale_name_raw.lower() in ("", "unknown", "unknown whale"):
                wallet = pending.get("whale_address", "")
                if wallet:
                    whale_name_raw = f"whale_0x{wallet[:6].lower()}"
                    import logging as _lg
                    _lg.getLogger("whale_follower").warning(
                        f"Fallback naming: {whale_name_raw!r} for wallet {wallet[:10]}..."
                    )
            whale_name = whale_name_raw

            market_title = pending.get("market_title", "") or ""
            category = pending.get("category", "") or ""
            whale_address = pending.get("whale_address", "") or ""
            edge_score = pending.get("edge_score", 0.0) or 0.0
            confidence = pending.get("confidence", 0.0) or 0.0
            entry_reason = pending.get("entry_reason", "") or ""
            kelly_fraction = pending.get("kelly_fraction", s.config.kelly_fraction) or s.config.kelly_fraction
            signal_source = pending.get("signal_source", "whale_tracker") or "whale_tracker"
            is_fade = pending.get("is_fade", False)
            validation_signal_id = pending.get("_validation_signal_id", "")
            validation_snapshot_id = pending.get("_validation_snapshot_id", "")

            # Categorize market
            inst_key = str(event.instrument_id)
            cond_id = inst_key.split("-")[0] if "-" in inst_key else inst_key
            if not category:
                try:
                    from strategies.whale_tracker_new import _categorize_market
                    category = _categorize_market(inst_key) or "Unknown"
                except Exception:
                    category = "Unknown"

            trade_id = str(uuid.uuid4())
            side_str = "BUY" if str(event.order_side) == "OrderSide.BUY" else "SELL"

            # Track in _open_positions
            s._open_positions[inst_key] = {
                "whale_name": whale_name,
                "market_title": market_title,
                "category": category,
                "side": side_str,
                "entry_price": entry_price,
                "size": size_usd,
                "entry_time": time.time(),
                "trade_id": trade_id,
                "condition_id": cond_id,
                "venue_position_id": "",
                "edge_score": edge_score,
                "confidence": confidence,
                "is_fade": is_fade,
            }
            save_open_positions(s._open_positions)

            # Log to DB
            log_trade_to_db(
                trade_id=trade_id,
                whale_name=whale_name,
                whale_address=whale_address,
                category=category,
                market_title=market_title,
                condition_id=cond_id,
                side=side_str,
                entry_price=entry_price,
                position_size_usd=size_usd,
                kelly_fraction=kelly_fraction,
                confidence=confidence,
                edge_score=edge_score,
                signal_source=signal_source,
                entry_reason=entry_reason,
                instrument_id=inst_key,
                config_version=ACTIVE_CONFIG_VERSION,
                whale_type=getattr(s, '_last_whale_type', ''),
            )

            # Validation events
            filled_ts = time.monotonic_ns()
            client_order_id = str(event.client_order_id)

            if _validation_available and log_event and EventType and validation_signal_id:
                try:
                    if s._validation_context:
                        try:
                            s._validation_context.register_fill(
                                client_order_id=client_order_id,
                                filled_ts=filled_ts,
                                actual_price=float(actual_fill_price),
                                filled_size=float(size_usd),
                            )
                        except Exception as ctx_err:
                            s.log.warning(f"Trade context fill registration failed: {ctx_err}")

                    # Compute slippage directly from actual_fill_price and entry_price.
                    # BUG FIX: TradeContext.compute_slippage() was returning 0 for all trades
                    # due to a context lookup issue (intended_entry_price=0 even when
                    # register_submission was called). We bypass it and compute directly.
                    # For BUY: adverse = paid MORE than intended → (actual - intended) / intended * 10000
                    # For SELL: adverse = received LESS than intended → (intended - actual) / intended * 10000
                    if entry_price and entry_price > 0 and actual_fill_price and actual_fill_price > 0:
                        price_diff = actual_fill_price - entry_price
                        if side_str == "SELL":
                            price_diff = -price_diff
                        slippage_bps_raw = (price_diff / entry_price) * 10000
                        # Clamp extreme values: realistic slippage is [-500, +500] bps
                        slippage_bps_final = max(-500.0, min(500.0, round(slippage_bps_raw, 1)))
                    else:
                        slippage_bps_final = 0.0
                    slippage = {"slippage_bps": slippage_bps_final, "fill_completion_pct": 100.0}
                    latencies = {"detection_delay_ms": 0, "execution_delay_ms": 0, "fill_delay_ms": 0, "total_latency_ms": 0}
                    if s._validation_context:
                        try:
                            latencies = s._validation_context.compute_latencies(client_order_id)
                        except Exception:
                            pass

                    log_event(
                        event_type=EventType.TRADE_FILLED,
                        payload={
                            "signal_id": validation_signal_id,
                            "snapshot_id": validation_snapshot_id,
                            "trade_id": trade_id,
                            "client_order_id": client_order_id,
                            "whale_name": whale_name,
                            "market_title": market_title[:80],
                            "category": category,
                            "side": event.order_side.name if hasattr(event, 'order_side') else 'BUY',
                            "actual_fill_price": float(actual_fill_price),
                            "filled_size_usd": float(size_usd),
                            "quantity": float(qty),
                            "instrument_id": str(event.instrument_id)[:80],
                            "detection_delay_ms": latencies["detection_delay_ms"],
                            "execution_delay_ms": latencies["execution_delay_ms"],
                            "fill_delay_ms": latencies["fill_delay_ms"],
                            "total_latency_ms": latencies["total_latency_ms"],
                            "slippage_bps": slippage["slippage_bps"],
                            "fill_completion_pct": slippage["fill_completion_pct"],
                            "ts_mono_ns": filled_ts,
                        },
                        correlation_id=validation_signal_id,
                        mode=get_current_mode(),
                        strategy_id="whale_follower",
                        run_id=s._validation_run_id,
                    )
                    s.log.debug(f"Validation: TRADE_FILLED {trade_id[:8]}... latency={latencies['total_latency_ms']}ms slippage={slippage['slippage_bps']:.1f}bps")

                    try:
                        # actual_fill_price was already registered above; pass it directly
                        # to avoid a redundant context lookup. The local variable is the
                        # event.last_px value (real modeled fill price from paper_executor).
                        update_trade_latency_fields(
                            trade_id=trade_id,
                            detection_delay_ms=latencies["detection_delay_ms"],
                            execution_delay_ms=latencies["execution_delay_ms"],
                            fill_delay_ms=latencies["fill_delay_ms"],
                            total_latency_ms=latencies["total_latency_ms"],
                            slippage_bps=slippage["slippage_bps"],
                            fill_completion_pct=slippage["fill_completion_pct"],
                            actual_fill_price=actual_fill_price,
                        )
                    except Exception as lat_err:
                        s.log.debug(f"Latency DB update failed: {lat_err}")
                except Exception as e:
                    s.log.warning(f"Validation event emission failed: {e}")

            # Track fade positions
            if is_fade:
                s._fade_positions.add(inst_key)
                s.log.info(f"FADE position opened: {whale_name} | {inst_key[:50]}... ({len(s._fade_positions)}/{s._fade_max_concurrent})")

            # Price pump tracking hook
            try:
                from components.price_tracker import subscribe as _pt_subscribe
                _pt_subscribe(
                    market_id=cond_id, signal_id=trade_id, entry_price=entry_price,
                    whale_address=whale_address, whale_name=whale_name, market_title=market_title,
                )
            except ImportError:
                pass
            except Exception as _pt_err:
                s.log.warning("Price tracker hook failed: %s", _pt_err)

        except Exception as e:
            s.log.error(f"[DB] Failed to log trade error={e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Position Recovery ─────────────────────────────────────────────────────

    def recover_open_positions(self) -> None:
        """Reload unfinished positions from DB on restart."""
        s = self._s
        try:
            db_path = Path(__file__).parent.parent / "research" / "trades.db"
            if not db_path.exists():
                s.log.info("[RECOVER] No trades DB found, skipping recovery")
                return

            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                "SELECT instrument_id, trade_id, whale_name, market_title, category, "
                "side, entry_price, position_size_usd, condition_id, edge_score "
                "FROM trades WHERE exit_reason IS NULL "
                "AND instrument_id IS NOT NULL ORDER BY timestamp"
            ).fetchall()
            conn.close()

            if not rows:
                s.log.info("[RECOVER] No orphan positions to recover")
                return

            recovered = 0
            for row in rows:
                inst_id, trade_id, whale_name, market_title, category, side, entry_price, size, cond_id, edge_score = row
                try:
                    from nautilus_trader.model.identifiers import InstrumentId
                    inst_key = str(InstrumentId.from_str(inst_id))
                except Exception:
                    inst_key = inst_id

                if inst_key not in s._open_positions:
                    s._open_positions[inst_key] = {
                        "whale_name": whale_name or "unknown",
                        "market_title": market_title or inst_id[:80],
                        "category": category or "Unknown",
                        "side": side or "BUY",
                        "entry_price": entry_price or 0.5,
                        "size": size or 0.0,
                        "entry_time": 0.0,
                        "trade_id": trade_id,
                        "condition_id": cond_id or "",
                        "venue_position_id": "",
                        "edge_score": edge_score or 0.0,
                    }
                    recovered += 1

            s.log.info(f"[RECOVER] Recovered {recovered} open positions from DB (total tracked: {len(s._open_positions)})")
        except Exception as e:
            s.log.error(f"[RECOVER] Failed to recover open positions: {e}")


# ── Gap State ────────────────────────────────────────────────────────────────
# Merged from gap_state.py

DEFAULT_GAP_STATE = {
    "prev_open_count": 0,
    "stall_start": None,
    "consecutive_gaps": 0,
    "recent_signals": 0,
    "latest_signal": None,
    "recent_trades": 0,
    "latest_trade": None,
    "open_trade_count": 0,
}


def update_gap_state(signal, state_file: Path | None = None) -> None:
    """Update signal_trade_gap_state.json on every valid signal.

    Increments signal/trade counters, updates timestamps, resets gap counters.
    Atomic write via temp file + rename.

    Args:
        signal: A WhaleSignal object (attributes not used, just triggers the update).
        state_file: Path to the state file. Defaults to project_root/.signal_trade_gap_state.json
    """
    if state_file is None:
        state_file = Path(__file__).parent.parent / ".signal_trade_gap_state.json"

    try:
        if state_file.exists():
            with open(state_file) as f:
                state = json.load(f)
        else:
            state = DEFAULT_GAP_STATE.copy()
    except (FileNotFoundError, json.JSONDecodeError):
        state = DEFAULT_GAP_STATE.copy()

    now_iso = datetime.now(timezone.utc).isoformat()
    state["recent_signals"] = state.get("recent_signals", 0) + 1
    state["latest_signal"] = now_iso
    state["recent_trades"] = state.get("recent_trades", 0) + 1
    state["latest_trade"] = now_iso
    state["consecutive_gaps"] = 0

    tmp = state_file.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    tmp.rename(state_file)
