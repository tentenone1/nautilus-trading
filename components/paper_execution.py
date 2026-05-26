import time
try:
    import psycopg2 as _psycopg2
    HAS_PG = True
except ImportError:
    HAS_PG = False
"""Auto-fill paper execution client — fills every submitted order immediately.
Fills at real Polymarket prices when available, falls back to $0.50.

Generates fill events directly (bypasses Cython SandboxExecutionClient.submit_order)
so our computed fill price is actually used for the OrderFilled event.

Fill priority:
1. The command price (for limit orders)
2. The intended price registry (set by strategy pre-submit)
3. Cached price data (last/mid/quote tick)
4. Polymarket public API midpoint price
5. $0.50 as last resort
"""

import json
import uuid
import threading
import urllib.request
from urllib.error import URLError

from nautilus_trader.model.identifiers import TradeId, VenueOrderId
from nautilus_trader.model.objects import Price, Money
from nautilus_trader.model.enums import LiquiditySide


PAPER_FILL_PRICE_CACHE: dict[str, float] = {}
POLYMARKET_MIDPOINT_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"
_lock = threading.Lock()

# Slippage model parameters
PAPER_SLIPPAGE_ENABLED = True
PAPER_SLIPPAGE_PCT = 0.020        # 2% adverse slippage on market orders
PAPER_SPREAD_HALF = 0.005         # 0.5% half-spread cost
PAPER_EXIT_FILL_PROB = 0.70       # 70% fill probability on stop exits
PAPER_EXIT_WORSE_SLIPPAGE = 0.030 # +3% worse slippage when exit misses trigger
# G2 constants: realistic slippage model (SLIPPAGE_BPS=200, FILL_PROB=0.70)
PAPER_SLIPPAGE_BPS = 200           # 200 bps = 2% adverse price movement
PAPER_FILL_PROB = 0.70             # 70% probability of getting filled at modeled price


def _apply_slippage(fill_price, side_str, is_exit=False, condition_id=None, token_id=None):
    """Apply realistic slippage and spread costs to fill price.

    Returns a (fill_price, filled) tuple:
      - filled=True: trade executes at fill_price
      - filled=False: no fill (skip this trade)

    Uses real PostgreSQL orderbook spread when available, falls back to flat
    0.5% half-spread. Exit orders have 30% chance of missing their trigger price
    (PAPER_EXIT_FILL_PROB=0.70), with 3% worse slippage when they do.
    """
    import random
    if not PAPER_SLIPPAGE_ENABLED:
        return fill_price, True

    # Try to use actual spread from orderbook data
    actual_spread_half = PAPER_SPREAD_HALF  # fallback
    if condition_id:
        depth = get_orderbook_depth_from_pg(condition_id, token_id or "")
        if depth and depth.get("spread", 0) > 0:
            # Use actual half-spread from orderbook (capped at 5% to prevent extreme values)
            actual_spread_half = min(depth["spread"] / 2, 0.05)

    spread_cost = fill_price * actual_spread_half
    slip_pct = PAPER_SLIPPAGE_PCT
    if is_exit and random.random() > PAPER_EXIT_FILL_PROB:
        slip_pct = PAPER_SLIPPAGE_PCT + PAPER_EXIT_WORSE_SLIPPAGE

    # ── No-fill check (G2) ────────────────────────────────────────────────────
    # With PAPER_FILL_PROB=0.70, ~30% of attempted trades are skipped.
    # This is the key change from instant-instant paper fills to realistic ones.
    if random.random() > PAPER_FILL_PROB:
        return fill_price, False  # No fill — skip this trade

    slippage_cost = fill_price * slip_pct
    if side_str.upper() == "BUY":
        adjusted = fill_price + spread_cost + slippage_cost
    else:
        adjusted = fill_price - spread_cost - slippage_cost
    return max(0.01, min(0.99, adjusted)), True


def set_fill_price(instrument_id_str: str, price: float) -> None:
    PAPER_FILL_PRICE_CACHE[instrument_id_str] = price


def get_fill_price(instrument_id_str: str) -> float | None:
    return PAPER_FILL_PRICE_CACHE.get(instrument_id_str)




def get_orderbook_depth_from_pg(condition_id: str, token_id: str) -> dict:
    """Look up actual orderbook spread/depth from PostgreSQL adapter data."""
    if not HAS_PG:
        return {}
    try:
        conn = _psycopg2.connect(
            "postgresql://polymarket:polymarket@localhost:5432/polymarket_orderbook",
            connect_timeout=3,
        )
        cur = conn.cursor()
        # Get latest snapshot for this condition_id + token_id
        if token_id:
            cur.execute(
                "SELECT best_bid, best_ask, midpoint, spread, bid_depth, ask_depth "
                "FROM clob_orderbook_snapshots "
                "WHERE condition_id = %s AND token_id = %s "
                "ORDER BY id DESC LIMIT 1",
                (condition_id, token_id),
            )
        else:
            cur.execute(
                "SELECT best_bid, best_ask, midpoint, spread, bid_depth, ask_depth "
                "FROM clob_orderbook_snapshots "
                "WHERE condition_id = %s "
                "ORDER BY id DESC LIMIT 1",
                (condition_id,),
            )
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                "best_bid": float(row[0]),
                "best_ask": float(row[1]),
                "midpoint": float(row[2]),
                "spread": float(row[3]),
                "bid_depth": float(row[4]),
                "ask_depth": float(row[5]),
            }
    except Exception:
        pass
    return {}

class PaperExecClient:
    """Standalone mixin — monkey-patched onto SandboxExecutionClient by run_paper.py.

    The submit_order method generates OrderFilled events directly so our
    computed fill price is actually used, instead of the Cython default ($0.50).
    """

    @staticmethod
    def _resolve_fill_price(self, command) -> float | None:
        """Determine the best fill price for this order.

        Priority: order price → intended registry → cache → Polymarket API.
        Returns None if no real price is available (no fake $0.50 fallback).
        """
        order = command.order
        inst_key = str(command.instrument_id)
        fill_price = None

        # 1. Use explicit order price (limit orders)
        if order.has_price and order.price is not None:
            try:
                fill_price = float(order.price.as_double())
            except Exception:
                try:
                    fill_price = float(order.price)
                except Exception:
                    pass

        # 2. Use intended price from strategy registry
        if fill_price is None:
            intended = get_fill_price(inst_key)
            if intended is not None:
                fill_price = intended

        # 3-6: Use cache prices or API fallback or $0.50
        if fill_price is None:
            try:
                instrument = self._cache.instrument(command.instrument_id)
                if instrument is not None:
                    # LAST price
                    try:
                        last = self._cache.price(command.instrument_id, self._cache.price_type("LAST"))
                        if last is not None:
                            fill_price = float(last.as_double())
                    except Exception:
                        pass
                    # MID price
                    if fill_price is None:
                        try:
                            mid = self._cache.price(command.instrument_id, self._cache.price_type("MID"))
                            if mid is not None:
                                fill_price = float(mid.as_double())
                        except Exception:
                            pass
                    # Quote tick midpoint
                    if fill_price is None:
                        try:
                            tick = self._cache.quote_tick(command.instrument_id)
                            if tick is not None:
                                bid = tick.bid.as_double()
                                ask = tick.ask.as_double()
                                fill_price = (bid + ask) / 2
                        except Exception:
                            pass
            except Exception:
                pass

        # 4. Polymarket API midpoint (final source — no fake fallback)
        mp_price = None
        if fill_price is None:
            try:
                parts = inst_key.split("-")
                if len(parts) >= 2:
                    token_id = parts[-1].split(".")[0]
                    url = POLYMARKET_MIDPOINT_URL.format(token_id=token_id)
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        data = json.loads(resp.read().decode())
                    price_str = data.get("midpoint") or data.get("price")
                    if price_str is not None:
                        mp_price = float(price_str)
            except Exception:
                pass

        # No real price available — retry once after short delay (race condition with new instruments)
        if mp_price is None and fill_price is None:
            time.sleep(1.5)  # Wait for quotes to arrive after instrument subscription
            # Retry cache lookup
            try:
                instrument = self._cache.instrument(command.instrument_id)
                if instrument is not None:
                    try:
                        last = self._cache.price(command.instrument_id, self._cache.price_type("LAST"))
                        if last is not None:
                            fill_price = float(last.as_double())
                    except Exception:
                        pass
                    if fill_price is None:
                        try:
                            mid = self._cache.price(command.instrument_id, self._cache.price_type("MID"))
                            if mid is not None:
                                fill_price = float(mid.as_double())
                        except Exception:
                            pass
                    if fill_price is None:
                        try:
                            tick = self._cache.quote_tick(command.instrument_id)
                            if tick is not None:
                                bid = tick.bid.as_double()
                                ask = tick.ask.as_double()
                                fill_price = (bid + ask) / 2
                        except Exception:
                            pass
            except Exception:
                pass
            # Retry API midpoint
            if fill_price is None:
                try:
                    parts = inst_key.split("-")
                    if len(parts) >= 2:
                        token_id = parts[-1].split(".")[0]
                        url = POLYMARKET_MIDPOINT_URL.format(token_id=token_id)
                        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                        with urllib.request.urlopen(req, timeout=5) as resp:
                            data = json.loads(resp.read().decode())
                        price_str = data.get("midpoint") or data.get("price")
                        if price_str is not None:
                            fill_price = float(price_str)
                except Exception:
                    pass

        # Use intended price from strategy registry as final fallback
        if fill_price is None:
            intended = get_fill_price(inst_key)
            if intended is not None:
                fill_price = intended
                print(f"[PaperExecClient] Using intended price fallback for {inst_key[:50]}...")

        if fill_price is None:
            print(f"[PaperExecClient] No real price for {inst_key[:50]}..., REJECTING fill")
            return None

        return fill_price

    @staticmethod
    def submit_order(self, command):
        """Replacement for SandboxExecutionClient.submit_order.

        Generates OrderSubmitted → OrderAccepted → OrderFilled events
        directly using the Cython self.generate_* methods, so our
        computed fill price reaches the OrderFilled event.

        'self' is the actual SandboxExecutionClient instance.
        'command' is the SubmitOrder wrapper.
        """
        order = command.order
        inst_key = str(command.instrument_id)

        # Resolve fill price using priority chain
        fill_price = PaperExecClient._resolve_fill_price(self, command)
        if fill_price is None:
            # Retry intended price from registry
            intended = get_fill_price(inst_key)
            if intended is not None:
                fill_price = intended
            else:
                print(f"[PaperExecClient] No real price for {inst_key[:50]}..., skipping fill")
            return
        # Apply realistic slippage model (returns (price, filled) tuple)
        side_str = str(order.side.name) if hasattr(order.side, "name") else str(order.side)
        is_exit = getattr(order, "reduce_only", False)
        original_price = fill_price
        # Extract condition_id and token_id from instrument key for depth-aware fills
        _cond_id = inst_key.split("-")[0] if "-" in inst_key else inst_key.split(".")[0]
        _tok_id = inst_key.split("-")[1].split(".")[0] if "-" in inst_key and len(inst_key.split("-")) > 1 else None
        modeled_price, filled = _apply_slippage(
            fill_price, side_str, is_exit=is_exit, condition_id=_cond_id, token_id=_tok_id
        )
        if not filled:
            print(f"[PaperExecClient] SLIPPAGE_NO_FILL: {inst_key[:50]}... — skipping trade")
            return
        fill_price = modeled_price
        slippage_bps = abs(fill_price - original_price) / original_price * 10000 if original_price > 0 else 0
        print(f"[PaperExecClient] Filling {inst_key[:50]}... at ${fill_price:.4f} (slip:{slippage_bps:.0f}bps exit={is_exit})")

        # Get instrument for precision/currency info
        instrument = None
        try:
            instrument = self._cache.instrument(command.instrument_id)
        except Exception:
            pass

        price_precision = instrument.price_precision if instrument is not None else 8
        quote_currency = instrument.quote_currency if instrument is not None else None

        # Timestamps
        ts_now = self._clock.timestamp_ns()
        venue_order_id = VenueOrderId(str(uuid.uuid4()))
        trade_id = TradeId(str(uuid.uuid4()))

        # 1. Order Submitted
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=ts_now,
        )

        # 2. Order Accepted
        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            ts_event=ts_now,
        )

        # 3. Order Filled — full fill at our computed price
        self.generate_order_filled(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,
            trade_id=trade_id,
            order_side=order.side,
            order_type=order.order_type,
            last_qty=order.quantity,
            last_px=Price(fill_price, precision=price_precision),
            quote_currency=quote_currency,
            commission=Money(0, quote_currency) if quote_currency else Money(0, "USDC"),
            liquidity_side=LiquiditySide.TAKER,
            ts_event=ts_now,
        )
