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
import time
import functools

from nautilus_trader.model.identifiers import TradeId, VenueOrderId
from nautilus_trader.model.objects import Price, Money
from nautilus_trader.model.enums import LiquiditySide


PAPER_FILL_PRICE_CACHE: dict[str, float] = {}
POLYMARKET_MIDPOINT_URL = "https://clob.polymarket.com/midpoint?token_id={token_id}"
_lock = threading.Lock()

# 30‑second TTL cache for Polymarket midpoint prices
_POLY_PRICE_CACHE: dict[str, tuple[float, float]] = {}
_POLY_CACHE_TTL = 30.0  # seconds

def _poly_price_cache_get(token_id: str) -> float | None:
    entry = _POLY_PRICE_CACHE.get(token_id)
    if entry is None:
        return None
    price, ts = entry
    if time.time() - ts < _POLY_CACHE_TTL:
        return price
    # Expired – remove
    del _POLY_PRICE_CACHE[token_id]
    return None

def _poly_price_cache_set(token_id: str, price: float) -> None:
    _POLY_PRICE_CACHE[token_id] = (price, time.time())


def set_fill_price(instrument_id_str: str, price: float) -> None:
    PAPER_FILL_PRICE_CACHE[instrument_id_str] = price


def get_fill_price(instrument_id_str: str) -> float | None:
    return PAPER_FILL_PRICE_CACHE.get(instrument_id_str)


class PaperExecClient:
    """Standalone mixin — monkey-patched onto SandboxExecutionClient by run_paper.py.

    The submit_order method generates OrderFilled events directly so our
    computed fill price is actually used, instead of the Cython default ($0.50).
    """

    @staticmethod
    def _resolve_fill_price(self, command) -> float:
        """Determine the best fill price for this order.

        Priority: order price → intended registry → cache → API → $0.50.

        'self' here is the SandboxExecutionClient instance (passed explicitly
        because this is a static method assigned as a replacement).
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

        # 4. Polymarket API midpoint with 30‑second TTL cache
        if fill_price is None:
            try:
                parts = inst_key.split("-")
                if len(parts) >= 2:
                    token_id = parts[-1].split(".")[0]
                    # Use cached price if fresh
                    cached = _poly_price_cache_get(token_id)
                    if cached is not None:
                        fill_price = cached
                    else:
                        url = POLYMARKET_MIDPOINT_URL.format(token_id=token_id)
                        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                        with urllib.request.urlopen(req, timeout=5) as resp:
                            data = json.loads(resp.read().decode())
                        price_str = data.get("midpoint") or data.get("price")
                        if price_str is not None:
                            fill_price = float(price_str)
                            _poly_price_cache_set(token_id, fill_price)
            except Exception:
                pass

        # 5. Last resort
        if fill_price is None:
            fill_price = 0.50

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
        print(f"[PaperExecClient] Filling {inst_key[:50]}... at ${fill_price:.4f}")

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
