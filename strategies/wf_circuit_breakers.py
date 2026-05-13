"""Whale Follower — Circuit Breakers.

Protects external dependencies (LLM scorer, Polymarket API, DB) with
per-component circuit breakers. Prevents cascade failures when a downstream
service goes down.

Three breakers:
  - llm_scorer:   3 failures → open, 60s recovery
  - polymarket_api: 5 failures → open, 30s recovery
  - trades_db:    3 failures → open, 30s recovery
"""

from __future__ import annotations

import logging
import time
from enum import Enum, auto
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

from dataclasses import dataclass, field

P = ParamSpec("P")
T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreakerOpen(Exception):
    """Raised when a circuit breaker is OPEN and no fallback was provided."""

    pass


@dataclass
class CircuitBreaker:
    """Standard circuit breaker with half-open probing.

    State transitions:
      CLOSED     → OPEN       (failure_count >= failure_threshold)
      OPEN       → HALF_OPEN (recovery_timeout elapsed)
      HALF_OPEN  → CLOSED    (success_count >= half_open_success_threshold)
      HALF_OPEN  → OPEN      (any failure while probing)
      CLOSED     → CLOSED    (any success resets failure_count)

    Usage:
        breaker = CircuitBreaker(name="my_service", failure_threshold=5, recovery_timeout=30.0)

        # As a decorator:
        @breaker
        def my_func():
            ...

        # As a context manager:
        with breaker:
            my_func()

        # Directly:
        result = breaker.call(my_func, fallback=None)  # raises CircuitBreakerOpen
        result = breaker.call(my_func, fallback="default")  # returns "default"
    """

    name: str
    failure_threshold: int = 3
    recovery_timeout: float = 60.0
    half_open_success_threshold: int = 1

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Current consecutive failure count."""
        return self._failure_count

    def call(
        self,
        func: Callable[P, T],
        *args: P.args,
        fallback: T | None = None,
        **kwargs: P.kwargs,
    ) -> T | None:
        """Execute func through the circuit breaker.

        Args:
            func:    Function to execute.
            *args:   Positional arguments to pass to func.
            fallback: Value to return if circuit is OPEN (no exception raised).
                      If None and circuit is OPEN, raises CircuitBreakerOpen.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            The result of func(), or fallback if circuit is OPEN.

        Raises:
            CircuitBreakerOpen: If circuit is OPEN and no fallback given.
        """
        logger = logging.getLogger("whale_follower")

        # ── OPEN circuit: reject immediately ─────────────────────────────────
        if self._state == CircuitState.OPEN:
            now = time.time()
            if now - self._last_failure_time >= self.recovery_timeout:
                # Transition to HALF_OPEN — allow one probe call
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.warning(f"Circuit breaker [{self.name}] HALF_OPEN (recovery timeout)")
            else:
                logger.debug(f"Circuit breaker [{self.name}] OPEN (rejecting request)")
                if fallback is not None:
                    return fallback
                raise CircuitBreakerOpen(f"Circuit breaker [{self.name}] is OPEN")

        # ── Execute the protected call ──────────────────────────────────────
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            logger.warning(f"Circuit breaker [{self.name}] call failed: {e}")
            if fallback is not None:
                return fallback
            raise

    def _record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.half_open_success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                logger = logging.getLogger("whale_follower")
                logger.info(f"Circuit breaker [{self.name}] CLOSED (recovery confirmed)")
        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success in CLOSED state
            self._failure_count = 0

    def _record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            # Any failure while probing → immediately OPEN again
            self._state = CircuitState.OPEN
            logger = logging.getLogger("whale_follower")
            logger.warning(f"Circuit breaker [{self.name}] OPEN (half-open failure)")
            return

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger = logging.getLogger("whale_follower")
            logger.error(
                f"Circuit breaker [{self.name}] OPEN (threshold exceeded: "
                f"{self._failure_count} failures)"
            )

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        """Decorator usage: @breaker decorator."""

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
            return self.call(func, *args, **kwargs)

        return wrapper

    def __enter__(self) -> "CircuitBreaker":
        """Context manager usage: with breaker: ..."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """On exit from context manager: record success or failure."""
        if exc_type is None:
            self._record_success()
        else:
            self._record_failure()
        return False  # Don't suppress exceptions


# ── Module-level singletons ────────────────────────────────────────────────────

llm_circuit_breaker = CircuitBreaker(
    name="llm_scorer",
    failure_threshold=3,
    recovery_timeout=60.0,
)

api_circuit_breaker = CircuitBreaker(
    name="polymarket_api",
    failure_threshold=5,
    recovery_timeout=30.0,
)

db_circuit_breaker = CircuitBreaker(
    name="trades_db",
    failure_threshold=3,
    recovery_timeout=30.0,
)


def get_whale_api_breaker() -> CircuitBreaker:
    """Return the circuit breaker for the Polymarket whale position API."""
    return api_circuit_breaker


def get_clob_breaker() -> CircuitBreaker:
    """Return the circuit breaker for the Polymarket CLOB API."""
    return api_circuit_breaker
