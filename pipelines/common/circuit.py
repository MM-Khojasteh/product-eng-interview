"""Circuit breaker with failure classification (spec §1 / dossier §6).

    blocked      (403/401/auth wall)  threshold 3 → OPEN for 600 s  — "change approach"
    rate_limited (429 exhausted)      immediate → OPEN for a short cooldown (backoff)
    error        (transport/5xx)      threshold 5 → OPEN for 120 s

OPEN → HALF_OPEN after the window (one probe allowed); probe success closes,
probe failure re-opens. Thread-safe: the cadence loop and the on-demand scan
executor record through the same breaker, so transitions are serialized.
"""

from __future__ import annotations

import enum
import threading
import time
from collections.abc import Callable


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        blocked_threshold: int = 3,
        error_threshold: int = 5,
        blocked_open_seconds: float = 600.0,      # dossier §6: 10 min
        error_open_seconds: float = 120.0,
        rate_limited_open_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.blocked_threshold = blocked_threshold
        self.error_threshold = error_threshold
        self.blocked_open_seconds = blocked_open_seconds
        self.error_open_seconds = error_open_seconds
        self.rate_limited_open_seconds = rate_limited_open_seconds
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._open_until = 0.0
        self._consecutive_blocked = 0
        self._consecutive_errors = 0
        self._lock = threading.Lock()

    def _resolved_state(self) -> CircuitState:  # call with the lock held
        if self._state is CircuitState.OPEN and self._clock() >= self._open_until:
            self._state = CircuitState.HALF_OPEN  # window elapsed — probe permitted
        return self._state

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._resolved_state()

    def allow(self) -> bool:
        return self.state is not CircuitState.OPEN

    def record_success(self) -> None:
        with self._lock:
            recovered = self._state is not CircuitState.CLOSED
            self._state = CircuitState.CLOSED
            self._consecutive_blocked = 0
            self._consecutive_errors = 0
        if recovered:
            # the OPEN transition logs (see _open); the recovery must too — a
            # circuit that closes invisibly is a breaker nobody can trust
            import logging

            logging.getLogger("torob.circuit").info(
                "circuit '%s' CLOSED — recovered", self.name
            )

    def record_failure(self, kind: str) -> None:
        with self._lock:
            half_open_probe = self._resolved_state() is CircuitState.HALF_OPEN
            if kind == "blocked":
                self._consecutive_blocked += 1
                self._consecutive_errors = 0
                if self._consecutive_blocked >= self.blocked_threshold or half_open_probe:
                    # a FAILED probe from half-open re-opens immediately
                    self._open(self.blocked_open_seconds)
            elif kind == "rate_limited":
                # a single exhausted 429 already means we hammered — cool down now
                self._open(self.rate_limited_open_seconds)
            else:
                self._consecutive_errors += 1
                self._consecutive_blocked = 0
                if self._consecutive_errors >= self.error_threshold or half_open_probe:
                    self._open(self.error_open_seconds)

    def _open(self, seconds: float) -> None:
        was_open = self._state is CircuitState.OPEN
        self._state = CircuitState.OPEN
        self._open_until = self._clock() + seconds
        self._consecutive_blocked = 0
        self._consecutive_errors = 0
        if not was_open:
            # log-worthy transition; import here to avoid a logging cycle at module load
            import logging

            logging.getLogger("torob.circuit").warning(
                "circuit '%s' OPEN for %.0fs", self.name, seconds
            )

    def summary(self) -> dict[str, object]:
        with self._lock:
            state = self._resolved_state()
            return {
                "name": self.name,
                "state": state.value,
                "open_for_s": max(0.0, self._open_until - self._clock())
                if state is CircuitState.OPEN
                else 0.0,
            }
