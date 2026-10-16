"""Token-bucket rate limiter — the per-supplier politeness ceiling (dossier §6:
"≤ 6 initiated searches/min per OTA"). Thread-safe: the cadence loop and the
on-demand scan executor share one bucket per supplier, and the ceiling must
hold across both — the lock serializes the token math; waiting for a refill
happens OUTSIDE the lock so a blocked acquirer never freezes the bucket."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class TokenBucket:
    def __init__(
        self,
        name: str,
        capacity: float,
        refill_per_minute: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.name = name
        self.capacity = float(capacity)
        self.rate_per_second = refill_per_minute / 60.0
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(capacity)
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:  # call with the lock held
        now = self._clock()
        self._tokens = min(
            self.capacity, self._tokens + (now - self._updated) * self.rate_per_second
        )
        self._updated = now

    def try_acquire(self, timeout: float = 0.0) -> bool:
        """Consume one token; wait up to `timeout` for a refill if empty."""
        deadline = self._clock() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                need = 1.0 - self._tokens
            remaining = deadline - self._clock()
            if remaining <= 0:
                return False
            self._sleep(max(0.0, min(need / self.rate_per_second, remaining)))

    @property
    def tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens
