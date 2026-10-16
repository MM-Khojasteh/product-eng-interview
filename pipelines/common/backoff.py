"""Linear retry backoff — the transport/DB retry loops' shared arithmetic.

Four call sites used to hand-roll `min(2.0 * attempt, CAP)` sleeps (kafkaio ×2,
kafka_to_db ×2); the supplier layer's exponential+ jitter backoff is a
different formula and stays in suppliers/base.py (`SupplierStrategy._backoff`).
"""

from __future__ import annotations

import time


def backoff_seconds(attempt: int, base: float = 2.0, cap: float = 10.0) -> float:
    return min(base * attempt, cap)


def sleep_backoff(attempt: int, base: float = 2.0, cap: float = 10.0,
                  sleep=time.sleep) -> None:
    sleep(backoff_seconds(attempt, base, cap))
