"""The insert/refresh/evict core shared by every OrderedDict-LRU in the stack.

Extracted from four byte-identical bodies (raw_worker.OnDemandScans._seen,
normalizer._remember_scan, and the BFF's flight/pulse indexes in
src) — callers keep their own limits and wrap this with any pre-checks
(dedupe, rollback) that are specific to their use.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")


def remember_latest(cache: "OrderedDict[K, V]", key: K, value: V, limit: int) -> None:
    """Insert or refresh `key`, move it to the MRU end, evict past `limit`."""
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)
