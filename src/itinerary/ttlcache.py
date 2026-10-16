"""Process-wide TTL memos for the planner's read-mostly seams.

The app_state pattern (an honest module of module-level mutable state) applied
to the planner's two expensive inputs:

  places     db.fetch_places rows — Place entities change WEEKLY (models.py's
             own docstring); re-querying Postgres per page view is pure tax.
  narrative  successful LLM narratives — the only impure/async stage (گام ۳,
             ~25 s on the OpenRouter seam). A repeat view of the same routed
             plan replays the stored Narrative instead of re-billing the LLM.
             Deterministic fallback narratives are NOT cached — they are pure
             and instant, so the offline/demo path keeps today's behaviour
             byte-for-byte.

TTL is the only invalidation channel (no invalidation events exist for
places; they change via the scripts/enrich backfill). One env knob,
TOROB_ITINERARY_CACHE_TTL_SECONDS, overrides every TTL (0 disables caching)
for test/ops hermeticity.
"""

from __future__ import annotations

import hashlib
import os
import time
from collections import OrderedDict
from typing import Any, Tuple

from pipelines.common.lrucache import remember_latest


def _env_ttl(default_seconds: float) -> float:
    """Per-cache default, unless the single env knob overrides/disables."""
    override = os.getenv("TOROB_ITINERARY_CACHE_TTL_SECONDS")
    try:
        return float(override) if override not in (None, "") else default_seconds
    except ValueError:
        return default_seconds


class TTLCache:
    """Key → (monotonic timestamp, value) with LRU eviction past `limit`.
    get() returns (hit, value); hit is False once `ttl` has elapsed."""

    def __init__(self, name: str, ttl: float, limit: int = 128) -> None:
        self.name = name
        self.ttl = ttl
        self._limit = limit
        self._store: "OrderedDict[Any, Tuple[float, Any]]" = OrderedDict()

    def get(self, key: Any) -> "Tuple[bool, Any]":
        if self.ttl <= 0:  # disabled — caller always takes the slow path
            return False, None
        hit = self._store.get(key)
        if hit is None or time.monotonic() - hit[0] >= self.ttl:
            return False, None
        return True, hit[1]

    def put(self, key: Any, value: Any) -> None:
        if self.ttl <= 0:
            return
        remember_latest(self._store, key, (time.monotonic(), value), self._limit)

    def clear(self) -> None:
        self._store.clear()


places_cache = TTLCache("itinerary.places", _env_ttl(300.0))
embed_cache = TTLCache("itinerary.embed", _env_ttl(900.0))
narrative_cache = TTLCache("itinerary.narrative", _env_ttl(600.0))


def clear_all() -> None:
    """Test/ops hook — every memo empty, next reads take the slow path."""
    for cache in (places_cache, embed_cache, narrative_cache):
        cache.clear()


def vector_key(qvec: "list[float] | None") -> "str | None":
    """Stable digest for a query vector (1536 floats must not be the dict key
    itself — 12 KB per entry; a 16-byte digest serves). None stays None so the
    popularity-ranking path keeps its own key shape."""
    if qvec is None:
        return None
    return hashlib.blake2b(repr(qvec).encode(), digest_size=16).hexdigest()
