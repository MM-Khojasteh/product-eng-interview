"""گام ۱ — retrieval: interests → vectors → ranked places.

Extracted from services.py (zero functional changes). Live path = Postgres
via a worker thread (an async endpoint must never block its loop on a Neon
cold start); demo path = seed.demo_places. Embeddings are an enhancement,
never a hard dependency of the page (the degradation contract in
services.py's docstring).
"""

from __future__ import annotations

import asyncio
import logging

from src.config import OPENAI_API_KEY, TOROB_ITINERARY_EMBEDDING_MODEL
from src.itinerary import db, seed
from src.itinerary.openai_client import _openai, _openai_client
from src.itinerary.seed import CATEGORY_LABELS_FA, PlaceOut
from src.itinerary.ttlcache import embed_cache, places_cache, vector_key

log = logging.getLogger(__name__)


async def embed_interests(text: str) -> list[float] | None:
    """Interests → text-embedding-3-small (1536-d, matching models.Place).
    None (no key / API failure / no module) ⇒ retrieval falls back to
    popularity ranking — vector search is an enhancement, never a hard
    dependency of the page. Successful vectors are memoized (same text → same
    embedding per model); failures are NOT, so a transient API error retries
    on the next request."""
    if _openai is None or not OPENAI_API_KEY or not text.strip():
        return None
    key = text[:500]
    hit, cached = embed_cache.get(key)
    if hit:
        return cached
    try:
        response = await _openai_client().embeddings.create(
            model=TOROB_ITINERARY_EMBEDDING_MODEL,
            input=key,
        )
        vector = list(response.data[0].embedding)
        embed_cache.put(key, vector)
        return vector
    except Exception as exc:  # noqa: BLE001 — see above
        log.warning("interests embedding failed (%s) — popularity ranking",
                    exc.__class__.__name__)
        return None


def _keyword_rank(places: list[PlaceOut], interests: str) -> list[PlaceOut]:
    """Offline interest proxy for the demo catalog (and for live mode when
    the embedding call failed): substring hits over the Persian text, then
    popularity. Deliberately naive — the pgvector path is the real answer;
    this only needs to be deterministic and sane."""
    tokens = [t for t in interests.lower().split() if len(t) > 1]
    if not tokens:
        return sorted(places, key=lambda p: p.popularity, reverse=True)

    def score(p: PlaceOut) -> tuple[int, float]:
        hay = f"{p.name_fa} {p.description_fa} {CATEGORY_LABELS_FA.get(p.category, '')}".lower()
        return sum(hay.count(t) for t in tokens), p.popularity

    return sorted(places, key=score, reverse=True)


async def retrieve_places(city_code: str, interests: str | None = None,
                          limit: int = 12) -> list[PlaceOut]:
    """Stage 1: the ranked city feed. Live path = Postgres via a worker
    thread (an async endpoint must never block its loop on a Neon cold
    start), memoized per (city, limit, query vector) — Place rows change
    weekly, so repeat views skip the PG round trip entirely (identical rows
    out; a fresh list copy per hit so callers can never mutate the cache).
    The demo path is NOT cached: it is pure in-process work whose ranking
    depends on `interests` (keyword path). Either way the result is plain
    PlaceOut records — the rest of the pipeline never knows the source."""
    qvec = await embed_interests(interests) if interests else None
    places: list[PlaceOut] | None = None
    if db.engine is not None:
        cache_key = (city_code, limit, vector_key(qvec))
        hit, cached = places_cache.get(cache_key)
        if hit:
            places = list(cached)
        else:
            places = await asyncio.to_thread(db.fetch_places, city_code, limit, qvec)
            if places is not None:  # None = not armed ⇒ demo; [] = real answer
                places_cache.put(cache_key, places)
    if places is None:  # demo mode (db not armed)
        demo = seed.demo_places(city_code)
        places = _keyword_rank(demo, interests) if interests else \
            sorted(demo, key=lambda p: p.popularity, reverse=True)
        places = places[:limit]
    return places
