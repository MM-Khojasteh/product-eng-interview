"""Live read-model seam — Redis client, quote reads, Jalali/Gregorian + currency
conversion.

Extracted from main.py §"BFF liveness seam" (zero functional changes). The BFF
only ever READS Redis here (one HGETALL per read — the INFRASTRUCTURE §2.6
free-tier command budget); grouping quotes into cards lives in flights.py.

This is the module tests monkeypatch (read_quotes / rds) — the crm-monitoring
convention of patching the collaborator's owning module, never a re-export.
"""

from __future__ import annotations

import logging
import time
from contextvars import ContextVar

from pipelines.common.dto import redis_quote_key

from src.config import REDIS_URL

try:  # Jalali → Gregorian for Redis keys (frontend stays Jalali, dossier §2.3)
    import jdatetime
except ImportError:  # frontend-only dev environment
    jdatetime = None

try:
    import orjson

    def _loads(raw):  # bytes (decode_responses=False) or str — orjson takes both
        return orjson.loads(raw)
except ImportError:  # stdlib fallback — json.loads also accepts bytes AND str
    import json

    def _loads(raw):
        return json.loads(raw)

try:
    import redis.asyncio as aioredis
    from redis.asyncio.retry import Retry
    from redis.backoff import ExponentialBackoff
except ImportError:  # frontend-only dev environment
    aioredis = None

log = logging.getLogger(__name__)

rds = aioredis.from_url(
    REDIS_URL,
    # decode_responses=False: values go straight into _loads — orjson/json
    # parse bytes natively, so skipping the str round-trip halves the decode
    # work on the hottest read in the system (HGETALL of the whole hash).
    decode_responses=False,
    # A SLOW Redis must degrade to cold-cache semantics, never hang the event
    # loop: with the socket defaults (no timeout) a stalled Redis (fsync, swap,
    # network partition) parks every awaiting request at the OS TCP timeout —
    # an exception never arrives, so read_quotes' "treated as cold cache"
    # branch below never engages. One bounded retry absorbs transients.
    socket_timeout=0.5,
    socket_connect_timeout=0.5,
    health_check_interval=30,
    max_connections=64,
    retry=Retry(ExponentialBackoff(cap=0.2, base=0.05), retries=1),
    retry_on_timeout=True,
) if aioredis else None

# Per-request read memo, armed by src.main's middleware. The PDP reads the
# same route-day hash up to three times per page view (resolve_flight →
# offers_for_flight → similar flights) and the Compare modal twice — within
# one request those reads are, by construction, one click-time snapshot, so
# the first read's result is reused for the rest of THAT request (and only
# that request — a fresh dict per request, tests/scripts see None and skip
# the memo entirely, keeping direct-call semantics unchanged).
_request_quotes: "ContextVar[dict[str, list[dict]] | None]" = ContextVar(
    "torob_request_quotes", default=None
)


def toman_from_rial(rial: int | None) -> int | None:
    """Canonical DTO/Redis stores Rial (dto.py: price_total_rial); the UI domain
    is Toman. The ONLY place the ÷10 happens — templates stay Toman-only."""
    return None if rial is None else rial // 10


def jalali_to_greg_iso(datenum: str | None) -> str | None:
    """'14050713' → '2026-10-05'. Redis keys carry the Gregorian search date
    (liveness.route_day_key); the frontend stays Jalali (dossier §2.3)."""
    if jdatetime is None or not (datenum and datenum.isdigit() and len(datenum) == 8):
        return None
    try:
        return jdatetime.date(
            int(datenum[:4]), int(datenum[4:6]), int(datenum[6:])
        ).togregorian().isoformat()
    except ValueError:
        return None


def quote_key(origin: str, destination: str, datenum: str) -> str | None:
    """Mirror of liveness.route_day_key — quote:{o}-{d}:{gregorian search date}
    (the format itself is owned by dto.redis_quote_key, one definition)."""
    greg = jalali_to_greg_iso(datenum)
    return redis_quote_key(origin, destination, greg) if greg else None


def _rd(origin: str, destination: str, datenum: str) -> str:
    """Route-day tag for logs — one grep-able prefix across the whole BFF trace."""
    return f"[{origin}→{destination} {jalali_to_greg_iso(datenum) or datenum}]"


route_day_tag = _rd  # public seam (routers / src/pulse.py); the underscore name
                     # stays for the root main.py re-export shim


_redis_checked = False  # first successful read logs once (connection proof + RTT)


async def read_quotes(origin: str, destination: str, datenum: str) -> list[dict]:
    """HGETALL the route-day hash → live quotes ([] when cold/unreachable).
    One command per request — the INFRASTRUCTURE §2.6 free-tier discipline
    (per-request memo above collapses the PDP's triple read into this one).
    Redis being DOWN vs an EMPTY cache are different failure modes — log them
    differently so the operator never confuses the two."""
    global _redis_checked
    key = quote_key(origin, destination, datenum)
    if rds is None or key is None:
        return []
    memo = _request_quotes.get()
    if memo is not None and key in memo:
        return memo[key]
    try:
        started = time.perf_counter()
        raw = await rds.hgetall(key)
        elapsed_ms = (time.perf_counter() - started) * 1000
    except Exception as exc:  # RedisError or transport — cold-cache semantics either way
        log.warning("%s redis read FAILED (%s: %s) — treated as cold cache. "
                    "Is `docker compose up -d` (redis) running?",
                    _rd(origin, destination, datenum), type(exc).__name__, exc,
                    exc_info=True)
        return []  # NOT memoized — a later read in the same request may retry
    if not _redis_checked:
        _redis_checked = True
        log.info("%s redis connected — first HGETALL round-trip %.1f ms",
                 _rd(origin, destination, datenum), elapsed_ms)
    quotes = []
    dropped = 0
    for value in raw.values():
        try:
            quotes.append(_loads(value))
        except ValueError:
            dropped += 1
            continue
    if memo is not None and len(memo) < 8:  # a request touches ≤2 keys; guard anyway
        memo[key] = quotes
    log.debug("%s HGETALL → %d quote(s) in %.1f ms (%d unparseable member(s) dropped)",
              _rd(origin, destination, datenum), len(quotes), elapsed_ms, dropped)
    return quotes
