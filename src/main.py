"""
Torob Travel — «سفر و بلیت» (flight meta-search frontend MVP).

Stack: FastAPI + Jinja2 + HTMX + Tailwind (Play CDN) — no SPA framework.

Page model mirrors torob.com's own journey (torob-design-capture/design-system.json
→ touchpoints/pageFamilies):

  Home `/`                    textSearch   — search-first, minimal
  Results /search/flights*    priceComparison — filter rail + dense cards (PGP)
  Flight `/flights/{id}`      productEvaluation — seller offers, price history,
                                              specifications, similar flights (PDP)
  Handoff                     «خرید» deeplink to the agency (CPC)

HTMX flow for the results surface (HTML fragments, no JSON):
  1. Search form  ── hx-get /search/flights         → skeleton loader
  2. Skeleton     ── hx-get /search/flights/results → browse shell (self-swaps).
     While the Redis hash is cold it keeps re-fetching every 2 s — the RESPONSE
     decides whether to keep polling (server-driven polling) — and each miss
     publishes one on-demand scan request to Kafka's search.requests topic
     (consumed by raw_worker's OnDemandScans → same politeness budget as cadence).
  3. Live ticker  ── hx-get /search/flights/live    → hx-swap-oob pulse: price
     boxes refresh, FULL CARDS stream in for flights that landed in later
     Alibaba chunks (beforeend insert into #flight-cards, filter-aware), and
     tombstoned cards vanish in place — the shell never re-renders. Polling
     self-extinguishes when the scan completes (2 agreeing reads).
  4. Compare      ── hx-get /flights/{ref}/offers   → ds-modal fed by a
     click-time Redis read — «ظرفیت تکمیل شد» when the fare was tombstoned
     between list render and click. A COLD (TTL-expired) hash is NOT a
     sellout: the modal re-arms itself (bounded polling) while search.request
     refills the hash — expired ≠ tombstoned.

Read model (live mode): the route-day hash written by pipelines/liveness.py
(quote:{origin}-{destination}:{gregorian-date}, TTL 60–120 s, members purged by
tombstones) is grouped into ONE card per physical_flight_key (dto.py §3 dedup);
Rial → Toman happens exactly once at this seam (toman_from_rial). /flights/{ref}
resolves statelessly against that same hash (resolve_flight) — no in-process
index, so it holds across uvicorn workers. Live mode arms when jdatetime+redis
are importable and TOROB_FORCE_MOCK is unset; otherwise the deterministic mock
catalog serves everything — generate_flights()/_mock_offer_rows()/price_history()
are the only mock seams left to delete at cutover, and NONE of them render in
live mode.

App assembly (this module): create the app, mount statics, register the three
routers explicitly (no auto-discovery — the crm-monitoring src/main.py pattern),
emit the one bootstrap trace line. Everything else lives in the sibling modules;
the root main.py is a thin entry shim re-exporting `app`.

Run:  uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from src import config  # noqa: F401  — imported FIRST: it configures root
from src import app_state, readmodel, scanbus  # logging before any import-time log
from src.guard import guarded
from src.itinerary import db as itinerary_db
from src.itinerary import router as itinerary_router
from src.routers import flight, pages, search
from src.view_templates import BASE_DIR

log = logging.getLogger(__name__)


class CachedStaticFiles(StaticFiles):
    """crm-monitoring pattern: statics are immutable and cache forever — safe
    because every asset URL carries a busting ?v= (view_templates.asset_url:
    import-time content hash + per-file mtime suffix), so a changed asset is
    a changed URL even without a process restart; browsers never need to
    revalidate the old one."""

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """crm-monitoring main.py pattern: teardown closes every client this
    process armed, in order (read paths first, the producer — the write
    path — last so in-flight scans get their delivery window), each inside a
    guard: a failing close must never mask the next. Multi-worker uvicorn:
    each worker closes its own clients."""
    yield
    with guarded("scan-trigger drain", log):
        # in-flight fire-and-forget triggers get their produce window BEFORE
        # the producer flush closes (the write path closes last)
        await scanbus.drain_scan_triggers(timeout=2.0)
    with guarded("itinerary engine shutdown", log):
        if itinerary_db.engine is not None:
            await asyncio.to_thread(itinerary_db.engine.dispose)
    with guarded("redis shutdown", log):
        if readmodel.rds is not None:
            await readmodel.rds.aclose()
    with guarded("kafka producer shutdown", log):
        if scanbus.producer is not None:
            await asyncio.to_thread(scanbus.producer.flush, 10.0)


app = FastAPI(title="Torob Travel — سفر و بلیت", lifespan=lifespan)
app.mount("/static", CachedStaticFiles(directory=BASE_DIR / "static"), name="static")


@app.middleware("http")
async def _request_scoped_caches(request: Request, call_next):
    """Arm readmodel's per-request quote memo: the PDP/modal read the same
    route-day hash up to 3×/2× per request — the memo collapses that to ONE
    click-time HGETALL (the request IS the click). Fresh dict per request;
    readmodel skips the memo entirely outside a request (tests/scripts)."""
    token = readmodel._request_quotes.set({})
    try:
        return await call_next(request)
    finally:
        readmodel._request_quotes.reset(token)


@app.middleware("http")
async def _revalidate_html(request: Request, call_next):
    """crm-monitoring pattern: HTML must revalidate on every load so deploys
    are picked up without a hard refresh (a fresh page references fresh ?v=
    asset URLs); statics are exempt — see CachedStaticFiles."""
    response = await call_next(request)
    if response.headers.get("content-type", "").startswith("text/html"):
        response.headers["Cache-Control"] = "no-cache"
    return response

# Router registration — explicit and greppable, no auto-discovery magic.
app.include_router(pages.router)
app.include_router(search.router)
app.include_router(flight.router)
app.include_router(itinerary_router.router)  # /itinerary/{city_code} — AI planner

log.info(
    "bootstrap: mode=%s · redis=%s · kafka=%s · scan_topic=%s",
    "LIVE (cache-first, Redis read model)" if app_state._LIVE_READY
    else "MOCK (TOROB_FORCE_MOCK or deps missing)",
    config.REDIS_URL if readmodel.rds is not None else "OFF — pip install redis jdatetime",
    "producer armed" if scanbus.producer is not None
    else "OFF — pip install confluent-kafka (on-demand scan triggers disabled)",
    scanbus.SEARCH_TOPIC,
)
