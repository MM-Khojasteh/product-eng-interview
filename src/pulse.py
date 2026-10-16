"""BFF-side liveness/pulse state machine — extracted from routers/search.py
(refactor §3.3, zero functional changes): the MISS/GIVEUP arm of the results
liveness machine and the OOB ticker's 2-agreement diff/streak rule. The HTTP
layer keeps signature parsing, response selection and the deliberately
per-handler error copy; everything that DECIDES liveness lives here.

Not named liveness.py on purpose — pipelines/liveness.py is the WRITER
(canonical topic → Redis route-day hashes); this is the reader-side machine.
The shared cross-request pulse state stays in app_state (one worker, the
2-agreement rule documented there).
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlencode

from fastapi import Request

from src import app_state, readmodel, scanbus
from src.constants import AIRLINES, MAX_POLL_ATTEMPTS
from src.view_templates import templates

log = logging.getLogger(__name__)


async def trigger_scan(origin: str, destination: str, datenum: str) -> None:
    """Cache-miss → publish one on-demand scan request, off the event loop
    (scanbus.request_scan blocks in librdkafka's flush)."""
    await asyncio.to_thread(scanbus.request_scan, origin, destination, datenum)


async def miss_response(
    request: Request,
    base: dict,
    attempt: int,
    origin_code: str,
    destination_code: str,
    datenum: str,
):
    """The MISS/GIVEUP arm of the liveness state machine (notes - Ticket/Liveness.md
    cache-miss scenario): MISS → publish one on-demand scan (search.requests)
    and re-emit the self-polling skeleton; GIVEUP → the honest timeout error,
    distinct from "no flights on this route" (the empty-state inside the
    shell). Server-driven polling: the RESPONSE decides whether to keep
    polling."""
    tag = readmodel.route_day_tag(origin_code, destination_code, datenum)
    if attempt < MAX_POLL_ATTEMPTS:
        # Fire-and-forget: the skeleton (this response) must not wait on the
        # broker — the NEXT poll, not this response, consumes the scan.
        scanbus.trigger_scan_later(origin_code, destination_code, datenum)
        log.info("%s cache MISS — skeleton re-armed (attempt %d/%d, ~%ds elapsed)",
                 tag, attempt, MAX_POLL_ATTEMPTS, attempt * 2)
        return templates.TemplateResponse(
            request,
            "flights/partials/flights_skeleton.html",
            {
                "qs": urlencode({**base, "attempt": attempt + 1}),
                "attempt": attempt,
                "max_attempts": MAX_POLL_ATTEMPTS,
            },
        )
    log.warning(
        "%s GAVE UP after %d attempts (~%ds): nothing filled the Redis hash. "
        "Check → (1) data plane: docker compose up -d  (2) pipeline overlay: "
        "docker compose -f docker-compose.yml -f docker-compose.pipeline.yml up -d "
        "with ROUTES/DEPARTURE_DATE matching this search  (3) BFF kafka: pip install "
        "confluent-kafka (bootstrap log said 'kafka=OFF'?)  (4) or demo it: "
        "python scripts/seed_redis_demo.py %s %s %s",
        tag, attempt, attempt * 2, origin_code, destination_code, datenum,
    )
    return templates.TemplateResponse(
        request,
        "flights/partials/search_error.html",
        {"message": "دریافت قیمت‌های زنده این مسیر بیش از حد طول کشید؛ لطفاً چند لحظه بعد دوباره جستجو کنید."},
    )


def baseline(pulse_key: str | None, quotes: list[dict]) -> None:
    """Baseline the OOB pulse against exactly what THIS render paints, so the
    first /live tick diffs adds/removes against the DOM — not against a stale
    set left by a previous visit (the duplicate-card guard). signature None ⇒
    the first tick can never declare the scan complete."""
    if not pulse_key:
        return
    app_state.remember_pulse(
        pulse_key,
        frozenset(q["physical_flight_key"] for q in quotes if q.get("physical_flight_key")),
        None,
        0,
    )


def tick(pulse_key: str | None, quotes: list[dict]) -> tuple[frozenset, frozenset, int]:
    """One /live tick against the cross-request state: returns (prev_ids,
    ids_now, streak) — the diff drives the OOB adds/removes, and the streak of
    agreeing signatures (count + max captured_at) drives completion (2 agreeing
    reads; one agreement can fall in a quiet gap between suppliers)."""
    ids_now = frozenset(q["physical_flight_key"] for q in quotes if q.get("physical_flight_key"))
    signature = (len(quotes), max((q.get("captured_at") or "") for q in quotes))
    prev_ids, prev_signature, prev_streak = app_state._pulse_state.get(
        pulse_key, (frozenset(), None, 0))
    streak = (
        prev_streak + 1
        if (prev_signature is not None and prev_signature == signature)
        else 0
    )
    if pulse_key:
        app_state.remember_pulse(pulse_key, ids_now, signature, streak)
    return prev_ids, ids_now, streak


def cold_stop_context(base: dict) -> dict:
    """LIVE + cold: the route-day hash expired (TTL) — live updates are over.
    Cards keep their last-known prices; clicks revalidate via /offers, which
    shows the refresh state (an expired cache is not a sellout). Stop polling
    cleanly instead of emitting content with no targets."""
    return {"changed": [], "added": [], "removed": [], "complete": True,
            "live_qs": urlencode(base), "airlines": AIRLINES}
