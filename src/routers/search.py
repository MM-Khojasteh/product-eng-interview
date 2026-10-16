"""The results surface — /search/flights + /results + /live (skeleton → shell →
OOB pulse; the HTMX flow documented in src.main's module docstring).

The liveness state machine (HIT/MISS/GIVEUP) and the OOB pulse diff/streak
rule live in src/pulse.py (extracted, zero functional changes); this module
keeps the HTTP layer — signature parsing, response selection, and the
deliberately DISTINCT per-handler error copy (see _search_inputs).
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src import app_state, flights, mock_catalog, parsing, pulse, readmodel, scanbus
from src.constants import AIRLINES, AIRPORTS, MAX_POLL_ATTEMPTS
from src.filters import (
    Filters,
    apply_filters,
    flight_results_context,
    index_context,
    parse_filters,
)
from src.formatting import date_fa
from src.view_templates import templates

log = logging.getLogger(__name__)

router = APIRouter()


def _search_inputs(origin: str, destination: str, date: str, passengers: int):
    """Shared preamble of the three /search/flights* handlers: airport codes,
    datenum, passenger clamp (1–9), and the canonical query dict. Handlers
    keep their own DISTINCT error responses — only the parsing is shared."""
    origin_code = parsing.parse_airport(origin)
    destination_code = parsing.parse_airport(destination)
    datenum = parsing.parse_date(date)
    passengers = max(1, min(passengers, 9))
    base = {
        "origin": origin_code,
        "destination": destination_code,
        "date": datenum,
        "passengers": passengers,
    }
    return origin_code, destination_code, datenum, passengers, base


@router.get("/search/flights", response_class=HTMLResponse)
async def search_flights(
    request: Request,
    origin: str = "",
    destination: str = "",
    date: str = "",
    passengers: int = 1,
):
    origin_code, destination_code, datenum, passengers, params = _search_inputs(
        origin, destination, date, passengers
    )

    def fail(message: str):
        if request.headers.get("HX-Request"):
            return templates.TemplateResponse(
                request, "flights/partials/search_error.html", {"message": message}
            )
        return templates.TemplateResponse(
            request, "flights/index.html", index_context(error_message=message)
        )

    if origin_code is None or destination_code is None:
        return fail("فرودگاه مبدأ یا مقصد معتبر نیست؛ از فهرست پیشنهادی انتخاب کنید.")
    if origin_code == destination_code:
        return fail("مبدأ و مقصد نمی‌توانند یکسان باشند.")
    if datenum is None:
        return fail("تاریخ رفت را به شکل ۱۴۰۵/۰۷/۱۳ وارد کنید.")

    if request.headers.get("HX-Request"):
        # Step 1 — paint the skeleton instantly; it self-fetches the browse shell
        # (and keeps re-fetching every 2 s while the Redis hash is cold).
        log.info("%s search accepted (passengers=%d) → skeleton served, poll loop armed "
                 "(≤%d attempts ≈ %ds)", readmodel.route_day_tag(origin_code, destination_code, datenum),
                 passengers, MAX_POLL_ATTEMPTS, MAX_POLL_ATTEMPTS * 2)
        return templates.TemplateResponse(
            request,
            "flights/partials/flights_skeleton.html",
            {"qs": urlencode(params), "attempt": 1, "max_attempts": MAX_POLL_ATTEMPTS},
        )

    # Direct visit / refresh / no-JS: server-render the page so hx-push-url'd
    # URLs stay shareable and work without JavaScript. Warm cache (or mock mode)
    # inlines the results; a cold LIVE cache inlines the self-polling skeleton —
    # with no data there is honestly nothing else to show.
    all_flights = await flights.flights_for(origin_code, destination_code, datenum)
    if all_flights is not None:
        ctx = flight_results_context(
            all_flights, origin_code, destination_code, datenum, passengers, "price", Filters()
        )
        results_html = templates.env.get_template("flights/partials/flight_results.html").render(**ctx)
    else:
        results_html = templates.env.get_template("flights/partials/flights_skeleton.html").render(
            qs=urlencode(params), attempt=1, max_attempts=MAX_POLL_ATTEMPTS
        )
    return templates.TemplateResponse(
        request,
        "flights/index.html",
        index_context(
            form_origin=f"{AIRPORTS[origin_code]} ({origin_code})",
            form_destination=f"{AIRPORTS[destination_code]} ({destination_code})",
            form_date=date_fa(datenum),
            form_passengers=passengers,
            results_html=results_html,
        ),
    )


@router.get("/search/flights/results", response_class=HTMLResponse)
async def search_flights_results(
    request: Request,
    origin: str = "",
    destination: str = "",
    date: str = "",
    passengers: int = 1,
    sort: str = "price",
    airline: list[str] | None = Query(None),
    time: list[str] | None = Query(None),
    type: list[str] | None = Query(None),
    min_price: str = "",
    max_price: str = "",
    attempt: int = 1,
):
    # ── Liveness state machine (notes - Ticket/Liveness.md cache-miss scenario) ──
    #   HIT   → render the browse shell (with the OOB live ticker armed)
    #   MISS  → pulse.miss_response: publish one on-demand scan (search.requests)
    #           and re-emit the skeleton — it re-fetches itself after 2 s.
    #           Server-driven polling: the RESPONSE decides whether to keep
    #           polling.
    #   GIVEUP→ honest "collection timed out" error, distinct from "no flights
    #           on this route" (the empty-state inside the shell).
    origin_code, destination_code, datenum, passengers, base = _search_inputs(
        origin, destination, date, passengers
    )
    if not all((origin_code, destination_code, datenum)) or origin_code == destination_code:
        return templates.TemplateResponse(
            request,
            "flights/partials/search_error.html",
            {"message": "نتایج جستجو منقضی شده‌اند؛ لطفاً دوباره جستجو کنید."},
        )

    quotes = await readmodel.read_quotes(origin_code, destination_code, datenum)
    if not quotes and app_state._LIVE_READY:
        return await pulse.miss_response(
            request, base, attempt, origin_code, destination_code, datenum
        )

    all_flights = (
        flights.group_quotes(quotes, origin_code, destination_code, datenum)
        if quotes
        else mock_catalog.generate_flights(origin_code, destination_code, datenum)  # MOCK SEAM (delete at cutover)
    )
    sort = sort if sort in ("price", "time") else "price"
    filters = parse_filters(airline, time, type, parsing.opt_int(min_price), parsing.opt_int(max_price))
    ctx = flight_results_context(
        all_flights, origin_code, destination_code, datenum,
        max(1, min(passengers, 9)), sort, filters,
    )

    if quotes:
        # Baseline the OOB pulse against exactly what THIS render paints
        # (pulse.baseline) — the duplicate-card guard.
        pulse.baseline(readmodel.quote_key(origin_code, destination_code, datenum), quotes)
        log.info("%s cache HIT: %d quotes → %d cards (sort=%s, filters=%s) — "
                 "pulse baselined, ticker armed",
                 readmodel.route_day_tag(origin_code, destination_code, datenum), len(quotes),
                 len(ctx["flights"]), sort,
                 "on" if filters.is_active else "off")
    else:
        log.info("%s mock catalog: %d cards (live stack not armed)",
                 readmodel.route_day_tag(origin_code, destination_code, datenum), len(ctx["flights"]))

    return templates.TemplateResponse(request, "flights/partials/flight_results.html", ctx)


@router.get("/search/flights/live", response_class=HTMLResponse)
async def search_flights_live(
    request: Request,
    origin: str = "",
    destination: str = "",
    date: str = "",
    passengers: int = 1,
    sort: str = "price",
    airline: list[str] | None = Query(None),
    time: list[str] | None = Query(None),
    type: list[str] | None = Query(None),
    min_price: str = "",
    max_price: str = "",
):
    """OOB liveness pulse — hx-swap-oob fragments only: refreshed price boxes,
    FULL CARDS for flights that streamed in after the shell was painted (late
    Alibaba chunks insert beforeend into #flight-cards, filter-aware), deletes
    for tombstoned cards, and the next ticker (absent ⇒ polling stops).
    Alibaba's chunked scans and FlyToday's one-shot blob therefore BOTH appear
    as live in-place updates. Cost: one HGETALL per tick (§2.6 command budget);
    polling self-extinguishes after two consecutive agreeing reads (one
    agreement can fall in a quiet gap between suppliers)."""
    origin_code, destination_code, datenum, passengers, base = _search_inputs(
        origin, destination, date, passengers
    )
    if not all((origin_code, destination_code, datenum)) or origin_code == destination_code:
        return HTMLResponse("")

    quotes = await readmodel.read_quotes(origin_code, destination_code, datenum)

    if not quotes:
        scanbus.trigger_scan_later(origin_code, destination_code, datenum)
        if not app_state._LIVE_READY:
            # MOCK SEAM (delete at cutover; the live branch is below): cold
            # cache → keep the pipeline moving, and demo the OOB mechanics with
            # mock drift (mock_catalog.pulse_drift).
            return templates.TemplateResponse(
                request,
                "flights/partials/live_pulse.html",
                mock_catalog.pulse_drift(origin_code, destination_code, datenum, base),
            )
        # LIVE + cold: ticker stopped cleanly (pulse.cold_stop_context).
        log.info("%s pulse: hash COLD (TTL expired) — ticker stopped cleanly, "
                 "cards keep last-known prices",
                 readmodel.route_day_tag(origin_code, destination_code, datenum))
        return templates.TemplateResponse(
            request, "flights/partials/live_pulse.html", pulse.cold_stop_context(base)
        )

    flights_now = flights.group_quotes(quotes, origin_code, destination_code, datenum)
    key = readmodel.quote_key(origin_code, destination_code, datenum)
    prev_ids, ids_now, streak = pulse.tick(key, quotes)

    filters = parse_filters(airline, time, type, parsing.opt_int(min_price), parsing.opt_int(max_price))
    # Late chunks: flights absent from the previous tick (and from the DOM) get
    # a FULL card insert; filtered-out ones stay hidden until a shell re-render.
    added = apply_filters(
        [f for f in flights_now if f.id in (ids_now - prev_ids)], filters
    )

    # Per-tick trace is DEBUG (one line per client per 2 s — INFO here is a
    # log firehose at ticker concurrency); the completion transition stays
    # INFO because it is the LAST tick for this route-day.
    tick_log = log.info if streak >= 2 else log.debug
    tick_log("%s pulse tick: %d quotes → %d cards · added=%d removed=%d "
             "streak=%d complete=%s",
             readmodel.route_day_tag(origin_code, destination_code, datenum), len(quotes),
             len(flights_now), len(added), len(prev_ids - ids_now),
             streak, streak >= 2)
    return templates.TemplateResponse(
        request,
        "flights/partials/live_pulse.html",
        {
            "changed": flights_now,
            "added": added,
            "removed": sorted(prev_ids - ids_now),  # tombstoned between ticks
            "complete": streak >= 2,
            "live_qs": urlencode(filters.as_params(sort=sort, **base)),
            "airlines": AIRLINES,  # flight_card.html (full-card inserts)
        },
    )
