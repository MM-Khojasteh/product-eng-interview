"""The flight surfaces — PDP (/flights/{ref}) + Compare modal (/flights/{ref}/offers).

Extracted from main.py §"Routes" 6–7 (zero functional changes). Both are
click-time Redis reads through flights.py; the tri-state discipline
(live offers / sold-out / refresh-pending) is documented in offers_for_flight().
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src import app_state, flights, mock_catalog, readmodel
from src.constants import AIRLINES, AIRPORTS, MAX_OFFER_POLL_ATTEMPTS
from src.view_templates import templates

log = logging.getLogger(__name__)

router = APIRouter()


def _nav_urls(flight) -> tuple[str, str]:
    """(results_url, back_url) for a flight's route-day — the PDP's breadcrumbs
    and CTAs and the modal's back link all navigate the same search."""
    base = {"origin": flight.origin, "destination": flight.destination,
            "date": flight.datenum, "passengers": 1}
    return "/search/flights?" + urlencode(base), "/search/flights/results?" + urlencode(base)


@router.get("/flights/{flight_id}", response_class=HTMLResponse)
async def flight_page(request: Request, flight_id: str):
    """PDP — the productEvaluation surface: seller offers + specs + similar
    flights. resolve_flight() rebuilds live cards from the route-day hash
    itself (mock catalog in mock mode). Offers are a click-time Redis read: a
    tombstoned fare is simply absent from the hash — that's the sold-out state,
    not a dead deeplink. Price history stays mock-only (PG hourly rollups are
    the roadmap source) — it never renders in live mode; same for mock
    similar-flights."""
    flight, _cold = await flights.resolve_flight(flight_id)
    if flight is None:
        log.info("PDP %s → flight not found (cold or tombstoned)", flight_id)
        return templates.TemplateResponse(
            request,
            "flights/detail.html",
            {
                "flight": None, "agencies": [], "best": None, "live_count": 0,
                "refresh_pending": False,
                "history": [], "similar": [], "airlines": AIRLINES,
            },
        )

    # Tri-state via the shared view helper (cold hash ≠ tombstoned — the same
    # discipline the Compare modal applies).
    agencies, best, refresh_pending, live_count = flights.offer_view(
        await flights.offers_for_flight(flight))
    if app_state._LIVE_READY:  # same read model as the cards — never the mock catalog
        quotes = await readmodel.read_quotes(flight.origin, flight.destination, flight.datenum)
        similar = [f for f in flights.group_quotes(
            quotes, flight.origin, flight.destination, flight.datenum
        ) if f.id != flight.id]
        history: list[dict] = []  # MOCK SEAM: price_history() until PG rollups land
    else:
        similar = [f for f in mock_catalog.generate_flights(  # MOCK SEAM (delete at cutover)
            flight.origin, flight.destination, flight.datenum
        ) if f.id != flight.id]
        history = mock_catalog.price_history(flight)  # MOCK SEAM (delete at cutover)
    similar.sort(key=lambda f: f.min_price)
    results_url, back_url = _nav_urls(flight)
    return templates.TemplateResponse(
        request,
        "flights/detail.html",
        {
            "flight": flight,
            "agencies": agencies,
            "best": best,
            "live_count": live_count,
            "chip_counts": flights.chip_counts(agencies),
            "refresh_pending": refresh_pending,
            "history": history,
            "similar": similar[:4],
            "airlines": AIRLINES,
            "origin_name": AIRPORTS[flight.origin],
            "destination_name": AIRPORTS[flight.destination],
            "results_url": results_url,
            "back_url": back_url,
        },
    )


@router.get("/flights/{flight_id}/offers", response_class=HTMLResponse)
async def flight_offers(request: Request, flight_id: str, attempt: int = 1):
    """HTMX partial — the Compare modal body, ALWAYS a click-time Redis read so
    a sellout between list render and click renders «ظرفیت تکمیل شد» instead of
    a dead deeplink (the worst trust-breaker per notes - Ticket/Liveness.md). A COLD
    (TTL-expired) hash is a third, distinct state: the modal re-arms itself
    (bounded, server-driven polling — the skeleton idiom) while search.request
    refills the hash, then falls back to an honest retry CTA."""
    flight, rows, cold = await flights.live_offers(flight_id)
    # Tri-state: cold hash (or a resolved flight whose re-read went cold) ⇒
    # refresh; resolvable-but-tombstoned ⇒ sold-out; else live offers.
    offers, best, rows_refresh, _live_count = flights.offer_view(rows)
    refresh_pending = cold or (flight is not None and rows_refresh)
    log.info("modal %s → %s (attempt %d/%d, %d offer(s))",
             flight_id,
             "REFRESH (cold hash, re-polling)" if refresh_pending and attempt <= MAX_OFFER_POLL_ATTEMPTS
             else "RETRY-CTA (refresh budget exhausted)" if refresh_pending
             else "SOLD-OUT (warm tombstone)" if best is None
             else "LIVE OFFERS",
             attempt, MAX_OFFER_POLL_ATTEMPTS, len(offers))
    back = None
    if flight is not None:
        _results_url, back = _nav_urls(flight)
    return templates.TemplateResponse(
        request,
        "flights/partials/offers_modal.html",
        {
            "flight": flight,
            "flight_ref": flight.ref if flight else flight_id,
            "offers": offers,
            "offer_agencies": len({row["agency"].key for row in offers}),
            "chip_counts": flights.chip_counts(offers),
            "best": best,
            "refresh_pending": refresh_pending,
            "attempt": attempt,
            "max_attempts": MAX_OFFER_POLL_ATTEMPTS,
            "back_url": back,
            "origin_name": AIRPORTS.get(flight.origin, "") if flight else "",
            "destination_name": AIRPORTS.get(flight.destination, "") if flight else "",
            "airlines": AIRLINES,
        },
    )
