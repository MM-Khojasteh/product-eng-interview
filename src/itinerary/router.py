"""The itinerary surface — GET /itinerary/{city_code} (HTML, HTMX-friendly).

One request runs the whole pipeline (services) and renders BOTH views of the
SAME plan: the Persian narrative + stop timeline (server-side Jinja, left
column) and the Leaflet feed (a JSON payload embedded in a script tag, right
column). Because both derive from one `routed` list, the map and the
timeline can never disagree — the map's polylines ARE the timeline's order.

Registry-not-table: the MVP is Kish only (notes §جمع‌بندی); a `cities` table
arrives with the second city — until then a module constant IS the registry
(the CONTENT_PAGES pattern in routers/pages.py).

`days` is bounded (1–5, DAY_COLORS length) and `interests` capped at 200
chars: both feed either an embedding call or a K-Means run, so the page
cannot be turned into a compute hose.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from src.itinerary import db, services
from src.itinerary.seed import CATEGORY_LABELS_FA, DAY_ORDINALS_FA
from src.view_templates import templates

log = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class City:
    code: str
    name_fa: str
    tagline_fa: str
    center: tuple[float, float]  # routing origin + initial map center
    zoom: int
    default_days: int


CITIES: dict[str, City] = {
    "kish": City(
        code="kish",
        name_fa="کیش",
        tagline_fa="جزیرهٔ مرجانی خلیج فارس",
        center=(26.5350, 53.9800),
        zoom=12,
        default_days=3,
    ),
}


@router.get("/itinerary", response_class=HTMLResponse)
async def itinerary_landing(request: Request):
    """The feature's front door: what it is, how it works, FAQ — the header
    touchpoint. The Kish handoff strip (same copy as the homepage) CTA-jumps
    straight to the planner page, which remains the destination of every
    contextual touchpoint."""
    return templates.TemplateResponse(request, "itinerary/landing.html", {
        "meta": {
            "title": "برنامهٔ سفر با هوش مصنوعی | ترب بلیت",
            "description": "بعد از خرید بلیت، یک برنامهٔ سفر چند روزهٔ قابل‌پیاده‌شدن با نقشهٔ تعاملی، تایم‌لاین فارسی و روایت هوش مصنوعی — فعلاً کیش، به‌زودی شهرهای بیشتر.",
        },
        "cities": sorted(CITIES.values(), key=lambda c: c.name_fa),
    })


@router.get("/itinerary/{city_code}", response_class=HTMLResponse)
async def itinerary(
    request: Request,
    city_code: str,
    days: int = Query(3, ge=1, le=5),
    interests: str | None = Query(None, max_length=200),
):
    """The planner page: retrieval → clustering → routing → narration."""
    city = CITIES.get(city_code.strip().lower())
    if city is None:
        raise HTTPException(status_code=404)

    # Pipeline گام ۱→۳ — routed is the single source of truth from here on.
    places = await services.retrieve_places(city.code, interests, limit=days * 4)
    # Trust layer (enrichment.py): attach build-time photos + sourced Wikipedia
    # summaries by slug, BEFORE clustering — the symbolic stages read only
    # lat/lng/category, so media never perturbs the plan, and both data sources
    # (demo seed, PG rows) enrich identically.
    places = services.enrich_places(places, city.code)
    clusters = services.cluster_places(places, days)
    routed = [services.route_day(cluster, origin=city.center) for cluster in clusters]
    narrative = await services.generate_narrative(routed, city_name_fa=city.name_fa)

    # Timeline view-model: each routed day paired with its narrative section.
    days_view = []
    for day_no, day in enumerate(routed, start=1):
        days_view.append({
            "no": day_no,
            "color": services.DAY_COLORS[(day_no - 1) % len(services.DAY_COLORS)],
            "section": narrative.sections[day_no - 1]
                       if day_no <= len(narrative.sections) else None,
            "places": day,
        })

    log.info(
        "itinerary %s → %s · %d/%d day(s) · %d place(s) · narrative=%s",
        city.code, "pg" if db.armed() else "demo", len(routed), days,
        len(places), narrative.source,
    )
    return templates.TemplateResponse(
        request,
        "itinerary/planner.html",
        {
            "city": city,
            "requested_days": days,
            "days_count": len(days_view),
            "interests": interests,
            "days_view": days_view,
            "map_payload": services.build_map_payload(routed, center=city.center, zoom=city.zoom),
            "narrative_source": narrative.source,
            "place_count": len(places),
            "category_labels": CATEGORY_LABELS_FA,
            "day_ordinals": DAY_ORDINALS_FA,
        },
    )
