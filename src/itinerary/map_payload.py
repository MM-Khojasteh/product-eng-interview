"""View payload — one dict feeds the narrative timeline + the Leaflet map.

Extracted from services.py (zero functional changes).
"""

from __future__ import annotations

from collections.abc import Sequence

from src.itinerary.seed import CATEGORY_LABELS_FA, PlaceOut

# Day route colours — map polylines, stop dots and legend. Brand red leads;
# the rest stay distinguishable over OSM tiles in both page themes.
DAY_COLORS = ("#e91e33", "#0284c7", "#059669", "#d97706", "#7c3aed")


def build_map_payload(routed_plan: Sequence[Sequence[PlaceOut]], *,
                      center: tuple[float, float], zoom: int) -> dict:
    """The Leaflet feed. Embedded verbatim via Jinja |tojson into
    <script type="application/json" id="itinerary-data"> — HTML-escaped by
    the filter, so names/URLs can't break out of the tag. Categories arrive
    pre-labeled in Persian; colors come from DAY_COLORS so the timeline dots
    and the map polylines can never disagree."""
    days = []
    for day_no, day in enumerate(routed_plan, 1):
        days.append({
            "no": day_no,
            "color": DAY_COLORS[(day_no - 1) % len(DAY_COLORS)],
            "path": [[round(p.lat, 6), round(p.lng, 6)] for p in day],
            "places": [
                {
                    "slug": p.slug,
                    "name": p.name_fa,
                    "category": CATEGORY_LABELS_FA.get(p.category, p.category),
                    "lat": round(p.lat, 6),
                    "lng": round(p.lng, 6),
                    "stop": stop_no,
                    "visit_minutes": p.est_visit_minutes,
                    "has_ticket": p.has_ticket,
                    "redirect_url": p.redirect_url or "",
                    "lowest_price": p.lowest_price,
                }
                for stop_no, p in enumerate(day, 1)
            ],
        })
    return {"center": [center[0], center[1]], "zoom": zoom, "days": days}
