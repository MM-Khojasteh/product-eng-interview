"""View-model layer — PGP filter state + the Jinja context builders.

Extracted from main.py §"Shared contexts" (zero functional changes).
flight_results_context() is the single place card-surface template keys are
produced; _index_context() the same for the home shell — the template contract
lives here, not in the routes.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from urllib.parse import urlencode

from src.constants import AIRLINES, AIRPORTS, TIME_BUCKETS
from src.formatting import date_fa, toman
from src.models import Flight
from src.parsing import bucket_of

def _default_datenum() -> str:
    """Form default: tomorrow, Jalali ('14050719') — near-future dates always
    have live inventory; the old pinned 14050713 drifted into the past and
    every default search came back empty after the 30 s poll. Mock-mode envs
    without jdatetime keep the pinned demo date (tests pin their own dates)."""
    try:
        import jdatetime
    except ImportError:
        return "14050713"
    from datetime import timedelta

    tomorrow = jdatetime.date.today() + timedelta(days=1)  # crosses month/year bounds
    return f"{tomorrow.year:04d}{tomorrow.month:02d}{tomorrow.day:02d}"


DEFAULT_DATENUM = _default_datenum()
POPULAR_ROUTES = [("THR", "MHD"), ("THR", "KIH"), ("MHD", "THR"), ("SYZ", "THR")]
# (o, d, «NAME به NAME») — built once; _index_context and flight_results_context
# used to re-derive identical lists per request.
POPULAR = [(o, d, f"{AIRPORTS[o]} به {AIRPORTS[d]}") for o, d in POPULAR_ROUTES]


def _index_context(
    form_origin: str = "",
    form_destination: str = "",
    form_date: str | None = None,
    form_passengers: int = 1,
    results_html: str = "",
    error_message: str = "",
) -> dict:
    return {
        "airports": AIRPORTS,
        "popular": POPULAR,
        "default_datenum": DEFAULT_DATENUM,
        "form_origin": form_origin,
        "form_destination": form_destination,
        "form_date": form_date or date_fa(DEFAULT_DATENUM),
        "form_passengers": form_passengers,
        "results_html": results_html,
        "error_message": error_message,
    }


@dataclass
class Filters:
    """PGP filter state — mirrors the /search/ filter rail on torob.com."""
    airlines: tuple[str, ...] = ()
    times: tuple[str, ...] = ()
    types: tuple[str, ...] = ()
    min_price: int | None = None
    max_price: int | None = None

    @property
    def is_active(self) -> bool:
        return bool(self.airlines or self.times or self.types or self.min_price or self.max_price)

    def as_params(self, **overrides) -> dict:
        """Base params + filters; overrides may add, replace or remove (None) filters."""
        base_keys = ("origin", "destination", "date", "passengers", "sort")
        params = {k: v for k, v in overrides.items() if k in base_keys and v is not None}
        for key, attr in (("airline", "airlines"), ("time", "times"), ("type", "types")):
            value = overrides.get(key, ",".join(getattr(self, attr)) or None)
            if value:
                params[key] = value
        for key in ("min_price", "max_price"):
            value = overrides.get(key, getattr(self, key))
            if value:
                params[key] = value
        return params


def parse_filters(
    airline: list[str] | None = None,
    time: list[str] | None = None,
    type_: list[str] | None = None,
    min_price: int | None = None,
    max_price: int | None = None,
) -> Filters:
    """Query values → Filters (csv lists arrive as 'airline=IR,W5' from chips/sort)."""
    def _clean(raw, allowed):
        if not raw:
            return ()
        flat = []
        for item in raw:
            flat.extend(part.strip() for part in str(item).split(","))
        return tuple(dict.fromkeys(v for v in flat if v in allowed))

    return Filters(
        airlines=_clean(airline, AIRLINES.keys()),
        times=_clean(time, TIME_BUCKETS.keys()),
        types=_clean(type_, ("charter", "system")),
        min_price=max(0, min_price) if min_price else None,
        max_price=max(0, max_price) if max_price else None,
    )


def apply_filters(flights: list[Flight], filters: Filters) -> list[Flight]:
    """The PGP predicates, extracted so the /live OOB pulse can filter newly
    streamed-in cards EXACTLY like the shell render — a filtered-out flight
    must not appear just because it landed in a later Alibaba chunk."""
    if filters.airlines:
        flights = [f for f in flights if f.airline in filters.airlines]
    if filters.times:
        flights = [f for f in flights if bucket_of(f) in filters.times]
    if filters.types:
        want_charter = "charter" in filters.types
        want_system = "system" in filters.types
        flights = [f for f in flights
                   if (f.is_charter and want_charter) or (not f.is_charter and want_system)]
    if filters.min_price:
        flights = [f for f in flights if f.min_price >= filters.min_price]
    if filters.max_price:
        flights = [f for f in flights if f.min_price <= filters.max_price]
    return flights


def flight_results_context(
    all_flights: list[Flight],
    origin: str, destination: str, datenum: str, passengers: int, sort: str, filters: Filters
) -> dict:
    # Facet counts on the unfiltered set (like torob.com's filter rail)
    airline_counts = Counter(f.airline for f in all_flights)
    time_counts = Counter(b for f in all_flights if (b := bucket_of(f)))
    type_counts = {
        "charter": sum(1 for f in all_flights if f.is_charter),
        "system": sum(1 for f in all_flights if not f.is_charter),
    }

    flights = apply_filters(all_flights, filters)

    # Observed price range of the WHOLE result set — powers the rail's
    # suggestion chip (torob.com's PriceSuggestions: «از X تا Y تومان»)
    prices = [f.min_price for f in all_flights]

    key = (
        (lambda f: (f.min_price, f.departs))
        if sort == "price"
        else (lambda f: (f.departs, f.min_price))
    )
    flights.sort(key=key)

    base = {"origin": origin, "destination": destination, "date": datenum,
            "passengers": passengers}
    sort_urls = {
        s: "/search/flights/results?" + urlencode(filters.as_params(sort=s, **base))
        for s in ("price", "time")
    }
    results_url = "/search/flights?" + urlencode(base)
    # Quick-filter pill URLs (captured drawer pills row) — each pill toggles
    # ONE flight type; pressing the active pill drops it again.
    type_urls = {
        t: "/search/flights/results?" + urlencode(
            filters.as_params(**base, sort=sort, type=None if t in filters.types else t))
        for t in ("charter", "system")
    }
    # The OOB ticker carries the CURRENT filter state so streamed-in cards are
    # filtered identically to the shell render (apply_filters in /live).
    live_qs = urlencode(filters.as_params(sort=sort, **base))

    # Active-filter chips (ds-active-filter) — each removes just itself
    chips = []
    for code in filters.airlines:
        rest = filters.as_params(
            **base, airline=",".join(a for a in filters.airlines if a != code) or None, sort=sort
        )
        chips.append({"label": AIRLINES[code]["name"], "url": "/search/flights/results?" + urlencode(rest)})
    for key_ in filters.times:
        rest = filters.as_params(
            **base, time=",".join(t for t in filters.times if t != key_) or None, sort=sort
        )
        chips.append({"label": TIME_BUCKETS[key_][2], "url": "/search/flights/results?" + urlencode(rest)})
    for t in filters.types:
        fa = "چارتری" if t == "charter" else "سیستمی"
        rest = filters.as_params(
            **base, type=",".join(x for x in filters.types if x != t) or None, sort=sort
        )
        chips.append({"label": fa, "url": "/search/flights/results?" + urlencode(rest)})
    if filters.min_price:
        rest = filters.as_params(**base, sort=sort)
        chips.append({"label": f"از {toman(filters.min_price)}", "url": "/search/flights/results?" + urlencode(rest)})
    if filters.max_price:
        rest = filters.as_params(**base, sort=sort)
        chips.append({"label": f"تا {toman(filters.max_price)}", "url": "/search/flights/results?" + urlencode(rest)})

    return {
        "flights": flights,
        "all_count": len(all_flights),
        "airline_counts": airline_counts,
        "time_counts": time_counts,
        "type_counts": type_counts,
        "price_min": min(prices) if prices else None,
        "price_max": max(prices) if prices else None,
        "time_buckets": TIME_BUCKETS,
        "origin": origin,
        "destination": destination,
        "origin_name": AIRPORTS[origin],
        "destination_name": AIRPORTS[destination],
        "datenum": datenum,
        "passengers": passengers,
        "sort": sort,
        "filters": filters,
        "sort_urls": sort_urls,
        "type_urls": type_urls,
        "popular": POPULAR,  # built once at import — identical to the per-request rebuild it replaced
        "results_url": results_url,
        "results_qs": urlencode(base),
        "live_qs": live_qs,
        "filter_qs": urlencode({**base, "sort": sort}),
        "active_chips": chips,
        "airlines": AIRLINES,
    }


index_context = _index_context  # public seam (routers); the underscore name
                                # stays for the root main.py re-export shim
