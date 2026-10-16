"""Flight read model — Redis quotes → cards/offers, stateless PDP resolution.

Extracted from main.py §"Live read model" (zero functional changes).
group_quotes()/offers_for_flight() are the live successors of mock_catalog.py's
generators. Everything here assumes the quote_value payload written by
pipelines/liveness.py (enriched with physical_flight_key, card-render fields
and unit_verified).
"""

from __future__ import annotations

import logging
import random

from src import app_state, mock_catalog, parsing, readmodel, scanbus
from src.constants import AGENCIES, AIRLINES, AIRPORTS
from src.formatting import duration_fa
from src.models import Agency, Flight
from src.readmodel import _rd, jalali_to_greg_iso, toman_from_rial

log = logging.getLogger(__name__)

AGENCY_BY_KEY = {a.key: a for a in AGENCIES}


def agency_deeplink(supplier: str | None, origin: str, destination: str,
                    datenum: str) -> str:
    """Where the خرید button lands — the supplier's OWN search page, never a
    stub (the former /torob-demo/ links 400'd on the supplier's site). All
    three formats are browser-verified (2026-10-16); the quotes are scanned
    at the worker's 1-adult config, hence the single-adult params.

    Alibaba takes the JALALI date dashed (1405-07-25 — what their SPA writes);
    FlyToday takes lowercase IATA pairs and a Gregorian ISO date."""
    if supplier == "alibaba":
        jalali = (f"{datenum[:4]}-{datenum[4:6]}-{datenum[6:8]}"
                  if len(datenum) == 8 else "")
        date_q = f"&departing={jalali}" if jalali else ""
        return (f"https://www.alibaba.ir/flights/{origin}-{destination}"
                f"?adult=1&child=0&infant=0{date_q}")
    if supplier == "flytoday":
        greg = jalali_to_greg_iso(datenum)
        date_q = f"&departureDate={greg}" if greg else ""
        return (f"https://www.flytoday.ir/flight/search"
                f"?departure={origin.lower()},1&arrival={destination.lower()},1"
                f"{date_q}&adt=1&chd=0&inf=0&cabin=1&isDomestic=true&isAnyWhere=false")
    if supplier == "mrbilit":
        return f"https://mrbilit.com/flights/{origin}-{destination}"
    agency = AGENCY_BY_KEY.get(supplier or "")
    return f"https://{agency.domain}" if agency else "#"


def _hm(local: str | None) -> str:
    """'2026-11-15T19:30:00+03:30' | '19:30' → '19:30' (card-facing HH:MM)."""
    if not local:
        return "--:--"
    return local.replace(" ", "T").split("T")[-1][:5]


def _priced(offers: list[dict]) -> list[dict]:
    """Quotes that carry a price — the agency_count basis and trust-pool input."""
    return [o for o in offers if o.get("price_total_rial")]


def _trusted_pool(offers: list[dict], priced: list[dict] | None = None) -> list[dict]:
    """Priced quotes with verified units preferred — an unverified unit never
    undercuts a verified one for کف قیمت (the ghost-agency bait guard, notes/
    Blind Spot.md §3). ONE definition of the trust rule, shared by
    group_quotes (card floor) and offers_for_flight (modal rows); `priced` may
    be passed in when the caller already computed it."""
    if priced is None:
        priced = _priced(offers)
    verified = [o for o in priced if o.get("unit_verified") is not False]
    return verified or priced  # unverified prices may show, never undercut


def group_quotes(quotes: list[dict], origin: str, destination: str, datenum: str) -> list[Flight]:
    """Redis hash members → ONE card per physical flight (spec §3 dedup): two
    OTAs selling IR600 collapse into one card; agencies live in the Compare
    modal. Rial → Toman happens here (the only ÷10 in the codebase), and an
    unverified unit never undercuts a verified one for کف قیمت — the Ghost-
    Agency bait vector (notes - Ticket/Blind Spot.md §3)."""
    by_physical: dict[str, list[dict]] = {}
    for quote in quotes:
        if quote.get("physical_flight_key"):
            by_physical.setdefault(quote["physical_flight_key"], []).append(quote)

    flights: list[Flight] = []
    for key, offers in by_physical.items():
        priced = _priced(offers)  # agency_count basis
        pool = _trusted_pool(offers, priced)
        if not pool:
            continue
        best = min(pool, key=lambda o: o["price_total_rial"])
        seats = [o["capacity_remaining"] for o in offers
                 if o.get("capacity_remaining") is not None]
        code = best.get("airline_code") or "IR"
        AIRLINES.setdefault(code, {"name": code, "color": "#64748b"})
        flights.append(
            Flight(
                id=key,
                origin=origin,
                destination=destination,
                datenum=datenum,
                airline=code,
                flight_no=best.get("flight_no") or "",
                departs=_hm(best.get("dep_local")),
                arrives=_hm(best.get("arr_local")),
                duration_text=duration_fa(int(best.get("journey_duration_minutes") or 0)),
                is_charter=bool(best.get("is_charter")),
                min_price=toman_from_rial(best["price_total_rial"]) or 0,
                seats_left=min(seats) if seats else 0,
                agency_count=len({o.get("supplier") for o in priced}),
            )
        )
    return flights


def _mark_best(rows: list[dict]) -> list[dict]:
    """Flag the cheapest purchasable row. Jinja's `loop` doesn't propagate
    through {% include %}, so seller_row.html reads row.is_best instead."""
    for row in rows:
        row["is_best"] = False
    if rows:
        rows[0]["is_best"] = True
    return rows


def _sort_rows(rows: list[dict]) -> list[dict]:
    """Cheapest-first, sold-out rows sunk to the bottom (one definition — the
    mock and live row builders used to hand-roll the same lambda)."""
    rows.sort(key=lambda row: (row["sold_out"], row["price"]))
    return rows


def offer_view(rows: list[dict] | None) -> tuple[list[dict], dict | None, bool, int]:
    """The tri-state surface shared by the PDP and the Compare modal:
    (offers, best, refresh_pending, live_count). rows is None ⇔ the route-day
    hash was COLD at read time — a TTL expiry, never a sellout claim."""
    refresh_pending = rows is None
    offers = rows or []
    best = next((row for row in offers if not row["sold_out"]), None)
    live_count = sum(1 for row in offers if not row["sold_out"])
    return offers, best, refresh_pending, live_count


def chip_counts(rows: list[dict]) -> dict[str, int]:
    """«خدمات آژانس‌ها» chip counts for offer_chips.html — BUYABLE rows only:
    a tombstoned fare is not a service offering."""
    live = [row for row in rows if not row.get("sold_out")]
    return {
        "tpay": sum(1 for row in live if row["agency"].torob_pay),
        "baggage": sum(1 for row in live if row.get("baggage_kg")),
        "refundable": sum(1 for row in live if row.get("refundable")),
    }


def _mock_offer_rows(flight: Flight) -> list[dict]:
    """MOCK SEAM (delete at cutover): cheapest-first offers with a deterministic
    per-agency sellout so the «ظرفیت تکمیل شد» row state is demoable."""
    rows = []
    for i, row in enumerate(mock_catalog.agencies_for(flight)):
        sold_out = i > 0 and random.Random(
            f"soldout:{flight.id}:{row['agency'].key}"
        ).random() < 0.25
        rows.append({**row, "sold_out": sold_out})
    _sort_rows(rows)
    _mark_best(rows)
    return rows


async def offers_for_flight(flight: Flight) -> list[dict] | None:
    """Click-time revalidation: read the Redis hash NOW and keep only this
    physical flight's quotes. A fare HDEL'd by a tombstone (liveness.py) simply
    isn't here anymore — that IS the sold-out state ([]). Returns None when the
    route-day hash is COLD: a TTL expiry is not a tombstone, so the caller must
    show a refresh state — never mock offers, never a sellout claim. Rows are
    cheapest-first; sold-out (capacity 0) rows sink to the bottom."""
    quotes = await readmodel.read_quotes(flight.origin, flight.destination, flight.datenum)
    if not quotes:
        if not app_state._LIVE_READY:
            return _mock_offer_rows(flight)  # MOCK SEAM (delete at cutover)
        scanbus.trigger_scan_later(flight.origin, flight.destination, flight.datenum)
        return None

    mine = [q for q in quotes if q.get("physical_flight_key") == flight.id]
    if not mine:
        return []  # warm cache, zero offers ⇒ the whole flight was tombstoned

    pool = _trusted_pool(mine)
    if not pool:
        return []
    floor = toman_from_rial(min(q["price_total_rial"] for q in pool))

    rows = []
    for quote in pool:
        price = toman_from_rial(quote["price_total_rial"])
        agency = AGENCY_BY_KEY.get(quote.get("supplier")) or Agency(
            quote.get("supplier") or "?", quote.get("supplier") or "?", "example.com", 3.5
        )
        rows.append(
            {
                "agency": agency,
                "price": price,
                "delta": price - floor,
                # None = capacity not reported by this supplier ≠ 0. Real
                # sellouts arrive as tombstone HDELs (absent), so unknown
                # capacity must never render «ظرفیت تکمیل شد».
                "sold_out": quote.get("capacity_remaining") == 0,
                "deeplink": agency_deeplink(quote.get("supplier"),
                                            flight.origin, flight.destination,
                                            flight.datenum),
                # per-fare detail (torob sellers-list anatomy) — absent keys on
                # pre-rollout hash members degrade to hidden chips, never errors
                "is_charter": bool(quote.get("is_charter")),
                "seats": quote.get("capacity_remaining"),
                "baggage_kg": parsing.baggage_kg(quote.get("baggage")),
                "baggage_cabin": None,  # suppliers don't split cabin/checked yet
                "refundable": parsing.refundable_flag(quote.get("refund_method")),
            }
        )
    _sort_rows(rows)
    log.info("%s offers → %d row(s) (%d sold-out) for %s %s",
             _rd(flight.origin, flight.destination, flight.datenum),
             len(rows), sum(1 for r in rows if r["sold_out"]),
             flight.airline, flight.departs)
    return _mark_best(rows)


async def resolve_flight(ref: str) -> tuple[Flight | None, bool]:
    """Stateless resolution for /flights/{ref}: the physical key IS the
    identity, so a live-rendered card rebuilds from the same route-day hash it
    was painted from — no reliance on the in-process mock index (which never
    contains live keys), and it works across uvicorn workers and cold
    processes. Returns (flight, cold): cold=True means the hash itself was
    empty/unreachable, so 'no flight' carries NO sold-out meaning — an expired
    cache is not a tombstone (liveness.py HDELs members from a WARM hash); the
    caller shows a refresh state instead.

    The Redis fallback runs in MOCK mode too: TOROB_FORCE_MOCK=1 with the
    pipeline's Redis still warm serves LIVE keys from /results (group_quotes
    runs whenever the hash has quotes), so short-circuiting on !LIVE_READY here
    would 404 every live-rendered card's PDP. Only a COLD hash falls back to
    mock-catalog semantics."""
    flight = parsing.find_flight(ref)  # mock catalog hit / warm in-process index
    if flight is not None:
        log.debug("resolve %s → warm index/mock catalog hit", ref)
        return flight, False
    parts = ref.split(".")
    if len(parts) != 4:
        log.info("resolve %s → malformed ref (need ORIGIN.DEST.datenum.<physical_key>)", ref)
        return None, False
    origin, destination, datenum, key = parts
    if origin not in AIRPORTS or destination not in AIRPORTS:
        log.info("resolve %s → unknown airport in ref", ref)
        return None, False
    quotes = await readmodel.read_quotes(origin, destination, datenum)
    if not quotes:
        if not app_state._LIVE_READY:
            return None, False  # frontend-only env: the mock catalog is the truth
        scanbus.trigger_scan_later(origin, destination, datenum)
        log.info("%s resolve %s → hash COLD (TTL expired?) — refresh state, scan requested",
                 _rd(origin, destination, datenum), key)
        return None, True
    # Filter the raw quotes to THIS physical flight first (offers_for_flight's
    # pattern) — group_quotes then builds one Flight instead of every card on
    # the route just to scan for the match.
    mine = [q for q in quotes if q.get("physical_flight_key") == key]
    rebuilt = group_quotes(mine, origin, destination, datenum) if mine else []
    if rebuilt:
        flight = rebuilt[0]
        mock_catalog._remember_flight(flight)  # warm the LRU so the PDP / next click is cheap
        log.info("%s resolve %s → rebuilt from Redis hash (%s %s)",
                 _rd(origin, destination, datenum), key,
                 flight.airline, flight.departs)
        return flight, False
    log.info("%s resolve %s → warm hash, key ABSENT ⇒ fully tombstoned (sold out)",
             _rd(origin, destination, datenum), key)
    return None, False  # warm hash, key absent ⇒ the whole flight was tombstoned


async def live_offers(ref: str) -> tuple[Flight | None, list[dict] | None, bool]:
    """Resolve the /flights/{ref} URL pointer, then click-time-revalidate.
    (flight, rows, cold) — rows is None ⇔ the hash was cold at click time."""
    flight, cold = await resolve_flight(ref)
    if flight is None:
        return None, None, cold
    return flight, await offers_for_flight(flight), cold


async def _flights_for(origin: str, destination: str, datenum: str) -> list[Flight] | None:
    """Live-first read model for full-page renders. Warm Redis hash → grouped
    cards. Cold hash → mock catalog in frontend-only mode, or None (caller shows
    the self-polling skeleton) when the live stack is armed."""
    quotes = await readmodel.read_quotes(origin, destination, datenum)
    if quotes:
        return group_quotes(quotes, origin, destination, datenum)
    if not app_state._LIVE_READY:
        return mock_catalog.generate_flights(origin, destination, datenum)  # MOCK SEAM (delete at cutover)
    scanbus.trigger_scan_later(origin, destination, datenum)
    return None


flights_for = _flights_for  # public seam (routers); the underscore name stays
                            # for the root main.py re-export shim
