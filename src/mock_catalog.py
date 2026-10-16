"""MOCK SEAM (delete at cutover) — the deterministic catalog that stands in for
the Redis read model in frontend-only environments.

Extracted from main.py §"Mock generators" + the mock-drift branch of
/search/flights/live (zero functional changes). Live replacements live in
flights.py (group_quotes()/offers_for_flight()); NONE of these render in live
mode.
"""

from __future__ import annotations

import random
from collections import OrderedDict
from dataclasses import replace
from datetime import datetime, timezone
from urllib.parse import urlencode

from pipelines.common.dto import physical_flight_key
from pipelines.common.lrucache import remember_latest

from src.constants import AGENCIES, AIRLINES
from src.formatting import duration_fa
from src.models import Flight

_FLIGHT_INDEX: "OrderedDict[str, Flight]" = OrderedDict()  # ref → Flight (mock catalog)
_FLIGHT_INDEX_LIMIT = 500

_mock_pulse_ticks: dict[str, int] = {}


def _remember_flight(flight: Flight) -> None:
    remember_latest(_FLIGHT_INDEX, flight.ref, flight, _FLIGHT_INDEX_LIMIT)


def _agency_count(flight_id: str) -> int:
    # Must consume randomness in the same order as agencies_for() so the two agree.
    return random.Random(f"agencies-{flight_id}").randint(3, len(AGENCIES))


def generate_flights(origin: str, destination: str, datenum: str) -> list[Flight]:
    """Deterministic inventory: the same route+date always yields the same flights."""
    rng = random.Random(f"{origin}-{destination}-{datenum}")

    codes = rng.sample(sorted(AIRLINES), rng.randint(4, 6))
    route_factor = rng.randint(2, 9)  # stands in for real great-circle distance
    base_price = 1_200_000 + route_factor * 210_000
    duration = 65 + route_factor * 11 + rng.randint(0, 25)

    flights: list[Flight] = []
    for i, code in enumerate(codes):
        dep_total = (6 + 2 * i + rng.randint(0, 1)) * 60 + rng.choice((0, 15, 20, 35, 40, 50))
        arr_total = dep_total + duration
        charter = rng.random() < 0.28
        price = base_price + rng.randint(-6, 20) * 10_000
        if charter:
            price += rng.randint(-3, 6) * 10_000
        flight_no = f"{code}{rng.randint(100, 899)}"
        departs = f"{dep_total // 60:02d}:{dep_total % 60:02d}"
        # Cross-supplier identity (dto.py §3): the same physical flight from two
        # OTAs maps to ONE card — never a per-agency card, never a list position.
        fid = physical_flight_key(code, flight_no, departs, origin, destination)
        flight = Flight(
            id=fid,
            origin=origin,
            destination=destination,
            datenum=datenum,
            airline=code,
            flight_no=flight_no,
            departs=departs,
            arrives=f"{arr_total // 60 % 24:02d}:{arr_total % 60:02d}",
            duration_text=duration_fa(duration),
            is_charter=charter,
            min_price=int(round(price, -4)),
            seats_left=rng.randint(2, 33),
            agency_count=_agency_count(fid),
        )
        flights.append(flight)
        _remember_flight(flight)
    return flights


def agencies_for(flight: Flight) -> list[dict]:
    """Every agency selling this exact flight — strictly cheapest first."""
    rng = random.Random(f"agencies-{flight.id}")
    chosen = rng.sample(AGENCIES, rng.randint(3, len(AGENCIES)))
    rows, price = [], flight.min_price
    for agency in chosen:
        # Per-fare detail (torob sellers-list anatomy) from a DEDICATED rng —
        # the agencies-{id} draw order (randint → sample → ladder) that
        # _agency_count() replays must stay untouched.
        frng = random.Random(f"fare:{flight.id}:{agency.key}")
        refund_roll = frng.random()
        rows.append(
            {
                "agency": agency,
                "price": price,
                "delta": price - flight.min_price,
                # CPC handoff to the agency (mock deeplink for the MVP)
                "deeplink": f"https://{agency.domain}/torob-demo/{flight.id}",
                # one physical flight, mixed fares: charter flights are all-charter,
                # system flights occasionally get a charter reseller
                "is_charter": flight.is_charter or frng.random() < 0.12,
                # None = capacity not reported (≠ 0 ≠ sold out); else 1–12 so both
                # «آخرین N صندلی» (≤9) and the hidden >9 state demo
                "seats": None if frng.random() < 0.2 else frng.randint(1, 12),
                "baggage_kg": frng.randint(15, 25),
                "baggage_cabin": frng.choice((5, 7)),
                # tri-state: hidden / قابل استرداد / غیرقابل استرداد
                "refundable": None if refund_roll < 0.15 else refund_roll < 0.8,
            }
        )
        price += rng.randint(2, 24) * 10_000
    return rows


def price_history(flight: Flight) -> list[dict]:
    """7-day mock price history for the PDP chart (deterministic per flight)."""
    rng = random.Random(f"hist-{flight.id}")
    prices = [flight.min_price + rng.randint(-8, 26) * 10_000 for _ in range(7)]
    floor = min(prices)
    spread = max(prices) - floor or 1
    return [
        {"i": i, "price": p, "height": 18 + round((p - floor) / spread * 54)}
        for i, p in enumerate(prices)
    ]


def pulse_drift(origin: str, destination: str, datenum: str, base: dict) -> dict:
    """MOCK SEAM (delete at cutover; the live branch is in routers/search.py):
    cold cache → keep the pipeline moving, and demo the OOB mechanics with mock
    drift. Extracted verbatim from the /search/flights/live route; returns the
    live_pulse.html context, the route renders it."""
    tick_key = f"mock:{origin}-{destination}-{datenum}"
    _mock_pulse_ticks[tick_key] = _mock_pulse_ticks.get(tick_key, 0) + 1
    drift_seed = int(datetime.now(timezone.utc).timestamp()) // 2
    rng = random.Random(f"{tick_key}:{drift_seed}")  # drift every ~2 s
    changed: list[Flight] = []
    removed: list[str] = []
    for flight in generate_flights(origin, destination, datenum):
        if rng.random() < 0.05:  # an occasional tombstone, to demo removal
            removed.append(flight.id)
            continue
        changed.append(
            replace(
                flight,
                min_price=max(
                    400_000,
                    flight.min_price + rng.choice((-2, -1, 0, 0, 1, 2)) * 10_000,
                ),
                seats_left=max(0, flight.seats_left + rng.choice((-1, 0, 0, 1))),
            )
        )
    return {
        "changed": changed,
        "added": [],
        "removed": removed,
        "complete": _mock_pulse_ticks[tick_key] >= 10,  # demo: ~20 s then stop
        "live_qs": urlencode(base),
        "airlines": AIRLINES,  # flight_card.html (full-card inserts)
    }
