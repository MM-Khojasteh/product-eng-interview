#!/usr/bin/env python
"""Offline tests for the web BFF seam (no Kafka/Redis/network needed).

Run:  python -m pytest tests/web/test_frontend.py -q   (or plain `pytest` —
pytest.ini sets testpaths). TOROB_FORCE_MOCK=1 is pinned at import so no Redis
connection is attempted; the liveness state machine and tombstone semantics are
tested by monkeypatching readmodel.read_quotes / app_state._LIVE_READY (the
owning modules — root main.py is a thin shim whose re-exports don't propagate
assignments).
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path

os.environ.setdefault("TOROB_FORCE_MOCK", "1")  # must precede the main import

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from src import app_state, flights, mock_catalog, parsing, readmodel  # noqa: E402
from src.constants import MAX_POLL_ATTEMPTS  # noqa: E402

# The skeleton renders the budget in Persian digits — derive it so tuning the
# constant never breaks these assertions.
MAX_POLL_FA = str(MAX_POLL_ATTEMPTS).translate(str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹"))

# Hermetic suite: never touch a real Redis even when one runs on localhost —
# the async pool binds to one event loop and TestClient + asyncio.run() mix
# loops ("Event loop is closed"). Live-read behavior is faked via read_quotes.
readmodel.rds = None


def _await(coro):
    return asyncio.run(coro)


def check(condition: bool, label: str) -> None:
    assert condition, label


# ---------------------------------------------------------------------------
# Fakes for the live read model
# ---------------------------------------------------------------------------

K_IR600 = main.physical_flight_key("IR", "IR600", "09:30", "THR", "MHD")
K_W5301 = main.physical_flight_key("W5", "W5301", "16:00", "THR", "MHD")


def _quote(supplier: str, key: str, rial: int, **overrides) -> dict:
    quote = {
        "supplier": supplier,
        "physical_flight_key": key,
        "airline_code": "IR",
        "flight_no": "IR600",
        "dep_local": "2026-10-05T09:30:00+03:30",
        "arr_local": "2026-10-05T11:15:00+03:30",
        "journey_duration_minutes": 105,
        "price_total_rial": rial,
        "unit_verified": True,
        "capacity_remaining": 5,
        "is_charter": False,
        "baggage": "20 کیلوگرم",
        "refund_method": "Online",
        "captured_at": "2026-10-02T10:00:00+00:00",
    }
    quote.update(overrides)
    return quote


WARM_QUOTES = [
    _quote("flytoday", K_IR600, 12_400_000),
    _quote("alibaba", K_IR600, 13_100_000),
    _quote("alibaba", K_IR600, 9_900_000, unit_verified=False),  # ghost bait
    _quote("mrbilit", K_W5301, 22_000_000),
]


async def _warm_read(origin: str, destination: str, datenum: str) -> list[dict]:
    return WARM_QUOTES


async def _cold_read(origin: str, destination: str, datenum: str) -> list[dict]:
    return []


# ---------------------------------------------------------------------------
# 1. Currency + Jalali conversions (the Redis seam)
# ---------------------------------------------------------------------------


def test_conversions() -> None:
    assert main.toman_from_rial(12_400_000) == 1_240_000, "Rial → Toman ÷10 at the seam"
    assert main.toman_from_rial(None) is None
    assert main.toman(1_240_000).endswith("تومان") and "۱٬۲۴۰٬۰۰۰" in main.toman(1_240_000)
    greg = main.jalali_to_greg_iso("14050713")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", greg or ""), greg
    assert main.quote_key("THR", "MHD", "14050713") == f"quote:THR-MHD:{greg}"
    assert main.jalali_to_greg_iso("14051340") is None  # invalid Jalali date
    assert main.parse_airport("تهران") == "THR", "bare Persian city name → first code"
    assert main.parse_airport("تهران (IKA)") == "IKA" and main.parse_airport("SYZ") == "SYZ"
    check(True, "rial→toman + jalali→gregorian + airport parsing")


# ---------------------------------------------------------------------------
# 2. Dedup: ONE card per physical flight; ghost prices never undercut
# ---------------------------------------------------------------------------


def test_group_quotes_dedup() -> None:
    flights = main.group_quotes(WARM_QUOTES, "THR", "MHD", "14050713")
    assert len(flights) == 2, "two physical flights → exactly two cards"
    card = next(f for f in flights if f.id == K_IR600)
    assert card.agency_count == 2, "alibaba+flytoday collapse into one card"
    assert card.min_price == 1_240_000, "floor from the VERIFIED quote (12.4M rial), not the 9.9M ghost"
    assert card.departs == "09:30" and card.arrives == "11:15", "ISO local → card HH:MM"
    assert main.toman(card.min_price).startswith("۱٬۲۴۰")
    check(True, "group_quotes dedup + unit guard")


# ---------------------------------------------------------------------------
# 2b. Sellers-list anatomy: agency trust signals, per-fare fields, chips
# ---------------------------------------------------------------------------


def test_agencies_catalog() -> None:
    from src.constants import AGENCIES
    assert all(re.fullmatch(r"#[0-9a-f]{6}", a.color) for a in AGENCIES), "avatar colors"
    assert all(1 <= (a.years_active or 0) <= 50 for a in AGENCIES), "«N سال سابقه» sane"
    assert next(a for a in AGENCIES if a.key == "alibaba").torob_pay, \
        "«با ترب‌پی» demos on the flagship agency"
    # the unknown-supplier fallback in offers_for_flight() stays constructible
    unknown = main.Agency("x", "ناشناخته", "x.example", 3.5)
    assert unknown.color and not unknown.torob_pay and unknown.years_active is None
    check(True, "agency catalog: color / torob_pay / since defaults")


def test_mock_offers_enriched() -> None:
    catalog = main.generate_flights("THR", "MHD", "14050713")
    assert catalog and all(
        f.agency_count == len(main.agencies_for(f)) for f in catalog
    ), "regression guard: the agencies-{id} draw order still agrees with _agency_count"
    for f in catalog + main.generate_flights("IFN", "KIH", "14050801"):
        rows = main.agencies_for(f)
        assert rows == main.agencies_for(f), "per-fare fields are deterministic"
        for row in rows:
            assert 15 <= row["baggage_kg"] <= 25 and row["baggage_cabin"] in (5, 7)
            assert row["seats"] is None or 1 <= row["seats"] <= 12
            assert row["refundable"] in (True, False, None)
            if f.is_charter:
                assert row["is_charter"], "charter flights are all-charter fares"
    check(True, "mock per-fare fields: determinism + ranges + agency_count guard")


def test_fare_field_parsing() -> None:
    kg = parsing.baggage_kg
    assert kg("30 کیلوگرم") == 30, "flytoday Persian form"
    assert kg("2×23KG") == 23, "the kg-adjacent number, not the multiplier"
    assert kg("۲۳") == 23, "digits-only Persian fallback"
    assert kg("1 قطعه") is None, "piece counts never render as kg"
    assert kg(None) is None and kg("") is None
    refund = parsing.refundable_flag
    assert refund("Offline") is True and refund("Online") is True
    assert refund("non-refundable") is False and refund("غیرقابل استرداد") is False
    assert refund("mystery") is None and refund(None) is None, "unknown stays hidden"
    check(True, "baggage/refund supplier vocabularies → chip fields")


def test_chip_counts() -> None:
    rows = [
        {"agency": main.Agency("a", "آ", "a.ir", 4.0, torob_pay=True),
         "sold_out": False, "baggage_kg": 20, "refundable": True},
        {"agency": main.Agency("b", "ب", "b.ir", 4.0, torob_pay=False),
         "sold_out": False, "baggage_kg": None, "refundable": False},
        {"agency": main.Agency("c", "پ", "c.ir", 4.0, torob_pay=True),
         "sold_out": True, "baggage_kg": 25, "refundable": True},
    ]
    assert flights.chip_counts(rows) == {"tpay": 1, "baggage": 1, "refundable": 1}, \
        "a tombstoned fare is not a service offering"


# ---------------------------------------------------------------------------
# 3. Identity: physical_flight_key refs, never positional indexes
# ---------------------------------------------------------------------------


def test_flight_ref_roundtrip() -> None:
    first = main.generate_flights("THR", "MHD", "14050713")
    again = main.generate_flights("THR", "MHD", "14050713")
    assert [f.id for f in first] == [f.id for f in again], "deterministic catalog"
    assert all(re.fullmatch(r"[0-9a-f]{16}", f.id) for f in first), "dto §3 physical keys"
    found = main.find_flight(first[0].ref)
    assert found is not None and found.id == first[0].id
    assert main.find_flight("THR.MHD.14050713.2") is None, "positional ids are gone"
    assert main.find_flight("THR.MHD.14050713.deadbeefdeadbeef") is None
    assert main.find_flight("THR.THR.14050713." + "a" * 16) is None, "unknown route"
    check(True, "flight ref roundtrip")


# ---------------------------------------------------------------------------
# 4. Offers: click-time revalidation + tombstone semantics
# ---------------------------------------------------------------------------


def test_offers_tombstones() -> None:
    ir600 = main.Flight(
        id=K_IR600, origin="THR", destination="MHD", datenum="14050713",
        airline="IR", flight_no="IR600", departs="09:30", arrives="11:15",
        duration_text="۱ ساعت و ۴۵ دقیقه", is_charter=False,
        min_price=1_240_000, seats_left=5, agency_count=2,
    )
    original = readmodel.read_quotes
    readmodel.read_quotes = _warm_read
    try:
        rows = _await(main.offers_for_flight(ir600))
        assert [r["price"] for r in rows] == [1_240_000, 1_310_000], \
            "cheapest verified first — the 990k ghost price never surfaces"
        assert rows[0]["agency"].key == "flytoday" and not rows[0]["sold_out"]
        # خرید lands on the supplier's REAL search surface — browser-verified
        # formats (2026-10-16), never the /torob-demo/ stubs that 400'd
        deeplinks = {r["agency"].key: r["deeplink"] for r in rows}
        assert deeplinks["alibaba"] == (
            "https://www.alibaba.ir/flights/THR-MHD"
            "?adult=1&child=0&infant=0&departing=1405-07-13"  # Jalali, dashed
        ), deeplinks["alibaba"]
        assert deeplinks["flytoday"] == (
            "https://www.flytoday.ir/flight/search"
            "?departure=thr,1&arrival=mhd,1"
            f"&departureDate={readmodel.jalali_to_greg_iso('14050713')}"
            "&adt=1&chd=0&inf=0&cabin=1&isDomestic=true&isAnyWhere=false"
        ), deeplinks["flytoday"]
        # per-fare passthrough (sellers-list anatomy): parsed baggage, refund
        # vocabulary, capacity → «آخرین N صندلی» when scarce
        assert rows[0]["baggage_kg"] == 20 and rows[0]["refundable"] is True
        assert rows[0]["seats"] == 5 and rows[0]["is_charter"] is False
        # whole-flight tombstone: warm hash, zero quotes for this key
        readmodel.read_quotes = _warm_read_other_flight
        assert _await(main.offers_for_flight(ir600)) == [], \
            "warm cache + absent key ⇒ sold out, not stale offers"
        # capacity-0 fare → sold-out row, not a dead خرید button
        readmodel.read_quotes = _warm_read_zero_capacity
        rows3 = _await(main.offers_for_flight(ir600))
        assert len(rows3) == 2 and rows3[-1]["sold_out"], "capacity 0 sinks to the bottom"
    finally:
        readmodel.read_quotes = original
    check(True, "offers click-time revalidation")


async def _warm_read_other_flight(origin, destination, datenum):
    return [_quote("mrbilit", K_W5301, 22_000_000)]


async def _warm_read_zero_capacity(origin, destination, datenum):
    return [
        _quote("flytoday", K_IR600, 12_400_000, capacity_remaining=0),
        _quote("alibaba", K_IR600, 13_100_000),
    ]


async def _warm_read_unknown_capacity(origin, destination, datenum):
    return [
        _quote("flytoday", K_IR600, 12_400_000, capacity_remaining=None),
        _quote("alibaba", K_IR600, 13_100_000, capacity_remaining=None),
    ]


async def _warm_read_ir600_only(origin, destination, datenum):
    return [
        _quote("flytoday", K_IR600, 12_400_000),
        _quote("alibaba", K_IR600, 13_100_000),
    ]


# ---------------------------------------------------------------------------
# 5. Routes: skeleton polling, OOB pulse, compare modal (mock mode)
# ---------------------------------------------------------------------------

client = TestClient(main.app)
QS = "origin=THR&destination=MHD&date=14050713&passengers=1"


# ---------------------------------------------------------------------------
# 4b. Live-mode contracts: stateless resolution, expired≠sold-out, pulse adds
# ---------------------------------------------------------------------------


def _ir600_flight() -> main.Flight:
    return main.Flight(
        id=K_IR600, origin="THR", destination="MHD", datenum="14050713",
        airline="IR", flight_no="IR600", departs="09:30", arrives="11:15",
        duration_text="۱ ساعت و ۴۵ دقیقه", is_charter=False,
        min_price=1_240_000, seats_left=5, agency_count=2,
    )


def test_offers_live_states() -> None:
    ir600 = _ir600_flight()
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    try:
        # COLD hash → None (refresh state), never mock rows in live mode
        readmodel.read_quotes = _cold_read
        assert _await(main.offers_for_flight(ir600)) is None, \
            "cold hash ⇒ refresh state — mock offers must not leak into live mode"
        # capacity None = not reported ≠ sold out
        readmodel.read_quotes = _warm_read_unknown_capacity
        rows = _await(main.offers_for_flight(ir600))
        assert rows is not None and not any(r["sold_out"] for r in rows), \
            "unknown capacity must never render «ظرفیت تکمیل شد»"
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
    check(True, "offers live states: cold→refresh, capacity None→sellable")


def test_resolve_flight_live() -> None:
    ref = f"THR.MHD.14050713.{K_IR600}"  # a live-rendered card's ref — never in
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY  # the mock index
    index_snapshot = dict(mock_catalog._FLIGHT_INDEX)
    app_state._LIVE_READY = True
    try:
        # warm hash → the card rebuilds from Redis alone
        readmodel.read_quotes = _warm_read
        mock_catalog._FLIGHT_INDEX.clear()
        flight, cold = _await(main.resolve_flight(ref))
        assert flight is not None and flight.id == K_IR600 and not cold
        # cold hash → (None, True): expired ≠ sold-out (a fresh process has no
        # index — resolve_flight warms _FLIGHT_INDEX, so clear it per branch)
        readmodel.read_quotes = _cold_read
        mock_catalog._FLIGHT_INDEX.clear()
        flight, cold = _await(main.resolve_flight(ref))
        assert flight is None and cold
        # warm hash, key absent ⇒ tombstoned (cold=False)
        readmodel.read_quotes = _warm_read_other_flight
        mock_catalog._FLIGHT_INDEX.clear()
        flight, cold = _await(main.resolve_flight(ref))
        assert flight is None and not cold
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
        mock_catalog._FLIGHT_INDEX.clear()
        mock_catalog._FLIGHT_INDEX.update(index_snapshot)
    check(True, "resolve_flight: live refs resolve statelessly")


def test_resolve_flight_mock_mode_warm_redis() -> None:
    # Dev seam: TOROB_FORCE_MOCK=1 while the pipeline's Redis stays warm —
    # /results serves LIVE keys (group_quotes runs on warm quotes), so the
    # mock-catalog short-circuit must not 404 those cards' PDPs.
    ref = f"THR.MHD.14050713.{K_IR600}"
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = False
    readmodel.read_quotes = _warm_read
    index_snapshot = dict(mock_catalog._FLIGHT_INDEX)
    try:
        mock_catalog._FLIGHT_INDEX.clear()
        flight, cold = _await(main.resolve_flight(ref))
        assert flight is not None and flight.id == K_IR600 and not cold, \
            "warm Redis resolves live keys even in mock mode"
        # cold hash in mock mode keeps mock-catalog semantics (no refresh state)
        readmodel.read_quotes = _cold_read
        mock_catalog._FLIGHT_INDEX.clear()  # the warm branch LRU-cached the rebuild
        flight, cold = _await(main.resolve_flight(ref))
        assert flight is None and not cold, "mock + cold hash → plain not-found"
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
        mock_catalog._FLIGHT_INDEX.clear()
        mock_catalog._FLIGHT_INDEX.update(index_snapshot)
    check(True, "resolve_flight: mock mode + warm Redis still resolves live keys")


def test_offers_modal_tri_state() -> None:
    ref = f"THR.MHD.14050713.{K_IR600}"
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    readmodel.read_quotes = _cold_read
    try:
        r = client.get(f"/flights/{ref}/offers")
        assert r.status_code == 200 and "در حال به‌روزرسانی" in r.text
        assert f"/flights/{ref}/offers?attempt=2" in r.text, "modal re-arms itself"
        assert "ظرفیت این پرواز تکمیل شد" not in r.text, "expired ≠ sold-out"
        # budget exhausted → honest retry CTA, still NOT a sellout claim
        r = client.get(f"/flights/{ref}/offers?attempt=9")
        assert "در دسترس نیست" in r.text
        assert "ظرفیت این پرواز تکمیل شد" not in r.text

        # warm + live → enriched rows + «خدمات فروشنده» chips (both agencies
        # here are tpay + baggage + refundable, so the chips are deterministic)
        readmodel.read_quotes = _warm_read
        r = client.get(f"/flights/{ref}/offers")
        assert "با ترب‌پی" in r.text and "بار ۲۰ کیلوگرم" in r.text
        assert "آخرین ۵ صندلی" in r.text, "capacity 5 ≤ 9 renders the scarcity line"

        # warm + fully tombstoned → the genuine sold-out state
        readmodel.read_quotes = _warm_read_other_flight
        r = client.get(f"/flights/{ref}/offers")
        assert "ظرفیت این پرواز تکمیل شد" in r.text
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
    check(True, "compare modal tri-state: refresh / retry / sold-out")


def test_pdp_expired_state() -> None:
    ref = main.generate_flights("THR", "MHD", "14050713")[0].ref
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    readmodel.read_quotes = _cold_read
    try:
        r = client.get(f"/flights/{ref}")
        assert r.status_code == 200 and "در دسترس نیست" in r.text
        assert "ظرفیت این پرواز تکمیل شد" not in r.text, "expired ≠ sold-out on the PDP"
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
    check(True, "PDP expired state")


def test_live_pulse_inserts_and_removes() -> None:
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    readmodel.read_quotes = _warm_read  # IR600 + W5301 both present now
    try:
        key = main.quote_key("THR", "MHD", "14050713")
        app_state._pulse_state.clear()
        # simulate the shell having painted ONLY IR600 (the first Alibaba chunk):
        # the W5301 chunk lands after the shell → its FULL card must be inserted
        app_state._pulse_state[key] = (frozenset({K_IR600}), None, 0)
        r = client.get("/search/flights/live?" + QS)
        assert f'id="flight-{K_W5301}"' in r.text, "late-chunk flight gets a card"
        assert 'hx-swap-oob="beforeend:#flight-cards"' in r.text, \
            "insertion targets the card list container, not a missing price box"
        assert f'id="price-{K_W5301}"' in r.text

        # tombstone between ticks: W5301 vanished from the hash → OOB delete
        readmodel.read_quotes = _warm_read_ir600_only
        app_state._pulse_state[key] = (frozenset({K_IR600, K_W5301}), ("sig",), 0)
        r = client.get("/search/flights/live?" + QS)
        assert f'<article id="flight-{K_W5301}" hx-swap-oob="delete">' in r.text
        assert 'id="flight-' + K_IR600 + '" hx-swap-oob' not in r.text, \
            "unchanged flights refresh price boxes only — no card churn"

        # scan completes only after two consecutive agreeing reads
        readmodel.read_quotes = _warm_read_ir600_only
        r1 = client.get("/search/flights/live?" + QS)
        r2 = client.get("/search/flights/live?" + QS)
        r3 = client.get("/search/flights/live?" + QS)
        assert 'id="live-ticker"' in r1.text and 'id="live-ticker"' in r2.text
        assert 'id="live-ticker" hx-swap-oob="delete"' in r3.text, \
            "third identical read self-extinguishes the ticker (2-agreement rule)"
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
        app_state._pulse_state.clear()
    check(True, "live pulse: chunk insertions, tombstone deletes, completion")


def test_live_pulse_cold_in_live_mode_stops_cleanly() -> None:
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    readmodel.read_quotes = _cold_read
    try:
        r = client.get("/search/flights/live?" + QS)
        assert 'id="live-ticker" hx-swap-oob="delete"' in r.text, \
            "TTL expiry stops polling — no mock content in live mode"
        assert 'id="price-' not in r.text
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
    check(True, "live pulse: cold-in-live stops cleanly")


def test_routes_mock_flow() -> None:
    # Step 1 — skeleton re-arms itself (server-driven polling contract)
    r = client.get("/search/flights?" + QS, headers={"HX-Request": "true"})
    assert r.status_code == 200
    assert 'hx-trigger="load delay:2s"' in r.text and f"تلاش ۱ از {MAX_POLL_FA}" in r.text

    # Step 2 — browse shell with the OOB ticker armed; the ticker carries the
    # current filter state (base + sort) so streamed-in cards filter correctly
    r = client.get("/search/flights/results?" + QS)
    assert r.status_code == 200 and 'id="live-ticker"' in r.text
    ticker = re.search(r'id="live-ticker" hx-get="([^"]+)"', r.text)
    assert ticker is not None and ticker.group(1).startswith("/search/flights/live?")
    for part in QS.split("&") + ["sort=price"]:
        assert part in ticker.group(1)
    assert 'id="price-' in r.text, "price boxes carry OOB targets"
    # real brand marks: ≥3 of the 4–6 sampled airlines carry a bundled logo
    # (6 of the 7 catalog airlines have one; QB falls back to the color chip)
    assert r.text.count('src="/static/logos/airlines/') >= 3, "airline logo tiles"

    # Step 3 — pulse returns ONLY hx-swap-oob fragments and re-emits the ticker
    r = client.get("/search/flights/live?" + QS)
    assert r.status_code == 200 and "hx-swap-oob" in r.text
    assert r.text.count('id="price-') >= 1
    assert 'id="live-ticker"' in r.text

    # Step 4 — compare modal (click-time offers) from a card ref
    ref = re.search(r'href="/flights/([^"]+)"', client.get("/search/flights/results?" + QS).text)
    assert ref is not None
    r = client.get(f"/flights/{ref.group(1)}/offers")
    assert r.status_code == 200 and "data-modal-root" in r.text
    assert "خرید" in r.text or "ظرفیت تکمیل شد" in r.text
    assert "ترب‌پی" in r.text, "sellers-list trust signals render in the modal"
    assert r.text.count('src="/static/logos/agencies/') >= 2, "agency logo tiles"

    # PDP still renders server-side for direct visits / no-JS
    r = client.get(f"/flights/{ref.group(1)}")
    assert r.status_code == 200 and "کف قیمت" in r.text
    assert "سال سابقه" in r.text and "ترب‌پی" in r.text, "enriched PDP seller rows"
    assert '/static/logos/agencies/' in r.text, "PDP seller rows carry brand marks"
    check(True, "routes: skeleton → shell → pulse → modal → PDP")


def test_logos_bundled() -> None:
    from src.constants import AGENCIES, AIRLINES
    root = Path(__file__).resolve().parents[2]
    referenced = [a.logo for a in AGENCIES if a.logo] + [
        info["logo"] for info in AIRLINES.values() if info.get("logo")
    ]
    assert referenced, "catalogs reference bundled brand marks"
    for path in referenced:
        assert (root / path.lstrip("/")).is_file(), f"missing logo asset: {path}"


def test_content_pages() -> None:
    # Footer destinations render in the same shell, each with its own body copy
    for slug, needle in (
        ("about", "موتور جستجو و مقایسه قیمت بلیت هواپیما از خانوادهٔ ترب"),
        ("faq", "بلیت چارتری و سیستمی چه تفاوتی دارند؟"),
        ("terms", "بار مجاز و حضور در فرودگاه"),
    ):
        r = client.get(f"/pages/{slug}")
        assert r.status_code == 200 and needle in r.text, slug
    assert client.get("/pages/nope").status_code == 404, "unknown page slug 404s"
    # the footer links the pages from every shell render
    assert 'href="/pages/faq"' in client.get("/").text
    check(True, "content pages: about / faq / terms + footer wiring")


def test_results_state_machine() -> None:
    original_read, original_live = readmodel.read_quotes, app_state._LIVE_READY
    app_state._LIVE_READY = True
    readmodel.read_quotes = _cold_read
    try:
        # MISS → skeleton again: shows the CURRENT attempt, threads attempt+1
        # into its own next fetch (server-driven polling)
        r = client.get("/search/flights/results?" + QS + "&attempt=3")
        assert f"تلاش ۳ از {MAX_POLL_FA}" in r.text and "attempt=4" in r.text
        # budget exhausted → honest collection-timeout error (≠ "no flights")
        r = client.get(f"/search/flights/results?" + QS + f"&attempt={MAX_POLL_ATTEMPTS}")
        assert "بیش از حد طول کشید" in r.text
    finally:
        readmodel.read_quotes, app_state._LIVE_READY = original_read, original_live
    check(True, "results liveness state machine")
