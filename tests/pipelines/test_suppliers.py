"""Offline tests for pipelines/suppliers: FlyToday wire contract + envelope
drift, supplier error classification (403 vs 429), Alibaba polling session
model + gates/fallback.

Split from tests/test_offline.py (§3.1 of docs/ARCHITECTURE_REVIEW.md) — a pure
move; test bodies are byte-for-byte the originals. Stubs/fakes come from
tests/pipelines/_stubs.py, which must be imported before any `pipelines.*` import.
"""

import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports
from tests.pipelines._stubs import (
    ALIBABA_ITEM, FLYTODAY_BODY, QUERY, FakeResponse, FakeSession, check,
)

import json

from pipelines.common import dto
from pipelines.suppliers import (
    AlibabaSupplier,
    FlyTodaySupplier,
    RawScan,
    SearchQuery,
)
from pipelines.suppliers.base import HttpOptions
from pipelines.common.errors import (
    BlockedError, RateLimitedError, SchemaDriftError,
)


# ---------------------------------------------------------------------------
# 4. FlyToday supplier — headers, payload, session shape, parse
# ---------------------------------------------------------------------------

def test_flytoday_supplier() -> None:
    token_resp = FakeResponse(json_body={"token": "a" * 64})
    search_resp = FakeResponse(json_body=dict(FLYTODAY_BODY), headers={"x-token": "fresh"})
    session = FakeSession([token_resp, search_resp])
    # fixture paths — the production endpoints are env config (see
    # worker_settings._supplier_kwargs), never literals in code or tests
    supplier = FlyTodaySupplier(session=session, http=HttpOptions(max_retries=1),
                                search_path="/api/test/flight/search",
                                token_path="/api/test/token")

    scan = supplier.search(QUERY)
    assert len(session.calls) == 2, "token GET + search POST"
    method, url, kwargs = session.calls[1]
    assert method == "POST" and url.endswith("/api/test/flight/search")
    headers = kwargs["headers"]
    # exact captured set (§3.4): token from step 1 is echoed on the search call
    assert headers["x-token"] == "a" * 64
    assert headers["x-app"] == "www.flytoday.ir" and headers["x-currency"] == "IRR"
    assert headers["x-proxy-origin"] == "" and headers["sec-fetch-site"] == "same-origin"
    assert "departure=thr,1&arrival=mhd,1" in headers["referer"]
    # byte-faithful payload (§3.3)
    assert kwargs["json"] == {
        "pricingSourceType": 0, "adultCount": 1, "childCount": 0, "infantCount": 0,
        "travelPreference": {"cabinType": "Y", "maxStopsQuantity": "All", "airTripType": "OneWay"},
        "originDestinationInformations": [{
            "departureDateTime": "2026-11-15", "destinationLocationCode": "MHD",
            "destinationType": "City", "originLocationCode": "THR", "originType": "City"}],
        "isJalali": True,
    }
    # raw passthrough + echo update
    assert scan.content == search_resp.content
    assert supplier._token == "fresh"  # echo mechanism retained for next calls
    assert scan.meta["itineraries"] == 1 and scan.search_session_id == "122543300"
    assert scan.complete and scan.supplier == "flytoday"

    parsed = supplier.parse(scan)
    assert len(parsed.records) == 1
    record = parsed.records[0]
    assert record["flight_no"] == "B99702" and record["airline_name"] == "Iran Airtour"
    assert record["dep_local"] == "2026-11-15T19:30:00+03:30"
    assert record["dep_utc"] == "2026-11-15T16:00:00+00:00"
    assert record["price_total_rial"] == 29_925_000 and record["unit_verified"] is True
    assert record["capacity_remaining"] == 7 and len(record["proposal_key"]) == 16
    assert len(record["physical_flight_key"]) == 16
    assert record["segments"][0]["origin_name"] == "Mehrabad"
    assert record["request"]["origin"] == "THR"
    check(True, "flytoday supplier wire contract")


def test_flytoday_envelope_and_drift() -> None:
    supplier = FlyTodaySupplier()
    from pipelines.suppliers.base import RawScan
    nested = RawScan(supplier="flytoday", query=QUERY,
                     content=json.dumps({"search": dict(FLYTODAY_BODY)}).encode(),
                     captured_at="2026-10-02T00:00:00+00:00", scan_id="s1")
    assert len(supplier.parse(nested).records) == 1, "nested `search` envelope"

    from pipelines.suppliers.base import RawScan as RS
    drift = RS(supplier="flytoday", query=QUERY,
               content=json.dumps({"unexpected": 1}).encode(),
               captured_at="2026-10-02T00:00:00+00:00", scan_id="s2")
    try:
        supplier.parse(drift)
        raise AssertionError("expected SchemaDriftError")
    except SchemaDriftError:
        pass

    garbage = RS(supplier="flytoday", query=QUERY, content=b"not json{",
                 captured_at="2026-10-02T00:00:00+00:00", scan_id="s3")
    try:
        supplier.parse(garbage)
        raise AssertionError("expected SchemaDriftError for non-JSON")
    except SchemaDriftError:
        pass
    check(True, "flytoday envelope + drift")


def test_supplier_error_classification() -> None:
    supplier = FlyTodaySupplier(session=FakeSession([FakeResponse(status_code=403)]),
                                http=HttpOptions(max_retries=1))
    try:
        supplier._request("GET", "http://fake")
        raise AssertionError("expected BlockedError on 403")
    except BlockedError:
        pass
    session = FakeSession([FakeResponse(status_code=429), FakeResponse(status_code=429),
                           FakeResponse(status_code=429)])
    supplier2 = FlyTodaySupplier(session=session, http=HttpOptions(max_retries=3))
    try:
        supplier2._request("GET", "http://fake")
        raise AssertionError("expected RateLimitedError after exhausted 429s")
    except RateLimitedError:
        pass
    check(True, "error classification (403 vs 429)")


# ---------------------------------------------------------------------------
# 5. Alibaba supplier — polling session model
# ---------------------------------------------------------------------------

def test_alibaba_polling() -> None:
    post_body = {"result": {"requestId": "req-1", "isCompleted": False,
                            "tryLogin": False, "departing": [ALIBABA_ITEM]}}
    poll_body = {"result": {"requestId": "req-1", "isCompleted": True,
                            "departing": [ALIBABA_ITEM, dict(ALIBABA_ITEM, uniqueKey="uq-2")]}}
    sleeps: list = []
    session = FakeSession([
        FakeResponse(json_body=post_body),
        FakeResponse(json_body=poll_body, headers={}),  # no overseas header
    ])
    supplier = AlibabaSupplier(session=session, http=HttpOptions(max_retries=1),
                               sleep=sleeps.append,
                               search_path="/api/test/domestic/available",
                               ab_channel="test-ab-channel")
    scan = supplier.search(QUERY)

    method, url, kwargs = session.calls[0]
    assert method == "POST" and url.endswith("/api/test/domestic/available")
    assert kwargs["json"] == {"origin": "THR", "destination": "MHD",
                              "departureDate": "2026-11-15",
                              "adult": 1, "child": 0, "infant": 0}, "flat body (§2.3)"
    assert kwargs["headers"]["ab-channel"] == "test-ab-channel"
    assert kwargs["headers"]["Origin"] == "https://www.alibaba.ir"

    poll_method, poll_url, _ = session.calls[1]
    assert poll_method == "GET" and poll_url.endswith("/api/test/domestic/available/req-1")
    assert sleeps == [2.0], "poll cadence must be exactly 2000 ms (§2.7)"
    assert scan.complete and scan.search_session_id == "req-1"
    assert scan.content == FakeResponse(json_body=poll_body).content, "raw = FINAL poll body"
    assert scan.meta == {"itineraries": 2, "polls": 1}

    parsed = supplier.parse(scan)
    assert len(parsed.records) == 2
    record = parsed.records[0]
    assert record["proposal_key"] == "alibaba:uq-123"          # §5: prefer uniqueKey
    assert record["airline_code"] == "IR" and record["flight_no"] == "IR572"
    assert record["origin"] == "THR" and record["destination"] == "MHD"  # from query
    assert record["dep_local"] == "2026-11-15T07:15:00+03:30"
    assert record["capacity_remaining"] == 9 and record["price_total_rial"] == 18_500_000
    assert record["unit_verified"] is True   # §9 assert done 2026-10-16 (rial, cross-checked vs FlyToday)
    assert record["physical_flight_key"] == dto.physical_flight_key(
        "IR", "IR572", "2026-11-15T07:15:00+03:30", "THR", "MHD")
    check(True, "alibaba polling session model")


def test_alibaba_per_passenger_pricing() -> None:
    # 2026-10 wire drift: domestic departing items carry NO totalPrice — only
    # priceAdult/priceChild/priceInfant (rial). The fare total is the
    # passenger-weighted sum; a legacy totalPrice, when present, still wins.
    supplier = AlibabaSupplier(session=FakeSession([]), http=HttpOptions(max_retries=1),
                               sleep=lambda s: None)
    query = SearchQuery(origin="THR", destination="KIH", departure_date="2026-10-17",
                        adults=2, children=1, infants=1)
    item = {k: v for k, v in ALIBABA_ITEM.items() if k != "totalPrice"}
    item.update(priceAdult=10_000_000, priceChild=6_000_000, priceInfant=1_000_000,
                isCharter=True)
    scan = RawScan(supplier="alibaba", query=query, content=json.dumps(
        {"requestId": 1, "isCompleted": True, "departing": [item]}).encode(),
        captured_at="2026-10-16T16:00:00+00:00", scan_id="s", complete=True)

    record = supplier.parse(scan).records[0]
    assert record["price_total_rial"] == 2 * 10_000_000 + 6_000_000 + 1_000_000
    assert record["is_charter"] is True
    check(True, "alibaba per-passenger pricing (2026-10 drift)")


def test_alibaba_gates_and_fallback() -> None:
    # overseas-IP gate → BlockedError (§2.5)
    supplier = AlibabaSupplier(
        session=FakeSession([FakeResponse(json_body={"result": {"x": 1}},
                                          headers={"x-overseas-origin": "yes"})]),
        http=HttpOptions(max_retries=1), sleep=lambda s: None)
    try:
        supplier.search(QUERY)
        raise AssertionError("expected BlockedError for overseas gate")
    except BlockedError:
        pass

    # tryLogin wall → BlockedError
    supplier2 = AlibabaSupplier(
        session=FakeSession([FakeResponse(json_body={"result": {"tryLogin": True, "isCompleted": True}})]),
        http=HttpOptions(max_retries=1), sleep=lambda s: None)
    try:
        supplier2.search(QUERY)
        raise AssertionError("expected BlockedError for tryLogin")
    except BlockedError:
        pass

    # missing uniqueKey → §5 fallback hash proposal key
    from pipelines.suppliers.base import RawScan
    body = {"departing": [dict(ALIBABA_ITEM, uniqueKey=None)]}
    scan = RawScan(supplier="alibaba", query=QUERY,
                   content=json.dumps(body).encode(),
                   captured_at="2026-10-02T00:00:00+00:00", scan_id="s")
    record = AlibabaSupplier().parse(scan).records[0]
    assert len(record["proposal_key"]) == 16, "hash fallback when no uniqueKey"
    check(True, "alibaba gates + key fallback")
