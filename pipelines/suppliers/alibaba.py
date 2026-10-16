"""Alibaba.ir supplier — dossier §2 ([A-CODE] own production bundle, [A-GIST] legacy form).

Session model (the OTHER strategy shape, §2.7):
    1. POST <search_path> (flat body) → body carries requestId,
       possibly a first partial batch of results;
    2. poll GET <search_path>/{requestId} every 2000 ms, at most 11 iterations,
       stop when isCompleted === true — results accumulate server-side per session.
Access gates (§2.5): overseas-IP gate (`x-overseas-origin` header / tryLogin:true
forces login) → BlockedError; fetchers must egress from Iranian IP space.
Currency: totalPrice historically RIAL but UNVERIFIED (§2.6 "verify §9") →
unit_verified=False until a §9 capture magnitude-assert pins it.
"""

from __future__ import annotations

import logging
from typing import Any
from collections.abc import Mapping

import requests

from ..common.chrono import localize
from ..common.coerce import int_or_none
from ..common.dto import build_record, fare_proposal_key
from ..common.errors import BlockedError, SchemaDriftError
from .base import (
    CHROME_USER_AGENT,
    HttpOptions,
    ParseResult,
    RawScan,
    SearchQuery,
    SupplierStrategy,
    fresh_scan_stamp,
    parse_items,
    parse_scan_body,
    request_block,
    unwrap_envelope,
)

log = logging.getLogger("torob.supplier.alibaba")

# Defaults only — ALIBABA_POLL_INTERVAL_SECONDS / ALIBABA_MAX_POLLS override
# them per deployment (worker_settings._supplier_kwargs).
DEFAULT_POLL_INTERVAL_SECONDS = 2.0   # SPA polls every 2000 ms (§2.7)
DEFAULT_MAX_POLLS = 11                # capped at 11 iterations (§2.7)

# The ab-channel header VALUE is deployment config (ALIBABA_AB_CHANNEL, wired in
# worker_settings._supplier_kwargs), not public code — same discipline as the
# endpoint paths: a clone without the .env matrix is not a working scraper.


class AlibabaSupplier(SupplierStrategy):
    name = "alibaba"

    def __init__(
        self,
        base_url: str = "https://ws.alibaba.ir",
        search_path: str = "",
        user_agent: str = CHROME_USER_AGENT,
        ab_channel: str = "",
        http: HttpOptions | None = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
        max_polls: int = DEFAULT_MAX_POLLS,
        **kwargs: Any,
    ) -> None:
        super().__init__(http=http, **kwargs)
        self._base_url = base_url.rstrip("/")
        self._search_path = search_path
        self._user_agent = user_agent
        self._ab_channel = ab_channel
        self._poll_interval = poll_interval
        self._max_polls = max_polls

    # -- headers & body (§2.3/§2.4) ------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://www.alibaba.ir",
            "Referer": "https://www.alibaba.ir/",
            "User-Agent": self._user_agent,
            "ab-channel": self._ab_channel,
        }

    @staticmethod
    def _payload(query: SearchQuery) -> dict[str, Any]:
        """Flat body — passenger counts at TOP level, not nested (§2.3 [A-CODE]).
        Round-trip adds returnDate ([A-CODE]) — INFERRED flow, not built here."""
        return {
            "origin": query.origin,          # city domainCode (THR = Mehrabad city)
            "destination": query.destination,
            "departureDate": query.departure_date,  # Gregorian ISO YYYY-MM-DD
            "adult": query.adults,
            "child": query.children,
            "infant": query.infants,
        }

    # -- session-shape behavior: initiate → poll -------------------------------------
    # Legacy GET era nests under `result`; the 2025 client reads root keys off
    # the query-unwrapped body (§2.6) — unwrap_envelope defends both.

    def _check_gates(self, response: requests.Response) -> None:
        """Overseas-IP / login walls (§2.5) — hard blocks, not retryable."""
        if response.headers.get("x-overseas-origin"):
            raise BlockedError(
                "overseas IP gate (x-overseas-origin header, dossier §2.5) — "
                "fetchers must egress from Iranian IP space"
            )

    @staticmethod
    def _check_try_login(body: Mapping[str, Any]) -> None:
        if body.get("tryLogin"):
            raise BlockedError(
                "login wall (tryLogin=true, dossier §2.5) — session is auth-gated"
            )

    def search(self, query: SearchQuery) -> RawScan:
        url = self._base_url + self._search_path
        response = self._request("POST", url, json=self._payload(query),
                                 headers=self._headers())
        self._check_gates(response)
        body = unwrap_envelope(response.json(), "result")
        self._check_try_login(body)

        content = response.content
        request_id = body.get("requestId")
        complete = bool(body.get("isCompleted"))
        polls = 0
        while not complete and polls < self._max_polls:
            if not request_id:
                raise SchemaDriftError("no requestId on incomplete search response")
            self._sleep(self._poll_interval)  # SPA cadence: every 2000 ms (§2.7)
            polls += 1
            poll = self._request(
                "GET", f"{url}/{request_id}", headers=self._headers()
            )
            self._check_gates(poll)
            body = unwrap_envelope(poll.json(), "result")
            self._check_try_login(body)
            content = poll.content  # latest accumulated batch
            complete = bool(body.get("isCompleted"))

        departing = body.get("departing")
        captured_at, scan_id = fresh_scan_stamp()
        return RawScan(
            supplier=self.name,
            query=query,
            content=content,  # final accumulated body — raw bytes on the wire
            captured_at=captured_at,
            scan_id=scan_id,
            search_session_id=str(request_id) if request_id is not None else None,
            complete=complete,  # False = 11-poll budget exhausted, partial body
            meta={
                "itineraries": len(departing) if isinstance(departing, list) else 0,
                "polls": polls,
            },
        )

    # -- canonical mapping (runs in the normalizer) -----------------------------------

    def parse(self, scan: RawScan) -> ParseResult:
        body, departing = parse_scan_body(
            scan, envelope_key="result", items_key="departing"
        )

        result = ParseResult(search_session_id=scan.search_session_id)
        result.records, result.skipped = parse_items(
            departing,
            lambda item: self._record(scan, item),
            what="departing item",
        )
        return result

    @staticmethod
    def _total_price_rial(item: Mapping[str, Any], query: SearchQuery) -> int | None:
        """Fare total for the WHOLE party, rial (unit UNVERIFIED, §2.6).

        Older shapes carried a single totalPrice; the 2026-10 domestic wire
        sends only per-passenger prices (priceAdult/priceChild/priceInfant),
        so the total is the passenger-weighted sum of the request — without
        this every quote rendered price-less and never reached a card."""
        total = int_or_none(item.get("totalPrice"))
        if total is not None:
            return total
        parts = (
            (int_or_none(item.get("priceAdult")), query.adults),
            (int_or_none(item.get("priceChild")), query.children),
            (int_or_none(item.get("priceInfant")), query.infants),
        )
        priced = [fare * pax for fare, pax in parts if fare and pax]
        return sum(priced) if priced else None

    def _record(self, scan: RawScan, item: Mapping[str, Any]) -> dict[str, Any]:
        query = scan.query
        flight_no = str(item.get("flightNumber") or "")
        airline_code = str(item.get("airlineCode") or "").upper()
        dep_local, dep_utc = localize(item.get("leaveDateTime"))    # naive Asia/Tehran
        arr_local, arr_utc = localize(item.get("arrivalDateTime"))
        seat = int_or_none(item.get("seat"))
        unique_key = item.get("uniqueKey")
        # §5: "prefer supplier uniqueKey (alibaba exposes one); else hash(...)"
        proposal_key = (
            f"alibaba:{unique_key}"
            if unique_key
            else fare_proposal_key(
                airline_code, flight_no, dep_local or "",
                query.origin, query.destination, "",
            )
        )
        return build_record(
            supplier=self.name,
            scan_id=scan.scan_id,
            captured_at=scan.captured_at,
            search_session_id=scan.search_session_id,  # requestId = poll handle (§5)
            proposal_key=proposal_key,
            request=request_block(query),
            flight_no=flight_no,
            airline_code=airline_code,
            airline_name=item.get("airlineName"),
            origin=query.origin,          # city domainCodes from the request (§2.3)
            destination=query.destination,
            dep_local=dep_local, dep_utc=dep_utc,
            arr_local=arr_local, arr_utc=arr_utc,
            cabin_class_code=None,        # domestic search sends none (§2.3); client-side filter
            stops=None,
            journey_duration_minutes=None,
            # 2026-10 wire carries isCharter per item (the §2.1 registry-era
            # None would mis-badge charter flights as system)
            is_charter=None if item.get("isCharter") is None else bool(item["isCharter"]),
            capacity_remaining=seat,      # per-fare-bucket hint (§5)
            price_total_rial=self._total_price_rial(item, query),  # unit UNVERIFIED (§2.6)
            currency_source="IRR",
            # §9 magnitude-assert DONE (2026-10-16): per-passenger sums matched
            # FlyToday's verified IRR totals to the rial on three THR-MHD
            # flights (EP614/IS4310/VRH5840) — alibaba speaks rial. Until this
            # flag is True the BFF's ghost-bait guard hides alibaba rows from
            # the Compare modal whenever a verified supplier sells the flight.
            unit_verified=True,
            refund_method=None,
            fare_source_code=None,
            baggage=None,
            segments=[
                {
                    "flight_no": flight_no,
                    "marketing_airline_code": airline_code,
                    "dep_local": dep_local, "dep_utc": dep_utc,
                    "arr_local": arr_local, "arr_utc": arr_utc,
                    "seats_remaining": seat,
                }
            ],
        )
