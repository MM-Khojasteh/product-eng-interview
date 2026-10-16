"""FlyToday supplier — dossier §3 ([F-PM] captured request, [F-CLONE] captured response).

Session model: ONE blocking POST returns the complete result set (149 itineraries
in the reference capture, 2–4 s budget) — no polling, no pagination (§3.7).
Token: best-effort anonymous session-token fetch + x-token response echo (§3.4).
Currency: IRR confirmed via x-currency header → unit_verified=True.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from collections.abc import Mapping

from ..common.chrono import localize
from ..common.coerce import int_or_none, str_or_none
from ..common.dto import build_record, fare_proposal_key
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
)

log = logging.getLogger("torob.supplier.flytoday")


class FlyTodaySupplier(SupplierStrategy):
    name = "flytoday"

    def __init__(
        self,
        base_url: str = "https://www.flytoday.ir",
        search_path: str = "",
        token_path: str = "",
        user_agent: str = CHROME_USER_AGENT,
        http: HttpOptions | None = None,
        **kwargs: Any,
    ) -> None:
        # search_path/token_path default EMPTY by design: the reverse-engineered
        # endpoints are deployment config (FLYTODAY_SEARCH_PATH/FLYTODAY_TOKEN_PATH,
        # wired in worker_settings._supplier_kwargs), not public code — a clone of
        # this repo without the .env matrix is not a working scraper.
        super().__init__(http=http, **kwargs)
        self._base_url = base_url.rstrip("/")
        self._search_path = search_path
        self._token_path = token_path
        self._user_agent = user_agent
        self._token: str = ""  # anonymous x-token; never logged

    # -- request template --------------------------------------------------------

    def _page_url(self, query: SearchQuery) -> str:
        """Browser deep-link URL — used verbatim as referer/x-path (§3.1/§3.4)."""
        return (
            f"{self._base_url}/flight/search"
            f"?departure={query.origin.lower()},1&arrival={query.destination.lower()},1"
            f"&departureDate={query.departure_date}"
            f"&adt={query.adults}&chd={query.children}&inf={query.infants}"
            f"&cabin=1&isAnyWhere=false"
        )

    def _headers(self, query: SearchQuery) -> dict[str, str]:
        """Exact captured header set (§3.4 [F-PM])."""
        page = self._page_url(query)
        return {
            "accept": "*/*",
            "accept-language": "fa",
            "content-type": "application/json",
            "origin": self._base_url,
            "referer": page,
            "user-agent": self._user_agent,
            "x-app": "www.flytoday.ir",
            "x-currency": "IRR",
            "x-origin": self._base_url,
            "x-path": page,
            "x-proxy-cache": "no-store",
            "x-proxy-origin": "",
            "x-token": self._token,
            # standard sec-fetch-* set observed in the capture (same-origin XHR)
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
        }

    @staticmethod
    def _payload(query: SearchQuery) -> dict[str, Any]:
        """Byte-faithful to the captured Chrome-142 request (§3.3 [F-PM]).
        Round-trip bodies are [INFERRED] in the dossier and deliberately NOT built."""
        return {
            "pricingSourceType": 0,
            "adultCount": query.adults,
            "childCount": query.children,
            "infantCount": query.infants,
            "travelPreference": {
                "cabinType": query.cabin,
                "maxStopsQuantity": "All",
                "airTripType": "OneWay",
            },
            "originDestinationInformations": [
                {
                    "departureDateTime": query.departure_date,  # Gregorian ISO
                    "destinationLocationCode": query.destination,
                    "destinationType": "City",
                    "originLocationCode": query.origin,
                    "originType": "City",
                }
            ],
            "isJalali": True,
        }

    # -- session-shape behavior: single blocking POST ------------------------------

    def _fetch_token(self) -> None:
        try:
            response = self._request(
                "GET", self._base_url + self._token_path, headers={
                    "accept": "*/*",
                    "user-agent": self._user_agent,
                    "x-app": "www.flytoday.ir",
                }
            )
            token = response.json().get("token")
            if token:
                self._token = str(token)
        except Exception as exc:  # noqa: BLE001 — documented as best-effort (§3.4)
            # first request may carry an empty x-token; the echo mechanism recovers
            log.warning("token fetch failed (%s) — continuing with empty x-token", exc)

    def search(self, query: SearchQuery) -> RawScan:
        self._fetch_token()
        url = self._base_url + self._search_path
        response = self._request(
            "POST", url, json=self._payload(query), headers=self._headers(query)
        )
        echoed = response.headers.get("x-token")  # token-echo mechanism (§3.4)
        if echoed:
            self._token = echoed
        session_id, itineraries = self._quick_look(response.content)
        captured_at, scan_id = fresh_scan_stamp()
        return RawScan(
            supplier=self.name,
            query=query,
            content=response.content,  # raw bytes, untouched — this hits the wire
            captured_at=captured_at,
            scan_id=scan_id,
            search_session_id=session_id,
            complete=True,
            meta={"itineraries": itineraries},
        )

    @staticmethod
    def _quick_look(content: bytes) -> tuple[str | None, int]:
        """Cheap metadata for logs/headers only — parsing happens in the normalizer."""
        try:
            body = json.loads(content)
            body = body.get("search") or body if isinstance(body, dict) else {}
            items = body.get("pricedItineraries") or []
            sid = body.get("searchId")
            return (None if sid is None else str(sid)), len(items) if isinstance(items, list) else 0
        except ValueError:
            return None, 0

    # -- canonical mapping (runs in the normalizer) ---------------------------------

    def parse(self, scan: RawScan) -> ParseResult:
        # 2023 captures nest under `search`; 2025 reads the root (§3.6)
        body, itineraries = parse_scan_body(
            scan, envelope_key="search", items_key="pricedItineraries"
        )

        additional: Mapping[str, Any] = body.get("additionalData") or {}

        def _by_iata(items: Any) -> dict[str, Mapping[str, Any]]:
            indexed: dict[str, Mapping[str, Any]] = {}
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, Mapping) and item.get("iata"):
                        indexed[str(item["iata"]).lower()] = item
            return indexed

        airlines = _by_iata(additional.get("airlines"))
        airports = _by_iata(additional.get("airports"))

        result = ParseResult(search_session_id=None if body.get("searchId") is None
                             else str(body.get("searchId")))
        result.records, result.skipped = parse_items(
            itineraries,
            lambda itinerary: self._record(scan, itinerary, airlines, airports),
            what="itinerary",
        )
        return result

    def _record(
        self,
        scan: RawScan,
        itinerary: Mapping[str, Any],
        airlines: Mapping[str, Mapping[str, Any]],
        airports: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        segments_raw: list[Mapping[str, Any]] = []
        for option in itinerary.get("originDestinationOptions") or []:
            if isinstance(option, Mapping):
                for segment in option.get("flightSegments") or []:
                    if isinstance(segment, Mapping):
                        segments_raw.append(segment)
        if not segments_raw:
            raise ValueError("itinerary has no flightSegments")

        first, last = segments_raw[0], segments_raw[-1]
        pricing: Mapping[str, Any] = itinerary.get("airItineraryPricingInfo") or {}
        total_fare: Mapping[str, Any] = pricing.get("itinTotalFare") or {}
        query = scan.query

        origin = str(first.get("departureAirportLocationCode") or "").upper()
        destination = str(last.get("arrivalAirportLocationCode") or "").upper()
        airline_code = str(first.get("marketingAirlineCode") or "").upper()
        flight_no = str(first.get("flightNumber") or "")
        cabin = str(first.get("cabinClassCode") or "")
        dep_local, dep_utc = localize(first.get("departureDateTime"))
        arr_local, arr_utc = localize(last.get("arrivalDateTime"))

        segments = [self._segment(seg, airports) for seg in segments_raw]
        seats = [int(s["seats_remaining"]) for s in segments
                 if isinstance(s.get("seats_remaining"), int)]
        durations = [int(s["duration_minutes"]) for s in segments
                     if isinstance(s.get("duration_minutes"), int)]

        return build_record(
            supplier=self.name,
            scan_id=scan.scan_id,
            captured_at=scan.captured_at,
            search_session_id=scan.search_session_id,
            proposal_key=fare_proposal_key(
                airline_code, flight_no, dep_local or "", origin, destination, cabin
            ),
            request=self._request_block(query),
            flight_no=flight_no,
            airline_code=airline_code,
            airline_name=(airlines.get(airline_code.lower()) or {}).get("name"),
            origin=origin,
            destination=destination,
            origin_name=(airports.get(origin.lower()) or {}).get("name"),
            destination_name=(airports.get(destination.lower()) or {}).get("name"),
            dep_local=dep_local, dep_utc=dep_utc, arr_local=arr_local, arr_utc=arr_utc,
            cabin_class_code=cabin or None,
            stops=len(segments) - 1,
            journey_duration_minutes=sum(durations) if durations else None,
            is_charter=any(bool(s.get("is_charter")) for s in segments_raw),
            capacity_remaining=min(seats) if seats else None,  # bottleneck fare bucket
            price_total_rial=int_or_none(total_fare.get("totalFare")),
            currency_source="IRR",           # confirmed via x-currency [F-PM]
            unit_verified=True,
            refund_method=str_or_none(itinerary.get("refundMethod")),
            fare_source_code=str_or_none(itinerary.get("fareSourceCode")),
            baggage=str_or_none(first.get("baggage")),
            segments=segments,
        )

    def _request_block(self, query: SearchQuery) -> dict[str, Any]:
        return request_block(query, deep_link=self._page_url(query))

    @staticmethod
    def _segment(
        segment: Mapping[str, Any], airports: Mapping[str, Mapping[str, Any]]
    ) -> dict[str, Any]:
        operating: Mapping[str, Any] = segment.get("operatingAirline") or {}
        origin = str(segment.get("departureAirportLocationCode") or "").upper()
        destination = str(segment.get("arrivalAirportLocationCode") or "").upper()
        dep_local, dep_utc = localize(segment.get("departureDateTime"))
        arr_local, arr_utc = localize(segment.get("arrivalDateTime"))
        return {
            "flight_no": segment.get("flightNumber"),
            "marketing_airline_code": segment.get("marketingAirlineCode"),
            "operating_airline_code": operating.get("code"),
            "operating_equipment": operating.get("equipment"),
            "origin": origin,
            "origin_name": (airports.get(origin.lower()) or {}).get("name"),
            "destination": destination,
            "destination_name": (airports.get(destination.lower()) or {}).get("name"),
            "dep_local": dep_local, "dep_utc": dep_utc,
            "arr_local": arr_local, "arr_utc": arr_utc,
            "cabin_class_code": segment.get("cabinClassCode"),
            "booking_class": segment.get("resBookDesigCode"),
            "duration_minutes": int_or_none(segment.get("journeyDurationPerMinute")),
            "stop_quantity": int_or_none(segment.get("stopQuantity")),
            "seats_remaining": int_or_none(segment.get("seatsRemaining")),
            "baggage": str_or_none(segment.get("baggage")),
            "is_charter": bool(segment.get("isCharter")),
            "is_return": bool(segment.get("isReturn")),
        }
