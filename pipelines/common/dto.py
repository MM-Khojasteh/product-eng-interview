"""Canonical flight DTO (dossier §5) — the single shape every supplier maps into.

Two identities per record (spec §3 dedup algorithm):
    proposal_key        supplier-stable FARE identity — Alibaba exposes its own
                        `uniqueKey`; FlyToday gets the §5 fallback hash including
                        cabin. This is the storage/idempotency key.
    physical_flight_key cross-supplier PHYSICAL flight identity — sha256 over
                        (airline-prefix-stripped flight_no, dep_local, origin,
                        destination). Two OTAs selling the same plane produce
                        the same key; different cabins/buckets on one flight
                        share it.
"""

from __future__ import annotations

import hashlib
from typing import Any
from collections.abc import Mapping, Sequence

RECORD_VERSION = 2


def _digest(basis: str) -> str:
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]


def message_key(origin: str, destination: str, departure_date: str) -> str:
    """Kafka message key: '{origin}-{destination}-{date}' — co-partitions one
    route-day's scans/records together. The ONE definition shared by the raw
    producer (raw_scanner), the normalizer, and the BFF's search.requests
    publisher (scanbus) so the topics can never drift apart."""
    return f"{origin}-{destination}-{departure_date}"


def redis_quote_key(origin: str, destination: str, departure_date: str) -> str:
    """Redis route-day hash key: 'quote:{origin}-{destination}:{date}' (§2.6 —
    ONE hash per route-day, TTL'd; liveness.py owns the writes, readmodel.py
    owns the reads)."""
    return f"quote:{origin}-{destination}:{departure_date}"


def physical_flight_key(
    airline_code: str, flight_no: str, dep_local: str, origin: str, destination: str
) -> str:
    """Cross-supplier identity for one physical flight (spec §3 dedup key).

    Airline codes and number formats drift PER OTA on the 2026-10 wire
    (EP/EP614 vs SR/4310, VRH/VRH5840 vs VR/5840), so airline_code — kept on
    the record for display — is NOT part of the digest, and an airline-code
    prefix is stripped from the number. Route + local minute + bare number is
    the domestic identity; with the code in the hash two OTAs selling the
    same plane never merged into one card."""
    number = flight_no.strip().upper()
    prefix = (airline_code or "").strip().upper()
    if prefix and number.startswith(prefix):
        number = number[len(prefix):]
    return _digest(
        "|".join([number, dep_local, origin, destination]).lower()
    )


def fare_proposal_key(
    airline_code: str, flight_no: str, dep_local: str, origin: str,
    destination: str, cabin: str,
) -> str:
    """§5 fallback hash when the supplier exposes no uniqueKey (FlyToday)."""
    return _digest(
        "|".join(
            [airline_code, flight_no, dep_local, origin, destination, cabin]
        ).lower()
    )


def build_record(
    *,
    supplier: str,
    scan_id: str,
    captured_at: str,
    search_session_id: str | None,
    proposal_key: str,
    request: Mapping[str, Any],
    flight_no: str,
    airline_code: str,
    origin: str,
    destination: str,
    dep_local: str | None,
    dep_utc: str | None,
    arr_local: str | None,
    arr_utc: str | None,
    price_total_rial: int | None,
    currency_source: str,
    unit_verified: bool,
    capacity_remaining: int | None,
    segments: Sequence[Mapping[str, Any]],
    airline_name: str | None = None,
    origin_name: str | None = None,
    destination_name: str | None = None,
    cabin_class_code: str | None = None,
    stops: int | None = None,
    journey_duration_minutes: int | None = None,
    is_charter: bool | None = None,
    refund_method: str | None = None,
    fare_source_code: str | None = None,
    baggage: str | None = None,
) -> dict[str, Any]:
    """Assemble one canonical record — the ONLY place the shape is defined, so
    suppliers and the sink can never drift apart."""
    dep = dep_local or ""
    return {
        "record_version": RECORD_VERSION,
        "supplier": supplier,
        "scan_id": scan_id,
        "captured_at": captured_at,
        "search_session_id": search_session_id,
        "proposal_key": proposal_key,
        "physical_flight_key": physical_flight_key(
            airline_code, flight_no, dep, origin, destination
        ),
        "request": dict(request),
        "flight_no": flight_no,
        "airline_code": airline_code,
        "airline_name": airline_name,
        "origin": origin,
        "destination": destination,
        "origin_name": origin_name,
        "destination_name": destination_name,
        "dep_local": dep_local,
        "dep_utc": dep_utc,
        "arr_local": arr_local,
        "arr_utc": arr_utc,
        "cabin_class_code": cabin_class_code,
        "stops": stops,
        "journey_duration_minutes": journey_duration_minutes,
        "is_charter": is_charter,
        "capacity_remaining": capacity_remaining,  # per fare bucket, NOT aircraft seats
        "price_total_rial": price_total_rial,
        "currency_source": currency_source,
        "unit_verified": unit_verified,  # False until §9 capture magnitude-assert (Alibaba)
        "refund_method": refund_method,
        "fare_source_code": fare_source_code,
        "baggage": baggage,
        "segments": list(segments),
    }
