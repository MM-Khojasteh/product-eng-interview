"""Kafka scan-request publisher — the BFF's ONLY write to the data plane.

Extracted from main.py §"BFF liveness seam" (zero functional changes).
search.requests is the BFF ↔ raw_worker.OnDemandScans contract; the client
config comes from the shared pipelines KafkaSettings so producer and consumer
can never drift apart.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

from pipelines.common.dto import message_key

from src.readmodel import _rd, jalali_to_greg_iso

log = logging.getLogger(__name__)

producer = None
SEARCH_TOPIC = "search.requests"  # overridden by the shared KafkaSettings below
try:
    from confluent_kafka import Producer

    from pipelines.common.config import KafkaSettings, kafka_client_conf
    _kafka_settings = KafkaSettings.from_env()
    producer = Producer(kafka_client_conf(_kafka_settings, "torob-bff"))
    SEARCH_TOPIC = _kafka_settings.search_requests_topic  # BFF ↔ worker contract
except Exception:  # frontend-only dev environment — cadence scans still fill Redis
    log.warning("Kafka client unavailable — on-demand scans disabled (cadence only)",
                exc_info=True)

# s — one trigger per route-day per window (env-overridable threshold)
_SCAN_REQUEST_MIN_INTERVAL = float(os.getenv("TOROB_SCAN_REQUEST_MIN_INTERVAL", "10"))
_last_scan_request: dict[str, float] = {}

# Fire-and-forget scan triggers in flight (see trigger_scan_later) — kept
# referenced so completed tasks aren't GC'd mid-callback, and drained by
# src.main's lifespan BEFORE the producer flush (the write path closes last).
_scan_triggers: "set[asyncio.Task]" = set()


def request_scan(origin: str, destination: str, datenum: str) -> bool:
    """Cache-miss → publish one on-demand scan request (SEARCH_REQUESTS_TOPIC —
    notes - Ticket/Liveness.md cache-miss scenario; consumed by raw_worker's
    OnDemandScans). Fire-and-forget and rate-limited per route-day; the UI polls
    Redis while the fleet catches up. Sync — call from async routes via
    trigger_scan_later (or asyncio.to_thread) so the event loop never stalls."""
    if producer is None:
        return False
    key = message_key(origin, destination, jalali_to_greg_iso(datenum))
    now = time.monotonic()
    if now - _last_scan_request.get(key, float("-inf")) < _SCAN_REQUEST_MIN_INTERVAL:
        return False
    try:
        producer.produce(
            SEARCH_TOPIC,
            key=f"{origin}-{destination}".encode(),
            value=json.dumps({
                "origin": origin,
                "destination": destination,
                "departure_date": jalali_to_greg_iso(datenum),
                "requested_at": datetime.now(timezone.utc).isoformat(),
            }).encode(),
        )
        # poll(0) dispatches delivery callbacks; NO flush — librdkafka delivers
        # on its background thread and the lifespan flush is the delivery
        # window, so an in-request flush (up to 1 s on an unreachable broker)
        # bought nothing but response latency. Same messages, same semantics.
        producer.poll(0)
    except Exception as exc:  # Kafka down → the cadence scraper still refills Redis
        log.warning("search.requests produce failed: %s", exc, exc_info=True)
        return False
    _last_scan_request[key] = now
    log.info("%s scan REQUEST published → %s (raw_worker OnDemandScans should pick it up)",
             _rd(origin, destination, datenum), SEARCH_TOPIC)
    return True


def trigger_scan_later(origin: str, destination: str, datenum: str) -> None:
    """Fire-and-forget request_scan for async handlers: the response NEVER
    waits on the broker. No caller of request_scan ever consumed its result
    (it is rate-limited fire-and-forget by design) — but four call sites
    awaited it inline, parking the skeleton/modal response behind librdkafka
    for up to the flush timeout. The task self-references keep the loop from
    GC'ing it mid-run; exceptions are logged, never raised into the caller."""
    task = asyncio.create_task(
        asyncio.to_thread(request_scan, origin, destination, datenum))
    _scan_triggers.add(task)
    task.add_done_callback(_scan_triggers.discard)


async def drain_scan_triggers(timeout: float = 2.0) -> None:
    """Lifespan shutdown: give in-flight triggers their produce window before
    the producer flush closes (the write path closes last — src.main)."""
    if not _scan_triggers:
        return
    await asyncio.wait_for(
        asyncio.gather(*_scan_triggers, return_exceptions=True), timeout=timeout)
