"""On-demand scans — search.requests → immediate supplier scans.

Extracted from raw_worker.py (zero functional changes). The BFF publishes a
search request on every live cache-miss (notes - Ticket/Liveness.md); the consumer here
turns it into an immediate scan of that user route-day at the SAME politeness
budget as cadence — buckets and breakers are shared via RawWorker.scan_route.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict
from queue import Full, Queue
from typing import Any

from .common.config import KafkaSettings, env_float, env_str, kafka_client_conf
from .common.kafkaio import BatchConsumer, ensure_topic
from .common.lrucache import remember_latest
from .raw_scanner import RawWorker

# Same logger name as before the split — log output is observable/operational.
log = logging.getLogger("torob.worker")


def parse_search_request(payload: bytes | None) -> tuple[str, str, str] | None:
    """search.requests message body → (origin, destination, departure_date), or
    None when malformed. Module-level so tests can pin the BFF↔worker wire
    contract without a broker (mirrors scan_headers for the raw topic)."""
    try:
        request = json.loads(payload or b"")
    except ValueError:
        return None
    if not isinstance(request, dict):
        return None
    origin = str(request.get("origin") or "").strip().upper()
    destination = str(request.get("destination") or "").strip().upper()
    departure_date = str(request.get("departure_date") or "").strip()
    if not (origin and destination and departure_date):
        return None
    return origin, destination, departure_date


class OnDemandScans:
    """search.requests → immediate supplier scans (notes - Ticket/Liveness.md cache-miss).

    The Kafka handler only enqueues (in-memory, deduped per route-day for a
    bounded cooldown); scans run on executor threads because supplier HTTP
    blocks (Alibaba ≤ 11 polls × 2 s, FlyToday ~4 s) and must never stall the
    consumer loop. Scans go through RawWorker.scan_route, so the SHARED
    per-supplier token buckets and circuit breakers cap on-demand at the same
    §6 budget as cadence.

    The dedupe is a COOLDOWN, not a permanent remember: Redis quotes live only
    LIVENESS_TTL_SECONDS (60–120 s), so a route-day must become scannable
    again once its quotes expire — otherwise the second search for a route-day
    can never be served live for the worker's lifetime.
    """

    _SEEN_LIMIT = 500  # route-days remembered for dedupe (LRU-evicted)

    def __init__(
        self,
        worker: RawWorker,
        max_queue: int = 100,
        max_workers: int = 2,
        dedup_seconds: float = 60.0,
    ) -> None:
        self._worker = worker
        self._queue: "Queue[tuple[str, str, str]]" = Queue(maxsize=max_queue)
        self._seen: "OrderedDict[tuple[str, str, str], float]" = OrderedDict()
        self._dedup_seconds = dedup_seconds
        self._threads = [
            threading.Thread(target=self._drain, name=f"on-demand-{i}", daemon=True)
            for i in range(max_workers)
        ]

    def enqueue(self, origin: str, destination: str, departure_date: str) -> bool:
        """Queue one route-day scan; False when inside the dedup cooldown or
        the queue is full. Re-requests refresh the cooldown only when they
        result in an enqueue, so the window is measured from the last SCAN,
        not from the last suppressed request."""
        route_day = (origin, destination, departure_date)
        now = time.monotonic()
        seen_at = self._seen.get(route_day)
        if seen_at is not None and now - seen_at < self._dedup_seconds:
            log.debug("on-demand scan for %s→%s %s deduped (%.0fs into %.0fs cooldown)",
                      origin, destination, departure_date, now - seen_at,
                      self._dedup_seconds)
            return False
        remember_latest(self._seen, route_day, now, self._SEEN_LIMIT)
        try:
            self._queue.put_nowait(route_day)
        except Full:
            self._seen.pop(route_day, None)
            log.warning("on-demand scan queue full — dropping %s", route_day)
            return False
        return True

    def start(self) -> None:
        for thread in self._threads:
            thread.start()

    def _drain(self) -> None:
        while True:
            origin, destination, departure_date = self._queue.get()
            log.info("on-demand scan: %s→%s %s", origin, destination, departure_date)
            try:
                self._worker.scan_route(origin, destination, departure_date)
            except Exception as exc:  # noqa: BLE001 — executor threads must never die
                log.exception("on-demand scan failed %s→%s %s: %s",
                              origin, destination, departure_date, exc)


def run_on_demand(worker: RawWorker, kafka: KafkaSettings) -> int:
    """Consume search.requests → OnDemandScans, beside the cadence loop.

    offset_reset=latest: requests published before this process started are
    stale (the BFF re-requests on every cache miss anyway) — replaying topic
    history on first boot would burn the politeness budget for nothing.
    """
    ensure_topic(
        kafka_client_conf(kafka, "raw-worker-ondemand"),
        kafka.search_requests_topic, 3, 1,
    )
    scans = OnDemandScans(
        worker, dedup_seconds=env_float("ON_DEMAND_DEDUP_SECONDS", 60.0)
    )
    scans.start()

    def handle_batch(messages: list[Any]) -> None:
        for message in messages:
            route_day = parse_search_request(message.value())
            if route_day is None:
                log.warning("malformed search request on %s[%d]@%d — skipped",
                            message.topic(), message.partition(), message.offset())
                continue
            scans.enqueue(*route_day)

    consumer = BatchConsumer(
        client_conf=kafka_client_conf(kafka, "raw-worker-ondemand"),
        group_id=env_str("ON_DEMAND_GROUP_ID", "raw-worker-ondemand"),
        topics=[kafka.search_requests_topic],
        max_batch=50,
        max_wait=1.0,
        offset_reset="latest",
    )
    return consumer.run(handle_batch)
