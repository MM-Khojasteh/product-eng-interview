"""The cadence scanner — supplier strategies → raw_flight_prices topic (spec §1+§2).

Extracted from raw_worker.py (zero functional changes). For every
(supplier × route) search:
    TokenBucket      per-supplier politeness ceiling (≤6 initiated searches/min, §6)
    CircuitBreaker   403-run → 10 min open · exhausted-429 → cooldown · errors → open
    ResilientProducer publishes the RAW response bytes, untouched, with headers:
        supplier, captured_at, origin, destination, departure_date, adults,
        children, infants, scan_id, complete   (spec §2: raw payload + Supplier/Timestamp)

The entry facade (raw_worker.py) wires this with WorkerSettings (env contract)
and the on-demand consumer (on_demand.py), and owns the CLI.
"""

from __future__ import annotations

import logging
import random
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .common.config import KafkaSettings
from .common.circuit import CircuitBreaker
from .common.dto import message_key
from .common.errors import BlockedError, KafkaPipelineError, RateLimitedError, ScrapingError
from .common.kafkaio import ResilientProducer
from .common.ratelimit import TokenBucket
from .common.spool import DiskSpool
from .suppliers import RawScan, SearchQuery, SupplierStrategy
from .worker_settings import WorkerSettings

# Same logger name as before the split — log output is observable/operational.
log = logging.getLogger("torob.worker")

_MIN_DELAY_SECONDS = 30.0


def scan_headers(scan: RawScan):
    """Kafka headers for a raw scan — supplier + timestamp + full request context
    (spec §2). Kept module-level so tests can pin the worker↔normalizer contract."""
    query = scan.query
    return [
        ("supplier", scan.supplier.encode("utf-8")),
        ("captured_at", scan.captured_at.encode("utf-8")),
        ("origin", query.origin.encode("utf-8")),
        ("destination", query.destination.encode("utf-8")),
        ("departure_date", query.departure_date.encode("utf-8")),
        ("adults", str(query.adults).encode("utf-8")),
        ("children", str(query.children).encode("utf-8")),
        ("infants", str(query.infants).encode("utf-8")),
        ("scan_id", scan.scan_id.encode("utf-8")),
        ("complete", str(scan.complete).encode("utf-8")),
    ]


class RawWorker:
    def __init__(
        self,
        settings: WorkerSettings,
        kafka: KafkaSettings,
        strategies: dict[str, SupplierStrategy],
        buckets: dict[str, TokenBucket],
        breakers: dict[str, CircuitBreaker],
        producer: ResilientProducer,
        spool: DiskSpool | None = None,
        rng: random.Random | None = None,
        sleep=time.sleep,
    ) -> None:
        self._s = settings
        self._kafka = kafka
        self._strategies = strategies
        self._buckets = buckets
        self._breakers = breakers
        self._producer = producer
        self._spool = spool
        self._rng = rng or random.Random()
        self._sleep = sleep
        self._shutdown = False

    def _handle_signal(self, signum: int, frame: Any) -> None:
        log.info("signal %s received — stopping after current search", signum)
        self._shutdown = True

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._handle_signal)
            except (ValueError, OSError):
                pass

    # -- raw publishing (with disk-spool fallback) --------------------------------

    def _publish_raw(self, key: str, value: bytes, headers) -> None:
        self._producer.produce(self._kafka.raw_topic, key=key, value=value,
                               headers=headers)

    def _replay_spool(self) -> None:
        """Re-enqueue scans stranded by a previous broker outage (spec §2)."""
        if self._spool is None or not self._spool.pending():
            return
        log.info("replaying %d spooled scan(s)", self._spool.pending())
        enqueued, remaining = self._spool.replay(self._publish_raw)
        stats = self._producer.flush()
        if stats.failed or stats.pending:
            log.error("spool replay delivery incomplete (failed=%d, pending=%d) — "
                      "keeping files for the next pass", stats.failed, stats.pending)
            return
        self._spool.discard(enqueued)
        log.info("spool replay: enqueued=%d, remaining=%d", len(enqueued), remaining)

    # -- one search -----------------------------------------------------------------

    def _search_once(
        self, name: str, origin: str, destination: str,
        departure_date: str | None = None,
    ) -> dict[str, Any] | None:
        """One (supplier × route-day) search. departure_date defaults to the
        cadence DEPARTURE_DATE; the on-demand path (search.requests) passes the
        user's date through — the fixed-date default must not leak there."""
        strategy = self._strategies[name]
        breaker = self._breakers[name]
        bucket = self._buckets[name]
        if not breaker.allow():
            log.warning("circuit '%s' OPEN — skipping %s→%s (%s)",
                        name, origin, destination, breaker.summary())
            return None
        if not bucket.try_acquire(timeout=15.0):
            log.warning("rate budget for '%s' exhausted — skipping %s→%s this pass",
                        name, origin, destination)
            return None

        query = SearchQuery(
            origin=origin, destination=destination,
            departure_date=departure_date or self._s.departure_date,
            adults=self._s.adults, children=self._s.children,
            infants=self._s.infants, cabin=self._s.cabin,
        )
        try:
            scan = strategy.search(query)
        except BlockedError as exc:
            breaker.record_failure("blocked")
            log.error("BLOCKED %s %s→%s: %s", name, origin, destination, exc)
            return None
        except RateLimitedError as exc:
            breaker.record_failure("rate_limited")
            log.error("RATE-LIMITED %s %s→%s: %s", name, origin, destination, exc)
            return None
        except ScrapingError as exc:
            breaker.record_failure("error")
            log.error("scrape failed %s %s→%s: %s", name, origin, destination, exc)
            return None
        breaker.record_success()  # supplier is healthy — a Kafka outage is NOT its fault

        route_key = message_key(origin, destination, query.departure_date)
        headers = scan_headers(scan)
        try:
            self._publish_raw(route_key, scan.content, headers)
        except KafkaPipelineError as exc:
            if self._spool is None:
                raise  # spooling disabled — surface the failure (old behavior)
            path = self._spool.write(route_key, scan.content, headers)
            log.error("kafka produce failed — scan retained on disk (%s): %s",
                      path, exc)
            return {"scan_id": scan.scan_id, "itineraries": 0, "spooled": True}
        log.info(
            "scan complete: supplier=%s route=%s-%s date=%s itineraries=%d "
            "polls=%s complete=%s scan_id=%s bytes=%d",
            scan.supplier, origin, destination, query.departure_date,
            scan.meta.get("itineraries", 0), scan.meta.get("polls", "-"),
            scan.complete, scan.scan_id, len(scan.content),
        )
        return {"scan_id": scan.scan_id, "itineraries": scan.meta.get("itineraries", 0)}

    def scan_route(self, origin: str, destination: str, departure_date: str) -> None:
        """One on-demand (origin, destination, DATE) scan across suppliers —
        the search.requests path (BFF cache-miss) carries arbitrary user dates,
        unlike the fixed cadence DEPARTURE_DATE. Rides _search_once, so the
        shared per-supplier bucket, breaker, raw publishing and spool all apply
        unchanged — on-demand never bypasses the §6 politeness budget.

        Suppliers scan in PARALLEL (buckets and breakers are per-supplier, so
        cross-OTA concurrency stays inside §6): a hung supplier — FlyToday
        evening ReadTimeouts burned 60–90 s of the BFF poll window before the
        healthy OTA even started, 2026-10-16 — must not starve the user's fill."""
        live = [name for name in self._s.suppliers
                if not self._shutdown and self._breakers[name].allow()]
        if not live:
            return
        if len(live) == 1:
            self._search_once(live[0], origin, destination, departure_date)
            return
        failures: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=len(live),
                                thread_name_prefix="scan-route") as pool:
            futures = {
                name: pool.submit(self._search_once, name, origin,
                                  destination, departure_date)
                for name in live
            }
            for name, future in futures.items():
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001 — siblings must survive one supplier
                    failures.append((name, exc))
        if len(failures) == len(live):
            # every supplier failed (e.g. broker down, spooling disabled) —
            # propagate like the serial loop did, for the executor's catch-all
            raise failures[0][1]
        for name, exc in failures:
            log.error("scan_route: supplier %s failed (siblings completed): %s",
                      name, exc)

    # -- one pass over suppliers × routes ----------------------------------------------

    def run_pass(self) -> dict[str, int]:
        stats = {"searches": 0, "produced": 0, "failed": 0, "spooled": 0, "itineraries": 0}
        self._replay_spool()  # heal any stranded scans from a previous broker outage
        for name in self._s.suppliers:
            if self._shutdown:
                break
            for origin, destination in self._s.routes:
                if self._shutdown or not self._breakers[name].allow():
                    break
                stats["searches"] += 1
                outcome = self._search_once(name, origin, destination)
                if outcome is None:
                    stats["failed"] += 1
                elif outcome.get("spooled"):
                    stats["spooled"] += 1
                else:
                    stats["produced"] += 1
                    stats["itineraries"] += int(outcome.get("itineraries") or 0)
                gap = self._s.search_gap_seconds * (0.5 + self._rng.random())
                self._interruptible_sleep(gap)
        log.info(
            "pass summary: searches=%d produced=%d failed=%d spooled=%d itineraries_total=%d",
            stats["searches"], stats["produced"], stats["failed"],
            stats["spooled"], stats["itineraries"],
        )
        return stats

    # -- loop ------------------------------------------------------------------------------

    def flush(self) -> None:
        """Flush produced scans; raise if anything failed delivery."""
        self._producer.flush_or_raise()

    def _interruptible_sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self._shutdown:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            self._sleep(min(1.0, remaining))

    def run_loop(self) -> int:
        self.install_signal_handlers()
        s = self._s
        log.info(
            "worker loop: suppliers=%s routes=%s date=%s interval=%.0fs jitter=±%.0f%% "
            "budget=%.0f/min/supplier breakers=%s",
            list(s.suppliers), [f"{o}-{d}" for o, d in s.routes], s.departure_date,
            s.interval_seconds, s.jitter_percent * 100, s.searches_per_minute,
            {k: v.summary() for k, v in self._breakers.items()},
        )
        if not s.run_on_start:
            self._interruptible_sleep(
                max(s.interval_seconds * (1 + self._rng.uniform(-s.jitter_percent, s.jitter_percent)),
                    _MIN_DELAY_SECONDS)
            )
        while not self._shutdown:
            self.run_pass()
            self._producer.flush()
            if self._shutdown:
                break
            delay = max(
                s.interval_seconds * (1 + self._rng.uniform(-s.jitter_percent, s.jitter_percent)),
                _MIN_DELAY_SECONDS,
            )
            log.info("next pass in %.0fs", delay)
            self._interruptible_sleep(delay)
        return 0
