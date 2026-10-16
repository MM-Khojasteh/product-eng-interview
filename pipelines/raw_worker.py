"""Raw ingestion worker — supplier strategies → raw_flight_prices topic (spec §1+§2).

For every (supplier × route) search:
    TokenBucket      per-supplier politeness ceiling (≤6 initiated searches/min, §6)
    CircuitBreaker   403-run → 10 min open · exhausted-429 → cooldown · errors → open
    ResilientProducer publishes the RAW response bytes, untouched, with headers:
        supplier, captured_at, origin, destination, departure_date, adults,
        children, infants, scan_id, complete   (spec §2: raw payload + Supplier/Timestamp)

Modes
    loop (default) — cadence SCRAPE_INTERVAL_SECONDS ± jitter (absorbs the old
                     scheduler; sequential passes can never exceed the budget)
                     plus a search.requests consumer thread: BFF cache-misses
                     (notes - Ticket/Liveness.md) trigger immediate scans of arbitrary
                     user route-days at the SAME politeness budget (buckets and
                     breakers are shared between both paths).
    --once         — single cadence pass (smoke test); on-demand scans need loop mode

Exit codes (--once): 0 ok · 1 kafka fatal · 2 every search failed.
Run: python -m pipelines.raw_worker [--once]   (from the repo root)

Module layout (one concern per module; this file is the entry facade):
    worker_settings.py  WorkerSettings.from_env + _supplier_kwargs (env contract)
    raw_scanner.py      _MIN_DELAY_SECONDS, scan_headers, RawWorker (cadence loop)
    on_demand.py        parse_search_request, OnDemandScans, run_on_demand
    raw_worker.py       THIS module — re-exports the above + build_worker + CLI
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
from collections.abc import Sequence

from .common.config import KafkaSettings, env_bool, kafka_client_conf, setup_logging
from .common.circuit import CircuitBreaker
from .common.errors import KafkaPipelineError, PipelineError
from .common.kafkaio import ResilientProducer, ensure_topic
from .common.ratelimit import TokenBucket
from .common.spool import DiskSpool
from .on_demand import OnDemandScans, parse_search_request, run_on_demand  # noqa: F401  (facade re-export)
from .raw_scanner import _MIN_DELAY_SECONDS, RawWorker, scan_headers  # noqa: F401  (facade re-export)
from .suppliers import get_supplier
from .suppliers.base import HttpOptions
from .worker_settings import WorkerSettings, _supplier_kwargs

__all__ = [
    "WorkerSettings", "_supplier_kwargs",
    "RawWorker", "scan_headers", "_MIN_DELAY_SECONDS",
    "OnDemandScans", "parse_search_request", "run_on_demand",
    "build_worker", "main",
]

log = logging.getLogger("torob.worker")


def build_worker(settings: WorkerSettings, kafka: KafkaSettings) -> RawWorker:
    http = HttpOptions(
        timeout_seconds=settings.http_timeout,
        max_retries=settings.max_retries,
        backoff_base_seconds=settings.backoff_base,
        backoff_max_seconds=settings.backoff_max,
    )
    strategies = {
        name: get_supplier(name, http=http, **_supplier_kwargs(name))
        for name in settings.suppliers
    }
    buckets = {
        name: TokenBucket(
            name, capacity=settings.bucket_capacity,
            refill_per_minute=settings.searches_per_minute,
        )
        for name in settings.suppliers
    }
    breakers = {
        name: CircuitBreaker(
            name, blocked_threshold=settings.blocked_threshold,
            error_threshold=settings.error_threshold,
        )
        for name in settings.suppliers
    }
    producer = ResilientProducer(
        kafka_client_conf(kafka, "raw-worker"), flush_timeout=settings.flush_timeout
    )
    spool = DiskSpool(settings.spool_dir) if settings.spool_enabled else None
    return RawWorker(settings, kafka, strategies, buckets, breakers, producer, spool=spool)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="single pass, then exit")
    args = parser.parse_args(argv)

    kafka = KafkaSettings.from_env()
    settings = WorkerSettings.from_env()
    setup_logging(kafka.log_level)
    log.info("=== raw worker starting (suppliers=%s routes=%s) ===",
             list(settings.suppliers), [f"{o}-{d}" for o, d in settings.routes])

    try:
        ensure_topic(
            kafka_client_conf(kafka, "raw-worker"),
            kafka.raw_topic, settings.topic_partitions, settings.topic_replication,
        )
    except KafkaPipelineError as exc:
        log.error("%s", exc)
        return 1

    worker = build_worker(settings, kafka)
    if env_bool("ON_DEMAND_SCANS", True):
        def _start_on_demand() -> None:
            try:
                run_on_demand(worker, kafka)
            except KafkaPipelineError as exc:
                # broker/topic unreachable — cadence keeps running; on-demand
                # misses then simply wait for the next cadence pass (old behavior)
                log.error("on-demand scanner disabled: %s", exc)

        threading.Thread(target=_start_on_demand, name="on-demand-scans",
                         daemon=True).start()
    try:
        if args.once:
            stats = worker.run_pass()
            worker.flush()
            if stats["spooled"]:
                log.error("%d scan(s) retained in the local spool (broker unavailable) "
                          "— they will replay on the next pass", stats["spooled"])
                return 1
            if stats["searches"] > 0 and stats["produced"] == 0:
                log.error("every search failed this pass")
                return 2
            return 0
        return worker.run_loop()
    except KafkaPipelineError as exc:
        log.error("kafka fatal: %s", exc)
        return 1
    except PipelineError as exc:
        log.error("pipeline fatal: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
