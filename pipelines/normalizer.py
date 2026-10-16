"""Normalizer service — raw_flight_prices → flight-prices.canonical (+ tombstones).

Spec §3: the ONLY place supplier-specific → canonical transformation happens.
    * dispatches per-supplier parsers via the Strategy registry (header `supplier`)
    * Jalali stays a URL/frontend concern; bodies are Gregorian ISO (dossier §2.3)
    * naive local times → Asia/Tehran +03:30 → UTC (common.chrono)
    * Rial normalization with unit_verified flag (Alibaba pending §9 assertion)
    * proposal_key (supplier fare identity) + physical_flight_key (cross-supplier)
Tombstones (spec §4): a COMPLETE scan defines the live set per route-day; any
proposal present in the previous complete scan but absent now emits a tombstone
event to flight-prices.deltas so liveness can purge it from Redis instantly.

At-least-once: canonical + tombstone + DLQ produces are flushed BEFORE offsets
commit (BatchConsumer contract). Redelivered raw scans re-produce canonical
events — harmless, the sink upserts on (proposal_key, captured_at).

In-memory previous-scan state: single-instance MVP assumption (documented);
scale-out would move it to Redis keyed by route-day.

Run: python -m pipelines.normalizer [--idle-exit-seconds S] [--max-messages N]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import OrderedDict
from typing import Any
from collections.abc import Mapping, Sequence

from confluent_kafka import Message

from .common.config import KafkaSettings, env_float, env_int, env_str, kafka_client_conf, setup_logging
from .common.dto import message_key
from .common.errors import KafkaPipelineError, SchemaDriftError
from .common.kafkaio import BatchConsumer, ResilientProducer, ensure_topic, produce_dead_letter
from .common.lrucache import remember_latest
from .suppliers import RawScan, SearchQuery, get_supplier

log = logging.getLogger("torob.normalizer")

_STATE_LIMIT = 1000  # route-days of previous-scan state kept for tombstone diffing


def _decode_headers(message: Message) -> dict[str, str]:
    return {
        key: value.decode("utf-8", "replace")
        for key, value in (message.headers() or [])
    }


def _scan_from_message(message: Message, headers: Mapping[str, str]) -> RawScan:
    """Rebuild the RawScan envelope from a raw-topic message + its headers."""
    return RawScan(
        supplier=headers.get("supplier", ""),
        query=SearchQuery(
            origin=headers.get("origin", ""),
            destination=headers.get("destination", ""),
            departure_date=headers.get("departure_date", ""),
            adults=int(headers.get("adults", "1") or 1),
            children=int(headers.get("children", "0") or 0),
            infants=int(headers.get("infants", "0") or 0),
        ),
        content=message.value() or b"",
        captured_at=headers.get("captured_at", ""),
        scan_id=headers.get("scan_id", ""),
        search_session_id=None,  # parsers re-read it from the body
        complete=headers.get("complete", "true").lower() != "false",
    )


def compute_tombstones(
    previous: Mapping[str, str], current: set[str]
) -> list[str]:
    """Proposals seen last complete scan but gone now → tombstone list."""
    return sorted(set(previous) - current)


class Normalizer:
    def __init__(
        self,
        kafka: KafkaSettings,
        producer: ResilientProducer,
        max_batch: int = 200,
        max_wait: float = 2.0,
        poll_timeout: float = 1.0,
    ) -> None:
        self._kafka = kafka
        self._producer = producer
        self._max_batch = max_batch
        self._max_wait = max_wait
        self._poll_timeout = poll_timeout
        # f"{supplier}:{route_day}" → {proposal_key: captured_at of last complete scan}
        self._last_scan: "OrderedDict[str, dict[str, str]]" = OrderedDict()
        self.canonical_produced = 0
        self.tombstones_produced = 0
        self.dead_lettered = 0

    # -- tombstone state -----------------------------------------------------------

    def _remember_scan(self, route_day: str, proposals: Mapping[str, str]) -> None:
        remember_latest(self._last_scan, route_day, dict(proposals), _STATE_LIMIT)

    # -- batch handler (BatchConsumer calls this BEFORE committing) -------------------

    def handle_batch(self, messages: list[Message]) -> None:
        dead_letters: list[tuple[Message, str]] = []
        pending_scans: list[tuple[RawScan, list[Mapping[str, Any]]]] = []
        before = (self.canonical_produced, self.tombstones_produced, self.dead_lettered)

        for message in messages:
            headers = _decode_headers(message)
            supplier_name = headers.get("supplier", "").strip().lower()
            if not supplier_name:
                dead_letters.append((message, "missing 'supplier' header"))
                continue
            try:
                strategy = get_supplier(supplier_name)
            except KeyError as exc:
                dead_letters.append((message, str(exc)))
                continue
            scan = _scan_from_message(message, headers)
            try:
                parsed = strategy.parse(scan)
            except (SchemaDriftError, ValueError) as exc:
                dead_letters.append((message, f"{supplier_name} parse: {exc}"))
                continue
            pending_scans.append((scan, parsed.records))
            if parsed.skipped:
                log.warning("supplier=%s skipped %d malformed itinerary(s) in %s",
                            supplier_name, parsed.skipped, headers.get("scan_id"))

        scanned_routes: set[str] = set()
        for scan, records in pending_scans:
            route_key = message_key(
                scan.query.origin, scan.query.destination, scan.query.departure_date
            )
            scanned_routes.add(route_key)
            for record in records:
                self._producer.produce(
                    self._kafka.canonical_topic,
                    key=route_key,
                    value=json.dumps(record, ensure_ascii=False).encode("utf-8"),
                )
                self.canonical_produced += 1
            self._emit_tombstones(scan, records, route_key)

        for message, reason in dead_letters:
            produce_dead_letter(self._producer, self._kafka.dlq_topic,
                                message, reason, "flight-normalizer")
            self.dead_lettered += 1

        # everything (canonical + tombstones + dlq) must be delivered before commit
        self._producer.flush_or_raise()
        log.info(
            "batch: scans=%d (%s) canonical=+%d tombstones=+%d dlq=+%d",
            len(pending_scans), ", ".join(sorted(scanned_routes)) or "-",
            self.canonical_produced - before[0],
            self.tombstones_produced - before[1],
            self.dead_lettered - before[2],
        )

    def _emit_tombstones(
        self, scan: RawScan, records: Sequence[Mapping[str, Any]], message_key: str
    ) -> None:
        """Diff vs this supplier's last COMPLETE scan for the route-day (spec §4).

        Proposal keys are supplier-namespaced (Alibaba's uniqueKey vs the §5
        fallback digest), so the diff state carries the supplier too — a
        cross-supplier diff would tombstone every OTHER OTA's live quotes on
        each scan and the cache would ping-pong between suppliers."""
        state_key = f"{scan.supplier}:{message_key}"
        current_keys: set[str] = {str(r["proposal_key"]) for r in records}
        if not scan.complete:
            # partial poll-budget-exhausted body — an absent proposal here means
            # nothing; only complete scans may kill cache entries
            return
        previous = self._last_scan.get(state_key)
        if previous is not None:
            for proposal_key in compute_tombstones(previous, current_keys):
                self._producer.produce(
                    self._kafka.deltas_topic,
                    key=message_key,
                    value=json.dumps(
                        {
                            "type": "tombstone",
                            "proposal_key": proposal_key,
                            "origin": scan.query.origin,
                            "destination": scan.query.destination,
                            "departure_date": scan.query.departure_date,
                            "supplier": scan.supplier,
                            "last_seen_captured_at": previous.get(proposal_key),
                            "scan_id": scan.scan_id,
                            "removed_at": scan.captured_at,
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                )
                self.tombstones_produced += 1
        self._remember_scan(
            state_key, {k: scan.captured_at for k in current_keys}
        )

    # -- entry ---------------------------------------------------------------------

    def run(
        self,
        max_messages: int | None = None,
        idle_exit_seconds: float | None = None,
    ) -> int:
        consumer = BatchConsumer(
            client_conf=kafka_client_conf(self._kafka, "flight-normalizer"),
            group_id=env_str("NORMALIZER_GROUP_ID", "flight-normalizer"),
            topics=[self._kafka.raw_topic],
            poll_timeout=self._poll_timeout,
            max_batch=self._max_batch,
            max_wait=self._max_wait,
        )
        code = consumer.run(self.handle_batch, max_messages, idle_exit_seconds)
        log.info(
            "normalizer summary: canonical=%d tombstones=%d dead_lettered=%d",
            self.canonical_produced, self.tombstones_produced, self.dead_lettered,
        )
        return code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--idle-exit-seconds", type=float, default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--ensure-topics", action="store_true", default=True,
                        help="create canonical/deltas/dlq topics if missing (idempotent)")
    args = parser.parse_args(argv)

    kafka = KafkaSettings.from_env()
    setup_logging(kafka.log_level)
    try:
        conf = kafka_client_conf(kafka, "flight-normalizer")
        ensure_topic(conf, kafka.canonical_topic,
                     env_int("CANONICAL_TOPIC_PARTITIONS", 3),
                     env_int("CANONICAL_TOPIC_REPLICATION", 1))
        ensure_topic(conf, kafka.deltas_topic, 3, 1)
        ensure_topic(conf, kafka.dlq_topic, 1, 1)
    except KafkaPipelineError as exc:
        log.error("%s", exc)
        return 1

    producer = ResilientProducer(conf, flush_timeout=env_float("KAFKA_FLUSH_TIMEOUT_SECONDS", 30.0))
    normalizer = Normalizer(kafka, producer)
    return normalizer.run(args.max_messages, args.idle_exit_seconds)


if __name__ == "__main__":
    sys.exit(main())
