"""DB sink — flight-prices.canonical → PostgreSQL (at-least-once, idempotent).

Upsert identity: (proposal_key, captured_at) — a redelivered canonical event
re-writes the same row; a later re-scan is a NEW observation (price history).
Schema is self-provisioned (idempotent DDL + ALTER … IF NOT EXISTS migrations, so
existing volumes upgrade in place). _SCHEMA_SQL is the single source of truth.

Run: python -m pipelines.kafka_to_db [--idle-exit-seconds S]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from typing import Any
from collections.abc import Sequence

import psycopg2
import psycopg2.extras
from confluent_kafka import Message

from .common.backoff import sleep_backoff
from .common.config import KafkaSettings, env_int, env_str, kafka_client_conf, setup_logging
from .common.chrono import TEHRAN_TZ, UTC, to_aware
from .common.coerce import int_or_none, str_or_none
from .common.errors import KafkaPipelineError, StorageError
from .common.kafkaio import BatchConsumer, ResilientProducer, ensure_topic, produce_dead_letter

log = logging.getLogger("torob.sink")

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS flytoday_flights (
    proposal_key           TEXT        NOT NULL,
    captured_at            TIMESTAMPTZ NOT NULL,
    supplier               TEXT        NOT NULL DEFAULT 'flytoday',
    scan_id                TEXT,
    search_session_id      TEXT,
    flight_no              TEXT,
    airline_code           TEXT,
    airline_name           TEXT,
    origin                 TEXT,
    destination            TEXT,
    dep_utc                TIMESTAMPTZ,
    arr_utc                TIMESTAMPTZ,
    cabin_class_code       TEXT,
    stops                  INTEGER,
    journey_duration_minutes INTEGER,
    is_charter             BOOLEAN,
    capacity_remaining     INTEGER,
    price_total_rial       BIGINT,
    currency_source        TEXT,
    refund_method          TEXT,
    fare_source_code       TEXT,
    baggage                TEXT,
    segments               JSONB,
    raw_event              JSONB,
    PRIMARY KEY (proposal_key, captured_at)
);
-- v2 canonical columns (added by the multi-supplier normalizer; ALTER keeps
-- pre-existing volumes on the old table shape upgradable in place)
ALTER TABLE flytoday_flights ADD COLUMN IF NOT EXISTS physical_flight_key TEXT;
ALTER TABLE flytoday_flights ADD COLUMN IF NOT EXISTS unit_verified BOOLEAN;
ALTER TABLE flytoday_flights ADD COLUMN IF NOT EXISTS origin_name TEXT;
ALTER TABLE flytoday_flights ADD COLUMN IF NOT EXISTS destination_name TEXT;
CREATE INDEX IF NOT EXISTS idx_flytoday_flights_route
    ON flytoday_flights (origin, destination, dep_utc);
CREATE INDEX IF NOT EXISTS idx_flytoday_flights_captured
    ON flytoday_flights (captured_at);
-- Recency/history composites for the analytics surface (route dashboards;
-- PDP price-history rollups). idx_flights_physical_history supersedes the old
-- single-column physical index (prefix-covered) — dropped once the composite
-- exists. NB: plain CREATE INDEX (not CONCURRENTLY) — this DDL runs in the
-- sink's startup transaction on a cold/volume DB; run the CONCURRENTLY
-- variants by hand on a LIVE database instead.
CREATE INDEX IF NOT EXISTS idx_flights_route_recency
    ON flytoday_flights (origin, destination, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_flights_physical_history
    ON flytoday_flights (physical_flight_key, captured_at DESC);
DROP INDEX IF EXISTS idx_flytoday_flights_physical;
"""

_COLUMNS: tuple[str, ...] = (
    "proposal_key", "captured_at", "supplier", "scan_id", "search_session_id",
    "flight_no", "airline_code", "airline_name", "origin", "destination",
    "origin_name", "destination_name",
    "dep_utc", "arr_utc", "cabin_class_code", "stops", "journey_duration_minutes",
    "is_charter", "capacity_remaining", "price_total_rial", "currency_source",
    "refund_method", "fare_source_code", "baggage",
    "physical_flight_key", "unit_verified", "segments", "raw_event",
)

_UPSERT_SQL = (
    "INSERT INTO flytoday_flights ({columns}) VALUES %s "
    "ON CONFLICT (proposal_key, captured_at) DO UPDATE SET {updates}"
).format(
    columns=", ".join(_COLUMNS),
    updates=", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in _COLUMNS
        if column not in ("proposal_key", "captured_at")
    ),
)

_JSON_COLUMNS = frozenset({"segments", "raw_event"})


# ---------------------------------------------------------------------------
# Record validation
# ---------------------------------------------------------------------------


def parse_flight_record(raw_value: bytes | None) -> tuple[dict[str, Any] | None, str | None]:
    """Validate one canonical event → (row, None) or (None, DLQ reason)."""
    if not raw_value:
        return None, "empty payload"
    try:
        record = json.loads(raw_value)
    except (ValueError, UnicodeDecodeError) as exc:
        return None, f"invalid JSON: {exc}"
    if not isinstance(record, dict):
        return None, f"expected JSON object, got {type(record).__name__}"

    proposal_key = record.get("proposal_key")
    if not isinstance(proposal_key, str) or not proposal_key.strip():
        return None, "missing/empty proposal_key"
    if len(proposal_key) > 128:
        return None, "proposal_key longer than 128 chars"

    captured_at = to_aware(record.get("captured_at"), naive_tz=TEHRAN_TZ)
    if captured_at is None:
        return None, "missing/unparseable captured_at (part of the upsert identity)"

    segments = record.get("segments")
    unit_verified = record.get("unit_verified")
    row: dict[str, Any] = {
        "proposal_key": proposal_key.strip(),
        "captured_at": captured_at,
        "supplier": str_or_none(record.get("supplier")) or "flytoday",
        "scan_id": str_or_none(record.get("scan_id")),
        "search_session_id": str_or_none(record.get("search_session_id")),
        "flight_no": str_or_none(record.get("flight_no")),
        "airline_code": str_or_none(record.get("airline_code")),
        "airline_name": str_or_none(record.get("airline_name")),
        "origin": str_or_none(record.get("origin")),
        "destination": str_or_none(record.get("destination")),
        "origin_name": str_or_none(record.get("origin_name")),
        "destination_name": str_or_none(record.get("destination_name")),
        "dep_utc": to_aware(record.get("dep_utc"), naive_tz=UTC),
        "arr_utc": to_aware(record.get("arr_utc"), naive_tz=UTC),
        "cabin_class_code": str_or_none(record.get("cabin_class_code")),
        "stops": int_or_none(record.get("stops")),
        "journey_duration_minutes": int_or_none(record.get("journey_duration_minutes")),
        "is_charter": None if record.get("is_charter") is None else bool(record.get("is_charter")),
        "capacity_remaining": int_or_none(record.get("capacity_remaining")),
        "price_total_rial": int_or_none(record.get("price_total_rial")),
        "currency_source": str_or_none(record.get("currency_source")),
        "refund_method": str_or_none(record.get("refund_method")),
        "fare_source_code": str_or_none(record.get("fare_source_code")),
        "baggage": str_or_none(record.get("baggage")),
        "physical_flight_key": str_or_none(record.get("physical_flight_key")),
        "unit_verified": None if unit_verified is None else bool(unit_verified),
        "segments": segments if isinstance(segments, list) else None,
        "raw_event": record,  # full fidelity for replay / schema-drift forensics
    }
    return row, None


def row_to_args(rows: Sequence[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return [
        tuple(
            psycopg2.extras.Json(row[column]) if column in _JSON_COLUMNS else row[column]
            for column in _COLUMNS
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Postgres sink
# ---------------------------------------------------------------------------


class PostgresSink:
    """Micro-batch upserter with reconnect/backoff on connection loss
    (handles restarts and Neon scale-to-zero — INFRASTRUCTURE.md §2.3)."""

    def __init__(self, dsn: str, connect_attempts: int = 5, write_retries: int = 3) -> None:
        self._dsn = dsn
        self._connect_attempts = connect_attempts
        self._write_retries = write_retries
        self._conn: Any | None = None

    def connect(self) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._connect_attempts + 1):
            try:
                log.info("connecting to postgres (attempt %d/%d)",
                         attempt, self._connect_attempts)
                self._conn = psycopg2.connect(self._dsn, application_name="flight-db-sink")
                log.info("postgres connected")
                return
            except psycopg2.Error as exc:
                last_error = exc
                log.warning("postgres connect failed: %s", exc)
                sleep_backoff(attempt)
        raise StorageError(
            f"cannot connect to postgres after {self._connect_attempts} attempts: {last_error}"
        )

    def ensure_schema(self) -> None:
        assert self._conn is not None
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_SCHEMA_SQL)
            self._conn.commit()
            log.info("schema ensured: flytoday_flights ready")
        except psycopg2.Error as exc:
            self._conn.rollback()
            raise StorageError(f"schema creation failed: {exc}") from exc

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            log.info("postgres connection closed")

    def _ensure_connection(self) -> None:
        if self._conn is None or self._conn.closed:
            self.connect()

    def upsert_batch(self, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        args = row_to_args(rows)
        last_error: Exception | None = None
        for attempt in range(1, self._write_retries + 1):
            try:
                self._ensure_connection()
                assert self._conn is not None
                with self._conn.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, _UPSERT_SQL, args, page_size=500)
                self._conn.commit()
                return len(rows)
            except psycopg2.OperationalError as exc:
                last_error = exc
                log.warning("db write failed (attempt %d/%d, OperationalError): %s",
                            attempt, self._write_retries, exc)
                self.close()
                sleep_backoff(attempt)
            except psycopg2.Error as exc:
                if self._conn is not None and not self._conn.closed:
                    self._conn.rollback()
                raise StorageError(f"db upsert failed: {exc}") from exc
        raise StorageError(f"db write failed after {self._write_retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Service wiring
# ---------------------------------------------------------------------------


@dataclass
class SinkStats:
    upserted: int = 0
    dead_lettered: int = 0


class DbSinkService:
    def __init__(self, kafka: KafkaSettings, sink: PostgresSink,
                 dlq_producer: ResilientProducer | None = None) -> None:
        self._kafka = kafka
        self._sink = sink
        self._dlq_producer = dlq_producer
        self.stats = SinkStats()

    def handle_batch(self, messages: list[Message]) -> None:
        # (proposal_key, captured_at) → row, insertion-ordered: one scan can
        # emit the SAME proposal_key twice (two booking buckets on one
        # flight+cabin share the §5 digest) while the whole scan shares one
        # captured_at, so the PK would repeat inside a single execute_values
        # page — Postgres refuses ("cannot affect row a second time").
        # Last write wins, mirroring the Redis HSET these records also feed.
        rows: dict[tuple[str, Any], dict[str, Any]] = {}
        dead_letters: list[tuple[Message, str]] = []
        for message in messages:
            row, reason = parse_flight_record(message.value())
            if row is None:
                assert reason is not None
                dead_letters.append((message, reason))
            else:
                rows[(row["proposal_key"], row["captured_at"])] = row

        self._sink.upsert_batch(list(rows.values()))
        for message, reason in dead_letters:
            if self._dlq_producer is None:
                self._dlq_producer = ResilientProducer(
                    kafka_client_conf(self._kafka, "flight-db-sink-dlq")
                )
            produce_dead_letter(self._dlq_producer, self._kafka.dlq_topic,
                                message, reason, "flight-db-sink")
            self.stats.dead_lettered += 1
        if dead_letters and self._dlq_producer is not None:
            self._dlq_producer.flush_or_raise()  # DLQ delivered before offsets commit

        self.stats.upserted += len(rows)
        log.info("batch: consumed=%d upserted=%d dead_lettered=%d",
                 len(messages), len(rows), len(dead_letters))

    def run(self, max_messages: int | None = None,
            idle_exit_seconds: float | None = None) -> int:
        consumer = BatchConsumer(
            client_conf=kafka_client_conf(self._kafka, "flight-db-sink"),
            group_id=env_str("SINK_GROUP_ID", "flight-db-sink"),
            topics=[self._kafka.canonical_topic],
        )
        code = consumer.run(self.handle_batch, max_messages, idle_exit_seconds)
        log.info("sink summary: upserted=%d dead_lettered=%d",
                 self.stats.upserted, self.stats.dead_lettered)
        self._sink.close()
        return code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--idle-exit-seconds", type=float, default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args(argv)

    kafka = KafkaSettings.from_env()
    setup_logging(kafka.log_level)
    log.info("=== flight db-sink starting (topic=%s) ===", kafka.canonical_topic)

    database_url = env_str("DATABASE_URL")
    if not database_url:
        log.error("startup failed: DATABASE_URL is not set (see .env.example)")
        return 1
    sink = PostgresSink(
        dsn=database_url,
        connect_attempts=env_int("DB_CONNECT_ATTEMPTS", 5),
        write_retries=env_int("DB_WRITE_RETRIES", 3),
    )
    try:
        sink.connect()
        sink.ensure_schema()
        ensure_topic(kafka_client_conf(kafka, "flight-db-sink"), kafka.canonical_topic,
                     env_int("CANONICAL_TOPIC_PARTITIONS", 3),
                     env_int("CANONICAL_TOPIC_REPLICATION", 1))
    except (StorageError, KafkaPipelineError) as exc:
        log.error("startup failed: %s", exc)
        return 1

    service = DbSinkService(kafka, sink)
    return service.run(args.max_messages, args.idle_exit_seconds)


if __name__ == "__main__":
    sys.exit(main())
