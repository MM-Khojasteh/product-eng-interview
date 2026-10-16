"""Liveness service — canonical topic + tombstones → Redis hot price cache.

Spec §4 / INFRASTRUCTURE.md §2.6 ("coalesce, then write"):
    key    quote:{origin}-{destination}:{date}   — one HASH per route-day
    field  proposal_key
    value  compact live-quote JSON (identity keys, card-render fields, price,
           supplier, seats, unit_verified, captured_at)
    TTL    LIVENESS_TTL_SECONDS (60–120 s; an expired key MEANS "re-quote" — that
           is the product requirement, price liveness)
Tombstones (flight-prices.deltas, type="tombstone") HDEL the member immediately —
a sold-out bucket disappears from the cache between scans instead of lingering
until TTL expiry. When a hash empties, the key itself is deleted.

Writes go through a redis pipeline (one round trip per batch) — the §2.6 free-tier
command-budget discipline. At-least-once: offsets commit only after Redis writes
succeed; HSET/HDEL are idempotent so redelivery is safe.

Run: python -m pipelines.liveness [--idle-exit-seconds S]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any
from collections.abc import Mapping, Sequence

import redis
from confluent_kafka import Message

from .common.config import (
    KafkaSettings,
    env_float,
    env_int,
    env_str,
    kafka_client_conf,
    setup_logging,
)
from .common.dto import redis_quote_key
from .common.errors import StorageError
from .common.kafkaio import BatchConsumer

log = logging.getLogger("torob.liveness")

try:  # orjson: ~3-10x stdlib on the per-member encode this runs per quote
    import orjson

    def _dumps(record: Mapping[str, Any]) -> str:
        # .decode() keeps the historical str return type; identical JSON
        # semantics to json.dumps(..., ensure_ascii=False) — the BFF parses
        # values, it never compares bytes.
        return orjson.dumps(record).decode()
except ImportError:  # stdlib fallback (frontend-only / pre-upgrade envs)
    def _dumps(record: Mapping[str, Any]) -> str:
        return json.dumps(record, ensure_ascii=False)


def route_day_key(record: Mapping[str, Any]) -> str | None:
    """quote:{origin}-{destination}:{date} — date = the SEARCH date (§2.6).
    Format owned by dto.redis_quote_key (readmodel.py mirrors the read side)."""
    request = record.get("request") or {}
    origin = record.get("origin") or request.get("origin")
    destination = record.get("destination") or request.get("destination")
    date = request.get("departure_date")
    if not (origin and destination and date):
        return None
    return redis_quote_key(origin, destination, date)


def quote_value(record: Mapping[str, Any]) -> str:
    """The minimal per-proposal payload a hot read needs — MUST carry enough for
    the BFF to render a flight card and dedup by physical flight WITHOUT going
    to Postgres (cache-first hot path): identity keys, card-render fields and
    the unit_verified trust flag (ghost-price guard)."""
    return _dumps(
        {
            "supplier": record.get("supplier"),
            "physical_flight_key": record.get("physical_flight_key"),
            "airline_code": record.get("airline_code"),
            "flight_no": record.get("flight_no"),
            "dep_local": record.get("dep_local"),
            "arr_local": record.get("arr_local"),
            "journey_duration_minutes": record.get("journey_duration_minutes"),
            "price_total_rial": record.get("price_total_rial"),
            "unit_verified": record.get("unit_verified"),
            "dep_utc": record.get("dep_utc"),
            "arr_utc": record.get("arr_utc"),
            "capacity_remaining": record.get("capacity_remaining"),
            "is_charter": record.get("is_charter"),
            "baggage": record.get("baggage"),
            "refund_method": record.get("refund_method"),
            "captured_at": record.get("captured_at"),
        },
    )


class LivenessWriter:
    def __init__(self, kafka: KafkaSettings, redis_url: str, ttl_seconds: int,
                 max_batch: int = 200, max_wait: float = 2.0) -> None:
        self._kafka = kafka
        self._ttl = ttl_seconds
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self._max_batch = max_batch
        self._max_wait = max_wait
        self.written = 0
        self.purged = 0

    # -- batch handler -----------------------------------------------------------

    def handle_batch(self, messages: list[Message]) -> None:
        pipe = self._redis.pipeline(transaction=False)
        touched: set = set()
        purge_keys: list[str] = []
        written_before, purged_before = self.written, self.purged

        for message in messages:
            topic = message.topic()
            try:
                payload = json.loads(message.value() or b"")
            except ValueError:
                # cache is derived state — log and skip (upstream DLQ has the truth)
                log.warning("unparseable message on %s[%d]@%d — skipped",
                            topic, message.partition(), message.offset())
                continue

            if topic == self._kafka.canonical_topic and isinstance(payload, dict):
                key = route_day_key(payload)
                if not key or not payload.get("proposal_key"):
                    log.warning("canonical record missing route/proposal fields — skipped")
                    continue
                pipe.hset(key, str(payload["proposal_key"]), quote_value(payload))
                touched.add(key)
                self.written += 1

            elif topic == self._kafka.deltas_topic and payload.get("type") == "tombstone":
                origin, destination = payload.get("origin"), payload.get("destination")
                date = payload.get("departure_date")
                proposal = payload.get("proposal_key")
                if origin and destination and date and proposal:
                    key = redis_quote_key(origin, destination, date)
                    pipe.hdel(key, str(proposal))
                    purge_keys.append(key)
                    self.purged += 1

        if not touched and not purge_keys:
            return
        for key in touched:  # one EXPIRE per touched key — TTL re-arms per scan
            pipe.expire(key, self._ttl)
        try:
            pipe.execute()
        except redis.exceptions.RedisError as exc:
            raise StorageError(f"redis pipeline failed: {exc}") from exc

        if purge_keys:  # empty hash → drop the key entirely — pipelined
            # (HLEN per purge key + DEL per empty key used to be 2N sequential
            # round trips outside the main pipeline; the command count stays
            # identical — §2.6 budget — while the round trips collapse to two.)
            try:
                check = self._redis.pipeline(transaction=False)
                for key in purge_keys:
                    check.hlen(key)
                empties = [k for k, n in zip(purge_keys, check.execute()) if n == 0]
                if empties:
                    drop = self._redis.pipeline(transaction=False)
                    for key in empties:
                        drop.delete(key)
                    drop.execute()
            except redis.exceptions.RedisError as exc:
                raise StorageError(f"redis purge check failed: {exc}") from exc

        if touched or purge_keys:  # route-day tags match the BFF's [O→D DATE] prefix
            log.info("batch: written=+%d purged=+%d across %s",
                     self.written - written_before, self.purged - purged_before,
                     ", ".join(sorted(k.removeprefix("quote:") for k in touched | set(purge_keys))))

    # -- entry -----------------------------------------------------------------------

    def run(self, max_messages: int | None = None,
            idle_exit_seconds: float | None = None) -> int:
        try:
            self._redis.ping()
        except redis.exceptions.RedisError as exc:
            log.error("redis unreachable: %s", exc)
            return 1
        consumer = BatchConsumer(
            client_conf=kafka_client_conf(self._kafka, "flight-liveness"),
            group_id=env_str("LIVENESS_GROUP_ID", "flight-liveness"),
            topics=[self._kafka.canonical_topic, self._kafka.deltas_topic],
            max_batch=self._max_batch,
            max_wait=self._max_wait,
        )
        code = consumer.run(self.handle_batch, max_messages, idle_exit_seconds)
        log.info("liveness summary: written=%d purged=%d", self.written, self.purged)
        return code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--idle-exit-seconds", type=float, default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args(argv)

    kafka = KafkaSettings.from_env()
    setup_logging(kafka.log_level)
    writer = LivenessWriter(
        kafka,
        redis_url=env_str("REDIS_URL", "redis://127.0.0.1:6379/0"),
        ttl_seconds=env_int("LIVENESS_TTL_SECONDS", 120),
        # Snappier cache fill: the user's skeleton polls every 2 s, so a 2 s
        # batch window could add a whole extra poll-cycle of perceived search
        # latency after a scan lands. 0.5 s batches at this traffic level cost
        # nothing (command count per message is unchanged).
        max_wait=env_float("LIVENESS_MAX_WAIT_SECONDS", 0.5),
    )
    return writer.run(args.max_messages, args.idle_exit_seconds)


if __name__ == "__main__":
    sys.exit(main())
