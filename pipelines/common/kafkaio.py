"""Kafka transport layer — the ONLY module that talks confluent-kafka.

Spec's separation of concerns: the Kafka layer is purely transport.
    ResilientProducer   idempotent producer, BufferError drain, delivery accounting
    ensure_topic        declarative topic provisioning (auto-create is disabled)
    BatchConsumer       generic at-least-once micro-batch loop: handler(batch)
                        runs FIRST, offsets commit only after it returns — shared
                        verbatim by the normalizer, db-sink and liveness services.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable, Mapping, Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .backoff import sleep_backoff
from .errors import KafkaPipelineError

log = logging.getLogger("torob.kafka")

Handler = Callable[[list[Message]], None]


@dataclass
class DeliveryStats:
    delivered: int = 0
    failed: int = 0
    pending: int = 0


def ensure_topic(
    client_conf: Mapping[str, Any],
    topic: str,
    partitions: int,
    replication_factor: int,
    connect_attempts: int = 6,
    connect_timeout: float = 10.0,
) -> None:
    """Create `topic` if missing — the stack runs with auto-create disabled and
    the broker needs ~20-40 s to report healthy (INFRASTRUCTURE.md §1.4)."""
    admin = AdminClient(dict(client_conf))
    metadata = None
    for attempt in range(1, connect_attempts + 1):
        try:
            log.info("connecting to kafka at %s (attempt %d/%d) ...",
                     client_conf.get("bootstrap.servers"), attempt, connect_attempts)
            metadata = admin.list_topics(timeout=connect_timeout)
            break
        except KafkaException as exc:
            log.warning("kafka not reachable yet: %s", exc)
            if attempt == connect_attempts:
                raise KafkaPipelineError(
                    f"cannot reach kafka at {client_conf.get('bootstrap.servers')} "
                    f"after {connect_attempts} attempts — is `docker compose up -d` healthy?"
                ) from exc
            sleep_backoff(attempt)
    assert metadata is not None
    if metadata.topics.get(topic) is not None:
        log.info("topic '%s' exists (%d partition(s))", topic,
                 len(metadata.topics[topic].partitions))
        return
    log.info("topic '%s' not found — creating (partitions=%d, replication=%d)",
             topic, partitions, replication_factor)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
    )
    for name, future in futures.items():
        try:
            future.result()
            log.info("topic '%s' created", name)
        except KafkaException as exc:
            # NB: match on error CODE, not an exception class — some
            # confluent-kafka builds (e.g. 2.15.x wheels) ship no
            # TopicAlreadyExistsError at all.
            err = exc.args[0] if exc.args else None
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topic '%s' already exists (created concurrently)", name)
            else:
                raise KafkaPipelineError(f"failed to create topic '{name}': {exc}") from exc
        except Exception as exc:  # noqa: BLE001 — surface any admin failure uniformly
            raise KafkaPipelineError(f"failed to create topic '{name}': {exc}") from exc


class ResilientProducer:
    """Idempotent producer with local-queue drain and delivery accounting.

    flush_or_raise() is the at-least-once hook: it raises when anything failed or
    is still pending, so callers refuse to commit their consumer offsets.
    """

    def __init__(self, client_conf: Mapping[str, Any], flush_timeout: float = 30.0,
                 linger_ms: int = 100) -> None:
        conf = dict(client_conf)
        conf.update(
            {
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": linger_ms,
                "compression.type": "lz4",
                "delivery.timeout.ms": 120_000,
                "logger": logging.getLogger("torob.kafka_client"),
            }
        )
        self._producer = Producer(conf)
        self._flush_timeout = flush_timeout
        self._stats = DeliveryStats()

    @property
    def stats(self) -> DeliveryStats:
        return self._stats

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> None:
        deadline = time.monotonic() + self._flush_timeout
        while True:
            try:
                self._producer.produce(
                    topic, key=key.encode("utf-8"), value=value,
                    headers=list(headers) if headers else None,
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError:
                if time.monotonic() > deadline:
                    raise KafkaPipelineError(
                        f"producer queue still full after {self._flush_timeout}s (topic={topic})"
                    )
                log.warning("producer queue full — draining before retry (topic=%s)", topic)
                self._producer.poll(1.0)
            except KafkaException as exc:
                raise KafkaPipelineError(f"produce failed (topic={topic}): {exc}") from exc

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self._stats.failed += 1
            log.error("delivery failed: %s", error)
        else:
            self._stats.delivered += 1

    def flush(self) -> DeliveryStats:
        self._stats.pending = self._producer.flush(self._flush_timeout)
        return self._stats

    def flush_or_raise(self) -> None:
        stats = self.flush()
        if stats.failed or stats.pending:
            raise KafkaPipelineError(
                f"delivery incomplete (failed={stats.failed}, pending={stats.pending}) "
                "— offsets will NOT be committed"
            )


def produce_dead_letter(
    producer: ResilientProducer,
    topic: str,
    message: Message,
    reason: str,
    consumer_group: str,
) -> None:
    """Publish one consumed message to the DLQ with full provenance headers
    (source topic/partition/offset + consumer group + truncated reason).

    Extracted verbatim from the normalizer and db-sink — the DLQ envelope is a
    shared wire contract, so only the consumer-group tag may vary by caller."""
    producer.produce(
        topic,
        key=(message.key() or b"").decode("utf-8", "replace")
        or f"{message.topic()}[{message.partition()}]@{message.offset()}",
        value=message.value() or b"",
        headers=[
            ("source-topic", message.topic().encode("utf-8")),
            ("source-partition", str(message.partition()).encode("utf-8")),
            ("source-offset", str(message.offset()).encode("utf-8")),
            ("consumer-group", consumer_group.encode("utf-8")),
            ("error", reason[:500].encode("utf-8")),
        ],
    )


class BatchConsumer:
    """At-least-once micro-batch consume loop shared by every downstream service.

    Contract: `handler(batch)` performs the work (DB upserts, produces, Redis
    writes …) and must raise on failure. Offsets are committed only after the
    handler returns cleanly — a crash/rebalance re-delivers the batch.
    """

    def __init__(
        self,
        *,
        client_conf: Mapping[str, Any],
        group_id: str,
        topics: Sequence[str],
        poll_timeout: float = 1.0,
        max_batch: int = 200,
        max_wait: float = 2.0,
        offset_reset: str = "earliest",
    ) -> None:
        conf = dict(client_conf)
        conf.update(
            {
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": offset_reset,
                "logger": logging.getLogger("torob.kafka_client"),
            }
        )
        self._conf = conf
        self._group_id = group_id
        self._topics = list(topics)
        self._poll_timeout = poll_timeout
        self._max_batch = max_batch
        self._max_wait = max_wait
        self._shutdown = False
        self.consumed = 0
        self.batches = 0

    # -- signals ----------------------------------------------------------------

    def _handle_signal(self, signum: int, frame: Any) -> None:
        log.info("signal %s received — shutting down after current batch", signum)
        self._shutdown = True

    def _install_signal_handlers(self) -> None:
        import signal

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._handle_signal)
            except (ValueError, OSError):
                pass

    # -- offsets -----------------------------------------------------------------

    @staticmethod
    def commit_offsets(
        consumer: Consumer, batch: Sequence[Message], attempts: int = 3
    ) -> None:
        """Commit max(offset)+1 per partition (sync, retried on broker transients)."""
        highest: dict[tuple[str, int], int] = {}
        for message in batch:
            key = (message.topic(), message.partition())
            highest[key] = max(highest.get(key, -1), message.offset())
        offsets = [
            TopicPartition(topic, partition, offset + 1)
            for (topic, partition), offset in sorted(highest.items())
        ]
        last_error: KafkaException | None = None
        for attempt in range(1, attempts + 1):
            try:
                consumer.commit(offsets=offsets, asynchronous=False)
                log.debug("committed offsets: %s",
                          ", ".join(f"{tp.topic}[{tp.partition}]@{tp.offset}" for tp in offsets))
                return
            except KafkaException as exc:
                last_error = exc
                log.warning("offset commit failed (attempt %d/%d): %s", attempt, attempts, exc)
                sleep_backoff(attempt, cap=5.0)  # gentler cap than the connect loops
        raise KafkaPipelineError(f"offset commit failed after {attempts} attempts: {last_error}")

    # -- loop ----------------------------------------------------------------------

    def run(
        self,
        handler: Handler,
        max_messages: int | None = None,
        idle_exit_seconds: float | None = None,
    ) -> int:
        consumer = Consumer(self._conf)

        def _on_assign(_: Consumer, partitions: list[TopicPartition]) -> None:
            log.info("assigned: %s", ", ".join(
                f"{p.topic}[{p.partition}]" for p in partitions) or "(none)")

        self._install_signal_handlers()
        consumer.subscribe(self._topics, on_assign=_on_assign)
        log.info(
            "consuming topics=%s group=%s (batch<=%d or %.1fs%s%s)",
            self._topics, self._group_id, self._max_batch, self._max_wait,
            f", max_messages={max_messages}" if max_messages else "",
            f", idle_exit={idle_exit_seconds}s" if idle_exit_seconds else "",
        )

        exit_code = 0
        batch: list[Message] = []
        batch_started: float | None = None
        last_activity = time.monotonic()

        try:
            while not self._shutdown:
                message: Message | None = consumer.poll(self._poll_timeout)
                now = time.monotonic()

                if message is not None:
                    error = message.error()
                    if error is not None:
                        if error.code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaPipelineError(f"kafka consumer error: {error}")
                    batch.append(message)
                    last_activity = now
                    if batch_started is None:
                        batch_started = now

                batch_expired = (
                    batch_started is not None and (now - batch_started) >= self._max_wait
                )
                budget_reached = (
                    max_messages is not None
                    and self.consumed + len(batch) >= max_messages
                )

                if batch and (len(batch) >= self._max_batch or batch_expired or budget_reached):
                    handler(batch)                    # work first …
                    self.commit_offsets(consumer, batch)  # … then commit
                    self.batches += 1
                    self.consumed += len(batch)
                    batch = []
                    batch_started = None
                    last_activity = time.monotonic()
                    log.info("batch #%d done (total consumed=%d)", self.batches, self.consumed)

                if budget_reached:
                    log.info("message budget reached (%d) — exiting", max_messages)
                    break
                if (
                    idle_exit_seconds is not None
                    and not batch
                    and (now - last_activity) >= idle_exit_seconds
                ):
                    log.info("idle for %.0fs — exiting", idle_exit_seconds)
                    break
        except KafkaPipelineError as exc:
            log.error("fatal: %s (uncommitted messages will be re-consumed)", exc)
            exit_code = 1
        except Exception as exc:  # noqa: BLE001 — handler bugs must not zombie the loop
            head = (f"{batch[0].topic()}[{batch[0].partition()}]@{batch[0].offset()}"
                    if batch else "none")
            log.exception("unexpected handler failure: %s (batch of %d, head %s)",
                          exc, len(batch), head)
            exit_code = 1
        finally:
            if batch:
                log.info("processing final batch of %d before shutdown", len(batch))
                try:
                    handler(batch)
                    self.commit_offsets(consumer, batch)
                    self.consumed += len(batch)
                except Exception as exc:  # noqa: BLE001
                    log.error("final batch failed: %s", exc)
                    exit_code = 1
            consumer.close()

        log.info("consumer summary: batches=%d consumed=%d group=%s exit=%d",
                 self.batches, self.consumed, self._group_id, exit_code)
        return exit_code
