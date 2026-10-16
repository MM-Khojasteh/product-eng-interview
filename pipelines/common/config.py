"""Shared configuration: env parsing, logging, and the Kafka settings contract.

The Kafka env contract is INFRASTRUCTURE.md §1.7/§2.4 verbatim — local PLAINTEXT vs
cloud SASL_SSL is a two-line diff, and every service (worker, normalizer, sink,
liveness, status) builds its client from the same KafkaSettings so security
settings can never drift between them.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any


def env_str(name: str, default: str = "") -> str:
    value: str | None = os.environ.get(name)
    return value if value not in (None, "") else default


def env_int(name: str, default: int) -> int:
    raw = env_str(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name}={raw!r} is not an integer") from exc


def env_float(name: str, default: float) -> float:
    raw = env_str(name, str(default))
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name}={raw!r} is not a number") from exc


def env_bool(name: str, default: bool = False) -> bool:
    return env_str(name, "true" if default else "false").strip().lower() not in (
        "0", "false", "no", "off",
    )


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    )


@dataclass(frozen=True)
class KafkaSettings:
    """Bootstrap/security + topic names — the one config every service shares."""

    bootstrap_servers: str        # KAFKA_BOOTSTRAP_SERVERS
    security_protocol: str        # KAFKA_SECURITY_PROTOCOL
    sasl_mechanism: str           # KAFKA_SASL_MECHANISM
    username: str                 # KAFKA_USERNAME
    password: str                 # KAFKA_PASSWORD
    raw_topic: str                # RAW_TOPIC          (worker → normalizer, raw bytes)
    canonical_topic: str          # CANONICAL_TOPIC    (normalizer → sink/liveness)
    deltas_topic: str             # DELTAS_TOPIC       (tombstones / price deltas)
    search_requests_topic: str    # SEARCH_REQUESTS_TOPIC (BFF cache-miss → worker)
    dlq_topic: str                # KAFKA_DLQ_TOPIC
    log_level: str                # LOG_LEVEL

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        return cls(
            bootstrap_servers=env_str("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            security_protocol=env_str("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=env_str("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            username=env_str("KAFKA_USERNAME"),
            password=env_str("KAFKA_PASSWORD"),
            raw_topic=env_str("RAW_TOPIC", "raw_flight_prices"),
            canonical_topic=env_str("CANONICAL_TOPIC", "flight-prices.canonical"),
            deltas_topic=env_str("DELTAS_TOPIC", "flight-prices.deltas"),
            search_requests_topic=env_str("SEARCH_REQUESTS_TOPIC", "search.requests"),
            dlq_topic=env_str("KAFKA_DLQ_TOPIC", "scraper.dlq"),
            log_level=env_str("LOG_LEVEL", "INFO").upper(),
        )


def kafka_client_conf(settings: KafkaSettings, client_id: str) -> dict[str, Any]:
    """librdkafka client config; SASL keys only for non-PLAINTEXT protocols."""
    conf: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "socket.keepalive.enable": True,
    }
    protocol = settings.security_protocol.strip().upper()
    if protocol != "PLAINTEXT":
        conf["security.protocol"] = protocol
        if protocol.startswith("SASL"):
            conf["sasl.mechanisms"] = settings.sasl_mechanism
            conf["sasl.username"] = settings.username
            conf["sasl.password"] = settings.password
    return conf
