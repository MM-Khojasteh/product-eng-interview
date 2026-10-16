"""Shared import-time stubs + fakes for the pipeline test modules.

Import me FIRST in every pipeline test module — stubs must be installed
before any `import pipelines.*`. Installing the confluent_kafka / psycopg2 /
redis stubs happens as an import side effect of this module, exactly as the
former single-file tests/test_offline.py did it at its own import time.

This module also hosts the cross-suite fakes/fixtures (FakeResponse,
FakeSession, FakeMessage, kafka_settings(), QUERY, FLYTODAY_BODY,
ALIBABA_ITEM) and the `check()` helper, so each split test module starts:

    import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports

and then imports the production modules under test. Not a conftest fixture
on purpose (crm-monitoring convention: each test module stays self-contained;
conftest.py is path-setup only).
"""

from __future__ import annotations

import json
import sys
import types

# ---------------------------------------------------------------------------
# Stubs for uninstalled third-party packages
# ---------------------------------------------------------------------------


def _stub(name: str, **attrs: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


for _name in ("confluent_kafka", "confluent_kafka.admin", "confluent_kafka.error"):
    _stub(_name)
_ck = sys.modules["confluent_kafka"]
for _attr in ("KafkaError", "KafkaException", "Message", "Producer", "Consumer"):
    setattr(_ck, _attr, type(_attr, (), {}))


class _TopicPartition:
    def __init__(self, topic: str, partition: int, offset: int = -1) -> None:
        self.topic, self.partition, self.offset = topic, partition, offset


_ck.TopicPartition = _TopicPartition
sys.modules["confluent_kafka.admin"].AdminClient = type("AdminClient", (), {})
sys.modules["confluent_kafka.admin"].NewTopic = type("NewTopic", (), {})
sys.modules["confluent_kafka.error"].TopicAlreadyExistsError = type("TAE", (Exception,), {})


class _PgError(Exception):
    pass


_psycopg2 = _stub(
    "psycopg2",
    Error=_PgError,
    OperationalError=type("OpErr", (_PgError,), {}),
    ProgrammingError=_PgError,
    connect=lambda *a, **k: None,
)
# Real psycopg2 exposes SQLSTATE classes ONLY under psycopg2.errors.
_psycopg2.errors = _stub("psycopg2.errors", UndefinedTable=type("UndefinedTable", (_PgError,), {}))
_psycopg2.extras = types.SimpleNamespace(
    Json=lambda obj: ("JSON", obj),
    execute_values=lambda *a, **k: None,
    RealDictCursor=type("RDC", (), {}),
)
sys.modules["psycopg2.extras"] = _psycopg2.extras


class _RedisError(Exception):
    pass


class _FakeRedisBase:
    @classmethod
    def from_url(cls, url: str, **kwargs: object):
        return cls()


_redis = _stub("redis", Redis=_FakeRedisBase)
_redis.exceptions = types.SimpleNamespace(RedisError=_RedisError)
sys.modules["redis.exceptions"] = _redis.exceptions
_redis.exceptions = types.ModuleType("redis.exceptions")
_redis.exceptions.RedisError = _RedisError
sys.modules["redis.exceptions"] = _redis.exceptions

# ---------------------------------------------------------------------------
# Imports needed by the shared fakes below (after stubs)
# ---------------------------------------------------------------------------

from pipelines.common.config import KafkaSettings
from pipelines.suppliers import SearchQuery


def check(condition: bool, label: str) -> None:
    assert condition, label


# ---------------------------------------------------------------------------
# Fakes for HTTP / Kafka messages
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status_code=200, json_body=None, content=None, headers=None):
        self.status_code = status_code
        self._json = json_body
        self.content = content if content is not None else (
            json.dumps(json_body).encode("utf-8") if json_body is not None else b""
        )
        self.headers = headers or {}
        self.text = self.content.decode("utf-8", "replace")
        self.url = "http://fake"
        self.request = types.SimpleNamespace(method="GET")

    def json(self):
        if self._json is None:
            raise ValueError("no json body")
        return self._json


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeMessage:
    def __init__(self, topic="t", partition=0, offset=0, value=b"", key=b"",
                 headers=None):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._key, self._headers = value, key, headers or []

    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def value(self): return self._value
    def key(self): return self._key
    def headers(self): return self._headers


def kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="localhost:9092", security_protocol="PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256", username="", password="",
        raw_topic="raw_flight_prices", canonical_topic="flight-prices.canonical",
        deltas_topic="flight-prices.deltas", search_requests_topic="search.requests",
        dlq_topic="scraper.dlq", log_level="INFO",
    )


QUERY = SearchQuery(origin="THR", destination="MHD", departure_date="2026-11-15")

FLYTODAY_BODY = {
    "searchId": 122543300,
    "pricedItineraries": [
        {
            "airItineraryPricingInfo": {"itinTotalFare": {"totalFare": 29925000}},
            "originDestinationOptions": [
                {"flightSegments": [
                    {
                        "flightNumber": "B99702", "marketingAirlineCode": "B9",
                        "operatingAirline": {"code": "B9", "equipment": "Airbus A310"},
                        "departureDateTime": "2026-11-15T19:30:00",
                        "arrivalDateTime": "2026-11-15T22:30:00",
                        "departureAirportLocationCode": "thr",
                        "arrivalAirportLocationCode": "mhd",
                        "cabinClassCode": "Y", "resBookDesigCode": "U",
                        "journeyDuration": "03:30", "journeyDurationPerMinute": 210,
                        "stopQuantity": 0, "seatsRemaining": 7,
                        "baggage": "30 کیلوگرم", "isCharter": False, "isReturn": False,
                    }
                ]}
            ],
            "fareSourceCode": "deadbeef", "refundMethod": "Offline",
        }
    ],
    "additionalData": {
        "airlines": [{"iata": "B9", "name": "Iran Airtour", "nameFa": "ایرتور"}],
        "airports": [{"iata": "thr", "name": "Mehrabad", "nameFa": "مهرآباد"}],
    },
}

ALIBABA_ITEM = {
    "uniqueKey": "uq-123", "flightId": "THR:MHD:IR572:1",
    "flightNumber": "IR572", "airlineCode": "IR", "airlineName": "Iran Air",
    "leaveDateTime": "2026-11-15T07:15:00", "arrivalDateTime": "2026-11-15T08:45:00",
    "seat": 9, "totalPrice": 18_500_000, "type": "system",
}
