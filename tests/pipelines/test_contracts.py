"""Cross-module contract tests: pipeline_status compute_route_deltas and the
liveness↔BFF quote-hash field contract.

Split from tests/test_offline.py (§3.1 of docs/ARCHITECTURE_REVIEW.md) — a pure
move; test bodies are byte-for-byte the originals. Stubs/fakes come from
tests/pipelines/_stubs.py, which must be imported before any `pipelines.*` import.
(The original header's sys.path.insert is covered by tests/conftest.py.)
"""

import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports
from tests.pipelines._stubs import check

import json
from datetime import datetime, timedelta, timezone

import pipelines.liveness as liveness_mod
import scripts.pipeline_status as status_mod  # noqa: E402  (scripts has no __init__)


def test_status_route_deltas() -> None:
    tz = timezone.utc
    def ts(hour): return datetime(2026, 10, 2, 6, 0, 0, tzinfo=tz) + timedelta(hours=hour)
    history = [
        {"origin": "THR", "destination": "MHD", "scan_ts": ts(0),
         "min_price_rial": 30_000_000, "observations": 100},
        {"origin": "THR", "destination": "MHD", "scan_ts": ts(1),
         "min_price_rial": 31_500_000, "observations": 95},
        {"origin": "THR", "destination": "MHD", "scan_ts": ts(2),
         "min_price_rial": 28_500_000, "observations": 98},
        {"origin": "SYZ", "destination": "THR", "scan_ts": ts(1),
         "min_price_rial": 12_000_000, "observations": 20},
    ]
    deltas = status_mod.compute_route_deltas(history, top=10)
    assert deltas[0]["route"] == "THR→MHD" and deltas[0]["scans"] == 3
    assert deltas[0]["latest_min_price_rial"] == 28_500_000
    assert deltas[0]["delta_pct"] == round((28_500_000 - 31_500_000) / 31_500_000 * 100, 1)
    assert deltas[1]["delta_pct"] is None
    assert status_mod.compute_route_deltas([], top=5) == []
    check(True, "status route deltas")


def test_status_without_database_url(monkeypatch) -> None:
    from argparse import Namespace

    from tests.pipelines._stubs import kafka_settings

    def _no_connect(*args, **kwargs):
        raise AssertionError("must not connect without DATABASE_URL")

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(status_mod.psycopg2, "connect", _no_connect)
    monkeypatch.setattr(status_mod, "fetch_lag", lambda kafka: {})
    monkeypatch.setattr(status_mod, "fetch_topic_tail",
                        lambda kafka, topic, sample: {"topic": topic, "recent": []})
    report, healthy = status_mod.build_report(
        kafka_settings(), Namespace(scans=5, history_hours=24, top=10, sample=5)
    )
    assert not healthy
    assert "DATABASE_URL is not set" in report["sections"]["scans"]["error"]
    assert report["sections"]["routes"]["ok"] is False
    assert report["sections"]["consumer_lag"]["ok"] is True, "kafka sections still run"
    check(True, "status reports a missing DATABASE_URL instead of using a default")


def test_status_before_first_sink_run(monkeypatch) -> None:
    """No flytoday_flights table yet (the DB sink has never run): both Postgres
    sections report "no data", and each failed query rolls back its aborted
    transaction so the next one can run."""
    from argparse import Namespace

    from tests.pipelines._stubs import kafka_settings

    undefined_table = status_mod.psycopg2.errors.UndefinedTable

    class _Cursor:
        def __enter__(self): return self
        def __exit__(self, *exc): return False
        def execute(self, sql, params):
            raise undefined_table('relation "flytoday_flights" does not exist')

    class _Connection:
        def __init__(self): self.rollbacks = 0
        def cursor(self, cursor_factory=None): return _Cursor()
        def rollback(self): self.rollbacks += 1
        def close(self): pass

    connection = _Connection()
    monkeypatch.setenv("DATABASE_URL", "postgresql://user@localhost:5432/flights")
    monkeypatch.setattr(status_mod.psycopg2, "connect", lambda *a, **k: connection)
    monkeypatch.setattr(status_mod, "fetch_lag", lambda kafka: {})
    monkeypatch.setattr(status_mod, "fetch_topic_tail",
                        lambda kafka, topic, sample: {"topic": topic, "recent": []})
    report, healthy = status_mod.build_report(
        kafka_settings(), Namespace(scans=5, history_hours=24, top=10, sample=5)
    )
    assert report["sections"]["scans"] == {"ok": True, "scans": []}
    assert report["sections"]["routes"]["ok"] is True
    assert report["sections"]["routes"]["routes"] == []
    assert connection.rollbacks == 2
    assert healthy
    check(True, "status treats a missing sink table as no data yet")


def test_status_kafka_sections_use_watermark_offsets(monkeypatch) -> None:
    """The fake consumer exposes only real confluent-kafka Consumer methods
    (get_watermark_offsets takes a TopicPartition), so a wrong API name fails
    here instead of only against a live broker."""
    from types import SimpleNamespace

    from tests.pipelines._stubs import kafka_settings

    watermarks = {0: (0, 10), 1: (4, 4)}
    committed_offsets = {0: 7, 1: -1001}  # -1001 = OFFSET_INVALID (never committed)

    class _Consumer:
        def list_topics(self, topic, timeout):
            partitions = dict.fromkeys(watermarks)
            return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=partitions)})
        def get_watermark_offsets(self, partition, timeout=None, cached=False):
            return watermarks[partition.partition]
        def committed(self, partitions, timeout):
            return [status_mod.TopicPartition(p.topic, p.partition, committed_offsets[p.partition])
                    for p in partitions]
        def close(self): pass

    monkeypatch.setattr(status_mod, "_metadata_consumer", lambda kafka, group_id: _Consumer())
    lag = status_mod._group_lag(kafka_settings(), "flight-normalizer", "raw_flight_prices")
    assert lag == {
        "partitions": [
            {"partition": 0, "log_end_offset": 10, "committed": 7, "lag": 3},
            {"partition": 1, "log_end_offset": 4, "committed": None, "lag": 4},
        ],
        "total_lag": 7,
    }
    tail = status_mod.fetch_topic_tail(kafka_settings(), "scraper.dlq", sample=0)
    assert tail == {"topic": "scraper.dlq", "exists": True, "total_messages": 14,
                    "retained_messages": 10, "recent": []}
    check(True, "status kafka sections use the real watermark API")


# ---------------------------------------------------------------------------
# Contract tests (crm-monitoring test_sql_migration_contracts pattern): pin the
# cross-module agreements the refactor must not disturb.
# ---------------------------------------------------------------------------


def test_liveness_bff_field_contract() -> None:
    """Producer→consumer contract across the process boundary: every quote-hash
    field the BFF read path consumes (flights.group_quotes /
    offers_for_flight card + offer rendering) must be written by
    liveness.quote_value, and the hash key must match the BFF's readmodel
    quote_key shape `quote:{O}-{D}:{date}` (date = Gregorian search date)."""
    record = {
        "origin": "THR", "destination": "MHD",
        "request": {"departure_date": "2026-11-15"},
        "supplier": "flytoday", "physical_flight_key": "IR600|2026-11-15|THR|MHD",
        "airline_code": "IR", "flight_no": "IR600",
        "dep_local": "2026-11-15T07:20:00+03:30", "arr_local": "2026-11-15T09:05:00+03:30",
        "journey_duration_minutes": 105, "price_total_rial": 12_400_000,
        "unit_verified": True, "dep_utc": "2026-11-15T03:50:00Z", "arr_utc": "2026-11-15T05:35:00Z",
        "capacity_remaining": 3, "is_charter": False, "baggage": "20kg",
        "refund_method": "refundable", "captured_at": "2026-10-03T01:00:00Z",
    }
    bff_consumed_fields = {
        "physical_flight_key", "price_total_rial", "unit_verified",
        "capacity_remaining", "supplier", "airline_code", "flight_no",
        "dep_local", "arr_local", "journey_duration_minutes", "is_charter",
        "baggage", "refund_method", "captured_at",
    }
    assert bff_consumed_fields <= set(json.loads(liveness_mod.quote_value(record)))
    assert liveness_mod.route_day_key(record) == "quote:THR-MHD:2026-11-15"
