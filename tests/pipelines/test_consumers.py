"""Offline tests for the Kafka consumers: normalizer (dispatch/DLQ/tombstones),
kafka_to_db sink (record validation + schema), liveness writer (redis ops,
TTL, tombstone purge).

Split from tests/test_offline.py (§3.1 of docs/ARCHITECTURE_REVIEW.md) — a pure
move; test bodies are byte-for-byte the originals. Stubs/fakes come from
tests/pipelines/_stubs.py, which must be imported before any `pipelines.*` import.
"""

import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports
from tests.pipelines._stubs import (
    ALIBABA_ITEM,
    FLYTODAY_BODY,
    FakeMessage,
    check,
    kafka_settings,
)

import json

from pipelines.common import dto

import pipelines.normalizer as normalizer_mod
import pipelines.liveness as liveness_mod
import pipelines.kafka_to_db as sink_mod


# ---------------------------------------------------------------------------
# 6. Normalizer — dispatch, DLQ, tombstones
# ---------------------------------------------------------------------------


class RecordingProducer:
    def __init__(self): self.produced = []
    def produce(self, topic, key, value, headers=None):
        self.produced.append((topic, key, value, headers))
    def flush_or_raise(self): pass


def _raw_message(content: bytes, supplier="flytoday", complete="true") -> FakeMessage:
    return FakeMessage(
        topic="raw_flight_prices", partition=0, offset=1, value=content, key=b"THR-MHD",
        headers=[("supplier", supplier.encode()), ("origin", b"THR"),
                 ("destination", b"MHD"), ("departure_date", b"2026-11-15"),
                 ("adults", b"1"), ("children", b"0"), ("infants", b"0"),
                 ("captured_at", b"2026-10-02T10:00:00+00:00"),
                 ("scan_id", b"scan-1"), ("complete", complete.encode())],
    )


def test_normalizer() -> None:
    producer = RecordingProducer()
    norm = normalizer_mod.Normalizer(kafka_settings(), producer)

    scan_content = json.dumps(FLYTODAY_BODY).encode()
    norm.handle_batch([_raw_message(scan_content)])
    canonical = [p for p in producer.produced if p[0] == "flight-prices.canonical"]
    assert len(canonical) == 1 and canonical[0][1] == "THR-MHD-2026-11-15"
    record = json.loads(canonical[0][2])
    assert record["supplier"] == "flytoday" and record["scan_id"] == "scan-1"
    first_proposal = record["proposal_key"]

    # second COMPLETE scan without that itinerary → tombstone on deltas
    producer.produced.clear()
    norm.handle_batch([_raw_message(json.dumps({"searchId": 1, "pricedItineraries": []}).encode())])
    tombstones = [p for p in producer.produced if p[0] == "flight-prices.deltas"]
    assert len(tombstones) == 1, tombstones
    tomb = json.loads(tombstones[0][2])
    assert tomb["type"] == "tombstone" and tomb["proposal_key"] == first_proposal
    assert tomb["origin"] == "THR" and tomb["departure_date"] == "2026-11-15"

    # partial (incomplete) scans must NOT produce tombstones
    producer.produced.clear()
    norm.handle_batch([_raw_message(json.dumps({"searchId": 1, "pricedItineraries": []}).encode(),
                                    complete="false")])
    assert not [p for p in producer.produced if p[0] == "flight-prices.deltas"]

    # DLQ paths
    producer.produced.clear()
    norm.handle_batch([FakeMessage(topic="raw_flight_prices", value=b"garbage{",
                                   headers=[("origin", b"THR")]),            # no supplier
                       _raw_message(b"not json")])                            # parse fails
    dlq = [p for p in producer.produced if p[0] == "scraper.dlq"]
    assert len(dlq) == 2, dlq
    reasons = {h[0]: h[1].decode() for h in dlq[0][3]}
    assert "supplier" in reasons["error"]
    check(True, "normalizer dispatch + tombstones + DLQ")


def test_compute_tombstones() -> None:
    prev = {"a": "t1", "b": "t2", "c": "t3"}
    assert normalizer_mod.compute_tombstones(prev, {"a", "c"}) == ["b"]
    assert normalizer_mod.compute_tombstones(prev, {"a", "b", "c", "d"}) == []
    check(True, "compute_tombstones")


def test_normalizer_cross_supplier_scans_do_not_tombstone() -> None:
    # Proposal keys are supplier-namespaced (alibaba:uniqueKey vs the §5 digest),
    # so a scan by one supplier must never diff against ANOTHER supplier's last
    # scan for the same route-day — the bug that made search results show only
    # one OTA once SUPPLIERS gained a second entry.
    producer = RecordingProducer()
    norm = normalizer_mod.Normalizer(kafka_settings(), producer)

    norm.handle_batch([_raw_message(json.dumps(FLYTODAY_BODY).encode())])
    flytoday_proposals = {
        json.loads(p[2])["proposal_key"]
        for p in producer.produced if p[0] == "flight-prices.canonical"
    }
    assert flytoday_proposals

    # Alibaba scans the SAME route-day: its own diff state is empty, and
    # FlyToday's quotes must survive it untouched.
    alibaba_body = {"requestId": 1, "isCompleted": True,
                    "departing": [dict(ALIBABA_ITEM, uniqueKey="uq-999")]}
    producer.produced.clear()
    norm.handle_batch([_raw_message(json.dumps(alibaba_body).encode(), supplier="alibaba")])
    assert not [p for p in producer.produced if p[0] == "flight-prices.deltas"]

    # FlyToday's next scan still diffs against FlyToday's OWN last scan.
    producer.produced.clear()
    norm.handle_batch([_raw_message(json.dumps({"searchId": 1, "pricedItineraries": []}).encode())])
    tombstones = [p for p in producer.produced if p[0] == "flight-prices.deltas"]
    assert len(tombstones) == len(flytoday_proposals), tombstones
    check(True, "normalizer cross-supplier tombstone isolation")


# ---------------------------------------------------------------------------
# 7. Sink — validation, columns, migration DDL
# ---------------------------------------------------------------------------

def test_sink_record_and_schema() -> None:
    record = dto.build_record(
        supplier="flytoday", scan_id="s", captured_at="2026-10-02T10:00:00+00:00",
        search_session_id="122543300", proposal_key="k" * 16,
        request={"origin": "THR", "destination": "MHD", "departure_date": "2026-11-15"},
        flight_no="B99702", airline_code="B9", origin="THR", destination="MHD",
        dep_local="2026-11-15T19:30:00+03:30", dep_utc="2026-11-15T16:00:00+00:00",
        arr_local="2026-11-15T22:30:00+03:30", arr_utc="2026-11-15T19:30:00+00:00",
        price_total_rial=29_925_000, currency_source="IRR", unit_verified=True,
        capacity_remaining=7, segments=[{"flight_no": "B99702"}],
    )
    row, reason = sink_mod.parse_flight_record(json.dumps(record).encode())
    assert row is not None, reason
    assert set(row.keys()) == set(sink_mod._COLUMNS)
    assert len(sink_mod._COLUMNS) == 28
    assert row["physical_flight_key"] == record["physical_flight_key"]
    assert row["unit_verified"] is True
    assert row["captured_at"].tzinfo is not None

    args = sink_mod.row_to_args([row])
    assert len(args[0]) == 28
    json_positions = [sink_mod._COLUMNS.index(c) for c in ("segments", "raw_event")]
    for pos in json_positions:
        assert isinstance(args[0][pos], tuple) and args[0][pos][0] == "JSON"  # stubbed Json

    assert "ON CONFLICT (proposal_key, captured_at) DO UPDATE SET" in sink_mod._UPSERT_SQL
    assert "physical_flight_key = EXCLUDED.physical_flight_key" in sink_mod._UPSERT_SQL
    for migration in ("physical_flight_key", "unit_verified", "origin_name", "destination_name"):
        assert f"ADD COLUMN IF NOT EXISTS {migration}" in sink_mod._SCHEMA_SQL

    # invalid records → DLQ reasons
    for bad, expect in [
        (b"", "empty payload"),
        (b"{bad json", "invalid JSON"),
        (json.dumps({k: v for k, v in record.items() if k != "proposal_key"}).encode(),
         "proposal_key"),
        (json.dumps({**record, "captured_at": "whenever"}).encode(), "captured_at"),
    ]:
        parsed_row, why = sink_mod.parse_flight_record(bad)
        assert parsed_row is None and expect in why, (bad[:20], why)
    check(True, "sink record validation + schema")


def test_sink_handle_batch_dedupes_pk() -> None:
    # Same proposal_key twice in one scan (two booking buckets on one
    # flight+cabin share the §5 digest) + shared captured_at ⇒ the PK repeats
    # inside one execute_values page and Postgres refuses — the batch must be
    # deduped, last write winning like the Redis HSET.
    class _RecordingSink:
        def __init__(self): self.batches = []
        def upsert_batch(self, rows): self.batches.append(list(rows)); return len(rows)

    def _record(price: int) -> bytes:
        rec = dto.build_record(
            supplier="flytoday", scan_id="s", captured_at="2026-10-02T10:00:00+00:00",
            search_session_id="1", proposal_key="k" * 16,
            request={"origin": "THR", "destination": "MHD", "departure_date": "2026-11-15"},
            flight_no="B99702", airline_code="B9", origin="THR", destination="MHD",
            dep_local="2026-11-15T19:30:00+03:30", dep_utc="2026-11-15T16:00:00+00:00",
            arr_local="2026-11-15T22:30:00+03:30", arr_utc="2026-11-15T19:30:00+00:00",
            price_total_rial=price, currency_source="IRR", unit_verified=True,
            capacity_remaining=7, segments=[],
        )
        return json.dumps(rec).encode()

    sink = _RecordingSink()
    service = sink_mod.DbSinkService(kafka_settings(), sink)
    service.handle_batch([
        FakeMessage(topic="flight-prices.canonical", value=_record(29_925_000)),
        FakeMessage(topic="flight-prices.canonical", value=_record(28_500_000)),  # dup PK
    ])
    assert len(sink.batches) == 1 and len(sink.batches[0]) == 1
    assert sink.batches[0][0]["price_total_rial"] == 28_500_000, "last write wins"
    check(True, "sink batch PK dedupe")


def test_sink_refuses_to_start_without_database_url(monkeypatch) -> None:
    def _no_sink(**kwargs):
        raise AssertionError("PostgresSink must not be built without DATABASE_URL")

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(sink_mod, "PostgresSink", _no_sink)
    assert sink_mod.main([]) == 1, "no credential fallback — fail fast instead"
    check(True, "sink requires DATABASE_URL")


# ---------------------------------------------------------------------------
# 8. Liveness — redis ops, TTL, tombstone purge
# ---------------------------------------------------------------------------


class FakeRedis:
    def __init__(self):
        self.hsets, self.hdels, self.expires, self.deletes = [], [], [], []
        self.storage = {"quote:THR-MHD:2026-11-15": {"proposal-1": "{}"}}

    @classmethod
    def from_url(cls, url, **kwargs):
        return cls()

    def ping(self): return True

    def pipeline(self, transaction=False):
        parent = self
        class _Pipe:
            def __init__(self):
                self._results = []  # pipelined reads (hlen) → execute() output
            def hset(self, key, field, value):
                parent.hsets.append((key, field, value))
                parent.storage.setdefault(key, {})[field] = value
            def hdel(self, key, *fields):
                parent.hdels.append((key,) + fields)
                for field in fields:
                    parent.storage.get(key, {}).pop(field, None)
            def expire(self, key, ttl): parent.expires.append((key, ttl))
            def hlen(self, key):
                self._results.append(parent.hlen(key))
            def delete(self, *keys):
                parent.deletes.extend(keys)
                for key in keys:
                    parent.storage.pop(key, None)
            def execute(self):
                out, self._results = self._results, []
                return out
        return _Pipe()

    def hlen(self, key):
        return len(self.storage.get(key, {}))

    def delete(self, *keys): self.deletes.extend(keys)


def test_liveness() -> None:
    liveness_mod.redis.Redis = FakeRedis  # inject the fake client
    writer = liveness_mod.LivenessWriter(kafka_settings(), "redis://fake", ttl_seconds=120)
    fake = writer._redis  # the instance from_url actually constructed

    canonical = dto.build_record(
        supplier="flytoday", scan_id="s", captured_at="2026-10-02T10:00:00+00:00",
        search_session_id=None, proposal_key="proposal-1",
        request={"origin": "THR", "destination": "MHD", "departure_date": "2026-11-15"},
        flight_no="B99702", airline_code="B9", origin="THR", destination="MHD",
        dep_local=None, dep_utc="2026-11-15T16:00:00+00:00", arr_local=None,
        arr_utc="2026-11-15T19:30:00+00:00", price_total_rial=29_925_000,
        currency_source="IRR", unit_verified=True, capacity_remaining=7, segments=[],
        baggage="30 کیلوگرم", refund_method="Offline",
    )
    tombstone = json.dumps({
        "type": "tombstone", "proposal_key": "proposal-1", "origin": "THR",
        "destination": "MHD", "departure_date": "2026-11-15",
    }).encode()

    writer.handle_batch([
        FakeMessage(topic="flight-prices.canonical", value=json.dumps(canonical).encode()),
        FakeMessage(topic="flight-prices.deltas", value=tombstone),
        FakeMessage(topic="flight-prices.canonical", value=b"{broken"),  # skipped, not fatal
    ])
    assert fake.hsets and fake.hsets[0][0] == "quote:THR-MHD:2026-11-15"
    assert fake.hsets[0][1] == "proposal-1"
    payload = json.loads(fake.hsets[0][2])
    assert payload["price_total_rial"] == 29_925_000
    # the BFF renders cards / dedups / guards ghost prices from the hash alone —
    # quote_value MUST carry the identity, card-render and trust fields
    assert payload["physical_flight_key"] == canonical["physical_flight_key"]
    assert payload["airline_code"] == "B9" and payload["flight_no"] == "B99702"
    assert payload["unit_verified"] is True
    assert payload["capacity_remaining"] == 7 and payload["captured_at"]
    # per-fare detail for the sellers-list chips must reach the BFF from the
    # hash alone (baggage/refund chips on offer rows)
    assert payload["baggage"] == "30 کیلوگرم" and payload["refund_method"] == "Offline"
    assert fake.expires == [("quote:THR-MHD:2026-11-15", 120)], "TTL per §2.6 (60–120s)"
    assert fake.hdels == [("quote:THR-MHD:2026-11-15", "proposal-1")]
    assert "quote:THR-MHD:2026-11-15" in fake.deletes  # emptied hash → key deleted
    assert writer.written == 1 and writer.purged == 1
    check(True, "liveness redis ops + tombstone purge")
