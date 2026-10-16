"""Offline tests for pipelines/common primitives: chrono, dto, TokenBucket
(incl. thread-safety), CircuitBreaker, BatchConsumer.commit_offsets, DiskSpool.

Split from tests/test_offline.py (§3.1 of docs/ARCHITECTURE_REVIEW.md) — a pure
move; test bodies are byte-for-byte the originals. Stubs/fakes come from
tests/pipelines/_stubs.py, which must be imported before any `pipelines.*` import.
"""

import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports
from tests.pipelines._stubs import FakeMessage, check

from datetime import timedelta
from pathlib import Path

from pipelines.common import chrono, dto
from pipelines.common.circuit import CircuitBreaker, CircuitState
from pipelines.common.ratelimit import TokenBucket
from pipelines.common.kafkaio import BatchConsumer
from pipelines.common.spool import DiskSpool
from pipelines.common.errors import KafkaPipelineError


# ---------------------------------------------------------------------------
# 1. chrono + dto
# ---------------------------------------------------------------------------

def test_chrono() -> None:
    local, utc = chrono.localize("2026-11-15T19:30:00")
    assert local == "2026-11-15T19:30:00+03:30", local
    assert utc == "2026-11-15T16:00:00+00:00", utc
    assert chrono.localize("garbage") == ("garbage", None)
    assert chrono.localize(None) == (None, None)
    aware = chrono.to_aware("2026-10-02T10:00:00", chrono.TEHRAN_TZ)
    assert aware is not None and aware.utcoffset() == timedelta(hours=3, minutes=30)
    check(True, "chrono")


def test_dto_cross_supplier_identity() -> None:
    # same physical flight sold by two OTAs → same physical key, distinct proposal keys
    flytoday_proposal = dto.fare_proposal_key("B9", "B99702", "2026-11-15T19:30:00+03:30", "THR", "MHD", "Y")
    record = dto.build_record(
        supplier="flytoday", scan_id="s", captured_at="2026-10-02T10:00:00+00:00",
        search_session_id="1", proposal_key=flytoday_proposal, request={},
        flight_no="B99702", airline_code="B9", origin="THR", destination="MHD",
        dep_local="2026-11-15T19:30:00+03:30", dep_utc="2026-11-15T16:00:00+00:00",
        arr_local="2026-11-15T22:30:00+03:30", arr_utc="2026-11-15T19:30:00+00:00",
        price_total_rial=29_925_000, currency_source="IRR", unit_verified=True,
        capacity_remaining=7, segments=[],
    )
    record2 = dto.build_record(
        supplier="alibaba", scan_id="s2", captured_at="2026-10-02T11:00:00+00:00",
        search_session_id="2", proposal_key="alibaba:uq-1", request={},
        flight_no="B99702", airline_code="B9", origin="THR", destination="MHD",
        dep_local="2026-11-15T19:30:00+03:30", dep_utc="2026-11-15T16:00:00+00:00",
        arr_local="2026-11-15T22:30:00+03:30", arr_utc="2026-11-15T19:30:00+00:00",
        price_total_rial=28_500_000, currency_source="IRR", unit_verified=False,
        capacity_remaining=4, segments=[],
    )
    assert record["physical_flight_key"] == record2["physical_flight_key"]
    assert record["proposal_key"] != record2["proposal_key"]
    assert record["record_version"] == 2
    # 2026-10 wire drift: the OTAs disagree on airline codes AND number format
    # (VRH/VRH5840 vs VR/5840 for the same plane) — the key must still merge.
    assert dto.physical_flight_key(
        "VRH", "VRH5840", "2026-11-15T14:00:00+03:30", "THR", "MHD"
    ) == dto.physical_flight_key(
        "VR", "5840", "2026-11-15T14:00:00+03:30", "THR", "MHD"
    )
    check(True, "dto cross-supplier identity")


# ---------------------------------------------------------------------------
# 2. TokenBucket + CircuitBreaker
# ---------------------------------------------------------------------------

def test_token_bucket() -> None:
    now = [0.0]
    bucket = TokenBucket("flytoday", capacity=6, refill_per_minute=6.0,
                         clock=lambda: now[0], sleep=lambda s: None)
    acquired = [bucket.try_acquire() for _ in range(6)]
    assert all(acquired) and not bucket.try_acquire(), "6 capacity, 7th must fail"
    now[0] = 10.0  # 10 s × (6/min) = exactly 1 token refilled
    assert bucket.try_acquire(), "refill after 10s"
    assert not bucket.try_acquire(timeout=0.0)
    check(True, "token bucket")


def test_circuit_breaker() -> None:
    now = [0.0]
    breaker = CircuitBreaker("flytoday", blocked_threshold=3, blocked_open_seconds=600.0,
                             clock=lambda: now[0])
    assert breaker.allow() and breaker.state is CircuitState.CLOSED
    breaker.record_failure("error"); breaker.record_failure("error")
    assert breaker.state is CircuitState.CLOSED          # below error threshold (5)
    breaker.record_failure("blocked"); breaker.record_failure("blocked")
    assert breaker.state is CircuitState.CLOSED          # below blocked threshold (3)
    breaker.record_failure("blocked")
    assert breaker.state is CircuitState.OPEN and not breaker.allow()
    now[0] = 599.0
    assert not breaker.allow()                            # still open
    now[0] = 601.0
    assert breaker.state is CircuitState.HALF_OPEN and breaker.allow()
    breaker.record_failure("blocked")                     # failed probe → reopen
    assert breaker.state is CircuitState.OPEN
    now[0] = 601.0 + 600.0
    assert breaker.allow()
    breaker.record_success()                              # good probe → closed
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure("rate_limited")                # single 429 → cooldown
    assert breaker.state is CircuitState.OPEN
    now[0] += 31.0
    assert breaker.allow()
    check(True, "circuit breaker")


def test_token_bucket_thread_safe() -> None:
    import threading

    bucket = TokenBucket("t", capacity=5, refill_per_minute=0,
                         clock=lambda: 0.0, sleep=lambda s: None)
    granted = []
    threads = [threading.Thread(target=lambda: granted.append(bucket.try_acquire(0)))
               for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(granted) == 5, \
        "the per-supplier politeness ceiling must hold across cadence + on-demand threads"
    check(True, "token bucket thread safety")


# ---------------------------------------------------------------------------
# 3. Kafkaio — offset commit math
# ---------------------------------------------------------------------------

def test_commit_offsets() -> None:
    class FakeConsumer:
        def __init__(self): self.committed = None
        def commit(self, offsets=None, asynchronous=False):
            self.committed = [(tp.topic, tp.partition, tp.offset) for tp in offsets]
    consumer = FakeConsumer()
    BatchConsumer.commit_offsets(
        consumer,
        [FakeMessage("t", 0, 5), FakeMessage("t", 0, 9),
         FakeMessage("t", 1, 3), FakeMessage("t", 0, 2)],
    )
    assert consumer.committed == [("t", 0, 10), ("t", 1, 4)], consumer.committed
    check(True, "commit offsets")


# ---------------------------------------------------------------------------
# 11. Disk spool (spec §2: no data loss when the broker is down)
# ---------------------------------------------------------------------------


def test_spool_roundtrip() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        spool = DiskSpool(tmp)
        assert spool.pending() == 0
        spool.write("THR-MHD-2026-11-15", b'{"raw":"bytes"}',
                    [("supplier", b"flytoday"), ("captured_at", b"2026-10-02T10:00:00Z")])
        spool.write("THR-SYZ-2026-11-15", b"{}", [("supplier", b"alibaba")])
        assert spool.pending() == 2

        published = []

        def publish(key, value, headers):
            published.append((key, value, headers))

        enqueued, remaining = spool.replay(publish)
        assert len(enqueued) == 2 and remaining == 2, \
            "files must stay on disk until delivery is confirmed (discard)"
        assert published[0] == ("THR-MHD-2026-11-15", b'{"raw":"bytes"}',
                                [("supplier", b"flytoday"),
                                 ("captured_at", b"2026-10-02T10:00:00Z")])
        assert published[1][0] == "THR-SYZ-2026-11-15"   # FIFO
        spool.discard(enqueued)
        assert spool.pending() == 0
    check(True, "spool round-trip")


def test_spool_keeps_files_on_broker_failure() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        spool = DiskSpool(tmp)
        spool.write("k1", b"v1", [("supplier", b"flytoday")])
        spool.write("k2", b"v2", [("supplier", b"flytoday")])

        def failing_publish(key, value, headers):
            raise KafkaPipelineError("broker down")

        enqueued, remaining = spool.replay(failing_publish)
        assert enqueued == [] and remaining == 2, "nothing discarded on failure"

        # corrupt file is moved aside, never deleted, and does not stop replay
        bad = Path(tmp) / "zz-bad.json"
        bad.write_text("{not json", encoding="utf-8")
        ok = []

        def ok_publish(key, value, headers):
            ok.append(key)

        enqueued2, remaining2 = spool.replay(ok_publish)
        assert ok == ["k1", "k2"] and len(enqueued2) == 2
        assert (Path(tmp) / "zz-bad.corrupt").exists()
    check(True, "spool failure + corruption handling")
