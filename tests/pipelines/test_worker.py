"""Offline tests for pipelines/raw_worker: WorkerSettings route parsing, the
worker↔normalizer raw-topic header contract, search.requests wire format, and
the on-demand scan queue / scan_route date threading.

(The TokenBucket thread-safety test lives in tests/pipelines/test_common.py — one place
only.)

Split from tests/test_offline.py (§3.1 of docs/ARCHITECTURE_REVIEW.md) — a pure
move; test bodies are byte-for-byte the originals. Stubs/fakes come from
tests/pipelines/_stubs.py, which must be imported before any `pipelines.*` import.
"""

import tests.pipelines._stubs  # noqa: F401 — stubs before pipelines imports
from tests.pipelines._stubs import QUERY, check

import os
import random
import threading

from pipelines.common.circuit import CircuitBreaker
from pipelines.common.config import KafkaSettings
from pipelines.common.ratelimit import TokenBucket

import pipelines.raw_worker as raw_worker_mod


# ---------------------------------------------------------------------------
# 9. Worker settings + status route deltas
# ---------------------------------------------------------------------------

def test_worker_settings_routes() -> None:
    os.environ.update({"ROUTES": "thr-mhd, SYZ-THR", "SUPPLIERS": "FlyToday,Alibaba"})
    settings = raw_worker_mod.WorkerSettings.from_env()
    assert settings.routes == (("THR", "MHD"), ("SYZ", "THR"))
    assert settings.suppliers == ("flytoday", "alibaba")
    assert settings.searches_per_minute == 6.0 and settings.bucket_capacity == 6.0
    assert settings.spool_enabled is True and settings.spool_dir == ".spool"
    os.environ.update({"ROUTES": "THR"})
    try:
        raw_worker_mod.WorkerSettings.from_env()
        raise AssertionError("expected ValueError for route without dash")
    except ValueError:
        pass
    finally:
        os.environ.pop("ROUTES", None); os.environ.pop("SUPPLIERS", None)
    check(True, "worker settings routes")


def test_supplier_kwargs_require_endpoint_matrix(monkeypatch) -> None:
    for var in ("FLYTODAY_SEARCH_PATH", "FLYTODAY_TOKEN_PATH",
                "ALIBABA_SEARCH_PATH", "ALIBABA_AB_CHANNEL"):
        monkeypatch.delenv(var, raising=False)
    for supplier, first_missing in (("flytoday", "FLYTODAY_SEARCH_PATH"),
                                    ("alibaba", "ALIBABA_SEARCH_PATH")):
        try:
            raw_worker_mod._supplier_kwargs(supplier)
            raise AssertionError(f"expected ValueError for unconfigured {supplier}")
        except ValueError as exc:
            assert first_missing in str(exc), "error must name the missing variable"
    assert raw_worker_mod._supplier_kwargs("stub") == {}, "unknown suppliers carry no config"
    check(True, "supplier endpoint matrix is required config")


def test_supplier_kwargs_tuning_overrides(monkeypatch) -> None:
    from pipelines.suppliers import AlibabaSupplier, FlyTodaySupplier
    from pipelines.suppliers.alibaba import DEFAULT_MAX_POLLS, DEFAULT_POLL_INTERVAL_SECONDS
    from pipelines.suppliers.base import CHROME_USER_AGENT

    monkeypatch.setenv("FLYTODAY_SEARCH_PATH", "/fixture/search")
    monkeypatch.setenv("FLYTODAY_TOKEN_PATH", "/fixture/token")
    monkeypatch.setenv("ALIBABA_SEARCH_PATH", "/fixture/available")
    monkeypatch.setenv("ALIBABA_AB_CHANNEL", "fixture-channel")
    for var in ("SCRAPER_USER_AGENT", "ALIBABA_POLL_INTERVAL_SECONDS", "ALIBABA_MAX_POLLS"):
        monkeypatch.delenv(var, raising=False)

    defaults = raw_worker_mod._supplier_kwargs("alibaba")
    assert defaults["user_agent"] == CHROME_USER_AGENT
    assert defaults["poll_interval"] == DEFAULT_POLL_INTERVAL_SECONDS
    assert defaults["max_polls"] == DEFAULT_MAX_POLLS

    monkeypatch.setenv("SCRAPER_USER_AGENT", "FixtureBrowser/1.0")
    monkeypatch.setenv("ALIBABA_POLL_INTERVAL_SECONDS", "0.5")
    monkeypatch.setenv("ALIBABA_MAX_POLLS", "4")
    # kwargs must match the constructors exactly — a typo would raise TypeError
    alibaba = AlibabaSupplier(**raw_worker_mod._supplier_kwargs("alibaba"))
    flytoday = FlyTodaySupplier(**raw_worker_mod._supplier_kwargs("flytoday"))
    assert alibaba._poll_interval == 0.5 and alibaba._max_polls == 4
    assert alibaba._user_agent == flytoday._user_agent == "FixtureBrowser/1.0"
    check(True, "supplier tuning knobs are env-overridable")


# ---------------------------------------------------------------------------
# 10. Worker↔normalizer header contract (the raw-topic wire format)
# ---------------------------------------------------------------------------


def test_worker_normalizer_header_contract() -> None:
    from pipelines.suppliers.base import RawScan
    scan = RawScan(
        supplier="flytoday", query=QUERY, content=b"{}",
        captured_at="2026-10-02T10:00:00+00:00", scan_id="a" * 32, complete=True,
    )
    written = {name for name, _ in raw_worker_mod.scan_headers(scan)}
    # every header the normalizer reads must be present on every scan the
    # worker publishes (and vice versa: no unread junk keys)
    read = {
        "supplier", "origin", "destination", "departure_date", "adults",
        "children", "infants", "captured_at", "scan_id", "complete",
    }
    assert written == read, (written - read, read - written)
    headers = dict(raw_worker_mod.scan_headers(scan))
    assert headers["supplier"] == b"flytoday"
    assert headers["captured_at"] == b"2026-10-02T10:00:00+00:00"
    check(True, "worker↔normalizer header contract")


# ---------------------------------------------------------------------------
# 10b. On-demand scans (BFF search.requests → worker; same politeness budget)
# ---------------------------------------------------------------------------


def test_parse_search_request() -> None:
    parse = raw_worker_mod.parse_search_request
    assert parse(b'{"origin":"thr","destination":"MHD","departure_date":"2026-10-05"}') \
        == ("THR", "MHD", "2026-10-05"), "codes normalized to upper-case"
    assert parse(b'{"origin":"THR","destination":"KIH","departure_date":"2026-10-05"}') \
        == ("THR", "KIH", "2026-10-05")
    assert parse(b"not json") is None
    assert parse(b'{"origin":"THR"}') is None, "missing fields → None"
    assert parse(b'["array"]') is None, "non-object payloads rejected"
    assert parse(None) is None
    check(True, "search.requests wire contract (BFF↔worker)")


def test_on_demand_scan_queue() -> None:
    scanned = []

    class _StubWorker:
        def scan_route(self, origin, destination, departure_date):
            scanned.append((origin, destination, departure_date))

    scans = raw_worker_mod.OnDemandScans(_StubWorker(), max_queue=10)
    assert scans.enqueue("THR", "MHD", "2026-10-05")
    assert not scans.enqueue("THR", "MHD", "2026-10-05"), "route-day dedupe"
    assert scans.enqueue("THR", "KIH", "2026-10-05")
    assert scans.enqueue("THR", "MHD", "2026-10-06"), "a different date is a new scan"
    assert scans._queue.qsize() == 3
    assert scanned == [], "nothing scans until start() spins the executor threads"
    # dedupe is a cooldown: quotes expire (TTL 60–120 s), so the route-day must
    # become scannable again — a search minutes later must NOT be dropped.
    scans._seen[("THR", "MHD", "2026-10-05")] -= scans._dedup_seconds
    assert scans.enqueue("THR", "MHD", "2026-10-05"), "dedupe lifts after cooldown"
    check(True, "on-demand queue dedupes per route-day with a bounded cooldown")


def test_on_demand_scan_route_carries_request_date() -> None:
    from pipelines.suppliers.base import RawScan

    os.environ.update({"SUPPLIERS": "stub"})
    try:
        settings = raw_worker_mod.WorkerSettings.from_env()
    finally:
        os.environ.pop("SUPPLIERS", None)

    class _RecordingStrategy:
        name = "stub"

        def __init__(self) -> None:
            self.queries = []

        def search(self, query):
            self.queries.append(query)
            return RawScan(supplier="stub", query=query, content=b"{}",
                           captured_at="2026-10-02T10:00:00+00:00", scan_id="s" * 32)

    produced = []

    class _StubProducer:
        def produce(self, topic, key, value, headers=None):
            produced.append((topic, key))

    strategy = _RecordingStrategy()
    worker = raw_worker_mod.RawWorker(
        settings, KafkaSettings.from_env(),
        {"stub": strategy},
        {"stub": TokenBucket("stub", capacity=6, refill_per_minute=6)},
        {"stub": CircuitBreaker("stub")},
        _StubProducer(), spool=None, rng=random.Random(0), sleep=lambda s: None,
    )
    worker.scan_route("THR", "KIH", "2026-10-07")
    assert strategy.queries and strategy.queries[0].departure_date == "2026-10-07", \
        "on-demand scans carry the USER's date, not the fixed cadence DEPARTURE_DATE"
    assert produced == [("raw_flight_prices", "THR-KIH-2026-10-07")], \
        "the raw message key carries the scanned route-day"
    check(True, "scan_route threads the request date into the raw pipeline")


def test_scan_route_scans_suppliers_in_parallel() -> None:
    # A hung supplier (FlyToday evening ReadTimeouts, 2026-10-16) must not eat
    # the BFF poll window before the healthy OTA scans: both searches rendezvous
    # on a Barrier — serial head-of-line blocking times it out instead.
    from pipelines.suppliers.base import RawScan

    os.environ.update({"SUPPLIERS": "stub,stub2"})
    try:
        settings = raw_worker_mod.WorkerSettings.from_env()
    finally:
        os.environ.pop("SUPPLIERS", None)

    rendezvous = threading.Barrier(2, timeout=10)

    class _Strategy:
        def __init__(self, name: str) -> None:
            self.name = name

        def search(self, query):
            rendezvous.wait()  # passes only if BOTH suppliers run concurrently
            return RawScan(supplier=self.name, query=query, content=b"{}",
                           captured_at="2026-10-02T10:00:00+00:00", scan_id="s" * 32)

    produced = []

    class _StubProducer:
        def produce(self, topic, key, value, headers=None):
            produced.append(key)

    names = ("stub", "stub2")
    worker = raw_worker_mod.RawWorker(
        settings, KafkaSettings.from_env(),
        {n: _Strategy(n) for n in names},
        {n: TokenBucket(n, capacity=6, refill_per_minute=6) for n in names},
        {n: CircuitBreaker(n) for n in names},
        _StubProducer(), spool=None, rng=random.Random(0), sleep=lambda s: None,
    )
    worker.scan_route("THR", "KIH", "2026-10-07")
    assert len(produced) == 2, produced
    check(True, "scan_route scans suppliers concurrently (no head-of-line blocking)")
