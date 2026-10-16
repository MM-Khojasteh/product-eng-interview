#!/usr/bin/env python
"""Pipeline health report — multi-service ingestion observability (one command).

Sections
    scans         per scan: total proposals vs UNIQUE physical flights — detects
                  fare-bucket inflation and shows cross-supplier coverage
    routes        top routes: latest scan timestamp + min-price delta vs previous
    consumer_lag  Kafka lag per partition for EVERY service group
                  (normalizer on raw, sink + liveness on canonical/deltas)
    dlq           message count + recent error reasons from scraper.dlq
    tombstones    recent sold-out purge events from flight-prices.deltas

Usage
    python scripts/pipeline_status.py             # human-readable
    python scripts/pipeline_status.py --json      # machine-readable (alerts, CI)
Exit codes: 0 = all sections healthy · 1 = at least one section failed
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
import psycopg2.errors
import psycopg2.extras
from confluent_kafka import Consumer, TopicPartition

from pipelines.common.config import KafkaSettings, env_str, kafka_client_conf

# service groups whose lag we report: (name, group env, default group, topics)
_GROUPS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("normalizer", "NORMALIZER_GROUP_ID", "flight-normalizer", ("raw",)),
    ("sink", "SINK_GROUP_ID", "flight-db-sink", ("canonical",)),
    ("liveness", "LIVENESS_GROUP_ID", "flight-liveness", ("canonical", "deltas")),
)

_SCANS_SQL = """
    SELECT scan_id,
           MIN(captured_at)                       AS scan_ts,
           COUNT(*)                               AS proposals,
           COUNT(DISTINCT (airline_code, flight_no, dep_utc)) AS unique_flights,
           COUNT(DISTINCT physical_flight_key)    AS unique_physical_flights,
           COUNT(DISTINCT supplier)               AS suppliers,
           MIN(price_total_rial)                  AS min_price_rial,
           MAX(price_total_rial)                  AS max_price_rial
    FROM flytoday_flights
    GROUP BY scan_id
    ORDER BY scan_ts DESC
    LIMIT %s
"""

_ROUTE_HISTORY_SQL = """
    SELECT origin, destination, captured_at AS scan_ts,
           MIN(price_total_rial) AS min_price_rial,
           COUNT(*) AS observations
    FROM flytoday_flights
    WHERE price_total_rial IS NOT NULL
      AND captured_at > now() - (%s * interval '1 hour')
    GROUP BY 1, 2, 3
    ORDER BY scan_ts
"""


# ---------------------------------------------------------------------------
# PostgreSQL sections
# ---------------------------------------------------------------------------


def _fetch_rows(conn: Any, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Rows as dicts; [] until the DB sink's first run creates flytoday_flights."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
    except psycopg2.errors.UndefinedTable:
        conn.rollback()  # the failed statement aborted the transaction
        return []


def fetch_scan_stats(conn: Any, limit: int) -> list[dict[str, Any]]:
    return _fetch_rows(conn, _SCANS_SQL, (limit,))


def fetch_route_history(conn: Any, history_hours: int) -> list[dict[str, Any]]:
    return _fetch_rows(conn, _ROUTE_HISTORY_SQL, (history_hours,))


def compute_route_deltas(
    history: Sequence[Mapping[str, Any]], top: int
) -> list[dict[str, Any]]:
    by_route: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in history:
        by_route.setdefault((str(row["origin"]), str(row["destination"])), []).append(row)

    deltas: list[dict[str, Any]] = []
    for (origin, destination), scans in by_route.items():
        scans = sorted(scans, key=lambda r: r["scan_ts"])
        latest, previous = scans[-1], (scans[-2] if len(scans) > 1 else None)
        latest_price = latest["min_price_rial"]
        prev_price = previous["min_price_rial"] if previous else None
        deltas.append(
            {
                "route": f"{origin}→{destination}",
                "scans": len(scans),
                "observations": sum(int(r["observations"]) for r in scans),
                "latest_scan_ts": latest["scan_ts"].isoformat(),
                "latest_min_price_rial": latest_price,
                "prev_min_price_rial": prev_price,
                "delta_pct": (
                    round((latest_price - prev_price) / prev_price * 100.0, 1)
                    if prev_price else None
                ),
            }
        )
    deltas.sort(key=lambda d: (-d["observations"], d["route"]))
    return deltas[:top]


# ---------------------------------------------------------------------------
# Kafka sections
# ---------------------------------------------------------------------------


def _topic(kafka: KafkaSettings, key: str) -> str:
    return {"raw": kafka.raw_topic, "canonical": kafka.canonical_topic,
            "deltas": kafka.deltas_topic, "dlq": kafka.dlq_topic}[key]


def _metadata_consumer(kafka: KafkaSettings, group_id: str) -> Consumer:
    conf = kafka_client_conf(kafka, "pipeline-status")
    conf.update({"group.id": group_id, "enable.auto.commit": False})
    return Consumer(conf)


def _group_lag(kafka: KafkaSettings, group_id: str, topic: str) -> dict[str, Any]:
    """Committed vs log-end per partition (metadata-only; never subscribes/commits)."""
    consumer = _metadata_consumer(kafka, group_id)
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            raise RuntimeError(f"topic '{topic}' not found on the broker")
        partitions, total_lag = [], 0
        for partition_id in sorted(topic_meta.partitions):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id), timeout=10
            )
            committed_tp = consumer.committed(
                [TopicPartition(topic, partition_id)], timeout=10
            )[0]
            committed = committed_tp.offset if committed_tp.offset >= 0 else None
            lag = high - committed if committed is not None else high
            total_lag += lag
            partitions.append(
                {"partition": partition_id, "log_end_offset": high,
                 "committed": committed, "lag": lag}
            )
        return {"partitions": partitions, "total_lag": total_lag}
    finally:
        consumer.close()


def fetch_lag(kafka: KafkaSettings) -> dict[str, Any]:
    groups = {}
    for name, env, default, topic_keys in _GROUPS:
        group_id = env_str(env, default)
        topics = {}
        for key in topic_keys:
            topics[_topic(kafka, key)] = _group_lag(kafka, group_id, _topic(kafka, key))
        groups[name] = {"group": group_id, "topics": topics}
    return groups


def fetch_topic_tail(kafka: KafkaSettings, topic: str, sample: int) -> dict[str, Any]:
    """Watermark totals + a tail sample of recent messages (headers + parsed bits)."""
    consumer = _metadata_consumer(kafka, "pipeline-status")
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            return {"topic": topic, "exists": False, "total_messages": 0, "recent": []}
        total, retained, recent = 0, 0, []
        for partition_id in sorted(topic_meta.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id), timeout=10
            )
            total += high
            retained += high - low
            if high > low and len(recent) < sample:
                consumer.assign([TopicPartition(topic, partition_id, max(low, high - sample))])
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and len(recent) < sample:
                    message = consumer.poll(0.5)
                    if message is None or message.error():
                        continue
                    headers = {k: v.decode("utf-8", "replace")
                               for k, v in (message.headers() or [])}
                    recent.append({"headers": headers, "value": (message.value() or b"")[:160]})
        return {"topic": topic, "exists": True, "total_messages": total,
                "retained_messages": retained, "recent": recent}
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Report assembly & rendering
# ---------------------------------------------------------------------------


def build_report(kafka: KafkaSettings, args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bootstrap": kafka.bootstrap_servers,
        "sections": {},
    }
    healthy = True

    def add(name: str, ok: bool, payload: dict[str, Any], error: str | None = None) -> None:
        nonlocal healthy
        section: dict[str, Any] = {"ok": ok, **payload}
        if error:
            section["error"] = error
        report["sections"][name] = section
        healthy = healthy and ok

    database_url = env_str("DATABASE_URL")
    if not database_url:
        add("scans", False, {}, error="DATABASE_URL is not set (see .env.example)")
        add("routes", False, {}, error="postgres unavailable")
    else:
        try:
            conn = psycopg2.connect(database_url, connect_timeout=10)
            try:
                add("scans", True, {"scans": fetch_scan_stats(conn, args.scans)})
                add("routes", True, {
                    "history_hours": args.history_hours,
                    "routes": compute_route_deltas(
                        fetch_route_history(conn, args.history_hours), top=args.top
                    ),
                })
            finally:
                conn.close()
        except psycopg2.Error as exc:
            add("scans", False, {}, error=f"postgres: {str(exc).strip()}")
            add("routes", False, {}, error="postgres unavailable")

    try:
        add("consumer_lag", True, {"groups": fetch_lag(kafka)})
    except Exception as exc:  # noqa: BLE001
        add("consumer_lag", False, {}, error=str(exc).strip())
    try:
        add("dlq", True, fetch_topic_tail(kafka, kafka.dlq_topic, args.sample))
    except Exception as exc:  # noqa: BLE001
        add("dlq", False, {}, error=str(exc).strip())
    try:
        add("tombstones", True, fetch_topic_tail(kafka, kafka.deltas_topic, args.sample))
    except Exception as exc:  # noqa: BLE001
        add("tombstones", False, {}, error=str(exc).strip())

    return report, healthy


def _fmt_rial(value: Any) -> str:
    return f"{value:,}" if isinstance(value, int) else "–"


def render_text(report: dict[str, Any]) -> None:
    sections = report["sections"]
    print(f"FLIGHT PIPELINE STATUS — {report['generated_at']} (kafka={report['bootstrap']})")

    print("\n── Scans (latest) ── proposals vs unique flights ───────────────────")
    if sections["scans"]["ok"]:
        scans = sections["scans"]["scans"]
        if not scans:
            print("  (no data yet — the DB sink has not written any scans)")
        for scan in scans:
            print(
                f"  {str(scan['scan_ts'])[:19]}  supplier(s)={scan['suppliers']}  "
                f"proposals={scan['proposals']:>4}  unique_flights={scan['unique_flights']:>4}  "
                f"physical={scan['unique_physical_flights']:>4}  "
                f"price[{_fmt_rial(scan['min_price_rial'])} .. {_fmt_rial(scan['max_price_rial'])}]"
            )
    else:
        print(f"  ERROR: {sections['scans'].get('error')}")

    print("\n── Top routes: latest scan + min-price delta ──────────────────────")
    if sections["routes"]["ok"]:
        routes = sections["routes"]["routes"]
        if not routes:
            print(f"  (no price history within {sections['routes']['history_hours']}h)")
        for route in routes:
            delta = f"{route['delta_pct']:+.1f}%" if route["delta_pct"] is not None else "n/a"
            print(
                f"  {route['route']:<10} scans={route['scans']:>3}  "
                f"latest={str(route['latest_scan_ts'])[:19]}  "
                f"min={_fmt_rial(route['latest_min_price_rial'])} IRR  Δ={delta}"
            )
    else:
        print(f"  ERROR: {sections['routes'].get('error')}")

    print("\n── Consumer lag (service groups) ───────────────────────────────────")
    lag = sections["consumer_lag"]
    if lag["ok"]:
        for name, group in lag["groups"].items():
            for topic, info in group["topics"].items():
                print(f"  {name:<11} group={group['group']:<18} topic={topic}  "
                      f"TOTAL LAG={info['total_lag']}")
                for part in info["partitions"]:
                    committed = part["committed"] if part["committed"] is not None else "none"
                    print(f"      partition {part['partition']}: committed={committed}  "
                          f"log_end={part['log_end_offset']}  lag={part['lag']}")
    else:
        print(f"  ERROR: {lag.get('error')}")

    for label, key in (("DLQ (dead letters)", "dlq"), ("Tombstones (sold-out purges)", "tombstones")):
        section = sections[key]
        print(f"\n── {label} ──────────────────────────────────────────────────")
        if section["ok"]:
            if not section.get("exists"):
                print(f"  topic '{section['topic']}' does not exist yet")
            else:
                print(f"  topic='{section['topic']}'  total={section['total_messages']}  "
                      f"retained={section['retained_messages']}")
                for item in section["recent"]:
                    print(f"  ← {item['headers'].get('error', item['value'][:100])}")
        else:
            print(f"  ERROR: {section.get('error')}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--scans", type=int, default=5)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--history-hours", type=int, default=168)
    parser.add_argument("--sample", type=int, default=5)
    args = parser.parse_args(argv)

    report, healthy = build_report(KafkaSettings.from_env(), args)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        render_text(report)
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
