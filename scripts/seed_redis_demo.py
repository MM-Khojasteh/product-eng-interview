"""Seed the Redis hot cache with deterministic demo quotes — no suppliers, no
Kafka, no Postgres. Exercises the FULL live read path (cache-first cards, OOB
pulse, click-time Compare modal, tombstones) with data shaped exactly like
pipelines/liveness.py's quote_value payloads.

    python scripts/seed_redis_demo.py THR KIH 14050713            # seed once
    python scripts/seed_redis_demo.py THR KIH 14050713 --watch    # live demo
    python scripts/seed_redis_demo.py THR KIH 14050713 --flights 12 --ttl 300

--watch rewrites the hash every ~4 s with price drift, a late-arriving flight
(chunk-streaming demo: its card INSERTS mid-view via the OOB pulse) and an
eventual sellout (HDEL-equivalent rewrite: the card VANISHES in place).
Ctrl-C stops; the hash then expires after --ttl seconds like a real cadence gap.

The BFF must run in live mode (jdatetime+redis importable, TOROB_FORCE_MOCK
unset). Field values = liveness.quote_value's shape; hash fields are
"{supplier}:{physical_flight_key}" so two agencies selling one physical flight
coexist — the cross-supplier dedup demo.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # Windows consoles default to cp1252 — the ✓/→ glyphs below need UTF-8
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

import redis

try:
    import jdatetime
except ImportError:
    print("pip install jdatetime  (Jalali date → Redis key conversion)")
    sys.exit(1)

from pipelines.common.dto import physical_flight_key

AIRLINES = ["IR", "W5", "EP", "B9", "QB", "YK", "NV"]
SUPPLIERS = ["flytoday", "alibaba", "mrbilit"]


def gregorian(datenum: str) -> str:
    return jdatetime.date(
        int(datenum[:4]), int(datenum[4:6]), int(datenum[6:])
    ).togregorian().isoformat()


def build_quotes(origin: str, destination: str, datenum: str, count: int,
                 rng: random.Random) -> list[dict]:
    quotes: list[dict] = []
    base_rial = 9_000_000 + rng.randint(0, 14) * 1_000_000
    for i in range(count):
        airline = AIRLINES[i % len(AIRLINES)]
        dep_total = (6 + i) * 60 + rng.choice((0, 15, 30, 45))
        dep_hm = f"{dep_total // 60:02d}:{dep_total % 60:02d}"
        duration = 70 + rng.randint(0, 12) * 5
        flight_no = f"{airline}{rng.randint(100, 899)}"
        pkey = physical_flight_key(airline, flight_no, dep_hm, origin, destination)
        day = f"{gregorian(datenum)}T"
        for j, supplier in enumerate(rng.sample(SUPPLIERS, rng.randint(2, 3))):
            quotes.append({
                "supplier": supplier,
                "physical_flight_key": pkey,
                "airline_code": airline,
                "flight_no": flight_no,
                "dep_local": f"{day}{dep_hm}:00+03:30",
                "arr_local": f"{day}{(dep_total + duration) // 60 % 24:02d}:"
                             f"{(dep_total + duration) % 60:02d}:00+03:30",
                "journey_duration_minutes": duration,
                "price_total_rial": base_rial + i * 400_000 + j * rng.randint(1, 9) * 100_000,
                # one ghost-priced fare (i=1, cheapest slot) so the
                # unit_verified-never-undercuts guard is visible in the UI
                "unit_verified": not (i == 1 and j == 0),
                "dep_utc": None, "arr_utc": None,
                "capacity_remaining": rng.choice([9, 7, 5, 4, 3, None, None, 0]) if j else 9,
                "is_charter": rng.random() < 0.25,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            })
    return quotes


def write_hash(rds: redis.Redis, origin: str, destination: str, datenum: str,
               quotes: list[dict], ttl: int) -> None:
    """Full-state rewrite per tick (DELETE + HSETs in one pipeline) — the §2.6
    'coalesce, then write' discipline. The DELETE matters: HSET alone never
    removes members, so a sellout would leave the stale fare in the hash
    (pipelines/liveness.py achieves the same via tombstone HDELs)."""
    key = f"quote:{origin}-{destination}:{gregorian(datenum)}"
    pipe = rds.pipeline(transaction=False)
    pipe.delete(key)
    for quote in quotes:
        pipe.hset(key, f"{quote['supplier']}:{quote['physical_flight_key']}",
                  json.dumps(quote, ensure_ascii=False))
    pipe.expire(key, ttl)
    pipe.execute()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("origin"), parser.add_argument("destination")
    parser.add_argument("datenum", help="Jalali date, digits only — e.g. 14050713")
    parser.add_argument("--flights", type=int, default=8)
    parser.add_argument("--ttl", type=int, default=120)
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--watch", action="store_true",
                        help="keep rewriting: price drift + a late flight + a sellout")
    args = parser.parse_args()

    try:
        rds = redis.Redis.from_url(args.redis_url, decode_responses=True)
        rds.ping()
    except redis.exceptions.RedisError as exc:
        print(f"redis unreachable at {args.redis_url}: {exc}\n→ docker compose up -d")
        return 1

    origin, destination = args.origin.upper(), args.destination.upper()
    datenum = args.datenum
    rng = random.Random(f"{origin}-{destination}-{datenum}")
    quotes = build_quotes(origin, destination, datenum, args.flights, rng)
    late = build_quotes(origin, destination, datenum, 1,
                        random.Random(f"late-{origin}-{destination}-{datenum}"))

    write_hash(rds, origin, destination, datenum, quotes, args.ttl)
    physical = {q["physical_flight_key"] for q in quotes}
    print(f"✓ seeded quote:{origin}-{destination}:{gregorian(datenum)}: "
          f"{len(quotes)} quotes · {len(physical)} physical flights · TTL {args.ttl}s")
    print(f"→ open: http://127.0.0.1:8000/search/flights?origin={origin}"
          f"&destination={destination}&date={datenum}&passengers=1")

    if not args.watch:
        return 0

    print("watching: drift every 4 s · late flight at t≈12 s · sellout at t≈24 s "
          "(Ctrl-C to stop)")
    victim = sorted(physical)[0]
    start = time.monotonic()
    try:
        while True:
            time.sleep(4)
            elapsed = time.monotonic() - start
            captured = datetime.now(timezone.utc).isoformat()
            live = [dict(q, captured_at=captured,
                         price_total_rial=q["price_total_rial"]
                         + rng.choice((-2, -1, 0, 0, 1, 2)) * 100_000)
                    for q in quotes if q["physical_flight_key"] != victim or elapsed < 24]
            if 12 <= elapsed:
                live += [dict(q, captured_at=captured) for q in late]  # the late chunk
            write_hash(rds, origin, destination, datenum, live, args.ttl)
            events = []
            if 12 <= elapsed < 16:
                events.append("late flight added — its card INSERTS in place")
            if 24 <= elapsed < 28:
                events.append("one flight sold out — its card VANISHES in place")
            print(f"  t+{elapsed:4.0f}s: {len(live)} quotes"
                  + (" · " + "; ".join(events) if events else ""))
    except KeyboardInterrupt:
        print("\nstopped — the hash expires naturally within the TTL")
    return 0


if __name__ == "__main__":
    sys.exit(main())
