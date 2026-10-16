"""Guarded PostgreSQL seam for the planner (readmodel.py's discipline: one
honest module-level engine, guarded imports, never a hard dependency).

Arming chain — the planner serves the demo seed (seed.py) unless ALL hold:
  1. sqlalchemy + geoalchemy2 + pgvector + a driver import (models.py raises
     ImportError otherwise — caught here, not in models, so the ORM file
     stays a plain declaration);
  2. DATABASE_URL set (config.py — unset in frontend-only dev);
  3. ensure_schema() has succeeded once (extensions + DDL + first-boot seed).

Lazy on first request, not at import: a dev machine without Docker running
must still boot the BFF instantly (the same reason ensure_schema is not
called from main.py). Every failure logs ONE warning naming the mode it fell
back to, mirroring main.py's bootstrap trace.

The engine is sync + psycopg (INFRASTRUCTURE §2.5) and every BFF call goes
through asyncio.to_thread in services.retrieve_places — an async endpoint
must never block its loop on Neon's scale-to-zero cold start (~0.5–2 s,
INFRASTRUCTURE §2.3). pool_pre_ping covers the dropped-idle connection.
"""

from __future__ import annotations

import logging
import threading

from src.config import DATABASE_URL

try:  # ORM + extension types; ImportError anywhere ⇒ demo mode (arming chain §1).
    from geoalchemy2 import WKTElement
    from sqlalchemy import create_engine, func, select
    from sqlalchemy.orm import Session

    from src.itinerary import models, seed
except ImportError:  # frontend-only dev environment
    WKTElement = None  # type: ignore[assignment]
    create_engine = None  # type: ignore[assignment]
    Session = None  # type: ignore[assignment]
    models = None  # type: ignore[assignment]
    seed = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

engine = None if models is None or not DATABASE_URL else create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # survives Neon scale-to-zero / idle drops
    # Recycle proactively BELOW Neon's ~5-min idle drop so the common case is
    # a warm pooled connection, not a pre-ping that discovers a corpse (each
    # discovery costs a full remote reconnect — TLS handshake included).
    pool_size=10,
    max_overflow=20,
    pool_recycle=280,
    connect_args={"connect_timeout": 3},  # a down local PG must not hang the page
)
_armed = False  # flipped once ensure_schema() succeeds — read via armed()
_schema_lock = threading.Lock()  # single-flight for ensure_schema (see below)


def armed() -> bool:
    """True ⇒ live Postgres reads; False ⇒ demo seed (mock mode)."""
    return _armed


def ensure_schema() -> bool:
    """Idempotent DDL + first-boot seed. Extensions first (Geometry/Vector
    columns need them at CREATE TABLE time), then create_all (checkfirst),
    then load seed.KISH_DEMO when the table is empty — so a fresh `docker
    compose up -d postgres` is a complete, one-call setup.

    Single-flight (crm-monitoring thundering-herd pattern): fetch_places runs
    on asyncio.to_thread workers, so two concurrent first requests can enter
    here simultaneously; without the lock both would see count=0 on a fresh DB
    and race the seed into Place.slug's unique constraint — one thread's
    IntegrityError would then leave _armed stuck False (spurious demo mode)
    even though the schema + seed actually landed. threading.Lock, not
    asyncio.Lock, precisely because this executes on worker threads."""
    global _armed
    if _armed or engine is None:
        return _armed
    with _schema_lock:
        if _armed or engine is None:  # double-check: armed while we waited
            return _armed
        try:
            with engine.begin() as conn:  # extensions are cluster-wide, not transactional
                conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis")
                conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS vector")
            models.Base.metadata.create_all(engine)
            with Session(engine) as session:
                count = session.scalar(select(func.count()).select_from(models.Place))
                if not count:
                    _seed_from_catalog(session)
                    session.commit()
                    log.info("itinerary schema: seeded %d demo places (first boot)", len(seed.KISH_DEMO))
            _armed = True
            log.info("itinerary schema: armed (places table live)")
        except Exception as exc:  # noqa: BLE001 — any failure = honest demo mode
            log.warning("itinerary schema unavailable (%s: %s) — serving demo seed",
                        exc.__class__.__name__, exc)
    return _armed


def _seed_from_catalog(session) -> None:
    """seed.PlaceOut → Place rows. location via WKT ('POINT(lng lat)' — GeoJSON
    order, lng FIRST); embedding stays NULL (the enrichment pipeline backfills
    per notes §1ب — retrieval ranks NULL-embedding rows by popularity)."""
    for p in seed.KISH_DEMO:
        session.add(models.Place(
            slug=p.slug, name_fa=p.name_fa, category=p.category, city_code=p.city_code,
            description_fa=p.description_fa,
            location=WKTElement(f"POINT({p.lng} {p.lat})", srid=4326),
            est_visit_minutes=p.est_visit_minutes, popularity=p.popularity,
            has_ticket=p.has_ticket, redirect_url=p.redirect_url, lowest_price=p.lowest_price,
        ))


def fetch_places(city_code: str, limit: int, qvec: list[float] | None = None) -> list[seed.PlaceOut] | None:
    """Live read → PlaceOut seam (ST_Y/ST_X extracted here, once). None means
    "not armed" (caller falls back to the demo catalog); an empty list is a
    real answer (unknown city) and is returned as such.

    qvec set ⇒ cosine ranking over embedded rows, falling back to popularity
    when no row carries an embedding yet (pre-backfill); qvec None ⇒
    popularity ranking."""
    if not ensure_schema():
        return None
    stmt = select(
        models.Place,
        func.ST_Y(models.Place.location).label("lat"),
        func.ST_X(models.Place.location).label("lng"),
    ).where(models.Place.city_code == city_code)

    with Session(engine) as session:
        if qvec is not None:
            vec_rows = session.execute(
                stmt.where(models.Place.embedding.is_not(None))
                .order_by(models.Place.embedding.cosine_distance(qvec))
                .limit(limit)
            ).all()
            if vec_rows:
                return [_to_placeout(row) for row in vec_rows]
            log.info("itinerary: no embedded rows for %s — popularity ranking", city_code)
        rows = session.execute(
            stmt.order_by(models.Place.popularity.desc()).limit(limit)
        ).all()
    return [_to_placeout(row) for row in rows]


def _to_placeout(row) -> seed.PlaceOut:
    place, lat, lng = row
    return seed.PlaceOut(
        slug=place.slug, name_fa=place.name_fa, category=place.category,
        city_code=place.city_code, lat=float(lat), lng=float(lng),
        description_fa=place.description_fa or "",
        est_visit_minutes=place.est_visit_minutes, popularity=place.popularity,
        has_ticket=place.has_ticket, redirect_url=place.redirect_url,
        lowest_price=place.lowest_price,
    )
