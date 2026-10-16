"""AI Itinerary Planner — the Place entity (PostGIS geography + pgvector).

The BFF's first SQLAlchemy surface. The flight side deliberately has no ORM:
its read model is the Redis route-day hash (readmodel.py) with frozen
dataclasses at the view seam (src/models.py). Places are the opposite
shape of data — SLOW-moving entities a POI changes weekly at most, per-city
and geo/vector-queryable — which is exactly where INFRASTRUCTURE.md's
architecture puts PostgreSQL ("cold + entities"). Hence the planner owns the
BFF's declarative base: post-cutover flight entities should import THIS
Base so one metadata.create_all() stays authoritative.

Two extension-backed columns (notes «AI Itinerary Planner» §4):
  location   PostGIS POINT(4326) — the map + K-Means/routing feed (ST_Y/ST_X
             are extracted once at the retrieval seam, like toman_from_rial).
  embedding  pgvector Vector(1536) — text-embedding-3-small summaries of
             description_fa. NULL until the enrichment pipeline (notes §1ب)
             backfills them; retrieval treats NULL as "rank by popularity",
             never as an error.

Monetization (notes §1ج — the aggregator contract): Torob SELLS NOTHING.
`has_ticket` marks places needing an entry/activity ticket; those rows carry
`redirect_url` (affiliate deeplink to a ticket partner) and `lowest_price`
(Toman, کف قیمت across partners) — the itinerary UI renders the
«تأمین‌کنندگان» handoff exactly like flight deeplinks (seller_row.html):
target="_blank", rel="noopener noreferrer", purchase on the partner's site.
"""

from __future__ import annotations

from geoalchemy2 import Geometry
from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, Boolean, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# text-embedding-3-small output width — services.embed_interests() must
# produce vectors of exactly this rank or cosine_distance raises.
EMBEDDING_DIMENSIONS = 1536


class Base(DeclarativeBase):
    """Shared declarative base for the BFF's (future) Postgres entities."""


class Place(Base):
    """One point of interest (کشتی یونانی، پارک آبی اوشن، …) in one city."""

    __tablename__ = "places"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Stable identity for deeplinks/seeding (unique across cities).
    slug: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    name_fa: Mapped[str] = mapped_column(String(200), nullable=False)
    # Free-form but constrained vocabulary — seed.CATEGORY_LABELS_FA maps
    # these to the Persian chip the UI renders.
    category: Mapped[str] = mapped_column(String(40), nullable=False)
    city_code: Mapped[str] = mapped_column(String(16), nullable=False)  # 'kish'
    description_fa: Mapped[str | None] = mapped_column(Text)

    # geography column; queries go through ST_Y/ST_X at the seam (db.py).
    location = mapped_column(
        Geometry(geometry_type="POINT", srid=4326, spatial_index=True),
        nullable=False,
    )
    embedding = mapped_column(Vector(EMBEDDING_DIMENSIONS))

    # Planning inputs (services.route_day / generate_narrative digest):
    est_visit_minutes: Mapped[int] = mapped_column(Integer, default=90)
    popularity: Mapped[float] = mapped_column(Float, default=0.5)  # 0..1 retrieval prior

    # Monetization — the aggregator redirect contract (see module docstring).
    has_ticket: Mapped[bool] = mapped_column(Boolean, default=False)
    redirect_url: Mapped[str | None] = mapped_column(String(500))  # required when has_ticket
    lowest_price: Mapped[int | None] = mapped_column(BigInteger)   # Toman, کف قیمت

    __table_args__ = (
        # The city feed: every page view filters city_code, ranks popularity.
        Index("ix_places_city_popularity", "city_code", "popularity"),
        # ANN index for «جای دنج و عاشقانه»-style interest queries. HNSW over
        # ivfflat: no training step, so seeding a few rows then growing
        # (the Kish playbook → Mashhad/Istanbul) needs no index rebuild.
        Index(
            "ix_places_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
        # (location gets its GiST from Geometry(spatial_index=True))
    )
