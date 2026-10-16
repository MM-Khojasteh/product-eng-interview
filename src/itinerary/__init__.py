"""AI Itinerary Planner — the post-booking travel companion (notes
«Itinerary Planner/AI Itinerary Planner.md»: turn Torob from a ticket
comparator into a «همراه سفر», monetising high-margin activities through the
same CPC aggregator contract as flights).

Module map (dependency order, no cycles):
  seed      leaf — PlaceOut transport record + Kish demo catalog
  models    Place ORM (PostGIS POINT + pgvector 1536 + monetization fields)
  db        guarded Postgres seam (engine → ensure_schema → fetch_places)
  services  the pipeline: retrieve → cluster (K-Means) → route (greedy TSP)
            → narrate (Async OpenAI, deterministic fallback)
  router    GET /itinerary/{city_code} — renders itinerary/planner.html

Every stage degrades, never breaks: no DATABASE_URL ⇒ demo seed, no
sklearn ⇒ longitude banding, no OpenAI ⇒ fallback narrative. Torob itself
sells nothing — «تأمین‌کنندگان» buttons deeplink to partners.
"""

from src.itinerary import router  # noqa: F401  — main.py registers router.router
