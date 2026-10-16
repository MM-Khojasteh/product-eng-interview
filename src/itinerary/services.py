"""Neuro-symbolic itinerary pipeline (notes «AI Itinerary Planner» §2):

  retrieve_places   گام ۱  pgvector interest match / popularity ranking
  cluster_places    گام ۱  K-Means — one geographic cluster per day
  route_day         گام ۲  greedy nearest-neighbour TSP within a day
  generate_narrative گام ۳  LLM voice over the mathematically-sound plan

The naive "یک برنامه ۳ روزه برای کیش بده" prompt hallucinates and produces
unwalkable routes (notes' own diagnosis); here the SYMBOLIC stages own truth
(places, distances, order — deterministic, testable) and the LLM only adds
voice. It is prompted to change no facts and, for ticketed places, to send
users to the «تأمین‌کنندگان» handoff — Torob sells nothing itself.

Degradation contract (the planner's mock/live seam): every optional
collaborator is guarded — sklearn missing ⇒ longitude banding, OpenAI
missing/failed ⇒ build_fallback_narrative(), embeddings missing ⇒ popularity
ranking. Given the same PlaceOut inputs the symbolic stages are pure and
deterministic (random_state pinned), so tests pin behaviour, not mocks.

Module layout (one concern per module; this file is the stable public seam):
    openai_client.py  guarded SDK import + lazy shared AsyncOpenAI
    geo.py            گام ۱ clustering + گام ۲ greedy TSP
    retrieval.py      گام ۱ retrieval (interests → vectors → ranked places)
    narrative.py      گام ۳ LLM narrative + deterministic fallback
                      (system prompt: prompts/narrative_system.txt)
    enrichment.py     trust layer — build-time POI photos + sourced summaries
                      (artifact: data/enrichment_{city}.json, fetched by the script)
    map_payload.py    view payload (Leaflet feed + DAY_COLORS)
    services.py       THIS module — facade re-export (router + tests import here)
"""

from __future__ import annotations

from .enrichment import PoiMedia, enrich_places, load_media  # noqa: F401  (facade re-export)
from .geo import EARTH_RADIUS_KM, cluster_places, route_day  # noqa: F401  (facade re-export)
from .map_payload import DAY_COLORS, build_map_payload  # noqa: F401  (facade re-export)
from .narrative import (  # noqa: F401  (facade re-export)
    NARRATIVE_SYSTEM_PROMPT,
    DayNarrative,
    Narrative,
    build_fallback_narrative,
    generate_narrative,
    parse_narrative,
)
from .retrieval import embed_interests, retrieve_places  # noqa: F401  (facade re-export)

__all__ = [
    "EARTH_RADIUS_KM",
    "cluster_places",
    "route_day",
    "DAY_COLORS",
    "build_map_payload",
    "PoiMedia",
    "enrich_places",
    "load_media",
    "NARRATIVE_SYSTEM_PROMPT",
    "DayNarrative",
    "Narrative",
    "build_fallback_narrative",
    "generate_narrative",
    "parse_narrative",
    "embed_interests",
    "retrieve_places",
]
