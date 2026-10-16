"""Trust layer — build-time POI media enrichment (photos + sourced summaries).

The planner's POIs are real places, but the page used to render them text-only:
no photo, nothing a reader could verify. This module attaches the sourced media
fetched ONCE by scripts/enrich_poi_media.py and committed beside it
(data/enrichment_{city}.json + static/poi/*.{jpg,png,webp} + static/poi/SOURCES.md):

  image_url/alt/credit  a real Wikimedia Commons photo of the actual place,
                        self-hosted (offline-first doctrine, static/logos/
                        SOURCES.md) — never hotlinked, never stock/AI imagery
  summary_fa            the Persian Wikipedia extract — the *sourced* text;
                        description_fa stays the editorial one-liner
  source_url/name       the article the summary/photo came from («منبع» link)

Keyed BY SLUG over the catalog, never by LLM output: retrieval is deterministic
(narrative.py's system prompt forbids the model adding facts), so every POI —
demo seed and future Postgres rows alike — enriches identically at the router
seam. No network at request time: a missing/corrupt artifact degrades to the
text-only page (the db.py degradation contract), never an error.

Pure text helpers live here (not in the script) so the offline suite can pin
them through the src. namespace (scripts/ stays I/O-only).
"""

from __future__ import annotations

import html
import json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, fields, replace
from pathlib import Path

from src.itinerary.seed import PlaceOut

log = logging.getLogger(__name__)

# The sourced summary must stay a teaser, not a copy of the article
# (license + layout): ~200 chars ≈ the two clamped lines the UI shows.
SUMMARY_LIMIT = 200

# Fields PoiMedia carries onto PlaceOut (the six PlaceOut additions).
_MEDIA_FIELDS = ("image_url", "image_alt_fa", "image_credit",
                 "summary_fa", "source_url", "source_name")


@dataclass(frozen=True)
class PoiMedia:
    """One POI's sourced media — mirrors the artifact entry 1:1."""

    wiki_title: str
    lang: str = "fa"
    image_url: str | None = None       # site-relative: /static/poi/<slug>.jpg
    image_alt_fa: str | None = None    # content-bearing Persian alt text
    image_credit: str | None = None    # «عکس: … · ویکی‌مدیا کامانز · CC BY-SA 4.0»
    image_page: str | None = None      # Commons file page (attribution link)
    summary_fa: str | None = None      # fa-Wikipedia extract, trimmed
    source_url: str | None = None      # Wikipedia desktop article page
    source_name: str | None = None     # «ویکی‌پدیا» / «ویکی‌پدیا (انگلیسی)»


_POI_FIELDS = frozenset(f.name for f in fields(PoiMedia))

_CACHE: dict[str, dict[str, PoiMedia]] = {}


def _artifact_path(city_code: str) -> Path:
    """Per-city JSON in data/ beside this module (prompts/ holds the LLM text)."""
    return Path(__file__).parent / "data" / f"enrichment_{city_code}.json"


def _read_media(path: Path) -> dict[str, PoiMedia]:
    """Parse one artifact file. Any problem ⇒ {} + one warning — the page
    renders text-only rather than erroring on a corrupted checkout."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        media: dict[str, PoiMedia] = {}
        for slug, entry in data["pois"].items():
            if isinstance(entry, dict):
                media[slug] = PoiMedia(**{k: v for k, v in entry.items() if k in _POI_FIELDS})
        return media
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        log.warning("POI media artifact unreadable (%s: %s) — rendering without photos",
                    path.name, exc)
        return {}


def load_media(city_code: str) -> dict[str, PoiMedia]:
    """slug → PoiMedia for one city (cached; empty dict when unenriched)."""
    if city_code not in _CACHE:
        _CACHE[city_code] = _read_media(_artifact_path(city_code))
    return _CACHE[city_code]


def enrich_places(places: Sequence[PlaceOut], city_code: str) -> list[PlaceOut]:
    """Attach sourced media to catalog POIs, keyed by slug.

    Pure and idempotent: unmatched POIs (no Wikipedia article — malls, the
    pier) pass through unchanged and honestly render the fallback tile;
    symbolic stages only read lat/lng/category, so routing order is untouched.
    """
    media = load_media(city_code)
    enriched: list[PlaceOut] = []
    for place in places:
        m = media.get(place.slug)
        if m is None:
            enriched.append(place)
            continue
        extras = {f: getattr(m, f) for f in _MEDIA_FIELDS if getattr(m, f) is not None}
        enriched.append(replace(place, **extras) if extras else place)
    return enriched


# ── Pure text helpers (shared with scripts/enrich_poi_media.py) ──────────────

def strip_html(text: str) -> str:
    """Commons extmetadata values carry wiki/HTML markup (<a> authors etc.)."""
    text = re.sub(r"<[^>]+>", " ", text)
    return " ".join(html.unescape(text).split())


def trim_summary(text: str, limit: int = SUMMARY_LIMIT) -> str:
    """Whitespace-collapse + cut at a sentence end (then word boundary) —
    never mid-word, never past `limit`."""
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    cut = text[:limit]
    for sep in (".", "!", "?", "؟", "؛", "،"):  # fa + en sentence/clause ends
        idx = cut.rfind(sep)
        if idx >= limit // 2:
            return cut[: idx + 1]
    idx = cut.rfind(" ")
    return (cut[:idx] if idx > 0 else cut).rstrip() + "…"


def alt_from_extract(name_fa: str, extract: str) -> str:
    """Content-bearing alt: the place's name plus its first sentence."""
    snippet = " ".join(extract.split())
    for sep in (".", "!", "؟", "?", "؛", "،"):
        idx = snippet.find(sep)
        if 0 < idx <= 90:
            snippet = snippet[:idx]
            break
    snippet = snippet.strip(" ،؛.")
    # Lead sentences usually open with the article title («کشتی یونانی، یکی
    # از …») — drop it so the alt doesn't read «کشتی یونانی، کشتی یونانی، …».
    if snippet.startswith(name_fa):
        snippet = snippet[len(name_fa):].lstrip(" ،؛:.")
    if len(snippet) > 90:
        cut = snippet[:90]
        snippet = (cut[: cut.rfind(" ")] if " " in cut else cut).rstrip() + "…"
    return f"{name_fa}، {snippet}" if snippet else f"تصویر {name_fa}"


def is_free_license(short_name: str | None) -> bool:
    """Commons gate: only freely-licensed files may be redistributed by a
    public repo. Fair-use/non-free files are skipped (fallback tile instead)."""
    name = (short_name or "").strip().lower()
    if not name or "fair use" in name or "non-free" in name:
        return False
    return any(marker in name for marker in
               ("cc0", "cc by", "cc-by", "public domain", "pd", "gfdl", "attribution"))


def build_credit(artist_html: str | None, license_short: str | None) -> str:
    """The visible CC BY-SA attribution line (tile tooltip; full record in
    static/poi/SOURCES.md)."""
    who = strip_html(artist_html or "")
    if len(who) > 40:
        who = who[:37].rstrip() + "…"
    parts = [who or "ویکی‌مدیا کامانز", "ویکی‌مدیا کامانز"]
    if license_short:
        parts.append(license_short.strip())
    return "عکس: " + " · ".join(parts)
