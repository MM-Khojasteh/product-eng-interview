#!/usr/bin/env python
"""Offline tests for the trust layer — enrichment.py + the committed artifact.

Run:  python -m pytest tests/web/test_poi_enrichment.py -q   (pytest.ini sets
testpaths). Same hermetic discipline as test_itinerary.py: TOROB_FORCE_MOCK=1,
db.engine = None, and ZERO network — everything asserts against the committed
files (src/itinerary/data/enrichment_kish.json + static/poi/*) so artifact and
binaries can never drift apart unnoticed.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

os.environ.setdefault("TOROB_FORCE_MOCK", "1")  # must precede the main import

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from src.itinerary import db, enrichment, seed, services  # noqa: E402
from src.itinerary.seed import PlaceOut  # noqa: E402

# Hermetic suite: never touch a real Postgres even when one runs on localhost.
db.engine = None

# Enriched POI (fa article + photo) and the honest-fallback POI — the latter
# computed FROM the committed artifact so the fallback path stays pinned as
# coverage grows (self-skips on the unlikely full-coverage day).
MEDIA = enrichment.load_media("kish")
PHOTO_SLUG = "greek-ship"
FALLBACK_SLUG = next((p.slug for p in seed.KISH_DEMO if p.slug not in MEDIA), None)


def check(condition: bool, label: str) -> None:
    assert condition, label


# ---------------------------------------------------------------------------
# artifact ↔ seed ↔ binaries stay in sync (the build-time contract)
# ---------------------------------------------------------------------------

def test_artifact_keys_and_entries():
    media = enrichment.load_media("kish")
    check(bool(media), "kish artifact present and non-empty")
    demo_slugs = {p.slug for p in seed.KISH_DEMO}
    check(set(media) <= demo_slugs, "artifact keys ⊆ KISH_DEMO slugs")
    for slug, m in media.items():
        check(m.source_url and m.source_name, f"{slug}: source link present")
        check(m.summary_fa is None or 0 < len(m.summary_fa) <= 220,
              f"{slug}: summary bounded (None for en-sourced entries)")
        if m.image_url:
            check(m.image_url.startswith("/static/poi/"), f"{slug}: site-relative image path")
            check((ROOT / m.image_url.lstrip("/")).is_file(),
                  f"{slug}: image file committed next to the artifact")


def test_artifact_loader_degrades(tmp_path):
    bad = tmp_path / "enrichment_kish.json"
    bad.write_text("{ this is not json", encoding="utf-8")
    check(enrichment._read_media(bad) == {}, "corrupt artifact → empty map, no raise")
    check(enrichment.load_media("no-such-city") == {}, "unenriched city → empty map")


# ---------------------------------------------------------------------------
# enrich_places — pure, idempotent, honest fallback
# ---------------------------------------------------------------------------

def test_enrich_places_changes_media_fields_only():
    places = list(seed.demo_places("kish"))
    enriched = services.enrich_places(places, "kish")
    check([p.slug for p in enriched] == [p.slug for p in places], "order preserved")
    base = {p.slug: p for p in places}
    got = {p.slug: p for p in enriched}
    photoed = 0
    for slug, p in base.items():
        e = got[slug]
        check((e.name_fa, e.category, e.city_code, e.lat, e.lng, e.description_fa,
               e.est_visit_minutes, e.popularity, e.has_ticket, e.redirect_url,
               e.lowest_price) ==
              (p.name_fa, p.category, p.city_code, p.lat, p.lng, p.description_fa,
               p.est_visit_minutes, p.popularity, p.has_ticket, p.redirect_url,
               p.lowest_price), f"{slug}: enrichment touches media fields only")
        photoed += bool(e.image_url)
    check(photoed >= 1, "at least one demo POI carries a photo")
    check(services.enrich_places(enriched, "kish") == enriched, "idempotent")


def test_enrich_places_passes_unknown_slugs_through():
    alien = PlaceOut(slug="not-in-catalog", name_fa="غیرمعمولی",
                     category="park", city_code="kish", lat=1.0, lng=1.0)
    check(services.enrich_places([alien], "kish") == [alien],
          "no artifact entry ⇒ POI untouched (fallback tile path)")


def test_pipeline_order_untouched_by_enrichment():
    origin = (26.5350, 53.9800)
    places = list(seed.demo_places("kish"))
    base_plan = [[p.slug for p in services.route_day(c, origin=origin)]
                 for c in services.cluster_places(places, 3)]
    enriched_plan = [[p.slug for p in services.route_day(c, origin=origin)]
                     for c in services.cluster_places(
                         services.enrich_places(places, "kish"), 3)]
    check(base_plan == enriched_plan,
          "media never perturbs the symbolic stages (cluster/route order)")


def test_map_payload_stays_text_only():
    # Enrich an actual photoed POI, then prove the Leaflet feed drops media —
    # the map payload stays lean by design (popup photos are a later add).
    enriched = services.enrich_places([seed.KISH_DEMO[0]], "kish")
    payload = services.build_map_payload([enriched], center=(26.5, 53.9), zoom=12)
    blob = json.dumps(payload, ensure_ascii=False)
    for field in ("image_url", "summary_fa", "source_url", "image_credit"):
        check(field not in blob, f"map feed carries no {field}")


# ---------------------------------------------------------------------------
# pure helpers (the seams scripts/enrich_poi_media.py relies on)
# ---------------------------------------------------------------------------

def test_trim_summary_boundaries():
    check(enrichment.trim_summary("کوتاه", 50) == "کوتاه", "short text untouched")
    sentences = "یک. دو. سه. " * 30
    cut = enrichment.trim_summary(sentences, limit=50)
    check(len(cut) <= 51 and cut.endswith("."),
          "cut lands on a sentence end within the limit")
    words = "کلمه‌هایی که هیچ نقطه‌ای ندارند و باید مرز واژه رعایت شود " * 10
    cut2 = enrichment.trim_summary(words, limit=50)
    check(len(cut2) <= 51 and cut2.endswith("…") and " " in cut2.strip("…"),
          "no separators ⇒ word boundary + ellipsis, never mid-word")


def test_is_free_license_gates_redistribution():
    check(enrichment.is_free_license("CC BY-SA 4.0"), "CC BY-SA redistributable")
    check(enrichment.is_free_license("CC0"), "CC0 redistributable")
    check(enrichment.is_free_license("Public domain"), "PD redistributable")
    check(not enrichment.is_free_license("Fair use"), "fair use rejected")
    check(not enrichment.is_free_license("Non-free"), "non-free rejected")
    check(not enrichment.is_free_license("") and not enrichment.is_free_license(None),
          "missing license rejected (unknown ⇒ not free)")


def test_alt_and_credit_helpers():
    alt = enrichment.alt_from_extract(
        "کشتی یونانی", "کشتی یونانی، یکی از جاذبه‌های گردشگری جزیره کیش است.")
    check(alt == "کشتی یونانی، یکی از جاذبه‌های گردشگری جزیره کیش است",
          "article-title lead is not duplicated in the alt text")
    credit = enrichment.build_credit('<a href="x">Ali Photographer</a>', "CC BY-SA 4.0")
    check("Ali Photographer" in credit and "CC BY-SA 4.0" in credit
          and credit.startswith("عکس:") and "<" not in credit,
          "credit carries the artist (HTML-stripped) and the license")


# ---------------------------------------------------------------------------
# GET /itinerary/kish — the trust layer, end to end in demo mode
# ---------------------------------------------------------------------------

client = TestClient(main.app)


def test_itinerary_page_renders_trust_layer():
    html = client.get("/itinerary/kish").text
    check("منبع: ویکی‌پدیا" in html, "sourced summary carries its «منبع» link")
    check(f"/static/poi/{PHOTO_SLUG}.jpg" in html, "the real photo renders (self-hosted)")
    check("openstreetmap.org/?mlat=" in html, "external map deep link per stop")
    # the why-trust copy moved behind the «؟» popover (ds-info-pop) — pin the
    # panel AND the OSM/Wikipedia sourcing sentence it must still carry
    check("این برنامه چگونه ساخته شده؟" in html and "ds-info-pop__panel" in html,
          "why-trust popover present")
    check("مختصات ثبت‌شده از OpenStreetMap و ویکی‌پدیا" in html,
          "popover still makes the sourcing claim")
    check('title="عکس:' in html, "photo tiles still carry their CC credit tooltip")


def test_user_review_links_follow_the_tickets():
    html = client.get("/itinerary/kish").text
    # ticketed POIs → the supplier's attraction page (real user comments live
    # there); free POIs → no comments link, because there is nothing honest
    # to link to
    ticketed = next(p.slug for p in seed.KISH_DEMO if p.has_ticket)
    free = next(p.slug for p in seed.KISH_DEMO if not p.has_ticket)
    row = re.search(r'data-place-slug="' + ticketed + r'".*?</li>', html, re.DOTALL)
    check(row is not None and "نظرات کاربران در سایت فروشنده" in row.group(0),
          "ticketed stop links out to the supplier's user reviews")
    row = re.search(r'data-place-slug="' + free + r'".*?</li>', html, re.DOTALL)
    check(row is not None and "نظرات کاربران" not in row.group(0),
          "free stop carries no reviews link (nothing honest to point at)")


def test_photo_img_tag_is_accessible_and_fallback_honest():
    html = client.get("/itinerary/kish").text
    img = re.search(r'<img[^>]+src="[^"]*greek-ship\.jpg[^"]*"[^>]*>', html)
    check(img is not None, "photo tile is an <img> (not a background)")
    tag = img.group(0)
    check('alt="' in tag and len(re.search(r'alt="([^"]*)"', tag).group(1)) > 3,
          "content-bearing alt text")
    check('loading="lazy"' in tag and 'width="100"' in tag, "lazy + fixed size (no CLS)")
    # the honest fallback: a POI with no article renders the initial tile, and
    # NO photo/src that could be mistaken for one
    if FALLBACK_SLUG is None:
        return  # every POI enriched — no fallback row left to pin
    row = re.search(r'data-place-slug="' + FALLBACK_SLUG + r'".*?</li>', html, re.DOTALL)
    check(row is not None, f"fallback POI ({FALLBACK_SLUG}) row found")
    check("static/poi/" not in row.group(0), "no fabricated photo on unenriched POI")
    check('aria-hidden="true"' in row.group(0), "decorative initial tile marked aria-hidden")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all POI enrichment tests passed")
