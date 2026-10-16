#!/usr/bin/env python
"""Offline tests for the AI Itinerary Planner (no Postgres/OpenAI/network).

Run:  python -m pytest tests/web/test_itinerary.py -q   (or plain `pytest` —
pytest.ini sets testpaths). TOROB_FORCE_MOCK=1 is pinned for the shared app
import; the planner's own seam is pinned below (db.engine = None), so a dev
machine with DATABASE_URL + a running Postgres still tests the demo path —
same hermetic discipline as test_frontend.py's readmodel.rds = None.

The pipeline's symbolic stages are pure, so these tests pin BEHAVIOUR, not
mocks: clustering/routing invariants hold identically under K-Means and the
longitude-banding fallback (which one runs depends on whether scikit-learn
is installed — the assertions below are deliberately engine-agnostic).
"""

from __future__ import annotations

import asyncio
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
from src.config import Settings  # noqa: E402
from src.itinerary import db, seed, services  # noqa: E402
from src.itinerary.seed import PlaceOut  # noqa: E402

# Hermetic suite: never touch a real Postgres even when one runs on localhost
# with DATABASE_URL exported — pin the demo path before the first request.
db.engine = None


def _await(coro):
    return asyncio.run(coro)


def check(condition: bool, label: str) -> None:
    assert condition, label


# ---------------------------------------------------------------------------
# Fixtures — synthetic POIs on a west→east line (cluster/route ground truth)
# ---------------------------------------------------------------------------

def _poi(slug: str, lng: float, lat: float = 26.5, **overrides) -> PlaceOut:
    kwargs = {"slug": slug, "name_fa": slug, "category": "attraction", "city_code": "kish",
              "lat": lat, "lng": lng}
    kwargs.update(overrides)
    return PlaceOut(**kwargs)


LINE = [_poi(f"p{i}", 53.90 + i * 0.01) for i in range(9)]  # 53.90 … 53.98


# ---------------------------------------------------------------------------
# گام ۱ — cluster_places
# ---------------------------------------------------------------------------

def test_cluster_places_partitions_every_poi():
    for days in (2, 3, 4):
        clusters = services.cluster_places(LINE, days)
        flat = [p for cluster in clusters for p in cluster]
        check(len(flat) == len(LINE), f"day {days}: every POI kept exactly once")
        check(0 < len(clusters) <= days, f"day {days}: cluster count within 1..days")


def test_cluster_places_orders_days_west_to_east():
    clusters = services.cluster_places(LINE, 3)
    centroids = [sum(p.lng for p in c) / len(c) for c in clusters]
    check(centroids == sorted(centroids), "day sweep runs west → east")
    # each cluster is itself contiguous in longitude (no criss-cross days)
    for cluster in clusters:
        lngs = [p.lng for p in cluster]
        check(max(lngs) - min(lngs) < 0.05, "clusters stay geographic")


def test_cluster_places_degenerate_cases():
    check(services.cluster_places([], 3) == [], "no POIs → no days")
    one = services.cluster_places(LINE[:1], 3)
    check([p.slug for c in one for p in c] == ["p0"], "single POI → single day")
    # fewer POIs than days → every POI still planned, no empty days emitted
    two = services.cluster_places(LINE[:2], 5)
    check(len([c for c in two if c]) == 2, "≤ days POIs → no empty day sections")


def test_cluster_places_deterministic():
    check(services.cluster_places(LINE, 3) == services.cluster_places(LINE, 3),
          "same input → same clustering (random_state pinned)")


# ---------------------------------------------------------------------------
# گام ۲ — route_day (greedy TSP)
# ---------------------------------------------------------------------------

def test_route_day_starts_near_origin_and_nearest_neighbours():
    routed = services.route_day(LINE, origin=(26.5, 53.89))  # start west of everything
    check(routed[0].slug == "p0", "starts at the POI nearest the origin")
    lngs = [p.lng for p in routed]
    check(lngs == sorted(lngs), "on a west line the greedy sweep is monotonic")
    check(sorted(p.slug for p in routed) == sorted(p.slug for p in LINE),
          "routing reorders, never drops")


def test_route_day_short_clusters_untouched():
    check(services.route_day(LINE[:1]) == LINE[:1], "singleton day unchanged")
    pair = LINE[:2]
    check(services.route_day(pair) == list(pair), "pair returned as-is (no choice to make)")


def test_route_day_avoids_the_cross_island_ping_pong():
    # 3 POIs east + 1 far west: from an eastern origin the greedy walk visits
    # the eastern trio first — the Kish "don't cross the island twice" rule.
    cluster = [_poi("e1", 54.00), _poi("e2", 54.01), _poi("e3", 54.02), _poi("w1", 53.90)]
    routed = services.route_day(cluster, origin=(26.5, 54.00))
    check(routed[0].slug == "e1", "starts nearest origin")
    check(routed[-1].slug == "w1", "the far-west outlier is visited last, not first")


# ---------------------------------------------------------------------------
# گام ۳ — narrative (parse + deterministic fallback; LLM path needs a key)
# ---------------------------------------------------------------------------

def test_parse_narrative_tolerates_real_llm_output():
    text = (
        "سلام! برنامهٔ شما آماده‌ست.\n"  # preamble junk before first header
        "روز ۱ | طلوع در ساحل غربی\n"
        "*صبح را با کشتی یونانی شروع کن…* برای بلیت روی دکمهٔ «تأمین‌کنندگان» بزن.\n"
        "\n"
        "## پاراگراف دوم\n"
        "عصر به مرکز خرید برو.\n"
        "روز ۳ | شرق جزیره\n"  # model skipped روز ۲ — parser must not desync
        "روز کامل تفریحات آبی است.\n"
    )
    sections = services.parse_narrative(text, expected_days=2)
    check(len(sections) == 2, "headers define sections regardless of numbering gaps")
    check(sections[0].day_no == 1 and sections[1].day_no == 2,
          "day numbers re-sequenced locally")
    check(sections[0].title == "طلوع در ساحل غربی", "title after | captured")
    check(len(sections[0].paragraphs) == 2, "blank lines split paragraphs")
    check("تأمین‌کنندگان" in sections[0].paragraphs[0], "CTA sentence survives parsing")
    check(not sections[0].paragraphs[0].startswith("*"), "markdown bullets stripped")


def test_fallback_narrative_carries_the_aggregator_cta():
    ticketed = _poi("ocean", 54.02, has_ticket=True, redirect_url="https://partner.example/ocean",
                    lowest_price=1_200_000)
    plain = _poi("beach", 54.03)
    plan = [[plain, ticketed]]
    narrative = services.build_fallback_narrative(plan, "کیش")
    check(narrative.source == "fallback", "source tells the truth")
    body = " ".join(narrative.sections[0].paragraphs)
    check("تأمین‌کننده" in body, "ticketed day teaches the handoff mark")
    check("ترب فقط مقایسه می‌کند" in body, "aggregator honesty line present")
    title = narrative.sections[0].title
    check("دیدنی" in title, "fallback title themes the day's categories")
    check(" از " not in title and " تا " not in title,
          "no mechanical first→last stop-list title")


def test_generate_narrative_degrades_offline(monkeypatch):
    # Hermetic even when the dev shell exports a real key (the local .env
    # carries one) — pin the seam off for this test.
    monkeypatch.setattr("src.itinerary.narrative.OPENAI_API_KEY", None)
    day = services.route_day(LINE[:3], origin=(26.5, 53.9))
    narrative = _await(services.generate_narrative([day], city_name_fa="کیش"))
    check(narrative.source == "fallback", "no OPENAI_API_KEY → deterministic text")
    check(len(narrative.sections) == 1, "one day in → one section out")
    check(_await(services.generate_narrative([], city_name_fa="کیش")).sections == (),
          "empty plan → empty narrative, no LLM call")


# ---------------------------------------------------------------------------
# config — the gateway seam (OPENAI_BASE_URL ⇒ any OpenAI-compatible provider)
# ---------------------------------------------------------------------------

def test_settings_parses_gateway_overrides(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
    monkeypatch.setenv("TOROB_ITINERARY_MODEL", "google/gemini-3.1-pro-preview")
    settings = Settings.from_env()
    check(settings.openai_base_url == "https://openrouter.ai/api/v1",
          "base URL override reaches the LLM seam")
    check(settings.itinerary_model == "google/gemini-3.1-pro-preview",
          "model override reaches the LLM seam")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    check(Settings.from_env().openai_base_url is None,
          "unset base URL ⇒ the SDK default endpoint")


# ---------------------------------------------------------------------------
# view payload — the timeline and the map must agree
# ---------------------------------------------------------------------------

def test_build_map_payload_matches_timeline_order():
    plan = [services.route_day(LINE[:4], origin=(26.5, 53.9)),
            services.route_day(LINE[4:], origin=(26.5, 53.95))]
    payload = services.build_map_payload(plan, center=(26.5, 53.98), zoom=12)
    check(len(payload["days"]) == 2, "one map day per routed day")
    first = payload["days"][0]
    check(first["color"] == services.DAY_COLORS[0], "day 1 gets the brand color")
    check([p["stop"] for p in first["places"]] == [1, 2, 3, 4], "stops numbered from 1")
    check(first["path"] == [[p.lat, p.lng] for p in plan[0]],
          "polyline follows the TSP order (map == timeline)")
    check(payload["days"][1]["color"] == services.DAY_COLORS[1], "day colors distinct")


def test_build_map_payload_carries_monetization_fields():
    ticketed = _poi("pier", 54.0, has_ticket=True, redirect_url="https://partner.example/p",
                    lowest_price=900_000)
    payload = services.build_map_payload([[ticketed]], center=(26.5, 54.0), zoom=12)
    place = payload["days"][0]["places"][0]
    check(place["redirect_url"] == "https://partner.example/p", "deeplink in payload")
    check(place["lowest_price"] == 900_000, "کف قیمت in payload")
    check(place["category"] == seed.CATEGORY_LABELS_FA["attraction"],
          "category pre-labeled in Persian")


# ---------------------------------------------------------------------------
# GET /itinerary/{city_code} — the page, end to end in demo mode
# ---------------------------------------------------------------------------

client = TestClient(main.app)


def test_itinerary_page_renders_plan_and_map_feed():
    response = client.get("/itinerary/kish")
    check(response.status_code == 200, "known city renders")
    html = response.text
    check("کشتی یونانی" in html, "demo POIs render")
    check("تأمین‌کنندگان" in html, "aggregator handoff button present")
    check("target=\"_blank\"" in html and "noopener" in html,
          "handoffs are external + isolated")
    check('id="itinerary-data"' in html, "map JSON feed embedded")
    check("/static/vendor/leaflet/leaflet.js" in html, "vendored Leaflet (no CDN)")
    # the map module lives in static/js/itinerary.js since the refactor
    # extraction — its options are asserted on the asset (rendered into the
    # page via the deferred script tag checked below)
    map_js = (ROOT / "static" / "js" / "itinerary.js").read_text(encoding="utf-8")
    check("preferCanvas: true" in map_js and "zoomSnap: 0.2" in map_js,
          "map options follow the strict map pattern")
    check("/static/js/itinerary.js" in html, "map module wired into the page")
    check("OpenStreetMap" in html, "OSM attribution kept")
    check('id="itinerary-daybar"' in html, "sticky day switcher present")
    check('id="itinerary-legend"' not in html,
          "map-header legend removed (day switcher is the single source of truth)")
    # the «؟» explainer popover (ds-info-pop) — methodology/trust copy moved
    # behind the trigger; the page keeps ONE visible meta row
    check('id="itin-how-built"' in html and "ds-info-pop__panel" in html,
          "how-built popover present")
    check("این برنامه چگونه ساخته شده؟" in html, "popover title renders")
    check("/static/js/info_pop.js" in html, "popover behaviour wired")
    check("جمع کف قیمت تفریحات" in html, "budget roll-up renders")
    check("static/logos/agencies/alibaba.png" in html,
          "supplier logo tile is the clickable handoff (ticket-part idiom)")
    check('aria-label="خرید بلیت از سایت تأمین‌کننده"' in html,
          "icon-only handoff stays labelled for screen readers")
    # the seller-summary touchpoint (capture PDP anatomy: ds-price-highlight
    # + ds-seller-summary + ds-seller-list) — the budget card carries the
    # «تأمین‌کنندگان» handoff the narrative names, opening the breakdown modal
    check("مشاهده و خرید از تأمین‌کنندگان" in html,
          "budget card carries the supplier touchpoint")
    check('data-modal-open="#suppliers-modal"' in html and 'id="suppliers-modal"' in html,
          "touchpoint wired to the suppliers breakdown modal")
    check('data-modal-root' in html and 'data-modal-close' in html,
          "static-modal behaviour contract present (open/close)")
    check("/static/js/static_modal.js" in html, "static-modal script wired")
    check("علی‌بابا" in html, "supplier named from the AGENCIES registry")
    check('از سایت علی‌بابا"' in html,
          "modal handoff labels name place + supplier (screen-reader parity)")


def test_itinerary_map_feed_is_valid_json():
    html = client.get("/itinerary/kish").text
    match = re.search(
        r'<script type="application/json" id="itinerary-data">(.*?)</script>', html, re.DOTALL)
    check(match is not None, "feed script tag found")
    payload = json.loads(match.group(1))
    check(payload["center"] == [26.5350, 53.98], "Kish center")
    check(len(payload["days"]) == 3, "default 3 days")
    stops = [p for day in payload["days"] for p in day["places"]]
    check(len(stops) == len(seed.KISH_DEMO), "every demo POI planned exactly once")
    check(all(day["path"] == [[p["lat"], p["lng"]] for p in day["places"]]
              for day in payload["days"]),
          "each polyline equals its day's stop order")


def test_itinerary_days_query_and_interests():
    check(client.get("/itinerary/kish?days=2").status_code == 200, "days=2 ok")
    check(client.get("/itinerary/kish?interests=پارک آبی").status_code == 200,
          "interests flow through the keyword path")
    check(client.get("/itinerary/kish?days=7").status_code == 422, "days capped at 5")


def test_itinerary_unknown_city_404():
    check(client.get("/itinerary/mars").status_code == 404, "registry city only")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("all itinerary tests passed")
