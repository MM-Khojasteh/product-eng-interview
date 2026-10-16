"""Demo catalog for the planner — Kish MVP (notes «AI Itinerary Planner» §جمع‌بندی:
pilot city = Kish, ~10 curated POIs before the 500-place enrichment sweep).

Leaf module (no src.* imports): it owns PlaceOut, the frozen transport record
the whole pipeline shares. Both data sources converge into it at their seam —
db.fetch_places() converts ORM rows into PlaceOut exactly once, and the demo
list below IS PlaceOut — so cluster/route/narrative stay pure functions over
plain records (the same discipline flights.py gets from Flight dataclasses).

DEMO DATA disclaimer: names are real, famous Kish POIs; prices and deeplinks
are plausible demo values (Oct 2026). COORDINATES ARE SOURCED (2026-10-03:
OSM via Nominatim / Wikipedia coords API — provenance noted per POI below;
only coral-beach is shoreline-anchored to the sourced pier because no OSM
node exists for it). Swap by seeding Postgres (db.ensure_schema() loads this
same list on first boot) and correcting rows there. Production feed = Neshan
grid scan + partner APIs (notes §1الف/§1ج); redirect_url is the affiliate
deeplink slot (utm-tagged demo URLs stand in for the CPC contract until
partners sign).
"""

from __future__ import annotations

from dataclasses import dataclass

# Persian chip labels for PlaceOut.category — the vocabulary models.Place
# stores raw; only the UI translates (constants.py's AIRLINES pattern).
# Persian ORDINAL words for day numbers (user direction: «روز اول», never
# «روز ۱») — the planner is bounded at 5 days (DAY_COLORS length), so the
# mapping is exhaustive; digits remain the fallback beyond it.
DAY_ORDINALS_FA: dict[int, str] = {
    1: "اول",
    2: "دوم",
    3: "سوم",
    4: "چهارم",
    5: "پنجم",
}

CATEGORY_LABELS_FA: dict[str, str] = {
    "attraction": "دیدنی",
    "beach": "ساحل",
    "park": "تفریحی",
    "mall": "خرید",
    "restaurant": "غذا و خوراک",
    "water_sport": "ورزش آبی",
    "activity": "فعالیت",
}


@dataclass(frozen=True)
class PlaceOut:
    """Transport record: retrieval → clustering → routing → narration → map.

    Prices are Toman (the UI domain — Rial stops at the pipeline seam);
    redirect_url is set iff has_ticket (the aggregator handoff).
    """

    slug: str
    name_fa: str
    category: str
    city_code: str
    lat: float
    lng: float
    description_fa: str = ""
    est_visit_minutes: int = 90
    popularity: float = 0.5
    has_ticket: bool = False
    redirect_url: str | None = None
    lowest_price: int | None = None  # Toman
    # ── Media + provenance (build-time only — enrichment.py attaches these by
    # slug; never fetched at request time). None ⇒ fallback tile and no «منبع»
    # row: a POI without a real Wikipedia article renders honestly rather than
    # with invented media. ──
    image_url: str | None = None       # site-relative: /static/poi/<slug>.jpg
    image_alt_fa: str | None = None    # content-bearing Persian alt text
    image_credit: str | None = None    # «عکس: … · ویکی‌مدیا کامانز · CC BY-SA …»
    summary_fa: str | None = None      # sourced fa-Wikipedia extract (trust text)
    source_url: str | None = None      # the Wikipedia article («منبع» link)
    source_name: str | None = None     # «ویکی‌پدیا» / «ویکی‌پدیا (انگلیسی)»


_utm = "?utm_source=torob_travel&utm_medium=itinerary"

# West → east across the island (the pipeline's day sweep runs the same way):
# Greek Ship (W beach) → Ocean Park (S-center) → Harireh (N) → Sarina Mall
# (center) → pier / Mir Mahna / coral beach (E) → dolphin+bird complex (SE).
# Provenance per POI: en-wiki = Wikipedia coordinates API, OSM = Nominatim.
KISH_DEMO: tuple[PlaceOut, ...] = (
    PlaceOut(  # en-wiki "Greek Ship" 26.52615, 53.90809
        slug="greek-ship", name_fa="کشتی یونانی", category="attraction", city_code="kish",
        lat=26.5261, lng=53.9081, description_fa="لاشهٔ کشتی کوروش روی ماسه‌های ساحل غربی؛ بهترین غروب جزیره همین‌جاست.",
        est_visit_minutes=60, popularity=0.98,
    ),
    PlaceOut(  # OSM «مرکز تجاری سارینا مال»، میدان امیرکبیر 26.54278, 54.01221
        slug="sarina-mall", name_fa="مرکز تجاری سارینا مال", category="mall", city_code="kish",
        lat=26.5428, lng=54.0122, description_fa="مرکز تجاری مدرن در میدان امیرکبیر، قلب شهر کیش؛ برندها و سوغات.",
        est_visit_minutes=90, popularity=0.80,
    ),
    PlaceOut(  # fa-wiki «شهر حریره» 26.566944, 53.974167
        slug="harireh", name_fa="شهر باستانی حریره", category="attraction", city_code="kish",
        lat=26.5669, lng=53.9742, description_fa="خرابه‌های شهر تاریخی در شمال جزیره؛ معماری ساده و بادگیرهای خشتی.",
        est_visit_minutes=45, popularity=0.85,
    ),
    PlaceOut(  # OSM «اسکله تفریحی» 26.548139, 54.025221
        slug="water-sports-pier", name_fa="اسکله تفریحی کیش", category="water_sport", city_code="kish",
        lat=26.5481, lng=54.0252, description_fa="جت‌اسکی، پاراسیلینگ و قایق کف شیشه‌ای از اسکله شرقی.",
        est_visit_minutes=120, popularity=0.78, has_ticket=True,
        redirect_url="https://www.alibaba.ir/travel-extras/category/KISH-Activities" + _utm,
        lowest_price=900_000,
    ),
    PlaceOut(  # en-wiki + OSM «پارک آبی اوشن» 26.49725, 53.98057
        slug="ocean-water-park", name_fa="پارک آبی اوشن", category="park", city_code="kish",
        lat=26.4972, lng=53.9806, description_fa="سرسره‌ها و استخر موج در جنوب جزیره؛ نصف روز تفریح تمام‌عیار.",
        est_visit_minutes=240, popularity=0.90, has_ticket=True,
        redirect_url="https://www.alibaba.ir/travel-extras/category/KISH-Activities" + _utm,
        lowest_price=1_200_000,
    ),
    PlaceOut(  # NO OSM node — shoreline-anchored ~450 m NE of the sourced pier
        slug="coral-beach", name_fa="ساحل مرجانی", category="beach", city_code="kish",
        lat=26.5520, lng=54.0280, description_fa="آب فیروزه‌ای ساحل شمال‌شرقی؛ اسنورکلینگ بین مرجان‌ها.",
        est_visit_minutes=120, popularity=0.95,
    ),
    PlaceOut(  # OSM «رستوران ساحلی میرمهنا» 26.540582, 54.024761
        slug="mir-mahna", name_fa="رستوران میرمهنا", category="restaurant", city_code="kish",
        lat=26.5406, lng=54.0248, description_fa="رستوران معروف دریایی جزیره با نمای ساحل مرجانی؛ میگو و ماهی تازه.",
        est_visit_minutes=90, popularity=0.84,
    ),
    PlaceOut(  # OSM «پارک دلفین‌ها و باغ پرندگان» relation 26.50698, 54.03552 (+nudge E)
        slug="dolphin-park", name_fa="دلفیناریوم کیش", category="park", city_code="kish",
        lat=26.5070, lng=54.0365, description_fa="نمایش دلفین‌ها و شیرهای دریایی در مجموعه تفریحی جنوب شرقی.",
        est_visit_minutes=120, popularity=0.92, has_ticket=True,
        redirect_url="https://www.alibaba.ir/travel-extras/product/act-kish-dolphinpark" + _utm,
        lowest_price=850_000,
    ),
    PlaceOut(  # same OSM relation (−nudge W so the two markers don't stack)
        slug="bird-garden", name_fa="باغ پرندگان کیش", category="park", city_code="kish",
        lat=26.5070, lng=54.0345, description_fa="هزاران پرندهٔ رنگی در بزرگ‌ترین باغ پرندگان خاورمیانه، هم‌جوار دلفیناریوم.",
        est_visit_minutes=90, popularity=0.72, has_ticket=True,
        redirect_url="https://www.alibaba.ir/travel-extras/category/KISH-Activities" + _utm,
        lowest_price=400_000,
    ),
    PlaceOut(  # OSM «مسیر دوچرخه‌سواری» east coastal segment 26.55790, 54.00768
        slug="island-cycleway", name_fa="مسیر دوچرخه‌سواری جزیره", category="activity", city_code="kish",
        lat=26.5579, lng=54.0077, description_fa="کرایه دوچرخه و گشت دورتادور جزیره در مسیر اختصاصی ساحلی.",
        est_visit_minutes=60, popularity=0.70,
    ),
)


def demo_places(city_code: str) -> list[PlaceOut]:
    """The planner's mock-mode feed (mock_catalog.generate_flights() role)."""
    return [p for p in KISH_DEMO if p.city_code == city_code]
