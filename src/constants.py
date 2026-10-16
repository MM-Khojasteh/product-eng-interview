"""Static domain catalogs + tuning constants — the "stop magic numbers drifting"
module (crm-monitoring constants.py pattern).

Extracted from main.py §"Domain data" + §"BFF liveness seam" (zero functional
changes). In production the catalogs come from Postgres (entities) and the live
prices from Redis (see docs/INFRASTRUCTURE.md).

NB: AIRLINES is deliberately mutated at runtime — group_quotes() setdefault()s
unknown supplier codes so live cards always carry a color; every import site
shares this one dict.
"""

from __future__ import annotations

import os

from src.models import Agency

AIRPORTS: dict[str, str] = {
    "THR": "تهران", "IKA": "تهران", "MHD": "مشهد", "IFN": "اصفهان",
    "SYZ": "شیراز", "TBZ": "تبریز", "KIH": "کیش", "AWZ": "اهواز",
    "BND": "بندرعباس", "KRM": "کرمان",
}

AIRLINES: dict[str, dict] = {
    # logo = bundled brand mark (static/logos/airlines/…; provenance in
    # static/logos/SOURCES.md). QB has no fetchable logo — None → color chip,
    # same as the setdefault() unknown-airline fallback in group_quotes().
    "IR": {"name": "ایران‌آیر", "color": "#16407c", "logo": "/static/logos/airlines/IR.png"},
    "W5": {"name": "ماهان", "color": "#a4123f", "logo": "/static/logos/airlines/W5.png"},
    "EP": {"name": "آسمان", "color": "#0f766e", "logo": "/static/logos/airlines/EP.png"},
    "B9": {"name": "ایران‌ایرتور", "color": "#6d28a8", "logo": "/static/logos/airlines/B9.png"},
    "QB": {"name": "قشم‌ایر", "color": "#b45309"},
    "YK": {"name": "کیش‌ایر", "color": "#1d4ed8", "logo": "/static/logos/airlines/YK.png"},
    # NV/ZV: FlyToday reports Zagros as IATA ZV in live inventory; the mark
    # ships under both names — NV kept for the mock catalog's legacy code.
    "NV": {"name": "زاگرس", "color": "#0369a1", "logo": "/static/logos/airlines/NV.png"},
    "ZV": {"name": "زاگرس", "color": "#0369a1", "logo": "/static/logos/airlines/ZV.png"},
    # live-data carriers (names = supplier's own nameLocal, THR→KIH 2026-10):
    # IS/TKN/MEH/QB — no fetchable logo → color chip fallback.
    "IS": {"name": "سپهران", "color": "#0c4a6e", "logo": "/static/logos/airlines/IS.png"},
    "TKN": {"name": "فلای‌کیش", "color": "#15803d"},
    "MEH": {"name": "مهر", "color": "#db2777"},
}

# Departure-time buckets for the filter rail (PGP pattern from src.com /search/)
TIME_BUCKETS: dict[str, tuple[int, int, str, str]] = {
    "morning": (6, 11, "صبح", "۶ تا ۱۲"),
    "afternoon": (12, 17, "بعدازظهر", "۱۲ تا ۱۸"),
    "evening": (18, 23, "شب", "۱۸ تا ۲۴"),
}

AGENCIES = [
    # color/torob_pay/since are the ds-seller-list trust signals (avatar bg,
    # «ترب‌پی» mark, «N سال سابقه»). 3-of-5 torob_pay ⇒ any ≥3-agency mock
    # sample always contains one, so the «با ترب‌پی» chip never renders empty.
    Agency("alibaba", "علی‌بابا", "alibaba.ir", 4.8, color="#7c3aed", torob_pay=True, since=2012,
           logo="/static/logos/agencies/alibaba.png"),
    Agency("flytoday", "فلای‌تیودی", "flytoday.ir", 4.6, color="#0e7490", torob_pay=True, since=2014,
           logo="/static/logos/agencies/flytoday.svg"),
    Agency("mrbilit", "مستربلیت", "mrbilit.com", 4.5, color="#be185d", torob_pay=False, since=2015,
           logo="/static/logos/agencies/mrbilit.png"),
    Agency("snapptrip", "اسنپ‌تریپ", "snapptrip.com", 4.4, color="#4d7c0f", torob_pay=True, since=2016,
           logo="/static/logos/agencies/snapptrip.png"),
    Agency("iranbilit", "ایران‌بلیت", "iranbilit.com", 4.2, color="#92400e", torob_pay=False, since=2018),
]

# Poll budgets (env-overridable tuning thresholds — defaults are the measured
# budgets): skeleton re-poll ≈ 2 s × attempts; sized to the slowest supplier
# session (Alibaba ≤ 11 polls × 2 s) + pipeline batch lag. 25 ≈ 50 s — a
# slow-but-alive pipeline must still fill the card: parallel supplier scans
# land ~10 s, but FlyToday evening ReadTimeouts once pushed the fill past a
# 30 s window and rendered the give-up card (2026-10-16).
MAX_POLL_ATTEMPTS = int(os.getenv("TOROB_MAX_POLL_ATTEMPTS", "25"))
MAX_OFFER_POLL_ATTEMPTS = int(os.getenv("TOROB_MAX_OFFER_POLL_ATTEMPTS", "8"))
