"""گام ۳ — narration: LLM voice over the mathematically-sound routed plan.

Extracted from services.py (zero functional changes). The LLM is prompted to
change no facts and, for ticketed places, to send users to the
«تأمین‌کنندگان» handoff — Torob sells nothing itself. Any failure (no key,
no module, timeout, bad format) degrades to build_fallback_narrative() — the
planner's mock_catalog counterpart — never a 500.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from src.config import OPENAI_API_KEY, TOROB_ITINERARY_MODEL
from src.formatting import fa_num
from src.itinerary.openai_client import _openai, _openai_client
from src.itinerary.seed import CATEGORY_LABELS_FA, DAY_ORDINALS_FA, PlaceOut
from src.itinerary.ttlcache import narrative_cache

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DayNarrative:
    day_no: int
    title: str
    paragraphs: tuple[str, ...]


@dataclass(frozen=True)
class Narrative:
    sections: tuple[DayNarrative, ...]
    source: str  # "llm" | "fallback" — the UI badge tells the truth


# The system prompt is plain text in prompts/ beside this module (LLM prompts
# do not belong inside Python source). Loaded once at import; .strip() keeps
# the file editor-friendly (trailing newline) without altering the prompt bytes.
NARRATIVE_SYSTEM_PROMPT = (
    (Path(__file__).parent / "prompts" / "narrative_system.txt").read_text(encoding="utf-8").strip()
)


def _plan_digest(routed_plan: Sequence[Sequence[PlaceOut]], city_name_fa: str) -> str:
    """The RAG payload (گام ۳): the already-sound plan as a compact JSON
    digest — names/categories/durations/ticket facts only. The LLM adds
    voice; it is not allowed to add facts (system prompt rule ۳)."""
    days = []
    for day_no, day in enumerate(routed_plan, 1):
        stops = []
        for stop_no, p in enumerate(day, 1):
            stop = {
                "stop": stop_no,
                "name": p.name_fa,
                "category": CATEGORY_LABELS_FA.get(p.category, p.category),
                "visit_minutes": p.est_visit_minutes,
                "needs_ticket": p.has_ticket,
            }
            if p.has_ticket and p.lowest_price:
                stop["lowest_price_toman"] = p.lowest_price
            stops.append(stop)
        days.append({"day": day_no, "stops": stops})
    return json.dumps({"city": city_name_fa, "days": days}, ensure_ascii=False)


_DAY_HEADER = re.compile(r"^\s*روز\s*[0-9۰-۹]+\s*\|\s*(.*?)\s*$")


# Thematic day titles (user direction): a day's h2 is its THEME — the mix of
# what the day is about — never a mechanical «از X تا Y» stop list. Themes
# derive ONLY from the day's categories (no invented facts); a single-stop
# day simply names its stop; an unknown category mix falls back to the arc.
_CATEGORY_THEMES_FA: dict[str, str] = {
    "water_sport": "ورزش‌های آبی",
    "beach": "ساحل",
    "mall": "خرید",
    "restaurant": "طعم‌های محلی",
    "attraction": "دیدنی‌ها",
    "park": "تفریح",
    "activity": "فعالیت",
}


def _day_title(day: Sequence[PlaceOut], city_name_fa: str) -> str:
    if len(day) == 1:
        return day[0].name_fa
    themes: list[str] = []
    for place in day:
        theme = _CATEGORY_THEMES_FA.get(place.category)
        if theme and theme not in themes:
            themes.append(theme)
    if not themes:
        return f"از {day[0].name_fa} تا {day[-1].name_fa}"
    if len(themes) == 1:
        return f"{themes[0]} در {city_name_fa}"  # «تفریح در کیش» — a lone theme gets a place
    if len(themes) == 2:
        return f"{themes[0]} و {themes[1]}"
    return f"{themes[0]}، {themes[1]} و {themes[2]}"


def parse_narrative(text: str, expected_days: int) -> list[DayNarrative]:
    """LLM text → DayNarrative sections. Tolerant by design: Persian OR
    Latin digits in headers, markdown bullets/asterisks stripped, preamble
    before the first header ignored, and hard-wrapped lines joined into
    blank-line-delimited paragraph BLOCKS (a model that wraps mid-sentence
    must not shatter one paragraph into three). Day numbers are re-sequenced
    locally so a model that skips «روز ۲» cannot desync narrative from
    timeline."""
    sections: list[DayNarrative] = []
    title: str | None = None
    paragraphs: list[str] = []
    open_para = False

    def flush() -> None:
        nonlocal title, paragraphs, open_para
        if title is not None:
            sections.append(DayNarrative(len(sections) + 1, title, tuple(paragraphs)))
        title, paragraphs, open_para = None, [], False

    for raw in text.splitlines():
        match = _DAY_HEADER.match(raw)
        if match:
            flush()
            # titles are labels, not sentences — a model's trailing «.»/«،»
            # must never leak into the h2 next to the day circle
            title = match.group(1).strip("*# ").rstrip(".،؛:!؟ ").rstrip()
            continue
        if title is None:
            continue
        line = raw.strip("*# ").strip()
        if line:
            if open_para and paragraphs:
                paragraphs[-1] += " " + line
            else:
                paragraphs.append(line)
                open_para = True
        else:
            open_para = False  # blank line closes the current paragraph block
    flush()
    if len(sections) != expected_days:
        log.warning("narrative parse: %d section(s) for %d day(s) — model head: %.120r",
                    len(sections), expected_days, raw)
    return sections


def _names_fa(items: Sequence[PlaceOut]) -> str:
    names = [p.name_fa for p in items]
    if len(names) == 1:
        return names[0]
    return "، ".join(names[:-1]) + " و " + names[-1]


def build_fallback_narrative(routed_plan: Sequence[Sequence[PlaceOut]],
                             city_name_fa: str) -> Narrative:
    """Deterministic Persian narration for when the LLM seam is off/failed —
    the planner's mock_catalog counterpart. Same shape and the same
    «تأمین‌کنندگان» CTA discipline as the LLM prompt, so downstream (the
    template) treats the two sources identically."""
    sections = []
    for day_no, day in enumerate(routed_plan, 1):
        first, last = day[0], day[-1]
        day_fa = DAY_ORDINALS_FA.get(day_no, fa_num(day_no))  # «روز دوم»، نه «روز ۲»
        paragraphs = [
            (
                f"مسیر روز {day_fa} در {city_name_fa} با {first.name_fa} باز می‌شود؛ "
                f"بعد از {_names_fa(day)}، روز در {last.name_fa} به پایان می‌رسد. "
                "ترتیب ایستگاه‌ها بر اساس نزدیک‌ترین مسیر چیده شده تا کمترین رفت‌وآمد را داشته باشی."
            )
        ]
        ticketed = [p for p in day if p.has_ticket]
        if ticketed:
            paragraphs.append(
                f"برای {_names_fa(ticketed)} بلیت لازم است: روی نشانِ تأمین‌کننده (لوگوی فروشنده) در کارت همان مکان بزن "
                "تا قیمت‌های فروشنده‌های مختلف را ببینی و مستقیم از سایت خودشان بخری — ترب فقط مقایسه می‌کند."
            )
        # the day pill in the card header already carries «روز N» — the title
        # is the day's theme (user direction), single-stop days name the stop
        sections.append(DayNarrative(day_no, _day_title(day, city_name_fa), tuple(paragraphs)))
    return Narrative(tuple(sections), "fallback")


async def generate_narrative(routed_plan: Sequence[Sequence[PlaceOut]], *,
                             city_name_fa: str) -> Narrative:
    """The LLM stage — the ONLY impure/async stage, and the last. Any
    failure (no key, no module, timeout, bad format) degrades to the
    deterministic fallback instead of failing the page: an Iranian-network
    user must still get their plan.

    Successful LLM narratives are memoized by (city, plan digest): the plan
    is deterministic given its inputs (random_state pinned), so a repeat view
    of the same routed plan replays the stored narrative instead of re-paying
    the ~25 s generation. Disclosed nuance: that makes the narrative stable
    per plan for the TTL window rather than freshly sampled each view.
    Fallback narratives are NOT cached — deterministic and instant, so the
    offline/demo path keeps today's behaviour exactly."""
    if not routed_plan:
        return Narrative((), "fallback")
    if _openai is None or not OPENAI_API_KEY:
        log.info("itinerary narrative: OpenAI off — deterministic fallback")
        return build_fallback_narrative(routed_plan, city_name_fa)
    digest = _plan_digest(routed_plan, city_name_fa)
    cache_key = (city_name_fa, hashlib.blake2b(digest.encode(), digest_size=16).hexdigest())
    hit, cached = narrative_cache.get(cache_key)
    if hit:
        log.debug("itinerary narrative: cache hit (%d days)", len(cached.sections))
        return cached
    try:
        response = await _openai_client().chat.completions.create(
            model=TOROB_ITINERARY_MODEL,
            temperature=0.7,
            # Reasoning-tier models (e.g. gemini-3.1-pro via OpenRouter) think
            # before writing — the 20 s client default would clip longer
            # narratives into the fallback path.
            timeout=45.0,
            messages=[
                {"role": "system", "content": NARRATIVE_SYSTEM_PROMPT},
                {"role": "user", "content": digest},
            ],
        )
        sections = parse_narrative(response.choices[0].message.content or "",
                                   expected_days=len(routed_plan))
        if len(sections) != len(routed_plan):
            return build_fallback_narrative(routed_plan, city_name_fa)
        log.info("itinerary narrative: llm (%s, %d days)", TOROB_ITINERARY_MODEL, len(sections))
        narrative = Narrative(tuple(sections), "llm")
        narrative_cache.put(cache_key, narrative)
        return narrative
    except Exception as exc:  # noqa: BLE001 — the LLM seam degrades, never 500s
        log.warning("itinerary narrative failed (%s: %s) — fallback",
                    exc.__class__.__name__, exc, exc_info=True)
        return build_fallback_narrative(routed_plan, city_name_fa)
