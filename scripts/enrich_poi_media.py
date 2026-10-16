#!/usr/bin/env python
"""One-time POI media enrichment — real photos + sourced Persian summaries for
the itinerary trust layer (itinerary.py: photos, «منبع» links, credits).

    python scripts/enrich_poi_media.py                     # fetch + write artifacts
    python scripts/enrich_poi_media.py --geosearch greek-ship   # wiki titles near a POI

Sources (free, key-less — Google Places needs a billing account, incompatible
with the $0 budget and Iran):
  {lang}.wikipedia.org/api/rest_v1/page/summary/{title}   extract · thumbnail · page URL
  commons.wikimedia.org/w/api.php prop=imageinfo          artist · license · 640px thumb

Everything is fetched ONCE here and committed, so the app stays offline-first
(static/logos/SOURCES.md doctrine): src/itinerary/data/enrichment_kish.json,
static/poi/<slug>.* and static/poi/SOURCES.md. Non-free (fair-use) Commons
files are skipped — a public repo only redistributes free licenses. POIs with
no article render the fallback tile; nothing is invented.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # Windows consoles default to cp1252 — Persian titles need UTF-8
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

try:
    import httpx
except ImportError:
    print("pip install httpx  (Wikipedia/Commons fetching)")
    sys.exit(1)

from src.itinerary import enrichment  # noqa: E402  (pure helpers + limits live there)
from src.itinerary.seed import KISH_DEMO  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
POI_DIR = ROOT / "static" / "poi"
# Wikimedia's robot policy (w.wiki/4wJS) 403s bot-looking traffic whose UA
# lacks contact info — the +repo URL is what gets this script through.
UA = ("torob-itinerary-enrichment/1.0 "
      "(+https://github.com/MM-Khojasteh/product-eng-interview; one-time build script)")
TIMEOUT = 30.0

# slug → candidate (lang, wiki title) pairs, tried in order; the first
# standard article wins. fa preferred (the UI language). Verified 2026-10-04:
# fa «کشتی یونانی» and fa «شهر حریره» resolve (REST summary + geosearch); the
# remaining candidates are verified at run time — a 404 just drops the POI.
WIKI_CANDIDATES: dict[str, list[tuple[str, str]]] = {
    "greek-ship": [("fa", "کشتی یونانی")],
    "harireh": [("fa", "شهر حریره")],
    "ocean-water-park": [("fa", "پارک آبی اوشن"), ("en", "Ocean Water Park")],
    "dolphin-park": [("fa", "دلفیناریوم کیش"), ("en", "Kish Dolphin Park")],
    "bird-garden": [("fa", "باغ پرندگان کیش"), ("en", "Kish Bird Garden")],
}

BY_SLUG = {p.slug: p for p in KISH_DEMO}
CONTENT_EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}

# Commons fallback for POIs without a (photographed) article: free-text FILE
# search + a token gate, then the same license/download path. must-tokens are
# the distinctive name parts — a hit whose title lacks them can't be a photo
# OF this place (the wrong-photo gate; every downloaded file is eyeballed
# before ship regardless). Commons is overwhelmingly English-titled.
COMMONS_FALLBACK: dict[str, tuple[set[str], list[str]]] = {
    "dolphin-park": ({"dolphin", "kish"}, ["Kish Dolphin Park", "Kish Dolphinarium"]),
    "bird-garden": ({"bird", "kish"}, ["Kish Bird Garden", "Bird Garden Kish"]),
    "sarina-mall": ({"sarina"}, ["Sarina Mall Kish", "Sarina Trade Center Kish"]),
    "coral-beach": ({"coral", "kish"}, ["Kish coral beach", "coral reef Kish"]),
    "mir-mahna": ({"mahna"}, ["Mir Mahna Kish"]),
    "water-sports-pier": ({"kish", "pier"}, ["Kish pier", "Grand Pier Kish", "Kish water sports",
                                             "Kish parasailing"]),
    "island-cycleway": ({"kish", "bicycle"}, ["Kish bicycle path", "Kish cycling"]),
}
# Titles that mean "not a photograph of the place", whatever the tokens say.
NON_PHOTO_TITLE = re.compile(
    r"(?i)logo|icon|map|chart|diagram|flag|coat of arms|seal|banner|\.svg$")


def _get(client: httpx.Client, url: str, **params) -> httpx.Response:
    """GET with one retry — Iran-network reality makes single attempts flaky."""
    last: Exception | None = None
    for _ in range(2):
        try:
            return client.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, **params)
        except httpx.HTTPError as exc:
            last = exc
    raise SystemExit(f"network failed twice for {url}: {last}")


def fetch_summary(client: httpx.Client, lang: str, title: str) -> dict | None:
    """REST summary for one candidate; None ⇒ caller tries the next candidate."""
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{quote(title)}"
    resp = _get(client, url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    if data.get("type") != "standard":  # disambiguation pages etc. are not POIs
        return None
    got = (data.get("titles") or {}).get("normalized") or data.get("title") or ""
    if got.replace(" ", "_").casefold() != title.replace(" ", "_").casefold():
        # page/summary follows redirects SILENTLY (no flag, target's content):
        # "Kish Dolphin Park" → "Kish Island" would attach another place's
        # photo, article and summary — reject; the POI falls back honestly.
        print(f"    · {lang}-wiki «{title}» redirects to «{got}» — skipped")
        return None
    return data


def commons_info(client: httpx.Client, original_url: str) -> dict | None:
    """License/artist/description-page for the Commons file behind a wiki image."""
    file_title = unquote(urlsplit(original_url).path.rsplit("/", 1)[-1])
    if not file_title:
        return None
    return imageinfo_for(client, file_title)


def imageinfo_for(client: httpx.Client, file_title: str) -> dict | None:
    """Commons imageinfo by file title: thumb, description page, license, artist."""
    resp = _get(client, "https://commons.wikimedia.org/w/api.php", params={
        "action": "query", "format": "json", "prop": "imageinfo",
        "iiprop": "url|extmetadata", "iiurlwidth": "640",
        "titles": f"File:{file_title}",
    })
    resp.raise_for_status()
    page = next(iter(resp.json().get("query", {}).get("pages", {}).values()), {})
    infos = page.get("imageinfo")
    if not infos:
        return None
    info = infos[0]
    meta = info.get("extmetadata", {})
    return {
        "file_title": file_title,
        "thumburl": info.get("thumburl") or info.get("url"),
        "page": info.get("descriptionurl"),
        "artist": meta.get("Artist", {}).get("value"),
        "license": meta.get("LicenseShortName", {}).get("value"),
    }


def commons_search(client: httpx.Client, slug: str) -> dict | None:
    """Free-text Commons FILE search for POIs without a photographed article:
    first hit whose title contains the slug's must-tokens (and isn't a logo/
    map/diagram) wins. No token gate ⇒ any Kish photo would match — the exact
    wrong-photo failure this exists to prevent."""
    if slug not in COMMONS_FALLBACK:
        return None
    must, queries = COMMONS_FALLBACK[slug]
    for query in queries:
        resp = _get(client, "https://commons.wikimedia.org/w/api.php", params={
            "action": "query", "format": "json", "list": "search",
            "srnamespace": "6", "srlimit": "10", "srsearch": query,
        })
        for hit in (resp.json().get("query", {}).get("search", [])
                    if resp.status_code == 200 else []):
            file_title = hit.get("title", "").split(":", 1)[-1]
            if NON_PHOTO_TITLE.search(file_title):
                continue
            # substring, not word-token: Commons titles camel-case too
            # («TheGrandPier-Kish») — a must-word inside a compound still counts
            lowered = file_title.lower()
            if all(m in lowered for m in must):
                print(f"    · Commons search «{query}» → {file_title}")
                return imageinfo_for(client, file_title)
    return None


def download(client: httpx.Client, url: str, slug: str) -> tuple[Path | None, str | None]:
    """Fetch the 640px thumbnail into static/poi/<slug>.<ext> (bounded size)."""
    resp = _get(client, url)
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}"
    ctype = resp.headers.get("content-type", "").split(";")[0].strip().lower()
    ext = CONTENT_EXT.get(ctype)
    if ext is None:  # svg/gif/other ⇒ not a photo thumbnail; skip honestly
        return None, f"unsupported content-type {ctype!r}"
    POI_DIR.mkdir(parents=True, exist_ok=True)
    dest = POI_DIR / f"{slug}{ext}"
    dest.write_bytes(resp.content)
    return dest, None


def resolve_article(client: httpx.Client, slug: str) -> tuple[str, str, dict] | None:
    for lang, title in WIKI_CANDIDATES.get(slug, ()):
        summary = fetch_summary(client, lang, title)
        if summary is not None:
            return lang, title, summary
    return None


def _attach_image(client: httpx.Client, slug: str, entry: dict,
                  info: dict, rows: list[tuple]) -> None:
    """Shared image tail: license gate → download → entry fields + SOURCES row."""
    if not enrichment.is_free_license(info["license"]):
        print(f"    · non-free license {info['license']!r} — image skipped")
        return
    dest, err = download(client, info["thumburl"], slug)
    if err is not None:
        print(f"    · download failed ({err}) — image skipped")
        return
    entry["image_url"] = f"/static/poi/{dest.name}"
    entry["image_credit"] = enrichment.build_credit(info["artist"], info["license"])
    entry["image_page"] = info["page"]
    entry.setdefault("image_alt_fa", f"تصویر {BY_SLUG[slug].name_fa}")
    rows.append((dest.name, BY_SLUG[slug].name_fa, info["page"] or "",
                 enrichment.strip_html(info["artist"] or ""), info["license"] or ""))


def build_entry(client: httpx.Client, slug: str) -> tuple[dict | None, str, list[tuple]]:
    """One POI's artifact entry (+ SOURCES.md rows for its image, if any).

    Article first (summary + lead photo); when that yields no usable photo —
    no article at all, or an article without one — the Commons file search
    takes over: photo-only entry, the file page as its source.
    """
    place = BY_SLUG[slug]
    entry: dict = {}
    rows: list[tuple] = []

    resolved = resolve_article(client, slug)
    if resolved is not None:
        lang, title, summary = resolved
        entry = {
            "wiki_title": title,
            "lang": lang,
            "source_url": summary.get("content_urls", {}).get("desktop", {}).get("page"),
            "source_name": "ویکی‌پدیا" if lang == "fa" else "ویکی‌پدیا (انگلیسی)",
        }
        extract = summary.get("extract") or ""
        if lang == "fa" and extract:
            # en extracts stay out of summary_fa — the UI is Persian; the
            # article link (source_name «(انگلیسی)») keeps it honest instead.
            entry["summary_fa"] = enrichment.trim_summary(extract)
            entry["image_alt_fa"] = enrichment.alt_from_extract(place.name_fa, extract)
        original = summary.get("originalimage", {}).get("source")
        if original:
            info = commons_info(client, original)
            if info is None:
                print("    · no Commons imageinfo — image skipped")
            else:
                _attach_image(client, slug, entry, info, rows)
        else:
            print("    · article has no photo")

    if not entry.get("image_url"):
        info = commons_search(client, slug)
        if info is not None:
            entry.setdefault("wiki_title", info["file_title"])
            entry["lang"] = "commons"
            entry["source_url"] = info["page"]
            entry["source_name"] = "ویکی‌مدیا کامانز"
            _attach_image(client, slug, entry, info, rows)

    if not entry:
        return None, "no article and no Commons photo for this POI", []
    if resolved is None and not entry.get("image_url"):
        return None, "Commons hit yielded no usable image", []
    if not entry.get("source_url"):
        return None, "no stable source page", []
    return entry, "ok", rows


def geosearch(client: httpx.Client, slug: str) -> None:
    """Helper mode: print fa+en wiki articles near a seed POI so a human can
    promote titles into WIKI_CANDIDATES (no auto-match — a wrong photo on a
    trust feature is worse than no photo)."""
    place = BY_SLUG[slug]
    print(f"geosearch ±3 km around {slug} ({place.name_fa}):")
    for lang in ("fa", "en"):
        resp = _get(client, f"https://{lang}.wikipedia.org/w/api.php", params={
            "action": "query", "format": "json", "list": "geosearch",
            "gscoord": f"{place.lat}|{place.lng}", "gsradius": "3000", "gslimit": "15",
        })
        for hit in (resp.json().get("query", {}).get("geosearch", [])
                    if resp.status_code == 200 else []):
            print(f"  [{lang}] {hit['title']}  ({hit['dist']:.0f} m)")


def write_sources_md(rows: list[tuple]) -> None:
    fetched = date.today().isoformat()
    lines = [
        "# POI photos — provenance",
        "",
        "Bundled locally on purpose (same doctrine as `static/logos/SOURCES.md`):",
        "the app is offline-first and Iran-network reality makes runtime Wikimedia",
        "hotlinks unreliable. All photos come from Wikimedia Commons under free",
        "licenses (CC BY-SA / CC0 / PD), redistributed verbatim with the per-file",
        "attribution below — which the UI also surfaces per photo (tile tooltip +",
        "the «اعتبار تصاویر» details on /itinerary/{city}). Non-free files are",
        "skipped by the script and render the fallback tile instead.",
        "",
        f"Fetched once by `scripts/enrich_poi_media.py` ({fetched}); re-run to refresh",
        "(then commit the JSON, the images and this file together).",
        "",
        "| File | POI | Commons file page | Artist | License | Fetched |",
        "|---|---|---|---|---|---|",
    ]
    lines += [f"| {name} | {name_fa} | {page} | {artist} | {lic} | {fetched} |"
              for name, name_fa, page, artist, lic in rows]
    POI_DIR.mkdir(parents=True, exist_ok=True)
    (POI_DIR / "SOURCES.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", default="kish")
    parser.add_argument("--geosearch", metavar="SLUG",
                        help="print wiki articles near a seed POI instead of enriching")
    args = parser.parse_args()

    unknown = set(WIKI_CANDIDATES) - set(BY_SLUG)
    if unknown:
        raise SystemExit(f"unknown slugs in WIKI_CANDIDATES: {sorted(unknown)}")

    with httpx.Client(follow_redirects=True) as client:
        if args.geosearch:
            if args.geosearch not in BY_SLUG:
                raise SystemExit(f"unknown slug {args.geosearch!r} (seed slugs: {sorted(BY_SLUG)})")
            geosearch(client, args.geosearch)
            return

        pois: dict[str, dict] = {}
        image_rows: list[tuple] = []
        for slug in BY_SLUG:  # every catalog POI: article path, then Commons
            print(f"→ {slug} ({BY_SLUG[slug].name_fa})")
            entry, status, rows = build_entry(client, slug)
            print(f"  {status}")
            if entry is not None:
                pois[slug] = entry
                image_rows.extend(rows)

    artifact = ROOT / "src" / "itinerary" / "data" / f"enrichment_{args.city}.json"
    artifact.write_text(json.dumps({
        "city_code": args.city,
        "generated_at": date.today().isoformat(),
        "generator": "scripts/enrich_poi_media.py",
        "pois": pois,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n✓ {artifact.relative_to(ROOT)}  ({len(pois)} POI(s))")

    if image_rows:
        write_sources_md(image_rows)
        print(f"✓ {POI_DIR.relative_to(ROOT) / 'SOURCES.md'}  ({len(image_rows)} photo(s))")


if __name__ == "__main__":
    main()
