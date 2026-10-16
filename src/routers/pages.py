"""Home shell + content pages (torob.com's own /pages/* family: plain
informational pages rendered in the same shell — header, prose column, footer).

Extracted from main.py §"Routes" 1–2 (zero functional changes). Content lives
in templates/pages/*.html; this table only carries the SEO head.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from src.filters import _index_context
from src.view_templates import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "flights/index.html", _index_context())


CONTENT_PAGES: dict[str, dict[str, str]] = {
    "about": {
        "title": "درباره ترب بلیت | ترب بلیت",
        "description": "ترب بلیت، موتور جستجو و مقایسه قیمت بلیت هواپیما از خانوادهٔ ترب — بدون افزایش قیمت، خرید مستقیم از ارزان‌ترین آژانس.",
    },
    "faq": {
        "title": "سوالات متداول | ترب بلیت",
        "description": "پاسخ ترب بلیت به سوالات متداول دربارهٔ مقایسه قیمت، خرید، استرداد و کنسلی بلیت هواپیما.",
    },
    "terms": {
        "title": "قوانین و مقررات | ترب بلیت",
        "description": "قوانین و مقررات استفاده از ترب بلیت؛ شرایط قیمت‌ها، صدور بلیط، استرداد و کنسلی و بار مجاز.",
    },
}


@router.get("/pages/{slug}", response_class=HTMLResponse)
async def content_page(request: Request, slug: str):
    meta = CONTENT_PAGES.get(slug)
    if meta is None:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(request, f"pages/{slug}.html", {"meta": meta})
