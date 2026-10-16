"""Jinja2 environment + custom filter registration (crm-monitoring
view_templates.py pattern).

BASE_DIR anchors to the PROJECT ROOT (src/ → parents[1]) so templates/
and static/ stay exactly where `uvicorn main:app`, the Dockerfile and the
tests expect them — the package layout must not move a single asset.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from fastapi.templating import Jinja2Templates

from src.constants import AGENCIES
from src.formatting import date_fa, fa_num, toman
from src.models import Agency

BASE_DIR = Path(__file__).resolve().parents[1]
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

templates.env.filters["toman"] = toman
templates.env.filters["fa_num"] = fa_num
templates.env.filters["date_fa"] = date_fa


def supplier_of(url: str | None) -> Agency | None:
    """The AGENCIES registry row whose domain appears in a redirect_url —
    the itinerary handoff's mark + name source (one registry for flights and
    attractions, no template-local copy to drift). Domain match
    (alibaba.ir), not key substring: a path segment of another host can
    never hit. None ⇒ unknown host — the template then renders the mark-less
    fallback tile, never an invented mark."""
    if not url:
        return None
    return next((a for a in AGENCIES if a.domain in url), None)


templates.env.filters["supplier_of"] = supplier_of

# Jinja's default auto_reload stats every template's mtime on every render —
# a per-render tax that buys nothing in production (deploys restart the
# process; `uvicorn --reload` already restarts on change). Dev keeps the
# default; deployments set TOROB_TEMPLATE_AUTORELOAD=0.
templates.env.auto_reload = os.getenv("TOROB_TEMPLATE_AUTORELOAD", "1") not in ("0", "false")


def _static_version() -> str:
    """Stable component of the cache-busting ?v= (crm-monitoring
    StaticAssetVersion pattern, adapted): a content hash over the static tree
    itself — computed once at import; ANY asset change bumps every ?v=, which
    is what makes `Cache-Control: immutable` (see CachedStaticFiles in
    main.py) safe. (A VCS SHA would work too where one exists; the tree hash
    needs neither.) The LIVE component is the per-file mtime suffix added by
    asset_url — see there for why the import-time hash alone is not enough."""
    static_root = BASE_DIR / "static"
    digest = hashlib.sha256()
    for path in sorted(static_root.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(static_root).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:10]


_STATIC_VERSION = _static_version()


def asset_url(path: str) -> str:
    """'/static/...' → '/static/...?v=<tree-hash>.<file-mtime>' (immutable
    caching).

    The mtime suffix is stat'ed PER CALL, so rebuilding an asset
    (scripts/build_css.sh) changes its URL on the very next render even on a
    long-lived process — uvicorn's StatReload watches only *.py, so a server
    that survives a CSS rebuild keeps serving HTML that references the OLD
    ?v=, and the browser (told that URL was immutable) keeps rendering the
    OLD bytes: stale-CSS breakage until a manual hard refresh (reported
    2026-10-16 — results page needed a hard refresh, and the flight-card
    overlay link mis-anchored page-wide). With the suffix, fresh HTML always
    references the current file."""
    try:
        mtime = (BASE_DIR / path.lstrip("/")).stat().st_mtime_ns
    except OSError:
        mtime = 0
    return f"{path}?v={_STATIC_VERSION}.{mtime}"


templates.env.globals["asset_url"] = asset_url
