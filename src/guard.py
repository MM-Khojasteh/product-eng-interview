"""Guard boundary — collaborators degrade, never raise (crm-monitoring
src/utilities/guard.py pattern).

Scope: use for NEW never-raise boundaries (lifespan teardown, background
tasks). The existing degrade-to-mock seams (readmodel.read_quotes,
scanbus.request_scan, itinerary narrative/embeddings) keep their inline,
distinctly-worded except blocks — their fallback values and log strings are
observable product behavior, each documented at its site.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def guarded(label: str, logger: logging.Logger | None = None):
    """Run a block; any Exception is logged (label + type + message) and
    swallowed — for boundaries where the caller must proceed regardless."""
    try:
        yield
    except Exception as exc:  # noqa: BLE001 — boundary, by design
        (logger or log).warning("%s failed (%s: %s) — ignored", label,
                                type(exc).__name__, exc, exc_info=True)
