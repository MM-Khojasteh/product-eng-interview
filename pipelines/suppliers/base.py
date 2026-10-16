"""Supplier strategy layer — spec §1: heterogeneous supplier behaviors behind
one interface (FlyToday = single blocking POST; Alibaba = POST → poll every 2 s,
≤11 times, until isCompleted — dossier §2.7/§3.7).

Separation of concerns: suppliers own HTTP + response parsing ONLY. They never
touch Kafka or storage; they return raw bytes (search) or canonical records
(parse) and let the worker/normalizer layers do the rest.
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from collections.abc import Callable, Mapping, Sequence

import requests

from ..common.errors import BlockedError, RateLimitedError, SchemaDriftError, ScrapingError

log = logging.getLogger("torob.supplier")


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SearchQuery:
    """One domestic one-way search (dossier baseline shape; Gregorian ISO date)."""

    origin: str
    destination: str
    departure_date: str
    adults: int = 1
    children: int = 0
    infants: int = 0
    cabin: str = "Y"


@dataclass
class RawScan:
    """One complete supplier response, UNTOUCHED — this is what goes on the wire."""

    supplier: str
    query: SearchQuery
    content: bytes                 # raw HTTP body exactly as received
    captured_at: str               # ISO-8601 UTC
    scan_id: str
    search_session_id: str | None = None  # flytoday searchId / alibaba requestId
    complete: bool = True          # False = polling budget exhausted, partial body
    meta: dict[str, Any] = field(default_factory=dict)  # e.g. {"itineraries": 149}


@dataclass
class ParseResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    search_session_id: str | None = None
    skipped: int = 0


@dataclass(frozen=True)
class HttpOptions:
    timeout_seconds: float = 20.0
    max_retries: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 30.0


# ---------------------------------------------------------------------------
# Shared supplier scaffolding (extracted from the per-supplier captures)
# ---------------------------------------------------------------------------

# Captured desktop Chrome identity — one identity for every supplier session
# (dossier §2.4 / §3.4); was duplicated per supplier before the extraction.
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def unwrap_envelope(payload: Any, key: str) -> Mapping[str, Any]:
    """Supplier envelope defense: 2023-era captures nest the payload under a
    per-supplier key (`search` for FlyToday, `result` for Alibaba); the 2025
    clients read root keys — defend both shapes. The error string is
    observable (it flows into DLQ reasons via normalizer)."""
    if not isinstance(payload, Mapping):
        raise SchemaDriftError(f"response is {type(payload).__name__}, expected object")
    inner = payload.get(key)
    return inner if isinstance(inner, Mapping) else payload


def parse_scan_body(
    scan: RawScan, *, envelope_key: str, items_key: str
) -> tuple[Mapping[str, Any], list]:
    """Shared parse scaffold: JSON-decode the raw scan body, unwrap the
    envelope, require the items list. Error strings are observable (DLQ
    reasons) — keep them byte-stable."""
    try:
        payload = json.loads(scan.content)
    except ValueError as exc:
        raise SchemaDriftError(f"non-JSON scan body: {exc}") from exc
    body = unwrap_envelope(payload, envelope_key)
    if items_key not in body:
        raise SchemaDriftError(
            f"'{items_key}' missing — top-level keys: "
            f"{sorted(str(k) for k in body.keys())[:15]}"
        )
    items = body.get(items_key) or []
    if not isinstance(items, list):
        raise SchemaDriftError(f"'{items_key}' is not a list")
    return body, items


def request_block(query: SearchQuery, deep_link: str | None = None) -> dict[str, Any]:
    """The canonical `request` block of a DTO record. Key ORDER matters: dict
    insertion order survives json.dumps and the raw topic carries the bytes —
    `deep_link` (FlyToday only) must stay LAST."""
    block: dict[str, Any] = {
        "origin": query.origin,
        "destination": query.destination,
        "departure_date": query.departure_date,
        "adults": query.adults,
        "children": query.children,
        "infants": query.infants,
        "cabin": query.cabin,
    }
    if deep_link is not None:
        block["deep_link"] = deep_link
    return block


def fresh_scan_stamp() -> tuple[str, str]:
    """(captured_at ISO-seconds UTC, scan_id hex) for a RawScan — both
    strategies stamped their scans with this exact pair; one definition now."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds"), uuid.uuid4().hex


def parse_items(
    items: Sequence[Any],
    build_one: Callable[[Any], dict],
    *,
    what: str,
) -> tuple[list[dict], int]:
    """Shared malformed-item scaffold for strategy.parse(): Mapping-checked,
    exception-quarantined record building — one bad item logs-and-skips, never
    fails the whole scan. `what` rides the (observable) warning string."""
    records: list[dict] = []
    skipped = 0
    for index, item in enumerate(items):
        if not isinstance(item, Mapping):
            skipped += 1
            continue
        try:
            records.append(build_one(item))
        except (KeyError, TypeError, AttributeError, ValueError) as exc:
            log.warning("skipping %s #%d (malformed): %s", what, index, exc)
            skipped += 1
    return records, skipped


# ---------------------------------------------------------------------------
# Strategy interface + shared resilient HTTP
# ---------------------------------------------------------------------------


class SupplierStrategy:
    """Base class: session-shape-agnostic scrape + parse contract.

    `search` returns raw response bytes (never parsed/canonicalized here);
    `parse` turns a RawScan into canonical DTO records (dossier §5). The split
    is what lets the raw topic carry supplier payloads byte-for-byte while the
    normalizer owns transformation.
    """

    name: str = "abstract"

    def __init__(
        self,
        http: HttpOptions | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.http = http or HttpOptions()
        self._session = session or requests.Session()
        self._sleep = sleep
        self._rng = rng or random.Random()

    # -- resilient HTTP (shared) ----------------------------------------------

    def _backoff(self, attempt: int, retry_after: str | None = None) -> float:
        delay = min(
            self.http.backoff_base_seconds * (2 ** (attempt - 1)),
            self.http.backoff_max_seconds,
        )
        delay *= 0.5 + self._rng.random()  # ±50% jitter
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                pass  # HTTP-date form — fall back to backoff
        return delay

    def _request(
        self, method: str, url: str, **kwargs: Any
    ) -> requests.Response:
        """Retry with exponential backoff on transport errors / 5xx / 429.

        Typed failures: 401/403 → BlockedError (circuit-break "blocked"),
        exhausted 429 → RateLimitedError (cooldown), exhausted transport/5xx →
        ScrapingError. Other 4xx fail fast (dossier §3.5: WAF won't clear).
        """
        last_error: str | None = None
        for attempt in range(1, self.http.max_retries + 1):
            try:
                response = self._session.request(
                    method, url, timeout=self.http.timeout_seconds, **kwargs
                )
            except requests.exceptions.RequestException as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("request failed (attempt %d/%d): %s %s → %s",
                            attempt, self.http.max_retries, method, url, last_error)
                if attempt == self.http.max_retries:
                    break
                self._sleep(self._backoff(attempt))
                continue

            if response.status_code == 429:
                last_error = "HTTP 429 rate-limited"
                log.warning("rate-limited (429) on attempt %d/%d — honoring Retry-After",
                            attempt, self.http.max_retries)
                if attempt == self.http.max_retries:
                    break
                self._sleep(self._backoff(attempt, response.headers.get("Retry-After")))
                continue

            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code} server error"
                log.warning("server error on attempt %d/%d: HTTP %d",
                            attempt, self.http.max_retries, response.status_code)
                if attempt == self.http.max_retries:
                    break
                self._sleep(self._backoff(attempt))
                continue

            if response.status_code in (401, 403):
                raise BlockedError(
                    f"HTTP {response.status_code} from {url} "
                    f"(body: {response.text[:200]!r}) — blocked/auth wall, not retryable"
                )
            if response.status_code >= 400:
                raise ScrapingError(
                    f"HTTP {response.status_code} from {url} "
                    f"(body: {response.text[:200]!r}) — not retryable"
                )
            self._log_wire(response)
            return response

        if last_error and "429" in last_error:
            raise RateLimitedError(f"{method} {url}: {last_error}")
        raise ScrapingError(
            f"{method} {url} failed after {self.http.max_retries} attempts: {last_error}"
        )

    @staticmethod
    def _log_wire(response: requests.Response) -> None:
        """DEBUG-only diagnostics: method/URL/status/headers, secrets redacted."""
        if log.isEnabledFor(logging.DEBUG):
            safe = {
                key: value for key, value in response.headers.items()
                if key.lower() not in ("x-token", "set-cookie", "authorization")
            }
            request = response.request
            log.debug("wire: %s %s → HTTP %d; response headers: %s",
                      request.method if request is not None else "?",
                      response.url, response.status_code, safe)

    # -- contract ----------------------------------------------------------------

    def search(self, query: SearchQuery) -> RawScan:
        raise NotImplementedError

    def parse(self, scan: RawScan) -> ParseResult:
        raise NotImplementedError

    @staticmethod
    def require_mapping(payload: Any, what: str) -> Mapping[str, Any]:
        if not isinstance(payload, Mapping):
            raise SchemaDriftError(f"{what}: expected JSON object, got {type(payload).__name__}")
        return payload
