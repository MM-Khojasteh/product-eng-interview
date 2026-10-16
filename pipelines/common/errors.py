"""Typed pipeline errors — failure classification drives retry vs circuit-break policy.

Taxonomy (spec: 429 = backoff, sustained 403 = change approach):
    PipelineError            – base, never raised directly
    ScrapingError            – supplier scrape failed (transport/HTTP/decoding)
      BlockedError           – 401/403 / auth wall / geo gate  → circuit breaker "blocked"
      RateLimitedError       – 429 exhausted after Retry-After backoff → breaker "rate_limited"
      SchemaDriftError       – response no longer matches the reverse-engineered contract
    KafkaPipelineError       – transport-layer failure (produce/connect/commit)
    StorageError             – Redis/Postgres failure in a writer service
"""

from __future__ import annotations


class PipelineError(RuntimeError):
    """Base class for all pipeline failures."""


class ScrapingError(PipelineError):
    """The supplier API could not be scraped (transport, HTTP status, decoding)."""


class BlockedError(ScrapingError):
    """Hard block: 401/403, CAPTCHA/WAF, login wall, geo gate (dossier §2.5/§3.5).

    Retrying is pointless; sustained occurrences must open the circuit breaker —
    "sustained 403s mean change approach, not rotate harder" (dossier §6).
    """


class RateLimitedError(ScrapingError):
    """429 exhausted after honoring Retry-After — back off, don't hammer."""


class SchemaDriftError(ScrapingError):
    """Response shape no longer matches the dossier (path-missing, §9.7)."""


class KafkaPipelineError(PipelineError):
    """Kafka transport failure (connectivity, produce, flush, commit)."""


class StorageError(PipelineError):
    """Redis / Postgres failure in a storage-bound service (sink, liveness)."""
