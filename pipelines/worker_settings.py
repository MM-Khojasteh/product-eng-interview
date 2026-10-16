"""Worker settings — the env contract for the raw ingestion worker.

Extracted from raw_worker.py (zero functional changes): every knob the worker
reads from the environment, frozen at construction. Its own module so the
settings surface (env names + defaults) is reviewable apart from the loop that
consumes it — the crm-monitoring settings-dataclass discipline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .common.config import env_bool, env_float, env_int, env_str
from .suppliers.alibaba import DEFAULT_MAX_POLLS, DEFAULT_POLL_INTERVAL_SECONDS
from .suppliers.base import CHROME_USER_AGENT


@dataclass(frozen=True)
class WorkerSettings:
    suppliers: tuple[str, ...]
    routes: tuple[tuple[str, str], ...]
    departure_date: str
    adults: int
    children: int
    infants: int
    cabin: str
    http_timeout: float
    max_retries: int
    backoff_base: float
    backoff_max: float
    # politeness (dossier §6)
    bucket_capacity: float
    searches_per_minute: float
    search_gap_seconds: float
    # circuit breaker
    blocked_threshold: int
    error_threshold: int
    # scheduler loop
    interval_seconds: float
    jitter_percent: float
    run_on_start: bool
    # kafka
    topic_partitions: int
    topic_replication: int
    flush_timeout: float
    # disk spool (spec §2: no data loss when the broker is down)
    spool_dir: str
    spool_enabled: bool

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        def _routes(raw: str) -> tuple[tuple[str, str], ...]:
            routes = []
            for chunk in raw.split(","):
                chunk = chunk.strip().upper()
                if not chunk:
                    continue
                if "-" not in chunk:
                    raise ValueError(f"route {chunk!r} must look like THR-MHD")
                origin, destination = chunk.split("-", 1)
                routes.append((origin.strip(), destination.strip()))
            return tuple(routes) or (("THR", "MHD"),)

        return cls(
            suppliers=tuple(
                s.strip().lower()
                for s in env_str("SUPPLIERS", "flytoday").split(",")
                if s.strip()
            ),
            routes=_routes(env_str("ROUTES", "THR-MHD")),
            departure_date=env_str("DEPARTURE_DATE", "2026-11-15"),
            adults=env_int("ADULTS", 1),
            children=env_int("CHILDREN", 0),
            infants=env_int("INFANTS", 0),
            cabin=env_str("CABIN", "Y"),
            http_timeout=env_float("SUPPLIER_HTTP_TIMEOUT_SECONDS", 20.0),
            max_retries=env_int("SCRAPER_MAX_RETRIES", 3),
            backoff_base=env_float("SCRAPER_BACKOFF_BASE_SECONDS", 2.0),
            backoff_max=env_float("SCRAPER_BACKOFF_MAX_SECONDS", 30.0),
            bucket_capacity=env_float("RATE_BUCKET_CAPACITY", 6.0),
            searches_per_minute=env_float("RATE_SEARCHES_PER_MINUTE", 6.0),
            search_gap_seconds=env_float("SEARCH_GAP_SECONDS", 2.0),
            blocked_threshold=env_int("CIRCUIT_BLOCKED_THRESHOLD", 3),
            error_threshold=env_int("CIRCUIT_ERROR_THRESHOLD", 5),
            interval_seconds=env_float("SCRAPE_INTERVAL_SECONDS", 900.0),
            jitter_percent=min(env_float("SCRAPE_JITTER_PERCENT", 0.30), 0.90),
            run_on_start=env_bool("SCRAPE_RUN_ON_START", True),
            topic_partitions=env_int("RAW_TOPIC_PARTITIONS", 3),
            topic_replication=env_int("RAW_TOPIC_REPLICATION", 1),
            flush_timeout=env_float("KAFKA_FLUSH_TIMEOUT_SECONDS", 30.0),
            spool_dir=env_str("SPOOL_DIR", ".spool"),
            spool_enabled=env_bool("SPOOL_ENABLED", True),
        )


def _supplier_kwargs(name: str) -> dict[str, Any]:
    """Per-supplier config (env contract; base-URL defaults follow the dossier).

    The reverse-engineered endpoint paths and the ab-channel value are
    REQUIRED env config, deliberately absent from the codebase: without this
    env matrix a clone of the repo cannot scrape anything (see .env.example).
    Fails at build_worker time, not mid-loop — misconfiguration is loud.
    The browser identity (SCRAPER_USER_AGENT) and the Alibaba poll cadence are
    optional overrides so a WAF fingerprint change is an env edit, not a deploy."""

    def _required(var: str) -> str:
        value = env_str(var, "")
        if not value:
            raise ValueError(
                f"{var} is not set — supplier {name!r} is enabled but this "
                "deployment carries no endpoint configuration (see .env.example)"
            )
        return value

    user_agent = env_str("SCRAPER_USER_AGENT", CHROME_USER_AGENT)
    if name == "flytoday":
        return {
            "base_url": env_str("FLYTODAY_BASE_URL", "https://www.flytoday.ir"),
            "search_path": _required("FLYTODAY_SEARCH_PATH"),
            "token_path": _required("FLYTODAY_TOKEN_PATH"),
            "user_agent": user_agent,
        }
    if name == "alibaba":
        return {
            "base_url": env_str("ALIBABA_BASE_URL", "https://ws.alibaba.ir"),
            "search_path": _required("ALIBABA_SEARCH_PATH"),
            "ab_channel": _required("ALIBABA_AB_CHANNEL"),
            "user_agent": user_agent,
            "poll_interval": env_float("ALIBABA_POLL_INTERVAL_SECONDS",
                                       DEFAULT_POLL_INTERVAL_SECONDS),
            "max_polls": env_int("ALIBABA_MAX_POLLS", DEFAULT_MAX_POLLS),
        }
    return {}
