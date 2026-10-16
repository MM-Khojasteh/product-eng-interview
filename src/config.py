"""BFF configuration: env parsing + root logging bootstrap.

crm-monitoring pattern (src/auth/config.py): a frozen Settings dataclass built
by from_env(), surfaced two ways — module-level snapshot constants keep every
existing `from src.config import X` import working with the SAME at-import
env-read timing (tests set TOROB_FORCE_MOCK before first import), and
get_settings() hands new code the typed settings object. The pipelines'
KafkaSettings contract is imported separately in scanbus.py so the BFF ↔
worker client config can never drift.

Importing this module configures the root logger, so it must be imported
BEFORE any module that logs at import time (scanbus's Kafka-availability
warning) — src.main imports it first for exactly that reason.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache

# Plain `uvicorn main:app` configures only uvicorn's own loggers — third-party
# INFO logs (the BFF trace) hit no handler and vanish. Configure the root
# handler so the bootstrap line in src.main is always visible in dev;
# TOROB_LOG_LEVEL to quiet it.
logging.basicConfig(
    level=os.getenv("TOROB_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)


@dataclass(frozen=True)
class Settings:
    """Every BFF-level knob in one frozen value object."""

    redis_url: str
    database_url: str | None
    openai_api_key: str | None
    openai_base_url: str | None
    itinerary_model: str
    itinerary_embedding_model: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            # AI Itinerary Planner (src/itinerary/) — all three are guarded
            # seams: unset DATABASE_URL ⇒ the planner serves the demo seed
            # (readmodel.py's None-client discipline), unset OPENAI_API_KEY ⇒
            # the deterministic fallback narrative + popularity ranking
            # instead of vector search. DATABASE_URL is already part of the
            # INFRASTRUCTURE §2.4 env contract (the pipelines read it too);
            # OPENAI_API_KEY (+ OPENAI_BASE_URL for gateways) are the additions.
            database_url=os.getenv("DATABASE_URL"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            # Any OpenAI-compatible gateway (OpenRouter:
            # https://openrouter.ai/api/v1 — one key, many vendors incl.
            # Gemini). Unset ⇒ the SDK default, api.openai.com.
            openai_base_url=os.getenv("OPENAI_BASE_URL"),
            itinerary_model=os.getenv("TOROB_ITINERARY_MODEL", "gpt-4o-mini"),
            itinerary_embedding_model="text-embedding-3-small",  # 1536-d — matches models.Place
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton — env is read exactly once per process."""
    return Settings.from_env()


# Snapshot constants — the historical import surface. Each delegates to the
# cached Settings once, so import-time env reads behave exactly as before the
# Settings object existed.
REDIS_URL = get_settings().redis_url
DATABASE_URL = get_settings().database_url
OPENAI_API_KEY = get_settings().openai_api_key
OPENAI_BASE_URL = get_settings().openai_base_url
TOROB_ITINERARY_MODEL = get_settings().itinerary_model
TOROB_ITINERARY_EMBEDDING_MODEL = get_settings().itinerary_embedding_model
