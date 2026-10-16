"""Lazy shared AsyncOpenAI client — the planner's single LLM seam.

Extracted from services.py (zero functional changes): narrative (chat) and
retrieval (embeddings) both go through _openai_client(), and the guarded SDK
import keeps importing the BFF side-effect-free on machines without the SDK
or a key (the degradation contract in services.py's docstring).
"""

from __future__ import annotations

from src.config import OPENAI_API_KEY, OPENAI_BASE_URL

try:
    import openai as _openai
except ImportError:  # frontend-only dev environment
    _openai = None  # type: ignore[assignment]

_client = None


def _openai_client():
    """Lazy shared AsyncOpenAI — created only when a key exists, so importing
    this module (and the whole BFF) stays side-effect-free without one.
    OPENAI_BASE_URL redirects the same seam to any OpenAI-compatible gateway
    (OpenRouter, vLLM, …); unset ⇒ the SDK default api.openai.com."""
    global _client
    if _client is None:
        _client = _openai.AsyncOpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL or None,
            timeout=20.0,
            max_retries=1,
        )
    return _client
