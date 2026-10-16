"""Time normalization — dossier §5: domestic times are Asia/Tehran, fixed +03:30
(Iran abolished DST in 2022). Store UTC, keep the local rendering alongside."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

TEHRAN_TZ = timezone(
    # fixed +03:30, no DST since 2022 (dossier §5 cross-cutting rules)
    timedelta(hours=3, minutes=30)
)

UTC = timezone.utc


def to_aware(value: Any, naive_tz: timezone) -> datetime | None:
    """ISO string → tz-aware datetime (naive values interpreted in `naive_tz`)."""
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=naive_tz)


def localize(value: Any) -> tuple[str | None, str | None]:
    """Supplier naive-local timestamp → (local ISO with +03:30, UTC ISO).

    Unparseable values pass through raw with a None UTC counterpart so downstream
    can flag them instead of silently dropping the record.
    """
    if value in (None, ""):
        return None, None
    try:
        local = datetime.fromisoformat(str(value)).replace(tzinfo=TEHRAN_TZ)
    except ValueError:
        return str(value), None
    return local.isoformat(), local.astimezone(UTC).isoformat()
