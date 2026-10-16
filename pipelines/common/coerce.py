"""Value coercion for untrusted supplier/JSON payloads.

Extracted from three byte-identical copies (suppliers/flytoday.py and
suppliers/alibaba.py `_int_or_none`, kafka_to_db.py `_to_int`/`_to_str`) —
one definition so the defensive semantics can never drift apart.
"""

from __future__ import annotations

from typing import Any


def str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
