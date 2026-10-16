"""Input parsing — user input (query strings, /flights/{ref}) → canonical
codes / datenum / Flight.

Extracted from main.py §"Input parsing" (zero functional changes).
"""

from __future__ import annotations

import re

from src import mock_catalog
from src.constants import AIRPORTS, TIME_BUCKETS
from src.formatting import EN_DIGITS
from src.models import Flight

_NAME_TO_CODE: dict[str, str] = {}  # 'تهران' → 'THR' (first code wins on shared names)
for _code, _name in AIRPORTS.items():
    _NAME_TO_CODE.setdefault(_name, _code)

# Hot-path patterns precompiled once (parse_airport runs 2× per search request;
# the _KG_RE precedent below) — identical semantics to the inline re.* calls.
_AIRPORT_CODE_RE = re.compile(r"([A-Za-z]{3})\s*\)?\s*$")
_DIGITS_RE = re.compile(r"\D")
_HEX_KEY_RE = re.compile(r"[0-9a-f]{16}")
_INT_RE = re.compile(r"\d+")


def parse_airport(raw: str | None) -> str | None:
    """'تهران (THR)' | 'تهران' | 'THR' → 'THR' (None when unknown).
    The bare-Persian-name form matters: the search field's placeholder invites
    typing «تهران», and datalist picks are not guaranteed (typing beats
    selecting on the path of least resistance)."""
    if not raw:
        return None
    match = _AIRPORT_CODE_RE.search(raw.strip())
    code = match.group(1).upper() if match else raw.strip().upper()
    if code in AIRPORTS:
        return code
    return _NAME_TO_CODE.get(raw.strip())


def parse_date(raw: str | None) -> str | None:
    """'۱۴۰۵/۰۷/۱۳' | '14050713' → '14050713' (Jalali, digits only)."""
    if not raw:
        return None
    digits = _DIGITS_RE.sub("", raw.translate(EN_DIGITS))
    return digits if len(digits) == 8 else None


def find_flight(ref: str) -> Flight | None:
    """Resolve '/flights/{ref}' — 'ORIGIN.DEST.datenum.<physical_flight_key>'.
    The physical key (hex, dto.py §3) is the identity; the route-day prefix only
    lets the mock catalog rebuild statelessly. Post-cutover: PG lookup by key
    (SELECT … WHERE physical_flight_key = %(id)s) — never a list position."""
    parts = ref.split(".")
    if len(parts) != 4:
        return None
    origin, destination, datenum, key = parts
    if origin not in AIRPORTS or destination not in AIRPORTS:
        return None
    if len(datenum) != 8 or not datenum.isdigit() or not _HEX_KEY_RE.fullmatch(key):
        return None
    flight = mock_catalog._FLIGHT_INDEX.get(ref)
    if flight is not None:
        return flight
    return next(  # MOCK SEAM (delete at cutover) — the stateless mock-catalog rebuild
        (f for f in mock_catalog.generate_flights(origin, destination, datenum) if f.id == key), None
    )


def _opt_int(raw: str | int | None) -> int | None:
    """''-tolerant int for query params (empty form inputs are always sent)."""
    if raw is None:
        return None
    digits = _DIGITS_RE.sub("", str(raw))
    return int(digits) if digits else None


opt_int = _opt_int  # public seam — routers use this; the underscore name stays
                    # for the root main.py re-export shim


def bucket_of(flight: Flight) -> str | None:
    for key, (lo, hi, _, _) in TIME_BUCKETS.items():
        if lo <= flight.dep_hour <= hi:
            return key
    return None


_KG_RE = re.compile(r"(\d+)\s*(?:kg|kgs|کیلو\w*)", re.IGNORECASE)


def baggage_kg(raw: str | None) -> int | None:
    """Free-form supplier baggage string → checked-bag kg, or None (hide chip).
    Suppliers emit Persian («۳۰ کیلوگرم») and Latin («2×23KG») forms; piece
    counts («۱ قطعه») must never render «بار ۱ کیلوگرم», so a bare number is
    only accepted when the whole string is digits."""
    if not raw:
        return None
    normalized = raw.translate(EN_DIGITS).strip()
    match = _KG_RE.search(normalized)
    if match:
        return int(match.group(1))
    if re.fullmatch(r"\d+", normalized):
        return int(normalized)
    return None


def refundable_flag(method: str | None) -> bool | None:
    """Supplier refund vocabulary → tri-state. Only flytoday emits refund_method
    today («Offline» = agency-side refund); unknown tokens stay None — an honest
    hidden chip, never a guessed one."""
    if not method:
        return None
    token = method.strip().lower()
    if "non" in token or "غیر" in token:
        return False
    if token in ("online", "offline", "refundable"):
        return True
    return None
