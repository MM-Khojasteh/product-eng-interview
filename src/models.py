"""Domain value objects for the BFF (frozen dataclasses, like dto.py's records).

Extracted from main.py §"Domain data" (zero functional changes).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class Agency:
    key: str
    name: str
    domain: str
    rating: float
    # ds-seller-list trust signals (torob.com sellers-list anatomy). Defaults
    # keep the unknown-supplier fallback in flights.offers_for_flight() valid.
    color: str = "#64748b"      # avatar bg — same gray as group_quotes' unknown airline
    torob_pay: bool = False     # «ترب‌پی» mark (پرداخت امن)
    since: int | None = None    # Gregorian founding year — Postgres entity post-cutover
    logo: str | None = None     # bundled brand mark (/static/logos/agencies/…); None → color chip

    @property
    def years_active(self) -> int | None:
        """«N سال سابقه» — computed at render so it never goes stale."""
        if self.since is None:
            return None
        years = datetime.now(timezone.utc).year - self.since
        return years if years >= 1 else None


@dataclass(frozen=True)
class Flight:
    id: str  # physical_flight_key (dto.py §3) — cross-supplier flight identity
    origin: str
    destination: str
    datenum: str
    airline: str
    flight_no: str
    departs: str
    arrives: str
    duration_text: str
    is_charter: bool
    min_price: int  # کف قیمت بین آژانس‌ها (تومان — converted at the Redis seam)
    seats_left: int
    agency_count: int

    @property
    def ref(self) -> str:
        """URL id: 'ORIGIN.DEST.datenum.<physical_flight_key>'. The route-day
        prefix exists only so the mock catalog can rebuild statelessly on cold
        processes / direct visits — post-cutover the physical key alone resolves
        against the Postgres flight entity."""
        return f"{self.origin}.{self.destination}.{self.datenum}.{self.id}"

    @property
    def dep_hour(self) -> int:
        return int(self.departs[:2])
