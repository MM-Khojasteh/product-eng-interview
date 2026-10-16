"""Geographic planning — گام ۱ K-Means day clustering + گام ۲ greedy TSP routing.

Extracted from services.py (zero functional changes). The SYMBOLIC stages own
truth: deterministic (random_state pinned) and pure given PlaceOut inputs, so
tests pin behaviour, not mocks. sklearn missing ⇒ longitude-banding fallback
(the degradation contract in services.py's docstring).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from statistics import fmean

from src.itinerary.seed import PlaceOut

try:  # day clustering; ImportError ⇒ longitude banding fallback (below)
    from sklearn.cluster import KMeans
except ImportError:  # frontend-only dev environment
    KMeans = None  # type: ignore[assignment]

EARTH_RADIUS_KM = 6371.0


# ── گام ۱ — geographic day clustering ───────────────────────────────────────

def cluster_places(places: Sequence[PlaceOut], days: int) -> list[list[PlaceOut]]:
    """Split `places` into `days` geographic clusters — one per day, so the
    traveller never crosses the island twice (Kish is ~15 km, but the same
    contract scales to Mashhad/Istanbul).

    K-Means on raw lat/lng (degrees): at city scale equirectangular
    distortion is negligible and it keeps the feature dependency-free of
    coordinate reprojection. Deterministic — random_state pinned, n_init
    fixed — the tests and any future response cache rely on it.

    Degenerate cases (no POIs, ≤ days POIs, sklearn missing) fall to
    _lng_split_clusters: west→east banding, which for an island is a
    legitimate day sweep on its own."""
    days = max(1, int(days))
    if not places:
        return []
    if len(places) <= days or KMeans is None:
        return _lng_split_clusters(places, days)

    labels = KMeans(n_clusters=days, n_init=10, random_state=42).fit_predict(
        [[p.lat, p.lng] for p in places]
    )
    clusters: list[list[PlaceOut]] = [[] for _ in range(days)]
    for place, label in zip(places, labels):
        clusters[int(label)].append(place)
    clusters = [c for c in clusters if c]
    # Day order = west→east centroid sweep, so the plan reads as one
    # progression across the island instead of random day numbering.
    clusters.sort(key=lambda c: fmean(p.lng for p in c))
    return clusters


def _lng_split_clusters(places: Sequence[PlaceOut], days: int) -> list[list[PlaceOut]]:
    """Deterministic fallback: sort west→east, cut into even contiguous
    bands. Also the degenerate-case path (fewer POIs than days)."""
    ordered = sorted(places, key=lambda p: (p.lng, p.lat))
    base, extra = divmod(len(ordered), days)
    out: list[list[PlaceOut]] = []
    i = 0
    for d in range(days):
        n = base + (1 if d < extra else 0)
        band = ordered[i:i + n]
        if band:
            out.append(band)
        i += n
    return out


# ── گام ۲ — per-day routing (greedy TSP) ────────────────────────────────────

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    ph1, ph2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(ph1) * math.cos(ph2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def route_day(cluster: Sequence[PlaceOut],
              origin: tuple[float, float] | None = None) -> list[PlaceOut]:
    """Greedy nearest-neighbour TSP over one day's cluster: start from the
    POI closest to `origin` (the city centre — where hotels/transfer drop
    you), then always hop to the nearest unvisited POI. Straight-line
    haversine; the OSRM road-distance upgrade (notes §2 گام ۲) swaps this
    helper without touching the caller. O(n²) — fine at ≤ ~6 stops/day."""
    remaining = list(cluster)
    if len(remaining) <= 2:
        return remaining
    if origin is not None:
        first = min(remaining,
                    key=lambda p: _haversine_km(origin[0], origin[1], p.lat, p.lng))
    else:
        first = remaining[0]
    remaining.remove(first)
    ordered = [first]
    while remaining:
        cur = ordered[-1]
        nxt = min(remaining,
                  key=lambda p: _haversine_km(cur.lat, cur.lng, p.lat, p.lng))
        remaining.remove(nxt)
        ordered.append(nxt)
    return ordered
