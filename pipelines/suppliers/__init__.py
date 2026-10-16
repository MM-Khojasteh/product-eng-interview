"""Supplier registry — the Strategy pattern's entry point.

Adding a new OTA = new module implementing SupplierStrategy + one registry line;
the worker, normalizer and everything downstream are untouched (spec § separation
of concerns)."""

from __future__ import annotations

from .alibaba import AlibabaSupplier
from .base import HttpOptions, ParseResult, RawScan, SearchQuery, SupplierStrategy
from .flytoday import FlyTodaySupplier

REGISTRY: dict[str, type[SupplierStrategy]] = {
    FlyTodaySupplier.name: FlyTodaySupplier,
    AlibabaSupplier.name: AlibabaSupplier,
}


def get_supplier(name: str, **kwargs) -> SupplierStrategy:
    try:
        return REGISTRY[name.strip().lower()](**kwargs)
    except KeyError as exc:
        raise KeyError(
            f"unknown supplier {name!r} — registered: {sorted(REGISTRY)}"
        ) from exc


__all__ = [
    "REGISTRY", "get_supplier",
    "SupplierStrategy", "SearchQuery", "RawScan", "ParseResult", "HttpOptions",
    "FlyTodaySupplier", "AlibabaSupplier",
]
