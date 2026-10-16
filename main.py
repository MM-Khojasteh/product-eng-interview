"""Torob Travel — BFF entry shim.

The application lives in src/ (the crm-monitoring src/ layout: thin root
shim → package). This file only re-exports the public surface so every
historical invocation keeps working unchanged:

    uvicorn main:app --reload --port 8000
    python main.py
    import main                      (tests/web/test_frontend.py)

The architecture narrative (page model, HTMX flow, read model) is the module
docstring of src/main.py.

NB — test monkeypatching: patch runtime collaborators on their OWNING module
(readmodel.read_quotes / readmodel.rds / app_state._LIVE_READY /
app_state._pulse_state / mock_catalog._FLIGHT_INDEX) — assignments on this
shim's re-exports do NOT propagate into the package. In-place mutations of
re-exported mutable objects (the OrderedDicts) do.
"""

from __future__ import annotations

from pipelines.common.dto import physical_flight_key  # (re-export shim — see module docstring)
from src import app_state, flights, mock_catalog, readmodel, scanbus  # noqa: F401  (re-export shim — see module docstring)
from src.constants import (  # noqa: F401  (re-export shim — see module docstring)
    AGENCIES,
    AIRLINES,
    AIRPORTS,
    MAX_OFFER_POLL_ATTEMPTS,
    MAX_POLL_ATTEMPTS,
    TIME_BUCKETS,
)
from src.filters import (  # noqa: F401  (re-export shim — see module docstring)
    DEFAULT_DATENUM,
    POPULAR_ROUTES,
    Filters,
    _index_context,
    apply_filters,
    flight_results_context,
    parse_filters,
)
from src.formatting import date_fa, fa_num, toman  # noqa: F401  (re-export shim — see module docstring)
from src.main import app  # noqa: F401  (re-export shim — see module docstring)
from src.models import Agency, Flight  # noqa: F401  (re-export shim — see module docstring)
from src.parsing import _opt_int, bucket_of, find_flight, parse_airport, parse_date  # noqa: F401  (re-export shim — see module docstring)
from src.readmodel import (  # noqa: F401  (re-export shim — see module docstring)
    REDIS_URL,
    _rd,
    jalali_to_greg_iso,
    quote_key,
    read_quotes,
    toman_from_rial,
)
from src.routers.pages import CONTENT_PAGES  # noqa: F401  (re-export shim — see module docstring)
from src.view_templates import templates  # noqa: F401  (re-export shim — see module docstring)

# Historical main.* names — module-attribute references (not copies) so reads
# keep working for callers/tests; see the shim NB above before PATCHING here.
rds = readmodel.rds
producer = scanbus.producer
SEARCH_TOPIC = scanbus.SEARCH_TOPIC
_LIVE_READY = app_state._LIVE_READY
_FLIGHT_INDEX = mock_catalog._FLIGHT_INDEX
_pulse_state = app_state._pulse_state

group_quotes = flights.group_quotes
offers_for_flight = flights.offers_for_flight
resolve_flight = flights.resolve_flight
live_offers = flights.live_offers
generate_flights = mock_catalog.generate_flights
agencies_for = mock_catalog.agencies_for
price_history = mock_catalog.price_history
request_scan = scanbus.request_scan

if __name__ == "__main__":
    import os

    import uvicorn

    # Dev default: single process + reload (template edits appear instantly).
    # Production: TOROB_WEB_WORKERS=4 python main.py — the read model is
    # stateless across workers BY DESIGN (resolve_flight rebuilds from Redis;
    # the pulse LRU is documented per-worker), so N workers is N× render/CPU
    # throughput on one bounded Redis pool each.
    workers = max(1, int(os.getenv("TOROB_WEB_WORKERS", "1") or "1"))
    if workers > 1:
        uvicorn.run("main:app", host="127.0.0.1", port=8000, workers=workers)
    else:
        uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
