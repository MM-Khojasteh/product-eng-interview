"""Live-mode arming flag + cross-request pulse state (crm-monitoring
app_state.py pattern: an honest, documented module of module-level mutable
state instead of a DI container).

_LIVE_READY is the single flag every live-vs-mock branch reads DYNAMICALLY
(module attribute, never a frozen import binding) — that is what lets the test
suite flip the whole BFF between live and mock semantics in-process. The
pulse LRU must be shared across requests on one worker for the OOB ticker's
2-agreement completion rule to hold.
"""

from __future__ import annotations

import os
from collections import OrderedDict

from pipelines.common.lrucache import remember_latest

from src import readmodel

# Live mode arms only when the conversion + cache layers are importable.
# TOROB_FORCE_MOCK=1 pins the deterministic mock catalog regardless.
_LIVE_READY = (
    readmodel.rds is not None
    and readmodel.jdatetime is not None
    and os.getenv("TOROB_FORCE_MOCK", "") not in ("1", "true")
)

# route-key → (last tick's physical ids, last (count, max captured_at) signature,
# consecutive signature agreements) — drives OOB adds/removes and scan-completion
_pulse_state: "OrderedDict[str, tuple[frozenset, tuple[int, str] | None, int]]" = OrderedDict()
_PULSE_STATE_LIMIT = 500


def _remember_pulse(key: str, ids: frozenset,
                    signature: "tuple[int, str] | None", streak: int) -> None:
    remember_latest(_pulse_state, key, (ids, signature, streak), _PULSE_STATE_LIMIT)


remember_pulse = _remember_pulse  # public seam (src/pulse.py); underscore name
                                  # stays for the root main.py re-export shim
