"""Pytest bootstrap for the torob suites (crm-monitoring convention: a minimal
conftest — only path setup; each test module stays self-contained).

Transport stubs (confluent_kafka / psycopg2 / redis) live in
tests/pipelines/_stubs.py and the TOROB_FORCE_MOCK pin at the top of each
tests/web/ module — deliberately NOT shared here, so each module's
dependency surface stays exactly what it was as a standalone script.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
