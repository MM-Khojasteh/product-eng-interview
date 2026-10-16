"""Run a Python module with the project .env loaded into os.environ.

The Git-Bash-proof launcher: MSYS rewrites leading-slash env values (the
supplier endpoint paths) when they cross a shell boundary into a native Windows
process, but values parsed out of the file inside Python never do. Keys
already present in the environment win, so explicit `FOO=bar python …`
still overrides.

    python scripts/run_with_env.py main                  # the BFF (reload)
    python scripts/run_with_env.py pipelines.raw_worker  # any pipeline
    python scripts/run_with_env.py uvicorn main:app --port 8000
"""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[1]


def load_env(path: Path) -> int:
    """setdefault() every KEY=VALUE line; return how many were applied."""
    applied = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value
            applied += 1
    return applied


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    env_path = ROOT / ".env"
    if env_path.exists():
        print(f"env: {load_env(env_path)} vars from .env (explicit env wins)")
    target, *args = sys.argv[1:]
    sys.argv = [target, *args]
    runpy.run_module(target, run_name="__main__", alter_sys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
