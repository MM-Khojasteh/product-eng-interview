"""One-command dev stack — every service in ONE terminal, logs interleaved.

    python scripts/dev.py

Starts (assuming the data plane is up: `docker compose up -d kafka-init
redis postgres`):

    [web]    python main.py            — uvicorn :8000, reload on
    [worker] python -m pipelines.raw_worker
    [norm]   python -m pipelines.normalizer
    [live]   python -m pipelines.liveness

.env is parsed and exported BEFORE spawning (the Git-Bash-proof trick from
run_with_env — MSYS never touches the values; explicit env still wins), and
every child inherits it. Each output line is prefixed with its service tag
so one glance shows who did what. Ctrl-C stops the whole tree (taskkill /T
on Windows so the uvicorn reload child doesn't orphan).
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:  # Windows consoles default to cp1252 — supplier bodies are UTF-8 JSON
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

from scripts.run_with_env import load_env  # noqa: E402  (path insert above)

# tag → argv. Order matters only for log readability (worker last = the
# noisiest startup lines end the boot sequence).
SERVICES: list[tuple[str, list[str]]] = [
    ("norm", [sys.executable, "-m", "pipelines.normalizer"]),
    ("live", [sys.executable, "-m", "pipelines.liveness"]),
    ("web", [sys.executable, "main.py"]),
    ("worker", [sys.executable, "-m", "pipelines.raw_worker"]),
]

TAG_WIDTH = max(len(tag) for tag, _ in SERVICES)

# uvicorn's Windows reloader restarts its server with CTRL_C_EVENT, which
# reaches EVERY process on the console — without its own (hidden) console a
# single .py save would stop this runner and every sibling service.
OWN_CONSOLE_FLAGS = getattr(subprocess, "CREATE_NO_WINDOW", 0)
OWN_CONSOLE_TAGS = frozenset({"web"})


def pump(tag: str, stream, *, err: bool = False) -> None:
    """Relay one child's stdout/stderr, one prefixed line at a time."""
    for line in iter(stream.readline, ""):
        pipe = sys.stderr if err else sys.stdout
        print(f"[{tag:<{TAG_WIDTH}}] {line.rstrip()}", file=pipe, flush=True)
    stream.close()


def stop_all(procs: list[subprocess.Popen]) -> None:
    for proc in procs:
        if proc.poll() is not None:
            continue
        if os.name == "nt":  # TerminateProcess alone orphans uvicorn's reload child
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           capture_output=True)
        else:
            proc.terminate()
    for proc in procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> int:
    if (ROOT / ".env").exists():
        print(f"[dev] {load_env(ROOT / '.env')} vars loaded from .env "
              f"(explicit env wins)", flush=True)
    else:
        print("[dev] no .env — supplier scrapes will fail; cp .env.example "
              ".env first", flush=True)
    print("[dev] data plane assumed up: docker compose up -d kafka-init "
          "redis postgres", flush=True)

    procs: list[subprocess.Popen] = []
    threads: list[threading.Thread] = []
    try:
        for tag, argv in SERVICES:
            proc = subprocess.Popen(
                argv, cwd=ROOT, env=os.environ.copy(),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, encoding="utf-8", errors="replace",
                creationflags=OWN_CONSOLE_FLAGS if tag in OWN_CONSOLE_TAGS else 0,
            )
            procs.append(proc)
            threads.append(threading.Thread(target=pump, args=(tag, proc.stdout),
                                             daemon=True))
            threads.append(threading.Thread(target=pump, args=(tag, proc.stderr),
                                             kwargs={"err": True}, daemon=True))
            for t in threads[-2:]:
                t.start()
            print(f"[dev] started {tag} (pid {proc.pid}): "
                  f"{' '.join(argv[1:])}", flush=True)

        print("[dev] all services up — http://localhost:8000 · Ctrl-C stops "
              "everything", flush=True)
        procs[0].wait()  # any service exiting (or Ctrl-C) unwinds to finally
    except KeyboardInterrupt:
        print("\n[dev] Ctrl-C — stopping all services…", flush=True)
    finally:
        stop_all(procs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
