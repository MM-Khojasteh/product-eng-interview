"""Offline tests for scripts/dev.py — the one-command dev stack runner.

Run:  python -m pytest tests/web/test_dev_runner.py -q
No process is spawned: subprocess.Popen is replaced by a recorder.
"""

from __future__ import annotations

import io
import subprocess

from scripts import dev


class _FakeProcess:
    def __init__(self, argv: list[str], **kwargs: object) -> None:
        self.argv, self.kwargs, self.pid = argv, kwargs, 4242
        self.stdout, self.stderr = io.StringIO(""), io.StringIO("")

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def poll(self) -> int:
        return 0


def test_only_web_runs_on_its_own_console(monkeypatch, tmp_path) -> None:
    """uvicorn's Windows reload sends CTRL_C_EVENT to its whole console: web
    must be isolated, while the pipeline services share the runner's console
    so a real Ctrl-C still reaches them."""
    launched: list[_FakeProcess] = []

    def _record(argv, **kwargs):
        launched.append(_FakeProcess(argv, **kwargs))
        return launched[-1]

    monkeypatch.setattr(dev, "ROOT", tmp_path)  # no .env here: os.environ stays untouched
    monkeypatch.setattr(dev.subprocess, "Popen", _record)

    assert dev.main() == 0
    flags = {tag: process.kwargs["creationflags"]
             for (tag, _), process in zip(dev.SERVICES, launched, strict=True)}
    assert flags == {"norm": 0, "live": 0, "worker": 0,
                     "web": getattr(subprocess, "CREATE_NO_WINDOW", 0)}
