"""Local disk spool for scans that could not reach Kafka (spec §2 DLQ/disk item).

If the broker is unreachable at produce time, the worker persists the raw scan
(key + bytes + headers) here instead of dropping it, and replays the queue on a
later pass once the broker recovers — "the worker must not crash or drop data".

Durability semantics:
    * writes are atomic (tmp file + os.replace) — a crash can never leave a
      half-written spool entry;
    * files are deleted only AFTER the flush that confirms delivery, so a crash
      mid-replay re-delivers some scans — at-least-once, which downstream
      idempotency (upsert PKs, HSET/HDEL) absorbs by design;
    * corrupt files are moved aside (.corrupt), never silently deleted.

Boundary: this covers produce-time unavailability (the spec's case). Asynchronous
delivery failures after the broker accepted a message are surfaced by
flush_or_raise()/exit codes — the next pass re-scrapes.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
import uuid
from pathlib import Path
from collections.abc import Callable, Sequence

from .errors import KafkaPipelineError

log = logging.getLogger("torob.spool")

# (key, value, headers) — mirrors ResilientProducer.produce's signature
PublishFn = Callable[[str, bytes, list[tuple[str, bytes]]], None]


class DiskSpool:
    def __init__(self, directory: str) -> None:
        self._dir = Path(directory)
        self._last_ns = 0

    # -- write -----------------------------------------------------------------

    def write(
        self, key: str, value: bytes, headers: Sequence[tuple[str, bytes]]
    ) -> str:
        """Persist one scan atomically; returns the spool file path."""
        self._dir.mkdir(parents=True, exist_ok=True)
        envelope = {
            "version": 1,
            "spooled_at": time.time(),
            "key": key,
            "value_b64": base64.b64encode(value).decode("ascii"),
            "headers": [
                [name, base64.b64encode(blob).decode("ascii")]
                for name, blob in headers
            ],
        }
        # Filename clock must be STRICTLY increasing or name-sort breaks FIFO:
        # time.time_ns() quantizes on Windows (adjacent rapid calls collide or
        # invert ~always), and the uuid suffix would then break the tie randomly.
        now = time.time_ns()
        if now <= self._last_ns:
            now = self._last_ns + 1
        self._last_ns = now
        final = self._dir / f"{now}-{uuid.uuid4().hex[:8]}.json"
        tmp = final.with_suffix(".tmp")
        tmp.write_text(json.dumps(envelope), encoding="utf-8")
        os.replace(tmp, final)
        return str(final)

    # -- read / replay -----------------------------------------------------------

    def _files(self) -> list[Path]:
        if not self._dir.exists():
            return []
        return sorted(self._dir.glob("*.json"))  # ns-precision names → FIFO order

    def pending(self) -> int:
        return len(self._files())

    def replay(self, publish: PublishFn) -> tuple[list[Path], int]:
        """Enqueue spooled scans FIFO (stopping if the broker is still down).

        Returns (enqueued_paths, still_pending). Files are NOT deleted here —
        call discard() only after the flush that confirms delivery.
        """
        enqueued: list[Path] = []
        for path in self._files():
            try:
                envelope = json.loads(path.read_text(encoding="utf-8"))
                value = base64.b64decode(envelope["value_b64"])
                headers = [
                    (name, base64.b64decode(blob))
                    for name, blob in envelope.get("headers", [])
                ]
                key = str(envelope["key"])
            except (ValueError, KeyError, TypeError, OSError) as exc:
                log.error("corrupt spool file %s moved aside (.corrupt): %s",
                          path.name, exc)
                os.replace(path, path.with_suffix(".corrupt"))
                continue
            try:
                publish(key, value, headers)
            except KafkaPipelineError as exc:
                log.error("spool replay stopped — broker still unreachable: %s", exc)
                break
            enqueued.append(path)
        return enqueued, self.pending()

    def discard(self, paths: Sequence[Path]) -> None:
        for path in paths:
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:  # pragma: no cover — platform-level failure
                log.warning("could not discard spool file %s: %s", path, exc)
