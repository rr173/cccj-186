"""Thread-safe structured event log.

Every arbiter/job/io action emits an event.  Arbiter events carry a
``total_reserved`` snapshot so the budget invariant
``total_reserved <= budget`` can be checked *per event* from the log alone.
"""
from __future__ import annotations

import json
import threading
import time


class EventLog:
    def __init__(self, path: str | None = None, keep_in_memory: bool = True):
        self._lock = threading.Lock()
        self._seq = 0
        self._events: list[dict] = []
        self._keep = keep_in_memory
        self.path = path
        self._fp = open(path, "a", buffering=1, encoding="utf-8") if path else None
        self._t0 = time.monotonic()

    def emit(self, kind: str, job_id: str | None = None, **fields) -> dict:
        with self._lock:
            self._seq += 1
            ev = {
                "seq": self._seq,
                "ts": round(time.monotonic() - self._t0, 6),
                "kind": kind,
                "job": job_id,
            }
            ev.update(fields)
            if self._keep:
                self._events.append(ev)
            if self._fp:
                self._fp.write(json.dumps(ev, ensure_ascii=False, default=str) + "\n")
            return ev

    def snapshot(self) -> list[dict]:
        with self._lock:
            return list(self._events)

    def count(self, kind: str, job_id: str | None = None) -> int:
        with self._lock:
            return sum(
                1
                for e in self._events
                if e["kind"] == kind and (job_id is None or e.get("job") == job_id)
            )

    def close(self):
        with self._lock:
            if self._fp:
                self._fp.flush()
                self._fp.close()
                self._fp = None
