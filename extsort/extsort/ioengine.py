"""Asynchronous disk-writer engine.

Each job owns one :class:`JobWriter` thread.  Disk *writes* (spill runs,
intermediate merge runs, final output) are handed to that thread; disk reads
happen synchronously on the job's own worker thread.  Either way, every byte
in flight is covered by a live reservation (kind "io").

Publish protocol: data is written to a ``.tmp`` file and atomically renamed
to its final name only when the file is closed successfully and the job is
still alive.  A cancelled/failed job's late write completions therefore
cannot publish anything: the tmp file is deleted instead.

Cancellation: ``cancel()`` lets the currently executing write syscall finish
(it cannot be interrupted), discards queued items (returning their buffer
reservations to the job's pool), suppresses all pending renames and deletes
the tmp files.  ``join()`` then guarantees every reservation owned by the
writer has been handed back before the job's final cleanup runs -- so each
reservation is released exactly once.
"""
from __future__ import annotations

import os
import threading
from collections import deque

from .arbiter import BufferArbiter, Reservation
from .events import EventLog


class JobWriter:
    def __init__(self, engine: "IOEngine", job_id: str):
        self.engine = engine
        self.job_id = job_id
        self._arb: BufferArbiter = engine.arbiter
        self._events: EventLog = engine.events
        self._cond = threading.Condition()
        self._q: deque[tuple] = deque()
        self._cancelled = False
        self._failed: BaseException | None = None
        self._files: dict[int, dict] = {}
        self._next_idx = 0
        self._busy = False
        self._writes_done = 0
        self._thread = threading.Thread(target=self._run, name=f"writer-{job_id}", daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ API
    def open_file(self, tmp_path: str, final_path: str, on_closed) -> int:
        with self._cond:
            idx = self._next_idx
            self._next_idx += 1
            self._q.append(("open", idx, tmp_path, final_path, on_closed))
            self._cond.notify_all()
            return idx

    def write_buffer(self, idx: int, buf: bytearray, res: Reservation, nbytes: int):
        """Hand a filled buffer to the writer.  The reservation is transferred
        to kind "io" here (same object, counted once) and transferred back to
        the job's buffer pool by the writer thread on completion."""
        self._arb.transfer(res, "io")
        with self._cond:
            self._q.append(("write", idx, buf, res, nbytes))
            self._cond.notify_all()

    def close_file(self, idx: int):
        with self._cond:
            self._q.append(("close", idx))
            self._cond.notify_all()

    def cancel(self):
        with self._cond:
            self._cancelled = True
            self._cond.notify_all()

    def wait_idle(self):
        """Block until the queue is drained and no item is executing."""
        with self._cond:
            while self._q or self._busy:
                self._cond.wait()

    def join(self, timeout: float | None = None):
        self._thread.join(timeout)
        return not self._thread.is_alive()

    @property
    def failed(self):
        return self._failed

    # ------------------------------------------------------------------ impl
    def _run(self):
        while True:
            with self._cond:
                while not self._q:
                    if self._cancelled:
                        return
                    self._cond.wait()
                item = self._q.popleft()
                self._busy = True
            try:
                self._process(item)
            finally:
                with self._cond:
                    self._busy = False
                    self._cond.notify_all()

    def _process(self, item):
        op = item[0]
        if op == "open":
            _, idx, tmp_path, final_path, on_closed = item
            if self._cancelled or self._failed is not None:
                self._files[idx] = {"tmp": tmp_path, "final": final_path,
                                    "fp": None, "on_closed": on_closed, "dead": True}
                return
            fp = open(tmp_path, "wb")
            self._files[idx] = {"tmp": tmp_path, "final": final_path, "fp": fp,
                                "on_closed": on_closed, "dead": False}
            self._events.emit("IO_OPEN", self.job_id, path=tmp_path,
                              total_reserved=self._arb.total_reserved, budget=self._arb.budget)
            return

        if op == "write":
            _, idx, buf, res, nbytes = item
            f = self._files.get(idx)
            try:
                if self._cancelled or self._failed is not None or f is None or f["dead"]:
                    raise _Discarded()
                hook = self.engine.before_write_hook  # test-only hook
                if hook is not None:
                    hook(self.job_id, f["tmp"])
                if self.engine.fail_first_write_for == self.job_id and self._writes_done == 0:
                    raise OSError("injected write failure")
                self._events.emit("IO_WRITE_START", self.job_id, path=f["tmp"], nbytes=nbytes,
                                  total_reserved=self._arb.total_reserved, budget=self._arb.budget)
                f["fp"].write(memoryview(buf)[:nbytes])
                self._writes_done += 1
                self._events.emit("IO_WRITE_DONE", self.job_id, path=f["tmp"], nbytes=nbytes,
                                  total_reserved=self._arb.total_reserved, budget=self._arb.budget)
            except _Discarded:
                pass
            except BaseException as exc:  # real or injected disk failure
                self._failed = exc
                f["dead"] = True
                self._events.emit("IO_WRITE_FAIL", self.job_id, path=f["tmp"], error=repr(exc),
                                  total_reserved=self._arb.total_reserved, budget=self._arb.budget)
            finally:
                # hand the buffer back to the job pool in every case
                self._arb.transfer(res, "buffer")
                pool = self.engine.buffer_pools.get(self.job_id)
                if pool is not None:
                    pool.buffer_returned(res)
            return

        if op == "close":
            _, idx = item
            f = self._files.pop(idx, None)
            if f is None:
                return
            ok, err = False, self._failed
            try:
                if f["fp"] is not None and not f["dead"] and not self._cancelled and err is None:
                    f["fp"].flush()
                    if self.engine.fsync:
                        os.fsync(f["fp"].fileno())
                    f["fp"].close()
                    os.replace(f["tmp"], f["final"])  # atomic publish
                    ok = True
                    self._events.emit("IO_PUBLISH", self.job_id, path=f["final"],
                                      total_reserved=self._arb.total_reserved,
                                      budget=self._arb.budget)
                else:
                    if f["fp"] is not None:
                        f["fp"].close()
                    if os.path.exists(f["tmp"]):
                        os.unlink(f["tmp"])  # unpublished tmp file is deleted
            except BaseException as exc:
                ok, err = False, exc
                self._failed = self._failed or exc
                try:
                    if os.path.exists(f["tmp"]):
                        os.unlink(f["tmp"])
                except OSError:
                    pass
            cb = f.get("on_closed")
            if cb is not None:
                cb(ok=ok, error=err, tmp=f["tmp"], final=f["final"])
            return

        raise AssertionError(f"unknown writer op {op!r}")


class _Discarded(Exception):
    pass


class IOEngine:
    def __init__(self, arbiter: BufferArbiter, events: EventLog, fsync: bool = False):
        self.arbiter = arbiter
        self.events = events
        self.fsync = fsync
        self.buffer_pools: dict[str, object] = {}   # job_id -> BufferPool
        # ---- test/fault-injection hooks ----
        self.before_write_hook = None               # fn(job_id, tmp_path)
        self.fail_first_write_for: str | None = None

    def new_writer(self, job_id: str) -> JobWriter:
        return JobWriter(self, job_id)
