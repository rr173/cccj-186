"""External sort / dedup job.

Phases (each admitted under its minimal-working-set envelope):

  READ    -- read the JSONL input in reserved blocks, parse, extract keys,
             accumulate a run; spill (sort + write) when the run cannot grow.
  MERGE   -- k-way merge the spill files; multi-pass when the number of runs
             exceeds the fan-in the envelope can hold; the final pass writes
             the output (applying dedup) and publishes it atomically.

Spilling needs *no* extra memory: the run's line/key bytes are already
reserved, and the serialisation buffers are part of the envelope -- this is
what makes both self-reclaim (fallback) and cooperative reclaim (the reclaim
callback) deadlock-free.

Lock order (never inverted):  run_lock  ->  arbiter lock  ->  pool/writer cond.
The arbiter lock is never held while waiting for I/O; reclaim callbacks are
invoked without it.
"""
from __future__ import annotations

import heapq
import json
import os
import shutil
import struct
import threading

from .arbiter import AllocFailed, BufferArbiter, Cancelled, ConfigError, Reservation

HDR = struct.Struct(">IIQ")  # klen, llen, lineno  (16 bytes, big-endian)


class JobFailed(Exception):
    pass


class LineTooLong(JobFailed):
    pass


class InvalidLine(JobFailed):
    pass


# ---------------------------------------------------------------------- config
class JobConfig:
    def __init__(self, input_path, output_path, *,
                 max_line_bytes=256 * 1024,
                 dedup_key=False,
                 read_block_bytes=256 * 1024,
                 write_buf_bytes=512 * 1024,
                 merge_chunk_bytes=256 * 1024,
                 max_fan_in=16,
                 extra_buffer_timeout=30.0,
                 extra_write_buffers=0,          # test hook: beyond-envelope buffers
                 extra_buffer_bytes=0,
                 extra_after_blocks=0,            # test hook: request extras after N blocks
                 inject_io_failure=False,        # test hook: first write fails
                 hooks=None):                    # test hook: {name: callable}
        self.input_path = input_path
        self.output_path = output_path
        self.max_line_bytes = int(max_line_bytes)
        self.dedup_key = bool(dedup_key)
        self.read_block_bytes = int(read_block_bytes)
        self.write_buf_bytes = int(write_buf_bytes)
        self.merge_chunk_bytes = int(merge_chunk_bytes)
        self.max_fan_in = max(2, int(max_fan_in))
        # how long the (test-only) beyond-envelope extra buffer request may
        # wait; read-path growth reserves are fail-fast (timeout=0): one
        # reclaim round, then the job falls back to spilling its own run
        self.extra_buffer_timeout = float(extra_buffer_timeout)
        self.extra_write_buffers = int(extra_write_buffers)
        self.extra_buffer_bytes = int(extra_buffer_bytes)
        self.extra_after_blocks = int(extra_after_blocks)
        self.inject_io_failure = bool(inject_io_failure)
        self.hooks = hooks or {}


# ---------------------------------------------------------------------- buffer pool
class BufferPool:
    """Fixed set of reserved bytearray buffers shared between the job worker
    (serialisation) and the writer thread (which returns them after I/O).
    Cancellation wakes acquirers via the job's cancel()."""

    def __init__(self, job: "SortJob"):
        self.job = job
        self._cond = threading.Condition()
        self._free: list[tuple[bytearray, Reservation]] = []
        self._by_rid: dict[int, bytearray] = {}

    def add(self, buf: bytearray, res: Reservation):
        with self._cond:
            self._by_rid[res.rid] = buf
            self._free.append((buf, res))
            self._cond.notify_all()

    def acquire(self) -> tuple[bytearray, Reservation]:
        with self._cond:
            while not self._free:
                self.job._check_cancel()
                self._cond.wait()   # woken by buffer_returned() or job.cancel()
            return self._free.pop()

    def try_acquire(self):
        """Pop a free buffer without blocking, or None (caller may then
        reserve a fresh one within its envelope)."""
        with self._cond:
            if not self._free:
                return None
            return self._free.pop()

    def buffer_returned(self, res: Reservation):
        # called by the writer thread after the I/O completed (the arbiter has
        # already transferred the reservation back to kind "buffer")
        buf = self._by_rid.get(res.rid)
        if buf is None:
            return  # pool already torn down; cleanup releases the reservation
        with self._cond:
            self._free.append((buf, res))
            self._cond.notify_all()

    def release_all(self):
        with self._cond:
            items, self._free = self._free, []
            self._by_rid.clear()
        for _, res in items:
            self.job.arbiter.release(res)

    def notify_cancel(self):
        with self._cond:
            self._cond.notify_all()


# ---------------------------------------------------------------------- run reader
class RunReader:
    """Buffered reader for one spill file.

    Reads reserved chunks; records are zero-copy memoryviews into the chunk;
    keys are materialised per chunk under one reservation (needed for heap
    comparisons).  A record straddling a chunk boundary is completed with an
    exact-size remainder read (reserved before the read).  Headers straddling
    a boundary go through a tiny standing scratch buffer (also reserved).
    All reservations of a chunk are released when its last record is consumed.
    """

    def __init__(self, job: "SortJob", path: str):
        self.job = job
        self.path = path
        self.fp = open(path, "rb")
        self.chunk_size = job.merge_chunk
        self._records: list[tuple] = []   # (key_bytes, lineno, line_pieces)
        self._pos = 0
        self._chunk_res: Reservation | None = None
        self._key_res: Reservation | None = None
        self._rem_res: Reservation | None = None
        self._live = 0
        self._eof = False
        self._scratch = bytearray(HDR.size)
        self._scratch_res = job.arbiter.reserve(job.job_id, HDR.size, "merge-read")

    def _fill(self):
        if self._eof:
            return
        arb = self.job.arbiter
        jid = self.job.job_id
        res = arb.reserve(jid, self.chunk_size, "merge-read")
        chunk = self.fp.read(self.chunk_size)
        if len(chunk) < self.chunk_size:
            arb.adjust(res, len(chunk))
        if not chunk:
            arb.release(res)
            self._eof = True
            return
        self._chunk_res = res
        buf = memoryview(chunk)
        n = len(chunk)
        off = 0
        records = []
        while off < n:
            if n - off < HDR.size:
                tail = buf[off:]
                self._scratch[:len(tail)] = tail
                got = self.fp.readinto(memoryview(self._scratch)[len(tail):])
                if got != HDR.size - len(tail):
                    raise JobFailed(f"truncated run file {self.path}")
                klen, llen, lineno = HDR.unpack(self._scratch)
                off = n
            else:
                klen, llen, lineno = HDR.unpack_from(buf, off)
                off += HDR.size
            body = klen + llen
            if n - off >= body:
                key_pieces = [buf[off:off + klen]]
                line_pieces = [buf[off + klen:off + body]]
                off += body
            else:
                # straddling record: exact remainder read, reserved up-front
                have = n - off
                need = body - have
                self._rem_res = arb.reserve(jid, need, "merge-read")
                rem = self.fp.read(need)
                if len(rem) != need:
                    raise JobFailed(f"truncated run file {self.path}")
                remv = memoryview(rem)
                if klen <= have:
                    key_pieces = [buf[off:off + klen]]
                    line_pieces = [buf[off + klen:n], remv]
                else:
                    key_pieces = [buf[off:n], remv[:klen - have]]
                    line_pieces = [remv[klen - have:]]
                off = n
            records.append((key_pieces, lineno, line_pieces))
        # materialise keys for heap comparison: one reservation per chunk
        total_klen = sum(sum(len(p) for p in r[0]) for r in records)
        self._key_res = arb.reserve(jid, total_klen, "merge-key")
        self._records = [(b"".join(r[0]), r[1], r[2]) for r in records]
        self._live = len(self._records)
        self._pos = 0

    def next(self):
        """Return (key_bytes, lineno, line_pieces) or None at EOF.  The caller
        must call release_record() once it is done with the record."""
        if self._pos >= len(self._records):
            self._fill()
            if not self._records:
                return None
        rec = self._records[self._pos]
        self._pos += 1
        return rec

    def release_record(self):
        self._live -= 1
        if self._live == 0 and self._pos >= len(self._records):
            self._records = []
            self._pos = 0
            for res in (self._chunk_res, self._key_res, self._rem_res):
                if res is not None:
                    self.job.arbiter.release(res)
            self._chunk_res = self._key_res = self._rem_res = None

    def close(self):
        # force-release any chunk still held (cancel/error path); on the
        # normal path the reservations are already gone (live == 0)
        self._records = []
        self._pos = 0
        self._live = 0
        for res in (self._chunk_res, self._key_res, self._rem_res):
            if res is not None:
                self.job.arbiter.release(res)
        self._chunk_res = self._key_res = self._rem_res = None
        self.job.arbiter.release(self._scratch_res)
        self.fp.close()


# ---------------------------------------------------------------------- the job
class SortJob:
    def __init__(self, runner, job_id: str, cfg: JobConfig):
        self.runner = runner
        self.job_id = job_id
        self.cfg = cfg
        self.arbiter: BufferArbiter = runner.arbiter
        self.events = runner.events

        # ---- derived sizes / minimal working set (see README) ----
        ml = cfg.max_line_bytes
        self.max_record = HDR.size + 5 * ml          # key <= 4*line bytes (UTF-8)
        self.read_block = max(cfg.read_block_bytes, 2 * (ml + 2))
        self.write_buf = max(cfg.write_buf_bytes, self.max_record)
        self.merge_chunk = max(cfg.merge_chunk_bytes, self.max_record)
        read_mws = self.read_block + 4 * self.read_block + 2 * self.write_buf
        per_reader = 2 * self.merge_chunk + self.max_record + HDR.size
        merge_mws = cfg.max_fan_in * per_reader + 2 * self.write_buf
        self.mws = max(read_mws, merge_mws)
        if self.mws > self.arbiter.budget:
            raise ConfigError(
                f"job {job_id}: minimal working set {self.mws}B exceeds budget "
                f"{self.arbiter.budget}B -- rejected at submit")

        # ---- state ----
        self.state = "PENDING"
        self.phase = "init"
        self.error: str | None = None
        self.lines_read = 0
        self.lines_written = 0
        self.runs: list[tuple[str, int]] = []       # (path, nrecords)
        self.spills = 0
        self.cancel_event = threading.Event()
        self.fail_event = threading.Event()
        self.done_event = threading.Event()
        self._state_lock = threading.Lock()
        self.run_lock = threading.Lock()            # guards _run/_run_res/_revocable
        self._run: list[tuple[bytes, int, bytes]] = []
        self._run_res: list[Reservation] = []
        self._revocable = 0
        self._spill_lock = threading.Lock()         # serialises serialisation
        self._spill_seq = 0
        self._max_run_records = 0

        self.tmpdir = os.path.join(runner.work_dir, f"job-{job_id}")
        os.makedirs(self.tmpdir, exist_ok=True)
        self.writer = runner.engine.new_writer(job_id)
        self.pool = BufferPool(self)
        runner.engine.buffer_pools[job_id] = self.pool
        if cfg.inject_io_failure:
            runner.engine.fail_first_write_for = job_id
        self.arbiter.register_job(job_id, self.mws, reclaim_handler=self._reclaim_handler)

    # ------------------------------------------------------------------ util
    def _hook(self, name: str, **kw):
        fn = self.cfg.hooks.get(name)
        if fn is not None:
            fn(self, **kw)

    def _check_cancel(self):
        if self.cancel_event.is_set():
            raise Cancelled()
        if self.fail_event.is_set():
            raise JobFailed(self.error or "I/O failure")

    def cancel(self):
        self.cancel_event.set()
        self.arbiter.cancel_job(self.job_id)
        self.writer.cancel()
        self.pool.notify_cancel()

    def _fail(self, err):
        with self._state_lock:
            if self.state in ("DONE", "FAILED", "CANCELLED"):
                return
            self.error = str(err)
        self.fail_event.set()
        self.arbiter.cancel_job(self.job_id)   # same wake-up machinery as cancel
        self.pool.notify_cancel()

    def _set_phase(self, phase: str):
        self.phase = phase
        self.events.emit("JOB_PHASE", self.job_id, phase=phase,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)

    # ------------------------------------------------------------------ main
    def run(self):
        try:
            self._read_phase()
            self._check_cancel()
            self._merge_phase()
            self._check_cancel()
            self._finish("DONE")
        except Cancelled:
            self._finish("CANCELLED")
        except BaseException as exc:
            self._finish("FAILED", exc)

    def _finish(self, state: str, exc: BaseException | None = None):
        if exc is not None and self.error is None:
            self.error = f"{type(exc).__name__}: {exc}"
        # 1. stop the writer; join guarantees in-flight reservations came back
        self.writer.cancel()
        self.writer.join()
        # 2. release buffer pool and anything still held (safety net)
        self.pool.release_all()
        self.arbiter.release_all(self.job_id)
        self.arbiter.end_phase(self.job_id)
        self.arbiter.unregister_job(self.job_id)
        self.runner.engine.buffer_pools.pop(self.job_id, None)
        if self.runner.engine.fail_first_write_for == self.job_id:
            self.runner.engine.fail_first_write_for = None
        # 3. delete only our own temp files
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
        if state != "DONE" and os.path.exists(out_tmp):
            try:
                os.unlink(out_tmp)
            except OSError:
                pass
        with self._state_lock:
            self.state = state
        self.events.emit("JOB_STATE", self.job_id, state=state, error=self.error,
                         lines_read=self.lines_read, lines_written=self.lines_written,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        self.done_event.set()

    # ------------------------------------------------------------------ read
    def _read_phase(self):
        self._set_phase("read")
        arb = self.arbiter
        jid = self.job_id
        cfg = self.cfg
        arb.begin_phase(jid, self.mws)
        extra: list[Reservation] = []
        try:
            # serialisation buffers (part of the envelope)
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)

            lineno = 0
            blocks = 0
            with open(cfg.input_path, "rb") as f:
                while True:
                    self._check_cancel()
                    block_res, lines = self._read_block(f, lineno)
                    if not lines:
                        arb.release(block_res)
                        break
                    key_res, keys = self._extract_keys(lines)
                    with self.run_lock:
                        base = lineno
                        for i, (k, ln) in enumerate(zip(keys, lines)):
                            self._run.append((k, base + i, ln))
                        self._run_res.append(block_res)
                        self._run_res.append(key_res)
                        self._revocable += sum(len(x) for x in lines) + sum(len(k) for k in keys)
                        self._max_run_records = max(self._max_run_records, len(self._run))
                        arb.set_revocable(jid, self._revocable)
                    lineno += len(lines)
                    blocks += 1
                    self._hook("block_read", blocks=blocks)
                    # optional beyond-envelope extra buffers, requested only
                    # after the job already holds run data (barrier test)
                    if cfg.extra_write_buffers and blocks == cfg.extra_after_blocks:
                        for _ in range(cfg.extra_write_buffers):
                            self._hook("before_extra_buffer")
                            res = arb.reserve(jid, cfg.extra_buffer_bytes, "buffer",
                                              timeout=cfg.extra_buffer_timeout)
                            extra.append(res)
                            self._hook("after_extra_buffer")
            self._spill()  # final partial run
            self.writer.wait_idle()
            self._check_cancel()
        finally:
            for res in extra:
                arb.release(res)
            self.pool.release_all()
            arb.end_phase(jid)

    def _read_block(self, f, lineno):
        """Reserve a block, read whole lines into it (bounded readline), adjust
        down to the actual size.  The reservation pre-exists the bytes; the
        loop stops early enough that the next bounded readline always fits."""
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        try:
            # fail-fast growth: one reclaim round, then fall back to self-spill
            res = arb.reserve(jid, self.read_block, "line", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()  # self-reclaim: free our own run, then retry within envelope
            res = arb.reserve(jid, self.read_block, "line", revocable=True)
        lines, total = [], 0
        limit = cfg.max_line_bytes
        while total + (limit + 2) <= self.read_block:
            raw = f.readline(limit + 2)
            if not raw:
                break
            if len(raw) > limit:
                arb.adjust(res, total)
                if not lines:
                    arb.release(res)
                raise LineTooLong(
                    f"line {lineno + len(lines)} exceeds max_line_bytes={limit}")
            lines.append(raw)
            total += len(raw)
        if not lines:
            return res, []
        arb.adjust(res, total)
        return res, lines

    def _extract_keys(self, lines):
        """Parse JSON, validate, encode keys.  The key reservation (4 bytes per
        char upper bound) is made *before* encoding, then adjusted to actual."""
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        keys_str = []
        for ln in lines:
            try:
                obj = json.loads(ln)
            except Exception as exc:
                raise InvalidLine(f"invalid JSON: {exc!r}")
            if not isinstance(obj, dict):
                raise InvalidLine("line is not a JSON object")
            key = obj.get("text")
            if not isinstance(key, str) or not key:
                raise InvalidLine("missing or empty 'text' key")
            keys_str.append(key)
        bound = 4 * sum(len(k) for k in keys_str)
        try:
            kres = arb.reserve(jid, bound, "key", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()
            kres = arb.reserve(jid, bound, "key", revocable=True)
        keys = []
        try:
            for k in keys_str:
                keys.append(k.encode("utf-8"))
        except UnicodeEncodeError as exc:
            arb.release(kres)
            raise InvalidLine(f"key not encodable as UTF-8: {exc!r}")
        arb.adjust(kres, sum(len(k) for k in keys))
        self.lines_read += len(lines)
        return kres, keys

    # ------------------------------------------------------------------ spill
    def _spill(self) -> int:
        """Sort the in-memory run and hand it to the writer.  Frees the run's
        line/key reservations as soon as serialisation is done (the write
        buffers stay reserved).  Returns reclaimed bytes.  Also used as the
        cooperative reclaim callback via _reclaim_handler."""
        with self._spill_lock:
            with self.run_lock:
                if not self._run:
                    return 0
                run, self._run = self._run, []
                run_res, self._run_res = self._run_res, []
                freed = self._revocable
                self._revocable = 0
                self.arbiter.set_revocable(self.job_id, 0)
            self._check_cancel()
            run.sort(key=lambda r: (r[0], r[1]))   # (key_bytes, lineno) == codepoint order
            seq = self._spill_seq
            self._spill_seq += 1
            tmp = os.path.join(self.tmpdir, f"run-{seq:06d}.tmp")
            final = os.path.join(self.tmpdir, f"run-{seq:06d}.run")
            self.events.emit("SPILL_START", self.job_id, records=len(run), bytes_to_free=freed,
                             total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
            idx = self.writer.open_file(
                tmp, final,
                on_closed=lambda **kw: self._on_run_closed(seq, len(run), **kw))
            buf, buf_res = self.pool.acquire()
            off = 0
            try:
                for key, ln, line in run:
                    rec = HDR.size + len(key) + len(line)
                    if off + rec > len(buf):
                        self.writer.write_buffer(idx, buf, buf_res, off)
                        buf_res = None
                        buf, buf_res = self.pool.acquire()
                        off = 0
                    HDR.pack_into(buf, off, len(key), len(line), ln)
                    off += HDR.size
                    buf[off:off + len(key)] = key
                    off += len(key)
                    buf[off:off + len(line)] = line
                    off += len(line)
                if off:
                    self.writer.write_buffer(idx, buf, buf_res, off)
                    buf_res = None
            finally:
                if buf_res is not None:
                    self.pool.buffer_returned(buf_res)
            self.writer.close_file(idx)
            # serialisation done: the run's data reservations can be freed now
            for res in run_res:
                self.arbiter.release(res)
            self.spills += 1
            self.events.emit("SPILL_SERIALIZED", self.job_id, records=len(run), freed=freed,
                             total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
            return freed

    def _reclaim_handler(self, nbytes: int) -> int:
        """Cooperative reclaim callback, invoked by the arbiter on another
        job's thread, WITHOUT the arbiter lock held.  Never calls reserve();
        only spills revocable run data.  Reentrancy-safe: serialised by the
        arbiter's reclaim lock and our own spill lock; cancellation mid-callback
        just makes it return early -- the memory it already freed stays freed."""
        self._hook("reclaim_enter")
        try:
            if self.cancel_event.is_set() or self.fail_event.is_set():
                return 0
            return self._spill()
        except (Cancelled, JobFailed):
            return 0
        finally:
            self._hook("reclaim_exit")

    def _on_run_closed(self, seq: int, nrecords: int, *, ok, error, tmp, final):
        with self._state_lock:
            alive = self.state not in ("DONE", "FAILED", "CANCELLED") \
                and not self.cancel_event.is_set() and not self.fail_event.is_set()
        if ok and alive:
            self.runs.append((final, nrecords))
            self.events.emit("SPILL_DONE", self.job_id, run=final, records=nrecords,
                             total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        elif not ok and not self.cancel_event.is_set():
            self._fail(error or f"spill run {seq} was discarded")

    # ------------------------------------------------------------------ merge
    def _merge_phase(self):
        self._set_phase("merge")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        arb.begin_phase(jid, self.mws)
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            try:
                runs = list(self.runs)
                if not runs:
                    self._write_empty_output()
                else:
                    fan_in = cfg.max_fan_in
                    pass_no = 0
                    while len(runs) > fan_in:
                        self._check_cancel()
                        self.events.emit("MERGE_PASS_START", self.job_id, pass_no=pass_no,
                                         runs=len(runs), fan_in=fan_in,
                                         total_reserved=arb.total_reserved, budget=arb.budget)
                        groups = [runs[i:i + fan_in] for i in range(0, len(runs), fan_in)]
                        new_runs = []
                        for g in groups:
                            if len(g) == 1:
                                new_runs.append(g[0])
                            else:
                                new_runs.append(self._merge_group(g, pass_no))
                        runs = new_runs
                        self.events.emit("MERGE_PASS_DONE", self.job_id, pass_no=pass_no,
                                         runs=len(runs),
                                         total_reserved=arb.total_reserved, budget=arb.budget)
                        pass_no += 1
                    self._merge_final(runs)
                self.writer.wait_idle()
                self._check_cancel()
            finally:
                self.pool.release_all()
        finally:
            arb.end_phase(jid)

    def _merge_group(self, group, pass_no):
        """Merge one group of runs into a new intermediate run file."""
        seq = self._spill_seq
        self._spill_seq += 1
        tmp = os.path.join(self.tmpdir, f"merge-{pass_no}-{seq:06d}.tmp")
        final = os.path.join(self.tmpdir, f"merge-{pass_no}-{seq:06d}.run")
        idx = self.writer.open_file(tmp, final, on_closed=self._on_merge_run_closed)
        n_out = self._kway(group, idx, dedup=False, to_run_format=True)
        self.writer.close_file(idx)
        self.writer.wait_idle()
        self._check_cancel()
        self.events.emit("MERGE_RUN_WRITTEN", self.job_id, run=final, records=n_out,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        return (final, n_out)

    def _on_merge_run_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or "intermediate merge run discarded")

    def _merge_final(self, runs):
        out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
        idx = self.writer.open_file(out_tmp, self.cfg.output_path,
                                    on_closed=self._on_output_closed)
        n_out = self._kway(runs, idx, dedup=self.cfg.dedup_key, to_run_format=False)
        self.writer.close_file(idx)
        self.lines_written = n_out

    def _write_empty_output(self):
        out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
        idx = self.writer.open_file(out_tmp, self.cfg.output_path,
                                    on_closed=self._on_output_closed)
        self.writer.close_file(idx)
        self.lines_written = 0

    def _kway(self, group, writer_idx, *, dedup: bool, to_run_format: bool) -> int:
        """k-way merge of ``group`` into writer file ``writer_idx``; returns
        records written.  Heap order (key_bytes, lineno, reader_idx) gives
        codepoint order with stable original-line-number tie-breaking."""
        readers = [RunReader(self, path) for path, _ in group]
        heap = []
        buf_res = None
        try:
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    key, lineno, pieces = rec
                    heapq.heappush(heap, (key, lineno, i, pieces))
            buf, buf_res = self.pool.acquire()
            off = 0
            n_out = 0
            last_key = None
            while heap:
                self._check_cancel()
                key, lineno, i, pieces = heapq.heappop(heap)
                r = readers[i]
                emit = True
                if dedup and last_key is not None and key == last_key:
                    emit = False  # keep only the earliest original line per key
                if emit:
                    if to_run_format:
                        line_len = sum(len(p) for p in pieces)
                        rec_len = HDR.size + len(key) + line_len
                        if off + rec_len > len(buf):
                            self.writer.write_buffer(writer_idx, buf, buf_res, off)
                            buf_res = None
                            buf, buf_res = self.pool.acquire()
                            off = 0
                        HDR.pack_into(buf, off, len(key), line_len, lineno)
                        off += HDR.size
                        buf[off:off + len(key)] = key
                        off += len(key)
                        for p in pieces:
                            buf[off:off + len(p)] = p
                            off += len(p)
                    else:
                        line_len = sum(len(p) for p in pieces)
                        if off + line_len > len(buf):
                            self.writer.write_buffer(writer_idx, buf, buf_res, off)
                            buf_res = None
                            buf, buf_res = self.pool.acquire()
                            off = 0
                        for p in pieces:
                            buf[off:off + len(p)] = p
                            off += len(p)
                    last_key = key
                    n_out += 1
                r.release_record()
                rec = r.next()
                if rec is not None:
                    k2, ln2, p2 = rec
                    heapq.heappush(heap, (k2, ln2, i, p2))
            if off:
                self.writer.write_buffer(writer_idx, buf, buf_res, off)
                buf_res = None
            else:
                self.pool.buffer_returned(buf_res)
                buf_res = None
            return n_out
        finally:
            if buf_res is not None:
                self.arbiter.release(buf_res)
            for r in readers:
                r.close()

    def _on_output_closed(self, *, ok, error, tmp, final):
        if ok and not self.cancel_event.is_set() and not self.fail_event.is_set():
            self.events.emit("OUTPUT_PUBLISH", self.job_id, output=final,
                             total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        elif not ok and not self.cancel_event.is_set():
            self._fail(error or "output discarded")

    # ------------------------------------------------------------------ report
    def metadata_estimate(self) -> int:
        """Fixed Python object overhead NOT covered by B (estimate)."""
        per_record = 8 + 56 + 49 + 49  # list slot + tuple + 2 bytes-object headers
        return self._max_run_records * per_record + len(self.runs) * 256

    def status(self) -> dict:
        with self._state_lock:
            return {
                "job_id": self.job_id,
                "state": self.state,
                "phase": self.phase,
                "error": self.error,
                "lines_read": self.lines_read,
                "lines_written": self.lines_written,
                "spills": self.spills,
                "runs": len(self.runs),
                "held_bytes": self.arbiter.held(self.job_id),
                "mws": self.mws,
                "input": self.cfg.input_path,
                "output": self.cfg.output_path,
            }
