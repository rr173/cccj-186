"""External repartition job: route JSONL records into fixed shards.

Given one unordered UTF-8 JSONL input whose every line is a JSON object, an
RFC 6901 JSON Pointer selecting a string field, and a partition count
``1 <= N <= 4096``, every record is routed to exactly one shard file::

    shard = uint64_be(sha256(key_utf8_bytes).digest()[:8]) mod N

No sorting, dedup or aggregation happens; records keep their input order
inside each shard and their original bytes (a single LF is appended only when
the physical last line of the input has no line ending).

Output dataset contract
-----------------------
Every execution writes a brand-new unique directory::

    <output_dir>/
      CURRENT                       # UTF-8 small file: the live generation id
      generations/<generation_id>/
        manifest.json
        part-00000.jsonl ... part-(N-1).jsonl   # empty shards are empty files

The manifest records the format version, key pointer, partition algorithm and
count, total record count, input SHA-256 and, per shard, the record count,
byte count and SHA-256.  Shard files and the manifest are fully closed before
``CURRENT`` is atomically replaced (tmp file + ``os.replace``); that single
replace is the commit boundary.  Old generations are never deleted, and any
failure/cancellation before the boundary only removes this job's own new
generation -- an existing CURRENT and its data are byte-for-byte untouched.

Bounded file descriptors / buffers
----------------------------------
At most ``max_open_shards`` (1..N) shard data files are open at once: a
bounded LRU shard cache flushes the least recently used shard's dirty buffer
to one shared, append-only spill file and closes its descriptor when a new
shard must be opened; the per-shard temporal ``(offset, length)`` segment list
is concatenated back into the part file in order after routing, so repeated
open/close never reorders records.  Shard write buffers are a bounded set
(recycled through an idle pool, never more than ``max_open_shards`` resident);
every resident byte (input blocks, encoded key bytes, shard write buffers,
synchronously in-flight write bytes, merge-read chunks) is reserved through
the job's single BufferArbiter id before the bytes are copied, and
cooperative reclaim only flushes/returns already-reserved buffers -- it never
calls back into ``reserve`` and runs under a route lock that never spans a
blocking reserve, so it cannot deadlock.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
import uuid
from collections import OrderedDict

from .arbiter import AllocFailed, Cancelled, ConfigError
from .job import BufferPool, JobFailed

FORMAT_VERSION = 1
PARTITION_ALGORITHM = "sha256-uint64-be-mod"
MAX_PARTITIONS = 4096


class RepartitionInputError(JobFailed):
    """A data-contract violation; carries the 1-based physical line number."""

    def __init__(self, lineno: int, reason: str):
        super().__init__(f"line {lineno}: {reason}")
        self.lineno = lineno
        self.reason = reason


def _reject_constant(token: str):
    raise RepartitionInputError(0, f"non-standard numeric constant {token!r}")


# ------------------------------------------------------------- JSON Pointer
def validate_pointer(pointer: str) -> str:
    if not isinstance(pointer, str) or not pointer:
        raise ConfigError("key_pointer must be a non-empty RFC 6901 pointer")
    if not pointer.startswith("/"):
        raise ConfigError(
            f"key_pointer {pointer!r} is not a valid RFC 6901 pointer "
            "(must start with '/')")
    for token in pointer[1:].split("/"):
        i = 0
        while i < len(token):
            ch = token[i]
            if ch == "~":
                if i + 1 >= len(token) or token[i + 1] not in "01":
                    raise ConfigError(
                        f"key_pointer {pointer!r} has an invalid escape in "
                        f"token {token!r} ('~' must be followed by '0' or '1')")
                i += 2
            else:
                i += 1
    return pointer


def _unescape_token(token: str) -> str:
    # validate_pointer() already guaranteed well-formed escapes
    return token.replace("~1", "/").replace("~0", "~")


def resolve_pointer(obj, pointer: str, lineno: int):
    """Resolve an RFC 6901 pointer against a parsed JSON value.

    Independent of any third-party pointer library; raises
    RepartitionInputError (with the physical line) on any missing property,
    invalid/out-of-range array index or type mismatch.
    """
    cur = obj
    tokens = pointer[1:].split("/")
    for raw_token in tokens:
        token = _unescape_token(raw_token)
        if isinstance(cur, dict):
            if token not in cur:
                raise RepartitionInputError(
                    lineno, f"JSON Pointer {pointer!r}: missing property {token!r}")
            cur = cur[token]
        elif isinstance(cur, list):
            if token == "-":
                raise RepartitionInputError(
                    lineno,
                    f"JSON Pointer {pointer!r}: '-' references a non-existent "
                    f"array element (length {len(cur)})")
            if not (token.isdigit() and (token == "0" or not token.startswith("0"))):
                raise RepartitionInputError(
                    lineno,
                    f"JSON Pointer {pointer!r}: invalid array index {token!r}")
            idx = int(token)
            if idx >= len(cur):
                raise RepartitionInputError(
                    lineno,
                    f"JSON Pointer {pointer!r}: array index {idx} out of range "
                    f"(length {len(cur)})")
            cur = cur[idx]
        else:
            raise RepartitionInputError(
                lineno,
                f"JSON Pointer {pointer!r}: cannot descend into {type(cur).__name__} "
                f"with token {token!r}")
    if not isinstance(cur, str):
        raise RepartitionInputError(
            lineno,
            f"JSON Pointer {pointer!r}: resolved value must be a string, got "
            f"{type(cur).__name__}")
    return cur


def route_partition(key_bytes: bytes, num_partitions: int) -> int:
    """The fixed, runtime-independent mapping."""
    digest = hashlib.sha256(key_bytes).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % num_partitions


# ---------------------------------------------------------------------- config
class RepartitionConfig:
    def __init__(self, input_path, output_dir, key_pointer, num_partitions, *,
                 max_line_bytes=256 * 1024,
                 max_open_shards=None,
                 read_block_bytes=256 * 1024,
                 write_buf_bytes=512 * 1024,
                 merge_chunk_bytes=256 * 1024,
                 inject_io_failure=False,
                 hooks=None):
        self.input_path = input_path
        self.output_dir = output_dir
        self.key_pointer = validate_pointer(key_pointer)
        if isinstance(num_partitions, bool) or not isinstance(num_partitions, int):
            raise ConfigError("num_partitions must be an int")
        if not 1 <= num_partitions <= MAX_PARTITIONS:
            raise ConfigError(
                f"num_partitions must be in [1, {MAX_PARTITIONS}], got {num_partitions}")
        self.num_partitions = num_partitions
        self.max_line_bytes = int(max_line_bytes)
        if self.max_line_bytes <= 0:
            raise ConfigError("max_line_bytes must be positive")
        if max_open_shards is None:
            max_open_shards = min(64, num_partitions)
        max_open_shards = int(max_open_shards)
        if not 1 <= max_open_shards <= num_partitions:
            raise ConfigError(
                f"max_open_shards must be in [1, {num_partitions}], "
                f"got {max_open_shards}")
        self.max_open_shards = max_open_shards
        self.read_block_bytes = int(read_block_bytes)
        self.write_buf_bytes = int(write_buf_bytes)
        self.merge_chunk_bytes = int(merge_chunk_bytes)
        self.inject_io_failure = bool(inject_io_failure)
        self.hooks = hooks or {}


# ------------------------------------------------------------- bounded shard cache
class _ShardEntry:
    __slots__ = ("fp", "buf", "res", "off", "path", "shard")

    def __init__(self, fp, buf, res, path, shard):
        self.fp = fp
        self.buf = buf
        self.res = res
        self.off = 0
        self.path = path
        self.shard = shard


class ShardCache:
    """Bounded LRU cache of open shard data files with ordered spill segments.

    Only the job worker thread (or a reclaim callback serialised by the job's
    route lock) touches this object, so no extra locking is needed.  At most
    ``max_open_shards`` shard data files are open at once: when a new shard
    must be opened the least recently used one flushes its dirty buffer to one
    shared, append-only spill file and closes -- no renames on the hot path and
    no per-shard permanent descriptor or unbounded buffer.  Each shard keeps
    the ordered list ``(offset, length)`` of its spill segments; part files are
    created only at finalize by concatenating each shard's temporal segments,
    so parts preserve the exact input order of the records each shard received
    even though shards were opened and closed many times.  Empty shards are
    zero-byte files.

    Memory discipline: at most ``max_open_shards`` shard write buffers exist,
    recycled on eviction through a bounded idle list; a flush reclassifies the
    same reservation buffer -> io -> buffer (synchronous write, counted once)
    and the merge phase reserves its read chunk before every read.
    """

    def __init__(self, job: "RepartitionJob", gen_dir: str, spill_dir: str):
        self.job = job
        self.gen_dir = gen_dir
        self.spill_dir = spill_dir
        self.n = job.cfg.num_partitions
        self.limit = job.cfg.max_open_shards
        self._open: "OrderedDict[int, _ShardEntry]" = OrderedDict()
        # shard index -> [records, bytes, hasher]
        self.stats = {i: [0, 0, hashlib.sha256()] for i in range(self.n)}
        # shard index -> ordered list of (offset, length) in the shared spill
        self.segments: dict[int, list] = {i: [] for i in range(self.n)}
        self.evictions = 0
        self.flushes = 0
        self.merges = 0
        self.open_high_water = 0
        self._writes_done = 0
        self.dirty_bytes = 0      # total unflushed shard buffer bytes resident
        self.spill_path = os.path.join(spill_dir, "spill.bin")
        self.spill_fp = open(self.spill_path, "w+b")
        self.spill_bytes = 0
        self._idle: list[tuple] = []   # recycled (buf, res), bounded by limit

    def part_path(self, idx: int) -> str:
        return os.path.join(self.gen_dir, f"part-{idx:05d}.jsonl")

    @property
    def open_count(self) -> int:
        return len(self._open)

    def _take_buffer_locked(self):
        """Pop an already-reserved idle buffer.  Must never reserve: this runs
        under the route lock, and a blocking reserve there would deadlock a
        cooperative reclaim callback (which needs the same lock).  The worker
        guarantees the idle pool is non-empty before routing each block."""
        if not self._idle:
            raise JobFailed("shard buffer pool exhausted inside route lock")
        return self._idle.pop()

    def refill_idle_buffers(self):
        """Reserve (blocking, outside the route lock) enough buffers so the
        idle pool plus the open-shard buffers total the open-shard cap.
        Reclaim may have returned buffers to the arbiter; this restores them
        before the next locked routing section, so ``_take_buffer_locked`` can
        stay non-blocking."""
        held = len(self._idle) + len(self._open)
        while held < self.limit:
            buf, res = self.job.acquire_write_buf()
            self._idle.append((buf, res))
            held += 1

    def _recycle_buffer(self, buf, res):
        """Park a buffer freed by an eviction into the bounded idle pool for
        reuse on the next shard open (hot-path recycling, no re-reserve)."""
        self._idle.append((buf, res))

    def release_idle_to_arbiter(self):
        """Release every recycled idle buffer reservation back to the arbiter.
        The cache (not the coordination-only BufferPool) owns these
        reservations, so the release is issued directly here.  This is what
        makes cooperative reclaim actually free budget."""
        arb = self.job.arbiter
        for _buf, res in self._idle:
            arb.release(res)
        self._idle.clear()

    def _write_bytes(self, fp, data, res, path_for_hook):
        self.job._hook("shard_write", path=path_for_hook, nbytes=len(data))
        if self.job.cfg.inject_io_failure and self._writes_done == 0:
            raise OSError("injected shard write failure")
        arb = self.job.arbiter
        arb.transfer(res, "io")
        try:
            fp.write(data)
        finally:
            arb.transfer(res, "buffer")
        self._writes_done += 1

    def _flush_to_spill(self, entry: _ShardEntry):
        """Drain an open shard's buffer to the shared, append-only spill file
        and record the temporal segment for this shard.  All shard bytes live
        in spill segments until finalize, so segment order is exactly the
        input order of the shard's records."""
        if entry.off == 0:
            return
        data = memoryview(entry.buf)[:entry.off]
        self._write_bytes(self.spill_fp, data, entry.res, self.spill_path)
        self.segments[entry.shard].append((self.spill_bytes, entry.off))
        self.spill_bytes += entry.off
        self.flushes += 1
        self.dirty_bytes -= entry.off
        entry.off = 0

    def _evict(self, idx: int, entry: _ShardEntry):
        """Flush the dirty buffer to the shared spill file and close the
        shard's (still empty) part file.  The flushed bytes become the next
        ordered spill segment of this shard."""
        self._flush_to_spill(entry)
        entry.fp.close()
        self._recycle_buffer(entry.buf, entry.res)
        self.evictions += 1
        self.job.events.emit("RP_SHARD_EVICT", self.job.job_id, shard=idx,
                             open_shards=len(self._open),
                             total_reserved=self.job.arbiter.total_reserved,
                             budget=self.job.arbiter.budget)

    def route(self, idx: int, data: bytes):
        entry = self._open.get(idx)
        if entry is None:
            while len(self._open) >= self.limit:
                ev_idx, ev_entry = self._open.popitem(last=False)
                self._evict(ev_idx, ev_entry)
            buf, res = self._take_buffer_locked()
            # the part file is only materialised at finalize; keep a closed
            # placeholder's fd slot by opening it here and writing nothing
            fp = open(self.part_path(idx), "wb")
            entry = _ShardEntry(fp, buf, res, self.part_path(idx), idx)
            self._open[idx] = entry
            self.open_high_water = max(self.open_high_water, len(self._open))
        else:
            self._open.move_to_end(idx)
        if entry.off + len(data) > len(entry.buf):
            self._flush_to_spill(entry)
        entry.buf[entry.off:entry.off + len(data)] = data
        entry.off += len(data)
        self.dirty_bytes += len(data)
        st = self.stats[idx]
        st[0] += 1
        st[1] += len(data)
        st[2].update(data)

    def release_all_buffers(self):
        """Flush/close every open shard into the spill file and return every
        shard buffer (open and idle) to the arbiter.  Used by cooperative
        reclaim / fail-fast self-spill: order is preserved via the segment
        lists and the reservations return to B.  The worker later calls
        refill_idle_buffers() (outside the route lock) before routing again."""
        while self._open:
            idx, entry = self._open.popitem(last=False)
            self._evict(idx, entry)
        # _evict parked the buffers in _idle; now actually free them
        self.release_idle_to_arbiter()

    def finalize(self, merge_chunk: int):
        """Build every final part file in shard order.

        Open shards first drain their remaining buffer into their last spill
        segment; every part is then written from that shard's temporal segment
        list.  Empty shards are zero-byte files.  At most one part output and
        one reserved merge-read chunk are live at a time.
        """
        # 1. drain the currently open shards; their bytes go to spill segments
        while self._open:
            idx, entry = self._open.popitem(last=False)
            self._flush_to_spill(entry)
            entry.fp.close()
            self._recycle_buffer(entry.buf, entry.res)
        # return the recycled idle buffers to the arbiter (cache owns them)
        for _buf, res in self._idle:
            self.job.arbiter.release(res)
        self._idle.clear()
        if self.spill_bytes:
            self.spill_fp.flush()
            if self.job.fsync_enabled:
                os.fsync(self.spill_fp.fileno())
        # 2. build each part from its temporal spill segments, in order
        arb, jid = self.job.arbiter, self.job.job_id
        for idx in range(self.n):
            final = self.part_path(idx)
            segs = self.segments[idx]
            if not segs:
                open(final, "wb").close()              # empty shard
                continue
            self.merges += 1
            with open(final, "wb") as out_fp:
                for offset, length in segs:
                    self.spill_fp.seek(offset)
                    remaining = length
                    while remaining:
                        nbytes = min(merge_chunk, remaining)
                        res = arb.reserve(jid, nbytes, "merge-read")
                        chunk = self.spill_fp.read(nbytes)
                        if len(chunk) != nbytes:
                            arb.release(res)
                            raise JobFailed(
                                f"truncated spill segment for shard {idx}")
                        try:
                            out_fp.write(chunk)
                        finally:
                            arb.release(res)
                        remaining -= len(chunk)
                out_fp.flush()
                if self.job.fsync_enabled:
                    os.fsync(out_fp.fileno())
        self.spill_fp.close()
        self.spill_fp = None
        # 3. defensive: every shard file must exist
        for idx in range(self.n):
            path = self.part_path(idx)
            if not os.path.exists(path):
                open(path, "wb").close()

    def cleanup_spill(self):
        """Failure/cancel path: close open files and delete spill scratch.

        Reservations are deliberately NOT released here: the job's single
        teardown (``BufferArbiter.release_all`` safety net) owns every release,
        so releasing here as well would double-free the same reservation. The
        idle/open buffers stay reserved until that net runs."""
        while self._open:
            _idx, entry = self._open.popitem(last=False)
            try:
                entry.fp.close()
            except OSError:
                pass
        if self.spill_fp is not None:
            try:
                self.spill_fp.close()
            except OSError:
                pass
            self.spill_fp = None
        shutil.rmtree(self.spill_dir, ignore_errors=True)

    def shard_records(self) -> int:
        return sum(s[0] for s in self.stats.values())

    def shard_bytes(self) -> int:
        return sum(s[1] for s in self.stats.values())


# ---------------------------------------------------------------------- the job
class RepartitionJob:
    job_type = "repartition"

    def __init__(self, runner, job_id: str, cfg: RepartitionConfig):
        self.runner = runner
        self.job_id = job_id
        self.cfg = cfg
        self.arbiter = runner.arbiter
        self.events = runner.events
        self.fsync_enabled = bool(getattr(runner.engine, "fsync", False))

        # ---- derived sizes / minimal working set ----
        ml = cfg.max_line_bytes
        self.read_block = max(cfg.read_block_bytes, 2 * (ml + 2))
        self.write_buf = max(cfg.write_buf_bytes, ml + 2)
        # the final ordered merge reads reserved chunks one at a time; keep
        # that chunk inside the same envelope (>= one full record is needed)
        self.merge_chunk = max(cfg.merge_chunk_bytes, ml + 2)
        # one input block + its key bytes (<= 4x block, UTF-8 bound) + one
        # write buffer per open-shard slot (open + recycled idle buffers never
        # exceed max_open_shards) + one merge read + slack
        self.mws = (5 * self.read_block
                    + cfg.max_open_shards * self.write_buf
                    + self.merge_chunk + 4096)
        if self.mws > self.arbiter.budget:
            raise ConfigError(
                f"job {job_id}: minimal working set {self.mws}B exceeds budget "
                f"{self.arbiter.budget}B -- rejected at submit")

        # ---- state ----
        self.state = "PENDING"
        self.phase = "init"
        self.error: str | None = None
        self.lines_read = 0
        self.records_routed = 0
        self.input_bytes = 0
        self.evictions = 0
        self.cancel_event = threading.Event()
        self.fail_event = threading.Event()
        self.done_event = threading.Event()
        self._state_lock = threading.Lock()
        # serialises shard-cache/pool mutation between the worker thread and
        # cooperative reclaim callbacks running on another job's thread
        # (lock order: _route_lock -> arbiter lock -> pool cond)
        self._route_lock = threading.Lock()
        self._pool: BufferPool | None = None
        self._cache: ShardCache | None = None
        self.generation_id: str | None = None
        self.gen_dir: str | None = None
        self.generations_dir: str | None = None
        self.current_path: str | None = None
        self.tmpdir: str | None = None
        self.spill_dir: str | None = None
        self._committed = False
        self._max_block_records = 0
        self._input_hasher = hashlib.sha256()

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
        if self._pool is not None:
            self._pool.notify_cancel()

    def _fail(self, err):
        with self._state_lock:
            if self.state in ("DONE", "FAILED", "CANCELLED"):
                return
            if self.error is None:
                self.error = str(err)
        self.fail_event.set()
        self.arbiter.cancel_job(self.job_id)
        if self._pool is not None:
            self._pool.notify_cancel()

    def _set_phase(self, phase: str):
        self.phase = phase
        self.events.emit("JOB_PHASE", self.job_id, phase=phase,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)

    # -------------------------------------------------------- write-buffer pool
    def acquire_write_buf(self):
        """Reserve one shard write buffer.  Always called OUTSIDE the route
        lock (cache construction / replenishment), so an unbounded wait inside
        the admitted MWS envelope is legal and deadlock-free.  The buffer is
        owned by the ShardCache from here on; the empty BufferPool only
        provides cancel wake-up coordination."""
        res = self.arbiter.reserve(self.job_id, self.write_buf, "buffer")
        return bytearray(self.write_buf), res

    # ------------------------------------------------------------------ main
    def run(self):
        try:
            self._prepare()
            self._read_and_route()
            self._check_cancel()
            self._write_manifest()
            self._check_cancel()
            self._publish_current()
            # commit boundary already crossed: a cancel landing here must still
            # report success, so there are no further cancellation checkpoints
            self._finish("DONE")
        except Cancelled:
            self._finish("CANCELLED")
        except BaseException as exc:
            self._finish("FAILED", exc)

    def _prepare(self):
        self._set_phase("read")
        arb, jid = self.arbiter, self.job_id
        arb.begin_phase(jid, self.mws)
        # unique generation id: UTC timestamp + random component, no PID/hash
        # randomisation of the process involved in the *routing* mapping
        self.generation_id = (
            time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            + "-" + uuid.uuid4().hex[:12])
        self.generations_dir = os.path.join(self.cfg.output_dir, "generations")
        self.gen_dir = os.path.join(self.generations_dir, self.generation_id)
        self.current_path = os.path.join(self.cfg.output_dir, "CURRENT")
        os.makedirs(self.gen_dir)
        # scratch spill segments live in the runner work dir, never inside the
        # published dataset; removed on every terminal state
        self.tmpdir = os.path.join(self.runner.work_dir, f"job-{jid}")
        self.spill_dir = os.path.join(self.tmpdir, "spill")
        os.makedirs(self.spill_dir)
        self._pool = BufferPool(self)
        self.runner.engine.buffer_pools[jid] = self._pool
        self._cache = ShardCache(self, self.gen_dir, self.spill_dir)
        # the cache is the single owner of every shard write buffer (bounded
        # idle list + open entries == max_open_shards); the pool is only used
        # for cancel wake-up, so no reservation is owned in two places
        self._cache.refill_idle_buffers()
        self.events.emit("RP_JOB_START", jid, generation_id=self.generation_id,
                         num_partitions=self.cfg.num_partitions,
                         key_pointer=self.cfg.key_pointer,
                         max_open_shards=self.cfg.max_open_shards,
                         total_reserved=arb.total_reserved, budget=arb.budget)

    def _read_and_route(self):
        self._set_phase("route")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        lineno = 0
        blocks = 0
        with open(cfg.input_path, "rb") as f:
            while True:
                self._check_cancel()
                # ---- blocking reserves happen WITHOUT the route lock, so a
                # cooperative reclaim callback (which needs the lock) can never
                # deadlock against this worker parked in reserve()
                block_res, lines = self._read_block(f, lineno)
                if not lines:
                    arb.release(block_res)
                    break
                key_strs = self._validate_block(lines, lineno)
                key_res, key_bytes_list = self._encode_keys(key_strs)
                # restore any shard buffers a prior reclaim returned; still
                # outside the route lock, so a blocking wait is deadlock-free
                self._cache.refill_idle_buffers()
                try:
                    # ---- only non-blocking shard-cache mutation is locked
                    with self._route_lock:
                        for raw, key_bytes in zip(lines, key_bytes_list):
                            self._route_one(raw, key_bytes)
                finally:
                    arb.release(key_res)
                    arb.release(block_res)
                lineno += len(lines)
                blocks += 1
                self._max_block_records = max(self._max_block_records, len(lines))
                self._hook("block_read", blocks=blocks)
        # all records routed: merge ordered spill segments into the part files
        # (also closes open shards and creates zero-byte empty shard files)
        self._set_phase("merge")
        self._cache.finalize(self.merge_chunk)
        if self._cache.shard_records() != self.lines_read:
            raise JobFailed(
                f"internal routing count mismatch: {self._cache.shard_records()} "
                f"sharded vs {self.lines_read} read")
        self.records_routed = self.lines_read
        self.evictions = self._cache.evictions
        self.events.emit("RP_ROUTE_DONE", jid, records=self.lines_read,
                         shard_bytes=self._cache.shard_bytes(),
                         evictions=self._cache.evictions,
                         flushes=self._cache.flushes,
                         merges=self._cache.merges,
                         open_high_water=self._cache.open_high_water,
                         total_reserved=arb.total_reserved, budget=arb.budget)

    def _read_block(self, f, lineno):
        """Reserve a block, read whole bounded lines into it, adjust down."""
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        try:
            res = arb.reserve(jid, self.read_block, "line", revocable=True, timeout=0)
        except AllocFailed:
            self._spill_buffers()  # self-reclaim, then retry inside envelope
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
                raise RepartitionInputError(
                    lineno + len(lines) + 1,
                    f"line exceeds max_line_bytes={limit}")
            lines.append(raw)
            total += len(raw)
            self._input_hasher.update(raw)
            self.input_bytes += len(raw)
        if not lines:
            return res, []
        arb.adjust(res, total)
        return res, lines

    def _validate_block(self, lines, base_lineno):
        """Parse/validate every line and resolve the pointer to a string."""
        key_strs = []
        for i, raw in enumerate(lines):
            lineno = base_lineno + i + 1
            if not raw.strip():
                raise RepartitionInputError(lineno, "empty line")
            try:
                obj = json.loads(raw, parse_constant=_reject_constant)
            except RepartitionInputError as exc:
                # parse_constant reports lineno 0; re-anchor to this line
                raise RepartitionInputError(lineno, exc.reason) from None
            except Exception as exc:
                raise RepartitionInputError(
                    lineno, f"invalid JSON: {exc!r}") from None
            if not isinstance(obj, dict):
                raise RepartitionInputError(lineno, "line is not a JSON object")
            key_strs.append(resolve_pointer(obj, self.cfg.key_pointer, lineno))
        self.lines_read += len(lines)
        return key_strs

    def _encode_keys(self, key_strs):
        """Reserve the UTF-8 bound (4 bytes/char) before encoding."""
        arb, jid = self.arbiter, self.job_id
        bound = 4 * sum(len(k) for k in key_strs)
        try:
            res = arb.reserve(jid, bound, "key", revocable=True, timeout=0)
        except AllocFailed:
            self._spill_buffers()
            res = arb.reserve(jid, bound, "key", revocable=True)
        keys = [k.encode("utf-8") for k in key_strs]
        arb.adjust(res, sum(len(k) for k in keys))
        return res, keys

    def _route_one(self, raw: bytes, key_bytes: bytes):
        idx = route_partition(key_bytes, self.cfg.num_partitions)
        # preserve original bytes; only add an LF to a final line without one
        data = raw if raw.endswith(b"\n") else raw + b"\n"
        self._cache.route(idx, data)
        arb = self.arbiter
        arb.set_revocable(self.job_id, self._dirty_bytes())

    def _dirty_bytes(self) -> int:
        if self._cache is None:
            return 0
        return self._cache.dirty_bytes

    def _spill_buffers(self):
        """Fail-fast self-reclaim: flush/close shards and release shard bufs.
        Runs outside the route lock (from a fail-fast reserve), so it takes
        the lock itself; only non-blocking cache/pool work happens inside."""
        if self._cache is not None:
            with self._route_lock:
                self._cache.release_all_buffers()
        self._pool.release_all()
        self.arbiter.set_revocable(self.job_id, 0)

    def _reclaim_handler(self, nbytes: int) -> int:
        """Cooperative reclaim, invoked on another job's thread WITHOUT the
        arbiter lock.  Never calls reserve(); only flushes revocable shard
        buffers and returns their reservations.  Re-acquire is lazy later.
        Serialised against the worker's routing section by _route_lock."""
        self._hook("reclaim_enter")
        try:
            if self.cancel_event.is_set() or self.fail_event.is_set():
                return 0
            if self.phase != "route" or self._cache is None:
                return 0
            with self._route_lock:
                if self.phase != "route" or self._cache is None:
                    return 0
                before = self.arbiter.held(self.job_id)
                self._cache.release_all_buffers()
                self._pool.release_all()
                self.arbiter.set_revocable(self.job_id, 0)
                return before - self.arbiter.held(self.job_id)
        except (Cancelled, JobFailed):
            return 0
        finally:
            self._hook("reclaim_exit")

    # ---------------------------------------------------------------- manifest
    def _write_manifest(self):
        self._set_phase("manifest")
        self._hook("before_manifest")
        shards = []
        for idx in range(self.cfg.num_partitions):
            records, nbytes, hasher = self._cache.stats[idx]
            shards.append({
                "part": idx,
                "file": f"part-{idx:05d}.jsonl",
                "records": records,
                "bytes": nbytes,
                "sha256": hasher.hexdigest(),
            })
        manifest = {
            "format_version": FORMAT_VERSION,
            "generation_id": self.generation_id,
            "key_pointer": self.cfg.key_pointer,
            "partition_algorithm": {
                "name": PARTITION_ALGORITHM,
                "hash": "sha256",
                "prefix_bytes": 8,
                "integer_encoding": "unsigned-big-endian",
                "operation": "modulo",
                "modulo": self.cfg.num_partitions,
            },
            "num_partitions": self.cfg.num_partitions,
            "max_open_shards": self.cfg.max_open_shards,
            "total_records": self.lines_read,
            "input_sha256": self._input_hasher.hexdigest(),
            "shards": shards,
        }
        payload = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
                   + "\n").encode("utf-8")
        final = os.path.join(self.gen_dir, "manifest.json")
        tmp = final + f".tmp.{self.job_id}"
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            if self.fsync_enabled:
                os.fsync(f.fileno())
        os.replace(tmp, final)
        self._fsync_dir(self.gen_dir)
        self.events.emit("RP_MANIFEST_DONE", self.job_id, path=final,
                         bytes=len(payload),
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)

    # ----------------------------------------------------------------- publish
    def _publish_current(self):
        self._set_phase("publish")
        payload = (self.generation_id + "\n").encode("utf-8")
        tmp = self.current_path + f".tmp.{self.job_id}"
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            if self.fsync_enabled:
                os.fsync(f.fileno())
        # deterministic gate for the cancel/commit-boundary tests
        self._hook("before_publish", tmp=tmp, current=self.current_path)
        self._check_cancel()
        os.replace(tmp, self.current_path)   # the single atomic commit point
        self._committed = True
        self._fsync_dir(self.cfg.output_dir)
        self.events.emit("RP_CURRENT_PUBLISH", self.job_id,
                         current=self.current_path, generation_id=self.generation_id,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)
        # deterministic gate for the post-commit-boundary cancel test
        self._hook("after_publish", tmp=tmp, current=self.current_path)

    @staticmethod
    def _fsync_dir(path: str):
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    # ---------------------------------------------------------------- teardown
    def _finish(self, state: str, exc: BaseException | None = None):
        if exc is not None and self.error is None:
            self.error = f"{type(exc).__name__}: {exc}"
        committed = self._committed
        # 1. return any buffers still held (safety net) and all reservations
        if self._pool is not None:
            self._pool.release_all()
        self.arbiter.release_all(self.job_id)
        self.arbiter.end_phase(self.job_id)
        self.arbiter.unregister_job(self.job_id)
        self.runner.engine.buffer_pools.pop(self.job_id, None)
        # 2. before the commit boundary a failure/cancel removes only this
        #    job's brand-new generation; a published dataset is never touched
        if not committed and self.gen_dir is not None:
            if self._cache is not None:
                self._cache.cleanup_spill()
            shutil.rmtree(self.gen_dir, ignore_errors=True)
            # prune an empty generations/ dir but never anything pre-existing
            try:
                if self.generations_dir is not None and not os.listdir(self.generations_dir):
                    os.rmdir(self.generations_dir)
            except OSError:
                pass
            if self.current_path is not None:
                tmp = self.current_path + f".tmp.{self.job_id}"
                if os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
            self.events.emit("RP_CLEANUP", self.job_id, generation_id=self.generation_id,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
        # spill scratch is this job's private temp state on every outcome
        if self.tmpdir is not None:
            shutil.rmtree(self.tmpdir, ignore_errors=True)
        with self._state_lock:
            self.state = state
        self.events.emit("JOB_STATE", self.job_id, state=state, error=self.error,
                         lines_read=self.lines_read, records_routed=self.records_routed,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)
        self.done_event.set()

    # ---------------------------------------------------------------- report
    def metadata_estimate(self) -> int:
        """Fixed Python object overhead NOT covered by B (estimate)."""
        per_record = 8 + 56 + 49          # list slot + dict header + str header
        return self._max_block_records * per_record + self.cfg.num_partitions * 64

    def status(self) -> dict:
        with self._state_lock:
            cache = self._cache
            return {
                "job_id": self.job_id,
                "job_type": self.job_type,
                "state": self.state,
                "phase": self.phase,
                "error": self.error,
                "key_pointer": self.cfg.key_pointer,
                "num_partitions": self.cfg.num_partitions,
                "max_open_shards": self.cfg.max_open_shards,
                "open_shards": cache.open_count if cache is not None else 0,
                "open_shards_high_water": cache.open_high_water if cache else 0,
                "evictions": cache.evictions if cache else self.evictions,
                "flushes": cache.flushes if cache else 0,
                "segment_merges": cache.merges if cache else 0,
                "lines_read": self.lines_read,
                "records_routed": self.records_routed,
                "input_bytes": self.input_bytes,
                "shard_bytes_written": cache.shard_bytes() if cache else 0,
                "held_bytes": self.arbiter.held(self.job_id),
                "mws": self.mws,
                "generation_id": self.generation_id,
                "committed": self._committed,
                "input": self.cfg.input_path,
                "output_dir": self.cfg.output_dir,
                "current": self.current_path,
            }
