"""External full-out interval join job.

Given two unordered JSONL files -- a *left* side of business records valid on
half-open intervals ``[start, end)`` and a *right* side of observations taken
at a point ``at`` -- produce the full outer join

    text(left) == text(right)  AND  start <= at < end

as JSONL detail records, without asking the caller to sort anything first.

The job reuses the external-sort kernel (same :class:`BufferArbiter`,
:class:`IOEngine`, :class:`BufferPool`, same byte-budget accounting) and runs
in four phases, each admitted under its minimal-working-set envelope:

  ORGANIZE     validate + externally sort each side.  Left runs/side files are
               ordered by (key, start, lineno), right by (key, at, lineno), so
               every per-key group already arrives in sweep order.
  JOIN         merge-join the two side files by key; inside a key, sweep the
               observations in ``at`` order while maintaining the active
               interval set (start <= at).  The active set is a min-heap by
               ``end`` backed, when it outgrows the envelope, by end-sorted
               *active runs* on disk; matched rows are marked in paged bitmap
               files; every emitted record (match / left_only / right_only)
               goes to join-result runs on disk.
  OUTPUT_SORT  k-way merge the join-result runs into the contract order
               (key codepoints, then left-bearing records by (left_row,
               right_row), then right_only by right_row) and serialise JSONL.
  PUBLISH      tmp -> os.replace, atomic; a failed/cancelled job never
               publishes and only ever deletes its own temp files.

Hot keys never require the whole group, active set or match product in
memory: side groups are streamed from disk, active runs and marker pages are
bounded cache structures, and the match product is spilt as it is produced.

Candidate checks (a real record of one side meeting a real time predicate of
the other) are counted in exactly two methods, ``_check_start`` and
``_evaluate`` -- no comparison hides in an uncounted helper.  For N disjoint
intervals with one observation each the sweep performs O(N) checks.

Disk organisation, per key:
  * left/right side files -- global (key, time, lineno) sorted binary runs,
    support the range hit (sweep cursor, end-ordered active runs), keep every
    source line for traceability and make the orphan re-stream a plain seek;
  * marker-<key>.bit -- paged bitmap over left-row ordinals: the "already
    matched" mark, resident cache bounded by marker_cache_pages;
  * active-<key>-*.run -- end-sorted spills of the still-active intervals,
    each with a persistent expiry mark so suffix scans never redo checks;
  * result-*.run -- join-result records carrying both source lines, k-way
    merged again for the final source-row order.
"""
from __future__ import annotations

import heapq
import json
import os
import shutil
import struct
import threading
from collections import OrderedDict

from .arbiter import AllocFailed, BufferArbiter, Cancelled, ConfigError, Reservation
from .job import BufferPool, JobFailed

# --------------------------------------------------------------------- formats
# All headers are big-endian, fixed size, lengths in bytes.
# left  run/side record: klen, llen, lineno, start, end | key | line
LHDR = struct.Struct(">IIQqq")
# right run/side record: klen, llen, lineno, at        | key | line
RHDR = struct.Struct(">IIQq")
# active-run record:    llen, lineno, ordinal, start, end | line
AHDR = struct.Struct(">IQQqq")
# join-result record:   klen, kind, left_row, right_row, llen_l, llen_r
#                       | key | left line | right line
JHDR = struct.Struct(">IBQQII")

KIND_MATCH = 0
KIND_LEFT_ONLY = 1
KIND_RIGHT_ONLY = 2
KIND_NAME = {KIND_MATCH: "match", KIND_LEFT_ONLY: "left_only",
             KIND_RIGHT_ONLY: "right_only"}
# output sort flag: every left-bearing record precedes right_only
_FLAG = {KIND_MATCH: 0, KIND_LEFT_ONLY: 0, KIND_RIGHT_ONLY: 1}

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


class IntervalInputError(JobFailed):
    """A data-contract violation; names the side and physical line number."""

    def __init__(self, side: str, lineno: int, reason: str):
        super().__init__(f"{side} line {lineno}: {reason}")
        self.side = side
        self.lineno = lineno
        self.reason = reason


# --------------------------------------------------------------------- config
class IntervalConfig:
    def __init__(self, left_path, right_path, output_path, *,
                 max_line_bytes=256 * 1024,
                 read_block_bytes=256 * 1024,
                 write_buf_bytes=512 * 1024,
                 merge_chunk_bytes=256 * 1024,
                 max_fan_in=8,
                 marker_page_bytes=8 * 1024,
                 marker_cache_pages=4,
                 active_flush_bytes=256 * 1024,
                 max_active_runs=4,
                 result_run_bytes=4 * 1024 * 1024,
                 inject_io_failure=False,
                 hooks=None):
        self.left_path = left_path
        self.right_path = right_path
        self.output_path = output_path
        self.max_line_bytes = int(max_line_bytes)
        self.read_block_bytes = int(read_block_bytes)
        self.write_buf_bytes = int(write_buf_bytes)
        self.merge_chunk_bytes = int(merge_chunk_bytes)
        self.max_fan_in = max(2, int(max_fan_in))
        self.marker_page_bytes = max(64, int(marker_page_bytes))
        self.marker_cache_pages = max(1, int(marker_cache_pages))
        self.active_flush_bytes = max(64, int(active_flush_bytes))
        self.max_active_runs = max(2, int(max_active_runs))
        self.result_run_bytes = max(write_buf_bytes, int(result_run_bytes))
        self.inject_io_failure = bool(inject_io_failure)
        self.hooks = hooks or {}


# --------------------------------------------------------------------- reader
class _Chunk:
    """One reserved read chunk plus the records parsed out of it.

    The chunk reservation, the per-chunk key reservation and any straddle
    remainder reservation live exactly as long as the chunk's records: they
    are released when the chunk's last record is released (or on close).
    Memoryviews handed out therefore never dangle while a record is live.
    """

    __slots__ = ("chunk_res", "key_res", "rem_res", "records", "live", "base_pos")

    def __init__(self, chunk_res, key_res, rem_res, records, base_pos):
        self.chunk_res = chunk_res
        self.key_res = key_res
        self.rem_res = rem_res
        # records: list of (fields, body_pieces, key, offset_in_chunk)
        self.records = records
        self.live = len(records)
        self.base_pos = base_pos


class BinReader:
    """Buffered reader for the fixed-header record files above.

    Records are parsed out of reserved chunks; a record straddling a chunk
    boundary is completed with an exact-size remainder read (reserved before
    the read); a header straddle uses a reserved scratch buffer.  When the
    layout carries a key body (``key_index`` is not None), chunk keys are
    materialised once per chunk under one ``merge-key`` reservation.

    A record is returned as ``(fields, body_pieces, key)`` where
    ``body_pieces`` is a list with one list of memoryviews per body field (in
    header order).  Records may outlive the read that produced them (the k-way
    heaps keep several records at once): each record holds its :class:`_Chunk`
    alive, and the chunk's reservations are released only after every record of
    that chunk has been released.  At most two chunks are resident per reader
    (the chunk being filled and the chunk whose records are still live).
    """

    def __init__(self, job: "IntervalJoinJob", path: str, hdr: struct.Struct,
                 body_idx: tuple[int, ...], *, key_index: int | None = 0,
                 start_offset: int = 0):
        self.job = job
        self.path = path
        self.hdr = hdr
        self.body_idx = body_idx
        self.n_body = len(body_idx)
        self.key_index = key_index
        self.fp = open(path, "rb")
        # dedicated handle for the block-sort cursor (read_chunk_records), so
        # its position never collides with the sequential next() cursor
        self._bfp = open(path, "rb")
        self.start_offset = start_offset
        if start_offset:
            self.fp.seek(start_offset)
        self._bfp.seek(start_offset)
        self._consumed = 0
        self._last_record_pos = start_offset
        self._eof = False
        self._closed = False
        # independent block-read cursor for read_chunk_records()
        self._block_pos = start_offset
        self._block_eof = False
        # chunks still holding live records, in read order
        self._chunks: list[_Chunk] = []
        # pending records not yet returned, as (chunk, record_index)
        self._pending: list[tuple[_Chunk, int]] = []
        self._cur: _Chunk | None = None
        self._cur_pos = 0
        arb = job.arbiter
        self._scratch = bytearray(hdr.size)
        self._scratch_res = arb.reserve(job.job_id, hdr.size, "merge-read")

    # ------------------------------------------------------------- positions
    def last_record_pos(self) -> int:
        """Absolute file offset of the record most recently returned by next()."""
        return self._last_record_pos

    def seek(self, absolute_pos: int):
        self._close_chunks()
        self.fp.seek(absolute_pos)
        self._bfp.seek(absolute_pos)
        self.start_offset = absolute_pos
        self._consumed = 0
        self._last_record_pos = absolute_pos
        self._eof = False
        self._block_pos = absolute_pos
        self._block_eof = False
        self._pending = []
        self._cur = None
        self._cur_pos = 0

    def _close_chunks(self):
        for c in self._chunks:
            self._release_chunk_res(c)
        self._chunks = []
        self._pending = []
        self._cur = None
        self._cur_pos = 0

    def _release_chunk_res(self, c: _Chunk):
        arb = self.job.arbiter
        for res in (c.chunk_res, c.key_res, c.rem_res):
            if res is not None:
                arb.release(res)
        c.chunk_res = c.key_res = c.rem_res = None

    # ----------------------------------------------------------------- fill
    def _fill(self):
        if self._eof:
            return
        # the previous chunk may still hold live records in a heap; it stays
        # resident.  A reader therefore holds at most two chunks at a time.
        arb, jid = self.job.arbiter, self.job.job_id
        chunk_size = self.job.merge_chunk
        base_pos = self.start_offset + self._consumed
        res = arb.reserve(jid, chunk_size, "merge-read")
        chunk = self.fp.read(chunk_size)
        self.job.bytes_read += len(chunk)
        if len(chunk) < chunk_size:
            arb.adjust(res, len(chunk))
        if not chunk:
            arb.release(res)
            self._eof = True
            self._cur = None
            return
        chunk_res = res
        rem_res = None
        buf = memoryview(chunk)
        n = len(chunk)
        off = 0
        records = []
        while off < n:
            rec_offset = off
            if n - off < self.hdr.size:
                tail = buf[off:]
                self._scratch[:len(tail)] = tail
                got = self.fp.readinto(memoryview(self._scratch)[len(tail):])
                self.job.bytes_read += self.hdr.size - len(tail)
                if got != self.hdr.size - len(tail):
                    raise JobFailed(f"truncated record file {self.path}")
                fields = self.hdr.unpack(self._scratch)
                off = n
            else:
                fields = self.hdr.unpack_from(buf, off)
                off += self.hdr.size
            lens = [fields[i] for i in self.body_idx]
            body_pieces: list[list] = [[] for _ in range(self.n_body)]
            field_no = 0
            while field_no < self.n_body:
                length = lens[field_no]
                if length == 0:
                    field_no += 1
                    continue
                if n - off >= length:
                    body_pieces[field_no].append(buf[off:off + length])
                    off += length
                    field_no += 1
                else:
                    # straddle: the current field starts in this chunk and may
                    # finish (along with later fields) in a remainder read.
                    # Read exactly the bytes still missing for this record.
                    have = n - off
                    rest_lens = lens[field_no:]
                    need = sum(rest_lens) - have
                    rem_res = arb.reserve(jid, need, "merge-read")
                    rem = self.fp.read(need)
                    self.job.bytes_read += need
                    if len(rem) != need:
                        raise JobFailed(f"truncated record file {self.path}")
                    remv = memoryview(rem)
                    rem_cursor = 0
                    # current field: the part already in the chunk, then the
                    # rest out of the remainder buffer
                    if have:
                        body_pieces[field_no].append(buf[off:n])
                        take = lens[field_no] - have
                        body_pieces[field_no].append(remv[rem_cursor:rem_cursor + take])
                        rem_cursor += take
                    else:
                        take = lens[field_no]
                        body_pieces[field_no].append(remv[rem_cursor:rem_cursor + take])
                        rem_cursor += take
                    field_no += 1
                    # remaining whole fields live entirely in the remainder
                    for length2 in rest_lens[1:]:
                        body_pieces[field_no].append(
                            remv[rem_cursor:rem_cursor + length2])
                        rem_cursor += length2
                        field_no += 1
                    off = n
            records.append((fields, body_pieces, rec_offset))
        key_res = None
        keys = [None] * len(records)
        if self.key_index is not None:
            total_klen = sum(f[self.key_index] for f, _, _ in records)
            key_res = arb.reserve(jid, total_klen, "merge-key")
            keys = [b"".join(rec[1][0]) for rec in records]
        full = [(f, p, k, roff) for (f, p, roff), k in zip(records, keys)]
        c = _Chunk(chunk_res, key_res, rem_res, full, base_pos)
        self._chunks.append(c)
        self._cur = c
        self._cur_pos = 0
        self._pending = [(c, i) for i in range(len(full))]

    def next(self):
        # Fetch a new chunk when no record is pending.  Older chunks may still
        # be live (their records are checked out in a heap); they stay resident
        # until every record is released, so cross-chunk heaps stay valid.
        if not self._pending and not self._eof:
            self._fill()
        if not self._pending:
            return None
        c, idx = self._pending.pop(0)
        self._cur = c
        self._cur_pos = idx
        fields, pieces, _key, rec_off = c.records[idx]
        rec_len = self.hdr.size + sum(len(p) for ps in pieces for p in ps)
        self._consumed += rec_len
        self._last_record_pos = c.base_pos + rec_off
        # return fields, body_pieces, key, owning chunk
        return fields, pieces, _key, c

    def read_chunk_records(self):
        """Read exactly one fresh chunk and return all of its records (each
        carrying its owning chunk).  Returns None at EOF.  Independent of the
        ``next()``/``_pending`` sequential cursor: it parses a whole chunk
        straight from the file at the block-read position.  The chunk stays
        reserved until every returned record is released; block-sort callers
        therefore keep at most one chunk live, bounded by merge_chunk bytes.
        """
        if self._block_eof:
            return None
        arb, jid = self.job.arbiter, self.job.job_id
        chunk_size = self.job.merge_chunk
        base_pos = self._block_pos
        res = arb.reserve(jid, chunk_size, "merge-read")
        chunk = self._bfp.read(chunk_size)
        self.job.bytes_read += len(chunk)
        if len(chunk) < chunk_size:
            arb.adjust(res, len(chunk))
        if not chunk:
            arb.release(res)
            self._block_eof = True
            return None
        chunk_res = res
        rem_res = None
        buf = memoryview(chunk)
        n = len(chunk)
        off = 0
        parsed = []
        while off < n:
            rec_offset = off
            if n - off < self.hdr.size:
                tail = buf[off:]
                self._scratch[:len(tail)] = tail
                got = self._bfp.readinto(memoryview(self._scratch)[len(tail):])
                self.job.bytes_read += self.hdr.size - len(tail)
                if got != self.hdr.size - len(tail):
                    raise JobFailed(f"truncated record file {self.path}")
                fields = self.hdr.unpack(self._scratch)
                off = n
            else:
                fields = self.hdr.unpack_from(buf, off)
                off += self.hdr.size
            lens = [fields[i] for i in self.body_idx]
            body_pieces: list[list] = [[] for _ in range(self.n_body)]
            field_no = 0
            while field_no < self.n_body:
                length = lens[field_no]
                if length == 0:
                    field_no += 1
                    continue
                if n - off >= length:
                    body_pieces[field_no].append(buf[off:off + length])
                    off += length
                    field_no += 1
                else:
                    have = n - off
                    rest_lens = lens[field_no:]
                    need = sum(rest_lens) - have
                    rem_res = arb.reserve(jid, need, "merge-read")
                    rem = self._bfp.read(need)
                    self.job.bytes_read += need
                    if len(rem) != need:
                        raise JobFailed(f"truncated record file {self.path}")
                    remv = memoryview(rem)
                    rem_cursor = 0
                    if have:
                        body_pieces[field_no].append(buf[off:n])
                        take = lens[field_no] - have
                        body_pieces[field_no].append(remv[rem_cursor:rem_cursor + take])
                        rem_cursor += take
                    else:
                        take = lens[field_no]
                        body_pieces[field_no].append(remv[rem_cursor:rem_cursor + take])
                        rem_cursor += take
                    field_no += 1
                    for length2 in rest_lens[1:]:
                        body_pieces[field_no].append(
                            remv[rem_cursor:rem_cursor + length2])
                        rem_cursor += length2
                        field_no += 1
                    off = n
            parsed.append((fields, body_pieces, rec_offset))
        keys = [None] * len(parsed)
        key_res = None
        if self.key_index is not None:
            total_klen = sum(f[self.key_index] for f, _, _ in parsed)
            key_res = arb.reserve(jid, total_klen, "merge-key")
            keys = [b"".join(p[0]) for f, p, _o in parsed]
        records = [(f, p, k, roff) for (f, p, roff), k in zip(parsed, keys)]
        c = _Chunk(chunk_res, key_res, rem_res, records, base_pos)
        self._chunks.append(c)
        self._block_pos = self._bfp.tell()
        return [rec + (c,) for rec in records]

    def release_record(self, chunk: "_Chunk | None" = None):
        """Release one live record.  When the chunk holding it reaches zero
        live records, that chunk's reservations are returned exactly once."""
        if chunk is None:
            return
        chunk.live -= 1
        if chunk.live <= 0:
            try:
                self._chunks.remove(chunk)
            except ValueError:
                return
            self._release_chunk_res(chunk)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._close_chunks()
        self.job.arbiter.release(self._scratch_res)
        for handle in (self.fp, self._bfp):
            try:
                handle.close()
            except OSError:
                pass


# --------------------------------------------------------------------- marker
class MarkerBitmap:
    """Per-key matched-row bitmap with a bounded paged cache.

    Indexed by the left row's ordinal within the key (0-based, side-file
    order).  Pages are reserved bytearrays (kind "marker"); dirty evictions
    are written back to the marker file.  At most ``cache_pages`` pages are
    ever resident, so marking an arbitrarily large key stays inside the
    envelope.  The orphan pass streams the file sequentially with one page.
    """

    def __init__(self, job: "IntervalJoinJob", path: str, page_size: int,
                 cache_pages: int):
        self.job = job
        self.path = path
        self.page_size = page_size
        self.cache_pages = cache_pages
        self.fp = open(path, "w+b")
        self._cache: "OrderedDict[int, tuple[bytearray, Reservation, bool]]" = OrderedDict()
        self._file_size = 0

    def _read_page_into(self, buf, idx: int):
        self.fp.seek(idx * self.page_size)
        got = self.fp.readinto(buf)
        self.job.bytes_read += got
        if got < self.page_size:
            buf[got:] = b"\x00" * (self.page_size - got)

    def _page(self, idx: int):
        found = self._cache.get(idx)
        if found is not None:
            self._cache.move_to_end(idx)
            return found
        if len(self._cache) >= self.cache_pages:
            old_idx, (buf, res, dirty) = self._cache.popitem(last=False)
            if dirty:
                self.fp.seek(old_idx * self.page_size)
                self.fp.write(bytes(buf))
                self.job.marker_bytes_written += self.page_size
                self.job.bytes_written += self.page_size
            if idx * self.page_size < self._file_size:
                self._read_page_into(buf, idx)
            else:
                buf[:] = b"\x00" * self.page_size
            self._cache[idx] = (buf, res, False)
            return self._cache[idx]
        res = self.job.arbiter.reserve(self.job.job_id, self.page_size, "marker")
        buf = bytearray(self.page_size)
        if idx * self.page_size < self._file_size:
            self._read_page_into(buf, idx)
        self._cache[idx] = (buf, res, False)
        self._file_size = max(self._file_size, (idx + 1) * self.page_size)
        return self._cache[idx]

    def set_bit(self, ordinal: int):
        idx, bit = divmod(ordinal, self.page_size * 8)
        buf, res, _dirty = self._page(idx)
        buf[bit >> 3] |= 1 << (bit & 7)
        self._cache[idx] = (buf, res, True)

    def flush(self):
        for idx, (buf, res, dirty) in list(self._cache.items()):
            if dirty:
                self.fp.seek(idx * self.page_size)
                self.fp.write(bytes(buf))
                self.job.marker_bytes_written += self.page_size
                self.job.bytes_written += self.page_size
                self._cache[idx] = (buf, res, False)
        self.fp.flush()

    def stream_orphans(self, n_left: int):
        """Yield ordinals of rows whose bit is still zero (sequential scan)."""
        self.flush()
        self.close_cache()
        self.fp.seek(0)
        arb, jid = self.job.arbiter, self.job.job_id
        res = arb.reserve(jid, self.page_size, "marker")
        page = bytearray(self.page_size)
        try:
            ordinal = 0
            while ordinal < n_left:
                got = self.fp.readinto(page)
                self.job.bytes_read += max(got, 0)
                if got <= 0:
                    page[:] = b"\x00" * self.page_size
                limit = min(self.page_size * 8, n_left - ordinal)
                for bit in range(limit):
                    if not (page[bit >> 3] & (1 << (bit & 7))):
                        yield ordinal + bit
                ordinal += self.page_size * 8
                if got < self.page_size:
                    page[:] = b"\x00" * self.page_size
        finally:
            arb.release(res)

    def close_cache(self):
        items = list(self._cache.values())
        self._cache.clear()
        for _buf, res, _dirty in items:
            self.job.arbiter.release(res)

    def close(self):
        self.close_cache()
        try:
            self.fp.close()
        except OSError:
            pass


# ----------------------------------------------------------------- small types
class _IntervalView:
    __slots__ = ("start", "end")

    def __init__(self, start, end):
        self.start = start
        self.end = end


class _HeapRec:
    __slots__ = ("start", "end")

    def __init__(self, entry):
        self.end = entry[0]
        self.start = entry[3]


# --------------------------------------------------------------------- the job
class IntervalJoinJob:
    def __init__(self, runner, job_id: str, cfg: IntervalConfig):
        self.runner = runner
        self.job_id = job_id
        self.cfg = cfg
        self.arbiter: BufferArbiter = runner.arbiter
        self.events = runner.events

        ml = cfg.max_line_bytes
        self.max_record = max(LHDR.size + 5 * ml + ml,
                              RHDR.size + 5 * ml + ml,
                              AHDR.size + ml,
                              JHDR.size + 4 * ml + 2 * ml)
        self.read_block = max(cfg.read_block_bytes, 2 * (ml + 2))
        self.write_buf = max(cfg.write_buf_bytes, self.max_record)
        self.merge_chunk = max(cfg.merge_chunk_bytes, self.max_record)
        read_mws = self.read_block + 4 * self.read_block + 2 * self.write_buf
        per_reader = 2 * self.merge_chunk + self.max_record + max(LHDR.size, JHDR.size)
        organize_merge_mws = cfg.max_fan_in * per_reader + 2 * self.write_buf
        join_mws = (
            2 * (2 * self.merge_chunk + max(LHDR.size, RHDR.size))
            + cfg.max_active_runs * (2 * self.merge_chunk + AHDR.size)
            + cfg.marker_cache_pages * cfg.marker_page_bytes
            + cfg.active_flush_bytes + self.max_record
            + 2 * self.write_buf
        )
        output_mws = cfg.max_fan_in * per_reader + 2 * self.write_buf
        self.mws = max(read_mws, organize_merge_mws, join_mws, output_mws)
        if self.mws > self.arbiter.budget:
            raise ConfigError(
                f"job {job_id}: minimal working set {self.mws}B exceeds budget "
                f"{self.arbiter.budget}B -- rejected at submit")

        self.state = "PENDING"
        self.phase = "init"
        self.error: str | None = None
        self.left_input = 0
        self.right_input = 0
        self.matches = 0
        self.left_only = 0
        self.right_only = 0
        self.candidate_checks = 0
        self.spills = 0
        self.merge_passes = 0
        self.active_flushes = 0
        self.active_merges = 0
        self.marker_pages_touched = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self.marker_bytes_written = 0
        self.result_runs: list[str] = []
        self._sort_seq = 0   # job-wide monotonic sequence for ordered sort runs
        self.cancel_event = threading.Event()
        self.fail_event = threading.Event()
        self.done_event = threading.Event()
        self._state_lock = threading.Lock()
        self.run_lock = threading.Lock()
        self._spill_lock = threading.Lock()
        self._revocable = 0
        self._hook_pairs_fired = False
        self._hook_spill_fired = False
        # current organize-side run (the structure cooperative reclaim spills)
        self._org_side: str | None = None
        self._org_sub: str | None = None
        self._org_hdr: struct.Struct | None = None
        self._org_records: list[tuple] = []
        self._org_res: list[Reservation] = []
        self._org_runs: list[tuple[str, int]] = []

        self.tmpdir = os.path.join(runner.work_dir, f"job-{job_id}")
        for sub in ("left", "right", "join"):
            os.makedirs(os.path.join(self.tmpdir, sub), exist_ok=True)
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
        self.arbiter.cancel_job(self.job_id)
        self.pool.notify_cancel()

    def _set_phase(self, phase: str):
        self.phase = phase
        self.events.emit("JOB_PHASE", self.job_id, phase=phase,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)

    # ------------------------------------------------------- candidate checks
    def _check_start(self, view: _IntervalView, at: int) -> bool:
        """Counted start predicate of a real interval record against at."""
        self.candidate_checks += 1
        return view.start <= at

    def _evaluate(self, view: _IntervalView, at: int) -> bool:
        """The full counted time predicate on a real interval record."""
        self.candidate_checks += 1
        return view.start <= at < view.end

    # ------------------------------------------------------------------ main
    def run(self):
        try:
            left_side, right_side = self._organize_phase()
            self._check_cancel()
            self._join_phase(left_side, right_side)
            self._check_cancel()
            self._output_phase()
            self._check_cancel()
            self._finish("DONE")
        except Cancelled:
            self._finish("CANCELLED")
        except BaseException as exc:
            self._finish("FAILED", exc)

    def _finish(self, state: str, exc: BaseException | None = None):
        if exc is not None and self.error is None:
            self.error = f"{type(exc).__name__}: {exc}"
        self.writer.cancel()
        self.writer.join()
        self.pool.release_all()
        self.arbiter.release_all(self.job_id)
        self.arbiter.end_phase(self.job_id)
        self.arbiter.unregister_job(self.job_id)
        self.runner.engine.buffer_pools.pop(self.job_id, None)
        if self.runner.engine.fail_first_write_for == self.job_id:
            self.runner.engine.fail_first_write_for = None
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
        if state != "DONE" and os.path.exists(out_tmp):
            try:
                os.unlink(out_tmp)
            except OSError:
                pass
        with self._state_lock:
            self.state = state
        self.events.emit(
            "JOB_STATE", self.job_id, state=state, error=self.error,
            left_input=self.left_input, right_input=self.right_input,
            matches=self.matches, left_only=self.left_only,
            right_only=self.right_only, candidate_checks=self.candidate_checks,
            total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        self.done_event.set()

    # =============================================================== organize
    def _organize_phase(self):
        self._set_phase("organize")
        arb, jid = self.arbiter, self.job_id
        arb.begin_phase(jid, self.mws)
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            try:
                left_side = self._organize_side("left", self.cfg.left_path,
                                                 LHDR, "start")
                self._check_cancel()
                right_side = self._organize_side("right", self.cfg.right_path,
                                                  RHDR, "at")
                self.writer.wait_idle()
                self._check_cancel()
            finally:
                self.pool.release_all()
        finally:
            arb.end_phase(jid)
        return left_side, right_side

    def _organize_side(self, side: str, path: str, hdr: struct.Struct,
                       time_field: str) -> str:
        sub = os.path.join(self.tmpdir, side)
        with self.run_lock:
            self._org_side, self._org_sub, self._org_hdr = side, sub, hdr
            self._org_records, self._org_res, self._org_runs = [], [], []
        lineno = 0
        with open(path, "rb") as f:
            while True:
                self._check_cancel()
                block_res, lines = self._read_block(side, f, lineno)
                if not lines:
                    self.arbiter.release(block_res)
                    break
                parsed, key_res = self._validate_and_extract(
                    side, lines, lineno, time_field)
                with self.run_lock:
                    self._org_records.extend(parsed)
                    self._org_res.append(block_res)
                    self._org_res.append(key_res)
                    self._revocable += (sum(len(r[4]) for r in parsed)
                                        + sum(len(r[0]) for r in parsed))
                    self.arbiter.set_revocable(self.job_id, self._revocable)
                lineno += len(lines)
                if side == "left":
                    self.left_input += len(lines)
                else:
                    self.right_input += len(lines)
                self._maybe_spill_org()
        if self._org_records:
            self._spill_org()
        self.writer.wait_idle()
        self._check_cancel()
        side_file = os.path.join(self.tmpdir, f"{side}.side")
        runs = list(self._org_runs)
        self._merge_runs_to_side(side, sub, runs, hdr, side_file)
        with self.run_lock:
            self._org_side = self._org_sub = self._org_hdr = None
            self._org_records, self._org_res, self._org_runs = [], [], []
        return side_file

    def _read_block(self, side: str, f, lineno):
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        try:
            res = arb.reserve(jid, self.read_block, "line", revocable=True, timeout=0)
        except AllocFailed:
            self._reclaim_spill()
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
                raise IntervalInputError(
                    side, lineno + len(lines) + 1,
                    f"line exceeds max_line_bytes={limit}")
            lines.append(raw)
            total += len(raw)
        if not lines:
            return res, []
        arb.adjust(res, total)
        return res, lines

    def _validate_and_extract(self, side, lines, base_lineno, time_field):
        """Parse + validate a block.  Left tuples carry end explicitly so the
        serialiser never re-parses JSON."""
        arb, jid = self.arbiter, self.job_id
        parsed = []
        keys_str = []
        for i, raw in enumerate(lines):
            n = base_lineno + i + 1
            if not raw.strip():
                raise IntervalInputError(side, n, "empty line")
            try:
                obj = json.loads(raw)
            except Exception as exc:
                raise IntervalInputError(side, n, f"invalid JSON: {exc!r}")
            if not isinstance(obj, dict):
                raise IntervalInputError(side, n, "line is not a JSON object")
            key = obj.get("text")
            if not isinstance(key, str) or not key:
                raise IntervalInputError(side, n, "missing or empty 'text'")
            if side == "left":
                start = self._strict_int64(obj.get("start"), side, n, "start")
                end = self._strict_int64(obj.get("end"), side, n, "end")
                if start > end:
                    raise IntervalInputError(side, n, f"start {start} > end {end}")
            else:
                start = end = 0
                at = self._strict_int64(obj.get("at"), side, n, "at")
            keys_str.append(key)
            if side == "left":
                parsed.append((None, start, n, end, raw))
            else:
                parsed.append((None, at, n, 0, raw))
        bound = 4 * sum(len(k) for k in keys_str)
        try:
            kres = arb.reserve(jid, bound, "key", revocable=True, timeout=0)
        except AllocFailed:
            self._reclaim_spill()
            kres = arb.reserve(jid, bound, "key", revocable=True)
        keys = [k.encode("utf-8") for k in keys_str]
        arb.adjust(kres, sum(len(k) for k in keys))
        out = []
        for rec, k in zip(parsed, keys):
            out.append((k,) + rec[1:])
        return out, kres

    @staticmethod
    def _strict_int64(v, side, lineno, field) -> int:
        if type(v) is not int:  # bool has type bool -> rejected, floats rejected
            raise IntervalInputError(
                side, lineno,
                f"field '{field}' must be a signed 64-bit integer, got {type(v).__name__}")
        if not (INT64_MIN <= v <= INT64_MAX):
            raise IntervalInputError(side, lineno,
                                     f"field '{field}' {v} outside signed 64-bit range")
        return v

    def _maybe_spill_org(self):
        if sum(len(r[0]) + len(r[4]) for r in self._org_records) >= self.read_block * 2:
            self._spill_org()

    def _spill_org(self) -> int:
        with self._spill_lock:
            with self.run_lock:
                if not self._org_records:
                    return 0
                side, sub, hdr = self._org_side, self._org_sub, self._org_hdr
                recs = self._org_records[:]
                self._org_records = []
                reses = self._org_res[:]
                self._org_res = []
                freed = self._revocable
                self._revocable = 0
                self.arbiter.set_revocable(self.job_id, 0)
            self._check_cancel()
            recs.sort(key=lambda r: (r[0], r[1], r[2]))
            seq = len(self._org_runs)
            tmp = os.path.join(sub, f"run-{seq:06d}.tmp")
            final = os.path.join(sub, f"run-{seq:06d}.run")
            idx = self.writer.open_file(tmp, final,
                                        on_closed=self._on_intermediate_closed)
            buf, buf_res = self.pool.acquire()
            off = 0
            try:
                for rec in recs:
                    key, tval, lineno = rec[0], rec[1], rec[2]
                    line = rec[4]
                    if side == "left":
                        head = hdr.pack(len(key), len(line), lineno, tval, rec[3])
                    else:
                        head = hdr.pack(len(key), len(line), lineno, tval)
                    rec_len = len(head) + len(key) + len(line)
                    if off + rec_len > len(buf):
                        self.writer.write_buffer(idx, buf, buf_res, off)
                        self.bytes_written += off
                        buf_res = None
                        buf, buf_res = self.pool.acquire()
                        off = 0
                    buf[off:off + len(head)] = head
                    off += len(head)
                    buf[off:off + len(key)] = key
                    off += len(key)
                    buf[off:off + len(line)] = line
                    off += len(line)
                if off:
                    self.writer.write_buffer(idx, buf, buf_res, off)
                    self.bytes_written += off
                    buf_res = None
            finally:
                if buf_res is not None:
                    self.pool.buffer_returned(buf_res)
            self.writer.close_file(idx)
            for res in reses:
                self.arbiter.release(res)
            self.spills += 1
            with self.run_lock:
                self._org_runs.append((final, len(recs)))
            self.events.emit("SPILL_SERIALIZED", self.job_id, side=side,
                             records=len(recs), freed=freed,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
            return freed

    def _on_intermediate_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or f"intermediate file {final} discarded")

    def _merge_runs_to_side(self, side, sub, runs, hdr, side_file):
        body_idx = (0, 1)  # key length, line length
        fan_in = self.cfg.max_fan_in
        current = list(runs)
        pass_no = 0
        while len(current) > fan_in:
            self._check_cancel()
            new_runs = []
            for gi in range(0, len(current), fan_in):
                group = current[gi:gi + fan_in]
                if len(group) == 1:
                    new_runs.append(group[0])
                    continue
                seq = 100000 * (pass_no + 1) + gi
                tmp = os.path.join(sub, f"merge-{pass_no}-{seq:06d}.tmp")
                final = os.path.join(sub, f"merge-{pass_no}-{seq:06d}.run")
                idx = self.writer.open_file(tmp, final,
                                            on_closed=self._on_intermediate_closed)
                n = self._copy_kway(group, hdr, body_idx, idx)
                self.writer.close_file(idx)
                self.writer.wait_idle()
                self._check_cancel()
                new_runs.append((final, n))
            current = new_runs
            self.merge_passes += 1
            pass_no += 1
        idx = self.writer.open_file(side_file + ".tmp", side_file,
                                    on_closed=self._on_intermediate_closed)
        if current:
            self._copy_kway(current, hdr, body_idx, idx)
        self.writer.close_file(idx)
        self.writer.wait_idle()
        self._check_cancel()

    def _copy_kway(self, group, hdr, body_idx, writer_idx) -> int:
        readers = [BinReader(self, path, hdr, body_idx) for path, _ in group]
        heap = []
        buf_res = None
        try:
            # standard k-way: one head record per reader; each record keeps its
            # chunk reserved until it is popped and released, so cross-chunk
            # memoryviews stay valid.  Order: (key, time, lineno) -- LHDR/RHDR
            # both place the sweep time (start/at) in field 3, lineno in 2.
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    fields, pieces, key, chunk = rec
                    heapq.heappush(
                        heap, (key, fields[3], fields[2], i, fields, pieces, chunk))
            buf, buf_res = self.pool.acquire()
            off = 0
            n_out = 0
            while heap:
                self._check_cancel()
                _key, _time, _ln, i, fields, pieces, chunk = heapq.heappop(heap)
                r = readers[i]
                head = hdr.pack(*fields)
                body = b"".join(p for ps in pieces for p in ps)
                rec_len = len(head) + len(body)
                if off + rec_len > len(buf):
                    self.writer.write_buffer(writer_idx, buf, buf_res, off)
                    self.bytes_written += off
                    buf_res = None
                    buf, buf_res = self.pool.acquire()
                    off = 0
                buf[off:off + len(head)] = head
                off += len(head)
                buf[off:off + len(body)] = body
                off += len(body)
                n_out += 1
                r.release_record(chunk)
                nxt = r.next()
                if nxt is not None:
                    f2, p2, k2, c2 = nxt
                    heapq.heappush(
                        heap, (k2, f2[3], f2[2], i, f2, p2, c2))
            if off:
                self.writer.write_buffer(writer_idx, buf, buf_res, off)
                self.bytes_written += off
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

    # ==================================================================== join
    def _join_phase(self, left_side: str, right_side: str):
        self._set_phase("join")
        arb, jid = self.arbiter, self.job_id
        arb.begin_phase(jid, self.mws)
        self._key_seq = 0
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            result = _ResultWriter(self)
            try:
                L = BinReader(self, left_side, LHDR, (0, 1))
                R = BinReader(self, right_side, RHDR, (0, 1))
                try:
                    lcur = L.next()
                    rcur = R.next()
                    while lcur is not None or rcur is not None:
                        self._check_cancel()
                        lk = lcur[2] if lcur is not None else None
                        rk = rcur[2] if rcur is not None else None
                        key = lk if (rk is None or (lk is not None and lk <= rk)) else rk
                        lcur, rcur = self._process_key(
                            key, L, R, lcur, rcur, result)
                        self._emit_join_stats(key)
                finally:
                    L.close()
                    R.close()
            finally:
                result.close_all()
                self.pool.release_all()
        finally:
            arb.end_phase(jid)

    def _emit_join_stats(self, key):
        self.events.emit(
            "JOIN_STATS", self.job_id, key=key.decode("utf-8", "replace"),
            left_input=self.left_input, right_input=self.right_input,
            matches=self.matches, left_only=self.left_only,
            right_only=self.right_only, candidate_checks=self.candidate_checks,
            active_flushes=self.active_flushes,
            total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)

    def _process_key(self, key, L: BinReader, R: BinReader, lcur, rcur,
                     result: "_ResultWriter"):
        has_left = lcur is not None and lcur[2] == key
        has_right = rcur is not None and rcur[2] == key
        self._key_seq += 1
        if not has_left:
            while rcur is not None and rcur[2] == key:
                self._emit_orphan(KIND_RIGHT_ONLY, None, rcur, result)
                R.release_record(rcur[3])
                rcur = R.next()
            return lcur, rcur
        if not has_right:
            while lcur is not None and lcur[2] == key:
                self._emit_orphan(KIND_LEFT_ONLY, lcur, None, result)
                L.release_record(lcur[3])
                lcur = L.next()
            return lcur, rcur

        # lcur is the first record of the group (most recently returned by
        # L.next()), so its own file offset -- not the next-record mark -- is
        # where the orphan re-stream must begin.
        group_start = L.last_record_pos()
        marker_path = os.path.join(self.tmpdir, "join",
                                   f"marker-{self._key_seq:06d}.bit")
        marker = MarkerBitmap(self, marker_path, self.cfg.marker_page_bytes,
                              self.cfg.marker_cache_pages)
        self.marker_pages_touched += 1
        heap = []          # (end, ordinal, lineno, start, line_bytearray, res)
        heap_bytes = 0
        active = _ActiveRuns(self, self._key_seq)
        ordinal = 0
        n_left_seen = 0
        try:
            while rcur is not None and rcur[2] == key:
                self._check_cancel()
                rfields, rpieces, _rk, rchunk = rcur
                r_lineno, at = rfields[2], rfields[3]
                # activate intervals with start <= at (sorted cursor)
                while lcur is not None and lcur[2] == key:
                    lfields, lpieces, _lk, lchunk = lcur
                    view = _IntervalView(lfields[3], lfields[4])
                    if not self._check_start(view, at):
                        break
                    line = b"".join(lpieces[1])
                    res = self.arbiter.reserve(self.job_id, len(line), "active")
                    line_bytes = bytearray(line)
                    entry = (lfields[4], ordinal, lfields[2], lfields[3],
                             line_bytes, res)
                    heapq.heappush(heap, entry)
                    heap_bytes += len(line_bytes)
                    ordinal += 1
                    n_left_seen += 1
                    L.release_record(lchunk)
                    lcur = L.next()
                # expire end <= at
                while heap and not self._evaluate(_HeapRec(heap[0]), at):
                    _end, _o, _ln, _st, line_bytes, res = heapq.heappop(heap)
                    heap_bytes -= len(line_bytes)
                    self.arbiter.release(res)
                if heap_bytes >= self.cfg.active_flush_bytes:
                    heap_bytes = active.flush(heap)
                    self._fire_hot_spill_hook()
                n_match = 0
                for entry in heap:
                    if self._evaluate(_HeapRec(entry), at):
                        self._emit_match(entry, rcur, result)
                        marker.set_bit(entry[1])
                        n_match += 1
                for run in active.runs:
                    n_match += run.scan(at, rcur, marker, self, result)
                if active.maybe_merge():
                    self._fire_hot_spill_hook()
                if n_match == 0:
                    self._emit_orphan(KIND_RIGHT_ONLY, None, rcur, result)
                R.release_record(rchunk)
                rcur = R.next()

            # drain remaining intervals without checks (no later observation)
            self._release_heap(heap)
            active.close()
            marker.flush()
            lcur, n_drained = self._drain_remaining(L, key, lcur)
            n_left_total = n_left_seen + n_drained
            re_reader = BinReader(self, L.path, LHDR, (0, 1),
                                  start_offset=group_start)
            orphans = marker.stream_orphans(n_left_total)
            try:
                next_orphan = next(orphans, None)
                ord_no = 0
                rec = re_reader.next()
                while rec is not None and rec[2] == key:
                    rfields, rpieces, rk, rchunk2 = rec
                    if next_orphan is not None and ord_no == next_orphan:
                        self._emit_orphan(KIND_LEFT_ONLY, rec, None, result)
                        try:
                            next_orphan = next(orphans)
                        except StopIteration:
                            next_orphan = None
                    ord_no += 1
                    re_reader.release_record(rchunk2)
                    rec = re_reader.next()
            finally:
                # deterministically return the generator's page reservation
                orphans.close()
                re_reader.close()
        finally:
            marker.close()
            try:
                os.unlink(marker_path)
            except OSError:
                pass
        return lcur, rcur

    def _drain_remaining(self, L: BinReader, key, lcur):
        """Consume left rows of this key never activated (at never reached).

        Returns (advanced_cursor, count).  The advanced cursor must replace
        the caller's lcur, otherwise the same key is processed twice.
        """
        n = 0
        while lcur is not None and lcur[2] == key:
            n += 1
            L.release_record(lcur[3])
            lcur = L.next()
        return lcur, n

    def _release_heap(self, heap):
        for entry in heap:
            self.arbiter.release(entry[5])
        heap.clear()

    def _fire_hot_spill_hook(self):
        if not self._hook_spill_fired:
            self._hook_spill_fired = True
            self._hook("hot_group_spilled")

    # ------------------------------------------------------------- emit records
    def _emit_match(self, left_entry, rcur, result: "_ResultWriter"):
        rfields, rpieces, rkey, _rchunk = rcur
        rline = b"".join(rpieces[1])
        head = JHDR.pack(len(rkey), KIND_MATCH, left_entry[2], rfields[2],
                         len(left_entry[4]), len(rline))
        result.write(head, rkey, [left_entry[4]], [rline])
        self._bump_matches()

    def _emit_orphan(self, kind, lcur, rcur, result: "_ResultWriter"):
        if kind == KIND_LEFT_ONLY:
            fields, pieces, key, _chunk = lcur
            line = b"".join(pieces[1])
            head = JHDR.pack(len(key), KIND_LEFT_ONLY, fields[2], 0,
                             len(line), 0)
            result.write(head, key, [line], [])
            self.left_only += 1
        else:
            fields, pieces, key, _chunk = rcur
            line = b"".join(pieces[1])
            head = JHDR.pack(len(key), KIND_RIGHT_ONLY, 0, fields[2], 0,
                             len(line))
            result.write(head, key, [], [line])
            self.right_only += 1

    def _bump_matches(self):
        self.matches += 1
        if not self._hook_pairs_fired:
            self._hook_pairs_fired = True
            self._hook("pairs_generated", matches=self.matches)

    # =============================================================== output
    def _output_phase(self):
        self._set_phase("output_sort")
        arb, jid = self.arbiter, self.job_id
        arb.begin_phase(jid, self.mws)
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            runs = list(self.result_runs)
            # Result runs are produced in sweep order, not output order.  First
            # turn each one into a set of internally-ordered runs via bounded
            # block sorts (one chunk resident at a time), then k-way merge the
            # ordered runs in the usual (one head record per reader) way.
            ordered: list[tuple[str, int]] = []
            for src in runs:
                ordered.extend(self._sort_result_run(src))
            current = ordered
            fan_in = self.cfg.max_fan_in
            pass_no = 0
            while len(current) > fan_in:
                self._check_cancel()
                new_runs = []
                for gi in range(0, len(current), fan_in):
                    group = current[gi:gi + fan_in]
                    if len(group) == 1:
                        new_runs.append(group[0])
                        continue
                    tmp = os.path.join(self.tmpdir, "join",
                                       f"outmerge-{pass_no}-{gi:06d}.tmp")
                    final = os.path.join(self.tmpdir, "join",
                                         f"outmerge-{pass_no}-{gi:06d}.run")
                    idx = self.writer.open_file(tmp, final,
                                                on_closed=self._on_intermediate_closed)
                    n = self._merge_join_runs(group, idx, jsonl=False)
                    self.writer.close_file(idx)
                    self.writer.wait_idle()
                    self._check_cancel()
                    new_runs.append((final, n))
                current = new_runs
                self.merge_passes += 1
                pass_no += 1
            self._set_phase("publish")
            self._hook("before_publish")
            out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
            idx = self.writer.open_file(out_tmp, self.cfg.output_path,
                                        on_closed=self._on_output_closed)
            if current:
                self._merge_join_runs(current, idx, jsonl=True)
            self.writer.close_file(idx)
            self.writer.wait_idle()
            self._check_cancel()
        finally:
            self.pool.release_all()
            arb.end_phase(jid)

    def _sort_result_run(self, src_path: str) -> list[tuple[str, int]]:
        """Turn one unordered join-result run into internally ordered runs.

        Reads one bounded chunk at a time (each record keeps its chunk
        reserved until released), heap-sorts the chunk by the output key and
        writes it as its own ordered run.  Memory stays bounded by one chunk;
        the resulting ordered runs are later merged by ``_merge_join_runs``.
        """
        out_runs: list[tuple[str, int]] = []
        reader = BinReader(self, src_path, JHDR, (0, 4, 5))
        try:
            while True:
                recs = reader.read_chunk_records()
                if not recs:
                    break
                ordered_recs = sorted(
                    recs,
                    key=lambda rc: (rc[2], _FLAG[rc[0][1]], rc[0][2], rc[0][3]))
                seq = self._sort_seq
                self._sort_seq += 1
                tmp = os.path.join(self.tmpdir, "join",
                                   f"sort-{seq:06d}.tmp")
                final = os.path.join(self.tmpdir, "join",
                                     f"sort-{seq:06d}.run")
                idx = self.writer.open_file(tmp, final,
                                            on_closed=self._on_intermediate_closed)
                buf, buf_res = self.pool.acquire()
                off = 0
                try:
                    for fields, pieces, key, _off, chunk in ordered_recs:
                        data = JHDR.pack(*fields) + b"".join(
                            p for ps in pieces for p in ps)
                        if off + len(data) > len(buf):
                            self.writer.write_buffer(idx, buf, buf_res, off)
                            self.bytes_written += off
                            buf_res = None
                            buf, buf_res = self.pool.acquire()
                            off = 0
                        buf[off:off + len(data)] = data
                        off += len(data)
                    if off:
                        self.writer.write_buffer(idx, buf, buf_res, off)
                        self.bytes_written += off
                        buf_res = None
                    else:
                        self.pool.buffer_returned(buf_res)
                        buf_res = None
                finally:
                    if buf_res is not None:
                        self.arbiter.release(buf_res)
                    for _f, _p, _k, _o, chunk in ordered_recs:
                        reader.release_record(chunk)
                self.writer.close_file(idx)
                self.writer.wait_idle()
                self._check_cancel()
                out_runs.append((final, len(ordered_recs)))
                seq += 1
        finally:
            reader.close()
        return out_runs

    def _merge_join_runs(self, group, writer_idx, *, jsonl: bool) -> int:
        # Standard k-way merge over already-ordered runs: one head record per
        # reader; each record keeps its chunk reserved until popped/released.
        readers = [BinReader(self, path, JHDR, (0, 4, 5)) for path, _ in group]
        heap = []
        buf_res = None
        try:
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    fields, pieces, key, chunk = rec
                    _klen, kind, lrow, rrow, ll, rl = fields
                    heapq.heappush(
                        heap, (key, _FLAG[kind], lrow, rrow, i,
                               fields, pieces, chunk))
            buf, buf_res = self.pool.acquire()
            off = 0
            n_out = 0
            while heap:
                self._check_cancel()
                key, _flag, _lrow, _rrow, i, fields, pieces, chunk = \
                    heapq.heappop(heap)
                r = readers[i]
                if jsonl:
                    data = self._render_json(fields, pieces, key) + b"\n"
                else:
                    data = JHDR.pack(*fields) + b"".join(
                        p for ps in pieces for p in ps)
                if off + len(data) > len(buf):
                    self.writer.write_buffer(writer_idx, buf, buf_res, off)
                    self.bytes_written += off
                    buf_res = None
                    buf, buf_res = self.pool.acquire()
                    off = 0
                buf[off:off + len(data)] = data
                off += len(data)
                n_out += 1
                r.release_record(chunk)
                nxt = r.next()
                if nxt is not None:
                    f2, p2, k2, c2 = nxt
                    heapq.heappush(
                        heap,
                        (k2, _FLAG[f2[1]], f2[2], f2[3], i, f2, p2, c2))
            if off:
                self.writer.write_buffer(writer_idx, buf, buf_res, off)
                self.bytes_written += off
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

    def _render_json(self, fields, pieces, key_bytes) -> bytes:
        _klen, kind, lrow, rrow, llen, rlen = fields
        key = key_bytes.decode("utf-8")
        left_obj = right_obj = None
        # body fields: pieces[0]=key, pieces[1]=left, pieces[2]=right
        if llen:
            raw = b"".join(pieces[1])
            left_obj = json.loads(bytes(raw))
        if rlen:
            raw = b"".join(pieces[2])
            right_obj = json.loads(bytes(raw))
        out = {
            "kind": KIND_NAME[kind],
            "key": key,
            "left_row": lrow if kind != KIND_RIGHT_ONLY else None,
            "right_row": rrow if kind != KIND_LEFT_ONLY else None,
            "left": left_obj,
            "right": right_obj,
        }
        return json.dumps(out, ensure_ascii=False).encode("utf-8")

    def _on_output_closed(self, *, ok, error, tmp, final):
        if ok and not self.cancel_event.is_set() and not self.fail_event.is_set():
            self.events.emit("OUTPUT_PUBLISH", self.job_id, output=final,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
        elif not ok and not self.cancel_event.is_set():
            self._fail(error or "output discarded")

    # ------------------------------------------------------------------ reclaim
    def _reclaim_spill(self) -> int:
        return self._reclaim_handler(1 << 30)

    def _reclaim_handler(self, nbytes: int) -> int:
        """Cooperative reclaim, invoked on another job's thread WITHOUT the
        arbiter lock.  Never calls reserve(); only spills the organize-phase
        run (whose bytes are already reserved)."""
        self._hook("reclaim_enter")
        try:
            if self.cancel_event.is_set() or self.fail_event.is_set():
                return 0
            if self.phase != "organize":
                return 0
            return self._spill_org()
        except (Cancelled, JobFailed):
            return 0
        finally:
            self._hook("reclaim_exit")

    # ------------------------------------------------------------------ report
    def metadata_estimate(self) -> int:
        per_record = 8 + 64 + 49
        return (self.left_input + self.right_input) * per_record

    def status(self) -> dict:
        with self._state_lock:
            return {
                "job_id": self.job_id,
                "job_type": "interval-join",
                "state": self.state,
                "phase": self.phase,
                "error": self.error,
                "left_input": self.left_input,
                "right_input": self.right_input,
                "matches": self.matches,
                "left_only": self.left_only,
                "right_only": self.right_only,
                "candidate_checks": self.candidate_checks,
                "spills": self.spills,
                "merge_passes": self.merge_passes,
                "active_flushes": self.active_flushes,
                "active_merges": self.active_merges,
                "marker_pages": self.marker_pages_touched,
                "bytes_written": self.bytes_written,
                "bytes_read": self.bytes_read,
                "held_bytes": self.arbiter.held(self.job_id),
                "mws": self.mws,
                "left_input_path": self.cfg.left_path,
                "right_input_path": self.cfg.right_path,
                "output": self.cfg.output_path,
            }


# ---------------------------------------------------------------- active runs
class _ActiveRun:
    """One end-sorted spill of active intervals, with a persistent expiry mark.

    ``scan`` seeks to the mark (first record not known expired), reads
    sequentially: an expired record advances the mark and is checked once;
    every remaining record has end > at (the run is end-sorted) but still goes
    through the counted full predicate before becoming a match.  The match
    suffix is re-read for later observations, so scan I/O is linear in the
    candidate checks performed.
    """

    def __init__(self, controller: "_ActiveRuns", path: str, mark: int = 0):
        self.controller = controller
        self.path = path
        self.mark = mark

    def scan(self, at, rcur, marker, job, result) -> int:
        reader = BinReader(job, self.path, AHDR, (0,), key_index=None)
        n_match = 0
        try:
            reader.seek(self.mark)
            rfields, rpieces, rkey, _rc = rcur
            while True:
                rec = reader.next()
                if rec is None:
                    break
                fields, pieces, _k, chunk = rec
                _llen, lineno, ordinal, start, end = fields
                view = _IntervalView(start, end)
                if not job._evaluate(view, at):
                    self.mark = reader.last_record_pos()
                    reader.release_record(chunk)
                    continue
                line = b"".join(pieces[0])
                rline = b"".join(rpieces[1])
                head = JHDR.pack(len(rkey), KIND_MATCH, lineno, rfields[2],
                                 len(line), len(rline))
                result.write(head, rkey, [line], [rline])
                job._bump_matches()
                marker.set_bit(ordinal)
                n_match += 1
                reader.release_record(chunk)
        finally:
            reader.close()
        return n_match


class _ActiveRuns:
    def __init__(self, job: IntervalJoinJob, key_seq: int):
        self.job = job
        self.key_seq = key_seq
        self.runs: list[_ActiveRun] = []
        self._seq = 0

    def flush(self, heap) -> int:
        """Pop the whole heap in end order and write an active run."""
        self._seq += 1
        path = os.path.join(self.job.tmpdir, "join",
                            f"active-{self.key_seq:06d}-{self._seq:06d}.run")
        idx = self.job.writer.open_file(path + ".tmp", path,
                                        on_closed=self.job._on_intermediate_closed)
        buf, buf_res = self.job.pool.acquire()
        off = 0
        written = 0
        try:
            while heap:
                end, ordinal, lineno, start, line_bytes, res = heapq.heappop(heap)
                head = AHDR.pack(len(line_bytes), lineno, ordinal, start, end)
                rec_len = len(head) + len(line_bytes)
                if off + rec_len > len(buf):
                    self.job.writer.write_buffer(idx, buf, buf_res, off)
                    self.job.bytes_written += off
                    written += off
                    buf_res = None
                    buf, buf_res = self.job.pool.acquire()
                    off = 0
                buf[off:off + len(head)] = head
                off += len(head)
                buf[off:off + len(line_bytes)] = line_bytes
                off += len(line_bytes)
                self.job.arbiter.release(res)
            if off:
                self.job.writer.write_buffer(idx, buf, buf_res, off)
                self.job.bytes_written += off
                written += off
                buf_res = None
        finally:
            if buf_res is not None:
                self.job.pool.buffer_returned(buf_res)
        self.job.writer.close_file(idx)
        self.job.writer.wait_idle()
        self.job._check_cancel()
        self.job.active_flushes += 1
        self.job.events.emit("JOIN_ACTIVE_FLUSH", self.job.job_id, run=path,
                             bytes=written,
                             total_reserved=self.job.arbiter.total_reserved,
                             budget=self.job.arbiter.budget)
        self.runs.append(_ActiveRun(self, path))
        return 0

    def maybe_merge(self) -> bool:
        if len(self.runs) < self.job.cfg.max_active_runs:
            return False
        self._seq += 1
        merged_path = os.path.join(
            self.job.tmpdir, "join",
            f"active-{self.key_seq:06d}-m{self._seq:06d}.run")
        # honour each run's expiry mark: expired records never re-enter checks
        readers = []
        for run in self.runs:
            rr = BinReader(self.job, run.path, AHDR, (0,), key_index=None)
            rr.seek(run.mark)
            readers.append(rr)
        idx = self.job.writer.open_file(merged_path + ".tmp", merged_path,
                                        on_closed=self.job._on_intermediate_closed)
        heap = []
        buf_res = None
        old_paths = [r.path for r in self.runs]
        marks = [r.mark for r in self.runs]
        try:
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    fields, pieces, _k, chunk = rec
                    heapq.heappush(heap, (fields[4], i, fields, pieces, chunk))
            buf, buf_res = self.job.pool.acquire()
            off = 0
            while heap:
                self.job._check_cancel()
                _end, i, fields, pieces, chunk = heapq.heappop(heap)
                r = readers[i]
                data = AHDR.pack(*fields) + b"".join(pieces[0])
                if off + len(data) > len(buf):
                    self.job.writer.write_buffer(idx, buf, buf_res, off)
                    self.job.bytes_written += off
                    buf_res = None
                    buf, buf_res = self.job.pool.acquire()
                    off = 0
                buf[off:off + len(data)] = data
                off += len(data)
                r.release_record(chunk)
                nxt = r.next()
                if nxt is not None:
                    f2, p2, _k2, c2 = nxt
                    heapq.heappush(heap, (f2[4], i, f2, p2, c2))
            if off:
                self.job.writer.write_buffer(idx, buf, buf_res, off)
                self.job.bytes_written += off
                buf_res = None
            else:
                self.job.pool.buffer_returned(buf_res)
                buf_res = None
        finally:
            if buf_res is not None:
                self.job.arbiter.release(buf_res)
            for r in readers:
                r.close()
        self.job.writer.close_file(idx)
        self.job.writer.wait_idle()
        self.job._check_cancel()
        for p in old_paths:
            try:
                os.unlink(p)
            except OSError:
                pass
        # records were read from each mark onward, so the merged mark is zero
        self.runs = [_ActiveRun(self, merged_path, mark=0)]
        self.job.active_merges += 1
        self.job.events.emit("JOIN_ACTIVE_MERGE", self.job.job_id,
                             run=merged_path, old_marks=marks,
                             total_reserved=self.job.arbiter.total_reserved,
                             budget=self.job.arbiter.budget)
        return True

    def close(self):
        for r in self.runs:
            try:
                os.unlink(r.path)
            except OSError:
                pass
        self.runs = []


# -------------------------------------------------------------- result writer
class _ResultWriter:
    """Rolling writer for join-result runs (JHDR records).

    Holds one fill buffer from the pool at a time; fills it across records,
    hands full buffers to the writer thread and rolls to a new run past
    ``result_run_bytes``.  Every byte written is already covered by the
    buffer's reservation (transferred to kind "io" while in flight).
    """

    def __init__(self, job: IntervalJoinJob):
        self.job = job
        self._seq = 0
        self._idx = None
        self._path = None
        self._run_bytes = 0
        self._buf = None
        self._buf_res = None
        self._off = 0

    def _ensure_run(self):
        if self._idx is None:
            self._seq += 1
            self._path = os.path.join(self.job.tmpdir, "join",
                                      f"result-{self._seq:06d}.run")
            self._idx = self.job.writer.open_file(
                self._path + ".tmp", self._path,
                on_closed=self.job._on_intermediate_closed)
            self._run_bytes = 0
        if self._buf is None:
            self._buf, self._buf_res = self.job.pool.acquire()
            self._off = 0

    def _flush_buffer(self):
        if self._off:
            self.job.writer.write_buffer(self._idx, self._buf, self._buf_res,
                                         self._off)
            self.job.bytes_written += self._off
            self._run_bytes += self._off
            self._buf = None
            self._buf_res = None
            self._off = 0
        else:
            self.job.pool.buffer_returned(self._buf_res)
            self._buf = None
            self._buf_res = None

    def _roll(self):
        self._flush_buffer()
        self.job.writer.close_file(self._idx)
        self.job.writer.wait_idle()
        self.job._check_cancel()
        self.job.result_runs.append(self._path)
        self._idx = None

    def write(self, head: bytes, key: bytes, left_chunks, right_chunks):
        self._ensure_run()
        total = len(head) + len(key) + sum(map(len, left_chunks)) \
            + sum(map(len, right_chunks))
        if total > len(self._buf):
            raise JobFailed(f"join result record {total}B exceeds write buffer")
        if self._run_bytes and self._run_bytes + total > self.job.cfg.result_run_bytes:
            self._roll()
            self._ensure_run()
        for chunk in [head, key, *left_chunks, *right_chunks]:
            if not chunk:
                continue
            if self._off + len(chunk) > len(self._buf):
                self._flush_buffer()
                self._ensure_run()
            self._buf[self._off:self._off + len(chunk)] = chunk
            self._off += len(chunk)

    def close_all(self):
        if self._idx is not None:
            self._roll()
