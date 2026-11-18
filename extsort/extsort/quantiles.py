"""Group quantile job: exact nearest-rank thresholds per ``text`` group.

Given one unordered UTF-8 JSONL input whose every line is a JSON object with a
non-empty string ``text`` and a JSON number ``value``, and one set of unique
integer basis points ``bp`` (0..10000, 10000 == 100%), produce -- without ever
materialising a re-sorted full detail output -- one result row per
``(text, bp)``:

    text, bp, n (group size), r (nearest rank, 1-based),
    source_line (physical input line, 1-based), value_token (original spelling)

Rank rule: ``r = max(1, ceil(bp*n/10000))``; bp=0 therefore selects the group
minimum.  Equal values share one ordering position family and the source of a
rank landing on an equal-value run is the smallest physical line number among
them (records sort by (value, lineno)).  Rows are ordered by text Unicode
codepoints and then by numeric bp.

Exact decimal semantics
------------------------
Values are *never* converted to binary float.  The JSON decoder hands the
original number lexeme through parse hooks; a pure string-operation canonical
form (:func:`decimal_order_key`) turns the lexeme into a byte key whose
lexicographic order is the exact numeric order, so integers, fractions and
extreme exponents compare exactly (1, 1.0 and 1e0 share a key; adjacent
integers beyond float precision stay distinct).  The original lexeme is kept
byte-for-byte as ``value_token``.

Phases (each admitted under its own minimal-working-set envelope)
-----------------------------------------------------------------
  PARSE     read reserved blocks, validate every line (any contract violation
            fails the whole job and names the physical line), build exact
            order keys, accumulate bounded runs and spill them sorted by
            (text, value, lineno) -- the *single* organise pass shared by every
            requested bp.
  MERGE     multi-pass k-way merge of the spill runs into one globally sorted
            run plus a small per-group index (group key, record count).
  EXTRACT   stream the sorted run group by group against the index, keep only
            the records sitting on requested ranks, render result rows.
  PUBLISH   tmp -> os.replace, atomic; failure/cancel never publishes and only
            the job's own temp files are deleted.

A single hot group may be far larger than the budget B and the number of
groups may be far larger than B: neither a whole group nor any per-group count
table is ever resident.  All resident byte content (input blocks, text keys,
value tokens, order keys, merge buffers, write buffers, in-flight I/O) is
reserved through the job's single BufferArbiter id before the bytes are copied;
Python fixed metadata is estimated separately and reported next to real RSS.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import struct
import sys
import threading

from .arbiter import AllocFailed, Cancelled, ConfigError
from .job import BufferPool, JobFailed
from .arbiter import Reservation

# enormous decimal exponents/int strings must not be rejected by the interpreter
if hasattr(sys, "set_int_max_str_digits"):
    sys.set_int_max_str_digits(0)

# Run record: klen, tlen (value token), lineno, vklen (decimal order key).
HDR = struct.Struct(">IIQH")           # 20 bytes, big-endian
# Group index record: group size n, key length | key bytes
IHDR = struct.Struct(">QI")            # 12 bytes

# The JSON number grammar (RFC 8259), anchored; product input tokens always match.
DECIMAL_RE = re.compile(rb"^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")

# JSON constants that are not finite decimal numbers.
def _reject_constant(token: str):
    raise _ConstantRejected(token)


class _ConstantRejected(JobFailed):
    def __init__(self, token: str):
        super().__init__(f"non-finite numeric constant {token!r}")
        self.token = token


def _num_tag(lexeme: str):
    """json parse hook: keep the exact number lexeme instead of a binary number."""
    return ("__gq_num__", lexeme)


class QuantileInputError(JobFailed):
    """A data-contract violation; carries the 1-based physical line number."""

    def __init__(self, lineno: int, reason: str):
        super().__init__(f"line {lineno}: {reason}")
        self.lineno = lineno
        self.reason = reason


# ------------------------------------------------------------- exact decimals
def decimal_order_key(token: bytes) -> bytes:
    """Map a JSON number lexeme to a byte string whose lexicographic order is
    the exact numeric order.

    Pure string arithmetic: no float, and no big-integer magnitude is ever
    built.  Layout (sign classes ordered neg < zero < pos)::

        zero       b"\\x02"
        positive   b"\\x03" + <lead:2B> + digits + b"\\x00"
        negative   b"\\x01" + inv(<lead:2B>) + comp(digits) + b"\\xff"

    ``lead`` is the decimal power of the most significant digit
    (10**lead <= |x| < 10**(lead+1)), stored biased by 32768 as an unsigned
    big-endian short so byte order == numeric order; for negatives it is
    bit-inverted so magnitude classes order descending.  ``digits`` are the
    significant decimal digits with trailing zeros removed.  Positive digit
    strings end with a 0x00 terminator (below every digit byte 0x30..0x39), so
    a prefix-shorter number sorts first; negative digits are complemented
    (``0x78 - d``, reversing 0..9) and terminated with 0xFF (above every
    complemented digit byte), giving the reversed magnitude order including
    the prefix case.  Numerically equal lexemes (1, 1.0, 1e0) normalise to the
    same key.
    """
    neg = token[:1] == b"-"
    s = token[1:] if neg else token
    mant, _, exp = s.partition(b"e")
    if not exp:
        mant, _, exp = s.partition(b"E")
    e = int(exp) if exp else 0
    ip, _, fp = mant.partition(b".")
    raw = ip + fp
    significant = raw.lstrip(b"0")
    if not significant:                       # +/- zero in any spelling
        return b"\x02"
    first_sig = len(raw) - len(significant)
    lead = len(ip) + e - first_sig - 1
    digits = significant.rstrip(b"0")
    if not digits:                            # all-zero significand (e.g. -0.0)
        return b"\x02"
    if not -32768 <= lead <= 32767 or len(digits) > 65535:
        raise QuantileInputError(0, "decimal magnitude outside encodable range")
    lead_u = (lead + 32768).to_bytes(2, "big")
    if neg:
        inv_lead = bytes((~b) & 0xFF for b in lead_u)
        return b"\x01" + inv_lead + bytes(0x78 - d for d in digits) + b"\xff"
    return b"\x03" + lead_u + digits + b"\x00"


# ---------------------------------------------------------------------- config
class QuantileConfig:
    def __init__(self, input_path, output_path, percentiles_bp, *,
                 max_line_bytes=256 * 1024,
                 read_block_bytes=256 * 1024,
                 write_buf_bytes=512 * 1024,
                 merge_chunk_bytes=256 * 1024,
                 max_fan_in=16,
                 run_record_capacity=0,        # 0 == spill only on memory pressure
                 inject_io_failure=False,
                 hooks=None):
        self.input_path = input_path
        self.output_path = output_path
        bps = list(percentiles_bp)
        if not bps:
            raise ConfigError("percentiles_bp must contain at least one basis point")
        for bp in bps:
            if isinstance(bp, bool) or not isinstance(bp, int):
                raise ConfigError(f"percentile basis point must be int, got {bp!r}")
            if not 0 <= bp <= 10000:
                raise ConfigError(f"percentile basis point {bp} outside [0, 10000]")
        if len(set(bps)) != len(bps):
            raise ConfigError("percentile basis points must be unique within one job")
        self.percentiles_bp = tuple(sorted(bps))
        self.max_line_bytes = int(max_line_bytes)
        self.read_block_bytes = int(read_block_bytes)
        self.write_buf_bytes = int(write_buf_bytes)
        self.merge_chunk_bytes = int(merge_chunk_bytes)
        self.max_fan_in = max(2, int(max_fan_in))
        if int(run_record_capacity) < 0:
            raise ConfigError("run_record_capacity must be >= 0")
        self.run_record_capacity = int(run_record_capacity)
        self.inject_io_failure = bool(inject_io_failure)
        self.hooks = hooks or {}


# ------------------------------------------------------------------ run reader
class QRunReader:
    """Buffered reader for quantile spill runs (HDR | key | token | vkey).

    Same reservation discipline as the sort kernel: one reserved chunk plus a
    per-chunk materialised-key reservation (key *and* order key, needed for
    heap comparisons), exact-size remainder reads for straddling records, a
    reserved fixed scratch buffer for header straddles.  Reservations of a
    chunk are released once its last live record is released.
    """

    def __init__(self, job: "GroupQuantileJob", path: str):
        self.job = job
        self.path = path
        self.fp = open(path, "rb")
        self.chunk_size = job.merge_chunk
        self._records: list[tuple] = []   # (key, vkey, lineno, token_pieces)
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
        self.job.bytes_read += len(chunk)
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
        parsed = []
        while off < n:
            if n - off < HDR.size:
                tail = buf[off:]
                self._scratch[:len(tail)] = tail
                got = self.fp.readinto(memoryview(self._scratch)[len(tail):])
                self.job.bytes_read += HDR.size - len(tail)
                if got != HDR.size - len(tail):
                    raise JobFailed(f"truncated run file {self.path}")
                klen, tlen, lineno, vklen = HDR.unpack(self._scratch)
                off = n
            else:
                klen, tlen, lineno, vklen = HDR.unpack_from(buf, off)
                off += HDR.size
            body = klen + tlen + vklen
            if n - off >= body:
                key_pieces = [buf[off:off + klen]]
                token_pieces = [buf[off + klen:off + klen + tlen]]
                vkey_pieces = [buf[off + klen + tlen:off + body]]
                off += body
            else:
                have = n - off
                need = body - have
                self._rem_res = arb.reserve(jid, need, "merge-read")
                rem = self.fp.read(need)
                self.job.bytes_read += need
                if len(rem) != need:
                    raise JobFailed(f"truncated run file {self.path}")
                remv = memoryview(rem)
                key_pieces = _field_pieces(buf, off, n, remv, 0, klen)
                token_pieces = _field_pieces(buf, off, n, remv, klen, tlen)
                vkey_pieces = _field_pieces(buf, off, n, remv, klen + tlen, vklen)
                off = n
            parsed.append((key_pieces, vkey_pieces, lineno, token_pieces))
        # materialise comparison keys once per chunk under one reservation
        total_klen = sum(
            sum(len(p) for p in r[0]) + sum(len(p) for p in r[1]) for r in parsed)
        self._key_res = arb.reserve(jid, total_klen, "merge-key")
        records = []
        for key_pieces, vkey_pieces, lineno, token_pieces in parsed:
            key = b"".join(key_pieces)
            vkey = b"".join(vkey_pieces)
            records.append((key, vkey, lineno, token_pieces))
        self._records = records
        self._live = len(records)
        self._pos = 0

    def next(self):
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
        self._records = []
        self._pos = 0
        self._live = 0
        for res in (self._chunk_res, self._key_res, self._rem_res):
            if res is not None:
                self.job.arbiter.release(res)
        self._chunk_res = self._key_res = self._rem_res = None
        self.job.arbiter.release(self._scratch_res)
        self.fp.close()


def _field_pieces(buf, off, n, remv, rel_start, length):
    """Split one record body field across the chunk tail and the remainder.

    ``rel_start`` is the field start relative to the record start at ``off``.
    Returns one or two memoryview pieces covering exactly ``length`` bytes."""
    start, end = off + rel_start, off + rel_start + length
    pieces = []
    if start < n:
        pieces.append(buf[start:min(end, n)])
    r0, r1 = max(0, start - n), end - n
    if r1 > 0:
        pieces.append(remv[r0:r1])
    return pieces


# ----------------------------------------------------------------- the job
class GroupQuantileJob:
    job_type = "group-quantiles"

    def __init__(self, runner, job_id: str, cfg: QuantileConfig):
        self.runner = runner
        self.job_id = job_id
        self.cfg = cfg
        self.arbiter = runner.arbiter
        self.events = runner.events

        # ---- derived sizes / minimal working set ----
        ml = cfg.max_line_bytes
        self.max_record = HDR.size + 4 * ml + ml + 2 * ml + 8
        self.max_output_row = 5 * ml + 256
        self.read_block = max(cfg.read_block_bytes, 2 * (ml + 2))
        self.write_buf = max(cfg.write_buf_bytes, self.max_record, self.max_output_row)
        self.merge_chunk = max(cfg.merge_chunk_bytes, self.max_record)
        read_mws = 9 * self.read_block + 2 * self.write_buf
        per_reader = 2 * self.merge_chunk + self.max_record + HDR.size
        # final merge writes two files at once, each with a dedicated buffer
        # pair (one resident, one possibly in flight): 4 write buffers
        merge_mws = (cfg.max_fan_in * per_reader + 4 * self.write_buf
                     + self.max_record)
        extract_mws = (per_reader + self.max_record + 2 * self.write_buf
                       + len(cfg.percentiles_bp) * ml + self.max_output_row)
        self.mws = max(read_mws, merge_mws, extract_mws)
        if self.mws > self.arbiter.budget:
            raise ConfigError(
                f"job {job_id}: minimal working set {self.mws}B exceeds budget "
                f"{self.arbiter.budget}B -- rejected at submit")

        # ---- state ----
        self.state = "PENDING"
        self.phase = "init"
        self.error: str | None = None
        self.lines_read = 0
        self.groups = 0
        self.spills = 0
        self.merge_passes = 0
        self.records_merged = 0
        self.results_written = 0
        self.bytes_read = 0
        self.bytes_written = 0
        self.cancel_event = threading.Event()
        self.fail_event = threading.Event()
        self.done_event = threading.Event()
        self._state_lock = threading.Lock()
        self.run_lock = threading.Lock()
        self._spill_lock = threading.Lock()
        self._run: list[tuple] = []          # (key, vkey, lineno, token)
        self._run_res: list[Reservation] = []
        self._revocable = 0
        self._spill_seq = 0
        self._max_run_records = 0
        self.runs: list[tuple[str, int]] = []

        self.tmpdir = os.path.join(runner.work_dir, f"job-{job_id}")
        os.makedirs(self.tmpdir, exist_ok=True)
        self.sorted_path = os.path.join(self.tmpdir, "sorted.run")
        self.index_path = os.path.join(self.tmpdir, "groups.idx")
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
            # keep the first (most specific, e.g. physical-line data error);
            # late teardown discards must not overwrite it with a generic msg
            if self.error is None:
                self.error = str(err)
        self.fail_event.set()
        self.arbiter.cancel_job(self.job_id)
        self.pool.notify_cancel()

    def _set_phase(self, phase: str):
        self.phase = phase
        self.events.emit("JOB_PHASE", self.job_id, phase=phase,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)

    # ------------------------------------------------------------------ main
    def run(self):
        try:
            self._parse_phase()
            self._check_cancel()
            self._merge_phase()
            self._check_cancel()
            if self.runs:
                self._extract_phase()
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
        self.events.emit("JOB_STATE", self.job_id, state=state, error=self.error,
                         lines_read=self.lines_read, groups=self.groups,
                         results_written=self.results_written,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)
        self.done_event.set()

    # ================================================================ PARSE
    def _parse_phase(self):
        self._set_phase("parse")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        arb.begin_phase(jid, self.mws)
        try:
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
                    parsed = self._parse_block(lines, lineno)
                    key_res, tok_res, vkey_res, records = parsed
                    with self.run_lock:
                        self._run.extend(records)
                        self._run_res.extend((block_res, key_res, tok_res, vkey_res))
                        self._revocable += (
                            sum(len(r[0]) + len(r[1]) + len(r[3]) for r in records)
                            + sum(len(x) for x in lines))
                        self._max_run_records = max(self._max_run_records, len(self._run))
                        arb.set_revocable(jid, self._revocable)
                    lineno += len(lines)
                    blocks += 1
                    self._hook("block_read", blocks=blocks)
                    cap = cfg.run_record_capacity
                    if cap and len(self._run) >= cap:
                        self._spill()
            self._spill()                  # final partial run
            self.writer.wait_idle()
            self._check_cancel()
        finally:
            self.pool.release_all()
            arb.end_phase(jid)

    def _read_block(self, f, lineno):
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        try:
            res = arb.reserve(jid, self.read_block, "line", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()
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
                raise QuantileInputError(
                    lineno + len(lines) + 1,
                    f"line exceeds max_line_bytes={limit}")
            lines.append(raw)
            total += len(raw)
        if not lines:
            return res, []
        arb.adjust(res, total)
        return res, lines

    def _parse_block(self, lines, base_lineno):
        """Validate every line and build (text key, decimal order key, lineno,
        value token) records.  All byte copies are reserved first."""
        arb, jid = self.arbiter, self.job_id
        texts: list[str] = []
        tokens: list[bytes] = []
        for i, raw in enumerate(lines):
            lineno = base_lineno + i + 1
            if not raw.strip():
                raise QuantileInputError(lineno, "empty line")
            try:
                obj = json.loads(raw, parse_int=_num_tag, parse_float=_num_tag,
                                 parse_constant=_reject_constant)
            except _ConstantRejected as exc:
                raise QuantileInputError(lineno, exc.args[0]) from None
            except Exception as exc:
                raise QuantileInputError(lineno, f"invalid JSON: {exc!r}") from None
            if not isinstance(obj, dict):
                raise QuantileInputError(lineno, "line is not a JSON object")
            text = obj.get("text")
            if not isinstance(text, str) or not text:
                raise QuantileInputError(
                    lineno, "missing, non-string or empty 'text' field")
            if "value" not in obj:
                raise QuantileInputError(lineno, "missing 'value' field")
            value = obj["value"]
            # finite numbers arrive tagged by the parse hooks; bool/null/str/
            # list/dict are never tagged and therefore rejected
            if not (isinstance(value, tuple) and len(value) == 2
                    and value[0] == "__gq_num__" and isinstance(value[1], str)):
                raise QuantileInputError(
                    lineno,
                    f"'value' must be a JSON number, got "
                    f"{type(value).__name__ if not isinstance(value, tuple) else 'value'}")
            lexeme = value[1]
            tok = lexeme.encode("ascii")      # JSON numbers are pure ASCII
            if not DECIMAL_RE.match(tok):
                raise QuantileInputError(lineno, f"unparsable number {lexeme!r}")
            texts.append(text)
            tokens.append(tok)

        # reservations before copies (4 bytes/char UTF-8 upper bound for text)
        text_bound = 4 * sum(len(t) for t in texts)
        tok_bound = sum(len(t) for t in tokens)
        # order key: sign byte + 2-byte lead + digits (<= token length) + terminator
        vkey_bound = sum(len(t) + 4 for t in tokens)
        try:
            key_res = arb.reserve(jid, text_bound, "key", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()
            key_res = arb.reserve(jid, text_bound, "key", revocable=True)
        try:
            tok_res = arb.reserve(jid, tok_bound, "value", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()
            tok_res = arb.reserve(jid, tok_bound, "value", revocable=True)
        try:
            vkey_res = arb.reserve(jid, vkey_bound, "vkey", revocable=True, timeout=0)
        except AllocFailed:
            self._spill()
            vkey_res = arb.reserve(jid, vkey_bound, "vkey", revocable=True)

        records = []
        key_total = tok_total = vkey_total = 0
        for i, (text, tok) in enumerate(zip(texts, tokens)):
            lineno = base_lineno + i + 1
            key = text.encode("utf-8")
            vkey = decimal_order_key(tok)
            key_total += len(key)
            tok_total += len(tok)
            vkey_total += len(vkey)
            records.append((key, vkey, lineno, tok))
        arb.adjust(key_res, key_total)
        arb.adjust(tok_res, tok_total)
        arb.adjust(vkey_res, vkey_total)
        self.lines_read += len(lines)
        return key_res, tok_res, vkey_res, records

    # ---------------------------------------------------------------- spill
    def _spill(self) -> int:
        """Sort the in-memory run by (text, value, lineno) and hand it to the
        writer.  The run's data reservations are released once serialisation
        completes; write buffers stay reserved (part of the envelope)."""
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
            run.sort(key=lambda r: (r[0], r[1], r[2]))
            seq = self._spill_seq
            self._spill_seq += 1
            tmp = os.path.join(self.tmpdir, f"run-{seq:06d}.tmp")
            final = os.path.join(self.tmpdir, f"run-{seq:06d}.run")
            self.events.emit("GQ_SPILL_START", self.job_id, records=len(run),
                             bytes_to_free=freed,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
            idx = self.writer.open_file(tmp, final,
                                        on_closed=lambda **kw: self._on_run_closed(seq, len(run), **kw))
            buf, buf_res = self.pool.acquire()
            off = 0
            try:
                for key, vkey, lineno, tok in run:
                    rec = HDR.size + len(key) + len(tok) + len(vkey)
                    if off + rec > len(buf):
                        self._hand_buffer(idx, buf, buf_res, off)
                        buf, buf_res = self.pool.acquire()
                        off = 0
                    HDR.pack_into(buf, off, len(key), len(tok), lineno, len(vkey))
                    off += HDR.size
                    buf[off:off + len(key)] = key
                    off += len(key)
                    buf[off:off + len(tok)] = tok
                    off += len(tok)
                    buf[off:off + len(vkey)] = vkey
                    off += len(vkey)
                if off:
                    self._hand_buffer(idx, buf, buf_res, off)
                    buf_res = None
            finally:
                if buf_res is not None:
                    self.pool.buffer_returned(buf_res)
            self.writer.close_file(idx)
            for res in run_res:
                self.arbiter.release(res)
            self.spills += 1
            self.events.emit("GQ_SPILL_DONE", self.job_id, records=len(run), freed=freed,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
            return freed

    def _hand_buffer(self, idx, buf, res, nbytes):
        self.bytes_written += nbytes
        self.writer.write_buffer(idx, buf, res, nbytes)

    def _reclaim_handler(self, nbytes: int) -> int:
        """Cooperative reclaim, invoked on another job's thread WITHOUT the
        arbiter lock.  Never calls reserve(); only spills parse-phase run data."""
        self._hook("reclaim_enter")
        try:
            if self.cancel_event.is_set() or self.fail_event.is_set():
                return 0
            if self.phase != "parse":
                return 0
            return self._spill()
        except (Cancelled, JobFailed):
            return 0
        finally:
            self._hook("reclaim_exit")

    def _on_run_closed(self, seq, nrecords, *, ok, error, tmp, final):
        with self._state_lock:
            alive = (self.state not in ("DONE", "FAILED", "CANCELLED")
                     and not self.cancel_event.is_set() and not self.fail_event.is_set())
        if ok and alive:
            self.runs.append((final, nrecords))
            self.events.emit("GQ_RUN_DONE", self.job_id, run=final, records=nrecords,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
        elif not ok and not self.cancel_event.is_set():
            self._fail(error or f"spill run {seq} was discarded")

    # ================================================================ MERGE
    def _merge_phase(self):
        self._set_phase("merge")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        arb.begin_phase(jid, self.mws)
        try:
            # intermediate passes use one pair; the final pass uses two
            # dedicated pairs (sorted run + group index), so keep four buffers
            for _ in range(4):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            try:
                runs = list(self.runs)
                if not runs:
                    self._publish_empty()
                    return
                fan_in = cfg.max_fan_in
                pass_no = 0
                while len(runs) > fan_in:
                    self._check_cancel()
                    self.events.emit("GQ_MERGE_PASS_START", self.job_id,
                                     pass_no=pass_no, runs=len(runs), fan_in=fan_in,
                                     total_reserved=arb.total_reserved, budget=arb.budget)
                    groups = [runs[i:i + fan_in] for i in range(0, len(runs), fan_in)]
                    new_runs = []
                    n_out_total = 0
                    for g in groups:
                        if len(g) == 1:
                            new_runs.append(g[0])
                        else:
                            new_runs.append(self._merge_group(g, pass_no))
                            n_out_total += new_runs[-1][1]
                    runs = new_runs
                    self.merge_passes += 1
                    self.events.emit("GQ_MERGE_PASS_DONE", self.job_id,
                                     pass_no=pass_no, runs=len(runs),
                                     records=n_out_total,
                                     total_reserved=arb.total_reserved, budget=arb.budget)
                    pass_no += 1
                n_records, n_groups = self._merge_final(runs)
                self.groups = n_groups
                self.writer.wait_idle()
                self._check_cancel()
                self.events.emit("GQ_FINAL_MERGE_DONE", self.job_id,
                                 records=n_records, groups=n_groups,
                                 total_reserved=arb.total_reserved, budget=arb.budget)
            finally:
                self.pool.release_all()
        finally:
            arb.end_phase(jid)

    def _merge_group(self, group, pass_no):
        seq = self._spill_seq
        self._spill_seq += 1
        tmp = os.path.join(self.tmpdir, f"merge-{pass_no}-{seq:06d}.tmp")
        final = os.path.join(self.tmpdir, f"merge-{pass_no}-{seq:06d}.run")
        idx = self.writer.open_file(tmp, final, on_closed=self._on_merge_run_closed)
        n_out = self._merge_runs(group, idx)
        self.writer.close_file(idx)
        self.writer.wait_idle()
        self._check_cancel()
        return (final, n_out)

    def _on_merge_run_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or "intermediate merge run discarded")

    def _merge_runs(self, group, writer_idx) -> int:
        """k-way merge of spill ``group`` into one writer file (run format).
        Returns the number of records written.  Heap order is
        (text key, decimal order key, lineno, reader index)."""
        import heapq
        readers = [QRunReader(self, path) for path, _ in group]
        buf, buf_res = self.pool.acquire()
        off = 0
        n_out = 0
        heap = []
        try:
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    key, vkey, lineno, token_pieces = rec
                    heapq.heappush(heap, (key, vkey, lineno, i, token_pieces))
            while heap:
                self._check_cancel()
                key, vkey, lineno, i, token_pieces = heapq.heappop(heap)
                r = readers[i]
                tlen = sum(len(p) for p in token_pieces)
                rec_len = HDR.size + len(key) + tlen + len(vkey)
                if off + rec_len > len(buf):
                    self._hand_buffer(writer_idx, buf, buf_res, off)
                    buf, buf_res = self.pool.acquire()
                    off = 0
                HDR.pack_into(buf, off, len(key), tlen, lineno, len(vkey))
                off += HDR.size
                buf[off:off + len(key)] = key
                off += len(key)
                for p in token_pieces:
                    buf[off:off + len(p)] = p
                    off += len(p)
                buf[off:off + len(vkey)] = vkey
                off += len(vkey)
                n_out += 1
                self.records_merged += 1
                r.release_record()
                rec = r.next()
                if rec is not None:
                    k2, vk2, ln2, tp2 = rec
                    heapq.heappush(heap, (k2, vk2, ln2, i, tp2))
            if off:
                self._hand_buffer(writer_idx, buf, buf_res, off)
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

    def _merge_final(self, runs):
        """Final k-way pass: write the globally sorted run and, in the same
        sweep, the per-group index (key, count).  Each output file owns a
        dedicated buffer pair so in-flight buffers are never reused for a
        different file (the writer returns them to the shared pool on done)."""
        sorted_tmp = self.sorted_path + ".tmp"
        index_tmp = self.index_path + ".tmp"
        s_idx = self.writer.open_file(sorted_tmp, self.sorted_path,
                                      on_closed=self._on_sorted_closed)
        i_idx = self.writer.open_file(index_tmp, self.index_path,
                                      on_closed=self._on_index_closed)
        readers = [QRunReader(self, path) for path, _ in runs]
        # dedicated pairs: acquire two, then never let either cross files
        sbuf, sres = self.pool.acquire()
        ibuf, ires = self.pool.acquire()
        soff = ioff = 0
        heap = []
        n_records = n_groups = 0
        cur_key = None
        cur_res = None
        cur_count = 0
        try:
            import heapq
            for i, r in enumerate(readers):
                rec = r.next()
                if rec is not None:
                    key, vkey, lineno, token_pieces = rec
                    heapq.heappush(heap, (key, vkey, lineno, i, token_pieces))
            while heap:
                self._check_cancel()
                key, vkey, lineno, i, token_pieces = heapq.heappop(heap)
                r = readers[i]
                # ---- group boundary -> index entry for the previous group
                if cur_key is None:
                    cur_res = self.arbiter.reserve(self.job_id, len(key), "merge-key")
                    cur_key = bytes(key)
                    cur_count = 1
                elif key != cur_key:
                    ibuf, ires, ioff = self._write_index(
                        i_idx, ibuf, ires, ioff, cur_key, cur_count)
                    self.arbiter.release(cur_res)
                    cur_res = self.arbiter.reserve(self.job_id, len(key), "merge-key")
                    cur_key = bytes(key)
                    cur_count = 1
                    n_groups += 1
                else:
                    cur_count += 1
                # ---- sorted-run record (dedicated sorted-file buffer only)
                tlen = sum(len(p) for p in token_pieces)
                rec_len = HDR.size + len(key) + tlen + len(vkey)
                if soff + rec_len > len(sbuf):
                    self._hand_buffer(s_idx, sbuf, sres, soff)
                    sbuf, sres = self.pool.acquire()
                    soff = 0
                HDR.pack_into(sbuf, soff, len(key), tlen, lineno, len(vkey))
                soff += HDR.size
                sbuf[soff:soff + len(key)] = key
                soff += len(key)
                for p in token_pieces:
                    sbuf[soff:soff + len(p)] = p
                    soff += len(p)
                sbuf[soff:soff + len(vkey)] = vkey
                soff += len(vkey)
                n_records += 1
                self.records_merged += 1
                r.release_record()
                rec = r.next()
                if rec is not None:
                    k2, vk2, ln2, tp2 = rec
                    heapq.heappush(heap, (k2, vk2, ln2, i, tp2))
            if cur_key is not None:
                ibuf, ires, ioff = self._write_index(
                    i_idx, ibuf, ires, ioff, cur_key, cur_count)
                n_groups += 1
            if soff:
                self._hand_buffer(s_idx, sbuf, sres, soff)
                sres = None
            if ioff:
                self._hand_buffer(i_idx, ibuf, ires, ioff)
                ires = None
        finally:
            if sres is not None:
                self.pool.buffer_returned(sres)
            if ires is not None:
                self.pool.buffer_returned(ires)
            if cur_res is not None:
                self.arbiter.release(cur_res)
            for r in readers:
                r.close()
        self.writer.close_file(s_idx)
        self.writer.close_file(i_idx)
        return n_records, n_groups

    def _write_index(self, idx, buf, res, off, key: bytes, count: int):
        """Append one index entry using this file's dedicated buffer pair."""
        entry = IHDR.size + len(key)
        if off + entry > len(buf):
            self._hand_buffer(idx, buf, res, off)
            buf, res = self.pool.acquire()
            off = 0
        IHDR.pack_into(buf, off, count, len(key))
        off += IHDR.size
        buf[off:off + len(key)] = key
        return buf, res, off + len(key)

    def _on_sorted_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or "sorted run discarded")

    def _on_index_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or "group index discarded")

    def _publish_empty(self):
        out_tmp = self.cfg.output_path + f".tmp.{self.job_id}"
        idx = self.writer.open_file(out_tmp, self.cfg.output_path,
                                    on_closed=self._on_output_closed)
        self.writer.close_file(idx)
        self.writer.wait_idle()
        self._check_cancel()
        self.groups = 0
        self.results_written = 0
        self._set_phase("publish")
        self.events.emit("GQ_OUTPUT_PUBLISH", self.job_id,
                         output=self.cfg.output_path, rows=0,
                         total_reserved=self.arbiter.total_reserved,
                         budget=self.arbiter.budget)

    # =============================================================== EXTRACT
    def _extract_phase(self):
        self._set_phase("extract")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        bps = cfg.percentiles_bp
        arb.begin_phase(jid, self.mws)
        out_tmp = cfg.output_path + f".tmp.{self.job_id}"
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            out_idx = self.writer.open_file(out_tmp, cfg.output_path,
                                            on_closed=self._on_output_closed)
            reader = QRunReader(self, self.sorted_path)
            index_fp = open(self.index_path, "rb")
            scratch = bytearray(IHDR.size)
            scratch_res = arb.reserve(jid, IHDR.size, "merge-read")
            buf, buf_res = self.pool.acquire()
            off = 0
            total_rows = 0
            try:
                while True:
                    self._check_cancel()
                    got = index_fp.readinto(scratch)
                    self.bytes_read += got
                    if got == 0:
                        break
                    if got != IHDR.size:
                        raise JobFailed("truncated group index")
                    n, klen = IHDR.unpack(scratch)
                    key_res = arb.reserve(jid, klen, "merge-key")
                    key = index_fp.read(klen)
                    self.bytes_read += klen
                    if len(key) != klen:
                        raise JobFailed("truncated group index key")
                    try:
                        # rank -> list of bps hitting that rank (bp order kept)
                        bp_rank: dict[int, int] = {}
                        rank_bps: dict[int, list[int]] = {}
                        for bp in bps:
                            rank = max(1, (bp * n + 9999) // 10000)
                            bp_rank[bp] = rank
                            rank_bps.setdefault(rank, []).append(bp)
                        wanted = set(rank_bps)
                        # Stream the group in (value, lineno) order one record
                        # at a time, tracking the current equal-value run.  A
                        # target rank landing on a value resolves to the smallest
                        # physical line in that value's run.  One token
                        # reservation per value run is held while the run is
                        # open and released when the run closes; at most one
                        # token per distinct value run is ever resident.
                        captured: dict[int, tuple] = {}   # rank -> (lineno, token)
                        run_line: int | None = None       # min line in run
                        run_token: bytes | None = None
                        run_tres: Reservation | None = None
                        run_wanted: list[int] = []

                        def _close_run():
                            nonlocal run_tres
                            if run_wanted:
                                for rk in run_wanted:
                                    captured[rk] = (run_line, bytes(run_token))
                            if run_tres is not None:
                                arb.release(run_tres)
                                run_tres = None

                        pos = 0
                        prev_vkey = None
                        while pos < n:
                            rec = reader.next()
                            if rec is None:
                                raise JobFailed("sorted run ended before group completed")
                            _k, vkey, lineno, token_pieces = rec
                            pos += 1
                            if prev_vkey is not None and vkey != prev_vkey:
                                _close_run()
                                run_line = run_token = None
                                run_wanted = []
                            if run_tres is None:
                                run_line = lineno
                                tok = b"".join(token_pieces)
                                run_tres = arb.reserve(jid, len(tok), "value")
                                run_token = bytes(tok)
                            elif lineno < run_line:
                                # records in a run are lineno-ascending; keep min
                                run_line = lineno
                            if pos in wanted:
                                run_wanted.append(pos)
                            prev_vkey = vkey
                            reader.release_record()
                        _close_run()
                        text = key.decode("utf-8")
                        for bp in bps:
                            rank = bp_rank[bp]
                            lineno, tok = captured[rank]
                            # rendered row is transient Python metadata; the
                            # copy below goes into the reserved write buffer
                            row = self._render_row(text, bp, n, rank, lineno, tok)
                            if off + len(row) > len(buf):
                                self._hand_buffer(out_idx, buf, buf_res, off)
                                buf, buf_res = self.pool.acquire()
                                off = 0
                            buf[off:off + len(row)] = row
                            off += len(row)
                            total_rows += 1
                        self.events.emit("GQ_GROUP_DONE", self.job_id, n=n,
                                         rows=len(bps),
                                         total_reserved=arb.total_reserved,
                                         budget=arb.budget)
                        self._hook("group_done", n=n)
                    finally:
                        arb.release(key_res)
                tail = reader.next()
                if tail is not None:
                    raise JobFailed("sorted run has records beyond the group index")
                if off:
                    self._hand_buffer(out_idx, buf, buf_res, off)
                    buf_res = None
            finally:
                if buf_res is not None:
                    self.pool.buffer_returned(buf_res)
                index_fp.close()
                arb.release(scratch_res)
                reader.close()
            self.writer.close_file(out_idx)
            # drain so the atomic rename completes (and any write failure is
            # surfaced) before the phase ends and _finish() stops the writer
            self.writer.wait_idle()
            self._check_cancel()
            self.results_written = total_rows
            self._set_phase("publish")
            self.events.emit("GQ_OUTPUT_PUBLISH", self.job_id,
                             output=cfg.output_path, rows=total_rows,
                             total_reserved=arb.total_reserved, budget=arb.budget)
        finally:
            self.pool.release_all()
            arb.end_phase(jid)

    def _render_row(self, text: str, bp: int, n: int, rank: int,
                    lineno: int, tok: bytes) -> bytes:
        obj = {
            "text": text,
            "bp": bp,
            "n": n,
            "rank": rank,
            "source_line": lineno,
            "value_token": tok.decode("ascii"),
        }
        return (json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
                + "\n").encode("utf-8")

    def _on_output_closed(self, *, ok, error, tmp, final):
        if ok and not self.cancel_event.is_set() and not self.fail_event.is_set():
            self.events.emit("OUTPUT_PUBLISH", self.job_id, output=final,
                             total_reserved=self.arbiter.total_reserved,
                             budget=self.arbiter.budget)
        elif not ok and not self.cancel_event.is_set():
            self._fail(error or "output discarded")

    # ---------------------------------------------------------------- report
    def metadata_estimate(self) -> int:
        """Fixed Python object overhead NOT covered by B (estimate)."""
        per_record = 8 + 64 + 49 + 49 + 49   # list slot + tuple + 4 bytes headers
        return self._max_run_records * per_record + len(self.runs) * 256

    def status(self) -> dict:
        with self._state_lock:
            return {
                "job_id": self.job_id,
                "job_type": self.job_type,
                "state": self.state,
                "phase": self.phase,
                "error": self.error,
                "percentiles_bp": list(self.cfg.percentiles_bp),
                "lines_read": self.lines_read,
                "groups": self.groups,
                "spills": self.spills,
                "merge_passes": self.merge_passes,
                "records_merged": self.records_merged,
                "results_written": self.results_written,
                "bytes_read": self.bytes_read,
                "bytes_written": self.bytes_written,
                "held_bytes": self.arbiter.held(self.job_id),
                "mws": self.mws,
                "budget_high_water": self.arbiter.high_water,
                "input": self.cfg.input_path,
                "output": self.cfg.output_path,
            }
