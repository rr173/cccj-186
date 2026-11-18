"""Cross-process resumable external sort/dedup with persistent progress.

This module adds a sort job whose progress survives the process dying
(SIGKILL included).  The operator points a *new* Python process at the same
state directory and the job continues from the last durable checkpoint --
without re-reading, re-parsing or re-sorting any confirmed prefix of the
source, and without re-executing any confirmed merge group.

State directory layout (private workspace of exactly one task; survives
crashes; never mixes with the input or the target file)::

    state_dir/
      LOCK            flock(LOCK_EX|LOCK_NB) holder file.  The kernel releases
                      the lock when the holder dies, so liveness is never
                      inferred from PIDs or timeouts.  Content is diagnostic.
      task.json       written once at creation: task_id, input binding
                      (canonical path, dev, ino, size, mtime_ns, ctime_ns),
                      observed output identity (or its absence), and the sort
                      semantics (dedup_key, max_line_bytes).  Immutable.
      progress.json   the durable state machine (see below); every update is
                      write-tmp -> fsync(file) -> os.replace -> fsync(dir).
      runs/           confirmed and in-flight run files
                      (run-NNNNNN.run / merge-NNNNNN.run, written via
                      .tmp -> rename with the file fsync'd before the rename).

progress.json fields::

    version, generation          -- generation increments at every checkpoint
    phase                        -- read | merge | publish | done
    stopped                      -- null, or {reason: cancelled|failed|io_error,
                                    recoverable, error, attempt}
    read                         -- {confirmed_offset, next_lineno, eof}
    runs                         -- confirmed runs in merge-queue order:
                                    [{name, records, bytes, sha256}]
    counters                     -- {run_seq, merge_seq} (monotonic across
                                    attempts -> file names never collide)
    publish                      -- null or {tmp, final, bytes, sha256, lines}
    stats                        -- totals + per-attempt work records

Durability / recovery protocol:

  * READ phase: sorted runs are spilled to runs/ (tmp -> fsync -> rename).
    A checkpoint (a) spills the in-memory run, (b) waits for the writer,
    (c) fsyncs runs/, then (d) persists {confirmed_offset, next_lineno} and
    the full confirmed run list.  Checkpoints are triggered by
    ``checkpoint_input_bytes``: once the unconfirmed prefix exceeds the
    threshold, at most one more valid line is read before the checkpoint
    starts, and nothing further is read until the checkpoint is durable.
    Runs spilled but not yet covered by a checkpoint are *unconfirmed*:
    after a crash they are deleted as orphans and that tail is re-read
    (bounded by one checkpoint interval).  Confirmed prefixes are never
    re-read: the source is opened with an explicit seek to confirmed_offset.

  * MERGE phase: one group = merge the first ``max_fan_in`` confirmed runs
    into merge-NNNNNN.run (tmp -> fsync -> rename), fsync runs/, persist the
    new run list (inputs removed, output appended), *then* reclaim the input
    files.  A crash between persist and reclaim leaves orphans that the next
    resume deletes; a crash before the persist leaves a partial/orphan output
    that is deleted and the group re-executed (bounded redo: <= 1 group).
    Confirmed groups are never re-executed.

  * PUBLISH: the final merge writes ``<output>.tmp.<task_id>.part`` ->
    rename to ``<output>.tmp.<task_id>`` (fsync'd), then progress records
    phase=publish with the staged size+sha256, then the target is re-checked
    against the identity observed at creation, then a single os.replace
    publishes, the parent dir is fsync'd, and phase=done is persisted.
    Recovery distinguishes:
      - target already holds our exact result (hash match)  -> record done;
      - target still the observed old file and staged tmp ok -> rename again;
      - staged tmp lost/corrupt                            -> redo final merge;
      - target is something else entirely                  -> PublishConflict,
        leave everything untouched.

  * Terminal states: cancel and data errors persist ``stopped`` (not
    recoverable) and are never revived; a transient OSError persists
    ``stopped={reason: io_error, recoverable: true}`` and the next resume
    continues from the last confirmed boundary.  A crashed process simply
    leaves no ``stopped`` marker, so it is resumable.

  * Reclaim policy: DONE and CANCELLED delete run files and the staged
    output tmp (state_dir keeps LOCK/task.json/progress.json/events as the
    durable record; the whole directory may be deleted by the operator).
    FAILED keeps runs for diagnosis.  ERROR (recoverable) keeps everything.

Memory accounting: run files are written through the same reserved buffer
pool / writer thread as SortJob (kinds buffer/io), merge reads use the same
RunReader reservations (merge-read/merge-key), and the standing STATE_BUF
used for state encoding and checksum verification is reserved from the
arbiter as kind "state".  Nothing ever loads the whole input or all runs
into memory.  In-memory reservations are per-process and are (obviously)
never persisted; the durable state only references on-disk artifacts.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import threading
import time
import uuid

from .arbiter import AllocFailed, Cancelled, ConfigError
from .job import (HDR, BufferPool, InvalidLine, JobConfig, JobFailed,
                  LineTooLong, RunReader)

FORMAT_VERSION = 1
STATE_BUF = 64 * 1024          # standing reservation: state encode + hash I/O
DEFAULT_CHECKPOINT_BYTES = 64 * 1024 * 1024


# --------------------------------------------------------------------- errors
class ResumeError(JobFailed):
    """Base class for resumable-sort control errors (refusals)."""


class StateLocked(ResumeError):
    """The state directory is currently held by another live process."""


class StateCorrupt(ResumeError):
    """Persistent state or a confirmed run is damaged; refusing to proceed."""


class ResumeRefused(ResumeError):
    """The stored task binding does not match reality (input replaced,
    semantics changed, ...).  Raised before anything is touched."""


class PublishConflict(ResumeError):
    """The target file no longer matches what this task observed."""


# ------------------------------------------------------------------ fs helpers
def _fsync_dir(path: str):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_json(path: str, obj):
    """Durable replace: tmp file -> fsync -> rename -> fsync(dir)."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path) or ".")


def _read_json(path: str, what: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except FileNotFoundError:
        raise StateCorrupt(f"{what} missing: {path}")
    except (OSError, ValueError) as exc:
        raise StateCorrupt(f"{what} unreadable ({path}): {exc!r}")
    if not isinstance(obj, dict) or obj.get("version") != FORMAT_VERSION:
        raise StateCorrupt(f"{what} has unsupported format/version: {path}")
    return obj


def _ident(st) -> dict:
    return {"dev": st.st_dev, "ino": st.st_ino, "size": st.st_size,
            "mtime_ns": st.st_mtime_ns, "ctime_ns": st.st_ctime_ns}


def _ident_diff(want: dict, got: dict) -> str | None:
    for k in ("dev", "ino", "size", "mtime_ns", "ctime_ns"):
        if want.get(k) != got.get(k):
            return k
    return None


def _sha256_file(path: str, buf: bytearray) -> tuple[str, int]:
    """Stream a file through a hash using the reserved buffer ``buf``."""
    h = hashlib.sha256()
    total = 0
    with open(path, "rb") as f:
        while True:
            n = f.readinto(buf)
            if not n:
                break
            h.update(memoryview(buf)[:n])
            total += n
    return h.hexdigest(), total


def _acquire_lock(state_dir: str, task_id: str) -> int:
    """Exclusive, non-blocking flock on the state dir.  The kernel releases
    it when the holder process dies (SIGKILL included) -- no PID files, no
    timeouts.  Returns the open fd (closing it releases the lock)."""
    path = os.path.join(state_dir, "LOCK")
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise StateLocked(
            f"state dir is held by another live process: {state_dir}")
    try:
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({"pid": os.getpid(), "task_id": task_id,
                                 "host": os.uname().nodename,
                                 "ts": time.time()}).encode() + b"\n")
    except OSError:
        pass  # diagnostic content only; the lock itself is what matters
    return fd


# ---------------------------------------------------------------------- config
class ResumableConfig(JobConfig):
    """JobConfig plus the checkpoint threshold.  Semantics bound to the task
    (recorded in task.json, verified on resume): dedup_key, max_line_bytes.
    Tunables (may differ per attempt): read/write/merge sizes, fan-in,
    checkpoint_input_bytes, and the runner's total budget."""

    def __init__(self, input_path, output_path, *,
                 checkpoint_input_bytes=DEFAULT_CHECKPOINT_BYTES, **kw):
        super().__init__(input_path, output_path, **kw)
        c = int(checkpoint_input_bytes)
        self.checkpoint_input_bytes = c if c > 0 else DEFAULT_CHECKPOINT_BYTES


def derive_sizes(cfg: ResumableConfig) -> dict:
    """Mirror of SortJob's derived sizes, plus the STATE_BUF slack that
    covers state encoding / checksum verification inside the envelope."""
    ml = cfg.max_line_bytes
    max_record = HDR.size + 5 * ml
    read_block = max(cfg.read_block_bytes, 2 * (ml + 2))
    write_buf = max(cfg.write_buf_bytes, max_record)
    merge_chunk = max(cfg.merge_chunk_bytes, max_record)
    read_mws = read_block + 4 * read_block + 2 * write_buf
    per_reader = 2 * merge_chunk + max_record + HDR.size
    merge_mws = cfg.max_fan_in * per_reader + 2 * write_buf
    mws = max(read_mws, merge_mws) + 2 * STATE_BUF
    return {"max_record": max_record, "read_block": read_block,
            "write_buf": write_buf, "merge_chunk": merge_chunk, "mws": mws}


def _validate_progress(p: dict):
    """Structural validation of progress.json (raises StateCorrupt)."""
    if not isinstance(p.get("generation"), int) or p["generation"] < 0:
        raise StateCorrupt("progress.json: bad generation")
    if p.get("phase") not in ("read", "merge", "publish", "done"):
        raise StateCorrupt(f"progress.json: bad phase {p.get('phase')!r}")
    rd = p.get("read")
    if not (isinstance(rd, dict) and isinstance(rd.get("confirmed_offset"), int)
            and isinstance(rd.get("next_lineno"), int)
            and isinstance(rd.get("eof"), bool)):
        raise StateCorrupt("progress.json: bad read section")
    runs = p.get("runs")
    if not isinstance(runs, list):
        raise StateCorrupt("progress.json: bad runs section")
    seen = set()
    for r in runs:
        if not (isinstance(r, dict) and isinstance(r.get("name"), str)
                and isinstance(r.get("records"), int)
                and isinstance(r.get("bytes"), int)
                and isinstance(r.get("sha256"), str)):
            raise StateCorrupt(f"progress.json: bad run entry {r!r}")
        if os.path.basename(r["name"]) != r["name"] or r["name"] in seen:
            raise StateCorrupt(f"progress.json: bad run name {r['name']!r}")
        seen.add(r["name"])
    c = p.get("counters")
    if not (isinstance(c, dict) and isinstance(c.get("run_seq"), int)
            and isinstance(c.get("merge_seq"), int)):
        raise StateCorrupt("progress.json: bad counters")
    if not isinstance(p.get("attempts"), int) or p["attempts"] < 1:
        raise StateCorrupt("progress.json: bad attempts")
    st = p.get("stopped")
    if st is not None and not (isinstance(st, dict)
                               and st.get("reason") in ("cancelled", "failed", "io_error")):
        raise StateCorrupt("progress.json: bad stopped marker")
    if p.get("publish") is not None:
        pub = p["publish"]
        if not (isinstance(pub, dict) and isinstance(pub.get("tmp"), str)
                and isinstance(pub.get("final"), str)
                and isinstance(pub.get("bytes"), int)
                and isinstance(pub.get("sha256"), str)):
            raise StateCorrupt("progress.json: bad publish section")
    if not isinstance(p.get("stats"), dict):
        raise StateCorrupt("progress.json: bad stats section")


def _initial_progress() -> dict:
    return {
        "version": FORMAT_VERSION,
        "generation": 0,
        "attempts": 1,
        "phase": "read",
        "stopped": None,
        "read": {"confirmed_offset": 0, "next_lineno": 0, "eof": False},
        "runs": [],
        "counters": {"run_seq": 0, "merge_seq": 0},
        "publish": None,
        "stats": {
            "totals": {"source_bytes_read": 0, "lines_read": 0, "runs_spilled": 0,
                       "merge_groups_executed": 0, "merge_records_in": 0,
                       "merge_bytes_in": 0, "final_merges_executed": 0},
            "per_attempt": [{"attempt": 1, "finished": False, "reused_runs": 0,
                             "source_bytes_read": 0, "lines_read": 0,
                             "runs_spilled": 0, "merge_groups_executed": 0}],
        },
    }


def inspect_state_dir(state_dir: str, verify_runs: bool = False) -> dict:
    """Read-only diagnostic view of a state dir (used by the CLI).  Never
    mutates anything; reports whether another process holds the lock."""
    task = _read_json(os.path.join(state_dir, "task.json"), "task.json")
    progress = _read_json(os.path.join(state_dir, "progress.json"), "progress.json")
    _validate_progress(progress)
    locked = False
    try:
        fd = _acquire_lock(state_dir, task.get("task_id", "?"))
    except StateLocked:
        locked = True
    else:
        os.close(fd)
    runs = progress["runs"]
    info = {
        "ok": True,
        "state_dir": state_dir,
        "task_id": task.get("task_id"),
        "input": task["input"]["path"],
        "output": task["output"]["path"],
        "semantics": task.get("semantics"),
        "phase": progress["phase"],
        "generation": progress["generation"],
        "attempts": progress["attempts"],
        "recoveries": progress["attempts"] - 1,
        "locked_by_other_process": locked,
        "stopped": progress.get("stopped"),
        "confirmed_offset": progress["read"]["confirmed_offset"],
        "next_lineno": progress["read"]["next_lineno"],
        "input_size": task["input"]["size"],
        "runs": {"count": len(runs), "bytes": sum(r["bytes"] for r in runs)},
        "publish": progress.get("publish"),
        "stats": progress.get("stats"),
    }
    if verify_runs:
        stopped = progress.get("stopped") or {}
        if progress["phase"] == "done" or stopped.get("reason") == "cancelled":
            # run files are intentionally reclaimed on DONE/CANCELLED
            info["runs"]["verify"] = "reclaimed"
        else:
            buf = bytearray(STATE_BUF)
            bad = []
            for r in runs:
                path = os.path.join(state_dir, "runs", r["name"])
                try:
                    sha, nbytes = _sha256_file(path, buf)
                except OSError as exc:
                    bad.append({"name": r["name"], "error": repr(exc)})
                    continue
                if nbytes != r["bytes"] or sha != r["sha256"]:
                    bad.append({"name": r["name"],
                                "want": (r["bytes"], r["sha256"]),
                                "got": (nbytes, sha)})
            info["runs"]["verify"] = "ok" if not bad else "CORRUPT"
            if bad:
                info["runs"]["bad"] = bad
                info["ok"] = False
    return info


# ---------------------------------------------------------------------- the job
class ResumableSortJob:
    """Sort/dedup job whose every checkpoint is durable in ``state_dir``.

    Created via :meth:`create` (new task) or :meth:`resume` (continue an
    existing state dir); both classmethods raise *before* touching anything
    when the binding does not match.  Managed through the runner's usual
    status/cancel/wait API.
    """

    # ------------------------------------------------------------ construction
    def __init__(self, runner, job_id: str, cfg: ResumableConfig,
                 state_dir: str, task: dict, progress: dict,
                 lock_fd: int, attempt: int, fresh: bool):
        self.runner = runner
        self.job_id = job_id
        self.cfg = cfg
        self.state_dir = state_dir
        self.runs_dir = os.path.join(state_dir, "runs")
        self.task = task
        self.task_id = task["task_id"]
        self.progress = progress
        self._lock_fd = lock_fd
        self.attempt = attempt
        self.fresh = fresh
        self.arbiter = runner.arbiter
        self.events = runner.events

        sizes = derive_sizes(cfg)
        self.max_record = sizes["max_record"]
        self.read_block = sizes["read_block"]
        self.write_buf = sizes["write_buf"]
        self.merge_chunk = sizes["merge_chunk"]
        self.mws = sizes["mws"]
        if self.mws > self.arbiter.budget:
            raise ConfigError(
                f"job {job_id}: minimal working set {self.mws}B exceeds budget "
                f"{self.arbiter.budget}B -- rejected")

        # ---- durable state mirrored in memory ----
        self.confirmed_offset = progress["read"]["confirmed_offset"]
        self.next_lineno = progress["read"]["next_lineno"]
        self.runs_confirmed = [dict(r) for r in progress["runs"]]
        self._run_seq = progress["counters"]["run_seq"]
        self._merge_seq = progress["counters"]["merge_seq"]
        self._pending_runs: list[dict] = []     # spilled, not yet checkpointed

        # ---- per-attempt work counters (never hidden: redone work counts) ----
        self.lines_read = 0
        self.lines_written = 0
        self.spills = 0
        self.source_bytes_read = 0
        self._blocks_read = 0
        self.merge_groups_executed = 0
        self.merge_records_in = 0
        self.merge_bytes_in = 0
        self.reused_runs = 0 if fresh else len(self.runs_confirmed)
        self._tot = dict(progress["stats"]["totals"])
        self._attempt_stat = progress["stats"]["per_attempt"][-1]

        # ---- lifecycle ----
        self.state = "PENDING"
        self.phase = "init"
        self.error: str | None = None
        self._exc: BaseException | None = None
        # set when the durable record already says "done": a failure from
        # that point on (e.g. the published target was tampered with) is
        # reported but must not rewrite the terminal record
        self._done_record_intact = False
        self._stopped_cleared = False
        # set once THIS attempt has performed the atomic publish: a cancel
        # or error landing afterwards is past the point of no return
        self._published_this_attempt = False
        self.cancel_event = threading.Event()
        self.fail_event = threading.Event()
        self.done_event = threading.Event()
        self._state_lock = threading.Lock()
        self._suspended = False

        # ---- in-memory run accumulation (same accounting as SortJob) ----
        self.run_lock = threading.Lock()
        self._run: list[tuple[bytes, int, bytes]] = []
        self._run_res: list = []
        self._revocable = 0
        self._spill_lock = threading.Lock()
        self._max_run_records = 0

        self._progress_path = os.path.join(state_dir, "progress.json")
        self.writer = runner.engine.new_writer(job_id)
        self.pool = BufferPool(self)
        runner.engine.buffer_pools[job_id] = self.pool
        if cfg.inject_io_failure:
            runner.engine.fail_first_write_for = job_id
        self.arbiter.register_job(job_id, self.mws,
                                  reclaim_handler=self._reclaim_handler)

    # ---------------------------------------------------------- create/resume
    @staticmethod
    def _check_state_dir_placement(state_dir: str, real_in: str, real_out: str):
        real_sd = os.path.realpath(state_dir)
        if real_sd in (real_in, real_out,
                       os.path.dirname(real_in), os.path.dirname(real_out)):
            raise ConfigError(
                f"state_dir must be a dedicated directory, not the "
                f"input/output location: {state_dir}")

    @classmethod
    def create(cls, runner, cfg: ResumableConfig, state_dir: str, task_id: str):
        """Bind a new task.  Raises ConfigError/ResumeError before creating
        anything when the placement or config is invalid."""
        real_in = os.path.realpath(cfg.input_path)
        real_out = os.path.realpath(cfg.output_path)
        if real_in == real_out:
            raise ConfigError("input and output must be different files")
        cls._check_state_dir_placement(state_dir, real_in, real_out)
        sizes = derive_sizes(cfg)
        if sizes["mws"] > runner.arbiter.budget:
            raise ConfigError(
                f"minimal working set {sizes['mws']}B exceeds budget "
                f"{runner.arbiter.budget}B -- rejected at submit")
        try:
            istat = os.stat(real_in)
        except OSError as exc:
            raise ConfigError(f"input not readable: {real_in}: {exc}")
        if not os.path.isdir(os.path.dirname(real_out) or "."):
            raise ConfigError(f"output directory does not exist: {real_out}")
        cfg.input_path = real_in
        cfg.output_path = real_out

        state_dir = os.path.abspath(state_dir)
        os.makedirs(state_dir, exist_ok=True)
        # the only pre-existing entry tolerated is this runner's own event
        # log (created when the runner was constructed); anything else means
        # the directory is not a fresh workspace
        allowed = set()
        ev_path = getattr(runner.events, "path", None)
        if ev_path and os.path.dirname(os.path.abspath(ev_path)) == state_dir:
            allowed.add(os.path.basename(ev_path))
        stray = set(os.listdir(state_dir)) - allowed
        if stray:
            raise ResumeError(
                f"state dir must not exist or be empty on create: {state_dir} "
                f"(contains {sorted(stray)[:4]})")
        lock_fd = _acquire_lock(state_dir, task_id)
        try:
            os.makedirs(os.path.join(state_dir, "runs"))
            try:
                observed = dict(_ident(os.stat(real_out)))
                observed["exists"] = True
            except FileNotFoundError:
                observed = {"exists": False}
            task = {
                "version": FORMAT_VERSION,
                "task_id": task_id,
                "created_ts": time.time(),
                "input": dict(_ident(istat), path=real_in),
                "output": {"path": real_out, "observed": observed},
                "semantics": {"dedup_key": cfg.dedup_key,
                              "max_line_bytes": cfg.max_line_bytes},
                "tunables": {"read_block_bytes": cfg.read_block_bytes,
                             "write_buf_bytes": cfg.write_buf_bytes,
                             "merge_chunk_bytes": cfg.merge_chunk_bytes,
                             "max_fan_in": cfg.max_fan_in,
                             "checkpoint_input_bytes": cfg.checkpoint_input_bytes},
            }
            _atomic_write_json(os.path.join(state_dir, "task.json"), task)
            progress = _initial_progress()
            _atomic_write_json(os.path.join(state_dir, "progress.json"), progress)
            _fsync_dir(state_dir)   # make the runs/ dir itself durable
        except BaseException:
            os.close(lock_fd)
            raise
        job_id = f"{task_id}-a1"
        job = cls(runner, job_id, cfg, state_dir, task, progress,
                  lock_fd, attempt=1, fresh=True)
        runner.events.emit(
            "RSORT_TASK_BOUND", job_id, task_id=task_id, input=real_in,
            input_ident={k: task["input"][k] for k in
                         ("dev", "ino", "size", "mtime_ns", "ctime_ns")},
            output=real_out, output_observed=task["output"]["observed"],
            dedup_key=cfg.dedup_key, max_line_bytes=cfg.max_line_bytes,
            checkpoint_input_bytes=cfg.checkpoint_input_bytes,
            total_reserved=runner.arbiter.total_reserved,
            budget=runner.arbiter.budget)
        return job

    @classmethod
    def resume(cls, runner, state_dir: str, overrides: dict):
        """Bind an existing task.  All refusals happen before any mutation:
        config/semantics -> lock -> input identity -> progress validity."""
        state_dir = os.path.abspath(state_dir)
        task = _read_json(os.path.join(state_dir, "task.json"), "task.json")
        for k in ("input", "output", "semantics", "task_id"):
            if k not in task:
                raise StateCorrupt(f"task.json: missing {k}")
        sem = task["semantics"]
        # explicit semantics-change refusal (before anything is touched)
        if overrides.get("dedup_key") is not None and \
                bool(overrides["dedup_key"]) != bool(sem["dedup_key"]):
            raise ConfigError(
                f"cannot change dedup_key on resume: bound={sem['dedup_key']} "
                f"got={overrides['dedup_key']}")
        if overrides.get("max_line_bytes") is not None and \
                int(overrides["max_line_bytes"]) != int(sem["max_line_bytes"]):
            raise ConfigError(
                f"cannot change max_line_bytes on resume: bound={sem['max_line_bytes']} "
                f"got={overrides['max_line_bytes']}")
        tun = dict(task.get("tunables") or {})
        for k in ("read_block_bytes", "write_buf_bytes", "merge_chunk_bytes",
                  "max_fan_in", "checkpoint_input_bytes"):
            if overrides.get(k) is not None:
                tun[k] = int(overrides[k])
        cfg = ResumableConfig(
            task["input"]["path"], task["output"]["path"],
            dedup_key=sem["dedup_key"], max_line_bytes=sem["max_line_bytes"],
            hooks=overrides.get("hooks"),
            inject_io_failure=bool(overrides.get("inject_io_failure")),
            **tun)
        sizes = derive_sizes(cfg)
        if sizes["mws"] > runner.arbiter.budget:
            raise ConfigError(
                f"minimal working set {sizes['mws']}B exceeds budget "
                f"{runner.arbiter.budget}B -- resume rejected (progress untouched)")

        lock_fd = _acquire_lock(state_dir, task["task_id"])
        try:
            # identity of the source: replaced / appended / truncated /
            # metadata-touched inputs are all refused before any mutation
            try:
                st = os.stat(task["input"]["path"])
            except OSError as exc:
                raise ResumeRefused(
                    f"input missing or unreadable: {task['input']['path']}: {exc}")
            diff = _ident_diff(task["input"], _ident(st))
            if diff is not None:
                raise ResumeRefused(
                    f"input file changed since task creation ({diff}): "
                    f"bound={ {k: task['input'][k] for k in ('dev','ino','size','mtime_ns','ctime_ns')} } "
                    f"now={_ident(st)}")
            progress = _read_json(os.path.join(state_dir, "progress.json"),
                                  "progress.json")
            _validate_progress(progress)
            progress["attempts"] += 1
            attempt = progress["attempts"]
            progress["stats"]["per_attempt"].append({
                "attempt": attempt, "finished": False,
                "reused_runs": len(progress["runs"]),
                "source_bytes_read": 0, "lines_read": 0, "runs_spilled": 0,
                "merge_groups_executed": 0})
            _atomic_write_json(os.path.join(state_dir, "progress.json"), progress)
        except BaseException:
            os.close(lock_fd)
            raise
        job_id = f"{task['task_id']}-a{attempt}"
        return cls(runner, job_id, cfg, state_dir, task, progress,
                   lock_fd, attempt=attempt, fresh=False)

    # ------------------------------------------------------------------ util
    def _ev(self, kind: str, **fields):
        fields.setdefault("total_reserved", self.arbiter.total_reserved)
        fields.setdefault("budget", self.arbiter.budget)
        fields.setdefault("attempt", self.attempt)
        self.events.emit(kind, self.job_id, **fields)

    def _hook(self, name: str, **kw):
        fn = self.cfg.hooks.get(name)
        if fn is not None:
            fn(self, **kw)

    def _check_cancel(self):
        if self.cancel_event.is_set():
            raise Cancelled()
        if self.fail_event.is_set():
            if self._exc is not None:
                raise self._exc
            raise JobFailed(self.error or "I/O failure")

    def cancel(self):
        with self._state_lock:
            if self.state in ("DONE", "FAILED", "CANCELLED"):
                return
            if self.state == "ERROR":
                # worker already exited; persist the abandonment ourselves
                if not self._suspended:
                    self.error = self.error or "cancelled after recoverable error"
                    try:
                        self._persist_stopped("cancelled", recoverable=False,
                                              bounded=True)
                        self._delete_run_files()
                        self._delete_output_tmps()
                    except OSError as exc:
                        self._ev("RSORT_STATE_PERSIST_FAIL", error=repr(exc))
                self.state = "CANCELLED"
                return
        self.cancel_event.set()
        self.arbiter.cancel_job(self.job_id)
        self.writer.cancel()
        self.pool.notify_cancel()

    def suspend(self):
        """Graceful-stop equivalent of a crash: unwind without recording a
        terminal state, so the task stays resumable (used by shutdown())."""
        self._suspended = True
        self.cancel()

    def _fail(self, err):
        with self._state_lock:
            if self.state in ("DONE", "FAILED", "CANCELLED", "ERROR"):
                return
            if isinstance(err, BaseException):
                self._exc = err
                self.error = f"{type(err).__name__}: {err}"
            else:
                self.error = str(err)
        self.fail_event.set()
        self.arbiter.cancel_job(self.job_id)
        self.pool.notify_cancel()

    def _set_phase(self, phase: str):
        self.phase = phase
        self._ev("JOB_PHASE", phase=phase)

    def _release_lock(self):
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

    def abort_construction(self):
        """Undo a fully constructed but never-started job (e.g. duplicate
        job_id at registration): stop the writer, release reservations and
        the state-dir lock.  The on-disk task itself is untouched."""
        self.writer.cancel()
        self.writer.join()
        self.arbiter.release_all(self.job_id)
        self.arbiter.unregister_job(self.job_id)
        self.runner.engine.buffer_pools.pop(self.job_id, None)
        if self.runner.engine.fail_first_write_for == self.job_id:
            self.runner.engine.fail_first_write_for = None
        self._release_lock()

    # ------------------------------------------------------------------ main
    def run(self):
        try:
            stopped = self.progress.get("stopped")
            if stopped and not stopped.get("recoverable"):
                # terminal on disk already: echo it, do not revive anything
                self.error = stopped.get("error")
                self._finish("CANCELLED" if stopped["reason"] == "cancelled"
                             else "FAILED", already_stopped=True)
                return
            if stopped and stopped.get("recoverable"):
                self.progress["stopped"] = None     # fault cleared by operator
                self._stopped_cleared = True
            self._verify_and_recover()
            if self.progress["phase"] == "done":
                self._finish("DONE")
                return
            if self.progress["phase"] == "read":
                self._read_phase()
                self._check_cancel()
            if self.progress["phase"] == "merge":
                self._merge_phase()
                self._check_cancel()
            self._finish("DONE")
        except Cancelled:
            self._finish("CANCELLED")
        except BaseException as exc:
            recoverable = isinstance(exc, OSError)
            self._finish("ERROR" if recoverable else "FAILED", exc,
                         persist_stopped=not self._done_record_intact)

    def _finish(self, state: str, exc: BaseException | None = None,
                already_stopped: bool = False, persist_stopped: bool = True):
        # once the atomic publish happened in THIS attempt the task is DONE,
        # however it got here: a cancel/error landing after the point of no
        # return must not downgrade the durable record
        if state != "DONE" and self._published_this_attempt:
            state = "DONE"
        if exc is not None:
            self._exc = self._exc or exc
            if self.error is None:
                self.error = f"{type(exc).__name__}: {exc}"
        # 1. stop the writer; join guarantees in-flight reservations came back
        #    and no background thread can still touch the state afterwards
        self.writer.cancel()
        self.writer.join()
        # 2. release memory
        self.pool.release_all()
        self.arbiter.release_all(self.job_id)
        # 3. persistent terminal state / reclaim policy (envelope still held)
        try:
            if state == "DONE":
                self._delete_run_files()
                self._delete_output_tmps()
            elif state == "ERROR":
                self._persist_stopped("io_error", recoverable=True)
            elif state == "CANCELLED" and not self._suspended and not already_stopped:
                self._persist_stopped("cancelled", recoverable=False)
                self._delete_run_files()
                self._delete_output_tmps()
            elif state == "FAILED" and not already_stopped and persist_stopped:
                self._persist_stopped("failed", recoverable=False)
        except Exception as pexc:
            self._ev("RSORT_STATE_PERSIST_FAIL", error=repr(pexc))
        finally:
            self.arbiter.end_phase(self.job_id)
            self.arbiter.unregister_job(self.job_id)
            self.runner.engine.buffer_pools.pop(self.job_id, None)
            if self.runner.engine.fail_first_write_for == self.job_id:
                self.runner.engine.fail_first_write_for = None
        with self._state_lock:
            self.state = state
        self._ev("JOB_STATE", state=state, error=self.error,
                 lines_read=self.lines_read, lines_written=self.lines_written)
        self.done_event.set()
        self._release_lock()

    # ------------------------------------------------------------- persistence
    def _persist(self, bounded: bool = False):
        """Write progress.json durably.  The standing STATE_BUF reservation
        covers the encoded state bytes (kind "state").  Inside an admitted
        phase the reservation is a normal in-envelope blocking one; during
        teardown (no envelope held) it falls back to a bounded wait, and as
        a last resort (job already arbiter-cancelled) proceeds unreserved --
        one bounded STATE_BUF-sized document from a job that is exiting."""
        self.progress["generation"] += 1
        self.progress["stats"]["totals"] = dict(self._tot)
        res = None
        try:
            if bounded:
                res = self.arbiter.reserve(self.job_id, STATE_BUF, "state",
                                           timeout=10.0)
            else:
                res = self.arbiter.reserve(self.job_id, STATE_BUF, "state")
        except ConfigError:
            # no admission envelope held right now (teardown): bounded wait
            try:
                res = self.arbiter.reserve(self.job_id, STATE_BUF, "state",
                                           timeout=10.0)
            except (AllocFailed, Cancelled):
                self._ev("RSORT_STATE_PERSIST_UNRESERVED", reason="no-envelope")
        except (AllocFailed, Cancelled):
            self._ev("RSORT_STATE_PERSIST_UNRESERVED",
                     reason="no-memory" if not self.cancel_event.is_set()
                     else "job-cancelled")
        try:
            _atomic_write_json(self._progress_path, self.progress)
        finally:
            if res is not None:
                self.arbiter.release(res)

    def _persist_stopped(self, reason: str, recoverable: bool,
                         bounded: bool = False):
        self.progress["stopped"] = {"reason": reason, "recoverable": recoverable,
                                    "error": self.error, "attempt": self.attempt}
        self._attempt_stat["finished"] = True
        self._persist(bounded=bounded)
        self._ev("RSORT_STOPPED", reason=reason, recoverable=recoverable,
                 error=self.error)

    def _update_attempt_stats(self, finished: bool = False):
        st = self._attempt_stat
        st["finished"] = finished
        st["source_bytes_read"] = self.source_bytes_read
        st["lines_read"] = self.lines_read
        st["runs_spilled"] = self.spills
        st["merge_groups_executed"] = self.merge_groups_executed

    # ------------------------------------------------------- verify / recover
    def _verify_and_recover(self):
        self._set_phase("verify")
        self._ev("RSORT_RESUME", phase=self.progress["phase"],
                 confirmed_offset=self.confirmed_offset,
                 next_lineno=self.next_lineno,
                 runs_confirmed=len(self.runs_confirmed),
                 reused_runs=self.reused_runs,
                 attempts=self.progress["attempts"])
        self.arbiter.begin_phase(self.job_id, self.mws)
        try:
            res = self.arbiter.reserve(self.job_id, STATE_BUF, "state")
            try:
                buf = bytearray(STATE_BUF)
                if self._stopped_cleared:
                    self._persist()      # record the cleared recoverable stop
                    self._stopped_cleared = False
                phase = self.progress["phase"]
                if phase == "done":
                    self._done_record_intact = True
                    self._verify_done_target(buf)
                    self._cleanup_orphans(done=True)
                    return
                if phase == "publish":
                    self._publish_recovery(buf)
                    if self.progress["phase"] == "done":
                        self._cleanup_orphans(done=True)
                        return
                # read / merge (or publish that fell back to merge)
                self._verify_runs(buf)
                self._cleanup_orphans(done=False)
            finally:
                self.arbiter.release(res)
        finally:
            self.arbiter.end_phase(self.job_id)
        self._hook("resume_verified", attempt=self.attempt,
                   phase=self.progress["phase"])

    def _verify_runs(self, buf: bytearray):
        """Every confirmed run must exist with the recorded length and
        checksum.  Corruption is refused explicitly -- never silently
        recomputed from scratch."""
        total = 0
        for r in self.runs_confirmed:
            self._check_cancel()
            path = os.path.join(self.runs_dir, r["name"])
            try:
                sha, nbytes = _sha256_file(path, buf)
            except OSError as exc:
                raise StateCorrupt(f"confirmed run unreadable: {path}: {exc!r}")
            if nbytes != r["bytes"]:
                raise StateCorrupt(
                    f"confirmed run {r['name']} length changed: "
                    f"want {r['bytes']}B got {nbytes}B")
            if sha != r["sha256"]:
                raise StateCorrupt(
                    f"confirmed run {r['name']} checksum mismatch "
                    f"(content corrupted)")
            total += nbytes
        self._ev("RSORT_RUNS_VERIFIED", runs=len(self.runs_confirmed),
                 bytes=total)

    def _cleanup_orphans(self, done: bool):
        """Delete files that the durable state does not reference:
        unconfirmed spill runs, replaced merge inputs, stale tmp files.
        Confirmed runs are never touched."""
        confirmed = set() if done else {r["name"] for r in self.runs_confirmed}
        for name in sorted(os.listdir(self.runs_dir)):
            if name not in confirmed:
                try:
                    os.unlink(os.path.join(self.runs_dir, name))
                except FileNotFoundError:
                    continue
                self._ev("RSORT_ORPHAN_DELETED", name=name)
        if done or self.progress["phase"] != "publish":
            self._delete_output_tmps()

    def _delete_run_files(self):
        n = 0
        for name in os.listdir(self.runs_dir):
            try:
                os.unlink(os.path.join(self.runs_dir, name))
                n += 1
            except FileNotFoundError:
                pass
        if n:
            self._ev("RSORT_RUNS_RECLAIMED", count=n)

    def _out_tmp(self) -> str:
        return self.cfg.output_path + f".tmp.{self.task_id}"

    def _delete_output_tmps(self):
        for p in (self._out_tmp(), self._out_tmp() + ".part"):
            if os.path.exists(p):
                try:
                    os.unlink(p)
                    self._ev("RSORT_ORPHAN_DELETED", name=p)
                except OSError:
                    pass

    def _target_matches_observed(self) -> bool:
        observed = self.task["output"]["observed"]
        try:
            st = os.stat(self.cfg.output_path)
        except FileNotFoundError:
            return not observed.get("exists")
        if not observed.get("exists"):
            return False
        return _ident_diff(observed, _ident(st)) is None

    def _verify_done_target(self, buf: bytearray):
        pub = self.progress.get("publish")
        final = self.cfg.output_path
        if not pub:
            raise StateCorrupt("progress.json: done without publish record")
        try:
            sha, nbytes = _sha256_file(final, buf)
        except OSError:
            raise PublishConflict(
                f"task is DONE but the published target is missing: {final}")
        if sha != pub["sha256"] or nbytes != pub["bytes"]:
            raise PublishConflict(
                f"task is DONE but the target no longer holds this task's "
                f"published result: {final}")
        self.lines_written = pub["lines"]
        self._ev("RSORT_DONE_VERIFIED", output=final, bytes=nbytes)

    def _publish_recovery(self, buf: bytearray):
        """Resolve the 'published but done not recorded' window (and all
        other publish interruptions) without redoing confirmed work."""
        pub = self.progress["publish"]
        final = self.cfg.output_path
        tmp = pub["tmp"]
        # 1. is our exact result already sitting at the target?
        if os.path.exists(final):
            sha, nbytes = _sha256_file(final, buf)
            if sha == pub["sha256"] and nbytes == pub["bytes"]:
                self._ev("RSORT_PUBLISH_RECOVERED", how="target-already-published",
                         output=final)
                self.progress["phase"] = "done"
                self.lines_written = pub["lines"]
                self._persist()
                return
        # 2. target still the file we observed at creation -> we may publish
        if self._target_matches_observed():
            if os.path.exists(tmp):
                sha, nbytes = _sha256_file(tmp, buf)
                if sha == pub["sha256"] and nbytes == pub["bytes"]:
                    os.replace(tmp, final)
                    self._published_this_attempt = True
                    _fsync_dir(os.path.dirname(final) or ".")
                    self._ev("RSORT_PUBLISH_RECOVERED", how="renamed-staged-tmp",
                             output=final)
                    self.progress["phase"] = "done"
                    self.lines_written = pub["lines"]
                    self._persist()
                    return
                os.unlink(tmp)   # staged tmp damaged -> redo the final merge
                self._ev("RSORT_ORPHAN_DELETED", name=tmp)
            else:
                self._ev("RSORT_ORPHAN_DELETED", name=tmp, missing=True)
            self.progress["publish"] = None
            self.progress["phase"] = "merge"
            self._persist()
            return
        # 3. someone else changed the target: report, preserve, do not touch
        raise PublishConflict(
            f"target changed after this task staged its result; refusing to "
            f"overwrite: {final}")

    # ------------------------------------------------------------------ read
    def _read_phase(self):
        self._set_phase("read")
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        arb.begin_phase(jid, self.mws)
        try:
            for _ in range(2):
                res = arb.reserve(jid, self.write_buf, "buffer")
                self.pool.add(bytearray(self.write_buf), res)
            offset = self.confirmed_offset
            lineno = self.next_lineno
            with open(cfg.input_path, "rb") as f:
                if offset:
                    f.seek(offset)
                self._ev("RSORT_SOURCE_OPEN", offset=offset)
                while True:
                    self._check_cancel()
                    block_res, lines = self._read_block(f, lineno, offset)
                    if not lines:
                        arb.release(block_res)
                        break
                    key_res, keys = self._extract_keys(lines, lineno)
                    with self.run_lock:
                        base = lineno
                        for i, (k, ln) in enumerate(zip(keys, lines)):
                            self._run.append((k, base + i, ln))
                        self._run_res.append(block_res)
                        self._run_res.append(key_res)
                        self._revocable += sum(len(x) for x in lines) \
                            + sum(len(k) for k in keys)
                        self._max_run_records = max(self._max_run_records,
                                                    len(self._run))
                        arb.set_revocable(jid, self._revocable)
                    block_start = offset
                    offset += sum(len(x) for x in lines)
                    lineno += len(lines)
                    self._blocks_read += 1
                    self.source_bytes_read += offset - block_start
                    self._tot["source_bytes_read"] += offset - block_start
                    self._tot["lines_read"] += len(lines)
                    self._ev("RSORT_SOURCE_BLOCK", start=block_start, end=offset,
                             lines=len(lines))
                    self._hook("block_read", blocks=self._blocks_read,
                               offset=offset, lineno=lineno)
                    if offset - self.confirmed_offset >= cfg.checkpoint_input_bytes:
                        self._checkpoint(offset, lineno, eof=False)
            self._checkpoint(offset, lineno, eof=True)
            self.writer.wait_idle()
            self._check_cancel()
        finally:
            self.pool.release_all()
            arb.end_phase(jid)

    def _read_block(self, f, lineno, offset):
        """Reserve a block and read whole lines into it.  Stops early when
        the checkpoint threshold is crossed: after the line that crosses
        ``checkpoint_input_bytes`` no further line is read before the
        checkpoint is durable."""
        arb, jid, cfg = self.arbiter, self.job_id, self.cfg
        try:
            res = arb.reserve(jid, self.read_block, "line", revocable=True,
                              timeout=0)
        except AllocFailed:
            self._spill()      # self-reclaim: free our own run, then retry
            res = arb.reserve(jid, self.read_block, "line", revocable=True)
        lines, total = [], 0
        limit = cfg.max_line_bytes
        threshold = cfg.checkpoint_input_bytes
        while total + (limit + 2) <= self.read_block:
            if offset + total - self.confirmed_offset >= threshold:
                break          # threshold crossed: checkpoint before more reads
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

    def _extract_keys(self, lines, base_lineno):
        arb, jid = self.arbiter, self.job_id
        keys_str = []
        for i, ln in enumerate(lines):
            try:
                obj = json.loads(ln)
            except Exception as exc:
                raise InvalidLine(
                    f"line {base_lineno + i}: invalid JSON: {exc!r}")
            if not isinstance(obj, dict):
                raise InvalidLine(f"line {base_lineno + i}: not a JSON object")
            key = obj.get("text")
            if not isinstance(key, str) or not key:
                raise InvalidLine(
                    f"line {base_lineno + i}: missing or empty 'text' key")
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
            raise InvalidLine(f"line {base_lineno}: key not UTF-8 encodable: {exc!r}")
        arb.adjust(kres, sum(len(k) for k in keys))
        self.lines_read += len(lines)
        return kres, keys

    # ------------------------------------------------------------------ spill
    def _spill(self) -> int:
        """Sort the in-memory run and hand it to the writer as
        run-NNNNNN.run (tmp -> fsync -> rename).  The run becomes a *pending*
        run: it only enters the durable confirmed list at the next
        checkpoint.  Also used as the cooperative reclaim callback."""
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
            run.sort(key=lambda r: (r[0], r[1]))   # (key_bytes, lineno): codepoint order, stable
            seq = self._run_seq
            self._run_seq += 1
            name = f"run-{seq:06d}.run"
            tmp = os.path.join(self.runs_dir, name + ".tmp")
            final = os.path.join(self.runs_dir, name)
            hasher = hashlib.sha256()
            nbytes = 0
            self._ev("RSORT_SPILL_START", records=len(run), bytes_to_free=freed)
            idx = self.writer.open_file(tmp, final, fsync=True,
                                        on_closed=self._on_file_closed)
            buf, buf_res = self.pool.acquire()
            off = 0
            try:
                for key, ln, line in run:
                    rec = HDR.size + len(key) + len(line)
                    if off + rec > len(buf):
                        hasher.update(memoryview(buf)[:off])
                        nbytes += off
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
                    hasher.update(memoryview(buf)[:off])
                    nbytes += off
                    self.writer.write_buffer(idx, buf, buf_res, off)
                    buf_res = None
            finally:
                if buf_res is not None:
                    self.pool.buffer_returned(buf_res)
            self.writer.close_file(idx)
            for res in run_res:
                self.arbiter.release(res)
            self.spills += 1
            self._tot["runs_spilled"] += 1
            desc = {"name": name, "records": len(run), "bytes": nbytes,
                    "sha256": hasher.hexdigest()}
            self._pending_runs.append(desc)
            self._ev("RSORT_RUN_SPILT", run=name, records=len(run),
                     bytes=nbytes, confirmed=False)
            return freed

    def _reclaim_handler(self, nbytes: int) -> int:
        """Cooperative reclaim (other job's thread, no arbiter lock).  Only
        spills revocable run data; never reserves; never persists."""
        if self.cancel_event.is_set() or self.fail_event.is_set():
            return 0
        try:
            return self._spill()
        except (Cancelled, JobFailed):
            return 0

    def _on_file_closed(self, *, ok, error, tmp, final):
        if not ok and not self.cancel_event.is_set():
            self._fail(error or f"write failed: {tmp}")

    # ------------------------------------------------------------- checkpoint
    def _checkpoint(self, offset: int, lineno: int, eof: bool):
        """Make everything read so far durable: spill the in-memory run,
        wait for the writer, fsync the runs dir, then persist the confirmed
        prefix (offset + next physical line number) and the run list."""
        self._spill()
        self.writer.wait_idle()
        self._check_cancel()
        _fsync_dir(self.runs_dir)
        self._hook("before_checkpoint_persist", offset=offset, lineno=lineno)
        with self._spill_lock:
            pending, self._pending_runs = self._pending_runs, []
        self.confirmed_offset = offset
        self.next_lineno = lineno
        self.runs_confirmed.extend(pending)
        p = self.progress
        p["read"] = {"confirmed_offset": offset, "next_lineno": lineno,
                     "eof": eof}
        p["runs"] = [dict(r) for r in self.runs_confirmed]
        p["counters"]["run_seq"] = self._run_seq
        if eof:
            p["phase"] = "merge"
        self._update_attempt_stats()
        self._persist()
        self._ev("RSORT_CHECKPOINT", generation=p["generation"],
                 confirmed_offset=offset, next_lineno=lineno,
                 runs=len(self.runs_confirmed), eof=eof)
        self._hook("checkpoint_done", offset=offset, lineno=lineno,
                   generation=p["generation"])

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
                runs = list(self.runs_confirmed)
                fan_in = cfg.max_fan_in
                while len(runs) > fan_in:
                    self._check_cancel()
                    group = runs[:fan_in]
                    desc = self._merge_group(group)
                    self._hook("merge_group_done_unconfirmed",
                               output=desc["name"])
                    self.writer.wait_idle()
                    self._check_cancel()
                    _fsync_dir(self.runs_dir)
                    runs = runs[fan_in:] + [desc]
                    self.runs_confirmed = runs
                    p = self.progress
                    p["runs"] = [dict(r) for r in runs]
                    p["counters"]["merge_seq"] = self._merge_seq
                    self._update_attempt_stats()
                    self._persist()
                    self._ev("RSORT_MERGE_GROUP_CONFIRMED", output=desc["name"],
                             inputs=[g["name"] for g in group],
                             records=desc["records"],
                             generation=p["generation"])
                    self._hook("merge_confirmed", output=desc["name"],
                               generation=p["generation"])
                    # the confirmed output now replaces its inputs: reclaim
                    for g in group:
                        try:
                            os.unlink(os.path.join(self.runs_dir, g["name"]))
                            self._ev("RSORT_MERGE_INPUT_RECLAIMED",
                                     name=g["name"])
                        except FileNotFoundError:
                            pass
                self._final_merge_and_publish(runs)
                self.writer.wait_idle()
                self._check_cancel()
            finally:
                self.pool.release_all()
        finally:
            arb.end_phase(jid)

    def _merge_group(self, group) -> dict:
        seq = self._merge_seq
        self._merge_seq += 1
        name = f"merge-{seq:06d}.run"
        tmp = os.path.join(self.runs_dir, name + ".tmp")
        final = os.path.join(self.runs_dir, name)
        rec_in = sum(g["records"] for g in group)
        byte_in = sum(g["bytes"] for g in group)
        self._ev("RSORT_MERGE_GROUP_START", output=name,
                 inputs=[g["name"] for g in group],
                 records_in=rec_in, bytes_in=byte_in)
        self._hook("merge_group_start", output=name,
                   inputs=[g["name"] for g in group])
        idx = self.writer.open_file(tmp, final, fsync=True,
                                    on_closed=self._on_file_closed)
        n_out, sha, nbytes = self._kway(group, idx, dedup=False,
                                        to_run_format=True)
        self.writer.close_file(idx)
        self.merge_groups_executed += 1
        self.merge_records_in += rec_in
        self.merge_bytes_in += byte_in
        self._tot["merge_groups_executed"] += 1
        self._tot["merge_records_in"] += rec_in
        self._tot["merge_bytes_in"] += byte_in
        self._ev("RSORT_MERGE_GROUP_EXEC", output=name,
                 inputs=[g["name"] for g in group],
                 records_in=rec_in, records_out=n_out, bytes_in=byte_in)
        return {"name": name, "records": n_out, "bytes": nbytes, "sha256": sha}

    def _final_merge_and_publish(self, runs):
        out_tmp = self._out_tmp()
        wip = out_tmp + ".part"
        for p in (wip, out_tmp):
            if os.path.exists(p):
                os.unlink(p)   # stale fragment of a crashed attempt
                self._ev("RSORT_ORPHAN_DELETED", name=p)
        rec_in = sum(g["records"] for g in runs)
        byte_in = sum(g["bytes"] for g in runs)
        self._ev("RSORT_FINAL_MERGE_START", inputs=[g["name"] for g in runs],
                 records_in=rec_in, bytes_in=byte_in,
                 dedup=self.cfg.dedup_key)
        self._hook("final_merge_start", inputs=[g["name"] for g in runs])
        idx = self.writer.open_file(wip, out_tmp, fsync=True,
                                    on_closed=self._on_file_closed)
        n_out, sha, nbytes = self._kway(runs, idx, dedup=self.cfg.dedup_key,
                                        to_run_format=False)
        self.writer.close_file(idx)
        self.writer.wait_idle()
        self._check_cancel()
        self.lines_written = n_out
        self._tot["final_merges_executed"] += 1
        self._tot["merge_records_in"] += rec_in
        self._tot["merge_bytes_in"] += byte_in
        self._ev("RSORT_FINAL_MERGE_EXEC", records_in=rec_in, records_out=n_out,
                 bytes_out=nbytes)
        self._hook("before_publish_persist", tmp=out_tmp)
        # staged result is durable -> record it *before* touching the target
        self.progress["publish"] = {"tmp": out_tmp, "final": self.cfg.output_path,
                                    "bytes": nbytes, "sha256": sha,
                                    "lines": n_out}
        self.progress["phase"] = "publish"
        self._update_attempt_stats()
        self._persist()
        self._ev("RSORT_PUBLISH_PREPARED", tmp=out_tmp, bytes=nbytes)
        self._hook("before_publish_rename")
        # never overwrite a file that changed since this task first looked
        if not self._target_matches_observed():
            raise PublishConflict(
                f"target changed while the task was running; refusing to "
                f"overwrite: {self.cfg.output_path}")
        self._check_cancel()       # a cancel can still prevent the publish
        os.replace(out_tmp, self.cfg.output_path)   # the single atomic publish
        self._published_this_attempt = True
        _fsync_dir(os.path.dirname(self.cfg.output_path) or ".")
        self._hook("after_rename_before_done")
        self.progress["phase"] = "done"
        self._update_attempt_stats(finished=True)
        self._persist()
        self._ev("RSORT_PUBLISHED", output=self.cfg.output_path, bytes=nbytes,
                 lines=n_out, generation=self.progress["generation"])

    def _kway(self, group, writer_idx, *, dedup: bool, to_run_format: bool):
        """k-way merge of run descriptors; heap order (key_bytes, lineno,
        reader) gives codepoint order with stable line-number tie-breaking.
        Output mode appends '\\n' to any record whose original line lacked
        it, so records are never glued together and the delivered file
        always ends with a newline.  Returns (records, sha256, bytes)."""
        readers = [RunReader(self, os.path.join(self.runs_dir, d["name"]))
                   for d in group]
        import heapq
        heap = []
        buf_res = None
        hasher = hashlib.sha256()
        nbytes = 0
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
                    emit = False   # keep only the earliest original line
                if emit:
                    line_len = sum(len(p) for p in pieces)
                    if to_run_format:
                        rec_len = HDR.size + len(key) + line_len
                        if off + rec_len > len(buf):
                            hasher.update(memoryview(buf)[:off])
                            nbytes += off
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
                        add_nl = 0 if pieces[-1][-1:] == b"\n" else 1
                        if off + line_len + add_nl > len(buf):
                            hasher.update(memoryview(buf)[:off])
                            nbytes += off
                            self.writer.write_buffer(writer_idx, buf, buf_res, off)
                            buf_res = None
                            buf, buf_res = self.pool.acquire()
                            off = 0
                        for p in pieces:
                            buf[off:off + len(p)] = p
                            off += len(p)
                        if add_nl:
                            buf[off] = 0x0A
                            off += 1
                    last_key = key
                    n_out += 1
                r.release_record()
                rec = r.next()
                if rec is not None:
                    k2, ln2, p2 = rec
                    heapq.heappush(heap, (k2, ln2, i, p2))
            if off:
                hasher.update(memoryview(buf)[:off])
                nbytes += off
                self.writer.write_buffer(writer_idx, buf, buf_res, off)
                buf_res = None
            else:
                self.pool.buffer_returned(buf_res)
                buf_res = None
            return n_out, hasher.hexdigest(), nbytes
        finally:
            if buf_res is not None:
                self.arbiter.release(buf_res)
            for r in readers:
                r.close()

    # ------------------------------------------------------------------ report
    def metadata_estimate(self) -> int:
        per_record = 8 + 56 + 49 + 49
        return self._max_run_records * per_record + len(self.runs_confirmed) * 256

    def status(self) -> dict:
        with self._state_lock:
            return {
                "job_id": self.job_id,
                "task_id": self.task_id,
                "state": self.state,
                "phase": self.progress["phase"],
                "activity": self.phase,
                "error": self.error,
                "attempt": self.attempt,
                "recoveries": self.attempt - 1,
                "checkpoint_generation": self.progress["generation"],
                "confirmed_offset": self.confirmed_offset,
                "next_lineno": self.next_lineno,
                "lines_read": self.lines_read,
                "lines_written": self.lines_written,
                "spills": self.spills,
                "runs_confirmed": len(self.runs_confirmed),
                "runs_reused": self.reused_runs,
                "source_bytes_read": self.source_bytes_read,
                "merge_groups_executed": self.merge_groups_executed,
                "merge_records_in": self.merge_records_in,
                "merge_bytes_in": self.merge_bytes_in,
                "totals": dict(self._tot),
                "stopped": self.progress.get("stopped"),
                "held_bytes": self.arbiter.held(self.job_id),
                "mws": self.mws,
                "state_dir": self.state_dir,
                "input": self.cfg.input_path,
                "output": self.cfg.output_path,
            }
