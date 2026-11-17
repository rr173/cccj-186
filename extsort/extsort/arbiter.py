"""Shared buffer arbiter.

Owns the budget ``B`` that covers, for *all* jobs in the process:

  * encoded input-line bytes held in memory        (kind "line")
  * encoded sort-key bytes held in memory          (kind "key")
  * merge read buffers / straddle remainders       (kind "merge-read")
  * materialised merge keys                        (kind "merge-key")
  * serialisation / output write buffers           (kind "buffer")
  * bytes actually in flight to the disk writer    (kind "io")

Accounting rules (the "口径"):
  * one distinct bytes/bytearray object is counted exactly once, no matter how
    many references exist (reservations are created when the object is created
    and released when the last reference is dropped by the owner);
  * a copy is always reserved *before* it is made (reserve an upper bound,
    then ``adjust`` down to the actual size, e.g. bounded ``readline`` and
    UTF-8 encode with a 4-bytes/char bound);
  * a reservation may be *transferred* (``transfer``) between kinds, e.g. a
    write buffer becomes in-flight I/O and comes back -- same object, same
    reservation, counted once throughout;
  * disk files (spill runs, outputs) are *not* resident and are not counted;
  * fixed Python object metadata (list slots, tuple/dict headers, decoded str
    during JSON parsing, timsort scratch pointers) is *not* part of B; it is
    estimated separately and reported next to the real peak RSS.  We never
    claim B == process RSS.

Allocation outcomes are exactly: grant, wait, or fail (AllocFailed).
The invariant ``sum(outstanding reservations) <= B`` holds at all times and
is asserted after every mutation in debug mode; every event carries the
current total so the invariant can be re-checked from the event log.

Liveness / no-wait-cycle argument (see README for the full version):
  * a job may only *block* (wait without timeout) on a reservation that keeps
    it within its admitted minimal-working-set envelope (MWS);
  * envelopes are admitted FIFO and ``sum(active envelopes) <= B`` always;
  * requests beyond the envelope must pass a timeout and may fail; the job
    then falls back to spilling its own revocable run (which needs no extra
    memory: the spilled bytes are already reserved, and the write buffers are
    part of the envelope) or to a smaller merge fan-in;
  * when memory is short the arbiter asks other jobs to spill their revocable
    run data (cooperative reclaim) -- reclaim callbacks are invoked *without*
    the arbiter lock held and never call back into ``reserve``.
"""
from __future__ import annotations

import itertools
import threading
import time
from collections import defaultdict, deque

from .events import EventLog


class AllocFailed(Exception):
    """A bounded wait for memory expired: caller must fall back (spill / smaller fan-in)."""


class Cancelled(Exception):
    """The job was cancelled (or failed) while waiting for memory or admission."""


class ConfigError(Exception):
    """Configuration cannot even hold its minimal working set, or API misuse."""


class Reservation:
    __slots__ = ("rid", "job_id", "kind", "nbytes", "revocable", "released", "beyond")

    def __init__(self, rid, job_id, kind, nbytes, revocable, beyond=False):
        self.rid = rid
        self.job_id = job_id
        self.kind = kind
        self.nbytes = nbytes
        self.revocable = revocable
        self.released = False
        self.beyond = beyond  # granted beyond the job's MWS envelope

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"<Res#{self.rid} {self.job_id} {self.kind} {self.nbytes}B rel={self.released}>"


_INFINITE = None


class BufferArbiter:
    def __init__(self, budget_bytes: int, events: EventLog, debug: bool = True):
        if budget_bytes <= 0:
            raise ValueError("budget must be positive")
        self.budget = int(budget_bytes)
        self.events = events
        self.debug = debug

        self._cond = threading.Condition()
        self._total = 0
        self._high_water = 0
        self._by_kind: dict[str, int] = defaultdict(int)
        self._kind_peak: dict[str, int] = defaultdict(int)
        self._res: dict[int, Reservation] = {}
        self._by_job: dict[str, set[int]] = defaultdict(set)
        self._rids = itertools.count(1)

        # job registry: job_id -> {"mws": int, "handler": callable|None, "revocable": int}
        self._jobs: dict[str, dict] = {}
        self._cancelled: set[str] = set()

        # minimal-working-set envelope admission (FIFO)
        self._envelopes: dict[str, int] = defaultdict(int)
        self._beyond_bytes: dict[str, int] = defaultdict(int)
        self._active_env = 0
        self._env_waiters: deque[str] = deque()

        # serialises cooperative reclaim rounds
        self._reclaim_lock = threading.Lock()

        # ---- test-only hooks (never used in production paths) ----
        self.on_wait_start = None      # fn(job_id, nbytes) called w/o lock before blocking
        self.on_reclaim_call = None    # fn(victim_job_id) called w/o lock inside reclaim round

    # ------------------------------------------------------------------ util
    def _free_locked(self) -> int:
        return self.budget - self._total

    def _held_locked(self, job_id: str) -> int:
        return sum(self._res[r].nbytes for r in self._by_job.get(job_id, ()))

    def held(self, job_id: str) -> int:
        with self._cond:
            return self._held_locked(job_id)

    @property
    def total_reserved(self) -> int:
        with self._cond:
            return self._total

    @property
    def high_water(self) -> int:
        with self._cond:
            return self._high_water

    def kind_peaks(self) -> dict[str, int]:
        with self._cond:
            return dict(self._kind_peak)

    def _assert_invariant_locked(self):
        if not self.debug:
            return
        assert self._total <= self.budget, f"budget violated: {self._total} > {self.budget}"
        assert self._total == sum(r.nbytes for r in self._res.values()), "accounting drift"
        assert self._total == sum(self._by_kind.values()), "kind accounting drift"

    def _ev(self, kind: str, job_id: str | None, **fields):
        fields.setdefault("total_reserved", self._total)
        fields.setdefault("budget", self.budget)
        self.events.emit(kind, job_id, **fields)

    # --------------------------------------------------------- job lifecycle
    def register_job(self, job_id: str, mws: int, reclaim_handler=None):
        with self._cond:
            self._jobs[job_id] = {"mws": mws, "handler": reclaim_handler, "revocable": 0}

    def unregister_job(self, job_id: str):
        with self._cond:
            self._jobs.pop(job_id, None)
            self._cancelled.discard(job_id)

    def set_revocable(self, job_id: str, nbytes: int):
        """Job reports how many of its held bytes are currently spillable run data."""
        with self._cond:
            j = self._jobs.get(job_id)
            if j is not None:
                j["revocable"] = nbytes

    # ------------------------------------------------------- phase admission
    def begin_phase(self, job_id: str, mws: int):
        """Admit the job's minimal-working-set envelope (FIFO, blocking)."""
        if mws > self.budget:
            raise ConfigError(f"job {job_id}: minimal working set {mws} > budget {self.budget}")
        with self._cond:
            if job_id in self._cancelled:
                raise Cancelled()
            self._env_waiters.append(job_id)
            self._ev("JOB_ADMIT_WAIT", job_id, mws=mws, active_env=self._active_env,
                     queue=len(self._env_waiters))
            try:
                while True:
                    if job_id in self._cancelled:
                        raise Cancelled()
                    if self._env_waiters[0] == job_id and self._active_env + mws <= self.budget:
                        self._env_waiters.popleft()
                        self._envelopes[job_id] = mws
                        self._active_env += mws
                        self._ev("JOB_ADMIT", job_id, mws=mws, active_env=self._active_env)
                        self._cond.notify_all()
                        return
                    self._cond.wait()
            finally:
                if job_id in self._env_waiters:
                    self._env_waiters.remove(job_id)
                    self._cond.notify_all()

    def end_phase(self, job_id: str):
        with self._cond:
            mws = self._envelopes.get(job_id, 0)
            if mws:
                self._envelopes[job_id] = 0
                self._active_env -= mws
                self._ev("JOB_PHASE_RELEASE", job_id, mws=mws, active_env=self._active_env)
                self._cond.notify_all()

    # ------------------------------------------------------------- reserve
    def reserve(self, job_id: str, nbytes: int, kind: str, *,
                revocable: bool = False, timeout: float | None = _INFINITE) -> Reservation:
        """Allocate ``nbytes``.  Returns a Reservation (grant), or waits, or
        raises AllocFailed (bounded wait expired) / Cancelled.

        A call with ``timeout=None`` (potentially unbounded wait) is only
        legal when the request keeps the job within its admitted envelope --
        this is what makes unbounded waits provably deadlock-free.
        """
        if nbytes < 0:
            raise ValueError("negative reservation")
        if nbytes == 0:
            nbytes = 0  # allowed; tracked like any other reservation
        with self._cond:
            self._assert_invariant_locked()
            if job_id in self._cancelled:
                raise Cancelled()
            if self._free_locked() >= nbytes:
                return self._grant_locked(job_id, nbytes, kind, revocable, waited=False)
            env = self._envelopes.get(job_id, 0)
            held_within = self._held_locked(job_id) - self._beyond_bytes.get(job_id, 0)
            if timeout is None and held_within + nbytes > env:
                raise ConfigError(
                    f"unbounded blocking reserve beyond envelope: job={job_id} "
                    f"held_within={held_within} req={nbytes} envelope={env}")
            victims = self._pick_victims_locked(job_id, nbytes)

        # --- cooperative reclaim, invoked WITHOUT the arbiter lock held ---
        if victims:
            self._reclaim_round(job_id, victims, nbytes)

        if self.on_wait_start is not None:
            self.on_wait_start(job_id, nbytes)  # test hook, no lock held

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            waiting_logged = False
            while True:
                if job_id in self._cancelled:
                    self._ev("RESERVE_ABORT", job_id, res_kind=kind, nbytes=nbytes)
                    raise Cancelled()
                if self._free_locked() >= nbytes:
                    if waiting_logged:
                        self._ev("RESERVE_WAKE", job_id, res_kind=kind, nbytes=nbytes,
                                 free=self._free_locked())
                    res = self._grant_locked(job_id, nbytes, kind, revocable, waited=waiting_logged)
                    return res
                if not waiting_logged:
                    self._ev("RESERVE_WAIT_START", job_id, res_kind=kind, nbytes=nbytes,
                             free=self._free_locked())
                    waiting_logged = True
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._ev("RESERVE_FAIL", job_id, res_kind=kind, nbytes=nbytes,
                                 free=self._free_locked())
                        raise AllocFailed(f"job {job_id}: could not reserve {nbytes}B of {kind}")
                    self._cond.wait(remaining)
                else:
                    self._cond.wait()

    def _grant_locked(self, job_id, nbytes, kind, revocable, waited) -> Reservation:
        env = self._envelopes.get(job_id, 0)
        beyond = self._held_locked(job_id) + nbytes > env
        res = Reservation(next(self._rids), job_id, kind, nbytes, revocable, beyond)
        self._res[res.rid] = res
        self._by_job[job_id].add(res.rid)
        if beyond:
            self._beyond_bytes[job_id] += nbytes
        self._total += nbytes
        self._by_kind[kind] += nbytes
        if self._by_kind[kind] > self._kind_peak[kind]:
            self._kind_peak[kind] = self._by_kind[kind]
        if self._total > self._high_water:
            self._high_water = self._total
        self._ev("RESERVE_GRANT", job_id, rid=res.rid, res_kind=kind, nbytes=nbytes,
                 revocable=revocable, waited=waited, free=self._free_locked())
        self._assert_invariant_locked()
        return res

    def _pick_victims_locked(self, requester: str, nbytes: int) -> list[str]:
        cands = [
            (j["revocable"], jid)
            for jid, j in self._jobs.items()
            if jid != requester and j["revocable"] > 0 and j["handler"] is not None
        ]
        cands.sort(reverse=True)
        picked, need = [], nbytes - self._free_locked()
        for rev, jid in cands:
            if need <= 0:
                break
            picked.append(jid)
            need -= rev
        return picked

    def _reclaim_round(self, requester: str, victims: list[str], nbytes: int):
        # serialised so two requesters do not interleave callbacks on one victim
        with self._reclaim_lock:
            for victim in victims:
                with self._cond:
                    j = self._jobs.get(victim)
                    handler = j["handler"] if j else None
                    revocable = j["revocable"] if j else 0
                if handler is None or revocable <= 0:
                    continue
                self.events.emit("RECLAIM_REQUEST", victim, by=requester,
                                 need=nbytes, victim_revocable=revocable,
                                 total_reserved=self._total, budget=self.budget)
                if self.on_reclaim_call is not None:
                    self.on_reclaim_call(victim)
                try:
                    reclaimed = handler(nbytes) or 0
                except Exception as exc:  # a broken/cancelled victim must not break the arbiter
                    self.events.emit("RECLAIM_ERROR", victim, error=repr(exc),
                                     total_reserved=self._total, budget=self.budget)
                    reclaimed = 0
                self.events.emit("RECLAIM_RESULT", victim, by=requester, reclaimed=reclaimed,
                                 total_reserved=self._total, budget=self.budget)

    # ------------------------------------------------------------- release
    def release(self, res: Reservation):
        """Release a reservation.  Each reservation has exactly one logical
        owner that releases it once; this method is defensively idempotent and
        a second release attempt is logged as DOUBLE_RELEASE (tests assert it
        never happens)."""
        with self._cond:
            if res.released:
                self._ev("DOUBLE_RELEASE", res.job_id, rid=res.rid, res_kind=res.kind,
                         nbytes=res.nbytes)
                return
            self._release_locked(res)

    def _release_locked(self, res: Reservation):
        res.released = True
        self._res.pop(res.rid, None)
        self._by_job[res.job_id].discard(res.rid)
        if res.beyond:
            self._beyond_bytes[res.job_id] -= res.nbytes
        self._total -= res.nbytes
        self._by_kind[res.kind] -= res.nbytes
        self._ev("RELEASE", res.job_id, rid=res.rid, res_kind=res.kind, nbytes=res.nbytes,
                 free=self._free_locked())
        self._assert_invariant_locked()
        self._cond.notify_all()

    def release_all(self, job_id: str):
        """Safety net at job teardown: release everything still held."""
        with self._cond:
            rids = list(self._by_job.get(job_id, ()))
            total = 0
            for rid in rids:
                res = self._res.get(rid)
                if res is not None and not res.released:
                    total += res.nbytes
                    self._release_locked(res)
            if total:
                self._ev("RELEASE_ALL", job_id, nbytes=total)

    # ------------------------------------------------------------- adjust / transfer
    def adjust(self, res: Reservation, new_nbytes: int):
        """Shrink a reservation to the actual size (reserve-bound-then-adjust).
        Growth is not allowed here -- reserve separately."""
        if new_nbytes < 0:
            raise ValueError("negative size")
        with self._cond:
            if res.released:
                self._ev("DOUBLE_RELEASE", res.job_id, rid=res.rid, res_kind="adjust-after-release")
                return
            if new_nbytes > res.nbytes:
                raise ConfigError("adjust() may only shrink; reserve more instead")
            delta = res.nbytes - new_nbytes
            if delta:
                res.nbytes = new_nbytes
                if res.beyond:
                    self._beyond_bytes[res.job_id] -= delta
                self._total -= delta
                self._by_kind[res.kind] -= delta
                self._ev("RESERVE_ADJUST", res.job_id, rid=res.rid, res_kind=res.kind,
                         nbytes=new_nbytes, freed=delta, free=self._free_locked())
                self._assert_invariant_locked()
                self._cond.notify_all()

    def transfer(self, res: Reservation, new_kind: str, *, revocable: bool | None = None):
        """Reclassify the *same* byte object (e.g. buffer -> in-flight io ->
        buffer).  Counted once throughout."""
        with self._cond:
            if res.released:
                self._ev("DOUBLE_RELEASE", res.job_id, rid=res.rid, res_kind="transfer-after-release")
                return
            old = res.kind
            self._by_kind[old] -= res.nbytes
            self._by_kind[new_kind] += res.nbytes
            if self._by_kind[new_kind] > self._kind_peak[new_kind]:
                self._kind_peak[new_kind] = self._by_kind[new_kind]
            res.kind = new_kind
            if revocable is not None:
                res.revocable = revocable
            self._ev("TRANSFER", res.job_id, rid=res.rid, from_kind=old, to_kind=new_kind,
                     nbytes=res.nbytes)
            self._assert_invariant_locked()

    # ------------------------------------------------------------- cancel
    def cancel_job(self, job_id: str):
        """Wake every arbiter wait this job is parked in (reserve / admit).
        Waiters re-check the cancelled set and raise Cancelled."""
        with self._cond:
            self._cancelled.add(job_id)
            self._ev("CANCEL_ARBITER", job_id)
            self._cond.notify_all()

    # ------------------------------------------------------------- report
    def report(self) -> dict:
        with self._cond:
            return {
                "budget_bytes": self.budget,
                "total_reserved_now": self._total,
                "high_water_bytes": self._high_water,
                "kind_peaks": dict(self._kind_peak),
                "outstanding_reservations": len(self._res),
            }
