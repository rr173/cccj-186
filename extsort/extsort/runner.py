"""Job runner: submit / status / cancel / wait over a shared BufferArbiter."""
from __future__ import annotations

import os
import threading

from .arbiter import BufferArbiter, ConfigError
from .events import EventLog
from .ioengine import IOEngine
from .job import JobConfig, SortJob


class JobRunner:
    def __init__(self, budget_bytes: int, work_dir: str,
                 event_log_path: str | None = None, fsync: bool = False,
                 keep_events_in_memory: bool = True):
        os.makedirs(work_dir, exist_ok=True)
        self.work_dir = work_dir
        self.events = EventLog(event_log_path, keep_in_memory=keep_events_in_memory)
        self.arbiter = BufferArbiter(budget_bytes, self.events)
        self.engine = IOEngine(self.arbiter, self.events, fsync=fsync)
        self._jobs: dict[str, SortJob] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
        self._seq = 0

    # ------------------------------------------------------------------ API
    def submit(self, input_path: str, output_path: str | None = None, *,
               job_id: str | None = None, **cfg_kwargs) -> str:
        """Submit one external-sort job.  Raises ConfigError at submit time if
        the configuration's minimal working set cannot ever fit the budget."""
        with self._lock:
            self._seq += 1
            jid = job_id or f"job{self._seq}"
            if jid in self._jobs:
                raise ValueError(f"duplicate job_id {jid}")
        if output_path is None:
            output_path = input_path + ".sorted"
        cfg = JobConfig(input_path, output_path, **cfg_kwargs)
        job = SortJob(self, jid, cfg)   # computes MWS; ConfigError -> rejected here
        t = threading.Thread(target=job.run, name=f"job-{jid}", daemon=True)
        with self._lock:
            self._jobs[jid] = job
            self._threads[jid] = t
        self.events.emit("JOB_SUBMIT", jid, input=input_path, output=output_path,
                         mws=job.mws, dedup_key=cfg.dedup_key,
                         max_line_bytes=cfg.max_line_bytes,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)
        t.start()
        return jid

    def status(self, job_id: str | None = None):
        with self._lock:
            if job_id is not None:
                job = self._jobs.get(job_id)
                return job.status() if job else {"job_id": job_id, "state": "UNKNOWN"}
            return {jid: job.status() for jid, job in self._jobs.items()}

    def cancel(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        job.cancel()
        # emitted only after the cancellation is fully signalled, so any event
        # after CANCEL_REQUEST is strictly post-cancel
        self.events.emit("CANCEL_REQUEST", job_id,
                         total_reserved=self.arbiter.total_reserved, budget=self.arbiter.budget)

    def wait(self, job_id: str, timeout: float | None = None) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if not job.done_event.wait(timeout):
            return {**job.status(), "timeout": True}
        return job.status()

    def wait_all(self, timeout: float | None = None) -> dict:
        with self._lock:
            ids = list(self._jobs)
        return {jid: self.wait(jid, timeout) for jid in ids}

    def report(self) -> dict:
        rep = self.arbiter.report()
        rep["jobs"] = self.status()
        rep["python_metadata_estimate_bytes"] = sum(
            j.metadata_estimate() for j in list(self._jobs.values()))
        rep["peak_rss_bytes"] = peak_rss_bytes()
        rep["accounting_note"] = (
            "B covers reserved byte content only (lines/keys/merge buffers/"
            "write buffers/in-flight I/O). Python object metadata is estimated "
            "separately; peak_rss_bytes is the kernel high-water mark (VmHWM).")
        return rep

    def shutdown(self):
        with self._lock:
            jobs = list(self._jobs.values())
        for j in jobs:
            if j.state not in ("DONE", "FAILED", "CANCELLED"):
                j.cancel()
        for j in jobs:
            j.done_event.wait(30)
        self.events.close()


def peak_rss_bytes() -> int:
    """Real peak RSS of the process (kernel high-water mark)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except Exception:
        return -1
