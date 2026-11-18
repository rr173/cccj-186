"""Subprocess driver for resumable-sort kill/resume acceptance tests.

Usage:  python3 tests/rsort_driver.py spec.json

The spec (JSON) drives one attempt of a resumable sort task:

  {
    "cmd": "new" | "resume",
    "input": "...", "output": "...", "state_dir": "...",
    "task_id": "name",                      # optional (new only)
    "budget": 900000,
    "dedup": false,                          # new only (bound semantics)
    "max_line_bytes": 2048,                  # new only (bound semantics)
    "read_block_bytes": 32768, ... ,         # tunables, either cmd
    "checkpoint_input_bytes": 65536,
    "event_log": "path",                     # default <state_dir>/events.jsonl
    "hooks": {                               # test gates
       "block_read": {"at": 5, "signal": "/tmp/sig", "action": "kill"},
       "merge_confirmed": {"at": 1, "signal": "...", "action": "wait",
                          "wait": "/tmp/release"},
       "checkpoint_done": {"at": 2, "action": "cancel"}
    },
    "fail_write": [{"contains": "run-000001", "times": 1}]
  }

Hook actions:
  kill   -- write the signal file, then SIGKILL itself (deterministic gate)
  wait   -- write the signal file, then wait for the "wait" file to appear
  cancel -- write the signal file, then cancel the job from inside

Exit codes: 0 DONE, 1 FAILED, 3 refused (ResumeError/ConfigError),
4 ERROR (recoverable), 5 CANCELLED, 2 driver error.
The last stdout lines are "SUBMITTED <jid>" and "STATUS <json>" or
"REFUSED <json>".
"""
import json
import os
import signal
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner
from extsort.arbiter import ConfigError
from extsort.resumable import ResumeError


def main() -> int:
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        spec = json.load(f)

    state = {"jid": None}
    jid_set = threading.Event()

    def make_hook(hspec):
        count = {"n": 0}

        def _hook(job, **kw):
            count["n"] += 1
            if count["n"] != hspec.get("at", 1):
                return
            sig = hspec.get("signal")
            if sig:
                with open(sig, "w") as sf:
                    sf.write(json.dumps({"count": count["n"], "pid": os.getpid()}))
            action = hspec.get("action")
            if action == "kill":
                os.kill(os.getpid(), signal.SIGKILL)
            elif action == "cancel":
                jid_set.wait(30)
                runner.cancel(state["jid"])
            elif action == "wait":
                deadline = time.monotonic() + hspec.get("timeout", 120)
                while not os.path.exists(hspec["wait"]):
                    if time.monotonic() > deadline:
                        raise RuntimeError("gate wait timeout")
                    time.sleep(0.005)
        return _hook

    hooks = {name: make_hook(hs) for name, hs in (spec.get("hooks") or {}).items()}

    event_log = spec.get("event_log") or os.path.join(spec["state_dir"],
                                                      "events.jsonl")
    runner = JobRunner(spec["budget"], spec["state_dir"],
                       event_log_path=event_log)

    if spec.get("fail_write"):
        rules = [dict(r) for r in spec["fail_write"]]

        def before_write(job_id, tmp_path):
            for r in rules:
                if r["times"] > 0 and r["contains"] in tmp_path:
                    r["times"] -= 1
                    raise OSError(f"injected write failure on {tmp_path}")
        runner.engine.before_write_hook = before_write

    tun = {k: spec[k] for k in
           ("read_block_bytes", "write_buf_bytes", "merge_chunk_bytes",
            "max_fan_in", "checkpoint_input_bytes") if k in spec}
    code = 2
    try:
        if spec["cmd"] == "new":
            jid = runner.submit_resumable_sort(
                spec["input"], spec["output"], spec["state_dir"],
                job_id=spec.get("task_id"),
                dedup_key=spec.get("dedup", False),
                max_line_bytes=spec.get("max_line_bytes", 256 * 1024),
                hooks=hooks, **tun)
        else:
            jid = runner.resume_sort(spec["state_dir"], hooks=hooks, **tun)
        state["jid"] = jid
        jid_set.set()
        print("SUBMITTED " + jid, flush=True)
        st = runner.wait(jid, 900)
        print("STATUS " + json.dumps(st, default=str), flush=True)
        code = {"DONE": 0, "FAILED": 1, "ERROR": 4, "CANCELLED": 5}.get(
            st["state"], 1)
    except (ResumeError, ConfigError) as exc:
        print("REFUSED " + json.dumps(
            {"error": f"{type(exc).__name__}: {exc}"}), flush=True)
        code = 3
    finally:
        runner.shutdown()
    return code


if __name__ == "__main__":
    raise SystemExit(main())
