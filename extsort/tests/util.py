"""Shared test helpers: input generation, event-log invariant checks."""
from __future__ import annotations

import json
import os
import random
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def make_input(path, n_lines, *, keys=None, seed=0, pad=(0, 120), line_prefix=""):
    """Generate a JSONL input.  Every line carries payload.orig = line number
    so stability can be checked independently."""
    rng = random.Random(seed)
    keys = keys or ["alpha", "beta", "gamma", "delta", "中", "🙂", "ä", "z"]
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n_lines):
            obj = {"text": rng.choice(keys),
                   "payload": {"orig": i,
                               "pad": line_prefix + "x" * rng.randint(*pad)}}
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    return path


def check_invariants(events, budget, *, allow=()):
    """Per-event budget-identity check: every event's total_reserved snapshot
    must satisfy 0 <= total <= budget, and no forbidden event kinds appear."""
    violations = []
    for e in events:
        t = e.get("total_reserved")
        if t is not None and not (0 <= t <= budget):
            violations.append(("budget", e))
        if e["kind"] in ("DOUBLE_RELEASE", "RECLAIM_ERROR") and e["kind"] not in allow:
            violations.append((e["kind"], e))
    return violations


def assert_no_violations(events, budget):
    v = check_invariants(events, budget)
    assert not v, f"invariant violations: {v[:3]}"
    # budget identity must end clean
    finals = [e for e in events if e["kind"] == "JOB_STATE"]
    assert finals, "no terminal job states in log"
    last = events[-1]
    assert last.get("total_reserved", 0) == 0, f"reservations leaked: {last}"


class Gate:
    """Deterministic test synchronisation (no sleeps): an Event pair."""

    def __init__(self):
        import threading
        self.entered = threading.Event()
        self.release = threading.Event()

    def hook_fn(self):
        def _hook(*a, **kw):
            self.entered.set()
            self.release.wait(30)
        return _hook


def new_workspace(name):
    d = tempfile.mkdtemp(prefix=f"extsort-{name}-")
    os.makedirs(os.path.join(d, "out"), exist_ok=True)
    return d
