"""Interval-join lifecycle tests:

  * cancel at two deterministic join points -- after a hot key's active set has
    spilt to disk, and after some match pairs have been generated but before
    publish -- using hook-based gates (no sleeps);
  * a real injected write failure fails only the interval job, its target is
    never published and its temp files are removed;
  * an interval job runs concurrently with an ordinary sort job under one
    shared arbiter: both complete, reservations return to zero;
  * a late invalid record (near the end of the file) fails the job without
    leaving a successful-prefix output.
"""
import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import make_input, check_invariants, new_workspace  # noqa: E402
from extsort import JobRunner  # noqa: E402
import ij_ref  # noqa: E402

BUDGET = 1024 * 1024
CFG = dict(max_line_bytes=2048, read_block_bytes=8192, write_buf_bytes=16384,
           merge_chunk_bytes=8192, max_fan_in=3, marker_page_bytes=512,
           marker_cache_pages=2, active_flush_bytes=512, max_active_runs=2,
           result_run_bytes=8192)
SORT_CFG = dict(max_line_bytes=2048, read_block_bytes=32768,
                write_buf_bytes=32768, merge_chunk_bytes=32768, max_fan_in=4)


def write(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for o in rows:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")


def dense_inputs(d, n=80, obs=6):
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    left = [{"text": "hot", "start": 0, "end": 1000,
             "payload": "p" * 40 + str(i)} for i in range(n)]
    right = [{"text": "hot", "at": 100 + j * 100, "payload": j}
             for j in range(obs)]
    write(lp, left)
    write(rp, right)
    return lp, rp


class Gate:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()

    def hook(self, job, **kw):
        self.entered.set()
        self.release.wait(30)


def test_cancel_after_hot_spill():
    d = new_workspace("ij-cancel-spill")
    lp, rp = dense_inputs(os.path.join(d, "data"))
    out = os.path.join(d, "out.jsonl")
    gate = Gate()
    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    try:
        cfg = dict(CFG, hooks={"hot_group_spilled": gate.hook})
        jid = runner.submit_interval_join(lp, rp, out, **cfg)
        assert gate.entered.wait(30), "hot group never spilt"
        runner.cancel(jid)
        gate.release.set()
        st = runner.wait(jid, 30)
        assert st["state"] == "CANCELLED", st
        assert st["held_bytes"] == 0
        assert not os.path.exists(out)
        assert not os.path.exists(os.path.join(d, "work", f"job-{jid}"))
        ev = runner.events.snapshot()
        assert not check_invariants(ev, BUDGET)
        assert runner.events.count("DOUBLE_RELEASE") == 0
        assert runner.arbiter.total_reserved == 0
    finally:
        runner.shutdown()
    print("PASS cancel_after_hot_spill")


def test_cancel_after_pairs_before_publish():
    d = new_workspace("ij-cancel-pairs")
    lp, rp = dense_inputs(os.path.join(d, "data"))
    out = os.path.join(d, "out.jsonl")
    pairs_gate = Gate()
    pub_gate = Gate()

    def pairs_hook(job, **kw):
        pairs_gate.entered.set()
        pairs_gate.release.wait(30)

    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    try:
        cfg = dict(CFG, hooks={"pairs_generated": pairs_hook,
                               "before_publish": pub_gate.hook})
        jid = runner.submit_interval_join(lp, rp, out, **cfg)
        assert pairs_gate.entered.wait(30), "no pairs generated"
        runner.cancel(jid)
        pairs_gate.release.set()
        st = runner.wait(jid, 30)
        assert st["state"] == "CANCELLED", st
        assert st["matches"] > 0, st           # pairs existed ...
        assert not os.path.exists(out)         # ... but nothing published
        assert st["held_bytes"] == 0
        assert not os.path.exists(os.path.join(d, "work", f"job-{jid}"))
        assert runner.arbiter.total_reserved == 0
        # make sure the publish gate was not left blocking a finished worker
        pub_gate.release.set()
    finally:
        runner.shutdown()
    print("PASS cancel_after_pairs_before_publish")


def test_write_failure_isolation():
    d = new_workspace("ij-iofail")
    lp, rp = dense_inputs(os.path.join(d, "data"), n=40, obs=3)
    out_bad = os.path.join(d, "bad.jsonl")
    # a healthy interval job alongside the failing one
    lp2, rp2 = dense_inputs(os.path.join(d, "data2"), n=20, obs=2)
    out_good = os.path.join(d, "good.jsonl")
    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    try:
        jb = runner.submit_interval_join(lp, rp, out_bad,
                                         inject_io_failure=True, **CFG)
        jg = runner.submit_interval_join(lp2, rp2, out_good, **CFG)
        sb, sg = runner.wait(jb, 60), runner.wait(jg, 60)
        assert sb["state"] == "FAILED", sb
        assert "injected" in (sb["error"] or ""), sb["error"]
        assert sg["state"] == "DONE", sg
        assert not os.path.exists(out_bad)
        rows = ij_ref.read_output(out_good)
        exp, _ = ij_ref.reference_join(lp2, rp2)
        assert rows == exp
        assert sb["held_bytes"] == 0 and sg["held_bytes"] == 0
        assert runner.events.count("DOUBLE_RELEASE") == 0
        # arbiter still usable
        j3 = runner.submit_interval_join(
            lp2, rp2, os.path.join(d, "good2.jsonl"), **CFG)
        assert runner.wait(j3, 60)["state"] == "DONE"
        assert runner.arbiter.total_reserved == 0
    finally:
        runner.shutdown()
    print("PASS write_failure_isolation")


def test_concurrent_with_sort_job():
    d = new_workspace("ij-concurrent")
    lp, rp = dense_inputs(os.path.join(d, "ijdata"), n=50, obs=4)
    sort_in = make_input(os.path.join(d, "sort.jsonl"), 4000, seed=11,
                         pad=(40, 120))
    sort_out = os.path.join(d, "sorted.jsonl")
    out = os.path.join(d, "joined.jsonl")
    runner = JobRunner(2 * BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    try:
        js = runner.submit(sort_in, sort_out, job_id="S", **SORT_CFG)
        jj = runner.submit_interval_join(lp, rp, out, job_id="J", **CFG)
        ss, sj = runner.wait(js, 120), runner.wait(jj, 120)
        assert ss["state"] == "DONE", ss
        assert sj["state"] == "DONE", sj
        # both outputs are real and independently correct
        from extsort import verify
        assert verify.check_exact(sort_in, sort_out, False)["ok"]
        rows = ij_ref.read_output(out)
        exp, _ = ij_ref.reference_join(lp, rp)
        assert rows == exp
        ev = runner.events.snapshot()
        assert not check_invariants(ev, 2 * BUDGET)
        assert runner.events.count("DOUBLE_RELEASE") == 0
        assert runner.arbiter.total_reserved == 0
        # the two jobs genuinely overlapped under the shared arbiter
        admits = [e for e in ev if e["kind"] == "JOB_ADMIT"]
        dones = [e for e in ev if e["kind"] == "JOB_STATE"
                 and e.get("state") == "DONE"]
        assert len(admits) >= 2 and admits[1]["seq"] < dones[0]["seq"], \
            "jobs did not overlap"
    finally:
        runner.shutdown()
    print("PASS concurrent_with_sort_job")


def test_late_invalid_record_no_prefix():
    d = new_workspace("ij-late")
    lp = os.path.join(d, "l.jsonl")
    rp = os.path.join(d, "r.jsonl")
    out = os.path.join(d, "out.jsonl")
    rows = [{"text": "k", "start": i, "end": i + 5, "payload": i}
            for i in range(40)]
    rows.append({"text": "k", "start": 9, "end": 2, "payload": "bad-order"})
    write(lp, rows)
    write(rp, [{"text": "k", "at": i + 1, "payload": i} for i in range(41)])
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    try:
        jid = runner.submit_interval_join(lp, rp, out, **CFG)
        st = runner.wait(jid, 60)
        assert st["state"] == "FAILED", st
        assert "line 41" in (st["error"] or ""), st["error"]
        # no successful-looking prefix target
        assert not os.path.exists(out)
        assert st["held_bytes"] == 0
        assert runner.arbiter.total_reserved == 0
    finally:
        runner.shutdown()
    print("PASS late_invalid_record_no_prefix")


if __name__ == "__main__":
    test_cancel_after_hot_spill()
    test_cancel_after_pairs_before_publish()
    test_write_failure_isolation()
    test_concurrent_with_sort_job()
    test_late_invalid_record_no_prefix()
    print("ALL INTERVAL LIFECYCLE TESTS PASSED")
