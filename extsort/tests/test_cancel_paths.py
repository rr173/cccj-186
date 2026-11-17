"""Cancellation tests -- cancel(job_id) landing in three distinct places:

  1. while the job is blocked waiting for a quota (reserve wait);
  2. while the job's cooperative-reclaim callback is executing;
  3. while one of the job's writes is issued but not yet confirmed.

In every case the cancelled job must unwind itself, its reservations must go
to zero (each released exactly once -- DOUBLE_RELEASE must stay 0), late I/O
completions must not re-acquire or publish, and the other jobs must complete
for real (outputs checked against in-memory sorted()).
"""
import os
import threading

from util import make_input, check_invariants, new_workspace, Gate

from extsort import JobRunner
from extsort import verify

MWS = 368768
CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4)


def wait_event(runner, kind, job=None, timeout=30, **fields):
    """Deterministic wait for an event to appear (condition-driven, no sleep
    loop longer than 1ms granularity)."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for e in runner.events.snapshot():
            if e["kind"] != kind:
                continue
            if job is not None and e.get("job") != job:
                continue
            if all(e.get(k) == v for k, v in fields.items()):
                return e
        time.sleep(0.001)
    raise AssertionError(f"event {kind} job={job} {fields} never appeared")


# ---------------------------------------------------------------- case 1
def test_cancel_while_waiting_for_quota():
    d = new_workspace("cancel-wait")
    inp_a = make_input(os.path.join(d, "a.jsonl"), 2100, seed=1, pad=(100, 200))
    inp_b = make_input(os.path.join(d, "b.jsonl"), 300, seed=2, pad=(0, 60))
    runner = JobRunner(2 * MWS + 204800, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))

    # A will request a huge beyond-envelope extra buffer while alone -> no
    # reclaim victims -> it parks in the reserve wait (timeout 1h).
    # EXTRA > free-at-request-time (~745K) so the request genuinely blocks.
    EXTRA = 800000
    waited = threading.Event()
    runner.arbiter.on_wait_start = lambda jid, n: waited.set()

    ja = runner.submit(inp_a, os.path.join(d, "out", "a.sorted"), job_id="A",
                       extra_write_buffers=1, extra_buffer_bytes=EXTRA,
                       extra_after_blocks=4, **{**CFG, "extra_buffer_timeout": 3600.0})
    assert waited.wait(30), "A never reached the reserve wait"
    wait_event(runner, "RESERVE_WAIT_START", job="A", nbytes=EXTRA)
    runner.cancel(ja)
    sa = runner.wait(ja, 30)
    assert sa["state"] == "CANCELLED", sa
    assert sa["held_bytes"] == 0, sa
    assert runner.arbiter.total_reserved == 0
    assert not os.path.exists(os.path.join(d, "out", "a.sorted"))
    assert not os.path.exists(os.path.join(d, "work", "job-A")), "temp dir not cleaned"

    # arbiter is fully usable afterwards: B runs to completion
    jb = runner.submit(inp_b, os.path.join(d, "out", "b.sorted"), job_id="B", **CFG)
    sb = runner.wait(jb, 60)
    assert sb["state"] == "DONE", sb
    assert verify.check_exact(inp_b, os.path.join(d, "out", "b.sorted"), False)["ok"]

    ev = runner.events.snapshot()
    assert not check_invariants(ev, runner.arbiter.budget)
    aborts = [e for e in ev if e["kind"] == "RESERVE_ABORT" and e["job"] == "A"]
    assert aborts, "A must have been woken out of the reserve wait by cancel"
    runner.shutdown()
    print("PASS cancel_while_waiting_for_quota")


# ---------------------------------------------------------------- case 2
def test_cancel_during_reclaim_callback():
    d = new_workspace("cancel-reclaim")
    inp_a = make_input(os.path.join(d, "a.jsonl"), 2100, seed=3, pad=(100, 200))
    inp_b = make_input(os.path.join(d, "b.jsonl"), 400, seed=4, pad=(0, 60))
    inp_c = make_input(os.path.join(d, "c.jsonl"), 300, seed=5, pad=(0, 60))
    runner = JobRunner(2 * MWS + 204800, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))

    gate_a = Gate()       # keeps A's run data held (revocable) after 6 blocks
    gate_reclaim = Gate() # parks A's reclaim callback while we cancel A

    def a_block_hook(job, blocks, **kw):
        if blocks == 6:
            gate_a.entered.set()
            gate_a.release.wait(30)

    def reclaim_enter(job, **kw):
        gate_reclaim.entered.set()
        gate_reclaim.release.wait(30)

    ja = runner.submit(inp_a, os.path.join(d, "out", "a.sorted"), job_id="A",
                       hooks={"block_read": a_block_hook,
                              "reclaim_enter": reclaim_enter}, **CFG)

    def c_extra_hook(job, **kw):
        # C must not request its extra buffer before A has accumulated its
        # revocable run (otherwise the request is granted without reclaim)
        gate_a.entered.wait(30)

    # C requests a huge extra buffer -> must reclaim A; A's callback parks.
    jc = runner.submit(inp_c, os.path.join(d, "out", "c.sorted"), job_id="C",
                       extra_write_buffers=1, extra_buffer_bytes=700 * 1024,
                       extra_after_blocks=1,
                       hooks={"before_extra_buffer": c_extra_hook},
                       **{**CFG, "extra_buffer_timeout": 3600.0})
    jb = runner.submit(inp_b, os.path.join(d, "out", "b.sorted"), job_id="B", **CFG)

    assert gate_a.entered.wait(30), "A never accumulated its run"
    assert gate_reclaim.entered.wait(30), "A's reclaim callback never started"

    runner.cancel(ja)                 # cancel lands INSIDE the reclaim callback
    gate_reclaim.release.set()        # let the callback observe the cancel
    gate_a.release.set()              # let A's worker unwind

    sa = runner.wait(ja, 30)
    assert sa["state"] == "CANCELLED", sa
    assert sa["held_bytes"] == 0
    sc = runner.wait(jc, 60)
    sb = runner.wait(jb, 60)
    assert sc["state"] == "DONE", sc
    assert sb["state"] == "DONE", sb
    assert verify.check_exact(inp_c, os.path.join(d, "out", "c.sorted"), False)["ok"]
    assert verify.check_exact(inp_b, os.path.join(d, "out", "b.sorted"), False)["ok"]

    ev = runner.events.snapshot()
    assert not check_invariants(ev, runner.arbiter.budget)
    # cancel was requested while the reclaim callback was executing
    req = next(e for e in ev if e["kind"] == "RECLAIM_REQUEST" and e["job"] == "A")
    res = next(e for e in ev if e["kind"] == "RECLAIM_RESULT" and e["job"] == "A")
    can = next(e for e in ev if e["kind"] == "CANCEL_REQUEST" and e["job"] == "A")
    assert req["seq"] < can["seq"] < res["seq"], "cancel must land inside the callback"
    assert res.get("reclaimed") == 0, "cancelled victim must not publish reclaimed spill"
    # A's late releases happened exactly once
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS cancel_during_reclaim_callback")


# ---------------------------------------------------------------- case 3
def test_cancel_during_inflight_write():
    d = new_workspace("cancel-write")
    inp_a = make_input(os.path.join(d, "a.jsonl"), 2100, seed=6, pad=(100, 200))
    inp_b = make_input(os.path.join(d, "b.jsonl"), 400, seed=7, pad=(0, 60))
    inp_c = make_input(os.path.join(d, "c.jsonl"), 300, seed=8, pad=(0, 60))
    runner = JobRunner(3 * MWS + 204800, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))

    gate_io = Gate()

    def before_write(job_id, tmp_path):
        # park A's *output* write (issued, not yet confirmed)
        if job_id == "A" and tmp_path.endswith(".tmp.A"):
            gate_io.entered.set()
            gate_io.release.wait(30)

    runner.engine.before_write_hook = before_write

    ja = runner.submit(inp_a, os.path.join(d, "out", "a.sorted"), job_id="A", **CFG)
    jb = runner.submit(inp_b, os.path.join(d, "out", "b.sorted"), job_id="B", **CFG)
    jc = runner.submit(inp_c, os.path.join(d, "out", "c.sorted"), job_id="C", **CFG)

    assert gate_io.entered.wait(60), "A's output write never went in flight"
    runner.cancel(ja)                 # cancel while the write is unconfirmed
    gate_io.release.set()             # let the late write complete

    sa = runner.wait(ja, 30)
    assert sa["state"] == "CANCELLED", sa
    assert sa["held_bytes"] == 0
    sb = runner.wait(jb, 60)
    sc = runner.wait(jc, 60)
    assert sb["state"] == "DONE" and sc["state"] == "DONE", (sb, sc)
    assert verify.check_exact(inp_b, os.path.join(d, "out", "b.sorted"), False)["ok"]
    assert verify.check_exact(inp_c, os.path.join(d, "out", "c.sorted"), False)["ok"]

    ev = runner.events.snapshot()
    assert not check_invariants(ev, runner.arbiter.budget)
    can = next(e for e in ev if e["kind"] == "CANCEL_REQUEST" and e["job"] == "A")
    # the late write completed AFTER the cancel request ...
    late = [e for e in ev if e["kind"] == "IO_WRITE_DONE" and e["job"] == "A"
            and e["seq"] > can["seq"]]
    assert late, "expected a late in-flight write completion for A"
    # ... but nothing was published and no output file exists
    pubs = [e for e in ev if e["kind"] in ("IO_PUBLISH", "OUTPUT_PUBLISH")
            and e["job"] == "A" and e["seq"] > can["seq"]]
    assert not pubs, "cancelled job must not publish results"
    assert not os.path.exists(os.path.join(d, "out", "a.sorted"))
    assert not os.path.exists(os.path.join(d, "out", "a.sorted.tmp.A"))
    # the late completion did not re-acquire quota
    re_grants = [e for e in ev if e["kind"] == "RESERVE_GRANT" and e["job"] == "A"
                 and e["seq"] > can["seq"]]
    assert not re_grants, "cancelled job must not be granted new reservations"
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS cancel_during_inflight_write")


if __name__ == "__main__":
    test_cancel_while_waiting_for_quota()
    test_cancel_during_reclaim_callback()
    test_cancel_during_inflight_write()
    print("ALL CANCEL TESTS PASSED")
