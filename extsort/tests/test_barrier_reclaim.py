"""Barrier test: A and B both hold part of the budget, then simultaneously
request an extra (beyond-envelope) write buffer.  Shows real cross-job
cooperative reclaim, real spills, and a real wait/wake -- all synchronised
with a threading.Barrier, no sleeps.  The budget invariant is checked on
every event.
"""
import os
import threading

from util import make_input, check_invariants, new_workspace

from extsort import JobRunner
from extsort import verify

# geometry (see README): with max_line=2048, 32K buffers/chunks, fan-in 4
#   mws = max(5*32768 + 2*32768, 4*(2*32768+10256+16) + 2*32768) = 368768
MWS = 368768
BUDGET = 2 * MWS + 204800          # both envelopes fit; limited slack
EXTRA = 420 * 1024                 # beyond-envelope extra write buffer:
                                   # > free-after-both-spills, so exactly one
                                   # requester must wait for the other's release
CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4)


def main():
    d = new_workspace("barrier")
    inp_a = make_input(os.path.join(d, "a.jsonl"), 2100, seed=11, pad=(100, 200))
    inp_b = make_input(os.path.join(d, "b.jsonl"), 2100, seed=22, pad=(100, 200))

    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))

    barrier = threading.Barrier(2)

    def sync_hook(job, **kw):
        barrier.wait(30)

    hooks = {"before_extra_buffer": sync_hook}
    ja = runner.submit(inp_a, os.path.join(d, "out", "a.sorted"), job_id="A",
                       extra_write_buffers=1, extra_buffer_bytes=EXTRA,
                       extra_after_blocks=10, hooks=hooks, **CFG)
    jb = runner.submit(inp_b, os.path.join(d, "out", "b.sorted"), job_id="B",
                       extra_write_buffers=1, extra_buffer_bytes=EXTRA,
                       extra_after_blocks=10, hooks=hooks, **CFG)

    sa = runner.wait(ja, 120)
    sb = runner.wait(jb, 120)
    assert sa["state"] == "DONE", sa
    assert sb["state"] == "DONE", sb

    # outputs are real sorted files, checked against in-memory sorted()
    ra = verify.check_exact(inp_a, os.path.join(d, "out", "a.sorted"), False)
    rb = verify.check_exact(inp_b, os.path.join(d, "out", "b.sorted"), False)
    assert ra["ok"], ra
    assert rb["ok"], rb

    ev = runner.events.snapshot()
    violations = check_invariants(ev, BUDGET)
    assert not violations, violations[:3]

    waits = [e for e in ev if e["kind"] == "RESERVE_WAIT_START" and e.get("nbytes") == EXTRA]
    wakes = [e for e in ev if e["kind"] == "RESERVE_WAKE" and e.get("nbytes") == EXTRA]
    reclaims = [e for e in ev if e["kind"] == "RECLAIM_REQUEST"]
    spills = [e for e in ev if e["kind"] == "SPILL_SERIALIZED"]
    grants = [e for e in ev if e["kind"] == "RESERVE_GRANT" and e.get("nbytes") == EXTRA]

    # both extra-buffer requests were made while holding run data
    assert len(grants) == 2, f"expected 2 extra-buffer grants, got {len(grants)}"
    # cooperative reclaim happened with real spills (direction is a race;
    # exactly one requester can be satisfied from reclaimed memory, the other
    # must wait for the first one's phase-end release -- see README)
    assert len(reclaims) >= 1, "expected at least one cooperative reclaim"
    assert any(e.get("victim_revocable", 0) > 0 for e in reclaims), reclaims
    assert len(spills) >= 2, "reclaim/self-spill must have caused real spills"
    # exactly one requester waited and was woken by the other's release
    assert len(waits) == 1, waits
    assert len(wakes) == 1, wakes
    waiter = waits[0]["job"]
    waker = "B" if waiter == "A" else "A"
    assert wakes[0]["job"] == waiter
    # the wake came after the other job released its phase memory
    rel = [e for e in ev if e["kind"] == "JOB_PHASE_RELEASE" and e["job"] == waker]
    assert rel and rel[0]["seq"] < wakes[0]["seq"], "wake must follow the other's release"
    # every wait is preceded by a reclaim attempt (no infinite polling)
    assert any(e["kind"] == "RECLAIM_REQUEST" and e["seq"] < waits[0]["seq"] for e in ev) \
        or any(e["kind"] == "RESERVE_FAIL" and e["seq"] < waits[0]["seq"] for e in ev)

    # concurrency: both jobs were admitted before either finished
    admits = [e for e in ev if e["kind"] == "JOB_ADMIT"]
    dones = [e for e in ev if e["kind"] == "JOB_STATE" and e.get("state") == "DONE"]
    assert len(admits) >= 2 and admits[1]["seq"] < dones[0]["seq"], "jobs must overlap"

    rep = runner.report()
    assert rep["total_reserved_now"] == 0 and rep["outstanding_reservations"] == 0
    assert rep["high_water_bytes"] <= BUDGET
    runner.shutdown()
    print(f"PASS barrier_reclaim  (waiter={waiter}, spills={len(spills)}, "
          f"high_water={rep['high_water_bytes']}/{BUDGET})")


if __name__ == "__main__":
    main()
