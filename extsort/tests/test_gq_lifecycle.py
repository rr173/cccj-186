"""Group-quantiles lifecycle: cancellation, real write failure and concurrency.

Deterministic hooks gate the worker at parse/spill and at extract so a
cancellation lands inside a known phase.  A real injected OSError on the first
write fails only this job.  A group-quantile job and an ordinary sort run
concurrently under one arbiter and both finish; reservations return to zero.
Every failing case pre-creates an old target and asserts its bytes are intact.
"""
import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import Gate, check_invariants, new_workspace, make_input

from extsort import JobRunner
from extsort import verify

GQ_CFG = dict(max_line_bytes=2048, read_block_bytes=4096, write_buf_bytes=32768,
              merge_chunk_bytes=8192, max_fan_in=4, run_record_capacity=10)
SORT_CFG = dict(max_line_bytes=2048, read_block_bytes=8192, write_buf_bytes=32768,
                merge_chunk_bytes=16384, max_fan_in=4)


def _gq_input(path, n=400, groups=8):
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write('{"text":"g%02d","value":%d}\n' % (i % groups, i - n // 2))


def test_cancel_during_parse():
    d = new_workspace("gq-cancel-parse")
    inp = os.path.join(d, "in.jsonl")
    _gq_input(inp, 500)
    out = os.path.join(d, "out.jsonl")
    open(out, "wb").write(b"OLD-TARGET\n")
    gate = Gate()
    runner = JobRunner(2_000_000, os.path.join(d, "work"))
    jid = runner.submit_group_quantiles(
        inp, out, [0, 5000, 10000], job_id="C",
        hooks={"block_read": gate.hook_fn()}, **GQ_CFG)
    assert gate.entered.wait(10)
    runner.cancel(jid)
    gate.release.set()
    st = runner.wait(jid, 30)
    assert st["state"] == "CANCELLED", st
    assert open(out, "rb").read() == b"OLD-TARGET\n"
    assert not os.path.exists(out + ".tmp.C")
    assert not os.path.exists(os.path.join(d, "work", "job-C"))
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    runner.shutdown()
    print("PASS cancel_during_parse")


def test_cancel_during_extract():
    d = new_workspace("gq-cancel-extract")
    inp = os.path.join(d, "in.jsonl")
    _gq_input(inp, 600)
    out = os.path.join(d, "out.jsonl")
    open(out, "wb").write(b"OLD-TARGET\n")
    gate = Gate()
    runner = JobRunner(2_000_000, os.path.join(d, "work"))
    jid = runner.submit_group_quantiles(
        inp, out, list(range(0, 10001, 500)), job_id="CE",
        hooks={"group_done": gate.hook_fn()}, **GQ_CFG)
    assert gate.entered.wait(10)
    runner.cancel(jid)
    gate.release.set()
    st = runner.wait(jid, 30)
    assert st["state"] in ("CANCELLED", "FAILED"), st
    assert open(out, "rb").read() == b"OLD-TARGET\n"
    assert not os.path.exists(os.path.join(d, "work", "job-CE"))
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS cancel_during_extract")


def test_real_write_failure():
    d = new_workspace("gq-writefail")
    inp = os.path.join(d, "in.jsonl")
    _gq_input(inp, 300)
    out = os.path.join(d, "out.jsonl")
    open(out, "wb").write(b"OLD-TARGET\n")
    runner = JobRunner(2_000_000, os.path.join(d, "work"))
    jid = runner.submit_group_quantiles(
        inp, out, [0, 5000, 10000], job_id="WF",
        inject_io_failure=True, **GQ_CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert "injected" in (st["error"] or ""), st["error"]
    assert open(out, "rb").read() == b"OLD-TARGET\n"
    assert not os.path.exists(os.path.join(d, "work", "job-WF"))
    assert runner.arbiter.total_reserved == 0
    fails = [e for e in runner.events.snapshot() if e["kind"] == "IO_WRITE_FAIL"]
    assert fails, "expected an injected write-failure event"
    runner.shutdown()
    print("PASS real_write_failure")


def test_concurrent_with_plain_sort():
    d = new_workspace("gq-concurrent")
    gq_in = os.path.join(d, "gq.jsonl")
    sort_in = os.path.join(d, "sort.jsonl")
    _gq_input(gq_in, 800)
    make_input(sort_in, 4000, seed=11, pad=(20, 120))
    gq_out = os.path.join(d, "gq.out")
    sort_out = os.path.join(d, "sort.out")
    # tight shared budget so both jobs really contend for the arbiter
    runner = JobRunner(2_200_000, os.path.join(d, "work"))
    jq = runner.submit_group_quantiles(
        gq_in, gq_out, [0, 2500, 5000, 7500, 10000], job_id="Q", **GQ_CFG)
    js = runner.submit(sort_in, sort_out, job_id="S", **SORT_CFG)
    sq = runner.wait(jq, 120)
    ss = runner.wait(js, 120)
    assert sq["state"] == "DONE", sq
    assert ss["state"] == "DONE", ss
    # quantile output still matches the independent Fraction reference
    from gq_ref import expected_output_bytes
    assert open(gq_out, "rb").read() == expected_output_bytes(
        gq_in, [0, 2500, 5000, 7500, 10000])
    # ordinary sort output independently verified
    assert verify.check_exact(sort_in, sort_out, False)["ok"]
    ev = runner.events.snapshot()
    assert not check_invariants(ev, runner.arbiter.budget)
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    # both made progress under contention: reclaim activity is possible and
    # the quantile job really spilled instead of holding everything
    assert sq["spills"] >= 1 and ss["spills"] >= 1
    runner.shutdown()
    print("PASS concurrent_with_plain_sort")


def test_cancel_while_waiting_for_admission():
    # block the job in begin_phase by holding the budget elsewhere, then cancel
    d = new_workspace("gq-cancel-admit")
    inp = os.path.join(d, "in.jsonl")
    _gq_input(inp, 50)
    budget = 900_000
    runner = JobRunner(budget, os.path.join(d, "work"))
    arb = runner.arbiter
    holder = "holder"
    env = budget - 100_000
    arb.register_job(holder, env)
    arb.begin_phase(holder, env)
    hold = arb.reserve(holder, env, "buffer")
    try:
        jid = runner.submit_group_quantiles(
            inp, os.path.join(d, "o.jsonl"), [5000], job_id="CW", **GQ_CFG)
        assert runner._jobs[jid].mws > budget - env
        threading.Event().wait(0.3)
        runner.cancel(jid)
        st = runner.wait(jid, 10)
        assert st["state"] == "CANCELLED", st
        assert runner.arbiter.total_reserved == env   # only the holder remains
    finally:
        arb.release(hold)
        arb.end_phase(holder)
        arb.unregister_job(holder)
    # arbiter reusable after the admission-wait cancellation
    j2 = runner.submit_group_quantiles(
        inp, os.path.join(d, "o2.jsonl"), [5000], job_id="CW2", **GQ_CFG)
    assert runner.wait(j2, 30)["state"] == "DONE"
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS cancel_while_waiting_for_admission")


def test_phase_observability():
    d = new_workspace("gq-phases")
    inp = os.path.join(d, "in.jsonl")
    _gq_input(inp, 300)
    runner = JobRunner(2_000_000, os.path.join(d, "work"))
    jid = runner.submit_group_quantiles(
        inp, os.path.join(d, "o.jsonl"), [0, 5000, 10000], job_id="P", **GQ_CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "DONE", st
    phases = [e["phase"] for e in runner.events.snapshot()
              if e["kind"] == "JOB_PHASE" and e.get("job") == jid]
    # parse -> merge -> extract -> publish, in order
    assert phases == ["parse", "merge", "extract", "publish"], phases
    for key in ("lines_read", "groups", "records_merged", "results_written",
                "bytes_read", "bytes_written", "spills", "merge_passes",
                "held_bytes", "mws"):
        assert key in st, key
    assert st["lines_read"] == 300 and st["groups"] == 8
    assert st["results_written"] == 8 * 3
    rep = runner.report()
    assert rep["high_water_bytes"] <= rep["budget_bytes"]
    assert "peak_rss_bytes" in rep and "python_metadata_estimate_bytes" in rep
    runner.shutdown()
    print("PASS phase_observability")


if __name__ == "__main__":
    test_cancel_during_parse()
    test_cancel_during_extract()
    test_real_write_failure()
    test_concurrent_with_plain_sort()
    test_cancel_while_waiting_for_admission()
    test_phase_observability()
    print("ALL GQ LIFECYCLE TESTS PASSED")
