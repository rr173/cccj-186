"""Repartition validation, failure isolation and lifecycle.

Contract violations fail the whole job with physical line + reason and never
publish; every pre-commit failure (bad late line, cancel, shard write
failure, manifest failure) leaves a pre-established CURRENT and its shards
byte-for-byte identical.  Also covers MWS rejection, the cancel/commit
boundary on both sides, phase/counter observability and reuse of the arbiter.
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner  # noqa: E402
from extsort.arbiter import ConfigError  # noqa: E402
from util import Gate, check_invariants, new_workspace  # noqa: E402
import rp_ref  # noqa: E402

CFG = dict(max_line_bytes=4096, read_block_bytes=1024, write_buf_bytes=4096,
           merge_chunk_bytes=2048)


def _write_input(path, lines):
    with open(path, "wb") as f:
        for line in lines:
            f.write(line if isinstance(line, bytes) else line.encode())


def _seed_old_dataset(out):
    """Create a pre-existing CURRENT + generation and return their bytes."""
    os.makedirs(os.path.join(out, "generations", "oldgen"))
    part = os.path.join(out, "generations", "oldgen", "part-00000.jsonl")
    with open(part, "wb") as f:
        f.write(b'{"old":true}\n')
    with open(os.path.join(out, "CURRENT"), "wb") as f:
        f.write(b"oldgen\n")
    return (open(os.path.join(out, "CURRENT"), "rb").read(),
            open(part, "rb").read(), part)


def _assert_old_untouched(out, old_current, old_part_bytes, part):
    assert open(os.path.join(out, "CURRENT"), "rb").read() == old_current
    assert open(part, "rb").read() == old_part_bytes
    assert os.listdir(os.path.join(out, "generations")) == ["oldgen"]


def test_contract_violations():
    d = new_workspace("rp-val")
    inp = os.path.join(d, "in.jsonl")
    runner = JobRunner(3_000_000, os.path.join(d, "work"))
    good = b'{"k":"v","x":1}\n'
    # (lines, pointer, expected physical line, reason substring)
    cases = [
        ([b"\n"], "/k", 1, "empty"),
        ([b"   \n"], "/k", 1, "empty"),
        ([b"{not json}\n"], "/k", 1, "JSON"),
        ([b"[1, 2, 3]\n"], "/k", 1, "object"),
        ([b'{"other":1}\n'], "/k", 1, "missing"),
        ([b'{"k":12}\n'], "/k", 1, "string"),
        ([b'{"k":null}\n'], "/k", 1, "string"),
        ([b'{"k":true}\n'], "/k", 1, "string"),
        ([b'{"k":[]}\n'], "/k", 1, "string"),
        ([b'{"a":["x"]}\n', b'{"a":[]}\n'], "/a/0", 2, "out of range"),
        ([b'{"a":1}\n'], "/a/0", 1, "cannot descend"),
        ([b'{"k":NaN}\n'], "/k", 1, "constant"),
        ([b'{"k":Infinity}\n'], "/k", 1, "constant"),
        ([b'{"k":-Infinity}\n'], "/k", 1, "constant"),
        ([good] * 4 + [b'{"k":{}}\n'], "/k", 5, "string"),  # late bad line
    ]
    for i, (lines, pointer, bad_line, reason) in enumerate(cases):
        sub = os.path.join(d, f"c{i}")
        os.makedirs(sub)
        _write_input(os.path.join(sub, "in.jsonl"), lines)
        jid = runner.submit_repartition(
            os.path.join(sub, "in.jsonl"), os.path.join(sub, "ds"),
            pointer, 4, job_id=f"B{i}", **CFG)
        st = runner.wait(jid, 30)
        assert st["state"] == "FAILED", (i, st)
        assert f"line {bad_line}" in st["error"], (i, st["error"])
        assert reason in st["error"], (i, st["error"])
        # nothing published: no CURRENT, no generation, temp spill cleaned
        ds = os.path.join(sub, "ds")
        assert not os.path.exists(os.path.join(ds, "CURRENT"))
        assert not os.path.exists(os.path.join(ds, "generations"))
        assert runner.arbiter.held(jid) == 0
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    runner.shutdown()
    print("PASS contract_violations")


def test_oversized_line():
    d = new_workspace("rp-oversize")
    inp = os.path.join(d, "in.jsonl")
    _write_input(inp, [b'{"k":"a"}\n'] * 3 +
                 [b'{"k":"' + b"x" * 9000 + b'"}\n'])
    runner = JobRunner(3_000_000, os.path.join(d, "work"))
    jid = runner.submit_repartition(inp, os.path.join(d, "ds"), "/k", 4,
                                    max_line_bytes=2048,
                                    read_block_bytes=4096,
                                    write_buf_bytes=4096,
                                    merge_chunk_bytes=2048)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert "line 4" in st["error"] and "max_line_bytes" in st["error"]
    assert not os.path.exists(os.path.join(d, "ds", "CURRENT"))
    assert not os.path.exists(os.path.join(d, "ds", "generations"))
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS oversized_line")


def test_config_rejection():
    d = new_workspace("rp-cfg")
    inp = os.path.join(d, "in.jsonl")
    _write_input(inp, [b'{"k":"v"}\n'])
    runner = JobRunner(3_000_000, os.path.join(d, "work"))
    for kwargs in (
        dict(key_pointer="noslash", num_partitions=4),
        dict(key_pointer="/k", num_partitions=0),
        dict(key_pointer="/k", num_partitions=4097),
        dict(key_pointer="/k~2", num_partitions=4),       # bad escape
    ):
        try:
            runner.submit_repartition(inp, os.path.join(d, "x"),
                                      **kwargs, **CFG)
            raise AssertionError(f"should reject {kwargs}")
        except ConfigError:
            pass
    # max_open_shards out of range
    try:
        runner.submit_repartition(inp, os.path.join(d, "x"), "/k", 4,
                                  max_open_shards=0, **CFG)
        raise AssertionError("mos=0 must be rejected")
    except ConfigError:
        pass
    try:
        runner.submit_repartition(inp, os.path.join(d, "x"), "/k", 4,
                                  max_open_shards=5, **CFG)
        raise AssertionError("mos>N must be rejected")
    except ConfigError:
        pass
    # MWS that cannot fit the budget is rejected at submit
    try:
        runner.submit_repartition(inp, os.path.join(d, "x"), "/k", 4,
                                  max_line_bytes=2 << 20,
                                  read_block_bytes=4 << 20,
                                  write_buf_bytes=4 << 20,
                                  merge_chunk_bytes=4 << 20)
        raise AssertionError("oversized MWS must be rejected")
    except ConfigError as exc:
        assert "minimal working set" in str(exc)
    runner.shutdown()
    print("PASS config_rejection")


def test_cancel_and_failures_keep_old_current():
    d = new_workspace("rp-life")
    inp = os.path.join(d, "in.jsonl")
    _write_input(inp, [json.dumps({"id": i, "k": f"k{i % 29}"}) + "\n"
                       for i in range(3000)])
    runner = JobRunner(3_000_000, os.path.join(d, "work"))

    def fresh_out(tag):
        out = os.path.join(d, tag)
        cur, pb, part = _seed_old_dataset(out)
        return out, cur, pb, part

    # 1. cancel during read/route
    out, cur, pb, part = fresh_out("cancel-route")
    gate = Gate()
    jid = runner.submit_repartition(
        inp, out, "/k", 8, max_open_shards=2, **CFG,
        hooks={"block_read": gate.hook_fn()})
    assert gate.entered.wait(10)
    runner.cancel(jid)
    gate.release.set()
    st = runner.wait(jid, 30)
    assert st["state"] == "CANCELLED", st
    _assert_old_untouched(out, cur, pb, part)
    print("PASS cancel_during_route")

    # 2. cancel immediately before the atomic replace -> no publish
    out, cur, pb, part = fresh_out("cancel-pre")
    gate = Gate()
    jid = runner.submit_repartition(
        inp, out, "/k", 8, max_open_shards=2, **CFG,
        hooks={"before_publish": gate.hook_fn()})
    assert gate.entered.wait(10)
    runner.cancel(jid)
    gate.release.set()
    st = runner.wait(jid, 30)
    assert st["state"] == "CANCELLED", st
    _assert_old_untouched(out, cur, pb, part)
    print("PASS cancel_before_commit")

    # 3. cancel immediately after the replace -> committed, reports DONE
    out, cur, pb, part = fresh_out("cancel-post")
    gate = Gate()
    jid = runner.submit_repartition(
        inp, out, "/k", 8, max_open_shards=2, **CFG,
        hooks={"after_publish": gate.hook_fn()})
    assert gate.entered.wait(10)
    runner.cancel(jid)
    gate.release.set()
    st = runner.wait(jid, 30)
    assert st["state"] == "DONE", st
    assert st["committed"] is True
    new_cur = open(os.path.join(out, "CURRENT"), "rb").read()
    assert new_cur != cur
    # old generation retained byte-for-byte
    assert open(part, "rb").read() == pb
    assert "oldgen" in os.listdir(os.path.join(out, "generations"))
    print("PASS cancel_after_commit")

    # 4. injected shard write failure
    out, cur, pb, part = fresh_out("writefail")
    jid = runner.submit_repartition(inp, out, "/k", 8, max_open_shards=2,
                                   inject_io_failure=True, **CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert "injected" in st["error"]
    _assert_old_untouched(out, cur, pb, part)
    print("PASS shard_write_failure")

    # 5. manifest-stage failure
    out, cur, pb, part = fresh_out("manfail")

    def boom(job, **kw):
        raise OSError("manifest disk full")

    jid = runner.submit_repartition(inp, out, "/k", 8, max_open_shards=2,
                                    hooks={"before_manifest": boom}, **CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert "manifest disk full" in st["error"]
    _assert_old_untouched(out, cur, pb, part)
    print("PASS manifest_failure")

    # 6. late bad line after many routed records
    out, cur, pb, part = fresh_out("latebad")
    bad_in = os.path.join(d, "bad.jsonl")
    _write_input(bad_in, [json.dumps({"id": i, "k": f"k{i % 7}"}) + "\n"
                         for i in range(2000)] +
                 [b'{"id":2000,"k":99}\n'])
    jid = runner.submit_repartition(bad_in, out, "/k", 8,
                                    max_open_shards=2, **CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert "line 2001" in st["error"], st["error"]
    _assert_old_untouched(out, cur, pb, part)
    print("PASS late_bad_line")

    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    runner.shutdown()
    print("PASS cancel_and_failures_keep_old_current")


def test_observability_and_arbiter_reuse():
    d = new_workspace("rp-obs")
    inp = os.path.join(d, "in.jsonl")
    _write_input(inp, [json.dumps({"id": i, "k": f"k{i % 13}"}) + "\n"
                       for i in range(1500)])
    runner = JobRunner(3_000_000, os.path.join(d, "work"))
    jid = runner.submit_repartition(inp, os.path.join(d, "ds"), "/k", 16,
                                    max_open_shards=3, **CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "DONE", st
    phases = [e["phase"] for e in runner.events.snapshot()
              if e["kind"] == "JOB_PHASE" and e.get("job") == jid]
    # read -> route -> merge -> manifest -> publish
    assert phases == ["read", "route", "merge", "manifest", "publish"], phases
    for key in ("lines_read", "records_routed", "input_bytes",
                "shard_bytes_written", "open_shards_high_water", "evictions",
                "flushes", "segment_merges", "held_bytes", "mws",
                "num_partitions", "max_open_shards", "generation_id",
                "committed", "phase", "state"):
        assert key in st, key
    assert st["lines_read"] == 1500 == st["records_routed"]
    assert st["open_shards_high_water"] <= 3
    rep = runner.report()
    assert rep["high_water_bytes"] <= rep["budget_bytes"]
    assert "peak_rss_bytes" in rep
    assert "python_metadata_estimate_bytes" in rep
    # arbiter reusable after a completed job, reservations back to zero
    jid2 = runner.submit_repartition(inp, os.path.join(d, "ds2"), "/k", 8,
                                     max_open_shards=2, **CFG)
    st2 = runner.wait(jid2, 30)
    assert st2["state"] == "DONE", st2
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS observability_and_arbiter_reuse")


def test_reclaim_handler_on_foreign_thread():
    """The registered cooperative-reclaim handler must run on another job's
    thread without deadlock, actually free budget, and leave the final output
    correct.  The gate sits on block_read, which fires outside the route lock,
    so the cache holds dirty shard buffers when the handler is invoked."""
    import threading
    d = new_workspace("rp-reclaim-handler")
    inp = os.path.join(d, "in.jsonl")
    _write_input(inp, [json.dumps({"id": i, "k": f"k{i % 200:03d}",
                                   "pad": "z" * 80}) + "\n"
                       for i in range(8000)])
    budget = 900 * 1024
    runner = JobRunner(budget, os.path.join(d, "work"))
    gate = Gate()
    jid = runner.submit_repartition(
        inp, os.path.join(d, "ds"), "/k", 32, max_open_shards=8,
        max_line_bytes=4096, read_block_bytes=16384, write_buf_bytes=32768,
        merge_chunk_bytes=16384,
        hooks={"block_read": gate.hook_fn()})
    assert gate.entered.wait(30)
    arb = runner.arbiter
    held_before = arb.held(jid)
    handler = arb._jobs[jid]["handler"]
    assert handler is not None
    result = {}

    def foreign():
        result["reclaimed"] = handler(budget)

    t = threading.Thread(target=foreign, name="reclaim-test")
    t.start()
    t.join(15)
    assert not t.is_alive(), "reclaim handler deadlocked"
    assert result["reclaimed"] > 0
    assert arb.held(jid) < held_before
    gate.release.set()
    st = runner.wait(jid, 120)
    assert st["state"] == "DONE", st
    rp_ref.verify_dataset(os.path.join(d, "ds"), inp, "/k", 32)
    assert not check_invariants(runner.events.snapshot(), budget)
    assert arb.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    runner.shutdown()
    print("PASS reclaim_handler_on_foreign_thread")


def test_sibling_failure_isolation():
    d = new_workspace("rp-sibling")
    good = os.path.join(d, "good.jsonl")
    bad = os.path.join(d, "bad.jsonl")
    _write_input(good, [json.dumps({"id": i, "k": f"k{i % 5}"}) + "\n"
                        for i in range(800)])
    _write_input(bad, [b'{"k":"v"}\n'] * 20 + [b'{"k":1}\n'])
    runner = JobRunner(3_000_000, os.path.join(d, "work"))
    jg = runner.submit_repartition(good, os.path.join(d, "gds"), "/k", 6,
                                   job_id="G", max_open_shards=2, **CFG)
    jb = runner.submit_repartition(bad, os.path.join(d, "bds"), "/k", 6,
                                   job_id="B", max_open_shards=2, **CFG)
    sg, sb = runner.wait(jg, 60), runner.wait(jb, 60)
    assert sg["state"] == "DONE", sg
    assert sb["state"] == "FAILED" and "line 21" in sb["error"], sb
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS sibling_failure_isolation")


if __name__ == "__main__":
    test_contract_violations()
    test_oversized_line()
    test_config_rejection()
    test_cancel_and_failures_keep_old_current()
    test_observability_and_arbiter_reuse()
    test_reclaim_handler_on_foreign_thread()
    test_sibling_failure_isolation()
    print("ALL REPARTITION LIFECYCLE TESTS PASSED")
