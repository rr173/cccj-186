"""Group-quantiles input validation and failure isolation.

Every contract violation (bool/null/string value, NaN/Infinity, missing field,
empty line, invalid JSON, oversized line, non-object line, late bad record)
fails the whole job, names the 1-based physical line, never publishes a prefix
and leaves an existing output byte-for-byte untouched.  Sibling jobs and the
arbiter stay usable.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import check_invariants, new_workspace

from extsort import JobRunner

CFG = dict(max_line_bytes=2048, read_block_bytes=4096, write_buf_bytes=32768,
           merge_chunk_bytes=8192, max_fan_in=4, run_record_capacity=5)


def _runner(d, budget=1_500_000):
    return JobRunner(budget, os.path.join(d, "work"))


def _expect_failure(d, raw_lines, bad_line, reason_substr, *, budget=1_500_000):
    os.makedirs(d, exist_ok=True)
    inp = os.path.join(d, "in.jsonl")
    out = os.path.join(d, "out.jsonl")
    old_target = b'{"preexisting":true}\n'
    with open(out, "wb") as f:
        f.write(old_target)
    with open(inp, "w", encoding="utf-8") as f:
        f.writelines(raw_lines)
    runner = _runner(d, budget)
    jid = runner.submit_group_quantiles(inp, out, [0, 5000, 10000],
                                        job_id="BAD", **CFG)
    st = runner.wait(jid, 30)
    assert st["state"] == "FAILED", st
    assert f"line {bad_line}" in (st["error"] or ""), st["error"]
    assert reason_substr in (st["error"] or ""), st["error"]
    # target untouched, no tmp left, temp dir cleaned
    assert open(out, "rb").read() == old_target
    assert not os.path.exists(out + ".tmp.BAD")
    assert not os.path.exists(os.path.join(d, "work", "job-BAD"))
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    runner.shutdown()


def test_bad_values():
    d = new_workspace("gq-badval")
    good = ['{"text":"k","value":1}\n', '{"text":"k","value":2}\n']
    cases = [
        ('{"text":"k","value":true}\n', "number"),
        ('{"text":"k","value":false}\n', "number"),
        ('{"text":"k","value":null}\n', "number"),
        ('{"text":"k","value":"1"}\n', "number"),
        ('{"text":"k","value":[1]}\n', "number"),
        ('{"text":"k","value":{"x":1}}\n', "number"),
        ('{"text":"k","value":NaN}\n', "constant"),
        ('{"text":"k","value":Infinity}\n', "constant"),
        ('{"text":"k","value":-Infinity}\n', "constant"),
        ('{"text":"k"}\n', "value"),
        ('{"value":1}\n', "text"),
        ('{"text":"","value":1}\n', "text"),
        ('{"text":null,"value":1}\n', "text"),
        ('{"text":12,"value":1}\n', "text"),
        ('[1,2,3]\n', "object"),
        ('{"text":"k","value":1}\n{not json}\n', "JSON"),
    ]
    for i, (bad, reason) in enumerate(cases):
        sub = os.path.join(d, f"c{i}")
        os.makedirs(sub, exist_ok=True)
        # place the bad record late (after several good ones) to prove there
        # is no partial result and the line number is physical, not relative
        if reason == "JSON":
            # bare malformed JSON must be the first line for this case
            lines = [bad]
            expected_line = 1
        else:
            lines = good * 3 + [bad]
            expected_line = 7
        _expect_failure(sub, lines, expected_line, reason)
    print("PASS bad_values (all value/type/contract violations)")


def test_empty_and_malformed_lines():
    d = new_workspace("gq-emptyline")
    _expect_failure(os.path.join(d, "a"),
                    ['{"text":"k","value":1}\n', "\n",
                     '{"text":"k","value":2}\n'], 2, "empty")
    _expect_failure(os.path.join(d, "b"),
                    ['{"text":"k","value":1}\n', "   \n"], 2, "empty")
    _expect_failure(os.path.join(d, "c"),
                    ['{"text":"k","value":1}\n', '{"text":"k",}\n'], 2, "JSON")
    print("PASS empty_and_malformed_lines")


def test_oversized_line():
    d = new_workspace("gq-long")
    long_line = '{"text":"fat","value":1,"pad":"' + "x" * 4000 + '"}\n'
    lines = ['{"text":"k","value":1}\n'] * 4 + [long_line]
    _expect_failure(d, lines, 5, "max_line_bytes")
    print("PASS oversized_line")


def test_late_bad_record_after_spills():
    # bad line only after enough good records to force real spills/merge work
    d = new_workspace("gq-late")
    lines = ['{"text":"k%02d","value":%d}\n' % (i % 9, i - 50) for i in range(200)]
    lines = [l for l in lines]
    lines.append('{"text":"k","value":null}\n')          # physical line 201
    _expect_failure(d, lines, 201, "number")
    print("PASS late_bad_record_after_spills")


def test_sibling_job_unaffected():
    d = new_workspace("gq-sibling")
    bad_in = os.path.join(d, "bad.jsonl")
    good_in = os.path.join(d, "good.jsonl")
    with open(bad_in, "w") as f:
        f.writelines(['{"text":"k","value":1}\n'] * 10)
        f.write('{"text":"k","value":NaN}\n')
    with open(good_in, "w") as f:
        for i in range(60):
            f.write('{"text":"g","value":%d}\n' % i)
    runner = JobRunner(2_000_000, os.path.join(d, "work"))
    bad_out = os.path.join(d, "bad.jsonl.out")
    good_out = os.path.join(d, "good.jsonl.out")
    open(bad_out, "wb").write(b"OLD-BAD\n")
    jb = runner.submit_group_quantiles(bad_in, bad_out, [5000], job_id="B", **CFG)
    jg = runner.submit_group_quantiles(good_in, good_out, [0, 10000], job_id="G", **CFG)
    sb = runner.wait(jb, 30)
    sg = runner.wait(jg, 30)
    assert sb["state"] == "FAILED" and "constant" in sb["error"], sb
    assert sg["state"] == "DONE", sg
    assert open(bad_out, "rb").read() == b"OLD-BAD\n"
    rows = [json.loads(l) for l in open(good_out, encoding="utf-8")]
    assert len(rows) == 2 and rows[0]["source_line"] == 1
    assert rows[1]["source_line"] == 60 and rows[1]["value_token"] == "59"
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS sibling_job_unaffected")


def test_arbiter_reusable_after_failure():
    d = new_workspace("gq-reuse")
    bad_in = os.path.join(d, "bad.jsonl")
    open(bad_in, "w").write('{"text":"k","value":"nope"}\n')
    runner = JobRunner(1_500_000, os.path.join(d, "work"))
    jb = runner.submit_group_quantiles(bad_in, os.path.join(d, "b.out"),
                                       [5000], job_id="B", **CFG)
    assert runner.wait(jb, 30)["state"] == "FAILED"
    good_in = os.path.join(d, "good.jsonl")
    with open(good_in, "w") as f:
        for i in range(20):
            f.write('{"text":"g","value":%d}\n' % i)
    jg = runner.submit_group_quantiles(good_in, os.path.join(d, "g.out"),
                                       [5000], job_id="G", **CFG)
    sg = runner.wait(jg, 30)
    assert sg["state"] == "DONE", sg
    assert sg["results_written"] == 1
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS arbiter_reusable_after_failure")


if __name__ == "__main__":
    test_bad_values()
    test_empty_and_malformed_lines()
    test_oversized_line()
    test_late_bad_record_after_spills()
    test_sibling_job_unaffected()
    test_arbiter_reusable_after_failure()
    print("ALL GQ VALIDATION TESTS PASSED")
