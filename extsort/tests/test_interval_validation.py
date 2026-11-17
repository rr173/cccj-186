"""Interval-join input-contract validation tests.

Every malformed input must fail the job (no partial output published) with an
error naming the side and the 1-based physical line number; a late bad record
must still be caught (no successful prefix); valid sibling jobs and the shared
arbiter must stay usable.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner, IntervalInputError  # noqa: E402

BUDGET = 1024 * 1024
CFG = dict(max_line_bytes=2048, read_block_bytes=8192, write_buf_bytes=16384,
           merge_chunk_bytes=8192, max_fan_in=3, marker_page_bytes=512,
           marker_cache_pages=2, active_flush_bytes=4096, max_active_runs=2,
           result_run_bytes=8192)


def write(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            if isinstance(r, str):
                f.write(r if r.endswith("\n") else r + "\n")
            else:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def good_left(n=20):
    return [{"text": "k", "start": i, "end": i + 5, "payload": i}
            for i in range(n)]


def good_right(n=20):
    return [{"text": "k", "at": i + 1, "payload": i} for i in range(n)]


def run_expect_fail(d, left_rows, right_rows, *, side, lineno, reason_part):
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    out = os.path.join(d, "out.jsonl")
    write(lp, left_rows)
    write(rp, right_rows)
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    try:
        jid = runner.submit_interval_join(lp, rp, out, **CFG)
        st = runner.wait(jid, 60)
        assert st["state"] == "FAILED", st
        err = st["error"] or ""
        assert side in err and f"line {lineno}" in err, err
        assert reason_part in err, err
        # nothing published, no tmp left, temp dir cleaned
        assert not os.path.exists(out), out
        assert not os.path.exists(out + f".tmp.{jid}")
        assert not os.path.exists(os.path.join(d, "work", f"job-{jid}"))
        assert st["held_bytes"] == 0
        assert runner.events.count("DOUBLE_RELEASE") == 0
        return runner, st
    finally:
        runner.shutdown()


def test_blank_and_bad_json():
    d = "/tmp/ij-val-blank"
    # blank line on the left at physical line 2
    left = [json.dumps(good_left()[0]), "", json.dumps(good_left()[1])]
    runner, _ = run_expect_fail(
        d, left, [json.dumps(r) for r in good_right()],
        side="left", lineno=2, reason_part="empty line")
    # invalid JSON on right line 2
    d2 = "/tmp/ij-val-json"
    right = [json.dumps(good_right()[0]), "{not json", json.dumps(good_right()[1])]
    run_expect_fail(d2, good_left(), right, side="right", lineno=2,
                    reason_part="invalid JSON")
    print("PASS blank_and_bad_json")


def test_field_types():
    base_l = good_left()
    base_r = good_right()
    # bool and float must be rejected (only true signed int64 accepted)
    bad_left = [dict(o) for o in base_l]
    bad_left[5]["start"] = True
    run_expect_fail("/tmp/ij-val-bool", bad_left, base_r,
                    side="left", lineno=6, reason_part="start")
    bad_left2 = [dict(o) for o in base_l]
    bad_left2[5]["end"] = 1.5
    run_expect_fail("/tmp/ij-val-float", bad_left2, base_r,
                    side="left", lineno=6, reason_part="end")
    bad_right = [dict(o) for o in base_r]
    bad_right[7]["at"] = False
    run_expect_fail("/tmp/ij-val-rbool", base_l, bad_right,
                    side="right", lineno=8, reason_part="at")
    # missing field
    miss = [dict(o) for o in base_l]
    del miss[4]["end"]
    run_expect_fail("/tmp/ij-val-miss", miss, base_r,
                    side="left", lineno=5, reason_part="end")
    # empty / non-string text
    no_text = [dict(o) for o in base_l]
    no_text[3]["text"] = ""
    run_expect_fail("/tmp/ij-val-text", no_text, base_r,
                    side="left", lineno=4, reason_part="text")
    print("PASS field_types")


def test_int64_range_and_ordering():
    INT_MIN, INT_MAX = -(2 ** 63), 2 ** 63 - 1
    # out of range
    oor = [dict(o) for o in good_left()]
    oor[2]["end"] = INT_MAX + 1
    run_expect_fail("/tmp/ij-val-oor", oor, good_right(),
                    side="left", lineno=3, reason_part="64-bit")
    # start > end
    bad_order = [dict(o) for o in good_left()]
    bad_order[1]["start"], bad_order[1]["end"] = 10, 4
    run_expect_fail("/tmp/ij-val-order", bad_order, good_right(),
                    side="left", lineno=2, reason_part="start 10 > end 4")
    # boundary values are legal and run fine
    d = "/tmp/ij-val-ok"; os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    write(lp, [{"text": "k", "start": INT_MIN, "end": INT_MAX},
               {"text": "k", "start": 5, "end": 5}])  # empty interval legal
    write(rp, [{"text": "k", "at": INT_MIN}, {"text": "k", "at": INT_MAX}])
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    try:
        jid = runner.submit_interval_join(lp, rp, os.path.join(d, "o.jsonl"), **CFG)
        st = runner.wait(jid, 60)
        assert st["state"] == "DONE", st
        # INT_MIN..INT_MAX matches only the at=INT_MIN observation; the empty
        # interval matches nothing; at=INT_MAX finds no containing interval.
        assert (st["matches"], st["left_only"], st["right_only"]) == (1, 1, 1), st
    finally:
        runner.shutdown()
    print("PASS int64_range_and_ordering")


def test_oversized_line_and_late_failure():
    d = "/tmp/ij-val-late"
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    out = os.path.join(d, "out.jsonl")
    # a long line past max_line_bytes near the end of the file
    rows = good_left(30)
    rows.append({"text": "k", "start": 0, "end": 1, "pad": "x" * 5000})
    write(lp, rows)
    write(rp, good_right())
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    try:
        jid = runner.submit_interval_join(lp, rp, out, **CFG)
        st = runner.wait(jid, 60)
        assert st["state"] == "FAILED", st
        assert "max_line_bytes" in (st["error"] or ""), st["error"]
        assert "left line 31" in (st["error"] or ""), st["error"]
        assert not os.path.exists(out)
        # the long line may contain valid JSON but still be rejected
    finally:
        runner.shutdown()
    print("PASS oversized_line_and_late_failure")


def test_failure_isolation_and_arbiter_reuse():
    d = "/tmp/ij-val-iso"
    os.makedirs(d, exist_ok=True)
    bad_lp, rp = os.path.join(d, "badl.jsonl"), os.path.join(d, "r.jsonl")
    good_lp = os.path.join(d, "goodl.jsonl")
    out_bad, out_good = os.path.join(d, "bad.jsonl"), os.path.join(d, "good.jsonl")
    write(bad_lp, good_left()[:5] + [{"text": "k", "start": 9, "end": 1}])
    write(good_lp, good_left())
    write(rp, good_right())
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    try:
        jb = runner.submit_interval_join(bad_lp, rp, out_bad, **CFG)
        jg = runner.submit_interval_join(good_lp, rp, out_good, **CFG)
        sb, sg = runner.wait(jb, 60), runner.wait(jg, 60)
        assert sb["state"] == "FAILED", sb
        assert sg["state"] == "DONE", sg
        assert not os.path.exists(out_bad) and os.path.exists(out_good)
        # arbiter reusable after the failure
        j2 = runner.submit_interval_join(good_lp, rp,
                                         os.path.join(d, "good2.jsonl"), **CFG)
        s2 = runner.wait(j2, 60)
        assert s2["state"] == "DONE", s2
        assert runner.arbiter.total_reserved == 0
        assert runner.events.count("DOUBLE_RELEASE") == 0
    finally:
        runner.shutdown()
    print("PASS failure_isolation_and_arbiter_reuse")


def test_error_type():
    err = IntervalInputError("left", 7, "boom")
    assert err.side == "left" and err.lineno == 7 and err.reason == "boom"
    assert "left line 7: boom" in str(err)
    print("PASS error_type")


if __name__ == "__main__":
    test_blank_and_bad_json()
    test_field_types()
    test_int64_range_and_ordering()
    test_oversized_line_and_late_failure()
    test_failure_isolation_and_arbiter_reuse()
    test_error_type()
    print("ALL INTERVAL VALIDATION TESTS PASSED")
