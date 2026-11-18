"""Group-quantiles scale: hot group > B and many small groups.

All data is generated at runtime into a temp dir (nothing committed).  The
budget B is far smaller than the input so every job spills and multi-pass
merges.  Checks:

  * per-event replay of total_reserved <= B (the arbiter invariant);
  * output equals the independent Fraction reference, streamed row by row;
  * known ranks/sources/row counts for the single-hot-group case;
  * a single input organise pass is shared by every requested bp (one parse,
    spills/merge independent of the bp count);
  * reservations return to zero and RSS/budget are reported separately.
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import check_invariants, new_workspace

from extsort import JobRunner
from gq_ref import expected_rows

B = 1 << 20                       # 1 MiB hard budget
CFG = dict(max_line_bytes=4096, read_block_bytes=8192, write_buf_bytes=65536,
           merge_chunk_bytes=16384, max_fan_in=4, run_record_capacity=200)
BPS = [0, 1000, 2500, 5000, 7500, 9000, 9999, 10000]


def _assert_event_budget(runner):
    violations = check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    assert not violations, violations[:2]
    for e in runner.events.snapshot():
        t = e.get("total_reserved")
        assert t is None or t <= B, e
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0


def test_single_hot_group_larger_than_budget():
    d = new_workspace("gq-hot")
    inp = os.path.join(d, "hot.jsonl")
    n = 60_000                       # ~1.5MB of JSONL, ~1.5x B even before keys
    rng = random.Random(123)
    with open(inp, "w", encoding="utf-8") as f:
        for i in range(n):
            # wide value spread incl. negatives and fraction spellings
            v = rng.randint(-1_000_000, 1_000_000)
            token = str(v) if rng.random() < 0.8 else f"{v}.0"
            f.write('{"text":"hot","value":%s}\n' % token)
    size = os.path.getsize(inp)
    assert size > B, f"input {size} must exceed budget {B}"
    runner = JobRunner(B, os.path.join(d, "work"))
    out = os.path.join(d, "hot.out")
    jid = runner.submit_group_quantiles(inp, out, BPS, job_id="HOT", **CFG)
    st = runner.wait(jid, 180)
    assert st["state"] == "DONE", st
    assert st["spills"] > 1, st                       # really externalised
    assert st["groups"] == 1 and st["lines_read"] == n
    _assert_event_budget(runner)

    # streaming check against the Fraction reference
    exp_rows, err = expected_rows(inp, BPS)
    assert err is None
    with open(out, encoding="utf-8") as f:
        got = [json.loads(line) for line in f]
    assert len(got) == len(exp_rows) == len(BPS)
    for g, e in zip(got, exp_rows):
        assert g == e, (g, e)
    # known endpoint ranks/sources
    assert got[0]["bp"] == 0 and got[0]["rank"] == 1 and got[0]["n"] == n
    assert got[-1]["bp"] == 10000 and got[-1]["rank"] == n
    # multiple bp may share a rank; rank values are monotone non-decreasing
    ranks = [r["rank"] for r in got]
    assert ranks == sorted(ranks) and all(1 <= r <= n for r in ranks)

    # a second job requesting a single bp performs the same organise work:
    # spills and merge records are independent of the number of bps
    out1 = os.path.join(d, "hot1.out")
    jid1 = runner.submit_group_quantiles(inp, out1, [5000], job_id="HOT1", **CFG)
    st1 = runner.wait(jid1, 180)
    assert st1["state"] == "DONE", st1
    assert st1["spills"] == st["spills"], "bp count must not change organise work"
    assert st1["records_merged"] == st["records_merged"]
    single = [json.loads(l) for l in open(out1, encoding="utf-8")]
    assert single == [r for r in exp_rows if r["bp"] == 5000]
    _assert_event_budget(runner)
    rep = runner.report()
    assert rep["high_water_bytes"] <= B
    assert rep["peak_rss_bytes"] > rep["high_water_bytes"]   # RSS vs B distinct
    runner.shutdown()
    print(f"PASS single_hot_group_larger_than_budget "
          f"(input={size}B {size/B:.1f}x B, spills={st['spills']}, rows={len(got)})")


def test_many_small_groups():
    d = new_workspace("gq-many")
    inp = os.path.join(d, "many.jsonl")
    n_groups = 8_000
    per = 4
    rng = random.Random(99)
    with open(inp, "w", encoding="utf-8") as f:
        for g in range(n_groups):
            # unicode-heavy distinct keys to exercise codepoint ordering
            key = f"k-{g:05d}-中"
            for j in range(per):
                f.write(json.dumps({"text": key, "value": rng.randint(-99, 99)},
                                   ensure_ascii=False) + "\n")
    assert os.path.getsize(inp) > B
    runner = JobRunner(B, os.path.join(d, "work"))
    out = os.path.join(d, "many.out")
    jid = runner.submit_group_quantiles(inp, out, BPS, job_id="MANY", **CFG)
    st = runner.wait(jid, 240)
    assert st["state"] == "DONE", st
    assert st["groups"] == n_groups
    assert st["results_written"] == n_groups * len(BPS)
    _assert_event_budget(runner)

    # streaming reference comparison plus ordering contract
    exp_rows, err = expected_rows(inp, BPS)
    assert err is None
    prev_key, prev_bp = None, None
    seen = 0
    with open(out, encoding="utf-8") as f:
        for line, e in zip(f, exp_rows):
            row = json.loads(line)
            assert row == e, (row, e)
            if prev_key is not None:
                assert row["text"] > prev_key or (
                    row["text"] == prev_key and row["bp"] > prev_bp)
            prev_key, prev_bp = row["text"], row["bp"]
            seen += 1
    assert seen == len(exp_rows)
    rep = runner.report()
    assert rep["high_water_bytes"] <= B
    runner.shutdown()
    print(f"PASS many_small_groups ({n_groups} groups, {seen} rows,"
          f" spills={st['spills']}, passes={st['merge_passes']})")


def test_repeated_bp_shares_one_pass_via_events():
    # prove via the event stream that input is parsed once regardless of bp
    d = new_workspace("gq-share")
    inp = os.path.join(d, "s.jsonl")
    with open(inp, "w") as f:
        for i in range(2_000):
            f.write('{"text":"g%d","value":%d}\n' % (i % 50, i))
    runner = JobRunner(B, os.path.join(d, "work"))
    out = os.path.join(d, "s.out")
    many_bps = list(range(0, 10001, 250))      # 41 distinct bps
    jid = runner.submit_group_quantiles(inp, out, many_bps, job_id="SH", **CFG)
    st = runner.wait(jid, 120)
    assert st["state"] == "DONE", st
    ev = runner.events.snapshot()
    # exactly one parse phase and one final merge, never one per bp
    phases = [e["phase"] for e in ev if e["kind"] == "JOB_PHASE" and e.get("job") == jid]
    assert phases == ["parse", "merge", "extract", "publish"]
    # each input line produced exactly one record that reached the final merge
    final = [e for e in ev if e["kind"] == "GQ_FINAL_MERGE_DONE" and e.get("job") == jid]
    assert len(final) == 1 and final[0]["records"] == 2_000
    assert st["lines_read"] == 2_000
    assert st["results_written"] == 50 * len(many_bps)
    _assert_event_budget(runner)
    runner.shutdown()
    print("PASS repeated_bp_shares_one_pass_via_events")


if __name__ == "__main__":
    test_single_hot_group_larger_than_budget()
    test_many_small_groups()
    test_repeated_bp_shares_one_pass_via_events()
    print("ALL GQ SCALE TESTS PASSED")
