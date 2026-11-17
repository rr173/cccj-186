"""Edge cases:
  * empty input            -> empty output, DONE
  * oversized line         -> job fails (no truncation, no output), sibling OK
  * hot duplicate keys     -> dedup keeps the earliest original line per key
  * merge fan-in > buffer  -> multi-pass merge, still correct
  * config rejection       -> MWS > budget rejected at submit
  * unicode keys           -> codepoint order + stability vs sorted()
"""
import json
import os

from util import make_input, check_invariants, new_workspace

from extsort import JobRunner, ConfigError
from extsort import verify

MWS = 368768
CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4)


def test_empty_input():
    d = new_workspace("empty")
    inp = os.path.join(d, "empty.jsonl")
    open(inp, "w").close()
    runner = JobRunner(2 * MWS, os.path.join(d, "work"))
    j = runner.submit(inp, os.path.join(d, "out", "e.sorted"), job_id="E", **CFG)
    st = runner.wait(j, 30)
    assert st["state"] == "DONE", st
    assert st["lines_written"] == 0
    assert os.path.getsize(os.path.join(d, "out", "e.sorted")) == 0
    assert verify.check_exact(inp, os.path.join(d, "out", "e.sorted"), False)["ok"]
    runner.shutdown()
    print("PASS empty_input")


def test_oversized_line():
    d = new_workspace("toolong")
    inp = os.path.join(d, "t.jsonl")
    with open(inp, "w") as f:
        for i in range(50):
            f.write(json.dumps({"text": f"k{i}", "payload": {"orig": i}}) + "\n")
        f.write(json.dumps({"text": "fat", "payload": {"orig": 50,
                                                        "pad": "x" * 5000}}) + "\n")
        for i in range(51, 100):
            f.write(json.dumps({"text": f"k{i}", "payload": {"orig": i}}) + "\n")
    inp_ok = make_input(os.path.join(d, "ok.jsonl"), 300, seed=1, pad=(0, 60))
    runner = JobRunner(2 * MWS + 102400, os.path.join(d, "work"))
    jt = runner.submit(inp, os.path.join(d, "out", "t.sorted"), job_id="T", **CFG)
    jo = runner.submit(inp_ok, os.path.join(d, "out", "ok.sorted"), job_id="OK", **CFG)
    st = runner.wait(jt, 30)
    so = runner.wait(jo, 30)
    assert st["state"] == "FAILED", st
    assert "max_line_bytes" in (st["error"] or ""), st["error"]
    # no truncation, no partial output published
    assert not os.path.exists(os.path.join(d, "out", "t.sorted"))
    assert not os.path.exists(os.path.join(d, "out", "t.sorted.tmp.T"))
    # sibling job unaffected
    assert so["state"] == "DONE", so
    assert verify.check_exact(inp_ok, os.path.join(d, "out", "ok.sorted"), False)["ok"]
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS oversized_line")


def test_hot_keys_dedup():
    d = new_workspace("hotkeys")
    # 8 hot keys (incl. unicode), 20k lines: dedup must keep the earliest
    # original line per key; non-dedup must keep all, stably.  Tight geometry
    # (small blocks, fan-in 3) forces a multi-pass merge under dedup too.
    keys = ["hot", "hot2", "中", "🙂", "ä", "z", "alpha", "x"]
    inp = make_input(os.path.join(d, "hot.jsonl"), 20000, keys=keys, seed=5,
                     pad=(0, 40))
    runner = JobRunner(240000, os.path.join(d, "work"))
    cfg = dict(CFG, read_block_bytes=8192, write_buf_bytes=16384,
               merge_chunk_bytes=16384, max_fan_in=3)
    jd = runner.submit(inp, os.path.join(d, "out", "hot.dedup"), job_id="D",
                       dedup_key=True, **cfg)
    jn = runner.submit(inp, os.path.join(d, "out", "hot.sorted"), job_id="N",
                       dedup_key=False, **cfg)
    sd = runner.wait(jd, 60)
    sn = runner.wait(jn, 60)
    assert sd["state"] == "DONE" and sn["state"] == "DONE", (sd, sn)
    assert sd["lines_written"] == len(keys), sd
    passes = [e for e in runner.events.snapshot() if e["kind"] == "MERGE_PASS_DONE"]
    assert passes, "dedup test must also exercise multi-pass merge"
    rd = verify.check_exact(inp, os.path.join(d, "out", "hot.dedup"), True)
    rn = verify.check_exact(inp, os.path.join(d, "out", "hot.sorted"), False)
    assert rd["ok"], rd
    assert rn["ok"], rn
    # explicit check: kept line per key is the earliest original one
    first = {}
    with open(inp, encoding="utf-8") as f:
        for i, line in enumerate(f):
            k = json.loads(line)["text"]
            first.setdefault(k, i)
    with open(os.path.join(d, "out", "hot.dedup"), encoding="utf-8") as f:
        seen = [json.loads(l)["payload"]["orig"] for l in f]
    assert sorted(seen) == sorted(first.values()), (seen, first)
    runner.shutdown()
    print("PASS hot_keys_dedup")


def test_fanin_exceeds_buffer():
    d = new_workspace("fanin")
    # many runs (small blocks + tight budget) + fan-in 3 -> multi-pass merge
    inp = make_input(os.path.join(d, "m.jsonl"), 20000, seed=9, pad=(50, 150))
    runner = JobRunner(240000, os.path.join(d, "work"))
    cfg = dict(CFG, read_block_bytes=8192, write_buf_bytes=16384,
               merge_chunk_bytes=16384, max_fan_in=3)
    j = runner.submit(inp, os.path.join(d, "out", "m.sorted"), job_id="M", **cfg)
    st = runner.wait(j, 120)
    assert st["state"] == "DONE", st
    assert st["spills"] > 3, st
    passes = [e for e in runner.events.snapshot() if e["kind"] == "MERGE_PASS_DONE"]
    assert passes, "expected at least one intermediate merge pass (fan-in < runs)"
    assert verify.check_exact(inp, os.path.join(d, "out", "m.sorted"), False)["ok"]
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    runner.shutdown()
    print(f"PASS fanin_exceeds_buffer (spills={st['spills']}, passes={len(passes)})")


def test_config_rejection():
    d = new_workspace("reject")
    inp = make_input(os.path.join(d, "r.jsonl"), 10, seed=1)
    runner = JobRunner(2 * 1024 * 1024, os.path.join(d, "work"))
    # max_line_bytes=8MB -> MWS far exceeds the 2MB budget: reject at submit
    try:
        runner.submit(inp, os.path.join(d, "out", "r.sorted"),
                      max_line_bytes=8 * 1024 * 1024)
    except ConfigError:
        pass
    else:
        raise AssertionError("oversized MWS config was not rejected")
    # and a sane config still runs afterwards
    j = runner.submit(inp, os.path.join(d, "out", "r.sorted"), **CFG)
    assert runner.wait(j, 30)["state"] == "DONE"
    runner.shutdown()
    print("PASS config_rejection")


def test_unicode_stability():
    d = new_workspace("unicode")
    keys = ["a", "ä", "z", "中", "🙂", "é", "😀", "中中", "🙂🙂", "", None]
    keys = [k for k in keys if k]  # non-empty only
    inp = make_input(os.path.join(d, "u.jsonl"), 6000, keys=keys, seed=13,
                     pad=(0, 30))
    runner = JobRunner(2 * MWS, os.path.join(d, "work"))
    j = runner.submit(inp, os.path.join(d, "out", "u.sorted"), job_id="U", **CFG)
    st = runner.wait(j, 60)
    assert st["state"] == "DONE", st
    assert verify.check_exact(inp, os.path.join(d, "out", "u.sorted"), False)["ok"]
    # explicit codepoint order check (streaming)
    res = verify.check_output_streaming(os.path.join(d, "out", "u.sorted"),
                                        dedup=False)
    assert res["ok"], res
    runner.shutdown()
    print("PASS unicode_stability")


if __name__ == "__main__":
    test_empty_input()
    test_oversized_line()
    test_hot_keys_dedup()
    test_fanin_exceeds_buffer()
    test_config_rejection()
    test_unicode_stability()
    print("ALL EDGE TESTS PASSED")
