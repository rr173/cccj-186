"""Group-quantiles correctness vs an independent Fraction reference.

The reference (gq_ref.py) imports no product code: it parses with the stdlib,
converts every decimal to fractions.Fraction and applies nearest-rank itself.
Tests cover negatives, integer/fraction spellings, adjacent integers beyond
binary-float precision, extreme exponents, equal-value lexemes, repeats,
Unicode text, bp=0/10000, several bp collapsing onto one rank and empty input.
Outputs are compared byte-for-byte across read-block / run / fan-in geometries.
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import check_invariants, new_workspace

from extsort import JobRunner, ConfigError
from gq_ref import expected_output_bytes, expected_rows, read_records

BPS = [0, 1, 2500, 5000, 7500, 9999, 10000]


def _write(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def _run(d, inp, bps, budget=2_000_000, job_id="Q", **cfg):
    runner = JobRunner(budget, os.path.join(d, "work"))
    out = os.path.join(d, f"out-{job_id}.jsonl")
    base = dict(max_line_bytes=4096, read_block_bytes=4096,
                write_buf_bytes=32768, merge_chunk_bytes=8192,
                max_fan_in=4, run_record_capacity=4)
    base.update(cfg)
    jid = runner.submit_group_quantiles(inp, out, bps, job_id=job_id, **base)
    st = runner.wait(jid, 60)
    return runner, out, st


def test_hand_crafted_edge_values():
    d = new_workspace("gq-edge")
    lines = [
        '{"text":"neg","value":-100000000000000000000}\n',
        '{"text":"neg","value":-1}\n',
        '{"text":"neg","value":-0.5}\n',
        '{"text":"neg","value":-1e-300}\n',
        '{"text":"eq","value":1}\n',
        '{"text":"eq","value":1.0}\n',
        '{"text":"eq","value":1e0}\n',
        '{"text":"eq","value":1.00}\n',
        '{"text":"big","value":9007199254740992}\n',      # 2^53
        '{"text":"big","value":9007199254740993}\n',      # adjacent, differs exactly
        '{"text":"big","value":9007199254740994}\n',
        '{"text":"exp","value":1e300}\n',
        '{"text":"exp","value":9.99e299}\n',
        '{"text":"exp","value":1e-300}\n',
        '{"text":"mix","value":3}\n',
        '{"text":"mix","value":3.5}\n',
        '{"text":"mix","value":-2}\n',
        '{"text":"mix","value":0}\n',
        '{"text":"mix","value":-0.0}\n',
        '{"text":"uni中","value":7}\n',
        '{"text":"uni中","value":7}\n',                     # repeat
        '{"text":"🙂","value":42}\n',
        '{"text":"a","value":1e10}\n',
        '{"text":"A","value":1e10}\n',
    ]
    inp = os.path.join(d, "in.jsonl")
    _write(inp, lines)
    runner, out, st = _run(d, inp, BPS)
    assert st["state"] == "DONE", st
    got = open(out, "rb").read()
    exp = expected_output_bytes(inp, BPS)
    assert got == exp, (got, exp)
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    runner.shutdown()
    print("PASS hand_crafted_edge_values")


def test_randomized_against_fraction():
    rng = random.Random(7)
    d = new_workspace("gq-rand")
    keys = ["k", "a", "中", "🙂", "zzz", "eta", "alpha"]
    spell_pool = []
    for _ in range(400):
        m = rng.randint(-10 ** 12, 10 ** 12)
        k = rng.randint(0, 9)
        s = str(m)
        if k and len(s) > k + (1 if m < 0 else 0):
            s = s[:-k] + "." + s[-k:]
        token = s
        style = rng.randint(0, 3)
        if style == 1:
            token = f"{s}e{rng.randint(-6, 6)}"
        elif style == 2 and "." not in s:
            token = s + ".0"
        spell_pool.append(token)
    lines = []
    for i in range(1500):
        # value is a raw JSON number literal; text is properly JSON-encoded
        key = json.dumps(rng.choice(keys), ensure_ascii=False)
        lines.append('{"text":' + key + ',"value":' + rng.choice(spell_pool)
                     + ',"seq":' + str(i) + "}\n")
    inp = os.path.join(d, "in.jsonl")
    _write(inp, lines)

    geometries = [
        dict(read_block_bytes=2048, write_buf_bytes=16384,
             merge_chunk_bytes=4096, max_fan_in=3, run_record_capacity=5),
        dict(read_block_bytes=8192, write_buf_bytes=65536,
             merge_chunk_bytes=16384, max_fan_in=8, run_record_capacity=50),
        dict(read_block_bytes=4096, write_buf_bytes=32768,
             merge_chunk_bytes=8192, max_fan_in=2, run_record_capacity=13),
        dict(read_block_bytes=16384, write_buf_bytes=131072,
             merge_chunk_bytes=32768, max_fan_in=16, run_record_capacity=0),
    ]
    expected = expected_output_bytes(inp, BPS)
    outputs = set()
    for gi, geo in enumerate(geometries):
        runner, out, st = _run(d, inp, BPS, job_id=f"G{gi}",
                               budget=3_000_000, **geo)
        assert st["state"] == "DONE", st
        got = open(out, "rb").read()
        assert got == expected, f"geometry {gi} diverged from Fraction reference"
        outputs.add(got)
        assert runner.arbiter.total_reserved == 0
        assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
        runner.shutdown()
    assert len(outputs) == 1, "outputs must be byte-identical across geometries"
    # row count contract: one row per (group, bp)
    rows, err = expected_rows(inp, BPS)
    assert err is None
    n_groups = len({r["text"] for r in rows})
    assert len(rows) == n_groups * len(BPS)
    print(f"PASS randomized_against_fraction ({n_groups} groups, {len(rows)} rows,"
          f" {len(geometries)} geometries byte-identical)")


def test_bp_collapse_and_endpoints():
    d = new_workspace("gq-bp")
    # n=4: bp 0..2499 -> rank 1; several bps deliberately collapse
    lines = [
        '{"text":"g","value":10}\n',
        '{"text":"g","value":20}\n',
        '{"text":"g","value":30}\n',
        '{"text":"g","value":40}\n',
    ]
    inp = os.path.join(d, "in.jsonl")
    _write(inp, lines)
    bps = [0, 1, 2499, 2500, 5000, 7500, 10000]
    runner, out, st = _run(d, inp, bps)
    assert st["state"] == "DONE", st
    got = [json.loads(l) for l in open(out, encoding="utf-8")]
    exp = [json.loads(l) for l in expected_output_bytes(inp, bps).decode().splitlines()]
    assert got == exp
    # explicit endpoint/rank assertions
    by_bp = {r["bp"]: r for r in got}
    assert by_bp[0]["rank"] == 1 and by_bp[0]["source_line"] == 1
    assert by_bp[1]["rank"] == 1 and by_bp[2499]["rank"] == 1
    assert by_bp[2500]["rank"] == 1
    assert by_bp[5000]["rank"] == 2
    assert by_bp[7500]["rank"] == 3
    assert by_bp[10000]["rank"] == 4 and by_bp[10000]["source_line"] == 4
    runner.shutdown()
    print("PASS bp_collapse_and_endpoints")


def test_empty_input():
    d = new_workspace("gq-empty")
    inp = os.path.join(d, "empty.jsonl")
    open(inp, "w").close()
    runner, out, st = _run(d, inp, BPS)
    assert st["state"] == "DONE", st
    assert st["groups"] == 0 and st["results_written"] == 0
    assert os.path.getsize(out) == 0
    assert expected_output_bytes(inp, BPS) == b""
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS empty_input")


def test_tie_keeps_earliest_physical_line():
    d = new_workspace("gq-tie")
    # values 5 (five spellings) then 6; ranks inside the equal-5 run each pick
    # that rank position's record -- and because every member is equal, the
    # rank-1 record (the minimum) must be line 1 with its original spelling.
    lines = ['{"text":"t","value":' + v + "}\n"
             for v in ("5", "5.0", "5e0", "5.00", "0.5e1")]
    # six records total: five equal 5's (lines 1-5) then a 6 (line 6); bp 0 and
    # 5000 land on the value-5 run -> line 1, bp 10000 lands on the 6 -> line 6
    lines.append('{"text":"t","value":6}\n')
    inp = os.path.join(d, "in.jsonl")
    _write(inp, lines)
    bps = [0, 5000, 10000]
    runner, out, st = _run(d, inp, bps)
    assert st["state"] == "DONE", st
    got = [json.loads(l) for l in open(out, encoding="utf-8")]
    assert open(out, "rb").read() == expected_output_bytes(inp, bps)
    # bp 0 and 5000 land on the value-5 run -> minimum line 1, spelling "5"
    by_bp = {r["bp"]: r for r in got}
    for bp in (0, 5000):
        assert by_bp[bp]["source_line"] == 1, got
        assert by_bp[bp]["value_token"] == "5", got
    # maximum rank is the single 6 on line 6
    assert by_bp[10000]["source_line"] == 6 and by_bp[10000]["value_token"] == "6"

    # explicit tie case: duplicate equal values straddle the median rank;
    # the source for a rank landing on the duplicated value must be the
    # smallest physical line among the records occupying that value run.
    d2 = new_workspace("gq-tie2")
    inp2 = os.path.join(d2, "in.jsonl")
    vals = ["1", "2", "2", "2", "3"]          # n=5, value 2 on lines 2,3,4
    _write(inp2, ['{"text":"t","value":' + v + "}\n" for v in vals])
    runner2, out2, st2 = _run(d2, inp2, [5000], job_id="T2")
    assert st2["state"] == "DONE", st2
    row = json.loads(open(out2, encoding="utf-8").read())
    # median rank 3 sits inside the value-2 run; earliest line of that run = 2
    assert row["rank"] == 3 and row["source_line"] == 2 and row["value_token"] == "2"
    assert open(out2, "rb").read() == expected_output_bytes(inp2, [5000])
    runner.shutdown()
    runner2.shutdown()
    print("PASS tie_keeps_earliest_physical_line")


def test_config_rejection():
    d = new_workspace("gq-cfg")
    inp = os.path.join(d, "in.jsonl")
    _write(inp, ['{"text":"x","value":1}\n'])
    runner = JobRunner(4096, os.path.join(d, "work"))
    try:
        runner.submit_group_quantiles(inp, os.path.join(d, "o.jsonl"), [5000],
                                      max_line_bytes=1 << 20)
    except ConfigError:
        pass
    else:
        raise AssertionError("MWS > budget must be rejected at submit")
    # bad percentile requests are rejected before any work starts
    for bad in ([], [0, 0], [-1], [10001], [1.5], ["5000"]):
        try:
            runner.submit_group_quantiles(inp, os.path.join(d, "o2.jsonl"), bad)
        except ConfigError:
            continue
        raise AssertionError(f"bad bp spec accepted: {bad!r}")
    runner.shutdown()
    print("PASS config_rejection")


if __name__ == "__main__":
    test_hand_crafted_edge_values()
    test_randomized_against_fraction()
    test_bp_collapse_and_endpoints()
    test_empty_input()
    test_tie_keeps_earliest_physical_line()
    test_config_rejection()
    print("ALL GQ CORRECTNESS TESTS PASSED")
