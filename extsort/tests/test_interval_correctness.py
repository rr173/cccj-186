"""Interval-join correctness, part 1:

  * the hand-computable headline example (4 match, 1 left_only, 1 right_only,
    exact source rows and exact order);
  * random small inputs compared record-by-record with the independent
    nested-loop oracle (ij_ref), covering negatives, int64 endpoints, Unicode
    keys, duplicate content, empty files, single-side keys, overlaps and
    equal endpoints;
  * the same inputs under several buffer geometries (different block sizes,
    fan-in, marker/active thresholds) -> identical output every time;
  * the sparse-work bound candidate_checks <= 64*N for N disjoint intervals
    with exactly one observation each.
"""
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner  # noqa: E402
import ij_ref  # noqa: E402

# tight geometries so even small inputs force spills, multi-pass merges and
# the active-run/marker paging paths
GEOMETRIES = [
    dict(max_line_bytes=4096, read_block_bytes=8192, write_buf_bytes=8192,
         merge_chunk_bytes=8192, max_fan_in=3, marker_page_bytes=512,
         marker_cache_pages=2, active_flush_bytes=4096, max_active_runs=3,
         result_run_bytes=16384),
    dict(max_line_bytes=2048, read_block_bytes=4096, write_buf_bytes=16384,
         merge_chunk_bytes=4096, max_fan_in=2, marker_page_bytes=256,
         marker_cache_pages=3, active_flush_bytes=2048, max_active_runs=2,
         result_run_bytes=8192),
    dict(max_line_bytes=8192, read_block_bytes=32768, write_buf_bytes=16384,
         merge_chunk_bytes=16384, max_fan_in=4, marker_page_bytes=1024,
         marker_cache_pages=2, active_flush_bytes=8192, max_active_runs=4,
         result_run_bytes=32768),
]
BUDGET = 1024 * 1024


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for o in rows:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")


def run_join(work, left, right, out, geom):
    runner = JobRunner(BUDGET, work)
    try:
        jid = runner.submit_interval_join(left, right, out, **geom)
        st = runner.wait(jid, 120)
        assert st["state"] == "DONE", st
        assert runner.arbiter.total_reserved == 0
        assert runner.events.count("DOUBLE_RELEASE") == 0
        return st
    finally:
        runner.shutdown()


def test_hand_example():
    d = "/tmp/ij-test-hand"
    os.makedirs(d, exist_ok=True)
    left = os.path.join(d, "left.jsonl")
    right = os.path.join(d, "right.jsonl")
    out = os.path.join(d, "out.jsonl")
    write_jsonl(left, [
        {"text": "k", "start": 0, "end": 10, "payload": "L1"},
        {"text": "k", "start": 5, "end": 15, "payload": "L2"},
        {"text": "k", "start": 10, "end": 10, "payload": "L3-empty"},
    ])
    write_jsonl(right, [
        {"text": "k", "at": 0, "payload": "R1"},
        {"text": "k", "at": 5, "payload": "R2"},
        {"text": "k", "at": 10, "payload": "R3"},
        {"text": "k", "at": 15, "payload": "R4"},
    ])
    st = run_join(os.path.join(d, "work"), left, right, out, GEOMETRIES[0])
    rows = ij_ref.read_output(out)
    expected = [
        ("match", 1, 1), ("match", 1, 2), ("match", 2, 2), ("match", 2, 3),
        ("left_only", 3, None), ("right_only", None, 4),
    ]
    got = [(r["kind"], r["left_row"], r["right_row"]) for r in rows]
    assert got == expected, got
    assert (st["matches"], st["left_only"], st["right_only"]) == (4, 1, 1), st
    # traceability: side objects are the original objects verbatim
    m0 = rows[0]
    assert m0["left"]["payload"] == "L1" and m0["right"]["payload"] == "R1"
    assert rows[4]["left"]["payload"] == "L3-empty" and rows[4]["right"] is None
    assert rows[5]["right"]["payload"] == "R4" and rows[5]["left"] is None
    print("PASS hand_example (4 match, 1 left_only, 1 right_only, exact order)")


def _gen_random(seed):
    rng = random.Random(seed)
    keys = ["a", "b", "z", "ä", "中", "🙂", "key-9", ""]
    keys = [k for k in keys if k]
    INT_MIN, INT_MAX = -(2 ** 63), 2 ** 63 - 1
    nL = rng.randint(0, 80)
    nR = rng.randint(0, 80)
    left, right = [], []
    for i in range(nL):
        k = rng.choice(keys)
        # mix of small ranges, wide ranges, empty intervals, extreme values
        mode = rng.random()
        if mode < 0.1:
            start = rng.choice([INT_MIN, -1000, -1, 0, INT_MAX])
            end = start                       # legal empty interval
        elif mode < 0.2:
            start = rng.choice([INT_MIN, -5, 0])
            end = rng.choice([start, INT_MAX, 10 ** 12])
        else:
            start = rng.randint(-50, 50)
            end = start + rng.randint(0, 12)
        left.append({"text": k, "start": start, "end": end,
                     "payload": {"orig": i, "dup": rng.randint(0, 3)}})
    for i in range(nR):
        k = rng.choice(keys + ["only-right-键"])
        at = rng.choice([INT_MIN, INT_MAX, -51, -50, -1, 0, 1, 50, 51,
                         rng.randint(-60, 60)])
        right.append({"text": k, "at": at,
                      "payload": {"orig": i, "dup": rng.randint(0, 3)}})
    # duplicate identical content rows are distinct source rows
    if left:
        left.append(dict(left[rng.randrange(len(left))]))
    if right:
        right.append(dict(right[rng.randrange(len(right))]))
    rng.shuffle(left)
    rng.shuffle(right)
    return left, right


def test_random_against_oracle():
    n_geoms = 0
    for seed in range(40):
        d = f"/tmp/ij-test-rand-{seed}"
        os.makedirs(d, exist_ok=True)
        lp = os.path.join(d, "left.jsonl")
        rp = os.path.join(d, "right.jsonl")
        left, right = _gen_random(seed)
        write_jsonl(lp, left)
        write_jsonl(rp, right)
        expected, estats = ij_ref.reference_join(lp, rp)
        for gi, geom in enumerate(GEOMETRIES):
            out = os.path.join(d, f"out-{gi}.jsonl")
            st = run_join(os.path.join(d, f"work-{gi}"), lp, rp, out, geom)
            got = ij_ref.read_output(out)
            assert got == expected, (
                f"seed {seed} geom {gi} mismatch\n"
                f" exp={expected[:8]}\n got={got[:8]}")
            errs, _ = ij_ref.check_output_contract(got)
            assert not errs, (seed, gi, errs)
            assert (st["matches"], st["left_only"], st["right_only"]) == (
                estats["matches"], estats["left_only"], estats["right_only"]), st
            n_geoms += 1
    print(f"PASS random_against_oracle (40 seeds x {len(GEOMETRIES)} geometries "
          f"= {n_geoms} runs, all byte-semantic-equal to nested-loop oracle)")


def test_empty_and_single_side():
    d = "/tmp/ij-test-empty"
    os.makedirs(d, exist_ok=True)
    lp = os.path.join(d, "left.jsonl")
    rp = os.path.join(d, "right.jsonl")
    open(lp, "w").close()
    open(rp, "w").close()
    out = os.path.join(d, "out.jsonl")
    st = run_join(os.path.join(d, "w0"), lp, rp, out, GEOMETRIES[0])
    assert os.path.getsize(out) == 0
    assert (st["matches"], st["left_only"], st["right_only"]) == (0, 0, 0)
    # right-only keys
    write_jsonl(rp, [{"text": "x", "at": 1, "payload": 1},
                     {"text": "y", "at": 2, "payload": 2}])
    out2 = os.path.join(d, "out2.jsonl")
    st = run_join(os.path.join(d, "w1"), lp, rp, out2, GEOMETRIES[1])
    rows = ij_ref.read_output(out2)
    assert [r["kind"] for r in rows] == ["right_only", "right_only"]
    assert [r["key"] for r in rows] == ["x", "y"]
    # left-only keys
    open(rp, "w").close()
    write_jsonl(lp, [{"text": "x", "start": 0, "end": 1, "payload": 1}])
    out3 = os.path.join(d, "out3.jsonl")
    st = run_join(os.path.join(d, "w2"), lp, rp, out3, GEOMETRIES[2])
    rows = ij_ref.read_output(out3)
    assert [r["kind"] for r in rows] == ["left_only"]
    print("PASS empty_and_single_side")


def test_sparse_candidate_bound():
    # N disjoint intervals per key, each containing exactly one observation.
    N = 400
    d = "/tmp/ij-test-sparse"
    os.makedirs(d, exist_ok=True)
    lp = os.path.join(d, "left.jsonl")
    rp = os.path.join(d, "right.jsonl")
    left, right = [], []
    for i in range(N):
        base = i * 10
        left.append({"text": "hot", "start": base, "end": base + 5,
                     "payload": i})
        right.append({"text": "hot", "at": base + 2, "payload": i})
    rng = random.Random(7)
    rng.shuffle(left)
    rng.shuffle(right)
    write_jsonl(lp, left)
    write_jsonl(rp, right)
    geom = dict(GEOMETRIES[1], active_flush_bytes=1024, marker_page_bytes=256,
                marker_cache_pages=2, result_run_bytes=4096)
    st = run_join(os.path.join(d, "work"), lp, rp, out := os.path.join(d, "out.jsonl"), geom)
    assert st["matches"] == N, st
    assert st["left_only"] == 0 and st["right_only"] == 0, st
    limit = 64 * N
    assert st["candidate_checks"] <= limit, (st["candidate_checks"], limit)
    # and it must be linear-ish, not the N^2 nested product
    assert st["candidate_checks"] <= 8 * N, (st["candidate_checks"], N)
    # disjoint intervals expire immediately, so the heap stays small; the
    # active-run spill path is exercised separately by the dense tests.
    rows = ij_ref.read_output(out)
    exp, _ = ij_ref.reference_join(lp, rp)
    assert rows == exp
    print(f"PASS sparse_candidate_bound (N={N}, checks={st['candidate_checks']} "
          f"<= 8N={8*N}, active_flushes={st['active_flushes']})")


def test_dense_active_spill():
    # Many long overlapping intervals force the active set past its byte
    # threshold -> active runs on disk, their suffix re-scan at later
    # observations, marker write-back and active-run merging.  Result must
    # still equal the nested-loop oracle, including the full match product.
    d = "/tmp/ij-test-dense"
    os.makedirs(d, exist_ok=True)
    lp = os.path.join(d, "left.jsonl")
    rp = os.path.join(d, "right.jsonl")
    N = 60
    left = [{"text": "hot", "start": 0, "end": 1000,
             "payload": "p" * 40 + str(i)} for i in range(N)]
    right = [{"text": "hot", "at": 100 + j * 100, "payload": j}
             for j in range(6)]
    rng = random.Random(3)
    rng.shuffle(left)
    rng.shuffle(right)
    write_jsonl(lp, left)
    write_jsonl(rp, right)
    geom = dict(GEOMETRIES[1], active_flush_bytes=512, max_active_runs=2,
                marker_page_bytes=256, marker_cache_pages=2,
                result_run_bytes=4096)
    out = os.path.join(d, "out.jsonl")
    st = run_join(os.path.join(d, "work"), lp, rp, out, geom)
    assert st["active_flushes"] > 0, st
    exp, est = ij_ref.reference_join(lp, rp)
    rows = ij_ref.read_output(out)
    assert rows == exp
    assert (st["matches"], st["left_only"], st["right_only"]) == (
        est["matches"], est["left_only"], est["right_only"]), st
    print(f"PASS dense_active_spill (matches={st['matches']}, "
          f"flushes={st['active_flushes']}, merges={st['active_merges']})")


def test_straddle_across_tiny_chunks():
    # merge_chunk smaller than a single record forces every record through the
    # exact-size straddle/remainder path (including records spanning several
    # body fields), exercising the byte accounting that previously corrupted
    # cross-chunk joins.  Long payloads make records wider than the chunk.
    d = "/tmp/ij-test-straddle"
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    rng = random.Random(99)
    keys = ["a", "中", "zzz"]
    left, right = [], []
    for i in range(60):
        k = rng.choice(keys)
        start = rng.randint(-20, 20)
        left.append({"text": k, "start": start, "end": start + rng.randint(1, 25),
                     "pad": "L" * rng.randint(200, 900), "i": i})
    for i in range(60):
        k = rng.choice(keys + ["only-r"])
        right.append({"text": k, "at": rng.randint(-25, 45),
                      "pad": "R" * rng.randint(200, 900), "i": i})
    rng.shuffle(left)
    rng.shuffle(right)
    write_jsonl(lp, left)
    write_jsonl(rp, right)
    # 256B chunk while records can be ~1KB -> straddle on nearly every record
    geom = dict(max_line_bytes=4096, read_block_bytes=4096, write_buf_bytes=8192,
                merge_chunk_bytes=256, max_fan_in=2, marker_page_bytes=256,
                marker_cache_pages=2, active_flush_bytes=1024,
                max_active_runs=2, result_run_bytes=2048)
    out = os.path.join(d, "out.jsonl")
    st = run_join(os.path.join(d, "work"), lp, rp, out, geom)
    exp, est = ij_ref.reference_join(lp, rp)
    got = ij_ref.read_output(out)
    assert got == exp
    errs, _ = ij_ref.check_output_contract(got)
    assert not errs, errs
    assert (st["matches"], st["left_only"], st["right_only"]) == (
        est["matches"], est["left_only"], est["right_only"])
    print(f"PASS straddle_across_tiny_chunks "
          f"(records={len(left)+len(right)}, matches={st['matches']})")


if __name__ == "__main__":
    test_hand_example()
    test_random_against_oracle()
    test_empty_and_single_side()
    test_sparse_candidate_bound()
    test_dense_active_spill()
    test_straddle_across_tiny_chunks()
    print("ALL INTERVAL CORRECTNESS TESTS PASSED")
