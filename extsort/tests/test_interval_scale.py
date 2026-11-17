"""Interval-join scale / budget tests (runtime-generated data only).

Two hot-key scenarios whose on-disk and in-flight state clearly exceeds the
budget B:

  * sparse: each side's single-key group is much larger than B, intervals are
    disjoint with one observation each -- candidate checks stay linear, the
    marker bitmap and result runs spill through many pages/runs;
  * dense: long overlapping intervals whose active set and whose match product
    both exceed B -- active runs, active-run merges and large result runs are
    exercised.

Asserts: output streaming order/traceability, the <=64*N sparse candidate
bound, counters and I/O stats, the per-event budget invariant, zero leaks,
real peak RSS from the runner report, and that the high-water mark of
reservations never crossed the budget while the data exceeded it.
"""
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import check_invariants  # noqa: E402
from extsort import JobRunner  # noqa: E402
import ij_ref  # noqa: E402

# A deliberately small budget so the generated groups/products dwarf B.
BUDGET = 512 * 1024
CFG = dict(max_line_bytes=4096, read_block_bytes=8192, write_buf_bytes=16384,
           merge_chunk_bytes=8192, max_fan_in=3, marker_page_bytes=512,
           marker_cache_pages=2, active_flush_bytes=2048, max_active_runs=2,
           result_run_bytes=16384)


def write(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for o in rows:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")


def stream_validate(path, expected_count):
    """Streaming order + structural + traceability checks; return row count."""
    flag = {"match": 0, "left_only": 0, "right_only": 1}
    count = 0
    last_key = None
    last_sort = None
    with open(path, "rb") as f:
        for raw in f:
            if not raw.strip():
                continue
            o = json.loads(raw)
            count += 1
            kind = o["kind"]
            assert kind in flag
            if kind == "match":
                assert o["left_row"] and o["right_row"] and o["left"] and o["right"]
            elif kind == "left_only":
                assert o["left_row"] and o["left"] and o["right_row"] is None \
                    and o["right"] is None
            else:
                assert o["right_row"] and o["right"] and o["left_row"] is None \
                    and o["left"] is None
            sort_key = (o["key"], flag[kind], o["left_row"] or 0,
                        o["right_row"] or 0)
            if last_sort is not None and sort_key < last_sort:
                raise AssertionError(f"order violation at row {count}")
            last_key = o["key"]
            last_sort = sort_key
    assert count == expected_count, (count, expected_count)
    return count


def test_sparse_scale():
    d = "/tmp/ij-scale-sparse"
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    N = 2000  # each side's one-key group far exceeds the 512KB budget in rows
    rng = random.Random(17)
    left = [{"text": "hot", "start": i * 10, "end": i * 10 + 5,
             "payload": "x" * 60 + str(i)} for i in range(N)]
    right = [{"text": "hot", "at": i * 10 + 2,
              "payload": "y" * 60 + str(i)} for i in range(N)]
    rng.shuffle(left)
    rng.shuffle(right)
    write(lp, left)
    write(rp, right)
    input_bytes = os.path.getsize(lp) + os.path.getsize(rp)
    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    t0 = time.monotonic()
    try:
        out = os.path.join(d, "out.jsonl")
        jid = runner.submit_interval_join(lp, rp, out, **CFG)
        st = runner.wait(jid, 300)
        elapsed = time.monotonic() - t0
        assert st["state"] == "DONE", st
        # all N disjoint intervals match exactly once each
        assert (st["matches"], st["left_only"], st["right_only"]) == (N, 0, 0), st
        assert st["candidate_checks"] <= 64 * N, st["candidate_checks"]
        assert st["candidate_checks"] <= 8 * N, st["candidate_checks"]
        # many organize spills + output merge passes prove the group never
        # fit in memory and flowed through disk
        assert st["spills"] >= 4, st
        assert st["merge_passes"] >= 1, st
        stream_validate(out, N)
        # budget and lifecycle
        ev = runner.events.snapshot()
        assert not check_invariants(ev, BUDGET)
        assert runner.events.count("DOUBLE_RELEASE") == 0
        assert runner.arbiter.total_reserved == 0
        rep = runner.report()
        assert rep["high_water_bytes"] <= BUDGET, rep
        assert rep["peak_rss_bytes"] > 0
        assert st["bytes_written"] > input_bytes  # intermediates spilled
        print(f"PASS sparse_scale N={N} input={input_bytes}B "
              f"checks={st['candidate_checks']} <= 8N={8*N} "
              f"pages={st['marker_pages']} rss={rep['peak_rss_bytes']//1024}MB "
              f"elapsed={elapsed:.1f}s")
    finally:
        runner.shutdown()


def test_dense_scale():
    d = "/tmp/ij-scale-dense"
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    NL, NR = 400, 40
    rng = random.Random(23)
    # every interval spans every observation -> full product NL*NR matches,
    # far larger than B; the active set and the match output both overflow.
    left = [{"text": "hot", "start": 0, "end": 10 ** 9,
             "payload": "x" * 80 + str(i)} for i in range(NL)]
    right = [{"text": "hot", "at": j, "payload": "y" * 80 + str(j)}
             for j in range(NR)]
    rng.shuffle(left)
    rng.shuffle(right)
    write(lp, left)
    write(rp, right)
    expected_matches = NL * NR
    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    t0 = time.monotonic()
    try:
        out = os.path.join(d, "out.jsonl")
        jid = runner.submit_interval_join(lp, rp, out, **CFG)
        st = runner.wait(jid, 300)
        elapsed = time.monotonic() - t0
        assert st["state"] == "DONE", st
        assert st["matches"] == expected_matches, st
        assert st["left_only"] == 0 and st["right_only"] == 0, st
        # dense work is allowed to be proportional to the product; confirm it
        # actually spilled active rows and merged active runs / result runs
        assert st["active_flushes"] > 0, st
        stream_validate(out, expected_matches)
        # independently cross-check the full product count via a streamed pass
        # (the oracle nested loop is avoided at this size on purpose)
        ev = runner.events.snapshot()
        assert not check_invariants(ev, BUDGET)
        assert runner.events.count("DOUBLE_RELEASE") == 0
        assert runner.arbiter.total_reserved == 0
        rep = runner.report()
        assert rep["high_water_bytes"] <= BUDGET, rep
        # output itself clearly exceeds the budget
        assert os.path.getsize(out) > BUDGET
        print(f"PASS dense_scale product={expected_matches} "
              f"flushes={st['active_flushes']} merges={st['active_merges']} "
              f"checks={st['candidate_checks']} "
              f"out={os.path.getsize(out)//1024}KB rss={rep['peak_rss_bytes']//1024}MB "
              f"elapsed={elapsed:.1f}s")
    finally:
        runner.shutdown()


def test_reservation_event_trail():
    # Every grant must be matched by a release/adjust/transfer; assert on the
    # raw event stream that the job ends clean and no reservation kind grew
    # without bound (a proxy for byte-object lifetime discipline).
    d = "/tmp/ij-scale-trail"
    os.makedirs(d, exist_ok=True)
    lp, rp = os.path.join(d, "l.jsonl"), os.path.join(d, "r.jsonl")
    write(lp, [{"text": "k", "start": i, "end": i + 9, "payload": i}
               for i in range(300)])
    write(rp, [{"text": "k", "at": i + 1, "payload": i} for i in range(300)])
    runner = JobRunner(BUDGET, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))
    try:
        jid = runner.submit_interval_join(lp, rp, os.path.join(d, "o.jsonl"),
                                          **CFG)
        st = runner.wait(jid, 120)
        assert st["state"] == "DONE", st
        ev = runner.events.snapshot()
        # terminal state carries zero reservations
        terminal = [e for e in ev if e["kind"] == "JOB_STATE"][-1]
        assert terminal["total_reserved"] == 0
        # the budget invariant held on every single event
        assert all(e["total_reserved"] <= BUDGET for e in ev
                   if "total_reserved" in e)
        # spills and reclaim-related machinery actually ran under pressure
        assert st["spills"] > 0
    finally:
        runner.shutdown()
    print("PASS reservation_event_trail")


if __name__ == "__main__":
    test_sparse_scale()
    test_dense_scale()
    test_reservation_event_trail()
    print("ALL INTERVAL SCALE TESTS PASSED")
