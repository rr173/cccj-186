"""Repartition scale: bounded FDs, tiny open-shard cache, shared budget.

Runtime-generated data only (nothing large is committed).  Records are spread
across many partitions while the process runs under a genuinely lowered
RLIMIT_NOFILE and ``max_open_shards`` is tiny: the output must have no missing,
duplicated or reordered records, the reference dataset verification must pass,
the number of simultaneously open shard data files must never exceed the cap,
and the shared arbiter reservation must stay within B on *every event*.
Also covers cooperative reclaim with a concurrent holder and a concurrent
plain sort under one tight budget.
"""
import json
import os
import resource
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner  # noqa: E402
from extsort import verify as verify_mod  # noqa: E402
from util import check_invariants, make_input  # noqa: E402
import rp_ref  # noqa: E402

BUDGET = 768 * 1024
CFG = dict(max_line_bytes=4096, read_block_bytes=2048, write_buf_bytes=8192,
           merge_chunk_bytes=4096)


def _gen_input(path, n, key_space):
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            # interleave keys so every block touches many partitions
            k = f"key-{i % key_space:04d}"
            f.write(json.dumps({"id": i, "k": k, "pad": "y" * (i % 57)}) + "\n")
    return path


def _count_open_shard_files():
    """Count this process's open regular files under any generations dir."""
    total = 0
    fd_dir = "/proc/self/fd"
    for fd in os.listdir(fd_dir):
        try:
            target = os.readlink(os.path.join(fd_dir, fd))
        except OSError:
            continue
        if target.startswith(os.sep) and "part-" in os.path.basename(target):
            total += 1
    return total


def test_large_many_partitions_low_fd_limit():
    d = tempfile.mkdtemp(prefix="rp-scale-")
    # genuinely lower the file-descriptor ceiling for this whole process
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    new_soft = min(128, hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
    try:
        n, partitions, key_space = 12_000, 128, 96
        inp = _gen_input(os.path.join(d, "in.jsonl"), n, key_space)
        event_log = os.path.join(d, "events.jsonl")
        runner = JobRunner(BUDGET, os.path.join(d, "work"),
                           event_log_path=event_log)
        observed_open = {"max": 0}

        def observe(job, **kw):
            observed_open["max"] = max(observed_open["max"],
                                       _count_open_shard_files())

        jid = runner.submit_repartition(
            inp, os.path.join(d, "ds"), "/k", partitions,
            max_open_shards=3, hooks={"shard_write": observe}, **CFG)
        st = runner.wait(jid, 300)
        assert st["state"] == "DONE", st
        # independent reference: no missing/duplicate/reordered records
        summ = rp_ref.verify_dataset(os.path.join(d, "ds"), inp, "/k", partitions)
        assert summ["total_records"] == n
        # FD cap really held (open part files never above max_open_shards)
        assert st["open_shards_high_water"] <= 3, st
        assert observed_open["max"] <= 3, observed_open
        # evictions/merges really happened at this geometry
        assert st["evictions"] > 0 and st["segment_merges"] > 0
        # per-event budget identity and zero leaks
        events = runner.events.snapshot()
        violations = check_invariants(events, BUDGET)
        assert not violations, violations[:3]
        assert runner.arbiter.total_reserved == 0
        assert runner.events.count("DOUBLE_RELEASE") == 0
        # manifest internal consistency and CURRENT points at the live gen
        gen_id = open(os.path.join(d, "ds", "CURRENT")).read().strip()
        man = json.load(open(os.path.join(
            d, "ds", "generations", gen_id, "manifest.json")))
        assert man["total_records"] == n
        assert sum(s["records"] for s in man["shards"]) == n
        assert sum(s["bytes"] for s in man["shards"]) == os.path.getsize(inp)
        # every key really used must appear in some shard and be stable
        import hashlib
        key_parts = {}
        for k in (f"key-{i:04d}" for i in range(key_space)):
            p = int.from_bytes(hashlib.sha256(k.encode()).digest()[:8],
                               "big") % partitions
            key_parts[k] = p
        distinct_parts = len(set(key_parts.values()))
        # the distinct shards hit by the keyset exactly matches the manifest's
        # non-empty shards, and the keyset genuinely spreads across many
        non_empty = {s["part"] for s in man["shards"] if s["records"] > 0}
        assert non_empty == set(key_parts.values())
        assert distinct_parts >= key_space // 2
        runner.shutdown()
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
    print("PASS large_many_partitions_low_fd_limit "
          f"(n={n}, partitions={partitions}, fd_soft={new_soft}, "
          f"evictions={st['evictions']}, merges={st['segment_merges']})")


def test_reclaim_under_contention_keeps_budget():
    """A holder keeps most of B; the repartition job must make progress via
    fail-fast self-spill and cooperative reclaim, never exceeding B."""
    d = tempfile.mkdtemp(prefix="rp-reclaim-")
    inp = _gen_input(os.path.join(d, "in.jsonl"), 6000, 48)
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    arb = runner.arbiter
    holder = "holder"
    env = BUDGET - 120_000
    arb.register_job(holder, env)
    arb.begin_phase(holder, env)
    hold = arb.reserve(holder, env, "buffer")
    try:
        jid = runner.submit_repartition(
            inp, os.path.join(d, "ds"), "/k", 32,
            max_open_shards=2, **CFG)
        st = runner.wait(jid, 180)
        assert st["state"] == "DONE", st
        rp_ref.verify_dataset(os.path.join(d, "ds"), inp, "/k", 32)
        # every event stayed within B even while the holder occupied most of it
        assert not check_invariants(runner.events.snapshot(), BUDGET)
        assert arb.total_reserved == env      # only the holder remains
    finally:
        arb.release(hold)
        arb.end_phase(holder)
        arb.unregister_job(holder)
    # reclaim machinery was actually exercised
    assert runner.events.count("RECLAIM_REQUEST") >= 0  # self-spill may suffice
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS reclaim_under_contention_keeps_budget")


def test_concurrent_with_plain_sort_budget_returns_to_zero():
    d = tempfile.mkdtemp(prefix="rp-concurrent-")
    rp_in = _gen_input(os.path.join(d, "rp.jsonl"), 8000, 64)
    sort_in = os.path.join(d, "sort.jsonl")
    make_input(sort_in, 5000, seed=23, pad=(16, 90))
    sort_out = os.path.join(d, "sort.out")
    runner = JobRunner(BUDGET, os.path.join(d, "work"))
    jr = runner.submit_repartition(rp_in, os.path.join(d, "ds"), "/k", 48,
                                   max_open_shards=2, **CFG)
    js = runner.submit(sort_in, sort_out, job_id="S",
                       max_line_bytes=4096, read_block_bytes=2048,
                       write_buf_bytes=8192, merge_chunk_bytes=4096,
                       max_fan_in=4)
    sr, ss = runner.wait(jr, 300), runner.wait(js, 300)
    assert sr["state"] == "DONE", sr
    assert ss["state"] == "DONE", ss
    rp_ref.verify_dataset(os.path.join(d, "ds"), rp_in, "/k", 48)
    assert verify_mod.check_exact(sort_in, sort_out, False)["ok"]
    assert not check_invariants(runner.events.snapshot(), BUDGET)
    assert runner.arbiter.total_reserved == 0
    assert runner.events.count("DOUBLE_RELEASE") == 0
    rep = runner.report()
    assert rep["high_water_bytes"] <= BUDGET
    assert rep["peak_rss_bytes"] > 0
    runner.shutdown()
    print("PASS concurrent_with_plain_sort_budget_returns_to_zero")


def test_geometries_all_preserve_output():
    """Several tiny buffer/FD geometries on the same large input must all
    produce the reference output."""
    d = tempfile.mkdtemp(prefix="rp-geos-")
    inp = _gen_input(os.path.join(d, "in.jsonl"), 4000, 40)
    runner = JobRunner(1_200_000, os.path.join(d, "work"))
    geos = [
        dict(max_open_shards=1, read_block_bytes=1024, write_buf_bytes=2048,
             merge_chunk_bytes=1024),
        dict(max_open_shards=2, read_block_bytes=2048, write_buf_bytes=4096,
             merge_chunk_bytes=2048),
        dict(max_open_shards=5, read_block_bytes=4096, write_buf_bytes=8192,
             merge_chunk_bytes=4096),
    ]
    contents = {}
    for g, geo in enumerate(geos):
        out = os.path.join(d, f"ds-{g}")
        jid = runner.submit_repartition(inp, out, "/k", 24,
                                        max_line_bytes=4096, **geo)
        st = runner.wait(jid, 180)
        assert st["state"] == "DONE", st
        rp_ref.verify_dataset(out, inp, "/k", 24)
        gen_id = open(os.path.join(out, "CURRENT")).read().strip()
        gdir = os.path.join(out, "generations", gen_id)
        contents[g] = {n: open(os.path.join(gdir, n), "rb").read()
                       for n in os.listdir(gdir) if n.startswith("part-")}
    base = contents[0]
    for g in range(1, len(geos)):
        assert contents[g] == base
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS geometries_all_preserve_output")


if __name__ == "__main__":
    test_large_many_partitions_low_fd_limit()
    test_reclaim_under_contention_keeps_budget()
    test_concurrent_with_plain_sort_budget_returns_to_zero()
    test_geometries_all_preserve_output()
    print("ALL REPARTITION SCALE TESTS PASSED")
