"""Repartition correctness against the independent reference (rp_ref.py).

Covers: nested object/array pointers, JSON Pointer escapes, empty-string key,
repeated identical keys, empty shards, input whose final line has no line
ending, and several buffer/open-shard geometries.  The same input rerun in
this process and in fresh ``python -I`` subprocesses must route every record
to the identical shard, and shard content/order/counts/bytes/hashes match the
reference exactly.
"""
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extsort import JobRunner, route_partition  # noqa: E402
import rp_ref  # noqa: E402

GEOMETRIES = [
    dict(max_open_shards=1, read_block_bytes=512, write_buf_bytes=1024,
         merge_chunk_bytes=768),
    dict(max_open_shards=2, read_block_bytes=1024, write_buf_bytes=2048,
         merge_chunk_bytes=1024),
    dict(max_open_shards=5, read_block_bytes=2048, write_buf_bytes=4096,
         merge_chunk_bytes=2048),
    dict(max_open_shards=16, read_block_bytes=4096, write_buf_bytes=8192,
         merge_chunk_bytes=4096),
]


def geo(n_partitions: int, idx: int) -> dict:
    g = dict(GEOMETRIES[idx])
    g["max_open_shards"] = min(g["max_open_shards"], n_partitions)
    return g


def _runner(d, budget=4_000_000):
    return JobRunner(budget, os.path.join(d, "work"),
                     event_log_path=os.path.join(d, "events.jsonl"))


def test_pointer_shapes_and_escapes():
    d = tempfile.mkdtemp(prefix="rp-corr-ptr-")
    inp = os.path.join(d, "in.jsonl")
    rows = [
        # nested objects, array indices, escaped tokens, unicode, empty key
        {"a": {"b/c": "v0"}, "arr": ["s0", "s1", ""]},
        {"a": {"b/c": "v1"}, "arr": ["x", "y", "z"]},
        {"a": {"b~d": "v2"}, "arr": ["", ""]},
        {"a": {"b~d": "v3"}, "arr": ["🙂", "中", "ä"]},
        {"a": {"b/c": ""}, "arr": ["deep", {"e": ["w"]}]},
    ]
    with open(inp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    pointers = ["/a/b~1c", "/a/b~0d", "/arr/0", "/arr/2",
                "/arr/2/0", "/a/b~1c/0", "/arr/1/0", "/nope"]
    runner = _runner(d)
    for ptr in pointers:
        out = os.path.join(d, "ds-" + ptr.replace("/", "_").replace("~", "t"))
        # independent reference decides whether the whole stream resolves
        _exp, ref_bad = rp_ref.ref_expected(inp, ptr, 5)
        jid = runner.submit_repartition(inp, out, ptr, 5,
                                        max_line_bytes=8192, **GEOMETRIES[0])
        st = runner.wait(jid, 30)
        if ref_bad is not None:
            assert st["state"] == "FAILED", (ptr, st)
            assert f"line {ref_bad[0]}" in st["error"], (ptr, st["error"])
            continue
        assert st["state"] == "DONE", (ptr, st)
        rp_ref.verify_dataset(out, inp, ptr, 5)
    runner.shutdown()
    print("PASS pointer_shapes_and_escapes")


def test_empty_key_repeated_keys_empty_shards():
    d = tempfile.mkdtemp(prefix="rp-corr-keys-")
    inp = os.path.join(d, "in.jsonl")
    keys = ["", "same", "same", "same", "", "αβγ", "same", ""]
    with open(inp, "w", encoding="utf-8") as f:
        for i, k in enumerate(keys):
            f.write(json.dumps({"id": i, "k": k}, ensure_ascii=False) + "\n")
    runner = _runner(d)
    # many partitions relative to the key cardinality: at least one empty shard
    for n in (1, 2, 17, 64):
        out = os.path.join(d, f"ds-{n}")
        jid = runner.submit_repartition(inp, out, "/k", n,
                                        max_line_bytes=4096, **GEOMETRIES[0])
        st = runner.wait(jid, 30)
        assert st["state"] == "DONE", st
        summ = rp_ref.verify_dataset(out, inp, "/k", n)
        man = json.load(open(os.path.join(
            out, "generations", summ["generation_id"], "manifest.json")))
        if n > len(set(keys)):
            assert any(s["records"] == 0 for s in man["shards"])
        # same key always lands in one shard
        same_parts = {route_partition(b"same", n)}
        assert len(same_parts) == 1
        empty_parts = {route_partition(b"", n)}
        assert len(empty_parts) == 1
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS empty_key_repeated_keys_empty_shards")


def test_no_newline_final_line_and_byte_preservation():
    d = tempfile.mkdtemp(prefix="rp-corr-tail-")
    inp = os.path.join(d, "in.jsonl")
    payload = [json.dumps({"id": i, "k": f"k{i % 6}"}, ensure_ascii=False)
               for i in range(50)]
    with open(inp, "wb") as f:
        f.write(("\n".join(payload[:-1]) + "\n").encode())
        f.write(payload[-1].encode())          # no LF on the physical last line
    runner = _runner(d)
    for g in range(len(GEOMETRIES)):
        out = os.path.join(d, f"ds-{g}")
        jid = runner.submit_repartition(inp, out, "/k", 7,
                                        max_line_bytes=4096, **geo(7, g))
        st = runner.wait(jid, 30)
        assert st["state"] == "DONE", st
        rp_ref.verify_dataset(out, inp, "/k", 7)
    runner.shutdown()
    print("PASS no_newline_final_line_and_byte_preservation")


def test_all_geometries_match_reference():
    d = tempfile.mkdtemp(prefix="rp-corr-geo-")
    inp = os.path.join(d, "in.jsonl")
    rng_keys = [f"key-{i:04d}" for i in range(40)]
    with open(inp, "w", encoding="utf-8") as f:
        for i in range(600):
            f.write(json.dumps({"id": i, "k": rng_keys[i % len(rng_keys)],
                                "pad": "x" * (i % 73)}) + "\n")
    runner = _runner(d, budget=6_000_000)
    gens = []
    for g in range(len(GEOMETRIES)):
        out = os.path.join(d, f"ds-{g}")
        jid = runner.submit_repartition(inp, out, "/k", 11,
                                        max_line_bytes=4096, **geo(11, g))
        st = runner.wait(jid, 60)
        assert st["state"] == "DONE", st
        summ = rp_ref.verify_dataset(out, inp, "/k", 11)
        gens.append((out, summ["generation_id"]))
        assert st["open_shards_high_water"] <= geo(11, g)["max_open_shards"]
    # reruns with different geometries must produce byte-identical shards
    ref_bytes = {}
    for g, (out, gen_id) in enumerate(gens):
        gdir = os.path.join(out, "generations", gen_id)
        for name in sorted(os.listdir(gdir)):
            if name.startswith("part-"):
                data = open(os.path.join(gdir, name), "rb").read()
                if g == 0:
                    ref_bytes[name] = data
                else:
                    assert data == ref_bytes[name], (g, name)
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS all_geometries_match_reference")


def test_determinism_across_subprocesses():
    """Fresh python -I subprocesses must route every record identically."""
    d = tempfile.mkdtemp(prefix="rp-corr-subproc-")
    inp = os.path.join(d, "in.jsonl")
    with open(inp, "w", encoding="utf-8") as f:
        for i in range(400):
            f.write(json.dumps({"id": i, "k": f"k{i % 23}"}) + "\n")
    pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    driver = os.path.join(d, "driver.py")
    with open(driver, "w") as f:
        f.write(
            "import sys, json\n"
            f"sys.path.insert(0, {pkg_root!r})\n"
            "from extsort import JobRunner\n"
            f"runner = JobRunner(8_000_000, {os.path.join(d, 'work')!r})\n"
            f"jid = runner.submit_repartition({inp!r}, sys.argv[1], '/k', 13,\n"
            "    max_line_bytes=4096, max_open_shards=int(sys.argv[2]),\n"
            "    read_block_bytes=1024, write_buf_bytes=2048)\n"
            "st = runner.wait(jid, 60)\n"
            "assert st['state'] == 'DONE', st\n"
            "runner.shutdown()\n")
    outs = []
    for attempt, mos in enumerate((1, 3, 13)):
        out = os.path.join(d, f"sub-{attempt}")
        r = subprocess.run([sys.executable, "-I", driver, out, str(mos)],
                           capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        gen_id = open(os.path.join(out, "CURRENT"), encoding="utf-8").read().strip()
        outs.append(os.path.join(out, "generations", gen_id))
    base = outs[0]
    for gdir in outs[1:]:
        for name in sorted(os.listdir(base)):
            if name.startswith("part-"):
                assert open(os.path.join(gdir, name), "rb").read() == \
                    open(os.path.join(base, name), "rb").read(), name
    # and every part still matches the in-process reference
    rp_ref.verify_dataset(os.path.join(d, "sub-0"), inp, "/k", 13)
    print("PASS determinism_across_subprocesses")


def test_single_partition_is_exact_input():
    d = tempfile.mkdtemp(prefix="rp-corr-one-")
    inp = os.path.join(d, "in.jsonl")
    payload = [json.dumps({"id": i, "k": f"k{i % 3}"}) for i in range(120)]
    with open(inp, "wb") as f:
        f.write(("\n".join(payload) + "\n").encode())
    runner = _runner(d)
    out = os.path.join(d, "ds")
    g = dict(GEOMETRIES[1]); g.pop("max_open_shards")
    jid = runner.submit_repartition(inp, out, "/k", 1, max_line_bytes=4096,
                                    max_open_shards=1, **g)
    st = runner.wait(jid, 30)
    assert st["state"] == "DONE", st
    gen_id = open(os.path.join(out, "CURRENT")).read().strip()
    part = open(os.path.join(out, "generations", gen_id,
                             "part-00000.jsonl"), "rb").read()
    assert part == open(inp, "rb").read()
    rp_ref.verify_dataset(out, inp, "/k", 1)
    runner.shutdown()
    print("PASS single_partition_is_exact_input")


if __name__ == "__main__":
    test_pointer_shapes_and_escapes()
    test_empty_key_repeated_keys_empty_shards()
    test_no_newline_final_line_and_byte_preservation()
    test_all_geometries_match_reference()
    test_determinism_across_subprocesses()
    test_single_partition_is_exact_input()
    print("ALL REPARTITION CORRECTNESS TESTS PASSED")
