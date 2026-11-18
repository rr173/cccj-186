"""Independent reference for repartition acceptance tests.

Nothing here imports the production package: the JSON Pointer resolution and
the SHA-256 routing are re-implemented from the specification, and the
published dataset is verified by reading CURRENT + manifest directly.
"""
from __future__ import annotations

import hashlib
import json
import os


def ref_unescape(token: str) -> str:
    out = []
    i = 0
    while i < len(token):
        ch = token[i]
        if ch == "~":
            assert i + 1 < len(token) and token[i + 1] in "01"
            out.append("~" if token[i + 1] == "0" else "/")
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def ref_resolve(obj, pointer: str):
    """Independent RFC 6901 resolution. Returns (ok, value_or_reason)."""
    if not pointer.startswith("/"):
        return False, "pointer must start with '/'"
    cur = obj
    for raw in pointer[1:].split("/"):
        token = ref_unescape(raw)
        if isinstance(cur, dict):
            if token not in cur:
                return False, f"missing property {token!r}"
            cur = cur[token]
        elif isinstance(cur, list):
            if token == "-":
                return False, " '-' references no element"
            if not token.isdigit() or (len(token) > 1 and token[0] == "0"):
                return False, f"bad array index {token!r}"
            idx = int(token)
            if idx >= len(cur):
                return False, f"index {idx} out of range"
            cur = cur[idx]
        else:
            return False, f"cannot descend into {type(cur).__name__}"
    if not isinstance(cur, str):
        return False, f"not a string ({type(cur).__name__})"
    return True, cur


def ref_route(key: str, num_partitions: int) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) % num_partitions


def ref_expected(input_path: str, pointer: str, num_partitions: int):
    """Stream the input independently and return the expected per-shard list
    of (physical line, exact output bytes) in input order."""
    shards = [[] for _ in range(num_partitions)]
    bad = None
    with open(input_path, "rb") as f:
        for lineno, raw in enumerate(f, 1):
            if not raw.strip():
                bad = (lineno, "empty line")
                break
            try:
                obj = json.loads(raw)
            except Exception as exc:  # noqa: BLE001
                bad = (lineno, f"invalid JSON: {exc!r}")
                break
            if not isinstance(obj, dict):
                bad = (lineno, "not an object")
                break
            ok, val = ref_resolve(obj, pointer)
            if not ok:
                bad = (lineno, val)
                break
            out_bytes = raw if raw.endswith(b"\n") else raw + b"\n"
            shards[ref_route(val, num_partitions)].append((lineno, out_bytes))
    return shards, bad


def verify_dataset(output_dir: str, input_path: str, pointer: str,
                   num_partitions: int):
    """Full independent verification of a published dataset. Raises
    AssertionError on any mismatch; returns a summary dict on success."""
    current_path = os.path.join(output_dir, "CURRENT")
    gen_id = open(current_path, encoding="utf-8").read().strip()
    assert gen_id, "CURRENT must name a generation id"
    gen_dir = os.path.join(output_dir, "generations", gen_id)
    assert os.path.isdir(gen_dir), f"generation dir missing: {gen_dir}"

    expected, bad = ref_expected(input_path, pointer, num_partitions)
    assert bad is None, f"reference found bad input: {bad}"

    manifest = json.load(open(os.path.join(gen_dir, "manifest.json"),
                              encoding="utf-8"))
    assert manifest["format_version"] >= 1
    assert manifest["key_pointer"] == pointer
    assert manifest["num_partitions"] == num_partitions
    algo = manifest["partition_algorithm"]
    assert algo["hash"] == "sha256" and algo["prefix_bytes"] == 8
    assert algo["integer_encoding"] == "unsigned-big-endian"
    assert algo["modulo"] == num_partitions
    assert manifest["total_records"] == sum(len(s) for s in expected)

    # input hash independently recomputed
    h = hashlib.sha256()
    with open(input_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    assert manifest["input_sha256"] == h.hexdigest()

    assert len(manifest["shards"]) == num_partitions
    seen_files = set()
    total_records = total_bytes = 0
    for i in range(num_partitions):
        fname = f"part-{i:05d}.jsonl"
        seen_files.add(fname)
        path = os.path.join(gen_dir, fname)
        assert os.path.exists(path), f"missing shard file {fname}"
        data = open(path, "rb").read()
        # exact per-record bytes and per-shard input order: the expected
        # sequence is built in input order, so an exact ordered byte equality
        # proves both membership and order without sharing production code
        exp = expected[i]
        exp_bytes = b"".join(p for _, p in exp)
        assert data == exp_bytes, f"shard {i}: ordered byte content mismatch"
        # every emitted record ends in LF and no two records are glued
        assert all(p.endswith(b"\n") for _, p in exp)
        sm = manifest["shards"][i]
        assert sm["file"] == fname and sm["part"] == i
        assert sm["records"] == len(exp)
        assert sm["bytes"] == len(data)
        assert sm["sha256"] == hashlib.sha256(data).hexdigest()
        total_records += sm["records"]
        total_bytes += sm["bytes"]

    on_disk = {n for n in os.listdir(gen_dir) if n.startswith("part-")}
    assert on_disk == seen_files
    # manifest.json must be the only other entry
    assert set(os.listdir(gen_dir)) == seen_files | {"manifest.json"}
    assert total_records == manifest["total_records"]
    return {"generation_id": gen_id, "total_records": total_records,
            "total_bytes": total_bytes, "num_partitions": num_partitions}
