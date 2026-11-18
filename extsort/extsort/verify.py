"""Independent output verification (does not share code with the sorter).

Two modes:

* exact: build the expected output in memory with plain ``sorted`` (stable)
  plus the dedup rule, and compare byte-for-byte;
* streaming: for large outputs -- check key order (codepoints), stability via
  the ``orig`` field the test generator embeds, dedup adjacency, line count
  and an order-independent additive SHA-256 fingerprint against the input.
"""
from __future__ import annotations

import hashlib
import json


def _fingerprint_update(acc: int, line: bytes) -> int:
    return (acc + int.from_bytes(hashlib.sha256(line).digest()[:8], "little")) & 0xFFFFFFFFFFFFFFFF


def stream_fingerprint(path: str) -> dict:
    """Order-independent stats over a JSONL file: count + additive hash."""
    count = 0
    acc = 0
    with open(path, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            count += 1
            acc = _fingerprint_update(acc, line)
    return {"count": count, "fingerprint": acc}


def expected_output_bytes(input_path: str, dedup: bool) -> bytes:
    """Reference implementation: stable sort by text codepoints, optional
    dedup keeping the earliest original line per key."""
    with open(input_path, "rb") as f:
        lines = [ln for ln in f.read().splitlines(keepends=True) if ln.strip()]
    keyed = []
    for i, ln in enumerate(lines):
        obj = json.loads(ln)
        keyed.append((obj["text"], i, ln))
    keyed.sort(key=lambda t: t[0])          # list.sort is stable -> (key, lineno)
    out = []
    last = None
    for key, _i, ln in keyed:
        if dedup and key == last:
            continue
        last = key
        out.append(ln)
    return b"".join(out)


def expected_output_bytes_norm(input_path: str, dedup: bool) -> bytes:
    """Reference for the resumable sorter: same stable codepoint sort and
    dedup rule, but every delivered line is newline-terminated (a final
    input line without '\\n' gets one wherever it lands in the output, so
    adjacent records are never glued and the file always ends with '\\n')."""
    with open(input_path, "rb") as f:
        lines = [ln for ln in f.read().splitlines(keepends=True) if ln.strip()]
    keyed = []
    for i, ln in enumerate(lines):
        obj = json.loads(ln)
        keyed.append((obj["text"], i, ln))
    keyed.sort(key=lambda t: t[0])
    out = []
    last = None
    for key, _i, ln in keyed:
        if dedup and key == last:
            continue
        last = key
        if not ln.endswith(b"\n"):
            ln = ln + b"\n"
        out.append(ln)
    return b"".join(out)


def check_exact(input_path: str, output_path: str, dedup: bool) -> dict:
    exp = expected_output_bytes(input_path, dedup)
    with open(output_path, "rb") as f:
        got = f.read()
    ok = got == exp
    res = {"ok": ok, "expected_bytes": len(exp), "got_bytes": len(got)}
    if not ok:
        # locate first divergence for debugging
        n = min(len(exp), len(got))
        idx = next((i for i in range(n) if exp[i] != got[i]), n)
        res["first_diff_at"] = idx
    return res


def check_output_streaming(output_path: str, *, dedup: bool,
                           expect_count: int | None = None,
                           expect_fingerprint: int | None = None,
                           check_stability: bool = True) -> dict:
    """Streaming order/stability/count/fingerprint check of an output file."""
    count = 0
    acc = 0
    last_key = None
    last_orig = -1
    errors = []
    with open(output_path, "rb") as f:
        for raw in f:
            if not raw.strip():
                continue
            count += 1
            acc = _fingerprint_update(acc, raw)
            obj = json.loads(raw)
            key = obj["text"]
            if last_key is not None:
                if key < last_key:
                    errors.append(f"order violation at line {count}: {key!r} < {last_key!r}")
                elif dedup and key == last_key:
                    errors.append(f"dedup violation at line {count}: duplicate {key!r}")
                elif key == last_key and check_stability:
                    orig = obj.get("payload", {}).get("orig")
                    if orig is not None and orig <= last_orig:
                        errors.append(
                            f"stability violation at line {count}: orig {orig} <= {last_orig}")
            last_key = key
            pl = obj.get("payload", {})
            if isinstance(pl, dict) and pl.get("orig") is not None:
                last_orig = pl["orig"]
            if len(errors) > 5:
                break
    if expect_count is not None and count != expect_count:
        errors.append(f"count {count} != expected {expect_count}")
    if expect_fingerprint is not None and acc != expect_fingerprint:
        errors.append(f"fingerprint mismatch: {acc} != {expect_fingerprint}")
    return {"ok": not errors, "errors": errors[:8], "count": count, "fingerprint": acc}
