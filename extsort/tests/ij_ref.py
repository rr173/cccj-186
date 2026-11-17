"""Independent reference implementation for the interval join.

Deliberately written from scratch: it loads both (small) inputs, performs the
literal nested-loop predicate

    text(left) == text(right) and start <= at < end

with plain Python dicts/lists, builds the full outer join and sorts the result
with the built-in ``sorted``.  It shares no code with extsort's production
matching or sorting paths, so tests can compare the external job against an
oracle that is only assumed to restate the data contract.
"""
from __future__ import annotations

import json

KIND_MATCH = "match"
KIND_LEFT_ONLY = "left_only"
KIND_RIGHT_ONLY = "right_only"


def read_jsonl(path):
    """Return [(lineno_1based, obj), ...]; blank lines are an error here."""
    out = []
    with open(path, "rb") as f:
        for i, raw in enumerate(f, 1):
            if not raw.strip():
                raise ValueError(f"{path}: blank line {i}")
            out.append((i, json.loads(raw.decode("utf-8"))))
    return out


def reference_join(left_path, right_path):
    """Return (expected_output_dicts, stats) from the nested-loop oracle."""
    left = read_jsonl(left_path)
    right = read_jsonl(right_path)

    out = []
    matched_l = set()
    matched_r = set()
    n_matches = 0
    for lrow, lo in left:
        for rrow, ro in right:
            if lo["text"] == ro["text"] and lo["start"] <= ro["at"] < lo["end"]:
                out.append({
                    "kind": KIND_MATCH,
                    "key": lo["text"],
                    "left_row": lrow,
                    "right_row": rrow,
                    "left": lo,
                    "right": ro,
                })
                matched_l.add(lrow)
                matched_r.add(rrow)
                n_matches += 1
    n_left_only = 0
    for lrow, lo in left:
        if lrow not in matched_l:
            out.append({
                "kind": KIND_LEFT_ONLY,
                "key": lo["text"],
                "left_row": lrow,
                "right_row": None,
                "left": lo,
                "right": None,
            })
            n_left_only += 1
    n_right_only = 0
    for rrow, ro in right:
        if rrow not in matched_r:
            out.append({
                "kind": KIND_RIGHT_ONLY,
                "key": ro["text"],
                "left_row": None,
                "right_row": rrow,
                "left": None,
                "right": ro,
            })
            n_right_only += 1

    flag = {KIND_MATCH: 0, KIND_LEFT_ONLY: 0, KIND_RIGHT_ONLY: 1}
    # codepoint order on key; left-bearing first; then source row numbers
    out.sort(key=lambda o: (o["key"], flag[o["kind"]],
                            o["left_row"] or 0, o["right_row"] or 0))
    stats = {
        "left_input": len(left),
        "right_input": len(right),
        "matches": n_matches,
        "left_only": n_left_only,
        "right_only": n_right_only,
    }
    return out, stats


def read_output(path):
    rows = []
    with open(path, "rb") as f:
        for raw in f:
            if raw.strip():
                rows.append(json.loads(raw.decode("utf-8")))
    return rows


def check_output_contract(rows):
    """Structural/order/traceability checks independent of the reference."""
    errors = []
    flag = {KIND_MATCH: 0, KIND_LEFT_ONLY: 0, KIND_RIGHT_ONLY: 1}
    last_key = None
    last_sort = None
    seen_pairs = set()
    for i, o in enumerate(rows):
        for field in ("kind", "key", "left_row", "right_row", "left", "right"):
            if field not in o:
                errors.append(f"row {i}: missing field {field}")
        kind = o.get("kind")
        if kind not in flag:
            errors.append(f"row {i}: bad kind {kind!r}")
            continue
        if kind == KIND_MATCH:
            if o["left_row"] is None or o["right_row"] is None:
                errors.append(f"row {i}: match missing a row id")
            if o["left"] is None or o["right"] is None:
                errors.append(f"row {i}: match missing a side object")
        elif kind == KIND_LEFT_ONLY:
            if o["right_row"] is not None or o["right"] is not None:
                errors.append(f"row {i}: left_only carries right data")
            if o["left"] is None or o["left_row"] is None:
                errors.append(f"row {i}: left_only missing left data")
        else:
            if o["left_row"] is not None or o["left"] is not None:
                errors.append(f"row {i}: right_only carries left data")
            if o["right"] is None or o["right_row"] is None:
                errors.append(f"row {i}: right_only missing right data")
        sort_key = (o["key"], flag[kind], o["left_row"] or 0, o["right_row"] or 0)
        if last_key is not None:
            if (o["key"], flag[kind]) < (last_key, flag[rows[i - 1]["kind"]]):
                errors.append(f"row {i}: key/flag order violation")
            elif sort_key < last_sort:
                errors.append(f"row {i}: row order violation {sort_key} < {last_sort}")
        last_key = o["key"]
        last_sort = sort_key
        seen_pairs.add((kind, o["left_row"], o["right_row"], o["key"]))
    return errors, seen_pairs
