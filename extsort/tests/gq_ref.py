"""Independent Fraction reference calculator for group quantiles.

This module deliberately shares NO code with the product: it does not import
anything from :mod:`extsort`, and it never uses the product's decimal parser,
order keys, comparisons or sort routines.  It reads the raw JSONL with the
stdlib ``json`` decoder (number lexemes captured through parse hooks), turns
every value into an exact :class:`fractions.Fraction`, sorts each group in
plain Python and applies the nearest-rank rule independently.

Expected output rows are rendered with the same on-the-wire JSON shape the job
contract requires, so the product output can be compared byte-for-byte.
"""
from __future__ import annotations

import json
from fractions import Fraction


def _lexeme(x):
    # stdlib parse hooks hand the exact number text straight through
    return ("num", x)


def _is_num_tag(v):
    return isinstance(v, tuple) and len(v) == 2 and v[0] == "num"


def read_records(input_path: str):
    """Return (records, error).

    records: list of (lineno, text, Fraction value, original value lexeme).
    error:   (lineno, reason) for the first contract violation, else None.
    Mirrors the product contract but implements every check independently.
    """
    records = []
    with open(input_path, "rb") as f:
        for lineno, raw in enumerate(f, start=1):
            if not raw.strip():
                return records, (lineno, "empty line")
            try:
                obj = json.loads(raw, parse_int=_lexeme, parse_float=_lexeme,
                                 parse_constant=lambda c: ("const", c))
            except Exception as exc:  # noqa: BLE001 - reference mirrors product
                return records, (lineno, f"invalid JSON: {exc!r}")
            if not isinstance(obj, dict):
                return records, (lineno, "line is not a JSON object")
            text = obj.get("text")
            if not isinstance(text, str) or not text:
                return records, (lineno, "missing, non-string or empty 'text' field")
            if "value" not in obj:
                return records, (lineno, "missing 'value' field")
            value = obj["value"]
            if isinstance(value, tuple) and len(value) == 2 and value[0] == "const":
                return records, (lineno, f"non-finite numeric constant {value[1]!r}")
            if not _is_num_tag(value):
                return records, (lineno, "'value' must be a JSON number")
            token = value[1]
            try:
                frac = Fraction(token)
            except (ValueError, ZeroDivisionError) as exc:
                return records, (lineno, f"unparsable number {token!r}: {exc}")
            records.append((lineno, text, frac, token))
    return records, None


def nearest_rank(bp: int, n: int) -> int:
    # r = max(1, ceil(bp*n/10000)); exact integer ceiling, no float
    return max(1, (bp * n + 9999) // 10000)


def expected_rows(input_path: str, percentiles_bp):
    """Compute the full expected ordered result rows for one job."""
    records, error = read_records(input_path)
    if error is not None:
        return None, error
    groups: dict[str, list] = {}
    for lineno, text, frac, token in records:
        groups.setdefault(text, []).append((frac, lineno, token))
    bps = sorted(percentiles_bp)
    rows = []
    for text in sorted(groups):                       # Unicode codepoint order
        members = groups[text]
        members.sort(key=lambda m: (m[0], m[1]))     # exact Fraction, then line
        n = len(members)
        # contract rule: when the target rank falls on a value that occurs
        # several times, the source is the smallest physical line (and its
        # spelling) within that equal-value run.
        run_min: dict = {}
        for frac, lineno, token in members:
            cur = run_min.get(frac)
            if cur is None or lineno < cur[0]:
                run_min[frac] = (lineno, token)
        for bp in bps:
            rank = nearest_rank(bp, n)
            frac, _at_rank_line, _at_rank_token = members[rank - 1]
            lineno, token = run_min[frac]
            rows.append({
                "text": text,
                "bp": bp,
                "n": n,
                "rank": rank,
                "source_line": lineno,
                "value_token": token,
            })
    return rows, None


def render(rows) -> bytes:
    out = []
    for r in rows:
        out.append(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    return "".join(out).encode("utf-8")


def expected_output_bytes(input_path: str, percentiles_bp):
    rows, error = expected_rows(input_path, percentiles_bp)
    if error is not None:
        raise AssertionError(f"reference hit unexpected input error: {error}")
    return render(rows)
