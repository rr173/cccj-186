"""Command line interface.

Run several files concurrently under one shared budget:

    python -m extsort run --budget 64MB --workdir /tmp/esx -o /tmp/out a.jsonl b.jsonl

Verify an output independently:

    python -m extsort verify --input a.jsonl --output /tmp/out/a.jsonl.sorted [--dedup]
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .runner import JobRunner
from .arbiter import ConfigError
from . import verify as verify_mod


def _size(s: str) -> int:
    s = s.strip().upper()
    for suf, mul in (("KB", 1024), ("MB", 1024**2), ("GB", 1024**3),
                     ("K", 1024), ("M", 1024**2), ("G", 1024**3), ("B", 1)):
        if s.endswith(suf):
            return int(float(s[:-len(suf)]) * mul)
    return int(s)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="extsort")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="sort several JSONL files concurrently under one budget")
    run.add_argument("inputs", nargs="+")
    run.add_argument("-o", "--outdir", required=True)
    run.add_argument("--budget", required=True, type=_size)
    run.add_argument("--workdir", required=True)
    run.add_argument("--max-line-bytes", type=_size, default=256 * 1024)
    run.add_argument("--dedup", action="store_true")
    run.add_argument("--read-block", type=_size, default=256 * 1024)
    run.add_argument("--write-buf", type=_size, default=512 * 1024)
    run.add_argument("--merge-chunk", type=_size, default=256 * 1024)
    run.add_argument("--max-fan-in", type=int, default=16)
    run.add_argument("--fsync", action="store_true")
    run.add_argument("--event-log", default=None)

    ver = sub.add_parser("verify", help="independently verify a sorted output")
    ver.add_argument("--input", default=None, help="original input (for fingerprint/count)")
    ver.add_argument("--output", required=True)
    ver.add_argument("--dedup", action="store_true")
    ver.add_argument("--exact", action="store_true", help="in-memory sorted() comparison")

    args = ap.parse_args(argv)

    if args.cmd == "run":
        os.makedirs(args.outdir, exist_ok=True)
        runner = JobRunner(args.budget, args.workdir,
                           event_log_path=args.event_log, fsync=args.fsync)
        ids = []
        try:
            for path in args.inputs:
                out = os.path.join(args.outdir, os.path.basename(path) + ".sorted")
                try:
                    jid = runner.submit(
                        path, out,
                        max_line_bytes=args.max_line_bytes,
                        dedup_key=args.dedup,
                        read_block_bytes=args.read_block,
                        write_buf_bytes=args.write_buf,
                        merge_chunk_bytes=args.merge_chunk,
                        max_fan_in=args.max_fan_in,
                    )
                except ConfigError as exc:
                    print(f"REJECTED {path}: {exc}", file=sys.stderr)
                    continue
                ids.append(jid)
                print(f"submitted {jid}: {path} -> {out}")
            results = runner.wait_all()
            ok = True
            for jid, st in results.items():
                print(json.dumps(st, ensure_ascii=False))
                ok = ok and st["state"] == "DONE"
            rep = runner.report()
            rep.pop("jobs", None)
            print("REPORT " + json.dumps(rep, ensure_ascii=False, indent=2))
            return 0 if ok else 1
        finally:
            runner.shutdown()

    if args.cmd == "verify":
        if args.exact:
            if not args.input:
                print("--exact needs --input", file=sys.stderr)
                return 2
            res = verify_mod.check_exact(args.input, args.output, args.dedup)
        else:
            kw = {}
            if args.input and not args.dedup:
                fp = verify_mod.stream_fingerprint(args.input)
                kw["expect_count"] = fp["count"]
                kw["expect_fingerprint"] = fp["fingerprint"]
            res = verify_mod.check_output_streaming(args.output, dedup=args.dedup, **kw)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if res["ok"] else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
