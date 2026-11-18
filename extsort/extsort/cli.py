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
from .resumable import ResumeError, inspect_state_dir
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

    ij = sub.add_parser("interval-join",
                        help="full outer interval join of two unordered JSONL files")
    ij.add_argument("--left", required=True, help="left JSONL (text,start,end)")
    ij.add_argument("--right", required=True, help="right JSONL (text,at)")
    ij.add_argument("-o", "--output", required=True)
    ij.add_argument("--budget", required=True, type=_size)
    ij.add_argument("--workdir", required=True)
    ij.add_argument("--max-line-bytes", type=_size, default=256 * 1024)
    ij.add_argument("--read-block", type=_size, default=256 * 1024)
    ij.add_argument("--write-buf", type=_size, default=512 * 1024)
    ij.add_argument("--merge-chunk", type=_size, default=256 * 1024)
    ij.add_argument("--max-fan-in", type=int, default=8)
    ij.add_argument("--fsync", action="store_true")
    ij.add_argument("--event-log", default=None)

    def rsort_common(p, *, semantics: bool):
        p.add_argument("--state-dir", required=True,
                       help="private task workspace (checkpoints, runs, lock)")
        p.add_argument("--budget", required=True, type=_size)
        p.add_argument("--read-block", type=_size, default=None)
        p.add_argument("--write-buf", type=_size, default=None)
        p.add_argument("--merge-chunk", type=_size, default=None)
        p.add_argument("--max-fan-in", type=int, default=None)
        p.add_argument("--checkpoint-input-bytes", type=_size, default=None)
        p.add_argument("--event-log", default=None,
                       help="default: <state-dir>/events.jsonl")
        p.add_argument("--fsync", action="store_true")
        if semantics:
            p.add_argument("--dedup", action="store_true")
            p.add_argument("--max-line-bytes", type=_size, default=256 * 1024)

    rnew = sub.add_parser("rsort-new",
                          help="create a resumable sort task (state dir must be empty)")
    rnew.add_argument("--input", required=True)
    rnew.add_argument("--output", required=True)
    rsort_common(rnew, semantics=True)

    rres = sub.add_parser("rsort-resume",
                          help="resume the task stored in a state dir")
    rsort_common(rres, semantics=False)

    rstat = sub.add_parser("rsort-status",
                           help="inspect a state dir (read-only)")
    rstat.add_argument("--state-dir", required=True)
    rstat.add_argument("--verify-runs", action="store_true",
                       help="re-hash every confirmed run and compare")

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

    if args.cmd == "interval-join":
        runner = JobRunner(args.budget, args.workdir,
                           event_log_path=args.event_log, fsync=args.fsync)
        try:
            jid = runner.submit_interval_join(
                args.left, args.right, args.output,
                max_line_bytes=args.max_line_bytes,
                read_block_bytes=args.read_block,
                write_buf_bytes=args.write_buf,
                merge_chunk_bytes=args.merge_chunk,
                max_fan_in=args.max_fan_in)
            print(f"submitted {jid}: {args.left} >< {args.right} -> {args.output}")
            st = runner.wait(jid)
            print(json.dumps(st, ensure_ascii=False))
            rep = runner.report()
            rep.pop("jobs", None)
            print("REPORT " + json.dumps(rep, ensure_ascii=False, indent=2))
            return 0 if st["state"] == "DONE" else 1
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

    if args.cmd in ("rsort-new", "rsort-resume"):
        event_log = args.event_log or os.path.join(args.state_dir, "events.jsonl")
        runner = JobRunner(args.budget, args.state_dir,
                           event_log_path=event_log, fsync=args.fsync)
        tun = {}
        for k, src in (("read_block_bytes", "read_block"),
                       ("write_buf_bytes", "write_buf"),
                       ("merge_chunk_bytes", "merge_chunk"),
                       ("max_fan_in", "max_fan_in"),
                       ("checkpoint_input_bytes", "checkpoint_input_bytes")):
            v = getattr(args, src)
            if v is not None:
                tun[k] = v
        try:
            if args.cmd == "rsort-new":
                jid = runner.submit_resumable_sort(
                    args.input, args.output, args.state_dir,
                    dedup_key=args.dedup, max_line_bytes=args.max_line_bytes,
                    **tun)
            else:
                jid = runner.resume_sort(args.state_dir, **tun)
        except (ResumeError, ConfigError) as exc:
            print(f"REFUSED {type(exc).__name__}: {exc}", file=sys.stderr)
            runner.shutdown()
            return 3
        print(f"submitted {jid} (state dir: {args.state_dir})")
        st = runner.wait(jid)
        print(json.dumps(st, ensure_ascii=False))
        rep = runner.report()
        rep.pop("jobs", None)
        print("REPORT " + json.dumps(rep, ensure_ascii=False, indent=2))
        runner.shutdown()
        return {"DONE": 0, "FAILED": 1, "ERROR": 4, "CANCELLED": 5}.get(
            st["state"], 1)

    if args.cmd == "rsort-status":
        try:
            info = inspect_state_dir(args.state_dir,
                                     verify_runs=args.verify_runs)
        except ResumeError as exc:
            print(f"INVALID STATE {type(exc).__name__}: {exc}", file=sys.stderr)
            return 3
        print(json.dumps(info, ensure_ascii=False, indent=2, default=str))
        return 0 if info["ok"] else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
