"""Large concurrent run (the headline scenario):

* inputs generated at runtime, total size clearly exceeds the budget B;
* two long jobs run while a stream of short jobs is submitted;
* every output is verified independently (streaming order + stability via
  orig line numbers + line count + additive SHA-256 fingerprint);
* the full event log, output heads/tails and the accounting report (incl.
  real peak RSS) are saved to artifacts/;
* the budget invariant is checked on every single event.
"""
import json
import os
import time

from util import make_input, check_invariants

from extsort import JobRunner
from extsort import verify

BUDGET = 18 * 1024 * 1024
CFG = dict(max_line_bytes=4096, read_block_bytes=256 * 1024,
           write_buf_bytes=512 * 1024, merge_chunk_bytes=256 * 1024,
           max_fan_in=8)
KEYS = ([f"key-{i:05d}" for i in range(5000)] +
        ["中-%d" % i for i in range(500)] + ["🙂-%d" % i for i in range(200)])

ART = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "artifacts", time.strftime("large-%Y%m%d-%H%M%S"))


def head_tail(path, n=5):
    lines = []
    with open(path, "rb") as f:
        for i, line in enumerate(f):
            if i < n:
                lines.append(("head", i, line.decode("utf-8").rstrip()))
    tail = lines_tail(path, n)
    return lines + tail


def lines_tail(path, n):
    from collections import deque
    dq = deque(maxlen=n)
    with open(path, "rb") as f:
        for line in f:
            dq.append(line.decode("utf-8").rstrip())
    return [("tail", i, l) for i, l in enumerate(dq)]


def main():
    os.makedirs(ART, exist_ok=True)
    data = os.path.join(ART, "data")
    out = os.path.join(ART, "out")
    os.makedirs(data)
    os.makedirs(out)

    print("generating inputs ...")
    long_inputs, short_inputs = [], []
    for i in range(2):
        p = os.path.join(data, f"long{i}.jsonl")
        make_input(p, 100000, keys=KEYS, seed=100 + i, pad=(200, 400))
        long_inputs.append(p)
    for i in range(8):
        p = os.path.join(data, f"short{i}.jsonl")
        make_input(p, 5000, keys=KEYS, seed=200 + i, pad=(200, 400))
        short_inputs.append(p)
    total_in = sum(os.path.getsize(p) for p in long_inputs + short_inputs)
    print(f"total input {total_in/1e6:.1f} MB vs budget {BUDGET/1e6:.1f} MB "
          f"({total_in/BUDGET:.1f}x)")

    runner = JobRunner(BUDGET, os.path.join(ART, "work"),
                       event_log_path=os.path.join(ART, "events.jsonl"),
                       keep_events_in_memory=True)

    t0 = time.monotonic()
    long_ids = [runner.submit(p, os.path.join(out, f"long{i}.sorted"),
                              job_id=f"long{i}", **CFG)
                for i, p in enumerate(long_inputs)]

    # stream of short jobs while the long ones are running
    short_stats = []
    shorts_done_while_long_running = 0
    for i, p in enumerate(short_inputs):
        jid = runner.submit(p, os.path.join(out, f"short{i}.sorted"),
                            job_id=f"short{i}", **CFG)
        st = runner.wait(jid, 300)
        short_stats.append(st)
        assert st["state"] == "DONE", st
        long_states = [runner.wait(j, 0)["state"] for j in long_ids]
        if any(s not in ("DONE", "FAILED", "CANCELLED") for s in long_states):
            shorts_done_while_long_running += 1
    long_stats = [runner.wait(j, 600) for j in long_ids]
    for st in long_stats:
        assert st["state"] == "DONE", st
    elapsed = time.monotonic() - t0

    # ---- independent verification (streaming, no sorter code) ----
    verifications = {}
    for i, p in enumerate(long_inputs):
        fp = verify.stream_fingerprint(p)
        verifications[f"long{i}"] = verify.check_output_streaming(
            os.path.join(out, f"long{i}.sorted"), dedup=False,
            expect_count=fp["count"], expect_fingerprint=fp["fingerprint"])
    for i, p in enumerate(short_inputs):
        fp = verify.stream_fingerprint(p)
        verifications[f"short{i}"] = verify.check_output_streaming(
            os.path.join(out, f"short{i}.sorted"), dedup=False,
            expect_count=fp["count"], expect_fingerprint=fp["fingerprint"])
    bad = {k: v for k, v in verifications.items() if not v["ok"]}
    assert not bad, bad

    # ---- per-event budget invariant over the full log ----
    ev = runner.events.snapshot()
    violations = check_invariants(ev, BUDGET)
    assert not violations, violations[:5]
    assert runner.events.count("DOUBLE_RELEASE") == 0

    # ---- concurrency evidence: the two long jobs overlapped ----
    admits = [e for e in ev if e["kind"] == "JOB_ADMIT" and e["job"] in ("long0", "long1")]
    long_dones = [e for e in ev if e["kind"] == "JOB_STATE" and e.get("state") == "DONE"
                  and e["job"] in ("long0", "long1")]
    assert len(admits) >= 2 and admits[1]["seq"] < long_dones[0]["seq"], \
        "long jobs did not run concurrently"
    assert shorts_done_while_long_running > 0, "no short job completed during long jobs"
    waits = runner.events.count("RESERVE_WAIT_START")
    fails = runner.events.count("RESERVE_FAIL")
    admit_waits = runner.events.count("JOB_ADMIT_WAIT")
    reclaims = runner.events.count("RECLAIM_REQUEST")
    spills = sum(1 for e in ev if e["kind"] == "SPILL_SERIALIZED")
    # memory limit actually enforced: peak never exceeded budget, and the
    # arbiter actively reclaimed + queued admissions (fairness) under pressure
    assert reclaims > 0, "no cooperative reclaim happened"
    assert admit_waits > 0, "no admission queuing happened (fairness not exercised)"

    rep = runner.report()
    rep.pop("jobs")
    rep.update({
        "elapsed_sec": round(elapsed, 2),
        "total_input_bytes": total_in,
        "input_over_budget": round(total_in / BUDGET, 2),
        "events": len(ev),
        "reserve_waits": waits,
        "reserve_fails": fails,
        "admit_waits": admit_waits,
        "reclaim_rounds": reclaims,
        "spills": spills,
        "shorts_done_while_long_running": shorts_done_while_long_running,
        "verifications": {k: v["ok"] for k, v in verifications.items()},
    })
    assert rep["high_water_bytes"] <= BUDGET, rep
    assert rep["total_reserved_now"] == 0

    with open(os.path.join(ART, "report.json"), "w") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    with open(os.path.join(ART, "heads_tails.txt"), "w") as f:
        for name in sorted(os.listdir(out)):
            f.write(f"== {name} ==\n")
            for where, i, line in head_tail(os.path.join(out, name)):
                f.write(f"{where}[{i}] {line}\n")
    with open(os.path.join(ART, "verify.json"), "w") as f:
        json.dump(verifications, f, ensure_ascii=False, indent=2)

    runner.shutdown()
    print(f"PASS large_concurrent  elapsed={elapsed:.1f}s  "
          f"high_water={rep['high_water_bytes']}/{BUDGET}  "
          f"peak_rss={rep['peak_rss_bytes']/1e6:.0f}MB  "
          f"waits={waits} fails={fails} admit_waits={admit_waits} reclaims={reclaims} "
          f"spills={spills} shorts_during_long={shorts_done_while_long_running}")
    print(f"artifacts: {ART}")


if __name__ == "__main__":
    main()
