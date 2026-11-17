"""Failure isolation: a real disk-write failure (OSError injected at the write
syscall) fails only the affected job; the arbiter stays usable and the other
jobs complete for real."""
import os

from util import make_input, check_invariants, new_workspace

from extsort import JobRunner
from extsort import verify

MWS = 368768
CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4)


def main():
    d = new_workspace("iofail")
    inp_f = make_input(os.path.join(d, "f.jsonl"), 2100, seed=1, pad=(100, 200))
    inp_g = make_input(os.path.join(d, "g.jsonl"), 800, seed=2, pad=(0, 100))
    inp_h = make_input(os.path.join(d, "h.jsonl"), 300, seed=3, pad=(0, 60))

    runner = JobRunner(3 * MWS + 204800, os.path.join(d, "work"),
                       event_log_path=os.path.join(d, "events.jsonl"))

    jf = runner.submit(inp_f, os.path.join(d, "out", "f.sorted"), job_id="F",
                       inject_io_failure=True, **CFG)
    jg = runner.submit(inp_g, os.path.join(d, "out", "g.sorted"), job_id="G", **CFG)

    sf = runner.wait(jf, 60)
    sg = runner.wait(jg, 60)
    assert sf["state"] == "FAILED", sf
    assert "injected" in (sf["error"] or ""), sf["error"]
    assert sg["state"] == "DONE", sg
    assert verify.check_exact(inp_g, os.path.join(d, "out", "g.sorted"), False)["ok"]

    # failed job: reservations zeroed, temp files gone, no output published
    assert sf["held_bytes"] == 0
    assert not os.path.exists(os.path.join(d, "out", "f.sorted"))
    assert not os.path.exists(os.path.join(d, "work", "job-F"))

    # arbiter still usable after the failure: a new job runs fine
    jh = runner.submit(inp_h, os.path.join(d, "out", "h.sorted"), job_id="H", **CFG)
    sh = runner.wait(jh, 60)
    assert sh["state"] == "DONE", sh
    assert verify.check_exact(inp_h, os.path.join(d, "out", "h.sorted"), False)["ok"]

    ev = runner.events.snapshot()
    assert not check_invariants(ev, runner.arbiter.budget)
    fails = [e for e in ev if e["kind"] == "IO_WRITE_FAIL" and e["job"] == "F"]
    assert fails, "expected the injected write failure in the log"
    assert runner.events.count("DOUBLE_RELEASE") == 0
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS io_failure_isolation")


if __name__ == "__main__":
    main()
