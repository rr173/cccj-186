"""Resumable sort -- in-process tests.

Covers: create/complete (sort + dedup, byte-exact vs independent reference),
newline-less final line, empty input, checkpoint cadence, recoverable I/O
error + resume with a different budget, persistent cancel, suspend via
shutdown, run/progress corruption refusal, input identity binding,
semantics-change refusal, state-dir locking (two runners, one process),
sharing the arbiter with a plain sort job, idempotent resume of a DONE
task, target tampering after DONE, and the pre-publish conflict check.

Subprocess SIGKILL gates live in test_resume_kill.py.
"""
import json
import os
import threading

from util import make_input, check_invariants, new_workspace, Gate

from extsort import JobRunner, ConfigError
from extsort import verify
from extsort.resumable import (ResumeError, ResumeRefused, StateCorrupt,
                               StateLocked, inspect_state_dir)

MWS = 368768 + 2 * 65536          # SortJob geometry + 2*STATE_BUF slack
CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4, checkpoint_input_bytes=32768)


def read_progress(d):
    with open(os.path.join(d, "state", "progress.json")) as f:
        return json.load(f)


def make_runner(d, budget=2 * MWS, name="work"):
    return JobRunner(budget, os.path.join(d, name),
                     event_log_path=os.path.join(d, "events.jsonl"))


def submit(runner, d, inp, **kw):
    cfg = dict(CFG)
    cfg.update(kw)
    return runner.submit_resumable_sort(
        inp, os.path.join(d, "out", "out.sorted"), os.path.join(d, "state"),
        **cfg)


# ------------------------------------------------------------------ basics
def test_create_complete_sort_and_dedup():
    d = new_workspace("rs-basic")
    inp = make_input(os.path.join(d, "a.jsonl"), 8000, seed=11, pad=(0, 90))
    runner = make_runner(d)
    j1 = submit(runner, d, inp)
    st = runner.wait(j1, 60)
    assert st["state"] == "DONE", st
    got = open(os.path.join(d, "out", "out.sorted"), "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    assert got.endswith(b"\n")
    prog = read_progress(d)
    assert prog["phase"] == "done" and prog["stopped"] is None
    assert prog["generation"] == st["checkpoint_generation"] > 3
    # DONE reclaims run files; the durable record remains
    assert os.listdir(os.path.join(d, "state", "runs")) == []
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()

    d2 = new_workspace("rs-basic-dedup")
    inp2 = make_input(os.path.join(d2, "a.jsonl"), 8000, seed=12, pad=(0, 90))
    runner2 = make_runner(d2)
    j2 = submit(runner2, d2, inp2, dedup_key=True)
    st2 = runner2.wait(j2, 60)
    assert st2["state"] == "DONE", st2
    got2 = open(os.path.join(d2, "out", "out.sorted"), "rb").read()
    assert got2 == verify.expected_output_bytes_norm(inp2, True)
    assert st2["lines_written"] == 8   # 8 distinct keys in make_input
    runner2.shutdown()
    print("PASS create_complete_sort_and_dedup")


def test_newlineless_last_line():
    d = new_workspace("rs-nonl")
    inp = os.path.join(d, "nl.jsonl")
    with open(inp, "w") as f:
        f.write(json.dumps({"text": "zzz", "payload": {"orig": 0}}) + "\n")
        f.write(json.dumps({"text": "mmm", "payload": {"orig": 1}}) + "\n")
        # final line WITHOUT trailing newline, key sorts first
        f.write(json.dumps({"text": "aaa", "payload": {"orig": 2}}))
    runner = make_runner(d)
    j = submit(runner, d, inp)
    st = runner.wait(j, 30)
    assert st["state"] == "DONE", st
    got = open(os.path.join(d, "out", "out.sorted"), "rb").read()
    exp = verify.expected_output_bytes_norm(inp, False)
    assert got == exp, (got, exp)
    # the newline-less line landed first and is NOT glued to the next record
    lines = got.split(b"\n")
    assert lines[0].startswith(b'{"text": "aaa"') and len(lines) == 4
    assert got.endswith(b"\n")
    runner.shutdown()
    print("PASS newlineless_last_line")


def test_empty_input():
    d = new_workspace("rs-empty")
    inp = os.path.join(d, "e.jsonl")
    open(inp, "w").close()
    runner = make_runner(d)
    j = submit(runner, d, inp)
    st = runner.wait(j, 30)
    assert st["state"] == "DONE", st
    assert st["lines_written"] == 0
    assert os.path.getsize(os.path.join(d, "out", "out.sorted")) == 0
    runner.shutdown()
    print("PASS empty_input")


def test_checkpoint_cadence():
    d = new_workspace("rs-ckpt")
    inp = make_input(os.path.join(d, "c.jsonl"), 9000, seed=13, pad=(40, 90))
    size = os.path.getsize(inp)
    offsets = []

    def ckpt_hook(job, offset=0, **kw):
        offsets.append(offset)

    runner = make_runner(d)
    j = submit(runner, d, inp, checkpoint_input_bytes=65536,
               hooks={"checkpoint_done": ckpt_hook})
    st = runner.wait(j, 60)
    assert st["state"] == "DONE", st
    # real progress was persisted DURING the read, not only at the end
    assert offsets, "no checkpoints happened"
    assert offsets[0] < size, "first checkpoint only happened at EOF"
    assert offsets == sorted(offsets), "checkpoint offsets must be monotone"
    # threshold respected: consecutive confirmed offsets differ by about one
    # checkpoint interval (<= threshold + one read block)
    gaps = [b - a for a, b in zip([0] + offsets, offsets)]
    assert all(g <= 65536 + CFG["read_block_bytes"] + 2048 for g in gaps), gaps
    ev = runner.events.snapshot()
    ck = [e for e in ev if e["kind"] == "RSORT_CHECKPOINT"]
    assert ck and all(e["generation"] == i + 1
                      for i, e in enumerate(
            sorted(ck, key=lambda e: e["generation"])))
    runner.shutdown()
    print(f"PASS checkpoint_cadence ({len(offsets)} checkpoints, "
          f"first at {offsets[0]}/{size})")


def test_checkpoint_smaller_than_block():
    """checkpoint_input_bytes < read_block: the block read stops after the
    line that crosses the threshold (at most one more valid line), so the
    confirmed prefix advances in threshold-sized steps."""
    d = new_workspace("rs-ckpt-tiny")
    inp = make_input(os.path.join(d, "c.jsonl"), 6000, seed=30, pad=(40, 90))
    offsets = []
    runner = make_runner(d)
    j = submit(runner, d, inp, checkpoint_input_bytes=8192,
               hooks={"checkpoint_done":
                      lambda job, offset=0, **kw: offsets.append(offset)})
    st = runner.wait(j, 60)
    assert st["state"] == "DONE", st
    got = open(os.path.join(d, "out", "out.sorted"), "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    assert len(offsets) > 20, f"expected many small checkpoints: {len(offsets)}"
    gaps = [b - a for a, b in zip([0] + offsets, offsets)]
    # overshoot beyond the threshold is below one max line (we stop right
    # after the crossing line -- never a whole extra block)
    assert all(g <= 8192 + 2048 + 2 for g in gaps), gaps[:10]
    runner.shutdown()
    print(f"PASS checkpoint_smaller_than_block ({len(offsets)} checkpoints)")


# ------------------------------------------------------- error / cancel paths
def test_recoverable_io_error_then_resume_new_budget():
    d = new_workspace("rs-ioerr")
    inp = make_input(os.path.join(d, "io.jsonl"), 12000, seed=14, pad=(30, 80))
    runner = make_runner(d)
    # fail exactly one write, of the second spill run
    fails = {"n": 1}

    def before_write(job_id, tmp_path):
        if fails["n"] and "run-000001" in tmp_path:
            fails["n"] -= 1
            raise OSError("injected write failure")

    runner.engine.before_write_hook = before_write
    j = submit(runner, d, inp)
    st = runner.wait(j, 60)
    assert st["state"] == "ERROR", st
    assert "injected" in (st["error"] or "")
    prog = read_progress(d)
    assert prog["stopped"]["reason"] == "io_error"
    assert prog["stopped"]["recoverable"] is True
    confirmed = prog["read"]["confirmed_offset"]
    n_events = len(runner.events.snapshot())
    runner.shutdown()   # ERROR jobs are left resumable by shutdown

    # fault cleared; resume with a DIFFERENT budget and block sizes
    runner2 = JobRunner(MWS + 200000, os.path.join(d, "work2"),
                        event_log_path=os.path.join(d, "events2.jsonl"))
    j2 = runner2.resume_sort(os.path.join(d, "state"),
                             read_block_bytes=49152, max_fan_in=3)
    st2 = runner2.wait(j2, 60)
    assert st2["state"] == "DONE", st2
    assert st2["attempt"] == 2 and st2["recoveries"] == 1
    got = open(os.path.join(d, "out", "out.sorted"), "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    # the resumed attempt did not re-read the confirmed prefix
    opens = [e for e in runner2.events.snapshot()
             if e["kind"] == "RSORT_SOURCE_OPEN"]
    assert opens and opens[0]["offset"] == confirmed, (opens, confirmed)
    assert runner2.arbiter.total_reserved == 0
    runner2.shutdown()
    print(f"PASS recoverable_io_error_then_resume (resumed at {confirmed})")


def test_cancel_persists_and_never_revives():
    d = new_workspace("rs-cancel")
    inp = make_input(os.path.join(d, "c.jsonl"), 12000, seed=15, pad=(30, 80))
    runner = make_runner(d)
    seen = {"n": 0}

    def ckpt_hook(job, **kw):
        seen["n"] += 1
        if seen["n"] == 2:
            runner.cancel(jid_holder[0])

    jid_holder = []
    j = submit(runner, d, inp, hooks={"checkpoint_done": ckpt_hook})
    jid_holder.append(j)
    st = runner.wait(j, 60)
    assert st["state"] == "CANCELLED", st
    prog = read_progress(d)
    assert prog["stopped"]["reason"] == "cancelled"
    assert prog["stopped"]["recoverable"] is False
    assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
    # runs were reclaimed on cancel
    assert os.listdir(os.path.join(d, "state", "runs")) == []
    runner.shutdown()

    # resume must echo the terminal state and do NO work
    runner2 = make_runner(d, name="work2")
    j2 = runner2.resume_sort(os.path.join(d, "state"))
    st2 = runner2.wait(j2, 30)
    assert st2["state"] == "CANCELLED", st2
    ev2 = [e for e in runner2.events.snapshot()
           if e["kind"] in ("RSORT_SOURCE_OPEN", "RSORT_SOURCE_BLOCK",
                            "RSORT_MERGE_GROUP_EXEC", "RSORT_PUBLISHED")]
    assert not ev2, f"cancelled task did work: {ev2[:2]}"
    assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
    runner2.shutdown()
    print("PASS cancel_persists_and_never_revives")


def test_suspend_via_shutdown_is_resumable():
    d = new_workspace("rs-suspend")
    inp = make_input(os.path.join(d, "s.jsonl"), 12000, seed=16, pad=(30, 80))
    runner = make_runner(d)
    gate = Gate()
    seen = {"n": 0}

    def block_hook(job, **kw):
        seen["n"] += 1
        if seen["n"] == 4:
            gate.entered.set()
            gate.release.wait(30)

    j = submit(runner, d, inp, hooks={"block_read": block_hook})
    assert gate.entered.wait(30)
    # let one checkpoint land, then stop the process gracefully
    deadline = threading.Event()
    while not read_progress(d)["read"]["confirmed_offset"]:
        assert not deadline.wait(0.02), "no checkpoint happened"
    t = threading.Thread(target=runner.shutdown)
    t.start()
    gate.release.set()
    t.join(30)
    assert not t.is_alive()
    prog = read_progress(d)
    assert prog["stopped"] is None, "suspend must not record a terminal state"
    offset = prog["read"]["confirmed_offset"]
    assert offset > 0

    runner2 = make_runner(d, name="work2")
    j2 = runner2.resume_sort(os.path.join(d, "state"))
    st2 = runner2.wait(j2, 60)
    assert st2["state"] == "DONE", st2
    got = open(os.path.join(d, "out", "out.sorted"), "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    runner2.shutdown()
    print(f"PASS suspend_via_shutdown_is_resumable (offset {offset})")


# ------------------------------------------------------- corruption refusal
def _suspended_task(name, seed):
    """A task stopped mid-read (controlled crash) with confirmed runs."""
    d = new_workspace(name)
    inp = make_input(os.path.join(d, "t.jsonl"), 12000, seed=seed, pad=(30, 80))
    runner = make_runner(d)
    gate = Gate()
    seen = {"n": 0}

    def block_hook(job, **kw):
        seen["n"] += 1
        if seen["n"] == 5:
            gate.entered.set()
            gate.release.wait(30)

    j = submit(runner, d, inp, hooks={"block_read": block_hook})
    assert gate.entered.wait(30)
    deadline = threading.Event()
    while not read_progress(d)["read"]["confirmed_offset"]:
        assert not deadline.wait(0.02)
    t = threading.Thread(target=runner.shutdown)
    t.start()
    gate.release.set()
    t.join(30)
    return d, inp


def test_corrupt_run_refused():
    for mode in ("flip", "truncate"):
        d, inp = _suspended_task(f"rs-corrupt-{mode}", seed=17)
        prog = read_progress(d)
        victim = prog["runs"][0]
        path = os.path.join(d, "state", "runs", victim["name"])
        if mode == "flip":
            with open(path, "r+b") as f:
                b = bytearray(f.read())
                b[len(b) // 2] ^= 0xFF
                f.seek(0)
                f.write(b)
        else:
            os.truncate(path, victim["bytes"] - 100)
        gen_before = read_progress(d)["generation"]
        runner2 = make_runner(d, name="work2")
        j2 = runner2.resume_sort(os.path.join(d, "state"))
        st2 = runner2.wait(j2, 30)
        assert st2["state"] == "FAILED", st2
        assert "StateCorrupt" in (st2["error"] or ""), st2["error"]
        # explicit refusal: nothing recomputed, nothing deleted, no output
        assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
        assert os.path.exists(path), "corrupt run must be preserved"
        prog2 = read_progress(d)
        assert [r["name"] for r in prog2["runs"]] == \
               [r["name"] for r in prog["runs"]]
        assert prog2["stopped"]["reason"] == "failed"
        runner2.shutdown()
        print(f"PASS corrupt_run_refused ({mode})")


def test_corrupt_progress_refused_synchronously():
    d, inp = _suspended_task("rs-corrupt-prog", seed=18)
    with open(os.path.join(d, "state", "progress.json"), "w") as f:
        f.write("{ not json !!")
    runner2 = make_runner(d, name="work2")
    try:
        runner2.resume_sort(os.path.join(d, "state"))
    except StateCorrupt:
        pass
    else:
        raise AssertionError("corrupt progress.json was not refused")
    runner2.shutdown()
    print("PASS corrupt_progress_refused_synchronously")


# ------------------------------------------------------------- task binding
def test_input_identity_refused():
    for mode in ("replace-same-bytes", "append", "truncate", "touch"):
        d, inp = _suspended_task(f"rs-ident-{mode}", seed=19)
        with open(inp, "rb") as f:
            original = f.read()
        if mode == "replace-same-bytes":
            os.rename(inp, inp + ".bak")
            with open(inp, "wb") as f:
                f.write(original)          # same bytes, different inode
        elif mode == "append":
            with open(inp, "ab") as f:
                f.write(original[:100])
        elif mode == "truncate":
            os.truncate(inp, len(original) - 1000)
        else:
            os.utime(inp, None)
        gen_before = read_progress(d)["generation"]
        runs_before = sorted(os.listdir(os.path.join(d, "state", "runs")))
        runner2 = make_runner(d, name="work2")
        try:
            runner2.resume_sort(os.path.join(d, "state"))
        except ResumeRefused as exc:
            assert mode != "touch" or "mtime_ns" in str(exc) or "ctime_ns" in str(exc)
        else:
            raise AssertionError(f"{mode}: changed input was not refused")
        # refused BEFORE writing anything: progress and runs untouched
        assert read_progress(d)["generation"] == gen_before
        assert sorted(os.listdir(os.path.join(d, "state", "runs"))) == runs_before
        assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
        runner2.shutdown()
        print(f"PASS input_identity_refused ({mode})")


def test_semantics_change_refused():
    d, inp = _suspended_task("rs-semantics", seed=20)
    gen_before = read_progress(d)["generation"]
    runner2 = make_runner(d, name="work2")
    for kw in ({"dedup_key": True},
               {"max_line_bytes": 4096},
               {"max_fan_in": 10_000_000}):      # MWS blows the budget
        try:
            runner2.resume_sort(os.path.join(d, "state"), **kw)
        except ConfigError:
            pass
        else:
            raise AssertionError(f"{kw} was not refused")
    assert read_progress(d)["generation"] == gen_before
    # a compliant resume still works afterwards
    j = runner2.resume_sort(os.path.join(d, "state"))
    st = runner2.wait(j, 60)
    assert st["state"] == "DONE", st
    runner2.shutdown()
    print("PASS semantics_change_refused")


def test_state_dir_locking():
    d, inp = _suspended_task("rs-lock", seed=21)
    # runner A holds the lock with a parked job
    runner_a = make_runner(d, name="workA")
    gate = Gate()
    j = runner_a.resume_sort(os.path.join(d, "state"),
                             hooks={"resume_verified": gate.hook_fn()})
    assert gate.entered.wait(30)
    # runner B: resume and create are both refused immediately
    runner_b = make_runner(d, name="workB")
    try:
        runner_b.resume_sort(os.path.join(d, "state"))
    except StateLocked:
        pass
    else:
        raise AssertionError("locked state dir allowed a second resumer")
    try:
        runner_b.submit_resumable_sort(
            inp, os.path.join(d, "out", "b.sorted"), os.path.join(d, "state"),
            **CFG)
    except ResumeError:
        pass
    else:
        raise AssertionError("locked/non-empty state dir allowed create")
    gate.release.set()
    st = runner_a.wait(j, 60)
    assert st["state"] == "DONE", st
    runner_a.shutdown()
    runner_b.shutdown()
    print("PASS state_dir_locking")


def test_create_refusals():
    d = new_workspace("rs-create-ref")
    inp = make_input(os.path.join(d, "a.jsonl"), 10, seed=22)
    runner = make_runner(d)
    # non-empty state dir
    os.makedirs(os.path.join(d, "busy"))
    open(os.path.join(d, "busy", "junk"), "w").close()
    try:
        runner.submit_resumable_sort(inp, os.path.join(d, "out", "x"),
                                     os.path.join(d, "busy"), **CFG)
    except ResumeError:
        pass
    else:
        raise AssertionError("non-empty state dir accepted")
    # state dir mixed with input/output locations
    for bad in (os.path.dirname(inp), os.path.join(d, "out"), inp):
        try:
            runner.submit_resumable_sort(inp, os.path.join(d, "out", "x"),
                                         bad, **CFG)
        except ConfigError:
            pass
        else:
            raise AssertionError(f"state dir placement {bad} accepted")
    # MWS over budget rejected before anything is created
    try:
        runner.submit_resumable_sort(inp, os.path.join(d, "out", "x"),
                                     os.path.join(d, "s2"),
                                     max_line_bytes=8 * 1024 * 1024)
    except ConfigError:
        pass
    else:
        raise AssertionError("oversized MWS accepted")
    assert not os.path.exists(os.path.join(d, "s2", "task.json"))
    runner.shutdown()
    print("PASS create_refusals")


# ------------------------------------------------------------- done/publish
def test_done_resume_idempotent_and_target_tamper():
    for mode in ("intact", "missing", "replaced"):
        d = new_workspace(f"rs-done-{mode}")
        inp = make_input(os.path.join(d, "a.jsonl"), 3000, seed=23, pad=(0, 60))
        runner = make_runner(d)
        j = submit(runner, d, inp)
        assert runner.wait(j, 60)["state"] == "DONE"
        runner.shutdown()
        out = os.path.join(d, "out", "out.sorted")
        if mode == "missing":
            os.unlink(out)
        elif mode == "replaced":
            with open(out, "wb") as f:
                f.write(b'{"text": "attacker"}\n')
        runner2 = make_runner(d, name="work2")
        j2 = runner2.resume_sort(os.path.join(d, "state"))
        st2 = runner2.wait(j2, 30)
        if mode == "intact":
            assert st2["state"] == "DONE", st2
            ev2 = [e for e in runner2.events.snapshot()
                   if e["kind"] in ("RSORT_SOURCE_OPEN", "RSORT_MERGE_GROUP_EXEC",
                                    "RSORT_FINAL_MERGE_EXEC")]
            assert not ev2, "DONE task redid work"
        else:
            assert st2["state"] == "FAILED", st2
            assert "PublishConflict" in (st2["error"] or ""), st2["error"]
            # the durable record still says done; nothing was touched
            assert read_progress(d)["phase"] == "done"
            assert read_progress(d)["stopped"] is None
            if mode == "replaced":
                assert open(out, "rb").read() == b'{"text": "attacker"}\n'
        runner2.shutdown()
        print(f"PASS done_resume ({mode})")


def test_publish_conflict_before_rename():
    d = new_workspace("rs-pubconf")
    inp = make_input(os.path.join(d, "a.jsonl"), 3000, seed=24, pad=(0, 60))
    out = os.path.join(d, "out", "out.sorted")
    with open(out, "w") as f:
        f.write('{"text": "original"}\n')
    runner = make_runner(d)
    gate = Gate()

    def sneak(job, **kw):
        gate.entered.set()
        gate.release.wait(30)

    j = submit(runner, d, inp, hooks={"before_publish_rename": sneak})
    assert gate.entered.wait(30)
    with open(out, "w") as f:          # someone else rewrites the target
        f.write('{"text": "someone-else"}\n')
    gate.release.set()
    st = runner.wait(j, 30)
    assert st["state"] == "FAILED", st
    assert "PublishConflict" in (st["error"] or ""), st["error"]
    # the other writer's file is preserved; the task recorded the failure
    assert open(out, "rb").read() == b'{"text": "someone-else"}\n'
    assert read_progress(d)["stopped"]["reason"] == "failed"
    runner.shutdown()
    print("PASS publish_conflict_before_rename")


def test_cancel_races_with_publish():
    # cancel landing before the atomic rename: no publish, durable cancel
    d = new_workspace("rs-cancel-before-pub")
    inp = make_input(os.path.join(d, "a.jsonl"), 4000, seed=28, pad=(0, 60))
    out = os.path.join(d, "out", "out.sorted")
    runner = make_runner(d)
    gate = Gate()
    j = submit(runner, d, inp, hooks={"before_publish_rename": gate.hook_fn()})
    assert gate.entered.wait(30)
    runner.cancel(j)
    gate.release.set()
    st = runner.wait(j, 30)
    assert st["state"] == "CANCELLED", st
    assert not os.path.exists(out), "cancel before rename must not publish"
    assert read_progress(d)["stopped"]["reason"] == "cancelled"
    runner.shutdown()

    # cancel landing after the atomic rename: the task is DONE, the cancel
    # is too late and must not downgrade the durable record
    d = new_workspace("rs-cancel-after-pub")
    inp = make_input(os.path.join(d, "a.jsonl"), 4000, seed=29, pad=(0, 60))
    out = os.path.join(d, "out", "out.sorted")
    runner = make_runner(d)
    gate = Gate()
    j = submit(runner, d, inp,
               hooks={"after_rename_before_done": gate.hook_fn()})
    assert gate.entered.wait(30)
    runner.cancel(j)
    gate.release.set()
    st = runner.wait(j, 30)
    assert st["state"] == "DONE", st
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)
    prog = read_progress(d)
    assert prog["phase"] == "done" and prog["stopped"] is None
    runner.shutdown()
    print("PASS cancel_races_with_publish")


# ------------------------------------------------------------- data errors
def test_invalid_line_late_segment():
    d = new_workspace("rs-badline")
    inp = os.path.join(d, "bad.jsonl")
    nbad = 0
    with open(inp, "w") as f:
        for i in range(6000):
            f.write(json.dumps({"text": f"k{i % 50}", "payload": {"orig": i,
                               "pad": "y" * 40}}) + "\n")
        nbad = 6000
        f.write("{ this is not json }\n")
        for i in range(6001, 9000):
            f.write(json.dumps({"text": f"k{i % 50}", "payload": {"orig": i}}) + "\n")
    runner = make_runner(d)
    j = submit(runner, d, inp)
    st = runner.wait(j, 60)
    assert st["state"] == "FAILED", st
    assert f"line {nbad}" in (st["error"] or ""), st["error"]
    prog = read_progress(d)
    assert prog["stopped"]["reason"] == "failed"
    assert prog["stopped"]["recoverable"] is False
    assert prog["read"]["confirmed_offset"] > 0   # checkpoints happened first
    assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
    runner.shutdown()
    # resume echoes the terminal failure without reprocessing anything
    runner2 = make_runner(d, name="work2")
    j2 = runner2.resume_sort(os.path.join(d, "state"))
    st2 = runner2.wait(j2, 30)
    assert st2["state"] == "FAILED", st2
    ev2 = [e for e in runner2.events.snapshot()
           if e["kind"] in ("RSORT_SOURCE_OPEN", "RSORT_SOURCE_BLOCK")]
    assert not ev2, "terminal-failed task reprocessed input"
    runner2.shutdown()
    print("PASS invalid_line_late_segment")


def test_oversized_line_fails():
    d = new_workspace("rs-toolong")
    inp = os.path.join(d, "t.jsonl")
    with open(inp, "w") as f:
        for i in range(100):
            f.write(json.dumps({"text": f"k{i}", "payload": {"orig": i}}) + "\n")
        f.write(json.dumps({"text": "fat", "pad": "x" * 9000}) + "\n")
    runner = make_runner(d)
    j = submit(runner, d, inp)
    st = runner.wait(j, 30)
    assert st["state"] == "FAILED", st
    assert "max_line_bytes" in (st["error"] or "")
    assert not os.path.exists(os.path.join(d, "out", "out.sorted"))
    runner.shutdown()
    print("PASS oversized_line_fails")


# ------------------------------------------------------------- shared arbiter
def test_shared_arbiter_with_plain_sort():
    d = new_workspace("rs-shared")
    inp_a = make_input(os.path.join(d, "a.jsonl"), 9000, seed=25, pad=(20, 90))
    inp_b = make_input(os.path.join(d, "b.jsonl"), 3000, seed=26, pad=(0, 60))
    runner = make_runner(d, budget=2 * MWS + 102400)
    jr = submit(runner, d, inp_a)
    jp = runner.submit(inp_b, os.path.join(d, "out", "b.sorted"), job_id="plain",
                       max_line_bytes=2048, read_block_bytes=32768,
                       write_buf_bytes=32768, merge_chunk_bytes=32768,
                       max_fan_in=4)
    sr = runner.wait(jr, 60)
    sp = runner.wait(jp, 60)
    assert sr["state"] == "DONE" and sp["state"] == "DONE", (sr, sp)
    assert open(os.path.join(d, "out", "out.sorted"), "rb").read() == \
        verify.expected_output_bytes_norm(inp_a, False)
    assert verify.check_exact(inp_b, os.path.join(d, "out", "b.sorted"),
                              False)["ok"]
    assert not check_invariants(runner.events.snapshot(), runner.arbiter.budget)
    assert runner.arbiter.total_reserved == 0
    runner.shutdown()
    print("PASS shared_arbiter_with_plain_sort")


def test_inspect_state_dir():
    d = new_workspace("rs-inspect")
    inp = make_input(os.path.join(d, "a.jsonl"), 3000, seed=27, pad=(0, 60))
    runner = make_runner(d)
    j = submit(runner, d, inp)
    assert runner.wait(j, 60)["state"] == "DONE"
    info = inspect_state_dir(os.path.join(d, "state"), verify_runs=False)
    assert info["phase"] == "done" and info["ok"]
    assert info["locked_by_other_process"] is False
    assert info["attempts"] == 1
    runner.shutdown()
    print("PASS inspect_state_dir")


if __name__ == "__main__":
    test_create_complete_sort_and_dedup()
    test_newlineless_last_line()
    test_empty_input()
    test_checkpoint_cadence()
    test_checkpoint_smaller_than_block()
    test_recoverable_io_error_then_resume_new_budget()
    test_cancel_persists_and_never_revives()
    test_suspend_via_shutdown_is_resumable()
    test_corrupt_run_refused()
    test_corrupt_progress_refused_synchronously()
    test_input_identity_refused()
    test_semantics_change_refused()
    test_state_dir_locking()
    test_create_refusals()
    test_done_resume_idempotent_and_target_tamper()
    test_publish_conflict_before_rename()
    test_cancel_races_with_publish()
    test_invalid_line_late_segment()
    test_oversized_line_fails()
    test_shared_arbiter_with_plain_sort()
    test_inspect_state_dir()
    print("ALL RESUMABLE TESTS PASSED")
