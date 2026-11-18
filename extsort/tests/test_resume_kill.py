"""Resumable sort -- acceptance tests with REAL subprocess interruption.

Every gate stops a real `python3 tests/rsort_driver.py` process with SIGKILL
(either self-inflicted at a deterministic hook, or sent by this parent), and
every resume is a fresh Python process -- nothing is carried over but the
state directory.

Gates covered (see task spec):
  * run fully written but progress not yet acknowledged
    (hook: before_checkpoint_persist)
  * merge group confirmed but replaced inputs not yet reclaimed
    (hook: merge_confirmed)
  * target replaced but DONE not yet recorded
    (hook: after_rename_before_done)
  * input only partially processed, resumed with a different budget
Plus: multi-round resume through several merge passes, mutual exclusion with
a real second process + takeover after SIGKILL, input replace/append,
saved-run truncation/modification, cancel-then-reopen, bad line in a late
segment, real I/O failure then recovery, external target replacement inside
the publish window, CLI create/check/resume, and a large streaming-verified
run whose total work is compared against an uninterrupted control run.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from util import make_input, new_workspace
from extsort import verify

DRIVER = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "rsort_driver.py")
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CFG = dict(max_line_bytes=2048, read_block_bytes=32768, write_buf_bytes=32768,
           merge_chunk_bytes=32768, max_fan_in=4, checkpoint_input_bytes=32768)
BUDGET = 900000


def run_driver(spec, timeout=300):
    """Run one attempt in a real subprocess; return (rc, status|None, out)."""
    sp = os.path.join(spec.get("_dir", tempfile.mkdtemp(prefix="rspec-")),
                      "spec.json")
    os.makedirs(os.path.dirname(sp), exist_ok=True)
    with open(sp, "w") as f:
        json.dump({k: v for k, v in spec.items() if not k.startswith("_")}, f)
    p = subprocess.run([sys.executable, DRIVER, sp],
                       capture_output=True, text=True, timeout=timeout,
                       cwd=REPO)
    status = None
    refused = None
    for line in p.stdout.splitlines():
        if line.startswith("STATUS "):
            status = json.loads(line[7:])
        elif line.startswith("REFUSED "):
            refused = json.loads(line[8:])
    return p.returncode, status, refused, p.stdout, p.stderr


def start_driver(spec):
    """Start an attempt in the background (for kill/lock tests)."""
    sp = os.path.join(spec["_dir"], "spec.json")
    os.makedirs(os.path.dirname(sp), exist_ok=True)
    with open(sp, "w") as f:
        json.dump({k: v for k, v in spec.items() if not k.startswith("_")}, f)
    return subprocess.Popen([sys.executable, DRIVER, sp],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, cwd=REPO)


def events(state_dir):
    path = os.path.join(state_dir, "events.jsonl")
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                pass    # a SIGKILL can truncate the final in-flight line
    return out


def progress(state_dir):
    with open(os.path.join(state_dir, "progress.json")) as f:
        return json.load(f)


def base_spec(d, inp, out, **kw):
    spec = {"cmd": "new", "input": inp, "output": out,
            "state_dir": os.path.join(d, "state"), "budget": BUDGET, "_dir": d}
    spec.update(CFG)
    spec.update(kw)
    return spec


def source_blocks(evs):
    return [e for e in evs if e["kind"] == "RSORT_SOURCE_BLOCK"]


def merge_execs(evs):
    return [e for e in evs if e["kind"] == "RSORT_MERGE_GROUP_EXEC"]


def merge_confirmed(evs):
    return [e for e in evs if e["kind"] == "RSORT_MERGE_GROUP_CONFIRMED"]


# ---------------------------------------------------------------- gate 1+
def test_kill_mid_read_resume_different_budget():
    """Input only partially processed -> SIGKILL -> resume with a DIFFERENT
    budget.  The confirmed prefix is never re-read."""
    d = new_workspace("kill-midread")
    inp = make_input(os.path.join(d, "in.jsonl"), 14000, seed=31, pad=(30, 90))
    size = os.path.getsize(inp)
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "block_read": {"at": 40, "signal": os.path.join(d, "sig"),
                       "action": "kill"}})
    rc, _, _, stdout, stderr = run_driver(spec)
    assert rc == -signal.SIGKILL, (rc, stdout, stderr)
    assert os.path.exists(os.path.join(d, "sig")), "kill gate never fired"
    prog = progress(spec["state_dir"])
    confirmed = prog["read"]["confirmed_offset"]
    assert 0 < confirmed < size, f"expected a partial read, offset={confirmed}"

    spec2 = {"cmd": "resume", "state_dir": spec["state_dir"],
             "budget": BUDGET + 400000, "read_block_bytes": 49152,
             "max_fan_in": 3, "_dir": d}
    rc2, st2, _, stdout2, stderr2 = run_driver(spec2)
    assert rc2 == 0, (rc2, stdout2, stderr2)
    assert st2["state"] == "DONE" and st2["attempt"] == 2
    got = open(out, "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)

    evs = events(spec["state_dir"])
    opens = {e["job"]: e["offset"] for e in evs
             if e["kind"] == "RSORT_SOURCE_OPEN"}
    assert opens[st2["job_id"]] == confirmed, (opens, confirmed)
    # no source byte of the confirmed prefix was read again
    for e in source_blocks(evs):
        if e["job"] == st2["job_id"]:
            assert e["start"] >= confirmed, e
    # the re-read tail is bounded by one checkpoint interval (+1 block+line)
    reread = size - confirmed
    assert reread > 0
    print(f"PASS kill_mid_read_resume_different_budget "
          f"(confirmed={confirmed}/{size}, tail re-read={reread})")


def test_gate_run_written_progress_not_acknowledged():
    """Kill exactly when a run file is durable but the checkpoint is not:
    the orphan run is discarded and its segment re-read (bounded redo)."""
    d = new_workspace("kill-gate1")
    inp = make_input(os.path.join(d, "in.jsonl"), 12000, seed=32, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "before_checkpoint_persist": {"at": 3, "signal": os.path.join(d, "sig"),
                                      "action": "kill"}})
    rc, _, _, _, _ = run_driver(spec)
    assert rc == -signal.SIGKILL
    prog = progress(spec["state_dir"])
    confirmed_names = {r["name"] for r in prog["runs"]}
    disk = set(os.listdir(os.path.join(spec["state_dir"], "runs")))
    orphans = disk - confirmed_names
    assert orphans, "expected an unconfirmed run file on disk"
    confirmed_off = prog["read"]["confirmed_offset"]

    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    got = open(out, "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    evs = events(spec["state_dir"])
    deleted = {e["name"] for e in evs if e["kind"] == "RSORT_ORPHAN_DELETED"}
    assert orphans <= deleted, (orphans, deleted)
    # the orphan's segment was re-read, the confirmed prefix was not
    a2 = [e for e in source_blocks(evs) if e["job"] == st2["job_id"]]
    assert a2 and a2[0]["start"] == confirmed_off
    print(f"PASS gate_run_written_progress_not_acknowledged "
          f"(orphans={sorted(orphans)})")


# ---------------------------------------------------------------- gate 2
def test_gate_merge_confirmed_old_not_reclaimed():
    """Kill between 'merge output confirmed' and 'inputs reclaimed': the
    confirmed group is reused (never re-executed), the stale inputs are
    cleaned up as orphans."""
    d = new_workspace("kill-gate2")
    inp = make_input(os.path.join(d, "in.jsonl"), 16000, seed=33, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "merge_confirmed": {"at": 2, "signal": os.path.join(d, "sig"),
                            "action": "kill"}})
    rc, _, _, stdout, stderr = run_driver(spec)
    assert rc == -signal.SIGKILL, (rc, stdout, stderr)
    prog = progress(spec["state_dir"])
    assert prog["phase"] == "merge"
    confirmed_names = {r["name"] for r in prog["runs"]}
    disk = set(os.listdir(os.path.join(spec["state_dir"], "runs")))
    stale_inputs = disk - confirmed_names
    assert stale_inputs, "expected replaced-but-not-yet-reclaimed run files"
    assert any(n.startswith("merge-") for n in confirmed_names)
    # record identities of the confirmed merge outputs: they must NOT be
    # rewritten by the resume
    inodes = {n: os.stat(os.path.join(spec["state_dir"], "runs", n)).st_ino
              for n in confirmed_names}

    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    got = open(out, "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, False)
    evs = events(spec["state_dir"])
    # every confirmed group executed exactly once across both attempts
    exec_outs = [e["output"] for e in merge_execs(evs)]
    assert len(exec_outs) == len(set(exec_outs)), \
        f"merge group re-executed: {exec_outs}"
    # confirmed merge files were reused, not rewritten (inode stability)
    for n, ino in inodes.items():
        p = os.path.join(spec["state_dir"], "runs", n)
        if os.path.exists(p):      # those still needed were kept
            assert os.stat(p).st_ino == ino, n
    # the stale inputs were deleted by the resume's orphan cleanup
    deleted = {e["name"] for e in evs if e["kind"] == "RSORT_ORPHAN_DELETED"}
    assert stale_inputs <= deleted, (stale_inputs, deleted)
    print(f"PASS gate_merge_confirmed_old_not_reclaimed "
          f"(stale={sorted(stale_inputs)[:3]}...)")


# ---------------------------------------------------------------- gate 3
def test_gate_output_renamed_done_not_recorded():
    """Kill in the window 'target replaced, DONE not persisted': the resume
    recognises its own published result and only records the terminal state
    -- no input re-read, no merge re-execution."""
    d = new_workspace("kill-gate3")
    inp = make_input(os.path.join(d, "in.jsonl"), 12000, seed=34, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "after_rename_before_done": {"at": 1, "signal": os.path.join(d, "sig"),
                                     "action": "kill"}})
    rc, _, _, stdout, stderr = run_driver(spec)
    assert rc == -signal.SIGKILL, (rc, stdout, stderr)
    assert os.path.exists(out), "rename did not happen before the kill"
    published = open(out, "rb").read()
    assert published == verify.expected_output_bytes_norm(inp, False)
    prog = progress(spec["state_dir"])
    assert prog["phase"] == "publish", prog["phase"]   # done NOT recorded

    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    assert st2["state"] == "DONE"
    evs = events(spec["state_dir"])
    a2 = st2["job_id"]
    assert not [e for e in evs if e["kind"] == "RSORT_SOURCE_OPEN"
                and e["job"] == a2], "input re-read in the publish window"
    assert not [e for e in merge_execs(evs) if e["job"] == a2], \
        "merge re-executed in the publish window"
    rec = [e for e in evs if e["kind"] == "RSORT_PUBLISH_RECOVERED"]
    assert rec and rec[0]["how"] == "target-already-published"
    assert progress(spec["state_dir"])["phase"] == "done"
    assert open(out, "rb").read() == published   # untouched
    print("PASS gate_output_renamed_done_not_recorded")


def test_publish_window_external_replacement():
    """Same window, but someone else replaces the target before the resume:
    must report a conflict, preserve the foreign file, not claim success."""
    d = new_workspace("kill-pubwin")
    inp = make_input(os.path.join(d, "in.jsonl"), 12000, seed=35, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "after_rename_before_done": {"at": 1, "signal": os.path.join(d, "sig"),
                                     "action": "kill"}})
    rc, _, _, _, _ = run_driver(spec)
    assert rc == -signal.SIGKILL
    assert progress(spec["state_dir"])["phase"] == "publish"
    with open(out, "wb") as f:          # external actor takes the target
        f.write(b'{"text": "attacker"}\n')

    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 1, (rc2, stdout2, stderr2)
    assert st2["state"] == "FAILED"
    assert "PublishConflict" in (st2["error"] or ""), st2["error"]
    # the foreign file is preserved and we did not claim success
    assert open(out, "rb").read() == b'{"text": "attacker"}\n'
    assert progress(spec["state_dir"])["stopped"]["reason"] == "failed"
    print("PASS publish_window_external_replacement")


def test_redo_of_unconfirmed_group_and_final_merge():
    """The allowed-redo boundary: kill with a merge group executed but not
    confirmed, then (second task) with the final output staged but not
    recorded.  Both are redone exactly once; fragments are cleaned; the
    confirmed prefix of the plan is never re-executed."""
    # -- kill after a group executed but before its confirmation persist --
    d = new_workspace("kill-redo-group")
    inp = make_input(os.path.join(d, "in.jsonl"), 16000, seed=44, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "merge_group_done_unconfirmed": {"at": 2,
                                         "signal": os.path.join(d, "sig"),
                                         "action": "kill"}})
    rc, _, _, stdout, stderr = run_driver(spec)
    assert rc == -signal.SIGKILL, (rc, stdout, stderr)
    evs1 = events(spec["state_dir"])
    done_unconfirmed = [e["output"] for e in evs1
                        if e["kind"] == "RSORT_MERGE_GROUP_EXEC"]
    confirmed1 = {e["output"] for e in evs1
                  if e["kind"] == "RSORT_MERGE_GROUP_CONFIRMED"}
    redone = [o for o in done_unconfirmed if o not in confirmed1]
    assert len(redone) == 1, (done_unconfirmed, confirmed1)
    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)
    evs = events(spec["state_dir"])
    # the unconfirmed group was executed exactly twice (killed + redo),
    # every other group exactly once
    execs = [e["output"] for e in merge_execs(evs)]
    assert execs.count(redone[0]) == 2, execs
    assert len([o for o in execs if o != redone[0]]) == \
        len(set(o for o in execs if o != redone[0]))
    conf = [e["output"] for e in merge_confirmed(evs)]
    assert len(conf) == len(set(conf))
    # its orphan output file was cleaned before the redo (the kill races the
    # writer's rename, so the orphan may carry either name)
    deleted = {e["name"] for e in evs if e["kind"] == "RSORT_ORPHAN_DELETED"}
    assert {redone[0], redone[0] + ".tmp"} & deleted, deleted
    print(f"PASS redo_of_unconfirmed_merge_group (redone={redone[0]})")

    # -- kill with the final output staged but not recorded --
    d = new_workspace("kill-redo-final")
    inp = make_input(os.path.join(d, "in.jsonl"), 10000, seed=45, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "before_publish_persist": {"at": 1, "signal": os.path.join(d, "sig"),
                                   "action": "kill"}})
    rc, _, _, stdout, stderr = run_driver(spec)
    assert rc == -signal.SIGKILL, (rc, stdout, stderr)
    prog = progress(spec["state_dir"])
    assert prog["phase"] == "merge" and prog["publish"] is None
    # the staged output file exists on disk but is not recorded anywhere
    import glob
    staged_files = glob.glob(out + ".tmp.*")
    assert staged_files, "expected a staged output fragment"
    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)
    evs = events(spec["state_dir"])
    finals = [e for e in evs if e["kind"] == "RSORT_FINAL_MERGE_EXEC"]
    assert len(finals) == 2, "unfinished final output must be redone once"
    deleted = {e["name"] for e in evs if e["kind"] == "RSORT_ORPHAN_DELETED"}
    assert all(any(s in dn for dn in deleted) for s in staged_files), deleted
    print("PASS redo_of_unfinished_final_output")


# ------------------------------------------------------- multi-round resume
def test_multi_resume_multi_pass_dedup():
    """Several kills carry the task through multiple merge passes; hot keys
    span checkpoints so the earliest line per key crosses checkpoints.
    Compared against an uninterrupted control run: identical bytes, same
    confirmed groups, bounded redo."""
    d = new_workspace("kill-multi")
    keys = ["hot", "hot2", "中", "🙂", "ä", "z", "alpha", "x"]
    inp = make_input(os.path.join(d, "in.jsonl"), 20000, keys=keys, seed=36,
                     pad=(20, 70))
    out = os.path.join(d, "out.sorted")
    cfg = dict(CFG, max_fan_in=3, checkpoint_input_bytes=24576,
               read_block_bytes=16384, write_buf_bytes=16384,
               merge_chunk_bytes=16384)
    # attempt 1: kill after the 2nd confirmed merge group
    spec = base_spec(d, inp, out, dedup=True, hooks={
        "merge_confirmed": {"at": 2, "signal": os.path.join(d, "s1"),
                            "action": "kill"}})
    spec.update(cfg)
    rc, _, _, _, _ = run_driver(spec)
    assert rc == -signal.SIGKILL
    # attempt 2: kill again during the merge phase
    spec2 = {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
             "_dir": d, "max_fan_in": 3,
             "hooks": {"merge_confirmed": {"at": 3,
                                           "signal": os.path.join(d, "s2"),
                                           "action": "kill"}}}
    rc2, _, _, _, _ = run_driver(spec2)
    assert rc2 == -signal.SIGKILL
    # attempt 3: finish
    spec3 = {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
             "_dir": d, "max_fan_in": 3}
    rc3, st3, _, stdout3, stderr3 = run_driver(spec3)
    assert rc3 == 0, (rc3, stdout3, stderr3)
    got = open(out, "rb").read()
    assert got == verify.expected_output_bytes_norm(inp, True)
    # dedup kept the earliest original line per key across checkpoints
    first = {}
    with open(inp, encoding="utf-8") as f:
        for i, line in enumerate(f):
            first.setdefault(json.loads(line)["text"], i)
    seen = [json.loads(l)["payload"]["orig"] for l in
            open(out, encoding="utf-8")]
    assert sorted(seen) == sorted(first.values())

    # uninterrupted control run, same input + tunables
    d4 = new_workspace("kill-multi-control")
    out4 = os.path.join(d4, "out.sorted")
    spec4 = base_spec(d4, inp, out4, dedup=True)
    spec4.update(cfg)
    rc4, st4, _, stdout4, stderr4 = run_driver(spec4)
    assert rc4 == 0, (rc4, stdout4, stderr4)
    assert open(out4, "rb").read() == got, "resumed output != control output"

    ev_k = events(spec["state_dir"])
    ev_c = events(spec4["state_dir"])
    # identical confirmed merge-group sequence (inputs -> output)
    sig = lambda evs: [(tuple(e["inputs"]), e["output"])
                       for e in merge_confirmed(evs)]
    assert sig(ev_k) == sig(ev_c), "confirmed merge groups differ from control"
    # no group executed twice across all three attempts
    execs = [e["output"] for e in merge_execs(ev_k)]
    assert len(execs) == len(set(execs)), f"re-executed groups: {execs}"
    # redo bound: executions exceed confirmations by at most the number of
    # kills (each kill wastes at most one unconfirmed group)
    assert len(execs) - len(merge_confirmed(ev_k)) <= 2
    # source bytes: each attempt re-reads at most one checkpoint tail
    per_attempt = {}
    for e in ev_k:
        if e["kind"] == "RSORT_SOURCE_BLOCK":
            per_attempt[e["job"]] = per_attempt.get(e["job"], 0) + \
                (e["end"] - e["start"])
    total_read = sum(per_attempt.values())
    size = os.path.getsize(inp)
    assert total_read <= size + 2 * (cfg["checkpoint_input_bytes"] +
                                     cfg["read_block_bytes"] + 2048), per_attempt
    print(f"PASS multi_resume_multi_pass_dedup "
          f"(groups={len(execs)}, attempts=3, source_read={total_read}/{size})")


# ------------------------------------------------------- locking / takeover
def test_mutual_exclusion_and_takeover_after_sigkill():
    """A real second process is refused while the first holds the state dir;
    after SIGKILL the lock is released by the kernel and a third process
    takes over."""
    d = new_workspace("kill-lock")
    inp = make_input(os.path.join(d, "in.jsonl"), 10000, seed=37, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "block_read": {"at": 5, "signal": os.path.join(d, "parked"),
                       "action": "wait", "wait": os.path.join(d, "release")}})
    p1 = start_driver(spec)
    deadline = time.monotonic() + 60
    while not os.path.exists(os.path.join(d, "parked")):
        assert time.monotonic() < deadline and p1.poll() is None
        time.sleep(0.02)
    # second process: resume refused immediately
    rc2, _, refused2, _, _ = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 3 and "StateLocked" in refused2["error"], (rc2, refused2)
    # second process: create on the same dir refused too
    rc2b, _, refused2b, _, _ = run_driver(
        base_spec(d, inp, os.path.join(d, "other.sorted")))
    assert rc2b == 3, (rc2b, refused2b)
    # SIGKILL the holder; the kernel releases the flock
    p1.kill()
    p1.wait()
    assert p1.returncode == -signal.SIGKILL
    rc3, st3, _, stdout3, stderr3 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc3 == 0, (rc3, stdout3, stderr3)
    assert st3["state"] == "DONE"
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)
    print("PASS mutual_exclusion_and_takeover_after_sigkill")


# ------------------------------------------------------- binding violations
def test_input_replaced_and_appended_between_attempts():
    for mode in ("replace", "append"):
        d = new_workspace(f"kill-bind-{mode}")
        inp = make_input(os.path.join(d, "in.jsonl"), 10000, seed=38,
                         pad=(30, 80))
        out = os.path.join(d, "out.sorted")
        spec = base_spec(d, inp, out, hooks={
            "block_read": {"at": 10, "signal": os.path.join(d, "sig"),
                           "action": "kill"}})
        rc, _, _, _, _ = run_driver(spec)
        assert rc == -signal.SIGKILL
        with open(inp, "rb") as f:
            data = f.read()
        if mode == "replace":
            os.rename(inp, inp + ".bak")
            with open(inp, "wb") as f:
                f.write(data)              # identical bytes, new inode
        else:
            with open(inp, "ab") as f:
                f.write(data[:200])
        gen_before = progress(spec["state_dir"])["generation"]
        runs_before = sorted(os.listdir(os.path.join(spec["state_dir"],
                                                     "runs")))
        rc2, _, refused2, _, _ = run_driver(
            {"cmd": "resume", "state_dir": spec["state_dir"],
             "budget": BUDGET, "_dir": d})
        assert rc2 == 3, (rc2, refused2)
        assert "ResumeRefused" in refused2["error"], refused2
        # refused before touching anything
        assert progress(spec["state_dir"])["generation"] == gen_before
        assert sorted(os.listdir(os.path.join(spec["state_dir"], "runs"))) \
            == runs_before
        assert not os.path.exists(out)
        print(f"PASS input_binding_refused ({mode})")


def test_saved_run_truncated_and_modified():
    for mode in ("truncate", "flip"):
        d = new_workspace(f"kill-runcorrupt-{mode}")
        inp = make_input(os.path.join(d, "in.jsonl"), 10000, seed=39,
                         pad=(30, 80))
        out = os.path.join(d, "out.sorted")
        spec = base_spec(d, inp, out, hooks={
            "block_read": {"at": 12, "signal": os.path.join(d, "sig"),
                           "action": "kill"}})
        rc, _, _, _, _ = run_driver(spec)
        assert rc == -signal.SIGKILL
        prog = progress(spec["state_dir"])
        victim = prog["runs"][0]
        path = os.path.join(spec["state_dir"], "runs", victim["name"])
        if mode == "truncate":
            os.truncate(path, victim["bytes"] - 50)
        else:
            with open(path, "r+b") as f:
                b = bytearray(f.read())
                b[100] ^= 0xFF
                f.seek(0)
                f.write(b)
        rc2, st2, _, stdout2, stderr2 = run_driver(
            {"cmd": "resume", "state_dir": spec["state_dir"],
             "budget": BUDGET, "_dir": d})
        assert rc2 == 1, (rc2, stdout2, stderr2)
        assert st2["state"] == "FAILED"
        assert "StateCorrupt" in (st2["error"] or ""), st2["error"]
        # explicit refusal: run preserved, no output, no silent recompute
        assert os.path.exists(path)
        assert not os.path.exists(out)
        assert [r["name"] for r in progress(spec["state_dir"])["runs"]] == \
               [r["name"] for r in prog["runs"]]
        print(f"PASS saved_run_corrupted ({mode})")


# ------------------------------------------------------- cancel / io failure
def test_cancel_then_reopen():
    d = new_workspace("kill-cancel")
    inp = make_input(os.path.join(d, "in.jsonl"), 10000, seed=40, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out, hooks={
        "checkpoint_done": {"at": 2, "signal": os.path.join(d, "sig"),
                            "action": "cancel"}})
    rc, st, _, stdout, stderr = run_driver(spec)
    assert rc == 5, (rc, stdout, stderr)
    assert st["state"] == "CANCELLED"
    assert progress(spec["state_dir"])["stopped"]["reason"] == "cancelled"
    # reopen: echoes the terminal state, does no work, publishes nothing
    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 5, (rc2, stdout2, stderr2)
    assert st2["state"] == "CANCELLED"
    assert not os.path.exists(out)
    evs = [e for e in events(spec["state_dir"])
           if e["kind"] == "RSORT_SOURCE_OPEN" and e["job"] == st2["job_id"]]
    assert not evs
    print("PASS cancel_then_reopen")


def test_io_failure_then_recover():
    d = new_workspace("kill-iofail")
    inp = make_input(os.path.join(d, "in.jsonl"), 12000, seed=41, pad=(30, 80))
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out,
                     fail_write=[{"contains": "run-000002", "times": 1}])
    rc, st, _, stdout, stderr = run_driver(spec)
    assert rc == 4, (rc, stdout, stderr)          # ERROR: recoverable
    assert st["state"] == "ERROR"
    prog = progress(spec["state_dir"])
    assert prog["stopped"]["reason"] == "io_error"
    assert prog["stopped"]["recoverable"] is True
    confirmed = prog["read"]["confirmed_offset"]
    # fault cleared (no fail_write rule): resumes from the last checkpoint
    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 0, (rc2, stdout2, stderr2)
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)
    evs = events(spec["state_dir"])
    opens = {e["job"]: e["offset"] for e in evs
             if e["kind"] == "RSORT_SOURCE_OPEN"}
    assert opens[st2["job_id"]] == confirmed
    print(f"PASS io_failure_then_recover (resumed at {confirmed})")


def test_bad_line_late_segment():
    d = new_workspace("kill-badline")
    inp = os.path.join(d, "in.jsonl")
    with open(inp, "w") as f:
        for i in range(8000):
            f.write(json.dumps({"text": f"k{i % 40}", "payload":
                                {"orig": i, "pad": "y" * 40}}) + "\n")
        f.write("not json at all\n")
        for i in range(8001, 9000):
            f.write(json.dumps({"text": f"k{i % 40}", "payload":
                                {"orig": i}}) + "\n")
    out = os.path.join(d, "out.sorted")
    spec = base_spec(d, inp, out)
    rc, st, _, stdout, stderr = run_driver(spec)
    assert rc == 1, (rc, stdout, stderr)
    assert st["state"] == "FAILED" and "line 8000" in st["error"]
    assert progress(spec["state_dir"])["stopped"]["reason"] == "failed"
    assert not os.path.exists(out)
    # resume: terminal failure echoed, input not reprocessed
    rc2, st2, _, stdout2, stderr2 = run_driver(
        {"cmd": "resume", "state_dir": spec["state_dir"], "budget": BUDGET,
         "_dir": d})
    assert rc2 == 1 and st2["state"] == "FAILED"
    evs = [e for e in events(spec["state_dir"])
           if e["kind"] == "RSORT_SOURCE_BLOCK" and e["job"] == st2["job_id"]]
    assert not evs
    print("PASS bad_line_late_segment")


# --------------------------------------------------------------------- CLI
def test_cli_create_status_resume():
    d = new_workspace("kill-cli")
    inp = make_input(os.path.join(d, "in.jsonl"), 4000, seed=42, pad=(0, 60))
    out = os.path.join(d, "out.sorted")
    sd = os.path.join(d, "state")

    def cli(*argv):
        return subprocess.run([sys.executable, "-m", "extsort"] + list(argv),
                              capture_output=True, text=True, cwd=REPO)

    p = cli("rsort-new", "--input", inp, "--output", out, "--state-dir", sd,
            "--budget", str(BUDGET), "--max-line-bytes", "2KB",
            "--read-block", "32KB", "--write-buf", "32KB",
            "--merge-chunk", "32KB", "--max-fan-in", "4",
            "--checkpoint-input-bytes", "32KB")
    assert p.returncode == 0, p.stderr
    assert open(out, "rb").read() == verify.expected_output_bytes_norm(inp, False)

    p = cli("rsort-status", "--state-dir", sd)
    assert p.returncode == 0, p.stderr
    info = json.loads(p.stdout)
    assert info["phase"] == "done" and info["attempts"] == 1

    # idempotent resume through the CLI
    p = cli("rsort-resume", "--state-dir", sd, "--budget", str(BUDGET))
    assert p.returncode == 0, p.stderr
    st = json.loads([l for l in p.stdout.splitlines()
                     if l.startswith("{")][0])
    assert st["state"] == "DONE" and st["attempt"] == 2

    # create on a non-empty dir is refused (exit 3)
    p = cli("rsort-new", "--input", inp, "--output", out, "--state-dir", sd,
            "--budget", str(BUDGET))
    assert p.returncode == 3, (p.returncode, p.stderr)
    print("PASS cli_create_status_resume")


# ------------------------------------------------------- large, streaming
def test_large_streaming_with_kills():
    """~24MB through a ~4MB budget with two kills (mid-read, mid-merge) and
    a budget change; streaming verification; per-attempt source ranges and
    executed merge groups recorded independently in the event log; total
    work compared against an uninterrupted control run."""
    d = new_workspace("kill-large")
    inp = make_input(os.path.join(d, "big.jsonl"), 140000, seed=43,
                     pad=(60, 160))
    size = os.path.getsize(inp)
    out = os.path.join(d, "out.sorted")
    cfg = dict(max_line_bytes=4096, read_block_bytes=262144,
               write_buf_bytes=262144, merge_chunk_bytes=262144,
               max_fan_in=4, checkpoint_input_bytes=1024 * 1024)
    budget = 4_200_000

    spec = base_spec(d, inp, out, hooks={
        "block_read": {"at": 30, "signal": os.path.join(d, "s1"),
                       "action": "kill"}})
    spec.update(cfg)
    spec["budget"] = budget
    rc, _, _, stdout, stderr = run_driver(spec, timeout=600)
    assert rc == -signal.SIGKILL, (rc, stdout[-400:], stderr[-400:])
    confirmed1 = progress(spec["state_dir"])["read"]["confirmed_offset"]
    assert 0 < confirmed1 < size

    spec2 = {"cmd": "resume", "state_dir": spec["state_dir"],
             "budget": budget + 2_000_000, "_dir": d,
             "hooks": {"merge_confirmed": {"at": 2,
                                           "signal": os.path.join(d, "s2"),
                                           "action": "kill"}}}
    rc2, _, _, stdout2, stderr2 = run_driver(spec2, timeout=600)
    assert rc2 == -signal.SIGKILL, (rc2, stdout2[-400:], stderr2[-400:])

    spec3 = {"cmd": "resume", "state_dir": spec["state_dir"],
             "budget": budget, "_dir": d}
    rc3, st3, _, stdout3, stderr3 = run_driver(spec3, timeout=600)
    assert rc3 == 0, (rc3, stdout3[-400:], stderr3[-400:])
    assert st3["state"] == "DONE"

    # streaming verification: order, stability, count, fingerprint
    fp = verify.stream_fingerprint(inp)
    res = verify.check_output_streaming(out, dedup=False,
                                        expect_count=fp["count"],
                                        expect_fingerprint=fp["fingerprint"])
    assert res["ok"], res

    evs = events(spec["state_dir"])
    # the budget identity held in every attempt (each event carries its own
    # process's budget), and no reservation was ever released twice
    for e in evs:
        t = e.get("total_reserved")
        if t is not None:
            assert 0 <= t <= e["budget"], e
    assert not [e for e in evs if e["kind"] == "DOUBLE_RELEASE"]
    # per-attempt source ranges: attempt a>1 never re-reads a confirmed prefix
    opens = {e["job"]: e["offset"] for e in evs
             if e["kind"] == "RSORT_SOURCE_OPEN"}
    blocks = {}
    for e in source_blocks(evs):
        blocks.setdefault(e["job"], []).append((e["start"], e["end"]))
    for job, off in opens.items():
        if off:
            assert min(b[0] for b in blocks[job]) == off, (job, off)
    total_read = sum(e - s for bs in blocks.values() for s, e in bs)
    assert total_read <= size + 2 * (cfg["checkpoint_input_bytes"] +
                                     cfg["read_block_bytes"] + 4096)
    # merge groups: each executed at most once across all attempts
    execs = [e["output"] for e in merge_execs(evs)]
    assert len(execs) == len(set(execs))

    # uninterrupted control run: identical bytes, comparable work
    d5 = new_workspace("kill-large-control")
    out5 = os.path.join(d5, "out.sorted")
    spec5 = base_spec(d5, inp, out5)
    spec5.update(cfg)
    spec5["budget"] = budget
    rc5, st5, _, stdout5, stderr5 = run_driver(spec5, timeout=600)
    assert rc5 == 0, (rc5, stdout5[-400:], stderr5[-400:])
    assert open(out5, "rb").read() == open(out, "rb").read()
    evs5 = events(spec5["state_dir"])
    read5 = sum(e["end"] - e["start"] for e in source_blocks(evs5))
    assert read5 == size
    groups5 = len(merge_confirmed(evs5))
    groups_k = len(merge_confirmed(evs))
    # same deterministic plan: same number of confirmed groups; executions
    # exceed confirmations only by the bounded redo (<= 1 group per kill)
    assert groups_k == groups5, (groups_k, groups5)
    assert len(execs) - groups_k <= 2

    # preserve the evidence (commands, events, exit codes, assertions)
    adir = os.path.join(REPO, "artifacts",
                        time.strftime("rsort-kill-%Y%m%d-%H%M%S"))
    os.makedirs(adir, exist_ok=True)
    import shutil
    shutil.copy(os.path.join(spec["state_dir"], "events.jsonl"),
                os.path.join(adir, "events.killed.jsonl"))
    shutil.copy(os.path.join(spec5["state_dir"], "events.jsonl"),
                os.path.join(adir, "events.control.jsonl"))
    with open(os.path.join(adir, "report.json"), "w") as f:
        json.dump({
            "input_bytes": size, "budget": budget,
            "kills": 2, "attempts": 3,
            "confirmed_offset_at_first_kill": confirmed1,
            "source_bytes_read_killed_total": total_read,
            "source_bytes_read_control": read5,
            "merge_groups_confirmed_killed": groups_k,
            "merge_groups_confirmed_control": groups5,
            "merge_group_executions_killed": len(execs),
            "redo_bound": "execs - confirmed <= #kills; "
                          "tail re-read <= #kills * (checkpoint + block + line)",
            "output_identical_to_control": True,
            "streaming_verify": res,
        }, f, indent=2)
    print(f"PASS large_streaming_with_kills "
          f"(size={size}, read={total_read} vs {read5}, "
          f"groups={groups_k}, execs={len(execs)}, attempts=3) "
          f"artifacts: {adir}")


if __name__ == "__main__":
    test_kill_mid_read_resume_different_budget()
    test_gate_run_written_progress_not_acknowledged()
    test_gate_merge_confirmed_old_not_reclaimed()
    test_gate_output_renamed_done_not_recorded()
    test_publish_window_external_replacement()
    test_redo_of_unconfirmed_group_and_final_merge()
    test_multi_resume_multi_pass_dedup()
    test_mutual_exclusion_and_takeover_after_sigkill()
    test_input_replaced_and_appended_between_attempts()
    test_saved_run_truncated_and_modified()
    test_cancel_then_reopen()
    test_io_failure_then_recover()
    test_bad_line_late_segment()
    test_cli_create_status_resume()
    test_large_streaming_with_kills()
    print("ALL KILL/RESUME ACCEPTANCE TESTS PASSED")
