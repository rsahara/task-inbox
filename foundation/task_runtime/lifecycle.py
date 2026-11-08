"""Optional local supervisor and conservative, resumable worktree cleanup."""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from . import leases, runtime
from .store import (CONTROL, Snapshot, TaskError, atomic_json, digest, encode, git,
                    git_support, identity, now, refs_overlap)


def execute(store, task_id, run_id, command):
    """Heartbeats run in the supervisor, independently of a long child command.

    The supervisor never auto-restarts the child after losing its own process.
    Replayed execution requires explicit inspection, unlike replayed acquisition.
    """
    if not command:
        raise TaskError("A command is required after --")
    state_path = runtime.local_state(store, task_id, run_id)
    marker = state_path.with_suffix(".execution.json")
    with store.lock(state_path.with_suffix(".execution.lock")):
        run = runtime.check(store, task_id, run_id)
        if run.get("continuation"):
            raise TaskError("A checkpoint continuation is delivery-only; do not execute its old computation")
        if not state_path.exists():
            raise TaskError("Prepare the run workspace first")
        state = json.loads(state_path.read_text())
        if marker.exists():
            raise TaskError("This run already launched a command; inspect its execution receipt")
        record = {"run_id": run_id, "generation": run["generation"], "phase": "launching",
                  "launch_id": uuid.uuid4().hex, "started_at": now()}
        atomic_json(marker, record)
        # The local marker alone cannot exclude a second clone. Consume one
        # durable launch right before any worker process is created. A crash
        # between this commit and spawn is intentionally not auto-replayed.
        def claim(snapshot):
            current = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
            if current.get("execution_claim"):
                raise TaskError("This run already has an execution claim; inspect or recover it")
            current["execution_claim"] = {"launch_id": record["launch_id"], "claimed_at": now(),
                                          "generation": current["generation"]}
            return {runtime.run_path(task_id, run_id, snapshot) + "/run.json": encode(current)}, current["execution_claim"]
        store.transact(digest(["run-execution", task_id, run_id])[:48],
                       {"kind": "run-execution", "task_id": task_id, "run_id": run_id,
                        "launch_id": record["launch_id"]}, claim)
        runtime.heartbeat(store, task_id, run_id, progress="starting")
        # Register before spawn, and use the existing cancellation/process-group
        # owner for the worker and its descendants as well as Git children.
        owner = git_support.ProcessOwner()
        with git_support.cancellation_signals(owner):
            try:
                owner.start(command, state["path"])
                record.update(phase="running", pid=owner.process.pid)
                atomic_json(marker, record)
                # Drain output to owned files rather than retaining an unbounded
                # communicate buffer while sending heartbeat updates.
                import selectors
                selector = selectors.DefaultSelector()
                for stream, channel in ((owner.process.stdout, "stdout"), (owner.process.stderr, "stderr")):
                    os.set_blocking(stream.fileno(), False)
                    selector.register(stream, selectors.EVENT_READ, channel)
                owner.process.stdin.close()
                next_beat = time.monotonic() + run["execution"]["heartbeat_seconds"]
                with marker.with_suffix(".stdout.log").open("ab") as out, marker.with_suffix(".stderr.log").open("ab") as err:
                    logs = {"stdout": out, "stderr": err}
                    while owner.process.poll() is None:
                        for key, _ in selector.select(timeout=min(0.5, max(0, next_beat - time.monotonic()))):
                            data = os.read(key.fileobj.fileno(), 65536)
                            if data:
                                logs[key.data].write(data); logs[key.data].flush()
                            else:
                                selector.unregister(key.fileobj)
                        if time.monotonic() >= next_beat:
                            # Fail closed on an unconfirmed heartbeat or revoked
                            # grant. A new attempt gets a new run, never this ID.
                            runtime.heartbeat(store, task_id, run_id, progress="running")
                            next_beat = time.monotonic() + run["execution"]["heartbeat_seconds"]
                    for key in list(selector.get_map().values()):
                        while True:
                            try:
                                data = os.read(key.fileobj.fileno(), 65536)
                            except BlockingIOError:
                                break
                            if not data:
                                break
                            logs[key.data].write(data)
                selector.close()
                result = owner.process.returncode
                owner.process.stdin = None
                owner.close()
                record.update(phase="exited", exit_code=result, finished_at=now())
                atomic_json(marker, record)
                return record
            finally:
                # communicate cannot flush a manually closed stdin.
                if owner.process and owner.process.stdin and owner.process.stdin.closed:
                    owner.process.stdin = None
                owner.close()
                if record["phase"] != "exited":
                    record.update(phase="interrupted", stopped_at=now())
                    atomic_json(marker, record)


def reconcile_stop(store, operation_id, task_id, run_id, program):
    """Invoke an explicitly installed, trusted executor adapter, never repo code.

    The adapter owns authentication and verification of child termination and
    Git/external receipts. Its response is bound to this exact run generation.
    """
    program = Path(program)
    if not program.is_absolute() or not program.is_file():
        raise TaskError("Stop adapter must be an absolute path to an installed executable")
    receipt = store.snapshot().metadata(CONTROL + "/operations/" + identity(operation_id) + ".json")
    if receipt:
        previous = receipt["result"]
        if previous.get("task_id") != task_id or previous.get("run_id") != run_id or previous.get("recovery_kind") != "confirmed_stopped":
            raise TaskError("Operation ID was already used for another recovery")
        return runtime.recover(store, operation_id, task_id, run_id, stopped=True,
                               settled=True, evidence=previous["recovery_evidence"])
    run = runtime.check(store, task_id, run_id)
    try:
        result = git_support.run_process([str(program)], cwd=store.repo, input=encode(run), timeout=60)
    except subprocess.TimeoutExpired:
        raise TaskError("Executor stop adapter timed out; ownership retained") from None
    if result.returncode:
        raise TaskError("Executor stop adapter failed; ownership retained")
    try:
        if len(result.stdout) > 1024 * 1024:
            raise ValueError("oversize")
        evidence = json.loads(result.stdout)
    except (ValueError, UnicodeError):
        raise TaskError("Executor stop adapter returned invalid evidence") from None
    if not isinstance(evidence, dict):
        raise TaskError("Executor stop adapter returned invalid evidence")
    fields = ("repository_id", "authority_ref", "task_id", "run_id", "generation")
    if any(evidence.get(k) != run[k] for k in fields):
        raise TaskError("Executor stop evidence belongs to another execution")
    checks = ("writer_stopped", "children_stopped", "external_operations_settled", "pending_git_operations_settled")
    missing = [k for k in checks if evidence.get(k) is not True]
    if missing:
        raise TaskError("Executor evidence is missing: " + ", ".join(missing))
    if not isinstance(evidence.get("evidence"), str) or not evidence["evidence"]:
        raise TaskError("Executor did not provide a durable evidence reference")
    return runtime.recover(store, operation_id, task_id, run_id,
                           stopped=True, settled=True, evidence=evidence["evidence"])


def reap(store, task_id, *, apply=False, stop_check=None, observer_profile="unknown"):
    """Run at the beginning of an existing schedule; no daemon is required."""
    if observer_profile not in ("unknown", "retained", "stateless"):
        raise TaskError("Unsupported observer profile")
    snapshot = store.snapshot()
    _, registry = runtime.load_task(snapshot, task_id)
    rows = []
    for slot in registry["slots"].values():
        run_id = slot.get("run_id")
        if not run_id:
            continue
        operation = digest(["recover", task_id, run_id, slot["generation"]])[:48]
        try:
            record = runtime.load_run(store.snapshot(), task_id, run_id)[2]
            if record["execution"]["recovery_mode"] in leases.MODES:
                observation = leases.inspect(store, task_id, run_id)
                if apply and observation["expired"]:
                    result = leases.revoke(store, operation, task_id, run_id)
                    rows.append(dict(observation, status=result["status"], physical_stop_confirmed=False,
                                     business_recovery="only_destinations_covered_by_lease_policy"))
                else:
                    rows.append(dict(observation, status="recovery_ready" if observation["expired"] else
                                     "lease_active" if record["execution"]["recovery_mode"] == leases.MODE else "timeout_active"))
                continue
            if apply and stop_check:
                result = reconcile_stop(store, operation, task_id, run_id, stop_check)
                rows.append({"run_id": run_id, "status": result["status"]})
                continue
            observation = runtime.observe(store, task_id, run_id)
            if apply and observation["stale"] and observation["recovery_mode"] == "git_only":
                result = runtime.recover(store, operation, task_id, run_id, stale=True)
                rows.append({"run_id": run_id, "status": result["status"]})
            else:
                if observation["recovery_mode"] == "manual":
                    status = "stop_evidence_required"
                    reason = "run_stop_check_to_verify_evidence" if stop_check else "install_stop_check_or_verify_stop_manually"
                    horizon = "requires_stop_evidence"
                elif observer_profile == "stateless":
                    status, reason, horizon = ("recovery_policy_required", "stateless_observer_has_no_durable_clock",
                                               "not_established")
                elif observer_profile == "unknown" and observation["observation_reset"] in (
                        "first_observation", "boot_changed", "clock_regressed"):
                    status, reason, horizon = ("recovery_policy_required", "declare_retained_observer_or_install_stop_check",
                                               "not_established")
                elif observation["stale"]:
                    status, reason, horizon = "recovery_ready", "run_reap_apply", "observed"
                else:
                    status, reason, horizon = "observing", "observe_again_with_same_monotonic_clock_state", "retained_observer_required"
                rows.append(dict(observation, status=status, reason=reason, recovery_horizon=horizon,
                                 observer_profile=observer_profile,
                                 external_effect_safety="requires_stop_evidence" if observation["recovery_mode"] == "manual" else "git_only"))
        except (TaskError, git_support.GitFailure, subprocess.TimeoutExpired) as error:
            reason = ("stop_check_timeout" if isinstance(error, subprocess.TimeoutExpired) else
                      error.diagnostic.get("category", "git_failure") if isinstance(error, git_support.GitFailure) else str(error))
            rows.append({"run_id": run_id, "status": "unknown_or_blocked", "reason": reason})
    return {"task_id": task_id, "runs": rows, "needs_attention": any(
        row["status"] in ("unknown_or_blocked", "stop_evidence_required", "recovery_policy_required", "recovery_ready") for row in rows)}


def cleanup(store, operation_id, task_id, run_id, *, apply=False, stopped=False, metadata_only=False):
    identity(operation_id, "operation ID")
    snapshot = store.snapshot()
    if metadata_only:
        from . import maintenance
        return maintenance.retire_metadata(store, task_id, run_id, apply=apply)
    record = runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {})
    if record.get("version") == 2:
        from . import maintenance
        return maintenance.collect(store, task_id, run_id, apply=apply)
    return legacy_cleanup(store, operation_id, task_id, run_id, apply=apply, stopped=stopped)


def legacy_cleanup(store, operation_id, task_id, run_id, *, apply=False, stopped=False):
    from .maintenance import delete_ref

    identity(operation_id, "operation ID")
    snapshot = store.snapshot()
    task, _, run = runtime.load_run(snapshot, task_id, run_id)
    if any(refs_overlap(ref, store.ref) for ref in (run["heartbeat_ref"], run["work_ref"])):
        raise TaskError("Run cleanup cannot modify the execution authority namespace")
    if run["status"] == "running":
        raise TaskError("Cannot clean an active run")
    if run["status"] == "lost":
        raise TaskError("Recovered runs retain their work and checkpoint for inspection")
    if runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {}).get("status") == "retained":
        raise TaskError("This run's unadopted output must be inspected before cleanup")
    state_path = runtime.local_state(store, task_id, run_id)
    if not state_path.exists():
        raise TaskError("No local workspace ownership record; perform cleanup on its owning machine")
    with store.lock(state_path.with_suffix(".execution.lock")), store.lock(state_path.with_suffix(".lock")):
        state = json.loads(state_path.read_text())
        work = Path(state["path"])
        if state["mode"] == "workspace":
            raise TaskError("Provisioned workspaces belong to the executor and are never removed")
        if store.repo == work.resolve() or store.repo.is_relative_to(work.resolve()):
            raise TaskError("Invoke cleanup from a surviving checkout outside the worktree being removed")
        if state["generation"] != run["generation"] or state["repository_id"] != run["repository_id"]:
            raise TaskError("Workspace ownership does not match the finished run")
        pending = state_path.with_suffix(".cleanup.json")
        journal = json.loads(pending.read_text()) if pending.exists() else {"phase": "checked", "run_id": run_id}
        if work.exists():
            if work.is_symlink() or git(work, "rev-parse", "--show-toplevel") != str(work.resolve()):
                raise TaskError("Workspace identity changed")
            if git(work, "symbolic-ref", "HEAD") != run["work_ref"]:
                raise TaskError("Workspace branch changed")
            if git(work, "rev-parse", "HEAD") != run["source_head"]:
                raise TaskError("Workspace contains unsaved commits")
            if (git(work, "status", "--porcelain", "--untracked-files=all") or
                    git(work, "ls-files", "--others", "--ignored", "--exclude-standard")):
                raise TaskError("Workspace contains unpreserved files")
            # Verify the immutable run result's published blobs at the result
            # operation, not mutable task data in the latest main tree.
            source = Snapshot(store, run["source_head"])
            for path, entry in run["artifact_manifest"].items():
                if source.entries.get(path) != (tuple(entry) if entry else None):
                    raise TaskError("Saved source does not match the result manifest")
        elif journal.get("phase") not in ("removing", "removed", "done"):
            raise TaskError("Workspace disappeared without a cleanup journal")
        plan = {"run_id": run_id, "workspace": str(work), "state_ref": run["heartbeat_ref"],
                "work_ref": run["work_ref"], "preserve_tag": None}
        if not apply:
            return dict(plan, dry_run=True)
        execution = state_path.with_suffix(".execution.json")
        if not execution.exists() and not stopped:
            raise TaskError("Confirm manually launched processes and their children stopped before cleanup")
        if execution.exists() and json.loads(execution.read_text())["phase"] not in ("exited", "interrupted"):
            raise TaskError("Worker termination is not confirmed; retain workspace")
        # Accepted artifacts/results already live on the authority. Do not
        # preserve disposable source ancestry under a per-run tag.
        from . import task_io
        if 'io_manifest' not in runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {}):
            raise TaskError('Run I/O records are not saved on main; retain the stream')
        task_io.IOStore(store, task_id, run).optional()
        beat = store.fetch(run["heartbeat_ref"], missing=True)
        state_checkpoint = Snapshot(store, beat).json("state.json") if beat else None
        request = {"kind": "run-cleanup", "task_id": task_id, "run_id": run_id}
        if run["execution"]["recovery_mode"] in leases.MODES:
            request["generation"] = run["generation"]
        def record(snapshot):
            current = runtime.load_run(snapshot, task_id, run_id)[2]
            if current["status"] == "running" or current["generation"] != run["generation"]:
                raise TaskError("Execution authority changed")
            receipt = {"status": "remote_preserved", "run_id": run_id, "source_tag": None,
                       "source_head": run["source_head"], "heartbeat_head": beat,
                       "heartbeat_checkpoint": state_checkpoint,
                       "machine_id": run["machine_id"], "updated_at": now()}
            previous = runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {})
            if 'io_manifest' in previous:
                receipt['io_manifest'] = previous['io_manifest']
            return {runtime.run_path(task_id, run_id, snapshot) + "/cleanup.json": encode(receipt)}, receipt
        preserved = store.transact(operation_id, request, record)
        if "heartbeat_head" not in preserved:
            raise TaskError("Preservation receipt lacks a heartbeat SHA; retain refs for inspection")
        journal.update(phase="removing", operation_id=operation_id)
        atomic_json(pending, journal)
        if work.exists():
            git(store.repo, "worktree", "remove", str(work))
        journal["phase"] = "removed"
        atomic_json(pending, journal)
        for ref, expected in ((run["work_ref"], preserved["source_head"]),
                              (run["heartbeat_ref"], preserved["heartbeat_head"])):
            delete_ref(store, ref, expected)
        # Only this terminal run's immutable renewal attempts are disposable.
        if run["execution"]["recovery_mode"] == leases.GENERATION:
            prefix = leases.renewal_prefix(store, task_id, run_id)
            for ref, expected in store.list_refs([prefix]).items():
                delete_ref(store, ref, expected)
        def complete(snapshot):
            current = runtime.load_run(snapshot, task_id, run_id)[2]
            receipt = runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json')
            if (current["status"] not in ("succeeded", "failed", "cancelled") or
                    current["generation"] != run["generation"] or not receipt or
                    receipt.get("run_id") != run_id or
                    receipt.get("source_head") != preserved["source_head"] or
                    receipt.get("source_tag") != preserved["source_tag"]):
                raise TaskError("Cleanup preservation identity changed")
            receipt.update(status="done", finished_at=now())
            return {runtime.run_path(task_id, run_id, snapshot) + "/cleanup.json": encode(receipt)}, receipt
        finished = store.transact(digest([operation_id, "done"])[:48],
                                  dict(request, kind="run-cleanup-complete"), complete)
        journal["phase"] = "done"
        atomic_json(pending, journal)
        from . import maintenance
        maintenance.retire_metadata(store, task_id, run_id, apply=True)
        return finished
