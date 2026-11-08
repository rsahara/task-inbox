"""Collect terminal execution resources and retire their disposable metadata."""
import json
from pathlib import Path

from . import leases, runtime
from .store import CONTROL, GitFailure, Snapshot, TaskError, git, identity, local_lock, refs_overlap


def finish_record(run, operation_id, preparation, discard):
    record = {"status": "retained" if discard else "eligible", "version": 2,
              "task_id": run["task_id"], "run_id": run["run_id"],
              "generation": run["generation"], "repository_id": run["repository_id"],
              "machine_id": run["machine_id"], "source_head": run["source_head"],
              "finish_operation": operation_id}
    if discard:
        record["reason"] = "Output was not adopted"
    elif run.get("cleanup_protocol") != 2 or "cleanup" not in preparation:
        # An operation prepared by an older binary keeps its preservation contract.
        return {"status": "pending", "run_id": run["run_id"], "machine_id": run["machine_id"],
                "source_head": run["source_head"], "has_checkpoint": preparation["has_checkpoint"]}
    else:
        record.update(preparation["cleanup"])
        record["work_head"] = run["source_head"]
    return record


def terminal(store, snapshot, task_id, run_id):
    _, _, run = runtime.load_run(snapshot, task_id, run_id)
    from . import completion
    completion.require_terminal(run, run["status"])
    record = runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {})
    if run["status"] not in ("succeeded", "failed", "cancelled") or record.get("status") != "eligible":
        raise TaskError("Run output is not eligible for automatic cleanup")
    if record.get("version") != 2 or run.get("cleanup_protocol") != 2:
        raise TaskError("Legacy run requires its original preservation procedure")
    for name in ("task_id", "run_id", "generation", "repository_id", "source_head"):
        if record.get(name) != run.get(name):
            raise TaskError("Cleanup identity differs from the finished run")
    receipt = snapshot.metadata(CONTROL + "/operations/" + identity(record["finish_operation"]) + ".json")
    saved = receipt.get("result", {}) if receipt else {}
    for name in ("task_id", "run_id", "generation", "repository_id", "source_head", "status", "artifact_manifest", "completion_contract", "completion"):
        if saved.get(name) != run.get(name):
            raise TaskError("Finished run does not match its durable operation receipt")
    prefix = "refs/heads/" + store.ref_prefix + "/" + identity(task_id) + "/" + identity(run_id)
    from . import task_io
    heartbeat_ref = 'refs/heads/' + task_io.prefix(snapshot.json(CONTROL + '/config.json')) + '/' + task_id + '/runs/' + run_id
    if run.get('io_ref') != heartbeat_ref:
        raise TaskError('Cleanup I/O ref differs from its pinned run stream')
    if (run["work_ref"] != prefix + "/work" or run["heartbeat_ref"] != heartbeat_ref or
            any(refs_overlap(ref, store.ref) for ref in (run["work_ref"], run["heartbeat_ref"]))):
        raise TaskError("Cleanup ref is outside the original run namespace")
    if record.get("work_head") != run["source_head"]:
        raise TaskError("Cleanup source differs from the accepted source")
    return run, record


def delete_ref(store, ref, expected):
    actual = store.remote_tip(ref)
    if actual is None:
        return
    if actual != expected:
        raise TaskError("Run ref changed after finish; retained")
    try:
        store.push(ref, None, expected)
    except Exception:
        if store.remote_tip(ref) is not None:
            raise TaskError("Ref deletion is unconfirmed; retry cleanup") from None


def local_files(state_path):
    return [state_path.with_suffix(suffix) for suffix in
            (".json", ".observation.json", ".execution.json",
             ".execution.stdout.log", ".execution.stderr.log", ".cleanup.json",
             ".lock", ".observation.lock", ".execution.lock")]


def ref_match(value, pattern):
    before, wildcard, after = pattern.partition("*")
    if not wildcard:
        return "" if value == pattern else None
    if "*" in after or not value.startswith(before) or not value.endswith(after):
        return None
    if len(value) < len(before) + len(after):
        return None
    return value[len(before):len(value) - len(after) if after else None]


def tracking_refs(store, prefix):
    """Map this remote's local tracking cache back to original run refs."""
    if store.transport.name != "direct_git":
        return {}
    try:
        specs = git(store.repo, "config", "--get-all", "remote." + store.remote + ".fetch").splitlines()
    except GitFailure:
        return {}
    negatives = [spec[1:] for spec in specs if spec.startswith("^")]
    mappings = [spec.lstrip("+").split(":", 1) for spec in specs
                if not spec.startswith("^") and ":" in spec]
    result = {}
    for line in git(store.repo, "for-each-ref", "--format=%(refname) %(objectname) %(symref)",
                    "refs/remotes/").splitlines():
        parts = line.split()
        if len(parts) != 2:  # Never follow symbolic refs into another namespace.
            continue
        local, head = parts
        sources = set()
        for source, destination in mappings:
            if source.count("*") != destination.count("*") or source.count("*") > 1:
                continue
            middle = ref_match(local, destination)
            if middle is not None:
                original = source.replace("*", middle)
                if not any(ref_match(original, excluded) is not None for excluded in negatives):
                    sources.add(original)
        if len(sources) == 1:
            original = sources.pop()
            if original.startswith(prefix):
                result[local] = (original, head)
    return result


def delete_tracking(store, ref, expected):
    if not git(store.repo, "show-ref", "--verify", "--quiet", ref, predicate=True):
        return
    if git(store.repo, "rev-parse", ref) != expected:
        raise TaskError("Local tracking ref changed; retained")
    try:
        git(store.repo, "update-ref", "--no-deref", "-d", ref, expected)
    except GitFailure:
        if git(store.repo, "show-ref", "--verify", "--quiet", ref, predicate=True):
            raise


def collect_local(store, run):
    state_path = runtime.local_state(store, run["task_id"], run["run_id"])
    # Exclusive gate also excludes waiters on the individual lock files.
    # Never unlink a lock inode while a worker may still acquire that inode.
    with local_lock(store.local / ".locks.lock", timeout=0):
        if not state_path.exists():
            files = local_files(state_path)[1:]
            observation = state_path.with_suffix(".observation.json")
            disposable = [path for path in files if path == observation or path.suffix == ".lock"]
            # An observer or a finish replay need not own a workspace. Other
            # unowned journals/logs may represent unknown work and stay intact.
            if any(path.exists() for path in files if path not in disposable):
                return "unowned_state"
            if observation.exists() and json.loads(observation.read_text()).get("generation") != run["generation"]:
                raise TaskError("Local observation belongs to another generation")
            present = [path for path in disposable if path.exists()]
            for path in present:
                path.unlink()
            return "removed" if present else "absent"
        state = json.loads(state_path.read_text())
        for name in ("task_id", "run_id", "generation", "repository_id"):
            if state.get(name) != run.get(name):
                raise TaskError("Local workspace ownership changed")
        marker = state_path.with_suffix(".execution.json")
        if marker.exists() and json.loads(marker.read_text()).get("phase") not in ("exited", "interrupted"):
            raise TaskError("Worker termination is unconfirmed; retain local files")
        if state['mode'] != 'worktree':
            remove_local_records(state_path)
            return 'executor_owned'
        work = Path(state["path"])
        if (work.is_symlink() or store.repo == work.resolve() or
                store.repo.is_relative_to(work.resolve()) or
                Path.cwd().resolve().is_relative_to(work.resolve())):
            return "in_use"
        registrations = git(store.repo, "worktree", "list", "--porcelain")
        registered = any(line == "worktree " + str(work.resolve()) for line in registrations.splitlines())
        if work.exists():
            if not registered or git(work, "rev-parse", "--show-toplevel") != str(work.resolve()):
                raise TaskError("Workspace identity changed")
            common = (work / git(work, "rev-parse", "--git-common-dir")).resolve()
            if common != store.common or git(work, "symbolic-ref", "HEAD") != run["work_ref"]:
                raise TaskError("Workspace repository or branch changed")
            if git(work, "rev-parse", "HEAD") != run["source_head"]:
                raise TaskError("Workspace contains unpreserved commits")
            if (git(work, "status", "--porcelain", "--untracked-files=all") or
                    git(work, "ls-files", "--others", "--ignored", "--exclude-standard")):
                raise TaskError("Workspace contains unpreserved files")
            source = Snapshot(store, run["source_head"])
            for path, entry in run["artifact_manifest"].items():
                if source.entries.get(path) != (tuple(entry) if entry else None):
                    raise TaskError("Workspace does not match the accepted output")
            git(store.repo, "worktree", "remove", str(work))
        elif registered:
            # A missing registered directory may be a disconnected mount.
            raise TaskError("Workspace is missing but still registered; retained")
        if git(store.repo, "show-ref", "--verify", "--quiet", run["work_ref"], predicate=True):
            if git(store.repo, "rev-parse", run["work_ref"]) != run["source_head"]:
                raise TaskError("Local work branch changed; retained")
            if "branch " + run["work_ref"] in git(store.repo, "worktree", "list", "--porcelain").splitlines():
                raise TaskError("Local work branch is still checked out")
            git(store.repo, "update-ref", "-d", run["work_ref"], run["source_head"])
        remove_local_records(state_path)
        return "removed"


def remove_local_records(state_path):
    # State is removed last so interrupted resource collection stays resumable.
    for path in local_files(state_path)[1:]:
        if path.exists():
            path.unlink()
    state_path.unlink()


def collect(store, task_id, run_id, *, apply=False):
    snapshot = store.snapshot()
    run, record = terminal(store, snapshot, task_id, run_id)
    targets = {run["work_ref"]: record["work_head"],
               run["heartbeat_ref"]: record["heartbeat_head"]}
    tracking = tracking_refs(store, run["work_ref"].rsplit("/", 1)[0] + "/")
    tracking.update(tracking_refs(store, run["heartbeat_ref"]))
    if run["execution"]["recovery_mode"] == leases.GENERATION:
        renewal_prefix = leases.renewal_prefix(store, task_id, run_id)
        targets.update(store.list_refs([renewal_prefix]))
        for original, head in tracking.values():
            if original.startswith(renewal_prefix):
                targets.setdefault(original, head)
    tracking = {ref: (original, head) for ref, (original, head) in tracking.items() if original in targets}
    if any(head != targets[original] and
           (not targets[original] or not store.contains(targets[original], head))
           for original, head in tracking.values()):
        raise TaskError("Local tracking ref changed from the finished source; retained")
    # Reject changed targets before deleting any of this run's remote refs.
    for ref, expected in targets.items():
        actual = store.remote_tip(ref)
        if actual is not None and actual != expected:
            raise TaskError("Run ref changed after finish; retained")
    result = {"run_id": run_id, "refs": list(targets), "tracking_refs": list(tracking), "preserve_tag": None}
    if record['heartbeat_head']:
        from . import task_io
        if 'io_manifest' not in record:
            raise TaskError('Run I/O records are not saved on main; retain the stream')
        task_io.IOStore(store, task_id, run).optional()  # Verify main before deleting refs.
    if not apply:
        return dict(result, dry_run=True)
    for ref, expected in targets.items():
        delete_ref(store, ref, expected)
    for ref, (_, observed) in tracking.items():
        delete_tracking(store, ref, observed)
    had_workspace = runtime.local_state(store, task_id, run_id).exists()
    local = collect_local(store, run)
    result.update(status="done" if local in ("removed", "absent") else "local_retained", local=local,
                  local_workspace_collected=had_workspace and local == 'removed')
    # This evidence belongs to local collection, including explicit cleanup.
    # A later authority-metadata failure must not erase the confirmed fact.
    from . import reports
    reports.note_cleanup(store, snapshot, task_id, run_id, result, {})
    retired = retire_metadata(store, task_id, run_id, apply=True)
    return dict(result, metadata=retired['status'])


def retire_metadata(store, task_id, run_id, *, apply=False):
    """Delete execution bookkeeping, preserving task-owned output and Git history."""
    with store.lock(store.local / '.metadata-retirement.lock'):
        return _retire_metadata(store, task_id, run_id, apply=apply)


def _retire_metadata(store, task_id, run_id, *, apply):
    from .transport import TransportError
    for _ in range(8):
        snapshot = store.snapshot()
        _, registry, run = runtime.load_run(snapshot, task_id, run_id)
        from . import completion
        completion.require_terminal(run, run['status'])
        if run['status'] not in ('succeeded', 'failed', 'cancelled') or any(
                slot.get('run_id') == run_id for slot in registry['slots'].values()):
            raise TaskError('Only a confirmed terminal run can retire its execution metadata')
        prefix = runtime.run_path(task_id, run_id, snapshot)
        cleanup = runtime.run_metadata(snapshot, task_id, run_id, 'cleanup.json', {})
        if cleanup.get('status') == 'retained':
            raise TaskError('Unadopted output must be reconciled before retiring metadata')
        owned = {'task_id': task_id, 'run_id': run_id}
        paths = []
        for path in snapshot.entries:
            if path in (prefix + '/run.json', prefix + '/result.json', prefix + '/cleanup.json') or path.startswith(prefix + '/io/'):
                paths.append(path)
            elif path.startswith(CONTROL + '/operations/') and path.endswith('.json'):
                receipt = snapshot.json(path)
                scope = receipt.get('scope')
                result = receipt.get('result', {})
                if scope == owned or (not scope and isinstance(result, dict) and all(result.get(k) == v for k, v in owned.items())):
                    paths.append(path)
        plan = dict(run_id=run_id, status='absent' if not paths else 'eligible', paths=sorted(paths))
        if not apply:
            return dict(plan, dry_run=True)
        if not paths:
            return plan
        # The first transition fences old binaries in the same CAS as deletion.
        changes = runtime.metadata_policy_changes(store, snapshot)
        changes.update({path: None for path in paths})
        from . import leases
        request = dict(kind='run-cleanup', task_id=task_id, run_id=run_id,
                       generation=run['generation'], metadata_only=True)
        guard = leases.guard(store, snapshot, request)
        candidate = store.commit(snapshot.commit, changes, 'Retire execution metadata ' + task_id + '/' + run_id)
        pin = 'refs/task-runtime-cleanup/' + store.local.name
        git(store.repo, 'update-ref', pin, candidate)
        try:
            try:
                if guard and not guard.get('client_time_only'):
                    store.push(store.ref, candidate, snapshot.commit, guard=guard)
                else:
                    store.push(store.ref, candidate, snapshot.commit)
            except (GitFailure, TransportError) as error:
                if isinstance(error, TransportError) and error.code == 'lease_guard_rejected':
                    raise
                if error.definite_rejection:
                    continue
            confirmed = store.snapshot()
            if all(path not in confirmed.entries for path in paths):
                return dict(plan, status='retired')
            if store.contains(confirmed.commit, candidate):
                raise TaskError('Execution metadata changed after retirement; retain it for inspection')
            if confirmed.commit == snapshot.commit:
                raise TaskError('Metadata retirement is unconfirmed; retry cleanup')
        finally:
            git(store.repo, 'update-ref', '-d', pin, candidate)
    raise TaskError('Concurrent updates exceeded metadata cleanup retry limit')


def prune_confirmed_journals(store, snapshot, limit=64):
    # Remote receipts remain authoritative for idempotency after local GC.
    # Unknown outcomes and publications without their receipt remain untouched.
    with local_lock(store.local / ".locks.lock", timeout=0):
        count = 0
        for directory in ("operations", "publications"):
            for path in sorted((store.local / directory).glob("*.json")):
                if count >= limit:
                    return
                receipt = snapshot.metadata(CONTROL + "/operations/" + identity(path.stem) + ".json")
                if not receipt:
                    continue
                state = json.loads(path.read_text())
                if state.get("request_hash") != receipt.get("request_hash"):
                    continue
                pending = "refs/task-runtime-pending/" + store.local.name + "/" + path.stem
                if directory == "operations" and git(store.repo, "show-ref", "--verify", "--quiet", pending, predicate=True):
                    candidate = state.get("pending", {}).get("candidate")
                    if not candidate or git(store.repo, "rev-parse", pending) != candidate:
                        continue
                    git(store.repo, "update-ref", "-d", pending, candidate)
                path.unlink()
                lock = path.with_suffix(".lock")
                if lock.exists():
                    lock.unlink()
                count += 1


def maintain(store, task_id, run_id=None, limit=8):
    """Best effort on normal start/finish; never turn a committed result into a failure."""
    rows = []
    completed = 0
    try:
        snapshot = store.snapshot()
        from . import reports, task_io
        task_prefix = "refs/heads/" + store.ref_prefix + "/" + identity(task_id) + "/"
        io_prefix = 'refs/heads/' + task_io.prefix(snapshot.json(CONTROL + '/config.json')) + '/' + task_id + '/runs/'
        remote = store.list_refs([task_prefix, io_prefix])
        tracking = tracking_refs(store, task_prefix)
        tracking.update(tracking_refs(store, io_prefix))
        prefix = runtime.root(task_id, snapshot) + "/runs/"
        candidates = {path[len(prefix):].split('/')[0] for path in snapshot.entries
                      if path.startswith(prefix) and path.endswith('/cleanup.json') and
                      len(path[len(prefix):].split('/')) == 2}
        candidates.update(path.name.split('.')[0] for path in (store.local / 'runs' / task_id).glob('*') if path.is_file())
        for ref in list(remote) + [original for original, _ in tracking.values()]:
            for namespace in (task_prefix, io_prefix):
                if ref.startswith(namespace):
                    candidates.add(ref[len(namespace):].split('/')[0])
        if run_id is not None:
            candidates.add(run_id)
        for current_id in sorted(candidates):
            if run_id is not None and current_id != run_id:
                continue
            try:
                identity(current_id, 'run ID')
            except TaskError:
                continue
            record = runtime.run_metadata(snapshot, task_id, current_id, 'cleanup.json', {})
            run_prefix = "refs/heads/" + store.ref_prefix + "/" + task_id + "/" + current_id + "/"
            # Changed refs cannot be collected. Skipping them must not starve
            # eligible runs later in the listing.
            if any(remote.get(run_prefix + suffix) not in (None, record.get(field))
                   for suffix, field in (("work", "work_head"), ("state", "heartbeat_head"))):
                if record.get('status') == 'eligible':
                    result = {'run_id': current_id, 'status': 'retained', 'reason': 'ref_changed_after_finish'}
                    rows.append(result)
                    reports.note_cleanup(store, snapshot, task_id, current_id, result, remote)
                continue
            try:
                if record.get('version') == 2 and record.get('status') == 'eligible':
                    result = collect(store, task_id, current_id, apply=True)
                elif record.get('status') in ('pending', 'done'):
                    result = retire_metadata(store, task_id, current_id, apply=True)
                else:
                    continue
                rows.append(result)
                reports.note_cleanup(store, snapshot, task_id, current_id, result, remote)
                completed += result['status'] in ('done', 'retired')
            except (TaskError, GitFailure, OSError, ValueError, KeyError) as error:
                result = {'run_id': current_id, 'status': 'retained', 'reason': reports.cleanup_reason(error)}
                rows.append(result)
                reports.note_cleanup(store, snapshot, task_id, current_id, result, remote)
            # Retained workspaces and failures must not permanently hide runs
            # later in the listing. The quota counts completed collections.
            if completed >= limit:
                break
        prune_confirmed_journals(store, snapshot)
    except (TaskError, GitFailure, OSError, ValueError, KeyError):
        rows.append({"status": "retained"})
    return rows
