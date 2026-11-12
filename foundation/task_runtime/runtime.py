"""Task definitions and run authority live in one Git branch (main by default)."""
import json
import os
from pathlib import Path
import socket
import time
import uuid

from .store import (CONTROL, Snapshot, SupersededRenewal, TaskError, atomic_json, digest, encode, git,
                    identity, now, refs_overlap, safe_path)


def root(task_id, source=None):
    identity(task_id, "task ID")
    if isinstance(source, Snapshot):
        from . import layout
        return layout.task_path(task_id, source)
    return (source or {}).get('task_path', 'tasks/' + task_id)


def data_prefix(record, task_id, task_path=None):
    """A run pins its data directory independently of task status."""
    from . import layout
    return layout.path(record.get('data_path', (task_path or root(task_id, record)) + '/data')) + '/'


def load_task(snapshot, task_id):
    path = root(task_id, snapshot)
    task = snapshot.json(path + "/task.json")
    runtime = snapshot.json(path + "/runtime.json")
    if not task or not runtime or task.get("schema_version") != 2:
        raise TaskError("Task not found or unsupported task schema")
    if task.get('id') != task_id or task.get('task_path', path) != path:
        raise TaskError('Task definition differs from its indexed location')
    from . import layout
    layout.check_data(snapshot, task_id, path, data_prefix(task, task_id, path).rstrip('/'))
    return task, runtime


def run_path(task_id, run_id, source=None):
    return root(task_id, source) + "/runs/" + identity(run_id, "run ID")


def run_metadata_snapshot(snapshot, task_id, run_id, name):
    """Locate a run record and its consistent tree across retirement and moves."""
    safe_path(name)
    if '/' in name:
        raise TaskError('Run metadata must be a file name')
    current = run_path(task_id, run_id, snapshot) + '/' + name
    def locate(tree, location):
        prefix = location + '/runs/' + identity(run_id, 'run ID')
        candidate = prefix + '/' + name
        located = tree.metadata_snapshot(candidate)
        if candidate in located.entries:
            if name == 'run.json':
                owner = located.json(candidate)
            else:
                definition = located.json(location + '/task.json')
                owner = run_record(located, task_id, run_id) if definition and definition.get('id') == task_id else None
            if owner and owner.get('task_id') == task_id and owner.get('run_id') == run_id:
                return located, candidate
        return None
    found = locate(snapshot, root(task_id, snapshot))
    if found is not None:
        return found
    from . import layout
    for location, previous in layout.previous_locations(snapshot, task_id):
        found = locate(previous, location)
        if found is not None:
            return found
    return snapshot, current


def run_metadata(snapshot, task_id, run_id, name, default=None):
    located, path = run_metadata_snapshot(snapshot, task_id, run_id, name)
    return located.json(path, default)


def run_record(snapshot, task_id, run_id):
    return run_metadata(snapshot, task_id, run_id, 'run.json')


def load_run(snapshot, task_id, run_id, live=False):
    task, runtime = load_task(snapshot, task_id)
    run = run_record(snapshot, task_id, run_id)
    if not run:
        raise TaskError("Run not found")
    if run['status'] == 'running' and root(task_id, run) != root(task_id, snapshot):
        raise TaskError('Run location changed; reconcile the indexed task before continuing')
    slot = runtime["slots"].get(str(run["slot_id"]), {})
    if live and (run["status"] != "running" or slot.get("run_id") != run_id or
                 slot.get("generation") != run["generation"]):
        raise TaskError("Run no longer owns its execution slot")
    if live and run.get('io_protocol') != 1:
        raise TaskError('Finish or reconcile this run with its original runtime before enabling compact execution metadata')
    return task, runtime, run


def metadata_policy_changes(store, snapshot):
    """Fence older writers only after their admitted runs have finished."""
    config = snapshot.json(CONTROL + '/config.json')
    if config['schema_version'] >= 9:
        return {}
    from . import task_io
    configured = dict(config, io_ref_prefix=task_io.prefix(config))
    from . import layout
    store.require_history()
    changes = {}
    for location in layout.tasks(snapshot).values():
        path = location + '/runtime.json'
        registry = snapshot.json(path)
        if any(slot.get('run_id') for slot in registry['slots'].values()):
            raise TaskError('Finish or reconcile existing runs before enabling compact execution metadata')
        registry.pop('data_revision', None)
        for key, slot in registry['slots'].items():
            registry['slots'][key] = {'generation': slot['generation'], 'run_id': None}
        changes[path] = encode(registry)
    changes[CONTROL + '/config.json'] = encode(dict(configured, schema_version=9, workspace_provenance_protocol=1))
    return changes


def release_slot(registry, run):
    registry['slots'][str(run['slot_id'])] = {'generation': run['generation'], 'run_id': None}


def list_runs(snapshot, task_id):
    """Discover prior executions when a workflow must reconcile an old operation."""
    from . import layout
    prefixes = {location + '/runs/' for location in [root(task_id, snapshot),
                *(location for location, _ in layout.previous_locations(snapshot, task_id))]}
    paths = set(snapshot.entries)
    if snapshot.json(CONTROL + '/config.json', {}).get('schema_version', 0) >= 6:
        snapshot.store.require_history()
        raw = git(snapshot.store.repo, 'log', '-z', '--full-history', '--no-merges',
                         '--format=', '--name-only', '--diff-filter=D', snapshot.commit,
                         '--', *(':(literal)' + prefix for prefix in sorted(prefixes)),
                         binary=True, strip=False)
        paths.update(name.decode() for name in raw.split(b'\0') if name)
    run_ids = {path[len(prefix):].split('/')[0] for path in paths for prefix in prefixes
               if path.startswith(prefix) and len(path[len(prefix):].split('/')) == 2 and path.endswith('/run.json')}
    return [record for run_id in sorted(run_ids) if (record := run_record(snapshot, task_id, run_id)) is not None]


def positive(value, label, maximum=1000000):
    if type(value) is not int or not 1 <= value <= maximum:
        raise TaskError(label + " must be a positive integer within the supported limit")
    return value


def initialize(store, operation_id, *, index=None, task_root=None):
    from . import layout
    options = dict(index=index or "task-index.json", task_root=task_root or "tasks") if index is not None or task_root is not None else None
    if options:
        layout.settings(dict(schema_version=5, layout=options))
    def apply(snapshot):
        existing = snapshot.json(CONTROL + "/config.json")
        if existing:
            store.validate_config(existing)
            if options is not None and existing.get('layout') != options:
                raise TaskError('Repository already initialized with another layout')
            return {}, existing
        config = {"schema_version": 2, "repository_id": str(uuid.uuid4()),
                  "authority_ref": store.ref, "ref_prefix": "task-runtime", "created_at": now()}
        changes = {}
        if options:
            config.update(schema_version=5, layout=options)
            if any(refs_overlap(options['index'], p) for p in snapshot.entries):
                raise TaskError('Task index overlaps existing repository files')
            changes[options['index']] = encode(dict(schema_version=1, tasks={}))
        store.validate_config(config)
        changes[CONTROL + "/config.json"] = encode(config)
        return changes, config
    return store.transact(operation_id, dict(kind="initialize", **({"layout": options} if options else {})), apply, initialize=True)


def add_task(store, operation_id, *, title, task_id=None, body="", max_runners=1,
             recovery_mode="manual", heartbeat_seconds=60, stale_seconds=300, priority=3, path=None, data_path=None):
    task_id = identity(task_id or operation_id, "task ID")
    positive(max_runners, "max_runners", 1024)
    positive(heartbeat_seconds, "heartbeat interval")
    positive(stale_seconds, "stale threshold")
    positive(priority, "priority")
    if stale_seconds <= heartbeat_seconds or recovery_mode not in ("manual", "git_only"):
        raise TaskError("Invalid recovery settings")
    if not isinstance(title, str) or not title.strip():
        raise TaskError("Task title is required")
    request = dict(kind="task-add", task_id=task_id, title=title, body=body, max_runners=max_runners,
                   recovery_mode=recovery_mode, heartbeat_seconds=heartbeat_seconds,
                   stale_seconds=stale_seconds, priority=priority)
    if path is not None or data_path is not None:
        request.update(path=path, data_path=data_path)
    requested_path = path
    def apply(snapshot):
        from . import layout
        path, changes = layout.register(snapshot, task_id, requested_path)
        if any(p == path or p.startswith(path + "/") for p in snapshot.entries):
            raise TaskError("Task ID already exists; IDs cannot be reused")
        task = {"schema_version": 2, "id": task_id, "title": title, "priority": priority,
                "status": "open", "archived_at": None, "revision": 1, "created_at": now(),
                "execution": {"max_runners": max_runners, "recovery_mode": recovery_mode,
                              "heartbeat_seconds": heartbeat_seconds, "stale_seconds": stale_seconds}}
        if layout.settings(snapshot.json(CONTROL + '/config.json')) and path != 'tasks/' + task_id:
            task['task_path'] = path
        if data_path is not None:
            task['data_path'] = data_path
        layout.check_data(snapshot, task_id, path, data_prefix(task, task_id, path).rstrip('/'))
        runtime = {"schema_version": 2, "slots": {}}
        if snapshot.json(CONTROL + '/config.json')['schema_version'] < 9:
            runtime['data_revision'] = 0
        changes.update({path + "/task.json": encode(task), path + "/task.md": body.encode(),
                        path + "/runtime.json": encode(runtime)})
        return changes, task
    return store.transact(operation_id, request, apply)


def list_tasks(store, include_archived=False):
    snapshot = store.snapshot()
    tasks = []
    from . import layout
    for task_id in layout.tasks(snapshot):
        task, registry = load_task(snapshot, task_id)
        if include_archived or (not task["archived_at"] and task["status"] != "deleted"):
            tasks.append(dict(task, active_runs=[s["run_id"] for s in registry["slots"].values() if s.get("run_id")]))
    return {"commit": snapshot.commit, "tasks": sorted(tasks, key=lambda t: (t["priority"], t["id"]))}


def edit_task(store, operation_id, task_id, expected_revision, *, title=None, body=None,
              status=None, archived=None, max_runners=None, priority=None):
    request = dict(kind="task-edit", task_id=task_id, expected_revision=expected_revision,
                   title=title, body=body, status=status, archived=archived,
                   max_runners=max_runners, priority=priority)
    def apply(snapshot):
        task, runtime = load_task(snapshot, task_id)
        if task["revision"] != expected_revision:
            raise TaskError("Task definition changed; read it before editing again")
        if task["status"] == "deleted":
            raise TaskError("Deleted task IDs remain reserved")
        active = [s for s in runtime["slots"].values() if s.get("run_id")]
        if status is not None:
            if status not in ("open", "paused", "completed", "cancelled", "deleted"):
                raise TaskError("Invalid task status")
            if active and status in ("completed", "cancelled", "deleted"):
                raise TaskError("Finish or recover active runs before closing the task")
            task["status"] = status
            if status == "open":
                task["archived_at"] = None
        if title is not None:
            if not title.strip():
                raise TaskError("Task title is required")
            task["title"] = title
        if priority is not None:
            task["priority"] = positive(priority, "priority")
        if max_runners is not None:
            positive(max_runners, "max_runners", 1024)
            if any(int(s) > max_runners and v.get("run_id") for s, v in runtime["slots"].items()):
                raise TaskError("A slot being removed is occupied; finish it first")
            task["execution"]["max_runners"] = max_runners
        if archived is not None:
            if archived and task["status"] not in ("completed", "cancelled", "deleted"):
                raise TaskError("Close the task before archiving it")
            task["archived_at"] = now() if archived else None
        task["revision"] += 1
        task["updated_at"] = now()
        changes = {root(task_id, snapshot) + "/task.json": encode(task)}
        if body is not None:
            changes[root(task_id, snapshot) + "/task.md"] = body.encode()
        return changes, task
    return store.transact(operation_id, request, apply)


def start(store, operation_id, task_id, runner_id, *, run_id=None, machine_id=None, item_key=None,
          resume_from=None, checkpoint_id=None, completion_contract=None):
    from . import leases
    run_id = identity(run_id or operation_id, "run ID")
    identity(runner_id, "runner ID")
    requested_machine_id = machine_id
    machine_id = machine_id or socket.gethostname()
    if not isinstance(machine_id, str) or len(machine_id) > 256 or any(ord(c) < 32 for c in machine_id):
        raise TaskError("Invalid machine ID")
    if item_key is not None and (not isinstance(item_key, str) or not item_key or len(item_key) > 512):
        raise TaskError("Invalid item key")
    request = dict(kind="run-start", task_id=task_id, run_id=run_id, runner_id=runner_id,
                   machine_id=requested_machine_id, item_key=item_key)
    if resume_from is not None or checkpoint_id is not None:
        if not resume_from or not checkpoint_id:
            raise TaskError("Continuation requires the original run and checkpoint IDs")
        request.update(resume_from=resume_from, checkpoint_id=checkpoint_id)
    if completion_contract is not None:
        if resume_from is not None:
            raise TaskError('Checkpoint-only continuation cannot use a business completion contract')
        from . import completion
        request['completion_contract'] = completion.contract(completion_contract)
    from . import maintenance
    maintenance.maintain(store, task_id)
    # A normal scheduled admission can reclaim eligible expired leases from
    # remote state alone. A replay only reads its historical receipt: it must
    # never mutate another generation while reconciling an old start.
    initial = store.snapshot()
    initial_task = initial.json(root(task_id, initial) + "/task.json")
    if (not initial.metadata(CONTROL + "/operations/" + identity(operation_id) + ".json") and
            initial_task and initial_task["execution"]["recovery_mode"] in leases.MODES and
            not (store.local / "operations" / (operation_id + ".json")).exists()):
        from . import lifecycle
        lifecycle.reap(store, task_id, apply=True, observer_profile="stateless")
    def apply(snapshot):
        task, runtime = load_task(snapshot, task_id)
        config = snapshot.json(CONTROL + "/config.json")
        from . import task_io
        if config.get("migration", {}).get("state") == "staged":
            raise TaskError("Migration is staged; activate after stopping and reconciling legacy writers")
        if task["status"] != "open":
            raise TaskError("Task is not open for new runs")
        if run_record(snapshot, task_id, run_id) is not None:
            raise TaskError("Run ID already exists; replay the original operation ID")
        for slot in runtime["slots"].values():
            if slot.get("run_id"):
                other = load_run(snapshot, task_id, slot["run_id"])[2]
                if other["runner_id"] == runner_id:
                    raise TaskError("Runner already has an unfinished run; replay its operation ID")
                if item_key is not None and other.get("item_key") == item_key:
                    raise TaskError("Item already has an active run")
        free = next((str(n) for n in range(1, task["execution"]["max_runners"] + 1)
                     if not runtime["slots"].get(str(n), {}).get("run_id")), None)
        if free is None:
            raise TaskError("Task runner limit reached")
        generation = runtime["slots"].get(free, {}).get("generation", 0) + 1
        changes = metadata_policy_changes(store, snapshot)
        config = json.loads(changes.get(CONTROL + '/config.json', encode(config)))
        prefix = "refs/heads/" + config["ref_prefix"] + "/" + task_id + "/" + run_id
        io_ref = 'refs/heads/' + task_io.prefix(config) + '/' + task_id + '/runs/' + run_id
        run = {"schema_version": 2, "task_id": task_id, "run_id": run_id, "runner_id": runner_id,
               "machine_id": machine_id, "slot_id": int(free), "generation": generation,
               "repository_id": config["repository_id"], "authority_ref": store.ref,
               "definition_commit": snapshot.commit, "definition_revision": task["revision"],
               "source_head": snapshot.commit, "status": "running", "started_at": now(),
               "io_protocol": 1, "io_ref": io_ref, "heartbeat_ref": io_ref, "work_ref": prefix + "/work",
               "execution": task["execution"], "item_key": item_key,
               "task_path": root(task_id, snapshot),
               "data_path": data_prefix(task, task_id, root(task_id, snapshot)).rstrip("/"),
               "data_hash": snapshot.content_hash(data_prefix(task, task_id, root(task_id, snapshot))),
               "publication_count": 0, "artifact_manifest": {}, "cleanup_protocol": 2,
               "workspace_origin_id": None, "workspace_history_unobserved": False}
        effective_contract = completion_contract if completion_contract is not None else task.get('completion_contract')
        if effective_contract is not None:
            if resume_from is not None:
                raise TaskError('Checkpoint-only continuation cannot use a business completion contract')
            from . import completion
            run['completion_contract'] = completion.contract(effective_contract)
        leases.stamp_run(snapshot, run)
        if resume_from:
            if task["execution"]["recovery_mode"] not in leases.MODES:
                raise TaskError("Checkpoint continuation requires a fenced lease policy")
            run["continuation"] = leases.continuation(snapshot, task_id, resume_from, checkpoint_id)
        provenance_fenced = (config['schema_version'] >= 7 and
                             config.get('workspace_provenance_protocol') == 1)
        run['workspace_provenance_protocol'] = 1 if provenance_fenced else 0
        run['workspace_history_unobserved'] = not provenance_fenced
        if root(task_id, snapshot) + '/runtime.json' in changes:
            runtime = json.loads(changes[root(task_id, snapshot) + '/runtime.json'])
        runtime["slots"][free] = {"generation": generation, "run_id": run_id}
        if config['schema_version'] < 9:
            runtime['slots'][free].update(runner_id=runner_id, machine_id=machine_id)
        changes.update({root(task_id, snapshot) + "/runtime.json": encode(runtime),
                        run_path(task_id, run_id, snapshot) + "/run.json": encode(run)})
        return changes, run
    return store.transact(operation_id, request, apply)


def check(store, task_id, run_id):
    from . import leases
    snapshot = store.snapshot()
    run = load_run(snapshot, task_id, run_id, live=True)[2]
    if run["execution"]["recovery_mode"] in leases.MODES:
        leases.guard(store, snapshot, dict(kind="run-check", task_id=task_id, run_id=run_id))
    return run


def local_state(store, task_id, run_id):
    return store.local / "runs" / identity(task_id) / (identity(run_id) + ".json")


def record_workspace_preparation(store, task_id, run_id, run, state, state_path):
    """Record possible workspace history before materializing local files."""
    if state.get('workspace_evidence_recorded'):
        return
    if not state.get('workspace_evidence_id'):
        state['workspace_evidence_id'] = uuid.uuid4().hex
    # The same local intent survives a lost authority response or interrupted
    # worktree creation. Paths and private local state never go to authority.
    atomic_json(state_path, state)
    request = dict(kind='run-prepare-evidence', task_id=task_id, run_id=run_id,
                   generation=run['generation'], workspace_evidence_id=state['workspace_evidence_id'])

    def apply(snapshot):
        current = load_run(snapshot, task_id, run_id, live=True)[2]
        if current['generation'] != run['generation']:
            raise TaskError('Workspace preparation belongs to another run generation')
        origin = current.get('workspace_origin_id')
        current['workspace_history_unobserved'] = (
            'workspace_origin_id' not in current or
            current.get('workspace_history_unobserved') is not False or
            origin not in (None, state['workspace_evidence_id']))
        current['workspace_origin_id'] = origin or state['workspace_evidence_id']
        return {run_path(task_id, run_id, snapshot) + '/run.json': encode(current)}, {
            'task_id': task_id, 'run_id': run_id, 'generation': run['generation'],
            'workspace_evidence_id': state['workspace_evidence_id']}

    store.transact('prepare-evidence-' + digest(request), request, apply)
    state['workspace_evidence_recorded'] = True
    atomic_json(state_path, state)


def prepare(store, task_id, run_id, path, mode="worktree", *, restore=False, stopped=False, settled=False):
    path = Path(path).absolute()
    state_path = local_state(store, task_id, run_id)
    with store.lock(state_path.with_suffix(".lock")):
        run = check(store, task_id, run_id)
        if mode not in ("worktree", "workspace"):
            raise TaskError("Unknown workspace mode")
        if path.is_symlink():
            raise TaskError("Workspace cannot be a symlink")
        old = json.loads(state_path.read_text()) if state_path.exists() else None
        if old and (old["path"] != str(path) or old["mode"] != mode or old["generation"] != run["generation"]):
            raise TaskError("Run is already bound to another workspace")
        if old and old.get("prepared"):
            if not path.exists() or git(path, "rev-parse", "--show-toplevel") != str(path.resolve()):
                raise TaskError("Registered workspace is missing or changed")
            if not git(path, "merge-base", "--is-ancestor", run["source_head"], "HEAD", predicate=True):
                raise TaskError("Workspace does not contain the saved source")
            record_workspace_preparation(store, task_id, run_id, run, old, state_path)
            return old
        start_head = run["definition_commit"]
        if restore:
            if not stopped or not settled:
                raise TaskError("Restore requires confirmation that the previous writer/children stopped and operations settled")
            start_head = store.fetch(run["work_ref"], missing=True)
            if not start_head or not store.contains(start_head, run["source_head"]):
                raise TaskError("No compatible remote work checkpoint to restore")
            if git(store.repo, "rev-list", "--merges", run["definition_commit"] + ".." + start_head):
                raise TaskError("Checkpoint contains unsupported merge history")
            for commit in git(store.repo, "rev-list", run["definition_commit"] + ".." + start_head).splitlines():
                paths = names(store.repo, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", commit)
                if any(not allowed(task_id, run_id, p, run) for p in paths):
                    raise TaskError("Checkpoint crosses the run boundary")
        elif not old and (run["publication_count"] or store.remote_tip(run["work_ref"])):
            raise TaskError("A remote checkpoint exists; use restore after confirming the old writer stopped")
        state = old or {"path": str(path), "mode": mode, "task_id": task_id, "run_id": run_id,
                        "generation": run["generation"], "repository_id": run["repository_id"],
                        "definition_commit": run["definition_commit"], "prepared": False}
        if old and old.get("start_head", start_head) != start_head:
            raise TaskError("Interrupted preparation must retain its original checkpoint")
        state["start_head"] = start_head
        if mode == "workspace":
            if (git(path, "rev-parse", "HEAD") != start_head or
                    git(path, "status", "--porcelain", "--untracked-files=all")):
                raise TaskError("Provisioned workspace must be clean at the pinned definition commit")
            other_common = (path / git(path, "rev-parse", "--git-common-dir")).resolve()
            if other_common != store.common:
                raise TaskError("Use --repo pointing at the provisioned workspace")
            reservation = store.local / "workspace-reservation.json"
            with store.lock(reservation.with_suffix(".lock")):
                if reservation.exists():
                    owner = json.loads(reservation.read_text())
                    if owner["run_id"] != run_id or owner["task_id"] != task_id:
                        previous = load_run(store.snapshot(), owner["task_id"], owner["run_id"])[2]
                        confirmed_recovery = (previous["status"] == "lost" and
                                              previous.get("recovery_kind") == "confirmed_stopped" and
                                              previous.get("recovery_evidence"))
                        if previous["status"] not in ("succeeded", "failed", "cancelled") and not confirmed_recovery:
                            raise TaskError("Provisioned workspace is reserved by another unfinished run")
                record_workspace_preparation(store, task_id, run_id, run, state, state_path)
                atomic_json(reservation, state)
            atomic_json(state_path, state)
        else:
            branch = run["work_ref"].removeprefix("refs/heads/")
            if path.exists() and not old:
                raise TaskError("Worktree destination already exists")
            record_workspace_preparation(store, task_id, run_id, run, state, state_path)
            atomic_json(state_path, state)
            if not path.exists():
                exists = git(store.repo, "show-ref", "--verify", "--quiet", run["work_ref"], predicate=True)
                if exists:
                    if not old or git(store.repo, "rev-parse", run["work_ref"]) != start_head:
                        raise TaskError("Existing work branch needs inspection")
                    git(store.repo, "worktree", "add", str(path), branch)
                else:
                    git(store.repo, "worktree", "add", "-b", branch, str(path), start_head)
            elif (git(path, "symbolic-ref", "HEAD") != run["work_ref"] or
                  git(path, "rev-parse", "HEAD") != start_head):
                raise TaskError("Interrupted worktree setup needs inspection")
        state["prepared"] = True
        atomic_json(state_path, state)
        return state


def allowed(task_id, run_id, path, run=None):
    return (path.startswith(data_prefix(run or {}, task_id)) or
            path.startswith(run_path(task_id, run_id, run) + "/artifacts/"))


def names(repo, *args):
    return [p.decode() for p in git(repo, *args, binary=True).split(b"\0") if p]


def source_snapshot(store, task_id, run_id, state, run):
    work = Path(state["path"])
    if not state.get("prepared") or state["generation"] != run["generation"]:
        raise TaskError("Workspace is not prepared for this run")
    if not git(work, "merge-base", "--is-ancestor", run["source_head"], "HEAD", predicate=True):
        raise TaskError("Workspace source ancestry changed")
    commits = git(work, "rev-list", run["definition_commit"] + "..HEAD").splitlines()
    if git(work, "rev-list", "--merges", run["definition_commit"] + "..HEAD"):
        raise TaskError("Merge commits are not allowed in run work history")
    paths = set()
    for commit in commits:
        paths.update(names(work, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", commit))
    paths.update(names(work, "diff", "--name-only", "--no-renames", "-z"))
    paths.update(names(work, "diff", "--cached", "--name-only", "--no-renames", "-z"))
    paths.update(names(work, "ls-files", "--others", "--exclude-standard", "-z"))
    if any(not allowed(task_id, run_id, p, run) for p in paths):
        raise TaskError("Work history or pending files cross the run's data/artifact boundary")
    for path in paths:
        safe_path(path)
    # Refuse symlink/submodule entries before staging or remote publication.
    for path in paths:
        target = work / path
        if target.is_symlink() or any(p.is_symlink() for p in target.parents if p != work and work in p.parents):
            raise TaskError("Run outputs cannot contain symlinks")
    if paths:
        scope = [root(task_id, run)]
        if data_prefix(run, task_id) != root(task_id, run) + "/data/":
            scope.append(data_prefix(run, task_id).rstrip("/"))
        git(work, "--literal-pathspecs", "add", "--all", "--", *scope)
    if not git(work, "diff", "--cached", "--quiet", predicate=True):
        git(work, "commit", "-m", "Checkpoint task run " + run_id)
    head = git(work, "rev-parse", "HEAD")
    source = Snapshot(store, head)
    touched = set(names(work, "diff", "--name-only", "--no-renames", "-z", run["source_head"], head))
    for commit in git(work, "rev-list", run["source_head"] + ".." + head).splitlines():
        touched.update(names(work, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", commit))
    if any(not allowed(task_id, run_id, p, run) for p in touched):
        raise TaskError("Committed output crosses the run boundary; checkpoint was not pushed")
    if any(source.entries.get(p, ("100644",))[0] not in ("100644", "100755") for p in touched):
        raise TaskError("Run outputs must be regular files")
    return source, touched


def checkpoint_ref(store, ref, source):
    expected = store.fetch(ref, missing=True)
    if expected == source:
        return expected
    try:
        store.push(ref, source, expected)
    except Exception:
        head = store.fetch(ref, missing=True)
        if not head or not store.contains(head, source):
            raise TaskError("Checkpoint outcome is unknown; preserve the workspace") from None
    return expected


def publish(store, operation_id, task_id, run_id, *, outcome=None, summary="", discard_output=False, completion_record=None):
    result = _publish(store, operation_id, task_id, run_id, outcome=outcome,
                      summary=summary, discard_output=discard_output, completion_record=completion_record)
    from . import outbox
    delivery = outbox.flush(store, task_id=task_id, publication_id=operation_id)
    if outcome:
        from . import maintenance
        maintenance.maintain(store, task_id, run_id)
    return dict(result, notifications=delivery)


def _publish(store, operation_id, task_id, run_id, *, outcome=None, summary="", discard_output=False, completion_record=None):
    if outcome not in (None, "succeeded", "failed", "cancelled"):
        raise TaskError("Invalid run outcome")
    if discard_output and outcome not in ("failed", "cancelled"):
        raise TaskError("Only failed or cancelled runs can finish without adopting their output")
    state_path = local_state(store, task_id, run_id)
    request = dict(kind="run-finish" if outcome else "run-publish", task_id=task_id,
                   run_id=run_id, outcome=outcome, summary=summary, discard_output=discard_output)
    if completion_record is not None:
        if not outcome:
            raise TaskError('Completion evidence can be supplied only with finish')
        request['completion'] = completion_record
    # Replays must reconcile the original candidate even after termination or
    # workspace removal; never regenerate the requested operation's source.
    operation_path = store.local / "publications" / (identity(operation_id) + ".json")
    with store.lock(state_path.with_suffix(".lock")):
        receipt = store.snapshot().metadata(CONTROL + "/operations/" + operation_id + ".json")
        if receipt:
            if receipt["request_hash"] != digest(request):
                raise TaskError("Operation ID was already used for another request")
            return dict(receipt["result"], operation_id=operation_id, committed=True)
        if operation_path.exists():
            preparation = json.loads(operation_path.read_text())
            if preparation["request_hash"] != digest(request):
                raise TaskError("Operation ID was already used for another publication")
            if preparation["has_checkpoint"]:
                store.fetch(preparation["work_ref"])
        else:
            run = check(store, task_id, run_id)
            previous_work_ref = None
            workspace_evidence_id = None
            if outcome:
                from . import completion
                if completion_record is not None:
                    completion.update(run, completion_record)
                completion.require_terminal(run, outcome)
            if state_path.exists() and not discard_output:
                state = json.loads(state_path.read_text())
                workspace_evidence_id = state.get('workspace_evidence_id')
                source, touched = source_snapshot(store, task_id, run_id, state, run)
                previous_work_ref = checkpoint_ref(store, run["work_ref"], source.commit)
            else:
                if not discard_output:
                    raise TaskError("Prepare or restore the run workspace before publishing or finishing")
                if run["publication_count"]:
                    store.fetch(run["work_ref"])
                source, touched = Snapshot(store, run["source_head"]), set()
            preparation = {"request_hash": digest(request), "source_head": source.commit,
                           "previous_source": run["source_head"], "generation": run["generation"],
                           "touched": sorted(touched), "work_ref": run["work_ref"],
                           "has_checkpoint": state_path.exists() and not discard_output,
                           # An exposed checkpoint can leave workspaces in
                           # other clones; this clone cannot confirm their GC.
                           "workspace_history_unobserved": (outcome is None or discard_output or
                               previous_work_ref is not None or run['publication_count'] != 0 or
                               not workspace_evidence_id or run.get('workspace_origin_id') != workspace_evidence_id)}
            if outcome and not discard_output:
                beat = store.fetch(run["heartbeat_ref"], missing=True)
                preparation["cleanup"] = {
                    "heartbeat_head": beat,
                    "heartbeat_checkpoint": Snapshot(store, beat).json("state.json") if beat else None}
                from . import task_io
                preparation['cleanup']['io_manifest'] = task_io.finish_manifest(store, run, beat)
            atomic_json(operation_path, preparation)
        source = Snapshot(store, preparation["source_head"])
        baseline = Snapshot(store, preparation["previous_source"])
        def apply(snapshot):
            task, runtime, run = load_run(snapshot, task_id, run_id, live=True)
            if run["generation"] != preparation["generation"] or run["source_head"] != preparation["previous_source"]:
                raise TaskError("Another publication changed this run; do not replace the original candidate")
            if not discard_output and snapshot.content_hash(data_prefix(run, task_id)) != run["data_hash"]:
                raise TaskError("Shared task data changed; recompute in a new run")
            changes = {}
            for path in preparation["touched"]:
                if not allowed(task_id, run_id, path, run):
                    raise TaskError("Publication crosses the run boundary")
                expected, current, new = baseline.entries.get(path), snapshot.entries.get(path), source.entries.get(path)
                if current != expected:
                    raise TaskError("Output path changed since the source snapshot")
                changes[path] = new
                run["artifact_manifest"][path] = new
            for path, entry in run["artifact_manifest"].items():
                if path not in changes and snapshot.entries.get(path) != (tuple(entry) if entry else None):
                    # Shared data is covered by its revision; immutable run
                    # outputs must not drift between publications.
                    if not discard_output and path.startswith(run_path(task_id, run_id, run) + "/artifacts/"):
                        raise TaskError("Previously saved run artifact changed")
            if 'data_revision' in runtime and any(p.startswith(data_prefix(run, task_id)) for p in changes):
                runtime["data_revision"] += 1
            if not discard_output:
                from . import outbox, signals
                queued = outbox.capture(snapshot, run, source, preparation["touched"], operation_id)
                outbox.merge(queued, signals.publication(snapshot, run, source, preparation["touched"], operation_id))
                changes.update(queued)
            updated = dict(snapshot.entries)
            for path, entry in changes.items():
                if entry is None:
                    updated.pop(path, None)
                else:
                    updated[path] = entry
            run["data_hash"] = digest({p: e for p, e in updated.items() if p.startswith(data_prefix(run, task_id))})
            run["source_head"] = preparation["source_head"]
            run['workspace_history_unobserved'] = (
                run['publication_count'] != 0 or
                snapshot.json(CONTROL + '/config.json')['schema_version'] < 7 or
                run.get('workspace_provenance_protocol') != 1 or
                run.get('workspace_history_unobserved', False) is not False or
                preparation.get('workspace_history_unobserved') is not False)
            run["publication_count"] += 1
            run["last_publication_at"] = now()
            if outcome:
                from . import completion
                if completion_record is not None:
                    completion.update(run, completion_record)
                completion.require_terminal(run, outcome)
                run.update(status=outcome, finished_at=now(), summary=summary)
                release_slot(runtime, run)
                if snapshot.json(CONTROL + '/config.json')['schema_version'] < 9:
                    changes[run_path(task_id, run_id, snapshot) + "/result.json"] = encode(
                        {"run_id": run_id, "status": outcome, "summary": summary,
                         "source_head": run["source_head"], "artifacts": run["artifact_manifest"], "finished_at": run["finished_at"],
                         **({"completion": run["completion"]} if run.get("completion_contract") else {})})
                from . import maintenance
                for path, entry in preparation.get('cleanup', {}).get('io_manifest', {}).items():
                    target = run_path(task_id, run_id, snapshot) + '/io/' + path
                    if snapshot.entries.get(target) not in (None, tuple(entry)):
                        raise TaskError('Previously saved run I/O record changed')
                    changes[target] = entry
                changes[run_path(task_id, run_id, snapshot) + "/cleanup.json"] = encode(
                    maintenance.finish_record(run, operation_id, preparation, discard_output))
                if discard_output:
                    from . import reports
                    changes.update(reports.retained_changes(run, run_path(task_id, run_id, snapshot),
                                                           operation_id, 'output_not_adopted'))
            changes[root(task_id, snapshot) + "/runtime.json"] = encode(runtime)
            changes[run_path(task_id, run_id, snapshot) + "/run.json"] = encode(run)
            return changes, run
        return store.transact(operation_id, request, apply)


def validate_heartbeat(run, state):
    if (not isinstance(state, dict) or any(state.get(key) != run[key] for key in
            ('task_id', 'run_id', 'generation')) or state.get('schema_version') != 2 or
            type(state.get('sequence')) is not int or state['sequence'] < 0):
        raise TaskError('Heartbeat identity or sequence does not match execution authority')
    return state


def heartbeat_state(run, previous=None, progress=None, *, initial=False):
    previous = validate_heartbeat(run, previous) if previous else {}
    return dict(schema_version=2, task_id=run['task_id'], run_id=run['run_id'],
                generation=run['generation'], sequence=0 if initial else previous.get('sequence', 0) + 1,
                updated_at=now(), progress=progress if progress is not None else previous.get('progress'))


def heartbeat(store, task_id, run_id, *, progress=None, operation_id=None):
    from . import leases, task_io
    # Lease renewal uses the authority transaction, independently of the
    # detailed heartbeat ref. A fresh renewal gets a fresh operation identity.
    current = load_run(store.snapshot(), task_id, run_id, live=True)[2]
    if current["execution"]["recovery_mode"] in leases.MODES:
        for attempt in range(8):
            try:
                leases.renew(store, operation_id or uuid.uuid4().hex, task_id, run_id)
                break
            except SupersededRenewal:
                # A supervisor can issue a new renewal only after exact CAS
                # proves the previous one can never win. Explicit IDs stay fixed.
                if operation_id or attempt == 7:
                    raise
    return task_io.heartbeat(store, current, progress, operation_id)


def observer_boot():
    path = Path("/proc/sys/kernel/random/boot_id")
    if path.exists():
        return path.read_text().strip()
    # Unknown boot identity: restarting the observer starts a new observation
    # period. Never infer elapsed time from a remote commit's wall clock.
    return str(os.getpid())


def observe(store, task_id, run_id):
    run = check(store, task_id, run_id)
    tip = store.fetch(run["heartbeat_ref"], missing=True)
    if tip:
        beat = Snapshot(store, tip).json("state.json")
        validate_heartbeat(run, beat)
    path = local_state(store, task_id, run_id).with_suffix(".observation.json")
    with store.lock(path.with_suffix(".lock")):
        current = time.monotonic()
        old = json.loads(path.read_text()) if path.exists() else {}
        boot = observer_boot()
        reset = ("first_observation" if not old else "boot_changed" if old.get("boot") != boot else
                 "heartbeat_changed" if old.get("tip") != tip else
                 "generation_changed" if old.get("generation") != run["generation"] else
                 "clock_regressed" if old.get("since", current + 1) > current else None)
        if reset:
            old = {"boot": boot, "tip": tip, "generation": run["generation"], "since": current}
        atomic_json(path, old)
        elapsed = current - old["since"]
        return {"run_id": run_id, "generation": run["generation"], "observed_tip": tip,
                "unchanged_seconds": elapsed, "stale": elapsed >= run["execution"]["stale_seconds"],
                "recovery_mode": run["execution"]["recovery_mode"], "observation_reset": reset,
                "remaining_observation_seconds": max(0, run["execution"]["stale_seconds"] - elapsed)}


def recover(store, operation_id, task_id, run_id, *, stopped=False, settled=False, evidence=None, stale=False):
    request = dict(kind="run-recover", task_id=task_id, run_id=run_id, stale=stale,
                   stopped=stopped, settled=settled, evidence=evidence)
    receipt = store.snapshot().metadata(CONTROL + "/operations/" + identity(operation_id) + ".json")
    if receipt:
        if receipt["request_hash"] != digest(request):
            raise TaskError("Operation ID was already used for another request")
        return dict(receipt["result"], operation_id=operation_id, committed=True)
    observation = observe(store, task_id, run_id) if stale else None
    if stale and (not observation["stale"] or observation["recovery_mode"] != "git_only"):
        raise TaskError("Automatic recovery needs an observed stale Git-only run")
    if not stale and (not stopped or not settled or not evidence):
        raise TaskError("Recovery requires stopped writer/children, settled external operations, and an evidence reference")
    def apply(snapshot):
        task, runtime, run = load_run(snapshot, task_id, run_id, live=True)
        if observation and (run["generation"] != observation["generation"] or
                            run["execution"]["recovery_mode"] != "git_only"):
            raise TaskError("Observed execution authority changed")
        run.update(status="lost", finished_at=now(), recovery_evidence=evidence,
                   recovery_kind="stale_git_only" if stale else "confirmed_stopped")
        release_slot(runtime, run)
        changes = {root(task_id, snapshot) + "/runtime.json": encode(runtime),
                run_path(task_id, run_id, snapshot) + "/run.json": encode(run),
                run_path(task_id, run_id, snapshot) + "/result.json": encode(
                    {"run_id": run_id, "status": "lost", "evidence": evidence, "observation": observation}),
                run_path(task_id, run_id, snapshot) + "/cleanup.json": encode(
                    {"run_id": run_id, "status": "retained", "reason": "Recovery artifacts need inspection",
                     "machine_id": run["machine_id"]})}
        from . import reports
        changes.update(reports.retained_changes(run, run_path(task_id, run_id, snapshot),
                                               operation_id, 'recovery_artifacts_need_inspection', observation))
        return changes, run
    return store.transact(operation_id, request, apply)
