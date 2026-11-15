"""Schema 11: authority-resident inboxes, events and accepted input results."""
from pathlib import Path
import tempfile

from . import layout, runtime
from .store import CONTROL, Snapshot, TaskError, digest, encode, identity, git


def enabled(snapshot):
    return snapshot.json(CONTROL + '/config.json', {}).get('schema_version', 0) >= 11


def inbox(snapshot, task_id):
    runtime.load_task(snapshot, task_id)
    return runtime.root(task_id, snapshot) + '/io/inbox/messages/'


def append(snapshot, task_id, message, changes=None):
    """Build changes only; the owning authority transaction commits the fanout."""
    from .task_io import MessageConflict, bounded
    identity(message['id'], 'message ID'); identity(message['kind'], 'message kind')
    bounded(message['payload'])
    path = inbox(snapshot, task_id) + message['id'] + '.json'
    raw = (changes or {}).get(path)
    if raw is not None:
        import json
        old = json.loads(raw) if isinstance(raw, bytes) else None
    else:
        old = snapshot.json(path)
    if old is not None and old != message:
        raise MessageConflict('Message ID already used with different content')
    return {} if old is not None else {path: encode(message)}


def complete_changes(snapshot, task_id, run, results):
    from . import task_io
    if not isinstance(results, dict):
        raise TaskError('Input results must map captured message IDs to verified results')
    changes = {}
    for mid, value in results.items():
        identity(mid, 'message ID'); value = task_io.bounded(value)
        if value.get('status') not in ('succeeded', 'failed', 'superseded'):
            raise TaskError('Only reconciled input results can be accepted')
        source = snapshot.json(inbox(snapshot, task_id) + mid + '.json')
        if source is None:
            raise TaskError('Input message is not present on the authority')
        path = task_io.receipt_path(task_id, mid, snapshot)
        previous = snapshot.json(path)
        if previous:
            if previous['message_sha256'] != digest(source) or previous['result'] != value:
                raise TaskError('Message already completed with a different result')
            continue
        changes[path] = encode(dict(schema_version=2, task_id=task_id, message_id=mid,
            message_sha256=digest(source), run_id=run['run_id'], generation=run['generation'],
            result=value))
    return changes


def stage_results(store, task_id, run_id, results):
    """Locally confirm captured input results for the atomic finish transaction."""
    import json
    snapshot = store.snapshot()
    if not enabled(snapshot):
        raise TaskError('Deferred input results require main inbox storage')
    state_path = runtime.local_state(store, task_id, run_id)
    with store.lock(state_path.with_suffix('.lock')):
        run = runtime.check(store, task_id, run_id)
        # Validate message existence, result bounds and prior accepted receipts now,
        # then repeat that validation inside the final authority transaction.
        complete_changes(snapshot, task_id, run, results)
        state = json.loads(state_path.read_text())
        if not state.get('prepared') or state['generation'] != run['generation']:
            raise TaskError('Prepare the run before staging input results')
        staged = state.setdefault('input_results', {})
        for mid, value in results.items():
            if mid in staged and staged[mid] != value:
                raise TaskError('Captured input result already staged with different content')
            staged[mid] = value
        from .store import atomic_json
        atomic_json(state_path, state)
    return dict(staged=len(results), accepted=False)


def migrate(store, operation_id, *, drained=False, subscriptions=None):
    """Import immutable old inboxes after explicit, repository-wide writer drain."""
    from . import task_io
    if not drained:
        raise TaskError('Confirm all legacy writers are drained before migrating inbox storage')
    request = dict(kind='main-inbox-migrate')
    if subscriptions is not None:
        if not isinstance(subscriptions, list):
            raise TaskError('Migration subscriptions must be a list')
        request['subscriptions'] = subscriptions
    before = store.snapshot()
    if enabled(before):
        # Still replay through the receipt protocol to bind the operation ID.
        return store.transact(operation_id, request,
                              lambda snapshot: ({}, dict(schema_version=11, migrated=True)))
    imported = {}
    for task_id in layout.tasks(before):
        _, registry = runtime.load_task(before, task_id)
        if any(s.get('run_id') for s in registry['slots'].values()):
            raise TaskError('Finish or reconcile all runs before migrating inbox storage')
        old = task_io.IOStore(store, task_id).optional()
        for name in old.entries:
            if name.startswith('messages/') and name.endswith('.json'):
                value = old.json(name)
                if name != 'messages/' + identity(value.get('id'), 'message ID') + '.json':
                    raise TaskError('Malformed legacy inbox message')
                task_io.bounded(value['payload']); identity(value['kind'], 'message kind')
                imported[(task_id, value['id'])] = value
    def apply(snapshot):
        if enabled(snapshot):
            return {}, dict(schema_version=11, migrated=True)
        for task_id in layout.tasks(snapshot):
            if any(s.get('run_id') for s in runtime.load_task(snapshot, task_id)[1]['slots'].values()):
                raise TaskError('Finish or reconcile all runs before migrating inbox storage')
        changes = runtime.metadata_policy_changes(store, snapshot)
        import json
        config = json.loads(changes.get(CONTROL + '/config.json', encode(snapshot.json(CONTROL + '/config.json'))))
        config.update(schema_version=11, inbox_storage='authority', workspace_provenance_protocol=1)
        changes[CONTROL + '/config.json'] = encode(config)
        if subscriptions:
            from . import signals
            projected = Snapshot(store, snapshot.commit)
            projected.entries = dict(snapshot.entries)
            projected.entries[CONTROL + '/config.json'] = store.blob(encode(config))
            routes = snapshot.json(signals.SUBSCRIPTIONS, dict(schema_version=1, subscriptions=[]))
            for spec in subscriptions:
                route = signals.subscription(projected, **spec)
                if not any(s.get('id') == route.get('id') for s in routes['subscriptions']):
                    routes['subscriptions'].append(route)
            changes[signals.SUBSCRIPTIONS] = encode(routes)
        for (task_id, mid), value in imported.items():
            changes.update(append(snapshot, task_id, value, changes))
            receipt = snapshot.json(task_io.receipt_path(task_id, mid, snapshot))
            if receipt and receipt['message_sha256'] != digest(value):
                raise TaskError('Imported message differs from its accepted receipt')
        return changes, dict(schema_version=11, migrated=True, imported=len(imported))
    return store.transact(operation_id, request, apply)


def workspace_snapshot(store, task_id, run_id, state, run, *, include_signals=True):
    """Capture explicitly published files without touching worker HEAD or index."""
    work = Path(state['path'])
    if not state.get('prepared') or state['generation'] != run['generation']:
        raise TaskError('Workspace is not prepared for this run')
    if not git(work, 'merge-base', '--is-ancestor', run['definition_commit'], 'HEAD', predicate=True):
        raise TaskError('Workspace definition ancestry changed')
    paths = set()
    for commit in git(work, 'rev-list', run['definition_commit'] + '..HEAD').splitlines():
        paths.update(runtime.names(work, 'diff-tree', '--no-commit-id', '--name-only',
                                   '--no-renames', '-r', '-z', commit))
    for args in [('diff', '--name-only', '--no-renames', '-z'),
                 ('diff', '--cached', '--name-only', '--no-renames', '-z'),
                 ('ls-files', '--others', '--exclude-standard', '-z')]:
        paths.update(runtime.names(work, *args))
    for path, fingerprint in state.get('preserved_files', {}).items():
        if file_fingerprint(work / path) != fingerprint:
            raise TaskError('Pre-existing workspace changes must be preserved: ' + path)
        paths.discard(path)
    if any(not runtime.allowed(task_id, run_id, p, run) for p in paths):
        raise TaskError("Work history or pending files cross the run's data/artifact boundary")
    baseline = Snapshot(store, run['source_head'])
    changes = {}
    for path in paths:
        if not include_signals and path.startswith(runtime.data_prefix(run, task_id) + 'signals/'):
            continue
        target = work / path
        if target.is_symlink() or any(p.is_symlink() for p in target.parents if work in p.parents):
            raise TaskError('Run outputs cannot contain symlinks')
        if target.exists() and not target.is_file():
            raise TaskError('Run outputs must be regular files')
        entry = store.blob(target.read_bytes()) if target.is_file() else None
        if entry and target.stat().st_mode & 0o111:
            entry = ('100755', *entry[1:])
        if baseline.entries.get(path) != entry:
            changes[path] = entry
    commit = store.commit(baseline.commit, changes, 'Snapshot explicitly submitted task output ' + run_id)
    # Keep an ambiguous publication's objects alive without exposing work refs.
    ref = 'refs/task-runtime-snapshots/' + store.local.name + '/' + identity(task_id) + '/' + identity(run_id)
    git(store.repo, 'update-ref', ref, commit)
    return Snapshot(store, commit), set(changes)


def file_fingerprint(path):
    if path.is_symlink():
        return ['symlink', str(path.readlink())]
    if not path.exists():
        return None
    if not path.is_file():
        raise TaskError('Workspace changes must be regular files')
    return [bool(path.stat().st_mode & 0o111), digest(path.read_bytes().hex())]


def preserved_files(work):
    paths = set()
    for args in [('diff', '--name-only', '-z'), ('diff', '--cached', '--name-only', '-z'),
                 ('ls-files', '--others', '--exclude-standard', '-z')]:
        paths.update(runtime.names(work, *args))
    return {path: file_fingerprint(work / path) for path in sorted(paths)}


def merge_entry(store, path, base, current, submitted):
    """Ordinary three-way content reconciliation; never guess through a conflict."""
    from .store import GitFailure
    if submitted == base or submitted == current:
        return current
    if base is None or current is None or submitted is None:
        raise TaskError('Concurrent output add/delete conflict: ' + path)
    if not (base[0] == current[0] == submitted[0]):
        raise TaskError('Concurrent output mode conflict: ' + path)
    with tempfile.TemporaryDirectory(dir=store.local) as directory:
        files = []
        for name, entry in [('current', current), ('base', base), ('submitted', submitted)]:
            file = Path(directory) / name
            file.write_bytes(git(store.repo, 'cat-file', 'blob', entry[2], binary=True, strip=False))
            files.append(str(file))
        try:
            merged = git(store.repo, 'merge-file', '-p', *files, binary=True, strip=False)
        except GitFailure:
            raise TaskError('Concurrent output content conflict: ' + path) from None
    entry = store.blob(merged)
    return (current[0], *entry[1:])


def verify_saved_workspace(store, run, work):
    """Only discard dirty files matching the submitted, accepted snapshot."""
    paths = set()
    for args in [('diff', '--name-only', '-z'), ('diff', '--cached', '--name-only', '-z'),
                 ('ls-files', '--others', '--exclude-standard', '-z'),
                 ('ls-files', '--others', '--ignored', '--exclude-standard', '-z')]:
        paths.update(runtime.names(work, *args))
    source = Snapshot(store, run['source_head'])
    for name in paths:
        if not runtime.allowed(run['task_id'], run['run_id'], name, run):
            raise TaskError('Workspace contains unpreserved files')
        target = work / name
        if target.is_symlink() or (target.exists() and not target.is_file()):
            raise TaskError('Workspace contains unpreserved files')
        current = store.blob(target.read_bytes()) if target.is_file() else None
        if current and target.stat().st_mode & 0o111:
            current = ('100755', *current[1:])
        expected = source.entries.get(name)
        if current != (tuple(expected) if expected else None):
            raise TaskError('Workspace contains unpreserved files')
