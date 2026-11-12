"""Durable, publication-bound inbox delivery. Payload meaning belongs to callers."""
from pathlib import Path
import json
import re

from .store import CONTROL, TaskError, atomic_json, digest, encode, git, identity

PREFIX = CONTROL + '/outbox/'
BINDINGS = CONTROL + '/outbox-messages/'
INTENTS = 'notification-outbox/'


def enabled(snapshot):
    return snapshot.json(CONTROL + '/config.json', {}).get('schema_version', 0) >= 10


def require_enabled(snapshot):
    if not enabled(snapshot):
        raise TaskError('Enable signals with all writers drained before using the durable outbox')


def messages(snapshot, values):
    from . import runtime, task_io
    if not isinstance(values, list) or not values:
        raise TaskError('Outbox requires at least one message')
    seen = set()
    for item in values:
        if not isinstance(item, dict) or set(item) != {'task_id', 'message_id', 'kind', 'payload'}:
            raise TaskError('Invalid outbox message')
        for field in ('task_id', 'message_id', 'kind'):
            identity(item[field], field)
        runtime.load_task(snapshot, item['task_id'])
        if 'publication_commit' in item['payload']:
            raise TaskError('publication_commit is reserved for outbox delivery')
        task_io.bounded(dict(item['payload'], publication_commit='f' * 64))
        key = (item['task_id'], item['message_id'])
        if key in seen:
            raise TaskError('Duplicate outbox destination/message identity')
        seen.add(key)
    return values


def record(snapshot, task_id, operation_id, values, *, source_commit=None, channel="intent", publication_id=None):
    from . import runtime
    require_enabled(snapshot)
    runtime.load_task(snapshot, task_id)
    identity(operation_id, 'operation ID')
    config = snapshot.json(CONTROL + '/config.json')
    values = messages(snapshot, values)
    key = digest([task_id, operation_id, channel])
    body = dict(schema_version=1, repository_id=config['repository_id'], authority_ref=config['authority_ref'],
                task_id=task_id, operation_id=operation_id, publication_id=publication_id or operation_id, channel=channel, messages=values, source_commit=source_commit)
    path = PREFIX + key + '.json'
    if snapshot.json(path) not in (None, body):
        raise TaskError('Outbox record is immutable')
    changes = {path: encode(body)}
    for item in values:
        binding_path = BINDINGS + item['task_id'] + '/' + item['message_id'] + '.json'
        binding = dict(record_path=path, source_commit=source_commit, message=item)
        previous = snapshot.json(binding_path)
        if previous and previous != binding:
            if not (source_commit and previous.get('source_commit') == source_commit and previous.get('message') == item):
                raise TaskError('Outbox message ID already has different content or publication')
        elif not previous:
            changes[binding_path] = encode(binding)
    return changes


def merge(left, right):
    """Validate all records/bindings produced by the same authority transaction."""
    if any(path in left and left[path] != value for path, value in right.items()):
        raise TaskError('Conflicting outbox messages in one publication')
    left.update(right)
    return left


def prepare(store, task_id, run_id, operation_id, values):
    """Write an intent into the owning workspace, before publication."""
    from . import runtime
    snapshot = store.snapshot()
    require_enabled(snapshot)
    runtime.check(store, task_id, run_id)
    local = runtime.local_state(store, task_id, run_id)
    if not local.is_file():
        raise TaskError('Prepare the run before creating an outbox intent')
    workspace = Path(json.loads(local.read_text())['path'])
    run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
    values = messages(snapshot, values)
    identity(operation_id, 'operation ID')
    relative = runtime.data_prefix(run, task_id) + INTENTS + digest(operation_id) + '.json'
    path = workspace / relative
    value = dict(schema_version=1, kind='task-inbox-outbox', task_id=task_id,
                 operation_id=operation_id, messages=values)
    if path.is_symlink() or any(p.is_symlink() for p in path.parents if workspace in p.parents or p == workspace):
        raise TaskError('Outbox intent cannot follow symlinks')
    if path.exists() and json.loads(path.read_text()) != value:
        raise TaskError('Outbox operation reused with different messages')
    atomic_json(path, value)
    return relative


def capture(snapshot, run, source, touched, publication_id):
    """Called inside the publication CAS; data and delivery records commit together."""
    from . import runtime
    prefix = runtime.data_prefix(run, run['task_id']) + INTENTS
    changes = {}
    for path in touched:
        if not path.startswith(prefix) or not path.endswith('.json'):
            continue
        raw = source.read(path)
        if raw == snapshot.read(path):
            continue
        value = json.loads(raw) if raw is not None else None
        old = snapshot.json(path)
        if old and old.get('kind') == 'task-inbox-outbox':
            raise TaskError('Published outbox intents are immutable')
        if not value or value.get('kind') != 'task-inbox-outbox':
            continue  # Application records from before outbox adoption are data.
        if value.get('schema_version') != 1 or value.get('task_id') != run['task_id']:
            raise TaskError('Outbox intent owner/schema differs')
        merge(changes, record(snapshot, run['task_id'], value['operation_id'], value['messages'], publication_id=publication_id))
    return changes


def enqueue(store, operation_id, task_id, source_commit, values):
    """Reference data already saved on authority; retry with the same operation ID."""
    if not isinstance(source_commit, str) or not re.fullmatch(r'[a-f0-9]{40}|[a-f0-9]{64}', source_commit):
        raise TaskError('Outbox source must be a full commit ID')
    request = dict(kind='outbox-enqueue', task_id=task_id, source_commit=source_commit, messages=values)
    def apply(snapshot):
        if not git(store.repo, 'merge-base', '--is-ancestor', source_commit, snapshot.commit, predicate=True):
            raise TaskError('Outbox source must be a saved authority commit')
        return record(snapshot, task_id, operation_id, values, source_commit=source_commit), {'queued': True}
    result = store.transact(operation_id, request, apply)
    return dict(result, **flush(store, operation_id=operation_id, task_id=task_id))


def flush(store, *, operation_id=None, task_id=None, publication_id=None):
    """Recover delivery from Git, including partial fanout and lost responses."""
    from . import task_io
    snapshot = store.snapshot()
    if not enabled(snapshot):
        return dict(required=False, delivered=0)
    config = snapshot.json(CONTROL + '/config.json')
    inboxes = {}
    origins = None
    delivered, selected = 0, 0
    for path in sorted(p for p in snapshot.entries if p.startswith(PREFIX) and p.endswith('.json')):
        value = snapshot.json(path)
        if ((operation_id is not None and value['operation_id'] != operation_id) or
                (task_id is not None and value['task_id'] != task_id)):
            continue
        if publication_id is not None and value.get('publication_id') != publication_id:
            continue
        selected += 1
        if (value.get('schema_version') != 1 or value.get('repository_id') != config['repository_id'] or
                value.get('authority_ref') != config['authority_ref']):
            raise TaskError('Outbox belongs to another authority')
        if origins is None:
            store.require_history()
            origins = {}
            commit = None
            history = git(store.repo, 'log', '--full-history', '--no-merges', '--diff-filter=A',
                          '--format=commit:%H', '--name-only', snapshot.commit, '--', ':(literal)' + PREFIX)
            for line in history.splitlines():
                if line.startswith('commit:'):
                    commit = line[7:]
                elif line.startswith(PREFIX):
                    origins[line] = commit
        origin = origins.get(path)
        if not origin or git(store.repo, 'show', origin + ':' + path, binary=True, strip=False) != snapshot.read(path):
            raise TaskError('Outbox record changed since its publication')
        source_commit = value.get('source_commit') or origin
        if not git(store.repo, 'merge-base', '--is-ancestor', source_commit, snapshot.commit, predicate=True):
            raise TaskError('Outbox source is outside authority history')
        missing = []
        for item in messages(snapshot, value['messages']):
            target = item['task_id']
            if target not in inboxes:
                inboxes[target] = task_io.IOStore(store, target).optional()
            existing = inboxes[target].json('messages/' + item['message_id'] + '.json')
            if existing is None:
                missing.append(item)
            elif (existing.get('kind') != item['kind'] or
                  existing.get('payload') != dict(item['payload'], publication_commit=source_commit)):
                raise TaskError('Outbox message ID already has different content')
        if not missing:
            continue
        for item in missing:
            payload = dict(item['payload'], publication_commit=source_commit)
            send_id = 'outbox-' + digest([path, item['task_id'], item['message_id']])[:64]
            task_io.send(store, send_id, item['task_id'], item['message_id'], item['kind'], payload)
            delivered += 1
    return dict(required=bool(selected), delivered=delivered)
