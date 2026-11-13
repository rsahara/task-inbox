"""Typed task-owned signals. The runtime never interprets assessment or priority."""
import copy
import hashlib
import json
import sys
from pathlib import Path

from . import layout, runtime
from .store import CONTROL, TaskError, atomic_json, digest, encode, identity, git

SUBSCRIPTIONS = CONTROL + '/signal-subscriptions.json'
KIND = 'signal-changed'
def enabled(snapshot):
    return snapshot.json(CONTROL + '/config.json', {}).get('schema_version', 0) >= 10


def require_enabled(snapshot):
    if not enabled(snapshot):
        raise TaskError('Enable signals after all writers have drained')


def enable(store, operation_id):
    def apply(snapshot):
        config = snapshot.json(CONTROL + '/config.json')
        if enabled(snapshot):
            return {}, {'enabled': True}
        for location in layout.tasks(snapshot).values():
            if any(s.get('run_id') for s in snapshot.json(location + '/runtime.json')['slots'].values()):
                raise TaskError('Finish or reconcile all runs before enabling signals')
        changes = runtime.metadata_policy_changes(store, snapshot)
        config = json.loads(changes.get(CONTROL + '/config.json', encode(config)))
        config['schema_version'] = 10
        changes[CONTROL + '/config.json'] = encode(config)
        return changes, {'enabled': True}
    return store.transact(operation_id, {'kind':'signal-enable'}, apply)


def validate(value, task_id):
    if not isinstance(value, dict) or value.get('schema_version') != 1 or value.get('task_id') != task_id:
        raise TaskError('Signal schema/owner mismatch')
    identity(value.get('id'), 'signal ID')
    if type(value.get('revision')) is not int or value['revision'] < 1:
        raise TaskError('Signal revision must be a positive integer')
    if not isinstance(value.get('type'), str) or not value['type'].strip():
        raise TaskError('Signal type must be nonempty')
    if value.get('status') not in ('active', 'resolved', 'withdrawn'):
        raise TaskError('Invalid signal status')
    if value['status'] != 'active' and not (isinstance(value.get('resolution'), str) and value['resolution'].strip()):
        raise TaskError('Ended signal requires a resolution reason')
    return value


def path(snapshot, task_id):
    definition = runtime.load_task(snapshot, task_id)[0]
    return runtime.data_prefix(definition, task_id, runtime.root(task_id, snapshot)) + 'signals/'


def catalog(snapshot, task_id, *, include_ended=True):
    prefix = path(snapshot, task_id)
    result = {}
    for name in sorted(p for p in snapshot.entries if p.startswith(prefix) and p.endswith('.json')):
        value = validate(snapshot.json(name), task_id)
        if value['id'] in result:
            raise TaskError('Duplicate signal ID')
        if not include_ended and value['status'] != 'active':
            continue
        result[value['id']] = dict(value=value, path=name, sha256=hashlib.sha256(snapshot.read(name)).hexdigest())
    return result


def read_value(snapshot, name):
    """Read a signal's logical value from task data or its durable evidence path."""
    value = snapshot.json(name)
    if not isinstance(value, dict):
        raise TaskError('Missing signal evidence')
    return validate(value, identity(value.get('task_id'), 'source task ID'))


def put(store, task_id, run_id, value):
    value = validate(value, task_id)
    snapshot = store.snapshot()
    require_enabled(snapshot)
    run = runtime.check(store, task_id, run_id)
    state = runtime.local_state(store, task_id, run_id)
    if not state.is_file():
        raise TaskError('Prepare the run before updating signals')
    local = json.loads(state.read_text())
    if not local.get('prepared') or local.get('generation') != run['generation']:
        raise TaskError('Prepare the run before updating signals')
    work = Path(local['path'])
    if (work.is_symlink() or not work.is_dir() or
            git(work, 'rev-parse', '--show-toplevel') != str(work.resolve()) or
            not git(work, 'merge-base', '--is-ancestor', run['source_head'], 'HEAD', predicate=True)):
        raise TaskError('Prepared workspace is missing or changed')
    relative = runtime.data_prefix(run, task_id) + 'signals/' + value['id'] + '.json'
    previous = catalog(snapshot, task_id).get(value['id'])
    if previous and previous['path'] != relative:
        relative = previous['path']
    old = previous['value'] if previous else None
    if old and old != value and value['revision'] <= old['revision']:
        raise TaskError('Changed signal requires a newer revision')
    if old is None and value['revision'] != 1:
        raise TaskError('A new signal starts at revision 1')
    target = work / relative
    if target.is_symlink() or any(p.is_symlink() for p in target.parents if work in p.parents or p == work):
        raise TaskError('Signal path cannot follow symlinks')
    if target.exists() and target.read_bytes() not in (snapshot.read(relative), encode(value)):
        raise TaskError('Preserve unpublished signal changes before replacing them')
    atomic_json(target, value)
    return {'path': relative, 'publication': 'pending'}


def subscribe(store, operation_id, task_id, consumer):
    def apply(snapshot):
        require_enabled(snapshot)
        runtime.load_task(snapshot, task_id); runtime.load_task(snapshot, consumer)
        if task_id == consumer:
            raise TaskError('A task cannot subscribe to its own signals')
        config = snapshot.json(SUBSCRIPTIONS, {'schema_version':1, 'subscriptions':[]})
        item = dict(task_id=task_id, consumer=consumer)
        if item not in config['subscriptions']:
            config = copy.deepcopy(config)
            config['subscriptions'].append(item)
        return {SUBSCRIPTIONS: encode(config)}, item
    return store.transact(operation_id, dict(kind='signal-subscribe', task_id=task_id,
                          consumer=consumer), apply)


def publication(snapshot, run, source, touched, operation_id):
    """Validate owned signal updates inside the publication transaction."""
    if not enabled(snapshot):
        return {}
    task_id = run['task_id']
    prefix = runtime.data_prefix(run, task_id) + 'signals/'
    if not any(p.startswith(prefix) for p in touched):
        return {}
    before, after = catalog(snapshot, task_id), catalog(source, task_id)
    for signal_id in set(before) | set(after):
        old, new = before.get(signal_id), after.get(signal_id)
        if new is None:
            raise TaskError('Retain ended signal records; physical deletion is unsupported')
        value = new['value']
        if old is None and value['revision'] != 1:
            raise TaskError('A new signal starts at revision 1')
        if old and (old['value'] != value or old['path'] != new['path']) and value['revision'] <= old['value']['revision']:
            raise TaskError('Changed signal requires a newer revision')
    return {}


def message_id(task_id, signal_id, revision):
    return 'signal-' + digest([task_id, signal_id, revision])[:64]


def sync(store, *, task_id=None, participant=None):
    """Catch up to current main using inbox messages as the only delivery record."""
    from . import task_io
    snapshot = store.snapshot()
    if not enabled(snapshot):
        return dict(required=False, delivered=0, blocked=[], needs_attention=False)
    subscriptions = snapshot.json(SUBSCRIPTIONS, {'subscriptions': []})['subscriptions']
    if participant is not None:
        subscriptions = [s for s in subscriptions if participant in (s['task_id'], s['consumer'])]
    inboxes, blocked = {}, []
    delivered = selected = 0
    for producer in sorted({s['task_id'] for s in subscriptions}):
        if task_id is not None and producer != task_id:
            continue
        targets = sorted({s['consumer'] for s in subscriptions if s['task_id'] == producer})
        for record in catalog(snapshot, producer).values():
            value = record['value']
            mid = message_id(producer, value['id'], value['revision'])
            payload = dict(schema_version=1, task_id=producer, signal_id=value['id'], revision=value['revision'])
            expected = dict(schema_version=1, id=mid, kind=KIND, payload=payload)
            for target in targets:
                selected += 1
                if target not in inboxes:
                    inboxes[target] = task_io.IOStore(store, target).optional()
                previous = inboxes[target].json('messages/' + mid + '.json')
                try:
                    if previous:
                        if previous != expected:
                            raise task_io.MessageConflict('Signal message identity collision')
                        continue
                    task_io.send(store, mid, target, mid, KIND, payload)
                    delivered += 1
                except task_io.MessageConflict:
                    blocked.append(dict(task_id=target, message_id=mid, reason='message_id_conflict'))
    return dict(required=bool(selected), delivered=delivered, blocked=blocked, needs_attention=bool(blocked))


def startup_sync(store, task_id):
    result = sync(store, participant=task_id)
    if result['blocked']:
        # An owned repair run may advance the signal revision. Keep admission
        # available while reporting the immutable inbox collision explicitly.
        print(json.dumps(dict(event='signal-sync-blocked', task_id=task_id, **result)), file=sys.stderr)
    return result


def require_delivery(result):
    if result['blocked']:
        raise TaskError('Signal message ID has different content; inspect signal sync')
    return result


def read_notice(store, message):
    """A notice is a hint: consumers evaluate current main, not historical events."""
    value = message.get('payload', {})
    if message.get('kind') != KIND or value.get('schema_version') != 1:
        raise TaskError('Invalid signal notification')
    task_id = identity(value.get('task_id'), 'source task ID')
    signal_id = identity(value.get('signal_id'), 'signal ID')
    revision = value.get('revision')
    if type(revision) is not int or revision < 1 or message.get('id') != message_id(task_id, signal_id, revision):
        raise TaskError('Invalid signal notification identity')
    snapshot = store.snapshot()
    record = catalog(snapshot, task_id).get(signal_id)
    if not record or record['value']['revision'] < revision:
        raise TaskError('Signal notification is ahead of current main')
    return dict(value, commit=snapshot.commit, path=record['path'], sha256=record['sha256'],
                type=record['value']['type'], status=record['value']['status'])
