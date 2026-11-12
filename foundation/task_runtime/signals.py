"""Typed task-owned signals. The runtime never interprets assessment or priority."""
import copy
import hashlib
import json
from pathlib import Path

from . import layout, runtime, outbox
from .store import CONTROL, TaskError, atomic_json, digest, encode, identity

SUBSCRIPTIONS = CONTROL + '/signal-subscriptions.json'
KIND = 'signal-changed'
HEADS = CONTROL + '/signal-heads/'


def enable(store, operation_id):
    def apply(snapshot):
        config = snapshot.json(CONTROL + '/config.json')
        if outbox.enabled(snapshot):
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


def catalog(snapshot, task_id):
    prefix = path(snapshot, task_id)
    result = {}
    for name in sorted(p for p in snapshot.entries if p.startswith(prefix) and p.endswith('.json')):
        value = validate(snapshot.json(name), task_id)
        if value['id'] in result:
            raise TaskError('Duplicate signal ID')
        result[value['id']] = dict(value=value, path=name, sha256=hashlib.sha256(snapshot.read(name)).hexdigest())
    return result


def put(store, task_id, run_id, value):
    value = validate(value, task_id)
    snapshot = store.snapshot()
    outbox.require_enabled(snapshot)
    run = runtime.check(store, task_id, run_id)
    state = runtime.local_state(store, task_id, run_id)
    if not state.is_file():
        raise TaskError('Prepare the run before updating signals')
    work = Path(json.loads(state.read_text())['path'])
    relative = runtime.data_prefix(run, task_id) + 'signals/' + value['id'] + '.json'
    previous = catalog(snapshot, task_id).get(value['id'])
    if previous and previous['path'] != relative:
        relative = previous['path']
    head = snapshot.json(HEADS + task_id + '/' + value['id'] + '.json')
    old = previous['value'] if previous else (head or {}).get('value')
    if old and (old != value or (head or {}).get('removed')) and value['revision'] <= old['revision']:
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


def subscribe(store, operation_id, task_id, consumer, signal_type=None):
    def apply(snapshot):
        outbox.require_enabled(snapshot)
        runtime.load_task(snapshot, task_id); runtime.load_task(snapshot, consumer)
        if task_id == consumer:
            raise TaskError('A task cannot subscribe to its own signals')
        config = snapshot.json(SUBSCRIPTIONS, {'schema_version':1, 'subscriptions':[]})
        item = dict(task_id=task_id, consumer=consumer, type=signal_type)
        if item not in config['subscriptions']:
            config = copy.deepcopy(config)
            config['subscriptions'].append(item)
        return {SUBSCRIPTIONS: encode(config)}, item
    return store.transact(operation_id, dict(kind='signal-subscribe', task_id=task_id,
                          consumer=consumer, type=signal_type), apply)


def publication(snapshot, run, source, touched, operation_id):
    if not outbox.enabled(snapshot):
        return {}
    task_id = run['task_id']
    prefix = runtime.data_prefix(run, task_id) + 'signals/'
    if not any(p.startswith(prefix) for p in touched):
        return {}
    before, after = catalog(snapshot, task_id), catalog(source, task_id)
    subscriptions = snapshot.json(SUBSCRIPTIONS, {'subscriptions':[]})['subscriptions']
    changes, values = {}, []
    for signal_id in sorted(set(before) | set(after)):
        old, new = before.get(signal_id), after.get(signal_id)
        if old == new:
            continue
        head_path = HEADS + task_id + '/' + signal_id + '.json'
        head = snapshot.json(head_path)
        previous = old['value'] if old else (head or {}).get('value')
        if new:
            value = new['value']
            if previous is None and value['revision'] != 1:
                raise TaskError('A new signal starts at revision 1')
            if previous and (value != previous or (head or {}).get('removed')) and value['revision'] <= previous['revision']:
                raise TaskError('Changed signal requires a newer revision')
            changes[head_path] = encode(new)
        else:
            if old['value']['status'] == 'active':
                raise TaskError('Publish a resolved/withdrawn tombstone before removing an active signal')
            value = old['value']
            changes[head_path] = encode(dict(old, removed=True))
        selected = new or old
        payload = dict(schema_version=1, task_id=task_id, signal_id=signal_id,
            revision=value['revision'], type=value['type'], status=value['status'],
            removed=new is None, path=selected['path'], sha256=selected['sha256'])
        message_id = 'signal-' + digest([task_id, operation_id, signal_id])[:64]
        for target in sorted({s['consumer'] for s in subscriptions if s['task_id'] == task_id and
                              s.get('type') in (None, value['type'])}):
            values.append(dict(task_id=target, message_id=message_id, kind=KIND, payload=payload))
    if values:
        changes.update(outbox.record(snapshot, task_id, operation_id, values, channel="signals"))
    return changes


def read_notice(store, message):
    from .store import Snapshot, git
    value = message.get('payload', {})
    if message.get('kind') != KIND or value.get('schema_version') != 1:
        raise TaskError('Invalid signal notification')
    task_id = identity(value.get('task_id'), 'source task ID')
    signal_id = identity(value.get('signal_id'), 'signal ID')
    commit = value.get('publication_commit', '')
    import re
    current = store.snapshot()
    if not re.fullmatch(r'[a-f0-9]{40}|[a-f0-9]{64}', commit) or not git(
            store.repo, 'merge-base', '--is-ancestor', commit, current.commit, predicate=True):
        raise TaskError('Signal notification is outside authority history')
    original = Snapshot(store, commit)
    if value.get('removed') is True:
        record = original.json(HEADS + task_id + '/' + signal_id + '.json')
        if not record or record.get('removed') is not True or original.read(record['path']) is not None:
            raise TaskError('Missing signal removal evidence')
    else:
        record = catalog(original, task_id).get(signal_id)
    if (not record or value.get('path') != record['path'] or value.get('sha256') != record['sha256'] or
            any(value.get(k) != record['value'].get(k) for k in ('revision', 'type', 'status'))):
        raise TaskError('Signal notification differs from its saved source')
    return dict(value, commit=commit)
