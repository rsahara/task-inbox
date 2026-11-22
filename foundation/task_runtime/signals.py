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
    state = runtime.local_state(store, task_id, run_id)
    with store.lock(state.with_suffix('.lock')):
        return _put(store, task_id, run_id, value)


def _put(store, task_id, run_id, value):
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
    from . import main_io, interfaces
    ancestor = run['definition_commit'] if main_io.enabled(snapshot) else run['source_head']
    if (work.is_symlink() or not work.is_dir() or
            git(work, 'rev-parse', '--show-toplevel') != str(work.resolve()) or
            not git(work, 'merge-base', '--is-ancestor', ancestor, 'HEAD', predicate=True)):
        raise TaskError('Prepared workspace is missing or changed')
    relative = runtime.data_prefix(run, task_id) + 'signals/' + value['id'] + '.json'
    previous = catalog(snapshot, task_id).get(value['id'])
    if previous and previous['path'] != relative:
        relative = previous['path']
    old = previous['value'] if previous else None
    prior_confirmation = local.get('confirmed_signals', {}).get(value['id'])
    if main_io.enabled(snapshot):
        from .store import Snapshot
        definition = Snapshot(store, run['definition_commit'])
        task = runtime.load_task(definition, task_id)[0]
        contract = interfaces.output(task, value)
        if contract and contract.get('value_schema'):
            from .signal_contracts import validate_value
            schema = definition.json(runtime.root(task_id, definition) + '/' + contract['value_schema'])
            validate_value(value.get('value', value.get('payload')), schema)
        value = copy.deepcopy(old if old and logical(old) == logical(value) else value)
        value['revision'] = (old['revision'] if old and logical(old) == logical(value)
                             else (old or {}).get('revision', 0) + 1)
        local.setdefault('confirmed_signals', {})[value['id']] = digest(value)
    if old and old != value and value['revision'] <= old['revision']:
        raise TaskError('Changed signal requires a newer revision')
    if old is None and value['revision'] != 1:
        raise TaskError('A new signal starts at revision 1')
    target = work / relative
    if target.is_symlink() or any(p.is_symlink() for p in target.parents if work in p.parents or p == work):
        raise TaskError('Signal path cannot follow symlinks')
    if target.exists() and target.read_bytes() not in (snapshot.read(relative), encode(value)):
        own_confirmation = (main_io.enabled(snapshot) and prior_confirmation
                            and digest(json.loads(target.read_text())) == prior_confirmation)
        if not own_confirmation:
            raise TaskError('Preserve unpublished signal changes before replacing them')
    atomic_json(target, value)
    if main_io.enabled(snapshot):
        atomic_json(state, local)
    return {'path': relative, 'publication': 'pending'}


def subscription(snapshot, task_id, consumer, *, signal_id=None, on='change', initial='next', pending=None):
    require_enabled(snapshot)
    runtime.load_task(snapshot, task_id); runtime.load_task(snapshot, consumer)
    if task_id == consumer:
        raise TaskError('A task cannot subscribe to its own signals')
    if pending not in (None, 'all', 'latest'):
        raise TaskError('Unknown pending policy')
    if pending == 'latest' and (not signal_id or on != 'change'):
        raise TaskError('Latest pending requires a signal-level change subscription')
    item = dict(task_id=task_id, consumer=consumer)
    if signal_id is None:
        if on != 'change' or initial != 'next':
            raise TaskError('Delivery conditions require a declared signal ID')
        return item
    from . import main_io, interfaces
    identity(signal_id, 'signal ID')
    if not main_io.enabled(snapshot):
        raise TaskError('Migrate main inbox storage before adding signal-level subscriptions')
    if on not in ('change', 'run') or initial not in ('next', 'current') or (on == 'run' and initial != 'next'):
        raise TaskError('Invalid signal delivery condition')
    declared = interfaces.declaration(runtime.load_task(snapshot, task_id)[0])
    if declared['schema_version'] != 2:
        raise TaskError('Signal subscriptions require declared output IDs')
    matches = [s for s in declared['signals']['writes'] if s['id'] == signal_id]
    if len(matches) != 1:
        raise TaskError('Subscribe to an exact declared output ID')
    item.update(signal_id=signal_id, on=on, initial=initial)
    item['id'] = 'subscription-' + digest(item)[:40]
    if pending == 'latest':
        item['pending'] = pending
    current = catalog(snapshot, task_id).get(signal_id)
    item['after_revision'] = current['value']['revision'] if current and initial == 'next' else 0
    return item


def subscribe(store, operation_id, task_id, consumer, *, signal_id=None, on='change', initial='next', pending=None):
    def apply(snapshot):
        item = subscription(snapshot, task_id, consumer, signal_id=signal_id, on=on, initial=initial, pending=pending)
        config = snapshot.json(SUBSCRIPTIONS, {'schema_version': 1, 'subscriptions': []})
        for route in config['subscriptions']:
            if route == item or ('id' in item and route.get('id') == item['id']):
                if pending is None or route.get('pending', 'all') == pending:
                    return {}, route
                if pending == 'latest':
                    route['pending'] = 'latest'
                else:
                    # Freeze the cutoff when returning to all, so retired revisions stay retired.
                    route['superseded_before'] = _delivered_revision(snapshot, route)
                    route.pop('pending')
                return {SUBSCRIPTIONS: encode(config)}, route
        config['subscriptions'].append(item)
        return {SUBSCRIPTIONS: encode(config)}, item
    request = dict(kind='signal-subscribe', task_id=task_id, consumer=consumer)
    if signal_id is not None:
        request.update(signal_id=signal_id, on=on, initial=initial)
    if pending is not None:
        request['pending'] = pending
    return store.transact(operation_id, request, apply)


def confirmed_value(store, task_id, run_id, signal_id):
    """Read an owned local confirmation before atomic successful publication."""
    identity(signal_id, 'signal ID')
    run = runtime.check(store, task_id, run_id)
    state = json.loads(runtime.local_state(store, task_id, run_id).read_text())
    if not state.get('prepared') or state['generation'] != run['generation']:
        raise TaskError('Prepare the run before reading confirmed signals')
    previous = catalog(store.snapshot(), task_id).get(signal_id)
    relative = previous['path'] if previous else runtime.data_prefix(run, task_id) + 'signals/' + signal_id + '.json'
    value = validate(json.loads((Path(state['path']) / relative).read_text()), task_id)
    if state.get('confirmed_signals', {}).get(signal_id) != digest(value):
        raise TaskError('Signal is not confirmed by this run')
    return value


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
    from . import main_io
    if main_io.enabled(snapshot):
        preview, count = delivery_changes(snapshot, task_id=task_id, participant=participant)
        if not preview:
            return dict(required=False, delivered=0, blocked=[], needs_attention=False)
        def apply(current):
            changes, count = delivery_changes(current, task_id=task_id, participant=participant)
            return changes, dict(required=bool(count), delivered=count, blocked=[], needs_attention=False)
        # The request binds the snapshot and source filter; replay never invents a delivery.
        operation = 'signal-sync-' + digest([snapshot.commit, task_id, participant])[:48]
        return store.transact(operation, dict(kind='signal-sync', task_id=task_id, participant=participant,
                                              observed=snapshot.commit), apply)
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


def read_notice(store, message, *, snapshot=None):
    """Read fixed v2 evidence, or resolve a legacy hint against the given main."""
    snapshot = snapshot or store.snapshot()
    value = message.get('payload', {})
    if message.get('kind') != KIND or not isinstance(value, dict):
        raise TaskError('Invalid signal notification')
    if value.get('schema_version') == 2:
        identity(value.get('task_id'), 'source task ID')
        identity(value.get('signal_id'), 'signal ID')
        identity(value.get('subscription_id'), 'subscription ID')
        if value.get('trigger') == 'run':
            identity(value.get('run_id'), 'source run ID')
        elif value.get('trigger') != 'change' or value.get('run_id') is not None:
            raise TaskError('Invalid signal notification trigger')
        signal = validate(value.get('signal'), value.get('task_id'))
        if (signal['id'] != value.get('signal_id') or signal['revision'] != value.get('revision')
                or value.get('trigger') not in ('change', 'run')
                or message.get('id') != notice_id(value)):
            raise TaskError('Invalid fixed signal notification')
        # The embedded value is the evidence. A latest-main source path could
        # already contain a newer revision and must not be paired with this hash.
        return dict(value, commit=snapshot.commit, sha256=digest(signal),
                    type=signal['type'], status=signal['status'])
    if message.get('kind') != KIND or value.get('schema_version') != 1:
        raise TaskError('Invalid signal notification')
    task_id = identity(value.get('task_id'), 'source task ID')
    signal_id = identity(value.get('signal_id'), 'signal ID')
    revision = value.get('revision')
    if type(revision) is not int or revision < 1 or message.get('id') != message_id(task_id, signal_id, revision):
        raise TaskError('Invalid signal notification identity')
    record = catalog(snapshot, task_id).get(signal_id)
    if not record or record['value']['revision'] < revision:
        raise TaskError('Signal notification is ahead of current main')
    return dict(value, commit=snapshot.commit, path=record['path'], sha256=record['sha256'],
                type=record['value']['type'], status=record['value']['status'])


def logical(value):
    """A value field owns its meaningful timestamps; envelope times are observations."""
    ignored = {'revision', 'observed_at', 'generated_at', 'emitted_at', 'run_id'}
    return {k: v for k, v in value.items() if k not in ignored}


def notice_id(payload):
    key = [payload['task_id'], payload['signal_id'], payload['subscription_id'], payload['trigger']]
    key.append(payload['run_id'] if payload['trigger'] == 'run' else payload['revision'])
    return 'signal-' + digest(key)[:64]


def _route_revision(route, message):
    payload = message['payload']
    if (message['kind'] == KIND and payload.get('schema_version') == 2
            and payload.get('subscription_id') == route['id'] and payload.get('trigger') == 'change'
            and payload.get('task_id') == route['task_id'] and payload.get('signal_id') == route['signal_id']
            and type(payload.get('revision')) is int and message['id'] == notice_id(payload)):
        return payload['revision']
    return 0


def _notice(route, value, run=None):
    owner, sid, rev = route['task_id'], value['id'], value['revision']
    if 'id' in route:
        trigger = route.get('on', 'change')
        payload = dict(schema_version=2, task_id=owner, signal_id=sid, revision=rev,
                       subscription_id=route['id'], trigger=trigger, signal=value,
                       run_id=run['run_id'] if trigger == 'run' else None)
        mid = notice_id(payload)
    else:
        payload = dict(schema_version=1, task_id=owner, signal_id=sid, revision=rev)
        mid = message_id(owner, sid, rev)
    return dict(schema_version=1, id=mid, kind=KIND, payload=payload)


def _delivered_revision(snapshot, route):
    """The immutable notice is the delivery watermark, even after acceptance."""
    from . import main_io, task_io
    record = catalog(snapshot, route['task_id']).get(route['signal_id'])
    if not record:
        return 0
    current = record['value']
    prefix = main_io.inbox(snapshot, route['consumer'])
    expected = _notice(route, current)
    if snapshot.json(prefix + expected['id'] + '.json') == expected:
        return current['revision']
    # A newly installed route or an unsynchronized snapshot can lag the source.
    revisions = (_route_revision(route, m) for m in task_io.inbox_messages(snapshot, prefix))
    return max((rev for rev in revisions if rev < current['revision']), default=0)


def pending_cutoffs(snapshot, consumer):
    cutoffs = {}
    for route in snapshot.json(SUBSCRIPTIONS, {}).get('subscriptions', []):
        if route['consumer'] == consumer:
            cutoff = (_delivered_revision(snapshot, route) if route.get('pending') == 'latest'
                      else route.get('superseded_before', 0))
            if cutoff:
                cutoffs[route['id']] = (route, cutoff)
    return cutoffs


def is_superseded(message, cutoffs):
    sid = message['payload'].get('subscription_id')
    if isinstance(sid, str) and sid in cutoffs:
        route, cutoff = cutoffs[sid]
        return 0 < _route_revision(route, message) < cutoff
    return False


def delivery_changes(snapshot, *, task_id=None, participant=None, run=None, confirmed=()):
    """Append all selected inbox messages inside the producer's main transaction."""
    from . import main_io
    config = snapshot.json(SUBSCRIPTIONS, {'subscriptions': []})
    changes, count = {}, 0
    catalogs = {}
    for route in config['subscriptions']:
        owner, consumer = route['task_id'], route['consumer']
        if task_id is not None and task_id != owner:
            continue
        if participant is not None and participant not in (owner, consumer):
            continue
        trigger = route.get('on', 'change')
        if trigger == 'run' and run is None:
            continue  # Run events are never inferred from the latest state.
        if owner not in catalogs:
            catalogs[owner] = catalog(snapshot, owner)
        for record in catalogs[owner].values():
            value = record['value']
            if route.get('signal_id') is not None and route['signal_id'] != value['id']:
                continue
            if trigger == 'run' and value['id'] not in confirmed:
                continue
            if trigger == 'change' and value['revision'] <= route.get('after_revision', 0):
                continue
            message = _notice(route, value, run)
            added = main_io.append(snapshot, consumer, message, changes)
            changes.update(added); count += bool(added)
    return changes, count


def finish_changes(store, snapshot, source, run, outcome, changes, local):
    """Validate required outputs and atomically publish only successful signals."""
    from . import interfaces
    from .store import Snapshot
    definition = Snapshot(store, run['definition_commit'])
    task = runtime.load_task(definition, run['task_id'])[0]
    interface = interfaces.declaration(task)
    prefix = path(snapshot, run['task_id'])
    confirmed = local.get('confirmed_signals', {})
    if outcome != 'succeeded':
        for name in list(changes):
            if name.startswith(prefix):
                del changes[name]
        return {}, 0
    values = catalog(source, run['task_id'])
    if interface['schema_version'] == 2:
        for output in interface['signals']['writes']:
            if output.get('required') and output['id'] not in confirmed:
                raise TaskError('Required output signal was not confirmed by this run')
    for sid, fingerprint in confirmed.items():
        value = values.get(sid, {}).get('value')
        if value is None or digest(value) != fingerprint:
            raise TaskError('Confirmed signal changed after submission')
        interfaces.output(task, value)
    for name in changes:
        if name.startswith(prefix):
            value = source.json(name)
            if value is not None and value['id'] not in confirmed:
                raise TaskError('Publish signals through signal put to confirm this run')
    projected = Snapshot(store, snapshot.commit)
    projected.entries = dict(snapshot.entries)
    for name, entry in changes.items():
        if entry is None:
            projected.entries.pop(name, None)
        else:
            projected.entries[name] = entry if isinstance(entry, tuple) else store.blob(entry)
    return delivery_changes(projected, task_id=run['task_id'], run=run, confirmed=confirmed)
