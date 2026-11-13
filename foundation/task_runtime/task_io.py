"""Durable task inboxes and run streams; execution authority stays on main."""
import copy
import re
import uuid

from . import runtime
from .store import CONTROL, Snapshot, Store, TaskError, digest, encode, identity, refs_overlap, reservation_id

MAX_BYTES = 262144


class MessageConflict(TaskError):
    """An immutable inbox identity already belongs to different content."""


def prefix(config):
    value = config.get('io_ref_prefix', 'task-io')
    reserved = (config['authority_ref'], 'refs/heads/' + config['ref_prefix'])
    if 'io_ref_prefix' not in config:
        suffix = 1
        while any(refs_overlap('refs/heads/' + value, ref) for ref in reserved):
            suffix += 1
            value = 'task-io-' + str(suffix)
    git_format = isinstance(value, str) and re.fullmatch(r'[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*', value)
    if not git_format:
        raise TaskError('Invalid task I/O namespace')
    git_ref = 'refs/heads/' + value
    # Ref validation is also performed by Store on each concrete branch.
    if any(refs_overlap(git_ref, ref) for ref in reserved):
        raise TaskError('Task I/O namespace overlaps runtime authority or work refs')
    return value


class IOStore(Store):
    """Reuse Store's CAS, receipts and ambiguous-push reconciliation with a data manifest."""
    def __init__(self, authority, task_id, run=None):
        self.authority, self.run = authority, run
        config = authority.snapshot().json(CONTROL + '/config.json')
        task_id = identity(task_id, 'task ID')
        branch = prefix(config) + '/' + task_id + ('/runs/' + identity(run['run_id']) if run else '/inbox')
        if run and (run.get('io_ref') != 'refs/heads/' + branch or run.get('heartbeat_ref') != run['io_ref']):
            raise TaskError('Run I/O ref differs from its pinned namespace')
        super().__init__(authority.repo, authority.remote, branch,
                         transport_adapter=getattr(authority.transport, 'program', None))
        self.manifest = dict(schema_version=1, kind='task-io', repository_id=config['repository_id'],
                             authority_ref=authority.ref, task_id=task_id, ref=self.ref,
                             stream='run' if run else 'inbox')
        self.archive_ref = 'refs/tags/task-io/' + task_id + '/' + run['run_id'] if run else None
        if run:
            self.manifest.update(run_id=run['run_id'], generation=run['generation'])

    def validate_config(self, config):
        if config != self.manifest:
            raise TaskError('Task I/O branch belongs to another repository, task or run')

    def ensure(self):
        def apply(snapshot):
            existing = snapshot.json(CONTROL + '/config.json')
            if existing:
                self.validate_config(existing)
                return {}, self.manifest
            if snapshot.commit:
                raise TaskError('Ref already exists without a task I/O manifest')
            changes = {CONTROL + '/config.json': encode(self.manifest)}
            if self.manifest['stream'] == 'run':
                # A ref visible to observe() must already carry its identity,
                # even if emit/result happens before the first heartbeat.
                changes['state.json'] = encode(runtime.heartbeat_state(self.run, initial=True))
            return changes, self.manifest
        self.transact('io-initialize', {'kind': 'io-initialize', 'manifest': self.manifest}, apply, initialize=True)

    def optional(self):
        if self.run:
            authority = self.authority.snapshot()
            authority, record_path = runtime.run_metadata_snapshot(
                authority, self.run['task_id'], self.run['run_id'], 'cleanup.json')
            path = record_path.rsplit('/', 1)[0]
            cleanup = authority.json(record_path, {})
            saved = cleanup.get('io_manifest')
            if saved is not None:
                if not isinstance(saved, dict) or (cleanup.get('heartbeat_head') and not saved):
                    raise TaskError('Saved run I/O records lack the stream manifest')
                snapshot = copy.copy(authority)
                prefix = path + '/io/'
                snapshot.entries = {p[len(prefix):]: e for p, e in authority.entries.items() if p.startswith(prefix)}
                if snapshot.entries != {p: tuple(e) for p, e in saved.items()}:
                    raise TaskError('Saved run I/O records differ from the finish manifest')
                if saved:
                    self.validate_config(snapshot.json(CONTROL + '/config.json'))
                return snapshot
        commit = self.fetch(missing=True)
        if commit is None and self.archive_ref:
            commit = self.fetch(self.archive_ref, missing=True)
        snapshot = Snapshot(self, commit)
        if snapshot.commit:
            self.validate_config(snapshot.json(CONTROL + '/config.json'))
        return snapshot


def finish_manifest(store, run, head):
    """Keep final business records as blobs on main, without stream ancestry/logs."""
    if head is None:
        return {}
    stream = IOStore(store, run['task_id'], run)
    snapshot = Snapshot(store, head)
    stream.validate_config(snapshot.json(CONTROL + '/config.json'))
    entries = {p: e for p, e in snapshot.entries.items()
               if p in (CONTROL + '/config.json', 'state.json') or
               ((p.startswith('events/') or p.startswith('results/')) and p.endswith('.json'))}
    if any(mode not in ('100644', '100755') or kind != 'blob'
           for mode, kind, _ in entries.values()):
        raise TaskError('Metadata must be a regular file')
    return entries


def bounded(value):
    if not isinstance(value, dict) or len(encode(value)) > MAX_BYTES:
        raise TaskError('Task I/O payload must be an object of at most 256 KiB')
    return copy.deepcopy(value)


def send(store, operation_id, task_id, message_id, kind, payload):
    runtime.load_task(store.snapshot(), task_id)  # A paused task can still receive requests.
    identity(message_id, 'message ID'); identity(kind, 'message kind')
    message = dict(schema_version=1, id=message_id, kind=kind, payload=bounded(payload))
    stream = IOStore(store, task_id)
    stream.ensure()
    path = 'messages/' + message_id + '.json'
    def apply(snapshot):
        prior = snapshot.json(path)
        if prior and prior != message:
            raise MessageConflict('Message ID already used with different content')
        return ({path: encode(message)} if not prior else {}), message
    stream.transact(operation_id, dict(kind='io-send', message=message), apply)
    snapshot = stream.snapshot()
    return dict(message_id=message_id, task_id=task_id, ref=stream.ref, commit=snapshot.commit,
                message_sha256=digest(message), accepted=True, delivery_status='pending')


def message(store, task_id, message_id):
    identity(message_id, 'message ID')
    stream = IOStore(store, task_id)
    snapshot = stream.optional()
    value = snapshot.json('messages/' + message_id + '.json')
    if not value or value.get('id') != message_id:
        raise TaskError('Task I/O message not found')
    return value, dict(ref=stream.ref, commit=snapshot.commit, message_sha256=digest(value))


def receipt_path(task_id, message_id, snapshot=None):
    return runtime.root(task_id, snapshot) + '/io/receipts/' + identity(message_id) + '.json'


def pending(store, task_id):
    authority = store.snapshot()
    runtime.load_task(authority, task_id)
    stream = IOStore(store, task_id)
    snapshot = stream.optional()
    rows = []
    for path in sorted(snapshot.entries):
        if path.startswith('messages/') and path.endswith('.json'):
            value = snapshot.json(path)
            if path != 'messages/' + identity(value['id']) + '.json':
                raise TaskError('Malformed task I/O message path')
            if not authority.json(receipt_path(task_id, value['id'], authority)):
                rows.append(value)
    return dict(task_id=task_id, ref=stream.ref, commit=snapshot.commit, messages=rows)


def _result(store, operation_id, task_id, run_id, message_id, value):
    run = runtime.check(store, task_id, run_id)
    original, source = message(store, task_id, message_id)
    value = bounded(value)
    if value.get('status') not in ('succeeded', 'failed', 'superseded'):
        raise TaskError('Only a reconciled terminal result can be acknowledged')
    record = dict(schema_version=1, message_id=message_id, message_sha256=source['message_sha256'],
                  task_id=task_id, run_id=run_id, generation=run['generation'], result=value)
    stream = IOStore(store, task_id, run)
    stream.ensure()
    path = 'results/' + message_id + '.json'
    def apply(snapshot):
        runtime.check(store, task_id, run_id)
        prior = snapshot.json(path)
        if prior and prior != record:
            raise TaskError('A run result is immutable; reconcile its original operation')
        return ({path: encode(record)} if not prior else {}), record
    stream.transact(operation_id, dict(kind='io-result', record=record), apply)
    runtime.check(store, task_id, run_id)
    saved = stream.snapshot()
    return dict(ref=stream.ref, commit=saved.commit, path=path, result_sha256=digest(record))


def emit(store, operation_id, task_id, run_id, event_id, kind, payload):
    """Immutable run events include external-operation intents and their reconciled outcomes."""
    run = runtime.check(store, task_id, run_id)
    event = dict(schema_version=1, id=identity(event_id), kind=identity(kind), payload=bounded(payload),
                 task_id=task_id, run_id=run_id, generation=run['generation'])
    stream = IOStore(store, task_id, run)
    stream.ensure()
    path = 'events/' + event_id + '.json'
    def apply(snapshot):
        runtime.check(store, task_id, run_id)
        prior = snapshot.json(path)
        if prior and prior != event:
            raise TaskError('Run event ID already used with different content')
        return ({path: encode(event)} if not prior else {}), event
    value = stream.transact(operation_id, dict(kind='io-emit', event=event), apply)
    runtime.check(store, task_id, run_id)
    return value


def _receipt(store, task_id, run_id, message_id, run):
    original, source = message(store, task_id, message_id)
    stream = IOStore(store, task_id, run)
    saved = stream.snapshot()
    path = 'results/' + identity(message_id) + '.json'
    record = saved.json(path)
    if (not record or record['message_sha256'] != source['message_sha256'] or
            record['run_id'] != run_id or record['generation'] != run['generation'] or
            record['result'].get('status') not in ('succeeded', 'failed', 'superseded')):
        raise TaskError('Acknowledge requires this run\'s saved result for the exact message')
    return dict(schema_version=1, message_id=message_id, message_sha256=source['message_sha256'],
                   task_id=task_id, run_id=run_id, generation=run['generation'],
                   result=record['result'], result_ref=stream.ref, result_commit=saved.commit,
                   result_path=path, result_sha256=digest(record))


def complete(store, operation_id, task_id, run_id, message_id, value):
    """Save a candidate and accept it with one caller-owned retry identity."""
    identity(operation_id, 'operation ID')
    try:
        return _complete(store, operation_id, task_id, run_id, message_id, value)
    except TaskError as error:
        # Phase receipts are implementation details. Retry instructions must
        # name the public request, while structured transport errors stay intact.
        if type(error) is not TaskError:
            raise
        message = str(error)
        for internal in (reservation_id(operation_id),
                         digest(dict(kind='io-complete-candidate', operation_id=operation_id)),
                         'io-initialize'):
            if message.endswith('operation ' + internal):
                raise TaskError(message[:-len(internal)] + operation_id) from None
        raise


def _complete(store, operation_id, task_id, run_id, message_id, value):
    value = bounded(value)
    if value.get('status') not in ('succeeded', 'failed', 'superseded'):
        raise TaskError('Only a reconciled terminal result can be acknowledged')
    request = dict(kind='run-io-complete', task_id=task_id, run_id=run_id,
                   message_id=message_id, result=value)
    if not store.snapshot().metadata(CONTROL + '/operations/' + operation_id + '.json'):
        fingerprint = digest(request)
        def reserve(snapshot):
            prior = snapshot.metadata(CONTROL + '/operations/' + operation_id + '.json')
            if prior and prior['request_hash'] != fingerprint:
                raise TaskError('Operation ID was already used for another request')
            runtime.load_run(snapshot, task_id, run_id, live=True)
            return {}, dict(reserved_operation=operation_id, request_hash=fingerprint)
        # A run-local candidate receipt cannot bind an ID across other runs or
        # clones. Reuse authority receipts/history for this preparatory phase.
        store.transact(reservation_id(operation_id), dict(kind='run-io-complete-reserve',
            task_id=task_id, run_id=run_id, reserved_operation=operation_id,
            request_hash=fingerprint), reserve)
    # Domain separation keeps internal stream IDs independent of caller IDs.
    candidate_id = digest(dict(kind='io-complete-candidate', operation_id=operation_id))
    def apply(snapshot):
        run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
        _result(store, candidate_id, task_id, run_id, message_id, value)
        receipt = _receipt(store, task_id, run_id, message_id, run)
        if receipt['result'] != value:
            raise TaskError('Saved result differs from the completion request')
        location = receipt_path(task_id, message_id, snapshot)
        prior = snapshot.json(location)
        if prior:
            if prior['message_sha256'] != receipt['message_sha256'] or prior['result'] != receipt['result']:
                raise TaskError('Message already completed with a different result')
            return {}, prior
        return {location: encode(receipt)}, receipt
    # Store replays the original receipt before apply, including after run
    # retirement. Never require live ownership just to read an accepted result.
    return store.transact(operation_id, request, apply)


def status(store, task_id, message_id):
    original, source = message(store, task_id, message_id)
    snapshot = store.snapshot()
    receipt = snapshot.json(receipt_path(task_id, message_id, snapshot))
    if receipt and receipt['message_sha256'] != source['message_sha256']:
        raise TaskError('Accepted result does not match the current message')
    return dict(message=original, receipt=receipt, status=receipt['result']['status'] if receipt else 'pending')


def heartbeat(store, run, progress, operation_id):
    stream = IOStore(store, run['task_id'], run)
    stream.ensure()
    def apply(snapshot):
        runtime.check(store, run['task_id'], run['run_id'])
        prior = snapshot.json('state.json', {})
        state = runtime.heartbeat_state(run, prior, progress)
        return {'state.json': encode(state)}, state
    value = stream.transact(operation_id or uuid.uuid4().hex, dict(kind='io-heartbeat', progress=progress), apply)
    runtime.check(store, run['task_id'], run['run_id'])
    return value


def wake(store, operation_id, task_id, runner_id, run_id=None, *, force=False):
    """Admission only: the caller executes task.md when action=start is returned."""
    task, active = runtime.load_task(store.snapshot(), task_id)
    if task['execution']['max_runners'] != 1:
        raise TaskError('Inbox consumers require max_runners=1; run streams support parallel producers')
    from . import signals
    signals.startup_sync(store, task_id)
    if not force and not pending(store, task_id)['messages']:
        return dict(action='idle', task_id=task_id)
    if task['status'] != 'open':
        return dict(action='paused', task_id=task_id, delivery_status='pending')
    try:
        run = runtime.start(store, operation_id, task_id, runner_id, run_id=run_id)
    except TaskError as error:
        if str(error) != 'Task runner limit reached':
            raise
        return dict(action='busy', task_id=task_id, delivery_status='pending')
    # A historical successful start receipt grants no current execution authority.
    runtime.check(store, task_id, run['run_id'])
    return dict(action='start', run=run, task_id=task_id)
