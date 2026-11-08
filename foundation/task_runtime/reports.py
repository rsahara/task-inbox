"""Durable retention notices and read-only inventories of leftover run resources."""
import html
import json
from pathlib import Path

from .store import CONTROL, GitFailure, TaskError, atomic_json, encode, git, identity, now


def belongs(ref, run):
    return ref == run['heartbeat_ref'] or ref.startswith(run['work_ref'].rsplit('/', 1)[0] + '/')


def event_record(run, operation_id, reason, observation=None):
    record = {key: run[key] for key in ('repository_id', 'authority_ref', 'task_id', 'run_id',
                                       'generation', 'machine_id', 'status', 'source_head')}
    record.update(schema_version=1, recorded_at=now(), operation_id=operation_id, reason=reason,
                  recovery_kind=run.get('recovery_kind'),
                  refs=[{'ref': run['work_ref'], 'last_accepted_commit': run['source_head']},
                        {'ref': run['heartbeat_ref'],
                         'observed_commit': observation.get('observed_tip') if observation else None}])
    if run['execution']['recovery_mode'] == 'generation_timeout':
        record['renewal_ref_prefix'] = run['work_ref'].rsplit('/', 1)[0] + '/renewals/'
    return record


def cell(value):
    return html.escape(str(value), quote=False).replace('`', '&#96;').replace('|', '\\|').replace('\r', ' ').replace('\n', ' ')


def retained_changes(run, path, operation_id, reason, observation=None):
    """Persist one event record; render Markdown at inspection/export time."""
    return {path + '/retention-report.json': encode(event_record(run, operation_id, reason, observation))}


def note_cleanup(store, snapshot, task_id, run_id, result, remote):
    """Best-effort clone-local evidence; never make a confirmed operation fail."""
    from . import runtime
    try:
        run = runtime.load_run(snapshot, task_id, run_id)[2]
        path = store.local / 'reports' / identity(task_id) / (identity(run_id) + '.json')
        previous = json.loads(path.read_text()) if path.exists() else None
        same_run = isinstance(previous, dict) and all(previous.get(key) == run[key] for key in
                   ('repository_id', 'task_id', 'run_id', 'generation'))
        if result.get('status') in ('done', 'retired', 'absent'):
            # Ref/metadata collection in an observing clone cannot confirm that
            # the execution clone's workspace was collected.
            if not result.get('local_workspace_collected'):
                return
            if previous is not None and not same_run:
                return
            if previous is None:
                previous = event_record(run, None, 'local_workspace_collected')
            previous.update(resolved_at=now(), local_workspace_collected=True)
            atomic_json(path, previous)
            return
        record = event_record(run, None, result.get('reason') or result.get('local') or 'cleanup_deferred')
        if same_run and previous.get('local_workspace_collected') is True and isinstance(previous.get('resolved_at'), str):
            record.update(local_workspace_collected=True, resolved_at=previous['resolved_at'])
        record['observed_refs'] = [{'ref': ref, 'head': head} for ref, head in sorted(remote.items()) if belongs(ref, run)]
        atomic_json(path, record)
    except (TaskError, GitFailure, OSError, ValueError, KeyError, TypeError):
        pass


def cleanup_reason(error):
    if isinstance(error, GitFailure):
        return 'git_operation_unconfirmed'
    if isinstance(error, OSError):
        return 'local_filesystem_unavailable'
    # Only known runtime messages become public reasons; never copy raw errors.
    for message, reason in (
            ('Workspace contains unpreserved files', 'workspace_has_unpreserved_files'),
            ('Workspace contains unpreserved commits', 'workspace_has_unpreserved_commits'),
            ('Worker termination is unconfirmed', 'worker_termination_unconfirmed'),
            ('Local operation is busy', 'local_operation_busy'),
            ('Run ref changed after finish', 'ref_changed_after_finish'),
            ('Ref deletion is unconfirmed', 'ref_deletion_unconfirmed'),
            ('Local tracking ref changed', 'tracking_ref_changed'),
            ('Run I/O records are not saved', 'io_records_not_preserved')):
        if str(error).startswith(message):
            return reason
    return 'cleanup_blocked_or_unconfirmed'


def local_inventory(store, run, branches, tracking, provenance_fenced=False):
    from . import maintenance, runtime
    state_path = runtime.local_state(store, run['task_id'], run['run_id'])
    value = {'visibility': 'not_recorded_in_this_clone', 'files': [],
             'unobserved_prior_workspaces': (not provenance_fenced or
                 run.get('workspace_provenance_protocol') != 1 or
                 run.get('workspace_history_unobserved') is not False),
             'branches': [{'ref': ref, 'head': head} for ref, head in sorted(branches.items()) if belongs(ref, run)],
             'tracking_refs': [{'ref': ref, 'source_ref': original, 'head': head}
                               for ref, (original, head) in sorted(tracking.items()) if belongs(original, run)]}
    try:
        value['files'] = [str(path) for path in maintenance.local_files(state_path) if path.exists()]
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if not isinstance(state, dict) or any(state.get(key) != run[key] for key in ('repository_id', 'task_id', 'run_id', 'generation')):
                raise ValueError('identity')
            work = Path(state['path'])
            if not work.is_absolute() or state['mode'] not in ('worktree', 'workspace'):
                raise ValueError('workspace')
            value.update(visibility='recorded', workspace_path=str(work), workspace_exists=work.exists(), mode=state['mode'])
        note_path = store.local / 'reports' / run['task_id'] / (run['run_id'] + '.json')
        if note_path.exists():
            note = json.loads(note_path.read_text())
            if not isinstance(note, dict) or any(note.get(key) != run[key] for key in ('repository_id', 'task_id', 'run_id', 'generation')):
                raise ValueError('identity')
            if not isinstance(note.get('reason'), str) or not note['reason']:
                raise ValueError('reason')
            value.update(last_cleanup_notice=note, notice_path=str(note_path))
            if (not state_path.exists() and note.get('local_workspace_collected') is True and
                    isinstance(note.get('resolved_at'), str)):
                value['visibility'] = 'collected_in_this_clone'
    except (OSError, ValueError, KeyError, TypeError):
        value.update(visibility='unknown', error='invalid_local_state')
    return value


def inventory(store, task_id=None, run_id=None):
    """Inspect remote and current-clone leftovers without writes to authority."""
    from . import layout, maintenance, runtime, task_io
    if run_id is not None and task_id is None:
        raise TaskError('A run filter requires --task')
    snapshot = store.snapshot()
    config = snapshot.json(CONTROL + '/config.json')
    tasks = layout.tasks(snapshot)
    if task_id is not None:
        runtime.load_task(snapshot, task_id)
        tasks = {task_id: runtime.root(task_id, snapshot)}
    if run_id is not None:
        runtime.load_run(snapshot, task_id, run_id)
    runtime_prefix = 'refs/heads/' + store.ref_prefix + '/'
    io_prefix = 'refs/heads/' + task_io.prefix(config) + '/'
    prefixes = ([runtime_prefix + task_id + '/', io_prefix + task_id + '/runs/']
                if task_id is not None else [runtime_prefix, io_prefix])
    errors = []
    try:
        remote = store.list_refs(prefixes)
        # Inbox branches are permanent intake, not orphaned per-run resources.
        remote = {ref: head for ref, head in remote.items() if ref.startswith(runtime_prefix) or
                  (ref.startswith(io_prefix) and len(ref[len(io_prefix):].split('/')) >= 3 and
                   ref[len(io_prefix):].split('/')[1] == 'runs')}
    except (TaskError, GitFailure, OSError):
        remote = {}
        errors.append({'scope': 'remote_refs', 'reason': 'ref_inventory_unavailable'})
    try:
        branches = dict(line.split() for line in git(store.repo, 'for-each-ref',
                        '--format=%(refname) %(objectname)', 'refs/heads/' + store.ref_prefix + '/').splitlines())
        tracking = maintenance.tracking_refs(store, 'refs/heads/' + store.ref_prefix + '/')
        tracking.update(maintenance.tracking_refs(store, 'refs/heads/' + task_io.prefix(config) + '/'))
    except (TaskError, GitFailure, OSError, ValueError):
        branches, tracking = {}, {}
        errors.append({'scope': 'local_refs', 'reason': 'ref_inventory_unavailable'})
    rows, attributed = [], set()
    for task, location in sorted(tasks.items()):
        prefix = location + '/runs/'
        for run in runtime.list_runs(snapshot, task):
            current_id = run['run_id']
            path = prefix + current_id + '/run.json'
            refs = [{'ref': ref, 'head': head} for ref, head in sorted(remote.items()) if belongs(ref, run)]
            attributed.update(ref['ref'] for ref in refs)
            if run['status'] == 'running' or (run_id is not None and current_id != run_id):
                continue
            local = local_inventory(store, run, branches, tracking,
                config['schema_version'] >= 7 and config.get('workspace_provenance_protocol') == 1)
            if local['visibility'] == 'unknown':
                errors.append({'scope': path, 'reason': 'invalid_local_state'})
            cleanup = runtime.run_metadata(snapshot, task, current_id, 'cleanup.json', {})
            if (not refs and not local['files'] and not local['branches'] and not local['tracking_refs'] and
                    local['visibility'] == 'collected_in_this_clone' and cleanup.get('status') not in ('retained', 'pending') and
                    not local['unobserved_prior_workspaces'] and
                    not any(error['scope'] in ('remote_refs', 'local_refs') for error in errors)):
                continue
            event_path = prefix + current_id + '/retention-report.json'
            expected = {run['work_ref']: cleanup.get('work_head'), run['heartbeat_ref']: cleanup.get('heartbeat_head')}
            changed = cleanup.get('status') == 'eligible' and any(ref['ref'] in expected and
                      ref['head'] != expected[ref['ref']] for ref in refs)
            rows.append(dict(task_id=task, run_id=current_id, generation=run['generation'],
                             status=run['status'], machine_id=run['machine_id'], run_path=path,
                             last_accepted_commit=run['source_head'],
                             reason='ref_changed_after_finish' if changed else cleanup.get('reason') or
                                    ('workspace_history_unobserved' if local['unobserved_prior_workspaces'] else 'cleanup_pending'),
                             cleanup_status=cleanup.get('status'), remote_refs=refs, local=local,
                             retention_report=event_path if event_path in snapshot.entries else None))
    return dict(schema_version=1, repository_id=config['repository_id'], authority_ref=store.ref,
                authority_commit=snapshot.commit, generated_at=now(), runs=rows,
                unattributed_refs=[] if run_id else [{'ref': ref, 'head': head} for ref, head in sorted(remote.items()) if ref not in attributed],
                errors=errors, needs_attention=bool(errors))


def markdown(report):
    lines = ['# Retained runtime resources', '', 'Authority commit: `%s`' % cell(report['authority_commit']),
             '', 'Local resources describe only the clone generating this report.', '']
    if report['errors']:
        lines += ['Inventory is incomplete:'] + ['- `%s`: %s' % (cell(e['scope']), cell(e['reason'])) for e in report['errors']] + ['']
    for run in report['runs']:
        lines += ['## %s / %s' % (cell(run['task_id']), cell(run['run_id'])), '',
                  'Status: `%s`; generation: `%s`; machine: `%s`' % (cell(run['status']), run['generation'], cell(run['machine_id'])),
                  '', 'Reason: %s' % cell(run['reason']), '',
                  'Run record: `%s`' % cell(run['run_path']), '',
                  'Last accepted source commit: `%s`' % cell(run['last_accepted_commit'])]
        if run['retention_report']:
            lines += ['', 'Event report: `%s`' % cell(run['retention_report'])]
        lines += ['', '| Remote ref | Observed commit |', '| --- | --- |']
        for ref in run['remote_refs']:
            lines.append('| `%s` | `%s` |' % (cell(ref['ref']), ref['head']))
        local = run['local']
        lines += ['', 'Local visibility: `%s`' % cell(local['visibility'])]
        if local['unobserved_prior_workspaces']:
            lines += ['', 'Prior workspace history is unobserved; collection in this clone does not confirm other clones.']
        if local.get('workspace_path'):
            lines += ['', 'Workspace: `%s` (exists: %s)' % (cell(local['workspace_path']), local['workspace_exists'])]
        for item in local['branches'] + local['tracking_refs']:
            lines.append('- Local ref: `%s` at `%s`' % (cell(item['ref']), item['head']))
        for path in local['files']:
            lines.append('- Local record: `%s`' % cell(path))
        if local.get('notice_path'):
            lines += ['', 'Last cleanup notice: `%s`' % cell(local['notice_path']),
                      '', 'Cleanup reason: `%s`' % cell(local['last_cleanup_notice']['reason'])]
        lines.append('')
    if report['unattributed_refs']:
        lines += ['## Refs without a matching run record', '']
        lines += ['- `%s` at `%s`' % (cell(ref['ref']), ref['head']) for ref in report['unattributed_refs']]
    if not report['runs'] and not report['unattributed_refs'] and not report['errors']:
        lines += ['No retained run resources found in this scope.', '']
    lines += ['', 'This report does not authorize deletion or change execution ownership.', '']
    return '\n'.join(lines)
