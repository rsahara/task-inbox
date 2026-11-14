"""Caller-owned execution entrypoint; tasks describe work, never their executor."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

from . import completion, interfaces, layout, leases, lifecycle, maintenance, runtime, signals, task_io
from .store import CONTROL, TaskError, atomic_json, digest, encode, git_support, identity

PROTOCOL = 'task-inbox-execution/v1'


def installed(store, program, label):
    path = Path(program or '')
    if (not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK)
            or path.resolve().is_relative_to(store.repo)):
        raise TaskError(label + ' must be an absolute path to a trusted installed executable outside the task repository')
    return str(path.resolve())


def selected_tasks(task_ids):
    if not task_ids or len(task_ids) != len(set(task_ids)):
        raise TaskError('Supply a nonempty ordered allowlist of unique task IDs')
    return [identity(t, 'task ID') for t in task_ids]


def inputs(store, snapshot, task_id, interface):
    messages = task_io.pending(store, task_id, authority=snapshot)['messages']
    reads = interface['signals']['reads']
    catalogs = {}

    def catalog(owner):
        if owner not in catalogs:
            catalogs[owner] = signals.catalog(snapshot, owner)
        return catalogs[owner]

    current = []
    if reads and signals.enabled(snapshot):
        for owner in layout.tasks(snapshot):
            if not any(s.get('task_id', owner) == owner for s in reads):
                continue
            for record in catalog(owner).values():
                if any(interfaces.matches(record['value'], selector) for selector in reads):
                    current.append(dict(record, commit=snapshot.commit))
    eligible, ignored = [], []
    for message in messages:
        accepted = message['kind'] in interface['inbox']
        if accepted and message['kind'] == signals.KIND and reads:
            notice = message.get('payload', {})
            owner, sid, revision = notice.get('task_id'), notice.get('signal_id'), notice.get('revision')
            if (not isinstance(owner, str) or not isinstance(sid, str) or type(revision) is not int
                    or revision < 1 or message['id'] != signals.message_id(owner, sid, revision)):
                raise TaskError('Invalid signal notification in declared inbox')
            record = catalog(owner).get(sid)
            if not record or record['value']['revision'] < revision:
                raise TaskError('Signal notification is ahead of the fixed authority snapshot; retry')
            accepted = any(interfaces.matches(record['value'], selector) for selector in reads)
        (eligible if accepted else ignored).append(message['id'])
    return dict(message_ids=eligible, ignored_message_ids=ignored, signals=current)


def condition(store, program, snapshot, task, values):
    request = dict(protocol=PROTOCOL, task_id=task['id'], commit=snapshot.commit,
                   task=task, inputs=values)
    request['request_id'] = digest(request)
    try:
        response = git_support.run_process([program], cwd=store.repo, input=encode(request), timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        raise TaskError('Condition adapter failed; no run was admitted') from None
    if response.returncode or len(response.stdout) > 1024 * 1024:
        raise TaskError('Condition adapter failed; no run was admitted')
    try:
        value = json.loads(response.stdout)
    except (ValueError, UnicodeError):
        raise TaskError('Invalid condition adapter response') from None
    if (not isinstance(value, dict) or value.get('protocol') != PROTOCOL
            or value.get('request_id') != request['request_id'] or type(value.get('ready')) is not bool
            or not isinstance(value.get('reason'), str) or not 0 < len(value['reason']) <= 256):
        raise TaskError('Condition adapter response must bind readiness to this request')
    return dict(ready=value['ready'], reason=value['reason'])


def plan(store, task_ids, *, condition_adapter=None, force=False, snapshot=None, used=()):
    task_ids = selected_tasks(task_ids)
    program = installed(store, condition_adapter, 'Condition adapter') if condition_adapter else None
    snapshot = snapshot or store.snapshot()
    decisions = []
    for task_id in task_ids:
        task, active = runtime.load_task(snapshot, task_id)
        interface = interfaces.declaration(task)
        if interface['readiness'] == 'inbox' and task['execution']['max_runners'] != 1:
            raise TaskError('Inbox execution requires max_runners=1')
        values = inputs(store, snapshot, task_id, interface)
        ready, reason = False, 'manual_only'
        if task['status'] != 'open' or task.get('archived_at'):
            reason = 'paused'
        elif sum(bool(s.get('run_id')) for s in active['slots'].values()) >= task['execution']['max_runners']:
            reason = 'busy'
        elif force and task_id == task_ids[0] and task_id not in used:
            ready, reason = True, 'explicit_request'
        elif interface['readiness'] == 'inbox':
            ready = bool(values['message_ids'])
            reason = 'inputs_ready' if ready else 'inbox_empty'
        elif task_id in used:
            reason = 'already_executed'
        elif interface['readiness'] == 'condition':
            if program:
                result = condition(store, program, snapshot, task, values)
                ready, reason = result['ready'], result['reason']
            else:
                reason = 'condition_adapter_required'
        # Plans expose fixed references. The trusted condition adapter also sees
        # logical values; workers must read and verify data before side effects.
        refs = [dict(task_id=r['value']['task_id'], id=r['value']['id'],
                     revision=r['value']['revision'], type=r['value']['type'], status=r['value']['status'],
                     path=r['path'], sha256=r['sha256'], commit=r['commit']) for r in values['signals']]
        decisions.append(dict(task_id=task_id, ready=ready, reason=reason,
                              message_ids=values['message_ids'], ignored_message_ids=values['ignored_message_ids'],
                              signals=refs, interface=interface))
    return dict(protocol=PROTOCOL, commit=snapshot.commit, decisions=decisions)


def operation(execution_id, phase, index=None):
    return 'execute-' + digest([execution_id, phase, index])[:48]


def select(store, execution_id, index, task_ids, *, condition_adapter, force, used):
    request = dict(kind='execution-select', execution_id=execution_id, index=index)
    selection_id = operation(execution_id, 'select', index)
    snapshot = store.snapshot()
    # Match normal run-start's configured lease recovery before a new selection.
    # Historical/ambiguous selection replays must not revoke a replacement run.
    if (not snapshot.metadata(CONTROL + '/operations/' + selection_id + '.json')
            and not (store.local / 'operations' / (selection_id + '.json')).exists()):
        for task_id in task_ids:
            task = runtime.load_task(snapshot, task_id)[0]
            if task['execution']['recovery_mode'] in leases.MODES:
                lifecycle.reap(store, task_id, apply=True, observer_profile='stateless')

    def apply(snapshot):
        decision = plan(store, task_ids, condition_adapter=condition_adapter,
                        force=force, snapshot=snapshot, used=used)
        target = next((d for d in decision['decisions'] if d['ready']), None)
        return {}, dict(plan=decision, task_id=target['task_id'] if target else None,
                        run_id='execute-' + digest([execution_id, index])[:40])

    return store.transact(selection_id, request, apply)


def worker_result(path, run):
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 262144:
        raise TaskError('Executor must write a bounded result file before returning success')
    value = json.loads(path.read_text())
    if (not isinstance(value, dict) or value.get('protocol') != PROTOCOL
            or type(value.get('generation')) is not int
            or any(value.get(k) != run[k] for k in ('task_id', 'run_id', 'generation'))
            or value.get('outcome') not in ('succeeded', 'failed', 'cancelled')
            or not isinstance(value.get('summary'), str)):
        raise TaskError('Executor result must bind a terminal outcome to this run generation')
    return value


def execute(store, execution_id, task_ids, runner, executor, *, condition_adapter=None,
            continue_tasks=False, max_tasks=None, force=False, completion_contracts=None):
    identity(execution_id, 'execution ID')
    task_ids = selected_tasks(task_ids)
    identity(runner, 'runner ID')
    program = installed(store, executor, 'Executor')
    checker = installed(store, condition_adapter, 'Condition adapter') if condition_adapter else None
    limit = runtime.positive(max_tasks if max_tasks is not None else (16 if continue_tasks else 1), 'max_tasks', 100)
    if not continue_tasks and limit != 1:
        raise TaskError('More than one task requires --continue')
    contracts = completion_contracts if completion_contracts is not None else {}
    if not isinstance(contracts, dict) or not set(contracts) <= set(task_ids):
        raise TaskError('Completion contracts must map allowed task IDs to contracts')
    contracts = {task_id: completion.contract(value) for task_id, value in contracts.items()}
    request = dict(kind='execution-begin', tasks=task_ids, runner=runner, continue_tasks=continue_tasks,
                   max_tasks=limit, force=force, completion_contracts=contracts,
                   executor_sha256=hashlib.sha256(Path(program).read_bytes()).hexdigest(),
                   condition_sha256=hashlib.sha256(Path(checker).read_bytes()).hexdigest() if checker else None)
    # The shared operation receipt is the caller intent. No separate application
    # lock, mutable routing queue or host-specific launch state is needed.
    store.transact(operation(execution_id, 'begin'), request, lambda snapshot: ({}, {'execution_id': execution_id}))
    steps, used = [], []
    reason, attention = 'limit_reached', False
    with store.lock(store.local / ('execute-' + digest(execution_id) + '.lock')):
        for index in range(limit):
            signals.sync(store)
            choice = select(store, execution_id, index, task_ids, condition_adapter=checker,
                            force=force and index == 0, used=used)
            task_id, run_id = choice['task_id'], choice['run_id']
            if task_id is None:
                reason = 'no_ready_task'
                break
            current = runtime.run_record(store.snapshot(), task_id, run_id)
            if current is None:
                def recheck(snapshot):
                    decision = plan(store, [task_id], condition_adapter=checker,
                                    force=force and index == 0 and task_id == task_ids[0],
                                    used=used, snapshot=snapshot)['decisions'][0]
                    if not decision['ready']:
                        raise TaskError('Execution admission changed: ' + decision['reason'])

                try:
                    current = runtime.start(store, operation(execution_id, 'start', index), task_id, runner,
                                            run_id=run_id, admission_check=recheck,
                                            admission_key=operation(execution_id, 'select', index),
                                            completion_contract=contracts.get(task_id))
                except TaskError as error:
                    if str(error).startswith('Execution admission changed: '):
                        reason = str(error).split(': ', 1)[1]
                        break
                    if str(error) != 'Task runner limit reached':
                        raise
                    reason = 'busy'
                    break
            if current['status'] != 'running':
                if current['status'] in ('succeeded', 'failed', 'cancelled'):
                    signals.require_delivery(signals.sync(store, task_id=task_id))
                    maintenance.maintain(store, task_id, run_id)
                steps.append(dict(task_id=task_id, run_id=run_id, outcome=current['status']))
                used.append(task_id)
                if current['status'] != 'succeeded':
                    reason, attention = 'task_not_succeeded', True
                    break
                continue
            runtime.check(store, task_id, run_id)
            directory = store.local / 'executions' / digest(execution_id) / str(index)
            context_path, result_path = directory / 'context.json', directory / 'result.json'
            marker = runtime.local_state(store, task_id, run_id).with_suffix('.execution.json')
            execution = json.loads(marker.read_text()) if marker.is_file() else None
            if current.get('execution_claim') and not (execution and execution['phase'] == 'exited'):
                reason, attention = 'execution_outcome_unknown', True
                steps.append(dict(task_id=task_id, run_id=run_id, outcome='unknown'))
                break
            if not execution:
                workspace = directory / 'work'
                runtime.prepare(store, task_id, run_id, workspace)
                context = dict(protocol=PROTOCOL, repository=str(store.repo), remote=store.remote,
                               branch=store.ref.removeprefix('refs/heads/'), workspace=str(workspace),
                               transport_adapter=getattr(store.transport, 'program', None),
                               result_path=str(result_path), run=current,
                               task_path=runtime.root(task_id, current), input_plan=choice['plan'])
                atomic_json(context_path, context)
                execution = lifecycle.execute(store, task_id, run_id, [program, str(context_path)])
            if execution['phase'] != 'exited' or execution.get('exit_code') != 0:
                reason, attention = 'executor_failed_or_interrupted', True
                steps.append(dict(task_id=task_id, run_id=run_id, outcome='unknown'))
                break
            value = worker_result(result_path, current)
            runtime.publish(store, operation(execution_id, 'finish', index), task_id, run_id,
                            outcome=value['outcome'], summary=value['summary'],
                            completion_record=value.get('completion'))
            saved = runtime.load_run(store.snapshot(), task_id, run_id)[2]
            if saved['status'] != value['outcome']:
                raise TaskError('Terminal run readback failed; retain the execution ID')
            steps.append(dict(task_id=task_id, run_id=run_id, outcome=saved['status']))
            used.append(task_id)
            if saved['status'] != 'succeeded':
                reason, attention = 'task_not_succeeded', True
                break
        # Recheck after releasing the last task's slot. Arrivals in the close
        # gap remain durable and are visible to this caller or its next trigger.
        delivery = signals.sync(store)
        remaining = plan(store, task_ids, condition_adapter=checker, used=used)
        return dict(protocol=PROTOCOL, execution_id=execution_id, steps=steps, reason=reason,
                    needs_attention=attention or delivery['needs_attention'], notifications=delivery,
                    next=remaining, pending_tasks=[d['task_id'] for d in remaining['decisions'] if d['ready']])
