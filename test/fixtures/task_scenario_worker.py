"""Test-only callers for end-to-end scenarios over a local bare Git remote.

JSON lines on stdout/stdin provide barriers; no worker is a background service.
"""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'foundation'))
from task_runtime import runtime, task_io
from task_runtime.store import Store


def event(name, **value):
    print(json.dumps(dict(event=name, **value)), flush=True)


def barrier():
    if sys.stdin.readline() != 'continue\n':
        raise RuntimeError('Controller closed the barrier')


def prepare(store, task, run_id, path):
    runtime.prepare(store, task, run_id, path)
    runtime.heartbeat(store, task, run_id, progress='working')


def finish(store, task, run_id):
    return runtime.publish(store, 'finish-' + run_id, task, run_id, outcome='succeeded')


def checkpoint(store, args):
    run = runtime.start(store, 'start-' + args.run, 'task-a', args.run, run_id=args.run)
    prepare(store, 'task-a', args.run, args.work)
    output = args.work / f'tasks/task-a/runs/{args.run}/artifacts/checkpoint.txt'
    output.parent.mkdir(parents=True)
    output.write_text('durable checkpoint\n')
    runtime.publish(store, 'checkpoint-' + args.run, 'task-a', args.run)
    event('checkpoint', run=run)
    barrier()  # The controller kills this process after the durable push.
    raise RuntimeError('Checkpoint worker must be killed')


def forward(store, args):
    wake = task_io.wake(store, 'wake-' + args.run, 'task-a', args.run, args.run)
    if wake['action'] != 'start':
        raise RuntimeError('Forwarder did not acquire task-a')
    prepare(store, 'task-a', args.run, args.work)
    processed = []
    while True:
        messages = task_io.pending(store, 'task-a')['messages']
        if not messages:
            break
        # Deliberately refetch for each item, rather than drain a startup snapshot.
        message = messages[0]
        key = message['id']
        task_io.send(store, 'forward-' + key, 'task-b', key, 'forwarded', message['payload'])
        task_io.complete(store, 'result-' + key, 'task-a', args.run, key,
                       dict(status='succeeded', destination_task='task-b', message_id=key))
        processed.append(key)
        if len(processed) == 1:
            event('first-forwarded', message_id=key)
            barrier()  # An independent producer appends while task-a is running.
    finish(store, 'task-a', args.run)
    event('finished', processed=processed, pending=task_io.pending(store, 'task-a')['messages'])


def parent(store, args):
    runtime.start(store, 'start-' + args.run, 'task-a', args.run, run_id=args.run)
    prepare(store, 'task-a', args.run, args.work)
    request_id = 'request-' + args.run
    child_id = 'child-' + args.run
    task_io.send(store, 'send-' + request_id, 'task-b', request_id, 'multiply',
                 dict(parent_run=args.run, value=args.value))
    deadline = time.monotonic() + 30
    reported_busy = False
    while True:
        runtime.check(store, 'task-a', args.run)
        wake = task_io.wake(store, 'wake-' + child_id, 'task-b', child_id, child_id)
        if wake['action'] == 'start':
            break
        if wake['action'] != 'busy':
            raise RuntimeError('Unexpected child admission: ' + wake['action'])
        if not reported_busy:
            event('busy', parent_run=args.run)
            reported_busy = True
        if time.monotonic() >= deadline:
            raise TimeoutError('Timed out waiting for task-b')
        time.sleep(0.05)
    child_work = args.work.with_name(child_id)
    prepare(store, 'task-b', child_id, child_work)
    event('acquired', parent_run=args.run, run=wake['run'])
    barrier()  # Hold the serialized section until the test permits completion.
    # wake admits a run; the caller supplies execution and request correlation.
    request, _ = task_io.message(store, 'task-b', request_id)
    value = dict(status='succeeded', parent_run=request['payload']['parent_run'],
                 value=request['payload']['value'] * 7, child_run=child_id)
    task_io.complete(store, 'result-' + request_id, 'task-b', child_id, request_id, value)
    finish(store, 'task-b', child_id)
    # Read durable accepted results by request ID, after the child ref is removed.
    status = task_io.status(store, 'task-b', request_id)
    if status['status'] != 'succeeded':
        raise RuntimeError('Child result was not acknowledged')
    result = status['receipt']['result']
    output = args.work / f'tasks/task-a/runs/{args.run}/artifacts/result.json'
    output.parent.mkdir(parents=True)
    output.write_text(json.dumps(result))
    finish(store, 'task-a', args.run)
    event('finished', result=result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role', choices=('checkpoint', 'forward', 'parent'))
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--run', required=True)
    parser.add_argument('--value', type=int)
    args = parser.parse_args()
    globals()[args.role](Store(args.repo), args)
