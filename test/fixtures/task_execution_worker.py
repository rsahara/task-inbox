#!/usr/bin/env python3
"""Synthetic executor installed outside task data by integration tests."""
import json
from pathlib import Path
import sys

# The test installs this constant instead of depending on a working directory
# or a host-specific environment variable.
sys.path.insert(0, '__RUNTIME_FOUNDATION__')
from task_runtime import completion, runtime, signals, task_io
from task_runtime.store import Store, atomic_json, digest

context = json.loads(Path(sys.argv[1]).read_text())
run = context['run']
store = Store(context['repository'], context['remote'], context['branch'],
              transport_adapter=context.get('transport_adapter'))
task_id, run_id = run['task_id'], run['run_id']
data = Path(context['workspace']) / runtime.data_prefix(run, task_id)
data.mkdir(parents=True, exist_ok=True)
(data / 'report.txt').write_text('Synthetic output\n')
signals.put(store, task_id, run_id, dict(schema_version=1, task_id=task_id, id='report',
            revision=1, type='report', status='active', evidence=['report.txt']))
saved = runtime.publish(store, 'worker-publish-' + run_id, task_id, run_id)
assert store.snapshot().read(runtime.data_prefix(run, task_id) + 'report.txt') == b'Synthetic output\n'
for message in task_io.pending(store, task_id)['messages']:
    task_io.complete(store, 'ack-' + digest([run_id, message['id']]), task_id, run_id,
                     message['id'], {'status': 'succeeded'})
result = dict(protocol=context['protocol'], task_id=task_id, run_id=run_id,
              generation=run['generation'], outcome='succeeded', summary='Synthetic work verified')
if run.get('completion_contract'):
    result['completion'] = completion.receipt(run, {'git': dict(status='succeeded',
        operation_id=saved['operation_id'], evidence={'output': 'report.txt'})}, stopped=True)
atomic_json(Path(context['result_path']), result)
