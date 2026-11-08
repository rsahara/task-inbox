"""Provider-neutral business completion evidence, durable before releasing a run."""
import copy
from .store import TaskError, digest, encode, identity

STATES = {'succeeded', 'failed', 'unknown', 'not_started'}
KEYS = ('repository_id', 'task_id', 'run_id', 'generation')


def contract(value):
    if (not isinstance(value, dict) or set(value) != {'required', 'context'} or
            not isinstance(value['required'], list) or not value['required'] or
            len(value['required']) > 32 or not all(isinstance(x, str) for x in value['required']) or len(set(value['required'])) != len(value['required']) or
            not isinstance(value['context'], dict) or len(encode(value)) > 32768):
        raise TaskError('Invalid completion contract')
    for step in value['required']:
        identity(step, 'completion step')
    return copy.deepcopy(value)


def validate(run, value):
    required = run.get('completion_contract')
    if not required:
        raise TaskError('Run has no completion contract; use the legacy recovery procedure')
    if (not isinstance(value, dict) or len(encode(value)) > 262144 or
            any(value.get(k) != run[k] for k in KEYS) or
            value.get('context') != required['context'] or
            type(value.get('operations_stopped')) is not bool or
            not isinstance(value.get('steps'), dict) or set(value['steps']) != set(required['required'])):
        raise TaskError('Completion evidence does not match the run and its contract')
    for step in value['steps'].values():
        if (not isinstance(step, dict) or step.get('status') not in STATES or
                not isinstance(step.get('evidence'), dict) or not step['evidence']):
            raise TaskError('Each completion step needs a status and durable evidence')
        identity(step.get('operation_id'), 'business operation ID')
    statuses = [s['status'] for s in value['steps'].values()]
    status = ('unknown' if 'unknown' in statuses else
              'partial' if not value['operations_stopped'] else
              'succeeded' if all(s == 'succeeded' for s in statuses) else 'failed')
    return dict(copy.deepcopy(value), status=status)


def require_terminal(run, outcome=None):
    if not run.get('completion_contract'):
        return None
    value = validate(run, run.get('completion'))
    if value['status'] not in ('succeeded', 'failed'):
        raise TaskError('Business completion is partial or unknown; preserve ownership and evidence')
    if outcome and value['status'] != outcome:
        raise TaskError('Runtime outcome differs from the verified business result')
    return value


def record(store, operation_id, task_id, run_id, value):
    from . import runtime
    request = dict(kind='run-completion', task_id=task_id, run_id=run_id, evidence=value)
    def apply(snapshot):
        run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
        checked = update(run, value)
        return {runtime.run_path(task_id, run_id, snapshot) + '/run.json': encode(run)}, checked
    return store.transact(operation_id, request, apply)


def update(run, value):
    """Validate a completion transition for recording or atomic finish."""
    checked = validate(run, value)
    for name, prior in run.get('completion', {}).get('steps', {}).items():
        step = checked['steps'][name]
        if prior['status'] == 'unknown' and step['operation_id'] != prior['operation_id']:
            raise TaskError('Reconcile the original unknown operation before issuing another')
        if prior['status'] == 'succeeded' and step != prior:
            raise TaskError('A verified business step is immutable; retain its original evidence')
    run['completion'] = checked
    return checked


def receipt(run, steps, *, stopped=False):
    return dict({k: run[k] for k in KEYS}, context=run['completion_contract']['context'],
                operations_stopped=stopped, steps=steps)
