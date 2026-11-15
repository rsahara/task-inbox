"""Portable task input/output declarations, independent of routing and placement."""
import copy
import fnmatch
import re

from .store import TaskError


def strings(value, label):
    if (not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value)
            or len(value) != len(set(value))):
        raise TaskError(label + ' must be a list of unique nonempty strings')


def validate(value):
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'inbox', 'signals', 'readiness'}
            or type(value['schema_version']) is not int or value['schema_version'] not in (1, 2)
            or value['readiness'] not in ('inbox', 'manual', 'condition')):
        raise TaskError('Invalid task interface declaration')
    strings(value['inbox'], 'Interface inbox kinds')
    signals = value['signals']
    if not isinstance(signals, dict) or set(signals) != {'reads', 'writes'}:
        raise TaskError('Interface signals must declare reads and writes')
    if value['schema_version'] == 1:
        strings(signals['writes'], 'Interface signal output types')
    else:
        if not isinstance(signals['writes'], list):
            raise TaskError('Interface outputs must be signal declarations')
        ids = set()
        for output in signals['writes']:
            if (not isinstance(output, dict) or not {'id', 'type'} <= set(output)
                    or set(output) - {'id', 'type', 'required', 'value_schema'}
                    or any(not isinstance(output[k], str) or not output[k].strip() for k in ('id', 'type'))
                    or output['id'] in ids or type(output.get('required', False)) is not bool):
                raise TaskError('Invalid declared output signal')
            if output.get('required') and '*' in output['id']:
                raise TaskError('Required signals need an exact ID')
            if not re.fullmatch(r'[a-z0-9][a-z0-9_.-]*\*?[a-z0-9_.-]*', output['id']):
                raise TaskError('Output signal IDs use lowercase identities and at most one wildcard')
            if 'value_schema' in output:
                from .store import safe_path
                safe_path(output['value_schema'])
            ids.add(output['id'])
    if not isinstance(signals['reads'], list):
        raise TaskError('Interface signal reads must be a list')
    seen = []
    for item in signals['reads']:
        if (not isinstance(item, dict) or not {'type'} <= set(item) <= {'type', 'task_id', 'id'}
                or any(not isinstance(v, str) or not v.strip() for v in item.values()) or item in seen):
            raise TaskError('Invalid interface signal input selector')
        seen.append(item)
    return copy.deepcopy(value)


def declaration(task):
    value = task.get('interface')
    return validate(value) if value is not None else dict(
        schema_version=1, inbox=[], signals=dict(reads=[], writes=[]), readiness='manual')


def matches(value, selector):
    return all(value.get(key) == expected for key, expected in selector.items())


def output(task, value):
    """Return the declared output contract; legacy v1 writes remain descriptive."""
    interface = declaration(task)
    if interface['schema_version'] == 1:
        return None
    selected = [s for s in interface['signals']['writes']
                if fnmatch.fnmatchcase(value['id'], s['id']) and value['type'] == s['type']]
    if len(selected) != 1:
        raise TaskError('Signal ID/type must match exactly one declared output')
    return selected[0]
