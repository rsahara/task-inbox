"""Portable task input/output declarations, independent of routing and placement."""
import copy

from .store import TaskError


def strings(value, label):
    if (not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value)
            or len(value) != len(set(value))):
        raise TaskError(label + ' must be a list of unique nonempty strings')


def validate(value):
    if (not isinstance(value, dict) or set(value) != {'schema_version', 'inbox', 'signals', 'readiness'}
            or type(value['schema_version']) is not int or value['schema_version'] != 1
            or value['readiness'] not in ('inbox', 'manual', 'condition')):
        raise TaskError('Invalid task interface declaration')
    strings(value['inbox'], 'Interface inbox kinds')
    signals = value['signals']
    if not isinstance(signals, dict) or set(signals) != {'reads', 'writes'}:
        raise TaskError('Interface signals must declare reads and writes')
    strings(signals['writes'], 'Interface signal output types')
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
