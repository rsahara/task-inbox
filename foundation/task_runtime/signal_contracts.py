"""A bounded, offline JSON-schema subset for portable signal values."""
from .store import TaskError


def validate_value(value, schema, depth=0):
    if depth > 32 or not isinstance(schema, dict):
        raise TaskError('Invalid signal value schema')
    if set(schema) - {'type', 'required', 'properties', 'additionalProperties', 'items', 'enum',
                     'description', 'title', '$schema'}:
        raise TaskError('Unsupported signal schema keyword')
    types = {'object': lambda v: isinstance(v, dict), 'array': lambda v: isinstance(v, list),
             'string': lambda v: isinstance(v, str), 'integer': lambda v: type(v) is int,
             'number': lambda v: type(v) in (int, float), 'boolean': lambda v: type(v) is bool,
             'null': lambda v: v is None}
    kind = schema.get('type')
    if kind is not None and (not isinstance(kind, str) or kind not in types or not types[kind](value)):
        raise TaskError('Signal value does not match its declared type')
    if 'enum' in schema and not isinstance(schema['enum'], list):
        raise TaskError('Signal schema enum must be a list')
    if 'enum' in schema and value not in schema['enum']:
        raise TaskError('Signal value is outside the declared enum')
    if isinstance(value, dict):
        required, properties = schema.get('required', []), schema.get('properties', {})
        if (not isinstance(required, list) or any(not isinstance(k, str) for k in required)
                or not isinstance(properties, dict) or any(k not in value for k in required)):
            raise TaskError('Signal value is missing required properties')
        extra = set(value) - set(properties)
        additional = schema.get('additionalProperties', True)
        if type(additional) is not bool:
            raise TaskError('additionalProperties must be a boolean')
        if extra and not additional:
            raise TaskError('Signal value contains undeclared properties')
        for name in set(value) & set(properties):
            validate_value(value[name], properties[name], depth + 1)
    if isinstance(value, list) and 'items' in schema:
        for item in value:
            validate_value(item, schema['items'], depth + 1)
