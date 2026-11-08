"""Discover task identities from definitions; an explicit index is optional."""
from .store import CONTROL, Snapshot, TaskError, encode, git, identity, refs_overlap, safe_path


def path(value):
    if not isinstance(value, str):
        raise TaskError('Task paths must be repository-relative strings')
    safe_path(value)
    if any(part.lower() == '.git' for part in value.split('/')) or refs_overlap(value, CONTROL):
        raise TaskError('Task paths overlap reserved runtime metadata')
    return value


def settings(config):
    value = config.get('layout')
    if value is None:
        if config.get('schema_version') == 5:
            raise TaskError('Indexed layout is missing')
        return None
    if config.get('schema_version') not in (5, 6, 7, 8, 9) or not isinstance(value, dict) or set(value) != {'index', 'task_root'}:
        raise TaskError('Unsupported task layout')
    index, root = path(value['index']), path(value['task_root'])
    if index == root or root.startswith(index + '/'):
        raise TaskError('Task root overlaps index file')
    return value


def tasks(snapshot):
    if hasattr(snapshot, '_task_paths'):
        return snapshot._task_paths
    config = snapshot.json(CONTROL + '/config.json', {})
    options = settings(config)
    if options:
        index = snapshot.json(options['index'])
        if not isinstance(index, dict) or index.get('schema_version') != 1 or not isinstance(index.get('tasks'), dict):
            raise TaskError('Task index is missing or unsupported')
        result = index['tasks']
        for task_id, location in result.items():
            identity(task_id, 'task ID'); path(location)
            if refs_overlap(location, options['index']):
                raise TaskError('Task directory overlaps index file')
        locations = set(result.values())
        if len(locations) != len(result) or any('/'.join(location.split('/')[:n]) in locations
                for location in locations for n in range(1, len(location.split('/')))):
            raise TaskError('Task directories overlap')
    else:
        result = {}
        for name in sorted(snapshot.entries):
            if not (name.startswith('tasks/') and len(name.split('/')) == 3 and name.endswith('/task.json')):
                continue
            record = snapshot.json(name)
            task_id = identity(record.get('id'), 'task ID')
            if task_id in result:
                raise TaskError('Duplicate task ID in task definitions')
            result[task_id] = path(name.rsplit('/', 1)[0])
    snapshot._task_paths = result
    return result


def task_path(task_id, snapshot):
    identity(task_id, 'task ID')
    known = tasks(snapshot)
    if task_id in known:
        return known[task_id]
    if settings(snapshot.json(CONTROL + '/config.json', {})):
        raise TaskError('Task ID is absent from the index') from None
    return 'tasks/' + task_id


def previous_locations(snapshot, task_id):
    """Bind former locations to their owner at each deletion's parent tree."""
    if not snapshot.commit or snapshot.json(CONTROL + '/config.json', {}).get('schema_version', 0) < 6:
        return []
    snapshot.store.require_history()
    if not hasattr(snapshot, '_previous_task_locations'):
        snapshot._previous_task_locations = {}
        pattern = ':(glob)tasks/*/task.json'
        commits = git(snapshot.store.repo, 'log', '-m', '--full-history', '--no-renames',
                      '--format=%H', '--diff-filter=D', snapshot.commit, '--', pattern).splitlines()
        parents = {}
        for commit in dict.fromkeys(commits):
            for parent in git(snapshot.store.repo, 'rev-list', '--parents', '-n', '1', commit).split()[1:]:
                names = git(snapshot.store.repo, 'diff-tree', '--no-commit-id', '--no-renames',
                            '--diff-filter=D', '--name-only', '-r', '-z', parent, commit, '--', pattern,
                            binary=True, strip=False).split(b'\0')
                if not any(names):
                    continue
                if parent not in parents:
                    parents[parent] = Snapshot(snapshot.store, parent)
                previous = parents[parent]
                for raw in filter(None, names):
                    name = raw.decode()
                    record = previous.json(name)
                    if record and isinstance(record.get('id'), str):
                        snapshot._previous_task_locations.setdefault(record['id'], []).append(
                            (name.rsplit('/', 1)[0], previous))
    return snapshot._previous_task_locations.get(task_id, [])


def register(snapshot, task_id, requested=None):
    options = settings(snapshot.json(CONTROL + '/config.json'))
    known = tasks(snapshot)
    location = path(requested or ((options['task_root'] if options else 'tasks') + '/' + task_id))
    if not options and (not location.startswith('tasks/') or len(location.split('/')) != 2):
        raise TaskError('Custom task paths outside tasks/ require an indexed repository')
    if task_id in known or any(refs_overlap(location, other) for other in known.values()):
        raise TaskError('Task ID or directory already exists; identities cannot be reused')
    if options and refs_overlap(location, options['index']):
        raise TaskError('Task directory overlaps index file')
    if any(p == location or p.startswith(location + '/') or location.startswith(p + '/') for p in snapshot.entries):
        raise TaskError('Task directory overlaps existing repository files')
    # An existing task may own a data directory outside its metadata directory.
    for tid, other in known.items():
        record = snapshot.json(other + '/task.json')
        data = record.get('data_path', other + '/data')
        if refs_overlap(location, data):
            raise TaskError('Task directory overlaps another task data directory')
    changes = {options['index']: encode(dict(schema_version=1, tasks=dict(known, **{task_id: location})))} if options else {}
    return location, changes


def check_data(snapshot, task_id, location, data):
    path(data)
    if refs_overlap(data, location) and data != location + '/data':
        raise TaskError('Task data overlaps its definition or run metadata')
    options = settings(snapshot.json(CONTROL + '/config.json', {}))
    if options and refs_overlap(data, options['index']):
        raise TaskError('Task data overlaps index file')
    for tid, other in tasks(snapshot).items():
        if tid == task_id:
            continue
        record = snapshot.json(other + '/task.json')
        other_data = record.get('data_path', other + '/data')
        if refs_overlap(data, other) or refs_overlap(data, other_data):
            raise TaskError('Task data overlaps another task')
