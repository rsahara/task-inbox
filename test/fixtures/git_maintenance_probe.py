"""Own the probe's descendants so a regression never leaks zombies to PID 1."""
import ctypes
import json
import os
from pathlib import Path
import runpy
import sys
import time

if sys.platform != 'linux':
    print(json.dumps({'unsupported': 'Linux subreaper is unavailable'}))
    raise SystemExit(0)
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER, this process only.
    print(json.dumps({'unsupported': 'Cannot supervise probe descendants'}))
    raise SystemExit(0)
root = Path(sys.argv[1])
helper = runpy.run_path(sys.argv[2])
git = helper['git']
origin, clone = root / 'origin.git', root / 'clone'
git(root, 'init', '--bare', str(origin))
tree = git(origin, 'mktree', input='')
commit = git(origin, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
             'commit-tree', tree, '-m', 'Probe')
git(origin, 'update-ref', 'refs/heads/main', commit)
git(root, 'init', str(clone))
git(clone, 'remote', 'add', 'origin', str(origin))
orphans = []


def collect():
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid:
            orphans.append(pid)
        else:
            time.sleep(0.01)
    raise RuntimeError('Probe still owns running descendants')


try:
    for _ in range(3):
        git(clone, 'fetch', '--no-tags', 'origin', 'refs/heads/main')
        collect()
finally:
    collect()
print(json.dumps({'reaped_orphans': len(orphans)}))
