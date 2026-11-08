#!/usr/bin/env python3
"""Bounded Git subprocess ownership and sanitized diagnostics."""
from contextlib import contextmanager
import itertools
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading


EVENTS = itertools.count(1)
OPERATIONS = frozenset(('rev-parse', 'ls-remote', 'fetch', 'push', 'update-ref',
    'commit-tree', 'cat-file', 'worktree', 'branch', 'diff', 'add', 'commit',
    'status', 'merge-base', 'apply', 'write-tree', 'ls-tree', 'rev-list',
    'config', 'check-ref-format', 'ls-files', 'show'))

# Auto maintenance must remain a child of the Git command that we own. Git
# 2.52 can detach even when no task needs work; a non-reaping container PID 1
# otherwise accumulates one orphaned maintenance process per fetch. Keep the
# maintenance work enabled, but include it in the same deadline and cleanup.
GIT_PROCESS_OPTIONS = ('-c', 'maintenance.autoDetach=false', '-c', 'gc.autoDetach=false')


class GitFailure(RuntimeError):
    def __init__(self, message, diagnostic):
        super().__init__(message)
        self.diagnostic = diagnostic
        self.definite_rejection = diagnostic['definite_rejection']


class OwnedProcess(subprocess.Popen):
    """Register ownership before Popen can create a child or be interrupted."""
    def __init__(self, owner, *args, **kwargs):
        owner.process = self
        super().__init__(*args, **kwargs)


class ProcessOwner:
    def __init__(self):
        self.process = None
        self.protected = False
        self.cancelled = None
        self.closed = False
        self.output = (b'', b'')

    def raise_cancelled(self):
        if self.cancelled == signal.SIGINT:
            raise KeyboardInterrupt
        if self.cancelled is not None:
            raise SystemExit(128 + self.cancelled)

    def start(self, args, cwd):
        # Defer default cancellation during Popen's internal fork/pid handoff.
        # The child inherits ordinary signal dispositions, not a blocked mask.
        self.protected = True
        try:
            OwnedProcess(self, args, cwd=cwd, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         start_new_session=True)
        finally:
            self.protected = False
        self.raise_cancelled()

    def close(self):
        if self.closed:
            return self.output
        self.protected = True
        try:
            if getattr(self.process, 'pid', None):
                self.output = stop_group(self.process)
        finally:
            for name in ('stdin', 'stdout', 'stderr'):
                stream = getattr(self.process, name, None)
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass
            self.closed = True
            self.protected = False
        return self.output


def rejected_push(stdout):
    """Recognize one definite local Git porcelain rejection, never remote stderr."""
    flags = (b'!\t', b' \t', b'*\t', b'+\t', b'=\t', b'-\t')
    statuses = [line.split(b'\t', 2) for line in stdout.splitlines() if line[:2] in flags]
    reasons = (b'[rejected] (fetch first)', b'[rejected] (non-fast-forward)',
               b'[rejected] (stale info)',
               b'[remote rejected] (incorrect old value provided)',
               b'[remote rejected] (failed to update ref)')
    return (len(statuses) == 1 and len(statuses[0]) == 3 and
            statuses[0][0] == b'!' and statuses[0][2] in reasons)


def failure_category(stderr):
    """Only return fixed categories; remote text never becomes diagnostic data."""
    value = stderr.lower()
    for category, patterns in (
        ('authentication', (b'authentication failed', b'permission denied', b'could not read username')),
        ('policy_denied', (b'not allowed', b'prohibited', b'blocked by policy')),
        ('non_fast_forward', (b'non-fast-forward', b'fetch first', b'stale info', b'incorrect old value provided')),
        ('ref_missing', (b"couldn't find remote ref", b'remote ref does not exist')),
        ('conflict', (b'conflict', b'patch failed', b'does not apply')),
    ):
        if any(pattern in value for pattern in patterns):
            return category
    return 'unknown'


def git_failure(operation, event, stdout, stderr, code=None, timed_out=False,
                definite_rejection=False, launch_failed=False):
    record = {'event': event, 'operation': operation,
              'category': ('launch_failed' if launch_failed else 'timeout' if timed_out
                           else failure_category(stdout + b'\n' + stderr)),
              'exit_code': code if code is not None and code >= 0 else None,
              'signal': -code if code is not None and code < 0 else None,
              'timeout': timed_out, 'stdout_bytes': len(stdout), 'stderr_bytes': len(stderr),
              'definite_rejection': bool(definite_rejection and code is not None and code > 0 and not timed_out),
              'output': 'omitted'}
    # Emit before reconciliation/cleanup, which must not hide the first failure.
    print(json.dumps(record, sort_keys=True), file=sys.stderr, flush=True)
    if launch_failed:
        return GitFailure(f'Git {operation} could not start; output omitted', record)
    if timed_out:
        return GitFailure(f'Git {operation} timed out; retain state and reconcile remote outcome', record)
    return GitFailure(f'Git {operation} failed (exit {code}); output omitted', record)


def stop_group(process):
    """Terminate helpers even after Git exited but a helper still holds a pipe."""
    output = (b'', b'')
    def collect():
        # Popen already closes pipes and reaps a child whose exec failed.
        if all(stream is None or stream.closed for stream in
               (getattr(process, name, None) for name in ('stdin', 'stdout', 'stderr'))):
            process.wait(timeout=0.2)
            return b'', b''
        return process.communicate(timeout=0.2)
    try:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            output = collect()
        except subprocess.TimeoutExpired:
            pass
    finally:
        # A second interrupt during the grace period must still kill the group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            if process.returncode is None:
                try:
                    os.kill(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        try:
            drained = collect()
            output = drained[0] or output[0], drained[1] or output[1]
        except subprocess.TimeoutExpired as error:
            process.stdout.close()
            process.stderr.close()
            process.wait(timeout=0.2)
            output = error.output or b'', error.stderr or b''
    return output


@contextmanager
def cancellation_signals(owner):
    """Let default shell cancellation unwind cleanup; preserve caller handlers."""
    previous = {}
    def cancel(signum, _frame):
        already_cancelled = owner.cancelled is not None
        if not already_cancelled:
            owner.cancelled = signum
        if owner.protected or already_cancelled:
            return
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + signum)
    try:
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                handler = signal.getsignal(sig)
                if handler == signal.SIG_DFL or (sig == signal.SIGINT and handler is signal.default_int_handler):
                    previous[sig] = handler
                    signal.signal(sig, cancel)
        yield
    finally:
        try:
            # Fallback if the first cancellation interrupted cleanup's handoff.
            # Further default cancellation is deferred until ownership is closed.
            owner.close()
        finally:
            for sig, handler in previous.items():
                if signal.getsignal(sig) is cancel:
                    signal.signal(sig, handler)


def run_process(args, *, cwd=None, input=None, timeout):
    """Own spawn, handoff, monitoring and cleanup for Git and control children."""
    owner = ProcessOwner()
    with cancellation_signals(owner):
        try:
            owner.start(args, cwd)
            stdout, stderr = owner.process.communicate(input, timeout=timeout)
            return subprocess.CompletedProcess(args, owner.process.returncode, stdout, stderr)
        except subprocess.TimeoutExpired as error:
            stdout, stderr = owner.close()
            raise subprocess.TimeoutExpired(args, timeout, output=stdout or error.output,
                                            stderr=stderr or error.stderr) from None
        finally:
            unwinding = sys.exc_info()[0] is not None
            owner.close()
            if not unwinding:
                owner.raise_cancelled()


def git(repo, *args, input=None, strip=True, binary=False, predicate=False):
    try:
        seconds = float(os.environ.get("TASKS_GIT_TIMEOUT_SECONDS", "60"))
    except (TypeError, ValueError):
        raise ValueError("TASKS_GIT_TIMEOUT_SECONDS must be finite and positive") from None
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("TASKS_GIT_TIMEOUT_SECONDS must be finite and positive")
    operation = next((arg for arg in args if arg in OPERATIONS), 'other')
    event = next(EVENTS)
    data = input.encode() if isinstance(input, str) else input
    try:
        result = run_process(['git', *GIT_PROCESS_OPTIONS, *args], cwd=repo, input=data, timeout=seconds)
    except subprocess.TimeoutExpired as error:
        raise git_failure(operation, event, error.output or b'', error.stderr or b'',
                          timed_out=True) from None
    except OSError:
        raise git_failure(operation, event, b'', b'', launch_failed=True) from None
    stdout, stderr = result.stdout, result.stderr
    if predicate and result.returncode in (0, 1):
        return result.returncode == 0
    if result.returncode:
        raise git_failure(operation, event, stdout, stderr, code=result.returncode,
                          definite_rejection=operation == 'push' and '--porcelain' in args and rejected_push(stdout))
    output = stdout if binary else stdout.decode()
    return output.strip() if strip else output
