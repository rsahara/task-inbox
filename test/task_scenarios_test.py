"""User scenarios: actual worker death, a growing inbox, and serialized subruns.

Only the observer's clock is controlled. Git pushes, clones, workers, capacity
checks, result acknowledgement and collection all use the real implementation.
"""
import json
import os
import select
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

from task_runtime_test import ROOT, RuntimeCase
from task_runtime import lifecycle, runtime, task_io
from task_runtime.store import TaskError


class ScenarioTests(RuntimeCase):
    def worker(self, role, run, repo=None, value=None):
        log = (self.base / (run + '.stderr')).open('w+b')
        self.addCleanup(log.close)
        argv = [sys.executable, str(ROOT / 'test/fixtures/task_scenario_worker.py'), role,
                '--repo', str(repo or self.a), '--work', str(self.base / ('work-' + run)),
                '--run', run]
        if value is not None:
            argv += ['--value', str(value)]
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=log, bufsize=0, start_new_session=True)
        self.addCleanup(self.stop_worker, process)
        return SimpleNamespace(process=process, log=log, buffer=b'')

    @staticmethod
    def stop_worker(process):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        process.stdin.close()
        process.stdout.close()

    def event(self, worker, expected):
        deadline = time.monotonic() + 30
        while b'\n' not in worker.buffer:
            remaining = deadline - time.monotonic()
            self.assertGreater(remaining, 0, 'Worker stalled waiting for ' + expected)
            ready, _, _ = select.select([worker.process.stdout], [], [], remaining)
            self.assertTrue(ready, 'Worker stalled waiting for ' + expected)
            data = os.read(worker.process.stdout.fileno(), 65536)
            if not data:
                worker.log.seek(0)
                self.fail('Worker exited before %s:\n%s' % (expected, worker.log.read().decode()))
            worker.buffer += data
        line, worker.buffer = worker.buffer.split(b'\n', 1)
        value = json.loads(line)
        self.assertEqual(value['event'], expected, value)
        return value

    @staticmethod
    def resume(worker):
        worker.process.stdin.write(b'continue\n')
        worker.process.stdin.flush()

    def exited(self, worker):
        self.assertEqual(worker.process.wait(timeout=30), 0)

    def kill(self, worker):
        os.killpg(worker.process.pid, signal.SIGKILL)
        self.assertEqual(worker.process.wait(timeout=10), -signal.SIGKILL)

    def reap_at(self, task, seconds):
        # Replace this module's clock only; subprocess and lock deadlines stay real.
        with patch.object(runtime, 'time', SimpleNamespace(monotonic=lambda: seconds)):
            return lifecycle.reap(self.sb, task, apply=True, observer_profile='retained')

    def active(self, task):
        slots = runtime.load_task(self.sb.snapshot(), task)[1]['slots']
        return [slot['run_id'] for slot in slots.values() if slot.get('run_id')]

    def add_pair(self, parallel=False):
        self.add('task-a', max_runners=2 if parallel else 1, recovery_mode='git_only')
        self.add('task-b', recovery_mode='git_only')

    def test_scenario1_killed_run_blocks_until_stale_then_retains_evidence(self):
        self.add('task-a', recovery_mode='git_only')
        worker = self.worker('checkpoint', 'old')
        run = self.event(worker, 'checkpoint')['run']
        work_head = self.sb.remote_tip(run['work_ref'])
        beat_head = self.sb.remote_tip(run['heartbeat_ref'])
        self.assertIsNotNone(work_head)
        self.assertIsNotNone(beat_head)
        self.kill(worker)

        first = self.reap_at('task-a', 100)['runs'][0]
        self.assertFalse(first['stale'])
        self.assertEqual(first['observation_reset'], 'first_observation')
        with self.assertRaisesRegex(TaskError, 'runner limit'):
            self.start('too-soon', task='task-a', store=self.sb)
        self.assertFalse(self.reap_at('task-a', 399)['runs'][0]['stale'])
        self.assertEqual(self.active('task-a'), ['old'])

        # Expiry releases ownership, but does not prove that output is disposable.
        expired = self.reap_at('task-a', 400)
        self.assertFalse(expired['needs_attention'])
        self.assertEqual(expired['runs'], [{'run_id': 'old', 'status': 'lost'}])
        cleanup = self.sb.snapshot().json('tasks/task-a/runs/old/cleanup.json')
        self.assertEqual(cleanup['status'], 'retained')
        notice = self.sb.snapshot().json('tasks/task-a/runs/old/retention-report.json')
        self.assertEqual(notice['recovery_kind'], 'stale_git_only')
        self.assertEqual(notice['refs'][1]['observed_commit'], beat_head)
        with self.assertRaisesRegex(TaskError, 'retain their work'):
            lifecycle.cleanup(self.sb, 'cleanup-old', 'task-a', 'old', apply=True, stopped=True)
        replacement = self.start('next', task='task-a', store=self.sb)
        self.assertEqual(replacement['generation'], run['generation'] + 1)
        with self.assertRaises(TaskError):
            runtime.publish(self.sa, 'late-old', 'task-a', 'old', outcome='succeeded')
        self.assertEqual(self.active('task-a'), ['next'])
        self.assertEqual(self.sb.remote_tip(run['work_ref']), work_head)
        self.assertEqual(self.sb.remote_tip(run['heartbeat_ref']), beat_head)
        self.assertTrue((self.base / 'work-old').exists())
        self.prepare('next', task='task-a', store=self.sb)
        runtime.publish(self.sb, 'finish-next', 'task-a', 'next', outcome='succeeded')
        self.assertEqual(self.active('task-a'), [])
        self.assertEqual(self.sb.remote_tip(run['work_ref']), work_head)
        self.assertEqual(self.sb.remote_tip(run['heartbeat_ref']), beat_head)
        self.assertTrue((self.base / 'work-old').exists())

    def test_scenario1_confirmed_stop_can_restore_finish_and_collect(self):
        self.add('task-a', recovery_mode='git_only')
        worker = self.worker('checkpoint', 'old')
        run = self.event(worker, 'checkpoint')['run']
        self.kill(worker)
        self.reap_at('task-a', 100)
        with patch.object(runtime, 'time', SimpleNamespace(monotonic=lambda: 400)):
            self.assertTrue(runtime.observe(self.sb, 'task-a', 'old')['stale'])
        # Before revoking the run, a verified stop permits restoring its checkpoint
        # and adopting the output in a failed finish. Lost runs cannot be restored.
        restored = self.base / 'restored'
        runtime.prepare(self.sb, 'task-a', 'old', restored, restore=True, stopped=True, settled=True)
        artifact = 'tasks/task-a/runs/old/artifacts/checkpoint.txt'
        self.assertEqual((restored / artifact).read_text(), 'durable checkpoint\n')
        runtime.publish(self.sb, 'finish-old', 'task-a', 'old', outcome='failed')
        self.assertFalse(restored.exists())
        self.assertIsNone(self.sb.remote_tip(run['work_ref']))
        self.assertIsNone(self.sb.remote_tip(run['heartbeat_ref']))
        self.start('next', task='task-a')  # Owning clone collects its original worktree.
        self.assertFalse((self.base / 'work-old').exists())
        self.assertEqual(self.sa.snapshot().read(artifact), b'durable checkpoint\n')
        self.assertEqual(self.sa.snapshot().metadata('tasks/task-a/runs/old/run.json')['status'], 'failed')
        self.prepare('next', task='task-a')
        runtime.publish(self.sa, 'finish-next', 'task-a', 'next', outcome='succeeded')
        self.assertEqual(self.active('task-a'), [])

    def test_scenario2_drains_messages_added_while_forwarding(self):
        self.add_pair()
        payloads = {f'item-{i}': {'value': i, 'text': f'入力{i}'} for i in range(1, 5)}
        for key in ('item-1', 'item-2'):
            task_io.send(self.sb, 'send-' + key, 'task-a', key, 'input', payloads[key])
        worker = self.worker('forward', 'forwarder')
        self.assertEqual(self.event(worker, 'first-forwarded')['message_id'], 'item-1')
        self.assertEqual(self.active('task-a'), ['forwarder'])
        self.assertEqual([m['id'] for m in task_io.pending(self.sb, 'task-a')['messages']], ['item-2'])
        for key in ('item-3', 'item-4'):
            task_io.send(self.sb, 'send-' + key, 'task-a', key, 'input', payloads[key])
        self.resume(worker)
        finished = self.event(worker, 'finished')
        self.exited(worker)
        self.assertEqual(finished['processed'], list(payloads))
        self.assertEqual(finished['pending'], [])
        messages = task_io.pending(self.sb, 'task-b')['messages']
        self.assertEqual({m['id']: m['payload'] for m in messages}, payloads)
        for key in payloads:
            status = task_io.status(self.sb, 'task-a', key)
            self.assertEqual(status['status'], 'succeeded')
            self.assertEqual(status['receipt']['run_id'], 'forwarder')
        self.assertEqual(self.active('task-a'), [])
        self.assertEqual(task_io.wake(self.sb, 'wake-empty', 'task-a', 'idle')['action'], 'idle')
        # A push after finish needs the next invocation; wake does not spawn a worker.
        task_io.send(self.sb, 'send-later', 'task-a', 'later', 'input', {'value': 5})
        self.assertEqual(self.active('task-a'), [])
        next_worker = self.worker('forward', 'forwarder-next', repo=self.b)
        self.assertEqual(self.event(next_worker, 'first-forwarded')['message_id'], 'later')
        self.resume(next_worker)
        self.assertEqual(self.event(next_worker, 'finished')['processed'], ['later'])
        self.exited(next_worker)
        self.assertEqual(len(task_io.pending(self.sa, 'task-b')['messages']), 5)

    def parallel_parents(self):
        self.add_pair(parallel=True)
        first = self.worker('parent', 'first', value=11)
        acquired = self.event(first, 'acquired')['run']
        second = self.worker('parent', 'second', repo=self.b, value=13)
        self.event(second, 'busy')
        self.assertCountEqual(self.active('task-a'), ['first', 'second'])
        self.assertEqual(self.active('task-b'), ['child-first'])
        self.assertEqual(task_io.status(self.sb, 'task-b', 'request-second')['status'], 'pending')
        return first, second, acquired

    def assert_parent_result(self, name, value):
        expected = dict(status='succeeded', parent_run=name, value=value, child_run='child-' + name)
        status = task_io.status(self.sa, 'task-b', 'request-' + name)
        self.assertEqual(status['receipt']['result'], expected)
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.json(f'tasks/task-a/runs/{name}/artifacts/result.json'), expected)
        run = runtime.load_run(snapshot, 'task-b', 'child-' + name)[2]
        self.assertIsNone(self.sa.remote_tip(run['heartbeat_ref']))
        self.assertEqual(snapshot.metadata(f'tasks/task-b/runs/child-{name}/io/results/request-{name}.json')['result'], expected)

    def test_scenario3_parallel_parents_serialize_child_and_read_their_own_results(self):
        first, second, acquired = self.parallel_parents()
        self.resume(first)
        self.assertEqual(self.event(first, 'finished')['result']['value'], 77)
        self.exited(first)
        successor = self.event(second, 'acquired')['run']
        self.assertEqual(successor['generation'], acquired['generation'] + 1)
        self.assertEqual(self.active('task-b'), ['child-second'])
        self.assertEqual(task_io.status(self.sa, 'task-b', 'request-second')['status'], 'pending')
        self.resume(second)
        self.assertEqual(self.event(second, 'finished')['result']['value'], 91)
        self.exited(second)
        self.assert_parent_result('first', 77)
        self.assert_parent_result('second', 91)
        self.assertEqual(task_io.pending(self.sa, 'task-b')['messages'], [])
        self.assertEqual(self.active('task-a'), [])
        self.assertEqual(self.active('task-b'), [])

    def test_scenario3_killed_child_is_reaped_and_waiter_gets_only_its_own_result(self):
        first, second, acquired = self.parallel_parents()
        self.kill(first)  # The child section runs in the same process; no children remain.
        self.assertFalse(self.reap_at('task-b', 100)['runs'][0]['stale'])
        self.assertEqual(self.active('task-b'), ['child-first'])
        self.assertEqual(self.reap_at('task-b', 400)['runs'],
                         [{'run_id': 'child-first', 'status': 'lost'}])
        successor = self.event(second, 'acquired')['run']
        self.assertEqual(successor['generation'], acquired['generation'] + 1)
        with self.assertRaises(TaskError):
            task_io.complete(self.sa, 'late-result', 'task-b', 'child-first', 'request-first',
                           {'status': 'succeeded', 'value': 77})
        self.resume(second)
        self.assertEqual(self.event(second, 'finished')['result']['value'], 91)
        self.exited(second)
        self.assert_parent_result('second', 91)
        self.assertEqual(task_io.status(self.sa, 'task-b', 'request-first')['status'], 'pending')
        self.assertNotIn('tasks/task-a/runs/first/artifacts/result.json', self.sa.snapshot().entries)
        self.assertIsNotNone(self.sa.remote_tip(acquired['heartbeat_ref']))
        runtime.recover(self.sa, 'recover-parent', 'task-a', 'first', stopped=True, settled=True,
                        evidence='fixture:process-group-killed-and-git-calls-settled')
        self.assertEqual(self.active('task-a'), [])
        self.assertEqual(self.active('task-b'), [])
