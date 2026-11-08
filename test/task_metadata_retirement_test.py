"""Completed executions leave task output, with retries reconciled via Git history."""
from task_runtime_test import RuntimeCase
from unittest.mock import patch
from task_runtime import maintenance, runtime, task_io
from task_runtime.store import CONTROL, GitFailure, Snapshot, Store, TaskError, encode


class MetadataRetirementTest(RuntimeCase):
    def test_finish_removes_execution_metadata_and_keeps_task_output(self):
        self.add(); self.start(); work = self.prepare()
        output = 'tasks/t/runs/r/artifacts/result.json'
        self.output(work, output, b'{"task_output": true}\n')
        runtime.heartbeat(self.sa, 't', 'r')
        result = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.read(output), b'{"task_output": true}\n')
        self.assertEqual([p for p in snapshot.entries if p.startswith('tasks/t/runs/r/')], [output])
        self.assertNotIn(CONTROL + '/operations/finish.json', snapshot.entries)
        self.assertEqual(runtime.load_run(snapshot, 't', 'r')[2]['status'], 'succeeded')
        self.assertEqual(runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded'), result)
        with self.assertRaisesRegex(TaskError, 'Run ID already exists'):
            runtime.start(self.sb, 'different-start', 't', 'different-runner', run_id='r')

    def test_completed_io_is_readable_from_history_after_metadata_retirement(self):
        self.add()
        self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'save-intent', 'external-intent', {'operation_id': 'original'})
        finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertFalse(any(p.startswith('tasks/t/runs/r/') for p in self.sb.snapshot().entries))
        saved = task_io.IOStore(self.sb, 't', finished).optional()
        self.assertEqual(saved.json('events/save-intent.json')['payload']['operation_id'], 'original')

    def test_shallow_clone_fetches_history_before_replaying_retired_operations(self):
        self.add(); self.start(); self.prepare()
        result = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        cold = self.base / 'shallow'
        self.command('git', 'clone', '--depth=1', '--branch=main', self.remote.as_uri(), str(cold))
        self.assertEqual(self.git(cold, 'rev-parse', '--is-shallow-repository'), 'true')
        store = Store(cold)
        self.assertEqual(runtime.publish(store, 'finish', 't', 'r', outcome='succeeded'), result)
        self.assertEqual(self.git(cold, 'rev-parse', '--is-shallow-repository'), 'false')
        with self.assertRaisesRegex(TaskError, 'Run ID already exists'):
            runtime.start(store, 'reuse', 't', 'another', run_id='r')

    def test_finish_readback_reconciles_after_another_collector_retires_receipt(self):
        self.add(); self.start(); self.prepare()
        push = self.sa.push
        def lose(ref, candidate, expected, **kwargs):
            result = push(ref, candidate, expected, **kwargs)
            if ref == self.sa.ref:
                maintenance.retire_metadata(self.sb, 't', 'r', apply=True)
                raise GitFailure('lost finish reply', {'definite_rejection': False})
            return result
        with patch.object(self.sa, 'push', side_effect=lose):
            finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(finished['status'], 'succeeded')
        self.assertIsNone(self.sb.snapshot().json(CONTROL + '/operations/finish.json'))
        self.assertEqual(runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded'), finished)

    def test_retirement_failure_keeps_metadata_and_lost_reply_is_reconciled(self):
        self.add(); self.start(); self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        before = self.sa.fetch()
        with patch.object(self.sa, 'push', side_effect=GitFailure('offline', {'definite_rejection': False})):
            with self.assertRaisesRegex(TaskError, 'unconfirmed'):
                maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        self.assertEqual(self.sa.fetch(), before)
        push = self.sa.push
        def lose(*args, **kwargs):
            push(*args, **kwargs)
            raise GitFailure('lost retirement reply', {'definite_rejection': False})
        with patch.object(self.sa, 'push', side_effect=lose):
            self.assertEqual(maintenance.retire_metadata(self.sa, 't', 'r', apply=True)['status'], 'retired')
        after = self.sa.fetch()
        self.assertEqual(maintenance.retire_metadata(self.sa, 't', 'r', apply=True)['status'], 'absent')
        self.assertEqual(self.sa.fetch(), after)

    def test_policy_transition_waits_until_existing_writers_finish(self):
        self.add(); self.start(); self.add('other')
        snapshot = self.sa.snapshot()
        config = snapshot.json(CONTROL + '/config.json')
        candidate = self.sa.commit(snapshot.commit, {CONTROL + '/config.json': encode(dict(config, schema_version=2))}, 'Old writer fixture')
        self.sa.push(self.sa.ref, candidate, snapshot.commit)
        with self.assertRaisesRegex(TaskError, 'Finish or reconcile existing runs'):
            self.start('other-run', task='other')
        self.assertEqual(self.sa.fetch(), candidate)
        self.prepare()
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.json(CONTROL + '/config.json')['schema_version'], 9)
        self.assertEqual(snapshot.json('tasks/t/runtime.json')['slots']['1'], {'generation': 1, 'run_id': None})
        self.assertNotIn('tasks/t/runs/r/run.json', snapshot.entries)
        self.assertEqual(self.start('other-run', task='other')['status'], 'running')

    def test_running_lost_and_discarded_runs_keep_recovery_metadata(self):
        self.add(); self.start(); self.prepare()
        with self.assertRaisesRegex(TaskError, 'confirmed terminal'):
            maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='failed', discard_output=True)
        with self.assertRaisesRegex(TaskError, 'Unadopted output'):
            maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        self.assertIsNotNone(self.sa.snapshot().json('tasks/t/runs/r/run.json'))
        self.start('lost'); self.prepare('lost')
        runtime.recover(self.sa, 'recover', 't', 'lost', stopped=True, settled=True, evidence='fixture:stopped')
        with self.assertRaisesRegex(TaskError, 'confirmed terminal'):
            maintenance.retire_metadata(self.sa, 't', 'lost', apply=True)
        self.assertIsNotNone(self.sa.snapshot().json('tasks/t/runs/lost/run.json'))

    def test_merge_deletion_preserves_recovery_run_enumeration(self):
        self.add(); self.start(); self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        before = self.sa.fetch()
        maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        retired = self.sa.fetch()
        # A provider can merge a cleanup branch; deletion then occurs on first parent.
        tree = self.git(self.a, 'rev-parse', retired + '^{tree}')
        merged = self.command('git', '-C', str(self.a), 'commit-tree', tree, '-p', before, '-p', retired,
                              input='Merge metadata cleanup\n').stdout.strip()
        snapshot = Snapshot(self.sa, merged)
        self.assertIsNone(snapshot.json('tasks/t/runs/r/run.json'))
        self.assertEqual([(r['run_id'], r['status']) for r in runtime.list_runs(snapshot, 't')], [('r', 'succeeded')])

    def test_run_history_on_second_merge_parent_still_prevents_duplicate_execution(self):
        self.check_merged_history(branch_after_start=False)

    def test_merge_with_a_running_first_parent_recovers_the_terminal_record(self):
        self.check_merged_history(branch_after_start=True)

    def check_merged_history(self, *, branch_after_start):
        self.add()
        base = self.sa.fetch()
        original = self.start(); self.prepare()
        if branch_after_start:
            base = self.sa.fetch()
        feature = self.sa.commit(base, {'feature.txt': b'feature\n'}, 'Independent feature')
        result = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        retired = self.sa.fetch()
        combined = self.sa.commit(retired, {'feature.txt': b'feature\n'}, 'Combined tree')
        tree = self.git(self.a, 'rev-parse', combined + '^{tree}')
        merged = self.command('git', '-C', str(self.a), 'commit-tree', tree, '-p', feature, '-p', retired,
                              input='Merge main into feature\n').stdout.strip()
        self.sa.push(self.sa.ref, merged, retired)
        snapshot = self.sb.snapshot()
        self.assertEqual(runtime.load_run(snapshot, 't', 'r')[2]['status'], 'succeeded')
        self.assertEqual([r['run_id'] for r in runtime.list_runs(snapshot, 't')], ['r'])
        self.assertEqual(self.start(store=self.sb), original)
        self.assertEqual(runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded'), result)
        with self.assertRaisesRegex(TaskError, 'Run ID already exists'):
            runtime.start(self.sb, 'new-start', 't', 'new-runner', run_id='r')
        self.assertEqual(self.sb.fetch(), merged)
