"""Compact records preserve history, migration barriers and heartbeat identity."""
from unittest.mock import patch

from task_runtime_test import RuntimeCase
from task_runtime import runtime
from task_runtime.store import CONTROL, TaskError, encode


class SlimRuntimeTest(RuntimeCase):
    def test_finish_has_one_result_source_and_keeps_historical_operation_snapshots(self):
        self.add(); self.start(); work = self.prepare()
        self.output(work, 'tasks/t/data/latest.json', b'{"artifact":"runs/r/artifacts/report.txt"}')
        self.output(work, 'tasks/t/runs/r/artifacts/report.txt', b'one report body')
        published = runtime.publish(self.sa, 'publish', 't', 'r', summary='intermediate')
        with patch('task_runtime.maintenance.maintain'):
            finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded', summary='done')
        snapshot = self.sb.snapshot()
        self.assertIsNone(snapshot.read('tasks/t/runs/r/result.json'))
        self.assertEqual(snapshot.json('tasks/t/runs/r/run.json')['summary'], 'done')
        self.assertNotIn('data_revision', snapshot.json('tasks/t/runtime.json'))
        self.assertEqual(snapshot.json('tasks/t/runtime.json')['slots']['1'], {'generation': 1, 'run_id': None})
        self.assertEqual(runtime.publish(self.sb, 'publish', 't', 'r', summary='intermediate'), published)
        self.assertEqual(runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded', summary='done'), finished)

    def test_schema8_migration_waits_for_all_tasks_and_retries_after_drain(self):
        self.add(); self.add('other')
        snapshot = self.sa.snapshot()
        config = dict(snapshot.json(CONTROL + '/config.json'), schema_version=8, workspace_provenance_protocol=1)
        self.sa.push(self.sa.ref, self.sa.commit(snapshot.commit, {CONTROL + '/config.json': encode(config)}, 'Legacy schema8'), snapshot.commit)
        with patch.object(runtime, 'metadata_policy_changes', return_value={}):
            self.start()
        old = runtime.load_task(self.sa.snapshot(), 't')[1]
        self.assertIn('data_revision', old)
        self.assertIn('runner_id', old['slots']['1'])
        with self.assertRaisesRegex(TaskError, 'Finish or reconcile existing runs'):
            self.start('other', task='other', store=self.sb)
        self.assertEqual(self.sa.snapshot().json(CONTROL + '/config.json')['schema_version'], 8)
        runtime.recover(self.sa, 'recover', 't', 'r', stopped=True, settled=True, evidence='fixture:stopped')
        self.start('other', task='other', store=self.sb)
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.json(CONTROL + '/config.json')['schema_version'], 9)
        self.assertNotIn('data_revision', snapshot.json('tasks/t/runtime.json'))
        self.assertEqual(snapshot.json('tasks/other/runtime.json')['slots']['1'], {'generation': 1, 'run_id': 'other'})
        self.assertEqual(snapshot.json('tasks/t/runs/r/result.json')['evidence'], 'fixture:stopped')
        self.assertEqual(runtime.load_run(snapshot, 't', 'r')[2]['runner_id'], 'runner-r')

    def test_heartbeat_uses_the_same_identity_sequence_and_progress_rules_for_all_runs(self):
        for task in ('one', 'two'):
            self.add(task)
            run = self.start(task, task=task)
            first = runtime.heartbeat(self.sa, task, task, progress='working', operation_id='first-' + task)
            second = runtime.heartbeat(self.sb, task, task, operation_id='second-' + task)
            self.assertEqual((first['sequence'], second['sequence'], second['progress']), (1, 2, 'working'))
            tip = self.sa.fetch(run['heartbeat_ref'])
            corrupt = self.sa.commit(tip, {'state.json': encode(dict(second, generation=999))}, 'Corrupt heartbeat')
            self.sa.push(run['heartbeat_ref'], corrupt, tip)
            with self.assertRaisesRegex(TaskError, 'Heartbeat'):
                runtime.heartbeat(self.sb, task, task, operation_id='third-' + task)
            with self.assertRaisesRegex(TaskError, 'Heartbeat'):
                runtime.observe(self.sa, task, task)
            self.assertEqual(self.sa.remote_tip(run['heartbeat_ref']), corrupt)
