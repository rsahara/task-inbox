"""Portable contracts, readiness, continuation and durable launch recovery."""
import concurrent.futures
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import task_runtime_test as fixtures
import task_lease_test as shared
from task_runtime import execution, interfaces, leases, lifecycle, runtime, signals, task_io
from task_runtime.store import GitFailure, TaskError, atomic_json, digest


def declaration(readiness='inbox', inbox=None, reads=None):
    return dict(schema_version=1, inbox=inbox if inbox is not None else ['work', 'signal-changed'],
                signals=dict(reads=reads or [], writes=['report']), readiness=readiness)


class ExecutionTest(fixtures.RuntimeCase):
    def setUp(self):
        super().setUp()
        fixture = fixtures.ROOT / 'test/fixtures/task_execution_worker.py'
        self.worker = self.base / 'installed-worker'
        self.worker.write_text(fixture.read_text().replace('__RUNTIME_FOUNDATION__', str(fixtures.ROOT / 'foundation')))
        self.worker.chmod(0o700)

    def setup_tasks(self, *tasks):
        for task_id in tasks or ('t',):
            self.add(task_id, interface=declaration())
        signals.enable(self.sa, 'signals')

    def send(self, task='t', mid='one', kind='work'):
        return task_io.send(self.sa, 'send-' + task + '-' + mid, task, mid, kind, {})

    def execute(self, operation='invocation', tasks=None, store=None, **kwargs):
        return execution.execute(store or self.sa, operation, tasks or ['t'], 'worker', str(self.worker), **kwargs)

    def script(self, text):
        self.worker.write_text('#!' + sys.executable + '\n' + text)
        return self.worker

    def checker(self):
        path = self.base / 'installed-condition'
        path.write_text('#!' + sys.executable + '\nimport json,sys\nr=json.load(sys.stdin)\n'
                        'json.dump(dict(protocol=r["protocol"],request_id=r["request_id"],'
                        'ready=bool(r["inputs"]["signals"]),reason="saved_inputs"),sys.stdout)\n')
        path.chmod(0o700)
        return str(path)

    def test_interface_edit_is_revisioned_and_old_tasks_are_manual(self):
        self.add()
        self.assertEqual(execution.plan(self.sa, ['t'])['decisions'][0]['reason'], 'manual_only')
        runtime.edit_task(self.sa, 'declare', 't', 1, interface=declaration())
        self.assertEqual(runtime.load_task(self.sb.snapshot(), 't')[0]['interface'], declaration())
        with self.assertRaisesRegex(TaskError, 'changed'):
            runtime.edit_task(self.sb, 'stale', 't', 1, interface=declaration())
        invalid = declaration(); invalid['signals']['reads'] = [{'consumer': 'other'}]
        with self.assertRaises(TaskError):
            interfaces.validate(invalid)

    def test_check_is_read_only_and_unknown_messages_remain_pending(self):
        self.setup_tasks(); self.send(kind='unsupported')
        before = self.sa.fetch()
        plan = execution.plan(self.sb, ['t'])
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(plan['decisions'][0]['ignored_message_ids'], ['one'])
        result = self.execute()
        self.assertEqual(result['steps'], [])
        self.assertEqual(len(task_io.pending(self.sb, 't')['messages']), 1)

    def test_single_entrypoint_prepares_supervises_finishes_and_replays_across_clones(self):
        self.setup_tasks(); self.send()
        result = self.execute()
        run_id = result['steps'][0]['run_id']
        self.assertEqual(result['steps'][0]['outcome'], 'succeeded')
        self.assertEqual(task_io.pending(self.sb, 't')['messages'], [])
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', run_id)[2]['publication_count'], 2)
        self.assertEqual(self.execute(store=self.sb)['steps'], result['steps'])
        with self.assertRaisesRegex(TaskError, 'another request'):
            self.execute(continue_tasks=True)

    def test_continuation_is_opt_in_and_signal_delivery_launches_only_allowlisted_tasks(self):
        self.setup_tasks('a', 'b', 'excluded')
        signals.subscribe(self.sa, 'subscribe-b', 'a', 'b')
        signals.subscribe(self.sa, 'subscribe-excluded', 'a', 'excluded')
        self.send('a')
        first = self.execute(tasks=['a', 'b'])
        self.assertEqual([s['task_id'] for s in first['steps']], ['a'])
        self.assertEqual(first['pending_tasks'], ['b'])
        second = self.execute('next', tasks=['a', 'b'], continue_tasks=True)
        self.assertEqual([s['task_id'] for s in second['steps']], ['b'])
        self.assertEqual(second['reason'], 'no_ready_task')
        self.assertEqual(len(task_io.pending(self.sa, 'excluded')['messages']), 1)
        self.assertEqual(runtime.list_runs(self.sa.snapshot(), 'excluded'), [])

    def test_one_invocation_can_chain_signals_and_each_task_gets_its_own_grant(self):
        self.setup_tasks('a', 'b'); signals.subscribe(self.sa, 'subscribe', 'a', 'b'); self.send('a')
        result = self.execute(tasks=['a', 'b'], continue_tasks=True)
        self.assertEqual([s['task_id'] for s in result['steps']], ['a', 'b'])
        self.assertNotEqual(result['steps'][0]['run_id'], result['steps'][1]['run_id'])
        for step in result['steps']:
            run = runtime.load_run(self.sb.snapshot(), step['task_id'], step['run_id'])[2]
            self.assertIn('execution_claim', run)
            self.assertEqual(run['status'], 'succeeded')

    def test_conditions_use_fixed_signals_and_execute_at_most_once_per_invocation(self):
        self.setup_tasks('a', 'b')
        runtime.edit_task(self.sa, 'condition', 'b', 1,
                          interface=declaration('condition', inbox=[], reads=[{'type': 'report', 'task_id': 'a'}]))
        checker = self.checker()
        self.assertFalse(execution.plan(self.sa, ['b'], condition_adapter=checker)['decisions'][0]['ready'])
        self.send('a')
        result = self.execute(tasks=['a', 'b'], continue_tasks=True, condition_adapter=checker)
        self.assertEqual([s['task_id'] for s in result['steps']], ['a', 'b'])
        decision = result['next']['decisions'][1]
        self.assertEqual(decision['reason'], 'already_executed')
        self.assertEqual(decision['signals'][0]['sha256'], signals.catalog(self.sa.snapshot(), 'a')['report']['sha256'])

    def test_missing_condition_adapter_and_unbound_response_never_admit(self):
        self.add(interface=declaration('condition'))
        self.assertEqual(execution.plan(self.sa, ['t'])['decisions'][0]['reason'], 'condition_adapter_required')
        checker = Path(self.checker())
        checker.write_text('#!' + sys.executable + '\nprint("{\\"ready\\":true}")\n')
        with self.assertRaisesRegex(TaskError, 'bind'):
            self.execute(condition_adapter=str(checker))
        self.assertEqual(runtime.list_runs(self.sa.snapshot(), 't'), [])

    def test_force_is_first_task_only_and_does_not_override_pause(self):
        self.add('a'); self.add('b')
        plan = execution.plan(self.sa, ['a', 'b'], force=True)
        self.assertEqual([r['ready'] for r in plan['decisions']], [True, False])
        runtime.edit_task(self.sa, 'pause', 'a', 1, status='paused')
        self.assertEqual(execution.plan(self.sa, ['a'], force=True)['decisions'][0]['reason'], 'paused')

    def test_nonzero_exit_preserves_slot_and_does_not_chain(self):
        self.setup_tasks('a', 'b'); self.send('a'); self.send('b')
        self.script('import sys\nsys.exit(7)\n')
        result = self.execute(tasks=['a', 'b'], continue_tasks=True)
        self.assertTrue(result['needs_attention'])
        self.assertEqual(result['reason'], 'executor_failed_or_interrupted')
        self.assertEqual(runtime.list_runs(self.sa.snapshot(), 'b'), [])
        self.assertEqual(len(task_io.pending(self.sa, 'a')['messages']), 1)
        self.assertEqual(self.execute(tasks=['a', 'b'], continue_tasks=True)['reason'], result['reason'])

    def test_successful_process_without_bound_result_cannot_finish(self):
        self.setup_tasks(); self.send(); self.script('pass\n')
        with self.assertRaisesRegex(TaskError, 'result file'):
            self.execute()
        run = runtime.list_runs(self.sa.snapshot(), 't')[0]
        self.assertEqual(run['status'], 'running')
        self.assertEqual(len(task_io.pending(self.sa, 't')['messages']), 1)

    def test_ambiguous_launch_is_never_relaunched_from_another_clone(self):
        self.setup_tasks(); self.send()
        original = lifecycle.execute

        def interrupted(*args):
            result = original(*args)
            raise TaskError('Simulated controller interruption')

        with patch.object(lifecycle, 'execute', side_effect=interrupted):
            with self.assertRaisesRegex(TaskError, 'interruption'):
                self.execute()
        result = self.execute(store=self.sb)
        self.assertEqual(result['reason'], 'execution_outcome_unknown')
        self.assertEqual(len(runtime.list_runs(self.sa.snapshot(), 't')), 1)
        # The original controller can read its exited marker and finish without
        # restarting the worker or repeating its external operations.
        result = self.execute()
        self.assertEqual(result['steps'][0]['outcome'], 'succeeded')

    def test_finish_failure_retries_after_worker_success_without_relaunch(self):
        self.setup_tasks(); self.send()
        original = runtime.publish

        def fail(*args, **kwargs):
            if kwargs.get('outcome'):
                raise GitFailure('Simulated save failure', {'definite_rejection': True})
            return original(*args, **kwargs)

        with patch.object(runtime, 'publish', side_effect=fail):
            with self.assertRaises(GitFailure):
                self.execute()
        with patch.object(lifecycle, 'execute', side_effect=AssertionError('Worker relaunched')):
            result = self.execute()
        self.assertEqual(result['steps'][0]['outcome'], 'succeeded')

    def test_arrival_between_empty_check_and_slot_release_is_visible_after_finish(self):
        self.setup_tasks(); self.send()
        original = runtime.publish

        def arrive(*args, **kwargs):
            if kwargs.get('outcome'):
                self.send(mid='late')
            return original(*args, **kwargs)

        with patch.object(runtime, 'publish', side_effect=arrive):
            result = self.execute()
        self.assertEqual(result['pending_tasks'], ['t'])
        self.assertEqual([m['id'] for m in task_io.pending(self.sb, 't')['messages']], ['late'])

    def test_independent_callers_cannot_launch_the_same_task_twice(self):
        self.setup_tasks(); self.send()
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda pair: self.execute(pair[0], store=pair[1]),
                                    [('caller-a', self.sa), ('caller-b', self.sb)]))
        self.assertEqual(sum(len(r['steps']) for r in results), 1)
        self.assertEqual(len(runtime.list_runs(self.sa.snapshot(), 't')), 1)

    def test_executor_path_and_continuation_limits_are_caller_owned(self):
        self.setup_tasks()
        with self.assertRaisesRegex(TaskError, '--continue'):
            self.execute(max_tasks=2)
        with self.assertRaisesRegex(TaskError, 'installed executable'):
            execution.execute(self.sa, 'bad', ['t'], 'worker', 'relative-command')
        with self.assertRaises(TaskError):
            execution.plan(self.sa, ['t', 't'])
        with self.assertRaises(TaskError):
            self.execute(max_tasks=0)

    def test_completion_contract_is_bound_at_admission_and_verified_at_finish(self):
        self.setup_tasks(); self.send()
        result = self.execute(completion_contracts={'t': {'required': ['git'], 'context': {'input': 'synthetic'}}})
        run = runtime.load_run(self.sb.snapshot(), 't', result['steps'][0]['run_id'])[2]
        self.assertEqual(run['completion']['status'], 'succeeded')
        self.assertEqual(run['completion_contract']['required'], ['git'])
        with self.assertRaises(TaskError):
            self.execute('other', completion_contracts={'outside': {'required': ['git']}})

    def test_late_arrival_is_processed_by_a_separate_run_within_the_bound(self):
        self.setup_tasks(); self.send()
        original = runtime.publish
        arrivals = []

        def arrive(*args, **kwargs):
            if kwargs.get('outcome') and not arrivals:
                arrivals.append(self.send(mid='late'))
            return original(*args, **kwargs)

        with patch.object(runtime, 'publish', side_effect=arrive):
            result = self.execute(continue_tasks=True, max_tasks=2)
        self.assertEqual([s['task_id'] for s in result['steps']], ['t', 't'])
        self.assertEqual(result['pending_tasks'], [])
        self.assertEqual(task_io.pending(self.sb, 't')['messages'], [])

    def test_cli_has_one_entry_for_check_execute_and_interface_updates(self):
        def invoke(*args):
            result = self.command(sys.executable, str(fixtures.ROOT / 'bin/task-inbox'),
                                  '--repo', str(self.b), *args)
            return json.loads(result.stdout), fixtures.cli_json_lines(result.stderr)

        interface = self.base / 'interface.json'; interface.write_text(json.dumps(declaration()))
        invoke('--operation-id', 'cli-add', 'task', 'add', '--id', 't', '--title', 'Task',
               '--interface-file', str(interface))
        signals.enable(self.sa, 'signals'); self.send()
        before = self.sa.fetch()
        checked, diagnostics = invoke('execute', '--task', 't', '--check')
        self.assertEqual(diagnostics, [])
        self.assertEqual(checked['commit'], before)
        self.assertTrue(checked['decisions'][0]['ready'])
        executed, diagnostics = invoke('--operation-id', 'cli-invocation', 'execute', '--task', 't',
                                       '--runner', 'cli', '--executor', str(self.worker))
        self.assertEqual(executed['steps'][0]['outcome'], 'succeeded')
        self.assertEqual(diagnostics[0]['operation_id'], 'cli-invocation')
        edited, _ = invoke('--operation-id', 'cli-edit', 'task', 'edit', '--id', 't',
                           '--expected-revision', '1', '--interface-file', str(interface))
        self.assertEqual(edited['revision'], 2)

    def test_saved_finish_with_lost_reply_is_read_back_without_relaunch(self):
        self.setup_tasks(); self.send()
        original = runtime.publish

        def lost_reply(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get('outcome'):
                raise TaskError('Simulated lost finish response')
            return result

        with patch.object(runtime, 'publish', side_effect=lost_reply):
            with self.assertRaisesRegex(TaskError, 'lost finish'):
                self.execute()
        with patch.object(lifecycle, 'execute', side_effect=AssertionError('Worker relaunched')):
            result = self.execute(store=self.sb)
        self.assertEqual(result['steps'][0]['outcome'], 'succeeded')
        self.assertEqual(task_io.pending(self.sb, 't')['messages'], [])

    def test_admission_rechecks_inbox_after_another_worker_consumed_selected_inputs(self):
        self.setup_tasks(); self.send()
        original = runtime.start
        arrivals = []

        def competing_worker(*args, **kwargs):
            if kwargs.get('admission_check') and not arrivals:
                arrivals.append(True)
                original(self.sb, 'competitor', 't', 'other', run_id='other')
                self.prepare(run='other', store=self.sb)
                task_io.complete(self.sb, 'competing-ack', 't', 'other', 'one', {'status': 'succeeded'})
                runtime.publish(self.sb, 'competing-finish', 't', 'other', outcome='succeeded')
            return original(*args, **kwargs)

        with patch.object(runtime, 'start', side_effect=competing_worker):
            result = self.execute()
        self.assertEqual(result['reason'], 'inbox_empty')
        self.assertEqual(result['steps'], [])
        self.assertEqual(len(runtime.list_runs(self.sa.snapshot(), 't')), 1)

    def test_signal_selectors_plan_current_state_once_with_multiple_notice_revisions(self):
        self.setup_tasks('a', 't')
        runtime.edit_task(self.sa, 'selectors', 't', 1,
                          interface=declaration(reads=[{'type': 'report', 'task_id': 'a'}]))
        signals.subscribe(self.sa, 'subscribe', 'a', 't')
        self.start('source', task='a'); self.prepare('source', task='a')
        value = dict(schema_version=1, task_id='a', id='one', type='report', status='active', revision=1)
        signals.put(self.sa, 'a', 'source', value)
        signals.put(self.sa, 'a', 'source', dict(value, id='other', type='unsupported'))
        runtime.publish(self.sa, 'first', 'a', 'source')
        signals.put(self.sa, 'a', 'source', dict(value, revision=2, status='resolved', resolution='verified'))
        runtime.publish(self.sa, 'second', 'a', 'source', outcome='succeeded')
        plan = execution.plan(self.sb, ['t'])
        row = plan['decisions'][0]
        self.assertEqual(len(row['message_ids']), 2)
        self.assertEqual(len(row['ignored_message_ids']), 1)
        self.assertEqual([(r['id'], r['revision'], r['status']) for r in row['signals']], [('one', 2, 'resolved')])
        self.assertEqual(row['signals'][0]['commit'], plan['commit'])
        self.assertEqual(len(task_io.pending(self.sa, 't')['messages']), 3)

    def test_a_notice_ahead_of_fixed_main_never_admits(self):
        self.setup_tasks('a', 't')
        runtime.edit_task(self.sa, 'selectors', 't', 1,
                          interface=declaration(reads=[{'type': 'report'}]))
        mid = signals.message_id('a', 'missing', 99)
        task_io.send(self.sa, 'ahead', 't', mid, signals.KIND,
                     dict(schema_version=1, task_id='a', signal_id='missing', revision=99))
        with self.assertRaisesRegex(TaskError, 'ahead'):
            self.execute()
        self.assertEqual(runtime.list_runs(self.sa.snapshot(), 't'), [])

    def test_readiness_does_not_detect_execution_host(self):
        self.setup_tasks(); self.send()
        expected = execution.plan(self.sa, ['t'])
        for values in ({'GITHUB_ACTIONS': 'true'}, {'CODEX_CLOUD': '1'}, {'CODEX_LOCAL': '1'}):
            with patch.dict(os.environ, values):
                self.assertEqual(execution.plan(self.sa, ['t']), expected)

    def test_failed_terminal_result_stops_continuation(self):
        self.setup_tasks('a', 'b'); self.send('a'); self.send('b')
        self.script('import json,sys\nfrom pathlib import Path\nc=json.loads(Path(sys.argv[1]).read_text())\n'
                    'r={k:c["run"][k] for k in ("task_id","run_id","generation")}\n'
                    'r.update(protocol=c["protocol"],outcome="failed",summary="Verified failure")\n'
                    'Path(c["result_path"]).write_text(json.dumps(r))\n')
        result = self.execute(tasks=['a', 'b'], continue_tasks=True)
        self.assertTrue(result['needs_attention'])
        self.assertEqual(result['steps'][0]['outcome'], 'failed')
        self.assertEqual(runtime.list_runs(self.sa.snapshot(), 'b'), [])

    def test_result_generation_is_an_integer_bound_to_the_admitted_run(self):
        run = dict(task_id='t', run_id='r', generation=1)
        path = self.base / 'result.json'
        result = dict(run, protocol=execution.PROTOCOL, outcome='succeeded', summary='Verified')
        for bad in (True, 2, '1', None):
            atomic_json(path, dict(result, generation=bad))
            with self.assertRaisesRegex(TaskError, 'bind'):
                execution.worker_result(path, run)
        atomic_json(path, result)
        self.assertEqual(execution.worker_result(path, run), result)

    def test_configured_lease_recovery_is_reused_without_reaping_on_old_intent_replay(self):
        for name, mode in [('task_lease_adapter.py', leases.MODE), ('task_timeout_adapter.py', leases.GENERATION)]:
            with self.subTest(mode=mode):
                case = shared.LeaseCase()
                case.adapter = fixtures.ROOT / 'test/fixtures' / name
                case.mode = mode
                case.setUp(); self.addCleanup(case.doCleanups)
                c = case.c
                runtime.edit_task(c.sa, 'interface', 't', 1, interface=declaration())
                signals.enable(c.sa, 'signals')
                leases.configure(c.sa, 'lease', 't', 2, mode=mode, profile='test', drained=True)
                c.start('expired')
                task_io.send(c.sa, 'input', 't', 'one', 'work', {})
                case.settings(now_ms=1300001)
                result = execution.execute(c.sb, 'lease-invocation', ['t'], 'worker', str(self.worker))
                self.assertEqual(result['steps'][0]['outcome'], 'succeeded')
                self.assertEqual(runtime.load_run(c.sa.snapshot(), 't', 'expired')[2]['status'], 'lost')
                c.start('replacement')
                before = runtime.load_run(c.sa.snapshot(), 't', 'replacement')[2]
                case.settings(now_ms=1700002)
                execution.execute(c.sa, 'lease-invocation', ['t'], 'worker', str(self.worker))
                self.assertEqual(runtime.load_run(c.sa.snapshot(), 't', 'replacement')[2], before)


if __name__ == '__main__':
    unittest.main()
