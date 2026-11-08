"""Exercise the public CLI with real repositories and sanitized failure output."""
from contextlib import redirect_stderr
import io
import json
import sys
import unittest
from unittest.mock import patch

from task_runtime_test import ROOT, RuntimeCase, cli_json_lines
from task_runtime import cli, completion, runtime
from task_runtime.store import GitFailure, TaskError
from task_runtime.transport import TransportError


class CliTest(RuntimeCase):
    def invoke(self, *argv, operation='cli-operation', expected=0):
        output = io.BytesIO()
        errors = io.StringIO()
        stdout = io.TextIOWrapper(output, encoding='utf-8')
        args = ['--repo', str(self.b)]
        if operation is not None:
            args += ['--operation-id', operation]
        with patch('sys.stdout', stdout), redirect_stderr(errors):
            code = cli.main(args + list(argv))
        stdout.flush()
        self.assertEqual(code, expected, errors.getvalue())
        raw = output.getvalue()
        diagnostics = cli_json_lines(errors.getvalue())
        stdout.detach()
        return json.loads(raw) if raw else None, diagnostics

    def test_task_commands_read_body_files_edit_archive_and_show_runtime(self):
        body = self.base / 'instructions.md'
        body.write_text('日本語の指示\n')
        task, diagnostics = self.invoke('task', 'add', '--id', 't', '--title', 'Task',
                                        '--body-file', str(body), '--priority', '2', '--max-runners', '2',
                                        operation='cli-add')
        self.assertEqual((task['priority'], task['execution']['max_runners']), (2, 2))
        self.assertEqual(diagnostics, [{'operation_id': 'cli-add'}])
        shown, diagnostics = self.invoke('task', 'show', '--id', 't')
        self.assertEqual(shown['body'], body.read_text())
        self.assertEqual(shown['runtime']['slots'], {})
        self.assertEqual(diagnostics, [])
        body.write_text('Updated instructions')
        edited, _ = self.invoke('task', 'edit', '--id', 't', '--expected-revision', '1',
                                '--title', 'Edited', '--body-file', str(body), '--priority', '1',
                                '--max-runners', '1', '--status', 'completed', '--archive', operation='cli-edit')
        self.assertIsNotNone(edited['archived_at'])
        listed, _ = self.invoke('task', 'list')
        self.assertEqual(listed['tasks'], [])
        listed, _ = self.invoke('task', 'list', '--all')
        self.assertEqual([t['title'] for t in listed['tasks']], ['Edited'])
        self.invoke('task', 'edit', '--id', 't', '--expected-revision', '2', '--unarchive',
                    '--status', 'open', operation='cli-reopen')
        shown, _ = self.invoke('task', 'show', '--id', 't')
        self.assertEqual(shown['body'], 'Updated instructions')
        self.assertIsNone(shown['task']['archived_at'])

    def test_inbox_run_completion_and_cleanup_commands_work_across_clones(self):
        self.add()
        payload = self.base / 'payload.json'
        payload.write_text(json.dumps({'request': 'one'}))
        self.invoke('io', 'send', '--task', 't', '--message-id', 'one', '--kind', 'work',
                    '--payload-file', str(payload), operation='cli-send')
        pending, _ = self.invoke('io', 'pending', '--task', 't')
        self.assertEqual(pending['messages'][0]['payload'], {'request': 'one'})
        wake, _ = self.invoke('io', 'wake', '--task', 't', '--runner', 'worker', '--run-id', 'r', operation='cli-wake')
        self.assertEqual(wake['action'], 'start')
        for action in ('show', 'check', 'observe'):
            shown, _ = self.invoke('run', action, '--task', 't', '--run-id', 'r')
            self.assertEqual(shown['run_id'], 'r')
        work = self.base / 'cli-work'
        self.invoke('run', 'prepare', '--task', 't', '--run-id', 'r', '--path', str(work))
        self.invoke('run', 'heartbeat', '--task', 't', '--run-id', 'r', '--progress', 'working', operation='cli-beat')
        emitted, _ = self.invoke('io', 'emit', '--task', 't', '--run-id', 'r', '--event-id', 'intent',
                                 '--kind', 'intent', '--payload-file', str(payload), operation='cli-emit')
        self.assertEqual(emitted['payload'], {'request': 'one'})
        self.output(work, 'tasks/t/data/report.txt', b'result')
        self.invoke('run', 'publish', '--task', 't', '--run-id', 'r', '--summary', 'Saved', operation='cli-publish')
        payload.write_text(json.dumps({'status': 'succeeded', 'output': 'report.txt'}))
        self.invoke('io', 'complete', '--task', 't', '--run-id', 'r', '--message-id', 'one',
                    '--result-file', str(payload), operation='cli-result')
        status, _ = self.invoke('io', 'status', '--task', 't', '--message-id', 'one')
        self.assertEqual(status['status'], 'succeeded')
        self.invoke('run', 'finish', '--task', 't', '--run-id', 'r', '--outcome', 'succeeded', operation='cli-finish')
        self.assertEqual(self.sa.snapshot().read('tasks/t/data/report.txt'), b'result')
        self.assertFalse(work.exists())
        self.invoke('run', 'cleanup', '--task', 't', '--run-id', 'r', '--apply',
                    '--confirm-stopped', operation='cli-cleanup')

    def test_start_completion_file_and_generated_operation_identity(self):
        self.add()
        contract = self.base / 'contract.json'
        contract.write_text(json.dumps({'required': ['git'], 'context': {'request': 'one'}}))
        run, diagnostics = self.invoke('run', 'start', '--task', 't', '--runner', 'worker', '--run-id', 'r',
                                       '--machine', 'fixture', '--item-key', 'one',
                                       '--completion-contract-file', str(contract), operation=None)
        generated = diagnostics[0]['operation_id']
        self.assertEqual(len(generated), 32)
        self.assertEqual((run['machine_id'], run['item_key']), ('fixture', 'one'))
        self.assertTrue(run['committed'])
        value = completion.receipt(run, {'git': {'status': 'succeeded', 'operation_id': 'saved',
                                                 'evidence': {'commit': 'durable'}}}, stopped=True)
        record = self.base / 'completion.json'
        record.write_text(json.dumps(value))
        saved, _ = self.invoke('run', 'completion', '--task', 't', '--run-id', 'r',
                               '--record-file', str(record), operation='cli-completion')
        self.assertEqual(saved['status'], 'succeeded')
        self.assertEqual(runtime.check(self.sa, 't', 'r')['completion']['steps'], value['steps'])
        self.invoke('run', 'prepare', '--task', 't', '--run-id', 'r', '--path', str(self.base / 'completion-work'))
        finished, _ = self.invoke('run', 'finish', '--task', 't', '--run-id', 'r', '--outcome', 'succeeded',
                                  '--completion-file', str(record), operation='cli-finish-with-evidence')
        self.assertEqual(finished['completion']['steps'], value['steps'])

    def test_exec_reports_nonzero_exit_and_keeps_child_output_in_logs(self):
        self.add()
        self.start()
        work = self.base / 'cli-work'
        self.invoke('run', 'prepare', '--task', 't', '--run-id', 'r', '--path', str(work))
        result, _ = self.invoke('run', 'exec', '--task', 't', '--run-id', 'r', '--', sys.executable, '-c',
                                'import sys; print("worker output"); sys.exit(7)', expected=1)
        self.assertEqual((result['phase'], result['exit_code']), ('exited', 7))
        marker = runtime.local_state(self.sb, 't', 'r').with_suffix('.execution.json')
        self.assertIn('worker output', marker.with_suffix('.stdout.log').read_text())

    def test_recovery_policy_reap_and_confirmed_recovery(self):
        self.add()
        self.invoke('task', 'recovery-policy', '--id', 't', '--expected-revision', '1',
                    '--mode', 'git_only', '--confirm-writers-drained', operation='cli-policy')
        self.start()
        plan, _ = self.invoke('run', 'reap', '--task', 't', '--observer-profile', 'stateless', expected=2)
        self.assertTrue(plan['needs_attention'])
        recovered, _ = self.invoke('run', 'recover', '--task', 't', '--run-id', 'r', '--confirm-stopped',
                                   '--confirm-operations-settled', '--evidence', 'fixture:stopped', operation='cli-recover')
        self.assertEqual(recovered['status'], 'lost')
        self.assertIsNone(runtime.load_task(self.sa.snapshot(), 't')[1]['slots']['1']['run_id'])

    def test_transport_check_and_idempotent_initialization(self):
        check, errors = self.invoke('transport-check')
        self.assertTrue(check['supported'])
        self.assertEqual(errors, [])
        initialized, _ = self.invoke('init', operation='cli-init')
        self.assertEqual(initialized['repository_id'], self.sa.snapshot().json('.task-system/config.json')['repository_id'])

    def test_lease_commands_reject_ineligible_legacy_runs(self):
        self.add(); self.start()
        for action, options, expected in [
                ('renew', [], 'does not use renewable recovery'),
                ('checkpoint', ['--checkpoint-id', 'saved', '--computation-id', 'compute',
                                '--input-digest', 'a' * 64, '--artifact', 'missing'], 'original fenced computation'),
                ('deliver-checkpoint', [], 'No matching durable continuation')]:
            with self.subTest(action=action):
                _, diagnostics = self.invoke('run', action, '--task', 't', '--run-id', 'r',
                                              *options, operation='cli-' + action, expected=1)
                self.assertIn(expected, diagnostics[-1]['error'])
        self.assertEqual(runtime.check(self.sa, 't', 'r')['status'], 'running')

    def test_recover_invokes_installed_stop_adapter(self):
        self.add(); self.start()
        adapter = self.base / 'stop-adapter'
        adapter.write_text('#!' + sys.executable + '\nimport json,sys\nrun=json.load(sys.stdin)\n'
                           'run.update(writer_stopped=True, children_stopped=True, external_operations_settled=True, '
                           'pending_git_operations_settled=True, evidence="fixture:verified-stop")\n'
                           'print(json.dumps(run))\n')
        adapter.chmod(0o755)
        recovered, _ = self.invoke('run', 'recover', '--task', 't', '--run-id', 'r',
                                   '--stop-check', str(adapter), operation='cli-stop')
        self.assertEqual(recovered['recovery_evidence'], 'fixture:verified-stop')

    def test_module_entry_point_exposes_the_same_version_as_the_script(self):
        result = self.command(sys.executable, '-c',
                              'import runpy,sys; sys.path.insert(0,sys.argv.pop(1)); '
                              'runpy.run_module("task_runtime.cli",run_name="__main__")',
                              str(ROOT / 'foundation'), '--version')
        script = self.command(sys.executable, str(ROOT / 'bin/task-inbox'), '--version')
        self.assertEqual(result.stdout, script.stdout)


class CliDiagnosticsTest(unittest.TestCase):
    def invoke_error(self, error):
        errors = io.StringIO()
        output = io.BytesIO()
        stdout = io.TextIOWrapper(output, encoding='utf-8')
        with patch.object(cli, 'dispatch', side_effect=error), patch('sys.stdout', stdout), redirect_stderr(errors):
            code = cli.main(['task', 'list'])
        stdout.flush()
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), b'')
        stdout.detach()
        return cli_json_lines(errors.getvalue())[0]

    def test_filesystem_json_and_local_state_errors_hide_sensitive_details(self):
        for error in (OSError('PRIVATE_TOKEN'), ValueError('PRIVATE_TOKEN'),
                      KeyError('PRIVATE_TOKEN'), TypeError('PRIVATE_TOKEN')):
            with self.subTest(error=type(error).__name__):
                diagnostic = self.invoke_error(error)
                self.assertNotIn('PRIVATE_TOKEN', json.dumps(diagnostic))
                self.assertIn('preserve run state', diagnostic['error'])

    def test_runtime_and_transport_errors_keep_machine_readable_details(self):
        self.assertEqual(self.invoke_error(TaskError('Task not found')), {'error': 'Task not found'})
        self.assertEqual(self.invoke_error(GitFailure('Git operation failed', {'definite_rejection': False})),
                         {'error': 'Git operation failed'})
        error = TransportError('unsupported_transport', 'preflight', missing=['conditional_delete'])
        diagnostic = self.invoke_error(error)
        self.assertEqual(diagnostic['code'], 'unsupported_transport')
        self.assertEqual(diagnostic['operation'], 'preflight')
        self.assertEqual(diagnostic['missing_capabilities'], ['conditional_delete'])

    def test_parser_requires_commands_identifiers_and_exclusive_archive_flags(self):
        for args in ([], ['task'], ['run', 'start', '--task', 't'],
                     ['io', 'result', '--task', 't'], ['io', 'ack', '--task', 't'],
                     ['io', 'send', '--task', 't', '--message-id', 'one'],
                     ['task', 'edit', '--id', 't', '--expected-revision', '1', '--archive', '--unarchive']):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                cli.parser().parse_args(args)
            self.assertEqual(raised.exception.code, 2)
