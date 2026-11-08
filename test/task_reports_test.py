"""Retention evidence survives recovery, cleanup failures, and independent clones."""
import importlib.util
import json
import sys
import tarfile
from unittest.mock import patch

from task_runtime_test import ROOT, RuntimeCase
import task_lease_test as lease_fixtures
from task_runtime import leases, maintenance, reports, runtime, task_io
from task_runtime.store import GitFailure, Store, TaskError

legacy_spec = importlib.util.spec_from_file_location('task_runtime._legacy_prepare_fixture',
    ROOT / 'test/fixtures/task_legacy_prepare.py')
legacy = importlib.util.module_from_spec(legacy_spec)
legacy_spec.loader.exec_module(legacy)


class ReportTests(RuntimeCase):
    def checkpoint(self):
        self.add()
        run = self.start('old')
        work = self.prepare('old')
        self.output(work, 'tasks/t/runs/old/artifacts/checkpoint.txt', b'preserved')
        runtime.heartbeat(self.sa, 't', 'old')
        runtime.publish(self.sa, 'checkpoint', 't', 'old')
        return run, work

    def recover(self):
        return runtime.recover(self.sa, 'recover', 't', 'old', stopped=True, settled=True,
                               evidence='fixture:all-writers-and-operations-stopped')

    def test_recovery_saves_notice_atomically_and_replay_preserves_it(self):
        run, work = self.checkpoint()
        heads = {ref: self.sa.remote_tip(ref) for ref in (run['work_ref'], run['heartbeat_ref'])}
        before = self.sa.fetch()
        self.recover()
        snapshot = self.sa.snapshot()
        self.assertEqual(self.git(self.a, 'rev-list', '--count', before + '..' + snapshot.commit), '1')
        path = 'tasks/t/runs/old/retention-report.json'
        notice = snapshot.json(path)
        self.assertEqual(notice['reason'], 'recovery_artifacts_need_inspection')
        self.assertEqual(notice['status'], 'lost')
        self.assertEqual(notice['generation'], run['generation'])
        self.assertEqual({ref['ref'] for ref in notice['refs']}, set(heads))
        self.assertIsNone(snapshot.read(path.replace('.json', '.md')))
        self.assertIn(run['heartbeat_ref'], reports.markdown(reports.inventory(self.sb)))
        self.start('replacement', store=self.sb)
        after_start = self.sa.fetch()
        runtime.recover(self.sb, 'recover', 't', 'old', stopped=True, settled=True,
                        evidence='fixture:all-writers-and-operations-stopped')
        self.assertEqual(self.sa.fetch(), after_start)
        self.assertEqual(self.sa.snapshot().json(path), notice)
        self.assertTrue(work.exists())
        self.assertEqual({ref: self.sa.remote_tip(ref) for ref in heads}, heads)

    def test_inventory_works_from_fresh_clone_without_claiming_remote_workspace_absence(self):
        run, work = self.checkpoint()
        self.recover()
        cold = self.base / 'cold'
        self.command('git', 'clone', '--no-local', '--branch', 'main', str(self.remote), str(cold))
        store = Store(cold)
        before = store.fetch()
        value = reports.inventory(store)
        self.assertEqual(store.fetch(), before)
        self.assertEqual(len(value['runs']), 1)
        row = value['runs'][0]
        self.assertEqual(row['retention_report'], 'tasks/t/runs/old/retention-report.json')
        self.assertEqual({ref['ref'] for ref in row['remote_refs']}, {run['work_ref'], run['heartbeat_ref']})
        self.assertEqual(row['local']['visibility'], 'not_recorded_in_this_clone')
        self.assertNotIn('workspace_exists', row['local'])
        own = reports.inventory(self.sa)['runs'][0]
        self.assertEqual(own['local']['workspace_path'], str(work))
        self.assertTrue(own['local']['workspace_exists'])
        self.assertEqual(value['unattributed_refs'], [])

    def test_cleanup_failure_leaves_local_notice_without_changing_success(self):
        self.add(); self.start(); work = self.prepare()
        self.output(work, 'tasks/t/runs/r/artifacts/out')
        before = self.sa.fetch()
        with patch.object(maintenance, 'delete_ref', side_effect=TaskError('PRIVATE_TOKEN')):
            finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        after = self.sa.fetch()
        self.assertEqual(finished['status'], 'succeeded')
        self.assertEqual(self.git(self.a, 'rev-list', '--count', before + '..' + after), '1')
        value = reports.inventory(self.sa)
        local = value['runs'][0]['local']
        self.assertEqual(local['last_cleanup_notice']['reason'], 'cleanup_blocked_or_unconfirmed')
        self.assertNotIn('PRIVATE_TOKEN', json.dumps(value))
        self.assertNotIn('PRIVATE_TOKEN', reports.markdown(value))
        self.assertTrue(work.exists())
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.git(self.a, 'rev-list', '--count', after + '..' + self.sa.fetch()), '1')
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        note = json.loads((self.sa.local / 'reports/t/r.json').read_text())
        self.assertIn('resolved_at', note)

    def test_local_dirty_workspace_is_reported_after_remote_refs_are_collected(self):
        self.add(); self.start(); work = self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        (work / 'unpreserved').write_text('keep')
        before = self.sa.fetch()
        maintenance.maintain(self.sa, 't')
        value = reports.inventory(self.sa)['runs'][0]
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(value['remote_refs'], [])
        self.assertTrue(value['local']['workspace_exists'])
        self.assertEqual(value['local']['last_cleanup_notice']['reason'], 'workspace_has_unpreserved_files')
        self.assertEqual((work / 'unpreserved').read_text(), 'keep')

    def test_changed_ref_is_recorded_with_actual_head_and_never_deleted(self):
        self.add(); run = self.start(); self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        old = self.sa.fetch(run['work_ref'])
        changed = self.sa.commit(old, {'diagnostic.txt': b'late write'}, 'Late output')
        self.sa.push(run['work_ref'], changed, old)
        before = self.sa.fetch()
        maintenance.maintain(self.sa, 't')
        row = reports.inventory(self.sa, 't', 'r')['runs'][0]
        self.assertEqual(row['reason'], 'ref_changed_after_finish')
        self.assertEqual(row['last_accepted_commit'], old)
        self.assertIn({'ref': run['work_ref'], 'head': changed}, row['remote_refs'])
        self.assertEqual(row['local']['last_cleanup_notice']['reason'], 'ref_changed_after_finish')
        self.assertEqual(self.sa.remote_tip(run['work_ref']), changed)
        self.assertEqual(self.sa.fetch(), before)

    def test_notice_write_failure_does_not_block_collection_or_a_later_start(self):
        self.add(); self.start(); self.prepare()
        with patch.object(reports, 'atomic_json', side_effect=OSError('PRIVATE_TOKEN')):
            with patch.object(maintenance, 'delete_ref', side_effect=TaskError('blocked')):
                self.assertEqual(runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')['status'], 'succeeded')
            self.assertEqual(self.start('next')['status'], 'running')
        self.assertEqual(runtime.load_run(self.sa.snapshot(), 't', 'next')[2]['status'], 'running')

    def test_discarded_finish_records_refs_without_copying_business_output(self):
        self.checkpoint()
        runtime.publish(self.sa, 'discard', 't', 'old', outcome='failed',
                        discard_output=True, summary='PRIVATE_TOKEN')
        snapshot = self.sa.snapshot()
        notice = snapshot.json('tasks/t/runs/old/retention-report.json')
        self.assertEqual(notice['reason'], 'output_not_adopted')
        self.assertNotIn('PRIVATE_TOKEN', json.dumps(notice))
        self.assertEqual(reports.inventory(self.sb)['runs'][0]['status'], 'failed')

    def test_running_and_locally_collected_runs_are_omitted_but_unattributed_refs_are_listed(self):
        self.add(max_runners=2)
        self.start('active'); self.prepare('active')
        runtime.heartbeat(self.sa, 't', 'active')
        self.start('done', store=self.sb); self.prepare('done', store=self.sb)
        runtime.publish(self.sb, 'finish-done', 't', 'done', outcome='succeeded')
        task_io.send(self.sa, 'send', 't', 'pending', 'input', {})
        ref = 'refs/heads/task-runtime/unregistered/orphan/work'
        self.sa.push(ref, self.sa.fetch(), None)
        value = reports.inventory(self.sa)
        self.assertEqual([row['run_id'] for row in value['runs']], ['done'])
        self.assertEqual(value['runs'][0]['local']['visibility'], 'not_recorded_in_this_clone')
        self.assertEqual(reports.inventory(self.sb)['runs'], [])
        self.assertEqual([r['ref'] for r in value['unattributed_refs']], [ref])
        self.assertEqual(reports.inventory(self.sa, 't')['unattributed_refs'], [])

    def test_ref_collection_in_other_clone_does_not_confirm_workspace_cleanup(self):
        self.add(); self.start(); work = self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertTrue(reports.inventory(self.sb)['runs'][0]['remote_refs'])
        maintenance.maintain(self.sb, 't')
        row = reports.inventory(self.sb)['runs'][0]
        self.assertEqual(row['remote_refs'], [])
        self.assertEqual(row['local']['visibility'], 'not_recorded_in_this_clone')
        self.assertNotIn('workspace_exists', row['local'])
        self.assertTrue(work.exists())
        self.assertTrue(reports.inventory(self.sa)['runs'][0]['local']['workspace_exists'])
        maintenance.maintain(self.sa, 't')
        self.assertFalse(work.exists())
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        self.assertEqual(reports.inventory(self.sb)['runs'][0]['local']['visibility'], 'not_recorded_in_this_clone')

    def test_explicit_cleanup_records_local_collection_but_dry_run_does_not(self):
        self.add(); self.start(); work = self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        command = [sys.executable, str(ROOT / 'bin/task-inbox'), '--repo', str(self.a),
                   'run', 'cleanup', '--task', 't', '--run-id', 'r']
        note = self.sa.local / 'reports/t/r.json'
        self.command(*command)
        self.assertTrue(work.exists()); self.assertFalse(note.exists())
        result = json.loads(self.command(*command, '--apply').stdout)
        self.assertEqual(result['status'], 'done')
        self.assertFalse(work.exists())
        self.assertTrue(json.loads(note.read_text())['local_workspace_collected'])
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        self.assertEqual(reports.inventory(self.sb)['runs'][0]['local']['visibility'], 'not_recorded_in_this_clone')

    def test_metadata_retirement_failure_and_retry_preserve_local_collection_evidence(self):
        self.add(); self.start(); work = self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        with patch.object(maintenance, 'retire_metadata', side_effect=TaskError('PRIVATE_TOKEN')):
            result = maintenance.maintain(self.sa, 't')
        self.assertEqual(result[0]['status'], 'retained')
        self.assertFalse(work.exists())
        note = self.sa.local / 'reports/t/r.json'
        evidence = json.loads(note.read_text())
        self.assertTrue(evidence['local_workspace_collected'])
        self.assertIn('resolved_at', evidence)
        self.assertNotIn('PRIVATE_TOKEN', note.read_text())
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        maintenance.maintain(self.sa, 't')
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        self.assertEqual(json.loads(note.read_text())['resolved_at'], evidence['resolved_at'])

    def test_restored_clone_collection_does_not_confirm_the_original_workspace(self):
        self.checkpoint()
        original = self.base / 'work-old'
        restored = self.base / 'restored'
        runtime.prepare(self.sb, 't', 'old', restored, restore=True, stopped=True, settled=True)
        runtime.publish(self.sb, 'finish-restored', 't', 'old', outcome='failed')
        self.assertFalse(restored.exists()); self.assertTrue(original.exists())
        row = reports.inventory(self.sb)['runs'][0]
        self.assertEqual(row['remote_refs'], [])
        self.assertEqual(row['local']['visibility'], 'collected_in_this_clone')
        self.assertTrue(row['local']['unobserved_prior_workspaces'])
        self.assertIn('workspace_history_unobserved', row['reason'])
        self.assertIn('Prior workspace history is unobserved', reports.markdown(reports.inventory(self.sb)))

    def test_second_prepare_before_checkpoint_keeps_original_workspace_unobserved(self):
        self.add(); self.start(); original = self.prepare()
        other = self.base / 'second-prepare'
        runtime.prepare(self.sb, 't', 'r', other)
        runtime.publish(self.sb, 'finish-second', 't', 'r', outcome='succeeded')
        self.assertTrue(original.exists()); self.assertFalse(other.exists())
        row = reports.inventory(self.sb)['runs'][0]
        self.assertEqual(row['local']['visibility'], 'collected_in_this_clone')
        self.assertTrue(row['local']['unobserved_prior_workspaces'])
        self.assertEqual(row['remote_refs'], [])

    def test_first_prepare_collection_does_not_confirm_other_unpublished_workspace(self):
        self.add(); self.start(); original = self.prepare()
        other = self.base / 'second-prepare'
        runtime.prepare(self.sb, 't', 'r', other)
        runtime.publish(self.sa, 'finish-first', 't', 'r', outcome='succeeded')
        self.assertFalse(original.exists()); self.assertTrue(other.exists())
        self.assertTrue(reports.inventory(self.sa)['runs'][0]['local']['unobserved_prior_workspaces'])
        maintenance.maintain(self.sb, 't')
        self.assertFalse(other.exists())
        self.assertTrue(reports.inventory(self.sb)['runs'][0]['local']['unobserved_prior_workspaces'])

    def test_prepare_reconciles_lost_response_without_duplicate_history(self):
        self.add(); self.start(); work = self.base / 'interrupted-prepare'
        push = self.sa.push
        def lose_response(ref, candidate, expected):
            push(ref, candidate, expected)
            raise TaskError('Simulated interruption after authority acceptance')
        with patch.object(self.sa, 'push', side_effect=lose_response), self.assertRaises(TaskError):
            runtime.prepare(self.sa, 't', 'r', work)
        self.assertFalse(work.exists())
        accepted = self.sa.fetch()
        runtime.prepare(self.sa, 't', 'r', work)
        runtime.prepare(self.sa, 't', 'r', work)
        self.assertEqual(self.sa.fetch(), accepted)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(reports.inventory(self.sa)['runs'], [])

    def test_legacy_prepare_history_without_evidence_remains_unobserved(self):
        self.add(); self.start(); work = self.prepare()
        snapshot = self.sa.snapshot()
        run = runtime.load_run(snapshot, 't', 'r')[2]
        run.pop('workspace_origin_id'); run.pop('workspace_history_unobserved')
        commit = self.sa.commit(snapshot.commit, {'tasks/t/runs/r/run.json': runtime.encode(run)}, 'Legacy run evidence')
        self.sa.push(self.sa.ref, commit, snapshot.commit)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertFalse(work.exists())
        self.assertTrue(reports.inventory(self.sa)['runs'][0]['local']['unobserved_prior_workspaces'])

    def test_writer_fence_rejects_28_prepare_before_it_creates_a_workspace(self):
        self.add(); self.start(); self.prepare()
        before = self.sa.fetch(); other = self.base / 'legacy-other'
        with self.assertRaisesRegex(TaskError, 'not initialized'):
            legacy.prepare(legacy.LegacyStore(self.b), 't', 'r', other)
        self.assertFalse(other.exists())
        self.assertEqual(self.sa.fetch(), before)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(reports.inventory(self.sa)['runs'], [])

    def test_idle_schema6_authority_upgrades_before_admitting_a_run(self):
        self.add()
        snapshot = self.sa.snapshot(); config = snapshot.json('.task-system/config.json')
        config.update(schema_version=6); config.pop('workspace_provenance_protocol', None)
        candidate = self.sa.commit(snapshot.commit, {'.task-system/config.json': runtime.encode(config)}, 'Existing schema6 authority')
        self.sa.push(self.sa.ref, candidate, snapshot.commit)
        self.start(); original = self.prepare(); other = self.base / 'legacy-other'
        with self.assertRaisesRegex(TaskError, 'not initialized'):
            legacy.prepare(legacy.LegacyStore(self.b), 't', 'r', other)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertFalse(original.exists()); self.assertFalse(other.exists())
        self.assertEqual(reports.inventory(self.sa)['runs'], [])
        self.start('next')
        self.assertEqual(self.sa.snapshot().json('.task-system/config.json')['schema_version'], 9)

    def test_old_false_history_marker_without_protocol_stamp_does_not_confirm_cleanup(self):
        self.add(); self.start(); work = self.prepare()
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        snapshot = self.sa.snapshot(); run = runtime.load_run(snapshot, 't', 'r')[2]
        run.pop('workspace_provenance_protocol'); run['workspace_history_unobserved'] = False
        candidate = self.sa.commit(snapshot.commit, {'tasks/t/runs/r/run.json': runtime.encode(run)}, 'Legacy terminal diagnostic fields')
        self.sa.push(self.sa.ref, candidate, snapshot.commit)
        maintenance.maintain(self.sa, 't')
        self.assertFalse(work.exists())
        row = reports.inventory(self.sa)['runs'][0]
        self.assertEqual(row['local']['visibility'], 'collected_in_this_clone')
        self.assertTrue(row['local']['unobserved_prior_workspaces'])

    def test_original_clone_collection_does_not_confirm_the_restored_workspace(self):
        self.checkpoint()
        original, restored = self.base / 'work-old', self.base / 'restored'
        runtime.prepare(self.sb, 't', 'old', restored, restore=True, stopped=True, settled=True)
        with patch.object(maintenance, 'maintain'):
            runtime.publish(self.sb, 'finish-restored', 't', 'old', outcome='failed')
        maintenance.maintain(self.sa, 't')
        self.assertFalse(original.exists()); self.assertTrue(restored.exists())
        row = reports.inventory(self.sa)['runs'][0]
        self.assertEqual(row['local']['visibility'], 'collected_in_this_clone')
        self.assertTrue(row['local']['unobserved_prior_workspaces'])
        maintenance.maintain(self.sb, 't')
        self.assertFalse(restored.exists())
        self.assertTrue(reports.inventory(self.sb)['runs'][0]['local']['unobserved_prior_workspaces'])

    def test_unaccepted_checkpoint_can_leave_prior_workspaces_after_restore(self):
        self.add(); run = self.start(); original = self.prepare()
        runtime.checkpoint_ref(self.sa, run['work_ref'], run['source_head'])
        self.assertEqual(runtime.load_run(self.sa.snapshot(), 't', 'r')[2]['publication_count'], 0)
        restored = self.base / 'restored'
        runtime.prepare(self.sb, 't', 'r', restored, restore=True, stopped=True, settled=True)
        runtime.publish(self.sb, 'finish-restored', 't', 'r', outcome='failed')
        self.assertTrue(original.exists()); self.assertFalse(restored.exists())
        row = reports.inventory(self.sb)['runs'][0]
        self.assertTrue(row['local']['unobserved_prior_workspaces'])

    def test_unavailable_ref_inventory_is_incomplete_and_does_not_expose_raw_errors(self):
        self.checkpoint(); self.recover()
        with patch.object(self.sa, 'list_refs', side_effect=GitFailure('PRIVATE_TOKEN', {'definite_rejection': False})):
            value = reports.inventory(self.sa)
        self.assertTrue(value['needs_attention'])
        self.assertEqual(value['errors'], [{'scope': 'remote_refs', 'reason': 'ref_inventory_unavailable'}])
        self.assertNotIn('PRIVATE_TOKEN', json.dumps(value))
        self.assertIn('Inventory is incomplete', reports.markdown(value))
        self.assertNotIn('No retained run resources', reports.markdown(value))

    def test_legacy_recovery_without_notice_and_invalid_local_state_are_supported(self):
        self.checkpoint(); self.recover()
        before = self.sa.fetch()
        changed = self.sa.commit(before, {'tasks/t/runs/old/retention-report.json': None,
                                         'tasks/t/runs/old/retention-report.md': None}, 'Legacy recovery')
        self.sa.push(self.sa.ref, changed, before)
        value = reports.inventory(self.sb, 't', 'old')
        self.assertIsNone(value['runs'][0]['retention_report'])
        runtime.local_state(self.sa, 't', 'old').write_text('null')
        value = reports.inventory(self.sa, 't', 'old')
        self.assertTrue(value['needs_attention'])
        self.assertEqual(value['runs'][0]['local']['visibility'], 'unknown')

    def test_cli_reports_json_markdown_and_validates_scope(self):
        self.checkpoint(); self.recover()
        before = self.sb.fetch()
        command = [sys.executable, str(ROOT / 'bin/task-inbox'), '--repo', str(self.b), 'run', 'report']
        result = self.command(*command)
        self.assertEqual(result.stderr, '')
        self.assertEqual(json.loads(result.stdout)['runs'][0]['run_id'], 'old')
        result = self.command(*command, '--task', 't', '--run-id', 'old', '--format', 'markdown')
        self.assertTrue(result.stdout.startswith('# Retained runtime resources\n'))
        self.assertIn('refs/heads/task-runtime/t/old/work', result.stdout)
        self.assertEqual(self.sb.fetch(), before)
        invalid = self.command(*command, '--run-id', 'old', ok=False)
        self.assertEqual(invalid.returncode, 1)
        self.assertIn('requires --task', invalid.stderr)

    def test_lost_recovery_reply_still_preserves_exact_notice(self):
        self.checkpoint()
        push = self.sa.push
        def lose_reply(ref, candidate, expected):
            push(ref, candidate, expected)
            if ref == self.sa.ref:
                raise GitFailure('lost response', {'definite_rejection': False})
        with patch.object(self.sa, 'push', side_effect=lose_reply):
            self.recover()
        path = 'tasks/t/runs/old/retention-report.json'
        before = self.sb.snapshot()
        self.recover()
        self.assertEqual(self.sa.fetch(), before.commit)
        self.assertEqual(self.sa.snapshot().read(path), before.read(path))

    def test_source_and_zipapp_distributions_include_reporting(self):
        self.checkpoint(); self.recover()
        bundle = self.base / 'runtime.pyz'
        self.command(sys.executable, str(ROOT / 'bin/build-task-inbox'), '--output', str(bundle))
        result = self.command(sys.executable, str(bundle), '--repo', str(self.b), 'run', 'report')
        self.assertEqual(json.loads(result.stdout)['runs'][0]['run_id'], 'old')
        source = self.base / 'source.tar.gz'
        self.command(sys.executable, str(ROOT / 'bin/build-task-inbox'), '--format', 'source', '--output', str(source))
        with tarfile.open(source) as archive:
            names = set(archive.getnames())
        self.assertTrue({'task-runtime/foundation/task_runtime/reports.py',
                         'task-runtime/test/task_reports_test.py',
                         'task-runtime/docs/task-runtime-reports.md'} <= names)


class IndexedReportTests(RuntimeCase):
    init_options = dict(index='registry/tasks.json', task_root='operations/items')

    def test_custom_paths_are_used_for_report_records_and_inventory(self):
        self.add('custom', path='work/[literal]*', data_path='output/custom')
        run = self.start('old', task='custom')
        self.prepare('old', task='custom')
        runtime.heartbeat(self.sa, 'custom', 'old')
        runtime.recover(self.sa, 'recover-custom', 'custom', 'old', stopped=True, settled=True,
                        evidence='fixture:stopped')
        row = reports.inventory(self.sb, 'custom')['runs'][0]
        self.assertEqual(row['retention_report'], 'work/[literal]*/runs/old/retention-report.json')
        self.assertEqual(row['run_path'], 'work/[literal]*/runs/old/run.json')
        self.assertEqual(row['generation'], run['generation'])


class LeaseReportTests(lease_fixtures.LeaseCase):
    def test_revocation_saves_notice_in_the_guarded_transaction(self):
        self.enable(); run = self.c.start('old')
        self.c.prepare('old')
        runtime.heartbeat(self.c.sa, 't', 'old')
        self.settings(now_ms=1400000)
        before = self.c.sa.fetch()
        leases.revoke(self.c.sa, 'revoke', 't', 'old')
        snapshot = self.c.sa.snapshot()
        self.assertEqual(self.c.git(self.c.a, 'rev-list', '--count', before + '..' + snapshot.commit), '1')
        notice = snapshot.json('tasks/t/runs/old/retention-report.json')
        self.assertEqual(notice['reason'], 'expired_authority_fenced')
        self.assertEqual(notice['generation'], run['generation'])
        self.assertEqual(notice['recovery_kind'], 'expired_' + self.mode)
        self.assertEqual(reports.inventory(self.c.sb)['runs'][0]['run_id'], 'old')

    def test_checkpoint_delivery_retains_a_notice_for_both_generations(self):
        self.enable(); self.completed()
        self.settings(now_ms=1400000)
        leases.revoke(self.c.sa, 'revoke', 't', 'old')
        self.c.start('next', store=self.c.sb, resume_from='old', checkpoint_id='review')
        leases.deliver(self.c.sb, 'deliver', 't', 'next')
        snapshot = self.c.sa.snapshot()
        self.assertEqual(snapshot.json('tasks/t/runs/next/retention-report.json')['reason'],
                         'checkpoint_delivery_preserves_artifacts')


class GenerationReportTests(LeaseReportTests):
    adapter = ROOT / 'test/fixtures/task_timeout_adapter.py'
    mode = leases.GENERATION
