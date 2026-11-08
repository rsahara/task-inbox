"""Terminal cleanup with real Git remotes, retries and independent clones."""
import concurrent.futures
import json
import subprocess
import sys
from unittest.mock import patch

from task_runtime_test import RuntimeCase
from task_runtime import lifecycle, maintenance, runtime, task_io
from task_runtime.store import CONTROL, GitFailure, Snapshot, Store, TaskError, atomic_json, encode


class LifecycleValidationTest(RuntimeCase):
    def test_execute_requires_a_command_and_prepared_workspace(self):
        self.add(); self.start()
        for command, message in [([], 'command is required'),
                                  ([sys.executable, '-c', 'pass'], 'Prepare the run workspace')]:
            with self.subTest(command=command), self.assertRaisesRegex(TaskError, message):
                lifecycle.execute(self.sa, 't', 'r', command)
        self.assertFalse(runtime.local_state(self.sa, 't', 'r').with_suffix('.execution.json').exists())

    def test_stop_adapter_rejects_invalid_evidence_and_preserves_authority(self):
        self.add(); run = self.start()
        program = self.base / 'installed-stop'
        program.write_text('fixture')
        valid = {name: run[name] for name in ('repository_id', 'authority_ref', 'task_id', 'run_id', 'generation')}
        valid.update(writer_stopped=True, children_stopped=True, external_operations_settled=True,
                     pending_git_operations_settled=True, evidence='fixture:durable-stop')
        for path in ('relative-stop', str(self.base / 'missing')):
            with self.subTest(path=path), self.assertRaisesRegex(TaskError, 'absolute path'):
                lifecycle.reconcile_stop(self.sa, 'stop', 't', 'r', path)
        responses = [(1, b'PRIVATE_TOKEN'), (0, b'not-json'), (0, b'\xff'), (0, b'[]'),
                     (0, b'x' * (1024 * 1024 + 1)), (0, encode(dict(valid, generation=999))),
                     (0, encode(dict(valid, children_stopped=1))), (0, encode(dict(valid, evidence='')))]
        before = self.sa.fetch()
        original_process = lifecycle.git_support.run_process
        for number, (returncode, stdout) in enumerate(responses):
            response = subprocess.CompletedProcess([], returncode, stdout, b'PRIVATE_TOKEN')
            def respond(argv, **kwargs):
                return response if argv == [str(program)] else original_process(argv, **kwargs)
            with self.subTest(case=number), patch('task_runtime.lifecycle.git_support.run_process', side_effect=respond):
                with self.assertRaises(TaskError) as raised:
                    lifecycle.reconcile_stop(self.sa, 'stop', 't', 'r', program)
                self.assertNotIn('PRIVATE_TOKEN', str(raised.exception))
                self.assertEqual(self.sb.fetch(), before)
        def timeout(argv, **kwargs):
            if argv == [str(program)]:
                raise subprocess.TimeoutExpired('fixture', 60)
            return original_process(argv, **kwargs)
        with patch('task_runtime.lifecycle.git_support.run_process', side_effect=timeout):
            with self.assertRaisesRegex(TaskError, 'timed out'):
                lifecycle.reconcile_stop(self.sa, 'stop', 't', 'r', program)
        with self.assertRaisesRegex(TaskError, 'another recovery'):
            lifecycle.reconcile_stop(self.sa, 'add-t', 't', 'r', program)
        self.assertEqual(runtime.check(self.sb, 't', 'r')['status'], 'running')

    def test_reaper_reports_git_and_timeout_failures_without_releasing_the_slot(self):
        self.add(); self.start()
        failures = [(GitFailure('fixture', {'definite_rejection': False, 'category': 'authentication'}), 'authentication'),
                    (subprocess.TimeoutExpired('fixture', 60), 'stop_check_timeout'),
                    (TaskError('fixture blocked'), 'fixture blocked')]
        for error, reason in failures:
            with self.subTest(reason=reason), patch.object(runtime, 'observe', side_effect=error):
                result = lifecycle.reap(self.sa, 't')
            self.assertTrue(result['needs_attention'])
            self.assertEqual(result['runs'], [{'run_id': 'r', 'status': 'unknown_or_blocked', 'reason': reason}])
        with self.assertRaisesRegex(TaskError, 'observer profile'):
            lifecycle.reap(self.sa, 't', observer_profile='invalid')
        self.assertEqual(runtime.check(self.sb, 't', 'r')['status'], 'running')


class CleanupTests(RuntimeCase):
    def assert_metadata_retired(self, before, count=1):
        after = self.sa.fetch()
        self.assertEqual(self.git(self.a, 'rev-list', '--count', before + '..' + after), str(count))
        deleted = self.git(self.a, 'diff', '--name-status', before, after).splitlines()
        self.assertTrue(deleted)
        for line in deleted:
            status, path = line.split('\t')
            self.assertEqual(status, 'D')
            if path.startswith(CONTROL + '/operations/'):
                receipt = Snapshot(self.sa, before).json(path)
                self.assertEqual(receipt.get('scope', receipt.get('result', {})).get('task_id'), 't')
            else:
                suffix = path.removeprefix('tasks/t/runs/').split('/', 1)[1]
                self.assertTrue(suffix in ('run.json', 'result.json', 'cleanup.json') or suffix.startswith('io/'), path)
        return after

    def finished(self, run="r", *, outcome="succeeded", automatic=True, store=None):
        store = store or self.sa
        record = self.start(run, store=store)
        work = self.prepare(run, store=store)
        self.output(work, "tasks/t/runs/" + run + "/artifacts/out")
        runtime.heartbeat(store, "t", run)
        if automatic:
            result = runtime.publish(store, "finish-" + run, "t", run, outcome=outcome)
        else:
            with patch.object(maintenance, "maintain"):
                result = runtime.publish(store, "finish-" + run, "t", run, outcome=outcome)
        return record, work, result

    def test_finish_publishes_atomically_then_retires_execution_metadata(self):
        self.add(); run = self.start(); work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out")
        runtime.heartbeat(self.sa, "t", "r")
        before = self.sa.fetch()
        result = runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        after = self.sa.fetch()
        self.assertEqual(self.git(self.a, "rev-list", "--count", before + ".." + after), "2")
        finished = Snapshot(self.sa, self.git(self.a, 'rev-parse', after + '^'))
        self.assertEqual(finished.json('tasks/t/runs/r/run.json')['status'], 'succeeded')
        self.assertEqual(finished.json('tasks/t/runtime.json')['slots']['1']['run_id'], None)
        self.assertEqual(finished.read('tasks/t/runs/r/artifacts/out'), b'output\n')
        self.assert_metadata_retired(finished.commit)
        self.assertFalse(work.exists())
        self.assertIsNone(self.sa.remote_tip(run["work_ref"]))
        self.assertIsNone(self.sa.remote_tip(run["heartbeat_ref"]))
        self.assertIsNone(self.sa.remote_tip("refs/tags/task-runs/t/r"))
        self.assertEqual(self.git(self.a, "for-each-ref", "--format=%(refname)", run["work_ref"]), "")
        self.assertEqual(list((self.sa.local / "runs/t").glob("r.*")), [])
        self.assertEqual(list((self.sa.local / "publications").glob("*.json")), [])
        self.assertEqual(list((self.sa.local / "operations").glob("*.json")), [])
        self.assertEqual(self.sa.snapshot().read("tasks/t/runs/r/artifacts/out"), b"output\n")
        self.assertEqual(runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded"), result)
        self.assertEqual(list((self.sb.local / "runs/t").glob("r.*")), [])
        self.assertEqual(self.sa.fetch(), after)

    def test_cleanup_rejects_modified_identity_receipt_namespace_and_source(self):
        self.add()
        _, work, _ = self.finished(automatic=False)
        snapshot = self.sa.snapshot()
        run_path = 'tasks/t/runs/r/run.json'
        cleanup_path = 'tasks/t/runs/r/cleanup.json'
        run = snapshot.json(run_path)
        record = snapshot.json(cleanup_path)
        receipt_path = CONTROL + '/operations/finish-r.json'
        changes = [({cleanup_path: encode(dict(record, status='retained'))}, 'not eligible'),
                   ({cleanup_path: encode(dict(record, version=1))}, 'Legacy run'),
                   ({cleanup_path: encode(dict(record, generation=999))}, 'identity differs'),
                   ({receipt_path: encode(dict(snapshot.json(receipt_path), result={}))}, 'durable operation receipt'),
                   ({run_path: encode(dict(run, work_ref='refs/heads/unrelated'))}, 'outside the original run namespace'),
                   ({cleanup_path: encode(dict(record, work_head='wrong'))}, 'accepted source')]
        for changed, message in changes:
            candidate = self.sa.commit(snapshot.commit, changed, 'altered cleanup evidence')
            with self.subTest(message=message), self.assertRaisesRegex(TaskError, message):
                maintenance.terminal(self.sa, Snapshot(self.sa, candidate), 't', 'r')
        self.assertTrue(work.exists())
        self.assertEqual(self.sb.fetch(), snapshot.commit)

    def test_missing_or_changed_main_io_records_preserve_run_stream(self):
        self.add()
        run, work, _ = self.finished(automatic=False)
        before = self.sa.remote_tip(run['heartbeat_ref'])
        snapshot = self.sa.snapshot()
        path = 'tasks/t/runs/r/cleanup.json'
        record = snapshot.json(path)
        legacy = dict(record); legacy.pop('io_manifest')
        for changes, error in [({path: encode(legacy)}, 'not saved on main'),
                               ({'tasks/t/runs/r/io/state.json': None}, 'differ from the finish manifest')]:
            parent = self.sa.fetch()
            candidate = self.sa.commit(parent, {path: encode(record), **changes}, 'Incomplete saved I/O fixture')
            self.sa.push(self.sa.ref, candidate, parent)
            with self.subTest(error=error), self.assertRaisesRegex(TaskError, error):
                maintenance.collect(self.sa, 't', 'r', apply=True)
            self.assertEqual(self.sa.remote_tip(run['heartbeat_ref']), before)
        self.assertIsNone(self.sa.remote_tip('refs/tags/task-io/t/r'))
        self.assertTrue(work.exists())

    def test_legacy_cleanup_preserves_unowned_mismatched_and_executor_workspaces(self):
        self.add(); self.start(); work = self.prepare()
        self.legacy_finish(self.sa, 'legacy-finish', 't', 'r', outcome='succeeded')
        path = runtime.local_state(self.sa, 't', 'r')
        original = json.loads(path.read_text())
        for changes, message in [(dict(mode='workspace'), 'belong to the executor'),
                                  (dict(path=str(self.a)), 'surviving checkout'),
                                  (dict(generation=999), 'ownership does not match')]:
            atomic_json(path, dict(original, **changes))
            with self.subTest(changes=changes), self.assertRaisesRegex(TaskError, message):
                lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        path.unlink()
        with self.assertRaisesRegex(TaskError, 'No local workspace ownership'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        atomic_json(path, original)
        plan = lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        self.assertTrue(plan['dry_run'])
        with self.assertRaisesRegex(TaskError, 'Confirm manually launched processes'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r', apply=True)
        self.assertTrue(work.exists())

    def test_local_collection_preserves_changed_branches_commits_manifests_and_missing_mounts(self):
        self.add()
        _, work, run = self.finished(automatic=False)
        self.git(work, 'symbolic-ref', 'HEAD', 'refs/heads/unowned')
        with self.assertRaisesRegex(TaskError, 'repository or branch changed'):
            maintenance.collect_local(self.sa, run)
        self.git(work, 'symbolic-ref', 'HEAD', run['work_ref'])
        self.git(work, 'commit', '--allow-empty', '-m', 'unpreserved work')
        with self.assertRaisesRegex(TaskError, 'unpreserved commits'):
            maintenance.collect_local(self.sa, run)
        self.git(work, 'reset', '--hard', run['source_head'])
        invalid = dict(run, artifact_manifest={'missing': ['100644', 'blob', 'a' * 40]})
        with self.assertRaisesRegex(TaskError, 'accepted output'):
            maintenance.collect_local(self.sa, invalid)
        moved = work.with_name('disconnected-workspace')
        work.rename(moved)
        with self.assertRaisesRegex(TaskError, 'missing but still registered'):
            maintenance.collect_local(self.sa, run)
        moved.rename(work)
        self.git(self.a, 'worktree', 'remove', str(work))
        changed = self.sa.commit(run['source_head'], {}, 'unpreserved branch')
        self.git(self.a, 'update-ref', run['work_ref'], changed)
        with self.assertRaisesRegex(TaskError, 'Local work branch changed'):
            maintenance.collect_local(self.sa, run)
        self.git(self.a, 'update-ref', run['work_ref'], run['source_head'])
        replacement = self.base / 'other-worktree'
        self.git(self.a, 'worktree', 'add', str(replacement), run['work_ref'].removeprefix('refs/heads/'))
        with self.assertRaisesRegex(TaskError, 'still checked out'):
            maintenance.collect_local(self.sa, run)
        self.assertTrue(replacement.exists())
        self.assertIsNotNone(self.sa.remote_tip(run['work_ref']))

    def test_legacy_cleanup_rejects_active_recovered_and_changed_workspaces(self):
        self.add(); self.start(); work = self.prepare()
        with self.assertRaisesRegex(TaskError, 'active run'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        self.legacy_finish(self.sa, 'legacy-finish', 't', 'r', outcome='succeeded')
        run = runtime.load_run(self.sa.snapshot(), 't', 'r')[2]
        self.git(work, 'symbolic-ref', 'HEAD', 'refs/heads/unowned')
        with self.assertRaisesRegex(TaskError, 'branch changed'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        self.git(work, 'symbolic-ref', 'HEAD', run['work_ref'])
        self.git(work, 'commit', '--allow-empty', '-m', 'unsaved')
        with self.assertRaisesRegex(TaskError, 'unsaved commits'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        self.git(work, 'reset', '--hard', run['source_head'])
        self.output(work, 'unpreserved')
        with self.assertRaisesRegex(TaskError, 'unpreserved files'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        (work / 'unpreserved').unlink()
        marker = runtime.local_state(self.sa, 't', 'r').with_suffix('.execution.json')
        atomic_json(marker, {'phase': 'running'})
        with self.assertRaisesRegex(TaskError, 'termination is not confirmed'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r', apply=True)
        moved = work.with_name('missing-legacy-work')
        work.rename(moved)
        with self.assertRaisesRegex(TaskError, 'disappeared without a cleanup journal'):
            lifecycle.cleanup(self.sa, 'legacy-cleanup', 't', 'r')
        moved.rename(work)
        self.start('next')
        runtime.recover(self.sa, 'recover-next', 't', 'next', stopped=True, settled=True, evidence='fixture:stopped')
        with self.assertRaisesRegex(TaskError, 'retain their work'):
            lifecycle.cleanup(self.sa, 'cleanup-lost', 't', 'next')

    def test_legacy_finish_record_retains_its_preservation_contract(self):
        self.add(); run = self.start()
        record = maintenance.finish_record(dict(run, cleanup_protocol=1), 'finish', {'has_checkpoint': True}, False)
        self.assertEqual(record['status'], 'pending')
        self.assertTrue(record['has_checkpoint'])
        self.assertNotIn('version', record)

    def test_retained_workspaces_do_not_spend_collection_quota(self):
        self.add()
        with patch.object(maintenance, "maintain"):
            runs = [self.finished(f"r{i:02d}") for i in range(9)]
        actual = maintenance.collect_local
        def executor_owned(store, run):
            return "executor_owned" if run["run_id"] < "r08" else actual(store, run)
        before = self.sa.fetch()
        with patch.object(maintenance, "collect_local", side_effect=executor_owned):
            maintenance.maintain(self.sa, "t")
        self.assertFalse(runs[-1][1].exists())
        self.assertIsNone(self.sa.remote_tip(runs[-1][0]["work_ref"]))
        self.assert_metadata_retired(before, count=9)
        for _, work, _ in runs[:-1]:
            (work / "unpreserved").write_text("keep")
        with patch.object(maintenance, "maintain"):
            last, last_work, _ = self.finished("r09")
        before = self.sa.fetch()
        maintenance.maintain(self.sa, "t")
        self.assertFalse(last_work.exists())
        self.assertIsNone(self.sa.remote_tip(last["work_ref"]))
        self.assertTrue(all((work / "unpreserved").exists() for _, work, _ in runs[:-1]))
        self.assert_metadata_retired(before)

    def test_observer_only_clone_collects_terminal_sidecars(self):
        self.add(); self.start(); self.prepare()
        runtime.observe(self.sb, "t", "r")
        state = runtime.local_state(self.sb, "t", "r")
        observation = state.with_suffix(".observation.json")
        lock = observation.with_suffix(".lock")
        self.assertFalse(state.exists())
        self.assertTrue(observation.exists())
        runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        before = self.sb.fetch()
        with self.sb.lock(lock):
            inode = lock.stat().st_ino
            maintenance.maintain(self.sb, "t")
            self.assertTrue(observation.exists())
            self.assertEqual(lock.stat().st_ino, inode)
        maintenance.maintain(self.sb, "t")
        self.assertEqual(list(state.parent.glob("r.*")), [])
        self.assertEqual(self.sb.fetch(), before)

    def test_unowned_unknown_state_and_mismatched_observations_are_retained(self):
        self.add(); self.finished()
        state = runtime.local_state(self.sb, "t", "r")
        observation = state.with_suffix(".observation.json")
        run = runtime.load_run(self.sb.snapshot(), "t", "r")[2]
        atomic_json(observation, {"generation": run["generation"] + 1})
        maintenance.maintain(self.sb, "t")
        self.assertTrue(observation.exists())
        atomic_json(observation, {"generation": run["generation"]})
        journal = state.with_suffix(".execution.json")
        atomic_json(journal, {"candidate": "unknown"})
        maintenance.maintain(self.sb, "t")
        self.assertTrue(journal.exists())
        self.assertTrue(observation.exists())

    def test_next_start_retries_failed_remote_deletion(self):
        self.add(); push = self.sa.push
        def fail_delete(ref, candidate, expected, **kwargs):
            if candidate is None and ref.endswith("/work"):
                raise TaskError("temporary delete failure")
            return push(ref, candidate, expected, **kwargs)
        with patch.object(self.sa, "push", side_effect=fail_delete):
            record, work, result = self.finished()
        self.assertEqual(result["status"], "succeeded")
        self.assertTrue(work.exists())
        before = self.sa.fetch()
        self.start("next")
        self.assertFalse(work.exists())
        self.assertIsNone(self.sa.remote_tip(record["work_ref"]))
        self.assertEqual(self.git(self.a, "rev-list", "--count", before + ".." + self.sa.fetch()), "2")
        self.assertEqual(runtime.check(self.sa, "t", "next")["status"], "running")

    def test_artifact_named_cleanup_is_not_a_management_manifest(self):
        self.add()
        for index, content in enumerate((b"[]", b"not JSON", b'{"version":2,"status":"eligible"}')):
            with self.subTest(content=content):
                run_id = f"r{index}"
                self.start(run_id)
                work = self.prepare(run_id)
                path = f"tasks/t/runs/{run_id}/artifacts/cleanup.json"
                self.output(work, path, content)
                result = runtime.publish(self.sa, "finish-" + run_id, "t", run_id, outcome="succeeded")
                self.assertEqual(result["status"], "succeeded")
                self.assertFalse(work.exists())
                self.assertEqual(self.sa.snapshot().read(path), content)
        self.assertEqual(self.start("next")["status"], "running")

    def test_failed_and_cancelled_runs_collect_adopted_output(self):
        self.add()
        for outcome in ("failed", "cancelled"):
            with self.subTest(outcome=outcome):
                run, work, result = self.finished(outcome, outcome=outcome)
                self.assertEqual(result["status"], outcome)
                self.assertFalse(work.exists())
                self.assertIsNone(self.sa.remote_tip(run["work_ref"]))

    def test_lost_delete_reply_is_reconciled(self):
        self.add(); run, work, _ = self.finished(automatic=False)
        before = self.sa.fetch(); push = self.sa.push
        def lose_reply(ref, candidate, expected, **kwargs):
            result = push(ref, candidate, expected, **kwargs)
            if candidate is None:
                raise TaskError("lost reply")
            return result
        with patch.object(self.sa, "push", side_effect=lose_reply):
            self.assertEqual(maintenance.collect(self.sa, "t", "r", apply=True)["status"], "done")
        self.assertFalse(work.exists())
        self.assert_metadata_retired(before)

    def test_fresh_clone_collects_remote_then_original_clone_collects_local(self):
        self.add(); run, work, _ = self.finished(automatic=False)
        before = self.sb.fetch()
        result = maintenance.collect(self.sb, "t", "r", apply=True)
        self.assertEqual(result["local"], "absent")
        self.assertTrue(work.exists())
        self.assertIsNone(self.sb.remote_tip(run["work_ref"]))
        after = self.assert_metadata_retired(before)
        maintenance.maintain(self.sa, "t")
        self.assertFalse(work.exists())
        for ref in (run["work_ref"], run["heartbeat_ref"]):
            tracking = ref.replace("refs/heads/", "refs/remotes/origin/", 1)
            self.assertEqual(self.git(self.a, "for-each-ref", "--format=%(refname)", tracking), "")
        self.assertEqual(self.sa.fetch(), after)

    def test_tracking_only_clone_uses_its_remote_fetch_mapping(self):
        self.add(); run, _, _ = self.finished(automatic=False)
        self.git(self.b, "remote", "rename", "origin", "company")
        self.git(self.b, "config", "--unset-all", "remote.company.fetch")
        self.git(self.b, "config", "--add", "remote.company.fetch", "+refs/heads/main:refs/remotes/company/main")
        self.git(self.b, "config", "--add", "remote.company.fetch",
                 "+refs/heads/task-runtime/*:refs/remotes/company/run-cache/*")
        other = Store(self.b, remote="company")
        for ref in (run["work_ref"], run["heartbeat_ref"]):
            other.fetch(ref)
        unrelated = "refs/remotes/company/unrelated"
        self.git(self.b, "update-ref", unrelated, other.fetch())
        before = other.remote_tip(other.ref)
        maintenance.collect(self.sa, "t", "r", apply=True)
        maintenance.maintain(other, "t")
        self.assertEqual(self.git(self.b, "for-each-ref", "--format=%(refname)",
                                  "refs/remotes/company/run-cache/t/r/"), "")
        self.assertEqual(self.git(self.b, "rev-parse", unrelated), before)
        self.assertEqual(other.remote_tip(other.ref), self.assert_metadata_retired(before))

    def test_changed_tracking_ref_is_retained_after_remote_collection(self):
        self.add(); run, work, _ = self.finished(automatic=False)
        tracking = run["work_ref"].replace("refs/heads/", "refs/remotes/origin/", 1)
        changed = self.sa.commit(run["source_head"], {}, "Unpreserved tracking tip")
        self.git(self.a, "update-ref", tracking, changed)
        maintenance.collect(self.sb, "t", "r", apply=True)
        with self.assertRaisesRegex(TaskError, "tracking ref"):
            maintenance.collect(self.sa, "t", "r", apply=True)
        self.assertEqual(self.git(self.a, "rev-parse", tracking), changed)
        self.assertTrue(work.exists())

    def test_negative_fetch_mapping_does_not_authorize_tracking_deletion(self):
        self.add(); run, _, _ = self.finished(automatic=False)
        self.git(self.b, 'fetch', 'origin', run["heartbeat_ref"])
        tracking = run["heartbeat_ref"].replace("refs/heads/", "refs/remotes/origin/", 1)
        expected = self.git(self.b, "rev-parse", tracking)
        self.git(self.b, "config", "--add", "remote.origin.fetch", "^" + run["heartbeat_ref"])
        maintenance.collect(self.sa, "t", "r", apply=True)
        maintenance.maintain(self.sb, "t")
        self.assertEqual(self.git(self.b, "rev-parse", tracking), expected)

    def test_cleanup_replay_never_writes_authority_or_receipts(self):
        self.add(); self.finished()
        before = self.sa.fetch()
        for operation in ("cleanup-1", "cleanup-2"):
            result = lifecycle.cleanup(self.sa, operation, "t", "r", apply=True)
            self.assertEqual(result["status"], "done")
            self.assertIsNone(self.sa.snapshot().json(CONTROL + "/operations/" + operation + ".json"))
        self.assertEqual(self.sa.fetch(), before)

    def test_changed_heartbeat_is_retained_without_starving_other_runs(self):
        self.add(); run, work, _ = self.finished("a", automatic=False)
        first = self.sa.fetch(run["heartbeat_ref"])
        state = Snapshot(self.sa, first).json("state.json"); state["sequence"] += 1
        changed = self.sa.commit(first, {"state.json": encode(state)}, "Late heartbeat")
        self.sa.push(run["heartbeat_ref"], changed, first)
        other, other_work, _ = self.finished("b", automatic=False)
        before = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, "changed"):
            maintenance.collect(self.sa, "t", "a", apply=True)
        maintenance.maintain(self.sa, "t")
        self.assertTrue(work.exists())
        self.assertEqual(self.sa.remote_tip(run["heartbeat_ref"]), changed)
        self.assertFalse(other_work.exists())
        self.assert_metadata_retired(before)

    def test_dirty_and_ignored_files_keep_the_local_workspace(self):
        self.add(); run, work, _ = self.finished(automatic=False)
        extra = work / "unpreserved"; extra.write_text("keep")
        with self.assertRaisesRegex(TaskError, "unpreserved"):
            maintenance.collect(self.sa, "t", "r", apply=True)
        self.assertTrue(extra.exists())
        extra.unlink()
        info = self.sa.common / "info/exclude"
        info.write_text(info.read_text() + "\nunpreserved\n")
        extra.write_text("ignored")
        with self.assertRaisesRegex(TaskError, "unpreserved"):
            maintenance.collect(self.sa, "t", "r", apply=True)
        self.assertTrue(extra.exists())

    def test_cleanup_defers_the_checkout_used_by_its_caller(self):
        self.add(); run = self.start(); work = self.prepare()
        result = runtime.publish(Store(work), "finish", "t", "r", outcome="succeeded")
        self.assertEqual(result["status"], "succeeded")
        self.assertTrue(work.exists())
        self.start("next")
        self.assertFalse(work.exists())

    def test_running_execution_receipt_keeps_local_files(self):
        self.add(); _, work, _ = self.finished(automatic=False)
        marker = runtime.local_state(self.sa, "t", "r").with_suffix(".execution.json")
        atomic_json(marker, {"phase": "running"})
        maintenance.maintain(self.sa, "t")
        self.assertTrue(work.exists())
        atomic_json(marker, {"phase": "exited"})
        maintenance.maintain(self.sa, "t")
        self.assertFalse(work.exists())

    def test_local_lock_holders_prevent_collection_and_inode_replacement(self):
        self.add(); _, work, _ = self.finished(automatic=False)
        path = runtime.local_state(self.sa, "t", "r").with_suffix(".observation.lock")
        with self.sa.lock(path):
            inode = path.stat().st_ino
            maintenance.maintain(self.sa, "t")
            self.assertTrue(work.exists())
            self.assertEqual(path.stat().st_ino, inode)
        maintenance.maintain(self.sa, "t")
        self.assertFalse(work.exists())
        self.assertFalse(path.exists())

    def test_unknown_operation_journal_and_pin_are_retained(self):
        self.add(); self.finished(automatic=False)
        candidate = self.sa.commit(self.sa.fetch(), {}, "Unknown operation")
        path = self.sa.local / "operations/unknown.json"
        atomic_json(path, {"request_hash": "unknown", "pending": {"candidate": candidate}})
        pin = "refs/task-runtime-pending/" + self.sa.local.name + "/unknown"
        self.git(self.a, "update-ref", pin, candidate)
        maintenance.maintain(self.sa, "t")
        self.assertTrue(path.exists())
        self.assertEqual(self.git(self.a, "rev-parse", pin), candidate)

    def test_discarded_output_is_not_automatically_deleted(self):
        self.add(); self.start(); work = self.prepare()
        out = self.output(work, "tasks/t/runs/r/artifacts/unsaved")
        runtime.publish(self.sa, "discard", "t", "r", outcome="failed", discard_output=True)
        self.start("next")
        self.assertTrue(out.exists())

    def test_worktree_removal_interruption_resumes_without_a_new_journal(self):
        self.add(); _, work, _ = self.finished(automatic=False)
        actual = maintenance.git
        def interrupted(repo, *args, **kwargs):
            result = actual(repo, *args, **kwargs)
            if args[:2] == ("worktree", "remove"):
                raise TaskError("interrupted after removal")
            return result
        with patch.object(maintenance, "git", side_effect=interrupted), self.assertRaises(TaskError):
            maintenance.collect(self.sa, "t", "r", apply=True)
        self.assertFalse(work.exists())
        self.assertTrue(runtime.local_state(self.sa, "t", "r").exists())
        self.assertEqual(maintenance.collect(self.sa, "t", "r", apply=True)["status"], "done")
        self.assertFalse(runtime.local_state(self.sa, "t", "r").exists())

    def test_two_clones_can_collect_same_remote_run(self):
        self.add(); run, _, _ = self.finished(automatic=False)
        before = self.sa.fetch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda s: maintenance.collect(s, "t", "r", apply=True), (self.sa, self.sb)))
        self.assertTrue(all(r["status"] == "done" for r in results))
        self.assertIsNone(self.sa.remote_tip(run["work_ref"]))
        self.assert_metadata_retired(before)


    def test_io_tracking_only_clone_is_collected_after_remote_and_sidecar_cleanup(self):
        from task_runtime import task_io
        self.add()
        run, work, _ = self.finished(automatic=False)
        self.git(self.b, 'fetch', 'origin', run["heartbeat_ref"])
        tracking = run["heartbeat_ref"].replace("refs/heads/", "refs/remotes/origin/", 1)
        preserved = self.git(self.b, "rev-parse", tracking)
        maintenance.collect(self.sa, "t", "r", apply=True)
        self.assertFalse(work.exists())
        self.assertIsNone(self.sb.remote_tip(run["heartbeat_ref"]))
        self.assertEqual(self.git(self.b, "rev-parse", tracking), preserved)
        before = self.sb.fetch()
        maintenance.maintain(self.sb, "t")
        self.assertEqual(self.git(self.b, "for-each-ref", "--format=%(refname)", tracking), "")
        self.assertIsNone(self.sb.remote_tip("refs/tags/task-io/t/r"))
        self.assertEqual(self.sb.fetch(), before)

    def test_changed_io_tracking_tip_is_preserved(self):
        from task_runtime import task_io
        self.add()
        run, _, _ = self.finished(automatic=False)
        tip = self.sb.fetch(run["heartbeat_ref"])
        tracking = run["heartbeat_ref"].replace("refs/heads/", "refs/remotes/origin/", 1)
        changed = self.sb.commit(tip, {"events/unpreserved.json": b"{}"}, "Unpreserved I/O")
        self.git(self.b, "update-ref", tracking, changed)
        maintenance.collect(self.sa, "t", "r", apply=True)
        with self.assertRaisesRegex(TaskError, "tracking ref"):
            maintenance.collect(self.sb, "t", "r", apply=True)
        self.assertEqual(self.git(self.b, "rev-parse", tracking), changed)

    def test_io_tracking_collection_respects_custom_fetch_mapping(self):
        from task_runtime import task_io
        self.add()
        run, _, _ = self.finished(automatic=False)
        self.git(self.b, "remote", "rename", "origin", "company")
        self.git(self.b, "config", "--unset-all", "remote.company.fetch")
        self.git(self.b, "config", "--add", "remote.company.fetch", "+refs/heads/main:refs/remotes/company/main")
        self.git(self.b, "config", "--add", "remote.company.fetch",
                 "+refs/heads/task-io/*:refs/remotes/company/io-cache/*")
        other = Store(self.b, remote="company")
        other.fetch(run["heartbeat_ref"])
        tracking = "refs/remotes/company/io-cache/t/runs/r"
        unrelated = "refs/remotes/company/unrelated"
        before = other.fetch()
        self.git(self.b, "update-ref", unrelated, before)
        maintenance.collect(self.sa, "t", "r", apply=True)
        maintenance.maintain(other, "t")
        self.assertEqual(self.git(self.b, "for-each-ref", "--format=%(refname)", tracking), "")
        self.assertEqual(self.git(self.b, "rev-parse", unrelated), before)
        self.assertEqual(other.fetch(), self.assert_metadata_retired(before))
