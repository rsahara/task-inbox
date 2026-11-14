"""Real Git integration coverage for the portable coordinator and distribution."""
import concurrent.futures
from contextlib import redirect_stderr
import io
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "foundation"))
from task_runtime import cli, lifecycle, runtime, task_io
from task_runtime.store import CONTROL, GitFailure, Snapshot, Store, TaskError, atomic_json, digest, encode


def cli_json_lines(value):
    """Keep strict JSON assertions while exposing no captured output on failure."""
    result = []
    for number, line in enumerate(value.splitlines(), 1):
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            raw = line.encode('utf-8', errors='replace')
            raise AssertionError('Non-JSON CLI stderr: line=%d bytes=%d sha256=%s' %
                                 (number, len(raw), hashlib.sha256(raw).hexdigest())) from None
    return result


class CliDiagnosticTest(unittest.TestCase):
    def test_non_json_diagnostic_retains_structure_without_captured_secrets(self):
        with self.assertRaises(AssertionError) as raised:
            cli_json_lines('{"operation_id":"example"}\nPRIVATE_TOKEN output\n')
        self.assertIn('line=2 bytes=', str(raised.exception))
        self.assertIn('sha256=', str(raised.exception))
        self.assertNotIn('PRIVATE_TOKEN', str(raised.exception))
        self.assertEqual(cli_json_lines('{"ok":true}\n'), [{'ok': True}])


class RuntimeCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.remote = self.base / "remote.git"
        self.a, self.b = self.base / "a", self.base / "b"
        self.command("git", "init", "--bare", str(self.remote))
        for repo in (self.a, self.b):
            self.command("git", "init", "-b", "main", str(repo))
            self.git(repo, "config", "user.name", "Fixture")
            self.git(repo, "config", "user.email", "fixture@example.invalid")
            self.git(repo, "remote", "add", "origin", str(self.remote))
        self.sa, self.sb = Store(self.a), Store(self.b)
        runtime.initialize(self.sa, "initialize", **getattr(self, "init_options", {}))
        self.git(self.a, "fetch", "origin", "main")
        self.git(self.a, "reset", "--hard", "FETCH_HEAD")

    def command(self, *args, ok=True, **kwargs):
        result = subprocess.run(args, capture_output=True, text=True, timeout=60, **kwargs)
        if ok:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def git(self, repo, *args):
        return self.command("git", "-C", str(repo), *args).stdout.strip()

    def add(self, task="t", **kwargs):
        return runtime.add_task(self.sa, "add-" + task, title="Task " + task, task_id=task, body="Instructions", **kwargs)

    def start(self, run="r", task="t", store=None, **kwargs):
        return runtime.start(store or self.sa, "start-" + run, task, "runner-" + run, run_id=run, machine_id="fixture", **kwargs)

    def prepare(self, run="r", task="t", store=None):
        path = self.base / ("work-" + run)
        runtime.prepare(store or self.sa, task, run, path)
        return path

    def output(self, work, path, content=b"output\n"):
        target = work / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return target

    def legacy_finish(self, *args, **kwargs):
        # Exercise the earlier cleanup contract with today's saved run stream.
        def legacy(run, operation, preparation, discard):
            return {"status": "pending", "run_id": run["run_id"], "machine_id": run["machine_id"],
                    "source_head": run["source_head"], "has_checkpoint": preparation["has_checkpoint"],
                    "io_manifest": preparation.get('cleanup', {}).get('io_manifest', {})}
        with patch("task_runtime.maintenance.finish_record", side_effect=legacy):
            return runtime.publish(*args, **kwargs)


class TaskRuntimeTest(RuntimeCase):
    def test_task_crud_has_fixed_paths_tombstones_and_optimistic_edits(self):
        self.add()
        runtime.edit_task(self.sb, "rename", "t", 1, title="Renamed", max_runners=2)
        with self.assertRaises(TaskError):
            runtime.edit_task(self.sa, "stale-edit", "t", 1, title="Wrong")
        runtime.edit_task(self.sb, "complete", "t", 2, status="completed", archived=True)
        self.assertEqual(runtime.list_tasks(self.sa)["tasks"], [])
        self.assertEqual(len(runtime.list_tasks(self.sa, True)["tasks"]), 1)
        runtime.edit_task(self.sb, "delete", "t", 3, status="deleted")
        with self.assertRaises(TaskError):
            runtime.add_task(self.sa, "reuse", title="New", task_id="t")
        self.assertIn("tasks/t/task.md", self.sa.snapshot().entries)

    def test_competing_clones_cannot_exceed_capacity(self):
        self.add()
        def attempt(pair):
            store, run = pair
            try:
                self.start(run, store=store)
                return True
            except (TaskError, GitFailure):
                return False
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            result = list(pool.map(attempt, [(self.sa, "one"), (self.sb, "two")]))
        self.assertEqual(sorted(result), [False, True])
        slots = runtime.load_task(self.sa.snapshot(), "t")[1]["slots"]
        self.assertEqual(sum(bool(v.get("run_id")) for v in slots.values()), 1)

    def test_capacity_item_claims_and_reduction(self):
        self.add(max_runners=2)
        self.start("one", item_key="same-item")
        with self.assertRaises(TaskError):
            self.start("duplicate", store=self.sb, item_key="same-item")
        self.start("two", store=self.sb, item_key="other")
        with self.assertRaises(TaskError):
            self.start("three")
        with self.assertRaises(TaskError):
            runtime.edit_task(self.sa, "shrink", "t", 1, max_runners=1)
        with self.assertRaises(TaskError):
            runtime.edit_task(self.sa, "close-active", "t", 1, status="completed")

    def test_replayed_start_retains_snapshot_and_does_not_launch_another_run(self):
        self.add()
        first = self.start()
        replay = self.start(store=self.sb)
        self.assertEqual(first["definition_commit"], replay["definition_commit"])
        runtime.edit_task(self.sa, "edit", "t", 1, body="New instructions")
        work = self.prepare()
        self.assertEqual((work / "tasks/t/task.md").read_text(), "Instructions")
        self.assertEqual(runtime.prepare(self.sa, "t", "r", work)["path"], str(work))
        with self.assertRaises(TaskError):
            runtime.start(self.sa, "other-op", "t", "runner-r", run_id="other")

    def test_operation_id_cannot_change_request(self):
        self.add()
        with self.assertRaises(TaskError):
            runtime.add_task(self.sb, "add-t", title="Changed request", task_id="t")

    def test_start_without_machine_replays_across_hosts_after_lost_response(self):
        self.add()
        original = self.sa.push
        def interrupt_after_push(ref, candidate, expected):
            original(ref, candidate, expected)
            raise TaskError("Simulated interruption before readback")
        with patch("task_runtime.runtime.socket.gethostname", return_value="host-a"), \
                patch.object(self.sa, "push", side_effect=interrupt_after_push), \
                self.assertRaisesRegex(TaskError, "interruption"):
            runtime.start(self.sa, "start", "t", "runner", run_id="r")
        snapshot = self.sb.snapshot()
        accepted = runtime.load_run(snapshot, "t", "r")[2]
        with patch("task_runtime.runtime.socket.gethostname", return_value="host-b"):
            replay = runtime.start(self.sb, "start", "t", "runner", run_id="r")
        self.assertEqual(replay["machine_id"], "host-a")
        self.assertEqual(replay["definition_commit"], accepted["definition_commit"])
        self.assertEqual(self.sb.fetch(), snapshot.commit)
        with self.assertRaises(TaskError):
            runtime.start(self.sb, "start", "t", "runner", run_id="r", machine_id="host-b")

    def test_primary_dirty_state_and_staging_are_untouched(self):
        (self.a / "unrelated").write_text("local content")
        self.git(self.a, "add", "unrelated")
        before = (self.git(self.a, "rev-parse", "HEAD"), (self.a / ".git/index").read_bytes())
        self.add()
        self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out", b"binary\0\xff\r\n")
        runtime.publish(self.sa, "publish", "t", "r")
        self.assertEqual(before, (self.git(self.a, "rev-parse", "HEAD"), (self.a / ".git/index").read_bytes()))
        self.assertEqual((self.a / "unrelated").read_text(), "local content")
        self.assertEqual(self.sa.snapshot().read("tasks/t/runs/r/artifacts/out"), b"binary\0\xff\r\n")

    def test_two_runs_publish_separate_artifacts_and_release_separately(self):
        self.add(max_runners=2)
        for name, store in (("one", self.sa), ("two", self.sb)):
            self.start(name, store=store)
            work = self.prepare(name, store=store)
            self.output(work, f"tasks/t/runs/{name}/artifacts/report", name.encode())
        for name, store in (("one", self.sa), ("two", self.sb)):
            runtime.publish(store, "finish-" + name, "t", name, outcome="succeeded")
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.read("tasks/t/runs/one/artifacts/report"), b"one")
        self.assertEqual(snapshot.read("tasks/t/runs/two/artifacts/report"), b"two")
        self.assertEqual(runtime.load_task(snapshot, "t")[0]["status"], "open")
        self.assertTrue(all(not s["run_id"] for s in runtime.load_task(snapshot, "t")[1]["slots"].values()))

    def test_shared_data_revision_prevents_nonoverlapping_write_skew(self):
        self.add(max_runners=2)
        for name, store in (("one", self.sa), ("two", self.sb)):
            self.start(name, store=store)
            work = self.prepare(name, store=store)
            self.output(work, "tasks/t/data/" + name, b"based on original data")
        runtime.publish(self.sa, "first", "t", "one")
        with self.assertRaisesRegex(TaskError, "Shared task data changed"):
            runtime.publish(self.sb, "second", "t", "two")
        self.assertNotIn("tasks/t/data/two", self.sa.snapshot().entries)

    def test_fencing_refuses_late_publication_and_preserves_replacement(self):
        self.add()
        self.start("old")
        work = self.prepare("old")
        self.output(work, "tasks/t/data/out", b"late")
        runtime.recover(self.sb, "revoke", "t", "old", stopped=True, settled=True, evidence="fixture:stopped")
        new = self.start("new", store=self.sb)
        self.assertEqual(new["generation"], 2)
        with self.assertRaises(TaskError):
            runtime.publish(self.sa, "late", "t", "old")
        runtime.recover(self.sb, "revoke", "t", "old", stopped=True, settled=True, evidence="fixture:stopped")
        self.assertEqual(runtime.check(self.sb, "t", "new")["generation"], 2)

    def test_revocation_between_validation_and_push_rejects_old_candidate(self):
        self.add()
        self.start("old")
        work = self.prepare("old")
        self.output(work, "tasks/t/data/out")
        original = self.sa.push
        fired = False
        def race(ref, candidate, expected):
            nonlocal fired
            if ref == self.sa.ref and not fired:
                fired = True
                runtime.recover(self.sb, "revoke", "t", "old", stopped=True, settled=True, evidence="fixture:stop")
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=race), self.assertRaises(TaskError):
            runtime.publish(self.sa, "late", "t", "old")
        self.assertNotIn("tasks/t/data/out", self.sa.snapshot().entries)

    def test_lost_push_reply_reconciles_original_operation(self):
        self.add()
        original = self.sa.push
        def lost(ref, candidate, expected):
            original(ref, candidate, expected)
            raise GitFailure("lost", {"definite_rejection": False})
        with patch.object(self.sa, "push", side_effect=lost):
            first = self.start()
        replay = self.start(store=self.sb)
        self.assertEqual(first["run_id"], replay["run_id"])

    def test_unknown_push_freezes_candidate_and_retries_exact_commit(self):
        self.add()
        with patch.object(self.sa, "push", side_effect=GitFailure("unknown", {"definite_rejection": False})):
            with self.assertRaises(TaskError):
                self.start()
        journal = self.sa.local / "operations/start-r.json"
        pending = json.loads(journal.read_text())["pending"]["candidate"]
        self.start()
        self.assertTrue(self.sa.contains(self.sa.fetch(), pending))

    def test_pending_candidate_survives_gc_until_reconciled(self):
        self.add()
        with patch.object(self.sa, "push", side_effect=GitFailure("unknown", {"definite_rejection": False})):
            with self.assertRaises(TaskError):
                self.start()
        journal = self.sa.local / "operations/start-r.json"
        candidate = json.loads(journal.read_text())["pending"]["candidate"]
        self.git(self.a, "gc", "--prune=now")
        self.start()
        self.assertTrue(self.sa.contains(self.sa.fetch(), candidate))
        self.assertFalse(self.git(self.a, "for-each-ref", "refs/task-runtime-pending/"))

    def test_interrupted_start_retries_after_another_runner_advances_main(self):
        self.add()
        with patch.object(self.sa, "push", side_effect=TaskError("Interrupted before push")), self.assertRaises(TaskError):
            self.start()
        pending = json.loads((self.sa.local / "operations/start-r.json").read_text())["pending"]
        runtime.edit_task(self.sb, "edit", "t", 1, title="Updated")
        advanced = self.sb.fetch()
        result = self.start()
        self.assertEqual(result["definition_commit"], advanced)
        self.assertEqual(result["definition_revision"], 2)
        accepted = self.sa.fetch()
        self.assertNotEqual(accepted, pending["candidate"])
        with self.assertRaises((TaskError, GitFailure)):
            self.sa.push(self.sa.ref, pending["candidate"], pending["expected"])
        self.assertEqual(self.start()["run_id"], result["run_id"])
        self.assertEqual(self.sa.fetch(), accepted)

    def test_interrupted_finish_retries_with_same_output_after_main_advances(self):
        self.add()
        self.start()
        work = self.prepare()
        output = self.output(work, "tasks/t/runs/r/artifacts/out", b"original")
        original = self.sa.push
        def interrupt(ref, candidate, expected):
            if ref == self.sa.ref:
                raise TaskError("Interrupted before push")
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=interrupt), self.assertRaises(TaskError):
            runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        runtime.edit_task(self.sb, "edit", "t", 1, title="Updated")
        output.write_bytes(b"later unsaved output")
        result = runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        self.assertEqual(result["status"], "succeeded")
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.read("tasks/t/runs/r/artifacts/out"), b"original")
        self.assertEqual(runtime.load_task(snapshot, "t")[0]["title"], "Updated")
        self.assertIsNone(runtime.load_task(snapshot, "t")[1]["slots"]["1"]["run_id"])

    def test_unknown_candidate_is_retained_if_authority_did_not_advance(self):
        self.add()
        before = self.sa.fetch()
        with patch.object(self.sa, "push", side_effect=TaskError("Interrupted before push")), self.assertRaises(TaskError):
            self.start()
        journal = self.sa.local / "operations/start-r.json"
        pending = json.loads(journal.read_text())["pending"]
        # Simulate an external rewrite, which is outside the writer protocol.
        earlier = self.git(self.a, "rev-parse", before + "^")
        self.git(self.remote, "update-ref", self.sa.ref, earlier)
        with self.assertRaisesRegex(TaskError, "unknown"):
            self.start()
        self.assertEqual(json.loads(journal.read_text())["pending"], pending)
        self.assertEqual(self.sa.remote_tip(self.sa.ref), earlier)

    def test_cross_clone_finish_retry_cannot_skip_unpublished_checkpoint(self):
        self.add()
        run = self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out", b"checkpoint output")
        original = self.sa.push
        def interrupt(ref, candidate, expected):
            if ref == self.sa.ref:
                raise TaskError("Interrupted before authority publication")
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=interrupt), self.assertRaises(TaskError):
            runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        checkpoint = self.sb.fetch(run["work_ref"])
        self.assertEqual(Snapshot(self.sb, checkpoint).read("tasks/t/runs/r/artifacts/out"), b"checkpoint output")
        with self.assertRaisesRegex(TaskError, "workspace"):
            runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded")
        runtime.check(self.sb, "t", "r")
        self.assertIsNone(self.sb.snapshot().json(CONTROL + "/operations/finish.json"))
        result = runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        self.assertEqual(result["source_head"], checkpoint)
        self.assertEqual(runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded"), result)
        self.assertEqual(self.sb.snapshot().read("tasks/t/runs/r/artifacts/out"), b"checkpoint output")

    def test_cross_clone_finish_can_resume_from_confirmed_restored_checkpoint(self):
        self.add()
        run = self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out", b"checkpoint output")
        original = self.sa.push
        def interrupt(ref, candidate, expected):
            if ref == self.sa.ref:
                raise TaskError("Interrupted before authority publication")
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=interrupt), self.assertRaises(TaskError):
            runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        checkpoint = self.sb.fetch(run["work_ref"])
        runtime.prepare(self.sb, "t", "r", self.base / "restored-finish", restore=True, stopped=True, settled=True)
        result = runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded")
        self.assertEqual(result["source_head"], checkpoint)
        self.assertEqual(self.sb.snapshot().read("tasks/t/runs/r/artifacts/out"), b"checkpoint output")
        self.assertEqual(runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded"), result)

    def test_finishing_without_workspace_requires_explicit_discard(self):
        self.add()
        self.start()
        for outcome in ("succeeded", "failed", "cancelled"):
            with self.subTest(outcome=outcome), self.assertRaisesRegex(TaskError, "workspace"):
                runtime.publish(self.sa, "finish-" + outcome, "t", "r", outcome=outcome)
        result = runtime.publish(self.sa, "cancel", "t", "r", outcome="cancelled", discard_output=True)
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(self.sa.snapshot().json("tasks/t/runs/r/cleanup.json")["status"], "retained")

    def test_heartbeat_changes_only_its_ref_and_requires_current_authority(self):
        self.add()
        run = self.start()
        before = self.sa.fetch()
        self.assertEqual(runtime.heartbeat(self.sa, "t", "r")["sequence"], 1)
        self.assertEqual(runtime.heartbeat(self.sa, "t", "r")["sequence"], 2)
        self.assertEqual(before, self.sa.fetch())
        heartbeat = Snapshot(self.sa, self.sa.fetch(run["heartbeat_ref"]))
        self.assertEqual(heartbeat.json(CONTROL + '/config.json')['stream'], 'run')
        self.assertEqual(heartbeat.json('state.json')['sequence'], 2)
        self.assertEqual(run['heartbeat_ref'], 'refs/heads/task-io/t/runs/r')
        self.assertIsNone(self.sa.remote_tip('refs/heads/task-runtime/t/r/state'))
        self.assertFalse(runtime.local_state(self.sa, 't', 'r').with_suffix('.heartbeat.json').exists())
        runtime.recover(self.sb, "revoke", "t", "r", stopped=True, settled=True, evidence="fixture:stop")
        with self.assertRaises(TaskError):
            runtime.heartbeat(self.sa, "t", "r")

    def test_staleness_uses_observed_sha_and_monotonic_elapsed_time(self):
        self.add(recovery_mode="git_only")
        self.start()
        runtime.heartbeat(self.sa, "t", "r")
        with patch("task_runtime.runtime.time.monotonic", return_value=100):
            self.assertFalse(runtime.observe(self.sb, "t", "r")["stale"])
        with patch("task_runtime.runtime.time.monotonic", return_value=401):
            self.assertTrue(runtime.observe(self.sb, "t", "r")["stale"])
            runtime.recover(self.sb, "reap", "t", "r", stale=True)
        self.assertEqual(runtime.load_run(self.sa.snapshot(), "t", "r")[2]["status"], "lost")
        runtime.recover(self.sb, "reap", "t", "r", stale=True)

    def test_updated_heartbeat_and_observer_restart_reset_suspicion(self):
        self.add(recovery_mode="git_only")
        self.start()
        runtime.heartbeat(self.sa, "t", "r")
        with patch("task_runtime.runtime.time.monotonic", return_value=100):
            runtime.observe(self.sb, "t", "r")
        runtime.heartbeat(self.sa, "t", "r")
        with patch("task_runtime.runtime.time.monotonic", return_value=1000):
            self.assertFalse(runtime.observe(self.sb, "t", "r")["stale"])
        with patch("task_runtime.runtime.time.monotonic", return_value=2000), patch("task_runtime.runtime.observer_boot", return_value="new-boot"):
            self.assertFalse(runtime.observe(self.sb, "t", "r")["stale"])

    def test_manual_external_run_cannot_be_recovered_only_by_age(self):
        self.add()
        self.start()
        with patch("task_runtime.runtime.time.monotonic", return_value=100):
            runtime.observe(self.sb, "t", "r")
        with patch("task_runtime.runtime.time.monotonic", return_value=1000), self.assertRaises(TaskError):
            runtime.recover(self.sb, "reap", "t", "r", stale=True)
        with self.assertRaises(TaskError):
            runtime.recover(self.sb, "unconfirmed", "t", "r", stopped=True, evidence="fixture:stop")
        runtime.check(self.sa, "t", "r")

    def test_forbidden_history_and_symlink_outputs_are_rejected(self):
        self.add()
        self.start()
        work = self.prepare()
        (work / "tasks/t/task.md").write_text("forbidden")
        self.git(work, "add", ".")
        self.git(work, "commit", "-m", "forbidden intermediate edit")
        self.git(work, "revert", "--no-edit", "HEAD")
        with self.assertRaises(TaskError):
            runtime.publish(self.sa, "bad-history", "t", "r")

    def test_symlink_output_is_not_published(self):
        self.add()
        self.start()
        work = self.prepare()
        path = self.output(work, "tasks/t/data/output")
        path.unlink()
        path.symlink_to("../../task.md")
        with self.assertRaises(TaskError):
            runtime.publish(self.sa, "symlink", "t", "r")

    def test_provisioned_workspace_needs_no_primary_checkout(self):
        self.add()
        definition = self.sb.fetch()
        self.git(self.b, "reset", "--hard", definition)
        self.start(store=self.sb)
        state = runtime.prepare(self.sb, "t", "r", self.b, "workspace")
        self.assertEqual(state["mode"], "workspace")
        self.output(self.b, "tasks/t/runs/r/artifacts/out")
        runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded")
        result = lifecycle.cleanup(self.sb, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertEqual(result["local"], "absent")
        self.assertFalse(runtime.local_state(self.sb, 't', 'r').exists())
        self.assertTrue(self.b.exists())

    def test_legacy_finish_and_cleanup_keep_main_results_without_source_tags(self):
        self.add()
        self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out")
        runtime.heartbeat(self.sa, "t", "r")
        finished = self.legacy_finish(self.sa, "finish", "t", "r", outcome="succeeded")
        self.assertTrue(lifecycle.cleanup(self.sa, "cleanup", "t", "r")["dry_run"])
        lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertFalse(work.exists())
        self.assertIsNone(self.sa.remote_tip("refs/tags/task-runs/t/r"))
        self.assertEqual(self.sa.snapshot().read('tasks/t/runs/r/artifacts/out'), b'output\n')
        self.assertIsNone(self.sa.remote_tip(finished["heartbeat_ref"]))
        self.assertIsNone(self.sa.remote_tip(finished["work_ref"]))
        runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded")
        lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)

    def test_cleanup_refuses_untracked_or_ignored_files(self):
        self.add()
        self.start()
        work = self.prepare()
        with patch("task_runtime.maintenance.maintain"):
            runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        (work / "unsaved").write_text("keep me")
        with self.assertRaises(TaskError):
            lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertTrue((work / "unsaved").exists())

    def test_supervisor_sends_heartbeat_owns_child_and_wont_repeat_command(self):
        self.add(heartbeat_seconds=1, stale_seconds=3)
        self.start()
        self.prepare()
        command = [sys.executable, "-c", "import time; print('work'); time.sleep(1.2)"]
        result = lifecycle.execute(self.sa, "t", "r", command)
        self.assertEqual(result["exit_code"], 0)
        with self.assertRaises(TaskError):
            lifecycle.execute(self.sa, "t", "r", command)

    def test_code_only_zipapp_works_in_independent_company_repository(self):
        bundle = self.base / "taskctl.pyz"
        self.command(sys.executable, str(ROOT / "bin/build-task-inbox"), "--output", str(bundle))
        with zipfile.ZipFile(bundle) as archive:
            self.assertEqual(archive.read("LICENSE"), (ROOT / "LICENSE").read_bytes())
            self.assertFalse(any(p.startswith(("active/", "archived/", "sites/")) for p in archive.namelist()))
            for name in archive.namelist():
                if name.endswith(".py"):
                    self.assertNotIn(b"rsahara", archive.read(name))
        result = self.command(sys.executable, str(bundle), "--repo", str(self.b), "--operation-id", "company-task", "task", "add", "--title", "Company operation", "--id", "company")
        self.assertEqual(json.loads(result.stdout)["id"], "company")
        listing = self.command(sys.executable, str(bundle), "--repo", str(self.b), "task", "list")
        self.assertEqual(json.loads(listing.stdout)["tasks"][0]["id"], "company")

    def test_restore_in_another_clone_uses_durable_checkpoint_and_pinned_instructions(self):
        self.add()
        original = self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/first", b"checkpoint")
        runtime.publish(self.sa, "checkpoint", "t", "r")
        restored = self.base / "restored"
        with self.assertRaises(TaskError):
            runtime.prepare(self.sb, "t", "r", restored)
        runtime.prepare(self.sb, "t", "r", restored, restore=True, stopped=True, settled=True)
        self.assertEqual((restored / "tasks/t/runs/r/artifacts/first").read_bytes(), b"checkpoint")
        self.assertEqual(runtime.check(self.sb, "t", "r")["definition_commit"], original["definition_commit"])
        self.output(restored, "tasks/t/runs/r/artifacts/second")
        runtime.publish(self.sb, "finish-restored", "t", "r", outcome="succeeded")

    def test_workspace_can_be_reused_after_finished_run(self):
        self.add()
        self.git(self.b, "reset", "--hard", self.sb.fetch())
        self.start(store=self.sb)
        runtime.prepare(self.sb, "t", "r", self.b, "workspace")
        runtime.publish(self.sb, "finish", "t", "r", outcome="succeeded")
        self.git(self.b, "reset", "--hard", self.sb.fetch())
        self.start("next", store=self.sb)
        runtime.prepare(self.sb, "t", "next", self.b, "workspace")

    def test_workspace_can_be_reused_after_confirmed_recovery_and_inspection(self):
        self.add()
        self.git(self.b, "reset", "--hard", self.sb.fetch())
        old = self.start(store=self.sb)
        runtime.prepare(self.sb, "t", "r", self.b, "workspace")
        self.output(self.b, "tasks/t/runs/r/artifacts/checkpoint", b"retained")
        runtime.publish(self.sb, "checkpoint", "t", "r")
        checkpoint = self.sb.fetch(old["work_ref"])
        runtime.recover(self.sa, "recover", "t", "r", stopped=True, settled=True, evidence="fixture:stopped")
        self.start("next", store=self.sb)
        unsaved = self.output(self.b, "tasks/t/data/unsaved", b"preserve me")
        with self.assertRaisesRegex(TaskError, "clean"):
            runtime.prepare(self.sb, "t", "next", self.b, "workspace")
        self.assertEqual(unsaved.read_bytes(), b"preserve me")
        unsaved.rename(self.base / "preserved-output")
        definition = runtime.check(self.sb, "t", "next")["definition_commit"]
        self.git(self.b, "reset", "--hard", definition)
        state = runtime.prepare(self.sb, "t", "next", self.b, "workspace")
        self.assertEqual(state["run_id"], "next")
        reservation = json.loads((self.sb.local / "workspace-reservation.json").read_text())
        self.assertEqual(reservation["run_id"], "next")
        self.assertEqual(self.sb.fetch(old["work_ref"]), checkpoint)
        self.assertEqual(self.sb.snapshot().read("tasks/t/runs/r/artifacts/checkpoint"), b"retained")
        self.assertEqual((self.base / "preserved-output").read_bytes(), b"preserve me")

    def test_stale_recovery_does_not_release_provisioned_workspace(self):
        self.add(recovery_mode="git_only")
        self.git(self.b, "reset", "--hard", self.sb.fetch())
        self.start(store=self.sb)
        runtime.prepare(self.sb, "t", "r", self.b, "workspace")
        with patch("task_runtime.runtime.time.monotonic", return_value=100):
            runtime.observe(self.sa, "t", "r")
        with patch("task_runtime.runtime.time.monotonic", return_value=401):
            runtime.recover(self.sa, "recover-stale", "t", "r", stale=True)
        self.git(self.b, "reset", "--hard", self.sb.fetch())
        self.start("next", store=self.sb)
        with self.assertRaisesRegex(TaskError, "reserved"):
            runtime.prepare(self.sb, "t", "next", self.b, "workspace")
        reservation = json.loads((self.sb.local / "workspace-reservation.json").read_text())
        self.assertEqual(reservation["run_id"], "r")

    def test_cancel_can_release_a_data_conflict_without_adopting_or_deleting_output(self):
        self.add(max_runners=2)
        for name, store in (("one", self.sa), ("two", self.sb)):
            self.start(name, store=store)
            work = self.prepare(name, store=store)
            self.output(work, "tasks/t/data/" + name)
        runtime.publish(self.sa, "published", "t", "one")
        runtime.publish(self.sb, "cancel", "t", "two", outcome="cancelled", discard_output=True)
        self.assertTrue((self.base / "work-two/tasks/t/data/two").exists())
        self.assertNotIn("tasks/t/data/two", self.sa.snapshot().entries)
        with self.assertRaises(TaskError):
            lifecycle.cleanup(self.sb, "cleanup", "t", "two", apply=True, stopped=True)
        self.start("replacement", store=self.sb)

    def test_deleted_authority_branch_is_not_recreated_by_publication(self):
        self.add()
        self.start()
        work = self.prepare()
        self.output(work, "tasks/t/data/out")
        original = self.sa.push
        def remove_before_push(ref, candidate, expected):
            if ref == self.sa.ref:
                self.git(self.remote, "update-ref", "-d", ref)
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=remove_before_push), self.assertRaises((TaskError, GitFailure)):
            runtime.publish(self.sa, "deleted", "t", "r")
        self.assertIsNone(self.sa.remote_tip(self.sa.ref))

    def test_copy_of_authority_branch_cannot_coordinate_another_writer(self):
        self.add()
        self.start()
        self.sa.push("refs/heads/feature", self.sa.fetch(), None)
        alternate = Store(self.b, branch="feature")
        with self.assertRaises(TaskError):
            runtime.heartbeat(alternate, "t", "r")

    def test_stop_adapter_binds_evidence_and_schedule_reaping_needs_no_daemon(self):
        self.add()
        self.start()
        adapter = self.base / "executor-stop-check"
        adapter.write_text("#!" + sys.executable + "\nimport json,sys\nr=json.load(sys.stdin)\n"
            "r.update(writer_stopped=True, children_stopped=True, external_operations_settled=True, "
            "pending_git_operations_settled=True, evidence='fixture:verified-receipts')\nprint(json.dumps(r))\n")
        adapter.chmod(0o755)
        result = lifecycle.reap(self.sb, "t", apply=True, stop_check=str(adapter))
        self.assertEqual(result["runs"][0]["status"], "lost")
        self.start("next", store=self.sb)

    def test_stop_adapter_with_unknown_children_does_not_release_authority(self):
        self.add()
        self.start()
        adapter = self.base / "executor-stop-check"
        adapter.write_text("#!" + sys.executable + "\nimport json,sys\nr=json.load(sys.stdin)\n"
            "r.update(writer_stopped=True, children_stopped=False, external_operations_settled=True, "
            "pending_git_operations_settled=True, evidence='fixture:incomplete')\nprint(json.dumps(r))\n")
        adapter.chmod(0o755)
        result = lifecycle.reap(self.sb, "t", apply=True, stop_check=str(adapter))
        self.assertTrue(result["needs_attention"])
        runtime.check(self.sa, "t", "r")

    def test_stop_adapter_timeout_returns_json_and_retains_authority(self):
        self.add()
        self.start()
        adapter = self.base / "executor-stop-check"
        adapter.write_text("fixture")
        original = lifecycle.git_support.run_process
        def timeout(command, *args, **kwargs):
            if command == [str(adapter)]:
                raise subprocess.TimeoutExpired(command, 60, output=b"private output", stderr=b"private error")
            return original(command, *args, **kwargs)
        stderr = io.StringIO()
        with patch.object(lifecycle.git_support, "run_process", side_effect=timeout), redirect_stderr(stderr):
            code = cli.main(["--repo", str(self.a), "--operation-id", "recover", "run", "recover",
                             "--task", "t", "--run-id", "r", "--stop-check", str(adapter)])
        self.assertEqual(code, 1)
        messages = cli_json_lines(stderr.getvalue())
        self.assertEqual(messages[0]["operation_id"], "recover")
        self.assertIn("timed out", messages[-1]["error"])
        self.assertIn("ownership retained", messages[-1]["error"])
        self.assertNotIn("private", stderr.getvalue())
        self.assertEqual(runtime.check(self.sa, "t", "r")["status"], "running")

    def test_cleanup_interruption_resumes_from_preserved_source(self):
        self.add()
        self.start()
        work = self.prepare()
        self.output(work, "tasks/t/runs/r/artifacts/out")
        run = self.legacy_finish(self.sa, "finish", "t", "r", outcome="succeeded")
        original = self.sa.push
        def stop_before_delete(ref, candidate, expected):
            if ref == run["work_ref"] and candidate is None:
                raise TaskError("injected interruption")
            return original(ref, candidate, expected)
        with patch.object(self.sa, "push", side_effect=stop_before_delete), self.assertRaises(TaskError):
            lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertFalse(work.exists())
        result = lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertEqual(result["status"], "done")

    def test_cleanup_retry_retains_heartbeat_changed_after_preservation(self):
        self.add()
        run = self.start()
        self.prepare()
        runtime.heartbeat(self.sa, "t", "r")
        first = self.sa.fetch(run["heartbeat_ref"])
        beat = Snapshot(self.sa, first).json("state.json")
        beat["sequence"] += 1
        second = self.sa.commit(first, {"state.json": encode(beat)}, "Delayed heartbeat")
        self.legacy_finish(self.sa, "finish", "t", "r", outcome="succeeded")
        original = self.sa.transact
        def delayed_heartbeat(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[0] == "cleanup":
                self.sa.push(run["heartbeat_ref"], second, first)
            return result
        with patch.object(self.sa, "transact", side_effect=delayed_heartbeat):
            with self.assertRaisesRegex(TaskError, "changed"):
                lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        for _ in range(2):
            with self.assertRaisesRegex(TaskError, "changed"):
                lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        saved = self.sa.snapshot().json("tasks/t/runs/r/cleanup.json")
        self.assertEqual(saved["heartbeat_checkpoint"]["sequence"], 1)
        self.assertEqual(self.sa.remote_tip(run["heartbeat_ref"]), second)
        result = lifecycle.cleanup(self.sa, "preserve-changed", "t", "r", apply=True, stopped=True)
        self.assertEqual(result["status"], "done")
        self.assertEqual(result["heartbeat_checkpoint"]["sequence"], 2)
        self.assertIsNone(self.sa.remote_tip(run["heartbeat_ref"]))

    def test_source_distribution_can_be_customized_without_personal_data(self):
        output = self.base / "source.tar.gz"
        self.command(sys.executable, str(ROOT / "bin/build-task-inbox"), "--format", "source", "--output", str(output))
        destination = self.base / "source"
        destination.mkdir()
        with tarfile.open(output) as archive:
            for member in archive.getmembers():
                self.assertTrue(member.name.startswith("task-runtime/"))
                self.assertNotIn("..", Path(member.name).parts)
                self.assertTrue(member.isfile())
                self.assertFalse(any(p in ("active", "archived", ".git", ".aws") for p in Path(member.name).parts))
                self.assertEqual((member.uname, member.gname), ("", ""))
                target = destination / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
                target.chmod(member.mode)
        exported = destination / "task-runtime"
        self.assertEqual((exported / "LICENSE").read_bytes(), (ROOT / "LICENSE").read_bytes())
        manifest = json.loads((exported / "MANIFEST.json").read_text())
        import hashlib
        for name, expected in manifest["files"].items():
            self.assertEqual(hashlib.sha256((exported / name).read_bytes()).hexdigest(), expected)
        module = exported / "foundation/task_runtime/__init__.py"
        module.write_text('VERSION = "company-custom"\n')
        result = self.command(sys.executable, str(exported / "bin/task-inbox"), "--version")
        self.assertEqual(result.stdout.strip(), "company-custom")

    def test_supervisor_stops_worker_on_heartbeat_failure(self):
        self.add(heartbeat_seconds=1, stale_seconds=3)
        self.start()
        self.prepare()
        original = runtime.heartbeat
        calls = 0
        def lose(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                raise TaskError("injected lost authority")
            return original(*args, **kwargs)
        with patch.object(runtime, "heartbeat", side_effect=lose), self.assertRaises(TaskError):
            lifecycle.execute(self.sa, "t", "r", [sys.executable, "-c", "import time; time.sleep(30)"])
        marker = runtime.local_state(self.sa, "t", "r").with_suffix(".execution.json")
        receipt = json.loads(marker.read_text())
        self.assertEqual(receipt["phase"], "interrupted")
        with self.assertRaises(ProcessLookupError):
            os.kill(receipt["pid"], 0)

    def test_stop_adapter_replay_does_not_recover_replacement(self):
        self.add()
        self.start()
        adapter = self.base / "executor-stop-check"
        adapter.write_text("#!" + sys.executable + "\nimport json,sys\nr=json.load(sys.stdin)\n"
            "r.update(writer_stopped=True, children_stopped=True, external_operations_settled=True, "
            "pending_git_operations_settled=True, evidence='fixture:verified')\nprint(json.dumps(r))\n")
        adapter.chmod(0o755)
        lifecycle.reconcile_stop(self.sb, "recover", "t", "r", str(adapter))
        self.start("next", store=self.sb)
        lifecycle.reconcile_stop(self.sb, "recover", "t", "r", str(adapter))
        runtime.check(self.sa, "t", "next")

    def test_execution_claim_prevents_duplicate_launch_from_another_clone(self):
        self.add()
        self.start()
        self.prepare()
        runtime.prepare(self.sb, "t", "r", self.base / "other-work")
        launched = self.base / "launches"
        release = self.base / "release-worker"
        command = [sys.executable, "-c",
            "from pathlib import Path; import sys,time\n"
            "with Path(sys.argv[1]).open('a') as out: out.write('launch\\n')\n"
            "while not Path(sys.argv[2]).exists(): time.sleep(0.01)\n",
            str(launched), str(release)]
        def attempt(store):
            try:
                lifecycle.execute(store, "t", "r", command)
                return True
            except (TaskError, GitFailure):
                return False
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            first = pool.submit(attempt, self.sa)
            try:
                deadline = time.monotonic() + 10
                while not launched.exists() and not first.done() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(launched.exists(), "first worker never started")
                second = pool.submit(attempt, self.sb)
                deadline = time.monotonic() + 10
                while not second.done() and launched.read_text().count("launch") < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                release.touch()
            self.assertTrue(first.result(timeout=10))
            self.assertFalse(second.result(timeout=10))
        self.assertEqual(launched.read_text(), "launch\n")

    def test_publication_supports_file_directory_replacements_in_both_directions(self):
        self.add()
        self.start()
        work = self.prepare()
        item = self.output(work, "tasks/t/data/item", b"file")
        self.output(work, "tasks/t/data/keep", b"unrelated")
        runtime.publish(self.sa, "save-file", "t", "r")
        item.unlink()
        self.output(work, "tasks/t/data/item/nested/value", b"nested\x00\xff")
        runtime.publish(self.sa, "save-directory", "t", "r")
        snapshot = self.sa.snapshot()
        self.assertNotIn("tasks/t/data/item", snapshot.entries)
        self.assertEqual(snapshot.read("tasks/t/data/item/nested/value"), b"nested\x00\xff")
        (item / "nested/value").unlink()
        (item / "nested").rmdir()
        item.rmdir()
        item.write_bytes(b"file again")
        item.chmod(0o755)
        runtime.publish(self.sa, "finish-file", "t", "r", outcome="succeeded")
        snapshot = self.sa.snapshot()
        self.assertEqual(snapshot.read("tasks/t/data/item"), b"file again")
        self.assertEqual(snapshot.entries["tasks/t/data/item"][0], "100755")
        self.assertNotIn("tasks/t/data/item/nested/value", snapshot.entries)
        self.assertEqual(snapshot.read("tasks/t/data/keep"), b"unrelated")

    def test_lost_execution_claim_reply_does_not_allow_another_clone_to_launch(self):
        self.add()
        self.start()
        self.prepare()
        runtime.prepare(self.sb, "t", "r", self.base / "other-work")
        launched = self.base / "launches"
        command = [sys.executable, "-c",
                   "from pathlib import Path; import sys\n"
                   "with Path(sys.argv[1]).open('a') as out: out.write('launch\\n')\n", str(launched)]
        original = self.sa.push
        def lose_reply(ref, candidate, expected):
            original(ref, candidate, expected)
            if ref == self.sa.ref:
                raise GitFailure("lost response", {"definite_rejection": False})
        with patch.object(self.sa, "push", side_effect=lose_reply):
            self.assertEqual(lifecycle.execute(self.sa, "t", "r", command)["exit_code"], 0)
        with self.assertRaises(TaskError):
            lifecycle.execute(self.sb, "t", "r", command)
        self.assertEqual(launched.read_text(), "launch\n")

    def test_execution_claim_is_retained_if_owner_disappears_before_spawn(self):
        self.add()
        self.start()
        self.prepare()
        runtime.prepare(self.sb, "t", "r", self.base / "other-work")
        launched = self.base / "launches"
        command = [sys.executable, "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).touch()", str(launched)]
        original = self.sa.transact
        def disappear(*args, **kwargs):
            original(*args, **kwargs)
            raise TaskError("Owner disappeared after claiming execution")
        with patch.object(self.sa, "transact", side_effect=disappear), self.assertRaises(TaskError):
            lifecycle.execute(self.sa, "t", "r", command)
        with self.assertRaises(TaskError):
            lifecycle.execute(self.sb, "t", "r", command)
        self.assertFalse(launched.exists())
        self.assertIn("execution_claim", runtime.check(self.sb, "t", "r"))

    def test_initialization_rejects_authority_inside_runtime_ref_namespace(self):
        original = self.sa.remote_tip(self.sa.ref)
        for branch in ("task-runtime", "task-runtime/t/r/state", "task-runtime/t/r/work"):
            with self.subTest(branch=branch):
                store = Store(self.b, branch=branch)
                with self.assertRaises(TaskError):
                    runtime.initialize(store, "reserved-init")
                self.assertIsNone(store.remote_tip(store.ref))
        self.assertEqual(self.sa.remote_tip(self.sa.ref), original)

    def test_cleanup_replay_cannot_delete_authority_aliased_by_a_run_ref(self):
        self.add()
        self.start()
        self.prepare()
        runtime.publish(self.sa, "finish", "t", "r", outcome="succeeded")
        lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        snapshot = self.sa.snapshot()
        path = "tasks/t/runs/r/run.json"
        record = snapshot.metadata(path)
        record["heartbeat_ref"] = self.sa.ref
        candidate = self.sa.commit(snapshot.commit, {path: encode(record)}, "Legacy colliding run metadata")
        self.sa.push(self.sa.ref, candidate, snapshot.commit)
        with self.assertRaises(TaskError):
            lifecycle.cleanup(self.sa, "cleanup", "t", "r", apply=True, stopped=True)
        self.assertEqual(self.sa.remote_tip(self.sa.ref), candidate)

    def test_colliding_existing_configuration_blocks_reads_and_operation_replay(self):
        self.add()
        snapshot = self.sa.snapshot()
        config = snapshot.json(CONTROL + "/config.json")
        config["ref_prefix"] = "main"
        candidate = self.sa.commit(snapshot.commit, {CONTROL + "/config.json": encode(config)}, "Legacy colliding configuration")
        self.sa.push(self.sa.ref, candidate, snapshot.commit)
        with self.assertRaises(TaskError):
            runtime.list_tasks(self.sa)
        with self.assertRaises(TaskError):
            self.add()
        self.assertEqual(self.sa.remote_tip(self.sa.ref), candidate)

    def test_ref_deletion_primitive_refuses_the_authority_branch(self):
        before = self.sa.fetch()
        with self.assertRaises(TaskError):
            self.sa.push(self.sa.ref, None, before)
        self.assertEqual(self.sa.remote_tip(self.sa.ref), before)


class RuntimeInputValidationTest(unittest.TestCase):
    def test_positive_integers_reject_booleans_fractions_and_out_of_range_values(self):
        for value in (None, True, False, 0, -1, 1.5, '1', 1025):
            with self.subTest(value=value), self.assertRaises(TaskError):
                runtime.positive(value, 'capacity', 1024)
        self.assertEqual(runtime.positive(1, 'capacity', 1024), 1)
        self.assertEqual(runtime.positive(1024, 'capacity', 1024), 1024)

    def test_invalid_admission_inputs_do_not_touch_local_or_remote_state(self):
        store = Mock()
        for options in [dict(machine_id=123), dict(machine_id='x' * 257), dict(machine_id='host\n'),
                        dict(item_key=123), dict(item_key=''), dict(item_key='x' * 513),
                        dict(resume_from='old'), dict(checkpoint_id='saved')]:
            with self.subTest(options=options), self.assertRaises(TaskError):
                runtime.start(store, 'start', 't', 'runner', **options)
        for options in [dict(title=None), dict(title='  '), dict(title='Task', recovery_mode='unknown'),
                        dict(title='Task', heartbeat_seconds=60, stale_seconds=60),
                        dict(title='Task', priority=0), dict(title='Task', max_runners=1025)]:
            with self.subTest(options=options), self.assertRaises(TaskError):
                runtime.add_task(store, 'add', **options)
        self.assertEqual(store.mock_calls, [])


class RuntimeStateValidationTest(RuntimeCase):
    def test_task_edits_reject_invalid_states_titles_and_archiving_without_commit(self):
        self.add()
        before = self.sa.fetch()
        for options in [dict(status='invalid'), dict(title=' '), dict(archived=True), dict(priority=0)]:
            with self.subTest(options=options), self.assertRaises(TaskError):
                runtime.edit_task(self.sa, 'invalid-' + next(iter(options)), 't', 1, **options)
            self.assertEqual(self.sb.fetch(), before)
        runtime.edit_task(self.sa, 'delete', 't', 1, status='deleted')
        with self.assertRaisesRegex(TaskError, 'remain reserved'):
            runtime.edit_task(self.sb, 'edit-deleted', 't', 2, title='Reused')

    def test_unavailable_tasks_missing_runs_and_reused_run_ids_cannot_be_started(self):
        with self.assertRaisesRegex(TaskError, 'Task not found'):
            runtime.load_task(self.sa.snapshot(), 'missing')
        self.add()
        with self.assertRaisesRegex(TaskError, 'Run not found'):
            runtime.load_run(self.sa.snapshot(), 't', 'missing')
        self.start()
        with self.assertRaisesRegex(TaskError, 'Run ID already exists'):
            runtime.start(self.sb, 'reuse-run', 't', 'other-runner', run_id='r')
        runtime.edit_task(self.sa, 'pause', 't', 1, status='paused')
        with self.assertRaisesRegex(TaskError, 'not open'):
            self.start('paused')
        with self.assertRaisesRegex(TaskError, 'indexed repository'):
            runtime.add_task(self.sa, 'custom-path', task_id='custom', title='Custom', path='custom/path')

    def test_workspace_rejects_invalid_modes_symlinks_and_preexisting_directories(self):
        self.add(); self.start()
        existing = self.base / 'existing'
        existing.mkdir()
        (existing / 'unowned').write_text('keep')
        link = self.base / 'linked'
        link.symlink_to(existing, target_is_directory=True)
        for path, mode, message in [(existing, 'invalid', 'Unknown workspace mode'),
                                    (link, 'worktree', 'symlink'), (existing, 'worktree', 'already exists')]:
            with self.subTest(path=path, mode=mode), self.assertRaisesRegex(TaskError, message):
                runtime.prepare(self.sa, 't', 'r', path, mode)
        self.assertEqual((existing / 'unowned').read_text(), 'keep')
        self.assertFalse(runtime.local_state(self.sa, 't', 'r').exists())

    def test_prepare_resumes_an_interrupted_worktree_binding_and_detects_missing_workspace(self):
        self.add(); run = self.start()
        self.git(self.a, 'update-ref', run['work_ref'], run['definition_commit'])
        work = self.base / 'interrupted-work'
        with self.assertRaisesRegex(TaskError, 'Existing work branch'):
            runtime.prepare(self.sa, 't', 'r', work)
        state = runtime.prepare(self.sa, 't', 'r', work)
        self.assertTrue(state['prepared'])
        with self.assertRaisesRegex(TaskError, 'another workspace'):
            runtime.prepare(self.sa, 't', 'r', self.base / 'other')
        self.git(self.a, 'worktree', 'remove', str(work))
        with self.assertRaisesRegex(TaskError, 'missing or changed'):
            runtime.prepare(self.sa, 't', 'r', work)

    def test_restore_requires_confirmed_stop_and_compatible_published_history(self):
        self.add(); run = self.start()
        work = self.base / 'restore'
        with self.assertRaisesRegex(TaskError, 'requires confirmation'):
            runtime.prepare(self.sb, 't', 'r', work, restore=True)
        with self.assertRaisesRegex(TaskError, 'No compatible remote work checkpoint'):
            runtime.prepare(self.sb, 't', 'r', work, restore=True, stopped=True, settled=True)
        candidate = self.sa.commit(run['source_head'], {'outside': b'forbidden'}, 'unsafe checkpoint')
        self.sa.push(run['work_ref'], candidate, None)
        with self.assertRaisesRegex(TaskError, 'use restore'):
            runtime.prepare(self.sb, 't', 'r', work)
        with self.assertRaisesRegex(TaskError, 'crosses the run boundary'):
            runtime.prepare(self.sb, 't', 'r', work, restore=True, stopped=True, settled=True)
        self.assertFalse(work.exists())

    def test_heartbeat_detects_foreign_identity_and_reconciles_a_lost_push_reply(self):
        self.add(); run = self.start()
        original = task_io.IOStore.push
        def lost_reply(*args, **kwargs):
            original(*args, **kwargs)
            raise GitFailure('fixture lost reply', {'definite_rejection': False})
        with patch.object(task_io.IOStore, 'push', autospec=True, side_effect=lost_reply):
            first = runtime.heartbeat(self.sa, 't', 'r', progress='saved')
        self.assertEqual(first['sequence'], 1)
        self.assertEqual(runtime.heartbeat(self.sb, 't', 'r')['progress'], 'saved')
        with patch.object(task_io.IOStore, 'push', side_effect=GitFailure('fixture unavailable', {'definite_rejection': False})):
            with self.assertRaisesRegex(TaskError, 'unknown; retry operation retry-beat'):
                runtime.heartbeat(self.sa, 't', 'r', operation_id='retry-beat')
        self.assertEqual(runtime.heartbeat(self.sa, 't', 'r', operation_id='retry-beat')['sequence'], 3)
        tip = self.sa.fetch(run['heartbeat_ref'])
        corrupt = self.sa.commit(tip, {'state.json': encode(dict(first, generation=999))}, 'wrong generation')
        self.sa.push(run['heartbeat_ref'], corrupt, tip)
        with self.assertRaisesRegex(TaskError, 'does not match'):
            runtime.heartbeat(self.sa, 't', 'r')
        with self.assertRaisesRegex(TaskError, 'does not match'):
            runtime.observe(self.sb, 't', 'r')

    def test_publication_outcomes_and_unprepared_state_are_rejected(self):
        self.add(); self.start()
        for options, message in [(dict(outcome='unknown'), 'Invalid run outcome'),
                                  (dict(outcome='succeeded', discard_output=True), 'Only failed or cancelled')]:
            with self.subTest(options=options), self.assertRaisesRegex(TaskError, message):
                runtime.publish(self.sa, 'invalid', 't', 'r', **options)
        work = self.prepare()
        path = runtime.local_state(self.sa, 't', 'r')
        state = json.loads(path.read_text())
        atomic_json(path, dict(state, prepared=False))
        with self.assertRaisesRegex(TaskError, 'not prepared'):
            runtime.publish(self.sa, 'unprepared', 't', 'r')
        self.assertTrue(work.exists())
        self.assertEqual(runtime.check(self.sb, 't', 'r')['status'], 'running')


class StopEvidenceTest(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.add()
        self.run_record = self.start()
        self.adapter = self.base / 'stop-adapter'
        self.adapter.write_text('Adapter execution is replaced by a process response fixture.\n')

    def evidence(self):
        value = {key: self.run_record[key] for key in
                 ('repository_id', 'authority_ref', 'task_id', 'run_id', 'generation')}
        value.update(writer_stopped=True, children_stopped=True,
                     external_operations_settled=True, pending_git_operations_settled=True,
                     evidence='fixture:verified-stop')
        return value

    def assert_rejected(self, stdout, error, *, returncode=0):
        before = self.sa.fetch()
        original = lifecycle.git_support.run_process
        response = subprocess.CompletedProcess([str(self.adapter)], returncode, stdout, b'PRIVATE_TOKEN')
        adapter_call = Mock(return_value=response)

        def respond(command, **kwargs):
            if command == [str(self.adapter)]:
                return adapter_call(command, **kwargs)
            return original(command, **kwargs)

        with patch.object(lifecycle.git_support, 'run_process', side_effect=respond):
            with self.assertRaisesRegex(TaskError, error) as raised:
                lifecycle.reconcile_stop(self.sb, 'recover', 't', 'r', str(self.adapter))
        adapter_call.assert_called_once()
        supplied_run = json.loads(adapter_call.call_args.kwargs['input'])
        for key in ('repository_id', 'authority_ref', 'task_id', 'run_id', 'generation'):
            self.assertEqual(supplied_run[key], self.run_record[key])
        self.assertNotIn('PRIVATE_TOKEN', str(raised.exception))
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(runtime.check(self.sa, 't', 'r')['status'], 'running')
        self.assertEqual(runtime.load_task(self.sb.snapshot(), 't')[1]['slots']['1']['run_id'], 'r')

    def test_stop_adapter_requires_an_existing_absolute_path(self):
        before = self.sa.fetch()
        for program in ('relative-adapter', str(self.base / 'missing'), str(self.base)):
            with self.subTest(program=program), patch.object(lifecycle.git_support, 'run_process') as process:
                with self.assertRaisesRegex(TaskError, 'absolute path to an installed executable'):
                    lifecycle.reconcile_stop(self.sb, 'recover', 't', 'r', program)
                process.assert_not_called()
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(runtime.check(self.sa, 't', 'r')['status'], 'running')

    def test_failed_adapter_cannot_release_ownership_even_with_valid_evidence(self):
        self.assert_rejected(encode(self.evidence()), 'failed; ownership retained', returncode=1)

    def test_malformed_non_object_and_oversized_evidence_cannot_release_ownership(self):
        responses = [b'PRIVATE_TOKEN', b'\xff', b'null', b'[]', b'"PRIVATE_TOKEN"',
                     encode(dict(self.evidence(), extra='x' * (1024 * 1024)))]
        for number, response in enumerate(responses):
            with self.subTest(case=number):
                self.assert_rejected(response, 'returned invalid evidence')

    def test_stop_evidence_must_match_every_execution_identity_field(self):
        for key in ('repository_id', 'authority_ref', 'task_id', 'run_id', 'generation'):
            for missing in (False, True):
                value = self.evidence()
                if missing:
                    del value[key]
                else:
                    value[key] = 999 if key == 'generation' else 'other'
                with self.subTest(field=key, missing=missing):
                    self.assert_rejected(encode(value), 'belongs to another execution')

    def test_every_stop_condition_requires_a_literal_true_confirmation(self):
        for key in ('writer_stopped', 'children_stopped', 'external_operations_settled',
                    'pending_git_operations_settled'):
            for confirmation in (False, 1, None):
                value = self.evidence()
                if confirmation is None:
                    del value[key]
                else:
                    value[key] = confirmation
                with self.subTest(field=key, confirmation=confirmation):
                    self.assert_rejected(encode(value), 'Executor evidence is missing: ' + key)

    def test_stop_evidence_requires_a_nonempty_durable_reference(self):
        for reference in (None, '', True, 1, [], {}):
            value = self.evidence()
            value['evidence'] = reference
            with self.subTest(reference=reference):
                self.assert_rejected(encode(value), 'durable evidence reference')

    def test_stop_recovery_cannot_reuse_another_operations_identity(self):
        before = self.sa.fetch()
        with patch.object(lifecycle.git_support, 'run_process', wraps=lifecycle.git_support.run_process) as process:
            with self.assertRaisesRegex(TaskError, 'already used for another recovery'):
                lifecycle.reconcile_stop(self.sb, 'start-r', 't', 'r', str(self.adapter))
        self.assertFalse(any(call.args[0] == [str(self.adapter)] for call in process.call_args_list))
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(runtime.check(self.sa, 't', 'r')['status'], 'running')


if __name__ == "__main__":
    unittest.main()
