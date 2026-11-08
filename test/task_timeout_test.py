"""Generation recovery with real Git CAS that has no receiver time predicate."""
import json
import shutil
import sys
import tarfile
import unittest
from unittest.mock import patch

import task_lease_test as shared
from task_runtime import leases, lifecycle, runtime
from task_runtime.store import CONTROL, Snapshot, TaskError


class TimeoutTests(shared.LeaseCase):
    adapter = shared.base.ROOT / "test/fixtures/task_timeout_adapter.py"
    mode = leases.GENERATION

    # Shared invariants run against the weaker CAS-only receiver as well.
    test_terminal_cleanup = shared.LeaseTests.test_terminal_cleanup_after_expiry_preserves_replacement_without_clock
    test_old_receipts = shared.LeaseTests.test_old_receipts_are_history_not_new_lease_authority
    test_publication_loses_to_replacement = shared.LeaseTests.test_old_publication_paused_after_validation_cannot_replace_new_owner
    test_late_immutable_output = shared.LeaseTests.test_late_immutable_output_cannot_replace_accepted_pointer
    test_clock_failure_and_regression = shared.LeaseTests.test_clock_failures_do_not_grant_or_revoke_ownership
    test_clock_round_trip = shared.LeaseTests.test_clock_response_delay_is_bounded_before_admission
    test_stale_clock_reply = shared.LeaseTests.test_stale_clock_reply_cannot_be_reused_for_another_sample
    test_concurrent_reapers_and_starters = shared.LeaseTests.test_two_reapers_and_two_starters_preserve_single_generation
    test_cancelled_recovery_and_lost_reply = shared.LeaseTests.test_lost_revoke_reply_and_cancelled_recovery_preserve_original_identity
    test_missing_checkpoint = shared.LeaseTests.test_checkpoint_requires_committed_artifacts_and_matching_provenance
    test_changed_checkpoint_provenance = shared.LeaseTests.test_definition_change_prevents_automatic_checkpoint_adoption

    def attempt_ref(self, operation, run="r"):
        record = runtime.load_run(self.c.sa.snapshot(), "t", run)[2]
        return record["work_ref"].rsplit("/", 1)[0] + "/renewals/" + operation

    def test_cleanup_preserves_a_replacement_pending_renewal(self):
        self.enable(); self.c.start("old")
        self.c.prepare("old")
        runtime.publish(self.c.sa, "finish", "t", "old", outcome="succeeded")
        self.settings(now_ms=1300001)
        self.c.start("new", store=self.c.sb)
        self.settings(reject_before_cas=True)
        with self.assertRaises(TaskError):
            leases.renew(self.c.sb, "pending-new", "t", "new")
        ref = self.attempt_ref("pending-new", "new")
        candidate = self.c.sb.fetch(ref)
        replacement = runtime.load_run(self.c.sb.snapshot(), "t", "new")[2]
        self.settings(clock_missing=True)
        lifecycle.cleanup(self.c.sa, "cleanup", "t", "old", apply=True, stopped=True)
        self.assertEqual(self.c.sb.remote_tip(ref), candidate)
        self.assertEqual(runtime.load_run(self.c.sb.snapshot(), "t", "new")[2], replacement)

    def test_strict_policy_still_refuses_cas_only_receiver(self):
        capabilities = self.c.sa.transport.lease_capabilities("t")
        self.assertFalse(capabilities["atomic_time_cas"])
        before = self.c.sa.fetch()
        with self.assertRaises(TaskError):
            leases.configure(self.c.sa, "strict", "t", 1, mode=leases.MODE, profile="test", drained=True)
        self.assertEqual(self.c.sa.fetch(), before)
        self.enable()

    def test_fresh_ticks_recover_and_deliver_committed_computation(self):
        self.enable()
        work, artifact, checkpoint = self.completed()
        shutil.rmtree(work)
        shutil.rmtree(self.c.sa.local)
        shutil.rmtree(self.c.sb.local)
        for number, clock in enumerate((1100000, 1290000)):
            self.settings(now_ms=clock)
            tick = self.fresh(number)
            result = lifecycle.reap(tick, "t", apply=True, observer_profile="stateless")
            self.assertEqual(result["runs"][0]["status"], "timeout_active")
            shutil.rmtree(tick.repo)
        self.settings(now_ms=1300001)
        tick = self.fresh(3)
        new = runtime.start(tick, "next-tick", "t", "next", run_id="new",
                            resume_from="old", checkpoint_id="review")
        self.assertEqual(new["generation"], 2)
        with self.assertRaisesRegex(TaskError, "delivery-only"):
            lifecycle.execute(tick, "t", "new", [sys.executable, "-c", "raise Exception('repeat')"])
        self.assertEqual(leases.deliver(tick, "deliver", "t", "new")["provenance"], checkpoint)
        shutil.rmtree(tick.repo)
        replay = self.fresh(4)
        self.assertEqual(leases.deliver(replay, "deliver", "t", "new")["provenance"], checkpoint)
        self.assertEqual(json.loads(replay.snapshot().read(artifact))["external_computations"], 1)
        self.assertEqual(runtime.load_run(replay.snapshot(), "t", "old")[2]["recovery_kind"],
                         "expired_generation_timeout")

    def test_healthy_renewals_survive_disposable_observers(self):
        self.enable(); self.c.start()
        for n in range(1, 5):
            self.settings(now_ms=1000000 + n * 100000)
            leases.renew(self.c.sa, "renew-" + str(n), "t", "r")
            tick = self.fresh(n)
            row = lifecycle.reap(tick, "t", apply=True)["runs"][0]
            self.assertEqual(row["status"], "timeout_active")
            shutil.rmtree(tick.repo)
        self.assertEqual(self.c.sa.list_refs([self.attempt_ref("")]), {})

    def test_delayed_renewal_wins_before_recovery_with_fixed_deadline(self):
        self.enable(); self.c.start()
        self.settings(now_ms=1280000, advance_on_cas_ms=200000)
        renewed = leases.renew(self.c.sa, "delayed", "t", "r")
        self.assertEqual(renewed["lease"]["expires_ms"], 1580000)
        self.assertEqual(json.loads(self.settings_path.read_text())["now_ms"], 1480000)
        self.assertEqual(lifecycle.reap(self.c.sb, "t", apply=True)["runs"][0]["status"], "timeout_active")
        self.settings(now_ms=1580001)
        tick = self.fresh(1)
        self.assertEqual(runtime.start(tick, "new", "t", "next")["generation"], 2)

    def test_unknown_renewal_is_identical_after_local_state_is_destroyed(self):
        self.enable(); self.c.start()
        self.settings(now_ms=1280000, reject_before_cas=True)
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "pending", "t", "r")
        ref = self.attempt_ref("pending")
        candidate = self.c.sa.fetch(ref)
        saved = Snapshot(self.c.sa, candidate).json("tasks/t/runs/r/run.json")
        shutil.rmtree(self.c.sa.local)
        self.settings(now_ms=1400000)
        tick = self.fresh(1)
        # Replaying the frozen request must not acquire a new clock sample.
        with patch.object(tick.transport, "trusted_time", side_effect=AssertionError("refreshed")):
            result = leases.renew(tick, "pending", "t", "r")
        self.assertEqual(result["lease"], saved["lease"])
        self.assertTrue(tick.contains(tick.fetch(), candidate))
        self.assertIsNone(tick.remote_tip(ref))
        self.settings(now_ms=1580001)
        replacement = self.fresh(2)
        self.assertEqual(runtime.start(replacement, "new", "t", "next")["generation"], 2)

    def test_revocation_wins_then_exact_old_renewal_cannot_publish(self):
        self.enable(); self.c.start("old")
        self.settings(now_ms=1280000)
        push = self.c.sa.push
        seen = []
        def paused(ref, candidate, expected, **kwargs):
            if ref == self.c.sa.ref and not seen:
                seen.append((candidate, expected))
                self.settings(now_ms=1300001)
                runtime.start(self.c.sb, "replacement", "t", "next", run_id="new")
            return push(ref, candidate, expected, **kwargs)
        with patch.object(self.c.sa, "push", side_effect=paused), self.assertRaises(TaskError):
            leases.renew(self.c.sa, "late", "t", "old")
        self.assertEqual(len(seen), 1, "The renewal must reach the authority CAS")
        before = self.c.sb.fetch()
        with self.assertRaises(TaskError):
            push(self.c.sa.ref, *seen[0])
        self.assertEqual(self.c.sb.fetch(), before)
        tick = self.fresh(1)
        with self.assertRaisesRegex(TaskError, "superseded"):
            leases.renew(tick, "late", "t", "old")
        self.assertEqual(runtime.check(tick, "t", "new")["generation"], 2)

    def test_changed_parent_never_rebases_same_renewal_id(self):
        self.enable(); self.c.start()
        self.settings(reject_before_cas=True)
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "frozen", "t", "r")
        runtime.edit_task(self.c.sb, "edit", "t", 2, title="Unrelated change")
        tick = self.fresh(1)
        before = tick.fetch()
        with self.assertRaisesRegex(TaskError, "superseded"):
            leases.renew(tick, "frozen", "t", "r")
        self.assertEqual(tick.fetch(), before)
        leases.renew(tick, "new-operation", "t", "r")

    def test_supervisor_heartbeat_retries_a_definitely_superseded_attempt(self):
        self.enable(); self.c.start()
        self.settings(now_ms=1100000)
        push = self.c.sa.push
        changed = False
        def race(ref, candidate, expected, **kwargs):
            nonlocal changed
            if ref == self.c.sa.ref and not changed:
                changed = True
                runtime.edit_task(self.c.sb, "edit", "t", 2, title="Concurrent task edit")
            return push(ref, candidate, expected, **kwargs)
        with patch.object(self.c.sa, "push", side_effect=race):
            runtime.heartbeat(self.c.sa, "t", "r")
        self.assertTrue(changed)
        self.assertEqual(runtime.check(self.c.sa, "t", "r")["lease"]["expires_ms"], 1400000)

    def test_lost_renewal_reply_uses_receipt_without_extending_again(self):
        self.enable(); self.c.start()
        self.settings(now_ms=1100000, lose_reply=True)
        first = leases.renew(self.c.sa, "renew", "t", "r")
        self.settings(now_ms=1400001)
        tick = self.fresh(1)
        before = tick.fetch()
        self.assertEqual(leases.renew(tick, "renew", "t", "r")["lease"], first["lease"])
        self.assertEqual(tick.fetch(), before)
        with self.assertRaises(TaskError):
            runtime.check(tick, "t", "r")

    def test_configuration_requires_coordinated_writers_and_draining(self):
        before = self.c.sa.fetch()
        for bad in ({"no_generation": True}, {"uncoordinated": True}, {"unfenced": True}):
            self.settings(**dict(dict(no_generation=False, uncoordinated=False, unfenced=False), **bad))
            with self.assertRaises(TaskError):
                self.enable()
        self.assertEqual(self.c.sa.fetch(), before)
        self.settings(no_generation=False, uncoordinated=False, unfenced=False)
        self.c.start()
        with self.assertRaisesRegex(TaskError, "Drain all"):
            self.enable()

    def test_schema_barrier_and_manual_recovery_survive_rollback(self):
        self.enable()
        self.assertEqual(self.c.sa.snapshot().json(CONTROL + "/config.json")["schema_version"], 4)
        leases.configure(self.c.sa, "manual-policy", "t", 2, mode="manual", drained=True)
        self.assertEqual(self.c.sa.snapshot().json(CONTROL + "/config.json")["schema_version"], 4)
        self.c.start()
        self.assertEqual(self.c.sa.snapshot().json(CONTROL + '/config.json')['schema_version'], 9)
        self.assertEqual(lifecycle.reap(self.c.sb, "t", apply=True)["runs"][0]["status"], "stop_evidence_required")

    def test_clock_age_and_uncertainty_block_new_authority(self):
        self.enable()
        self.settings(sample_age_ms=10001)
        with self.assertRaisesRegex(TaskError, "age"):
            self.c.start()
        self.settings(sample_age_ms=0)
        self.c.start()
        self.settings(now_ms=1299950, uncertainty_ms=100)
        self.assertEqual(lifecycle.reap(self.c.sb, "t", apply=True)["runs"][0]["status"], "timeout_active")
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "late", "t", "r")

    def test_zipapp_and_source_export_include_timeout_policy(self):
        bundle = self.c.base / "taskctl.pyz"
        source = self.c.base / "source.tar.gz"
        builder = str(shared.base.ROOT / "bin/build-task-inbox")
        self.c.command(sys.executable, builder, "--output", str(bundle))
        self.c.command(sys.executable, builder, "--format", "source", "--output", str(source))
        with tarfile.open(source) as archive:
            names = archive.getnames()
            self.assertIn("task-runtime/test/task_timeout_test.py", names)
            self.assertIn("task-runtime/test/fixtures/task_timeout_adapter.py", names)
        common = [sys.executable, str(bundle), "--repo", str(self.c.a),
                  "--transport-adapter", str(self.adapter)]
        self.c.command(*common, "--operation-id", "enable", "task", "recovery-policy", "--id", "t",
                       "--expected-revision", "1", "--mode", self.mode, "--profile", "test", "--confirm-writers-drained")
        self.c.command(*common, "--operation-id", "start", "run", "start", "--task", "t", "--runner", "runner")
        self.assertEqual(runtime.check(self.c.sa, "t", "start")["lease"]["expires_ms"], 1300000)


if __name__ == "__main__":
    unittest.main()
