"""Real Git conformance with a disposable clone on every scheduled invocation."""
import concurrent.futures
import json
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import Mock, patch

import task_runtime_test as base
from task_runtime import cli, leases, lifecycle, maintenance, runtime
from task_runtime.store import CONTROL, Snapshot, Store, TaskError, digest
from task_runtime.transport import TransportError

ADAPTER = base.ROOT / "test/fixtures/task_lease_adapter.py"


class LeaseCase(unittest.TestCase):
    adapter = ADAPTER
    mode = leases.MODE
    def setUp(self):
        self.c = base.TaskRuntimeTest()
        self.c.setUp()
        self.addCleanup(self.c.doCleanups)
        self.settings_path = self.c.remote / "lease-test.json"
        self.settings_path.write_text(json.dumps({"key": "test-only-clock-proof", "now_ms": 1000000}))
        self.c.sa = Store(self.c.a, transport_adapter=str(self.adapter))
        self.c.sb = Store(self.c.b, transport_adapter=str(self.adapter))
        self.c.add()

    def settings(self, **changes):
        current = json.loads(self.settings_path.read_text())
        current.update(changes)
        self.settings_path.write_text(json.dumps(current))

    def enable(self, **settings):
        self.settings(**settings)
        return leases.configure(self.c.sa, "enable", "t", 1, mode=self.mode, profile="test", drained=True)

    def fresh(self, number):
        path = self.c.base / ("tick-" + str(number))
        self.c.command("git", "clone", "--branch", "main", str(self.c.remote), str(path))
        self.c.git(path, "config", "user.name", "Fixture")
        self.c.git(path, "config", "user.email", "fixture@example.invalid")
        return Store(path, transport_adapter=str(self.adapter))

    def completed(self):
        self.c.start("old")
        work = self.c.prepare("old")
        artifact = "tasks/t/runs/old/artifacts/review.json"
        self.c.output(work, artifact, b'{"external_computations": 1, "result": "complete"}\n')
        runtime.publish(self.c.sa, "save", "t", "old")
        result = leases.checkpoint(self.c.sa, "checkpoint", "t", "old", "review", "external-review-1",
                                   digest({"input": "pinned"}), [artifact])
        return work, artifact, {k: v for k, v in result.items() if k not in ("operation_id", "committed")}


class LeaseValidationTest(unittest.TestCase):
    def setUp(self):
        self.policy = dict(version=1, profile='test', clock_id='clock', ttl_ms=300000,
                           max_uncertainty_ms=100, max_sample_age_ms=10000,
                           max_request_delay_ms=10000, effects='authority_only')
        self.store = Mock()
        self.store.transport.lease_capabilities.return_value = dict(
            version=1, atomic_time_cas=True, enforced_writers=True, destinations_fenced=True,
            generation_timeout=True, coordinated_writers=True, profiles={'test': self.policy})
        self.snapshot = Mock(commit='parent')
        self.snapshot.json.return_value = {}
        self.sample = dict(clock_id='clock', nonce='nonce', expected='parent', proof='signed',
                           lower_ms=1000000, upper_ms=1000100, sample_age_ms=0)

    def test_policy_schema_time_bounds_and_effects_are_strict(self):
        invalid = [None, [], {}, dict(self.policy, extra=True), dict(self.policy, version=True),
                   dict(self.policy, version=2), dict(self.policy, effects='mutable'),
                   dict(self.policy, ttl_ms=40000), dict(self.policy, profile='Invalid')]
        for name in ('ttl_ms', 'max_uncertainty_ms', 'max_sample_age_ms', 'max_request_delay_ms'):
            for value in (True, 0, -1, 86400001, 1.5):
                invalid.append(dict(self.policy, **{name: value}))
        for number, value in enumerate(invalid):
            with self.subTest(case=number), self.assertRaises(TaskError):
                leases.validate_policy(value)
        self.assertEqual(leases.validate_policy(dict(self.policy, effects='immutable_outputs'))['effects'], 'immutable_outputs')

    def test_installed_contract_requires_capabilities_profiles_and_pinned_policy(self):
        with self.assertRaisesRegex(TaskError, 'does not provide'):
            leases.policy_for(Mock(transport=object()), 't', 'test')
        contract = self.store.transport.lease_capabilities.return_value
        for value in [None, dict(contract, profiles=[]), dict(contract, destinations_fenced=1),
                      dict(contract, profiles={'test': dict(self.policy, profile='other')})]:
            self.store.transport.lease_capabilities.return_value = value
            with self.subTest(contract=value), self.assertRaises(TaskError):
                leases.policy_for(self.store, 't', 'test')
        self.store.transport.lease_capabilities.return_value = contract
        with self.assertRaisesRegex(TaskError, 'pinned policy'):
            leases.policy_for(self.store, 't', 'test', dict(self.policy, ttl_ms=400000))

    def trusted_sample(self, value, *, mode=leases.MODE, times=(1, 1)):
        self.store.transport.trusted_time.return_value = value
        with patch('task_runtime.leases.uuid.uuid4', return_value=Mock(hex='nonce')), \
                patch('task_runtime.leases.monotonic', side_effect=times):
            return leases.sample(self.store, self.snapshot, 't', self.policy, mode)

    def test_trusted_time_is_bound_to_clock_nonce_parent_and_strict_lease_proof(self):
        for value in [None, dict(self.sample, clock_id='other'), dict(self.sample, nonce='other'),
                      dict(self.sample, expected='other'), dict(self.sample, proof=''), dict(self.sample, proof=1)]:
            with self.subTest(value=value), self.assertRaisesRegex(TaskError, 'identity'):
                self.trusted_sample(value)
        self.assertEqual(self.trusted_sample(dict(self.sample, proof=None), mode=leases.GENERATION)['lower_ms'], 1000000)

    def test_trusted_time_rejects_invalid_intervals_delays_ages_and_clock_regression(self):
        for value in [dict(self.sample, lower_ms=True), dict(self.sample, upper_ms=1.5),
                      dict(self.sample, lower_ms=-1), dict(self.sample, upper_ms=999999),
                      dict(self.sample, upper_ms=1000101)]:
            with self.subTest(value=value), self.assertRaisesRegex(TaskError, 'uncertainty bounds'):
                self.trusted_sample(value)
        for times in ((2, 1), (1, 12)):
            with self.subTest(times=times), self.assertRaisesRegex(TaskError, 'uncertainty bounds'):
                self.trusted_sample(self.sample, times=times)
        for age in (None, True, -1, 10001):
            with self.subTest(age=age), self.assertRaisesRegex(TaskError, 'age'):
                self.trusted_sample(dict(self.sample, sample_age_ms=age), mode=leases.GENERATION)
        self.assertEqual(self.trusted_sample(dict(self.sample, sample_age_ms=10000), mode=leases.GENERATION)['sample_age_ms'], 10000)
        self.snapshot.json.return_value = dict(lower_ms=1000001, upper_ms=1000100)
        with self.assertRaisesRegex(TaskError, 'regressed'):
            self.trusted_sample(self.sample)

    def test_configuration_and_checkpoint_provenance_fail_before_transaction(self):
        for options in [dict(mode='other', drained=True), dict(mode='manual'),
                        dict(mode='manual', profile='test', drained=True),
                        dict(mode=leases.MODE, drained=True)]:
            with self.subTest(options=options), self.assertRaises(TaskError):
                leases.configure(self.store, 'policy', 't', 1, **options)
        for provenance, artifacts in [(None, ['artifact']), ('a' * 63, ['artifact']),
                                       ('g' * 64, ['artifact']), ('a' * 64, [])]:
            with self.subTest(provenance=provenance), self.assertRaises(TaskError):
                leases.checkpoint(self.store, 'checkpoint', 't', 'r', 'saved', 'computation', provenance, artifacts)
        self.store.transact.assert_not_called()

    def test_mutation_guard_requires_writer_protocol_and_exact_durable_lease_identity(self):
        execution = dict(recovery_mode=leases.MODE, lease_policy=self.policy)
        task = dict(execution=execution)
        lease = dict(holder='r', generation=1, policy_hash=digest(self.policy), expires_ms=1300000)
        run = dict(execution=execution, generation=1, slot_id=1, status='running', lease=lease)
        request = dict(kind='run-check', task_id='t', run_id='r')
        with patch.object(runtime, 'load_task', return_value=(task, {'slots': {}})), \
                patch.object(runtime, 'load_run', return_value=(task, {}, run)), \
                patch.object(leases, 'sample', return_value=self.sample):
            self.snapshot.json.return_value = {}
            with self.assertRaisesRegex(TaskError, 'Lease writer protocol'):
                leases.guard(self.store, self.snapshot, request)
            self.snapshot.json.return_value = {'lease_protocol': 1}
            for key, value in [('holder', 'other'), ('generation', 999), ('policy_hash', 'wrong'), ('expires_ms', True)]:
                run['lease'] = dict(lease, **{key: value})
                with self.subTest(field=key), self.assertRaisesRegex(TaskError, 'durable lease identity'):
                    leases.guard(self.store, self.snapshot, request)
            run['lease'] = lease
            run['execution'] = dict(execution, recovery_mode=leases.GENERATION)
            with self.assertRaisesRegex(TaskError, 'Generation timeout writer protocol'):
                leases.guard(self.store, self.snapshot, request)

    def test_terminal_cleanup_guard_checks_original_generation_without_sampling_time(self):
        task = dict(execution=dict(recovery_mode=leases.MODE, lease_policy=self.policy))
        run = dict(execution=task['execution'], generation=1, slot_id=1, status='succeeded', lease={'expires_ms': 1})
        registry = {'slots': {'1': {'run_id': 'replacement'}}}
        request = dict(kind='run-cleanup', task_id='t', run_id='r', generation=1)
        self.snapshot.json.return_value = {'lease_protocol': 1}
        with patch.object(runtime, 'load_task', return_value=(task, registry)), \
                patch.object(runtime, 'load_run', return_value=(task, registry, run)), patch.object(leases, 'sample') as sample:
            guard = leases.guard(self.store, self.snapshot, request)
            self.assertEqual(guard['action'], 'cleanup')
            self.assertIsNone(guard['sample'])
            sample.assert_not_called()
            with self.assertRaisesRegex(TaskError, 'original terminal run generation'):
                leases.guard(self.store, self.snapshot, dict(request, generation=2))
            registry['slots']['1']['run_id'] = 'r'
            with self.assertRaisesRegex(TaskError, 'original terminal run generation'):
                leases.guard(self.store, self.snapshot, request)


class LeaseTests(LeaseCase):
    def test_metadata_retirement_accepts_migration_to_a_noncolliding_io_prefix(self):
        store = Store(self.c.a, branch='task-io', transport_adapter=str(self.adapter))
        runtime.initialize(store, 'other-authority')
        runtime.add_task(store, 'other-task', task_id='t', title='Task')
        leases.configure(store, 'other-policy', 't', 1, mode=self.mode, profile='test', drained=True)
        runtime.start(store, 'other-start', 't', 'runner', run_id='r')
        runtime.prepare(store, 't', 'r', self.c.base / 'other-work')
        with patch.object(maintenance, 'maintain'):
            runtime.publish(store, 'other-finish', 't', 'r', outcome='succeeded')
        snapshot = store.snapshot()
        config = snapshot.json(CONTROL + '/config.json')
        config.update(schema_version=7); config.pop('io_ref_prefix')
        candidate = store.commit(snapshot.commit, {CONTROL + '/config.json': base.encode(config)}, 'Previous schema authority')
        store.push(store.ref, candidate, snapshot.commit)
        result = maintenance.retire_metadata(store, 't', 'r', apply=True)
        self.assertEqual(result['status'], 'retired')
        config = store.snapshot().json(CONTROL + '/config.json')
        self.assertEqual((config['schema_version'], config['io_ref_prefix']), (9, 'task-io-2'))
        self.assertEqual(runtime.load_run(store.snapshot(), 't', 'r')[2]['status'], 'succeeded')

    def test_checkpoint_identity_and_saved_input_artifacts_are_immutable(self):
        self.enable()
        _, artifact, record = self.completed()
        with self.assertRaisesRegex(TaskError, 'Checkpoint IDs are immutable'):
            leases.checkpoint(self.c.sa, 'other-checkpoint-op', 't', 'old', 'review', 'external-review-1',
                              digest({'input': 'pinned'}), [artifact])
        self.settings(now_ms=1400000)
        leases.revoke(self.c.sa, 'revoke', 't', 'old')
        snapshot = self.c.sa.snapshot()
        path = 'tasks/t/runs/old/checkpoints/review.json'
        for changed, message in [(dict(record, input_digest='invalid'), 'input provenance is incomplete'),
                                  (dict(record, artifacts={}), 'artifacts changed or disappeared')]:
            candidate = self.c.sa.commit(snapshot.commit, {path: base.encode(changed)}, 'corrupt checkpoint')
            with self.subTest(message=message), self.assertRaisesRegex(TaskError, message):
                leases.continuation(Snapshot(self.c.sa, candidate), 't', 'old', 'review')
        self.assertEqual(leases.continuation(snapshot, 't', 'old', 'review')['provenance']['input_digest'], record['input_digest'])

    def test_completed_checkpoint_survives_drained_directory_move(self):
        self.enable()
        _, artifact, checkpoint = self.completed()
        self.settings(now_ms=1400000)
        leases.revoke(self.c.sa, 'revoke', 't', 'old')
        original_path = 'tasks/t/runs/old/checkpoints/review.json'
        original = self.c.sa.snapshot().read(original_path)
        def move(snapshot):
            self.assertFalse(any(s.get('run_id') for s in snapshot.json('tasks/t/runtime.json')['slots'].values()))
            changes = {}
            for path in snapshot.entries:
                if path.startswith('tasks/t/'):
                    changes['tasks/t-readable/' + path[len('tasks/t/'):]] = snapshot.read(path)
                    changes[path] = None
            return changes, {}
        snapshot = self.c.sa.snapshot()
        changes, _ = move(snapshot)
        moved = self.c.sa.commit(snapshot.commit, changes, 'Move drained fixture task')
        self.c.git(self.c.a, 'push', 'origin', moved + ':refs/heads/main')
        snapshot = self.c.sa.snapshot()
        moved_artifact = artifact.replace('tasks/t/', 'tasks/t-readable/', 1)
        self.assertEqual(snapshot.read('tasks/t-readable/runs/old/checkpoints/review.json'), original)
        self.assertEqual(leases.continuation(snapshot, 't', 'old', 'review')['provenance'], checkpoint)
        corrupt = self.c.sa.commit(snapshot.commit, {moved_artifact: b'changed'}, 'Corrupt moved artifact')
        with self.assertRaisesRegex(TaskError, 'artifacts changed or disappeared'):
            leases.continuation(Snapshot(self.c.sa, corrupt), 't', 'old', 'review')
        runtime.start(self.c.sb, 'replacement', 't', 'new-runner', run_id='new',
                      resume_from='old', checkpoint_id='review')
        delivered = leases.deliver(self.c.sb, 'deliver', 't', 'new')
        self.assertEqual(delivered['provenance'], checkpoint)
        self.assertEqual(self.c.sb.snapshot().json('tasks/t-readable/data/accepted-checkpoints/review.json')['provenance'], checkpoint)

    def test_heartbeat_configuration_reserves_renewal_margin(self):
        self.c.add(task="tight", heartbeat_seconds=295, stale_seconds=600)
        with self.assertRaisesRegex(TaskError, "renewal safety margin"):
            leases.configure(self.c.sa, "tight-policy", "tight", 1, mode=leases.MODE,
                             profile="test", drained=True)
        self.c.add(task="safe", heartbeat_seconds=260, stale_seconds=600)
        leases.configure(self.c.sa, "safe-policy", "safe", 1, mode=leases.MODE,
                         profile="test", drained=True)
        self.c.start("safe-run", task="safe")
        self.settings(now_ms=1260000)
        runtime.heartbeat(self.c.sa, "safe", "safe-run")
        renewed = runtime.load_run(self.c.sa.snapshot(), "safe", "safe-run")[2]
        self.assertEqual(renewed["lease"]["expires_ms"], 1560000)

    def test_terminal_cleanup_after_expiry_preserves_replacement_without_clock(self):
        self.enable()
        self.c.start("old")
        work = self.c.prepare("old")
        self.c.output(work, "tasks/t/runs/old/artifacts/out")
        runtime.heartbeat(self.c.sa, "t", "old")
        finished = runtime.publish(self.c.sa, "finish", "t", "old", outcome="succeeded")
        self.settings(now_ms=1300001)
        self.c.start("new", store=self.c.sb)
        replacement = runtime.load_run(self.c.sb.snapshot(), "t", "new")[2]
        self.settings(clock_missing=True)
        result = lifecycle.cleanup(self.c.sa, "cleanup", "t", "old", apply=True, stopped=True)
        self.assertEqual(result["status"], "done")
        self.assertFalse(work.exists())
        self.assertIsNone(self.c.sa.remote_tip(finished["work_ref"]))
        self.assertEqual(runtime.load_run(self.c.sb.snapshot(), "t", "new")[2], replacement)

    def test_fresh_clone_recovery_and_delivery_without_old_workspace(self):
        self.enable()
        work, artifact, checkpoint = self.completed()
        # Destroy the computation checkout and both observer's local journals.
        shutil.rmtree(work)
        shutil.rmtree(self.c.sa.local)
        shutil.rmtree(self.c.sb.local)
        for number, clock in enumerate((1050000, 1250000)):
            self.settings(now_ms=clock)
            observer = self.fresh(number)
            result = lifecycle.reap(observer, "t", apply=True, observer_profile="stateless")
            self.assertEqual(result["runs"][0]["status"], "lease_active")
            shutil.rmtree(observer.repo)
        self.settings(now_ms=1300000)
        observer = self.fresh(3)
        result = lifecycle.reap(observer, "t", apply=True, observer_profile="stateless")
        self.assertEqual(result["runs"][0]["status"], "lost")
        self.assertFalse(result["runs"][0]["physical_stop_confirmed"])
        replacement = runtime.start(observer, "replacement", "t", "new-runner", run_id="new",
                                    resume_from="old", checkpoint_id="review")
        self.assertEqual(replacement["generation"], 2)
        with self.assertRaisesRegex(TaskError, "delivery-only"):
            lifecycle.execute(observer, "t", "new", [sys.executable, "-c", "raise Exception('must not rerun')"])
        delivered = leases.deliver(observer, "deliver", "t", "new")
        self.assertEqual(delivered["provenance"], checkpoint)
        self.assertEqual(observer.snapshot().read(artifact), b'{"external_computations": 1, "result": "complete"}\n')
        self.assertEqual(observer.snapshot().json("tasks/t/data/accepted-checkpoints/review.json"),
                         {k: v for k, v in delivered.items() if k not in ("operation_id", "committed")})
        shutil.rmtree(observer.repo)
        replay = self.fresh(4)
        self.assertEqual(leases.deliver(replay, "deliver", "t", "new")["provenance"], checkpoint)
        self.assertFalse(runtime.load_task(replay.snapshot(), "t")[1]["slots"]["1"]["run_id"])

    def test_renewed_long_run_survives_many_disposable_observers(self):
        self.enable(); self.c.start()
        for number in range(1, 5):
            self.settings(now_ms=1000000 + number * 100000)
            leases.renew(self.c.sa, "renew-" + str(number), "t", "r")
            observer = self.fresh(number)
            self.assertEqual(lifecycle.reap(observer, "t", apply=True, observer_profile="stateless")["runs"][0]["status"], "lease_active")
            shutil.rmtree(observer.repo)
        self.assertGreater(runtime.check(self.c.sa, "t", "r")["lease"]["expires_ms"], 1600000)

    def test_normal_start_recovers_expired_run_without_observer_directory(self):
        self.enable(); self.c.start("old")
        self.settings(now_ms=1300001)
        observer = self.fresh(1)
        new = runtime.start(observer, "normal-schedule", "t", "next")
        self.assertEqual(new["generation"], 2)
        self.assertEqual(runtime.load_run(observer.snapshot(), "t", "old")[2]["recovery_kind"], "expired_fenced_lease")

    def test_clock_response_delay_is_bounded_before_admission(self):
        self.enable()
        with patch.object(leases, "monotonic", side_effect=[0, 11]), self.assertRaisesRegex(TaskError, "stale"):
            self.c.start()
        self.assertIsNone(self.c.sa.snapshot().read("tasks/t/runs/r/run.json"))

    def test_old_receipts_are_history_not_new_lease_authority(self):
        self.enable(); first = self.c.start()
        leases.renew(self.c.sa, "renew", "t", "r")
        self.settings(now_ms=1300001)
        before = self.c.sa.fetch()
        leases.renew(self.c.sb, "renew", "t", "r")
        replay = self.c.start(store=self.c.sb)
        self.assertEqual(first["lease"], replay["lease"])
        self.assertEqual(before, self.c.sa.fetch())
        with self.assertRaises(TaskError):
            runtime.check(self.c.sb, "t", "r")
        with self.assertRaises(TaskError):
            leases.renew(self.c.sb, "new-renewal", "t", "r")

    def test_late_renewal_is_rejected_at_server_even_without_reaper(self):
        self.enable(); self.c.start()
        before = self.c.sa.fetch()
        self.settings(advance_on_cas_ms=300001)
        with self.assertRaisesRegex(TransportError, "lease_guard_rejected"):
            leases.renew(self.c.sa, "delayed", "t", "r")
        self.assertEqual(before, self.c.sa.fetch())
        self.assertEqual(runtime.load_run(self.c.sa.snapshot(), "t", "r")[2]["lease"]["expires_ms"], 1300000)

    def test_old_publication_paused_after_validation_cannot_replace_new_owner(self):
        self.enable(); self.c.start("old")
        work = self.c.prepare("old")
        self.c.output(work, "tasks/t/data/report", b"old shared report")
        original = self.c.sa.push
        fired = False
        def race(ref, candidate, expected, **kwargs):
            nonlocal fired
            if ref == self.c.sa.ref and not fired:
                fired = True
                self.settings(now_ms=1300001)
                lifecycle.reap(self.c.sb, "t", apply=True, observer_profile="stateless")
                self.c.start("new", store=self.c.sb)
            return original(ref, candidate, expected, **kwargs)
        with patch.object(self.c.sa, "push", side_effect=race), self.assertRaises(TaskError):
            runtime.publish(self.c.sa, "late-report", "t", "old")
        self.assertIsNone(self.c.sb.snapshot().read("tasks/t/data/report"))
        self.assertEqual(runtime.check(self.c.sb, "t", "new")["generation"], 2)

    def test_late_immutable_output_cannot_replace_accepted_pointer(self):
        self.enable(effects="immutable_outputs")
        _, _, checkpoint = self.completed()
        self.settings(now_ms=1300001)
        lifecycle.reap(self.c.sb, "t", apply=True, observer_profile="stateless")
        runtime.start(self.c.sb, "replacement", "t", "new-runner", run_id="new", resume_from="old", checkpoint_id="review")
        leases.deliver(self.c.sb, "deliver", "t", "new")
        before = self.c.sb.snapshot().read("tasks/t/data/accepted-checkpoints/review.json")
        # Model a late immutable provider upload in the old run's isolated namespace.
        isolated = self.c.base / "provider/old/late-output"
        isolated.parent.mkdir(parents=True)
        isolated.write_text("late immutable bytes")
        with self.assertRaises(TaskError):
            runtime.publish(self.c.sa, "late", "t", "old")
        self.assertEqual(before, self.c.sb.snapshot().read("tasks/t/data/accepted-checkpoints/review.json"))
        self.assertEqual(json.loads(before)["provenance"], checkpoint)

    def test_clock_failures_do_not_grant_or_revoke_ownership(self):
        self.enable()
        for settings in ({"clock_missing": True}, {"uncertainty_ms": 200}, {"wrong_nonce": True}):
            with self.subTest(settings=settings):
                self.settings(**dict(dict(clock_missing=False, uncertainty_ms=0, wrong_nonce=False), **settings))
                before = self.c.sa.fetch()
                with self.assertRaises(TaskError):
                    self.c.start()
                self.assertEqual(before, self.c.sa.fetch())
        self.settings(clock_missing=False, uncertainty_ms=0, wrong_nonce=False)
        self.c.start()
        self.settings(now_ms=900000)
        result = lifecycle.reap(self.c.sb, "t", apply=True, observer_profile="stateless")
        self.assertTrue(result["needs_attention"])
        self.assertEqual(runtime.load_run(self.c.sa.snapshot(), "t", "r")[2]["status"], "running")

    def test_stale_clock_reply_cannot_be_reused_for_another_sample(self):
        self.enable()
        original = self.c.sa.transport.trusted_time
        saved = None
        def stale(*args):
            nonlocal saved
            if saved is None:
                saved = original(*args)
            return saved
        with patch.object(self.c.sa.transport, "trusted_time", side_effect=stale):
            self.c.start()
            with self.assertRaisesRegex(TaskError, "time identity"):
                leases.renew(self.c.sa, "renew", "t", "r")

    def test_expiry_is_conservative_with_clock_uncertainty(self):
        self.enable(); self.c.start()
        self.settings(now_ms=1299950, uncertainty_ms=100)
        result = lifecycle.reap(self.c.sb, "t", apply=True, observer_profile="stateless")
        self.assertEqual(result["runs"][0]["status"], "lease_active")
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "renew", "t", "r")
        self.settings(now_ms=1300000)
        self.assertEqual(lifecycle.reap(self.c.sb, "t", apply=True)["runs"][0]["status"], "lost")

    def test_two_reapers_and_two_starters_preserve_single_generation(self):
        self.enable(); self.c.start("old"); self.settings(now_ms=1300001)
        stores = [self.fresh(1), self.fresh(2)]
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            list(pool.map(lambda s: lifecycle.reap(s, "t", apply=True, observer_profile="stateless"), stores))
        def start(pair):
            index, store = pair
            try:
                return runtime.start(store, "replace-" + str(index), "t", "new-" + str(index))
            except TaskError:
                return None
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            rows = list(pool.map(start, enumerate(stores)))
        self.assertEqual(sum(row is not None for row in rows), 1)
        self.assertEqual(next(row for row in rows if row)["generation"], 2)

    def test_lost_revoke_reply_and_cancelled_recovery_preserve_original_identity(self):
        self.enable(); self.c.start("old"); self.settings(now_ms=1300001, lose_reply=True)
        leases.revoke(self.c.sa, "revoke", "t", "old")
        self.c.start("new", store=self.c.sb)
        leases.revoke(self.c.sb, "revoke", "t", "old")
        self.assertEqual(runtime.check(self.c.sb, "t", "new")["generation"], 2)
        self.settings(now_ms=1600002, reject_before_cas=True)
        with self.assertRaises(TaskError):
            leases.revoke(self.c.sb, "cancelled", "t", "new")
        old_candidate = json.loads((self.c.sb.local / "operations/cancelled.json").read_text())["pending"]["candidate"]
        leases.revoke(self.c.sb, "cancelled", "t", "new")
        self.assertTrue(self.c.sb.contains(self.c.sb.fetch(), old_candidate))

    def test_unknown_pending_renewal_never_receives_a_fresh_proof(self):
        self.enable(); self.c.start(); self.settings(reject_before_cas=True)
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "renew", "t", "r")
        candidate = json.loads((self.c.sa.local / "operations/renew.json").read_text())["pending"]["candidate"]
        self.settings(now_ms=1300001)
        with self.assertRaises(TaskError):
            leases.renew(self.c.sa, "renew", "t", "r")
        self.assertFalse(self.c.sa.contains(self.c.sa.fetch(), candidate))

    def test_policy_requires_enforced_adapter_and_all_writers_drained(self):
        before = self.c.sa.fetch()
        with self.assertRaises(TaskError):
            leases.configure(Store(self.c.a), "bad", "t", 1, mode=leases.MODE, profile="test", drained=True)
        for settings in ({"no_atomic": True}, {"unfenced": True}, {"effects": "mutable_site"}):
            self.settings(no_atomic=False, unfenced=False, effects="authority_only")
            self.settings(**settings)
            with self.assertRaises(TaskError):
                self.enable()
        self.assertEqual(before, self.c.sa.fetch())
        self.settings(no_atomic=False, unfenced=False, effects="authority_only")
        self.c.start()
        with self.assertRaisesRegex(TaskError, "Drain all"):
            self.enable()
        self.assertNotIn("lease", runtime.load_run(self.c.sa.snapshot(), "t", "r")[2])

    def test_policy_upgrade_has_repository_writer_barrier_and_manual_still_works(self):
        self.enable()
        config = self.c.sa.snapshot().json(CONTROL + "/config.json")
        self.assertEqual(config["schema_version"], 3)
        self.c.add(task="manual")
        self.c.start(task="manual")
        self.assertEqual(self.c.sa.snapshot().json(CONTROL + '/config.json')['schema_version'], 9)
        result = lifecycle.reap(self.c.sb, "manual", apply=True, observer_profile="stateless")
        self.assertEqual(result["runs"][0]["status"], "stop_evidence_required")
        runtime.recover(self.c.sb, "manual-stop", "manual", "r", stopped=True, settled=True, evidence="test:stop")
        self.assertEqual(runtime.load_run(self.c.sa.snapshot(), "manual", "r")[2]["status"], "lost")

    def test_checkpoint_requires_committed_artifacts_and_matching_provenance(self):
        self.enable(); self.c.start("old")
        with self.assertRaises(TaskError):
            leases.checkpoint(self.c.sa, "invalid", "t", "old", "review", "compute", "a" * 64,
                              ["tasks/t/runs/old/artifacts/missing"])
        self.settings(now_ms=1300001)
        leases.revoke(self.c.sb, "revoke", "t", "old")
        with self.assertRaises(TaskError):
            runtime.start(self.c.sb, "new", "t", "new", resume_from="old", checkpoint_id="review")

    def test_definition_change_prevents_automatic_checkpoint_adoption(self):
        self.enable(); self.completed(); self.settings(now_ms=1300001)
        leases.revoke(self.c.sb, "revoke", "t", "old")
        runtime.edit_task(self.c.sb, "edit", "t", 2, body="different computation")
        with self.assertRaisesRegex(TaskError, "provenance"):
            runtime.start(self.c.sb, "new", "t", "new", resume_from="old", checkpoint_id="review")

    def test_packaged_cli_can_enable_lease_and_start(self):
        bundle = self.c.base / "taskctl.pyz"
        self.c.command(sys.executable, str(base.ROOT / "bin/build-task-inbox"), "--output", str(bundle))
        common = [sys.executable, str(bundle), "--repo", str(self.c.a), "--transport-adapter", str(ADAPTER)]
        self.c.command(*common, "--operation-id", "enable", "task", "recovery-policy", "--id", "t",
                       "--expected-revision", "1", "--mode", leases.MODE, "--profile", "test", "--confirm-writers-drained")
        self.c.command(*common, "--operation-id", "start", "run", "start", "--task", "t", "--runner", "runner")
        self.assertEqual(runtime.check(self.c.sa, "t", "start")["lease"]["expires_ms"], 1300000)


if __name__ == "__main__":
    unittest.main()
