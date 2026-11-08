"""Transport conformance against real Git, plus stateless recovery regressions."""
import json
from pathlib import Path
import sys
import subprocess
import tempfile
from unittest.mock import patch
import unittest

import task_runtime_test as base
from task_runtime import lifecycle, runtime, store as storage
from task_runtime.store import Store, TaskError
from task_runtime.transport import CAPABILITIES, PROTOCOL, DirectGit, External, TransportError, create, oid


ADAPTER = base.ROOT / "test/fixtures/task_transport_adapter.py"


class TransportValidationTest(unittest.TestCase):
    def setUp(self):
        # Bypass installation checks here to isolate the wire protocol. Actual
        # executable installation is covered separately and by TransportTests.
        self.adapter = External.__new__(External)
        self.adapter.program = '/installed/adapter'
        self.adapter.repo = Path('/repository')
        self.adapter.context = {'repository': '/repository', 'remote': 'origin',
                                'authority_ref': 'refs/heads/main', 'object_format': 'sha1'}
        self.adapter.object_format = 'sha1'

    def test_object_ids_require_exact_lowercase_hex_for_repository_format(self):
        for object_format, length in [('sha1', 40), ('sha256', 64)]:
            self.assertEqual(oid('a' * length, object_format), 'a' * length)
            for value in (None, 1, '', 'a' * (length - 1), 'a' * (length + 1), 'A' * length, 'g' * length):
                with self.subTest(format=object_format, value=value), self.assertRaises(TransportError):
                    oid(value, object_format)

    def test_call_binds_request_identity_and_uses_only_fixed_error_vocabulary(self):
        def respond(argv, **kwargs):
            request = json.loads(kwargs['input'])
            self.assertEqual(request['protocol'], PROTOCOL)
            self.assertEqual(request['context'], self.adapter.context)
            fingerprint = request.pop('request_id')
            self.assertEqual(fingerprint, storage.digest(request))
            return subprocess.CompletedProcess(argv, 0, storage.encode(dict(
                protocol=PROTOCOL, request_id=fingerprint, status='ok', oid=None)), b'PRIVATE_TOKEN')
        with patch('task_runtime.transport.storage.git_support.run_process', side_effect=respond):
            self.assertIsNone(self.adapter.read_ref('refs/heads/main'))
        for status, code in [('unsupported', 'unsupported_transport'), ('conflict', 'transport_conflict'),
                             ('guard_rejected', 'lease_guard_rejected'), ('unknown', 'transport_outcome_unknown'),
                             ('PRIVATE_TOKEN', 'transport_outcome_unknown')]:
            def failed(argv, **kwargs):
                request = json.loads(kwargs['input'])
                return subprocess.CompletedProcess(argv, 0, storage.encode(dict(
                    protocol=PROTOCOL, request_id=request['request_id'], status=status, error='PRIVATE_TOKEN')), b'')
            with self.subTest(status=status), patch('task_runtime.transport.storage.git_support.run_process', side_effect=failed):
                with self.assertRaises(TransportError) as raised:
                    self.adapter.call('read_ref')
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(raised.exception.definite_rejection, status == 'conflict')
                self.assertNotIn('PRIVATE_TOKEN', str(raised.exception))

    def test_invalid_or_ambiguous_process_responses_are_rejected(self):
        responses = [(0, b'not json'), (0, b'\xff'), (0, b'[]'), (0, b'{}'),
                     (0, storage.encode({'protocol': PROTOCOL, 'request_id': 'wrong', 'status': 'ok'})),
                     (0, b'x' * (1024 * 1024 + 1)), (1, b'PRIVATE_TOKEN')]
        for returncode, stdout in responses:
            response = subprocess.CompletedProcess([], returncode, stdout, b'PRIVATE_TOKEN')
            with self.subTest(returncode=returncode, size=len(stdout)), patch(
                    'task_runtime.transport.storage.git_support.run_process', return_value=response):
                with self.assertRaises(TransportError) as raised:
                    self.adapter.call('compare_exchange')
                self.assertNotIn('PRIVATE_TOKEN', str(raised.exception))
        for error in (OSError('PRIVATE_TOKEN'), subprocess.TimeoutExpired('PRIVATE_TOKEN', 60)):
            with patch('task_runtime.transport.storage.git_support.run_process', side_effect=error):
                with self.assertRaises(TransportError) as raised:
                    self.adapter.call('compare_exchange')
                self.assertEqual(raised.exception.code, 'transport_outcome_unknown')

    def test_capabilities_and_ref_responses_require_strict_types_and_requested_scope(self):
        with patch.object(self.adapter, 'call', return_value={'capabilities': {CAPABILITIES[0]: True, CAPABILITIES[1]: 1}}):
            capabilities = self.adapter.capabilities({})
            self.assertTrue(capabilities[CAPABILITIES[0]])
            self.assertFalse(capabilities[CAPABILITIES[1]])
            self.assertEqual(set(capabilities), set(CAPABILITIES))
        for result in ({}, {'capabilities': []}):
            with patch.object(self.adapter, 'call', return_value=result), self.assertRaises(TransportError):
                self.adapter.capabilities({})
        with patch.object(self.adapter, 'call', return_value={}), self.assertRaises(TransportError):
            self.adapter.read_ref('refs/heads/main')
        for result in ({}, {'refs': []}, {'refs': {'refs/tags/other': 'a' * 40}}, {'refs': {1: 'a' * 40}}):
            with self.subTest(result=result), patch.object(self.adapter, 'call', return_value=result), self.assertRaises(TransportError):
                self.adapter.list_refs(['refs/heads/'])

    def test_fetch_requires_requested_commit_and_complete_object_closure(self):
        tip = 'a' * 40
        with patch.object(self.adapter, 'read_ref', return_value=None):
            self.assertIsNone(self.adapter.fetch('refs/heads/missing', missing=True))
            with self.assertRaises(TransportError) as raised:
                self.adapter.fetch('refs/heads/missing')
            self.assertEqual(raised.exception.code, 'transport_ref_missing')
        with patch.object(self.adapter, 'read_ref', return_value=tip):
            for result, outputs in [({'oid': 'b' * 40}, []), ({'oid': tip}, ['blob']),
                                    ({'oid': tip}, ['commit', '?' + 'b' * 40])]:
                with self.subTest(result=result, outputs=outputs), patch.object(self.adapter, 'call', return_value=result), \
                        patch('task_runtime.transport.storage.git', side_effect=outputs), self.assertRaises(TransportError):
                    self.adapter.fetch('refs/heads/main')
            with patch.object(self.adapter, 'call', return_value={'oid': tip}), \
                    patch('task_runtime.transport.storage.git', side_effect=['commit', tip + '\n' + 'b' * 40 + ' file']):
                self.assertEqual(self.adapter.fetch('refs/heads/main'), tip)
        for result in ({}, {'oid': 'b' * 40}):
            with patch.object(self.adapter, 'call', return_value=result), self.assertRaises(TransportError):
                self.adapter.push('refs/heads/main', tip, None)

    def test_direct_git_rejects_inexact_refs_and_malformed_listing_and_cleans_fetch_ref(self):
        direct = DirectGit(Path('/repository'), 'origin', 'refs/heads/main')
        for output in ('a refs/heads/other', 'a refs/heads/main\nb refs/heads/main', 'malformed'):
            with self.subTest(output=output), patch('task_runtime.transport.storage.git', return_value=output), self.assertRaises(TransportError):
                direct.read_ref('refs/heads/main')
        with patch('task_runtime.transport.storage.git', return_value='bad'), self.assertRaises(TransportError):
            direct.list_refs(['refs/heads/task/'])
        with patch('task_runtime.transport.storage.git', return_value='a refs/heads/task/one\nb refs/heads/other'):
            self.assertEqual(direct.list_refs(['refs/heads/task/']), {'refs/heads/task/one': 'a'})
        error = storage.GitFailure('fixture', {'definite_rejection': False})
        with patch('task_runtime.transport.storage.git', side_effect=['.git/shallow', '', error, '']) as git:
            with self.assertRaises(TransportError):
                direct.fetch('refs/heads/main')
            self.assertEqual(git.call_args.args[1:3], ('update-ref', '-d'))

    def test_adapter_installation_requires_executable_absolute_path_outside_repository(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / 'repo'
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            inside = repo / 'adapter'
            inside.write_text('#!/bin/sh\n')
            inside.chmod(0o755)
            outside = Path(temp) / 'installed'
            outside.write_text('#!/bin/sh\n')
            outside.chmod(0o644)
            for program in ('relative', str(repo / 'missing'), str(inside), str(outside)):
                with self.subTest(program=program), self.assertRaises(TransportError):
                    External(repo, 'origin', 'refs/heads/main', program)
            with patch.dict('os.environ', {}, clear=True):
                self.assertIsInstance(create(repo, 'origin', 'refs/heads/main'), DirectGit)

    def test_adapter_installation_resolves_repository_and_executable_aliases(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo = root / 'repo'
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            alias = root / 'repo-alias'
            alias.symlink_to(repo.resolve(), target_is_directory=True)
            inside = repo / 'adapter'
            outside = root / 'installed'
            executed = root / 'executed'
            for path in (inside, outside):
                path.write_text('#!/bin/sh\ntouch "' + str(executed) + '"\nexit 99\n')
                path.chmod(0o755)
            executable_alias = root / 'adapter-alias'
            executable_alias.symlink_to(inside.resolve())
            constructors = (
                ('External', lambda repository, program: External(
                    repository, 'origin', 'refs/heads/main', str(program))),
                ('create', lambda repository, program: create(
                    repository, 'origin', 'refs/heads/main', str(program))),
                ('Store', lambda repository, program: Store(
                    repository, transport_adapter=str(program)).transport),
            )
            for name, construct in constructors:
                for repository in (repo, alias):
                    for program in (inside, alias / 'adapter', executable_alias):
                        with self.subTest(constructor=name, repository=repository, program=program):
                            with self.assertRaises(TransportError) as raised:
                                construct(repository, program)
                            self.assertEqual(raised.exception.code, 'invalid_transport_adapter')
                            self.assertEqual(raised.exception.operation, 'configure')
                    with self.subTest(constructor=name, repository=repository, external=True):
                        adapter = construct(repository, outside)
                        self.assertEqual(adapter.repo, repo.resolve())
                        self.assertEqual(adapter.context['repository'], str(repo.resolve()))
                        self.assertEqual(adapter.program, str(outside.resolve()))
            self.assertFalse(executed.exists(), 'Configuration must not execute an adapter')


class DirectGitIsolationTests(base.RuntimeCase):
    def test_fetch_leaves_configured_tracking_ref_and_fetch_head_untouched(self):
        initial = self.sb.fetch()
        self.git(self.b, 'update-ref', 'refs/remotes/origin/main', initial)
        fetch_head = self.sb.common / 'FETCH_HEAD'
        fetch_head.write_text('caller-owned fetch state\n')
        self.add(); self.start()
        expected = self.sa.fetch()
        self.assertNotEqual(expected, initial)
        self.assertEqual(self.sb.fetch(), expected)
        self.assertEqual(self.git(self.b, 'rev-parse', 'refs/remotes/origin/main'), initial)
        self.assertEqual(fetch_head.read_text(), 'caller-owned fetch state\n')
        self.assertEqual(self.git(self.b, 'for-each-ref', '--format=%(refname)', 'refs/task-runtime-fetch/'), '')


class TransportTests(base.TaskRuntimeTest):
    # Reuse the original lifecycle suite with independently provisioned stores.
    # Tests which explicitly build a CLI bundle additionally exercise its default
    # direct-Git mode; the tests below exercise the exported external entry point.
    def setUp(self):
        self.patch = patch.object(base, "Store", side_effect=lambda *a, **k: Store(*a, **k, transport_adapter=str(ADAPTER)))
        self.patch.start()
        self.addCleanup(self.patch.stop)
        super().setUp()

    def settings(self, value):
        (self.sa.common / "transport-fixture.json").write_text(json.dumps(value))

    def test_unsupported_capabilities_fail_before_admission(self):
        self.add()
        before = self.sa.fetch()
        head = self.git(self.a, "rev-parse", "HEAD")
        index = self.git(self.a, "write-tree")
        self.settings({"missing": ["compare_and_swap", "tag_refs", "conditional_delete"]})
        store = Store(self.a, transport_adapter=str(ADAPTER))
        with self.assertRaisesRegex(TransportError, "unsupported_transport.*compare_and_swap"):
            self.start(store=store)
        self.assertEqual(before, self.sa.fetch())
        self.assertEqual(head, self.git(self.a, "rev-parse", "HEAD"))
        self.assertEqual(index, self.git(self.a, "write-tree"))

    def test_lost_adapter_reply_reconciles_original_candidate(self):
        self.add()
        self.settings({"lose_reply": True})
        first = self.start()
        replay = self.start(store=self.sb)
        self.assertEqual(first["run_id"], replay["run_id"])
        self.assertEqual(first["definition_commit"], replay["definition_commit"])
        self.assertEqual(self.sa.snapshot().json("tasks/t/runtime.json")["slots"]["1"]["run_id"], "r")

    def test_candidate_selection_does_not_push_mounted_head(self):
        before = self.git(self.a, "rev-parse", "HEAD")
        first = self.sa.fetch()
        candidate = self.sa.commit(first, {"candidate.bin": b"\x00\xffexact"}, "candidate")
        self.sa.push(self.sa.ref, candidate, first)
        self.assertEqual(candidate, self.sb.fetch())
        self.assertEqual(before, self.git(self.a, "rev-parse", "HEAD"))
        self.assertEqual(self.sb.snapshot().read("candidate.bin"), b"\x00\xffexact")

    def test_adapter_diagnostics_never_echo_remote_text(self):
        self.settings({"leak": True})
        result = self.command(sys.executable, str(base.ROOT / "bin/task-inbox"), "--repo", str(self.a),
                              "--transport-adapter", str(ADAPTER), "transport-check", ok=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("secret-fixture-value", result.stdout + result.stderr)
        self.assertIn("transport_outcome_unknown", result.stderr)

    def test_code_only_bundle_loads_installed_external_adapter(self):
        bundle = self.base / "adapter-taskctl.pyz"
        self.command(sys.executable, str(base.ROOT / "bin/build-task-inbox"), "--output", str(bundle))
        result = self.command(sys.executable, str(bundle), "--repo", str(self.b),
                              "--transport-adapter", str(ADAPTER), "transport-check")
        check = json.loads(result.stdout)
        self.assertTrue(check["supported"])
        self.assertEqual(check["transport"], "external")
        self.command(sys.executable, str(bundle), "--repo", str(self.b), "--transport-adapter", str(ADAPTER),
                     "--operation-id", "bundle-add", "task", "add", "--id", "bundled", "--title", "Bundled")
        self.assertEqual(runtime.load_task(self.sa.snapshot(), "bundled")[0]["title"], "Bundled")


class RecoveryDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = base.TaskRuntimeTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.case = self.fixture

    def test_fresh_clone_each_tick_is_actionable_without_revocation(self):
        c = self.case
        c.add(recovery_mode="git_only"); c.start(); runtime.heartbeat(c.sa, "t", "r")
        before = c.sa.fetch()
        for number in range(4):
            path = c.base / ("fresh-" + str(number))
            c.command("git", "clone", str(c.remote), str(path))
            with patch.object(runtime, "observer_boot", return_value=str(number)), patch.object(runtime.time, "monotonic", return_value=100 + 900 * number):
                result = lifecycle.reap(Store(path), "t", apply=True, observer_profile="stateless")
            self.assertTrue(result["needs_attention"])
            self.assertEqual(result["runs"][0]["status"], "recovery_policy_required")
            self.assertEqual(result["runs"][0]["unchanged_seconds"], 0)
            self.assertEqual(result["runs"][0]["recovery_horizon"], "not_established")
        self.assertEqual(before, c.sa.fetch())
        self.assertEqual(runtime.check(c.sa, "t", "r")["status"], "running")

    def test_unknown_profile_does_not_silently_hide_first_observation(self):
        c = self.case
        c.add(recovery_mode="git_only"); c.start()
        result = lifecycle.reap(c.sb, "t", apply=True)
        self.assertTrue(result["needs_attention"])
        self.assertEqual(result["runs"][0]["reason"], "declare_retained_observer_or_install_stop_check")

    def test_manual_run_needs_evidence_even_before_staleness(self):
        c = self.case
        c.add(); c.start()
        result = lifecycle.reap(c.sb, "t", apply=True, observer_profile="retained")
        self.assertTrue(result["needs_attention"])
        self.assertEqual(result["runs"][0]["status"], "stop_evidence_required")
        self.assertFalse(result["runs"][0]["stale"])

    def test_retained_observer_still_recovers_after_observed_interval(self):
        c = self.case
        c.add(recovery_mode="git_only"); c.start()
        with patch.object(runtime.time, "monotonic", return_value=100):
            first = lifecycle.reap(c.sb, "t", apply=True, observer_profile="retained")
        self.assertFalse(first["needs_attention"])
        with patch.object(runtime.time, "monotonic", return_value=401):
            second = lifecycle.reap(c.sb, "t", apply=True, observer_profile="retained")
        self.assertEqual(second["runs"][0]["status"], "lost")

    def test_stateless_profile_can_use_verified_stop_evidence(self):
        c = self.case
        c.add(); c.start()
        adapter = c.base / "stop-check"
        adapter.write_text("#!" + sys.executable + "\nimport json,sys\nr=json.load(sys.stdin)\n"
                           "r.update(writer_stopped=True,children_stopped=True,external_operations_settled=True,"
                           "pending_git_operations_settled=True,evidence='fixture:verified')\nprint(json.dumps(r))\n")
        adapter.chmod(0o755)
        result = lifecycle.reap(c.sb, "t", apply=True, stop_check=str(adapter), observer_profile="stateless")
        self.assertEqual(result["runs"][0]["status"], "lost")
        c.start("replacement", store=c.sb)

    def test_renewed_long_run_and_clock_regression_do_not_release_slot(self):
        c = self.case
        c.add(recovery_mode="git_only"); c.start(); runtime.heartbeat(c.sa, "t", "r")
        with patch.object(runtime.time, "monotonic", return_value=100):
            lifecycle.reap(c.sb, "t", apply=True, observer_profile="retained")
        runtime.heartbeat(c.sa, "t", "r")
        with patch.object(runtime.time, "monotonic", return_value=1000):
            renewed = lifecycle.reap(c.sb, "t", apply=True, observer_profile="retained")
        self.assertEqual(renewed["runs"][0]["observation_reset"], "heartbeat_changed")
        self.assertFalse(renewed["runs"][0]["stale"])
        with patch.object(runtime.time, "monotonic", return_value=10):
            reset = lifecycle.reap(c.sb, "t", apply=True)
        self.assertEqual(reset["runs"][0]["observation_reset"], "clock_regressed")
        self.assertTrue(reset["needs_attention"])
        runtime.check(c.sa, "t", "r")

    def test_constructed_restricted_gateway_fails_before_admission(self):
        c = self.case
        c.add(); c.start(); runtime.heartbeat(c.sa, "t", "r")
        original = storage.git
        before = c.sa.fetch()
        head = c.git(c.a, "rev-parse", "HEAD")
        index = c.git(c.a, "write-tree")
        network_operations = []
        def gateway(repo, *args, **kwargs):
            if args[0] in ("ls-remote", "fetch", "push"):
                network_operations.append(args[0])
            if args[0] == "ls-remote":
                return original(repo, "ls-remote", "--refs", "origin", **kwargs)
            if args[0] == "fetch":
                source = args[-1].split(":")[0]
                return original(repo, "fetch", "origin", source + ":refs/remotes/origin/" +
                                source.removeprefix("refs/heads/"), **kwargs)
            if args[0] == "push":
                raise TaskError("restricted fixture rejects flags and refspecs")
            return original(repo, *args, **kwargs)
        with patch.object(storage, "git", side_effect=gateway):
            direct = Store(c.a)
            with self.assertRaisesRegex(TransportError, "unsupported_transport.*exact_ref_read"):
                direct.preflight()
            with self.assertRaisesRegex(TransportError, "unsupported_transport.*exact_object_fetch"):
                c.start("blocked", store=direct)
        self.assertNotIn("push", network_operations)
        self.assertEqual(before, c.sa.fetch())
        self.assertEqual(head, c.git(c.a, "rev-parse", "HEAD"))
        self.assertEqual(index, c.git(c.a, "write-tree"))

    def test_adapter_must_echo_identity_and_explicit_delete_result(self):
        c = self.case
        external = External(c.a, "origin", "refs/heads/main", str(ADAPTER))
        from subprocess import CompletedProcess
        response = CompletedProcess([], 0, b'{"protocol":"task-runtime-transport/v1","request_id":"other","status":"ok","oid":null}', b'')
        with patch.object(storage.git_support, "run_process", return_value=response):
            with self.assertRaisesRegex(TransportError, "invalid_transport_response"):
                external.read_ref("refs/heads/main")
        with patch.object(external, "call", return_value={"status": "ok"}):
            with self.assertRaisesRegex(TransportError, "invalid_transport_response"):
                external.push("refs/heads/test", None, "a" * 40)


if __name__ == "__main__":
    unittest.main()
