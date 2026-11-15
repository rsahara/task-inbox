"""Real Git regression tests for atomic main inbox and signal contracts."""
import json
import sys
from unittest.mock import patch

from task_runtime_test import ROOT, RuntimeCase
from task_runtime import lifecycle, main_io, maintenance, runtime, signals, task_io
from task_runtime.store import GitFailure, TaskError, encode, git


def interface(required=False):
    return dict(schema_version=2, inbox=['signal-changed', 'work'], readiness='inbox',
                signals=dict(reads=[], writes=[dict(id='data-a', type='example/data', required=required),
                                               dict(id='data-b', type='example/data')]))


def signal(sid='data-a', value=1):
    return dict(schema_version=1, task_id='source', id=sid, revision=1,
                type='example/data', status='active', value=dict(count=value))


class MainInboxTest(RuntimeCase):
    def setUp(self):
        super().setUp()
        for task in ('source', 'viewer', 'observer'):
            self.add(task, interface=interface())
        signals.enable(self.sa, 'signals')
        main_io.migrate(self.sa, 'migrate', drained=True)

    def run_source(self, rid, values):
        self.start(rid, task='source')
        work = self.prepare(rid, task='source')
        for value in values:
            signals.put(self.sa, 'source', rid, value)
        return work

    def finish(self, rid, **kwargs):
        return runtime.publish(self.sa, 'finish-' + rid, 'source', rid, outcome='succeeded', **kwargs)

    def test_changed_and_per_run_delivery_exact_ids_and_atomic_finish(self):
        signals.subscribe(self.sa, 'sub-a', 'source', 'viewer', signal_id='data-a')
        signals.subscribe(self.sa, 'sub-b', 'source', 'observer', signal_id='data-b', on='run')
        self.run_source('one', [signal(), signal('data-b')])
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        self.finish('one')
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 1)
        self.assertEqual(len(task_io.pending(self.sb, 'observer')['messages']), 1)
        self.finish('one')
        self.assertEqual(len(task_io.pending(self.sb, 'observer')['messages']), 1)
        self.run_source('two', [signal(), signal('data-b')])
        self.finish('two')
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 1)
        notices = task_io.pending(self.sb, 'observer')['messages']
        self.assertEqual({n['payload']['run_id'] for n in notices}, {'one', 'two'})
        self.assertEqual({n['payload']['revision'] for n in notices}, {1})
        self.assertFalse(self.sa.remote_tip('refs/heads/task-io/viewer/inbox'))
        self.run_source('three', [signal(value=2)])
        self.finish('three')
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 2)
        self.assertEqual(len(task_io.pending(self.sb, 'observer')['messages']), 2)
        # Each message addition shares its authority commit with terminal success.
        name = main_io.inbox(self.sa.snapshot(), 'viewer') + next(m['id'] for m in task_io.pending(self.sa, 'viewer')['messages'] if m['payload']['revision'] == 2) + '.json'
        commit = self.git(self.a, 'log', '-1', '--format=%H', self.sa.fetch(), '--', name)
        from task_runtime.store import Snapshot
        self.assertEqual(runtime.load_run(Snapshot(self.sa, commit), 'source', 'three')[2]['status'], 'succeeded')

    def test_input_result_output_and_notification_commit_or_none(self):
        signals.subscribe(self.sa, 'sub', 'source', 'viewer', signal_id='data-a')
        task_io.send(self.sa, 'request', 'source', 'input', 'work', {})
        self.run_source('one', [signal()])
        before = self.sa.fetch()
        with self.assertRaises(TaskError):
            self.finish('one', input_results={'missing': {'status': 'succeeded'}})
        self.assertEqual(before, self.sa.fetch())
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        # The failed operation binds its input body; use a new explicit finish operation.
        runtime.publish(self.sa, 'finish-correct', 'source', 'one', outcome='succeeded',
                        input_results={'input': {'status': 'succeeded'}})
        self.assertFalse(task_io.pending(self.sb, 'source')['messages'])
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 1)

    def test_staged_input_results_remain_pending_until_successful_finish(self):
        task_io.send(self.sa, 'request', 'source', 'input', 'work', {})
        self.run_source('one', [])
        before = self.sa.fetch()
        main_io.stage_results(self.sa, 'source', 'one', {'input': {'status': 'succeeded'}})
        self.assertEqual(self.sa.fetch(), before)
        self.assertTrue(task_io.pending(self.sb, 'source')['messages'])
        runtime.publish(self.sa, 'save', 'source', 'one')
        self.assertTrue(task_io.pending(self.sb, 'source')['messages'])
        self.finish('one')
        self.assertFalse(task_io.pending(self.sb, 'source')['messages'])
        self.assertEqual(self.finish('one')['status'], 'succeeded')

    def test_common_supervisor_allows_verified_finish_and_rejects_an_unknown_relaunch(self):
        definition = runtime.load_task(self.sa.snapshot(), 'source')[0]
        definition['execution']['heartbeat_seconds'] = 1
        self.sa.transact('fast-heartbeat', dict(kind='fixture'),
                         lambda snapshot: ({'tasks/source/task.json': encode(definition)}, {}))
        self.run_source('one', [])
        child = self.base / 'finishing-worker.py'
        done = self.base / 'worker-exited'
        child.write_text('import sys,time\nfrom pathlib import Path\n'
            + 'sys.path.insert(0, ' + repr(str(ROOT / 'foundation')) + ')\n'
            + 'from task_runtime import runtime\nfrom task_runtime.store import Store\n'
            + 'runtime.publish(Store(' + repr(str(self.a)) + '), "child-finish", "source", "one", outcome="succeeded")\n'
            + 'time.sleep(1.5)\nPath(' + repr(str(done)) + ').touch()\n')
        result = lifecycle.execute(self.sa, 'source', 'one', [sys.executable, str(child)], worker_finish=True)
        self.assertEqual(result['exit_code'], 0)
        self.assertTrue(done.exists())
        self.run_source('two', [])
        with patch.object(runtime, 'heartbeat', side_effect=TaskError('ownership lost')):
            with self.assertRaisesRegex(TaskError, 'ownership lost'):
                lifecycle.execute(self.sa, 'source', 'two', [sys.executable, '-c', 'raise AssertionError("launched")'], worker_finish=True)
        with self.assertRaisesRegex(TaskError, 'already launched'):
            lifecycle.execute(self.sa, 'source', 'two', [sys.executable, '-c', 'raise AssertionError("relaunched")'], worker_finish=True)

    def test_heartbeat_and_finish_do_not_commit_worker_files_or_merge_history(self):
        work = self.run_source('one', [])
        self.output(work, 'tasks/source/data/report.txt')
        head = self.git(work, 'rev-parse', 'HEAD')
        staged = self.git(work, 'diff', '--cached')
        runtime.heartbeat(self.sa, 'source', 'one')
        self.assertEqual(head, self.git(work, 'rev-parse', 'HEAD'))
        self.assertEqual(staged, self.git(work, 'diff', '--cached'))
        self.finish('one')
        self.assertEqual(self.sa.snapshot().read('tasks/source/data/report.txt'), b'output\n')
        self.assertFalse(self.git(self.a, 'rev-list', '--merges', self.sa.fetch()))
        self.assertFalse(self.sa.remote_tip('refs/heads/task-runtime/source/one/work'))

    def test_discard_after_publication_does_not_require_an_implicit_remote_checkpoint(self):
        work = self.run_source('one', [])
        self.output(work, 'tasks/source/data/accepted.txt')
        runtime.publish(self.sa, 'publish', 'source', 'one')
        self.output(work, 'tasks/source/data/unadopted.txt')
        runtime.publish(self.sa, 'cancel', 'source', 'one', outcome='cancelled', discard_output=True)
        snap = self.sa.snapshot()
        self.assertEqual(runtime.load_run(snap, 'source', 'one')[2]['status'], 'cancelled')
        self.assertIsNotNone(snap.read('tasks/source/data/accepted.txt'))
        self.assertIsNone(snap.read('tasks/source/data/unadopted.txt'))
        self.assertTrue((work / 'tasks/source/data/unadopted.txt').exists())

    def test_finish_captures_restored_and_deleted_published_files(self):
        name = 'tasks/source/data/existing.txt'
        new = 'tasks/source/data/new.txt'
        self.sa.transact('seed', dict(kind='fixture'), lambda s: ({name: b'original\n'}, {}))
        work = self.run_source('reverted', [])
        self.output(work, name, b'intermediate\n')
        self.output(work, new)
        runtime.publish(self.sa, 'intermediate', 'source', 'reverted')
        self.output(work, name, b'original\n')
        (work / new).unlink()
        self.assertFalse(self.git(work, 'status', '--porcelain'))
        self.finish('reverted')
        self.assertEqual(self.sb.snapshot().read(name), b'original\n')
        self.assertIsNone(self.sb.snapshot().read(new))
        self.assertFalse(work.exists())

    def test_cleanup_preserves_explicit_commit_history_in_remote_tag(self):
        work = self.run_source('committed', [])
        self.output(work, 'tasks/source/data/output.txt')
        self.git(work, 'add', '.')
        self.git(work, 'commit', '-m', 'Explicit output history')
        self.git(work, 'commit', '--allow-empty', '-m', 'Explicit empty checkpoint')
        head = self.git(work, 'rev-parse', 'HEAD')
        self.finish('committed')
        self.assertFalse(work.exists())
        ref = 'refs/tags/task-work-history/source/committed'
        self.assertEqual(self.sa.remote_tip(ref), head)
        self.git(self.b, 'fetch', 'origin', ref)
        self.assertEqual(self.git(self.b, 'rev-parse', 'FETCH_HEAD'), head)
        self.assertEqual(self.git(self.b, 'log', '-2', '--format=%s', head).splitlines(),
                         ['Explicit empty checkpoint', 'Explicit output history'])
        self.assertFalse(self.git(self.a, 'rev-list', '--merges', self.sa.fetch()))

    def test_finish_captures_files_created_only_in_an_explicit_merge_commit(self):
        work = self.run_source('merged', [])
        base = self.git(work, 'rev-parse', 'HEAD')
        other = self.git(work, 'commit-tree', base + '^{tree}', '-p', base, '-m', 'Other branch')
        self.git(work, 'commit', '--allow-empty', '-m', 'Worker branch')
        head = self.git(work, 'rev-parse', 'HEAD')
        name = 'tasks/source/data/merge-only.txt'
        self.output(work, name, b'merge output\n')
        self.git(work, 'add', name)
        tree = self.git(work, 'write-tree')
        merged = self.git(work, 'commit-tree', tree, '-p', head, '-p', other, '-m', 'Merge resolution')
        self.git(work, 'reset', '--hard', merged)
        self.assertFalse(self.git(work, 'status', '--porcelain'))
        self.finish('merged')
        self.assertEqual(self.sa.snapshot().read(name), b'merge output\n')
        self.assertFalse(work.exists())
        self.assertEqual(self.sa.remote_tip('refs/tags/task-work-history/source/merged'), merged)

    def test_finish_manifest_includes_authority_events_with_and_without_heartbeat(self):
        for heartbeat in (False, True):
            rid = 'events-' + str(heartbeat).lower()
            work = self.run_source(rid, [])
            if heartbeat:
                runtime.heartbeat(self.sa, 'source', rid)
            task_io.emit(self.sa, 'emit-' + rid, 'source', rid, 'intent', 'external-intent', {})
            with patch.object(maintenance, 'maintain'):
                self.finish(rid)
            run = runtime.load_run(self.sa.snapshot(), 'source', rid)[2]
            saved = task_io.IOStore(self.sa, 'source', run).optional()
            self.assertEqual(saved.json('events/intent.json')['id'], 'intent')
            self.assertEqual(maintenance.collect(self.sa, 'source', rid, apply=True)['status'], 'done')
            self.assertFalse(work.exists())

    def test_live_io_reader_combines_authority_events_and_isolated_heartbeat(self):
        self.run_source('live-events', [])
        run = runtime.check(self.sa, 'source', 'live-events')
        task_io.emit(self.sa, 'intent-live', 'source', 'live-events', 'intent', 'external-intent', {})
        view = task_io.IOStore(self.sb, 'source', run).optional()
        self.assertEqual(view.json('events/intent.json')['kind'], 'external-intent')
        self.assertIsNone(view.json('state.json'))
        runtime.heartbeat(self.sa, 'source', 'live-events')
        view = task_io.IOStore(self.sb, 'source', run).optional()
        self.assertEqual(view.json('events/intent.json')['kind'], 'external-intent')
        self.assertEqual(view.json('state.json')['run_id'], 'live-events')

    def test_run_io_transaction_rechecks_main_events_after_a_cas_conflict(self):
        self.run_source('decision', [])
        run = runtime.check(self.sa, 'source', 'decision')
        stream = task_io.IOStore(self.sa, 'source', run)
        stream.ensure()
        push = self.sa.push
        raced = []
        def competing_push(ref, candidate, expected, **kwargs):
            if ref == self.sa.ref and not raced:
                raced.append(True)
                task_io.emit(self.sb, 'external', 'source', 'decision', 'external', 'external-intent', {})
            return push(ref, candidate, expected, **kwargs)
        def decide(snapshot):
            if snapshot.json('events/external.json'):
                raise TaskError('External effects require reconciliation')
            return {'events/failure.json': encode(dict(kind='failure'))}, {}
        with patch.object(self.sa, 'push', side_effect=competing_push):
            with self.assertRaisesRegex(TaskError, 'External effects'):
                stream.transact('decide-failure', dict(kind='fixture-decision'), decide)
        self.assertIsNone(self.sa.snapshot().read('tasks/source/runs/decision/io/events/failure.json'))
        self.assertEqual(runtime.check(self.sa, 'source', 'decision')['status'], 'running')

    def test_required_output_and_type_contract(self):
        runtime.edit_task(self.sa, 'required', 'source', 1, interface=interface(True))
        self.run_source('one', [])
        with self.assertRaisesRegex(TaskError, 'Required output'):
            self.finish('one')
        with self.assertRaisesRegex(TaskError, 'declared output'):
            signals.put(self.sa, 'source', 'one', signal('undeclared'))

    def test_main_events_complete_and_lost_finish_reply(self):
        task_io.send(self.sa, 'request', 'source', 'input', 'work', {})
        self.run_source('one', [signal()])
        task_io.emit(self.sa, 'intent', 'source', 'one', 'intent', 'external-intent', {'operation': 'example'})
        self.assertIsNotNone(self.sa.snapshot().read('tasks/source/runs/one/io/events/intent.json'))
        task_io.complete(self.sa, 'complete', 'source', 'one', 'input', {'status': 'succeeded'})
        self.assertFalse(task_io.pending(self.sb, 'source')['messages'])
        original = self.sa.push
        lost = [False]
        def push(ref, candidate, expected, **kwargs):
            result = original(ref, candidate, expected, **kwargs)
            if ref == self.sa.ref and not lost[0]:
                lost[0] = True
                raise GitFailure('simulated lost response', {'definite_rejection': False})
            return result
        with patch.object(self.sa, 'push', side_effect=push):
            self.finish('one')
        self.assertEqual(self.finish('one')['status'], 'succeeded')

    def test_intermediate_publication_preserves_pending_signal_until_finish(self):
        signals.subscribe(self.sa, 'sub', 'source', 'viewer', signal_id='data-a')
        work = self.run_source('one', [signal()])
        self.output(work, 'tasks/source/data/report.txt')
        head = self.git(work, 'rev-parse', 'HEAD')
        runtime.publish(self.sa, 'checkpoint', 'source', 'one')
        self.assertEqual(self.git(work, 'rev-parse', 'HEAD'), head)
        self.assertEqual(signals.catalog(self.sa.snapshot(), 'source'), {})
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        signals.put(self.sa, 'source', 'one', signal(value=2))
        self.finish('one')
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 1)
        self.assertEqual(task_io.pending(self.sb, 'viewer')['messages'][0]['payload']['signal']['value'], dict(count=2))

    def test_initial_policy_subscription_identity_and_failed_runs(self):
        self.run_source('one', [signal()]); self.finish('one')
        route = signals.subscribe(self.sa, 'next', 'source', 'viewer', signal_id='data-a')
        signals.subscribe(self.sa, 'current', 'source', 'observer', signal_id='data-a', initial='current')
        signals.sync(self.sa)
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        self.assertEqual(len(task_io.pending(self.sb, 'observer')['messages']), 1)
        with self.assertRaisesRegex(TaskError, 'another request'):
            signals.subscribe(self.sa, 'next', 'source', 'viewer', signal_id='data-a', on='run')
        self.run_source('failed', [signal(value=2)])
        runtime.publish(self.sa, 'fail', 'source', 'failed', outcome='failed')
        self.assertEqual(signals.catalog(self.sa.snapshot(), 'source')['data-a']['value']['revision'], 1)
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        self.run_source('two', [signal(value=2)]); self.finish('two')
        same = signals.subscribe(self.sa, 'same', 'source', 'viewer', signal_id='data-a')
        self.assertEqual(route['after_revision'], same['after_revision'])
        first = next(m for m in task_io.pending(self.sb, 'observer')['messages'] if m['payload']['revision'] == 1)
        frozen = signals.read_notice(self.sb, first)['signal']
        self.assertEqual(frozen, signal())
        self.assertNotIn('path', signals.read_notice(self.sb, first))

    def test_concurrent_main_change_uses_normal_content_merge_and_conflicts_retain_run(self):
        name = 'tasks/source/data/shared.txt'
        self.sa.transact('seed-file', dict(kind='fixture'), lambda s: ({name: b'a\nb\nc\n'}, {}))
        work = self.run_source('one', [])
        self.output(work, name, b'a\nb\nworker\n')
        self.sa.transact('other-writer', dict(kind='fixture'), lambda s: ({name: b'main\nb\nc\n'}, {}))
        self.finish('one')
        self.assertEqual(self.sb.snapshot().read(name), b'main\nb\nworker\n')
        work = self.run_source('two', [])
        self.output(work, name, b'conflicting worker\n')
        self.sa.transact('other-conflict', dict(kind='fixture'), lambda s: ({name: b'conflicting main\n'}, {}))
        before = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, 'content conflict'):
            self.finish('two')
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(runtime.check(self.sa, 'source', 'two')['status'], 'running')

    def test_schema_validation_and_notification_conflict_prevent_finish(self):
        contract = interface()
        contract['signals']['writes'][0]['value_schema'] = 'schemas/data.json'
        runtime.edit_task(self.sa, 'schema', 'source', 1, interface=contract)
        self.sa.transact('schema-file', dict(kind='fixture'), lambda s: ({
            'tasks/source/schemas/data.json': encode(dict(type='object', required=['count'],
                properties=dict(count=dict(type='integer')), additionalProperties=False))}, {}))
        self.run_source('one', [])
        with self.assertRaisesRegex(TaskError, 'declared type'):
            signals.put(self.sa, 'source', 'one', dict(signal(), value=dict(count='invalid')))
        signals.put(self.sa, 'source', 'one', signal())
        route = signals.subscribe(self.sa, 'sub', 'source', 'viewer', signal_id='data-a')
        mid = signals.notice_id(dict(task_id='source', signal_id='data-a', subscription_id=route['id'],
                                    trigger='change', revision=1))
        task_io.send(self.sa, 'collision', 'viewer', mid, 'work', {})
        before = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, 'different content'):
            self.finish('one')
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(signals.catalog(self.sb.snapshot(), 'source'), {})

    def test_packaged_cli_validates_declared_values_and_delivers_at_finish(self):
        declared = interface()
        declared['signals']['writes'][0]['value_schema'] = 'schemas/value.json'
        runtime.edit_task(self.sa, 'declare-schema', 'source', 1, interface=declared)
        self.sa.transact('schema-file', dict(kind='fixture'), lambda s: ({
            'tasks/source/schemas/value.json': encode(dict(type='object', required=['count']))}, {}))
        signals.subscribe(self.sa, 'sub', 'source', 'viewer', signal_id='data-a')
        self.run_source('one', [])
        package = self.base / 'task-inbox.pyz'
        self.command(sys.executable, str(ROOT / 'bin/build-task-inbox'), '--output', str(package))
        value = self.base / 'signal.json'; value.write_bytes(encode(signal()))
        common = (sys.executable, str(package), '--repo', str(self.a))
        self.command(*common, 'signal', 'put', '--task', 'source', '--run-id', 'one', '--file', str(value))
        self.assertFalse(task_io.pending(self.sb, 'viewer')['messages'])
        self.command(*common, '--operation-id', 'packaged-finish', 'run', 'finish', '--task', 'source',
                     '--run-id', 'one', '--outcome', 'succeeded')
        self.assertEqual(len(task_io.pending(self.sb, 'viewer')['messages']), 1)


    def test_executor_owned_dirty_files_and_index_survive_finish(self):
        self.start('one', task='source')
        run = runtime.check(self.sa, 'source', 'one')
        work = self.base / 'executor-workspace'
        self.git(self.a, 'worktree', 'add', '--detach', str(work), run['definition_commit'])
        self.output(work, 'unrelated.txt', b'preserve me\n')
        self.git(work, 'add', 'unrelated.txt')
        self.output(work, 'another.txt', b'untracked\n')
        runtime.prepare(self.sa, 'source', 'one', work, mode='workspace')
        self.output(work, 'tasks/source/data/output.txt', b'accepted\n')
        self.git(work, 'add', 'tasks/source/data/output.txt')
        self.git(work, 'commit', '-m', 'Explicit task commit', '--only', '--', 'tasks/source/data/output.txt')
        self.output(work, 'tasks/source/data/later.txt', b'kept dirty until finish\n')
        head = self.git(work, 'rev-parse', 'HEAD')
        index = self.git(work, 'diff', '--cached')
        self.finish('one')
        self.assertEqual(self.git(work, 'rev-parse', 'HEAD'), head)
        self.assertEqual(self.git(work, 'diff', '--cached'), index)
        self.assertEqual((work / 'unrelated.txt').read_bytes(), b'preserve me\n')
        self.assertEqual((work / 'another.txt').read_bytes(), b'untracked\n')
        self.assertIsNone(self.sb.snapshot().read('unrelated.txt'))
        self.assertEqual(self.sb.snapshot().read('tasks/source/data/output.txt'), b'accepted\n')



class InboxMigrationTest(RuntimeCase):
    def test_existing_messages_receipts_and_busy_fence(self):
        self.add()
        task_io.send(self.sa, 'request', 't', 'one', 'work', {})
        self.start('old')
        with self.assertRaises(TaskError):
            main_io.migrate(self.sa, 'blocked', drained=True)
        runtime.publish(self.sa, 'cancel', 't', 'old', outcome='cancelled', discard_output=True)
        main_io.migrate(self.sa, 'migrate', drained=True)
        self.assertEqual([m['id'] for m in task_io.pending(self.sb, 't')['messages']], ['one'])
        self.assertTrue(main_io.enabled(self.sa.snapshot()))
        with self.assertRaises(TaskError):
            main_io.migrate(self.sa, 'no-confirmation')

    def test_cutover_installs_exact_routes_in_same_commit(self):
        self.add('a', interface=interface()); self.add('b', interface=interface())
        result = main_io.migrate(self.sa, 'cutover', drained=True, subscriptions=[
            dict(task_id='a', consumer='b', signal_id='data-a', on='run')])
        snap = self.sa.snapshot()
        self.assertTrue(main_io.enabled(snap))
        self.assertEqual(snap.json(signals.SUBSCRIPTIONS)['subscriptions'][0]['on'], 'run')
        self.assertEqual(main_io.migrate(self.sb, 'cutover', drained=True, subscriptions=[
            dict(task_id='a', consumer='b', signal_id='data-a', on='run')])['operation_id'], result['operation_id'])
