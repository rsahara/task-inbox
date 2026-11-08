"""Configurable locations preserve ownership, publication and I/O contracts."""
import concurrent.futures
import json
import sys
from pathlib import Path

import task_runtime_test as fixtures
from task_runtime import completion, layout, leases, lifecycle, maintenance, runtime, task_io
from task_runtime.store import CONTROL, Snapshot, Store, TaskError, encode


class DiscoveredLayoutTest(fixtures.RuntimeCase):
    def test_slug_directory_needs_no_index_or_repeated_path(self):
        self.add(path='tasks/t-issue-inbox')
        snapshot = self.sa.snapshot()
        definition = snapshot.json('tasks/t-issue-inbox/task.json')
        self.assertNotIn('task_path', definition)
        self.assertNotIn('data_path', definition)
        self.assertNotIn('layout', snapshot.json(CONTROL + '/config.json'))
        self.assertEqual(layout.tasks(snapshot), {'t': 'tasks/t-issue-inbox'})
        runtime.edit_task(self.sa, 'rename-title', 't', 1, title='New display title')
        task_io.send(self.sb, 'send', 't', 'request', 'work', {})
        wake = task_io.wake(self.sa, 'wake', 't', 'runner', 'r')
        self.assertEqual(wake['action'], 'start')
        work = self.prepare()
        self.output(work, 'tasks/t-issue-inbox/data/result.txt', b'saved')
        runtime.publish(self.sa, 'save', 't', 'r')
        task_io.complete(self.sa, 'complete', 't', 'r', 'request', {'status': 'succeeded'})
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.sa.snapshot().read('tasks/t-issue-inbox/data/result.txt'), b'saved')
        self.assertNotIn('task_path', self.sa.snapshot().json('tasks/t-issue-inbox/task.json'))
        runtime.edit_task(self.sa, 'archive', 't', 2, status='completed', archived=True)
        self.assertEqual(runtime.list_tasks(self.sb)['tasks'], [])
        self.assertEqual(layout.tasks(self.sb.snapshot()), {'t': 'tasks/t-issue-inbox'})

    def test_duplicate_definition_id_fails_closed(self):
        self.add(path='tasks/t-original')
        self.sa.transact('duplicate', {'kind': 'fixture'}, lambda snapshot: (
            {'tasks/t-copy/task.json': snapshot.read('tasks/t-original/task.json')}, {}))
        with self.assertRaisesRegex(TaskError, 'Duplicate task ID'):
            runtime.list_tasks(self.sb)

    def test_unindexed_locations_stay_under_task_root(self):
        for number, location in enumerate(('elsewhere/t', 'tasks/t/nested', 'tasks')):
            with self.assertRaises(TaskError):
                runtime.add_task(self.sa, 'bad-' + str(number), task_id='t', title='Task', path=location)

    def move_task(self, destination, task_id='t'):
        def move(snapshot):
            source = runtime.root(task_id, snapshot)
            changes = {}
            for name in snapshot.entries:
                if name.startswith(source + '/'):
                    changes[destination + name[len(source):]] = snapshot.read(name)
                    changes[name] = None
            return changes, {}
        self.sa.transact('move-' + task_id, {'kind': 'fixture-move', 'destination': destination}, move)

    def test_drained_move_preserves_retired_run_history_and_new_output_location(self):
        self.add()
        self.start('old')
        self.prepare('old')
        runtime.publish(self.sa, 'finish-old', 't', 'old', outcome='succeeded')
        old = runtime.load_run(self.sa.snapshot(), 't', 'old')[2]
        path = 'tasks/t/runs/old/run.json'
        if path in self.sa.snapshot().entries:
            self.sa.transact('retire-old', {'kind': 'fixture-retire'}, lambda snapshot: ({path: None}, {}))
        self.move_task('tasks/t-readable')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', 'old')[2], old)
        with self.assertRaisesRegex(TaskError, 'Run ID already exists'):
            runtime.start(self.sa, 'reuse-old', 't', 'runner', run_id='old')
        new = self.start('new')
        self.assertEqual(new['task_path'], 'tasks/t-readable')
        self.assertEqual(new['data_path'], 'tasks/t-readable/data')

    def test_moved_active_run_cannot_continue(self):
        self.add()
        self.start()
        self.move_task('tasks/t-readable')
        with self.assertRaisesRegex(TaskError, 'Run location changed'):
            runtime.check(self.sb, 't', 'r')

    def finished_io(self):
        self.add()
        task_io.send(self.sa, 'send', 't', 'request', 'work', {})
        self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'save-intent', 'external-intent', {'operation_id': 'original'})
        task_io.complete(self.sa, 'complete', 't', 'r', 'request', {'status': 'succeeded'})
        result = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertFalse(any(p.startswith('tasks/t/runs/r/') for p in self.sa.snapshot().entries))
        return result

    def test_retired_io_events_and_results_survive_directory_move(self):
        finished = self.finished_io()
        self.move_task('tasks/t-readable')
        saved = task_io.IOStore(self.sb, 't', finished).optional()
        self.assertEqual(saved.json('events/save-intent.json')['payload']['operation_id'], 'original')
        self.assertEqual(saved.json('results/request.json')['result']['status'], 'succeeded')
        self.assertEqual(task_io.status(self.sb, 't', 'request')['status'], 'succeeded')

    def test_run_listing_survives_directory_move_without_duplicates(self):
        self.finished_io()
        self.start('second'); self.prepare('second')
        runtime.publish(self.sa, 'finish-second', 't', 'second', outcome='succeeded')
        before = runtime.list_runs(self.sa.snapshot(), 't')
        self.move_task('tasks/t-readable')
        self.assertEqual(runtime.list_runs(self.sb.snapshot(), 't'), before)
        self.start('third')
        self.assertEqual({r['run_id'] for r in runtime.list_runs(self.sa.snapshot(), 't')}, {'r', 'second', 'third'})

    def test_retired_cleanup_is_resolved_after_directory_move(self):
        self.finished_io()
        self.move_task('tasks/t-readable')
        plan = lifecycle.cleanup(self.sb, 'inspect-cleanup', 't', 'r')
        self.assertTrue(plan['dry_run'])
        self.assertEqual(maintenance.retire_metadata(self.sb, 't', 'r')['status'], 'absent')

    def test_retired_legacy_cleanup_reads_its_post_move_completion(self):
        self.add(); self.start(); self.prepare()
        self.legacy_finish(self.sa, 'finish-legacy', 't', 'r', outcome='succeeded')
        before = runtime.run_metadata(self.sa.snapshot(), 't', 'r', 'cleanup.json')
        self.assertEqual(before['status'], 'pending')
        maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        self.move_task('tasks/t-readable')
        result = lifecycle.cleanup(self.sa, 'cleanup-legacy', 't', 'r', apply=True, stopped=True)
        self.assertEqual(result['status'], 'done')
        expected = {key: value for key, value in result.items() if key not in ('committed', 'operation_id')}
        self.assertEqual(runtime.run_metadata(self.sb.snapshot(), 't', 'r', 'cleanup.json'), expected)
        self.assertEqual(result['io_manifest'], before['io_manifest'])

    def test_reused_directory_keeps_retired_legacy_cleanup_owners_separate(self):
        self.add(); self.start(); self.prepare()
        self.legacy_finish(self.sa, 'finish-t', 't', 'r', outcome='succeeded')
        maintenance.retire_metadata(self.sa, 't', 'r', apply=True)
        self.add('u', path='tasks/t-readable')
        runtime.start(self.sa, 'start-u', 'u', 'runner-u', run_id='r')
        runtime.prepare(self.sa, 'u', 'r', self.base / 'work-u')
        self.legacy_finish(self.sa, 'finish-u', 'u', 'r', outcome='succeeded')
        maintenance.retire_metadata(self.sa, 'u', 'r', apply=True)
        self.move_task('tasks/u-readable', task_id='u')
        self.move_task('tasks/t-readable')
        original = lifecycle.cleanup(self.sa, 'cleanup-t', 't', 'r', apply=True, stopped=True)
        expected_original = {key: value for key, value in original.items() if key not in ('committed', 'operation_id')}
        self.assertEqual(runtime.run_metadata(self.sb.snapshot(), 't', 'r', 'cleanup.json'), expected_original)
        self.assertEqual(runtime.run_metadata(self.sb.snapshot(), 'u', 'r', 'cleanup.json')['status'], 'pending')
        replacement = lifecycle.cleanup(self.sa, 'cleanup-u', 'u', 'r', apply=True, stopped=True)
        self.assertNotEqual(original['source_head'], replacement['source_head'])
        expected_replacement = {key: value for key, value in replacement.items() if key not in ('committed', 'operation_id')}
        self.assertEqual(runtime.run_metadata(self.sb.snapshot(), 't', 'r', 'cleanup.json'), expected_original)
        self.assertEqual(runtime.run_metadata(self.sb.snapshot(), 'u', 'r', 'cleanup.json'), expected_replacement)

    def test_unicode_directory_lists_retired_runs(self):
        self.add(path='tasks/t-日本語 name')
        self.start(); self.prepare()
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual([r['run_id'] for r in runtime.list_runs(self.sb.snapshot(), 't')], ['r'])

    def test_move_from_unicode_directory_preserves_retired_records(self):
        self.add(path='tasks/t-日本語 name')
        self.start(); self.prepare()
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        finished = runtime.load_run(self.sa.snapshot(), 't', 'r')[2]
        self.move_task('tasks/t-readable')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', 'r')[2], finished)

    def test_reused_directory_keeps_run_and_stream_ownership(self):
        original = self.finished_io()
        self.move_task('tasks/t-readable')
        self.add('u', path='tasks/t')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', 'r')[2]['task_id'], 't')
        self.assertEqual([r['run_id'] for r in runtime.list_runs(self.sb.snapshot(), 't')], ['r'])
        self.assertEqual(runtime.list_runs(self.sb.snapshot(), 'u'), [])
        with self.assertRaisesRegex(TaskError, 'Run not found'):
            runtime.load_run(self.sb.snapshot(), 'u', 'r')
        replacement = runtime.start(self.sa, 'start-u', 'u', 'runner-u', run_id='r')
        self.assertEqual(task_io.IOStore(self.sb, 'u', replacement).optional().entries, {})
        self.prepare(task='u')
        task_io.emit(self.sa, 'u-event', 'u', 'r', 'own-event', 'event', {'owner': 'u'})
        finished = runtime.publish(self.sa, 'finish-u', 'u', 'r', outcome='succeeded')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 'u', 'r')[2]['task_id'], 'u')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', 'r')[2]['task_id'], 't')
        self.assertIsNone(task_io.IOStore(self.sb, 'u', finished).optional().json('events/save-intent.json'))
        self.assertEqual(task_io.IOStore(self.sb, 't', original).optional().json('events/save-intent.json')['task_id'], 't')

    def test_repeated_directory_reuse_preserves_each_owner(self):
        self.finished_io()
        self.move_task('tasks/t-readable')
        self.add('u', path='tasks/t')
        self.start('second', task='u'); self.prepare('second', task='u')
        runtime.publish(self.sa, 'finish-u', 'u', 'second', outcome='succeeded')
        self.move_task('tasks/u-readable', task_id='u')
        self.add('v', path='tasks/t')
        self.assertEqual([r['run_id'] for r in runtime.list_runs(self.sb.snapshot(), 't')], ['r'])
        self.assertEqual([r['run_id'] for r in runtime.list_runs(self.sb.snapshot(), 'u')], ['second'])
        self.assertEqual(runtime.list_runs(self.sb.snapshot(), 'v'), [])

    def test_merge_parent_includes_runs_retired_after_the_feature_move(self):
        self.finished_io()
        base = self.sa.snapshot()
        changes = {}
        for path in base.entries:
            if path.startswith('tasks/t/'):
                changes['tasks/t-readable/' + path[len('tasks/t/'):]] = base.read(path)
                changes[path] = None
        feature = self.sa.commit(base.commit, changes, 'Move on a feature branch')
        self.start('late'); self.prepare('late')
        runtime.publish(self.sa, 'finish-late', 't', 'late', outcome='succeeded')
        main = self.sa.snapshot()
        combined = {}
        for path in main.entries:
            if path.startswith('tasks/t/'):
                combined['tasks/t-readable/' + path[len('tasks/t/'):]] = main.read(path)
                combined[path] = None
        moved = self.sa.commit(main.commit, combined, 'Combined move tree')
        tree = self.git(self.a, 'rev-parse', moved + '^{tree}')
        merged = self.command('git', '-C', str(self.a), 'commit-tree', tree, '-p', feature, '-p', main.commit,
                              input='Merge task output into moved feature\n').stdout.strip()
        self.sa.push(self.sa.ref, merged, main.commit)
        self.assertEqual({r['run_id'] for r in runtime.list_runs(self.sb.snapshot(), 't')}, {'r', 'late'})

    def test_shallow_snapshot_requires_complete_history_before_directory_lookup(self):
        self.finished_io()
        self.move_task('tasks/t-readable')
        cold = self.base / 'shallow-moved'
        self.command('git', 'clone', '--depth=1', '--branch=main', self.remote.as_uri(), str(cold))
        store = Store(cold)
        snapshot = Snapshot(store, self.git(cold, 'rev-parse', 'HEAD'))
        with self.assertRaisesRegex(TaskError, 'Complete authority history'):
            runtime.list_runs(snapshot, 't')
        self.assertEqual([r['run_id'] for r in runtime.list_runs(store.snapshot(), 't')], ['r'])
        self.assertEqual(self.git(cold, 'rev-parse', '--is-shallow-repository'), 'false')


class LayoutTest(fixtures.RuntimeCase):
    init_options = dict(index='registry/task-list.json', task_root='work/items')

    def test_index_and_paths_are_independent_of_archive_state(self):
        self.add('one')
        self.add('two', path='elsewhere/active/custom', data_path='outputs/custom')
        self.add('three', path='completed/anywhere')
        snapshot = self.sb.snapshot()
        self.assertEqual(layout.tasks(snapshot), {'one': 'work/items/one',
                         'two': 'elsewhere/active/custom', 'three': 'completed/anywhere'})
        runtime.edit_task(self.sb, 'archive', 'two', 1, status='completed', archived=True)
        self.assertEqual([t['id'] for t in runtime.list_tasks(self.sa)['tasks']], ['one', 'three'])
        archived = self.sa.snapshot()
        self.assertEqual(layout.tasks(snapshot), layout.tasks(archived))
        self.assertEqual(archived.read('elsewhere/active/custom/task.md'), b'Instructions')
        runtime.edit_task(self.sa, 'reopen', 'two', 2, status='open')
        self.assertEqual(len(runtime.list_tasks(self.sb)['tasks']), 3)
        with self.assertRaises(TaskError):
            runtime.initialize(self.sa, 'relayout', index='other.json')
        self.assertEqual(runtime.initialize(self.sb, 'initialize', **self.init_options)['schema_version'], 5)

    def test_custom_locations_support_run_io_completion_and_cleanup(self):
        self.add('t', path='operations/live/job', data_path='reports/job')
        task_io.send(self.sb, 'send', 't', 'request', 'work', {'input': 1})
        wake = task_io.wake(self.sa, 'wake', 't', 'runner', 'r')
        self.assertEqual(wake['action'], 'start')
        work = self.prepare()
        runtime.heartbeat(self.sa, 't', 'r', progress='working')
        lifecycle.execute(self.sa, 't', 'r', [sys.executable, '-c', 'print("done")'])
        self.output(work, 'reports/job/result.txt', b'computed')
        self.output(work, 'operations/live/job/runs/r/artifacts/proof.json', b'{}')
        runtime.publish(self.sa, 'save', 't', 'r')
        task_io.complete(self.sa, 'complete', 't', 'r', 'request', {'status': 'succeeded'})
        self.assertEqual(task_io.pending(self.sb, 't')['messages'], [])
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(task_io.status(self.sb, 't', 'request')['status'], 'succeeded')
        snapshot = self.sb.snapshot()
        self.assertEqual(snapshot.read('reports/job/result.txt'), b'computed')
        self.assertIsNone(snapshot.read('operations/live/job/runs/r/result.json'))
        self.assertEqual(snapshot.metadata('operations/live/job/runs/r/run.json')['status'], 'succeeded')
        self.assertEqual(snapshot.read('operations/live/job/runs/r/artifacts/proof.json'), b'{}')
        self.assertFalse(any(p.startswith('tasks/') for p in snapshot.entries))
        lifecycle.cleanup(self.sa, 'cleanup', 't', 'r', apply=True)
        self.assertFalse(work.exists())

    def test_concurrent_adds_keep_both_index_entries(self):
        def add(store, name):
            return runtime.add_task(store, 'add-' + name, task_id=name, title=name, path='custom/' + name)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            a = pool.submit(add, self.sa, 'a'); b = pool.submit(add, self.sb, 'b')
            a.result(); b.result()
        self.assertEqual(set(layout.tasks(self.sa.snapshot())), {'a', 'b'})

    def test_git_pathspec_syntax_in_locations_is_literal(self):
        for number, (location, data) in enumerate([
                ('custom/one', ':(exclude)outputs'),
                (':(exclude)job', 'reports/[literal]*'),
                ('custom/[job]*', 'reports/three')]):
            task = 'literal-' + str(number)
            with self.subTest(location=location, data=data):
                self.add(task, path=location, data_path=data)
                self.start(run=task, task=task)
                work = self.prepare(run=task, task=task)
                output = data + '/result.txt'
                artifact = location + '/runs/' + task + '/artifacts/proof.txt'
                self.output(work, output, b'saved output')
                self.output(work, artifact, b'saved artifact')
                runtime.publish(self.sa, 'finish-' + task, task, task, outcome='succeeded')
                snapshot = self.sb.snapshot()
                self.assertEqual(snapshot.read(output), b'saved output')
                self.assertEqual(snapshot.read(artifact), b'saved artifact')

    def test_directory_data_index_and_metadata_overlap_are_rejected(self):
        self.add('one', path='custom/one', data_path='out/one')
        for n, location in enumerate(['../escape', '/absolute', '.git/objects', '.task-system/x',
                         'registry', 'registry/task-list.json/child', 'custom',
                         'custom/one/nested', 'out/one/nested']):
            with self.subTest(location=location), self.assertRaises(TaskError):
                runtime.add_task(self.sa, 'bad-location-' + str(n), task_id='bad', title='bad', path=location)
        for n, data in enumerate(['custom/one', 'out/one', 'registry', '.task-system/operations',
                     'work/items/bad/task.json', 'work/items/bad/runs']):
            with self.subTest(data=data), self.assertRaises(TaskError):
                runtime.add_task(self.sa, 'bad-data-' + str(n), task_id='bad', title='bad', data_path=data)
        self.assertEqual(set(layout.tasks(self.sb.snapshot())), {'one'})

    def test_run_output_cannot_write_another_definition_or_index(self):
        self.add(); self.start(); work = self.prepare()
        self.output(work, 'registry/task-list.json', b'{}')
        with self.assertRaisesRegex(TaskError, 'boundary'):
            runtime.publish(self.sa, 'escape', 't', 'r')
        self.assertEqual(set(layout.tasks(self.sb.snapshot())), {'t'})

    def test_recovery_and_completion_use_indexed_run_paths(self):
        self.add(); self.start(completion_contract={'required': ['git'], 'context': {}})
        run = runtime.check(self.sa, 't', 'r')
        proof = completion.receipt(run, {'git': {'status': 'failed', 'operation_id': 'git-failed',
                                                'evidence': {'reason': 'confirmed failure'}}}, stopped=True)
        completion.record(self.sa, 'record', 't', 'r', proof)
        runtime.recover(self.sb, 'recover', 't', 'r', stopped=True, settled=True, evidence='fixture:stopped')
        self.assertEqual(runtime.load_run(self.sa.snapshot(), 't', 'r')[2]['status'], 'lost')
        self.assertIsNotNone(self.sa.snapshot().read('work/items/t/runs/r/result.json'))

    def test_policy_drain_checks_tasks_in_all_indexed_directories(self):
        self.add('a', path='one/a'); self.add('b', path='two/b'); self.start(task='b')
        with self.assertRaisesRegex(TaskError, 'Drain all'):
            leases.configure(self.sa, 'policy', 'a', 1, mode='manual', drained=True)

    def test_cli_show_and_distribution_use_custom_layout(self):
        command = [sys.executable, str(fixtures.ROOT / 'bin/task-inbox'), '--repo', str(self.b)]
        result = self.command(*command, '--operation-id', 'cli-add', 'task', 'add', '--id', 'cli',
                              '--title', 'CLI', '--path', 'shared/cli')
        self.assertEqual(json.loads(result.stdout)['task_path'], 'shared/cli')
        result = self.command(*command, 'task', 'show', '--id', 'cli')
        self.assertEqual(json.loads(result.stdout)['body'], '')
        bundle = self.base / 'runtime.pyz'
        self.command(sys.executable, str(fixtures.ROOT / 'bin/build-task-inbox'), '--output', str(bundle))
        result = self.command(sys.executable, str(bundle), '--repo', str(self.b), 'task', 'list')
        self.assertEqual(json.loads(result.stdout)['tasks'][0]['id'], 'cli')
