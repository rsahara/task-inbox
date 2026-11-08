"""Storage invariants, local durability, and transactions against real Git objects."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from task_runtime_test import RuntimeCase
from task_runtime import layout, maintenance, runtime
from task_runtime.store import (CONTROL, GitFailure, Snapshot, Store, TaskError,
                                atomic_json, digest, encode, identity, local_lock,
                                refs_overlap, safe_path)


class StorageValidationTest(unittest.TestCase):
    def test_tracking_refspec_matches_are_literal_outside_one_wildcard(self):
        for value, pattern, expected in [
                ('refs/heads/main', 'refs/heads/main', ''),
                ('refs/heads/other', 'refs/heads/main', None),
                ('refs/cache/a/work', 'refs/cache/*/work', 'a'),
                ('refs/cache/a/work', 'refs/cache/*', 'a/work'),
                ('refs/cache/a/state', 'refs/cache/*/work', None),
                ('refs/other/a/work', 'refs/cache/*/work', None),
                ('abc', 'ab*bc', None), ('anything', '*/*', None)]:
            with self.subTest(value=value, pattern=pattern):
                self.assertEqual(maintenance.ref_match(value, pattern), expected)

    def test_identifiers_and_paths_enforce_portable_boundaries(self):
        for value in ('a', '0', 'a_b-c', 'x' * 96):
            self.assertEqual(identity(value), value)
        for value in (None, 1, '', 'A', '-a', 'a.b', 'a/b', 'a\n', 'x' * 97):
            with self.subTest(identifier=value), self.assertRaises(TaskError):
                identity(value)
        for value in ('tasks/a', 'space name/file', '日本語/結果', 'literal/[a]*'):
            self.assertEqual(safe_path(value), value)
        for value in ('', './a', '../a', 'a/../b', '/a', 'a//b', 'a/',
                      '.git/config', 'a/.git/b', 'a\\b', 'a\nb', 'a\x00b'):
            with self.subTest(path=value), self.assertRaises(TaskError):
                safe_path(value)
        self.assertTrue(refs_overlap('a', 'a/b'))
        self.assertTrue(refs_overlap('a/b', 'a'))
        self.assertTrue(refs_overlap('a', 'a'))
        self.assertFalse(refs_overlap('a', 'ab'))

    def test_encoding_and_digest_are_order_independent_and_preserve_unicode(self):
        value = {'z': '日本語', 'a': [1, True, None]}
        self.assertEqual(encode(value), encode(dict(reversed(list(value.items())))))
        self.assertTrue(encode(value).endswith(b'\n'))
        self.assertIn('日本語'.encode(), encode(value))
        self.assertEqual(json.loads(encode(value)), value)
        self.assertEqual(digest(value), digest(json.loads(encode(value))))
        self.assertNotEqual(digest(value), digest(dict(value, z='changed')))

    def test_atomic_write_failure_preserves_previous_value_and_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'nested/state.json'
            atomic_json(path, {'version': 1})
            with patch('task_runtime.store.os.replace', side_effect=OSError('fixture failure')):
                with self.assertRaises(OSError):
                    atomic_json(path, {'version': 2})
            self.assertEqual(json.loads(path.read_text()), {'version': 1})
            self.assertEqual(list(path.parent.iterdir()), [path])
            atomic_json(path, {'version': 3})
            self.assertEqual(json.loads(path.read_text()), {'version': 3})

    def test_lock_timeout_and_exception_release(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'operation.lock'
            with local_lock(path):
                with self.assertRaisesRegex(TaskError, 'busy'):
                    with local_lock(path, timeout=0):
                        self.fail('exclusive lock was acquired twice')
            with self.assertRaisesRegex(RuntimeError, 'fixture'):
                with local_lock(path):
                    raise RuntimeError('fixture')
            with local_lock(path, timeout=0):
                pass
            with local_lock(path, shared=True), local_lock(path, shared=True):
                with self.assertRaises(TaskError):
                    with local_lock(path, timeout=0):
                        self.fail('exclusive lock ignored shared readers')


class StoreTest(RuntimeCase):
    def snapshot_with(self, changes):
        parent = self.sa.fetch()
        return Snapshot(self.sa, self.sa.commit(parent, changes, 'fixture snapshot'))

    def test_snapshot_defaults_binary_content_and_regular_file_requirement(self):
        data = b'\x00\xff\r\n'
        blob = self.sa.blob(data)
        snapshot = self.snapshot_with({'binary': data, 'executable': ('100755', 'blob', blob[2]),
                                       'link': ('120000', 'blob', self.sa.blob(b'binary')[2])})
        self.assertEqual(snapshot.read('binary'), data)
        self.assertEqual(snapshot.read('executable'), data)
        self.assertEqual(snapshot.read('missing', b'fallback'), b'fallback')
        self.assertEqual(snapshot.json('missing', {'default': True}), {'default': True})
        with self.assertRaisesRegex(TaskError, 'regular file'):
            snapshot.read('link')
        empty = Snapshot(self.sa, None)
        self.assertEqual(empty.entries, {})
        self.assertEqual(empty.content_hash('binary'), digest({}))
        self.assertNotEqual(snapshot.content_hash('binary'), empty.content_hash('binary'))

    def test_tree_rejects_collisions_without_discarding_existing_files(self):
        first = self.sa.commit(None, {'dir/one': b'one', 'dir/two': b'two', 'file': b'file'}, 'fixture')
        for changes, error in [({'new': b'file', 'new/child': b'child'}, 'collision'),
                               ({'dir': b'replacement', 'dir/one': None}, 'undeleted files'),
                               ({'file/child': b'child'}, 'not a directory')]:
            with self.subTest(changes=list(changes)), self.assertRaisesRegex(TaskError, error):
                self.sa.commit(first, changes, 'invalid')
        replaced = self.sa.commit(first, {'dir': b'replacement', 'dir/one': None, 'dir/two': None}, 'replace')
        snapshot = Snapshot(self.sa, replaced)
        self.assertEqual(snapshot.read('dir'), b'replacement')
        self.assertNotIn('dir/one', snapshot.entries)
        deleted = self.sa.commit(first, {'dir/one': None, 'dir/two': None, 'missing/child': None}, 'delete')
        self.assertEqual(set(Snapshot(self.sa, deleted).entries), {'file'})
        self.assertEqual(Snapshot(self.sa, first).read('dir/two'), b'two')

    def test_push_rejects_history_replacement_and_invalid_remote_names(self):
        authority = self.sa.fetch()
        unrelated = self.sa.commit(None, {'other': b'value'}, 'unrelated history')
        with self.assertRaisesRegex(TaskError, 'History replacement'):
            self.sa.push(self.sa.ref, unrelated, authority)
        self.assertEqual(self.sb.fetch(), authority)
        self.assertFalse(self.sa.contains(None, authority))
        for remote in ('-origin', 'https://example.invalid/repo', 'origin/name', ''):
            with self.subTest(remote=remote), self.assertRaises(TaskError):
                Store(self.a, remote=remote)

    def test_configuration_rejects_missing_protocols_and_invalid_namespaces(self):
        config = self.sa.snapshot().json(CONTROL + '/config.json')
        invalid = [None, {}, dict(config, schema_version=1), dict(config, authority_ref='refs/heads/other'),
                   dict(config, schema_version=3), dict(config, schema_version=4, lease_protocol=1),
                   dict(config, schema_version=8), dict(config, schema_version=9), dict(config, schema_version=10),
                   dict(config, ref_prefix=''), dict(config, ref_prefix=42)]
        for number, value in enumerate(invalid):
            with self.subTest(case=number), self.assertRaises(TaskError):
                self.sa.validate_config(value)
        for version, extra in [(2, {}), (3, {'lease_protocol': 1}),
                               (4, {'lease_protocol': 1, 'generation_timeout_protocol': 1}),
                               (8, {'workspace_provenance_protocol': 1}), (9, {'workspace_provenance_protocol': 1})]:
            self.sa.validate_config(dict(config, schema_version=version, **extra))

    def test_remote_receipt_rejects_changed_request_without_local_journal(self):
        self.add()
        before = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, 'another request'):
            runtime.add_task(self.sb, 'add-t', task_id='t', title='Changed')
        self.assertEqual(self.sa.fetch(), before)

    def test_definite_push_rejections_exhaust_bounded_retries_without_retaining_candidate(self):
        before = self.sa.fetch()
        error = GitFailure('fixture rejection', {'definite_rejection': True})
        with patch.object(self.sa, 'push', side_effect=error) as push, patch('task_runtime.store.time.sleep'):
            with self.assertRaisesRegex(TaskError, 'retry limit'):
                self.add()
        self.assertEqual(push.call_count, 8)
        self.assertEqual(self.sb.fetch(), before)
        journal = json.loads((self.sa.local / 'operations/add-t.json').read_text())
        self.assertNotIn('pending', journal)
        self.assertEqual(self.git(self.a, 'for-each-ref', '--format=%(refname)', 'refs/task-runtime-pending/'), '')
        self.assertEqual(self.add()['id'], 't')

    def test_readback_failure_retains_original_candidate_for_retry(self):
        before = self.sa.fetch()
        with patch.object(self.sa, 'push', return_value=''):
            with self.assertRaisesRegex(TaskError, 'readback failed'):
                self.add()
        self.assertEqual(self.sb.fetch(), before)
        journal = json.loads((self.sa.local / 'operations/add-t.json').read_text())
        candidate = journal['pending']['candidate']
        self.assertEqual(self.add()['id'], 't')
        self.assertEqual(self.sb.fetch(), candidate)


class IndexedSnapshotTest(RuntimeCase):
    init_options = dict(index='registry/tasks.json', task_root='work')

    def test_malformed_index_and_overlapping_directories_fail_closed(self):
        invalid = [None, [], {}, {'schema_version': 2, 'tasks': {}}, {'schema_version': 1, 'tasks': []},
                   {'schema_version': 1, 'tasks': {'Bad': 'work/a'}},
                   {'schema_version': 1, 'tasks': {'a': 'registry'}},
                   {'schema_version': 1, 'tasks': {'a': 'work/a', 'b': 'work/a'}},
                   {'schema_version': 1, 'tasks': {'a': 'work/a', 'b': 'work/a/nested'}}]
        parent = self.sa.fetch()
        for number, value in enumerate(invalid):
            commit = self.sa.commit(parent, {'registry/tasks.json': encode(value)}, 'invalid index')
            with self.subTest(case=number), self.assertRaises(TaskError):
                layout.tasks(Snapshot(self.sa, commit))
        self.assertEqual(layout.tasks(self.sa.snapshot()), {})

    def test_missing_index_entry_and_mismatched_pinned_locations_are_rejected(self):
        self.add()
        self.start()
        parent = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, 'absent from the index'):
            runtime.load_task(Snapshot(self.sa, parent), 'missing')
        task = self.sa.snapshot().json('work/t/task.json')
        run = self.sa.snapshot().json('work/t/runs/r/run.json')
        for location, value, error in [('work/t/task.json', dict(task, task_path='elsewhere'), 'indexed location'),
                                       ('work/t/runs/r/run.json', dict(run, task_path='elsewhere'), 'Run location changed')]:
            commit = self.sa.commit(parent, {location: encode(value)}, 'mismatched location')
            with self.subTest(location=location), self.assertRaisesRegex(TaskError, error):
                runtime.load_run(Snapshot(self.sa, commit), 't', 'r')

    def test_registration_preserves_unowned_existing_files_and_index_data(self):
        parent = self.sa.fetch()
        candidate = self.sa.commit(parent, {'occupied/file': b'keep'}, 'unowned data')
        self.sa.push(self.sa.ref, candidate, parent)
        with self.assertRaisesRegex(TaskError, 'existing repository files'):
            runtime.add_task(self.sa, 'occupied', task_id='t', title='Task', path='occupied')
        with self.assertRaisesRegex(TaskError, 'data overlaps index'):
            runtime.add_task(self.sa, 'index-data', task_id='t', title='Task', data_path='registry')
        self.assertEqual(self.sa.snapshot().read('occupied/file'), b'keep')
        self.assertEqual(layout.tasks(self.sb.snapshot()), {})


class LayoutValidationTest(unittest.TestCase):
    def test_settings_reject_schema_mismatch_reserved_paths_and_index_file_ancestors(self):
        for value in [None, 1, '.GIT/config', '.task-system', '.task-system/x']:
            with self.subTest(path=value), self.assertRaises(TaskError):
                layout.path(value)
        options = {'index': 'registry/tasks.json', 'task_root': 'work'}
        invalid = [{'schema_version': 5}, {'schema_version': 2, 'layout': options},
                   {'schema_version': 5, 'layout': []},
                   {'schema_version': 5, 'layout': dict(options, extra=True)},
                   {'schema_version': 5, 'layout': dict(options, task_root='registry/tasks.json')},
                   {'schema_version': 5, 'layout': dict(options, task_root='registry/tasks.json/child')}]
        for config in invalid:
            with self.subTest(config=config), self.assertRaises(TaskError):
                layout.settings(config)
        self.assertIsNone(layout.settings({'schema_version': 2}))
        self.assertEqual(layout.settings({'schema_version': 5, 'layout': options}), options)
        self.assertEqual(layout.settings({'schema_version': 6, 'layout': options}), options)
        self.assertIsNone(layout.settings({'schema_version': 6}))
