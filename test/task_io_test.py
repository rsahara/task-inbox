"""Real Git, independent clones and failure races for task messaging."""
import concurrent.futures
import unittest
from unittest.mock import patch

import task_runtime_test as fixtures
from task_runtime import runtime, task_io
from task_runtime.store import GitFailure, Store, TaskError, encode


class TaskIOTest(fixtures.RuntimeCase):
    def setup_io(self):
        self.add()

    def send(self, key='one', store=None):
        return task_io.send(store or self.sa, 'send-' + key, 't', key, 'example', {'value': key})

    def test_authority_named_task_io_migrates_and_preserves_delivery_across_clones(self):
        store = Store(self.a, branch='task-io')
        reader = Store(self.b, branch='task-io')
        runtime.initialize(store, 'other-authority')
        runtime.add_task(store, 'other-task', task_id='t', title='Task')
        snapshot = store.snapshot()
        config = dict(snapshot.json('.task-system/config.json'), schema_version=7, workspace_provenance_protocol=1)
        candidate = store.commit(snapshot.commit, {'.task-system/config.json': encode(config)}, 'Previous authority')
        store.push(store.ref, candidate, snapshot.commit)
        sent = task_io.send(store, 'send', 't', 'one', 'work', {})
        self.assertEqual(sent['ref'], 'refs/heads/task-io-2/t/inbox')
        run = runtime.start(store, 'start', 't', 'runner', run_id='r')
        config = reader.snapshot().json('.task-system/config.json')
        self.assertEqual((config['schema_version'], config['io_ref_prefix']), (9, 'task-io-2'))
        self.assertEqual(run['io_ref'], 'refs/heads/task-io-2/t/runs/r')
        work = self.base / 'other-work'
        runtime.prepare(store, 't', 'r', work)
        runtime.heartbeat(reader, 't', 'r')
        task_io.complete(reader, 'complete', 't', 'r', 'one', {'status': 'succeeded'})
        runtime.publish(store, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(task_io.pending(reader, 't')['messages'], [])
        self.assertEqual(task_io.status(reader, 't', 'one')['status'], 'succeeded')
        self.assertIsNone(reader.remote_tip(run['heartbeat_ref']))
        self.assertEqual(reader.snapshot().json('.task-system/config.json')['authority_ref'], store.ref)

    def test_upgrade_keeps_existing_default_inbox_and_pending_messages(self):
        self.add(); sent = self.send(); run = self.start()
        self.assertEqual(self.sb.snapshot().json('.task-system/config.json')['io_ref_prefix'], 'task-io')
        self.assertEqual(runtime.check(self.sb, 't', 'r')['io_ref'], run['io_ref'])
        self.assertEqual(sent['ref'], 'refs/heads/task-io/t/inbox')
        runtime.heartbeat(self.sb, 't', 'r')
        self.assertEqual([m['id'] for m in task_io.pending(self.sb, 't')['messages']], ['one'])

    def test_every_task_uses_unified_io_without_an_enable_flag(self):
        for task_id, flag in [('plain', None), ('disabled', 0), ('enabled', 1)]:
            with self.subTest(task=task_id):
                self.add(task_id)
                if flag is not None:
                    snapshot = self.sa.snapshot()
                    task = snapshot.json('tasks/' + task_id + '/task.json')
                    task['io_protocol'] = flag
                    self.commit_changes(self.sa, {'tasks/' + task_id + '/task.json': encode(task)})
                run = self.start(task_id, task=task_id)
                self.assertEqual(run['io_ref'], 'refs/heads/task-io/' + task_id + '/runs/' + task_id)
                self.assertEqual(run['heartbeat_ref'], run['io_ref'])
                self.assertEqual(runtime.heartbeat(self.sa, task_id, task_id)['sequence'], 1)
                self.assertIsNone(self.sb.remote_tip('refs/heads/task-runtime/' + task_id + '/' + task_id + '/state'))
        self.assertEqual(self.sb.snapshot().json('.task-system/config.json')['schema_version'], 9)

    def test_old_live_run_cannot_execute_or_write_a_heartbeat_with_the_new_runtime(self):
        self.add(); run = self.start()
        run.pop('io_protocol'); run.pop('io_ref')
        run['heartbeat_ref'] = 'refs/heads/task-runtime/t/r/state'
        self.commit_changes(self.sa, {'tasks/t/runs/r/run.json': encode(run)})
        before = self.sa.fetch()
        for invoke in [lambda: runtime.check(self.sa, 't', 'r'),
                       lambda: runtime.heartbeat(self.sa, 't', 'r'),
                       lambda: runtime.prepare(self.sa, 't', 'r', self.base / 'legacy-work'),
                       lambda: task_io.emit(self.sa, 'event', 't', 'r', 'event', 'progress', {})]:
            with self.assertRaisesRegex(TaskError, 'original runtime'):
                invoke()
        self.assertEqual(self.sb.fetch(), before)
        self.assertIsNone(self.sb.remote_tip(run['heartbeat_ref']))
        self.assertFalse((self.base / 'legacy-work').exists())

    def test_concurrent_senders_keep_both_requests_without_a_run(self):
        self.setup_io()
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            sends = [(key, store, pool.submit(self.send, key, store))
                     for key, store in [('one', self.sa), ('two', self.sb)]]
        # A cold inbox can have an ambiguous initialization CAS. Once both
        # attempts settle, retry that outcome with the original operation ID.
        for key, store, future in sends:
            try:
                future.result()
            except TaskError as error:
                self.assertEqual(str(error), 'Push outcome is unknown; retry operation io-initialize')
                self.send(key, store)
        self.assertEqual([m['id'] for m in task_io.pending(self.sb, 't')['messages']], ['one', 'two'])
        self.assertEqual(runtime.load_task(self.sa.snapshot(), 't')[1]['slots'], {})

    def test_duplicate_is_idempotent_but_content_conflicts_are_rejected(self):
        self.setup_io(); self.send()
        self.send(store=self.sb)
        with self.assertRaises(TaskError):
            task_io.send(self.sb, 'conflict', 't', 'one', 'example', {'value': 'changed'})
        self.assertEqual(len(task_io.pending(self.sa, 't')['messages']), 1)

    def test_paused_task_receives_without_starting(self):
        self.setup_io()
        runtime.edit_task(self.sa, 'pause', 't', 1, status='paused')
        self.send()
        self.assertEqual(task_io.wake(self.sb, 'wake', 't', 'runner')['action'], 'paused')

    def test_empty_inbox_can_admit_explicit_maintenance_under_the_same_slot(self):
        self.setup_io()
        self.assertEqual(task_io.wake(self.sa, 'idle', 't', 'runner')['action'], 'idle')
        self.assertEqual(task_io.wake(self.sa, 'maintenance', 't', 'runner', force=True)['action'], 'start')

    def test_parallel_run_streams_are_independent_but_inbox_wake_requires_one_consumer(self):
        self.add(max_runners=2)
        self.start('one'); self.start('two', store=self.sb)
        runtime.heartbeat(self.sa, 't', 'one', progress='first')
        runtime.heartbeat(self.sb, 't', 'two', progress='second')
        with self.assertRaises(TaskError):
            task_io.wake(self.sa, 'wake', 't', 'consumer', force=True)
        self.assertEqual(runtime.observe(self.sa, 't', 'one')['run_id'], 'one')

    def test_wake_reuses_normal_single_runner_capacity(self):
        self.setup_io(); self.send()
        first = task_io.wake(self.sa, 'wake-one', 't', 'runner-one', 'one')
        self.assertEqual(first['action'], 'start')
        self.assertEqual(task_io.wake(self.sb, 'wake-two', 't', 'runner-two')['action'], 'busy')
        self.assertEqual(first['run']['heartbeat_ref'], 'refs/heads/task-io/t/runs/one')

    def test_progress_result_and_ack_survive_new_clone_and_cleanup(self):
        self.setup_io(); self.send(); self.start()
        self.prepare()
        runtime.heartbeat(self.sa, 't', 'r', progress='running')
        task_io.emit(self.sa, 'publish-intent', 't', 'r', 'publish-intent', 'external-intent',
                     {'operation_id': 'publish-original'})
        task_io._result(self.sa, 'result', 't', 'r', 'one', {'status': 'succeeded', 'evidence': 'fixture'})
        runtime.heartbeat(self.sb, 't', 'r', progress='done')
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')
        task_io.complete(self.sb, 'ack', 't', 'r', 'one', {'status': 'succeeded', 'evidence': 'fixture'})
        self.assertEqual(task_io.pending(self.sa, 't')['messages'], [])
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'succeeded')
        self.assertIsNone(self.sb.remote_tip('refs/tags/task-io/t/r'))
        self.assertIsNone(self.sb.remote_tip('refs/heads/task-io/t/runs/r'))
        finished = runtime.load_run(self.sb.snapshot(), 't', 'r')[2]
        archived = task_io.IOStore(self.sb, 't', finished).optional()
        self.assertEqual(archived.json('results/one.json')['result']['status'], 'succeeded')
        self.assertEqual(archived.json('events/publish-intent.json')['payload']['operation_id'], 'publish-original')
        self.assertFalse(any(p.startswith('.task-system/operations/') for p in archived.entries))
        snapshot = self.sb.snapshot()
        self.assertIsNone(snapshot.json('tasks/t/runs/r/io/results/one.json'))
        self.assertEqual(snapshot.metadata('tasks/t/runs/r/io/results/one.json')['result']['status'], 'succeeded')

    def test_finished_records_survive_remote_gc_and_a_completely_fresh_clone(self):
        self.setup_io(); self.send(); self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'original-intent', 'external-intent',
                     {'operation_id': 'original-save'})
        task_io._result(self.sa, 'result', 't', 'r', 'one', {'status': 'succeeded', 'version': 'original-version'})
        task_io.complete(self.sa, 'ack', 't', 'r', 'one', {'status': 'succeeded', 'version': 'original-version'})
        run = runtime.check(self.sa, 't', 'r')
        stream_head = self.sa.remote_tip(run['heartbeat_ref'])
        before = self.sa.fetch()
        finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.git(self.a, 'rev-list', '--count', before + '..' + self.sa.fetch()), '2')
        self.assertEqual(self.git(self.remote, 'for-each-ref', '--format=%(refname)', 'refs/tags'), '')
        self.git(self.remote, 'reflog', 'expire', '--expire=now', '--all')
        self.git(self.remote, 'gc', '--prune=now')
        self.assertNotEqual(self.command('git', '-C', str(self.remote), 'cat-file', '-e', stream_head, ok=False).returncode, 0)
        cold = self.base / 'cold'
        self.command('git', 'clone', '--no-local', '--single-branch', '--branch', 'main', str(self.remote), str(cold))
        store = Store(cold)
        saved = task_io.IOStore(store, 't', finished).optional()
        self.assertEqual(saved.json('events/original-intent.json')['payload']['operation_id'], 'original-save')
        self.assertEqual(saved.json('results/one.json')['result']['version'], 'original-version')
        self.assertEqual(task_io.status(store, 't', 'one')['status'], 'succeeded')
        accepted = task_io.complete(store, 'ack', 't', 'r', 'one',
                                    {'status': 'succeeded', 'version': 'original-version'})
        self.assertEqual(accepted['result']['version'], 'original-version')
        self.assertEqual(runtime.publish(store, 'finish', 't', 'r', outcome='succeeded')['status'], 'succeeded')

    def test_complete_resumes_after_candidate_save_from_another_clone(self):
        self.setup_io(); self.send(); self.start()
        value = {'status': 'succeeded', 'evidence': 'original'}
        with patch.object(task_io, '_receipt', side_effect=TaskError('interrupted after candidate')):
            with self.assertRaisesRegex(TaskError, 'interrupted'):
                task_io.complete(self.sa, 'complete', 't', 'r', 'one', value)
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')
        with self.assertRaisesRegex(TaskError, 'another request'):
            task_io.complete(self.sb, 'complete', 't', 'r', 'one', {'status': 'failed'})
        accepted = task_io.complete(self.sb, 'complete', 't', 'r', 'one', value)
        runtime.heartbeat(self.sa, 't', 'r', progress='later')
        replay = task_io.complete(self.sa, 'complete', 't', 'r', 'one', value)
        self.assertEqual(replay, accepted)

    def test_complete_reconciles_lost_authority_reply_and_rejects_changed_input(self):
        self.setup_io(); self.send(); self.start()
        push = self.sa.push
        def lose(*args, **kwargs):
            push(*args, **kwargs)
            raise GitFailure('lost reply', {'definite_rejection': False})
        with patch.object(self.sa, 'push', side_effect=lose):
            accepted = task_io.complete(self.sa, 'complete', 't', 'r', 'one', {'status': 'succeeded'})
        before = self.sb.fetch()
        self.assertEqual(task_io.complete(self.sb, 'complete', 't', 'r', 'one', {'status': 'succeeded'}), accepted)
        with self.assertRaisesRegex(TaskError, 'another request'):
            task_io.complete(self.sb, 'complete', 't', 'r', 'one', {'status': 'failed'})
        self.assertEqual(self.sb.fetch(), before)

    def test_interrupted_complete_binds_identity_across_runs_messages_and_other_operations(self):
        self.add(max_runners=2); self.send(); self.send('two')
        self.start('one'); self.start('two', store=self.sb)
        value = {'status': 'succeeded', 'evidence': 'original'}
        with patch.object(task_io, '_receipt', side_effect=TaskError('candidate saved')):
            with self.assertRaisesRegex(TaskError, 'candidate saved'):
                task_io.complete(self.sa, 'complete', 't', 'one', 'one', value)
        for run, message, result in [('two', 'one', value), ('two', 'two', {'status': 'failed'}),
                                     ('one', 'two', value), ('one', 'one', {'status': 'failed'})]:
            with self.subTest(run=run, message=message), self.assertRaisesRegex(TaskError, 'another request'):
                task_io.complete(self.sb, 'complete', 't', run, message, result)
        with self.assertRaisesRegex(TaskError, 'another request'):
            runtime.edit_task(self.sb, 'complete', 't', 1, title='wrong operation')
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')
        self.assertEqual(task_io.complete(self.sb, 'complete', 't', 'one', 'one', value)['result'], value)

    def test_pending_reservation_survives_retirement_gc_and_a_fresh_clone(self):
        self.setup_io(); self.send(); self.start(); self.prepare()
        with patch.object(task_io, '_result', side_effect=TaskError('before candidate')):
            with self.assertRaisesRegex(TaskError, 'before candidate'):
                task_io.complete(self.sa, 'reserved', 't', 'r', 'one', {'status': 'succeeded'})
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.git(self.remote, 'reflog', 'expire', '--expire=now', '--all')
        self.git(self.remote, 'gc', '--prune=now')
        cold = self.base / 'cold'
        self.command('git', 'clone', '--no-local', '--single-branch', '--branch', 'main', str(self.remote), str(cold))
        store = Store(cold)
        with self.assertRaisesRegex(TaskError, 'another request'):
            runtime.edit_task(store, 'reserved', 't', 1, title='wrong operation')
        self.assertEqual(task_io.status(store, 't', 'one')['status'], 'pending')

    def test_unconfirmed_internal_phases_instruct_retrying_the_public_operation(self):
        for phase in ('reservation', 'candidate'):
            with self.subTest(phase=phase):
                self.add(phase); self.start(phase, task=phase)
                task_io.send(self.sa, 'send-' + phase, phase, 'one', 'example', {})
                target = self.sa if phase == 'reservation' else task_io.IOStore
                if phase == 'candidate':
                    runtime.heartbeat(self.sa, phase, phase)
                operation = 'complete-io-initialize-' + phase
                with patch.object(target, 'push', side_effect=GitFailure('unconfirmed', {'definite_rejection': False})):
                    with self.assertRaises(TaskError) as raised:
                        task_io.complete(self.sa, operation, phase, phase, 'one', {'status': 'succeeded'})
                self.assertEqual(str(raised.exception), 'Push outcome is unknown; retry operation ' + operation)
                accepted = task_io.complete(self.sb, operation, phase, phase, 'one', {'status': 'succeeded'})
                self.assertEqual(accepted['result']['status'], 'succeeded')

    def test_revocation_between_candidate_and_acceptance_cannot_complete_message(self):
        self.setup_io(); self.send(); self.start('old')
        save = task_io._result
        def revoke(*args, **kwargs):
            result = save(*args, **kwargs)
            runtime.recover(self.sb, 'revoke', 't', 'old', stopped=True, settled=True, evidence='fixture:stopped')
            self.start('new', store=self.sb)
            return result
        with patch.object(task_io, '_result', side_effect=revoke), self.assertRaises(TaskError):
            task_io.complete(self.sa, 'complete', 't', 'old', 'one', {'status': 'succeeded'})
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')
        self.assertEqual(runtime.check(self.sb, 't', 'new')['status'], 'running')

    def test_finish_replay_keeps_original_records_after_lost_authority_reply(self):
        self.setup_io(); self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'save-intent', 'external-intent', {'operation_id': 'original'})
        push = self.sa.push
        def lose_reply(ref, *args, **kwargs):
            push(ref, *args, **kwargs)
            if ref == self.sa.ref:
                raise GitFailure('lost finish reply', {'definite_rejection': False})
        with patch.object(self.sa, 'push', side_effect=lose_reply):
            finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        before = self.sb.fetch()
        runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.sb.fetch(), before)
        self.assertEqual(task_io.IOStore(self.sb, 't', finished).optional().json('events/save-intent.json')['payload']['operation_id'], 'original')
        self.assertIsNone(self.sb.remote_tip('refs/tags/task-io/t/r'))

    def test_legacy_tags_remain_readable_without_creating_new_tags(self):
        self.setup_io(); self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'original', 'external-intent', {'operation_id': 'original'})
        run = runtime.check(self.sa, 't', 'r')
        original = self.sa.fetch(run['heartbeat_ref'])
        finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.sa.push('refs/tags/task-io/t/r', original, None)
        snapshot = self.sa.snapshot()
        cleanup = snapshot.metadata('tasks/t/runs/r/cleanup.json')
        cleanup.pop('io_manifest')
        changes = {p: None for p in snapshot.entries if p.startswith('tasks/t/runs/r/io/')}
        changes['tasks/t/runs/r/cleanup.json'] = encode(cleanup)
        self.sa.push(self.sa.ref, self.sa.commit(snapshot.commit, changes, 'Legacy finished fixture'), snapshot.commit)
        saved = task_io.IOStore(self.sb, 't', finished).optional()
        self.assertEqual(saved.json('events/original.json')['payload']['operation_id'], 'original')

    def test_no_stream_finish_has_an_empty_durable_manifest(self):
        self.setup_io(); self.start(); self.prepare()
        finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.sa.snapshot().metadata('tasks/t/runs/r/cleanup.json')['io_manifest'], {})
        self.assertEqual(task_io.IOStore(self.sb, 't', finished).optional().entries, {})
        self.assertEqual(self.git(self.remote, 'for-each-ref', '--format=%(refname)', 'refs/tags'), '')

    def test_late_stream_update_does_not_replace_main_records_or_get_deleted(self):
        from task_runtime import maintenance
        self.setup_io(); self.start(); self.prepare()
        task_io.emit(self.sa, 'intent', 't', 'r', 'save-intent', 'external-intent', {'operation_id': 'original'})
        with patch.object(maintenance, 'maintain'):
            finished = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        stream = task_io.IOStore(self.sa, 't', finished)
        head = stream.fetch()
        late = stream.commit(head, {'events/late.json': encode({'operation_id': 'late'})}, 'Late writer fixture')
        stream.push(stream.ref, late, head)
        saved = task_io.IOStore(self.sb, 't', finished).optional()
        self.assertIsNone(saved.json('events/late.json'))
        self.assertEqual(saved.json('events/save-intent.json')['payload']['operation_id'], 'original')
        with self.assertRaisesRegex(TaskError, 'changed after finish'):
            maintenance.collect(self.sb, 't', 'r', apply=True)
        self.assertEqual(self.sb.remote_tip(stream.ref), late)

    def test_nonregular_io_record_cannot_finish_or_release_its_run(self):
        self.setup_io(); self.start(); work = self.prepare()
        run = runtime.check(self.sa, 't', 'r')
        stream = task_io.IOStore(self.sa, 't', run)
        stream.ensure()
        head = stream.fetch()
        link = ('120000', 'blob', stream.blob(b'../state.json')[2])
        invalid = stream.commit(head, {'events/invalid.json': link}, 'Nonregular record fixture')
        stream.push(stream.ref, invalid, head)
        before = self.sa.fetch()
        with self.assertRaisesRegex(TaskError, 'Metadata must be a regular file'):
            runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertEqual(self.sa.fetch(), before)
        self.assertEqual(runtime.check(self.sb, 't', 'r')['status'], 'running')
        self.assertEqual(self.sb.remote_tip(stream.ref), invalid)
        self.assertTrue(work.exists())

    def test_old_generation_cannot_accept_a_result(self):
        self.setup_io(); self.send(); self.start('old')
        task_io._result(self.sa, 'result', 't', 'old', 'one', {'status': 'succeeded'})
        runtime.recover(self.sb, 'recover', 't', 'old', stopped=True, settled=True, evidence='fixture:stopped')
        self.start('new', store=self.sb)
        with self.assertRaises(TaskError):
            task_io.complete(self.sa, 'late-ack', 't', 'old', 'one', {'status': 'succeeded'})
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')

    def test_unknown_external_operation_stays_an_event_and_is_not_acknowledged(self):
        self.setup_io(); self.send(); self.start()
        task_io.emit(self.sa, 'intent', 't', 'r', 'save-intent', 'external-intent', {'operation_id': 'save-original'})
        runtime.heartbeat(self.sb, 't', 'r', progress='waiting')
        stream = task_io.IOStore(self.sb, 't', runtime.check(self.sb, 't', 'r'))
        self.assertEqual(stream.snapshot().json('events/save-intent.json')['payload']['operation_id'], 'save-original')
        with self.assertRaises(TaskError):
            task_io.complete(self.sa, 'unknown', 't', 'r', 'one', {'status': 'unknown'})
        self.assertEqual(len(task_io.pending(self.sb, 't')['messages']), 1)

    def test_lost_send_reply_reconciles_remotely(self):
        self.setup_io()
        original = task_io.IOStore.push
        def lost(stream, ref, candidate, expected, **kwargs):
            original(stream, ref, candidate, expected, **kwargs)
            raise GitFailure('lost reply', {'definite_rejection': False})
        with patch.object(task_io.IOStore, 'push', lost):
            self.send()
        self.assertEqual(self.send(store=self.sb)['message_id'], 'one')

    def test_ack_replay_does_not_depend_on_latest_heartbeat_commit(self):
        self.setup_io(); self.send(); self.start()
        task_io._result(self.sa, 'result', 't', 'r', 'one', {'status': 'succeeded'})
        task_io.complete(self.sa, 'ack', 't', 'r', 'one', {'status': 'succeeded'})
        runtime.heartbeat(self.sb, 't', 'r', progress='later')
        replay = task_io.complete(self.sb, 'ack', 't', 'r', 'one', {'status': 'succeeded'})
        self.assertEqual(replay['result']['status'], 'succeeded')

    def test_wrong_branch_manifest_is_rejected(self):
        self.setup_io(); self.send()
        stream = task_io.IOStore(self.sa, 't')
        tip = stream.fetch()
        candidate = stream.commit(tip, {'.task-system/config.json': b'{}\n'}, 'Corrupt fixture')
        stream.push(stream.ref, candidate, tip)
        with self.assertRaises(TaskError):
            task_io.pending(self.sa, 't')


    def test_emit_before_first_heartbeat_can_be_observed(self):
        self.setup_io(); self.start()
        task_io.emit(self.sa, 'early-event', 't', 'r', 'early-event', 'preparing', {})
        observed = runtime.observe(self.sb, 't', 'r')
        self.assertEqual(observed['run_id'], 'r')
        stream = task_io.IOStore(self.sa, 't', runtime.check(self.sa, 't', 'r'))
        state = stream.snapshot().json('state.json')
        self.assertEqual(state['sequence'], 0)
        runtime.heartbeat(self.sa, 't', 'r', progress='first real heartbeat')
        self.assertEqual(stream.snapshot().json('state.json')['sequence'], 1)

    def test_interruption_after_stream_creation_keeps_observation_available(self):
        self.setup_io(); self.start()
        stream = task_io.IOStore(self.sa, 't', runtime.check(self.sa, 't', 'r'))
        stream.ensure()
        self.assertEqual(runtime.observe(self.sb, 't', 'r')['run_id'], 'r')

    def commit_changes(self, store, changes):
        parent = store.fetch()
        candidate = store.commit(parent, changes, 'fixture corruption')
        store.push(store.ref, candidate, parent)

    def test_immutable_records_accept_identical_content_with_new_operation_ids(self):
        self.setup_io()
        self.send(); self.start()
        task_io.send(self.sb, 'same-message', 't', 'one', 'example', {'value': 'one'})
        event = task_io.emit(self.sa, 'event', 't', 'r', 'event', 'progress', {'step': 1})
        replay = task_io.emit(self.sb, 'event-again', 't', 'r', 'event', 'progress', {'step': 1})
        self.assertEqual(replay['payload'], event['payload'])
        with self.assertRaisesRegex(TaskError, 'different content'):
            task_io.emit(self.sb, 'event-changed', 't', 'r', 'event', 'progress', {'step': 2})
        task_io._result(self.sa, 'result', 't', 'r', 'one', {'status': 'succeeded'})
        task_io._result(self.sb, 'result-again', 't', 'r', 'one', {'status': 'succeeded'})
        with self.assertRaisesRegex(TaskError, 'immutable'):
            task_io._result(self.sb, 'result-changed', 't', 'r', 'one', {'status': 'failed'})
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')

    def test_ack_requires_saved_result_matching_message_run_generation_and_terminal_status(self):
        self.setup_io(); self.send(); self.start()
        run = runtime.check(self.sa, 't', 'r')
        stream = task_io.IOStore(self.sa, 't', run)
        stream.ensure()
        with self.assertRaisesRegex(TaskError, 'exact message'):
            task_io._receipt(self.sa, 't', 'r', 'one', run)
        task_io._result(self.sa, 'result', 't', 'r', 'one', {'status': 'succeeded'})
        record = stream.snapshot().json('results/one.json')
        for key, value in [('message_sha256', 'wrong'), ('run_id', 'other'), ('generation', 999),
                           ('result', {'status': 'unknown'})]:
            self.commit_changes(stream, {'results/one.json': encode(dict(record, **{key: value}))})
            with self.subTest(field=key), self.assertRaisesRegex(TaskError, 'exact message'):
                task_io._receipt(self.sb, 't', 'r', 'one', run)
        self.assertEqual(task_io.status(self.sb, 't', 'one')['status'], 'pending')
        self.commit_changes(stream, {'results/one.json': encode(record)})
        self.assertEqual(task_io.complete(self.sb, 'ack', 't', 'r', 'one', {'status': 'succeeded'})['result']['status'], 'succeeded')

    def test_ack_from_another_run_cannot_replace_an_accepted_result(self):
        self.add(max_runners=2)
        self.send()
        self.start('one'); self.start('two', store=self.sb)
        task_io._result(self.sa, 'result-one', 't', 'one', 'one', {'status': 'succeeded'})
        accepted = task_io.complete(self.sa, 'ack-one', 't', 'one', 'one', {'status': 'succeeded'})
        task_io._result(self.sb, 'result-two', 't', 'two', 'one', {'status': 'failed'})
        with self.assertRaisesRegex(TaskError, 'different result'):
            task_io.complete(self.sb, 'ack-two', 't', 'two', 'one', {'status': 'failed'})
        self.assertEqual(task_io.status(self.sa, 't', 'one')['receipt']['result'], accepted['result'])

    def test_corrupt_message_paths_and_receipt_hashes_are_rejected(self):
        self.setup_io()
        with self.assertRaisesRegex(TaskError, 'not found'):
            task_io.status(self.sa, 't', 'missing')
        self.send()
        stream = task_io.IOStore(self.sa, 't')
        original = stream.snapshot().json('messages/one.json')
        self.commit_changes(stream, {'messages/one.json': encode(dict(original, id='other'))})
        with self.assertRaisesRegex(TaskError, 'not found'):
            task_io.message(self.sb, 't', 'one')
        with self.assertRaisesRegex(TaskError, 'Malformed'):
            task_io.pending(self.sb, 't')
        self.commit_changes(stream, {'messages/one.json': encode(original)})
        self.commit_changes(self.sa, {'tasks/t/io/receipts/one.json': encode({'message_sha256': 'wrong'})})
        with self.assertRaisesRegex(TaskError, 'does not match'):
            task_io.status(self.sb, 't', 'one')

    def test_stream_rejects_wrong_pinned_ref_and_existing_branch_without_manifest(self):
        self.setup_io()
        run = self.start()
        with self.assertRaisesRegex(TaskError, 'pinned namespace'):
            task_io.IOStore(self.sa, 't', dict(run, io_ref='refs/heads/other'))
        stream = task_io.IOStore(self.sa, 't')
        candidate = stream.commit(None, {'unowned': b'keep'}, 'preexisting branch')
        stream.push(stream.ref, candidate, None)
        with self.assertRaisesRegex(TaskError, 'without a task I/O manifest'):
            stream.ensure()

    def test_completed_wake_receipt_cannot_grant_execution_again(self):
        self.setup_io(); self.send()
        task_io.wake(self.sa, 'wake', 't', 'runner', 'r')
        runtime.recover(self.sa, 'recover', 't', 'r', stopped=True, settled=True, evidence='fixture:stopped')
        with self.assertRaisesRegex(TaskError, 'no longer owns'):
            task_io.wake(self.sb, 'wake', 't', 'runner', 'r')
        self.assertIsNone(runtime.load_task(self.sa.snapshot(), 't')[1]['slots']['1']['run_id'])


class TaskIOValidationTest(unittest.TestCase):
    def test_payload_boundary_counts_encoded_bytes_and_returns_an_independent_copy(self):
        overhead = len(encode({'data': ''}))
        value = {'data': 'x' * (task_io.MAX_BYTES - overhead)}
        self.assertEqual(len(encode(value)), task_io.MAX_BYTES)
        self.assertEqual(task_io.bounded(value), value)
        for invalid in (None, [], 'text', dict(value, data=value['data'] + 'x'), {'data': 'あ' * task_io.MAX_BYTES}):
            with self.subTest(type=type(invalid).__name__), self.assertRaises(TaskError):
                task_io.bounded(invalid)
        original = {'nested': {'items': [1]}}
        copied = task_io.bounded(original)
        copied['nested']['items'].append(2)
        self.assertEqual(original, {'nested': {'items': [1]}})

    def test_io_namespace_rejects_invalid_git_refs_and_authority_overlap(self):
        config = dict(authority_ref='refs/heads/main', ref_prefix='task-runtime')
        self.assertEqual(task_io.prefix(config), 'task-io')
        self.assertEqual(task_io.prefix(dict(config, io_ref_prefix='custom/io')), 'custom/io')
        self.assertEqual(task_io.prefix(dict(config, authority_ref='refs/heads/task-io')), 'task-io-2')
        self.assertEqual(task_io.prefix(dict(config, authority_ref='refs/heads/task-io/authority')), 'task-io-2')
        self.assertEqual(task_io.prefix(dict(config, ref_prefix='task-io', authority_ref='refs/heads/task-io-2')), 'task-io-3')
        for value in (None, '', 1, '/io', 'io/', 'io//stream', 'Upper', 'io..ref', 'main',
                      'main/child', 'task-runtime', 'task-runtime/child'):
            with self.subTest(prefix=value), self.assertRaises(TaskError):
                task_io.prefix(dict(config, io_ref_prefix=value))
