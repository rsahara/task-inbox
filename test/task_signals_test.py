"""Real Git tests for typed signals and crash-recoverable publication delivery."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import patch

from task_runtime_test import RuntimeCase
from task_runtime import runtime, signals, outbox, task_io
from task_runtime.store import Store, Snapshot, TaskError, encode


def value(revision=1, **changes):
    return dict(dict(schema_version=1, task_id='source', id='check', revision=revision,
                     type='observation', status='active', payload={'detail':'Observed'}), **changes)


class SignalTest(RuntimeCase):
    def setUp(self):
        super().setUp()
        signals.enable(self.sa,'enable')
        self.add('source'); self.add('assessor'); self.add('viewer')
        signals.subscribe(self.sa,'subscribe-assessor','source','assessor')
        signals.subscribe(self.sa,'subscribe-viewer','source','viewer','observation')

    def publish(self, operation='publish', *, outcome=None, signal=None):
        if signal is not None:
            signals.put(self.sa,'source','run',signal)
        return runtime.publish(self.sa,operation,'source','run',outcome=outcome)

    def prepared(self):
        self.start('run',task='source')
        return self.prepare('run',task='source')

    def test_signal_finish_publishes_and_delivers_without_wrapper(self):
        self.prepared()
        result=self.publish(outcome='succeeded',signal=value())
        self.assertTrue(result['notifications']['required'])
        pending=task_io.pending(self.sb,'assessor')['messages']
        self.assertEqual(len(pending),1)
        notice=signals.read_notice(self.sb,pending[0])
        self.assertEqual(notice['revision'],1)
        self.assertNotIn('Observed',json.dumps(pending))
        self.assertEqual(len(task_io.pending(self.sb,'viewer')['messages']),1)
        replay=self.publish(outcome='succeeded')
        self.assertEqual(replay['notifications']['delivered'],0)

    def test_partial_delivery_lost_reply_and_fresh_clone_recovery(self):
        self.prepared();signals.put(self.sa,'source','run',value())
        send=task_io.send
        def fail_viewer(store,operation,task,*args):
            if task=='viewer':raise TaskError('Temporary delivery failure')
            return send(store,operation,task,*args)
        with patch.object(task_io,'send',side_effect=fail_viewer):
            with self.assertRaisesRegex(TaskError,'delivery failure'):
                self.publish(outcome='succeeded')
        self.assertEqual(signals.catalog(self.sb.snapshot(),'source')['check']['value'],value())
        self.assertEqual(len(task_io.pending(self.sb,'assessor')['messages']),1)
        self.assertFalse(task_io.pending(self.sb,'viewer')['messages'])
        self.assertEqual(outbox.flush(self.sb)['delivered'],1)
        self.assertEqual(outbox.flush(self.sb)['delivered'],0)
        self.publish(outcome='succeeded')

    def test_changed_signal_revision_type_filter_and_explicit_removal(self):
        work=self.prepared();self.publish(signal=value())
        self.assertFalse(self.publish('unchanged')['notifications']['required'])
        with self.assertRaisesRegex(TaskError,'newer revision'):
            signals.put(self.sa,'source','run',value(payload={'new':'evidence'}))
        self.publish('resolve',signal=value(2,status='resolved',resolution='Verified'))
        path=work/'tasks/source/data/signals/check.json'
        path.unlink()
        self.publish('remove')
        notices=task_io.pending(self.sb,'assessor')['messages']
        self.assertEqual(len(notices),3)
        removal=next(m for m in notices if m['payload']['removed'])
        self.assertTrue(signals.read_notice(self.sb,removal)['removed'])
        self.assertFalse(signals.catalog(self.sb.snapshot(),'source'))

    def test_publication_enforces_validation_for_direct_workspace_writes(self):
        work=self.prepared()
        path=work/'tasks/source/data/signals/check.json';path.parent.mkdir(parents=True)
        path.write_bytes(encode(value(5)))
        with self.assertRaisesRegex(TaskError,'starts at revision 1'):
            self.publish()

    def test_type_change_notifies_previous_and_current_subscribers_once(self):
        self.add('planner')
        signals.subscribe(self.sa, 'planner-type', 'source', 'planner', 'opportunity')
        signals.subscribe(self.sa, 'viewer-new-type', 'source', 'viewer', 'opportunity')
        self.prepared(); self.publish(signal=value())
        self.publish('change-type', signal=value(2, type='opportunity'))
        for task_id, count in [('assessor', 2), ('viewer', 2), ('planner', 1)]:
            messages = task_io.pending(self.sb, task_id)['messages']
            self.assertEqual(len(messages), count)
            changed = next(m for m in messages if m['payload']['revision'] == 2)
            self.assertEqual(signals.read_notice(self.sb, changed)['type'], 'opportunity')

    def test_durable_evidence_survives_tombstone_removal_and_path_reuse(self):
        work = self.prepared()
        ended = value(status='resolved', resolution='Verified')
        self.publish(signal=ended)
        before = signals.catalog(self.sb.snapshot(), 'source', include_removed=True)['check']
        evidence_path = before['evidence_path']
        self.assertEqual(signals.read_value(self.sb.snapshot(), evidence_path), ended)
        (work / 'tasks/source/data/signals/check.json').write_bytes(encode(value(id='replacement')))
        self.publish('replace-ended')
        self.assertNotIn('check', signals.catalog(self.sb.snapshot(), 'source'))
        after = signals.catalog(self.sb.snapshot(), 'source', include_removed=True)['check']
        self.assertTrue(after['removed'])
        self.assertEqual(after['evidence_path'], evidence_path)
        self.assertEqual(signals.read_value(self.sb.snapshot(), evidence_path), ended)

    def test_active_signal_cannot_disappear_silently(self):
        work=self.prepared();self.publish(signal=value())
        (work/'tasks/source/data/signals/check.json').unlink()
        with self.assertRaisesRegex(TaskError,'tombstone'):
            self.publish('delete-active')

    def test_generic_outbox_intent_is_committed_with_output(self):
        work=self.prepared()
        content=work/'tasks/source/data/report.json';content.parent.mkdir(parents=True,exist_ok=True)
        content.write_text('{"result":"ready"}')
        messages=[dict(task_id='assessor',message_id='report-one',kind='report-updated',payload={'path':'tasks/source/data/report.json'})]
        outbox.prepare(self.sa,'source','run','report-intent',messages)
        self.publish(outcome='succeeded')
        message=task_io.pending(self.sb,'assessor')['messages'][0]
        self.assertEqual(message['kind'],'report-updated')
        saved=Snapshot(self.sb,message['payload']['publication_commit'])
        self.assertEqual(saved.read('tasks/source/data/report.json'),b'{"result":"ready"}')

    def test_schema10_and_subscription_reject_self_delivery(self):
        self.assertEqual(self.sa.snapshot().json('.task-system/config.json')['schema_version'],10)
        self.prepared()
        with self.assertRaisesRegex(TaskError,'own signals'):
            signals.subscribe(self.sa,'self','source','source')

    def test_existing_message_cannot_silently_refer_to_another_commit(self):
        first = self.sa.snapshot().commit
        messages = [dict(task_id='assessor', message_id='same', kind='report', payload={'path':'report.json'})]
        outbox.enqueue(self.sa, 'first-reference', 'source', first, messages)
        self.sa.transact('advance', {'kind':'fixture'}, lambda s: ({'report.json':b'new contents'}, {}))
        second = self.sa.snapshot().commit
        with self.assertRaisesRegex(TaskError, 'different content'):
            outbox.enqueue(self.sa, 'second-reference', 'source', second, messages)
        self.assertEqual(task_io.pending(self.sa, 'assessor')['messages'][0]['payload']['publication_commit'], first)
        self.assertEqual(outbox.flush(self.sa)['delivered'], 0)

    def test_conflicting_intents_are_rejected_before_finishing(self):
        self.prepared()
        for operation, payload in [('one', {'value':1}), ('two', {'value':2})]:
            outbox.prepare(self.sa, 'source', 'run', operation,
                           [dict(task_id='assessor', message_id='conflict', kind='report', payload=payload)])
        with self.assertRaisesRegex(TaskError, 'Conflicting outbox'):
            self.publish(outcome='succeeded')
        self.assertEqual(runtime.check(self.sb, 'source', 'run')['status'], 'running')
        self.assertFalse(task_io.pending(self.sb, 'assessor')['messages'])

    def test_signal_and_application_intent_cannot_bind_the_same_message(self):
        from task_runtime.store import digest
        self.prepared(); signals.put(self.sa, 'source', 'run', value())
        message_id = 'signal-' + digest(['source','publish','check'])[:64]
        outbox.prepare(self.sa, 'source', 'run', 'intent',
            [dict(task_id='assessor',message_id=message_id,kind='report',payload={})])
        with self.assertRaisesRegex(TaskError, 'Conflicting outbox'):
            self.publish(outcome='succeeded')
        self.assertEqual(runtime.check(self.sb, 'source', 'run')['status'], 'running')

    def test_ended_signal_path_can_be_reused_for_another_identity(self):
        work = self.prepared()
        self.publish(signal=value(status='resolved', resolution='Verified'))
        (work / 'tasks/source/data/signals/check.json').write_bytes(encode(value(id='replacement')))
        self.publish('replace-path')
        pending = task_io.pending(self.sb,'assessor')['messages']
        self.assertEqual(len(pending),3)
        for message in pending:
            signals.read_notice(self.sb,message)
        self.assertEqual(set(signals.catalog(self.sb.snapshot(),'source')), {'replacement'})

    def test_cli_and_indexed_task_paths(self):
        result=self.command('python3',str(Path(__file__).resolve().parents[1]/'bin/task-inbox'),
                            '--repo',str(self.a),'signal','list','--task','source')
        self.assertEqual(json.loads(result.stdout),{'signals':{}})
        self.prepared(); self.publish(signal=value(id='needs_attention'))
        result=self.command('python3',str(Path(__file__).resolve().parents[1]/'bin/task-inbox'),
                            '--repo',str(self.a),'signal','list','--task','source')
        self.assertEqual(json.loads(result.stdout)['signals']['needs_attention']['value'], value(id='needs_attention'))


class EnableSignalsTest(RuntimeCase):
    def test_enable_seeds_evidence_for_existing_signals(self):
        self.add('source')
        relative = 'tasks/source/data/signals/check.json'
        self.sa.transact('fixture-signal', {'kind':'fixture'}, lambda s: ({relative: encode(value())}, {}))
        signals.enable(self.sa, 'enable')
        record = signals.catalog(self.sb.snapshot(), 'source', include_removed=True)['check']
        self.assertEqual(signals.read_value(self.sb.snapshot(), record['evidence_path']), value())

    def test_enable_requires_drained_runs_and_fences_old_schema(self):
        self.add('source'); self.start('old', task='source')
        with self.assertRaisesRegex(TaskError, 'all runs'):
            signals.enable(self.sa, 'enable-busy')
        runtime.publish(self.sa, 'finish-old', 'source', 'old', outcome='cancelled', discard_output=True)
        signals.enable(self.sa, 'enable-idle')
        self.assertEqual(self.sb.snapshot().json('.task-system/config.json')['schema_version'], 10)


class IndexedSignalTest(RuntimeCase):
    init_options = {'index':'registry/tasks.json', 'task_root':'units'}

    def test_configured_layout_and_type_subscription(self):
        signals.enable(self.sa,'enable')
        self.add('source', path='units/producer', data_path='outputs/producer')
        self.add('consumer', path='units/consumer')
        signals.subscribe(self.sa,'subscribe','source','consumer','opportunity')
        self.start('r',task='source'); work=self.prepare('r',task='source')
        saved=signals.put(self.sa,'source','r',value())
        self.assertTrue(saved['path'].startswith('outputs/producer/signals/'))
        runtime.publish(self.sa,'save','source','r',outcome='succeeded')
        self.assertEqual(task_io.pending(self.sb,'consumer')['messages'],[])
