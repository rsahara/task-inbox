"""Real Git coverage for current-state signals and inbox-only reconciliation."""
import json
from pathlib import Path
from unittest.mock import patch
from task_runtime_test import RuntimeCase
from task_runtime import runtime, signals, task_io
from task_runtime.store import TaskError, encode


def value(revision=1, **changes):
    return dict(dict(schema_version=1, task_id='source', id='check', revision=revision,
                     type='observation', status='active', payload={'detail': 'Observed'}), **changes)


class SignalTest(RuntimeCase):
    def setUp(self):
        super().setUp()
        signals.enable(self.sa, 'enable')
        for task in ('source', 'assessor', 'viewer'):
            self.add(task)
        signals.subscribe(self.sa, 'sub', 'source', 'assessor')
        signals.subscribe(self.sa, 'sub2', 'source', 'viewer')

    def prepared(self):
        self.start('run', task='source')
        return self.prepare('run', task='source')

    def publish(self, revision=1, **changes):
        signals.put(self.sa, 'source', 'run', value(revision, **changes))
        return runtime.publish(self.sa, 'publish-' + str(revision), 'source', 'run')

    def test_latest_state_batch_and_revision_identity(self):
        self.prepared()
        self.publish()
        self.publish(2, type='opportunity')
        messages = task_io.pending(self.sb, 'assessor')['messages']
        self.assertEqual(len(messages), 2)
        self.assertEqual({m['id'] for m in messages}, {signals.message_id('source','check',r) for r in (1,2)})
        for m in messages:
            self.assertEqual(signals.read_notice(self.sb, m)['type'], 'opportunity')
        self.assertEqual(signals.sync(self.sb)['delivered'], 0)
        self.start('consume', task='assessor')
        for i,m in enumerate(messages):
            task_io.complete(self.sa, 'ack-'+str(i), 'assessor', 'consume', m['id'], {'status':'succeeded'})
        self.assertFalse(task_io.pending(self.sb, 'assessor')['messages'])
        self.assertEqual(signals.sync(self.sb)['delivered'], 0)

    def test_partial_delivery_and_lost_response_recover_elsewhere(self):
        self.prepared()
        original = task_io.send
        def interrupted(store, op, task, *args):
            result = original(store, op, task, *args)
            if task == 'assessor':
                raise TaskError('lost response')
            return result
        with patch.object(task_io, 'send', side_effect=interrupted):
            with self.assertRaisesRegex(TaskError, 'lost response'):
                self.publish()
        self.assertEqual(len(task_io.pending(self.sb,'assessor')['messages']),1)
        self.assertFalse(task_io.pending(self.sb,'viewer')['messages'])
        self.assertEqual(signals.sync(self.sb)['delivered'],1)
        self.assertEqual(signals.sync(self.sb)['delivered'],0)
        self.assertFalse(any('outbox' in p or 'signal-heads' in p for p in self.sb.snapshot().entries))

    def test_intermediate_revisions_need_not_be_delivered(self):
        self.prepared()
        with patch.object(task_io,'send',side_effect=TaskError('offline')):
            for revision in (1,2):
                with self.assertRaisesRegex(TaskError,'offline'):
                    self.publish(revision)
        signals.sync(self.sb)
        self.assertEqual([m['payload']['revision'] for m in task_io.pending(self.sb,'viewer')['messages']], [2])

    def test_start_recovers_unfinished_sync(self):
        self.prepared()
        with patch.object(task_io,'send',side_effect=TaskError('offline')):
            with self.assertRaises(TaskError):
                self.publish()
        self.start('consumer',task='viewer',store=self.sb)
        self.assertEqual(len(task_io.pending(self.sb,'viewer')['messages']),1)

    def test_list_before_subscription_and_late_subscription(self):
        self.add('late')
        self.prepared(); self.publish()
        self.assertEqual(signals.catalog(self.sb.snapshot(),'source')['check']['value'],value())
        self.assertFalse(task_io.pending(self.sb,'late')['messages'])
        signals.subscribe(self.sb,'late-sub','source','late')
        signals.sync(self.sb)
        self.assertEqual(len(task_io.pending(self.sb,'late')['messages']),1)

    def test_ended_records_retained_and_hidden_by_default(self):
        work=self.prepared(); self.publish()
        self.publish(2,status='resolved',resolution='Verified')
        self.assertFalse(signals.catalog(self.sb.snapshot(),'source',include_ended=False))
        self.assertEqual(signals.catalog(self.sb.snapshot(),'source')['check']['value']['status'],'resolved')
        (work/'tasks/source/data/signals/check.json').unlink()
        with self.assertRaisesRegex(TaskError,'physical deletion'):
            runtime.publish(self.sa,'delete','source','run')
        cli=Path(__file__).resolve().parents[1]/'bin/task-inbox'
        result=self.command('python3',str(cli),'--repo',str(self.b),'signal','list','--task','source')
        self.assertEqual(json.loads(result.stdout),{'signals':{}})

    def test_conflict_reported_without_stopping_sibling_delivery(self):
        self.prepared()
        mid=signals.message_id('source','check',1)
        task_io.send(self.sa,'collision','assessor',mid,'manual',{})
        with self.assertRaisesRegex(TaskError,'different content'):
            self.publish()
        self.assertEqual(len(task_io.pending(self.sb,'viewer')['messages']),1)
        self.assertTrue(signals.sync(self.sb)['needs_attention'])

    def test_ordinary_output_does_not_notify(self):
        work=self.prepared()
        self.output(work,'tasks/source/data/report.json',encode({'answer':42}))
        result=runtime.publish(self.sa,'ordinary','source','run',outcome='succeeded')
        self.assertFalse(result['notifications']['required'])
        self.assertFalse(task_io.pending(self.sb,'viewer')['messages'])

    def test_revision_ownership_and_multiple_types(self):
        self.prepared(); self.publish()
        with self.assertRaisesRegex(TaskError,'newer revision'):
            signals.put(self.sa,'source','run',value(type='changed'))
        with self.assertRaisesRegex(TaskError,'owner'):
            signals.put(self.sa,'source','run',value(task_id='other'))
        signals.put(self.sa,'source','run',value(id='second',type='opportunity'))
        runtime.publish(self.sa,'second','source','run',outcome='succeeded')
        self.assertEqual(len(signals.catalog(self.sb.snapshot(),'source')),2)


class EnableSignalsTest(RuntimeCase):
    def test_enable_requires_drained_runs(self):
        self.add('source'); self.start('old',task='source')
        with self.assertRaisesRegex(TaskError,'all runs'):
            signals.enable(self.sa,'busy')
        runtime.publish(self.sa,'finish','source','old',outcome='cancelled',discard_output=True)
        signals.enable(self.sa,'idle')
        self.assertEqual(self.sb.snapshot().json('.task-system/config.json')['schema_version'],10)


class IndexedSignalTest(RuntimeCase):
    init_options={'index':'registry/tasks.json','task_root':'units'}
    def test_configured_layout(self):
        signals.enable(self.sa,'enable')
        self.add('source',path='units/producer',data_path='outputs/producer')
        self.add('consumer',path='units/consumer')
        signals.subscribe(self.sa,'subscribe','source','consumer')
        self.start('r',task='source'); self.prepare('r',task='source')
        self.assertTrue(signals.put(self.sa,'source','r',value())['path'].startswith('outputs/producer/signals/'))
        runtime.publish(self.sa,'save','source','r',outcome='succeeded')
        self.assertEqual(len(task_io.pending(self.sb,'consumer')['messages']),1)
