"""Business completion survives interrupted finish and temporary-resource cleanup."""
import copy
import hashlib
import unittest
from unittest.mock import Mock, patch
from task_runtime_test import RuntimeCase
from task_runtime import completion, runtime
from task_runtime.store import Store, TaskError


class CompletionTest(RuntimeCase):
    def setUp(self):
        super().setUp()
        self.add()
        self.run = self.start(completion_contract={'required': ['git', 'delivery'], 'context': {'request': 'request-1'}})
        self.work = self.prepare()

    def evidence(self, delivery='succeeded', stopped=True):
        steps = {name: {'status': 'succeeded', 'operation_id': name + '-1',
                        'evidence': {'saved_result': name + '-result'}} for name in ('git', 'delivery')}
        steps['delivery']['status'] = delivery
        return completion.receipt(self.run, steps, stopped=stopped)

    def save(self, value, operation='business-result'):
        return completion.record(self.sa, operation, 't', 'r', value)

    def test_missing_partial_and_unknown_results_cannot_finish_successfully(self):
        for value in (None, self.evidence('not_started', stopped=False), self.evidence('unknown')):
            if value:
                self.save(value, 'record-' + ('partial' if not value['operations_stopped'] else 'unknown'))
            with self.assertRaises(TaskError):
                runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
            self.assertEqual(runtime.check(self.sa, 't', 'r')['status'], 'running')
            self.assertTrue(self.work.exists())
        with self.assertRaisesRegex(TaskError, 'runner limit'):
            self.start('replacement')
        self.assertEqual(runtime.load_run(self.sb.snapshot(), 't', 'r')[2]['completion']['status'], 'unknown')

    def test_failure_releases_authority_without_claiming_business_success(self):
        self.save(self.evidence('failed'))
        with self.assertRaises(TaskError):
            runtime.publish(self.sa, 'wrong-success', 't', 'r', outcome='succeeded')
        result = runtime.publish(self.sa, 'finish-failure', 't', 'r', outcome='failed')
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.start('next')['status'], 'running')

    def test_cleanup_retains_evidence_and_finish_is_replayable_from_another_clone(self):
        value = self.evidence()
        self.save(value)
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')
        self.assertFalse(self.work.exists())
        snapshot = self.sb.snapshot()
        self.assertIsNone(snapshot.json('tasks/t/runs/r/result.json'))
        saved = snapshot.metadata('tasks/t/runs/r/run.json')
        self.assertEqual(saved['completion']['steps'], value['steps'])
        self.assertEqual(runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded')['status'], 'succeeded')

    def test_lost_completion_reply_reconciles_the_original_operation(self):
        original = Store.push
        def lose(store, *args):
            original(store, *args)
            raise TaskError('lost reply')
        value = self.evidence()
        with patch.object(Store, 'push', lose), self.assertRaises(TaskError):
            self.save(value)
        replay = completion.record(self.sb, 'business-result', 't', 'r', value)
        self.assertEqual(replay['status'], 'succeeded')
        runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded')

    def test_finish_can_record_terminal_evidence_atomically_and_replay_after_cleanup(self):
        value = self.evidence()
        saved = runtime.publish(self.sa, 'finish', 't', 'r', outcome='succeeded', completion_record=value)
        self.assertFalse(self.work.exists())
        self.assertEqual(saved['completion']['steps'], value['steps'])
        replay = runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded', completion_record=value)
        self.assertEqual(replay, saved)
        changed = copy.deepcopy(value); changed['steps']['git']['evidence'] = {'commit': 'other'}
        with self.assertRaisesRegex(TaskError, 'another request'):
            runtime.publish(self.sb, 'finish', 't', 'r', outcome='succeeded', completion_record=changed)

    def test_combined_finish_cannot_replace_unknown_identity_or_verified_steps(self):
        value = self.evidence('unknown')
        self.save(value)
        changed = self.evidence(); changed['steps']['delivery']['operation_id'] = 'new-delivery'
        with self.assertRaisesRegex(TaskError, 'original unknown'):
            runtime.publish(self.sa, 'bad-finish', 't', 'r', outcome='succeeded', completion_record=changed)
        changed = self.evidence(); changed['steps']['git']['evidence'] = {'commit': 'other'}
        with self.assertRaisesRegex(TaskError, 'immutable'):
            runtime.publish(self.sa, 'bad-step', 't', 'r', outcome='succeeded', completion_record=changed)
        for number, value in enumerate((self.evidence('unknown'), self.evidence(stopped=False))):
            with self.assertRaisesRegex(TaskError, 'partial or unknown'):
                runtime.publish(self.sa, 'partial-' + str(number), 't', 'r', outcome='succeeded', completion_record=value)
        self.assertEqual(runtime.check(self.sb, 't', 'r')['completion']['status'], 'unknown')
        self.assertTrue(self.work.exists())

    def test_unknown_operation_cannot_be_reissued_with_a_new_identity(self):
        value = self.evidence('unknown')
        self.save(value)
        changed = copy.deepcopy(value);changed['steps']['delivery']['operation_id'] = 'delivery-2'
        with self.assertRaisesRegex(TaskError, 'original unknown'):
            self.save(changed, 'replacement')
        value['steps']['delivery']['status'] = 'succeeded'
        self.assertEqual(self.save(value, 'readback')['status'], 'succeeded')

    def test_verified_step_cannot_change_status_identity_or_evidence(self):
        original = self.evidence()
        self.save(original)
        for key, value in [('status', 'failed'), ('operation_id', 'git-2'),
                           ('evidence', {'saved_result': 'other'})]:
            changed = copy.deepcopy(original)
            changed['steps']['git'][key] = value
            with self.subTest(field=key), self.assertRaisesRegex(TaskError, 'immutable'):
                self.save(changed, 'change-' + key)
        saved = runtime.check(self.sb, 't', 'r')['completion']
        self.assertEqual(saved['steps'], original['steps'])

    def test_evidence_from_another_generation_or_request_is_rejected(self):
        for key, replacement in [('generation', 999), ('run_id', 'other'), ('repository_id', 'other'), ('context', {})]:
            value = self.evidence();value[key] = replacement
            with self.assertRaises(TaskError): self.save(value, 'wrong-' + key)
        value = self.evidence();del value['steps']['delivery']
        with self.assertRaises(TaskError): self.save(value, 'missing-delivery')




class CompletionAdmissionTest(unittest.TestCase):
    def test_checkpoint_only_continuation_rejects_business_contract_before_mutation(self):
        store = Mock()
        with patch('task_runtime.maintenance.maintain') as maintain:
            with self.assertRaisesRegex(TaskError, 'Checkpoint-only continuation'):
                runtime.start(store, 'resume', 't', 'runner', resume_from='original', checkpoint_id='saved',
                              completion_contract={'required': ['delivery'], 'context': {'request': 'original'}})
            maintain.assert_not_called()
        self.assertEqual(store.mock_calls, [])


class CompletionValidationTest(unittest.TestCase):
    def setUp(self):
        self.run = dict(repository_id='repo', task_id='t', run_id='r', generation=1,
                        completion_contract={'required': ['git', 'delivery'], 'context': {'request': 'one'}})

    def evidence(self, statuses=('succeeded', 'succeeded'), stopped=True):
        steps = {name: dict(status=status, operation_id=name + '-1', evidence={'commit': 'saved'})
                 for name, status in zip(('git', 'delivery'), statuses)}
        return completion.receipt(self.run, steps, stopped=stopped)

    def test_contract_rejects_malformed_duplicate_and_oversized_requirements(self):
        valid = self.run['completion_contract']
        invalid = [None, [], {}, dict(valid, extra=True), dict(valid, required=[]),
                   dict(valid, required='git'), dict(valid, required=['git', 'git']),
                   dict(valid, required=[1]), dict(valid, required=['Invalid']),
                   dict(valid, required=['step-' + str(n) for n in range(33)]),
                   dict(valid, context=[]), dict(valid, context={'data': 'x' * 32768})]
        for value in invalid:
            with self.subTest(value_type=type(value).__name__), self.assertRaises(TaskError):
                completion.contract(value)
        checked = completion.contract(valid)
        checked['required'].append('other')
        checked['context']['request'] = 'changed'
        self.assertEqual(valid, {'required': ['git', 'delivery'], 'context': {'request': 'one'}})
        self.assertEqual(len(completion.contract(dict(valid, required=['s' + str(n) for n in range(32)]))['required']), 32)

    def test_receipt_status_requires_both_terminal_steps_and_stopped_operations(self):
        for statuses, stopped, expected in [
                (('succeeded', 'succeeded'), True, 'succeeded'),
                (('succeeded', 'failed'), True, 'failed'),
                (('not_started', 'failed'), True, 'failed'),
                (('succeeded', 'succeeded'), False, 'partial'),
                (('failed', 'not_started'), False, 'partial'),
                (('unknown', 'succeeded'), True, 'unknown'),
                (('failed', 'unknown'), False, 'unknown')]:
            with self.subTest(statuses=statuses, stopped=stopped):
                value = self.evidence(statuses, stopped)
                checked = completion.validate(self.run, dict(value, status='untrusted'))
                self.assertEqual(checked['status'], expected)
                checked['steps']['git']['evidence']['commit'] = 'changed'
                self.assertEqual(value['steps']['git']['evidence'], {'commit': 'saved'})

    def test_evidence_requires_exact_identity_context_steps_and_boolean_stop_flag(self):
        invalid = [None, [], dict(self.evidence(), context={}),
                   dict(self.evidence(), operations_stopped=1), dict(self.evidence(), steps=[]),
                   dict(self.evidence(), steps={}),
                   dict(self.evidence(), extra='x' * 262144)]
        for key in completion.KEYS:
            value = self.evidence()
            del value[key]
            invalid.append(value)
        for step in [None, {}, dict(status='pending', evidence={'saved': True}, operation_id='op'),
                     dict(status='succeeded', evidence={}, operation_id='op'),
                     dict(status='succeeded', evidence=[], operation_id='op'),
                     dict(status='succeeded', evidence={'saved': True}, operation_id='Invalid')]:
            value = self.evidence()
            value['steps']['delivery'] = step
            invalid.append(value)
        for number, value in enumerate(invalid):
            with self.subTest(case=number), self.assertRaises(TaskError):
                completion.validate(self.run, value)
        with self.assertRaisesRegex(TaskError, 'no completion contract'):
            completion.validate({}, self.evidence())

    def test_terminal_evidence_is_optional_for_legacy_runs_and_enforces_outcome(self):
        self.assertIsNone(completion.require_terminal({}))
        self.run['completion'] = self.evidence()
        self.assertEqual(completion.require_terminal(self.run, 'succeeded')['status'], 'succeeded')
        with self.assertRaisesRegex(TaskError, 'differs'):
            completion.require_terminal(self.run, 'failed')
        self.run['completion'] = self.evidence(stopped=False)
        with self.assertRaisesRegex(TaskError, 'partial or unknown'):
            completion.require_terminal(self.run)
