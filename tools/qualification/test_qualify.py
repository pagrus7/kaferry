"""Qualification must preserve failures, missing coverage and binary identity."""

import argparse
import hashlib
from contextlib import contextmanager
import json
import os
import subprocess
import threading
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.ci import verify_tests
from tools.qualification.fixtures import lab_lock
from tools.qualification.qualify import plan, report, run_suite, verify_replicated_tests, InconclusivePrecondition, valid_receipts
from tools.qualification.broker_probe import reconcile, records


class MatrixTest(unittest.TestCase):
    def setUp(self):
        identity = patch('tools.qualification.qualify.identity_platforms', return_value={
            'binary_platform': 'linux-amd64', 'execution_platform': 'linux-amd64'})
        identity.start()
        self.addCleanup(identity.stop)
        images = patch('tools.qualification.fixtures.fixture_images', return_value='fixture')
        images.start()
        self.addCleanup(images.stop)

    def test_reports_reject_missing_failed_duplicate_and_mismatched_runs(self):
        for problem in ('missing', 'failed', 'digest', 'commit', 'duplicate', None):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                cases = plan('pr')
                for index, case in enumerate(cases):
                    if problem == 'missing' and index == 0:
                        continue
                    row = dict(case, schema=1, status='failed' if problem == 'failed' and index == 0 else 'passed',
                               binary_sha256='other' if problem == 'digest' and index == 0 else 'a' * 64,
                               source_commit='other' if problem == 'commit' and index == 0 else 'b' * 40,
                               elapsed_seconds=1, requested_seconds=case.get('duration', 120) if case['suite'] == 'capture' else None)
                    folder = directory / str(index)
                    folder.mkdir()
                    (folder / 'manifest.json').write_text(json.dumps(row))
                if problem == 'duplicate':
                    folder = directory / 'duplicate'; folder.mkdir()
                    (folder / 'manifest.json').write_text(json.dumps(row))
                    with self.assertRaisesRegex(ValueError, 'duplicate'):
                        report(directory, 'pr')
                else:
                    self.assertEqual(0 if problem is None else 1, report(directory, 'pr'))
                    self.assertEqual('passed' if problem is None else 'incomplete',
                                     json.loads((directory / 'qualification.json').read_text())['status'])

    def test_release_identity_requires_clean_explicit_source(self):
        rows = [dict(case, status='passed', source_commit='b'*40, binary_sha256='a'*64,
                     working_tree_dirty=False) for case in plan('full')]
        self.assertTrue(valid_receipts(rows, 'full', 'b'*40, 'a'*64))
        rows[0].pop('working_tree_dirty')
        self.assertFalse(valid_receipts(rows, 'full', 'b'*40, 'a'*64))

    def test_replicated_precondition_reruns_once_and_keeps_each_attempt_under_one_lease(self):
        selected = 'ownership/zombie-worker'
        test = 'TestReplicatedZombieWorker'
        scenarios = [(['inconclusive', 'passed'], 'passed', None), (['passed-dirty'], 'passed', None),
                     (['inconclusive', 'inconclusive'], 'inconclusive', InconclusivePrecondition),
                     (['product'], 'failed', RuntimeError), (['cancel'], 'interrupted', InterruptedError),
                     (['cleanup'], 'failed', RuntimeError), (['safety'], 'failed', ValueError),
                     (['other-fail'], 'failed', ValueError), (['identity'], 'failed', ValueError),
                     (['cancel-after-precondition'], 'interrupted', InterruptedError), (['dirty-source'], 'failed', ValueError),
                     (['untracked-added'], 'failed', ValueError), (['untracked-before'], 'failed', ValueError),
                     (['passed-untracked'], 'passed', None)]
        for outcomes, expected_status, error in scenarios:
            with self.subTest(outcomes=outcomes), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                binary = root / 'candidate'
                binary.write_bytes(b'fixture')
                args = argparse.Namespace(suite='fault-replicated', source='4.4.0', destination='4.4.0',
                                          output=root / 'evidence', binary=str(binary), config=None,
                                          duration=60, case=selected)
                sequence, runs = [], []
                cancelled = threading.Event()
                checkout = root/'checkout'
                checkout.mkdir()
                tracked_source, untracked_source = checkout/'fixture.go', checkout/'extra_test.go'
                tracked_source.write_text('first fixture')
                for command in (['git', 'init', '-q'], ['git', 'add', 'fixture.go'],
                                ['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']):
                    subprocess.run(command, cwd=checkout, check=True, capture_output=True)
                if outcomes[0] in ('dirty-source', 'passed-dirty'):
                    tracked_source.write_text('dirty fixture')
                if outcomes[0] in ('untracked-before', 'passed-untracked'):
                    untracked_source.write_text('first fixture')
                def capture(command):
                    if command[0] == 'git':
                        return subprocess.check_output(command, cwd=checkout, text=True).strip()
                    return 'fixture'
                @contextmanager
                def fixture(attempt, spec, receipt, cancelled, *, lease_held):
                    self.assertTrue(lease_held)
                    with self.assertRaisesRegex(ValueError, 'owns this host'):
                        with lab_lock():
                            pass
                    number = len(runs)
                    sequence.append(('start', attempt.output.name))
                    try:
                        yield
                    finally:
                        sequence.append(('cleanup', attempt.output.name))
                        if outcomes[number] == 'cancel-after-precondition':
                            cancelled.set()
                        if outcomes[number] == 'cleanup':
                            raise RuntimeError('writer shutdown failed')
                def command(command, output, label, env, **kwargs):
                    number = len(runs)
                    runs.append((output, env.copy()))
                    outcome = outcomes[number]
                    action = 'pass' if outcome in ('passed', 'passed-dirty', 'passed-untracked') else 'skip'
                    events = [dict(Action=action, Test=test), dict(Action='pass')]
                    if outcome == 'other-fail':
                        events.append(dict(Action='fail', Test='UnrelatedCleanup'))
                    (output/'suite.log').write_text(''.join(json.dumps(event)+'\n' for event in events))
                    if outcome in ('untracked-added', 'untracked-before'):
                        untracked_source.write_text('changed fixture')
                    if outcome == 'dirty-source':
                        tracked_source.write_text('changed fixture')
                    if outcome == 'identity':
                        Path(args.binary).write_bytes(b'changed candidate')
                    if outcome == 'product':
                        raise RuntimeError('product assertion failed')
                    if outcome == 'cancel':
                        raise InterruptedError('cancelled')
                def collect(*unused):
                    if outcomes[len(runs)-1] == 'safety':
                        raise ValueError('unsafe saved prefix')
                    return [dict(id=selected, status='passed' if outcomes[len(runs)-1] in ('passed', 'passed-dirty', 'passed-untracked') else 'inconclusive')]
                # Given a proved pre-fault zombie attempt or an independent failure,
                # when qualification finishes, then only the former can retry.
                with patch('tools.qualification.fixtures.LAB_LOCK', root/'host.lock'), \
                     patch('tools.qualification.qualify.capture', side_effect=capture), \
                     patch('tools.qualification.fault_results.configure', side_effect=lambda env: env.update(KAFERRY_ORACLE_RUN=os.urandom(4).hex())), \
                     patch('tools.qualification.fixtures.broker_fixture', side_effect=fixture), \
                     patch('tools.qualification.run_io.logged', side_effect=command), \
                     patch('tools.qualification.replicated_results.collect', side_effect=collect):
                    if error:
                        with self.assertRaises(error):
                            run_suite(args, cancelled=cancelled)
                    else:
                        run_suite(args, cancelled=cancelled)
                receipt = json.loads((args.output/'manifest.json').read_text())
                self.assertEqual(expected_status, receipt['status'])
                self.assertEqual(len(outcomes), len(runs))
                self.assertEqual([item for number in range(1, len(runs)+1) for item in
                                  [('start', f'attempt-{number}'), ('cleanup', f'attempt-{number}')]], sequence)
                self.assertEqual(len(runs), len({env['KAFERRY_REPLICATED_RUN_ID'] for _, env in runs}))
                for number, (output, env) in enumerate(runs, 1):
                    attempt = json.loads((output/'manifest.json').read_text())
                    self.assertEqual(f'attempt-{number}', output.name)
                    self.assertEqual(attempt['status'], receipt['attempts'][number-1]['status'])
                    self.assertEqual(hashlib.sha256(b'fixture').hexdigest(), attempt['binary_sha256'])
                    self.assertEqual(outcomes[0] in ('dirty-source', 'passed-dirty', 'untracked-before', 'passed-untracked'), attempt['working_tree_dirty'])
                    self.assertTrue((output/'suite.log').is_file())
                if outcomes[0] in ('dirty-source', 'untracked-added', 'untracked-before'):
                    # Then changed tracked or untracked source with the same HEAD
                    # cannot start a second fixture or erase the first receipt.
                    first, second = receipt['attempts']
                    changed_source = tracked_source if outcomes == ['dirty-source'] else untracked_source
                    self.assertEqual('changed fixture', changed_source.read_text())
                    self.assertEqual(first['source_commit'], second['source_commit'])
                    self.assertEqual(outcomes != ['untracked-added'], first['working_tree_dirty'])
                    self.assertTrue(second['working_tree_dirty'])
                    self.assertEqual(('inconclusive', 'failed'), (first['status'], second['status']))
                    self.assertIn('requires a clean unchanged checkout', receipt['error'])
                if len(runs) == 2:
                    self.assertEqual('inconclusive', receipt['attempts'][0]['status'])
                    self.assertIn('"Action": "skip"', (runs[0][0]/'suite.log').read_text())

    def test_network_skips_need_exact_typed_proof_and_no_other_go_failures(self):
        test = 'TestReplicatedNetworkFaults/latency'
        proof = [dict(id='network/latency', status='inconclusive')]
        skipped = '\n'.join(json.dumps(event) for event in [dict(Action='skip', Test=test),
                                                         dict(Action='pass', Test='TestReplicatedNetworkFaults')])
        # Given a typed no-fault outcome, when a different test failed or the
        # selected leaf did not skip, then no marker can turn it into a retry.
        with patch('tools.qualification.qualify.verify_tests') as verify:
            self.assertEqual('inconclusive', verify_replicated_tests(skipped, [test], proof))
            verify.assert_not_called()
        with self.assertRaisesRegex(ValueError, 'Required tests did not pass: OtherCase'):
            verify_replicated_tests(skipped, [test, 'OtherCase'], proof)
        passed = json.dumps(dict(Action='pass', Test=test))
        with patch('tools.qualification.qualify.verify_tests', wraps=verify_tests) as verify:
            self.assertEqual('passed', verify_replicated_tests(passed, [test], []))
            verify.assert_called_once_with(passed, [test], suite='fault-replicated')
        for log, cases in [(skipped, []), ('', proof), (skipped+'\n'+json.dumps(dict(Action='fail', Test='Cleanup')), proof),
                           (passed, proof), (json.dumps(dict(Action='skip', Test=test)), proof),
                           (skipped+'\n'+json.dumps(dict(Action='skip', Test='Unexpected')), proof),
                           (skipped+'\n'+json.dumps(dict(Action='skip', Package='unexpected/package')), proof),
                           (skipped+'\n'+json.dumps(dict(Action='skip')), proof),
                           (skipped, proof*2)]:
            with self.subTest(log=log), self.assertRaises(ValueError):
                verify_replicated_tests(log, [test], cases)


class Message:
    def __init__(self, record): self.record = record
    def headers(self): return self.record['headers']
    def key(self): return self.record['key']
    def value(self): return self.record['value']
    def timestamp(self): return 1, self.record['timestamp_ms']
    def partition(self): return self.record['partition']


class ReconciliationTest(unittest.TestCase):
    def test_partial_phase_requires_exact_count_and_source_prefixes(self):
        expected = records(24)
        self.assertEqual(7, reconcile([Message(r) for r in expected[:7]], expected, True, 7)['verified'])
        with self.assertRaisesRegex(ValueError, 'expected 7'):
            reconcile([Message(r) for r in expected], expected, True, 7)
        with self.assertRaisesRegex(ValueError, 'prefix'):
            reconcile([Message(r) for r in expected[4:11]], expected, True, 7)


if __name__ == '__main__':
    unittest.main()
