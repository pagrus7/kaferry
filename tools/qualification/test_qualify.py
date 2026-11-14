"""Qualification must preserve failures, missing coverage and binary identity."""

import argparse
import hashlib
from contextlib import contextmanager, nullcontext
import json
import os
import subprocess
import threading
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.ci import verify_tests
from tools.qualification import qualify
from tools.qualification.qualify import identity_platforms, lab_lock, main, plan, report, resolve_run, run_suite, verify_replicated_tests, InconclusivePrecondition, suite_command, valid_receipts
from tools.qualification.broker_probe import logical, reconcile, records, replicated_topology, topic_ready


class MatrixTest(unittest.TestCase):
    def setUp(self):
        identity = patch('tools.qualification.qualify.identity_platforms', return_value={
            'binary_platform': 'linux-amd64', 'execution_platform': 'linux-amd64'})
        identity.start()
        self.addCleanup(identity.stop)
        images = patch('tools.qualification.qualify.fixture_images', return_value='fixture')
        images.start()
        self.addCleanup(images.stop)

    def suite_args(self, directory, suite, **options):
        binary = directory / 'candidate'
        binary.write_bytes(b'fixture')
        settings = dict(suite=suite, source='3.9.2', destination='3.9.2',
                        output=directory / 'evidence', binary=str(binary), config=None, duration=60)
        settings.update(options)
        return argparse.Namespace(**settings)

    def test_profiles_cover_directed_pairs_without_duplicates(self):
        for tier, count in [('pr', 6), ('nightly', 7), ('full', 31)]:
            cases = plan(tier)
            self.assertEqual(count, len(cases))
            self.assertEqual(count, len({c['id'] for c in cases}))
        self.assertIn(('4.4.0', '3.9.2'), {(c['source'], c['destination']) for c in plan('pr')})

    def test_recovery_runs_bounded_repetitions_separately_from_uptime(self):
        # Given a standalone recovery request, its default is two hours.
        with patch('tools.qualification.qualify.run_suite') as run:
            main(['run', 'recovery'])
        args = run.call_args.args[0]
        self.assertEqual(7200, args.duration)
        env = {}
        # When selecting tests, then allow a final bounded cycle and require execution.
        command, required = suite_command(args, env)
        self.assertIn('-timeout=7800s', command)
        self.assertEqual('7200s', env['KAFERRY_RECOVERY_DURATION'])
        self.assertEqual(['TestRecoveryBlockedOutput'], required)
        args.duration = 86401
        with self.assertRaisesRegex(ValueError, '24h'):
            suite_command(args, {})



    def test_dedicated_endurance_defaults_and_short_soak_remain_separate(self):
        for suite, seconds, rate in [('endurance', 86400, 200), ('soak', 120, 50)]:
            with patch('tools.qualification.qualify.run_suite') as run:
                main(['run', suite])
            args = run.call_args.args[0]
            self.assertEqual((seconds, rate), (args.duration, args.rate))
            env = {'KAFERRY_SOAK_PROFILE': 'inherited', 'KAFERRY_SOAK_TOPOLOGY': 'replicated'}
            command, _ = suite_command(args, env)
            self.assertIn(f'-timeout={seconds+310}s', command)
            self.assertEqual('application' if suite == 'endurance' else None, env.get('KAFERRY_SOAK_PROFILE'))
            self.assertEqual('single' if suite == 'endurance' else None, env.get('KAFERRY_SOAK_TOPOLOGY'))
            if suite == 'endurance':
                self.assertEqual('random', env['KAFERRY_ARCHIVE_PAYLOAD'])
                self.assertEqual('kaferry-endurance-kafka-dr-1', env['KAFERRY_SOAK_DEST_STALL_CONTAINER'])
        with self.assertRaisesRegex(ValueError, 'fixed'):
            suite_command(argparse.Namespace(suite='endurance', duration=60, rate=50), {})

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

    def test_suite_failure_survives_cleanup_and_records_binary_identity(self):
        for failure, log_error, status in [
                (KeyboardInterrupt(), None, 'interrupted'),
                (RuntimeError('original suite failure'), subprocess.TimeoutExpired('docker logs', 30), 'failed')]:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                # Given a broker suite that fails, with diagnostics that may also time out.
                args = self.suite_args(Path(temporary), 'broker', destination='4.4.0')
                calls = []
                def command(command, output, label, env, **kwargs):
                    calls.append(label)
                    if label == 'suite': raise failure
                with patch('tools.qualification.qualify.lab_lock', nullcontext), patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run', side_effect=log_error):
                    # When cleanup runs, then it preserves the original exception and evidence.
                    with self.assertRaises(type(failure)) as raised:
                        run_suite(args)
                self.assertIs(failure, raised.exception)
                self.assertEqual(['brokers-up', 'suite', 'brokers-down'], calls)
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual(status, receipt['status'])
                self.assertEqual(64, len(receipt['binary_sha256']))
                if log_error is not None:
                    self.assertEqual('original suite failure', receipt['suite_error'])
                    self.assertEqual('logs', receipt['cleanup_errors'][0]['phase'])

    def test_binary_resolution_failure_retains_failure_evidence(self):
        # Given an endurance candidate path that cannot resolve.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'candidate'
            resolve = Path.resolve
            def resolve_candidate(path, *positional, **keyword):
                if path == binary: raise OSError('candidate resolution failed')
                return resolve(path, *positional, **keyword)
            args = argparse.Namespace(suite='endurance', source='3.9.2', destination='3.9.2',
                output=root / 'evidence', binary=str(binary), config=None, duration=60, rate=200)
            with patch.object(Path, 'resolve', resolve_candidate), patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.qualify.logged') as execute:
                # When resolution fails, then retain the failed receipt and publication without starting fixtures.
                with self.assertRaisesRegex(OSError, 'candidate resolution failed'): run_suite(args)
            execute.assert_not_called()
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('failed', receipt['status'])
            self.assertIn('finished_at', receipt)
            published = json.loads((args.output / 'publication/endurance-results.json').read_text())
            self.assertEqual('failed', published['manifest']['status'])

    def test_release_identity_requires_clean_explicit_source(self):
        rows = [dict(case, status='passed', source_commit='b'*40, binary_sha256='a'*64,
                     working_tree_dirty=False) for case in plan('full')]
        self.assertTrue(valid_receipts(rows, 'full', 'b'*40, 'a'*64))
        rows[0].pop('working_tree_dirty')
        self.assertFalse(valid_receipts(rows, 'full', 'b'*40, 'a'*64))

    def test_migration_requires_explicit_previous_binary(self):
        args = argparse.Namespace(suite='migration', previous_binary=None)
        with self.assertRaisesRegex(ValueError, 'previous-binary'):
            suite_command(args, {})

    def test_binary_and_checkout_platforms_are_independent_and_require_build_metadata(self):
        # Given a macOS candidate checked by a Linux Go toolchain.
        build = 'candidate: go1.27.3\n\tbuild\tGOOS=darwin\n\tbuild\tGOARCH=arm64\n'
        with patch('tools.qualification.qualify.capture', side_effect=[build, 'linux\namd64']):
            # When reading identity, then neither platform is inferred from the host name.
            self.assertEqual({'binary_platform': 'darwin-arm64', 'execution_platform': 'linux-amd64'},
                             identity_platforms('/candidate', 'go'))
        for malformed in ('', build.replace('GOOS=darwin', 'GOOS=windows'), build.replace('GOARCH=arm64', 'GOARCH=386')):
            with self.subTest(build=malformed), patch('tools.qualification.qualify.capture', side_effect=[malformed, 'linux\namd64']):
                with self.assertRaisesRegex(ValueError, 'platform metadata'):
                    identity_platforms('/candidate', 'go')

    def test_replicated_defaults_use_only_newest_images_and_owned_storage(self):
        # Given the replicated entrypoint, the newest broker image and disposable storage are selected.
        with patch('tools.qualification.qualify.run_suite') as run:
            main(['run', 'replicated'])
        requested = run.call_args.args[0]
        self.assertEqual(('4.4.0', '4.4.0'), (requested.source, requested.destination))
        with tempfile.TemporaryDirectory() as temporary:
            args = self.suite_args(Path(temporary), 'replicated', source='4.4.0', destination='4.4.0')
            receipt = {}
            with patch('tools.qualification.qualify.capture', return_value='fixture'):
                # When resolving, then every worker uses the dedicated replicated fixture.
                spec = resolve_run(args, receipt)
            self.assertIn('src/tests/replicated/compose.yaml', spec.compose)
            self.assertEqual('kaferry-replicated', receipt['fixture_project'])
            self.assertEqual('disposable-volume', receipt['fixture_storage']['mode'])
            self.assertRegex(receipt['fixture_storage']['run_id'], r'^[a-f0-9]{32}$')
            self.assertEqual('127.0.0.1:46092,127.0.0.1:46093,127.0.0.1:46094', spec.env['KAFKA_TEST_BROKERS'])
            self.assertEqual('127.0.0.1:47092,127.0.0.1:47093,127.0.0.1:47094', spec.env['KAFKA_TEST_DEST_BROKERS'])
            self.assertIn('--replicated', spec.command)
            self.assertEqual(dict(topology='replicated', brokers_per_cluster=3, replication_factor=3,
                                  min_insync_replicas=2, client_paths=4), receipt['parameters'])
            self.assertEqual('127.0.0.1:46192,127.0.0.1:46193,127.0.0.1:46194',
                             spec.command[spec.command.index('--copier-source') + 1])
            self.assertNotEqual(spec.env['KAFKA_TEST_BROKERS'], spec.env['KAFERRY_WORKER3_SOURCE_BROKERS'])

    def test_replicated_fault_case_binds_selected_cli_and_independent_oracle(self):
        # Given the ownership case, use the dedicated newest replicated fixture.
        with patch('tools.qualification.qualify.run_suite') as run:
            main(['run', 'fault-replicated', '--case', 'ownership/zombie-worker'])
        requested = run.call_args.args[0]
        self.assertEqual(('4.4.0', '4.4.0'), (requested.source, requested.destination))
        with tempfile.TemporaryDirectory() as temporary:
            args = self.suite_args(Path(temporary), 'fault-replicated', source='4.4.0', destination='4.4.0',
                                   case='ownership/zombie-worker')
            receipt = {}
            def configure(env):
                env.update(KAFERRY_ORACLE_PYTHON='/independent/python', KAFERRY_ORACLE_RUN='fresh')
            # When resolved, then no broker is started and both CLI workers will
            # receive the same checked candidate executable and independent oracle.
            with patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.fault_results.configure', side_effect=configure):
                spec = resolve_run(args, receipt)
            self.assertEqual(['ownership/zombie-worker'], receipt['case_selection'])
            self.assertIn('src/tests/replicated/compose.yaml', spec.compose)
            self.assertIn('-tags=integration,fault,load', spec.command)
            self.assertIn('^TestReplicatedZombieWorker$', spec.command)
            self.assertEqual(['TestReplicatedZombieWorker'], spec.expected)
            self.assertEqual(300, spec.timeout)
            self.assertEqual(str(Path(args.binary).resolve()), spec.env['KAFERRY_TEST_BINARY'])
            self.assertEqual(receipt['binary_sha256'], spec.env['KAFERRY_EXPECT_BINARY_SHA256'])
            self.assertEqual('fresh', receipt['fault_oracle_run'])
            self.assertNotEqual(spec.env['KAFKA_TEST_BROKERS'], spec.env['KAFERRY_WORKER1_SOURCE_BROKERS'])
            self.assertNotEqual(spec.env['KAFERRY_WORKER1_SOURCE_BROKERS'], spec.env['KAFERRY_WORKER2_SOURCE_BROKERS'])

    def test_replicated_fault_case_selector_rejects_other_suites_and_unknown_cases(self):
        # Given an unsupported case or topology, when resolved, then no external command runs.
        with tempfile.TemporaryDirectory() as temporary:
            for suite, case, source in [('broker', 'ownership/zombie-worker', '4.4.0'),
                                        ('fault-replicated', 'not-built', '4.4.0'),
                                        ('fault-replicated', 'ownership/zombie-worker', '3.9.2')]:
                with self.subTest(suite=suite, case=case, source=source):
                    args = self.suite_args(Path(temporary), suite, case=case, source=source, destination='4.4.0')
                    with patch('tools.qualification.qualify.capture') as execute, self.assertRaises(ValueError):
                        resolve_run(args, {})
                    execute.assert_not_called()

    def test_network_precondition_reruns_once_and_keeps_each_attempt_under_one_lease(self):
        selected = 'network/destination-leaders-outage'
        test = 'TestReplicatedNetworkFaults/destination-leaders-outage'
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
                args = self.suite_args(root, 'fault-replicated', source='4.4.0', destination='4.4.0', case=selected)
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
                    events = [dict(Action=action, Test=test), dict(Action='pass', Test='TestReplicatedNetworkFaults'), dict(Action='pass')]
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
                # Given a completed low-rate attempt or an independent failure,
                # when qualification finishes, then only the former can retry.
                with patch('tools.qualification.qualify.LAB_LOCK', root/'host.lock'), \
                     patch('tools.qualification.qualify.capture', side_effect=capture), \
                     patch('tools.qualification.fault_results.configure', side_effect=lambda env: env.update(KAFERRY_ORACLE_RUN=os.urandom(4).hex())), \
                     patch('tools.qualification.qualify.broker_fixture', side_effect=fixture), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
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

    def test_replicated_broker_selection_and_all_case_budget(self):
        # Given each implemented broker case, when selected, then run its exact
        # subtest and retain its complete119s recovery window in both deadlines.
        from tools.ci.suites import REPLICATED_CASES
        with tempfile.TemporaryDirectory() as temporary:
            for selected in [*REPLICATED_CASES, None]:
                with self.subTest(selected=selected):
                    args = self.suite_args(Path(temporary), 'fault-replicated', source='4.4.0', destination='4.4.0', case=selected)
                    receipt = {}
                    with patch('tools.qualification.qualify.capture', return_value='fixture'), \
                         patch('tools.qualification.fault_results.configure', side_effect=lambda env: env.update(KAFERRY_ORACLE_RUN='fresh')):
                        spec = resolve_run(args, receipt)
                    expected = [REPLICATED_CASES[selected]] if selected else list(REPLICATED_CASES.values())
                    self.assertEqual(expected, spec.expected)
                    self.assertEqual([selected] if selected else list(REPLICATED_CASES), receipt['case_selection'])
                    seconds = (240 if selected == 'ownership/zombie-worker' else 353 if selected and selected.startswith('network/') else 389 if selected == 'ownership/coordinator-kill-rebalance' else 359 if selected else 4907)
                    self.assertEqual(seconds+60, spec.timeout)
                    self.assertIn('-timeout='+('4m' if selected == 'ownership/zombie-worker' else str(seconds)+'s'), spec.command)
                    if selected:
                        self.assertEqual('/'.join('^'+part+'$' for part in expected[0].split('/')), spec.command[spec.command.index('-run')+1])


class Message:
    def __init__(self, record): self.record = record
    def headers(self): return self.record['headers']
    def key(self): return self.record['key']
    def value(self): return self.record['value']
    def timestamp(self): return 1, self.record['timestamp_ms']
    def partition(self): return self.record['partition']


class ReconciliationTest(unittest.TestCase):
    def test_replicated_metadata_rejects_missing_brokers_isr_and_reused_clusters(self):
        # Given live-looking metadata, reject topologies that cannot establish the replicated fixture.
        source = argparse.Namespace(cluster_id='source', brokers={1: None, 2: None, 3: None})
        destination = argparse.Namespace(cluster_id='destination', brokers={1: None, 2: None, 3: None})
        self.assertEqual(3, replicated_topology(source, destination)['source_brokers'])
        for mutation in ('brokers', 'cluster'):
            bad = argparse.Namespace(cluster_id='source' if mutation == 'cluster' else 'destination',
                                     brokers={1: None} if mutation == 'brokers' else destination.brokers)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, 'three-broker'):
                replicated_topology(source, bad)
        partition = dict(error=None, leader=1, replicas=[1, 2, 3], isrs=[1, 2, 3])
        def metadata(**change):
            return argparse.Namespace(error=None, partitions={0: argparse.Namespace(**dict(partition, **change))})
        self.assertTrue(topic_ready(metadata(), 1, replicated=True))
        # When leader, replication or ISR is wrong, then publishing cannot begin.
        for mutation in ({'leader': -1}, {'replicas': [1]}, {'isrs': [1, 2]}, {'isrs': [1, 2, 4]}):
            with self.subTest(mutation=mutation):
                self.assertFalse(topic_ready(metadata(**mutation), 1, replicated=True))

    def test_detects_loss_duplicate_corruption_routing_and_null_changes(self):
        expected = records(24)
        messages = [Message(r.copy()) for r in expected]
        self.assertEqual(24, reconcile(messages, expected, True)['verified'])
        for problem in ('missing', 'duplicate', 'value', 'null', 'headers', 'partition', 'timestamp_ms'):
            with self.subTest(problem=problem):
                actual = [Message(r.copy()) for r in expected]
                if problem == 'missing': actual.pop()
                elif problem == 'duplicate': actual.append(actual[0])
                else:
                    actual[0].record[{'null': 'key'}.get(problem, problem)] = {
                        'value': b'changed', 'null': b'', 'headers': [], 'partition': 3, 'timestamp_ms': 0}[problem]
                with self.assertRaises(ValueError):
                    reconcile(actual, expected, True)

    def test_partial_phase_requires_exact_count_and_source_prefixes(self):
        expected = records(24)
        self.assertEqual(7, reconcile([Message(r) for r in expected[:7]], expected, True, 7)['verified'])
        with self.assertRaisesRegex(ValueError, 'expected 7'):
            reconcile([Message(r) for r in expected], expected, True, 7)
        with self.assertRaisesRegex(ValueError, 'prefix'):
            reconcile([Message(r) for r in expected[4:11]], expected, True, 7)

    def test_binary_json_preserves_null_empty_and_ordered_duplicate_headers(self):
        self.assertNotEqual(logical(None, None, [], 0), logical(b'', b'', [], 0))
        self.assertNotEqual(logical(None, None, [('dup', None), ('dup', b'')], 0),
                            logical(None, None, [('dup', b''), ('dup', None)], 0))


class FixtureCapacityTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name)
        execute = patch('tools.qualification.qualify.subprocess.run')
        self.execute = execute.start()
        self.addCleanup(execute.stop)

    def rejected_attempts(self, message):
        with self.assertRaisesRegex(ValueError, message):
            qualify.fixture_capacity(self.output, {})
        self.assertEqual(3, self.execute.call_count)
        records = [json.loads(path.read_text()) for path in sorted(self.output.glob('docker-info-attempt-*.json'))]
        self.assertEqual(3, len(records))
        self.assertFalse((self.output / 'capacity.json').exists())
        for call, record in zip(self.execute.call_args_list, records, strict=True):
            self.assertEqual(['docker', 'info', '--format', '{{json .}}'], call.args[0])
            self.assertEqual(60, call.kwargs['timeout'])
            self.assertTrue(call.kwargs['capture_output'])
            self.assertGreaterEqual(record['duration_seconds'], 0)
            self.assertTrue(record['error'])
        return records

    def test_timeout_is_explicit_and_keeps_partial_output(self):
        # Given repeated Docker timeouts with partial output.
        self.execute.side_effect = subprocess.TimeoutExpired('docker info', 60, output=b'partial', stderr=b'daemon busy')
        # When admission exhausts its attempts, then the cause and output survive.
        records = self.rejected_attempts('docker info.*timed out after 60 seconds')
        self.assertTrue(all(record['timed_out'] and record['exit_status'] is None for record in records))
        self.assertTrue(all(record['stdout'] == 'partial' and record['stderr'] == 'daemon busy' for record in records))

    def test_nonzero_exit_includes_stderr_even_with_json_stdout(self):
        # Given valid-looking output from a failed Docker command.
        self.execute.return_value = subprocess.CompletedProcess([], 7, b'{}', b'daemon refused the request')
        # When admission rejects it, then the exit status and stderr explain why.
        records = self.rejected_attempts('exited with code 7: daemon refused the request')
        self.assertTrue(all(record['exit_status'] == 7 and not record['timed_out'] for record in records))
        self.assertTrue(all(record['stderr'] == 'daemon refused the request' for record in records))

    def test_execution_failure_is_explicit_and_retained(self):
        # Given Docker cannot be executed, retain the cause instead of parsing empty output.
        self.execute.side_effect = FileNotFoundError('docker executable is unavailable')
        # When admission exhausts its attempts, then a ValueError identifies that cause.
        records = self.rejected_attempts('could not execute: docker executable is unavailable')
        self.assertTrue(all(record['exit_status'] is None and not record['timed_out'] for record in records))
        self.assertTrue(all(record['stderr'] == 'docker executable is unavailable' for record in records))

    def test_empty_invalid_and_non_object_bodies_fail_explicitly(self):
        # Given successful commands with unusable Docker-info bodies.
        for body, message in [(b'', 'empty output'), (b'not-json', 'unparsable output'), (b'[]', 'expected a JSON object')]:
            with self.subTest(body=body):
                self.execute.reset_mock()
                self.execute.return_value = subprocess.CompletedProcess([], 0, body, b'')
                # When all attempts fail to parse, then a ValueError replaces JSON decode errors.
                records = self.rejected_attempts(message)
                self.assertTrue(all(record['stdout'] == body.decode() and record['exit_status'] == 0 for record in records))

    def test_failure_then_success_records_both_attempts_before_admission(self):
        # Given a failed command followed by a usable Docker capacity snapshot.
        runtime = dict(NCPU=4, MemTotal=16 * 1024**3)
        self.execute.side_effect = [subprocess.TimeoutExpired('docker info', 60),
            subprocess.CompletedProcess([], 0, json.dumps(runtime).encode(), b'warning')]
        required = dict(required_cpus=2, required_runtime_memory_bytes=8 * 1024**3, required_evidence_bytes=1)
        with patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)):
            # When a retry succeeds, then capacity validation uses it and stops retrying.
            capacity = qualify.fixture_capacity(self.output, required)
        self.assertEqual(2, self.execute.call_count)
        first = json.loads((self.output / 'docker-info-attempt-1.json').read_text())
        second = json.loads((self.output / 'docker-info-attempt-2.json').read_text())
        self.assertTrue(first['timed_out'])
        self.assertEqual(('', '', None), (first['stdout'], first['stderr'], first['exit_status']))
        self.assertEqual((0, False, None), (second['exit_status'], second['timed_out'], second['error']))
        self.assertEqual(runtime, json.loads(second['stdout']))
        self.assertEqual('warning', second['stderr'])
        self.assertFalse((self.output / 'docker-info-attempt-3.json').exists())
        self.assertEqual((4, runtime['MemTotal']), (capacity['runtime_cpus'], capacity['runtime_memory_bytes']))
        self.assertEqual(capacity, json.loads((self.output / 'capacity.json').read_text()))


if __name__ == '__main__':
    unittest.main()
