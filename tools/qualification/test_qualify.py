"""Qualification must preserve failures, missing coverage and binary identity."""

import argparse
import hashlib
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import duration, endurance_capacity, lab_lock, logged, main, plan, report, resolve_run, run_suite, suite_command, valid_receipts, parallel_allowance, parallel_capacity, parallel_run
from tools.qualification.broker_probe import logical, reconcile, records


class MatrixTest(unittest.TestCase):
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

    def test_parallel_jobs_overlap_with_distinct_fixtures_and_combined_allowance(self):
        # Given two jobs sharing one host lease, require both to reach the barrier.
        barrier = threading.Barrier(2, timeout=2)
        observed = []
        def run(args, *, fixture, cancelled):
            args.output.mkdir()
            barrier.wait()
            observed.append((args.suite, fixture))
            (args.output / 'manifest.json').write_text(json.dumps({'suite': args.suite, 'status': 'passed'}))
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary)/'candidate'; binary.write_bytes(b'candidate')
            args = argparse.Namespace(output=Path(temporary)/'run', companion='recovery', duration=60,
                companion_duration=60, binary=str(binary), source='3.9.2', destination='3.9.2')
            with patch('tools.qualification.qualify.LAB_LOCK', Path(temporary)/'host.lock'), patch('tools.qualification.qualify.run_suite', side_effect=run):
                parallel_run(args)
            self.assertEqual(2, len(observed))
            self.assertNotEqual(observed[0][1]['project'], observed[1][1]['project'])
            self.assertFalse(set(observed[0][1]['ports']) & set(observed[1][1]['ports']))
            self.assertEqual(observed[0][1]['reserved_bytes'], observed[1][1]['reserved_bytes'])
            result = json.loads((args.output/'parallel.json').read_text())
            self.assertEqual('passed', result['status'])
            self.assertEqual(2, len(result['cases']))

    def test_parallel_failure_cancels_peer_and_preserves_both_receipts(self):
        barrier = threading.Barrier(2, timeout=2)
        def run(args, *, fixture, cancelled):
            args.output.mkdir()
            barrier.wait()
            status = 'failed' if args.suite == 'recovery' else 'interrupted'
            if args.suite != 'recovery': self.assertTrue(cancelled.wait(2))
            (args.output/'manifest.json').write_text(json.dumps({'suite': args.suite, 'status': status}))
            if args.suite == 'recovery': raise RuntimeError('injected failure')
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary)/'candidate'; binary.write_bytes(b'candidate')
            args = argparse.Namespace(output=Path(temporary)/'run', companion='recovery', duration=60,
                companion_duration=60, binary=str(binary), source='3.9.2', destination='3.9.2')
            with patch('tools.qualification.qualify.LAB_LOCK', Path(temporary)/'host.lock'), patch('tools.qualification.qualify.run_suite', side_effect=run):
                with self.assertRaisesRegex(RuntimeError, 'injected failure'): parallel_run(args)
            result = json.loads((args.output/'parallel.json').read_text())
            self.assertEqual('failed', result['status'])
            self.assertEqual({'failed', 'interrupted'}, {case['status'] for case in result['cases']})

    def test_interrupt_during_second_submission_cancels_started_worker(self):
        # Given an executor whose first job runs before the next submission is interrupted.
        with tempfile.TemporaryDirectory() as temporary, ThreadPoolExecutor(max_workers=2) as pool:
            root = Path(temporary)
            binary = root/'candidate'; binary.write_bytes(b'candidate')
            args = argparse.Namespace(output=root/'run', companion='recovery', duration=60,
                companion_duration=60, binary=str(binary), source='3.9.2', destination='3.9.2')
            started, stopped = threading.Event(), threading.Event()
            def run(args, *, fixture, cancelled):
                args.output.mkdir()
                started.set()
                if cancelled.wait(2): stopped.set()
                (args.output/'manifest.json').write_text(json.dumps({'suite': args.suite, 'status': 'interrupted'}))
            submit = pool.submit
            def interrupt(*args, **kwargs):
                if started.is_set(): raise KeyboardInterrupt()
                future = submit(*args, **kwargs)
                self.assertTrue(started.wait(2))
                return future
            # When submission is interrupted, then the first worker is cancelled before pool exit waits.
            with patch('tools.qualification.qualify.ThreadPoolExecutor', return_value=pool), patch.object(pool, 'submit', side_effect=interrupt), \
                 patch('tools.qualification.qualify.LAB_LOCK', root/'host.lock'), patch('tools.qualification.qualify.run_suite', side_effect=run):
                with self.assertRaises(KeyboardInterrupt): parallel_run(args)
            self.assertTrue(stopped.is_set(), 'started worker was left running after interruption')
            result = json.loads((args.output/'parallel.json').read_text())
            self.assertEqual('interrupted', result['status'])
            self.assertEqual({'interrupted', 'missing'}, {case['status'] for case in result['cases']})

    def test_parallel_capacity_rejects_space_that_only_fits_one_run(self):
        required = parallel_allowance(86400, 'capture', 86400)
        self.assertGreater(required, parallel_allowance(86400, 'recovery', 7200))
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            with patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=required-1)), \
                 patch('tools.qualification.qualify.subprocess.check_output', return_value=f'Filesystem Blocks Used Available Capacity Mounted\nfs 0 0 {required//1024+1} 0% /tmp\n'):
                with self.assertRaisesRegex(ValueError, 'combined'): parallel_capacity(required, output, ['docker'], {})
            self.assertEqual(required, json.loads((output/'parallel-capacity.json').read_text())['required_combined_bytes'])

    def test_parallel_child_uses_only_its_assigned_fixture_on_failure(self):
        # Given a child admitted by the coordinator with its own project and ports.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.suite_args(root, 'recovery', rate=50)
            fixture = dict(project='kaferry-lab-test-2', ports=[45292, 45293], reserved_bytes=1000,
                           binary_sha256=hashlib.sha256(Path(args.binary).read_bytes()).hexdigest())
            calls = []
            def command(command, output, label, env, **kwargs):
                calls.append((command, label, env.copy()))
                if label == 'suite': raise RuntimeError('injected failure')
            with patch('tools.qualification.qualify.capture', return_value='fixture'), patch('tools.qualification.qualify.parallel_capacity', return_value={}), \
                 patch('tools.qualification.qualify.lab_lock', side_effect=AssertionError('child reacquired coordinator lease')), \
                 patch('tools.qualification.qualify.logged', side_effect=command), patch('tools.qualification.qualify.subprocess.run') as logs:
                # When the suite fails, then diagnostics and teardown address only that project.
                with self.assertRaisesRegex(RuntimeError, 'injected failure'):
                    run_suite(args, fixture=fixture)
            for command, label, env in calls:
                if label != 'suite': self.assertEqual('kaferry-lab-test-2', command[command.index('-p') + 1])
                self.assertEqual('127.0.0.1:45292', env['KAFKA_TEST_BROKERS'])
                self.assertEqual('127.0.0.1:45293', env['KAFKA_TEST_DEST_BROKERS'])
                self.assertEqual('kaferry-lab-test-2-kafka-dr-1', env['KAFKA_FAULT_DEST_CONTAINER'])
                self.assertEqual('kaferry-lab-test-2', env['KAFERRY_FIXTURE_PROJECT'])
            self.assertEqual('kaferry-lab-test-2', logs.call_args.args[0][logs.call_args.args[0].index('-p') + 1])
            self.assertEqual('failed', json.loads((args.output/'manifest.json').read_text())['status'])

    def test_log_cancellation_stops_running_subprocess(self):
        with tempfile.TemporaryDirectory() as temporary:
            cancelled = threading.Event()
            timer = threading.Timer(0.2, cancelled.set)
            timer.start()
            started = time.monotonic()
            try:
                with self.assertRaises(InterruptedError):
                    logged([sys.executable, '-c', 'import time; time.sleep(30)'], Path(temporary),
                           'cancel', dict(os.environ), cancelled=cancelled)
            finally:
                timer.cancel()
            self.assertLess(time.monotonic()-started, 5)

    def test_host_lock_is_shared_across_checkouts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch('tools.qualification.qualify.LAB_LOCK', root / 'host.lock'), patch('tools.qualification.qualify.ROOT', root / 'checkout-a'):
                with lab_lock():
                    with patch('tools.qualification.qualify.ROOT', root / 'checkout-b'):
                        with self.assertRaisesRegex(ValueError, 'host'):
                            with lab_lock(): pass

    def test_duration_limits_and_units(self):
        self.assertEqual(259200, duration('72h'))
        self.assertEqual(604800, duration('7d'))
        for value in ('0h', '59s', '8d', '-1h', '1.5h', '1h30m', 'forever'):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                duration(value)

    def test_endurance_deadline_includes_ten_second_consume_only_period(self):
        for suite in ('soak', 'capture'):
            with self.subTest(suite=suite):
                args = argparse.Namespace(suite=suite, duration=259200, rate=50)
                command, required = suite_command(args, {})
                self.assertIn('-timeout=259510s', command)
                self.assertEqual(['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'], required)

        with tempfile.TemporaryDirectory() as temporary, patch('tools.qualification.qualify.capture', return_value='fixture'):
            root = Path(temporary)
            for suite, timeout in [('capture', 370), ('recovery', 660)]:
                with self.subTest(suite=suite):
                    spec = resolve_run(self.suite_args(root, suite, rate=50), {}, None)
                    self.assertEqual(timeout, spec.timeout)

    def test_dedicated_endurance_defaults_and_short_soak_remain_separate(self):
        for suite, seconds, rate in [('endurance', 86400, 200), ('soak', 120, 50)]:
            with patch('tools.qualification.qualify.run_suite') as run:
                main(['run', suite])
            args = run.call_args.args[0]
            self.assertEqual((seconds, rate), (args.duration, args.rate))
            env = {'KAFERRY_SOAK_PROFILE': 'inherited'}
            command, _ = suite_command(args, env)
            self.assertIn(f'-timeout={seconds+310}s', command)
            self.assertEqual('application' if suite == 'endurance' else None, env.get('KAFERRY_SOAK_PROFILE'))
            if suite == 'endurance':
                self.assertEqual('random', env['KAFERRY_ARCHIVE_PAYLOAD'])
                self.assertEqual('kaferry-endurance-kafka-dr-1', env['KAFERRY_SOAK_DEST_STALL_CONTAINER'])
        with self.assertRaisesRegex(ValueError, 'fixed'):
            suite_command(argparse.Namespace(suite='endurance', duration=60, rate=50), {})

    def test_endurance_requires_capacity_on_both_broker_filesystems_and_evidence_disk(self):
        for source, destination, evidence, success in [(10**12, 10**12, 10**12, True),
                (0, 10**12, 10**12, False), (10**12, 0, 10**12, False), (10**12, 10**12, 0, False)]:
            with self.subTest(source=source, destination=destination, evidence=evidence), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary)
                rows = [f'Filesystem 1024-blocks Used Available Capacity Mounted\nfs 0 0 {free//1024} 0% /tmp\n'
                        for free in (source, destination)]
                with patch('tools.qualification.qualify.subprocess.check_output', side_effect=rows), \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=evidence)):
                    if success:
                        capacity = endurance_capacity(86400, output, ['docker', 'compose'], {})
                        self.assertEqual(17856000, capacity['records'])
                    else:
                        with self.assertRaisesRegex(ValueError, 'insufficient'):
                            endurance_capacity(86400, output, ['docker', 'compose'], {})
                self.assertTrue((output / 'capacity.json').is_file(), 'failed preflight must retain evidence')

    def test_endurance_cleanup_owns_only_its_isolated_project(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            args = self.suite_args(directory, 'endurance', duration=1800, rate=200)
            calls = []
            def command(command, output, label, env, **kwargs):
                calls.append((command, label, env.copy()))
                if label == 'suite': raise KeyboardInterrupt()
            with patch('tools.qualification.qualify.lab_lock', nullcontext), patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.qualify.endurance_capacity', return_value={}), patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.subprocess.run') as diagnostics:
                with self.assertRaises(KeyboardInterrupt): run_suite(args)
            for command, label, env in calls:
                if label != 'suite': self.assertEqual('kaferry-endurance', command[command.index('-p') + 1])
                self.assertEqual('127.0.0.1:39192', env['KAFKA_TEST_BROKERS'])
                self.assertEqual('127.0.0.1:40192', env['KAFKA_TEST_DEST_BROKERS'])
                self.assertEqual('39192', env['KAFERRY_SOURCE_PORT'])
                self.assertEqual('40192', env['KAFERRY_DESTINATION_PORT'])
            self.assertEqual('kaferry-endurance', diagnostics.call_args.args[0][diagnostics.call_args.args[0].index('-p') + 1])
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('kaferry-endurance', receipt['fixture_project'])
            self.assertEqual('interrupted', receipt['status'])
            publication = args.output / 'publication'
            published = json.loads((publication / 'endurance-results.json').read_text())
            self.assertEqual('interrupted', published['manifest']['status'])
            self.assertEqual([], published['measurements'])
            self.assertTrue((publication / 'endurance-evidence.tar.gz').is_file())
            self.assertTrue((publication / 'endurance.SHA256SUMS').is_file())

    def test_report_error_does_not_replace_the_original_interruption(self):
        # Given an interrupted endurance run whose report directory cannot be written.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            args = self.suite_args(directory, 'endurance', rate=200)
            with patch('tools.qualification.qualify.lab_lock', nullcontext), patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.qualify.logged', side_effect=KeyboardInterrupt()), \
                 patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.endurance_results.prepare', side_effect=OSError('disk full')):
                # When finalization fails too, then the original interruption and receipt survive.
                with self.assertRaises(KeyboardInterrupt): run_suite(args)
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('interrupted', receipt['status'])
            self.assertEqual('interrupted', receipt['suite_error'])

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
                               elapsed_seconds=1)
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

    def test_teardown_failure_rejects_success_and_preserves_suite_failure(self):
        for suite_failure in (False, True):
            with self.subTest(suite_failure=suite_failure), tempfile.TemporaryDirectory() as temporary:
                # Given a recovery suite whose fixture teardown will fail.
                directory = Path(temporary)
                binary = directory / 'kaferry'; binary.write_bytes(b'fixture')
                args = argparse.Namespace(suite='recovery', source='3.9.2', destination='4.4.0',
                    output=directory / 'evidence', binary=str(binary), config=None, duration=60)
                original = vars(args).copy()
                def command(command, output, label, env, **kwargs):
                    if label == 'brokers-down': raise RuntimeError('teardown failure')
                    if label == 'suite':
                        if suite_failure: raise RuntimeError('original suite failure')
                        (output / 'suite.log').write_text(json.dumps({'Action': 'pass', 'Test': 'TestRecoveryBlockedOutput'}) + '\n')
                with patch('tools.qualification.qualify.LAB_LOCK', directory / 'host.lock'), patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.logged', side_effect=command), patch('tools.qualification.qualify.subprocess.run'):
                    # When cleanup fails, then success becomes failure and an existing failure remains primary.
                    message = 'original suite failure' if suite_failure else 'fixture diagnostics or teardown failed'
                    with self.assertRaisesRegex(RuntimeError, message): run_suite(args)
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual('failed', receipt['status'])
                self.assertEqual('teardown', receipt['cleanup_errors'][0]['phase'])
                self.assertEqual(original, vars(args), 'execution mutated the caller request')

    def test_timeout_kills_stdout_inheriting_descendant_after_parent_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            marker = directory / 'group'
            child = "import os,signal,sys,time; from pathlib import Path; signal.signal(signal.SIGTERM, signal.SIG_IGN); Path(sys.argv[1]).write_text(str(os.getpgrp())); print('descendant ready',flush=True); time.sleep(30)"
            parent = ("import subprocess,sys,time; from pathlib import Path; "
                      f"subprocess.Popen([sys.executable,'-c',{child!r},{str(marker)!r}]); "
                      f"marker=Path({str(marker)!r})\n"
                      "while not marker.exists(): time.sleep(0.01)\n"
                      "print('parent exited',flush=True)")
            def emergency_cleanup():
                if marker.exists():
                    try: os.killpg(int(marker.read_text()), signal.SIGKILL)
                    except ProcessLookupError: pass
            watchdog = threading.Timer(8, emergency_cleanup)
            watchdog.daemon = True; watchdog.start()
            start = time.monotonic()
            try:
                with patch('tools.qualification.qualify.STOP_GRACE', 0.1), patch('builtins.print'):
                    with self.assertRaisesRegex(RuntimeError, 'exceeded'):
                        logged([sys.executable, '-c', parent], directory, 'descendant', dict(os.environ), timeout=2)
                self.assertIn('parent exited', (directory / 'descendant.log').read_text())
                self.assertIn('descendant ready', (directory / 'descendant.log').read_text())
                self.assertLess(time.monotonic()-start, 6)
            finally:
                watchdog.cancel(); emergency_cleanup()

    def test_release_identity_requires_clean_explicit_source(self):
        rows = [dict(case, status='passed', source_commit='b'*40, binary_sha256='a'*64,
                     working_tree_dirty=False) for case in plan('full')]
        self.assertTrue(valid_receipts(rows, 'full', 'b'*40, 'a'*64))
        rows[0].pop('working_tree_dirty')
        self.assertFalse(valid_receipts(rows, 'full', 'b'*40, 'a'*64))

    def test_external_and_migration_require_explicit_inputs(self):
        args = argparse.Namespace(suite='migration', previous_binary=None)
        with self.assertRaisesRegex(ValueError, 'previous-binary'):
            suite_command(args, {})


class Message:
    def __init__(self, record): self.record = record
    def headers(self): return self.record['headers']
    def key(self): return self.record['key']
    def value(self): return self.record['value']
    def timestamp(self): return 1, self.record['timestamp_ms']
    def partition(self): return self.record['partition']


class ReconciliationTest(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
