"""Qualification must preserve failures, missing coverage and binary identity."""

import argparse
import hashlib
from contextlib import contextmanager, nullcontext
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
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.qualification.qualify import broker_fixture, cleanup_container_state, cleanup_replicated, duration, endurance_capacity, identity_platforms, lab_lock, logged, main, plan, replicated_admission, replicated_storage, report, resolve_run, run_suite, verify_replicated_tests, InconclusivePrecondition, suite_command, valid_receipts, parallel_allowance, parallel_capacity, parallel_run
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
        cleanup_state = patch('tools.qualification.qualify.cleanup_container_state')
        cleanup_state.start()
        self.addCleanup(cleanup_state.stop)

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

    def test_only_pr_capture_sets_a_shorter_duration(self):
        # Given each tier, when planning, then only PR capture requests one minute.
        for tier in ('pr', 'nightly', 'full'):
            with self.subTest(tier=tier):
                cases = plan(tier)
                capture = next(case for case in cases if case['suite'] == 'capture')
                self.assertEqual(60 if tier == 'pr' else None, capture.get('duration'))
                self.assertTrue(all('duration' not in case for case in cases if case['suite'] != 'capture'))

    def test_matrix_uses_planned_duration_and_preserves_explicit_override(self):
        # Given local matrix requests, when run, then duration defaults match CI's plan.
        for tier, override, expected in [('pr', [], 60), ('nightly', [], 120),
                                          ('full', [], 120), ('pr', ['--duration', '90s'], 90)]:
            with self.subTest(tier=tier, override=override), tempfile.TemporaryDirectory() as temporary:
                with patch('tools.qualification.qualify.run_suite') as run, patch('tools.qualification.qualify.report', return_value=0):
                    main(['matrix', '--tier', tier, '--output', str(Path(temporary) / 'matrix'), *override])
                capture = next(call.args[0] for call in run.call_args_list if call.args[0].suite == 'capture')
                self.assertEqual(expected, capture.duration)

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

    @patch('tools.qualification.fault_results.configure', new=lambda env: env.update(KAFERRY_ORACLE_RUN='fixture'))
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

    @patch('tools.qualification.fault_results.configure', new=lambda env: env.update(KAFERRY_ORACLE_RUN='fixture'))
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

    @patch('tools.qualification.fault_results.configure', new=lambda env: env.update(KAFERRY_ORACLE_RUN='fixture'))
    @patch('tools.qualification.fault_results.collect', new=lambda *args: {})
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
        # Given the replicated entrypoint, no broker version or external storage is implicit.
        with patch('tools.qualification.qualify.run_suite') as run:
            main(['run', 'replicated'])
        requested = run.call_args.args[0]
        self.assertEqual(('4.4.0', '4.4.0'), (requested.source, requested.destination))
        with tempfile.TemporaryDirectory() as temporary:
            args = self.suite_args(Path(temporary), 'replicated', source='4.4.0', destination='4.4.0')
            receipt = {}
            with patch('tools.qualification.qualify.capture', return_value='fixture'), patch.dict(os.environ, {
                    'KAFERRY_REPLICATED_BROKER_VOLUME': 'unrequested', 'KAFERRY_REPLICATED_VOLUME_EXTERNAL': 'true'}):
                # When resolving, then the fixture cannot inherit someone else's volume.
                spec = resolve_run(args, receipt, None)
            self.assertIn('src/tests/replicated/compose.yaml', spec.compose)
            self.assertEqual('kaferry-replicated', receipt['fixture_project'])
            self.assertEqual('disposable-volume', receipt['fixture_storage']['mode'])
            self.assertRegex(receipt['fixture_storage']['run_id'], r'^[a-f0-9]{32}$')
            self.assertEqual('false', spec.env['KAFERRY_REPLICATED_VOLUME_EXTERNAL'])
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
                spec = resolve_run(args, receipt, None)
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
                        resolve_run(args, {}, None)
                    execute.assert_not_called()

    def test_network_precondition_reruns_once_and_keeps_each_attempt_under_one_lease(self):
        selected = 'network/destination-leaders-outage'
        test = 'TestReplicatedNetworkFaults/destination-leaders-outage'
        scenarios = [(['inconclusive', 'passed'], 'passed', None),
                     (['inconclusive', 'inconclusive'], 'inconclusive', InconclusivePrecondition),
                     (['product'], 'failed', RuntimeError), (['cancel'], 'interrupted', InterruptedError),
                     (['cleanup'], 'failed', RuntimeError), (['safety'], 'failed', ValueError),
                     (['other-fail'], 'failed', ValueError), (['identity'], 'failed', ValueError),
                     (['cancel-after-precondition'], 'interrupted', InterruptedError)]
        for outcomes, expected_status, error in scenarios:
            with self.subTest(outcomes=outcomes), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                args = self.suite_args(root, 'fault-replicated', source='4.4.0', destination='4.4.0', case=selected)
                sequence, runs = [], []
                cancelled = threading.Event()
                @contextmanager
                def fixture(attempt, spec, receipt, assigned, cancelled, *, lease_held):
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
                    action = 'pass' if outcome == 'passed' else 'skip'
                    events = [dict(Action=action, Test=test), dict(Action='pass', Test='TestReplicatedNetworkFaults'), dict(Action='pass')]
                    if outcome == 'other-fail':
                        events.append(dict(Action='fail', Test='UnrelatedCleanup'))
                    (output/'suite.log').write_text(''.join(json.dumps(event)+'\n' for event in events))
                    if outcome == 'identity':
                        Path(args.binary).write_bytes(b'changed candidate')
                    if outcome == 'product':
                        raise RuntimeError('product assertion failed')
                    if outcome == 'cancel':
                        raise InterruptedError('cancelled')
                def collect(*unused):
                    if outcomes[len(runs)-1] == 'safety':
                        raise ValueError('unsafe saved prefix')
                    return [dict(id=selected, status='passed' if outcomes[len(runs)-1] == 'passed' else 'inconclusive')]
                # Given a completed low-rate attempt or an independent failure,
                # when qualification finishes, then only the former can retry.
                with patch('tools.qualification.qualify.LAB_LOCK', root/'host.lock'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
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
                    self.assertTrue((output/'suite.log').is_file())
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
        verify_replicated_tests(skipped, [test], proof)
        for log, cases in [(skipped, []), ('', proof), (skipped+'\n'+json.dumps(dict(Action='fail', Test='Cleanup')), proof),
                           (json.dumps(dict(Action='pass', Test=test)), proof)]:
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
                        spec = resolve_run(args, receipt, None)
                    expected = [REPLICATED_CASES[selected]] if selected else list(REPLICATED_CASES.values())
                    self.assertEqual(expected, spec.expected)
                    self.assertEqual([selected] if selected else list(REPLICATED_CASES), receipt['case_selection'])
                    seconds = (240 if selected == 'ownership/zombie-worker' else 353 if selected and selected.startswith('network/') else 359 if selected else 3800)
                    self.assertEqual(seconds+60, spec.timeout)
                    self.assertIn('-timeout='+('4m' if selected == 'ownership/zombie-worker' else str(seconds)+'s'), spec.command)
                    if selected:
                        self.assertEqual('/'.join('^'+part+'$' for part in expected[0].split('/')), spec.command[spec.command.index('-run')+1])

    def test_broker_volume_rejects_unsupported_suites_names_and_old_replicated_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            # Given explicit storage requests, when invalid, then no Docker command is issued.
            for suite, volume, source in [('cloud', 'lab', '4.4.0'), ('replicated', '-foreign', '4.4.0'),
                                           ('replicated', 'lab', '3.9.2')]:
                with self.subTest(suite=suite, volume=volume, source=source):
                    args = self.suite_args(Path(temporary), suite, source=source, destination='4.4.0', broker_volume=volume)
                    with patch('tools.qualification.qualify.capture') as execute, self.assertRaises(ValueError):
                        resolve_run(args, {}, None)
                    execute.assert_not_called()

    def test_replicated_admission_rejects_resource_shortages_and_foreign_volumes(self):
        runtime = {'NCPU': 4, 'MemTotal': 16 * 1024**3}
        env = {'KAFERRY_REPLICATED_BROKER_VOLUME': 'lab-storage', 'KAFERRY_REPLICATED_VOLUME_EXTERNAL': 'true'}
        # Given an existing external volume, reject missing ownership before a storage helper can start.
        for labels, accepted in [(None, False), ({'kaferry.lab.storage': 'false'}, False),
                                  ({'kaferry.lab.storage': 'true'}, True)]:
            with self.subTest(labels=labels), tempfile.TemporaryDirectory() as temporary:
                with patch('tools.qualification.qualify.capture', side_effect=[json.dumps(runtime), json.dumps(labels)]), \
                     patch('tools.qualification.qualify.socket.socket') as sockets, \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)):
                    if accepted:
                        replicated_admission(Path(temporary), env)
                        self.assertEqual(25, sockets.return_value.__enter__.return_value.bind.call_count)
                    else:
                        with self.assertRaisesRegex(ValueError, 'label'):
                            replicated_admission(Path(temporary), env)
        # When CPU, memory or evidence space is inadequate, then preserve the rejected capacity snapshot.
        for cpu, memory, disk in [(1, 16 * 1024**3, 10**12), (4, 1024**3, 10**12), (4, 16 * 1024**3, 0)]:
            with self.subTest(cpu=cpu, memory=memory, disk=disk), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch('tools.qualification.qualify.capture', return_value=json.dumps({'NCPU': cpu, 'MemTotal': memory})), \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=disk)):
                    with self.assertRaisesRegex(ValueError, 'capacity'):
                        replicated_admission(root, env)
                self.assertTrue((root / 'capacity.json').is_file())
        with tempfile.TemporaryDirectory() as temporary:
            with patch('tools.qualification.qualify.capture', return_value=json.dumps(runtime)), \
                 patch('tools.qualification.qualify.socket.socket') as sockets, \
                 patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)):
                sockets.return_value.__enter__.return_value.bind.side_effect = OSError('already bound')
                with self.assertRaisesRegex(ValueError, 'port 46092'):
                    replicated_admission(Path(temporary), dict(env, KAFERRY_REPLICATED_VOLUME_EXTERNAL='false'))

    def test_replicated_failures_clean_only_owned_scope_and_preserve_unverified_storage(self):
        # Given an interruption or a failed storage ownership check after copying.
        for suite, interruption, volume in [(suite, interruption, volume) for suite in ('replicated', 'endurance')
                for interruption, volume in [(True, 'dedicated-storage'), (False, 'dedicated-storage'), (False, None)]]:
            with self.subTest(suite=suite, interruption=interruption, volume=volume), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                args = self.suite_args(root, suite, source='4.4.0', destination='4.4.0', broker_volume=volume,
                                       topology='replicated' if suite == 'endurance' else None, rate=200)
                calls = []
                def command(command, output, label, env, **kwargs):
                    calls.append((command, label, env.copy()))
                    if label == 'storage-capacity':
                        (output / 'storage-capacity.log').write_text('10000000\n')
                    if label == 'proxy-proof':
                        (output / 'proxy-proof.log').write_text('{"Action":"pass","Test":"TestReplicatedProxyRoutes"}\n')
                    if label == 'suite' and interruption:
                        raise KeyboardInterrupt()
                    if label == 'storage-clean' and not interruption:
                        raise RuntimeError('ownership mismatch')
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'host.lock'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.replicated_admission', return_value={'required_broker_bytes': 4 * 1024**3}) as admission, \
                     patch('tools.qualification.qualify.verify_run'), patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run'):
                    # When finishing, then writers stop before cleanup and a cleanup failure cannot qualify.
                    with self.assertRaises(KeyboardInterrupt if interruption else RuntimeError):
                        run_suite(args)
                self.assertEqual(60 if suite == 'endurance' else None, admission.call_args.args[2])
                phases = ['storage-capacity', 'storage-prepare', 'proxy-up', 'proxy-configure', 'brokers-up']
                if suite == 'replicated':
                    phases.append('proxy-proof')
                phases += ['suite', 'brokers-stop', 'storage-clean', 'brokers-down']
                self.assertEqual(phases, [label for _, label, _ in calls])
                self.assertEqual(1, len({env['KAFERRY_REPLICATED_RUN_ID'] for _, _, env in calls}))
                for command, label, env in calls:
                    self.assertEqual('true' if volume else 'false', env['KAFERRY_REPLICATED_VOLUME_EXTERNAL'])
                    if label not in ('suite', 'proxy-configure', 'proxy-proof'):
                        self.assertEqual('kaferry-replicated', command[command.index('-p') + 1])
                    self.assertNotEqual(['docker', 'volume', 'rm'], command[:3])
                if not interruption:
                    self.assertNotIn('-v', calls[-1][0], 'failed ownership validation must preserve even a local volume')
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual('interrupted' if interruption else 'failed', receipt['status'])
                if suite == 'endurance':
                    self.assertNotIn('proxy_path_check', receipt, 'endurance must not run the cut/heal proxy proof')
                    self.assertEqual('replicated', receipt['workload']['topology'])
                    self.assertEqual((3, 2), (receipt['workload']['replication_factor'], receipt['workload']['min_insync_replicas']))
                self.assertNotIn('dedicated-storage', json.dumps(receipt), 'keep private resource names out of receipts')

    def test_replicated_broker_space_is_admitted_before_preparing_or_starting_brokers(self):
        # Given a volume with less than the full fixture allowance.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            def command(command, directory, label, env, **kwargs):
                (directory / 'storage-capacity.log').write_text('1\n')
            with patch('tools.qualification.qualify.logged', side_effect=command) as execute:
                # When checking space, then preparation never runs and the rejected snapshot survives.
                with self.assertRaisesRegex(ValueError, 'broker space'):
                    replicated_storage(output, ['compose'], {}, {'required_broker_bytes': 4096}, None)
            self.assertEqual(1, execute.call_count)
            self.assertEqual(1024, json.loads((output / 'capacity.json').read_text())['broker_free_bytes'])

    def test_fault_diagnostics_join_before_teardown_without_changing_failure_or_endurance(self):
        # Given a replicated fault failure or an ordinary replicated endurance run.
        for suite in ('fault-replicated', 'endurance'):
            with self.subTest(suite=suite), tempfile.TemporaryDirectory() as temporary:
                root, events, receipt = Path(temporary), [], {}
                args = SimpleNamespace(suite=suite, output=root, topology='replicated', duration=60)
                spec = SimpleNamespace(compose=['docker', 'compose', '-p', 'owned'], env={})
                @contextmanager
                def observations(*_):
                    events.append('monitor-start')
                    state = {}
                    try:
                        yield state
                    finally:
                        state['status'] = 'completed'
                        events.append('monitor-stop')
                def command(_command, _output, label, *_args, **_kwargs):
                    events.append(label)
                    if label == 'proxy-proof':
                        (root / 'proxy-proof.log').write_text('{"Action":"pass","Test":"TestReplicatedProxyRoutes"}\n')
                def cleanup(*_):
                    events.append('cleanup')
                    return []
                failure = RuntimeError('original assertion')
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'host.lock'), \
                     patch('tools.qualification.qualify.replicated_admission', return_value={}), \
                     patch('tools.qualification.qualify.replicated_storage'), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.cleanup_replicated', side_effect=cleanup), \
                     patch('tools.qualification.qualify.subprocess.run'), \
                     patch('tools.qualification.qualify.runtime_observations', side_effect=observations) as observe:
                    # When the suite finishes, then diagnostics stop before any
                    # writer cleanup and cannot replace its original exception.
                    try:
                        with broker_fixture(args, spec, receipt, None, None):
                            events.append('suite')
                            if suite == 'fault-replicated':
                                raise failure
                    except RuntimeError as error:
                        self.assertIs(failure, error)
                if suite == 'fault-replicated':
                    self.assertEqual('original assertion', receipt['suite_error'])
                    self.assertEqual('completed', receipt['runtime_observations']['status'])
                    self.assertLess(events.index('monitor-start'), events.index('proxy-proof'))
                    self.assertLess(events.index('suite'), events.index('monitor-stop'))
                    self.assertLess(events.index('monitor-stop'), events.index('cleanup'))
                else:
                    observe.assert_not_called()
                    self.assertNotIn('runtime_observations', receipt)

    def test_replicated_uses_the_same_host_lease_before_any_fixture_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = self.suite_args(Path(temporary), 'replicated', source='4.4.0', destination='4.4.0')
            with patch('tools.qualification.qualify.LAB_LOCK', args.output.parent / 'host.lock'), \
                 patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.qualify.logged') as execute, lab_lock():
                # Given another coordinator owns the lease, when requested, then no fixture starts or stops.
                with self.assertRaisesRegex(ValueError, 'host'):
                    run_suite(args)
            execute.assert_not_called()


class ReplicatedStorageTest(unittest.TestCase):
    def storage(self, root, run, operation, **overrides):
        script = Path(__file__).resolve().parents[2] / 'src/tests/replicated/storage.sh'
        env = dict(os.environ, KAFERRY_STORAGE_ROOT=str(root), KAFERRY_FIXTURE_PROJECT='kaferry-replicated',
                   KAFERRY_REPLICATED_RUN_ID=run, KAFERRY_STORAGE_UID=str(os.getuid()), KAFERRY_STORAGE_GID=str(os.getgid()))
        env.update(overrides)
        return subprocess.run(['/bin/sh', str(script), operation], env=env, capture_output=True, text=True)

    def run_path(self, root, run):
        return root / 'kaferry-replicated/kaferry-replicated' / run

    def prepare(self, root, run):
        result = self.storage(root, run, 'prepare')
        self.assertEqual(0, result.returncode, result.stderr)
        return self.run_path(root, run)

    @contextmanager
    def prepared_storage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, run = Path(temporary), 'a' * 32
            yield root, run, self.prepare(root, run)

    def interrupted_cleanup(self, root, run, command, failure, **overrides):
        tools = root / 'tools'; tools.mkdir()
        program = tools / command
        program.write_text('#!/bin/sh\nfor target do :; done\n' + failure + '\nexec /bin/' + command + ' "$@"\n')
        program.chmod(0o700)
        result = self.storage(root, run, 'clean', PATH=str(tools) + os.pathsep + os.environ['PATH'],
                              KAFERRY_TEST_RUN=str(self.run_path(root, run)), **overrides)
        self.assertNotEqual(0, result.returncode)
        return result

    def test_cleanup_removes_only_the_marked_run_and_preserves_a_peer(self):
        # Given two independent runs and unrelated external-volume data.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first, peer = 'a' * 32, 'b' * 32
            for run in (first, peer):
                (self.prepare(root, run) / 'source-1/data').write_bytes(b'broker data')
            unrelated = root / 'unrelated'; unrelated.write_bytes(b'keep')
            # When the first run is cleaned, then its peer and unrelated bytes remain intact.
            result = self.storage(root, first, 'clean')
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertFalse(self.run_path(root, first).exists())
            self.assertEqual(b'broker data', (self.run_path(root, peer) / 'source-1/data').read_bytes())
            self.assertEqual(b'keep', unrelated.read_bytes())
            self.assertEqual(0, self.storage(root, first, 'clean').returncode)

    def test_prepare_never_reuses_an_existing_run_and_cleanup_rejects_foreign_markers(self):
        with self.prepared_storage() as (root, run, folder):
            data = folder / 'source-1/data'; data.write_bytes(b'preserve')
            # Given a reused directory, when preparing, then existing data cannot be overwritten.
            self.assertNotEqual(0, self.storage(root, run, 'prepare').returncode)
            (folder / '.kaferry-owned').write_text('another owner\n')
            # When its ownership no longer matches, then cleanup refuses to delete it.
            result = self.storage(root, run, 'clean')
            self.assertNotEqual(0, result.returncode)
            self.assertIn('ownership', result.stderr)
            self.assertEqual(b'preserve', data.read_bytes())

    def test_partial_data_deletion_retains_ownership_and_reports_failure(self):
        # Given a marked run, a peer, and deletion that removes a file before failing.
        with tempfile.TemporaryDirectory() as temporary:
            root, run, peer = Path(temporary), 'a' * 32, 'b' * 32
            for id in (run, peer):
                (self.prepare(root, id) / 'source-1/keep').write_bytes(b'preserve')
            folder = self.run_path(root, run)
            removed = folder / 'source-1/removed'; removed.write_bytes(b'partial deletion')
            marker = folder / '.kaferry-owned'; original = marker.read_bytes()
            # When deletion fails, then partial changes remain, but ownership and peer data survive.
            result = self.interrupted_cleanup(root, run, 'rm', '''
case "$target" in
    "$KAFERRY_TEST_RUN")
        /bin/rm -f -- "$target/.kaferry-owned" "$target/source-1/removed"
        echo 'injected partial deletion failure' >&2; exit 1 ;;
    "$KAFERRY_TEST_RUN/source-1")
        /bin/rm -f -- "$target/removed"
        echo 'injected partial deletion failure' >&2; exit 1 ;;
esac
''')
            self.assertIn('injected partial deletion failure', result.stderr)
            self.assertFalse(removed.exists())
            self.assertEqual(original, marker.read_bytes())
            self.assertEqual(b'preserve', (folder / 'source-1/keep').read_bytes())
            self.assertEqual(b'preserve', (self.run_path(root, peer) / 'source-1/keep').read_bytes())
            # A later explicit cleanup can still authenticate this run after the fault is removed.
            result = self.storage(root, run, 'clean')
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertFalse(folder.exists())
            self.assertTrue(self.run_path(root, peer).exists())

    def test_failed_final_directory_removal_restores_the_marker(self):
        # Given deletion completes but a filesystem reports a new entry at final removal.
        for entry in ('late-entry', '.kaferry-owned'):
            with self.subTest(entry=entry), self.prepared_storage() as (root, run, folder):
                original = (folder / '.kaferry-owned').read_bytes()
                # When final removal fails, then restoration never overwrites the new entry.
                result = self.interrupted_cleanup(root, run, 'rmdir', '''
if [ "$target" = "$KAFERRY_TEST_RUN" ]; then
    printf 'preserve' > "$target/$KAFERRY_TEST_ENTRY"
    echo 'injected directory not empty' >&2
    exit 7
fi
''', KAFERRY_TEST_ENTRY=entry)
                self.assertEqual(7, result.returncode)
                if entry == 'late-entry':
                    self.assertIn('ownership marker restored', result.stderr)
                    self.assertEqual(original, (folder / '.kaferry-owned').read_bytes())
                else:
                    self.assertIn('cannot restore fixture storage ownership marker', result.stderr)
                self.assertEqual('preserve', (folder / entry).read_text())
                self.assertFalse((folder / 'source-1').exists())

    def test_cleanup_cannot_follow_run_or_marker_symlinks(self):
        for target in ('run', 'marker'):
            with self.subTest(target=target), self.prepared_storage() as (root, run, folder):
                foreign = root / 'foreign'; foreign.mkdir()
                data = foreign / 'keep'; data.write_bytes(b'preserve')
                if target == 'run':
                    moved = folder.with_name('moved'); folder.rename(moved)
                    folder.symlink_to(foreign, target_is_directory=True)
                else:
                    marker = folder / '.kaferry-owned'
                    external_marker = foreign / 'marker'; marker.rename(external_marker)
                    marker.symlink_to(external_marker)
                # Given a substituted path, when cleaning, then no foreign target is deleted.
                self.assertNotEqual(0, self.storage(root, run, 'clean').returncode)
                self.assertEqual(b'preserve', data.read_bytes())

    def diagnostic_cleanup(self, root, run, probe):
        tools = root / 'tools'; tools.mkdir()
        programs = {
            'rm': '''#!/bin/sh
for target do :; done
if [ "$target" = "$KAFERRY_TEST_RUN/destination-1" ]; then
    printf '%s\\n' "$target" >> "$KAFERRY_REMOVALS"
    echo 'injected deletion failure' >&2
    exit 7
fi
exec /bin/rm "$@"
''',
            'timeout': '#!' + sys.executable + '\n' + '''import os, signal, subprocess, sys
assert sys.argv[1:4] == ['-k', '1', '2']
p = subprocess.Popen(sys.argv[4:], start_new_session=True)
try:
    code = p.wait(timeout=2)
except subprocess.TimeoutExpired:
    os.killpg(p.pid, signal.SIGKILL)
    p.wait()
    code = 124
raise SystemExit(code)
''',
            'find': '#!' + sys.executable + '\n' + probe,
        }
        for name, text in programs.items():
            path = tools / name; path.write_text(text); path.chmod(0o700)
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'],
                   KAFERRY_STORAGE_ROOT=str(root), KAFERRY_FIXTURE_PROJECT='kaferry-replicated',
                   KAFERRY_REPLICATED_RUN_ID=run, KAFERRY_TEST_RUN=str(self.run_path(root, run)),
                   KAFERRY_REMOVALS=str(root / 'removals'))
        script = Path(__file__).resolve().parents[2] / 'src/tests/replicated/storage.sh'
        process = subprocess.Popen(['/bin/sh', str(script), 'clean'], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            out, err = process.communicate(timeout=6)
        finally:
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait()
        self.assertEqual(7, process.returncode, err)
        self.assertEqual([str(self.run_path(root, run) / 'destination-1')],
                         (root / 'removals').read_text().splitlines())
        return out, err

    def test_first_deletion_error_captures_metadata_without_following_links_or_retry(self):
        # Given an owned run with broker data and a link to unrelated bytes.
        with self.prepared_storage() as (root, run, folder):
            original = (folder / '.kaferry-owned').read_bytes()
            foreign = root / 'foreign'; foreign.mkdir()
            (foreign / 'secret').write_bytes(b'file contents must not be captured')
            target = folder / 'destination-1'
            (target / 'data').write_bytes(b'broker payload must not be captured')
            (target / 'link').symlink_to(foreign, target_is_directory=True)
            # When its first deletion fails, then the census never reads contents or follows the link.
            _, error = self.diagnostic_cleanup(root, run, '''import os, pathlib, stat, sys
assert sys.argv[1] == '-P' and sys.argv[3:5] == ['-maxdepth', '4']
assert sys.argv[5:8] == ['-exec', 'stat', '-c']
assert sys.argv[8] == 'type=%F device=%d inode=%i bytes=%s links=%h path=%n'
assert sys.argv[9:] == ['--', '{}', '+']
p = pathlib.Path(sys.argv[2])
for entry in [p, *p.iterdir()]:
    info = entry.lstat()
    kind = 'l' if stat.S_ISLNK(info.st_mode) else 'd' if stat.S_ISDIR(info.st_mode) else 'f'
    print(f'type={kind} inode={info.st_ino} bytes={info.st_size} links={info.st_nlink} path={entry}')
''')
            self.assertIn('type=l', error)
            self.assertIn('cleanup census exit=0', error)
            self.assertNotIn('must not be captured', error)
            self.assertNotIn(str(foreign / 'secret'), error)
            self.assertEqual(original, (folder / '.kaferry-owned').read_bytes())
            self.assertEqual(b'file contents must not be captured', (foreign / 'secret').read_bytes())
            self.assertTrue((folder / 'source-3').is_dir())

    def test_census_output_failure_and_timeout_preserve_the_original_deletion_error(self):
        # Given too much metadata, a failed census or a blocked metadata reader.
        probes = [('truncated', "import sys\nsys.stdout.write('x' * 200000)\n"),
                  ('failed', "raise SystemExit(19)\n"),
                  ('timed out', 'import signal\nsignal.pause()\n')]
        for expected, probe in probes:
            with self.subTest(expected=expected), self.prepared_storage() as (root, run, folder):
                marker = (folder / '.kaferry-owned').read_bytes()
                started = time.monotonic()
                # When diagnostics fail, then cleanup still finishes with its first status and marker.
                _, error = self.diagnostic_cleanup(root, run, probe)
                self.assertIn('cleanup census ' + expected, error)
                self.assertLess(len(error.encode()), 66560)
                self.assertLess(time.monotonic() - started, 5)
                self.assertEqual(marker, (folder / '.kaferry-owned').read_bytes())


class CleanupContainerStateTest(unittest.TestCase):
    def capture(self, root, program):
        env = dict(os.environ, KAFERRY_TEST_RESOLVED='this-run')
        cleanup_container_state(root, [sys.executable, '-c', program], env)
        return json.loads((root / 'cleanup-container-state.json').read_text())

    def test_snapshot_uses_resolved_environment_and_only_allowed_container_fields(self):
        # Given a state query whose environment belongs to this run.
        with tempfile.TemporaryDirectory() as temporary:
            # When captured, then stderr and unrelated container details cannot enter the receipt.
            result = self.capture(Path(temporary), '''import json, os, sys
assert os.environ['KAFERRY_TEST_RESOLVED'] == 'this-run'
assert sys.argv[1:3] == ['ps', '--all']
assert all(word not in sys.argv[-1] for word in ['Env', 'Mounts', 'HostConfig', 'Command'])
print('private daemon diagnostic', file=sys.stderr)
print(json.dumps(dict(service='source-1', state='exited', exit_code=0)))
''')
            self.assertEqual('complete', result['status'])
            self.assertEqual([dict(service='source-1', state='exited', exit_code=0)], result['containers'])
            self.assertNotIn('private daemon diagnostic', json.dumps(result))

    def test_failed_blocked_excessive_and_invalid_queries_are_bounded_and_explicit(self):
        # Given failure, an unresponsive command, excessive output or private unexpected fields.
        examples = [('failed', 'raise SystemExit(17)'),
                    ('timed out', 'import signal; signal.pause()'),
                    ('truncated', "import sys; sys.stdout.write('x' * 200000)"),
                    ('invalid output', "print('{\"HostConfig\":\"private-data\"}')")]
        for expected, program in examples:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                started = time.monotonic()
                # When inspected, then metadata is unavailable rather than replaced with unsafe raw output.
                result = self.capture(Path(temporary), program)
                self.assertEqual(expected, result['status'])
                self.assertNotIn('containers', result)
                self.assertNotIn('private-data', json.dumps(result))
                self.assertLess(time.monotonic() - started, 4)
                self.assertLess(len(json.dumps(result)), 512)

    def test_diagnostic_error_cannot_replace_cleanup_error_or_retry_deletion(self):
        # Given failed storage cleanup and a second failure retaining its state snapshot.
        with tempfile.TemporaryDirectory() as temporary:
            def execute(command, output, label, env, **kwargs):
                if label == 'storage-clean': raise RuntimeError('original deletion failure')
            with patch('tools.qualification.qualify.logged', side_effect=execute) as calls, \
                 patch('tools.qualification.qualify.cleanup_container_state', side_effect=OSError('private path')):
                # When cleanup fails, then stop and delete each run once and retain the original failure first.
                result = cleanup_replicated(Path(temporary), ['docker', 'compose'], {})
            self.assertEqual(['brokers-stop', 'storage-clean'], [c.args[2] for c in calls.call_args_list])
            self.assertEqual('original deletion failure', result[0]['error'])
            self.assertEqual('cleanup-diagnostics', result[1]['phase'])
            self.assertNotIn('private path', json.dumps(result))


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


if __name__ == '__main__':
    unittest.main()
