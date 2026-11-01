"""Qualification must preserve failures, missing coverage and binary identity."""

import argparse
from contextlib import contextmanager
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

from qualify import duration, lab_lock, logged, plan, report, run_suite, suite_command, valid_receipts
from broker_probe import logical, reconcile, records


class MatrixTest(unittest.TestCase):
    def test_profiles_cover_directed_pairs_without_duplicates(self):
        for tier, count in [('pr', 6), ('nightly', 7), ('full', 31)]:
            cases = plan(tier)
            self.assertEqual(count, len(cases))
            self.assertEqual(count, len({c['id'] for c in cases}))
        self.assertIn(('4.4.0', '3.9.2'), {(c['source'], c['destination']) for c in plan('pr')})

    def test_host_lock_is_shared_across_checkouts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch('qualify.LAB_LOCK', root / 'host.lock'), patch('qualify.ROOT', root / 'checkout-a'):
                with lab_lock():
                    with patch('qualify.ROOT', root / 'checkout-b'):
                        with self.assertRaisesRegex(ValueError, 'host'):
                            with lab_lock(): pass

    def test_duration_limits_and_units(self):
        self.assertEqual(259200, duration('72h'))
        self.assertEqual(604800, duration('7d'))
        for value in ('0h', '59s', '8d', '-1h', '1.5h', '1h30m', 'forever'):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                duration(value)

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

    def test_interruption_records_failure_and_cleans_up_brokers(self):
        @contextmanager
        def unlocked():
            yield
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            binary = directory / 'kaferry'; binary.write_bytes(b'executable fixture')
            args = argparse.Namespace(suite='broker', source='3.9.2', destination='4.4.0',
                                      output=directory / 'evidence', binary=str(binary), config=None, duration=60)
            calls = []
            def command(command, output, label, env, **kwargs):
                calls.append(label)
                if label == 'suite':
                    raise KeyboardInterrupt()
            with patch('qualify.lab_lock', unlocked), patch('qualify.capture', return_value='fixture'), \
                 patch('qualify.logged', side_effect=command), patch('qualify.subprocess.run'):
                with self.assertRaises(KeyboardInterrupt):
                    run_suite(args)
            self.assertEqual(['brokers-up', 'suite', 'brokers-down'], calls)
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('interrupted', receipt['status'])
            self.assertEqual(64, len(receipt['binary_sha256']))

    def test_log_timeout_still_tears_down_and_preserves_original_failure(self):
        @contextmanager
        def unlocked(): yield
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            binary = directory / 'kaferry'; binary.write_bytes(b'fixture')
            args = argparse.Namespace(suite='broker', source='3.9.2', destination='4.4.0',
                output=directory / 'evidence', binary=str(binary), config=None, duration=60)
            calls = []
            def command(command, output, label, env, **kwargs):
                calls.append(label)
                if label == 'suite': raise RuntimeError('original suite failure')
            with patch('qualify.lab_lock', unlocked), patch('qualify.capture', return_value='fixture'), \
                 patch('qualify.logged', side_effect=command), \
                 patch('qualify.subprocess.run', side_effect=subprocess.TimeoutExpired('docker logs', 30)):
                with self.assertRaisesRegex(RuntimeError, 'original suite failure'):
                    run_suite(args)
            self.assertEqual(['brokers-up', 'suite', 'brokers-down'], calls)
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('failed', receipt['status'])
            self.assertEqual('original suite failure', receipt['suite_error'])
            self.assertEqual('logs', receipt['cleanup_errors'][0]['phase'])

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
                with patch('qualify.STOP_GRACE', 0.1), patch('builtins.print'):
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
