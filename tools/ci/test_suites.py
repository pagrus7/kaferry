"""Suite consolidation must preserve selection, deadlines and execution gates."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import tools.ci.suites as suites
from tools.qualification.qualify import suite_command
from tools.qualification.fault_results import FAULT_CASES


class SuitesTest(unittest.TestCase):
    def test_fault_outer_budget_accounts_for_the_same_catalog_as_windows(self):
        # Given the fixture's actual per-case timeout catalog, with churn's
        # overlapping windows sharing one maximum extension of its case.
        catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
        self.assertLessEqual(FAULT_CASES, set(catalog))
        self.assertEqual(473000, sum(sum(catalog[id].values()) for id in FAULT_CASES))
        # When selecting the suite, then preserve the existing 600s execution
        # allowance plus the mandatory 473s observation work, without changing
        # individual configured timeouts or overriding an explicit caller limit.
        self.assertEqual(1073, suites.fault_execution_seconds())
        self.assertIn('-timeout=1073s', suites.command('fault'))
        self.assertIn('-timeout=12m', suites.command('fault', timeout='12m'))
        catalog['fault/churn']['delivery_timeout'] += 1000
        catalog['another-suite/registered-case'] = {'session.timeout.ms': 6000}
        with patch('tools.ci.suites.json.loads', return_value=catalog):
            self.assertEqual(1074, suites.fault_execution_seconds())
        catalog.pop('fault/crash')
        with patch('tools.ci.suites.json.loads', return_value=catalog):
            with self.assertRaisesRegex(ValueError, 'omitted a selected case'):
                suites.fault_execution_seconds()

    def test_failed_fault_run_replaces_prior_successful_receipt(self):
        # Given an older successful receipt in the reusable CI report directory.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory/'fault-results.json').write_text(json.dumps(dict(status='passed')))
            def configure(env):
                env['KAFERRY_ORACLE_RUN'] = 'fresh'
            # When this execution fails, then retain that failure and never leave
            # the prior success available for evidence collection.
            with patch.dict(os.environ, KAFERRY_REPORT_DIR=temporary), \
                 patch.object(sys, 'argv', ['suites', 'run', 'fault']), \
                 patch('tools.qualification.fault_results.configure', side_effect=configure), \
                 patch('tools.ci.suites.subprocess.run', return_value=subprocess.CompletedProcess([], 17)), \
                 patch('tools.qualification.fault_results.collect') as collect:
                self.assertEqual(17, suites.main())
            collect.assert_not_called()
            self.assertEqual(dict(schema=1, status='failed', oracle_run='fresh'),
                             json.loads((directory/'fault-results.json').read_text()))

    def test_qualification_keeps_shared_selection_and_its_deadlines(self):
        # Given local qualification with a non-default Go executable.
        for name, tags, pattern, timeout in [
            ('load', 'integration,load', '^TestLoad', '20m'),
            ('fault', 'integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', '1073s'),
            ('soak', 'integration,load', '^TestStreamingSoak', '370s'),
        ]:
            with self.subTest(suite=name):
                args = argparse.Namespace(suite=name, duration=60, rate=50)
                # When selecting the suite, then tags, selectors and caller budgets survive.
                command, expected = suite_command(args, {'GO': '/custom/go'})
                self.assertEqual(['/custom/go', 'test', '-count=1', '-json', '-timeout='+timeout,
                                  '-tags='+tags, '-run', pattern,
                                  './src/tests/compatibility' if name == 'soak' else './src/tests/system'], command)
                self.assertTrue(expected)
        command, _ = suite_command(argparse.Namespace(suite='integration'), {})
        self.assertEqual(['go', 'test', '-count=1', '-json', '-race', '-timeout=20m',
                          '-tags=integration', './src/tests/system'], command)

    def test_ci_keeps_extra_execution_gates_and_command_failures(self):
        profiles = [('pass', 'TestLoadBoundedKafkaCopy/' + name) for name in
                    ('near-limit-v1', 'hot-keys-v1', 'headers-v1', 'tombstones-v1',
                     'source-none-v1', 'source-gzip-v1', 'source-snappy-v1', 'source-lz4-v1', 'source-zstd-v1')]
        ordinary = [('pass', 'TestLoadBoundedKafkaCopy/default'), ('pass', 'TestLoadObservability/round-0/disabled')]
        for code, events, valid in [
            (9, [], False),
            (0, [('pass', 'TestLoadBoundedKafkaCopy/default')], False),
            (0, [('pass', 'TestLoadBoundedKafkaCopy/default'), ('skip', 'TestLoadObservability/round-0/disabled')], False),
            (0, ordinary, False),
            (0, ordinary + profiles[:-1] + [('skip', 'TestLoadBoundedKafkaCopy/source-zstd-v1')], False),
            (0, ordinary + profiles, True),
        ]:
            with self.subTest(code=code, events=events), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                (directory/'load.log').write_text('\n'.join(json.dumps({'Action': action, 'Test': test}) for action, test in events))
                # Given command results independent of the suite's expected-test definitions.
                with patch('tools.ci.suites.Path', return_value=directory), patch('tools.ci.suites.run', return_value=code) as run:
                    # When CI checks evidence, then failure or skipped required tests cannot pass.
                    if code or valid:
                        self.assertEqual(code, suites.ci('load'))
                    else:
                        with self.assertRaisesRegex(ValueError, 'Required tests did not pass'):
                            suites.ci('load')
                    run.assert_called_once_with('load', ['make', 'load', 'KAFERRY_TEST_OUTPUT=-json'], directory)

    def test_cli_forwards_make_overrides_and_exit_status(self):
        # Given an executable that records the invocation and exits unsuccessfully.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root/'go'
            output = root/'args.json'
            executable.write_text(f'#!{sys.executable}\nimport json, os, sys\nopen(os.environ["SUITE_ARGS"], "w").write(json.dumps(sys.argv[1:]))\nsys.exit(17)\n')
            executable.chmod(0o700)
            # When invoked as a Makefile caller, then use its executable, timeout and output flags.
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'run', 'soak',
                                     '--go='+str(executable), '--timeout=40m', '--output=-json'],
                                    env=dict(os.environ, SUITE_ARGS=str(output)), capture_output=True, text=True)
            self.assertEqual(17, result.returncode, result.stderr)
            self.assertEqual(['test', '-count=1', '-json', '-timeout=40m', '-tags=integration,load',
                              '-run', '^TestStreamingSoak', './src/tests/compatibility'], json.loads(output.read_text()))
