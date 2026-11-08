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


class SuitesTest(unittest.TestCase):
    def test_qualification_keeps_shared_selection_and_its_deadlines(self):
        # Given local qualification with a non-default Go executable.
        for name, tags, pattern, timeout in [
            ('load', 'integration,load', '^TestLoad', '20m'),
            ('fault', 'integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', '10m'),
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
