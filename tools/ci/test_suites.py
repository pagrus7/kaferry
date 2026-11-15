"""Suite consolidation must preserve selection, deadlines and execution gates."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import tools.ci.suites as suites


class SuitesTest(unittest.TestCase):
    def test_benchmarks_require_each_measurement_and_matching_package_pass(self):
        # Given complete per-package Go JSON benchmark output.
        avro = 'github.com/pagrus7/kaferry/src/internal/archive/avro'
        replay = 'github.com/pagrus7/kaferry/src/internal/archive/replay'
        events = [dict(Action='output', Package=package,
                       Output=name+'-2 1 42.5 ns/op 8 B/op 1 allocs/op\n')
                  for package, names in [(avro, ['BenchmarkEncode/tiny', 'BenchmarkDecode/tiny']),
                                         (replay, ['BenchmarkReplayOverlap', 'BenchmarkCheckpointEncoding', 'BenchmarkCheckpointSaving'])]
                  for name in names]
        events += [dict(Action='pass', Package=package) for package in (avro, replay)]
        log = lambda rows: '\n'.join(json.dumps(row) for row in rows)
        suites.verify_benchmarks(log(events))
        # When a measurement or package success is absent, then reject it;
        # output from another package and a Go "bench" event are not passes.
        for rows in (events[1:], events[:-1],
                     [dict(row, Package=replay) if row.get('Package') == avro else row for row in events],
                     [dict(row, Action='bench') if row['Action'] == 'pass' else row for row in events]):
            with self.subTest(events=rows), self.assertRaisesRegex(ValueError, 'Required benchmarks did not complete'):
                suites.verify_benchmarks(log(rows))

    def test_fuzz_requires_all_five_real_pass_events(self):
        # Given one fuzz target for each selected package, including a skipped target.
        names = ['FuzzProgressLifecycle', 'FuzzParse', 'FuzzContainer', 'FuzzState', 'FuzzCheckpoint']
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            events = [dict(Action='pass', Test=name) for name in names]
            for complete in (True, False):
                (directory/'fuzz.log').write_text('\n'.join(json.dumps(event) for event in
                    (events if complete else events[:-1]+[dict(Action='skip', Test=names[-1])])))
                with patch('tools.ci.suites.Path', return_value=directory), patch('tools.ci.suites.run', return_value=0):
                    if complete:
                        self.assertEqual(0, suites.ci('fuzz'))
                    else:
                        with self.assertRaisesRegex(ValueError, 'Required tests did not pass'):
                            suites.ci('fuzz')

    def test_cli_catalog_and_verification_keep_quickstart_contexts_distinct(self):
        # Given the Kafka job's full required evidence and its intentional opt-out.
        events = [dict(Action='pass', Test=name) for name in suites.EXPECTED['kafka']]
        events.append(dict(Action='skip', Test='TestKafkaQuickStart'))
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary)/'integration.log'
            log.write_text('\n'.join(json.dumps(event) for event in events))
            # When the CLI selects the evidence contract, then the Kafka context
            # permits that opt-out and enabled quickstart still requires a pass.
            for suite, valid in [('kafka', True), ('ci-integration', False)]:
                result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'verify', suite, str(log)],
                                        text=True, capture_output=True)
                self.assertEqual(valid, result.returncode == 0, result.stdout+result.stderr)
                if not valid:
                    self.assertIn('Required tests did not pass: TestKafkaQuickStart', result.stderr)
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'names', 'kafka'],
                                    text=True, capture_output=True, check=True)
            self.assertEqual(suites.EXPECTED['kafka'], result.stdout.splitlines())
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'pattern', 'smoke'],
                                    text=True, capture_output=True, check=True)
            self.assertEqual(suites.PATTERNS['smoke'], result.stdout.strip())

    def test_smoke_make_target_preserves_pattern_and_requires_passes(self):
        # Given a fake Go process that records Make's actual selector and emits
        # complete, incomplete or unexpectedly skipped test results.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            executable = directory/'go'
            arguments = directory/'args.json'
            executable.write_text('#!'+sys.executable+'\nimport json, os, sys\n'
                                  'from pathlib import Path\n'
                                  'Path(os.environ["SMOKE_ARGUMENTS"]).write_text(json.dumps(sys.argv[1:]))\n'
                                  'for event in json.loads(os.environ["SMOKE_EVENTS"]):\n'
                                  '    print(json.dumps(event))\n')
            executable.chmod(0o700)
            passes = [dict(Action='pass', Test=name) for name in suites.EXPECTED['smoke']]
            for events, error in [(passes, None), (passes[:-1], 'Required tests did not pass'),
                                  (passes + [dict(Action='skip', Test='Unexpected')], 'Unexpected skip events')]:
                env = dict(os.environ, KAFERRY_REPORT_DIR=str(directory/'results'),
                           SMOKE_ARGUMENTS=str(arguments), SMOKE_EVENTS=json.dumps(events))
                result = subprocess.run(['make', '--no-print-directory', 'smoke', 'GO='+str(executable),
                                         'PYTHON='+sys.executable, 'KAFERRY_TEST_BINARY=/unused'],
                                        env=env, text=True, capture_output=True, timeout=60)
                # When the actual target runs, then shell expansion preserves
                # the shared selector and its evidence gate rejects non-passes.
                self.assertEqual(error is None, result.returncode == 0, result.stdout+result.stderr)
                if error:
                    self.assertIn(error, result.stderr)
                args = json.loads(arguments.read_text())
                self.assertEqual(suites.PATTERNS['smoke'], args[args.index('-run')+1])
                self.assertIn('-json', args)
