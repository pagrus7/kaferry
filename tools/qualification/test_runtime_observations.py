"""The diagnostic sampler must remain scoped, bounded and joined on failure."""

import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from tools.qualification.runtime_observations import runtime_observations, SERVICES


FAKE_DOCKER = r'''
import json, os, signal, sys, time
from pathlib import Path
root = Path(os.environ['DIAGNOSTIC_FIXTURE'])
options = json.loads((root / 'options.json').read_text())
with (root / 'processes.jsonl').open('a') as output:
    output.write(json.dumps({'pid': os.getpid(), 'args': sys.argv[1:]}) + '\n')
ids = [format(n + 1, '064x') for n in range(7)]
if sys.argv[1] == 'compose':
    print('\n'.join(ids), flush=True)
elif sys.argv[1] == 'inspect':
    if options.get('inspect_sleep'):
        time.sleep(options['inspect_sleep'])
    for identifier, service in zip(ids, options['services']):
        row = dict(id=identifier, project='foreign' if options.get('foreign') else 'owned',
                   service=service, running=True, started_at='2026-11-09T00:00:00Z', restart_count=0,
                   environment='PRIVATE', mount='PRIVATE', health=dict(status='healthy', failing_streak=0,
                   log=[dict(start='2026-11-09T00:00:00Z', end='2026-11-09T00:00:00.1Z', exit_code=0, output='PRIVATE')]))
        print(json.dumps(row), flush=True)
elif sys.argv[1] == 'stats':
    if options.get('ignore_term'):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if options.get('oversized'):
        print('x' * (128 * 1024), flush=True)
    else:
        while True:
            for identifier in ids:
                row = dict(ID=identifier, CPUPerc='1.5%', MemUsage='128MiB / 1GiB', MemPerc='12.5%',
                           NetIO='1MB / 2MB', BlockIO='0B / 3MB', PIDs='12', Name='PRIVATE')
                print(json.dumps(row), flush=True)
            time.sleep(.02)
    time.sleep(30)
else:
    sys.exit(9)
'''


class RuntimeObservationsTest(unittest.TestCase):
    def fixture(self, root, **options):
        (root / 'options.json').write_text(json.dumps(dict(services=SERVICES, **options)))
        executable = root / 'docker'
        executable.write_text('#!' + sys.executable + '\n' + FAKE_DOCKER)
        executable.chmod(0o700)
        return dict(os.environ, PATH=str(root) + os.pathsep + os.environ.get('PATH', ''), DIAGNOSTIC_FIXTURE=str(root))

    def wait_for(self, condition):
        deadline = time.monotonic() + 3
        while not condition():
            self.assertLess(time.monotonic(), deadline, 'diagnostic observation did not arrive')
            time.sleep(.01)

    def assert_joined(self, root):
        processes = [json.loads(line) for line in (root / 'processes.jsonl').read_text().splitlines()]
        for process in processes:
            with self.subTest(command=process['args']):
                with self.assertRaises(ProcessLookupError):
                    os.kill(process['pid'], 0)
        return processes

    def test_owned_measurements_exclude_private_fields_and_join_before_return(self):
        # Given seven owned fixture containers and a live stream.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = self.fixture(root, ignore_term=True)
            # When sampling ends normally, then every child is reaped and only
            # selected resource/health fields reach bounded retained output.
            with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                self.wait_for(lambda: summary['rows'] >= 9)
            self.assertEqual('completed', summary['status'])
            rows = [json.loads(line) for line in (root / 'replicated-runtime.jsonl').read_text().splitlines()]
            self.assertEqual({'health', 'stats'}, {row['kind'] for row in rows})
            self.assertNotIn('PRIVATE', json.dumps(rows))
            self.assertEqual(summary['bytes'], (root / 'replicated-runtime.jsonl').stat().st_size)
            processes = self.assert_joined(root)
            stats = [row for row in processes if row['args'][0] == 'stats']
            self.assertEqual(1, len(stats), 'one stats process must cover all seven targets')
            self.assertEqual(7, len(stats[0]['args'][4:]))
            self.assertGreater(summary['command_wall_seconds'], 0)

    def test_foreign_fixture_is_rejected_before_starting_stats(self):
        # Given a Compose response whose label points at another project.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # When collecting, then no resource command touches that identity.
            with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], self.fixture(root, foreign=True)) as summary:
                self.assertEqual('unavailable', summary['status'])
            self.assertIn('owned replicated fixture', summary['errors'][0])
            self.assertFalse(any(row['args'][0] == 'stats' for row in self.assert_joined(root)))

    def test_row_and_wall_time_limits_stop_both_sampling_paths(self):
        for limit, value, expected in [('MAX_ROWS', 6, 'rows'), ('MAX_SECONDS', 1, 'seconds')]:
            # Given finite diagnostic budgets, when reached, then capture stops
            # explicitly instead of creating an unbounded long-run stream.
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch('tools.qualification.runtime_observations.' + limit, value):
                    with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], self.fixture(root)) as summary:
                        self.wait_for(lambda: expected in summary['limits_reached'])
                self.assertEqual('incomplete', summary['status'])
                self.assertLessEqual(summary['rows'], summary['max_rows'])
                self.assert_joined(root)

    def test_oversized_output_and_byte_budget_are_bounded(self):
        # Given daemon output beyond a line or total evidence allowance.
        for oversized, byte_limit in [(True, 8 * 1024**2), (False, 3000)]:
            with self.subTest(oversized=oversized), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch('tools.qualification.runtime_observations.MAX_BYTES', byte_limit):
                    with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], self.fixture(root, oversized=oversized)) as summary:
                        # When the bound is hit, then the original oversized
                        # data is not retained and all child processes join.
                        self.wait_for(lambda: summary['errors'] or summary['limits_reached'])
                self.assertEqual('incomplete', summary['status'])
                self.assertLessEqual(summary['bytes'], byte_limit)
                self.assert_joined(root)

    def test_diagnostic_timeout_preserves_the_original_suite_failure(self):
        # Given an unresponsive diagnostic command and a later suite failure.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = self.fixture(root, inspect_sleep=2)
            with patch('tools.qualification.runtime_observations.COMMAND_SECONDS', 1):
                # When unwinding, then the suite error survives and no child
                # outlives diagnostic cleanup.
                with self.assertRaisesRegex(RuntimeError, 'original suite failure'):
                    with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env):
                        raise RuntimeError('original suite failure')
            summary = json.loads((root / 'replicated-runtime.json').read_text())
            self.assertEqual('unavailable', summary['status'])
            self.assertIn('time limit', summary['errors'][0])
            self.assert_joined(root)

    def test_summary_write_failure_does_not_replace_the_suite_exception(self):
        # Given running measurements whose final summary cannot be written.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = self.fixture(root)
            with patch.object(Path, 'write_text', side_effect=OSError('disk full')):
                # When the suite also fails, then retain its exception and mark
                # diagnostics incomplete in the caller's manifest object.
                with self.assertRaisesRegex(RuntimeError, 'suite failed'):
                    with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                        self.wait_for(lambda: summary['rows'] >= 9)
                        raise RuntimeError('suite failed')
            self.assertEqual('incomplete', summary['status'])
            self.assertIn('summary: disk full', summary['errors'])
            self.assert_joined(root)


if __name__ == '__main__':
    unittest.main()
