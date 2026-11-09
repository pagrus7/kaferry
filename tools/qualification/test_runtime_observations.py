"""The diagnostic sampler must remain scoped, bounded and joined on failure."""

import json
import os
from contextlib import contextmanager
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from tools.qualification.runtime_observations import RuntimeObservations, runtime_observations, SERVICES


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
    if options.get('health_sleep') and (root / 'processes.jsonl').read_text().count('"inspect"') > 1:
        time.sleep(options['health_sleep'])
    for identifier, service in zip(ids, options['services']):
        row = dict(id=identifier, project='foreign' if options.get('foreign') else 'owned',
                   service=service, running=True, started_at='2026-11-09T00:00:00Z', restart_count=0,
                   environment='PRIVATE', mount='PRIVATE', health=dict(status='healthy', failing_streak=0,
                   log=[dict(start='2026-11-09T00:00:00Z', end='2026-11-09T00:00:00.1Z', exit_code=0, output='PRIVATE')]))
        print(json.dumps(row), flush=True)
elif sys.argv[1] == 'stats':
    if '--no-stream' not in sys.argv:
        print('Error: diagnostic fixture requires a finite snapshot', flush=True)
        sys.exit(9)
    def stats_row(identifier):
        return json.dumps(dict(ID=identifier, CPUPerc='1.5%', MemUsage='128MiB / 1GiB', MemPerc='12.5%',
                               NetIO='1MB / 2MB', BlockIO='0B / 3MB', PIDs='12', Name='PRIVATE'))
    if options.get('ignore_term'):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    (root / ('stats-ready-' + str(os.getpid()))).write_text('ready')
    if options.get('stats_sleep'):
        time.sleep(options['stats_sleep'])
    if 'stats_lines' in options:
        for line in options['stats_lines']:
            print(line.replace('RECORD', stats_row(ids[0])), flush=True)
    elif options.get('oversized'):
        print('x' * (128 * 1024), flush=True)
    else:
        for identifier in options.get('stats_ids', ids):
            print(stats_row(identifier), flush=True)
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

    @contextmanager
    def prepared_fixture(self, **options):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            yield root, self.fixture(root, **options)

    def assert_joined(self, root):
        processes = [json.loads(line) for line in (root / 'processes.jsonl').read_text().splitlines()]
        for process in processes:
            with self.subTest(command=process['args']):
                with self.assertRaises(ProcessLookupError):
                    os.kill(process['pid'], 0)
        return processes

    def test_owned_measurements_exclude_private_fields_and_join_before_return(self):
        # Given seven owned fixture containers and repeated finite snapshots.
        with self.prepared_fixture() as (root, env):
            # When sampling ends normally, then every child is reaped and only
            # selected resource/health fields reach bounded retained output.
            with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                self.wait_for(lambda: summary['errors'] or summary['rows'] >= 16)
                self.assertFalse(summary['errors'])
            self.assertEqual('completed', summary['status'])
            rows = [json.loads(line) for line in (root / 'replicated-runtime.jsonl').read_text().splitlines()]
            self.assertEqual({'health', 'stats'}, {row['kind'] for row in rows})
            self.assertNotIn('PRIVATE', json.dumps(rows))
            self.assertEqual(summary['bytes'], (root / 'replicated-runtime.jsonl').stat().st_size)
            processes = self.assert_joined(root)
            stats = [row for row in processes if row['args'][0] == 'stats']
            self.assertGreaterEqual(len(stats), 2, 'the next finite snapshot must follow the first')
            for process in stats:
                self.assertIn('--no-stream', process['args'])
                self.assertEqual(7, len(process['args'][5:]))
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

    def test_unknown_controls_and_malformed_stats_remain_diagnostic_errors(self):
        # Given unexpected controls, malformed JSON or a daemon error in a
        # finite snapshot, whose Docker path does not use cursor redraws.
        for line in ('\x1b[HRECORD', '\x1b[2JRECORD', '\x1b[31mRECORD', '\x1b[K', '{"ID":',
                     'Error response from daemon: unavailable', '{"ID":"x\x1b[K"}'):
            with self.subTest(line=line), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                env = self.fixture(root, stats_lines=[line])
                # When parsing, then no broad ANSI or invalid-row suppression
                # can turn the malformed stream into completed measurements.
                with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                    self.wait_for(lambda: summary['errors'])
                self.assertEqual('incomplete', summary['status'])
                self.assertTrue(summary['errors'][0].startswith('stats: '))
                self.assert_joined(root)

    def test_each_snapshot_requires_every_owned_container_once(self):
        # Given missing, duplicated or foreign identities in snapshot output.
        identifiers = [format(n + 1, '064x') for n in range(7)]
        for snapshot in (identifiers[:-1], identifiers[:-1] + identifiers[:1], identifiers[:-1] + ['f' * 64]):
            with self.subTest(snapshot=snapshot), self.prepared_fixture(stats_ids=snapshot) as (root, env):
                # When validating, then an incomplete or unowned cohort cannot
                # create even a partial set of retained resource measurements.
                with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                    self.wait_for(lambda: summary['errors'])
                rows = [json.loads(line) for line in (root / 'replicated-runtime.jsonl').read_text().splitlines()]
                self.assertFalse(any(row['kind'] == 'stats' for row in rows))
                self.assertEqual('incomplete', summary['status'])
                self.assert_joined(root)

    def test_snapshot_cancellation_kills_and_reaps_the_active_command(self):
        # Given a snapshot command that ignores termination while blocked.
        with self.prepared_fixture(stats_sleep=30, ignore_term=True) as (root, env):
            # When the suite stops, then the sampler escalates and reaps that
            # command before returning, without waiting for its natural exit.
            with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                self.wait_for(lambda: list(root.glob('stats-ready-*')))
            self.assertEqual('completed', summary['status'])
            self.assert_joined(root)

    def test_snapshot_timeout_is_reported_without_restarting_the_command(self):
        # Given a snapshot that cannot finish inside its diagnostic deadline.
        with self.prepared_fixture(stats_sleep=2) as (root, env), \
             patch('tools.qualification.runtime_observations.STATS_SECONDS', .5):
            # When it expires, then retain an explicit error and reap the sole
            # failed request instead of retrying until data looks healthy.
            with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env) as summary:
                self.wait_for(lambda: summary['errors'])
            self.assertEqual('incomplete', summary['status'])
            self.assertIn('stats: diagnostic command time limit', summary['errors'])
            processes = self.assert_joined(root)
            self.assertEqual(1, sum(row['args'][0] == 'stats' for row in processes))

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

    def test_thread_start_failures_preserve_the_suite_error_and_finish_cleanup(self):
        # Given failure to start either sampler, including a health command
        # already running when the second thread cannot start.
        real_start = threading.Thread.start
        for failing in (1, 2):
            with self.subTest(failing=failing), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                env = self.fixture(root, health_sleep=2)
                observer = RuntimeObservations(root, ['docker', 'compose', '-p', 'owned'], env)
                started = []
                def start(thread):
                    if len(started) + 1 == failing:
                        if started:
                            self.wait_for(lambda: (root / 'processes.jsonl').read_text().count('"inspect"') > 1)
                        raise RuntimeError('thread creation unavailable')
                    real_start(thread)
                    started.append(thread)
                failure = RuntimeError('original suite failure')
                with patch('tools.qualification.runtime_observations.RuntimeObservations', return_value=observer), \
                     patch.object(threading.Thread, 'start', start):
                    # When the suite fails too, then cleanup joins only actual
                    # threads, reaps every command, closes output and saves status.
                    with self.assertRaises(RuntimeError) as caught:
                        with runtime_observations(root, ['docker', 'compose', '-p', 'owned'], env):
                            raise failure
                self.assertIs(failure, caught.exception)
                self.assertEqual(failing - 1, len(observer.threads))
                self.assertTrue(all(not thread.is_alive() for thread in started))
                self.assertTrue(observer.stream.closed)
                summary = json.loads((root / 'replicated-runtime.json').read_text())
                self.assertEqual('incomplete', summary['status'])
                self.assertIn('startup: thread creation unavailable', summary['errors'])
                self.assert_joined(root)


if __name__ == '__main__':
    unittest.main()
