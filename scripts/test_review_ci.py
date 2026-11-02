"""CI watching must bound work and reject missing, failed and stale observations."""

import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import review_ci as ci


HEAD, BASE = 'a' * 40, 'b' * 40


def observation():
    pr = {'number': 12, 'state': 'open', 'head': {'sha': HEAD, 'repo': {'id': 1}},
          'base': {'sha': BASE, 'ref': 'main', 'repo': {'id': 1, 'full_name': 'owner/repo'}}}
    runs = [{'id': i, 'path': path, 'head_sha': HEAD, 'event': 'pull_request',
             'status': 'completed', 'conclusion': 'success', 'pull_requests': [copy.deepcopy(pr)]}
            for i, path in enumerate(sorted(ci.WORKFLOWS), 1)]
    checks = [{'id': 1, 'name': 'fast', 'head_sha': HEAD, 'app': {'id': 1},
               'status': 'completed', 'conclusion': 'success'}]
    return {'pr': pr,
            'runs': {'total_count': len(runs), 'workflow_runs': runs},
            'checks': {'total_count': len(checks), 'check_runs': checks},
            'statuses': {'total_count': 0, 'sha': HEAD, 'statuses': []}}


class WatchTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.data = observation()
        self.clock = 0
        self.calls, self.sleeps = [], []
        self.addCleanup(patch.stopall)
        patch.object(ci, 'local_head', return_value=HEAD).start()
        patch.object(ci.time, 'monotonic', side_effect=lambda: self.clock).start()
        patch.object(ci.time, 'sleep', side_effect=self.sleep).start()
        patch.object(ci, 'command_output', side_effect=self.api).start()

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.clock += seconds

    def api(self, command, **kwargs):
        self.calls.append(command[-1])
        self.assertLessEqual(kwargs['timeout'], 20)
        endpoint = command[-1]
        key = ('pr' if '/pulls/' in endpoint else 'runs' if '/actions/' in endpoint
               else 'checks' if '/check-runs?' in endpoint else 'statuses')
        return json.dumps(self.data[key])

    def watch(self, **kwargs):
        return ci.watch('owner/repo', 12, HEAD, self.output, **kwargs)

    def test_success_retains_raw_evidence_and_one_compact_terminal_result(self):
        # Given all expected workflows, checks and statuses are complete.
        result = self.watch()
        # When watched, then five bounded calls collect evidence and guard the final PR.
        self.assertEqual((result['state'], result['requests'], result['polls']), ('ci-complete', 5, 1))
        self.assertEqual(json.loads((self.output / 'observation.json').read_text()), self.data)
        self.assertEqual(json.loads((self.output / 'result.json').read_text()), {**result, 'base': list(result['base'])})
        self.assertEqual(self.sleeps, [])

    def test_backoff_reduces_requests_with_the_same_terminal_detection_time(self):
        # Given CI completes at 105 seconds; each poll requires the same four endpoints.
        original = self.api

        def delayed(command, **kwargs):
            self.data['runs']['workflow_runs'][0]['status'] = 'in_progress' if self.clock < 105 else 'completed'
            return original(command, **kwargs)

        with patch.object(ci, 'command_output', side_effect=delayed):
            result = self.watch()
        # When watched, then polls at 0/15/45/105 use 17 calls versus 33 at fixed 15s.
        self.assertEqual(result['state'], 'ci-complete')
        self.assertEqual((result['polls'], result['requests'], self.clock), (4, 17, 105))
        self.assertEqual(self.sleeps, [15, 30, 60])

    def test_missing_workflow_empty_checks_and_pending_status_wait_until_budget(self):
        for fault in ('workflow', 'checks', 'status'):
            with self.subTest(fault=fault):
                self.data = observation()
                if fault == 'workflow':
                    self.data['runs']['workflow_runs'].pop()
                    self.data['runs']['total_count'] -= 1
                elif fault == 'checks':
                    self.data['checks'] = {'total_count': 0, 'check_runs': []}
                else:
                    self.data['statuses'].update(total_count=1, statuses=[{'context': 'external', 'state': 'pending'}])
                result = self.watch(max_requests=5)
                self.assertEqual((result['state'], result['requests']), ('incomplete', 5))

    def test_each_failure_and_required_workflow_skip_blocks_success(self):
        for key, field, value in [('runs', 'conclusion', 'skipped'), ('runs', 'conclusion', 'failure'),
                                  ('checks', 'conclusion', 'cancelled'), ('checks', 'conclusion', 'timed_out')]:
            with self.subTest(key=key, value=value):
                self.data = observation()
                rows = 'workflow_runs' if key == 'runs' else 'check_runs'
                self.data[key][rows][0][field] = value
                self.assertEqual(self.watch()['state'], 'failed')
        self.data = observation()
        self.data['statuses'].update(total_count=1, statuses=[{'context': 'external', 'state': 'error'}])
        self.assertEqual(self.watch()['state'], 'failed')

    def test_newest_run_and_app_scoped_check_replace_old_results(self):
        # Given a new failed invocation after successful CI and an optional skipped workflow.
        self.data['runs']['workflow_runs'].append({**self.data['runs']['workflow_runs'][0], 'id': 20, 'conclusion': 'failure'})
        self.data['runs']['workflow_runs'].append({**self.data['runs']['workflow_runs'][0], 'id': 21,
                                                 'path': '.github/workflows/performance.yml', 'conclusion': 'skipped'})
        self.data['runs']['total_count'] += 2
        self.assertEqual(self.watch()['state'], 'failed')
        self.data['runs']['workflow_runs'][-2]['conclusion'] = 'success'
        self.data['checks']['check_runs'].insert(0, {**self.data['checks']['check_runs'][0], 'id': 0, 'conclusion': 'failure'})
        self.data['checks']['total_count'] += 1
        # When the newest check succeeds, then the old failure is superseded for that app only.
        self.assertEqual(self.watch()['state'], 'ci-complete')
        self.data['checks']['check_runs'][0]['app'] = {'id': 2}
        self.assertEqual(self.watch()['state'], 'failed')

    def test_other_pr_or_base_cannot_supply_success_for_the_same_head(self):
        # Given the requested PR is pending and newer successful runs exist for another scope.
        for fault in ('number', 'base_sha', 'base_ref', 'base_repo', 'head_repo', 'missing'):
            with self.subTest(fault=fault):
                self.data = observation()
                other = copy.deepcopy(self.data['runs']['workflow_runs'])
                for run in other:
                    run['id'] += 100
                    candidate = run['pull_requests'][0]
                    if fault == 'number':
                        candidate['number'] = 13
                    elif fault == 'base_sha':
                        candidate['base']['sha'] = 'c' * 40
                    elif fault == 'base_ref':
                        candidate['base']['ref'] = 'release'
                    elif fault == 'missing':
                        run['pull_requests'] = []
                    else:
                        candidate['base' if fault == 'base_repo' else 'head']['repo']['id'] = 2
                self.data['runs']['workflow_runs'][0]['status'] = 'in_progress'
                self.data['runs']['workflow_runs'] += other
                self.data['runs']['total_count'] += len(other)
                # When collected, then other-scope successes never supersede this PR's pending run.
                self.assertEqual(self.watch(max_requests=5)['state'], 'incomplete')
                self.data['runs']['workflow_runs'] = other
                self.data['runs']['total_count'] = len(other)
                self.assertEqual(self.watch(max_requests=5)['state'], 'incomplete')

    def test_malformed_paginated_or_wrong_revision_evidence_is_incomplete(self):
        for fault in ('runs', 'checks', 'statuses', 'shape', 'event', 'sha'):
            with self.subTest(fault=fault):
                self.data = observation()
                if fault in ('runs', 'checks', 'statuses'):
                    self.data[fault]['total_count'] += 1
                elif fault == 'shape':
                    del self.data['checks']['check_runs']
                elif fault == 'event':
                    self.data['runs']['workflow_runs'][0]['event'] = 'push'
                else:
                    self.data['statuses']['sha'] = BASE
                self.assertEqual(self.watch()['state'], 'incomplete')

    def test_remote_head_base_and_closed_pr_invalidate_complete_observation(self):
        original = self.api
        for fault in ('head', 'base', 'closed'):
            self.data = observation()
            calls = 0

            def changed(command, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 5:
                    if fault == 'closed':
                        self.data['pr']['state'] = 'closed'
                    else:
                        self.data['pr'][fault]['sha'] = 'c' * 40
                return original(command, **kwargs)

            with self.subTest(fault=fault), patch.object(ci, 'command_output', side_effect=changed):
                self.assertEqual(self.watch()['state'], 'stale')
        with patch.object(ci, 'local_head', side_effect=[HEAD, HEAD, BASE]):
            self.data = observation()
            self.assertEqual(self.watch()['state'], 'stale')

    def test_transient_error_retries_but_failed_final_guard_cannot_be_success(self):
        original = self.api
        for error in (subprocess.TimeoutExpired('gh', 20), subprocess.CalledProcessError(1, 'gh'),
                      json.JSONDecodeError('bad', '', 0)):
            calls = 0

            def unavailable(command, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 5:
                    raise error
                return original(command, **kwargs)

            with self.subTest(error=type(error).__name__), patch.object(ci, 'command_output', side_effect=unavailable):
                self.assertEqual(self.watch(max_requests=5)['state'], 'incomplete')
                calls = 0
                self.assertEqual(self.watch()['state'], 'ci-complete')
        with patch.object(ci, 'command_output', side_effect=subprocess.TimeoutExpired('gh', 20)):
            result = self.watch(timeout=50)
        self.assertEqual((result['state'], result['elapsed_seconds']), ('incomplete', 50))


class ProcessTest(unittest.TestCase):
    def test_timeout_and_sigterm_reap_the_active_request(self):
        # Given a CLI request that never completes, using local executables without network access.
        for cancel in (False, True):
            with self.subTest(cancel=cancel), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'git').write_text('#!/bin/sh\necho ' + HEAD + '\n')
                (root / 'gh').write_text(f'#!{sys.executable}\nimport os,time\nfrom pathlib import Path\nPath("child.pid").write_text(str(os.getpid()))\ntime.sleep(60)\n')
                for name in ('git', 'gh'):
                    (root / name).chmod(0o755)
                process = subprocess.Popen([sys.executable, str(Path(ci.__file__).resolve()), 'owner/repo', '12', HEAD,
                                            '--timeout', '5' if cancel else '1'], cwd=root,
                                           env={**os.environ, 'PATH': str(root) + os.pathsep + os.environ['PATH']},
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic() + 4
                    while not (root / 'child.pid').exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue((root / 'child.pid').exists(), 'request never started')
                    pid = int((root / 'child.pid').read_text())
                    if cancel:
                        process.send_signal(signal.SIGTERM)
                    stdout, stderr = process.communicate(timeout=4)
                    # When stopped or timed out, then the child is gone and a non-success result is saved.
                    self.assertEqual(process.returncode, 130 if cancel else 2, stderr)
                    self.assertEqual(len(stdout.splitlines()), 1)
                    self.assertEqual(json.loads(stdout)['state'], 'interrupted' if cancel else 'incomplete')
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()
