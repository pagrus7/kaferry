"""Scheduled reuse requires real executions, matching scope and recent evidence."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import scheduled


NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
HEAD = 'a' * 40


def stamp(days=1):
    return (NOW - timedelta(days=days)).isoformat()


def job(name, step, result='success', days=1):
    return {'name': name, 'conclusion': result, 'started_at': stamp(days), 'completed_at': stamp(days),
            'steps': [{'name': step, 'conclusion': result, 'completed_at': stamp(days)}]}


def suite(name, result='success', days=1):
    return job(f'qualification / suite ({name})', 'Run suite', result, days)


def matrix(tier='full', result='success', days=1):
    return [job(f'report ({tier})', 'Require complete matching evidence', result, days),
            job('cases (broker-fixture)', 'Run standalone suite', result, days)]


def run(identifier, jobs, head=HEAD, days=1):
    return {'id': identifier, 'head_sha': head, 'status': 'completed', 'updated_at': stamp(days),
            'html_url': f'https://github.com/example/repo/actions/runs/{identifier}', 'jobs': jobs}


class ScheduledTests(unittest.TestCase):
    def choose(self, kind, keys, runs, weekly=False):
        inventory = {r['id']: r['jobs'] for r in runs}
        return scheduled.select(kind, keys, HEAD, runs, inventory.__getitem__, NOW, weekly)

    def test_matching_success_reuses_each_suite_independently(self):
        # Given a passing fault suite and failed security suite at the same commit.
        history = [run(1, [suite('fault'), suite('qualification', 'failure')])]
        # When planning, then only the failed suite must run again.
        decisions = self.choose('qualification', ['fault', 'qualification'], history)
        self.assertEqual([False, True], [d['run'] for d in decisions])
        self.assertEqual(stamp(), decisions[0]['executed_at'])

    def test_other_commits_and_nightly_coverage_do_not_qualify_full_matrix(self):
        history = [run(2, matrix('nightly')), run(1, matrix(), head='b' * 40)]
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_latest_failed_or_cancelled_attempt_overrides_older_success(self):
        for result in ('failure', 'cancelled', 'timed_out'):
            with self.subTest(result=result):
                history = [run(2, [suite('fault', result, 0)]), run(1, [suite('fault')])]
                self.assertTrue(self.choose('qualification', ['fault'], history)[0]['run'])

    def test_rerun_of_old_run_id_can_be_latest_failed_attempt(self):
        # Given API creation order differs from the order of actual executions.
        history = [run(2, [suite('fault', days=2)]), run(1, [suite('fault', 'failure', 0)])]
        self.assertTrue(self.choose('qualification', ['fault'], history)[0]['run'])

    def test_newer_successful_rerun_clears_older_failure(self):
        history = [run(2, [suite('fault', 'failure', 2)]), run(1, [suite('fault', days=0)])]
        self.assertFalse(self.choose('qualification', ['fault'], history)[0]['run'])

    def test_report_only_rerun_cannot_erase_a_newer_failed_matrix(self):
        rerun = matrix(days=3)
        rerun[0] = matrix(days=0)[0]
        history = [run(2, matrix(result='failure', days=1)), run(1, rerun, days=0)]
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_skips_do_not_refresh_evidence_age(self):
        skipped = [run(i, [suite('soak', 'skipped', 0)], days=0) for i in range(3, 0, -1)]
        for age, must_run in ((29, False), (30, True), (31, True)):
            with self.subTest(age=age):
                history = skipped + [run(0, [suite('soak', days=age)], days=age)]
                decision = self.choose('qualification', ['soak'], history)[0]
                self.assertEqual(must_run, decision['run'])
                if not must_run:
                    self.assertEqual(stamp(age), decision['executed_at'])
                    self.assertTrue(decision['url'].endswith('/0'))

    def test_skipped_matrix_does_not_qualify_or_erase_failure(self):
        history = [run(3, matrix(result='skipped', days=0)), run(2, matrix(result='failure')), run(1, matrix(days=2))]
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_cancelled_cases_with_skipped_report_require_retry(self):
        jobs = matrix(result='skipped')
        jobs[1]['conclusion'] = 'cancelled'
        self.assertTrue(self.choose('compatibility', ['full'], [run(2, jobs), run(1, matrix(days=2))])[0]['run'])

    def test_passing_aggregate_without_real_passing_cases_is_insufficient(self):
        for cases in ([], matrix(result='skipped')[1:]):
            history = [run(1, matrix()[:1] + cases)]
            self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_partial_reruns_preserve_unrerun_cases_and_replace_old_outcomes(self):
        old = matrix(result='failure', days=2)
        fresh = [{**j, 'run_attempt': 2, 'id': i} for i, j in enumerate(matrix(days=1), 10)]
        history = [run(1, old + fresh)]
        self.assertFalse(self.choose('compatibility', ['full'], history)[0]['run'])
        # Given another case was not rerun, its original age still limits the matrix.
        history[0]['jobs'].append(job('cases (unrerun)', 'Run standalone suite', days=31))
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_skipped_rerun_jobs_keep_prior_execution_date_and_failure(self):
        for result, must_run in (('success', False), ('failure', True)):
            with self.subTest(result=result):
                original = matrix(result=result, days=5)
                skipped = [{**j, 'run_attempt': 2} for j in matrix(result='skipped', days=0)]
                skipped.append(job('cases', 'Run standalone suite', 'skipped', 0))
                decision = self.choose('compatibility', ['full'], [run(1, original + skipped, days=0)])[0]
                self.assertEqual(must_run, decision['run'])
                if not must_run:
                    self.assertEqual(stamp(5), decision['executed_at'])

    def test_all_job_history_pages_are_used(self):
        env = {'EVENT': 'schedule', 'GITHUB_REPOSITORY': 'example/repo', 'GITHUB_SHA': HEAD}
        jobs = matrix()
        old = job('cases (unrerun)', 'Run standalone suite', days=31)
        with patch('scheduled.gh', side_effect=[{'workflow_runs': [run(1, [], days=0)]}, [{'jobs': jobs}, {'jobs': [old]}]]) as api:
            decision = scheduled.plan('compatibility', 'full', env)[0]
            self.assertTrue(decision['run'])
            self.assertIn('expired', decision['reason'])
            self.assertIn('filter=all', api.call_args.args[0])
            self.assertTrue(api.call_args.kwargs['paginate'])

    def test_report_rerun_does_not_refresh_old_matrix_cases(self):
        jobs = matrix(days=31)
        jobs[0] = matrix(days=0)[0]
        history = [run(1, jobs, days=0)]
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_matrix_uses_oldest_case_for_freshness(self):
        history = [run(1, matrix() + [job('cases (older)', 'Run standalone suite', days=30)])]
        self.assertTrue(self.choose('compatibility', ['full'], history)[0]['run'])

    def test_missing_step_or_future_execution_cannot_qualify(self):
        for candidate in (suite('fault', days=-1), {**suite('fault'), 'steps': []}):
            self.assertTrue(self.choose('qualification', ['fault'], [run(1, [candidate])])[0]['run'])

    def test_weekly_fuzz_always_runs_but_other_suites_can_reuse(self):
        history = [run(1, [suite(name) for name in scheduled.WEEKLY])]
        decisions = self.choose('qualification', scheduled.WEEKLY, history, weekly=True)
        self.assertEqual(['fuzz'], [d['suite'] for d in decisions if d['run']])

    def test_explicit_pr_and_release_runs_do_not_read_history(self):
        with patch('scheduled.gh', side_effect=AssertionError('history must not be queried')):
            for event in ('pull_request', 'workflow_dispatch', 'push', 'workflow_call'):
                for kind in ('compatibility', 'qualification'):
                    with self.subTest(event=event, kind=kind):
                        decisions = scheduled.plan(kind, 'full', {'EVENT': event})
                        self.assertTrue(all(d['run'] for d in decisions))
                        if kind == 'qualification' and event == 'workflow_dispatch':
                            self.assertEqual(scheduled.WEEKLY, [d['suite'] for d in decisions])
            self.assertTrue(scheduled.plan('compatibility', 'full', {'EVENT': 'schedule', 'FORCE_RUN': 'true'})[0]['run'])

    def test_history_failure_runs_tests_and_hides_cli_stderr(self):
        env = {'EVENT': 'schedule', 'GITHUB_REPOSITORY': 'example/repo', 'GITHUB_SHA': HEAD}
        errors = [subprocess.TimeoutExpired(['gh'], 15), subprocess.CalledProcessError(1, ['gh'], stderr='private text'), ValueError('bad JSON')]
        for error in errors:
            with self.subTest(error=type(error).__name__), patch('scheduled.gh', side_effect=error):
                decisions = scheduled.plan('qualification', 'nightly', env)
                self.assertTrue(all(d['run'] for d in decisions))
                self.assertNotIn('private text', json.dumps(decisions))

    def test_history_query_is_bounded_and_uses_completed_scheduled_main_runs(self):
        env = {'EVENT': 'schedule', 'GITHUB_REPOSITORY': 'example/repo', 'GITHUB_SHA': HEAD}
        with patch('scheduled.gh', return_value={'workflow_runs': []}) as api:
            scheduled.plan('compatibility', 'full', env)
            self.assertIn('branch=main&event=schedule&status=completed&per_page=100', api.call_args.args[0])
        with patch('scheduled.time.monotonic', return_value=10), patch('scheduled.subprocess.check_output', return_value='{}') as command:
            with self.assertRaises(TimeoutError):
                scheduled.gh('unused', 9)
            command.assert_not_called()
            scheduled.gh('path', 55)
            self.assertLessEqual(command.call_args.kwargs['timeout'], 15)

    def test_outputs_skip_expensive_jobs_and_summary_links_original_execution(self):
        decisions = self.choose('qualification', ['fault'], [run(1, [suite('fault')])])
        with tempfile.TemporaryDirectory() as name:
            output, summary = Path(name) / 'output', Path(name) / 'summary'
            with patch.dict(os.environ, {'GITHUB_OUTPUT': str(output), 'GITHUB_STEP_SUMMARY': str(summary), 'GITHUB_SHA': HEAD}), patch('scheduled.plan', return_value=decisions):
                scheduled.main(['qualification'])
            self.assertEqual('run=false\nsuites=[]\n', output.read_text())
            self.assertIn('Reuse; tests skipped', summary.read_text())
            self.assertIn(stamp(), summary.read_text())
            self.assertIn('/actions/runs/1', summary.read_text())


if __name__ == '__main__':
    unittest.main()
