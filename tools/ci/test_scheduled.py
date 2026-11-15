"""Scheduled reuse requires real executions, matching scope and recent evidence."""

from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

import tools.ci.scheduled as scheduled


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

    def test_newer_cases_without_an_aggregate_job_require_retry(self):
        for result in ('failure', 'cancelled', 'success'):
            with self.subTest(result=result):
                history = [run(2, matrix(result=result, days=0)[1:]), run(1, matrix())]
                decision = self.choose('compatibility', ['full'], history)[0]
                self.assertTrue(decision['run'])
                self.assertIn('retry required', decision['reason'])

    def test_older_history_page_can_contain_the_latest_failed_rerun(self):
        env = {'EVENT': 'schedule', 'GITHUB_REPOSITORY': 'example/repo', 'GITHUB_SHA': HEAD}
        pages = [{'workflow_runs': [run(2, [], days=1)]}, {'workflow_runs': [run(1, [], days=0)]}]
        with patch('tools.ci.scheduled.gh', side_effect=[pages, [{'jobs': [suite('fault', days=1)]}], [{'jobs': [suite('fault', 'failure', 0)]}]]) as api:
            decision = scheduled.plan('qualification', 'nightly', env)[0]
            self.assertTrue(decision['run'])
            self.assertIn('retry required', decision['reason'])
            self.assertTrue(api.call_args_list[0].kwargs['paginate'])
            self.assertIn('head_sha=' + HEAD, api.call_args_list[0].args[0])

    def test_truncated_history_requires_execution_instead_of_reuse(self):
        env = {'EVENT': 'schedule', 'GITHUB_REPOSITORY': 'example/repo', 'GITHUB_SHA': HEAD}
        pages = [{'workflow_runs': [run(1, [])], 'total_count': 2}]
        with patch('tools.ci.scheduled.gh', return_value=pages) as api:
            decisions = scheduled.plan('qualification', 'nightly', env)
            self.assertTrue(all(d['run'] for d in decisions))
            self.assertTrue(all('history unavailable' in d['reason'] for d in decisions))
            self.assertEqual(1, api.call_count)


if __name__ == '__main__':
    unittest.main()
