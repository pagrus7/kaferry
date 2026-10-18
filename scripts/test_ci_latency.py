"""Report statistics only after a useful sample, without hiding failed heads."""

import unittest

from ci_latency import WORKFLOWS, summary


class LatencyTest(unittest.TestCase):
    def test_complete_heads_include_queue_and_failure_but_not_partial_runs(self):
        # Given twenty complete heads plus one head missing a workflow.
        runs = [{'head_sha': str(i), 'name': name, 'status': 'completed',
                 'conclusion': 'failure' if i == 19 and name == 'quality' else 'success',
                 'created_at': '2026-10-01T00:00:00Z',
                 'updated_at': f'2026-10-01T00:{i + 1:02d}:00Z'}
                for i in range(20) for name in WORKFLOWS]
        runs.append({**runs[0], 'head_sha': 'incomplete'})
        # When summarized, then percentiles use complete heads and failures remain visible.
        result = summary(runs)
        self.assertEqual((result['samples'], result['failed_heads']), (20, 1))
        self.assertEqual((result['p50_seconds'], result['p95_seconds']), (600, 1140))
        self.assertIsNone(summary(runs[:3])['p95_seconds'])
        # Given a rerun to green, when summarized, then its human wait cannot skew latency.
        runs[0]['run_attempt'] = 2
        result = summary(runs)
        self.assertEqual((result['samples'], result['rerun_heads'], result['failed_heads']), (19, 1, 1))
        self.assertIsNone(result['p95_seconds'])
