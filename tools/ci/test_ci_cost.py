"""Cache accounting follows transfers and successful saves, including failed jobs."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.ci_cost import cache_metrics, seconds, collect, miss_rate, write_summary


class CostTest(unittest.TestCase):
    def test_transfer_sizes_and_per_job_misses(self):
        # Given interleaved restores and uploads, including a failed reservation.
        log = '\n'.join([
            'fast\tsetup\tCache Size: ~2 MB (2345678 B)',
            'fault\tsetup\tCache not found for input keys: weekly',
            'fast\tpost\tSent 0 of 3456789 (0.0%)',
            'fast\tpost\tSent 3456789 of 3456789 (100.0%)',
            'fault\tpost\tSent 0 of 4567890 (0.0%)',
            'fast\tpost\tCache saved with key: weekly',
            'fault\tpost\tUnable to reserve cache with key weekly',
            'noise without tab fields',
        ])
        # When accounting, then count actual bytes once and successful saves only.
        jobs = cache_metrics(log)
        self.assertEqual(dict(restored_bytes=2345678, saved_bytes=3456789,
                              cache_miss=False, cache_job=True), jobs['fast'])
        self.assertEqual(dict(restored_bytes=0, saved_bytes=0,
                              cache_miss=True, cache_job=True), jobs['fault'])

    def test_missing_upload_size_is_unavailable(self):
        # Given a saved key with no transfer size, when parsing, then do not invent zero.
        with self.assertRaisesRegex(ValueError, 'no byte count'):
            cache_metrics('fast\tpost\tCache saved with key: weekly')

    def test_job_duration_crosses_midnight(self):
        self.assertEqual(2, seconds('2026-11-08T23:59:59Z', '2026-11-09T00:00:01Z'))

    def test_miss_rate_counts_all_executed_jobs_and_empty_is_unavailable(self):
        # Given one miss among ten executed jobs, when reporting, then use ten.
        self.assertEqual(.1, miss_rate([dict(job_count=10, cache_misses=1, cache_jobs=2)]))
        self.assertIsNone(miss_rate([dict(job_count=0, cache_misses=0)]))

    def test_collection_failure_removes_comparison_ready_outputs(self):
        # Given partial evidence, when summarizing, then fail and retain its cause.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root/'summary.json').write_text('{}')
            (root/'pushes.json').write_text('{}')
            with self.assertRaisesRegex(SystemExit, 'incomplete collection'):
                write_summary(root, [], [dict(id=12, attempt=1, error='logs unavailable')])
            self.assertFalse((root/'summary.json').exists())
            self.assertFalse((root/'pushes.json').exists())
            self.assertEqual(False, json.loads((root/'collection.json').read_text())['complete'])

    def test_rerun_collects_its_own_jobs_logs_and_start_time(self):
        # Given two attempts of one workflow with a long delay between them.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for attempt, start, end, size in [(1, '00:00:00', '00:00:10', 111),
                                              (2, '01:00:00', '01:00:20', 222)]:
                run = dict(id=12, run_attempt=attempt, name='checks', head_sha='abc',
                           head_branch='branch', event='pull_request', conclusion='success',
                           created_at='2026-11-08T00:00:00Z', run_started_at='2026-11-08T'+start+'Z')
                jobs = dict(jobs=[dict(name='fast', conclusion='success',
                                      started_at=run['run_started_at'], completed_at='2026-11-08T'+end+'Z')])
                with patch('tools.ci.ci_cost.gh', side_effect=[json.dumps([jobs]),
                           f'fast\tsetup\tCache Size: ~1 MB ({size} B)']) as api:
                    result = collect(run, root)
                self.assertEqual(size, result['restored_bytes'])
                self.assertEqual(attempt*10, result['wall_seconds'])
                self.assertIn(f'/attempts/{attempt}/jobs?', api.call_args_list[0].args[-1])
                self.assertIn(str(attempt), api.call_args_list[1].args)
                self.assertTrue((root/'12'/str(attempt)/'log.txt').is_file())

    def test_completed_run_refreshes_pending_job_before_measuring(self):
        # Given a completed workflow whose job list still has a pending row.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = dict(id=12, run_attempt=1, name='checks', head_sha='abc',
                       head_branch='branch', event='pull_request', conclusion='success',
                       run_started_at='2026-11-08T00:00:00Z')
            pending = dict(id=7, name='fast', conclusion=None,
                           started_at=run['run_started_at'], completed_at=None)
            finished = dict(pending, conclusion='success', completed_at='2026-11-08T00:00:20Z')
            with patch('tools.ci.ci_cost.gh', side_effect=[json.dumps([dict(jobs=[pending])]),
                       json.dumps(finished), 'fast\tsetup\tCache Size: ~1 MB (222 B)']) as api:
                result = collect(run, root)
            self.assertEqual(20, result['wall_seconds'])
            self.assertEqual('repos/pagrus7/kaferry/actions/jobs/7', api.call_args_list[1].args[-1])

    def test_pending_job_cannot_produce_metrics_and_cached_row_is_refreshed(self):
        # Given unavailable completion evidence, when collecting again, then
        # refresh the job and fail explicitly rather than cache a partial total.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root/'12'/'1'
            target.mkdir(parents=True)
            pending = dict(id=7, name='fast', conclusion=None,
                           started_at='2026-11-08T00:00:00Z', completed_at=None)
            (target/'jobs.json').write_text(json.dumps([pending]))
            run = dict(id=12, run_attempt=1)
            with patch('tools.ci.ci_cost.gh', return_value=json.dumps(pending)) as api:
                with self.assertRaisesRegex(ValueError, 'job evidence is incomplete'):
                    collect(run, root)
            api.assert_called_once()
            self.assertFalse((target/'metrics.json').exists())
