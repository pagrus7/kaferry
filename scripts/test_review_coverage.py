"""Coverage evidence must preserve repeated hits and expose all changed-line gaps."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from review_coverage import collect, gap_ledger, sum_blocks, verified_run


class ReviewCoverageTest(unittest.TestCase):
    def test_duplicate_blocks_sum_and_zero_cannot_erase_a_hit(self):
        # Given repeated blocks from both unit and integration instrumentation.
        profiles = ['mode: atomic\nm/internal/a.go:10.1,12.2 2 3\nm/internal/a.go:12.3,13.1 1 0\n',
                    'mode: atomic\nm/internal/a.go:10.1,12.2 2 0\nm/internal/a.go:10.1,12.2 2 4\n']
        # When summed, then the covered prefix remains covered and its shared final line is partial.
        blocks = sum_blocks(profiles)
        self.assertEqual(blocks[('m/internal/a.go', 10, 1, 12, 2)], (2, 7))
        codecov = {'report': {'files': [{'name': 'internal/a.go', 'line_coverage': [[10, 0], [12, 2]]}]}}
        self.assertEqual(gap_ledger(blocks, {'internal/a.go': {10, 12}}, codecov, 'm'),
                         [{'file': 'internal/a.go', 'line': 12, 'go': 'partial', 'codecov': 'partial', 'agreement': True, 'decision': None}])

    def test_codecov_only_gap_and_disagreement_are_not_dropped(self):
        # Given a Codecov missing line absent from Go blocks.
        report = {'report': {'files': [{'name': 'internal/a.go', 'line_coverage': [[20, 1]]}]}}
        # When reconciled, then it remains in the ledger for human resolution.
        result = gap_ledger({}, {'internal/a.go': {20}}, report, 'm')
        self.assertEqual(result[0]['codecov'], 'miss')
        self.assertFalse(result[0]['agreement'])

    def test_stale_or_failed_run_is_rejected(self):
        # Given final-head evidence or a stale, incomplete or failed run.
        run = {'head_sha': 'a' * 40, 'status': 'completed', 'conclusion': 'success'}
        verified_run(run, 'a' * 40)
        for changes in ({'head_sha': 'b' * 40}, {'status': 'in_progress'}, {'conclusion': 'failure'}):
            # When verified, then none can establish final-head coverage.
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, 'reviewed SHA'):
                verified_run({**run, **changes}, 'a' * 40)

    def test_non_production_patch_needs_no_coverage_network_requests(self):
        # Given a test/documentation-only frozen diff.
        with tempfile.TemporaryDirectory() as name, patch('review_coverage.git', side_effect=['a' * 40, 'README.md\ninternal/a_test.go']), patch('review_coverage.github') as api:
            # When collecting, then N/A is recorded without downloading profiles or Codecov.
            result = collect('a' * 40, 'b' * 40, 'owner/repo', Path(name) / 'new')
            self.assertEqual(result['patch_coverage'], 'N/A')
            self.assertEqual(result['gaps'], [])
            api.assert_not_called()

    def test_invalid_profiles_fail_instead_of_inventing_coverage(self):
        for profiles in (['wrong'], ['mode: atomic\nbad'], ['mode: atomic\nm/a.go:1.1,2.1 1 0\nm/a.go:1.1,2.1 2 1']):
            with self.subTest(profiles=profiles), self.assertRaises(ValueError):
                sum_blocks(profiles)
