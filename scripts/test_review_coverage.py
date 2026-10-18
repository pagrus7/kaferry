"""Coverage evidence must preserve repeated hits and expose all changed-line gaps."""

from pathlib import Path
import hashlib
import io
import json
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from review_coverage import collect, gap_ledger, sum_blocks, verified_run


class ReviewCoverageTest(unittest.TestCase):
    def test_production_collection_accepts_matching_evidence_and_rejects_substitutes(self):
        # Given actual ZIP/profile shapes and the API responses for their reviewed source.
        base, head, tested = 'a' * 40, 'b' * 40, 'c' * 40
        for fault in (None, 'digest', 'tree', 'dirty', 'identity', 'patch', 'codecov', 'sessions', 'compare'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                artifacts, archives = [], {}
                for index, (name, label, hits) in enumerate([('checks-fast', 'unit', 3), ('checks-kafka', 'integration', 0)]):
                    data = io.BytesIO()
                    with zipfile.ZipFile(data, 'w') as zipped:
                        zipped.writestr('coverage.out', f'mode: atomic\nm/internal/a.go:10.1,11.1 1 {hits}\nm/internal/a.go:12.1,13.1 2 0\n')
                        zipped.writestr(f'{label}.environment.json', json.dumps({'commit': '--invalid' if fault == 'identity' else tested, 'working_tree_dirty': fault == 'dirty'}))
                    archives[index] = data.getvalue()
                    digest = hashlib.sha256(data.getvalue()).hexdigest()
                    artifacts.append({'id': index, 'name': name, 'expired': False, 'digest': 'sha256:' + ('bad' if fault == 'digest' else digest)})

                def api(endpoint):
                    if '/workflows/' in endpoint:
                        return {'workflow_runs': [{'id': 12, 'head_sha': head, 'status': 'completed', 'conclusion': 'success'}]}
                    if endpoint.endswith('/artifacts'):
                        return {'artifacts': artifacts}
                    return {'tree': {'sha': 'different' if fault == 'tree' and endpoint.endswith(tested) else 'source-tree'}}

                def git(*args):
                    if args[0] == 'merge-base':
                        return base
                    if args[0] == 'show':
                        return 'module m\n'
                    if '--name-only' in args:
                        return 'internal/a.go'
                    return '@@ -10,3 +10,3 @@\n'

                codecov = {'commitid': head, 'state': 'pending' if fault == 'codecov' else 'complete', 'totals': {'sessions': 1 if fault == 'sessions' else 2},
                           'report': {'files': [{'name': 'internal/a.go', 'line_coverage': [[10, 0], [12, 1]]}]}}
                comparison = {'base_commit': 'wrong' if fault == 'compare' else base, 'head_commit': head,
                              'totals': {'patch': {'misses': 0 if fault == 'patch' else 1, 'partials': 0}}}
                responses = [io.BytesIO(json.dumps(value).encode()) for value in (codecov, comparison)]
                with patch('review_coverage.git', side_effect=git), patch('review_coverage.github', side_effect=api), \
                        patch('review_coverage.subprocess.check_output', side_effect=lambda command: archives[int(command[-1].split('/')[-2])]), \
                        patch('review_coverage.urlopen', side_effect=responses):
                    # When the entire evidence boundary runs, then a substituted artifact,
                    # source tree or patch report cannot produce a completed manifest.
                    destination = Path(directory) / 'evidence'
                    if fault:
                        with self.assertRaises(ValueError):
                            collect(base, head, 'owner/repo', destination)
                        self.assertFalse((destination / 'manifest.json').exists())
                    else:
                        result = collect(base, head, 'owner/repo', destination)
                        self.assertEqual(result['run_id'], 12)
                        self.assertEqual(result['gaps'], [{'file': 'internal/a.go', 'line': 12, 'go': 'miss', 'codecov': 'miss', 'agreement': True, 'decision': None}])
                        self.assertTrue((destination / 'manifest.json').exists())

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
