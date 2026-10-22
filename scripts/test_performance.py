"""Only complete, compatible workload results can become comparison data."""

from contextlib import chdir, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import patch

from performance import CASES, execute, measure, pair_summary, plan, previous


class PerformanceTest(unittest.TestCase):
    def test_pair_ratios_are_paired_before_summarizing(self):
        # Given large absolute differences between runs but known within-pair ratios.
        pairs = [{'base': {'records_per_second': base}, 'head': {'records_per_second': head}}
                 for base, head in [(100, 90), (1000, 1100), (200, 200)]]
        # When summarized, then the median is the paired ratio, not pooled throughput.
        result = pair_summary(pairs)
        self.assertEqual(result, {'head_over_base': [.9, 1.1, 1.0], 'median': 1.0, 'minimum': .9, 'maximum': 1.1})

    def test_calibration_always_runs_but_unchanged_comparison_can_skip(self):
        # Given nightly calibration or a retained compatible comparison baseline.
        for mode, baseline, run in [('calibration', None, True), ('comparison', {'head': 'b' * 40}, False),
                                     ('comparison', {'head': 'a' * 40}, True), ('comparison', None, True)]:
            with self.subTest(mode=mode, baseline=baseline), tempfile.TemporaryDirectory() as name:
                output = Path(name) / 'outputs'
                with patch.dict(os.environ, {'GITHUB_OUTPUT': str(output), 'EVENT': 'schedule', 'BASE_INPUT': '',
                                              'PERFORMANCE_MODE': mode, 'GITHUB_REPOSITORY': 'owner/repo'}), \
                        patch('performance.resolve', return_value='b' * 40), patch('performance.previous', return_value=baseline), redirect_stdout(io.StringIO()):
                    # When planned, then an absent baseline calibrates rather than claiming a comparison.
                    plan()
                    self.assertIn(f'run={str(run).lower()}', output.read_text())
                    self.assertIn('base=' + (baseline['head'] if baseline else 'b' * 40), output.read_text())

    def test_failed_skipped_or_wrong_workload_cannot_supply_a_measurement(self):
        # Given one expected workload and possible stale or invalid metrics.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            binary = root / 'binary'
            binary.write_bytes(b'fixture')
            metrics = {'test': CASES['file'], 'records': 10, 'records_per_second': 100, 'passed': True, 'skipped': False}
            for index, (changes, valid) in enumerate([({}, True), ({'passed': False}, False), ({'skipped': True}, False),
                                                     ({'records': 9}, False), ({'test': 'different'}, False)]):
                def record(_, command, directory):
                    (directory / 'case.metrics.json').write_text(json.dumps({**metrics, **changes}))
                    return 0
                with self.subTest(changes=changes), patch.dict(os.environ), patch('performance.run', side_effect=record):
                    destination = root / str(index)
                    # When measured, then invalid data never reaches the paired summary.
                    if valid:
                        self.assertEqual(measure(root / 'harness', binary, 'file', destination, {'file': 10, 'kafka': 10}), metrics)
                        with self.assertRaises(FileExistsError):
                            measure(root / 'harness', binary, 'file', destination, {'file': 10, 'kafka': 10})
                    else:
                        with self.assertRaises(ValueError):
                            measure(root / 'harness', binary, 'file', destination, {'file': 10, 'kafka': 10})

    def test_archive_measurement_requires_the_requested_format(self):
        # Given a complete report for a different archive representation.
        with tempfile.TemporaryDirectory() as name, patch.dict(os.environ):
            root = Path(name)
            binary = root / 'binary'
            binary.write_bytes(b'fixture')
            result = {'test': 'TestLoadArchiveCopy/file', 'records': 10,
                      'records_per_second': 100, 'passed': True, 'skipped': False,
                      'archive_format': 'cbor'}
            def record(_, command, directory):
                (directory / 'case.metrics.json').write_text(json.dumps(result))
                return 0
            with patch('performance.run', side_effect=record):
                # When Avro is requested, then CBOR cannot masquerade as an Avro measurement.
                with self.assertRaisesRegex(ValueError, 'wrong archive format'):
                    measure(root / 'harness', binary, 'archive_file', root / 'result',
                            {'file': 10, 'kafka': 10, 'archive_file': 10}, 'avro')
                self.assertEqual(os.environ['KAFERRY_LOAD_ARCHIVE_RECORDS'], '10')
                self.assertEqual(os.environ['KAFERRY_ARCHIVE_FORMAT'], 'avro')

    def test_pr_comparison_uses_the_merge_base(self):
        # Given a PR whose target has advanced beyond its branch point.
        with tempfile.TemporaryDirectory() as name, patch.dict(os.environ, {
                'GITHUB_OUTPUT': str(Path(name) / 'outputs'), 'EVENT': 'pull_request',
                'PERFORMANCE_MODE': 'comparison', 'PR_BASE': 'target', 'BASE_INPUT': ''}), \
                patch('performance.resolve', side_effect=['head', 'target']), \
                patch('performance.command', return_value='branch-point') as git, redirect_stdout(io.StringIO()):
            # When planned, then unrelated target changes are excluded.
            plan()
            git.assert_called_once_with('git', 'merge-base', 'target', 'head')
            self.assertIn('base=branch-point', (Path(name) / 'outputs').read_text())

    def test_retained_artifacts_cannot_reuse_a_rejected_baseline(self):
        # Given an expired artifact, a wrong-head report, then the latest usable report.
        for compatible in (True, False):
            with self.subTest(compatible=compatible):
                reports = [{'head': 'wrong', 'compatible': True}, {'head': 'latest', 'compatible': compatible},
                           {'head': 'older', 'compatible': True}]
                archives = []
                for report in reports:
                    data = io.BytesIO()
                    with zipfile.ZipFile(data, 'w') as zipped:
                        zipped.writestr('performance.json', json.dumps(report))
                    archives.append(data.getvalue())
                runs = [{'databaseId': index, 'headSha': head} for index, head in enumerate(('expired', 'wrong-head', 'latest', 'older'))]
                outputs = [json.dumps(runs)] + [json.dumps({'artifacts': [{'id': index, 'name': 'performance', 'expired': index == 0}]}) for index in range(4)]
                with patch('performance.command', side_effect=outputs), patch('performance.subprocess.check_output', side_effect=archives):
                    # When the matching report rejected its baseline, then older data cannot stall recalibration.
                    self.assertEqual(previous('owner/repo'), reports[1] if compatible else None)

    def test_execution_records_complete_pairs_or_an_explicit_unavailable_result(self):
        # Given successful warmups or a baseline/head failure in the head's correctness workload.
        for failure in (None, 'base', 'head'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as name, chdir(name):
                effects = ([{'records_per_second': 100}, ValueError('baseline failed')] if failure == 'base'
                           else [OSError('head failed')] if failure == 'head' else None)
                with patch('performance.resolve', side_effect=lambda ref: ref), \
                        patch('performance.command', return_value='fixture-images'), \
                        patch('performance.build', side_effect=lambda ref, root: (Path(ref), {'sha256': ref})), \
                        patch('performance.subprocess.run'), \
                        patch('performance.measure', side_effect=effects, return_value={'records_per_second': 100}), \
                        redirect_stdout(io.StringIO()):
                    # When executed, then no exception or rejected baseline can publish partial samples.
                    if failure == 'head':
                        with self.assertRaises(OSError):
                            execute('base', 'head', 2, {'kafka': 10, 'file': 10})
                    else:
                        execute('base', 'head', 2, {'kafka': 10, 'file': 10})
                report = json.loads(Path('build-results/performance.json').read_text())
                self.assertEqual(report['compatible'], failure is None)
                if failure:
                    self.assertEqual(report['workloads'], {})
                    self.assertIn('unavailable', report)
                else:
                    for workload in report['workloads'].values():
                        self.assertEqual([pair['first'] for pair in workload['samples']], ['base', 'head'])
                        self.assertEqual(workload['median'], 1)
