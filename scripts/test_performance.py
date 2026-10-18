"""Only complete, compatible workload results can become comparison data."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from performance import CASES, measure, pair_summary, plan


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
        for mode, baseline, run in [('calibration', None, True), ('comparison', {'head': 'b' * 40}, False), ('comparison', None, True)]:
            with self.subTest(mode=mode, baseline=baseline), tempfile.TemporaryDirectory() as name:
                output = Path(name) / 'outputs'
                with patch.dict(os.environ, {'GITHUB_OUTPUT': str(output), 'EVENT': 'schedule', 'BASE_INPUT': '',
                                              'PERFORMANCE_MODE': mode, 'GITHUB_REPOSITORY': 'owner/repo'}), \
                        patch('performance.resolve', return_value='b' * 40), patch('performance.previous', return_value=baseline), redirect_stdout(io.StringIO()):
                    # When planned, then an absent baseline calibrates rather than claiming a comparison.
                    plan()
                    self.assertIn(f'run={str(run).lower()}', output.read_text())

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
