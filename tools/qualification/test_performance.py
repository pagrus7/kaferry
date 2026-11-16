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

from tools.qualification.performance import CASES, execute, previous


class PerformanceTest(unittest.TestCase):
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
                with patch('tools.qualification.performance.command', side_effect=outputs), patch('tools.qualification.performance.subprocess.check_output', side_effect=archives):
                    # When the matching report rejected its baseline, then older data cannot stall recalibration.
                    self.assertEqual(previous('owner/repo'), reports[1] if compatible else None)

    def test_execution_records_complete_pairs_or_an_explicit_unavailable_result(self):
        # Given successful warmups or a baseline/head failure, including after one complete workload.
        for failure in (None, 'base', 'later-base', 'head'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as name, chdir(name):
                def workload(harness, binary, case, directory, counts):
                    if binary == Path('base') and (failure == 'base' or failure == 'later-base' and case == 'file'):
                        raise ValueError('baseline rejects current SQL')
                    if binary == Path('head') and failure == 'head':
                        raise OSError('head failed')
                    return {'records_per_second': 100}
                with patch.dict(os.environ), patch('tools.qualification.performance.resolve', side_effect=lambda ref: ref), \
                        patch('tools.qualification.performance.command', return_value='fixture-images'), \
                        patch('tools.qualification.performance.build', side_effect=lambda ref, root: (Path(ref), {'sha256': ref})), \
                        patch('tools.qualification.performance.subprocess.run'), \
                        patch('tools.qualification.performance.measure', side_effect=workload), \
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
                    if failure != 'head':
                        self.assertIn('baseline failed the current workload', report['unavailable'])
                else:
                    self.assertEqual(set(report['workloads']), set(CASES))
                    for workload in report['workloads'].values():
                        self.assertEqual([pair['first'] for pair in workload['samples']], ['base', 'head'])
                        self.assertEqual(workload['median'], 1)
