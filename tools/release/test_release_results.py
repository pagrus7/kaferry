"""Release reports must describe passing measurements of the release binary."""

import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from tools.qualification.qualify import plan
from tools.ci.suites import LOAD_PROFILES
from tools.release.release_results import LOAD_CASES, SOAK, write_results


class ReleaseResultsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.commit = 'a' * 40
        self.digest = hashlib.sha256(b'binary').hexdigest()
        self.archive = self.root / 'release.tar.gz'
        with tarfile.open(self.archive, 'w:gz') as archive:
            member = tarfile.TarInfo('kaferry')
            member.size = 6
            archive.addfile(member, io.BytesIO(b'binary'))
        cases = [dict(case, source_version=case['source'], destination_version=case['destination'],
                      status='passed', source_commit=self.commit,
                      working_tree_dirty=False, binary_sha256=self.digest) for case in plan('full')]
        self.matrix = self.root / 'compatibility.json'
        self.matrix.write_text(json.dumps({'schema': 1, 'tier': 'full', 'status': 'passed', 'cases': cases}))
        for suite, tests in [('load', list(LOAD_CASES)), ('soak', [SOAK])]:
            directory = self.root / f'qualification-{suite}'
            directory.mkdir()
            env = {'commit': self.commit, 'binary_sha256': self.digest, 'working_tree_dirty': False,
                   'system': 'Linux', 'machine': 'x86_64', 'platform': 'Linux fixture', 'go': 'go fixture',
                   'started_at': 'fixture-time', 'run_url': 'https://github.com/example/project/actions/runs/1',
                   'soak_duration': '30m'}
            (directory / f'{suite}.environment.json').write_text(json.dumps(env))
            (directory / f'{suite}.result.json').write_text(json.dumps({'exit_code': 0}))
            for index, test in enumerate(tests):
                profile = test.removeprefix('TestLoadBoundedKafkaCopy/')
                codec = profile.removeprefix('source-').removesuffix('-v1') if profile.startswith('source-') else 'none'
                measurement = {'test': test, 'binary_sha256': self.digest, 'passed': True, 'skipped': False,
                               'records': 16000, 'records_per_second': 12345.6 + index, 'elapsed_seconds': 1.3,
                               'sampled_peak_rss_bytes': 32 << 20, 'peak_uncommitted_records': 100,
                               'max_sampled_commit_lag': 7, 'shutdown_seconds': 0.01,
                               'workload': {'name': profile if profile in LOAD_PROFILES else 'cyclic-v1',
                                            'source_compression': codec, 'value_bytes_min_max': [1024, 5119],
                                            'key_count': 257, 'seed': 42},
                               'source_compression_records': {codec: 16000}}
                (directory / f'{index}.metrics.json').write_text(json.dumps(measurement))

    def generate(self):
        write_results(self.root, self.archive, self.matrix, self.commit, 'v1.0.0', self.root / 'report')

    def test_retains_measurements_and_renders_matrix_and_throughput(self):
        # Given passing matrix, suite and measurement evidence for one packaged binary.
        output = self.root / 'report'
        # When the report is generated, then the visible numbers and retained data agree.
        write_results(self.root, self.archive, self.matrix, self.commit, 'v1.0.0', output)
        report = json.loads((output / 'release-results.json').read_text())
        self.assertEqual(report['binary_sha256'], self.digest)
        self.assertEqual(len(report['compatibility']), 31)
        self.assertEqual(len(report['measurements']), len(LOAD_CASES) + 1)
        soak = next(item for item in report['measurements'] if item['test'] == SOAK)
        self.assertEqual(soak['max_sampled_commit_lag'], 7)
        self.assertEqual(soak['workload']['seed'], 42)
        compressed = next(item for item in report['measurements'] if item['test'].endswith('/source-zstd-v1'))
        self.assertEqual(compressed['source_compression_records'], {'zstd': 16000})
        self.assertEqual(compressed['workload']['value_bytes_min_max'], [1024, 5119])
        markdown = (output / 'release-results.md').read_text()
        self.assertIn('shared CI runner', markdown)
        self.assertIn('paced input', markdown)
        self.assertIn('16,000', markdown)
        for line in (output / 'release-results.SHA256SUMS').read_text().splitlines():
            expected, name = line.split('  ', 1)
            self.assertEqual(hashlib.sha256((output / name).read_bytes()).hexdigest(), expected)

    def test_rejects_incomplete_failed_and_mismatched_measurements(self):
        # Given valid evidence, then corrupting any required identity/result prevents publication.
        targets = [
            ('qualification-load/load.environment.json', 'commit', 'b' * 40),
            ('qualification-load/load.environment.json', 'binary_sha256', 'wrong'),
            ('qualification-load/load.environment.json', 'working_tree_dirty', True),
            ('qualification-load/load.environment.json', 'machine', 'arm64'),
            ('qualification-soak/soak.result.json', 'exit_code', 1),
            ('qualification-load/0.metrics.json', 'binary_sha256', 'wrong'),
            ('qualification-load/0.metrics.json', 'passed', False),
            ('qualification-load/0.metrics.json', 'skipped', True),
            ('qualification-load/0.metrics.json', 'records_per_second', float('nan')),
            ('qualification-load/0.metrics.json', 'records', 0),
            ('qualification-load/0.metrics.json', 'workload', {}),
            ('compatibility.json', 'status', 'incomplete'),
        ]
        for name, key, value in targets:
            with self.subTest(name=name, key=key):
                path = self.root / name
                original = path.read_text()
                changed = json.loads(original)
                changed[key] = value
                path.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    self.generate()
                self.assertFalse((self.root / 'report').exists())
                path.write_text(original)
        path = self.root / 'qualification-load/0.metrics.json'
        duplicate = path.with_name('duplicate.metrics.json')
        duplicate.write_bytes(path.read_bytes())
        with self.assertRaisesRegex(ValueError, 'duplicate measurement'):
            self.generate()
        duplicate.unlink()
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'required load or soak'):
            self.generate()
        self.assertFalse((self.root / 'report').exists())

    def test_rejects_mislabeled_profile_and_unobserved_compression(self):
        # Given a passing source-codec measurement, then declared settings cannot replace observed batches.
        path = next(path for path in (self.root / 'qualification-load').glob('*.metrics.json')
                    if json.loads(path.read_text())['test'].endswith('/source-zstd-v1'))
        original = path.read_text()
        for field, value in [('name', 'source-gzip-v1'), ('source_compression', 'gzip')]:
            with self.subTest(field=field):
                changed = json.loads(original)
                changed['workload'][field] = value
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, 'observed source compression'):
                    self.generate()
        changed = json.loads(original)
        changed['source_compression_records'] = {'none': 16000}
        path.write_text(json.dumps(changed))
        with self.assertRaisesRegex(ValueError, 'observed source compression'):
            self.generate()

    def test_retains_unrecorded_workload_status_for_original_receipts(self):
        # Given original release receipts without profile fields or the new cases.
        for suite in ('load', 'soak'):
            for path in (self.root / ('qualification-' + suite)).glob('*.metrics.json'):
                item = json.loads(path.read_text())
                profile = item['test'].removeprefix('TestLoadBoundedKafkaCopy/')
                if profile in LOAD_PROFILES:
                    path.unlink()
                    continue
                item.pop('workload')
                item.pop('source_compression_records')
                path.write_text(json.dumps(item))
        # When supplementing that release, then preserve the absence of profile evidence.
        self.generate()
        report = json.loads((self.root / 'report/release-results.json').read_text())
        self.assertEqual(len(report['measurements']), 6)
        self.assertTrue(all(item['workload'] == {'name': 'not recorded'} for item in report['measurements']))

    def test_rejects_mixed_recorded_and_unrecorded_workloads(self):
        # Given new-format evidence, when one identity disappears, then it cannot downgrade to an original receipt.
        path = self.root / 'qualification-load/0.metrics.json'
        item = json.loads(path.read_text())
        item.pop('workload')
        path.write_text(json.dumps(item))
        with self.assertRaisesRegex(ValueError, 'workload identity is missing'):
            self.generate()


if __name__ == '__main__':
    unittest.main()
