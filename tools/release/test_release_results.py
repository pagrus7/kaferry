"""Release reports must describe passing measurements of the release binary."""

from contextlib import redirect_stdout
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
                workload = {'name': 'cyclic-v1', 'source_compression': 'none',
                            'value_bytes_min_max': [1024, 5119], 'value_entropy': 'repeated-byte',
                            'key_count': 257, 'null_key_every': 7, 'zipf_s_v': [0, 0],
                            'seed': 42, 'header_count': 3}
                records = 16000
                if profile in LOAD_PROFILES:
                    workload.update(name=profile, source_compression=codec, source_buffer_records=64)
                    records = 3200
                if profile == 'near-limit-v1':
                    records = 256
                    workload.update(value_bytes_min_max=[999000, 999127],
                                    value_entropy='PCG seeded by record ID and 42', topic_max_message_bytes=1000000)
                elif profile == 'hot-keys-v1':
                    workload.update(null_key_every=0, zipf_s_v=[1.25, 1])
                elif profile == 'headers-v1':
                    workload.update(header_count=64, heavy_header_value_bytes=256)
                elif profile == 'tombstones-v1':
                    workload.update(key_count=32, null_key_every=0, header_count=4,
                                    tombstone_every=5, min_compaction_lag_ms=600000)
                elif test in ('TestLoadArchiveCopy/replay', 'TestLoadArchiveCopy/file'):
                    workload['source_compression'] = 'not applicable'
                measurement = {'test': test, 'binary_sha256': self.digest, 'passed': True, 'skipped': False,
                               'records': records, 'records_per_second': 12345.6 + index, 'elapsed_seconds': 1.3,
                               'sampled_peak_rss_bytes': 32 << 20, 'peak_uncommitted_records': 100,
                               'max_sampled_commit_lag': 7, 'shutdown_seconds': 0.01,
                               'workload': workload}
                if profile in LOAD_PROFILES:
                    measurement['source_compression_records'] = {codec: records}
                (directory / f'{index}.metrics.json').write_text(json.dumps(measurement))

    def generate(self):
        with redirect_stdout(io.StringIO()):
            write_results(self.root, self.archive, self.matrix, self.commit, 'v1.0.0', self.root / 'report')

    def load_measurement(self, profile):
        return next(path for path in (self.root / 'qualification-load').glob('*.metrics.json')
                    if json.loads(path.read_text())['test'].endswith('/' + profile))

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
        self.assertEqual(compressed['source_compression_records'], {'zstd': 3200})
        self.assertEqual(compressed['workload']['value_bytes_min_max'], [1024, 5119])
        near_limit = next(item for item in report['measurements'] if item['test'].endswith('/near-limit-v1'))
        self.assertEqual(near_limit['records'], 256)
        self.assertEqual(near_limit['workload']['value_bytes_min_max'], [999000, 999127])
        self.assertEqual(near_limit['workload']['topic_max_message_bytes'], 1000000)
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
            ('qualification-load/0.metrics.json', 'workload', None),
            ('qualification-load/0.metrics.json', 'workload', []),
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
        path = self.load_measurement('source-zstd-v1')
        original = path.read_text()
        for field, value in [('name', 'source-gzip-v1'), ('source_compression', 'gzip')]:
            with self.subTest(field=field):
                changed = json.loads(original)
                changed['workload'][field] = value
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, 'workload|observed source compression'):
                    self.generate()
        changed = json.loads(original)
        changed['source_compression_records'] = {'none': 3200}
        path.write_text(json.dumps(changed))
        with self.assertRaisesRegex(ValueError, 'observed source compression'):
            self.generate()

    def test_rejects_every_omitted_recorded_parameter(self):
        # Given complete common and profile-specific parameters for every measurement.
        for path in self.root.glob('qualification-*/*.metrics.json'):
            original = path.read_text()
            workload = json.loads(original)['workload']
            for field in workload:
                with self.subTest(test=json.loads(original)['test'], field=field):
                    changed = json.loads(original)
                    del changed['workload'][field]
                    path.write_text(json.dumps(changed))
                    # When any required field is omitted, then no report is published.
                    with self.assertRaisesRegex(ValueError, 'workload'):
                        self.generate()
                    self.assertFalse((self.root / 'report').exists())
                    path.write_text(original)

    def test_rejects_invalid_parameter_types_ranges_and_unknown_metadata(self):
        # Given a recorded versioned recipe, parameters must describe that recipe exactly.
        baseline = [
            ('name', 'unknown-v1'), ('source_compression', 'unknown'),
            ('value_bytes_min_max', [1024]), ('value_bytes_min_max', [5119, 1024]),
            ('value_bytes_min_max', [1024.0, 5119]), ('value_bytes_min_max', [True, 5119]),
            ('value_bytes_min_max', [1024, 10 ** 1000]), ('value_entropy', 'unknown'),
            ('key_count', 0), ('key_count', 257.0), ('key_count', True),
            ('null_key_every', -1), ('null_key_every', 7.0), ('zipf_s_v', [0, float('nan')]),
            ('zipf_s_v', [False, 0]), ('zipf_s_v', [0]),
            ('seed', -1), ('seed', 42.0), ('seed', 1 << 64), ('seed', True),
            ('header_count', -1), ('header_count', 3.0),
            ('private_fixture_field', 'private fixture value'),
        ]
        cases = [('default', field, value) for field, value in baseline] + [
            ('near-limit-v1', 'value_bytes_min_max', [1024, 5119]),
            ('near-limit-v1', 'topic_max_message_bytes', 999000),
            ('near-limit-v1', 'topic_max_message_bytes', 1000000.0),
            ('hot-keys-v1', 'zipf_s_v', [1, 1]),
            ('hot-keys-v1', 'zipf_s_v', [1.25, float('inf')]),
            ('headers-v1', 'heavy_header_value_bytes', 0),
            ('headers-v1', 'heavy_header_value_bytes', True),
            ('tombstones-v1', 'tombstone_every', -5),
            ('tombstones-v1', 'min_compaction_lag_ms', 0),
            ('source-gzip-v1', 'source_buffer_records', 0),
            ('source-gzip-v1', 'source_buffer_records', 64.0),
            ('source-gzip-v1', 'value_entropy', 'PCG seeded by record ID and 42'),
        ]
        for profile, field, value in cases:
            with self.subTest(profile=profile, field=field, value=value):
                path = self.load_measurement(profile)
                original = path.read_text()
                changed = json.loads(original)
                changed['workload'][field] = value
                path.write_text(json.dumps(changed))
                # When malformed or unknown metadata appears, then reject it without echoing its contents.
                with self.assertRaisesRegex(ValueError, 'workload') as raised:
                    self.generate()
                self.assertNotIn('private fixture', str(raised.exception))
                self.assertFalse((self.root / 'report').exists())
                path.write_text(original)

    def test_rejects_unknown_or_noninteger_observed_codec_counts(self):
        # Given observed compression metadata, only positive integer record counts for known codecs are accepted.
        path = self.load_measurement('source-zstd-v1')
        original = path.read_text()
        for counts in ({'unknown': 3200}, {'zstd': 3200.0}, {'zstd': True}, {'zstd': -1}, [], None):
            with self.subTest(counts=counts):
                changed = json.loads(original)
                changed['source_compression_records'] = counts
                path.write_text(json.dumps(changed))
                # When codec observations are invalid, then they cannot become release evidence.
                with self.assertRaisesRegex(ValueError, 'observed source compression'):
                    self.generate()
        path.write_text(original)

    def test_normalizes_unused_zero_parameters_and_retains_allowed_entropy(self):
        # Given a cyclic record recipe with explicit unused parameters and the supported entropy override.
        path = self.root / 'qualification-soak/0.metrics.json'
        item = json.loads(path.read_text())
        item['workload'].update(source_buffer_records=0, tombstone_every=0,
                                value_entropy='PCG seeded by record ID and 42')
        path.write_text(json.dumps(item))
        # When generating results, then retain the real input identity and omit unused zero-valued options.
        self.generate()
        report = json.loads((self.root / 'report/release-results.json').read_text())
        workload = next(item['workload'] for item in report['measurements'] if item['test'] == SOAK)
        self.assertEqual(workload['value_entropy'], 'PCG seeded by record ID and 42')
        self.assertNotIn('source_buffer_records', workload)
        self.assertNotIn('tombstone_every', workload)

    def test_retains_fixed_file_and_endurance_recipe_parameters(self):
        # Given the additional recipes emitted by observability and endurance helpers.
        path = self.root / 'qualification-soak/0.metrics.json'
        soak = json.loads(path.read_text())
        soak['workload'].update(name='application-24x32-v1', value_entropy='PCG seeded by record ID and 42')
        path.write_text(json.dumps(soak))
        fixed = json.loads((self.root / 'qualification-load/0.metrics.json').read_text())
        fixed['test'] = 'TestLoadObservability/round-0/disabled'
        fixed['workload'] = {'name': 'fixed-file-v1', 'source_compression': 'not applicable',
                             'value_bytes_min_max': [1024, 1024], 'value_entropy': 'zero bytes',
                             'key_count': 1, 'null_key_every': 0, 'zipf_s_v': [0, 0],
                             'seed': 0, 'header_count': 0}
        (self.root / 'qualification-load/observability.metrics.json').write_text(json.dumps(fixed))
        # When generating the report, then preserve both complete identities without changing their parameters.
        self.generate()
        report = json.loads((self.root / 'report/release-results.json').read_text())
        recipes = {item['workload']['name']: item['workload'] for item in report['measurements']}
        self.assertEqual(recipes['fixed-file-v1'], fixed['workload'])
        self.assertEqual(recipes['application-24x32-v1'], soak['workload'])

    def test_requires_each_profile_measurement_for_recorded_workloads(self):
        # Given modern receipts, when a profile measurement is absent, then other recipes cannot replace it.
        path = self.load_measurement('near-limit-v1')
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'required load or soak'):
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
                item.pop('source_compression_records', None)
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
