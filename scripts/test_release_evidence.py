"""Release summaries accept only passing results for the packaged source and binary."""

import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from ci import release_summary


class ReleaseEvidenceTest(unittest.TestCase):
    def test_summary_preserves_scope_and_rejects_incomplete_or_wrong_evidence(self):
        # Given results for the exact release source and packaged binary.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            archive = root / 'release.tar.gz'
            with tarfile.open(archive, 'w:gz') as package:
                item = tarfile.TarInfo('kaferry')
                item.size = 6
                package.addfile(item, io.BytesIO(b'binary'))
            commit = 'a' * 40
            environment = {'commit': commit, 'binary_sha256': hashlib.sha256(b'binary').hexdigest(),
                           'platform': 'Linux fixture', 'go': 'go fixture', 'started_at': 'fixture-time',
                           'run_url': 'https://example.invalid/run', 'broker_images': json.dumps([{'ID': 'plain-kafka:test'}]),
                           'soak_duration': '30m'}
            result = {'exit_code': 0, 'elapsed_seconds': 12.5}
            for suite in ('integration', 'load', 'fault', 'qualification', 'soak'):
                directory = root / f'qualification-{suite}'
                directory.mkdir()
                (directory / f'{suite}.environment.json').write_text(json.dumps(environment))
                (directory / f'{suite}.result.json').write_text(json.dumps(result))
            (root / 'qualification-qualification' / 'qualification.broker-images.json').write_text(json.dumps([{'ID': 'security-kafka:test'}]))
            output = root / 'qualification.md'
            # When summarized, then results retain source, binary, environment and scenario limits.
            release_summary(root, archive, commit, 'v1.0.0', output)
            text = output.read_text()
            self.assertIn(commit, text)
            self.assertIn(environment['binary_sha256'], text)
            self.assertIn('Linux fixture', text)
            self.assertIn('TLS/SASL', text)
            self.assertIn('security-kafka:test', text)
            self.assertIn('30m', text)
            self.assertEqual(text.count('| PASS |'), 5)
            self.assertIn('do not establish behavior for every deployment', text)
            # When evidence is missing, failed or belongs to another source/binary, reject it.
            output.unlink()
            directory = root / 'qualification-load'
            env_path = directory / 'load.environment.json'
            result_path = directory / 'load.result.json'
            for key, value in [('commit', 'b' * 40), ('binary_sha256', 'wrong'), ('broker_images', ''), ('broker_images', '[]')]:
                with self.subTest(key=key):
                    env_path.write_text(json.dumps(dict(environment, **{key: value})))
                    with self.assertRaisesRegex(ValueError, 'does not match|unavailable'):
                        release_summary(root, archive, commit, 'v1.0.0', output)
                    self.assertFalse(output.exists())
            env_path.write_text(json.dumps(environment))
            result_path.write_text(json.dumps(dict(result, exit_code=1)))
            with self.assertRaisesRegex(ValueError, 'did not pass'):
                release_summary(root, archive, commit, 'v1.0.0', output)
            self.assertFalse(output.exists())
            result_path.write_text(json.dumps(result))
            security_images = root / 'qualification-qualification' / 'qualification.broker-images.json'
            security_images.write_text('')
            with self.assertRaisesRegex(ValueError, 'broker image evidence is unavailable'):
                release_summary(root, archive, commit, 'v1.0.0', output)
            security_images.unlink()
            with self.assertRaises(FileNotFoundError):
                release_summary(root, archive, commit, 'v1.0.0', output)
            security_images.write_text(json.dumps([{'ID': 'security-kafka:test'}]))
            result_path.unlink()
            with self.assertRaises(FileNotFoundError):
                release_summary(root, archive, commit, 'v1.0.0', output)
            self.assertFalse(output.exists())
