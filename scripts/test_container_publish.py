"""Publishing must preserve version digests and fail closed on registry access errors."""

import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from container_publish import publish


class PublishTest(unittest.TestCase):
    def test_existing_equal_digest_is_idempotent(self):
        # Given an already published exact image index.
        data = b'{"schemaVersion":2}'
        with patch('container_publish.subprocess.check_output', return_value=data), patch('container_publish.subprocess.run', return_value=subprocess.CompletedProcess([], 0, stdout=data)) as run:
            # When publishing again, then do not copy or replace the tag.
            self.assertEqual(publish(Path('image.tar'), 'registry/image:v1'), 'sha256:' + hashlib.sha256(data).hexdigest())
            self.assertEqual(run.call_count, 1)

    def test_existing_different_digest_and_registry_denial_never_push(self):
        cases = [('registry/image:v1', subprocess.CompletedProcess([], 0, stdout=b'other image'))]
        for reference in ('registry/image:v1', 'registry/image:latest'):
            cases.extend((reference, subprocess.CompletedProcess([], 1, stderr=error))
                         for error in (b'unauthorized', b'network timeout'))
        for reference, response in cases:
            # Given a different version digest or an unverifiable registry state.
            with self.subTest(reference=reference, response=response), patch('container_publish.subprocess.check_output', return_value=b'candidate'), patch('container_publish.subprocess.run', return_value=response) as run:
                # When attempting to publish, then stop before copying any image.
                with self.assertRaises((ValueError, RuntimeError)):
                    publish(Path('image.tar'), reference)
                self.assertEqual(run.call_count, 1)

    def test_missing_version_and_latest_alias_copy_the_tested_digest(self):
        # Given a missing version tag or a latest alias pointing at an older image.
        manifest = b'candidate manifest'
        expected = 'sha256:' + hashlib.sha256(manifest).hexdigest()
        for reference, existing in (
                ('registry/image:v1', subprocess.CompletedProcess([], 1, stderr=b'manifest unknown')),
                ('registry/image:latest', subprocess.CompletedProcess([], 0, stdout=b'older image'))):
            for registry_digest in (expected, 'sha256:' + '0' * 64):
                with self.subTest(reference=reference, digest=registry_digest), tempfile.TemporaryDirectory() as name:
                    archive = Path(name) / 'image.tar'
                    def run(command, **kwargs):
                        if command[1] == 'inspect':
                            return existing
                        Path(str(archive) + '.digest').write_text(registry_digest + '\n')
                        return subprocess.CompletedProcess(command, 0)
                    with patch('container_publish.subprocess.check_output', return_value=manifest), patch('container_publish.subprocess.run', side_effect=run) as calls:
                        # When copying succeeds, then only the tested digest can be accepted.
                        if registry_digest == expected:
                            self.assertEqual(publish(archive, reference), expected)
                        else:
                            with self.assertRaises(ValueError):
                                publish(archive, reference)
                        self.assertEqual(calls.call_count, 2)
                        self.assertIn('--preserve-digests', calls.call_args.args[0])
                        self.assertEqual(calls.call_args.args[0][-1], 'docker://' + reference)
