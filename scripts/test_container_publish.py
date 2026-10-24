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
        for response in (subprocess.CompletedProcess([], 0, stdout=b'other image'),
                         subprocess.CompletedProcess([], 1, stderr=b'unauthorized'),
                         subprocess.CompletedProcess([], 1, stderr=b'network timeout')):
            # Given a different version digest or an unverifiable registry state.
            with self.subTest(response=response), patch('container_publish.subprocess.check_output', return_value=b'candidate'), patch('container_publish.subprocess.run', return_value=response) as run:
                # When attempting to publish, then stop before copying any image.
                with self.assertRaises((ValueError, RuntimeError)):
                    publish(Path('image.tar'), 'registry/image:v1')
                self.assertEqual(run.call_count, 1)

    def test_absent_tag_is_copied_and_registry_digest_is_checked(self):
        # Given a missing version tag and a fixed OCI manifest.
        manifest = b'candidate manifest'
        expected = 'sha256:' + hashlib.sha256(manifest).hexdigest()
        for registry_digest in (expected, 'sha256:' + '0' * 64):
            with tempfile.TemporaryDirectory() as name:
                archive = Path(name) / 'image.tar'
                def run(command, **kwargs):
                    if command[1] == 'inspect':
                        return subprocess.CompletedProcess(command, 1, stderr=b'manifest unknown')
                    Path(str(archive) + '.digest').write_text(registry_digest + '\n')
                    return subprocess.CompletedProcess(command, 0)
                with patch('container_publish.subprocess.check_output', return_value=manifest), patch('container_publish.subprocess.run', side_effect=run):
                    # When copying succeeds, then only the matching registry digest can be attested.
                    if registry_digest == expected:
                        self.assertEqual(publish(archive, 'registry/image:v1'), expected)
                    else:
                        with self.assertRaises(ValueError):
                            publish(archive, 'registry/image:v1')
