"""Container packaging must reject incomplete, altered or unsafe release archives."""

import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from tools.release.container_artifacts import extract_archive, extract_tutorial


class ArchiveTest(unittest.TestCase):
    def test_tutorial_extraction_preserves_files_and_creates_checkpoints(self):
        # Given a tutorial with SQL, configuration and one generated archive.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            path = root / 'tutorial.tar.gz'
            contents = {'copy.sql': b'query', 'tutorial.toml': b'config',
                        'input/' + '1' * 32 + '.' + '2' * 32 + '.avro': b'records'}
            with tarfile.open(path, 'w:gz') as archive:
                for filename, data in contents.items():
                    info = tarfile.TarInfo('archive-demo/' + filename)
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
            # When staging it for the image, then preserve its bytes and prepare writable progress storage.
            destination = root / 'demo'
            extract_tutorial(path, destination)
            for filename, expected in contents.items():
                self.assertEqual((destination / filename).read_bytes(), expected)
            self.assertEqual(list((destination / 'checkpoints').iterdir()), [])

    def test_exact_binary_and_licenses_are_preserved(self):
        # Given a package with distinct executable and license contents.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            path = root / 'release.tar.gz'
            contents = {'kaferry': b'binary', 'LICENSE': b'grant', 'THIRD_PARTY_NOTICES.txt': b'notices'}
            with tarfile.open(path, 'w:gz') as archive:
                for filename, data in contents.items():
                    info = tarfile.TarInfo(filename)
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
            checksum = hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n'
            # When staging a verified package, then copy its bytes without rebuilding.
            extract_archive(path, checksum, root / 'output')
            for filename, expected in contents.items():
                self.assertEqual((root / 'output' / filename).read_bytes(), expected)
            # When checksums are wrong or duplicated, then no output is created.
            for index, rows in enumerate(('', checksum + checksum)):
                destination = root / f'invalid-{index}'
                with self.assertRaises(ValueError):
                    extract_archive(path, rows, destination)
                self.assertFalse(destination.exists())
