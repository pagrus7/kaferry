"""Container packaging must reject incomplete, altered or unsafe release archives."""

import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from container_artifacts import extract_archive


class ArchiveTest(unittest.TestCase):
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

    def test_missing_licenses_and_link_members_fail(self):
        # Given matching checksums but invalid package structure.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            path = root / 'release.tar.gz'
            for index, files in enumerate((('kaferry',), ('kaferry', 'LICENSE', '../THIRD_PARTY_NOTICES.txt'))):
                with tarfile.open(path, 'w:gz') as archive:
                    for filename in files:
                        info = tarfile.TarInfo(filename)
                        info.size = 1
                        archive.addfile(info, io.BytesIO(b'x'))
                checksum = hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n'
                # When staging, then reject the package before writing files.
                with self.assertRaises(ValueError):
                    extract_archive(path, checksum, root / f'output-{index}')
