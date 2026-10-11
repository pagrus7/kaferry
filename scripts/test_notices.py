"""Release packaging preserves notices and requires review of changed native bundles."""

import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from notices import collect, license_text, native_text, NATIVE_CLIENT


class NoticesTest(unittest.TestCase):
    def test_license_and_attributions_are_preserved(self):
        # Given an upstream license with a separate attribution and patent grant.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            texts = {"LICENSE.txt": "permission and copyright\n", "NOTICE": "attribution\n",
                     "PATENTS": "patent grant\n"}
            for name, text in texts.items():
                (root / name).write_text(text)
            # When collecting notices, then preserve every text verbatim.
            result = license_text(root)
            for text in texts.values():
                self.assertIn(text, result)

    def test_missing_or_empty_license_blocks_packaging(self):
        # Given only a notice, then an empty license alongside it.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "NOTICE").write_text("attribution")
            for empty_license in (False, True):
                with self.subTest(empty_license=empty_license):
                    if empty_license:
                        (root / "LICENSE").touch()
                    # When collecting notices, then refuse incomplete licensing.
                    with self.assertRaisesRegex(ValueError, "missing license"):
                        license_text(root)

    def test_changed_native_client_or_archive_requires_review(self):
        # Given an archive that is not among the reviewed bundle hashes.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "librdkafka_darwin_arm64.a"
            archive.write_bytes(b"changed archive")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            for client, pins in ((NATIVE_CLIENT, {}), ("v99.0.0", {archive.name: digest})):
                with self.subTest(client=client):
                    # When packaging, then block before trusting stale notices.
                    with patch("notices.NATIVE_ARCHIVES", pins), \
                            self.assertRaisesRegex(ValueError, "native bundle changed"):
                        native_text(client, archive, root)

    def test_collect_linked_modules_runtime_vendor_and_native_notices(self):
        # Given package metadata for a binary and reviewed native archive.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "licenses").mkdir()
            (root / "licenses/native.txt").write_text("native extras\n")
            goroot, module = root / "go", root / "client"
            vendor = goroot / "src/vendor/golang.org/x/net"
            bundle = module / "kafka/librdkafka_vendor"
            for path, text in ((goroot, "Go copyright\n"), (module, "client copyright\n"),
                               (vendor, "vendor copyright\n"), (bundle, "bundle copyright\n")):
                path.mkdir(parents=True, exist_ok=True)
                (path / "LICENSE").write_text(text)
            archive = bundle / "test.a"
            archive.write_bytes(b"reviewed archive")
            packages = (f"github.com/confluentinc/confluent-kafka-go/v2\t{NATIVE_CLIENT}\t{module}\n"
                        f"vendor/golang.org/x/net/dns/dnsmessage\t\t{vendor / 'dns/dnsmessage'}\n\n")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            with patch("notices.subprocess.check_output", side_effect=[packages, str(goroot), str(archive)]), \
                    patch("notices.NATIVE_ARCHIVES", {archive.name: digest}):
                # When generating notices, then include all linked source categories.
                result = collect(root)
            for text in ("Go copyright\n", "client copyright\n", "vendor copyright\n",
                         "bundle copyright\n", "native extras\n"):
                self.assertIn(text, result)


if __name__ == "__main__":
    unittest.main()
