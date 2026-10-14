"""Release packaging preserves notices and requires review of changed native bundles."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from notices import collect, license_text, native_text


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

    def test_homebrew_go_layout_preserves_license_and_patent_grant(self):
        # Given a Go distribution with LICENSE above GOROOT and PATENTS inside it.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            goroot = root / "libexec"
            goroot.mkdir()
            (root / "LICENSE").write_text("Go permission\n")
            (goroot / "PATENTS").write_text("Go patent grant\n")
            # When collecting the runtime notices, then preserve both grants.
            result = license_text(goroot, root / "LICENSE")
            self.assertIn("Go permission\n", result)
            self.assertIn("Go patent grant\n", result)

    def test_changed_native_client_or_archive_requires_review(self):
        # Given an archive that is not among the reviewed bundle hashes.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "licenses").mkdir()
            archive = root / "librdkafka_darwin_arm64.a"
            archive.write_bytes(b"changed archive")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            for client, digest in (("v2.16.0", "outdated hash"), ("v99.0.0", digest)):
                with self.subTest(client=client):
                    pins = {"client": "v2.16.0", "archives": {archive.name: digest}}
                    (root / "licenses/native.json").write_text(json.dumps(pins))
                    # When packaging, then block before trusting stale notices.
                    with self.assertRaisesRegex(ValueError, "native bundle changed"):
                        native_text(client, archive, root)

    def test_changed_archive_for_another_platform_blocks_packaging(self):
        # Given an unchanged host archive and a changed archive for another platform.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "licenses").mkdir()
            archive = root / "host.a"
            archive.write_bytes(b"reviewed")
            (root / "other.a").write_bytes(b"changed")
            pins = {"client": "v2.16.0", "archives": {
                archive.name: hashlib.sha256(archive.read_bytes()).hexdigest(), "other.a": "outdated hash"}}
            (root / "licenses/native.json").write_text(json.dumps(pins))
            # When validating the bundle on this host, then the other platform also fails.
            with self.assertRaisesRegex(ValueError, "native bundle changed"):
                native_text("v2.16.0", archive, root)

    def test_binary_and_notice_toolchains_must_match(self):
        # Given a binary compiled by a different Go version.
        results = ["", json.dumps({"GOROOT": "/unused", "GOVERSION": "go1.26.9"}), "kaferry: go1.26.8\n"]
        with patch("notices.subprocess.check_output", side_effect=results):
            # When collecting notices, then avoid attributing the wrong runtime sources.
            with self.assertRaisesRegex(ValueError, "same Go toolchain"):
                collect(Path("/unused"), Path("kaferry"))

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
            packages = (f"github.com/confluentinc/confluent-kafka-go/v2\tv2.16.0\t{module}\n"
                        f"vendor/golang.org/x/net/dns/dnsmessage\t\t{vendor / 'dns/dnsmessage'}\n\n")
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            (root / "licenses/native.json").write_text(json.dumps({
                "client": "v2.16.0", "archives": {archive.name: digest}}))
            environment = json.dumps({"GOROOT": str(goroot), "GOVERSION": "go1.26.9"})
            with patch("notices.subprocess.check_output", side_effect=[packages, environment, "kaferry: go1.26.9\n", str(archive)]):
                # When generating notices, then include all linked source categories.
                result = collect(root, Path("kaferry"))
            for text in ("Go copyright\n", "client copyright\n", "vendor copyright\n",
                         "bundle copyright\n", "native extras\n"):
                self.assertIn(text, result)


if __name__ == "__main__":
    unittest.main()
