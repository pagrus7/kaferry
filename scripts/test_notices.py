"""Release packaging preserves notices for linked modules and vendored code."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from notices import collect, license_text, module_text


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

    def test_linked_subpackage_license_is_preserved(self):
        # Given a compression module with an additional copyright in a linked subpackage.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "zstd/internal/hash"
            package.mkdir(parents=True)
            (root / "LICENSE").write_text("module permission\n")
            (package.parent / "LICENSE").write_text("hash attribution\n")
            # When collecting that module, then both notices and their locations are retained.
            result = module_text(root, {package})
            self.assertIn("module permission\n", result)
            self.assertIn("zstd/internal/LICENSE", result)
            self.assertIn("hash attribution\n", result)

    def test_binary_and_notice_toolchains_must_match(self):
        # Given a binary compiled by a different Go version.
        results = ["", json.dumps({"GOROOT": "/unused", "GOVERSION": "go1.26.9"}), "kaferry: go1.26.8\n"]
        with patch("notices.subprocess.check_output", side_effect=results):
            # When collecting notices, then avoid attributing the wrong runtime sources.
            with self.assertRaisesRegex(ValueError, "same Go toolchain"):
                collect(Path("/unused"), Path("kaferry"))

    def test_collect_linked_modules_runtime_and_vendor_notices(self):
        # Given package metadata for a pure Go binary, including vendored standard library code.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            goroot, module = root / "go", root / "client"
            vendor = goroot / "src/vendor/golang.org/x/net"
            for path, text in ((goroot, "Go copyright\n"), (module, "client copyright\n"),
                               (vendor, "vendor copyright\n")):
                path.mkdir(parents=True, exist_ok=True)
                (path / "LICENSE").write_text(text)
            packages = (f"github.com/twmb/franz-go\tv1.22.2\t{module}\t{module}\n"
                        f"vendor/golang.org/x/net/dns/dnsmessage\t\t{vendor / 'dns/dnsmessage'}\t{vendor / 'dns/dnsmessage'}\n\n")
            environment = json.dumps({"GOROOT": str(goroot), "GOVERSION": "go1.26.9"})
            with patch("notices.subprocess.check_output", side_effect=[packages, environment, "kaferry: go1.26.9\n"]):
                # When generating notices, then all linked source categories are included without a native bundle.
                result = collect(root, Path("kaferry"))
            for text in ("Go copyright\n", "client copyright\n", "vendor copyright\n"):
                self.assertIn(text, result)


if __name__ == "__main__":
    unittest.main()
