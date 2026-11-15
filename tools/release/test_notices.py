"""Release packaging preserves notices for linked modules and vendored code."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.release.notices import collect, license_text, module_text


class NoticesTest(unittest.TestCase):
    def test_british_licence_spelling_preserves_module_and_subpackage_texts(self):
        # Given a dependency using LICENCE at its root and in a linked subpackage.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            package = root / 'sub'
            package.mkdir()
            (root / 'LICENCE').write_text('module permission\n')
            (package / 'LICENCE.txt').write_text('subpackage permission\n')
            fallback = root / 'fallback'
            fallback.write_text('unrelated fallback')
            # When collecting, then keep both real grants without substituting the fallback.
            self.assertNotIn('unrelated fallback', license_text(root, fallback))
            result = module_text(root, {package})
            self.assertIn('module permission\n', result)
            self.assertIn('sub/LICENCE.txt', result)
            self.assertIn('subpackage permission\n', result)

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
        with patch("tools.release.notices.subprocess.check_output", side_effect=results):
            # When collecting notices, then avoid attributing the wrong runtime sources.
            with self.assertRaisesRegex(ValueError, "same Go toolchain"):
                collect(Path("/unused"), Path("kaferry"))


if __name__ == "__main__":
    unittest.main()
