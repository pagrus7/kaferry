"""Release publication must use authored notes for the exact version."""

from pathlib import Path
import tempfile
import unittest

from release_notes import read_notes


class ReleaseNotesTest(unittest.TestCase):
    def test_authored_body_survives_unchanged(self):
        # Given versioned notes containing links and Unicode.
        body = "# Kaferry v1.2.3\n\nCopy topic→file.\n\n## Changes\n\n- [Fix](https://example.com).\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "releases").mkdir()
            (root / "releases/v1.2.3.md").write_text(body, encoding="utf-8")
            # When preparing the release body, then no text is generated or lost.
            self.assertEqual(read_notes("v1.2.3", root), body)

    def test_missing_notes_and_unsafe_versions_fail(self):
        # Given a checkout with no release notes.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # When selecting a missing version, then publication is blocked.
            with self.assertRaises(FileNotFoundError):
                read_notes("v1.2.3", root)
            # When a version could address another path, then it is rejected.
            with self.assertRaises(ValueError):
                read_notes("../v1.2.3", root)

    def test_wrong_version_or_empty_changes_fail(self):
        # Given notes for another version or without usable changes.
        invalid = (
            "# Kaferry v1.2.4\n\n## Changes\n\n- Fix.\n",
            "# Kaferry v1.2.3\n\nSummary only.\n",
            "# Kaferry v1.2.3\n\n## Changes\n\n## Downloads\n\nArchives.\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "releases").mkdir()
            for body in invalid:
                with self.subTest(body=body):
                    (root / "releases/v1.2.3.md").write_text(body, encoding="utf-8")
                    # When preparing the body, then publication is blocked.
                    with self.assertRaises(ValueError):
                        read_notes("v1.2.3", root)


if __name__ == "__main__":
    unittest.main()
