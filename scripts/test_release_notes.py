"""Release publication must use authored notes for the exact version."""

from pathlib import Path
import tempfile
import unittest

from release_notes import read_notes


class ReleaseNotesTest(unittest.TestCase):
    def test_release_body_preserves_content_without_duplicate_title(self):
        # Given versioned notes containing links and Unicode.
        content = "Copy topic→file.\n\n## Changes\n\n- [Fix](https://example.com).\n"
        body = "# Kaferry v1.2.3\n\n" + content
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "releases").mkdir()
            (root / "releases/v1.2.3.md").write_text(body, encoding="utf-8")
            # When preparing the body, then only the duplicate title is omitted.
            self.assertEqual(read_notes("v1.2.3", root), content)

    def test_tracked_release_notes_are_valid(self):
        # Given the versioned release notes shipped in this repository.
        root = Path(__file__).resolve().parent.parent
        notes = list((root / "releases").glob("v*.md"))
        self.assertTrue(notes, "no tracked versioned release notes")
        for path in notes:
            with self.subTest(version=path.stem):
                # When validating for publication, then each body is usable.
                self.assertTrue(read_notes(path.stem, root).strip())

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
