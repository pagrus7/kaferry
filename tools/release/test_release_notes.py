from pathlib import Path
import tempfile
import unittest

from tools.release.release_notes import read_notes


class ReleaseNotesTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.notes = self.root / "docs/releases/unreleased.md"
        self.notes.parent.mkdir(parents=True)

    def test_returns_authored_body_for_each_valid_tag(self):
        body = "Release summary.\n\n## Changes\n\n- A user-visible change.\n"
        self.notes.write_text("# Kaferry unreleased\n\n" + body, encoding="utf-8")
        for tag in ("v1.2.3", "v1.2.4-rc.1"):
            with self.subTest(tag=tag):
                self.assertEqual(read_notes(tag, self.root), body)

    def test_rejects_invalid_tag(self):
        with self.assertRaisesRegex(ValueError, "release tag"):
            read_notes("main", self.root)

    def test_rejects_version_heading(self):
        self.notes.write_text("# Kaferry v1.2.3\n\n## Changes\n\n- A fix.\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "# Kaferry unreleased"):
            read_notes("v1.2.3", self.root)

    def test_rejects_missing_or_empty_changes(self):
        for changes in ("", "## Changes\n\n"):
            with self.subTest(changes=changes):
                self.notes.write_text(
                    "# Kaferry unreleased\n\n" + changes + "## Upgrade\n\n- A migration step.\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, "nonempty '## Changes'"):
                    read_notes("v1.2.3", self.root)
