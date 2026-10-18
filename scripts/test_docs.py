"""Examples are parsed, never executed; anchors must point at real headings."""

from pathlib import Path
import tempfile
import unittest

from docs import check, prose_and_shell


class DocsTest(unittest.TestCase):
    def test_shell_examples_are_checked_without_execution(self):
        # Given a syntactically valid command with a destructive-looking operation.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            doc = root / 'README.md'
            marker = root / 'must-not-exist'
            doc.write_text(f'```sh\ntouch "{marker}"\n```\n')
            # When checked, then shell syntax passes without executing the command.
            check(doc)
            self.assertFalse(marker.exists())
            doc.write_text('```sh\nif true; then\n```\n')
            with self.assertRaisesRegex(ValueError, 'shell block 1'):
                check(doc)

    def test_links_and_duplicate_anchors_ignore_code_blocks(self):
        # Given a document containing headings, links and a fenced literal example.
        with tempfile.TemporaryDirectory() as directory:
            doc = Path(directory) / 'README.md'
            doc.write_text('# Title\n## Hello, `world`!\n## Hello, `world`!\n'
                           '[second](#hello-world-1)\n~~~markdown\n[example](missing)\n~~~\n')
            # When checked, then duplicate GitHub anchors work and literal links are ignored.
            check(doc)
            for target in ('missing.md', '#absent'):
                with self.subTest(target=target):
                    doc.write_text(f'[bad]({target})\n')
                    with self.assertRaisesRegex(ValueError, 'does not exist'):
                        check(doc)
        with self.assertRaisesRegex(ValueError, 'unclosed code fence'):
            prose_and_shell('```sh\necho incomplete\n')
