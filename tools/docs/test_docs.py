"""Examples are parsed, never executed; anchors must point at real headings."""

from pathlib import Path
import tempfile
import unittest

from tools.docs.docs import check, prose_and_shell


class DocsTest(unittest.TestCase):
    def test_shell_examples_are_checked_without_execution(self):
        # Given a syntactically valid command with a destructive-looking operation.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            doc = root / 'README.md'
            marker = root / 'must-not-exist'
            for language in ('sh', 'bash', 'shell'):
                with self.subTest(language=language):
                    doc.write_text(f'```{language}\ntouch "{marker}"\n```\n')
                    # When checked, then syntax passes without executing the command.
                    check(doc)
                    self.assertFalse(marker.exists())
                    doc.write_text(f'```{language}\nif true; then\n```\n')
                    with self.assertRaisesRegex(ValueError, 'shell block 1'):
                        check(doc)

    def test_reference_links_check_files_and_headings(self):
        # Given reference links with local and remote destinations.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            doc = root / 'README.md'
            (root / 'guide with spaces.md').write_text('# Usage\n')
            doc.write_text('[usage][guide]\n\n[guide]: <guide with spaces.md#usage> "Guide"\n'
                           '[remote]: https://example.invalid/guide\n'
                           '```markdown\n[example]: missing.md\n```\n')
            # When checked, then valid local targets pass and examples/remote URLs are ignored.
            check(doc)
            for target in ('missing.md', 'guide%20with%20spaces.md#missing'):
                with self.subTest(target=target):
                    doc.write_text(f'[usage][guide]\n\n[guide]: {target} "Guide"\n')
                    # Then a missing file or heading fails for a reference destination.
                    with self.assertRaisesRegex(ValueError, 'does not exist'):
                        check(doc)

    def test_html_images_check_local_sources(self):
        # Given HTML images, including an escaped path and a remote URL.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            doc = root / 'README.md'
            (root / 'logo&dark.svg').touch()
            doc.write_text('<picture><img src="logo&amp;dark.svg" alt="Logo"></picture>\n'
                           '<img src="https://example.invalid/logo.svg">\n'
                           '<!-- <img src="missing.svg"> -->\n'
                           '```html\n<img src="missing.svg">\n```\n')
            # When checked, then real local images pass and literal/comment examples are ignored.
            check(doc)
            doc.write_text("<img alt='Logo'\n src='missing.svg' />\n")
            # Then missing HTML image sources fail, including multiline/self-closing tags.
            with self.assertRaisesRegex(ValueError, 'local link does not exist: missing.svg'):
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
