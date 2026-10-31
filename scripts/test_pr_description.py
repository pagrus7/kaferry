"""PR description checks reject verbose or ambiguous review reports."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from pr_description import FORMATTING_ONLY, MAX_CHARACTERS, SECTIONS, validate


VALID = """## Changes

- Add local reviews.

## Validation

None

## Docs review

Passed

## Tests review

Passed

## Simplicity review

Passed
"""


class PRDescriptionTest(unittest.TestCase):
    def test_only_dependabot_keeps_its_generated_description(self):
        # Given generated release notes that exceed the human template's limit.
        body = "Dependency release notes\n" + "x" * MAX_CHARACTERS
        for author, exempt in (
            ({"login": "dependabot[bot]", "type": "Bot"}, True),
            ({"login": "dependabot[bot]", "type": "User"}, False),
            ({"login": "github-actions[bot]", "type": "Bot"}, False),
            ({"login": "contributor", "type": "User"}, False),
            (None, False),
        ):
            with self.subTest(author=author):
                # When checking the author, then only Dependabot skips formatting.
                self.assertEqual(not validate(body, author), exempt)

    def test_editing_dependabot_description_uses_pr_author_not_event_sender(self):
        # Given a human edit to Dependabot's generated PR description.
        script = Path(__file__).with_name("pr_description.py")
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / "event.json"
            event.write_text(json.dumps({
                "sender": {"login": "maintainer", "type": "User"},
                "pull_request": {"body": "Generated dependency notes", "user": {"login": "dependabot[bot]", "type": "Bot"}},
            }), encoding="utf-8")
            # When running the real check, then the original author controls formatting.
            result = subprocess.run([sys.executable, str(script)], env=dict(os.environ, GITHUB_EVENT_PATH=str(event)), capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_only_passed_reviews_can_mark_a_pr_ready(self):
        # Given a draft with source or coverage review still outstanding.
        for result in ("Passed", "Pending", "Failed: review incomplete; provider limit"):
            with self.subTest(result=result):
                body = VALID.replace("Passed", result)
                # When marked ready, then unfinished reviews block the description check.
                self.assertEqual(validate(body, draft=True), [])
                self.assertEqual(not validate(body), result == "Passed")

    def test_formatting_exemptions_require_docs_review_and_eligible_scope(self):
        # Given both exemptions, then only independently reviewed Markdown scope is ready.
        body = VALID.replace("## Tests review\n\nPassed", "## Tests review\n\n" + FORMATTING_ONLY)
        body = body.replace("## Simplicity review\n\nPassed", "## Simplicity review\n\n" + FORMATTING_ONLY)
        for paths in (["README.md"], ["docs/usage.md", "releases/v1.md"]):
            with self.subTest(paths=paths):
                self.assertEqual(validate(body, paths=paths), [])
                self.assertEqual(validate(body.replace("Passed", "Pending"), draft=True, paths=paths), [])
                self.assertTrue(validate(body.replace("Passed", "Pending"), paths=paths))
        # When file scope is missing, mixed or policy-bearing, then the exemption fails closed.
        for paths in (None, [], ["AGENTS.md"], ["review/README.md"], ["design/grammar.md"],
                      ["docs/AGENTS.md"], ["docs/SKILL.md"], ["README.md", "internal/app/cli.go"]):
            with self.subTest(paths=paths):
                self.assertTrue(validate(body, paths=paths))
        for section in ("Docs review", "Tests review", "Simplicity review"):
            # When exempting just one aspect, then standard review cannot be bypassed.
            self.assertTrue(validate(VALID.replace(f"## {section}\n\nPassed",
                                                 f"## {section}\n\n{FORMATTING_ONLY}"), paths=["README.md"]))

    def test_formatting_event_checks_the_actual_git_diff(self):
        # Given an eligible body and real base/head commits.
        script = Path(__file__).with_name("pr_description.py").resolve()
        body = VALID.replace("## Tests review\n\nPassed", "## Tests review\n\n" + FORMATTING_ONLY)
        body = body.replace("## Simplicity review\n\nPassed", "## Simplicity review\n\n" + FORMATTING_ONLY)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
                                               cwd=root, text=True, stderr=subprocess.PIPE).strip()

            git("init")
            (root / "README.md").write_text("Example\n")
            (root / "runtime.go").write_text("Original\n")
            git("add", "README.md", "runtime.go")
            git("commit", "-m", "base")
            base = git("rev-parse", "HEAD")
            event = root / "event.json"
            for name, code in (("README.md", 0), ("runtime.go", 1), ("docs/runtime.md", 1)):
                if "/" in name:
                    (root / "docs").mkdir()
                    git("mv", "runtime.go", name)
                (root / name).write_text("Original\n" if "/" in name else "Changed\n")
                git("add", name)
                git("commit", "-m", name)
                head = git("rev-parse", "HEAD")
                event.write_text(json.dumps({"pull_request": {"body": body, "base": {"sha": base}, "head": {"sha": head}}}))
                # When the entrypoint reads repository changes, then code edits and code renamed into Markdown are rejected.
                result = subprocess.run([sys.executable, str(script)], cwd=root,
                                        env=dict(os.environ, GITHUB_EVENT_PATH=str(event)), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)
            # Missing or invalid revision evidence must also reject the exemption.
            for refs in ({}, {"base": {"sha": "--invalid"}, "head": {"sha": head}}):
                event.write_text(json.dumps({"pull_request": {"body": body, **refs}}))
                result = subprocess.run([sys.executable, str(script)], cwd=root,
                                        env=dict(os.environ, GITHUB_EVENT_PATH=str(event)), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 1, result.stderr)

    def test_length_limit_counts_the_entire_unicode_body(self):
        # Given a description exactly at the limit, including a hidden comment.
        padding = MAX_CHARACTERS - len(VALID) - len("<!-- -->")
        body = VALID + "<!--" + "→" * padding + " -->"
        self.assertEqual(len(body), MAX_CHARACTERS)
        # When adding one character, then only the longer body is rejected.
        self.assertEqual(validate(body), [])
        self.assertTrue(validate(body + "x"))

    def test_required_sections_cannot_be_missing_duplicated_or_empty(self):
        # Given each required section missing, duplicated or containing only a comment.
        for name in SECTIONS:
            start = VALID.index(f"## {name}\n")
            end = VALID.find("\n## ", start + 1)
            end = len(VALID) if end == -1 else end
            invalid = (
                VALID[:start] + VALID[end:],
                VALID + f"\n## {name}\n\nPassed\n",
                VALID[:start] + f"## {name}\n\n<!-- placeholder -->\n" + VALID[end:],
            )
            for body in invalid:
                with self.subTest(section=name, body=body):
                    # When linting, then the missing or unusable section fails.
                    self.assertTrue(validate(body))

    def test_review_sections_reject_prose_and_ambiguous_results(self):
        # Given success narratives, unclear statuses or a missing failure reason.
        for result in ("Passed. Detailed review follows.", "Passed\n\nMore details.", "Complete", "Incomplete", "Failed", "Failed: "):
            with self.subTest(result=result):
                # When linting, then each review must use the agreed result format.
                self.assertTrue(validate(VALID.replace("Passed", result)))
                self.assertTrue(validate(VALID.replace("Passed", result), draft=True))
        for result in ('Pending review', 'pending', 'Failed:'):
            self.assertTrue(validate(VALID.replace('Passed', result), draft=True))

    def test_markdown_heading_variants_cannot_hide_duplicate_sections(self):
        # Given equivalent headings with indentation or optional closing hashes.
        for name in SECTIONS:
            for heading in (f"## {name} ##", f"   ## {name}", f"  ## {name} ###  "):
                with self.subTest(heading=heading):
                    # When linting, then one is valid but a second must fail.
                    self.assertEqual(validate(VALID.replace(f"## {name}\n", heading + "\n")), [])
                    self.assertTrue(validate(VALID + "\n" + heading + "\nIncomplete\n"))

    def test_headings_in_examples_do_not_count_as_real_sections(self):
        # Given a fenced example containing a review heading.
        example = "```markdown\n## Docs review\nPassed\n```\n"
        body = VALID.replace("- Add local reviews.", example)
        # When linting, then the example neither duplicates nor supplies a section.
        self.assertEqual(validate(body), [])
        self.assertTrue(validate(body.replace("## Changes", "Not a heading", 1)))
        fake_changes = VALID.replace("## Changes\n\n- Add local reviews.", "~~~markdown\n## Changes\n\nExample only.\n~~~")
        self.assertTrue(validate(fake_changes))

    def test_event_body_is_data_and_failures_return_nonzero(self):
        # Given PR bodies with shell syntax in a JSON event.
        script = Path(__file__).with_name("pr_description.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = root / "event.json"
            env = dict(os.environ, GITHUB_EVENT_PATH=str(event))
            for valid in (True, False):
                body = VALID.replace("- Add local reviews.", "$(touch unexpected); `touch unexpected`\n\n```text\n" + FORMATTING_ONLY + "\n```")
                if not valid:
                    body = body.replace("Passed", "Incomplete")
                event.write_text(json.dumps({"pull_request": {"body": body}}), encoding="utf-8")
                # When the real entry point runs, then it checks without executing input.
                result = subprocess.run([sys.executable, str(script)], env=env, cwd=root, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0 if valid else 1)
                self.assertFalse((root / "unexpected").exists())
                self.assertNotIn("$(touch", result.stdout + result.stderr)

    def test_draft_event_allows_pending_but_missing_flag_fails_closed(self):
        # Given the same pending body in a draft, ready or incomplete event.
        script = Path(__file__).with_name('pr_description.py')
        with tempfile.TemporaryDirectory() as name:
            event = Path(name) / 'event.json'
            for flags, code in (({'draft': True}, 0), ({'draft': False}, 1), ({}, 1)):
                event.write_text(json.dumps({'pull_request': {'body': VALID.replace('Passed', 'Pending'), **flags}}))
                # When checked through the entrypoint, then only the explicit draft is accepted.
                result = subprocess.run([sys.executable, str(script)], env=dict(os.environ, GITHUB_EVENT_PATH=str(event)), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)


if __name__ == "__main__":
    unittest.main()
