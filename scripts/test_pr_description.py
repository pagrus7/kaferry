"""PR description checks reject verbose or ambiguous review reports."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from pr_description import MAX_CHARACTERS, SECTIONS, validate


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
    def test_concise_results_and_failure_reasons_are_valid(self):
        # Given a concise description with successful or failed reviews.
        for result in ("Passed", "Failed: review incomplete; provider limit"):
            with self.subTest(result=result):
                body = VALID.replace("Passed", result)
                # When linting, then either explicit outcome has valid format.
                self.assertEqual(validate(body), [])

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
                body = VALID.replace("- Add local reviews.", "$(touch unexpected); `touch unexpected`")
                if not valid:
                    body = body.replace("Passed", "Incomplete")
                event.write_text(json.dumps({"pull_request": {"body": body}}), encoding="utf-8")
                # When the real entry point runs, then it checks without executing input.
                result = subprocess.run([sys.executable, str(script)], env=env, cwd=root, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0 if valid else 1)
                self.assertFalse((root / "unexpected").exists())
                self.assertNotIn("$(touch", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
