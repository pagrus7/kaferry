"""Unknown paths fail closed; prose-only changes keep all normal PR checks."""

import unittest
from unittest.mock import patch

from risk import select, suites


class RiskTest(unittest.TestCase):
    def test_only_known_prose_paths_skip_additional_suites(self):
        # Given prose, mixed changes, unknown files or executable test infrastructure.
        for paths, extra in (
            ([], False),
            (['README.md', 'docs/usage.md', 'design/decisions.md'], False),
            (['releases/v1.md', 'review/tests.md'], False),
            (['README.md', 'new-component.go'], True),
            (['internal/copy/runner.go'], True),
            (['design/grammar.md'], True),
            (['docs/fixture.sh'], True),
            (['go.mod'], True),
            (['.github/workflows/ci.yml'], True),
            (['tests/qualification/compose.yaml'], True),
        ):
            with self.subTest(paths=paths):
                # When selected, then only the prose allowlist can skip the integrity suites.
                self.assertEqual(suites(paths), ['fault', 'qualification'] if extra else [])

    def test_full_diff_includes_deleted_and_renamed_paths(self):
        # Given a full PR diff, including the old name of a moved file.
        base, head = 'a' * 40, 'b' * 40
        event = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
        with patch('risk.subprocess.check_output', return_value='internal/file/old.go\ndocs/new.md\n') as git:
            # When classifying, then Git cannot hide a deletion behind rename detection.
            self.assertEqual(select(event)['suites'], ['fault', 'qualification'])
            git.assert_called_once_with(['git', 'diff', '--no-renames', '--name-only', f'{base}...{head}'], text=True)
        event['pull_request']['head']['sha'] = '--help'
        with self.assertRaisesRegex(ValueError, 'full base and head'):
            select(event)
