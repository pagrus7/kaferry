"""Unknown paths fail closed; prose-only changes keep all normal PR checks."""

import unittest
from unittest.mock import patch

from tools.ci.risk import select, suites


class RiskTest(unittest.TestCase):
    def test_only_known_prose_paths_skip_additional_suites(self):
        # Given prose, mixed changes, unknown files or executable test infrastructure.
        for paths, extra in (
            ([], False),
            (['README.md', 'docs/usage.md', 'docs/design/decisions.md'], False),
            (['docs/releases/v1.md', 'docs/development/reviews/tests.md'], False),
            (['README.md', 'new-component.go'], True),
            (['src/internal/copy/runner.go'], True),
            (['docs/design/grammar.md'], True),
            (['docs/fixture.sh'], True),
            (['go.mod'], True),
            (['.github/workflows/ci.yml'], True),
            (['src/tests/qualification/compose.yaml'], True),
        ):
            with self.subTest(paths=paths):
                # When selected, then only the prose allowlist can skip the integrity suites.
                self.assertEqual(suites(paths), ['fault', 'qualification'] if extra else [])

    def test_full_diff_includes_deleted_and_renamed_paths(self):
        # Given a full PR diff, including the old name of a moved file.
        base, head = 'a' * 40, 'b' * 40
        event = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
        with patch('tools.ci.risk.subprocess.check_output', return_value='src/internal/archive/avro/old.go\ndocs/new.md\n') as git:
            # When classifying, then Git cannot hide a deletion behind rename detection.
            self.assertEqual(select(event)['suites'], ['fault', 'qualification'])
            git.assert_called_once_with(['git', 'diff', '--no-renames', '--name-only', f'{base}...{head}'], text=True)
        event['pull_request']['head']['sha'] = '--help'
        with self.assertRaisesRegex(ValueError, 'full base and head'):
            select(event)
