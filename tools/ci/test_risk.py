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

    def test_full_diff_reaches_suite_selector(self):
        event = {'pull_request': {'base': {'sha': 'a' * 40}, 'head': {'sha': 'b' * 40}}}
        with patch('tools.ci.risk.changed_paths', return_value=['src/internal/archive/avro/old.go', 'docs/new.md']):
            self.assertEqual(select(event)['suites'], ['fault', 'qualification'])
