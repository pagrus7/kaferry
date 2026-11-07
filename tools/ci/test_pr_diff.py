"""The shared PR diff reader must include every changed path."""

import unittest
from unittest.mock import patch

from tools.ci import pr_diff


class PullRequestDiffTest(unittest.TestCase):
    def test_reads_nul_delimited_paths_with_renames_disabled(self):
        base, head = 'a' * 40, 'b' * 40
        event = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}}}
        with patch.object(pr_diff.subprocess, 'check_output', return_value=b'docs/name\nline.md\0src/internal/copy/flow.go\0') as read:
            self.assertEqual(['docs/name\nline.md', 'src/internal/copy/flow.go'], pr_diff.changed_paths(event))
        read.assert_called_once_with(['git', 'diff', '--no-renames', '--name-only', '-z', f'{base}...{head}', '--'])

    def test_rejects_non_commit_input(self):
        event = {'pull_request': {'base': {'sha': 'a' * 40}, 'head': {'sha': '--help'}}}
        with self.assertRaisesRegex(ValueError, 'full base and head'):
            pr_diff.changed_paths(event)
