"""Tests for the PR compatibility matrix's fail-closed path decision."""

import subprocess
import unittest
from unittest.mock import patch

from tools.ci import compatibility_scope as scope


class CompatibilityScopeTests(unittest.TestCase):
    def test_unrelated_prose_and_tests_skip_matrix(self):
        paths = ['docs/usage.md', 'README.md', '.codex/agents/docs-reviewer.toml',
                 'src/internal/copy/flow_test.go', 'src/cmd/kaferry/command_test.go',
                 'src/tests/system/container_test.go', 'src/tests/system/fault_test.go',
                 'src/tests/system/new_test.go',
                 'tools/qualification/test_qualify.py']
        with patch.object(scope, 'changed_paths', return_value=paths):
            run, changed, reason = scope.decide({})
        self.assertFalse(run)
        self.assertEqual(paths, changed)
        self.assertIn('unrelated', reason)

    def test_runtime_harness_and_compatibility_tests_run_matrix(self):
        for path in ('src/internal/copy/flow.go', 'src/internal/archive/avro/schema.json',
                     'src/tests/system/soak.go', 'src/tests/system/fixtures.go',
                     'src/tests/system/qualification.go', 'src/tests/compatibility/soak_test.go',
                     'src/tests/compatibility/sasl_test.go', 'src/tests/qualification/matrix.json',
                     'tools/qualification/qualify.py', 'tools/ci/scheduled.py',
                     '.github/workflows/compatibility.yml', 'go.mod', 'go.sum', 'Makefile'):
            with self.subTest(path=path), patch.object(scope, 'changed_paths', return_value=[path]):
                self.assertTrue(scope.decide({})[0])

    def test_mixed_or_unknown_changes_run_matrix(self):
        for paths in (['docs/usage.md', 'src/internal/copy/flow.go'],
                      ['docs/site/index.html'], ['src/tests/compatibility/new_test.go']):
            with self.subTest(paths=paths), patch.object(scope, 'changed_paths', return_value=paths):
                self.assertTrue(scope.decide({})[0])

    def test_empty_or_unavailable_diff_runs_matrix(self):
        with patch.object(scope, 'changed_paths', return_value=[]):
            self.assertTrue(scope.decide({})[0])
        with patch.object(scope, 'changed_paths', side_effect=subprocess.CalledProcessError(1, 'git')):
            self.assertTrue(scope.decide({})[0])


if __name__ == '__main__':
    unittest.main()
