import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class GoChecksTest(unittest.TestCase):
    def test_make_fmt_rejects_unformatted_and_invalid_go(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory, 'src', 'example.go')
            source.parent.mkdir()
            for code, should_pass in [('package example\n', True),
                                      ('package  example\n', False),
                                      ('package\n', False)]:
                with self.subTest(code=code):
                    source.write_text(code)
                    result = subprocess.run(['make', '-s', '-f', str(ROOT / 'Makefile'), 'fmt'],
                                            cwd=directory, capture_output=True, text=True)
                    output = result.stdout + result.stderr
                    self.assertEqual(result.returncode == 0, should_pass, output)
                    if not should_pass:
                        self.assertIn('src/example.go', output)

    def test_make_lint_rejects_darwin_only_error_after_linux_passes(self):
        linter = shutil.which(os.environ.get('GOLANGCI_LINT', 'golangci-lint'))
        if not linter:
            self.skipTest('golangci-lint is required')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory, 'module')
            root.mkdir()
            shutil.copy(ROOT / '.golangci.yml', root)
            (root / 'go.mod').write_text('module example.org/fixture\n\ngo 1.26\n')
            capture = root / 'src/internal/archive/capture'
            system = root / 'src/tests/system'
            capture.mkdir(parents=True)
            system.mkdir(parents=True)
            (capture / 'capture.go').write_text('package capture\n')
            (system / 'system.go').write_text('package system\n')
            (capture / 'rename_darwin.go').write_text(
                '//go:build integration && load\n\npackage capture\n\nvar _ = missingDarwinSymbol\n')
            result = subprocess.run(['make', '-s', '-f', str(ROOT / 'Makefile'), 'lint',
                                     f'GOLANGCI_LINT={Path(linter).resolve()}',
                                     f'GOLANGCI_LINT_CACHE={root}/cache'],
                                    cwd=root, env={**os.environ, 'TMPDIR': directory},
                                    capture_output=True, text=True)
            output = result.stdout + result.stderr
            self.assertNotEqual(result.returncode, 0, output)
            self.assertIn('undefined: missingDarwinSymbol', output)
