"""Check the full Go lint gate and its Go test duplication exemption."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipUnless(os.environ.get("GOLANGCI_LINT"), "Set GOLANGCI_LINT to the pinned binary")
class LintGateTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.executable = str(Path(os.environ["GOLANGCI_LINT"]).resolve())
        repository = Path(__file__).resolve().parents[2]
        for name in ("Makefile", ".golangci.yml"):
            shutil.copyfile(repository / name, self.root / name)
        (self.root / "go.mod").write_text("module example.com/lintfixture\n\ngo 1.26.0\n")

    def lint(self):
        env = {**os.environ, "GOCACHE": str(self.root / "go-cache"),
               "GOMODCACHE": str(self.root / "modcache"),
               "GOLANGCI_LINT_CACHE": str(self.root / "lint-cache")}
        return subprocess.run(["make", "lint", "GOLANGCI_LINT=" + self.executable],
                              cwd=self.root, env=env, capture_output=True, text=True)

    def test_complex_function_fails_gate(self):
        # Given a function exceeding an isolated cognitive-complexity limit.
        (self.root / ".golangci.yml").write_text(
            'version: "2"\nlinters:\n  default: none\n  enable: [gocognit]\n'
            '  settings:\n    gocognit:\n      min-complexity: 4\n')
        (self.root / "fixture.go").write_text(
            "package fixture\n\nfunc calculate(n int) int {\n"
            "\tif n > 0 {\n\t\tif n > 1 {\n\t\t\tif n > 2 {\n"
            "\t\t\t\treturn n\n\t\t\t}\n\t\t}\n\t}\n\treturn 0\n}\n")
        # When running the target, then the complexity finding fails the gate.
        result = self.lint()
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("(gocognit)", result.stdout + result.stderr)

    def test_go_duplication_excludes_test_files(self):
        # Given duplicated Go functions and the repository's actual lint settings.
        body = "\tvalue += 2\n" * 30 + "\treturn value\n"
        source = self.root / "fixture.go"
        source.write_text("package fixture\n\n" + "\n".join(
            "func " + name + "(value int) int {\n" + body + "}\n"
            for name in ("First", "Second")))
        # When production code duplicates, then dupl fails the gate.
        result = self.lint()
        self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("(dupl)", result.stdout + result.stderr)
        # When the same helpers belong to tests, then the configured exemption applies.
        source.rename(self.root / "fixture_test.go")
        result = self.lint()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
