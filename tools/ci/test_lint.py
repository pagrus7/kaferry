"""Check that the lint gate rejects complexity in existing functions."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipUnless(os.environ.get("GOLANGCI_LINT"), "Set GOLANGCI_LINT to the pinned binary")
class LintGateTest(unittest.TestCase):
    def test_complex_function_fails_gate(self):
        # Given a function below the cognitive-complexity limit.
        executable = str(Path(os.environ["GOLANGCI_LINT"]).resolve())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copyfile(Path(__file__).resolve().parents[2] / "Makefile", root / "Makefile")
            (root / "go.mod").write_text("module example.com/lintfixture\n\ngo 1.26.0\n")
            (root / ".golangci.yml").write_text(
                'version: "2"\nlinters:\n  default: none\n  enable: [gocognit]\n'
                '  settings:\n    gocognit:\n      min-complexity: 4\n')
            source = root / "fixture.go"
            # When an existing function exceeds the cognitive-complexity limit.
            source.write_text("package fixture\n\nfunc calculate(n int) int {\n"
                              "\tif n > 0 {\n\t\tif n > 1 {\n\t\t\tif n > 2 {\n"
                              "\t\t\t\treturn n\n\t\t\t}\n\t\t}\n\t}\n\treturn 0\n}\n")
            env = {**os.environ, "GOCACHE": str(root / "go-cache"),
                   "GOMODCACHE": str(root / "modcache"), "GOLANGCI_LINT_CACHE": str(root / "lint-cache")}
            result = subprocess.run(["make", "lint", "GOLANGCI_LINT=" + executable],
                                    cwd=root, env=env, capture_output=True, text=True)

            # Then the complexity finding fails the full repository gate.
            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("(gocognit)", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
