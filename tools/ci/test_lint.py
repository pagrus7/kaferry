"""Check that the lint gate catches complexity added inside an existing function."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipUnless(os.environ.get("GOLANGCI_LINT"), "Set GOLANGCI_LINT to the pinned binary")
class LintGateTest(unittest.TestCase):
    def test_changed_body_crosses_complexity_limit(self):
        # Given a committed function below the cognitive-complexity limit.
        executable = str(Path(os.environ["GOLANGCI_LINT"]).resolve())
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copyfile(Path(__file__).resolve().parents[2] / "Makefile", root / "Makefile")
            (root / "go.mod").write_text("module example.com/lintfixture\n\ngo 1.26.0\n")
            (root / ".golangci.yml").write_text(
                'version: "2"\nlinters:\n  default: none\n  enable: [gocognit]\n'
                '  settings:\n    gocognit:\n      min-complexity: 4\n')
            source = root / "fixture.go"
            source.write_text("package fixture\n\nfunc calculate(n int) int {\n"
                              "\tif n > 0 { return n }\n\treturn 0\n}\n")
            for command in (["git", "init", "--quiet"], ["git", "add", "."],
                            ["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                             "commit", "--quiet", "-m", "baseline"]):
                subprocess.run(command, cwd=root, check=True, capture_output=True)

            # When only the body changes and the function declaration stays on its original line.
            source.write_text("package fixture\n\nfunc calculate(n int) int {\n"
                              "\tif n > 0 {\n\t\tif n > 1 {\n\t\t\tif n > 2 {\n"
                              "\t\t\t\treturn n\n\t\t\t}\n\t\t}\n\t}\n\treturn 0\n}\n")
            env = {**os.environ, "GOCACHE": str(root / "go-cache"),
                   "GOMODCACHE": str(root / "modcache"), "GOLANGCI_LINT_CACHE": str(root / "lint-cache")}
            result = subprocess.run(["make", "lint", "GOLANGCI_LINT=" + executable, "LINT_BASE=HEAD"],
                                    cwd=root, env=env, capture_output=True, text=True)

            # Then the new complexity finding fails the gate even though its line was unchanged.
            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("(gocognit)", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
