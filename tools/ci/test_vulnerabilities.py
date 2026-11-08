"""The vulnerability gate must reuse its scan without losing blocking findings."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class VulnerabilityGateTest(unittest.TestCase):
    def test_single_scan_reports_and_failure_handling(self):
        repository = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copyfile(repository / "Makefile", root / "Makefile")
            scanner = root / "scanner"
            scanner.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
from pathlib import Path
scenario = os.environ['VULN_SCENARIO']
converting = '-mode=convert' in sys.argv
operation = 'scan' if not converting else ('report' if '-format=sarif' in sys.argv else 'gate')
with Path('calls').open('a') as calls:
    calls.write(operation + '\\n')
if operation == 'scan':
    if scenario == 'scan-error':
        sys.exit(2)
    print(json.dumps({'finding': scenario == 'vulnerable'}))
else:
    finding = json.load(sys.stdin)['finding']
    if scenario == 'conversion-error':
        sys.exit(2)
    if operation == 'report':
        print(json.dumps({'version': '2.1.0', 'runs': [{'finding': finding}]}))
    elif finding:
        print('Reachable vulnerability found')
        sys.exit(3)
''')
            scanner.chmod(0o755)
            env = {**os.environ, "PYTHONPATH": str(repository)}
            for scenario in ("clean", "vulnerable", "scan-error", "conversion-error"):
                with self.subTest(scenario=scenario):
                    # Given stale reports and a scanner outcome from this invocation.
                    (root / "calls").write_text("")
                    reports = root / "build-results"
                    reports.mkdir(exist_ok=True)
                    sarif = reports / "govulncheck.sarif"
                    sarif.write_text('stale report')
                    # When running the same vulnerability target as local fast checks and CI.
                    result = subprocess.run(
                        ["make", "vulnerabilities", "GOVULNCHECK=" + str(scanner),
                         "PYTHON=" + sys.executable], cwd=root,
                        env={**env, "VULN_SCENARIO": scenario}, capture_output=True, text=True)
                    calls = (root / "calls").read_text().splitlines()
                    # Then scanning happens once; findings still block after producing the report.
                    self.assertEqual(1, calls.count("scan"), calls)
                    self.assertEqual(scenario == "clean", result.returncode == 0,
                                     result.stdout + result.stderr)
                    if scenario in {"clean", "vulnerable"}:
                        self.assertIn("gate", calls)
                        self.assertEqual(scenario == "vulnerable",
                                         json.loads(sarif.read_text())["runs"][0]["finding"])
                        recorded = json.loads((reports / "govulncheck.result.json").read_text())
                        self.assertEqual(3 if scenario == "vulnerable" else 0, recorded["exit_code"])
                    else:
                        self.assertNotIn("gate", calls)
                        self.assertNotIn('stale report', sarif.read_text() if sarif.exists() else '')
