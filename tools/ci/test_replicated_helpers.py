"""Fast checks must execute the tagged helper selection and reject missing passes."""

import ast
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from tools.ci.suites import EXPECTED


ROOT = Path(__file__).resolve().parents[2]
HELPERS = EXPECTED['replicated-helpers']


class ReplicatedHelpersTest(unittest.TestCase):
    def test_shared_fast_target_requires_exact_completed_helper_passes(self):
        # Given a Go command that exits zero with actual, absent or wrong events.
        self.assertTrue(HELPERS)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fake = directory/'go'
            arguments = directory/'arguments.json'
            fake.write_text('#!'+sys.executable+'\nimport json, os, sys\n'
                            'from pathlib import Path\n'
                            'Path(os.environ["HELPER_ARGUMENTS"]).write_text(json.dumps(dict(args=sys.argv[1:], cgo=os.environ.get("CGO_ENABLED"))))\n'
                            'for name in json.loads(os.environ["HELPER_EVENTS"]):\n'
                            '    print(json.dumps(dict(Action="pass", Test=name)))\n')
            fake.chmod(0o700)
            for emitted in (HELPERS, (), HELPERS[3:], HELPERS[4:], HELPERS[5:], HELPERS[:-1], ('TestDifferent',)):
                with self.subTest(emitted=emitted):
                    env = dict(os.environ, KAFERRY_REPORT_DIR=str(directory/'results'),
                               HELPER_ARGUMENTS=str(arguments), HELPER_EVENTS=json.dumps(emitted))
                    result = subprocess.run(['make', '--no-print-directory', 'replicated-helpers',
                                             'GO='+str(fake), 'PYTHON='+sys.executable],
                                            cwd=ROOT, env=env, text=True, capture_output=True, timeout=60)
                    # When the shared target verifies JSON, then exit zero alone
                    # cannot qualify an empty, partial or mismatched selection.
                    if emitted == HELPERS:
                        self.assertEqual(0, result.returncode, result.stdout+result.stderr)
                    else:
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn('Required tests did not pass', result.stderr)
                    captured = json.loads(arguments.read_text())
                    self.assertEqual('1', captured['cgo'])
                    args = captured['args']
                    for flag in ('-race', '-count=1', '-json', '-tags=integration,fault,load'):
                        self.assertIn(flag, args)
                    pattern = args[args.index('-run')+1]
                    self.assertTrue(all(re.fullmatch(pattern, name) for name in HELPERS))
                    for excluded in ('TestReplicatedZombieWorker', 'TestReplicatedProxyRoutes', 'TestBrokerCurrentLogChild', HELPERS[0]+'Extra'):
                        self.assertIsNone(re.fullmatch(pattern, excluded))

    def test_independent_collector_changes_select_real_cases(self):
        # Given the collector's independent checker, when it alone changes,
        # then the workflow's PR path filter selects the real replicated jobs.
        collector = ast.parse((ROOT/'tools/qualification/replicated_results.py').read_text())
        helpers = {node.module.replace('.', '/')+'.py' for node in ast.walk(collector)
                   if isinstance(node, ast.ImportFrom) and node.module and
                   node.module.startswith('tools.qualification.fault_')}
        self.assertEqual({'tools/qualification/fault_results.py'}, helpers)
        workflow = (ROOT/'.github/workflows/replicated-lab.yml').read_text()
        section = re.search(r"(?m)^  pull_request:\n    paths:\n((?:      - '[^']+'\n)+)(?=permissions:\n)", workflow)
        self.assertIsNotNone(section, 'read the actual PR paths, not another event or job')
        patterns = re.findall(r"      - '([^']+)'", section[1])
        # This workflow uses only positive literals, * and terminal **. GitHub
        # matches the whole rooted path; * excludes slash and ** includes it.
        # https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#filter-pattern-cheat-sheet
        for pattern in patterns:
            self.assertNotRegex(pattern, r'[!?+\[\]]')
            self.assertTrue('**' not in pattern or pattern.endswith('**'))
        expressions = ['.*'.join(re.escape(part).replace(r'\*', '[^/]*')
                                 for part in pattern.split('**')) for pattern in patterns]
        for changed, expected in [*((path, True) for path in sorted(helpers)),
                                  ('tools/qualification/fault_oracle.py', True),
                                  ('src/tests/qualification/matrix.json', True),
                                  ('src/tests/replicated/storage.sh', True),
                                  ('src/tests/system/replicated_broker_readiness_test.go', True),
                                  ('src/tests/system/nested/replicated_helper.go', False),
                                  ('src/tests/replicated/nested/check.py', True),
                                  ('docs/design/qualification.md', False)]:
            with self.subTest(single_file_change=changed):
                self.assertEqual(expected, any(re.fullmatch(expression, changed) for expression in expressions))

    def test_hosted_cases_keep_broker_and_outage_coverage_with_local_cases(self):
        # Given every registered local case, when selecting hosted physical runs,
        # then all four local cases remain exact candidate requirements.
        from tools.ci.suites import REPLICATED_CASES
        workflow = (ROOT/'.github/workflows/replicated-lab.yml').read_text()
        matrix = workflow.split('  faults:', 1)[1].split('    steps:', 1)[0]
        hosted = set(re.findall(r'case: ([a-z/-]+)', matrix))
        local = {'network/latency', 'network/bandwidth', 'network/slow-close', 'ownership/coordinator-kill-rebalance'}
        self.assertTrue(local <= set(REPLICATED_CASES))
        self.assertEqual(set(REPLICATED_CASES)-local-{'ownership/zombie-worker'}, hosted)
        self.assertIn('--case ownership/zombie-worker', workflow)

    def test_local_and_hosted_fast_checks_share_the_target(self):
        # Given the two fast entrypoints, when inspected, then both delegate the
        # selection and mandatory pass assertions to the same Make target.
        make = (ROOT/'Makefile').read_text()
        fast = make.split('\nfast:', 1)[1].split('\nsmoke:', 1)[0]
        self.assertIn('\t$(MAKE) replicated-helpers\n', fast)
        self.assertIn('run: make replicated-helpers', (ROOT/'.github/workflows/ci.yml').read_text())


if __name__ == '__main__':
    unittest.main()
