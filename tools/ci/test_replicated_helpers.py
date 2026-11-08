"""Fast checks must execute the tagged helper selection and reject missing passes."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
HELPERS = (
    'TestReplicatedPublisherAcknowledgesAllPartitionsAndStops',
    'TestReplicatedPublisherStopsAfterRejectedBatch',
    'TestReplicatedAuditFailureCleanupRetainsDiagnostics',
    'TestReplicatedAuditFailureCleanupSkipsPassingCases',
    'TestReplicatedAuditFailureCaptureSharesDeadline',
    'TestReplicatedAuditFailureOutputIsBounded',
    'TestReplicatedAuditArmsOnlyAfterCurrentGenerationAcceptedProgress',
    'TestReplicatedAuditPreservesDecodedConfigurationAndOriginalDigest',
    'TestReplicatedZombieRSSStopsAtProcessExit',
    'TestReplicatedZombieExposureSpansSessionWithinSaveDeadline',
)


class ReplicatedHelpersTest(unittest.TestCase):
    def test_shared_fast_target_requires_exact_completed_helper_passes(self):
        # Given a Go command that exits zero with actual, absent or wrong events.
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
            for emitted in (HELPERS, (), HELPERS[:-1], ('TestDifferent',)):
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
                    for excluded in ('TestReplicatedZombieWorker', 'TestReplicatedProxyRoutes', HELPERS[0]+'Extra'):
                        self.assertIsNone(re.fullmatch(pattern, excluded))

    def test_local_and_hosted_fast_checks_share_the_target(self):
        # Given the two fast entrypoints, when inspected, then both delegate the
        # selection and mandatory pass assertions to the same Make target.
        make = (ROOT/'Makefile').read_text()
        fast = make.split('\nfast:', 1)[1].split('\nsmoke:', 1)[0]
        self.assertIn('\t$(MAKE) replicated-helpers\n', fast)
        self.assertIn('run: make replicated-helpers', (ROOT/'.github/workflows/ci.yml').read_text())


if __name__ == '__main__':
    unittest.main()
