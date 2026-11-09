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
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
HELPERS = (
    'TestReplicatedPublisherAcknowledgesAllPartitionsAndStops',
    'TestReplicatedPublisherStopsAfterRejectedBatch',
    'TestReplicatedBrokerPublisherKeepsHealthyPartitionsAndDrainsUncertainWrites',
    'TestReplicatedBrokerPublisherCatchesUpDelayedSchedule',
    'TestReplicatedBrokerPublisherCancellationDrainsDelayedOffers',
    'TestReplicatedBrokerPublisherErrorDrainsDelayedOffers',
    'TestNetworkMeasurementRequiresCaughtUpSourceAndOutput',
    'TestNetworkOutageForcedExitBindsOwnedCutAndRetainedRSS',
    'TestNetworkOutageNegotiationEOFRequiresCompleteBoundWriteFailures',
    'TestBrokerFaultRestartRequiresClassifiedTerminalFailure',
    'TestBrokerCurrentProgressDoesNotWaitForChildExit',
    'TestBrokerCurrentProgressRequiresReadableCompleteEvidence',
    'TestBrokerCurrentProgressRejectsMalformedCompleteRows',
    'TestBrokerRestartRechecksFailureAfterHealthObservation',
    'TestBrokerNegotiationEOFRequiresAffectedPlainCopyAndEveryUnconfirmedError',
    'TestBrokerLabelledSourceEOFRequiresBoundAllConfirmedExit',
    'TestBrokerForcedCopyExitLeavesLifetimeHighwaterUnavailable',
    'TestBrokerExitWitnessRetainsUnsupportedAndIntentionalExitsBeforeAssertions',
    'TestBrokerISRSelectionRequiresActiveSameLeaderBacklog',
    'TestBrokerPollJournalRetainsFailedQueryAndHealthTail',
    'TestBrokerHealthInspectionRetainsBoundedProbeEvidence',
    'TestBrokerReadinessRequiresFreshPinnedMetadataWithinSharedBudget',
    'TestReplicatedBrokerPublisherRetriesNegotiationEOFAfterConnectionLoss',
    'TestReplicatedBrokerPublisherRetriesSafeLeaderResponsesWithinDeliveryBound',
) + (
    # Audit and zombie helpers retain their independent measurements.
    'TestReplicatedAuditFailureCleanupRetainsDiagnostics',
    'TestReplicatedAuditFailureCleanupSkipsPassingCases',
    'TestReplicatedAuditFailureCaptureSharesDeadline',
    'TestReplicatedAuditFailureOutputIsBounded',
    'TestReplicatedAuditArmsOnlyAfterCurrentGenerationAcceptedProgress',
    'TestReplicatedAuditPreservesDecodedConfigurationAndOriginalDigest',
    'TestReplicatedAuditReadinessReadsEveryBrokerConcurrently',
    'TestReplicatedAuditReadinessRejectsIncompleteBrokerReads',
    'TestReplicatedAuditReadinessSharesCancellation',
    'TestReplicatedZombieRSSStopsAtProcessExit',
    'TestReplicatedZombieExposureSpansSessionWithinSaveDeadline',
)


class ReplicatedHelpersTest(unittest.TestCase):
    def workflow_paths(self, name, event):
        workflow = (ROOT/f'.github/workflows/{name}.yml').read_text()
        section = re.search(
            rf"(?m)^  {event}:\n(?:    branches: \[main\]\n)?    paths:\n"
            r"((?:      - '[^'\n]+'\n)+)(?=^(?:  )?[a-z][a-z0-9_-]*:|\Z)", workflow)
        self.assertIsNotNone(section, f'read the complete {event} paths with supported syntax')
        patterns = re.findall(r"      - '([^']+)'", section[1])
        # These workflows use positive literals, * and terminal **. GitHub
        # matches the whole rooted path; * excludes slash and ** includes it.
        # https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#filter-pattern-cheat-sheet
        for pattern in patterns:
            self.assertNotRegex(pattern, r'[!?+\[\]]')
            self.assertTrue('**' not in pattern or pattern.endswith('**'))
        return ['.*'.join(re.escape(part).replace(r'\*', '[^/]*')
                          for part in pattern.split('**')) for pattern in patterns]

    def test_workflow_paths_rejects_unsupported_trailing_syntax(self):
        # Given a supported filter followed by another event boundary.
        prefix = "on:\n  push:\n    branches: [main]\n    paths:\n      - 'src/**'\n"
        suffix = "  workflow_dispatch:\npermissions: {}\n"
        with patch.object(Path, 'read_text', return_value=prefix+suffix):
            self.assertEqual(['src/.*'], self.workflow_paths('in-memory', 'push'))
        # When a valid broad filter or malformed entry follows the known paths,
        # then reject unsupported syntax rather than silently reading a prefix.
        for trailing in ('      - "**"\n', '  - "**"\n', '      - unquoted/path\n',
                         "      - 'unterminated\n", "      - 'other/path' # comment\n"):
            with self.subTest(trailing=trailing):
                with patch.object(Path, 'read_text', return_value=prefix+trailing+suffix), \
                        self.assertRaisesRegex(AssertionError, 'complete push paths'):
                    self.workflow_paths('in-memory', 'push')

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
                    for excluded in ('TestReplicatedZombieWorker', 'TestReplicatedProxyRoutes', 'TestBrokerCurrentLogChild', HELPERS[0]+'Extra'):
                        self.assertIsNone(re.fullmatch(pattern, excluded))

    def test_broker_collector_imports_select_real_cases_on_single_file_changes(self):
        # Given the real collector's broker helpers, when either alone changes,
        # then the workflow's PR path filter selects the real replicated jobs.
        collector = ast.parse((ROOT/'tools/qualification/replicated_results.py').read_text())
        helpers = {node.module.replace('.', '/')+'.py' for node in ast.walk(collector)
                   if isinstance(node, ast.ImportFrom) and node.module and
                   node.module.startswith('tools.qualification.broker_')}
        self.assertGreaterEqual(len(helpers), 2)
        expressions = self.workflow_paths('replicated-lab', 'pull_request')
        for changed, expected in [*((path, True) for path in sorted(helpers)),
                                  ('tools/qualification/storage_census_images_test.py', True),
                                  ('src/tests/qualification/matrix.json', True),
                                  ('src/tests/replicated/storage.sh', True),
                                  ('src/tests/system/replicated_broker_readiness_test.go', True),
                                  ('src/tests/system/nested/replicated_helper.go', False),
                                  ('src/tests/replicated/nested/check.py', True),
                                  ('docs/design/qualification.md', False)]:
            with self.subTest(single_file_change=changed):
                self.assertEqual(expected, any(re.fullmatch(expression, changed) for expression in expressions))

    def test_proxy_probe_inputs_refresh_main_and_validate_prs(self):
        # Given the proof's imported project module and its compile/render inputs.
        proof = 'tools/qualification/proxy_port_reservation_images_test.py'
        parsed = ast.parse((ROOT/proof).read_text())
        imports = {node.module.replace('.', '/')+'/'+alias.name+'.py'
                   for node in ast.walk(parsed) if isinstance(node, ast.ImportFrom)
                   and node.module == 'tools.qualification' for alias in node.names}
        self.assertTrue(imports)
        inputs = imports | {proof, 'src/tests/replicated/compose.yaml', 'go.mod', 'go.sum',
                            'tools/__init__.py', 'tools/qualification/__init__.py',
                            '.github/actions/setup-go/action.yml', 'codecov.yml',
                            '.github/workflows/proxy-probe.yml'}
        inputs.update(str(path.relative_to(ROOT)) for path in (ROOT/'src/tests/replicated/portprobe').glob('*.go'))
        # When any input alone changes, then refresh coverage on main and test
        # the same job on PRs; unrelated product, broker and docs edits stay out.
        for event in ('push', 'pull_request'):
            expressions = self.workflow_paths('proxy-probe', event)
            for changed, expected in [*((path, True) for path in sorted(inputs)),
                                      ('src/internal/copy/runner.go', False),
                                      ('src/tests/system/replicated_broker.go', False),
                                      ('tools/qualification/storage_census_images_test.py', False),
                                      ('docs/design/qualification.md', False)]:
                with self.subTest(event=event, single_file_change=changed):
                    self.assertEqual(expected, any(re.fullmatch(expression, changed) for expression in expressions))

    def test_local_and_hosted_fast_checks_share_the_target(self):
        # Given the two fast entrypoints, when inspected, then both delegate the
        # selection and mandatory pass assertions to the same Make target.
        make = (ROOT/'Makefile').read_text()
        fast = make.split('\nfast:', 1)[1].split('\nsmoke:', 1)[0]
        self.assertIn('\t$(MAKE) replicated-helpers\n', fast)
        self.assertIn('run: make replicated-helpers', (ROOT/'.github/workflows/ci.yml').read_text())


if __name__ == '__main__':
    unittest.main()
