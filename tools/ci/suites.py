"""Shared Go suite selection and required execution evidence."""

import argparse
import json
import os
from pathlib import Path
import subprocess

from tools.ci.ci import run, verify_tests


# Fixture ownership, binary selection and workload environment stay with callers.
GO_SUITES = {
    'integration': ('integration', None, True),
    'load': ('integration,load', '^TestLoad', False),
    'soak': ('integration,load', '^TestStreamingSoak', False),
    'fault': ('integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', False),
    'recovery': ('integration,fault', '^TestRecoveryBlockedOutput$', False),
    'fault-replicated': ('integration,fault,load', '^TestReplicatedZombieWorker$|^TestReplicatedBrokerFaults$|^TestReplicatedNetworkFaults$|^TestReplicatedOwnershipCases$', False),
}
REPLICATED_CASES = {'ownership/zombie-worker': 'TestReplicatedZombieWorker'} | {
    'broker/'+name: 'TestReplicatedBrokerFaults/'+name for name in
    ('source-leader', 'destination-leader', 'rolling-source', 'rolling-destination', 'destination-isr-loss')} | {
    'network/'+name: 'TestReplicatedNetworkFaults/'+name for name in
    ('destination-outage', 'destination-leaders-outage', 'latency', 'bandwidth', 'slow-close')} | {
    'ownership/'+name: 'TestReplicatedOwnershipCases/'+name for name in
    ('worker-kill-rebalance', 'coordinator-kill-rebalance', 'worker-scale')}
RECOVERY_CASES = {'recovery/'+name for name in
                  ('kill', 'signal', 'delivery-timeout', 'source-outage-shutdown', 'ownership-loss')}
FAULT_CASES = RECOVERY_CASES | {'fault/crash', 'fault/churn', 'fault/blocking-commit'}
LOAD_PROFILES = ('near-limit-v1', 'hot-keys-v1', 'headers-v1', 'tombstones-v1',
                 'source-none-v1', 'source-gzip-v1', 'source-snappy-v1', 'source-lz4-v1', 'source-zstd-v1')
OBSERVABILITY_TEST = 'TestLoadObservability/round-0/disabled'
EXPECTED = {
    'integration': ['TestAllFileDirectionsAndResume', 'TestTopicToTopicSlice',
                    'TestLatestEndRemainsPinnedAfterCompletion',
                    'TestKafkaArchiveConcurrentCapture',
                    'TestHistoricalFixtureTopicRetention/historical_default',
                    'TestHistoricalFixtureTopicRetention/finite_override',
                    'TestRunningSourceOffsetOutOfRangeFailsWithoutSavingProgress',
                    'TestExpiredSourceProgressRestartReportsGapAndCopiesRetainedRecords'],
    'load': ['TestLoadBoundedKafkaCopy/default'] + ['TestLoadBoundedKafkaCopy/' + name for name in LOAD_PROFILES],
    'soak': ['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'],
    'fault': ['TestBlockingCommitAcrossBrokerOutage', 'TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits',
              'TestCrashResumeReconcilesRetainedRecords', 'TestRecoveryBlockedOutput'],
    'recovery': ['TestRecoveryBlockedOutput'],
    'fault-replicated': list(REPLICATED_CASES.values()),
    'qualification': [
        'TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight', 'TestSaslTlsScramMechanisms/SCRAM-SHA-256',
        'TestSaslTlsRestrictedAclInspectionAndUnnamedCopy',
        'TestSaslTlsRestrictedAclInspectionAndUnnamedCopy/status',
        'TestSaslTlsRestrictedAclInspectionAndUnnamedCopy/explain',
        'TestSaslTlsRestrictedAclInspectionAndUnnamedCopy/unnamed-group',
        'TestSaslTlsScramMechanisms/SCRAM-SHA-512', 'TestSaslTlsExternalAuthentication/OAUTHBEARER',
        'TestSaslTlsExternalAuthentication/GSSAPI', 'TestSaslTlsOAuthTokenFile',
        'TestSaslTlsOAuthRenewalAndKeyRotation', 'TestMutualTlsCopy', 'TestServerTlsCopy',
        'TestReplicatedClusterLeaderFailurePreservesCopy'],
    'quickstart': ['TestKafkaQuickStart'],
    'smoke': [OBSERVABILITY_TEST, 'TestArchiveTutorial', 'TestArchiveTutorialCancellation'],
    'replicated-helpers': '''
        TestOwnershipCaseRejectsUnprovedMembership
        TestOwnershipCoordinatorRestartRequiresRecordedScope
        TestOwnershipKilledWorkerKeepsEarlierPeak
        TestOwnershipGenerationsReuseOneAuditSnapshot
        TestOwnershipObservationFollowsFreshCoordinatorBehindAcceptingProxy
        TestReplicatedPublisherAcknowledgesAllPartitionsAndStops
        TestReplicatedPublisherStopsAfterRejectedBatch
        TestReplicatedBrokerPublisherKeepsHealthyPartitionsAndDrainsUncertainWrites
        TestReplicatedBrokerPublisherCatchesUpDelayedSchedule
        TestReplicatedBrokerPublisherCancellationDrainsDelayedOffers
        TestReplicatedBrokerPublisherErrorDrainsDelayedOffers
        TestNetworkMeasurementRequiresCaughtUpSourceAndOutput
        TestNetworkHealthyPreconditionNeedsThirtySecondsAndBothRates
        TestNetworkDestinationLeadersRemainStableAcrossPhases
        TestBrokerWorkCapsRequireActualSamplesAndKeepMissingPeaksUnavailable
        TestBrokerRestartRequiresCurrentLifetimeInsideRecoveryWindow
        TestNetworkOutageRestartRequiresFirstWorkerRSSAndActiveCut
        TestBrokerCurrentProgressDoesNotWaitForChildExit
        TestBrokerCurrentProgressRequiresReadableCompleteEvidence
        TestBrokerCurrentProgressRejectsMalformedCompleteRows
        TestBrokerRestartRechecksFailureAfterHealthObservation
        TestBrokerExitWitnessRetainsUnsupportedAndIntentionalExitsBeforeAssertions
        TestBrokerISRSelectionRequiresActiveSameLeaderBacklog
        TestBrokerReadinessRequiresFreshPinnedMetadataWithinSharedBudget
        TestReplicatedBrokerPublisherRetriesNegotiationEOFAfterConnectionLoss
        TestReplicatedBrokerPublisherRetriesSafeLeaderResponsesWithinDeliveryBound
        TestReplicatedAuditLevelRetriesConnectionErrorOnce
        TestReplicatedAuditFailureCleanupRetainsDiagnostics
        TestReplicatedAuditFailureCleanupSkipsPassingCases
        TestReplicatedAuditFailureCaptureSharesDeadline
        TestReplicatedAuditFailureOutputIsBounded
        TestReplicatedAuditArmsOnlyAfterCurrentGenerationAcceptedProgress
        TestReplicatedAuditPreservesDecodedConfigurationAndOriginalDigest
        TestReplicatedAuditReadinessReadsEveryBrokerConcurrently
        TestReplicatedAuditReadinessRejectsIncompleteBrokerReads
        TestReplicatedAuditReadinessSharesCancellation
        TestReplicatedAuditCohortUsesOneCompleteParsedSnapshot
        TestReplicatedZombieRSSStopsAtProcessExit
        TestReplicatedZombieExposureSpansSessionWithinSaveDeadline
    '''.split(),
}
EXPECTED['kafka'] = EXPECTED['integration'] + ['TestCLIInterruptCommitsAndResumeDoesNotRecopy']
EXPECTED['ci-integration'] = EXPECTED['kafka'] + EXPECTED['quickstart']
EXPECTED['ci-load'] = EXPECTED['load'] + [OBSERVABILITY_TEST]
PATTERNS = {
    'smoke': '^(TestArchiveTutorial(Cancellation)?|TestLoadObservability)$/^round-0$/^disabled$',
    'replicated-helpers': '^(' + '|'.join(EXPECTED['replicated-helpers']) + ')$',
}


def fault_timeout_catalog():
    catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
    standard = {'session.timeout.ms', 'delivery_timeout', 'shutdown_timeout'}
    ownership = standard | {'socket.timeout.ms', 'progress_save_timeout'}
    broker = ownership | {'replica.lag.time.max.ms'}
    def allowed(id):
        if id in REPLICATED_CASES and id.startswith('ownership/') and id != 'ownership/zombie-worker':
            return ownership | {'max.poll.interval.ms'} | ({'replica.lag.time.max.ms'} if id == 'ownership/coordinator-kill-rebalance' else set())
        return broker if id in REPLICATED_CASES and id.startswith('broker/') else ownership if id in REPLICATED_CASES else standard
    if not catalog or any(not budgets or set(budgets)-allowed(id) or
                          id in REPLICATED_CASES and set(budgets) != allowed(id) or
                          any(type(ms) is not int or ms <= 0 for ms in budgets.values())
                          for id, budgets in catalog.items()):
        raise ValueError('fault observation timeout catalog must contain positive milliseconds')
    return catalog


def replicated_execution_seconds(selected):
    catalog = fault_timeout_catalog()
    if not selected or len(selected) != len(set(selected)) or any(id not in REPLICATED_CASES or id not in catalog for id in selected):
        raise ValueError('unknown or unregistered replicated fault case')
    # Preserve the zombie's existing four-minute outer deadline. Each new
    # deterministic case adds its own complete configured recovery sum. The
    # network healthy reference grows from one session to thirty seconds.
    return sum(240 + (sum(catalog[id].values())+999)//1000 +
               (30-catalog[id]['session.timeout.ms']//1000 if id.startswith('network/') else 0)
               if id != 'ownership/zombie-worker' else 240 for id in selected)


def fault_execution_seconds():
    catalog = fault_timeout_catalog()
    if not FAULT_CASES <= catalog.keys():
        raise ValueError('fault timeout catalog omitted a selected case')
    # Preserve the existing 600s execution allowance and account for every added
    # full recovery horizon. Churn's three windows overlap: only their last 135s
    # horizon extends the case, so its catalog entry is counted once.
    milliseconds = sum(sum(catalog[id].values()) for id in FAULT_CASES)
    return 600 + (milliseconds+999)//1000


def command(name, go='go', output='-json', timeout=None, packages='./src/tests/system'):
    tags, pattern, race = GO_SUITES[name]
    if name == 'soak' and packages == './src/tests/system':
        packages = './src/tests/compatibility'
    if name == 'fault' and timeout is None:
        timeout = f'{fault_execution_seconds()}s'
    if name == 'fault-replicated' and timeout is None:
        timeout = f'{replicated_execution_seconds(list(REPLICATED_CASES))}s'
    result = [go, 'test', '-count=1']
    if output:
        result.append(output)
    if race:
        result.append('-race')
    if timeout:
        result.append('-timeout=' + timeout)
    result.append('-tags=' + tags)
    if pattern:
        result += ['-run', pattern]
    return result + [packages]


def ci(name):
    if name == 'integration':
        cmd = ['env', 'CGO_ENABLED=1', 'KAFERRY_QUICKSTART=1'] + command(name)
    else:
        cmd = ['make', name, 'KAFERRY_TEST_OUTPUT=-json']
        if name == 'soak':
            cmd.append('SOAK_TEST_TIMEOUT=40m')
    directory = Path('build-results')
    code = run(name, cmd, directory)
    if code == 0:
        suite = 'ci-' + name if 'ci-' + name in EXPECTED else name
        verify_tests((directory / (name + '.log')).read_text(), EXPECTED.get(suite, []), suite=suite)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    local = sub.add_parser('run')
    local.add_argument('suite', choices=GO_SUITES)
    local.add_argument('--go', default='go')
    local.add_argument('--output', default='-v')
    local.add_argument('--timeout')
    local.add_argument('--packages', default='./src/tests/system')
    remote = sub.add_parser('ci')
    remote.add_argument('suite', choices=('integration', 'load', 'soak', 'qualification', 'fault', 'fuzz', 'benchmark'))
    names = sub.add_parser('names')
    names.add_argument('suite', choices=EXPECTED)
    pattern = sub.add_parser('pattern')
    pattern.add_argument('suite', choices=PATTERNS)
    verify = sub.add_parser('verify')
    verify.add_argument('suite', choices=EXPECTED)
    verify.add_argument('log', type=Path)
    args = parser.parse_args()
    if args.action == 'ci':
        return ci(args.suite)
    if args.action == 'names':
        print('\n'.join(EXPECTED[args.suite]))
        return 0
    if args.action == 'pattern':
        print(PATTERNS[args.suite])
        return 0
    if args.action == 'verify':
        if not EXPECTED[args.suite]:
            parser.error('suite has no required tests')
        verify_tests(args.log.read_text(), EXPECTED[args.suite], suite=args.suite)
        return 0
    cmd = command(args.suite, args.go, args.output, args.timeout, args.packages)
    if args.suite in ('fault', 'recovery'):
        from tools.qualification.fault_results import collect, configure
        env = dict(os.environ)
        env['GO'] = args.go
        configure(env)
        directory = Path(env.get('KAFERRY_REPORT_DIR', 'build-results'))
        directory.mkdir(parents=True, exist_ok=True)
        env['KAFERRY_REPORT_DIR'] = str(directory.resolve())
        report = directory/'fault-results.json'
        report.write_text(json.dumps(dict(schema=1, status='running', oracle_run=env['KAFERRY_ORACLE_RUN']))+'\n')
        code = subprocess.run(cmd, env=env, check=False).returncode
        if not code:
            collect(directory, args.suite, run=env['KAFERRY_ORACLE_RUN'])
        else:
            report.write_text(json.dumps(dict(schema=1, status='failed', oracle_run=env['KAFERRY_ORACLE_RUN']))+'\n')
        return code
    os.execvpe(cmd[0], cmd, os.environ)


if __name__ == '__main__':
    raise SystemExit(main())
