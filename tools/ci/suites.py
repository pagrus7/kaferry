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
    'fault-replicated': ('integration,fault,load', '^TestReplicatedZombieWorker$', False),
}
RECOVERY_CASES = {'recovery/'+name for name in
                  ('kill', 'signal', 'delivery-timeout', 'source-outage-shutdown', 'ownership-loss')}
FAULT_CASES = RECOVERY_CASES | {'fault/crash', 'fault/churn', 'fault/blocking-commit'}
LOAD_PROFILES = ('near-limit-v1', 'hot-keys-v1', 'headers-v1', 'tombstones-v1',
                 'source-none-v1', 'source-gzip-v1', 'source-snappy-v1', 'source-lz4-v1', 'source-zstd-v1')
EXPECTED = {
    'integration': ['TestAllFileDirectionsAndResume', 'TestTopicToTopicSlice',
                    'TestHistoricalFixtureTopicRetention/historical_default',
                    'TestHistoricalFixtureTopicRetention/finite_override',
                    'TestRunningSourceOffsetOutOfRangeFailsWithoutSavingProgress',
                    'TestExpiredSourceProgressRestartReportsGapAndCopiesRetainedRecords'],
    'load': ['TestLoadBoundedKafkaCopy/default'] + ['TestLoadBoundedKafkaCopy/' + name for name in LOAD_PROFILES],
    'soak': ['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'],
    'fault': ['TestBlockingCommitAcrossBrokerOutage', 'TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits',
              'TestCrashResumeReconcilesRetainedRecords', 'TestRecoveryBlockedOutput'],
    'recovery': ['TestRecoveryBlockedOutput'],
    'fault-replicated': ['TestReplicatedZombieWorker'],
    'qualification': [
        'TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight', 'TestSaslTlsScramMechanisms/SCRAM-SHA-256',
        'TestSaslTlsScramMechanisms/SCRAM-SHA-512', 'TestSaslTlsExternalAuthentication/OAUTHBEARER',
        'TestSaslTlsExternalAuthentication/GSSAPI', 'TestSaslTlsOAuthTokenFile',
        'TestSaslTlsOAuthRenewalAndKeyRotation', 'TestMutualTlsCopy', 'TestServerTlsCopy',
        'TestReplicatedClusterLeaderFailurePreservesCopy'],
}
CI_EXTRA = {
    'integration': ['TestCLIInterruptCommitsAndResumeDoesNotRecopy', 'TestKafkaQuickStart'],
    'load': ['TestLoadObservability/round-0/disabled'],
}


def fault_timeout_catalog():
    catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
    standard = {'session.timeout.ms', 'delivery_timeout', 'shutdown_timeout'}
    ownership = standard | {'socket.timeout.ms', 'progress_save_timeout'}
    if not catalog or any(not budgets or set(budgets)-(ownership if id == 'ownership/zombie-worker' else standard) or
                          any(type(ms) is not int or ms <= 0 for ms in budgets.values())
                          for id, budgets in catalog.items()):
        raise ValueError('fault observation timeout catalog must contain positive milliseconds')
    return catalog


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
        verify_tests((directory / (name + '.log')).read_text(), EXPECTED.get(name, []) + CI_EXTRA.get(name, []))
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
    args = parser.parse_args()
    if args.action == 'ci':
        return ci(args.suite)
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
