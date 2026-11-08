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
}
RECOVERY_CASES = {'recovery/'+name for name in
                  ('kill', 'signal', 'delivery-timeout', 'source-outage-shutdown', 'ownership-loss')}
FAULT_CASES = RECOVERY_CASES | {'fault/crash', 'fault/churn', 'fault/blocking-commit'}
FAULT_SHARDS = {'fault-processes': FAULT_CASES-RECOVERY_CASES, 'fault-recovery': RECOVERY_CASES}
GO_SUITES['fault-processes'] = ('integration,fault', GO_SUITES['fault'][1].rsplit('|', 1)[0], False)
GO_SUITES['fault-recovery'] = GO_SUITES['recovery']
LOAD_PROFILES = ('near-limit-v1', 'hot-keys-v1', 'headers-v1', 'tombstones-v1',
                 'source-none-v1', 'source-gzip-v1', 'source-snappy-v1', 'source-lz4-v1', 'source-zstd-v1')
EXPECTED = {
    'integration': ['TestAllFileDirectionsAndResume', 'TestTopicToTopicSlice',
                    'TestRunningSourceOffsetOutOfRangeFailsWithoutSavingProgress',
                    'TestExpiredSourceProgressRestartReportsGapAndCopiesRetainedRecords'],
    'load': ['TestLoadBoundedKafkaCopy/default'] + ['TestLoadBoundedKafkaCopy/' + name for name in LOAD_PROFILES],
    'soak': ['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'],
    'fault': ['TestBlockingCommitAcrossBrokerOutage', 'TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits',
              'TestCrashResumeReconcilesRetainedRecords', 'TestRecoveryBlockedOutput'],
    'recovery': ['TestRecoveryBlockedOutput'],
    'qualification': [
        'TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight', 'TestSaslTlsScramMechanisms/SCRAM-SHA-256',
        'TestSaslTlsScramMechanisms/SCRAM-SHA-512', 'TestSaslTlsExternalAuthentication/OAUTHBEARER',
        'TestSaslTlsExternalAuthentication/GSSAPI', 'TestSaslTlsOAuthTokenFile',
        'TestSaslTlsOAuthRenewalAndKeyRotation', 'TestMutualTlsCopy', 'TestServerTlsCopy',
        'TestReplicatedClusterLeaderFailurePreservesCopy'],
}
EXPECTED['fault-processes'] = EXPECTED['fault'][:3]
EXPECTED['fault-recovery'] = EXPECTED['recovery']
CI_EXTRA = {
    'integration': ['TestCLIInterruptCommitsAndResumeDoesNotRecopy', 'TestKafkaQuickStart'],
    'load': ['TestLoadObservability/round-0/disabled'],
}


def fault_timeout_catalog(tier=None):
    catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
    tier = tier or os.environ.get('KAFERRY_FAULT_TIER', 'full')
    if tier not in catalog:
        raise ValueError('unknown fault timeout tier: ' + tier)
    catalog = catalog[tier]
    if not catalog or any(not budgets or set(budgets)-{'session.timeout.ms', 'delivery_timeout', 'shutdown_timeout'} or
                          any(type(ms) is not int or ms <= 0 for ms in budgets.values())
                          for budgets in catalog.values()):
        raise ValueError('fault observation timeout catalog must contain positive milliseconds')
    return catalog


def fault_execution_seconds(cases=FAULT_CASES):
    catalog = fault_timeout_catalog()
    if not cases <= catalog.keys():
        raise ValueError('fault timeout catalog omitted a selected case')
    # Each case observes the full horizon for its selected fixture budgets.
    # Churn's windows overlap, so its final horizon is counted once.
    milliseconds = sum(sum(catalog[id].values()) for id in cases)
    return 600 + (milliseconds+999)//1000


def command(name, go='go', output='-json', timeout=None, packages='./src/tests/system'):
    tags, pattern, race = GO_SUITES[name]
    if name == 'soak' and packages == './src/tests/system':
        packages = './src/tests/compatibility'
    if (name == 'fault' or name in FAULT_SHARDS) and timeout is None:
        timeout = f'{fault_execution_seconds(FAULT_SHARDS.get(name, FAULT_CASES))}s'
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
    elif name in FAULT_SHARDS:
        cmd = ['python3', '-m', 'tools.ci.suites', 'run', name, '--output=-json']
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
    remote.add_argument('suite', choices=('integration', 'load', 'soak', 'qualification', 'fault', 'fault-processes', 'fault-recovery', 'fuzz', 'benchmark'))
    args = parser.parse_args()
    if args.action == 'ci':
        return ci(args.suite)
    cmd = command(args.suite, args.go, args.output, args.timeout, args.packages)
    if args.suite in ('fault', 'recovery') or args.suite in FAULT_SHARDS:
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
