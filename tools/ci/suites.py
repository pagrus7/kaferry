"""Shared Go suite selection and required execution evidence."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from tools.ci.ci import run, verify_tests
from tools.ci.quality import benchmarks
from tools.ci.replicated_suites import (
    GO_SUITES as REPLICATED_GO_SUITES, EXPECTED as REPLICATED_EXPECTED,
    REPLICATED_CASES, fault_timeout_catalog, replicated_execution_seconds,
)


def replicated_helpers(root=None, platform=sys.platform):
    root = root or Path(__file__).resolve().parents[2]
    names = []
    for directory in ('src/tests/system', 'src/tests/system/faults'):
        for path in (root/directory).glob('*_test.go'):
            if path.name.endswith('_darwin_test.go') and platform != 'darwin':
                continue
            source = path.read_text()
            tag = re.search(r'^//go:build (.+)$', source, re.MULTILINE)
            if tag and re.search(r'\bharness\b', tag[1]):
                if '!' in tag[1] or '||' in tag[1]:
                    raise ValueError(f'{path}: harness build tags must use positive conjunctions')
                names.extend(re.findall(r'^func (Test\w+)\(', source, re.MULTILINE))
    if not names:
        raise ValueError('harness build tag selected no tests')
    return sorted(names)


# Fixture ownership, binary selection and workload environment stay with callers.
GO_SUITES = {
    'integration': ('integration', None, True),
    'load': ('integration,load', '^TestLoad', False),
    'fault': ('integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', False),
    'recovery': ('integration,fault', '^TestRecoveryBlockedOutput$', False),
    **REPLICATED_GO_SUITES,
}
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
                    'TestArchiveCaptureCrashRecoversAcknowledgedData',
                    'TestHistoricalFixtureTopicRetention/historical_default',
                    'TestHistoricalFixtureTopicRetention/finite_override',
                    'TestRunningSourceOffsetOutOfRangeFailsWithoutSavingProgress',
                    'TestExpiredSourceProgressRestartReportsGapAndCopiesRetainedRecords'],
    'load': ['TestLoadBoundedKafkaCopy/default'] + ['TestLoadBoundedKafkaCopy/' + name for name in LOAD_PROFILES],
    'fault': ['TestBlockingCommitAcrossBrokerOutage', 'TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits',
              'TestCrashResumeReconcilesRetainedRecords', 'TestRecoveryBlockedOutput'],
    'recovery': ['TestRecoveryBlockedOutput'],
    **REPLICATED_EXPECTED,
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
    'fuzz': ['FuzzProgressLifecycle', 'FuzzParse', 'FuzzContainer', 'FuzzState', 'FuzzCheckpoint'],
    'replicated-helpers': replicated_helpers(),
}
EXPECTED['kafka'] = EXPECTED['integration'] + ['TestCLIInterruptCommitsAndResumeDoesNotRecopy']
EXPECTED['ci-integration'] = EXPECTED['kafka'] + EXPECTED['quickstart']
EXPECTED['ci-load'] = EXPECTED['load'] + [OBSERVABILITY_TEST]
EXPECTED['capture'] = EXPECTED['endurance'] = EXPECTED['soak']
PATTERNS = {
    'smoke': '^(TestArchiveTutorial(Cancellation)?|TestLoadObservability)$/^round-0$/^disabled$',
    'replicated-helpers': '^(' + '|'.join(EXPECTED['replicated-helpers']) + ')$',
}


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


def verify_benchmarks(log):
    required = {
        'github.com/pagrus7/kaferry/src/internal/archive/avro': ('BenchmarkEncode', 'BenchmarkDecode'),
        'github.com/pagrus7/kaferry/src/internal/archive/replay': (
            'BenchmarkReplayOverlap', 'BenchmarkCheckpointEncoding', 'BenchmarkCheckpointSaving'),
    }
    passed = set()
    output = {package: [] for package in required}
    for line in log.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get('Action') == 'pass' and not event.get('Test'):
            passed.add(event.get('Package'))
        if isinstance(event, dict) and event.get('Action') == 'output' and event.get('Package') in output:
            output[event['Package']].append(event.get('Output', ''))
    missing = [package for package in required if package not in passed]
    for package, names in required.items():
        measured = benchmarks(''.join(output[package]))
        missing += [name for name in names if not any(row == name or row.startswith(name+'/') for row in measured)]
    if missing:
        raise ValueError('Required benchmarks did not complete: ' + ', '.join(missing))


def ci(name):
    if name == 'integration':
        cmd = ['env', 'CGO_ENABLED=1', 'KAFERRY_QUICKSTART=1'] + command(name)
    else:
        cmd = ['make', 'soak' if name in ('capture', 'endurance') else name, 'KAFERRY_TEST_OUTPUT=-json']
        if name in ('soak', 'capture', 'endurance'):
            cmd.append('SOAK_TEST_TIMEOUT=40m')
        if name == 'capture':
            cmd = ['env', 'KAFERRY_SOAK_MODE=archive', 'KAFERRY_SOAK_PROFILE=',
                   'KAFERRY_SOAK_TOPOLOGY=single', 'KAFERRY_SOAK_DEST_STALL_CONTAINER='] + cmd
        if name == 'endurance':
            # Each matrix job owns its own plain fixture; retain its ownership identity.
            cmd = ['env', 'KAFERRY_SOAK_MODE=kafka', 'KAFERRY_SOAK_PROFILE=application',
                   'KAFERRY_SOAK_TOPOLOGY=single', 'KAFERRY_SOAK_RATE=200',
                   'KAFERRY_ARCHIVE_PAYLOAD=random', 'KAFERRY_FIXTURE_PROJECT=kaferry-compat'] + cmd
    directory = Path('build-results')
    code = run(name, cmd, directory)
    if code == 0:
        suite = 'ci-' + name if 'ci-' + name in EXPECTED else name
        log = (directory / (name + '.log')).read_text()
        verify_tests(log, EXPECTED.get(suite, []), suite=suite)
        if name == 'benchmark':
            verify_benchmarks(log)
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
    remote.add_argument('suite', choices=('integration', 'load', 'soak', 'capture', 'endurance', 'qualification', 'fault', 'fuzz', 'benchmark'))
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
