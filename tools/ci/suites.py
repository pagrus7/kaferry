"""Shared Go suite selection and required execution evidence."""

import argparse
import os
from pathlib import Path

from tools.ci.ci import run, verify_tests


# Fixture ownership, binary selection and workload environment stay with callers.
GO_SUITES = {
    'integration': ('integration', None, True),
    'load': ('integration,load', '^TestLoad', False),
    'soak': ('integration,load', '^TestStreamingSoak', False),
    'fault': ('integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', False),
    'recovery': ('integration,fault', '^TestRecoveryBlockedOutput$', False),
}
EXPECTED = {
    'integration': ['TestAllFileDirectionsAndResume', 'TestTopicToTopicSlice'],
    'load': ['TestLoadBoundedKafkaCopy/default'],
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
CI_EXTRA = {
    'integration': ['TestCLIInterruptCommitsAndResumeDoesNotRecopy', 'TestKafkaQuickStart'],
    'load': ['TestLoadObservability/round-0/disabled'],
}


def command(name, go='go', output='-json', timeout=None, packages='./src/tests/system'):
    tags, pattern, race = GO_SUITES[name]
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
    os.execvpe(cmd[0], cmd, os.environ)


if __name__ == '__main__':
    raise SystemExit(main())
