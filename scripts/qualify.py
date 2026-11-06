#!/usr/bin/env python3
"""Plan, execute and report standalone local/CI qualification suites."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time
import threading
import uuid
import venv

from ci import capture, verify_tests

ROOT = Path(__file__).resolve().parents[1]
IMAGES = json.loads((ROOT / 'tests/qualification/matrix.json').read_text())
VERSIONS = list(IMAGES)
RELEASES = json.loads((ROOT / 'tests/qualification/releases.json').read_text())
SUITES = ('broker', 'auth', 'fault', 'load', 'soak', 'capture', 'endurance', 'migration',
          'model', 'resources', 'integration', 'fuzz', 'benchmark', 'smoke', 'cloud')
STOP_GRACE = 5
SOAK_DRAIN_SECONDS = 300
STREAMING_SUITES = ('soak', 'capture', 'endurance')
LAB_LOCK = Path('/tmp/kaferry-qualification.lock')
VENV = ROOT / ('.cache/qualification/venv-' + str(sys.version_info.major) + '.' + str(sys.version_info.minor))


def plan(tier):
    versions = (VERSIONS[0], VERSIONS[-1]) if tier == 'pr' else VERSIONS
    pairs = [(v, v) for v in versions] if tier == 'nightly' else itertools.product(versions, repeat=2)
    cases = [{'id': f'broker-{a}-{b}', 'suite': 'broker', 'source': a, 'destination': b} for a, b in pairs]
    cases += [{'id': f'auth-{v}-{v}', 'suite': 'auth', 'source': v, 'destination': v}
              for v in (VERSIONS if tier == 'full' else [VERSIONS[-1]])]
    cases.append({'id': f'capture-{VERSIONS[0]}-{VERSIONS[0]}', 'suite': 'capture',
                  'source': VERSIONS[0], 'destination': VERSIONS[0]})
    return cases


def duration(value):
    try:
        seconds = int(value[:-1]) * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[value[-1]]
    except (ValueError, KeyError, IndexError):
        raise argparse.ArgumentTypeError('use a positive integer followed by s, m, h or d')
    if not 60 <= seconds <= 7 * 86400:
        raise argparse.ArgumentTypeError('duration must be between 60s and 7d')
    return seconds


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def logged(command, directory, label, env, timeout=1800):
    print('+ ' + ' '.join(map(str, command)), flush=True)
    with (directory / f'{label}.log').open('w') as log:
        process = subprocess.Popen(list(map(str, command)), cwd=ROOT, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, errors='replace', start_new_session=True)
        expired, finished = threading.Event(), threading.Event()
        def signal_group(signum):
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        def terminate():
            # An exited parent can leave a descendant holding the output pipe.
            expired.set()
            signal_group(signal.SIGTERM)
            if not finished.wait(STOP_GRACE):
                signal_group(signal.SIGKILL)
        timer = threading.Timer(timeout, terminate)
        timer.daemon = True
        timer.start()
        try:
            for line in process.stdout:
                print(line, end='', flush=True)
                log.write(line)
                log.flush()
            code = process.wait()
        finally:
            timer.cancel()
            signal_group(signal.SIGTERM)
            try:
                process.wait(timeout=STOP_GRACE)
            except subprocess.TimeoutExpired:
                signal_group(signal.SIGKILL)
                process.wait()
            signal_group(signal.SIGKILL)
            finished.set()
            process.stdout.close()
    if expired.is_set():
        raise RuntimeError(f'{label} exceeded {timeout}s; inspect {directory / (label + ".log")}')
    if code:
        raise RuntimeError(f'{label} exited {code}; inspect {directory / (label + ".log")}')


@contextmanager
def lab_lock():
    with LAB_LOCK.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('another qualification suite owns this host’s disposable broker ports')
        yield


def suite_command(args, env):
    prefix = [env.get('GO', 'go'), 'test', '-count=1', '-json']
    if args.suite in ('broker', 'cloud'):
        python = str(VENV / 'bin/python') if VENV.exists() else sys.executable
        return [python, 'scripts/broker_probe.py', '--binary', args.binary, '--output', str(args.output),
                '--source', env['KAFKA_TEST_BROKERS'], '--destination', env['KAFKA_TEST_DEST_BROKERS']] + (
                    ['--config', str(args.config.resolve())] if args.config else []), []
    if args.suite in STREAMING_SUITES:
        # Clear inherited workload selectors so the receipt describes this run.
        env.pop('KAFERRY_SOAK_PROFILE', None)
        env.update(KAFERRY_SOAK_DURATION=f'{args.duration}s', KAFERRY_SOAK_RATE=str(args.rate),
                   KAFERRY_SOAK_MODE='archive' if args.suite == 'capture' else 'kafka')
        if args.suite == 'endurance':
            if args.rate != 200:
                raise ValueError('endurance uses a fixed --rate 200')
            env.update(KAFERRY_SOAK_PROFILE='application', KAFERRY_ARCHIVE_PAYLOAD='random')
        if args.suite in ('soak', 'endurance'):
            env['KAFERRY_SOAK_DEST_STALL_CONTAINER'] = ('kaferry-endurance-kafka-dr-1' if args.suite == 'endurance' else 'kaferry-compat-kafka-dr-1')
        else:
            env.pop('KAFERRY_SOAK_DEST_STALL_CONTAINER', None)
        return prefix + [f'-timeout={args.duration + SOAK_DRAIN_SECONDS + 300}s', '-tags=integration,load', '-run',
                         '^TestStreamingSoak', './tests/compat'], ['TestStreamingSoakFidelityCommitLagMemoryAndShutdown']
    if args.suite == 'auth':
        env['KAFERRY_AUTH_IMAGE'] = IMAGES[args.source]
        return ['sh', 'tests/qualification/run.sh'], [
            'TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight', 'TestSaslTlsScramMechanisms/SCRAM-SHA-256',
            'TestSaslTlsScramMechanisms/SCRAM-SHA-512', 'TestSaslTlsExternalAuthentication/OAUTHBEARER',
            'TestSaslTlsExternalAuthentication/GSSAPI', 'TestSaslTlsOAuthTokenFile',
            'TestSaslTlsOAuthRenewalAndKeyRotation', 'TestMutualTlsCopy', 'TestServerTlsCopy',
            'TestReplicatedClusterLeaderFailurePreservesCopy']
    if args.suite == 'migration':
        if not args.previous_binary or not args.previous_binary.is_file():
            raise ValueError('migration requires --previous-binary naming a supported release executable')
        env['KAFERRY_PREVIOUS_BINARY'] = str(args.previous_binary.resolve())
        env['KAFERRY_PREVIOUS_STATE'] = args.state_contract
        name = 'TestPreviousBinaryRejectsUnsupportedCaptureState' if args.state_contract == 'reject' else 'TestPreviousBinaryProgressAndArchiveCompatibility'
        return prefix + ['-timeout=10m', '-tags=integration,migration', '-run', '^' + name + '$', './tests/compat'], [name]
    if args.suite == 'integration':
        return prefix + ['-race', '-timeout=20m', '-tags=integration', './tests/compat'], ['TestAllFileDirectionsAndResume', 'TestTopicToTopicSlice']
    if args.suite == 'resources':
        return prefix + ['-race', './internal/copy', './internal/archive/capture', './internal/checkpoint', './internal/archiveio'], [
            'TestCheckpointReplacementFailuresPreserveAtomicPublication',
            'TestEveryCaptureIOCutPreservesAcknowledgedRecords', 'TestEveryRecoveryIOCutCanBeRetriedByANewOwner']
    if args.suite == 'model':
        return prefix + ['-race', '-run', '^TestProgressLifecycleModel$', './internal/copy'], ['TestProgressLifecycleModel']
    if args.suite in ('fuzz', 'benchmark', 'smoke'):
        return ['make', args.suite], []
    if args.suite == 'load':
        return prefix + ['-timeout=20m', '-tags=integration,load', '-run', '^TestLoad',
                         './tests/compat'], ['TestLoadBoundedKafkaCopy/default']
    return prefix + ['-timeout=10m', '-tags=integration,fault', '-run',
                     '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume', './tests/compat'], [
        'TestBlockingCommitAcrossBrokerOutage', 'TestRebalanceChurnPreservesConfirmedProgressUnderFailFastCommits',
        'TestCrashResumeReconcilesRetainedRecords']


def endurance_capacity(seconds, output, compose, env):
    # No compression credit. Require the combined allowance on each filesystem
    # because Docker may put both writable container layers on the same disk.
    bursts = 1 + (seconds - 10) // 60
    records = seconds * 200 + bursts * 400
    broker_bytes = records * (5120 + 256) * 3 + 4 * 1024**3
    evidence_bytes = max(1024**3, seconds * 8192)
    free = {}
    for service in ('kafka', 'kafka-dr'):
        result = subprocess.check_output(compose + ['exec', '-T', service, 'df', '-Pk',
            '/tmp/kaferry-logs'], env=env, cwd=ROOT, text=True, timeout=15)
        free[service] = int(result.splitlines()[-1].split()[3]) * 1024
    evidence_free = shutil.disk_usage(output).free
    capacity = dict(records=records, required_broker_bytes=broker_bytes,
        broker_free_bytes=free, required_evidence_bytes=evidence_bytes, evidence_free_bytes=evidence_free)
    write_json(output / 'capacity.json', capacity)
    if min(free.values()) < broker_bytes or evidence_free < evidence_bytes:
        raise ValueError('insufficient endurance disk allowance; inspect capacity.json')
    return capacity


def run_suite(args):
    if args.suite == 'auth':
        args.destination = args.source
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {'schema': 1, 'id': f'{args.suite}-{args.source}-{args.destination}', 'suite': args.suite,
               'source_version': args.source, 'destination_version': args.destination,
               'source_commit': capture(['git', 'rev-parse', 'HEAD']),
               'working_tree_dirty': bool(capture(['git', 'status', '--porcelain', '--untracked-files=no'])),
               'platform': platform.platform(), 'machine': platform.machine(), 'python': platform.python_version(),
               'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'running',
               'go': capture([os.environ.get('GO', 'go'), 'version']),
               'execution': 'checkout component tests' if args.suite in ('model', 'resources', 'fuzz', 'benchmark') else 'CLI scenarios with checkout fixture tests',
               'requested_seconds': args.duration if args.suite in STREAMING_SUITES else None}
    if args.suite in STREAMING_SUITES:
        receipt['drain_seconds'] = SOAK_DRAIN_SECONDS
    if args.suite == 'endurance':
        receipt['workload'] = dict(profile='application-24x32-v1', processes=1, routing='KEY',
            source_partitions=24, destination_partitions=32, replication_factor=1,
            rate=200, payload='random', duration_claim='endurance' if args.duration >= 86400 else 'rehearsal')
    if args.suite == 'cloud':
        receipt.update(source_version='external', destination_version='external', id='cloud-external-external')
    started = time.monotonic()
    project = 'kaferry-endurance' if args.suite == 'endurance' else 'kaferry-compat'
    source_port, destination_port = (39192, 40192) if args.suite == 'endurance' else (39092, 40092)
    env = dict(os.environ, KAFERRY_SOURCE_PORT=str(source_port), KAFERRY_DESTINATION_PORT=str(destination_port), KAFERRY_REPORT_DIR=str(args.output), KAFERRY_TEST_OUTPUT='-json',
               KAFKA_TEST_BROKERS=f'127.0.0.1:{source_port}', KAFKA_TEST_DEST_BROKERS=f'127.0.0.1:{destination_port}',
               KAFKA_FAULT_CONTAINER='kaferry-compat-kafka-1', CGO_ENABLED='1')
    write_json(args.output / 'manifest.json', receipt)
    compose = ['docker', 'compose', '-p', project, '--profile', 'cross-cluster']
    fixtures = args.suite in ('broker', 'fault', 'load', 'soak', 'capture', 'endurance', 'integration') or (args.suite == 'migration' and args.state_contract == 'resume')
    try:
        args.binary = str(Path(args.binary).resolve())
        if not Path(args.binary).is_file():
            raise ValueError('binary is missing; run make build or pass --binary')
        receipt['binary_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
        receipt['binary_version'] = capture([args.binary, '--version'])
        if args.suite == 'migration' and args.previous_binary:
            receipt['previous_binary_sha256'] = hashlib.sha256(args.previous_binary.read_bytes()).hexdigest()
            receipt['state_contract'] = args.state_contract
        env.update(KAFERRY_TEST_BINARY=args.binary, KAFERRY_EXPECT_BINARY_SHA256=receipt['binary_sha256'],
                   KAFERRY_SOURCE_IMAGE=IMAGES[args.source], KAFERRY_DESTINATION_IMAGE=IMAGES[args.destination])
        receipt['requested_images'] = [env['KAFERRY_SOURCE_IMAGE'], env['KAFERRY_DESTINATION_IMAGE']] if fixtures else []
        if args.config and args.suite != 'cloud':
            raise ValueError('--config is only accepted by the external cloud suite')
        if args.suite == 'cloud' and not args.config:
            raise ValueError('cloud requires --config; this command never provisions a service')
        command, expected = suite_command(args, env)
        receipt.update(command=list(map(str, command)), expected_tests=expected)
        if fixtures:
            receipt.update(fixture_project=project, fixture_brokers=[env['KAFKA_TEST_BROKERS'], env['KAFKA_TEST_DEST_BROKERS']])
        write_json(args.output / 'manifest.json', receipt)
        with lab_lock():
            try:
                if fixtures:
                    logged(compose + ['up', '-d', '--wait', '--wait-timeout', '120'], args.output, 'brokers-up', env, timeout=180)
                    receipt['broker_images'] = capture(compose + ['images', '--format', 'json'])
                if args.suite == 'endurance':
                    receipt['capacity'] = endurance_capacity(args.duration, args.output, compose, env)
                    write_json(args.output / 'manifest.json', receipt)
                logged(command, args.output, 'suite', env, timeout=args.duration + SOAK_DRAIN_SECONDS + 300 if args.suite in STREAMING_SUITES else 1800)
                if expected:
                    verify_tests((args.output / 'suite.log').read_text(), expected)
                if args.suite == 'benchmark':
                    from quality import benchmarks
                    measured = benchmarks((args.output / 'suite.log').read_text())
                    for name in ('BenchmarkEncode', 'BenchmarkDecode', 'BenchmarkReplayOverlap', 'BenchmarkCheckpointEncoding', 'BenchmarkCheckpointSaving'):
                        if not any(key.startswith(name) for key in measured):
                            raise ValueError('required benchmark did not execute: ' + name)
                if args.suite in ('broker', 'cloud'):
                    probe = json.loads((args.output / 'reconciliation.json').read_text())
                    if (probe['status'] != 'passed' or probe['binary_sha256'] != receipt['binary_sha256'] or
                            {c['name'] for c in probe['cases']} != {route + '/' + phase for route in ('topic-preserve', 'topic-key', 'capture', 'clone', 'replay') for phase in ('partial', 'resume', 'completed')} or
                            any(c['verified'] <= 0 or c['verified'] != c['expected'] or
                                c['missing'] or c['duplicates'] or c['corrupted'] for c in probe['cases'])):
                        raise ValueError('independent reconciliation did not pass for the tested binary')
                    receipt['reconciliation'] = probe
                if args.suite == 'auth':
                    receipt['broker_images'] = (args.output / 'qualification.broker-images.json').read_text()
            except (Exception, KeyboardInterrupt) as error:
                receipt['suite_error'] = str(error) or 'interrupted'
                raise
            finally:
                if fixtures:
                    errors = []
                    try:
                        with (args.output / 'brokers.log').open('w') as log:
                            subprocess.run(compose + ['logs', '--no-color'], cwd=ROOT, env=env,
                                           stdout=log, stderr=subprocess.STDOUT, timeout=30, check=False)
                    except (OSError, subprocess.SubprocessError) as error:
                        errors.append({'phase': 'logs', 'error': str(error)})
                    finally:
                        try:
                            logged(compose + ['down', '-v', '--remove-orphans'], args.output, 'brokers-down', env, timeout=60)
                        except (OSError, RuntimeError) as error:
                            errors.append({'phase': 'teardown', 'error': str(error)})
                    if errors:
                        receipt['cleanup_errors'] = errors
                        if 'suite_error' not in receipt:
                            raise RuntimeError('fixture diagnostics or teardown failed; inspect manifest.json')
        receipt['status'] = 'passed'
    except (Exception, KeyboardInterrupt) as error:
        receipt.update(status='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed', error=str(error) or 'interrupted')
        raise
    finally:
        receipt.update(elapsed_seconds=time.monotonic() - started, finished_at=datetime.now(timezone.utc).isoformat())
        write_json(args.output / 'manifest.json', receipt)
        if args.suite == 'endurance':
            from endurance_results import prepare
            try:
                prepare(args.output, args.output / 'publication')
            except (OSError, ValueError) as error:
                if receipt['status'] == 'passed':
                    raise
                print(f'Endurance report unavailable: {error}', file=sys.stderr)


def released_binaries(args):
    import tarfile
    system = {'Darwin': 'darwin', 'Linux': 'linux'}.get(platform.system())
    machine = {'arm64': 'arm64', 'aarch64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())
    if not system or not machine:
        raise ValueError('release binaries support Linux/macOS AMD64/ARM64')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    download = ROOT / '.cache/qualification/releases' / uuid.uuid4().hex
    for version, contract in RELEASES.items():
        directory = download / version
        directory.mkdir(parents=True)
        filename = f'kaferry-{version}-{system}-{machine}.tar.gz'
        subprocess.run(['gh', 'release', 'download', version, '--repo', 'pagrus7/kaferry',
                        '--pattern', filename, '--pattern', 'SHA256SUMS', '--dir', str(directory)], check=True)
        checksums = {row.split()[1].lstrip('*'): row.split()[0]
                     for row in (directory / 'SHA256SUMS').read_text().splitlines() if row.strip()}
        archive = directory / filename
        if checksums.get(filename) != hashlib.sha256(archive.read_bytes()).hexdigest():
            raise ValueError('released archive checksum mismatch: ' + version)
        subprocess.run(['gh', 'attestation', 'verify', str(archive), '--repo', 'pagrus7/kaferry'], check=True)
        with tarfile.open(archive, 'r:gz') as package:
            binary = directory / 'kaferry'
            member = package.extractfile('kaferry')
            if member is None:
                raise ValueError('released archive lacks kaferry')
            binary.write_bytes(member.read())
            binary.chmod(0o700)
        case = argparse.Namespace(**dict(vars(args), suite='migration', source=VERSIONS[0],
            destination=VERSIONS[0], previous_binary=binary, state_contract=contract, output=args.output / version))
        run_suite(case)


def valid_receipts(receipts, tier, commit=None, digest=None):
    expected = {case['id'] for case in plan(tier)}
    ids = [r.get('id') for r in receipts]
    identities = {(r.get('source_commit'), r.get('binary_sha256')) for r in receipts}
    return (set(ids) == expected and len(ids) == len(expected) and len(identities) == 1 and
            all(r.get('status') == 'passed' and r.get('binary_sha256') and r.get('source_commit') and
                (commit is None or r['source_commit'] == commit and r.get('working_tree_dirty') is False) and
                (digest is None or r['binary_sha256'] == digest) for r in receipts))


def verify_release(path, archive, commit):
    import tarfile
    with tarfile.open(archive, 'r:gz') as package:
        member = package.extractfile('kaferry')
        if member is None:
            raise ValueError('release archive lacks kaferry')
        digest = hashlib.sha256(member.read()).hexdigest()
    document = json.loads(path.read_text())
    if not valid_receipts(document['cases'], 'full', commit, digest):
        raise ValueError('full compatibility evidence does not match the release source and binary')
    print('Full compatibility evidence matches the release binary ' + digest)


def report(directory, tier):
    expected = {case['id'] for case in plan(tier)}
    receipts = [json.loads(path.read_text()) for path in sorted(directory.glob('*/manifest.json'))]
    by_id = {}
    for receipt in receipts:
        if receipt['id'] in by_id:
            raise ValueError('duplicate qualification receipt: ' + receipt['id'])
        by_id[receipt['id']] = receipt
    rows = ['# Qualification matrix', '', '| Case | Status | Seconds |', '| --- | --- | ---: |']
    for name in sorted(expected):
        result = by_id.get(name, {})
        rows.append(f"| {name} | {result.get('status', 'missing')} | {result.get('elapsed_seconds', 0):.1f} |")
    passed = valid_receipts(receipts, tier)
    write_json(directory / 'qualification.json', {'schema': 1, 'tier': tier,
        'status': 'passed' if passed else 'incomplete', 'expected': sorted(expected), 'cases': receipts})
    (directory / 'qualification.md').write_text('\n'.join(rows) + '\n')
    print('\n'.join(rows))
    return 0 if passed else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    setup = sub.add_parser('setup', help='install independent checker into a checkout-local virtual environment')
    setup.add_argument('--cloud', action='store_true', help='also install the AWS IAM signer')
    planning = sub.add_parser('plan', help='print the CI matrix without running anything')
    planning.add_argument('--tier', choices=('pr', 'nightly', 'full'), default='pr')
    for name in ('run', 'matrix', 'releases'):
        running = sub.add_parser(name, help='run one suite' if name == 'run' else 'run every planned case sequentially')
        if name == 'run':
            running.add_argument('suite', choices=SUITES)
        elif name == 'matrix':
            running.add_argument('--tier', choices=('pr', 'nightly', 'full'), default='pr')
        running.add_argument('--source', choices=VERSIONS, default=VERSIONS[0])
        running.add_argument('--destination', choices=VERSIONS, default=VERSIONS[0])
        running.add_argument('--binary', default='bin/kaferry')
        running.add_argument('--previous-binary', type=Path)
        running.add_argument('--state-contract', choices=('resume', 'reject'), default='resume')
        running.add_argument('--config', type=Path)
        running.add_argument('--duration', type=duration)
        running.add_argument('--rate', type=int, choices=range(1, 10001), metavar='1..10000')
        running.add_argument('--output', type=Path, default=Path('build-results/qualification') /
            (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]))
    verifying = sub.add_parser('verify-release', help='validate full matrix evidence against an exact release archive')
    verifying.add_argument('report', type=Path)
    verifying.add_argument('archive', type=Path)
    verifying.add_argument('commit')
    reporting = sub.add_parser('report', help='require all planned cases with matching source and binary identities')
    reporting.add_argument('directory', type=Path)
    reporting.add_argument('--tier', choices=('pr', 'nightly', 'full'), default='pr')
    args = parser.parse_args(argv)
    if args.action in ('run', 'matrix', 'releases'):
        endurance = getattr(args, 'suite', None) == 'endurance'
        args.duration = args.duration if args.duration is not None else (86400 if endurance else 120)
        args.rate = args.rate if args.rate is not None else (200 if endurance else 50)
    os.chdir(ROOT)
    if args.action == 'setup':
        if sys.version_info < (3, 11):
            raise ValueError('qualification setup requires Python 3.11+; select that interpreter for setup and runs')
        venv.EnvBuilder(with_pip=True).create(VENV)
        requirements = 'requirements-cloud.txt' if args.cloud else 'requirements.txt'
        subprocess.run([str(VENV / 'bin/python'), '-m', 'pip', 'install', '--no-cache-dir', '-r',
                        str(ROOT / 'tests/qualification' / requirements)], check=True)
        return 0
    if args.action == 'plan':
        print(json.dumps({'include': plan(args.tier)}))
        return 0
    if args.action == 'verify-release':
        verify_release(args.report, args.archive, args.commit)
        return 0
    if args.action == 'report':
        return report(args.directory, args.tier)
    if args.action == 'releases':
        released_binaries(args)
        return 0
    if args.action == 'matrix':
        directory = args.output.resolve()
        directory.mkdir(parents=True, exist_ok=False)
        for case in plan(args.tier):
            case_args = argparse.Namespace(**dict(vars(args), **case, output=directory / case['id']))
            try:
                run_suite(case_args)
            except (OSError, ValueError, RuntimeError) as error:
                print(str(error), file=sys.stderr)
        return report(directory, args.tier)
    run_suite(args)
    return 0


if __name__ == '__main__':
    def stop(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, stop)
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, KeyboardInterrupt) as error:
        print(str(error) or 'qualification interrupted', file=sys.stderr)
        sys.exit(1)
