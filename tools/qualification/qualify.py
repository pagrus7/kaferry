#!/usr/bin/env python3
"""Plan, execute and report standalone local/CI qualification suites."""

import argparse
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from dataclasses import dataclass
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
import threading
import uuid
import venv

from tools.ci.ci import capture, verify_tests
from tools.ci.suites import command as go_suite, EXPECTED, REPLICATED_CASES, replicated_execution_seconds
from tools.qualification import network

ROOT = Path(__file__).resolve().parents[2]
IMAGES = json.loads((ROOT / 'src/tests/qualification/matrix.json').read_text())
VERSIONS = list(IMAGES)
RELEASES = json.loads((ROOT / 'src/tests/qualification/releases.json').read_text())
SUITES = ('broker', 'replicated', 'fault-replicated', 'auth', 'fault', 'load', 'soak', 'capture', 'endurance', 'recovery', 'migration',
          'model', 'resources', 'integration', 'fuzz', 'benchmark', 'smoke', 'cloud')
STOP_GRACE = 5
SOAK_DRAIN_SECONDS = 10
STREAMING_SUITES = ('soak', 'capture', 'endurance')
TWO_BROKER_SUITES = ('broker', 'fault', 'load', 'soak', 'capture', 'endurance', 'recovery', 'integration')
LAB_LOCK = Path('/tmp/kaferry-qualification.lock')
VENV = ROOT / ('.cache/qualification/venv-' + str(sys.version_info.major) + '.' + str(sys.version_info.minor))
REPLICATED_SOURCE_PORTS = (46092, 46093, 46094)
REPLICATED_DESTINATION_PORTS = (47092, 47093, 47094)
REPLICATED_SUITES = ('replicated', 'fault-replicated')


def replicated_fixture(args):
    return (getattr(args, 'suite', None) in REPLICATED_SUITES or
            getattr(args, 'suite', None) == 'endurance' and getattr(args, 'topology', None) == 'replicated')


def plan(tier):
    versions = (VERSIONS[0], VERSIONS[-1]) if tier == 'pr' else VERSIONS
    pairs = [(v, v) for v in versions] if tier == 'nightly' else itertools.product(versions, repeat=2)
    cases = [{'id': f'broker-{a}-{b}', 'suite': 'broker', 'source': a, 'destination': b} for a, b in pairs]
    cases += [{'id': f'auth-{v}-{v}', 'suite': 'auth', 'source': v, 'destination': v}
              for v in (VERSIONS if tier == 'full' else [VERSIONS[-1]])]
    cases.append({'id': f'capture-{VERSIONS[0]}-{VERSIONS[0]}', 'suite': 'capture',
                  'source': VERSIONS[0], 'destination': VERSIONS[0]})
    if tier == 'pr':
        cases[-1]['duration'] = 60
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


def canonical_platform(system, architecture):
    if system not in ('linux', 'darwin') or architecture not in ('amd64', 'arm64'):
        raise ValueError('qualification requires Linux/macOS AMD64/ARM64 platform metadata')
    return system + '-' + architecture


def identity_platforms(binary, go):
    settings = {}
    for line in capture([go, 'version', '-m', binary]).splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] == 'build' and '=' in fields[1]:
            key, value = fields[1].split('=', 1)
            settings[key] = value
    execution = capture([go, 'env', 'GOOS', 'GOARCH']).split()
    if len(execution) != 2:
        raise ValueError('Go did not report the checkout execution platform')
    return dict(binary_platform=canonical_platform(settings.get('GOOS'), settings.get('GOARCH')),
                execution_platform=canonical_platform(*execution))


def logged(command, directory, label, env, timeout=1800, cancelled=None):
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
        def watch():
            deadline = time.monotonic() + timeout
            while not finished.wait(0.1):
                if time.monotonic() >= deadline or cancelled is not None and cancelled.is_set():
                    terminate()
                    return
        watchdog = threading.Thread(target=watch, daemon=True)
        watchdog.start()
        try:
            for line in process.stdout:
                print(line, end='', flush=True)
                log.write(line)
                log.flush()
            code = process.wait()
        finally:
            finished.set()
            signal_group(signal.SIGTERM)
            try:
                process.wait(timeout=STOP_GRACE)
            except subprocess.TimeoutExpired:
                signal_group(signal.SIGKILL)
                process.wait()
            signal_group(signal.SIGKILL)
            process.stdout.close()
            watchdog.join(timeout=STOP_GRACE+1)
    if cancelled is not None and cancelled.is_set():
        raise InterruptedError(f'{label} cancelled')
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
    if args.suite != 'recovery':
        env.pop('KAFERRY_RECOVERY_DURATION', None)
    prefix = [env.get('GO', 'go'), 'test', '-count=1', '-json']
    if args.suite == 'fault-replicated':
        selected = [args.case] if getattr(args, 'case', None) else list(REPLICATED_CASES)
        command = go_suite(args.suite, go=prefix[0], timeout=('4m' if selected == ['ownership/zombie-worker'] else f'{replicated_execution_seconds(selected)}s'))
        if len(selected) == 1:
            command[command.index('-run')+1] = '/'.join('^'+part+'$' for part in REPLICATED_CASES[selected[0]].split('/'))
        return command, [REPLICATED_CASES[id] for id in selected]
    if args.suite in ('broker', 'replicated', 'cloud'):
        python = str(VENV / 'bin/python') if VENV.exists() else sys.executable
        return [python, '-m', 'tools.qualification.broker_probe', '--binary', args.binary, '--output', str(args.output),
                '--source', env['KAFKA_TEST_BROKERS'], '--destination', env['KAFKA_TEST_DEST_BROKERS']] + (
                    ['--config', str(args.config.resolve())] if args.config else []) + (
                    ['--replicated', '--copier-source', env['KAFERRY_WORKER1_SOURCE_BROKERS'],
                     '--copier-destination', env['KAFERRY_WORKER1_DESTINATION_BROKERS']]
                    if args.suite == 'replicated' else []), []
    if args.suite in STREAMING_SUITES:
        # Clear inherited workload selectors so the receipt describes this run.
        env.pop('KAFERRY_SOAK_PROFILE', None)
        env.pop('KAFERRY_SOAK_TOPOLOGY', None)
        env.update(KAFERRY_SOAK_DURATION=f'{args.duration}s', KAFERRY_SOAK_RATE=str(args.rate),
                   KAFERRY_SOAK_MODE='archive' if args.suite == 'capture' else 'kafka')
        if args.suite == 'endurance':
            if args.rate != 200:
                raise ValueError('endurance uses a fixed --rate 200')
            env.update(KAFERRY_SOAK_PROFILE='application', KAFERRY_ARCHIVE_PAYLOAD='random',
                       KAFERRY_SOAK_TOPOLOGY=getattr(args, 'topology', None) or 'single')
        if args.suite in ('soak', 'endurance'):
            env['KAFERRY_SOAK_DEST_STALL_CONTAINER'] = env.get('KAFKA_FAULT_DEST_CONTAINER', 'kaferry-endurance-kafka-dr-1' if args.suite == 'endurance' else 'kaferry-compat-kafka-dr-1')
        else:
            env.pop('KAFERRY_SOAK_DEST_STALL_CONTAINER', None)
        return go_suite('soak', go=prefix[0], timeout=f'{args.duration + SOAK_DRAIN_SECONDS + 300}s'), EXPECTED['soak']
    if args.suite == 'recovery':
        if args.duration > 86400:
            raise ValueError('recovery duration must not exceed 24h')
        env['KAFERRY_RECOVERY_DURATION'] = f'{args.duration}s'
        return go_suite('recovery', go=prefix[0], timeout=f'{args.duration+600}s'), EXPECTED['recovery']
    if args.suite == 'auth':
        env['KAFERRY_AUTH_IMAGE'] = IMAGES[args.source]
        env['KAFERRY_QUAL_PYTHON'] = sys.executable
        return ['sh', 'src/tests/qualification/run.sh'], EXPECTED['qualification']
    if args.suite == 'migration':
        if not args.previous_binary or not args.previous_binary.is_file():
            raise ValueError('migration requires --previous-binary naming a supported release executable')
        env['KAFERRY_PREVIOUS_BINARY'] = str(args.previous_binary.resolve())
        env['KAFERRY_PREVIOUS_STATE'] = args.state_contract
        name = 'TestPreviousBinaryRejectsUnsupportedCaptureState' if args.state_contract == 'reject' else 'TestPreviousBinaryProgressAndArchiveCompatibility'
        return prefix + ['-timeout=10m', '-tags=integration,migration', '-run', '^' + name + '$', './src/tests/system'], [name]
    if args.suite == 'integration':
        return go_suite('integration', go=prefix[0], timeout='20m'), EXPECTED['integration']
    if args.suite == 'resources':
        return prefix + ['-race', './src/internal/copy', './src/internal/archive/capture', './src/internal/checkpoint', './src/internal/archive/io'], [
            'TestCheckpointReplacementFailuresPreserveAtomicPublication',
            'TestEveryCaptureIOCutPreservesAcknowledgedRecords', 'TestEveryRecoveryIOCutCanBeRetriedByANewOwner']
    if args.suite == 'model':
        return prefix + ['-race', '-run', '^TestProgressLifecycleModel$', './src/internal/copy'], ['TestProgressLifecycleModel']
    if args.suite in ('fuzz', 'benchmark', 'smoke'):
        return ['make', args.suite], []
    if args.suite == 'fault':
        return go_suite('fault', go=prefix[0]), EXPECTED['fault']
    return go_suite(args.suite, go=prefix[0], timeout='20m' if args.suite == 'load' else '10m'), EXPECTED[args.suite]


def endurance_allowance(seconds, replicas=1):
    bursts = 1 + (seconds - 10) // 60
    records = seconds * 200 + bursts * 400
    return dict(records=records, required_broker_bytes=records * (5120 + 256) * 3 * replicas + 4 * 1024**3,
                required_evidence_bytes=max(1024**3, seconds * 8192))


def endurance_capacity(seconds, output, compose, env):
    # No compression credit. Require the combined allowance on each filesystem
    # because Docker may put both writable container layers on the same disk.
    capacity = endurance_allowance(seconds)
    free = {}
    for service in ('kafka', 'kafka-dr'):
        result = subprocess.check_output(compose + ['exec', '-T', service, 'df', '-Pk',
            '/tmp/kaferry-logs'], env=env, cwd=ROOT, text=True, timeout=15)
        free[service] = int(result.splitlines()[-1].split()[3]) * 1024
    evidence_free = shutil.disk_usage(output).free
    capacity.update(broker_free_bytes=free, evidence_free_bytes=evidence_free)
    write_json(output / 'capacity.json', capacity)
    if min(free.values()) < capacity['required_broker_bytes'] or evidence_free < capacity['required_evidence_bytes']:
        raise ValueError('insufficient endurance disk allowance; inspect capacity.json')
    return capacity


def fixture_capacity(output, required):
    for attempt in range(1, 4):
        started = time.monotonic()
        stdout, stderr, status, timed_out = b'', b'', None, False
        failure = None
        try:
            result = subprocess.run(['docker', 'info', '--format', '{{json .}}'],
                                    capture_output=True, cwd=ROOT, timeout=60)
            stdout, stderr, status = result.stdout or b'', result.stderr or b'', result.returncode
            if status != 0:
                failure = f'exited with code {status}: ' + stderr.decode(errors='replace').strip()[:400]
        except subprocess.TimeoutExpired as error:
            stdout, stderr = error.stdout or b'', error.stderr or b''
            timed_out = True
            failure = 'timed out after 60 seconds'
        except OSError as error:
            stderr = str(error).encode()
            failure = 'could not execute: ' + str(error)
        if failure is None:
            try:
                runtime = json.loads(stdout)
                if not isinstance(runtime, dict):
                    failure = 'returned unparsable output: expected a JSON object'
            except (json.JSONDecodeError, UnicodeDecodeError):
                failure = 'returned unparsable output' if stdout.strip() else 'returned empty output'
        write_json(output / f'docker-info-attempt-{attempt}.json', dict(
            stdout=stdout.decode(errors='replace'), stderr=stderr.decode(errors='replace'),
            exit_status=status, duration_seconds=time.monotonic() - started,
            timed_out=timed_out, error=failure))
        if failure is None:
            break
    else:
        raise ValueError('docker info failed after 3 attempts: ' + failure +
                         '; inspect docker-info-attempt-*.json')
    capacity = dict(required,
                    runtime_cpus=runtime.get('NCPU', 0), runtime_memory_bytes=runtime.get('MemTotal', 0),
                    evidence_free_bytes=shutil.disk_usage(output).free)
    write_json(output / 'capacity.json', capacity)
    if (capacity['runtime_cpus'] < capacity['required_cpus'] or
            capacity['runtime_memory_bytes'] < capacity['required_runtime_memory_bytes'] or
            capacity['evidence_free_bytes'] < capacity['required_evidence_bytes']):
        raise ValueError('insufficient fixture capacity; inspect capacity.json')
    return capacity


def replicated_admission(output, endurance_seconds=None):
    required = dict(required_cpus=2, required_runtime_memory_bytes=8 * 1024**3,
        required_broker_bytes=4 * 1024**3, required_evidence_bytes=1024**3)
    if endurance_seconds is not None:
        required.update(endurance_allowance(endurance_seconds, replicas=3))
    capacity = fixture_capacity(output, required)
    for port in (*network.client_ports(), 48074):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(('127.0.0.1', port))
        except OSError as error:
            raise ValueError(f'replicated fixture port {port} is unavailable') from error
    return capacity


def replicated_storage_command(compose, operation):
    # Send the script through argv: a remote Docker daemon cannot bind a client-side checkout path.
    script = (ROOT / 'src/tests/replicated/storage.sh').read_text()
    return compose + ['run', '--rm', '--no-deps', '--entrypoint', '/bin/sh',
                      'storage', '-ec', script, 'storage', operation]


def replicated_storage(output, compose, env, capacity, cancelled):
    logged(replicated_storage_command(compose, 'capacity'), output,
           'storage-capacity', env, timeout=180, cancelled=cancelled)
    readings = [int(line) for line in (output / 'storage-capacity.log').read_text().splitlines() if line.isdecimal()]
    if len(readings) != 1:
        raise ValueError('fixture storage did not report exactly one free-space reading')
    capacity['broker_free_bytes'] = readings[0] * 1024
    write_json(output / 'capacity.json', capacity)
    if capacity['broker_free_bytes'] < capacity['required_broker_bytes']:
        raise ValueError('insufficient fixture broker space; inspect capacity.json')
    logged(replicated_storage_command(compose, 'prepare'), output,
           'storage-prepare', env, timeout=60, cancelled=cancelled)


def cleanup_replicated(output, compose, env):
    # Stop writers before deleting only the directory bearing this run's marker.
    errors = []
    try:
        logged(compose + ['stop', '--timeout', '10'], output, 'brokers-stop', env, timeout=90)
        logged(replicated_storage_command(compose, 'clean'), output,
               'storage-clean', env, timeout=60)
    except (OSError, RuntimeError) as error:
        errors.append({'phase': 'storage-clean', 'error': str(error)})
    return errors


@dataclass(frozen=True)
class RunSpec:
    command: list
    expected: list
    env: dict
    compose: list
    timeout: int


def resolve_run(args, receipt):
    args = argparse.Namespace(**dict(vars(args), binary=str(Path(args.binary).resolve())))
    topology = getattr(args, 'topology', None)
    if topology is not None and (args.suite != 'endurance' or topology not in ('single', 'replicated')):
        raise ValueError('--topology is only accepted by run endurance: single or replicated')
    project = 'kaferry-endurance' if args.suite == 'endurance' else 'kaferry-compat'
    source_port, destination_port = (39192, 40192) if args.suite == 'endurance' else (39092, 40092)
    env = dict(os.environ, KAFERRY_SOURCE_PORT=str(source_port), KAFERRY_DESTINATION_PORT=str(destination_port), KAFERRY_REPORT_DIR=str(args.output), KAFERRY_TEST_OUTPUT='-json',
               KAFKA_TEST_BROKERS=f'127.0.0.1:{source_port}', KAFKA_TEST_DEST_BROKERS=f'127.0.0.1:{destination_port}',
               KAFKA_FAULT_CONTAINER=project+'-kafka-1', KAFKA_FAULT_DEST_CONTAINER=project+'-kafka-dr-1', KAFERRY_FIXTURE_PROJECT=project, CGO_ENABLED='1')
    compose = ['docker', 'compose', '-f', 'src/tests/compose.yaml', '-p', project, '--profile', 'cross-cluster']
    selected_case = getattr(args, 'case', None)
    if selected_case and args.suite != 'fault-replicated':
        raise ValueError('--case requires run fault-replicated')
    if args.suite == 'fault-replicated':
        if selected_case is not None and selected_case not in REPLICATED_CASES:
            raise ValueError('unknown replicated fault case')
        receipt['case_selection'] = [selected_case] if selected_case else list(REPLICATED_CASES)
    two_brokers = not replicated_fixture(args) and (args.suite in TWO_BROKER_SUITES or
        args.suite == 'migration' and args.state_contract == 'resume')
    if replicated_fixture(args):
        if args.source != VERSIONS[-1] or args.destination != VERSIONS[-1]:
            raise ValueError('replicated uses the newest catalog image and its dedicated fixture')
        project = 'kaferry-replicated'
        env.update(KAFKA_TEST_BROKERS=','.join(f'127.0.0.1:{port}' for port in REPLICATED_SOURCE_PORTS),
                   KAFKA_TEST_DEST_BROKERS=','.join(f'127.0.0.1:{port}' for port in REPLICATED_DESTINATION_PORTS),
                   KAFKA_FAULT_CONTAINER=project+'-source-1-1', KAFKA_FAULT_DEST_CONTAINER=project+'-destination-1-1',
                   KAFERRY_FIXTURE_PROJECT=project, KAFERRY_REPLICATED_IMAGE=IMAGES[VERSIONS[-1]],
                   KAFERRY_REPLICATED_RUN_ID=uuid.uuid4().hex)
        for worker in range(1, 4):
            env[f'KAFERRY_WORKER{worker}_SOURCE_BROKERS'] = network.endpoints('source', worker)
            env[f'KAFERRY_WORKER{worker}_DESTINATION_BROKERS'] = network.endpoints('destination', worker)
        env['KAFERRY_TOXIPROXY_API'] = network.API
        compose = ['docker', 'compose', '-f', 'src/tests/replicated/compose.yaml', '-p', project, '--profile', 'replicated']
        receipt['parameters'] = dict(topology='replicated', brokers_per_cluster=3,
                                     replication_factor=3, min_insync_replicas=2, client_paths=4)
        receipt['fixture_storage'] = dict(mode='disposable-volume',
                                         run_id=env['KAFERRY_REPLICATED_RUN_ID'])
    fixtures = two_brokers or replicated_fixture(args)
    if not Path(args.binary).is_file():
        raise ValueError('binary is missing; run make build or pass --binary')
    receipt['binary_sha256'] = hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()
    receipt['binary_version'] = capture([args.binary, '--version'])
    receipt.update(identity_platforms(args.binary, env.get('GO', 'go')))
    if args.suite == 'migration' and args.previous_binary:
        receipt['previous_binary_sha256'] = hashlib.sha256(args.previous_binary.read_bytes()).hexdigest()
        receipt['state_contract'] = args.state_contract
    env.update(KAFERRY_TEST_BINARY=args.binary, KAFERRY_EXPECT_BINARY_SHA256=receipt['binary_sha256'],
               KAFERRY_SOURCE_IMAGE=IMAGES[args.source], KAFERRY_DESTINATION_IMAGE=IMAGES[args.destination])
    receipt['requested_images'] = [env['KAFERRY_SOURCE_IMAGE'], env['KAFERRY_DESTINATION_IMAGE']] if fixtures else []
    if replicated_fixture(args):
        receipt['requested_images'].append(network.IMAGE)
    if args.config and args.suite != 'cloud':
        raise ValueError('--config is only accepted by the external cloud suite')
    if args.suite == 'cloud' and not args.config:
        raise ValueError('cloud requires --config; this command never provisions a service')
    if args.suite in ('fault', 'recovery', 'fault-replicated'):
        from tools.qualification.fault_results import configure
        configure(env)
        receipt['fault_oracle_run'] = env['KAFERRY_ORACLE_RUN']
    command, expected = suite_command(args, env)
    receipt.update(command=list(map(str, command)), expected_tests=expected)
    if fixtures:
        receipt.update(fixture_project=project, fixture_brokers=[env['KAFKA_TEST_BROKERS'], env['KAFKA_TEST_DEST_BROKERS']])
    timeout = 1800
    if args.suite in STREAMING_SUITES:
        timeout = args.duration + SOAK_DRAIN_SECONDS + 300
    elif args.suite == 'recovery':
        timeout = args.duration + 600
    elif args.suite == 'fault-replicated':
        timeout = replicated_execution_seconds(receipt['case_selection'])+60
    return RunSpec(command, expected, env, compose if fixtures else [], timeout)


def fixture_images(spec):
    images = subprocess.check_output(spec.compose + ['images', '--format', 'json'],
        env=spec.env, cwd=ROOT, text=True, timeout=10)
    return validate_fixture_images(images)


def validate_fixture_images(images):
    images = images.strip()
    if not images:
        raise ValueError('fixture did not report its broker images')
    rows = json.loads(images)
    if isinstance(rows, dict):
        rows = list(rows.values())
    if not isinstance(rows, list) or not rows or any(
            not isinstance(row, dict) or not isinstance(row.get('ID'), str) or not row['ID'].strip() for row in rows):
        raise ValueError('fixture did not report its broker image identities')
    return images


@contextmanager
def broker_fixture(args, spec, receipt, cancelled, *, lease_held=False):
    with nullcontext() if lease_held else lab_lock():
        owns_fixture = False
        try:
            if replicated_fixture(args):
                receipt['capacity'] = replicated_admission(args.output,
                    args.duration if args.suite == 'endurance' else None)
                owns_fixture = True
                replicated_storage(args.output, spec.compose, spec.env, receipt['capacity'], cancelled)
                logged(spec.compose + ['up', '-d', '--wait', 'toxiproxy'], args.output,
                       'proxy-up', spec.env, timeout=90, cancelled=cancelled)
                logged([sys.executable, '-m', 'tools.qualification.network', '--output', str(args.output)],
                       args.output, 'proxy-configure', spec.env, timeout=60, cancelled=cancelled)
            if spec.compose and args.suite != 'auth':
                owns_fixture = True
                logged(spec.compose + ['up', '-d', '--wait', '--wait-timeout', '120'], args.output,
                       'brokers-up', spec.env, timeout=180, cancelled=cancelled)
                receipt['broker_images'] = fixture_images(spec)
            if args.suite in REPLICATED_SUITES:
                logged([spec.env.get('GO', 'go'), 'test', '-count=1', '-json', '-tags=integration',
                        '-timeout=3m', '-run', '^TestReplicatedProxyRoutes$', './src/tests/system/faults'],
                       args.output, 'proxy-proof', spec.env, timeout=210, cancelled=cancelled)
                verify_tests((args.output / 'proxy-proof.log').read_text(), ['TestReplicatedProxyRoutes'])
                receipt['proxy_path_check'] = 'passed'
            if args.suite == 'endurance':
                if not replicated_fixture(args):
                    receipt['capacity'] = endurance_capacity(args.duration, args.output, spec.compose, spec.env)
                write_json(args.output / 'manifest.json', receipt)
            yield
        except (Exception, KeyboardInterrupt) as error:
            receipt['suite_error'] = str(error) or 'interrupted'
            raise
        finally:
            if spec.compose and owns_fixture:
                errors = []
                logs_captured = False
                def capture_logs():
                    nonlocal logs_captured
                    if logs_captured:
                        return
                    logs_captured = True
                    try:
                        with (args.output / 'brokers.log').open('w') as log:
                            subprocess.run(spec.compose + ['logs', '--no-color'], cwd=ROOT, env=spec.env,
                                           stdout=log, stderr=subprocess.STDOUT, timeout=30, check=False)
                    except (OSError, subprocess.SubprocessError) as error:
                        errors.append({'phase': 'logs', 'error': str(error)})
                try:
                    if args.suite == 'fault-replicated' or 'suite_error' in receipt or receipt.get('fault_results', {}).get('status') == 'inconclusive':
                        capture_logs()
                finally:
                    owned_storage = replicated_fixture(args)
                    if owned_storage:
                        errors.extend(cleanup_replicated(args.output, spec.compose, spec.env))
                    if errors:
                        capture_logs()
                    try:
                        # Keep storage if ownership validation or writer shutdown failed.
                        volumes = [] if owned_storage and errors else ['-v']
                        logged(spec.compose + ['down', *volumes, '--remove-orphans'], args.output, 'brokers-down', spec.env, timeout=60)
                    except (OSError, RuntimeError) as error:
                        errors.append({'phase': 'teardown', 'error': str(error)})
                        capture_logs()
                if errors:
                    receipt['cleanup_errors'] = errors
                    if 'suite_error' not in receipt:
                        raise RuntimeError('fixture diagnostics or teardown failed; inspect manifest.json')


def verify_run(args, spec, receipt):
    if spec.expected and args.suite != 'fault-replicated':
        verify_tests((args.output / 'suite.log').read_text(), spec.expected, suite=args.suite)
    if args.suite == 'benchmark':
        from tools.ci.quality import benchmarks
        measured = benchmarks((args.output / 'suite.log').read_text())
        for name in ('BenchmarkEncode', 'BenchmarkDecode', 'BenchmarkReplayOverlap', 'BenchmarkCheckpointEncoding', 'BenchmarkCheckpointSaving'):
            if not any(key.startswith(name) for key in measured):
                raise ValueError('required benchmark did not execute: ' + name)
    if args.suite in ('broker', 'replicated', 'cloud'):
        probe = json.loads((args.output / 'reconciliation.json').read_text())
        if (probe['status'] != 'passed' or probe['binary_sha256'] != receipt['binary_sha256'] or
                {c['name'] for c in probe['cases']} != {route + '/' + phase for route in ('topic-preserve', 'topic-key', 'capture', 'clone', 'replay') for phase in ('partial', 'resume', 'completed')} or
                any(c['verified'] <= 0 or c['verified'] != c['expected'] or
                    c['missing'] or c['duplicates'] or c['corrupted'] for c in probe['cases'])):
            raise ValueError('independent reconciliation did not pass for the tested binary')
        if args.suite == 'replicated' and probe.get('topology') != dict(
                source_brokers=3, destination_brokers=3, replication_factor=3, min_insync_replicas=2):
            raise ValueError('independent reconciliation did not verify the replicated topology')
        receipt['reconciliation'] = probe
    if args.suite in ('fault', 'recovery'):
        from tools.qualification.fault_results import collect
        receipt['fault_results'] = collect(args.output, args.suite, receipt, spec.env.get('KAFERRY_ORACLE_RUN'))
    if args.suite == 'fault-replicated':
        from tools.qualification.replicated_results import collect
        cases = collect(args.output, receipt['case_selection'], receipt, spec.env['KAFERRY_ORACLE_RUN'])
        status = verify_replicated_tests((args.output / 'suite.log').read_text(), spec.expected, cases)
        receipt['fault_results'] = dict(schema=1, status=status, cases=cases, oracle_run=spec.env['KAFERRY_ORACLE_RUN'],
            **{key: receipt[key] for key in ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')})


class InconclusivePrecondition(RuntimeError):
    """A completed safe attempt did not establish its fixture precondition."""


def failed_outcome(error):
    status = ('interrupted' if isinstance(error, (KeyboardInterrupt, InterruptedError)) else
              'inconclusive' if isinstance(error, InconclusivePrecondition) else 'failed')
    return dict(status=status, error=str(error) or 'interrupted')


def verify_replicated_tests(log, expected, cases):
    events = []
    for line in log.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict):
            events.append(event)
    if any(event.get('Action') == 'fail' for event in events):
        raise ValueError('replicated Go test or cleanup failed')
    inconclusive = {REPLICATED_CASES[case['id']] for case in cases if case.get('status') == 'inconclusive'}
    skipped = [event.get('Test') or event.get('Package') or '<unnamed>'
               for event in events if event.get('Action') == 'skip']
    if (set(skipped) != inconclusive or len(skipped) != len(inconclusive) or
            len(inconclusive) != sum(case.get('status') == 'inconclusive' for case in cases)):
        raise ValueError('replicated skip lacks an exact inconclusive precondition proof')
    if any(event.get('Action') == 'pass' and event.get('Test') in inconclusive for event in events):
        raise ValueError('inconclusive precondition cannot pass its Go case')
    if inconclusive:
        # A proved precondition skip ends this attempt without passing it.
        required = [name for name in expected if name not in inconclusive] + sorted({name.split('/')[0] for name in inconclusive})
        passed = {event['Test'] for event in events if event.get('Action') == 'pass' and event.get('Test')}
        if missing := [name for name in required if name not in passed]:
            raise ValueError('Required tests did not pass: ' + ', '.join(missing))
        return 'inconclusive'
    verify_tests(log, expected, suite='fault-replicated')
    return 'passed'


def run_suite(args, *, cancelled=None):
    selected = getattr(args, 'case', None)
    if args.suite != 'fault-replicated' or selected and not selected.startswith('network/'):
        return run_suite_attempt(args, cancelled=cancelled)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = dict(schema=1, suite=args.suite, status='running', attempts=[])
    write_json(output/'manifest.json', summary)
    try:
        # Keep the host lease across teardown and the one fresh-fixture rerun.
        with lab_lock():
            for number in (1, 2):
                if cancelled is not None and cancelled.is_set():
                    raise InterruptedError('qualification cancelled before fresh fixture')
                attempt = argparse.Namespace(**dict(vars(args), output=output/f'attempt-{number}'))
                try:
                    identity = summary['attempts'][0] if summary['attempts'] else None
                    result = run_suite_attempt(attempt, cancelled=cancelled, lease_held=True, identity=identity)
                except InconclusivePrecondition:
                    if number == 2:
                        raise
                finally:
                    path = attempt.output/'manifest.json'
                    if path.is_file():
                        receipt = json.loads(path.read_text())
                        summary['attempts'].append(dict(observations=f'attempt-{number}/manifest.json',
                            **{key: receipt[key] for key in ('status', 'source_commit', 'working_tree_dirty', 'binary_sha256', 'binary_platform', 'execution_platform') if key in receipt}))
                if receipt['status'] == 'passed':
                    summary.update(status='passed', qualified_attempt=f'attempt-{number}/manifest.json',
                                   **{key: result[key] for key in ('source_commit', 'working_tree_dirty', 'binary_sha256', 'binary_platform', 'execution_platform')})
                    return summary
                write_json(output/'manifest.json', summary)
    except (Exception, KeyboardInterrupt) as error:
        summary.update(failed_outcome(error))
        raise
    finally:
        write_json(output/'manifest.json', summary)


def run_suite_attempt(args, *, cancelled=None, lease_held=False, identity=None):
    args = argparse.Namespace(**dict(vars(args), output=args.output.resolve(),
        destination=args.source if args.suite == 'auth' else args.destination))
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {'schema': 1, 'id': f'{args.suite}-{args.source}-{args.destination}', 'suite': args.suite,
               'source_version': args.source, 'destination_version': args.destination,
               'source_commit': capture(['git', 'rev-parse', 'HEAD']),
               'working_tree_dirty': bool(capture(['git', 'status', '--porcelain', '--untracked-files=all'])),
               'platform': platform.platform(), 'machine': platform.machine(), 'python': platform.python_version(),
               'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'running',
               'go': capture([os.environ.get('GO', 'go'), 'version']),
               'execution': 'checkout component tests' if args.suite in ('model', 'resources', 'fuzz', 'benchmark') else 'CLI scenarios with checkout fixture tests',
               'requested_seconds': args.duration if args.suite in (*STREAMING_SUITES, 'recovery') else None}
    if args.suite in STREAMING_SUITES:
        receipt['drain_seconds'] = SOAK_DRAIN_SECONDS
    if args.suite == 'endurance':
        topology = getattr(args, 'topology', None) or 'single'
        receipt['workload'] = dict(profile='application-24x32-v1', processes=1, routing='KEY',
            source_partitions=24, destination_partitions=32, topology=topology,
            replication_factor=3 if topology == 'replicated' else 1,
            rate=200, payload='random', duration_claim='endurance' if args.duration >= 86400 else 'rehearsal')
        if topology == 'replicated':
            receipt['workload']['min_insync_replicas'] = 2
    if args.suite == 'cloud':
        receipt.update(source_version='external', destination_version='external', id='cloud-external-external')
    started = time.monotonic()
    write_json(args.output / 'manifest.json', receipt)
    try:
        spec = resolve_run(args, receipt)
        if identity is not None and (identity['working_tree_dirty'] or receipt['working_tree_dirty']):
            raise ValueError('automatic qualification retry requires a clean unchanged checkout')
        if identity is not None and any(identity[key] != receipt[key] for key in
                ('source_commit', 'working_tree_dirty', 'binary_sha256', 'binary_platform', 'execution_platform')):
            raise ValueError('qualification identity changed between attempts')
        write_json(args.output / 'manifest.json', receipt)
        with broker_fixture(args, spec, receipt, cancelled, lease_held=lease_held):
            try:
                logged(spec.command, args.output, 'suite', spec.env, timeout=spec.timeout, cancelled=cancelled)
            finally:
                if args.suite == 'auth':
                    suite_failed = sys.exc_info()[0] is not None
                    try:
                        receipt['broker_images'] = validate_fixture_images(
                            (args.output / 'qualification.broker-images.json').read_text())
                    except (OSError, ValueError) as error:
                        if not suite_failed:
                            raise
                        receipt['image_error'] = str(error)
            verify_run(args, spec, receipt)
        if receipt.get('fault_results', {}).get('status') == 'inconclusive':
            raise InconclusivePrecondition('healthy fixture rate precondition was not met')
        receipt['status'] = 'passed'
    except (Exception, KeyboardInterrupt) as error:
        receipt.update(failed_outcome(error))
        raise
    finally:
        if args.suite == 'fault-replicated':
            from tools.qualification.replicated_evidence import retain
            try:
                retain(args.output)
            except (OSError, ValueError, KeyError) as error:
                receipt['diagnostic_error'] = str(error)
        receipt.update(elapsed_seconds=time.monotonic() - started, finished_at=datetime.now(timezone.utc).isoformat())
        write_json(args.output / 'manifest.json', receipt)
        if args.suite == 'endurance':
            from tools.qualification.endurance_results import prepare
            try:
                prepare(args.output, args.output / 'publication')
            except (OSError, ValueError) as error:
                if receipt['status'] == 'passed':
                    raise
                print(f'Endurance report unavailable: {error}', file=sys.stderr)

    return receipt


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
            running.add_argument('--topology', choices=('single', 'replicated'), help='endurance topology (default: single)')
            running.add_argument('--case', choices=tuple(REPLICATED_CASES), help='one case within fault-replicated; default: all registered cases')
        elif name == 'matrix':
            running.add_argument('--tier', choices=('pr', 'nightly', 'full'), default='pr')
        running.add_argument('--source', choices=VERSIONS)
        running.add_argument('--destination', choices=VERSIONS)
        running.add_argument('--binary', default='bin/kaferry', help='existing candidate executable (default: %(default)s)')
        running.add_argument('--previous-binary', type=Path)
        running.add_argument('--state-contract', choices=('resume', 'reject'), default='resume')
        running.add_argument('--config', type=Path)
        running.add_argument('--duration', type=duration)
        running.add_argument('--rate', type=int, choices=range(1, 10001), metavar='1..10000')
        running.add_argument('--output', type=Path, help='new evidence directory; existing paths are refused',
            default=Path('build-results/qualification') /
            (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]))
    verifying = sub.add_parser('verify-release', help='validate full matrix evidence against an exact release archive')
    verifying.add_argument('report', type=Path)
    verifying.add_argument('archive', type=Path)
    verifying.add_argument('commit')
    reporting = sub.add_parser('report', help='require all planned cases with matching source and binary identities')
    reporting.add_argument('directory', type=Path)
    reporting.add_argument('--tier', choices=('pr', 'nightly', 'full'), default='pr')
    args = parser.parse_args(argv)
    requested_duration = getattr(args, 'duration', None)
    if args.action in ('run', 'matrix', 'releases'):
        endurance = getattr(args, 'suite', None) == 'endurance'
        newest = replicated_fixture(args)
        args.source = args.source or VERSIONS[-1 if newest else 0]
        args.destination = args.destination or VERSIONS[-1 if newest else 0]
        args.duration = args.duration if args.duration is not None else (86400 if endurance else 7200 if getattr(args, 'suite', None) == 'recovery' else 120)
        args.rate = args.rate if args.rate is not None else (200 if endurance else 50)
    os.chdir(ROOT)
    if args.action == 'setup':
        if sys.version_info < (3, 11):
            raise ValueError('qualification setup requires Python 3.11+; select that interpreter for setup and runs')
        venv.EnvBuilder(with_pip=True).create(VENV)
        requirements = 'requirements-cloud.txt' if args.cloud else 'requirements.txt'
        subprocess.run([str(VENV / 'bin/python'), '-m', 'pip', 'install', '--no-cache-dir', '-r',
                        str(ROOT / 'src/tests/qualification' / requirements)], check=True)
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
            settings = dict(case)
            if requested_duration is not None:
                settings['duration'] = requested_duration
            case_args = argparse.Namespace(**dict(vars(args), **settings, output=directory / case['id']))
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
