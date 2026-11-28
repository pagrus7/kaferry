"""Manage the host lease and disposable broker fixtures for qualification runs."""

from contextlib import contextmanager, nullcontext
import fcntl
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

from tools.qualification import network, run_io

LAB_LOCK = Path('/tmp/kaferry-qualification.lock')
REPLICATED_SUITES = ('replicated', 'fault-replicated')


def replicated_fixture(args):
    return (getattr(args, 'suite', None) in REPLICATED_SUITES or
            getattr(args, 'suite', None) == 'endurance' and getattr(args, 'topology', None) == 'replicated')


@contextmanager
def lab_lock():
    with LAB_LOCK.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('another qualification suite owns this host’s disposable broker ports')
        yield


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
            '/tmp/kaferry-logs'], env=env, cwd=run_io.ROOT, text=True, timeout=15)
        free[service] = int(result.splitlines()[-1].split()[3]) * 1024
    evidence_free = shutil.disk_usage(output).free
    capacity.update(broker_free_bytes=free, evidence_free_bytes=evidence_free)
    run_io.write_json(output / 'capacity.json', capacity)
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
                                    capture_output=True, cwd=run_io.ROOT, timeout=60)
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
        run_io.write_json(output / f'docker-info-attempt-{attempt}.json', dict(
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
    run_io.write_json(output / 'capacity.json', capacity)
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
    for port in (*network.client_ports(), network.API_PORT):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                # Closed fixture connections may still be in TIME_WAIT.
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(('127.0.0.1', port))
            # On macOS, SO_REUSEADDR lets a 127.0.0.1 bind succeed beside a wildcard listener.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.2)
                if probe.connect_ex(('127.0.0.1', port)) == 0:
                    raise ValueError(f'replicated fixture port {port} is unavailable')
        except OSError as error:
            raise ValueError(f'replicated fixture port {port} is unavailable') from error
    return capacity


def replicated_storage_command(compose, operation):
    # Send the script through argv: a remote Docker daemon cannot bind a client-side checkout path.
    script = (run_io.ROOT / 'src/tests/replicated/storage.sh').read_text()
    return compose + ['run', '--rm', '--no-deps', '--entrypoint', '/bin/sh',
                      'storage', '-ec', script, 'storage', operation]


def replicated_storage(output, compose, env, capacity, cancelled):
    run_io.logged(replicated_storage_command(compose, 'capacity'), output,
           'storage-capacity', env, timeout=180, cancelled=cancelled)
    readings = [int(line) for line in (output / 'storage-capacity.log').read_text().splitlines() if line.isdecimal()]
    if len(readings) != 1:
        raise ValueError('fixture storage did not report exactly one free-space reading')
    capacity['broker_free_bytes'] = readings[0] * 1024
    run_io.write_json(output / 'capacity.json', capacity)
    if capacity['broker_free_bytes'] < capacity['required_broker_bytes']:
        raise ValueError('insufficient fixture broker space; inspect capacity.json')
    run_io.logged(replicated_storage_command(compose, 'prepare'), output,
           'storage-prepare', env, timeout=60, cancelled=cancelled)


def cleanup_replicated(output, compose, env):
    # Stop writers before deleting only the directory bearing this run's marker.
    errors = []
    try:
        run_io.logged(compose + ['stop', '--timeout', '10'], output, 'brokers-stop', env, timeout=90)
        run_io.logged(replicated_storage_command(compose, 'clean'), output,
               'storage-clean', env, timeout=60)
    except (OSError, RuntimeError) as error:
        errors.append({'phase': 'storage-clean', 'error': str(error)})
    return errors


def fixture_images(spec):
    images = network.docker_query(spec.compose + ['images', '--format', 'json'], env=spec.env, cwd=run_io.ROOT)
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
                run_io.logged(spec.compose + ['up', '-d', '--wait', 'toxiproxy'], args.output,
                       'proxy-up', spec.env, timeout=90, cancelled=cancelled)
                # Allow both Docker query retry budgets, then route configuration.
                run_io.logged([sys.executable, '-m', 'tools.qualification.network', '--output', str(args.output)],
                       args.output, 'proxy-configure', spec.env, timeout=420, cancelled=cancelled)
            if spec.compose and args.suite != 'auth':
                owns_fixture = True
                run_io.logged(spec.compose + ['up', '-d', '--wait', '--wait-timeout', '120'], args.output,
                       'brokers-up', spec.env, timeout=180, cancelled=cancelled)
                receipt['broker_images'] = fixture_images(spec)
            if args.suite == 'endurance':
                if not replicated_fixture(args):
                    receipt['capacity'] = endurance_capacity(args.duration, args.output, spec.compose, spec.env)
                run_io.write_json(args.output / 'manifest.json', receipt)
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
                            subprocess.run(spec.compose + ['logs', '--no-color'], cwd=run_io.ROOT, env=spec.env,
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
                        run_io.logged(spec.compose + ['down', *volumes, '--remove-orphans'], args.output, 'brokers-down', spec.env, timeout=60)
                    except (OSError, RuntimeError) as error:
                        errors.append({'phase': 'teardown', 'error': str(error)})
                        capture_logs()
                if errors:
                    receipt['cleanup_errors'] = errors
                    if 'suite_error' not in receipt:
                        raise RuntimeError('fixture diagnostics or teardown failed; inspect manifest.json')
