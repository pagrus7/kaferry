"""Bounded diagnostic measurements of the owned replicated fixture."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import threading
import time


SERVICES = tuple(f'{side}-{node}' for side in ('source', 'destination') for node in range(1, 4)) + ('toxiproxy',)
MAX_SECONDS = 600
MAX_BYTES = 8 * 1024**2
MAX_ROWS = 8192
MAX_LINE = 64 * 1024
COMMAND_SECONDS = 2
STATS_SECONDS = 5
INSPECT = ('{"id":{{json .Id}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
           '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
           '"running":{{json .State.Running}},"started_at":{{json .State.StartedAt}},'
           '"restart_count":{{json .RestartCount}},"health":{{with .State.Health}}'
           '{"status":{{json .Status}},"failing_streak":{{json .FailingStreak}},"log":['
           '{{range $i, $p := .Log}}{{if $i}},{{end}}{"start":{{json $p.Start}},'
           '"end":{{json $p.End}},"exit_code":{{json $p.ExitCode}}}{{end}}]}{{else}}null{{end}}}')


def now():
    return datetime.now(timezone.utc).isoformat()


class RuntimeObservations:
    def __init__(self, output, compose, env):
        self.output, self.compose, self.env = output, compose, env
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.threads = []
        self.started = time.monotonic()
        self.summary = dict(status='unavailable', started_at=now(), rows=0, bytes=0,
                            max_seconds=MAX_SECONDS, max_rows=MAX_ROWS, max_bytes=MAX_BYTES,
                            command_wall_seconds=0.0, errors=[], omitted_errors=0, limits_reached=[])

    def error(self, message):
        with self.lock:
            if self.summary['status'] == 'completed':
                self.summary['status'] = 'incomplete'
            if len(self.summary['errors']) < 16:
                self.summary['errors'].append(message)
            else:
                self.summary['omitted_errors'] += 1

    def expired(self):
        if time.monotonic() - self.started >= MAX_SECONDS:
            with self.lock:
                if 'seconds' not in self.summary['limits_reached']:
                    self.summary['limits_reached'].append('seconds')
            self.stop.set()
        return self.stop.is_set()

    def write(self, kind, value, started=None):
        row = dict(kind=kind, observed_at=now(), value=value)
        if started is not None:
            row['query_started_at'] = started
        row = json.dumps(row, separators=(',', ':')).encode() + b'\n'
        with self.lock:
            if self.summary['bytes'] + len(row) > MAX_BYTES or self.summary['rows'] >= MAX_ROWS:
                self.summary['limits_reached'].append('bytes' if self.summary['bytes'] + len(row) > MAX_BYTES else 'rows')
                self.stop.set()
                return
            self.stream.write(row)
            self.stream.flush()
            self.summary['bytes'] += len(row)
            self.summary['rows'] += 1

    def lines(self, command, seconds):
        # Read incrementally: communicate()/capture_output would allocate all
        # daemon output before enforcing the diagnostic byte bound.
        started = time.monotonic()
        process = subprocess.Popen(command, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        pending = b''
        received = 0
        try:
            while True:
                if self.expired():
                    return
                if time.monotonic() - started >= seconds:
                    raise TimeoutError('diagnostic command time limit')
                if not selector.select(.05):
                    continue
                block = os.read(process.stdout.fileno(), 4096)
                if not block:
                    if pending:
                        yield pending.decode('utf-8')
                    if process.wait(timeout=COMMAND_SECONDS) != 0:
                        raise RuntimeError('diagnostic command exited unsuccessfully')
                    return
                received += len(block)
                pending += block
                if received > MAX_BYTES or len(pending) > MAX_LINE:
                    raise ValueError('diagnostic command output limit')
                while b'\n' in pending:
                    line, pending = pending.split(b'\n', 1)
                    if line:
                        yield line.decode('utf-8')
        finally:
            selector.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=COMMAND_SECONDS)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=COMMAND_SECONDS)
            process.stdout.close()
            with self.lock:
                self.summary['command_wall_seconds'] += time.monotonic() - started

    def inspect(self):
        rows = [json.loads(line) for line in self.lines(['docker', 'inspect', '--format', INSPECT, *self.ids], COMMAND_SECONDS)]
        if (len(rows) != len(SERVICES) or {row['id'] for row in rows} != set(self.ids) or
                {row['service'] for row in rows} != set(SERVICES) or
                any(row['project'] != self.project for row in rows)):
            raise ValueError('diagnostic containers do not match the owned replicated fixture')
        selected = []
        for row in rows:
            value = {key: row[key] for key in ('id', 'service', 'running', 'started_at', 'restart_count')}
            health = row['health']
            if health is not None:
                if len(health['log']) > 5:
                    raise ValueError('health history exceeds Docker retention')
                health = dict(status=health['status'], failing_streak=health['failing_streak'],
                              log=[{key: entry[key] for key in ('start', 'end', 'exit_code')} for entry in health['log']])
            value['health'] = health
            selected.append(value)
        return selected

    def health(self):
        while not self.expired():
            started = now()
            try:
                self.write('health', self.inspect(), started)
            except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, TimeoutError, RuntimeError) as error:
                if not self.stop.is_set():
                    self.error('health: ' + str(error))
            self.stop.wait(1)

    def stats(self):
        try:
            while not self.expired():
                started, next_sample = now(), time.monotonic() + 1
                rows = [json.loads(line) for line in self.lines(
                    ['docker', 'stats', '--no-stream', '--no-trunc', '--format', '{{json .}}', *self.ids], STATS_SECONDS)]
                if self.stop.is_set():
                    return
                if len(rows) != len(self.ids) or {row['ID'] for row in rows} != set(self.ids):
                    raise ValueError('stats snapshot must contain every owned container once')
                selected = [{key: row[key] for key in ('ID', 'CPUPerc', 'MemUsage', 'MemPerc', 'NetIO', 'BlockIO', 'PIDs')} for row in rows]
                if any(not isinstance(value, str) or len(value) > 2048 for row in selected for value in row.values()):
                    raise ValueError('stats fields must be bounded strings')
                for row in selected:
                    self.write('stats', row, started)
                self.stop.wait(max(0, next_sample - time.monotonic()))
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, TimeoutError, RuntimeError) as error:
            if not self.stop.is_set():
                self.error('stats: ' + str(error))

    def start(self):
        try:
            self.stream = (self.output / 'replicated-runtime.jsonl').open('xb')
            self.project = self.compose[self.compose.index('-p') + 1]
            self.ids = list(self.lines([*self.compose, 'ps', '--all', '--quiet', *SERVICES], COMMAND_SECONDS))
            if len(self.ids) != len(SERVICES) or len(set(self.ids)) != len(SERVICES) or any(not re.fullmatch('[a-f0-9]{64}', value) for value in self.ids):
                raise ValueError('diagnostics require seven distinct owned container IDs')
            started = now()
            self.write('health', self.inspect(), started)
            self.summary['status'] = 'recording'
            for target in (self.health, self.stats):
                thread = threading.Thread(target=target, daemon=True)
                thread.start()
                self.threads.append(thread)
        except (OSError, ValueError, KeyError, IndexError, TypeError, subprocess.SubprocessError, TimeoutError, RuntimeError) as error:
            self.error('startup: ' + str(error))

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join()
        if hasattr(self, 'stream'):
            try:
                self.stream.close()
            except OSError as error:
                self.error('close: ' + str(error))
        self.summary['finished_at'] = now()
        if self.summary['status'] == 'recording':
            self.summary['status'] = 'incomplete' if self.summary['errors'] or self.summary['limits_reached'] else 'completed'
        try:
            (self.output / 'replicated-runtime.json').write_text(json.dumps(self.summary, indent=2) + '\n')
        except OSError as error:
            self.error('summary: ' + str(error))


@contextmanager
def runtime_observations(output, compose, env):
    observer = RuntimeObservations(Path(output), compose, env)
    try:
        observer.start()
        yield observer.summary
    finally:
        observer.close()
