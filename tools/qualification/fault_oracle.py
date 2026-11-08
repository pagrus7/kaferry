"""Independent, bounded-state observer for deterministic Kafka fault fixtures.

The JSON-lines control channel is private to the Go fixture helper. The receipt
contains observations, not Kaferry progress counters. SQLite keeps record identity
and duplicate history off the Python heap during repeated recovery runs.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys
import time
import traceback

from tools.qualification.broker_probe import logical


def fingerprint(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def checksum(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


INITIALIZATION_ERRORS = ('_TIMED_OUT', '_TRANSPORT', '_WAIT_COORD', 'NOT_COORDINATOR')


def transient_initialization(error):
    from confluent_kafka import KafkaError, KafkaException
    if isinstance(error, KafkaException) and error.args:
        return next((name for name in INITIALIZATION_ERRORS if error.args[0].code() == getattr(KafkaError, name)), None)
    return None


class Reconciliation:
    def __init__(self, path, partitions):
        self.db = sqlite3.connect(path, timeout=0)
        if self.db.execute('PRAGMA journal_mode=DELETE').fetchone()[0] != 'delete':
            raise ValueError('independent index requires rollback-journal support')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE source (identity TEXT PRIMARY KEY, partition INTEGER, position INTEGER,
                                 copies INTEGER DEFAULT 0, last_destination INTEGER);
            CREATE UNIQUE INDEX source_position ON source(partition, position);
            CREATE INDEX source_missing ON source(partition, copies, position);
            CREATE TABLE duplicates (partition INTEGER, first_position INTEGER, second_position INTEGER);
        ''')
        self.partitions = partitions

    def source(self, partition, offset, record):
        try:
            self.db.execute('INSERT INTO source(identity, partition, position) VALUES (?, ?, ?)',
                            (fingerprint(record), partition, offset))
        except sqlite3.IntegrityError as error:
            raise ValueError('fault fixture needs unique full source records per partition') from error

    def destination(self, partition, offset, record):
        identity = fingerprint(record)
        found = self.db.execute('SELECT partition, copies, last_destination FROM source WHERE identity=?',
                                (identity,)).fetchone()
        if found is None or found[0] != partition:
            raise ValueError(f'unexpected or corrupted destination record at {partition}/{offset}')
        if found[1]:
            self.db.execute('INSERT INTO duplicates VALUES (?, ?, ?)', (partition, found[2], offset))
        self.db.execute('UPDATE source SET copies=copies+1, last_destination=? WHERE identity=?', (offset, identity))

    def prefix(self, partition):
        missing = self.db.execute('SELECT MIN(position) FROM source WHERE partition=? AND copies=0',
                                  (partition,)).fetchone()[0]
        if missing is not None:
            return missing
        return self.db.execute('SELECT COALESCE(MAX(position)+1, 0) FROM source WHERE partition=?',
                               (partition,)).fetchone()[0]

    def check(self, saved):
        result = []
        for partition, offset in enumerate(saved):
            prefix = self.prefix(partition)
            if offset > prefix:
                raise ValueError(f'unsafe saved progress: partition={partition} saved={offset} confirmed_prefix={prefix}')
            result.append(dict(partition=partition, saved=offset, confirmed_prefix=prefix))
        self.db.commit()
        return result

    def finish(self, windows, allow_empty=False):
        def inside(partition, offset):
            return any(window['destination_start'][partition] <= offset < window['destination_end'][partition]
                       for window in windows)
        bad = sum(not inside(partition, first) and not inside(partition, second)
                  for partition, first, second in self.db.execute('SELECT * FROM duplicates'))
        expected, verified, copies = self.db.execute(
            'SELECT COUNT(*), COALESCE(SUM(copies>0), 0), COALESCE(SUM(copies), 0) FROM source').fetchone()
        result = dict(implementation='librdkafka', expected=expected, verified=verified,
                      deliveries=copies, duplicates=copies-verified, missing=expected-verified,
                      corrupted=0, unexpected=0, unattributed_duplicates=bad,
                      partitions=[dict(partition=p, confirmed_prefix=self.prefix(p),
                          expected=self.db.execute('SELECT COUNT(*) FROM source WHERE partition=?', (p,)).fetchone()[0])
                          for p in range(self.partitions)])
        if expected != verified or bad or not allow_empty and not expected:
            raise ValueError('independent reconciliation failed: ' + json.dumps(result))
        return result


class KafkaReader:
    def __init__(self, brokers, topic, partitions, group):
        from confluent_kafka import Consumer, TopicPartition
        self.topic_partition = TopicPartition
        self.topic = topic
        self.partitions = partitions
        self.consumer = Consumer({'bootstrap.servers': brokers, 'group.id': group,
            'enable.auto.commit': False, 'isolation.level': 'read_committed', 'auto.offset.reset': 'error',
            'fetch.message.max.bytes': 1048576, 'queued.max.messages.kbytes': 1024,
            'socket.timeout.ms': 1000, 'fetch.wait.max.ms': 0})
        assigned = [TopicPartition(topic, p, 0) for p in range(partitions)]
        self.consumer.assign(assigned)
        self.consumer.pause(assigned)
        self.positions = [0] * partitions

    def ends(self):
        from confluent_kafka import KafkaError, KafkaException
        result = []
        for partition in range(self.partitions):
            offsets = self.consumer.get_watermark_offsets(self.topic_partition(self.topic, partition), timeout=1, cached=False)
            if offsets is None:
                raise KafkaException(KafkaError(KafkaError._TIMED_OUT, 'independent watermark query timed out'))
            result.append(offsets[1])
        return result

    def drain(self, ends, observe):
        deadline = time.monotonic() + 30
        while any(position < end for position, end in zip(self.positions, ends)):
            active = [self.topic_partition(self.topic, p) for p in range(self.partitions) if self.positions[p] < ends[p]]
            inactive = [self.topic_partition(self.topic, p) for p in range(self.partitions) if self.positions[p] >= ends[p]]
            if inactive:
                self.consumer.pause(inactive)
            self.consumer.resume(active)
            message = self.consumer.poll(0.05)
            if message is not None:
                if message.error():
                    from confluent_kafka import KafkaException
                    raise KafkaException(message.error())
                partition, offset = message.partition(), message.offset()
                if offset >= ends[partition]:
                    # A concurrent append is outside this observation. Rewind it;
                    # never let later output repair this sample's confirmed prefix.
                    self.consumer.seek(self.topic_partition(self.topic, partition, offset))
                    self.consumer.pause([self.topic_partition(self.topic, partition)])
                    continue
                observe(partition, offset, dict(partition=partition,
                    **logical(message.key(), message.value(), message.headers() or [], message.timestamp()[1])))
                self.positions[partition] = offset + 1
            if time.monotonic() >= deadline:
                raise ValueError('independent consumer did not reach captured end offsets within 30s')
        # With zero broker-side fetch wait, pause between fixed snapshots rather
        # than continuously sending empty fetches during the one-second cadence.
        self.consumer.pause([self.topic_partition(self.topic, p) for p in range(self.partitions)])


class Oracle:
    def __init__(self, args):
        from confluent_kafka import Consumer, TopicPartition
        from tools.ci.suites import fault_timeout_catalog
        from tools.qualification.qualify import identity_platforms
        self.args = args
        self.directory = args.output
        self.directory.mkdir(parents=True, exist_ok=False)
        self.timeout_tier = os.environ.get('KAFERRY_FAULT_TIER', 'full')
        registered = fault_timeout_catalog(self.timeout_tier).get(args.case)
        if registered is None:
            raise ValueError('fault case is not registered in the timeout catalog')
        self.timeout_basis = {key: milliseconds/1000 for key, milliseconds in registered.items()}
        self.binary_digest = checksum(args.binary)
        self.platforms = identity_platforms(str(args.binary), os.environ.get('GO', 'go'))
        self.source_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        self.index = Reconciliation(self.directory / 'records.sqlite', args.partitions)
        self.source = KafkaReader(args.source, args.source_topic, args.partitions, args.group+'-oracle-source')
        self.destination = KafkaReader(args.destination, args.destination_topic, args.partitions, args.group+'-oracle-destination')
        self.progress = Consumer({'bootstrap.servers': args.source, 'group.id': args.group,
                                 'enable.auto.commit': False, 'socket.timeout.ms': 1000, 'fetch.wait.max.ms': 0})
        self.parts = [TopicPartition(args.source_topic, p) for p in range(args.partitions)]
        self.samples = (self.directory / 'samples.jsonl').open('w')
        self.windows = []
        self.started = time.time()
        self.checked = 0
        self.gaps = 0
        self.error = None
        self.last_sample = 0

    def initialize(self, deadline_at):
        """Establish a fresh fixture baseline before the caller may start writers."""
        started = time.time()
        deadline = time.monotonic() + max(0, deadline_at-started)
        measured = self.samples
        attempts = 0
        with (self.directory/'bootstrap.jsonl').open('w') as setup:
            self.samples = setup
            try:
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('independent observer initialization exceeded its existing ready deadline')
                    attempts += 1
                    attempt_started = time.time()
                    try:
                        self.sample(reason='bootstrap')
                        if any(part['saved'] or part['confirmed_prefix'] for part in self.final_parts) or any(self.destination.positions):
                            raise ValueError('observer bootstrap requires fresh saved progress and empty destination')
                        if time.monotonic() >= deadline:
                            raise TimeoutError('independent observer initialization exceeded its existing ready deadline')
                    except Exception as error:
                        transient = transient_initialization(error)
                        self.emit(dict(kind='initialization-error', attempt=attempts, started_at=attempt_started,
                                       finished_at=time.time(), driver_error=transient, error=str(error)))
                        if not transient:
                            raise
                        time.sleep(min(1, max(0, deadline-time.monotonic())))
                    else:
                        ready_at = time.time()
                        self.emit(dict(kind='initialization-ready', attempt=attempts, started_at=attempt_started,
                                       finished_at=ready_at, deadline_at=deadline_at))
                        break
            except BaseException as error:
                self.emit(dict(kind='initialization-failure', started_at=started, finished_at=time.time(), error=str(error)))
                raise
            finally:
                self.samples = measured
                self.checked, self.gaps = 0, 0
        self.bootstrap = dict(observations='bootstrap.jsonl', sha256=checksum(self.directory/'bootstrap.jsonl'),
                              started_at=started, ready_at=ready_at, deadline_at=deadline_at, attempts=attempts)
        self.started = ready_at
        return dict(ready=True)

    def emit(self, value):
        self.samples.write(json.dumps(value, sort_keys=True)+'\n')
        self.samples.flush()

    def sample(self, reason='periodic'):
        started = time.time()
        try:
            saved = self.progress.committed(self.parts, timeout=1)
            for part in saved:
                if part.error is not None:
                    from confluent_kafka import KafkaException
                    raise KafkaException(part.error)
            offsets = [max(0, part.offset) for part in saved]
            saved_observed_at = time.time()
            # Read saved positions before capturing destination ends. Draining to
            # those fixed ends accommodates verifier lag, not future deliveries.
            destination_snapshot_started_at = time.time()
            destination_ends = self.destination.ends()
            destination_snapshot_finished_at = time.time()
            source_ends = self.source.ends()
        except Exception as error:
            # A declared broker outage makes these observations unavailable.
            # Record the gap; it is never counted as a passing safety sample.
            now = time.time()
            if not any(window.get('unavailable') and
                       (window.get('healed_at') is None or now < window['recovery_until']) for window in self.windows):
                raise
            self.gaps += 1
            self.emit(dict(kind='unavailable', started_at=started, finished_at=time.time(), error=str(error)))
            self.last_sample = time.monotonic()
            return False
        self.source.drain(source_ends, self.index.source)
        self.destination.drain(destination_ends, self.index.destination)
        parts = self.index.check(offsets)
        self.final_parts = parts
        self.checked += 1
        self.emit(dict(kind='sample', reason=reason, started_at=started, finished_at=time.time(), partitions=parts,
                       source_ends=source_ends, destination_ends=destination_ends,
                       saved_observed_at=saved_observed_at,
                       destination_snapshot_started_at=destination_snapshot_started_at,
                       destination_snapshot_finished_at=destination_snapshot_finished_at))
        now = time.time()
        for window in self.windows:
            if (window.get('recovery_until') is not None and destination_snapshot_started_at >= window['recovery_until']
                    and 'destination_end' not in window):
                window['destination_end'] = destination_ends
                window['closed_at'] = now
                window['destination_end_observed_at'] = destination_snapshot_started_at
        self.last_sample = time.monotonic()
        return True

    def command(self, command):
        kind = command['command']
        if kind == 'fault-start':
            if command['timeout_basis'] != self.timeout_basis or command['recovery_seconds'] != sum(self.timeout_basis.values()):
                raise ValueError('fault recovery timeout budgets differ from its registered scenario')
            self.sample()
            window = dict(id=len(self.windows), kind=command['kind'], target=command['target'],
                          destination_start=self.destination.ends(), started_at=time.time(),
                          unavailable=command.get('unavailable', False), recovery_seconds=command['recovery_seconds'],
                          timeout_basis=command['timeout_basis'], start_observation=command['observation'])
            self.windows.append(window)
            return dict(window=window['id'])
        if kind == 'fault-injected':
            self.windows[command['window']].setdefault('injections', []).append(
                dict(at=time.time(), kind=command['kind'], target=command['target'], observation=command['observation']))
            self.sample()
        elif kind == 'fault-healed':
            window = self.windows[command['window']]
            window.update(healed_at=time.time(), heal_observation=command['observation'])
            window['recovery_until'] = window['healed_at'] + window['recovery_seconds']
            self.sample()
        elif kind == 'sample':
            self.sample()
        elif kind == 'finish':
            if command.get('writers_stopped') is not True:
                raise ValueError('final snapshot requires stopped fixture writers')
            if not self.args.allow_empty and not command.get('writer_observations'):
                raise ValueError('final snapshot requires observed exits of registered fixture writers')
            writers_stopped_at = time.time()
            for window in self.windows:
                if not window.get('injections') or 'healed_at' not in window:
                    raise ValueError('fault missing actual injection or heal observation')
            # Process exit does not retract queued kernel/proxy output. Observe
            # the entire configured recovery allowance before fixing its end cut.
            while any('destination_end' not in window for window in self.windows):
                self.sample()
                if any('destination_end' not in window for window in self.windows):
                    time.sleep(1)
            # One fresh healthy observation beyond all recovery cuts is required.
            if not self.sample(reason='post-recovery'):
                raise ValueError('final safety observation was unavailable')
            for window in self.windows:
                window['recovery_observation_complete'] = True
            reconciliation = self.index.finish(self.windows, self.args.allow_empty)
            for part in self.final_parts:
                if part['saved'] != part['confirmed_prefix']:
                    raise ValueError('final saved progress does not equal the completely reconciled source prefix')
            reconciliation['final_progress'] = self.final_parts
            if checksum(self.args.binary) != self.binary_digest:
                raise ValueError('candidate binary changed during independent observation')
            self.samples.close()
            finished = time.time()
            report = dict(schema=1, id=self.args.case, status='passed',
                started_at=self.started, finished_at=finished, duration_seconds=finished-self.started,
                source_commit=self.source_commit, binary_sha256=self.binary_digest,
                **self.platforms,
                bootstrap=self.bootstrap,
                execution='checkout adapter probe' if self.args.allow_empty else 'candidate CLI',
                timeout_tier=self.timeout_tier, writers_stopped_at=writers_stopped_at,
                writer_observations=command.get('writer_observations', []),
                parameters=dict(source_partitions=self.args.partitions, routing='PRESERVE'),
                safety=dict(interval_seconds=1, ordering='saved-before-destination-snapshot',
                    samples=self.checked, unavailable=self.gaps,
                    observations='samples.jsonl', sha256=checksum(self.directory/'samples.jsonl')),
                fault_events=self.windows, independent_reconciliation=reconciliation)
            (self.directory/'case.json').write_text(json.dumps(report, indent=2)+'\n')
            return dict(finished=True)
        else:
            raise ValueError('unknown oracle command: '+kind)
        return {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'destination', 'source-topic', 'destination-topic', 'group', 'case'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--partitions', type=int, required=True)
    parser.add_argument('--ready-deadline', type=float, required=True)
    parser.add_argument('--allow-empty', action='store_true')
    args = parser.parse_args()
    oracle = Oracle(args)
    ready = oracle.initialize(args.ready_deadline)
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin, selectors.EVENT_READ)
    print(json.dumps(ready), flush=True)
    while True:
        events = selector.select(max(0, 1-(time.monotonic()-oracle.last_sample)))
        if events:
            line = sys.stdin.readline()
            if not line:
                return
            try:
                if oracle.error:
                    raise ValueError(oracle.error)
                result = oracle.command(json.loads(line))
                print(json.dumps(result), flush=True)
                if result.get('finished'):
                    return
            except Exception as error:
                traceback.print_exc(file=sys.stderr)
                print(json.dumps(dict(error=str(error))), flush=True)
                return 1
        else:
            try:
                oracle.sample()
            except Exception as error:
                traceback.print_exc(file=sys.stderr)
                oracle.error = str(error)
                oracle.emit(dict(kind='failure', at=time.time(), error=str(error)))
                oracle.last_sample = time.monotonic()


if __name__ == '__main__':
    raise SystemExit(main())
