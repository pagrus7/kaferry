"""Fault evidence must detect unsafe saves, every bad copy and unscoped replay."""

import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.ci.suites import fault_timeout_catalog
from tools.qualification.broker_probe import logical
from tools.qualification.fault_oracle import Oracle, Reconciliation, checksum
from tools.qualification.fault_results import RECOVERY_CASES, collect, validate_bootstrap, validate_case


class DriverError:
    _TIMED_OUT, _TRANSPORT, _WAIT_COORD = -185, -195, -180
    NOT_COORDINATOR = 16

    def __init__(self, code, message='driver failure'):
        self.value, self.message = code, message

    def code(self):
        return self.value


class DriverException(Exception):
    pass


DRIVER = SimpleNamespace(KafkaError=DriverError, KafkaException=DriverException)


def record(value):
    return dict(partition=0, **logical(b'key', bytes([value]), [('dup', None), ('dup', b'')], 1234))


class Reader:
    def __init__(self, records, order, name):
        self.records = records
        self.order, self.name = order, name
        self.positions = [0]
        self.snapshot = len(records)

    def ends(self):
        self.order.append(self.name+'-ends')
        return [self.snapshot]

    def drain(self, ends, observe):
        self.order.append(self.name+'-drain')
        while self.positions[0] < ends[0]:
            offset = self.positions[0]
            observe(0, offset, self.records[offset])
            self.positions[0] += 1


class Progress:
    def __init__(self, offset, order):
        self.offset, self.order = offset, order

    def committed(self, parts, timeout):
        self.order.append('saved')
        return [SimpleNamespace(offset=self.offset, error=None)]


class FaultOracleTest(unittest.TestCase):
    def oracle(self, path, saved=1, source=None, destination=None):
        oracle = Oracle.__new__(Oracle)
        oracle.directory = path
        oracle.args = SimpleNamespace(case='recovery/kill', partitions=1, allow_empty=False, binary=path/'binary')
        oracle.timeout_basis = {name: ms/1000 for name, ms in fault_timeout_catalog()[oracle.args.case].items()}
        oracle.platforms = dict(binary_platform='fixture/fixture', execution_platform='fixture/fixture')
        oracle.args.binary.write_bytes(b'candidate')
        oracle.binary_digest = checksum(oracle.args.binary)
        oracle.source_commit = 'a'*40
        oracle.index = Reconciliation(path/'records.sqlite', 1)
        oracle.order = []
        oracle.source = Reader(source if source is not None else [record(0), record(1)], oracle.order, 'source')
        oracle.destination = Reader(destination if destination is not None else [record(0)], oracle.order, 'destination')
        oracle.progress = Progress(saved, oracle.order)
        oracle.parts = []
        oracle.samples = (path/'samples.jsonl').open('w')
        self.addCleanup(oracle.samples.close)
        self.addCleanup(oracle.index.db.close)
        oracle.windows, oracle.checked, oracle.gaps = [], 0, 0
        oracle.started, oracle.last_sample, oracle.error = 1, 0, None
        baseline = dict(kind='sample', reason='bootstrap', started_at=0.1, finished_at=0.2,
            saved_observed_at=0.1, destination_snapshot_started_at=0.1, destination_snapshot_finished_at=0.2,
            source_ends=[oracle.source.snapshot], destination_ends=[0],
            partitions=[dict(partition=0, saved=0, confirmed_prefix=0)])
        ready = dict(kind='initialization-ready', attempt=1, started_at=0.1, finished_at=1, deadline_at=10)
        (path/'bootstrap.jsonl').write_text(json.dumps(baseline)+'\n'+json.dumps(ready)+'\n')
        oracle.bootstrap = dict(observations='bootstrap.jsonl', sha256=checksum(path/'bootstrap.jsonl'),
                                started_at=0, ready_at=1, deadline_at=10, attempts=1)
        return oracle

    def test_initialization_records_transient_setup_then_exact_baseline_before_ready(self):
        # Given a cold coordinator and preloaded source data, with no copier yet.
        for name in ('_TIMED_OUT', 'NOT_COORDINATOR'):
            with self.subTest(error=name):
                self.check_initialization_error_before_ready(name)

    def check_initialization_error_before_ready(self, name):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = self.oracle(root, saved=0, destination=[])
            clock = [1.0]
            def query(parts, timeout):
                self.assertEqual(1, timeout)
                if clock[0] == 1:
                    clock[0] += 1
                    raise DriverException(DriverError(getattr(DriverError, name)))
                return [SimpleNamespace(offset=0, error=None)]
            with patch.dict(sys.modules, confluent_kafka=DRIVER), \
                 patch('tools.qualification.fault_oracle.time.time', side_effect=lambda: clock[0]), \
                 patch('tools.qualification.fault_oracle.time.monotonic', side_effect=lambda: clock[0]), \
                 patch('tools.qualification.fault_oracle.time.sleep', side_effect=lambda seconds: clock.__setitem__(0, clock[0]+seconds)), \
                 patch.object(oracle.progress, 'committed', side_effect=query):
                # When setup completes, then only its fixed-cut zero-progress
                # baseline permits ready; failed setup isn't a measured sample.
                self.assertEqual(dict(ready=True), oracle.initialize(10))
            rows = [json.loads(line) for line in (root/'bootstrap.jsonl').read_text().splitlines()]
            self.assertEqual(['initialization-error', 'sample', 'initialization-ready'], [row['kind'] for row in rows])
            self.assertEqual(name, rows[0]['driver_error'])
            self.assertEqual(1, rows[0]['finished_at']-rows[0]['started_at'])
            self.assertEqual([2], rows[1]['source_ends'])
            self.assertEqual([0], rows[1]['destination_ends'])
            self.assertEqual((0, 0), (oracle.checked, oracle.gaps))
            self.assertEqual('', (root/'samples.jsonl').read_text())
            self.assertEqual(3, oracle.started)
            validate_bootstrap(dict(bootstrap=oracle.bootstrap, started_at=oracle.started), root, {0})
            # Then the receipt validator shares the narrow driver allowlist;
            # a rechecksummed unknown setup failure cannot qualify this proof.
            rows[0]['driver_error'] = 'UNKNOWN_ERROR'
            proof = root/'bootstrap.jsonl'
            proof.write_text(''.join(json.dumps(row)+'\n' for row in rows))
            oracle.bootstrap['sha256'] = checksum(proof)
            with self.assertRaisesRegex(ValueError, 'invalid transient observer initialization evidence'):
                validate_bootstrap(dict(bootstrap=oracle.bootstrap, started_at=oracle.started), root, {0})

    def complete_case(self, path, id='recovery/kill'):
        oracle = self.oracle(path, source=[record(0)])
        oracle.args.case = id
        oracle.timeout_basis = {name: ms/1000 for name, ms in fault_timeout_catalog()[id].items()}
        recovery = sum(oracle.timeout_basis.values())
        with patch('tools.qualification.fault_oracle.time.time', return_value=10):
            window = oracle.command(dict(command='fault-start', kind='kill', target=dict(worker='fixture'),
                recovery_seconds=recovery, timeout_basis=oracle.timeout_basis, observation='prefix 1'))['window']
        with patch('tools.qualification.fault_oracle.time.time', return_value=11):
            oracle.command(dict(command='fault-injected', window=window, kind='kill', target=dict(worker='fixture'), observation='process exited'))
        with patch('tools.qualification.fault_oracle.time.time', return_value=12):
            oracle.command(dict(command='fault-healed', window=window, observation='replacement owns partition 0'))
        with patch('tools.qualification.fault_oracle.time.time', return_value=13+recovery):
            oracle.command(dict(command='finish', writers_stopped=True,
                writer_observations=[dict(pid=1, exit_code=1, observation='process exited')]))
        return path/'case.json'

    def test_receipt_contains_event_derived_proof_and_checked_partition_coverage(self):
        # Given an observed injection and heal with configured timeout budgets.
        with tempfile.TemporaryDirectory() as directory:
            path = self.complete_case(Path(directory))
            # When validating, then retain real observations and require the
            # full recovery horizon plus healthy post-recovery observations.
            case = validate_case(path)
            event = case['fault_events'][0]
            self.assertEqual(51, event['recovery_until'])
            self.assertTrue(event['recovery_observation_complete'])
            self.assertEqual(52, case['writers_stopped_at'])
            self.assertEqual(5, case['safety']['partitions'][0]['samples'])
            self.assertEqual((10, 52), (case['safety']['first_observed_at'], case['safety']['last_observed_at']))

    def test_exact_registered_budgets_are_required_even_when_the_sum_is_consistent(self):
        # Given a valid recovery/kill receipt with its configured 6+30+3 seconds.
        with tempfile.TemporaryDirectory() as temporary:
            path = self.complete_case(Path(temporary))
            original = json.loads(path.read_text())
            budgets = [
                {'session.timeout.ms': 6, 'shutdown_timeout': 3},
                {'session.timeout.ms': 6, 'delivery_timeout': 29, 'shutdown_timeout': 3},
                {'session.timeout.ms': 6, 'delivery_timeout': 31, 'shutdown_timeout': 3},
                {'session.timeout.ms': 36, 'shutdown_timeout': 3},
                {'session.timeout.ms': 6, 'delivery_timeout': 30, 'shutdown_timeout': 3, 'extra': 1},
            ]
            for basis in budgets:
                with self.subTest(basis=basis):
                    altered = json.loads(json.dumps(original))
                    event = altered['fault_events'][0]
                    event.update(timeout_basis=basis, recovery_seconds=sum(basis.values()),
                                 recovery_until=event['healed_at']+sum(basis.values()))
                    path.write_text(json.dumps(altered))
                    # When keys are omitted/changed or values shrink/grow with
                    # a matching sum, then exact scenario binding still rejects.
                    with self.assertRaisesRegex(ValueError, 'registered scenario'):
                        validate_case(path)
            altered = dict(original, id='unregistered-case')
            path.write_text(json.dumps(altered))
            with self.assertRaisesRegex(ValueError, 'not registered'):
                validate_case(path)

    def test_producer_rejects_unregistered_budget_before_sampling_or_injection(self):
        # Given a known case and a shortened but internally consistent budget.
        with tempfile.TemporaryDirectory() as temporary:
            oracle = self.oracle(Path(temporary))
            with patch.object(oracle, 'sample') as sample:
                # When starting its window, then fail before reading a baseline
                # or permitting a fault under the incorrect recovery allowance.
                with self.assertRaisesRegex(ValueError, 'registered scenario'):
                    oracle.command(dict(command='fault-start', kind='kill', target=dict(worker='fixture'),
                        recovery_seconds=9, timeout_basis={'session.timeout.ms': 6, 'shutdown_timeout': 3},
                        observation='prefix 1'))
            sample.assert_not_called()
            self.assertEqual([], oracle.windows)

    def test_complete_cycle_requires_matching_binary_and_receipts(self):
        # Given every stable recovery case and its independent sample receipt.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for id in RECOVERY_CASES:
                path = root/'fault-oracle'/id.replace('/', '-')/'observations'
                path.mkdir(parents=True)
                paths.append(self.complete_case(path, id))
            original = json.loads(paths[0].read_text())
            (root/'recovery.json').write_text(json.dumps(dict(passed=True, completed_cycles=1)))
            identity = dict(source_commit=original['source_commit'], binary_sha256=original['binary_sha256'],
                            binary_platform='fixture/fixture', execution_platform='fixture/fixture')
            # When collecting, then reference the actual sample paths and retain
            # separate binary/execution platform identity from the outer receipt.
            report = collect(root, 'recovery', identity)
            self.assertEqual('passed', report['status'])
            self.assertTrue(all((root/case['safety']['observations']).is_file() for case in report['cases']))
            self.assertEqual('fixture/fixture', report['binary_platform'])
            # Then missing repeated cycles and a different binary cannot reuse
            # these successful observations as qualification for another run.
            with self.assertRaisesRegex(ValueError, 'does not match'):
                collect(root, 'recovery', dict(identity, binary_sha256='b'*64))
            changed = json.loads(paths[0].read_text()); changed['binary_sha256'] = 'b'*64
            paths[0].write_text(json.dumps(changed))
            with self.assertRaisesRegex(ValueError, 'mixes'):
                collect(root, 'recovery')
            (root/'recovery.json').write_text(json.dumps(dict(passed=True, completed_cycles=2)))
            with self.assertRaisesRegex(ValueError, 'completed recovery cycles'):
                collect(root, 'recovery')


if __name__ == '__main__':
    unittest.main()
