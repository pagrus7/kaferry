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
from tools.qualification.fault_oracle import KafkaReader, Oracle, Reconciliation, checksum
from tools.qualification.fault_results import RECOVERY_CASES, collect, configure, validate_bootstrap, validate_case


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
    def test_wire_reader_normalizes_absent_headers_and_preserves_header_values(self):
        # Given librdkafka's None for absent headers, alongside explicit empty
        # collections and distinct null/empty values within real header lists.
        for headers in (None, [], [('dup', None), ('dup', b'')]):
            with self.subTest(headers=headers):
                reader = KafkaReader.__new__(KafkaReader)
                reader.topic, reader.partitions, reader.positions = 'fixture', 1, [0]
                reader.topic_partition = lambda *args: args
                message = SimpleNamespace(error=lambda: None, partition=lambda: 0, offset=lambda: 0,
                    key=lambda: b'key', value=lambda: b'value', headers=lambda: headers,
                    timestamp=lambda: (1, 1234))
                reader.consumer = SimpleNamespace(pause=lambda parts: None, resume=lambda parts: None,
                                                   poll=lambda seconds: message)
                received = []
                # When the actual wire-reader path drains its fixed cut, then
                # headerless data is valid and individual header values survive.
                reader.drain([1], lambda part, offset, record: received.append((part, offset, record)))
                expected = dict(partition=0, **logical(b'key', b'value', headers or [], 1234))
                self.assertEqual([(0, 0, expected)], received)
                index = self.reconciliation()
                index.source(0, 0, expected)
                index.destination(*received[0])
                self.assertEqual([dict(partition=0, saved=1, confirmed_prefix=1)], index.check([1]))

    def reconciliation(self, partitions=1):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        index = Reconciliation(Path(directory.name)/'records.sqlite', partitions)
        self.addCleanup(index.db.close)
        return index

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

    def test_initialization_never_retries_unsafe_progress_or_changed_records(self):
        # Given an unsafe save, or destination bytes that do not match the source.
        for saved, destination in [(1, []), (0, [record(99)])]:
            with self.subTest(saved=saved), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                oracle = self.oracle(root, saved=saved, destination=destination)
                with patch.dict(sys.modules, confluent_kafka=DRIVER), \
                     patch.object(oracle.progress, 'committed', wraps=oracle.progress.committed) as query:
                    # When initialization observes real invalid proof, then it
                    # fails on that attempt rather than waiting for later output.
                    with self.assertRaisesRegex(ValueError, 'unsafe|corrupted'):
                        oracle.initialize(10**10)
                self.assertEqual(1, query.call_count)
                rows = [json.loads(line) for line in (root/'bootstrap.jsonl').read_text().splitlines()]
                self.assertEqual(['initialization-error', 'initialization-failure'], [row['kind'] for row in rows])

    def test_initialization_deadline_and_nontransient_failures_never_report_ready(self):
        # Given the existing ready deadline or a nontransient/data failure.
        for failure, deadline, expected_error in [
                (DriverException(DriverError(DriverError._TIMED_OUT)), 3, TimeoutError),
                (DriverException(DriverError(DriverError.NOT_COORDINATOR)), 3, TimeoutError),
                (DriverException(DriverError(29)), 10, DriverException),
                (DriverException(DriverError(9999)), 10, DriverException),
                (ValueError('corrupt fixture record'), 10, ValueError)]:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                oracle = self.oracle(root, saved=0, destination=[])
                clock = [1.0]
                def query(parts, timeout):
                    clock[0] += timeout
                    raise failure
                with patch.dict(sys.modules, confluent_kafka=DRIVER), \
                     patch('tools.qualification.fault_oracle.time.time', side_effect=lambda: clock[0]), \
                     patch('tools.qualification.fault_oracle.time.monotonic', side_effect=lambda: clock[0]), \
                     patch('tools.qualification.fault_oracle.time.sleep', side_effect=lambda seconds: clock.__setitem__(0, clock[0]+seconds)), \
                     patch.object(oracle.progress, 'committed', side_effect=query) as query_mock:
                    # When initialization cannot establish a valid baseline,
                    # then stop at the bound without starting measured proof.
                    with self.assertRaises(expected_error):
                        oracle.initialize(deadline)
                self.assertEqual(1, query_mock.call_count)
                if expected_error is TimeoutError:
                    self.assertEqual(deadline, clock[0])
                rows = [json.loads(line) for line in (root/'bootstrap.jsonl').read_text().splitlines()]
                self.assertNotIn('initialization-ready', [row['kind'] for row in rows])
                self.assertEqual('initialization-failure', rows[-1]['kind'])
                self.assertEqual(0, oracle.checked)

    def test_same_driver_error_during_a_measured_sample_is_fatal_without_retry(self):
        # Given the same driver errors after readiness and outside a fault.
        for code in (DriverError._TIMED_OUT, DriverError.NOT_COORDINATOR):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                oracle = self.oracle(Path(temporary))
                failure = DriverException(DriverError(code))
                with patch.object(oracle.progress, 'committed', side_effect=failure) as query:
                    # When sampling the run, then the error propagates immediately
                    # rather than becoming initialization, a gap or passing proof.
                    with self.assertRaises(DriverException) as raised:
                        oracle.sample()
                self.assertIs(failure, raised.exception)
                query.assert_called_once_with(oracle.parts, timeout=1)
                self.assertEqual((0, 0), (oracle.checked, oracle.gaps))

    def test_wire_timeout_remains_failure_and_snapshot_readers_pause_at_the_cut(self):
        # Given the documented None watermark timeout and an idle snapshot reader.
        reader = KafkaReader.__new__(KafkaReader)
        reader.topic, reader.partitions, reader.positions = 'fixture', 1, [0]
        reader.topic_partition = lambda *args: args
        pauses = []
        reader.consumer = SimpleNamespace(get_watermark_offsets=lambda *args, **kwargs: None,
                                           pause=lambda parts: pauses.append(parts))
        with patch.dict(sys.modules, confluent_kafka=DRIVER):
            # When the API times out, then report a driver timeout; when no data
            # is needed, pause rather than spin empty fetches between samples.
            with self.assertRaises(DriverException):
                reader.ends()
        reader.drain([0], lambda *args: self.fail('empty cut must not consume output'))
        self.assertEqual([[('fixture', 0)]], pauses)

    def test_lagging_independent_consumer_catches_up_only_to_captured_output(self):
        # Given safe saved progress while the independent consumer has read nothing.
        with tempfile.TemporaryDirectory() as directory:
            oracle = self.oracle(Path(directory))
            # When sampling, then consume captured output before checking, without
            # comparing stale observer state to an asynchronously advancing save.
            oracle.sample()
            self.assertEqual(['saved', 'destination-ends', 'source-ends', 'source-drain', 'destination-drain'], oracle.order)
            self.assertEqual([dict(partition=0, saved=1, confirmed_prefix=1)], oracle.final_parts)

    def test_later_output_cannot_repair_an_unsafe_saved_snapshot(self):
        # Given saved position 2, but captured output contains only position 0.
        with tempfile.TemporaryDirectory() as directory:
            oracle = self.oracle(Path(directory), saved=2, destination=[record(0), record(1)])
            oracle.destination.snapshot = 1
            # When later output is available beyond the captured end, then the
            # exact inequality still fails; the observer must never drain to 2.
            with self.assertRaisesRegex(ValueError, 'saved=2 confirmed_prefix=1'):
                oracle.sample()
            self.assertEqual([1], oracle.destination.positions)

    def test_prefix_requires_every_partition_position_not_just_a_count(self):
        # Given output for position 1 but a missing first input.
        index = self.reconciliation()
        index.source(0, 0, record(0)); index.source(0, 1, record(1))
        index.destination(0, 0, record(1))
        # When saved progress advances, then a gap cannot pass as one receipt.
        with self.assertRaisesRegex(ValueError, 'confirmed_prefix=0'):
            index.check([1])

    def test_unsafe_save_on_one_partition_cannot_hide_behind_another(self):
        # Given two partitions with only partition zero confirmed at output.
        index = self.reconciliation(2)
        index.source(0, 0, record(0))
        index.source(1, 0, dict(record(1), partition=1))
        index.destination(0, 0, record(0))
        # When both group positions advance, then partition one's unsafe
        # save fails independently of partition zero's correct progress.
        with self.assertRaisesRegex(ValueError, 'partition=1 saved=1 confirmed_prefix=0'):
            index.check([1, 1])

    def test_identical_source_records_are_rejected_instead_of_ambiguous_matching(self):
        # Given a fixture identity already used at another source position.
        index = self.reconciliation()
        index.source(0, 0, record(0))
        # When seeding a second identical record, then qualification refuses
        # ambiguity that could disguise a missing input as duplicate output.
        with self.assertRaisesRegex(ValueError, 'unique full source records'):
            index.source(0, 1, record(0))

    def test_every_repeated_copy_is_checked_for_corruption(self):
        # Given a correct first receipt, when each later field is corrupted,
        # then the independent fingerprint rejects every changed copy.
        index = self.reconciliation()
        index.source(0, 0, record(0))
        index.destination(0, 0, record(0))
        for field, value in [('key', None), ('value', ''), ('timestamp_ms', 999),
                             ('headers', [('dup', ''), ('dup', None)]), ('partition', 1)]:
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'corrupted'):
                index.destination(0, 1, dict(record(0), **{field: value}))

    def test_duplicate_offset_cuts_ignore_observer_lag_and_check_consecutive_pairs(self):
        # Given delayed observation of output before, during and after one fault.
        index = self.reconciliation()
        index.source(0, 0, record(0))
        for offset in range(3):
            index.destination(0, offset, record(0))
        window = dict(destination_start=[1], destination_end=[2])
        # When classifying by destination offset cuts, then a receipt in the
        # window permits its adjacent pair regardless of when it was consumed.
        self.assertEqual(2, index.finish([window])['duplicates'])
        index.destination(0, 3, record(0))
        # Then a second pair wholly after recovery fails even though the
        # original first receipt had an earlier fault-adjacent duplicate.
        with self.assertRaisesRegex(ValueError, 'unattributed_duplicates.*1'):
            index.finish([window])

    def test_unavailable_observation_is_a_gap_and_cannot_finish(self):
        # Given a declared source outage and a failed saved-offset request.
        with tempfile.TemporaryDirectory() as directory:
            oracle = self.oracle(Path(directory))
            oracle.windows = [dict(unavailable=True, injections=[dict(at=0)], healed_at=0,
                                   recovery_until=10**20, destination_end=[1])]
            with patch.object(oracle.progress, 'committed', side_effect=TimeoutError('broker unavailable')):
                # When sampling, then record an explicit gap rather than a pass.
                self.assertFalse(oracle.sample())
                self.assertEqual((0, 1), (oracle.checked, oracle.gaps))
                with self.assertRaisesRegex(ValueError, 'final safety observation was unavailable'):
                    oracle.command(dict(command='finish', writers_stopped=True,
                        writer_observations=[dict(pid=1, exit_code=1, observation='process exited')]))

    def test_no_fault_precondition_still_reconciles_and_rejects_unsafe_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            oracle = self.oracle(root, source=[record(0)])
            # Given no injected fault, finish the real independent observer after
            # a normal writer exit; only the explicit precondition reader accepts it.
            with patch('tools.qualification.fault_oracle.time.time', return_value=10):
                oracle.sample()
            with patch('tools.qualification.fault_oracle.time.time', return_value=11):
                oracle.command(dict(command='finish', writers_stopped=True,
                                    writer_observations=[dict(pid=1, exit_code=0, observation='process exited')]))
            path = root/'case.json'
            with self.assertRaisesRegex(ValueError, 'no observed fault'):
                validate_case(path)
            self.assertEqual([], validate_case(path, require_fault=False)['fault_events'])
            # When an independent sample is unsafe even with fresh checksums,
            # then the no-fault precondition cannot downgrade that safety error.
            rows = [json.loads(line) for line in (root/'samples.jsonl').read_text().splitlines()]
            rows[0]['partitions'][0]['saved'] = 2
            (root/'samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
            receipt = json.loads(path.read_text())
            receipt['safety']['sha256'] = checksum(root/'samples.jsonl')
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'unsafe saved progress'):
                validate_case(path, require_fault=False)

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

    def reject_mutations(self, root, section, mutations):
        path = self.complete_case(root)
        original = json.loads(path.read_text())
        observations = root/original[section]['observations']
        rows = [json.loads(line) for line in observations.read_text().splitlines()]
        for number, mutate in enumerate(mutations):
            with self.subTest(mutation=number):
                case, modified = json.loads(json.dumps(original)), json.loads(json.dumps(rows))
                mutate(case, modified)
                observations.write_text(''.join(json.dumps(row)+'\n' for row in modified))
                case[section]['sha256'] = checksum(observations)
                path.write_text(json.dumps(case))
                with self.assertRaises(ValueError):
                    validate_case(path)

    def test_bootstrap_proof_rejects_missing_baseline_ready_and_changed_setup_state(self):
        # Given a completed case, when its separate setup proof is changed and
        # rechecksummed, then it cannot qualify an unready or nonfresh fixture.
        with tempfile.TemporaryDirectory() as temporary:
            self.reject_mutations(Path(temporary), 'bootstrap', [
                lambda c, r: r.pop(),
                lambda c, r: r.pop(0),
                lambda c, r: r[0]['partitions'][0].update(saved=1),
                lambda c, r: r[0].update(destination_ends=[1]),
                lambda c, r: c['bootstrap'].update(ready_at=11),
                lambda c, r: r[0].update(saved_observed_at=float('nan')),
            ])

    def test_collector_rejects_missing_cases_mismatches_and_tampered_samples(self):
        # Given real, complete case observations, when evidence is incomplete,
        # mixed or modified, then none can produce a successful aggregate.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root/'fault-oracle/case/observations'
            path.mkdir(parents=True)
            case = self.complete_case(path)
            with self.assertRaisesRegex(ValueError, 'cases missing'):
                collect(root, 'recovery')
            original = json.loads(case.read_text())
            for mutation, message in [
                    (lambda c: c['fault_events'][0].update(injections=[]), 'injection'),
                    (lambda c: c['fault_events'][0].update(recovery_observation_complete=False), 'recovery'),
                    (lambda c: c['independent_reconciliation'].update(missing=1), 'incomplete')]:
                mutated = json.loads(json.dumps(original)); mutation(mutated)
                case.write_text(json.dumps(mutated))
                with self.assertRaisesRegex(ValueError, message):
                    validate_case(case)
            case.write_text(json.dumps(original))
            with (path/'samples.jsonl').open('a') as samples:
                samples.write('{}\n')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                validate_case(case)

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

    def test_sample_proof_rejects_invalid_numbers_cuts_and_missing_phases(self):
        # Given a complete case, when numeric or phase proof is altered and its
        # checksum is renewed, then semantic validation still rejects it.
        with tempfile.TemporaryDirectory() as directory:
            self.reject_mutations(Path(directory), 'safety', [
                lambda c, r: c.update(finished_at=float('nan')),
                lambda c, r: c.update(started_at=True),
                lambda c, r: r[0].update(started_at=float('inf')),
                lambda c, r: r[0]['partitions'][0].update(saved=0.5),
                lambda c, r: r[0]['partitions'][0].update(confirmed_prefix=True),
                lambda c, r: r[0].update(destination_ends=[0]),
                lambda c, r: r[0]['partitions'].append(dict(r[0]['partitions'][0])),
                lambda c, r: r[0].update(partitions=[]),
                lambda c, r: r[0].update(saved_observed_at=r[0]['finished_at']+1),
                lambda c, r: r[0]['partitions'][0].update(saved=0, confirmed_prefix=0),
                lambda c, r: r[-1].update(reason='periodic'),
                lambda c, r: c['safety'].update(interval_seconds=2),
                lambda c, r: c.update(writers_stopped_at=c['finished_at']+1),
            ])

    def test_independent_observations_require_each_phase_and_reject_overlap_or_unavailable_baselines(self):
        # Given a real independent observer receipt with healthy, fault, recovery
        # and fresh post-recovery samples, retain coverage beside its record oracle.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = self.complete_case(root)
            original = json.loads(path.read_text())
            samples = root/'samples.jsonl'
            rows = [json.loads(line) for line in samples.read_text().splitlines()]
            phases = validate_case(path)['safety']['phase_observations'][0]
            self.assertTrue(all(phases.values()))
            event = original['fault_events'][0]
            injected = min(item['at'] for item in event['injections'])
            predicates = {
                'healthy': lambda row: row['finished_at'] < injected,
                'fault': lambda row: row['started_at'] >= injected and row['finished_at'] <= event['healed_at'],
                'recovery': lambda row: row['started_at'] >= event['healed_at'] and row['finished_at'] <= event['recovery_until'],
                'post-recovery': lambda row: row.get('reason') == 'post-recovery',
            }
            for phase, predicate in predicates.items():
                case = json.loads(json.dumps(original))
                retained = [row for row in rows if not predicate(row)]
                # When a phase disappears even with correct counts/checksums,
                # then the independent observation coverage gate rejects it.
                samples.write_text(''.join(json.dumps(row)+'\n' for row in retained))
                case['safety'].update(samples=len(retained), sha256=checksum(samples))
                path.write_text(json.dumps(case))
                with self.subTest(missing_phase=phase), self.assertRaises(ValueError):
                    validate_case(path)
            for unavailable in (False, True):
                case = json.loads(json.dumps(original))
                altered = json.loads(json.dumps(rows))
                if unavailable:
                    # An unavailable healthy baseline is outside every fault.
                    altered[0] = dict(kind='unavailable', started_at=rows[0]['started_at'], finished_at=rows[0]['finished_at'])
                    case['safety'].update(samples=len(rows)-1, unavailable=1)
                else:
                    altered[1]['started_at'] = altered[0]['finished_at']-0.1
                samples.write_text(''.join(json.dumps(row)+'\n' for row in altered))
                case['safety']['sha256'] = checksum(samples)
                path.write_text(json.dumps(case))
                # Then neither overlapping samples nor unavailable observations
                # outside declared windows can count as continuous coverage.
                with self.subTest(unavailable_baseline=unavailable), self.assertRaisesRegex(ValueError, 'overlap|unavailable'):
                    validate_case(path)

    def test_checker_prerequisites_fail_without_running_brokers(self):
        # Given a missing or broken explicit independent-checker interpreter.
        with tempfile.TemporaryDirectory() as directory:
            python = Path(directory)/'python'
            with self.assertRaisesRegex(ValueError, 'qualification.qualify setup'):
                configure(dict(KAFERRY_ORACLE_PYTHON=str(python)))
            python.write_text('fixture')
            with patch('tools.qualification.fault_results.subprocess.run', side_effect=RuntimeError('import failed')):
                # When preflighting, then an import failure propagates; it never
                # becomes an empty successful oracle or an implicit install.
                with self.assertRaisesRegex(RuntimeError, 'import failed'):
                    configure(dict(KAFERRY_ORACLE_PYTHON=str(python)))


if __name__ == '__main__':
    unittest.main()
