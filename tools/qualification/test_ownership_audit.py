import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification import ownership_audit as audit


def when(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def member(client, identity, partitions):
    return dict(client_id=client, member_id=identity, partitions=partitions)


def snapshot(start, finish, members):
    return dict(started=when(start), finished=when(finish), members=members)


def record(at, client, api, request, response, correlation):
    return dict(at=at, requestHeader=dict(clientId=client, requestApiKeyName=api, correlationId=correlation),
                request=request, response=response, connection='connection-'+client, totalTimeMs=0,
                listener={'one': 'WORKER1', 'two': 'WORKER2'}.get(client, 'ADMIN'))


def commit(at, client, identity, generation, partitions, offset, error, correlation):
    return record(at, client, 'OFFSET_COMMIT',
                  dict(groupId='group', memberId=identity, generationIdOrMemberEpoch=generation,
                       topics=[dict(name='source', partitions=[dict(partitionIndex=p, committedOffset=str(offset)) for p in partitions])]),
                  dict(topics=[dict(name='source', partitions=[dict(partitionIndex=p, errorCode=error) for p in partitions])]), correlation)


def encode(rows):
    output = bytearray(b'KAFERRY_AUDIT_FILE kafka-request.log\n')
    for row in rows:
        row = dict(row)
        stamp = datetime.fromtimestamp(row.pop('at'), timezone.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        output.extend(f'[{stamp}] DEBUG Completed request:'.encode())
        output.extend(json.dumps(row, separators=(',', ':')).encode())
        output.extend(b' (kafka.request.logger)\n')
    return bytes(output)


def network_trace(rows):
    output = ''
    for row in rows:
        header, request = row['requestHeader'], row['request']
        if header['requestApiKeyName'] != 'OFFSET_COMMIT':
            continue
        stamp = datetime.fromtimestamp(row['at'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
        output += (f'[{stamp}] TRACE Processor 1 received request: RequestHeader(apiKey=OFFSET_COMMIT, '
            f'apiVersion=10, clientId={header["clientId"]}, correlationId={header["correlationId"]}, headerVersion=2) -- '
            f'OffsetCommitRequestData(groupId=\'{request["groupId"]}\', '
            f'generationIdOrMemberEpoch={request["generationIdOrMemberEpoch"]}, memberId=\'{request["memberId"]}\') '
            '(org.apache.kafka.network.Request)\n')
    return output


class OwnershipAuditTest(unittest.TestCase):
    def setUp(self):
        self.temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.directory = Path(self.temporary)
        self.config = b'verified synchronous request logging fixture\n'
        self.configuration = dict(Configuration=dict(
            Properties=dict(Property=[dict(name='logPattern', value='[%d] %p %m (%c)%n')]),
            Appenders=dict(Console=dict(name='STDOUT', PatternLayout=dict(pattern='${logPattern}')), RollingFile=[dict(name='RequestAppender',
                fileName='${sys:kafka.logs.dir}/kafka-request.log',
                filePattern='${sys:kafka.logs.dir}/kafka-request.log.%d{yyyy-MM-dd-HH}',
                PatternLayout=dict(pattern='${logPattern}'), TimeBasedTriggeringPolicy=dict(modulate=True, interval=1))]),
            Loggers=dict(Root=dict(level='INFO', AppenderRef=[dict(ref='STDOUT')]), Logger=[dict(name='kafka.request.logger', level='WARN',
                additivity=False, AppenderRef=dict(ref='RequestAppender'))])))
        self.safety = dict(id='ownership/zombie-worker', source_commit='a'*40, binary_sha256='b'*64,
                           binary_platform='linux/amd64', execution_platform='linux/amd64',
                           execution='candidate CLI', parameters=dict(source_partitions=24, routing='PRESERVE'),
                           safety=dict(observations='samples.jsonl'), bootstrap=dict(observations='bootstrap.jsonl'),
                           writer_observations=[dict(pid=10, exit_code=0), dict(pid=11, exit_code=0)])
        (self.directory/'case.json').write_text('{}\n')
        first = [member('one', 'old', list(range(12))), member('two', 'peer', list(range(12, 24)))]
        survivor = [member('two', 'peer', list(range(24)))]
        self.case = dict(id='ownership/zombie-worker', source_partitions=24, source_topic='source', group='group',
            workers=['one', 'two'], audit_guard='guard', audit_marks={
                'baseline': dict(started=when(0), finished=when(1)), 'final': dict(started=when(33), finished=when(34))},
            baseline=snapshot(5, 6, first), baseline_generation=2, baseline_saved=[10]*24,
            transfer=snapshot(17, 18, survivor), peer_progress=snapshot(19, 20, survivor), peer_saved=[20]*24,
            healed=snapshot(27, 28, [member('one', 'new', list(range(12))), member('two', 'peer', list(range(12, 24)))]),
            heal_started=when(20.5), fault=dict(start=when(10), heal=when(21),
                role=dict(kind='worker-coordinator', cluster='source', worker=1, group='group'), fault=dict(kind='timeout', stream='upstream'),
                targets=[dict(cluster='source', listener='worker1', broker=2)]),
            processes_alive_after_transfer=[True, True], processes_alive_before_heal=[True, True], processes_alive_after_heal=[True, True],
            publishing_after_heal=dict(observed=when(22), acknowledged_by_partition=[30]*24),
            publishing=dict(observed=when(30), stopped=when(29), acknowledged_by_partition=[60]*24, max_acknowledgement_gap_seconds=.1),
            worker_measurements=[dict(sampled_peak_rss_bytes=1000000, rss_samples=10, rss_failures=0,
                peak_uncommitted_records=20, record_cap=1024, peak_uncommitted_bytes=1000, byte_cap=134217728,
                pid=10+i, exit_code=0, started_at=when(3+i), stopped_at=when(31+i)) for i in range(2)],
            safety_case=dict(observations='case.json', sha256=hashlib.sha256(b'{}\n').hexdigest()))
        self.safety['fault_events'] = [dict(started_at=9, healed_at=21.1, recovery_seconds=31,
            timeout_basis={'session.timeout.ms': 6, 'socket.timeout.ms': 8, 'progress_save_timeout': 10, 'delivery_timeout': 2, 'shutdown_timeout': 5},
            target=self.case['fault']['role'], injections=[dict(at=10.1, kind='timeout', target={
                key: self.case['fault'][key] for key in ('role', 'fault', 'targets', 'start')})])]
        self.case['fault']['mutations'] = [dict(target=self.case['fault']['targets'][0],
                                               started=when(10.02), accepted=when(10.05))]
        self.case['worker_connection_milliseconds'] = {
            'session.timeout.ms': 6000, 'heartbeat.interval.ms': 1000, 'socket.timeout.ms': 8000}
        self.safety['fault_events'][0]['injections'][0]['target']['mutations'] = self.case['fault']['mutations']
        self.start = [record(.5, 'guard', 'API_VERSIONS', {}, {}, 0)]
        self.traffic = [commit(7, 'one', 'old', 2, range(12), 10, 0, 1),
                        commit(7, 'two', 'peer', 2, range(12, 24), 10, 0, 1),
                        commit(19, 'two', 'peer', 3, range(24), 20, 0, 2),
                        commit(22, 'one', 'old', 2, range(12), 30, 22, 2),
                        record(23, 'one', 'JOIN_GROUP', dict(groupId='group'), dict(errorCode=0, memberId='new', generationId=4), 3),
                        commit(24, 'one', 'new', 4, range(12), 30, 0, 4),
                        record(4.5, 'one', 'JOIN_GROUP', dict(groupId='group'), dict(errorCode=0, memberId='old', generationId=2), 6)]
        self.finish = [record(33.5, 'guard', 'API_VERSIONS', {}, {}, 1)]

    def write(self):
        for broker in range(1, 4):
            (self.directory/f'source-{broker}.log4j2.yaml').write_bytes(self.config)
            (self.directory/f'source-{broker}.log4j2.json').write_text(json.dumps(dict(
                raw_sha256=hashlib.sha256(self.config).hexdigest(), configuration=self.configuration)))
            (self.directory/f'source-{broker}.console.log').write_text('broker started\n'+(
                '[1970-01-01 00:00:17,000] INFO Member old in group group has failed, removing it from the group.\n'
                +network_trace(self.traffic) if broker == 2 else ''))
            (self.directory/f'source-{broker}.jvm-logging.txt').write_text(
                '-Dlog4j2.configurationFile=/opt/kafka/bin/../config/log4j2.yaml\n-Dkafka.logs.dir=/opt/kafka/logs\n')
            (self.directory/f'source-{broker}.baseline.requests.log').write_bytes(encode(self.start))
            (self.directory/f'source-{broker}.final.requests.log').write_bytes(encode(
                self.start+(self.traffic if broker == 2 else [])+self.finish))
        (self.directory/'replicated-ownership.json').write_text(json.dumps(self.case))

    def validate(self, identity=None):
        self.write()
        # Shared safety/bootstrap validation has its own real JSONL fixtures;
        # these tests isolate the additional selected-CLI ownership evidence.
        with patch.object(audit, 'validate_case', return_value=copy.deepcopy(self.safety)) as safety:
            result = audit.validate(self.directory, identity)
            safety.assert_called_once_with((self.directory/self.case['safety_case']['observations']).resolve())
            return result

    def test_actual_rejection_and_legitimate_rejoin_are_distinguished(self):
        # Given correlated selected-CLI baseline, transfer, rejection and rejoin.
        self.case['publishing']['max_acknowledgement_gap_seconds'] = 2.5
        # When validated, then only old-generation rejection is fencing evidence.
        proof = self.validate()['ownership_fencing']
        self.assertEqual((1, 1, 0, 1), tuple(proof[key] for key in
            ('stale_requests', 'stale_rejections', 'accepted_stale_commits', 'accepted_rejoin_commits')))
        self.assertEqual(list(range(12)), proof['accepted_peer_partitions'])
        self.assertEqual(19, len(proof['audit_files']))
        # Then zero or a gap above half the six-second session cannot qualify.
        for gap in (0, 3.01):
            with self.subTest(gap=gap):
                self.case['publishing']['max_acknowledgement_gap_seconds'] = gap
                with self.assertRaisesRegex(ValueError, 'steady-publishing'):
                    self.validate()

    def test_expiry_window_requires_causal_delay_but_allows_delayed_completion(self):
        # Given expiry at 17s, a six-second session and one-second heartbeat.
        for label, submission, acceptance, completion, qualifies, expected_margin in (
                ('delayed completion', 11, 11.01, 17.5, True, 1.0),
                ('inclusive boundary', 12, 12.01, 16.1, True, 0.0),
                ('1ms before causal bound', 12.001, 12.011, 16.1, False, None),
                ('expiry during preflight', 17.1, 17.2, 17.5, False, None),
                ('expiry only 1s after submission', 16, 16.01, 16.1, False, None)):
            with self.subTest(case=label):
                self.case['fault']['mutations'][0].update(started=when(submission), accepted=when(acceptance))
                self.safety['fault_events'][0]['injections'][0]['at'] = completion
                # When audited, then only causally possible expiry qualifies.
                if qualifies:
                    proof = self.validate()['ownership_fencing']
                    self.assertEqual(1, proof['session_expiry_observations'])
                    self.assertEqual([expected_margin], proof['session_expiry_causal_margins_seconds'])
                else:
                    with self.assertRaisesRegex(ValueError, 'session-expiry'):
                        self.validate()

    def test_missing_or_invalid_recorded_heartbeat_cannot_shorten_causal_bound(self):
        # Given missing, nonpositive, fractional or session-sized heartbeat evidence.
        for heartbeat in (None, False, 0, -1000, 1000.5, 6000, 7000):
            with self.subTest(heartbeat=heartbeat):
                self.case['worker_connection_milliseconds']['heartbeat.interval.ms'] = heartbeat
                # When checked, then invalid configuration cannot qualify expiry.
                with self.assertRaisesRegex(ValueError, 'heartbeat configuration'):
                    self.validate()
        del self.case['worker_connection_milliseconds']
        with self.assertRaisesRegex(ValueError, 'heartbeat configuration'):
            self.validate()

    def test_missing_or_inconsistent_mutation_evidence_fails_explicitly(self):
        # Given incomplete receipts, reversed clocks, or mismatched oracle evidence.
        original_case, original_safety = copy.deepcopy(self.case), copy.deepcopy(self.safety)
        for invalid in ('missing', 'no-start', 'no-acceptance', 'reversed', 'before-preflight',
                        'after-completion', 'wrong-target', 'oracle-mismatch', 'malformed'):
            with self.subTest(invalid=invalid):
                self.case, self.safety = copy.deepcopy(original_case), copy.deepcopy(original_safety)
                self.safety['fault_events'][0]['injections'][0]['target']['mutations'] = self.case['fault']['mutations']
                receipt = self.case['fault']['mutations'][0]
                if invalid == 'missing':
                    del self.case['fault']['mutations']
                elif invalid in ('no-start', 'no-acceptance'):
                    del receipt['started' if invalid == 'no-start' else 'accepted']
                elif invalid == 'reversed':
                    receipt['accepted'] = when(10.01)
                elif invalid == 'before-preflight':
                    receipt['started'] = when(9)
                elif invalid == 'after-completion':
                    receipt['accepted'] = when(11)
                elif invalid == 'wrong-target':
                    receipt['target'] = dict(broker=3)
                elif invalid == 'oracle-mismatch':
                    self.safety['fault_events'][0]['injections'][0]['target']['mutations'] = []
                else:
                    receipt['started'] = 'invalid'
                # When audited, then invalid mutation proof cannot silently pass.
                with self.assertRaisesRegex(ValueError, 'mutation'):
                    self.validate()

    def test_fatal_periodic_save_exit_requires_actual_rejection_before_exit(self):
        # Given a live old owner through pre-heal peer progress, whose late save
        # receives a fenced-generation response and then exits as documented.
        self.case['processes_alive_after_heal'] = [False, True]
        parts = [part for part in range(12) if part not in (5, 8)]
        traffic = [commit(22, 'one', 'old', 2, parts, 30, 22, 2) if row['at'] == 22 else row
                   for row in self.traffic if row['at'] not in (23, 24)]
        self.case['healed']['members'] = [member('two', 'peer', list(range(24)))]
        for label, exit_code, stopped_at, accepted_stale, error in (
                ('partial rejection before fatal exit', 1, 22.5, False, None),
                ('clean exit needs full rejection or rejoin', 0, 22.5, False, 'traffic absence alone'),
                ('accepted stale save on unattempted partition', 1, 22.5, True, 'stale generation committed'),
                ('rejection after exit', 1, 21.5, False, 'rejection before exit')):
            with self.subTest(case=label):
                self.case['worker_measurements'][0].update(exit_code=exit_code, stopped_at=when(stopped_at))
                self.safety['writer_observations'][0]['exit_code'] = exit_code
                self.traffic = traffic + ([commit(22.25, 'one', 'old', 2, [5], 30, 0, 99)] if accepted_stale else [])
                # When validated, then only rejection before a fatal exit qualifies;
                # an accepted stale save always fails, including on omitted partitions.
                if error:
                    with self.assertRaisesRegex(ValueError, error):
                        self.validate()
                else:
                    proof = self.validate()['ownership_fencing']
                    self.assertEqual('stale-generation-rejected', proof['fencing_mode'])
                    self.assertEqual(1, proof['worker1_exit_code'])
                    self.assertEqual(parts, proof['stale_fenced_partitions'])
                    self.assertEqual([5, 8], proof['stale_unattempted_partitions'])

if __name__ == '__main__':
    unittest.main()
