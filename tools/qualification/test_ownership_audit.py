import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification import ownership_audit as audit
from tools.ci.suites import fault_execution_seconds, fault_timeout_catalog


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
        # When validated, then only old-generation rejection is fencing evidence.
        proof = self.validate()['ownership_fencing']
        self.assertEqual((1, 1, 0, 1), tuple(proof[key] for key in
            ('stale_requests', 'stale_rejections', 'accepted_stale_commits', 'accepted_rejoin_commits')))
        self.assertEqual(list(range(12)), proof['accepted_peer_partitions'])
        self.assertEqual(19, len(proof['audit_files']))

    def test_case_uses_registered_budget_without_extending_the_legacy_suite(self):
        # Given the registered new case, when selecting its budgets, then they
        # match the actual fixture settings and leave the legacy allowance alone.
        self.assertEqual({'session.timeout.ms': 6000, 'socket.timeout.ms': 8000, 'progress_save_timeout': 10000,
                          'delivery_timeout': 2000, 'shutdown_timeout': 5000},
                         fault_timeout_catalog()['ownership/zombie-worker'])
        self.assertEqual(1073, fault_execution_seconds())

    def test_shared_observation_references_are_rebased_for_the_outer_receipt(self):
        # Given the real helper's nested case layout inside a suite directory.
        relative = 'fault-oracle/run/scenario/observations/case.json'
        path = self.directory/relative
        path.parent.mkdir(parents=True)
        path.write_text('{}\n')
        self.case['safety_case']['observations'] = relative
        # When reused by the outer receipt, then bootstrap and samples remain
        # valid contained paths from that receipt's root, as in legacy collect().
        result = self.validate()
        for key, name in (('safety', 'samples.jsonl'), ('bootstrap', 'bootstrap.jsonl')):
            self.assertEqual(str(Path(relative).parent/name), result[key]['observations'])

    def test_local_suppression_needs_complete_audit_and_peer_progress(self):
        # Given a correct client that suppresses stale commits locally.
        self.traffic = [row for row in self.traffic if row['at'] != 22]
        # When the audit is complete, then no broker rejection is invented.
        proof = self.validate()['ownership_fencing']
        self.assertEqual(0, proof['stale_requests'])
        self.assertEqual(0, proof['stale_rejections'])
        self.assertEqual('current-generation-rejoin', proof['fencing_mode'])

    def test_fatal_periodic_save_exit_requires_actual_rejection_before_exit(self):
        # Given a live old owner through pre-heal peer progress, whose late save
        # receives a fenced-generation response and then exits as documented.
        self.case['processes_alive_after_heal'] = [False, True]
        self.case['worker_measurements'][0].update(exit_code=1, stopped_at=when(22.5))
        self.safety['writer_observations'][0]['exit_code'] = 1
        self.traffic = [row for row in self.traffic if row['at'] not in (23, 24)]
        self.case['healed']['members'] = [member('two', 'peer', list(range(24)))]
        # When validated, then the independently observed rejection, rather than
        # continued in-process recovery, is the positive fencing evidence.
        proof = self.validate()['ownership_fencing']
        self.assertEqual('stale-generation-rejected', proof['fencing_mode'])
        self.assertEqual(1, proof['worker1_exit_code'])
        self.assertEqual(list(range(12)), proof['stale_fenced_partitions'])
        self.case['worker_measurements'][0]['stopped_at'] = when(21.5)
        with self.assertRaisesRegex(ValueError, 'rejection before exit'):
            self.validate()

    def test_rejoin_without_new_generation_progress_cannot_replace_fencing(self):
        # Given a successful rejoin without advancing the pre-heal peer prefix.
        self.traffic = [row for row in self.traffic if row['at'] != 22]
        for row in self.traffic:
            if row['at'] == 24:
                for partition in row['request']['topics'][0]['partitions']:
                    partition['committedOffset'] = '20'
        # When no stale rejection exists either, membership alone cannot pass.
        with self.assertRaisesRegex(ValueError, 'traffic absence alone'):
            self.validate()

    def test_transport_exit_or_traffic_absence_without_positive_fencing_fails(self):
        # Given no old-generation rejection, even a later legitimate rejoin
        # cannot justify a nonzero exit as a documented fatal fenced save.
        self.traffic = [row for row in self.traffic if row['at'] != 22]
        self.case['worker_measurements'][0]['exit_code'] = 1
        self.safety['writer_observations'][0]['exit_code'] = 1
        with self.assertRaisesRegex(ValueError, 'rejection before exit'):
            self.validate()
        # When there are no stale requests or new-generation accepted saves,
        # then transport blockage alone cannot qualify even a clean exit.
        self.case['worker_measurements'][0]['exit_code'] = 0
        self.safety['writer_observations'][0]['exit_code'] = 0
        self.traffic = [row for row in self.traffic if row['at'] not in (23, 24)]
        with self.assertRaisesRegex(ValueError, 'traffic absence alone'):
            self.validate()

    def test_old_close_stimulus_short_rpc_or_old_recovery_receipt_fails(self):
        # Given historical reset or incomplete-timeout receipts, when checked
        # against this exposure, then they cannot be reclassified as qualified.
        for mutation in ('close', 'downstream', 'finite-timeout', 'short-rpc', 'old-horizon'):
            with self.subTest(mutation=mutation):
                original_case, original_safety = copy.deepcopy(self.case), copy.deepcopy(self.safety)
                if mutation == 'close':
                    self.case['fault']['fault'] = dict(kind='cut')
                elif mutation == 'downstream':
                    self.case['fault']['fault']['stream'] = 'downstream'
                elif mutation == 'finite-timeout':
                    self.case['fault']['fault']['value'] = 1000
                elif mutation == 'short-rpc':
                    self.safety['fault_events'][0]['timeout_basis']['socket.timeout.ms'] = 3
                else:
                    self.safety['fault_events'][0]['recovery_seconds'] = 13
                with self.assertRaises(ValueError):
                    self.validate()
                self.case, self.safety = original_case, original_safety

    def test_both_workers_must_live_through_peer_progress_and_peer_must_keep_running(self):
        for field, value in (('processes_alive_after_transfer', [False, True]),
                             ('processes_alive_before_heal', [False, True]),
                             ('processes_alive_after_heal', [True, False])):
            with self.subTest(field=field):
                original = self.case[field]
                self.case[field] = value
                with self.assertRaises(ValueError):
                    self.validate()
                self.case[field] = original

    def test_accepted_stale_commit_fails_even_when_final_progress_is_safe(self):
        # Given a real accepted old-generation request after peer transfer.
        self.traffic[3]['response']['topics'][0]['partitions'][0]['errorCode'] = 0
        # When validated, then later rejoin progress cannot repair the violation.
        with self.assertRaisesRegex(ValueError, 'stale generation committed'):
            self.validate()

    def test_commit_partition_payloads_remain_strict_on_both_sides(self):
        # Given full request/response payloads with integer offsets and errors.
        for side, field in (('request', 'committedOffset'), ('response', 'errorCode')):
            for mutation in ('absent', 'duplicate-partition', 'duplicate-topic', 'index',
                             'boolean-value', 'fractional-value', 'missing-partition', 'empty-topics'):
                with self.subTest(side=side, mutation=mutation):
                    row = commit(22, 'one', 'old', 2, range(2), 30, 22, 2)
                    self.assertEqual([(0, 30, 22), (1, 30, 22)], audit.commits(row))
                    topics = row[side]['topics']
                    parts = topics[0]['partitions']
                    if mutation == 'absent':
                        row[side] = None
                    elif mutation == 'duplicate-partition':
                        parts.append(copy.deepcopy(parts[0]))
                    elif mutation == 'duplicate-topic':
                        topics.append(copy.deepcopy(topics[0]))
                    elif mutation == 'index':
                        parts[0]['partitionIndex'] = True
                    elif mutation == 'boolean-value':
                        parts[0][field] = False
                    elif mutation == 'fractional-value':
                        parts[0][field] = 1.5
                    elif mutation == 'missing-partition':
                        parts.pop()
                    else:
                        topics.clear()
                    # When either side is malformed, then neither duplicate
                    # topics nor coercion can create a usable commit result.
                    with self.assertRaises(ValueError):
                        audit.commits(row)

    def test_membership_alone_and_peer_progress_after_healing_do_not_qualify(self):
        # Given ownership transfer without accepted peer progress during the cut.
        self.traffic[2]['at'] = 21.5
        # When validated, then post-heal progress cannot substitute for the fault interval.
        with self.assertRaisesRegex(ValueError, 'accepted peer progress'):
            self.validate()

    def test_unobserved_rejoin_and_wrong_worker_route_fail(self):
        for mutation in ('rejoin', 'route'):
            with self.subTest(mutation=mutation):
                original = copy.deepcopy(self.traffic)
                if mutation == 'rejoin':
                    self.traffic[4]['response']['generationId'] = 5
                else:
                    self.traffic[3]['listener'] = 'ADMIN'
                with self.assertRaises(ValueError):
                    self.validate()
                self.traffic = original

    def test_legitimate_rejoin_can_finish_during_the_heal_operation(self):
        # Given a successful new-generation join after enabling starts but before
        # the proxy API reply returns, when audited, then it is a legitimate rejoin.
        self.traffic[4]['at'] = 20.6
        self.assertEqual(1, self.validate()['ownership_fencing']['accepted_rejoin_commits'])

    def test_explicit_leave_cannot_be_reported_as_session_expiry(self):
        self.traffic.append(record(12, 'one', 'LEAVE_GROUP', dict(groupId='group'), dict(errorCode=0), 5))
        with self.assertRaisesRegex(ValueError, 'rather than expiring'):
            self.validate()

    def test_missing_response_clock_or_preserved_log_fails_closed(self):
        self.write()
        for mutation in ('response', 'clock', 'truncation', 'configuration'):
            with self.subTest(mutation=mutation), patch.object(audit, 'validate_case', return_value=self.safety):
                self.write()
                path = self.directory/'source-2.final.requests.log'
                data = path.read_bytes()
                if mutation == 'response':
                    data = data.replace(b'"response":{"topics":', b'"response":null,"discarded":{"topics":', 1)
                elif mutation == 'clock':
                    data = data.replace(b'1970-01-01 00:00:33', b'1970-01-01 08:00:33')
                elif mutation == 'truncation':
                    data = data.replace(encode(self.start).split(b'\n', 1)[1], b'')
                else:
                    (self.directory/'source-2.log4j2.yaml').write_text('unknown asynchronous logging')
                path.write_bytes(data)
                with self.assertRaises(ValueError):
                    audit.validate(self.directory)

    def test_shared_safety_and_exact_binary_identity_remain_required(self):
        self.write()
        with self.assertRaises(ValueError):
            audit.validate(self.directory)
        wrong = dict(self.safety, binary_sha256='c'*64)
        with self.assertRaisesRegex(ValueError, 'executable identity'):
            self.validate(wrong)

    def test_membership_transfer_without_actual_session_expiry_does_not_qualify(self):
        # Given complete membership and commit observations, but no broker expiry.
        self.write()
        (self.directory/'source-2.console.log').write_text('broker started\n')
        # When validated, then disappearance alone cannot establish session expiry.
        with patch.object(audit, 'validate_case', return_value=self.safety), self.assertRaisesRegex(ValueError, 'session-expiry'):
            audit.validate(self.directory)

    def test_pending_or_missing_network_receipts_prevent_complete_audit_claim(self):
        # Given zero accepted stale commits, but an incomplete correlation audit.
        for mutation in ('pending', 'missing-receipt'):
            with self.subTest(mutation=mutation):
                self.write()
                path = self.directory/'source-2.console.log'
                data = path.read_text()
                if mutation == 'pending':
                    data += network_trace([commit(26, 'one', 'old', 2, range(12), 40, 0, 99)])
                else:
                    data = data.replace(network_trace([self.traffic[3]]), '')
                path.write_text(data)
                # When validated, then a queued or lost request cannot be mistaken for local fencing.
                with patch.object(audit, 'validate_case', return_value=self.safety), self.assertRaisesRegex(ValueError, 'correlated broker responses'):
                    audit.validate(self.directory)

    def test_filters_async_logging_rotation_deletion_and_logging_errors_fail(self):
        # Given logging configurations that may silently omit completed requests.
        original = copy.deepcopy(self.configuration)
        for mutation in ('filter', 'async', 'delete'):
            with self.subTest(mutation=mutation):
                self.configuration = copy.deepcopy(original)
                configuration = self.configuration['Configuration']
                if mutation == 'filter':
                    configuration['Loggers']['Logger'][0]['ThresholdFilter'] = dict(level='ERROR')
                elif mutation == 'async':
                    configuration['Loggers']['AsyncRoot'] = configuration['Loggers'].pop('Root')
                else:
                    configuration['Appenders']['RollingFile'][0]['DefaultRolloverStrategy'] = dict(max=1)
                # When validated, then absence of accepted stale commits cannot qualify them.
                with self.assertRaises(ValueError):
                    self.validate()
        self.configuration = original
        self.write()
        (self.directory/'source-1.console.log').write_text('ERROR StatusConsoleListener Unable to write to stream\n')
        with patch.object(audit, 'validate_case', return_value=self.safety), self.assertRaisesRegex(ValueError, 'logging failure'):
            audit.validate(self.directory)


if __name__ == '__main__':
    unittest.main()
