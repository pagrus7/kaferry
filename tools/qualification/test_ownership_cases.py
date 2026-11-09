"""Independent protocol and receipt binding survive scenario simplification."""

import base64
import copy
import hashlib
import itertools
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

from tools.ci import suites
from tools.qualification import ownership_cases as ownership
from tools.qualification import test_ownership_audit as audit_fixture
from tools.qualification.replicated_results import collect

when = audit_fixture.when

def encoded_assignment(parts):
    if not parts:
        return ''
    name = b'source'
    data = struct.pack('>hih', 0, 1, len(name))+name+struct.pack('>i', len(parts))
    return base64.b64encode(data+b''.join(struct.pack('>i', part) for part in parts)+struct.pack('>i', -1)).decode()



class OwnershipCasesTest(unittest.TestCase):
    def fixture(self, name, restart=False):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        directory = root / ('ownership-' + name)
        directory.mkdir()
        id = 'ownership/' + name
        identity = dict(source_commit='a' * 40, binary_sha256='b' * 64, binary_platform='linux-amd64', execution_platform='linux-amd64')
        budget = suites.fault_timeout_catalog()[id]
        case = dict(schema=1, id=id, complete=True, case_failed=False, source_partitions=24, source_topic='source', destination_topic='destination', group='group', fixture_project='fixture', binary_sha256=identity['binary_sha256'], timeout_milliseconds=budget, observations='ownership-observations.jsonl', required_recovery_seconds=sum(budget.values()) / 1000, events=[], states={}, worker_measurements=[], audit_guard='guard', audit_marks=dict(baseline=dict(started=when(0.1), finished=when(0.9)), final=dict(started=when(73), finished=when(74))), publishing=dict(started=when(3), stopped=when(65), offered_by_partition=[1000] * 24, acknowledged_by_partition=[1000] * 24, max_offer_gap_seconds=0.1))
        safety = dict(identity, id=id, execution='candidate CLI', parameters=dict(source_partitions=24, routing='PRESERVE'), started_at=1, writers_stopped_at=72, independent_reconciliation=dict(partitions=[dict(partition=p, expected=1000) for p in range(24)]), safety=dict(observations='samples.jsonl'), bootstrap=dict(observations='bootstrap.jsonl'), fault_events=[])
        rows = {broker: [] for broker in range(1, 4)}
        serial = itertools.count(1)
        topic_id = '00000000-0000-0000-0000-000000000001'

        def row(at, client, api, request, response, broker=2):
            value = audit_fixture.record(at, client, api, request, response, next(serial))
            value['listener'] = 'WORKER' + str(ownership.route(client)) if client.endswith(('worker1', 'worker2', 'worker3')) else 'ADMIN'
            rows[broker].append(value)
            return value

        def state(label, start, finish, routes, count, generation=0, broker=2, preparing=False):
            owned = [dict(client_id='kaferry-group-selected-worker' + str(route), member_id=('new' if restart and label == 'recovered' and (route in (1,)) else 'member') + str(route), partitions=list(range(index, 24, len(routes))) if not preparing else list(range(24)) if route == 1 else []) for (index, route) in enumerate(routes)]
            result = dict(started=when(start), finished=when(finish), state='PreparingRebalance' if preparing else 'Stable', coordinator=broker, members=owned, saved=[count] * 24, publishing=dict(acknowledged_by_partition=[count + 10] * 24), processes_alive={})
            if generation:
                result['accepted_generations'] = {member['client_id']: generation for member in owned}
                for member in owned:
                    row(start - 2, member['client_id'], 'JOIN_GROUP', dict(groupId='group', sessionTimeoutMs=6000, rebalanceTimeoutMs=30000), dict(errorCode=0, memberId=member['member_id'], generationId=generation), broker)
                    row(start - 1.5, member['client_id'], 'SYNC_GROUP', dict(groupId='group', memberId=member['member_id'], generationId=generation), dict(errorCode=0, assignment=encoded_assignment(member['partitions'])), broker)
                    request = dict(groupId='group', memberId=member['member_id'], generationIdOrMemberEpoch=generation, topics=[dict(topicId=topic_id, partitions=[dict(partitionIndex=p, committedOffset=str(count)) for p in member['partitions']])])
                    response = dict(topics=[dict(topicId=topic_id, partitions=[dict(partitionIndex=p, errorCode=0) for p in member['partitions']])])
                    row(start - 1, member['client_id'], 'OFFSET_COMMIT', request, response, broker)
            else:
                result['prior_accepted_generation'] = 1
            row((start + finish) / 2, 'observer', 'DESCRIBE_GROUPS', dict(groups=['group']), dict(groups=[dict(groupId='group', errorCode=0, groupState=result['state'], members=[dict(clientId=m['client_id'], memberId=m['member_id'], memberAssignment=encoded_assignment(m['partitions'])) for m in owned])]), broker)
            case['states'][label] = result
            return result

        def worker(route, start, stop, code=0, zero=False, terminal=None, phase=None):
            pid = 100 + len(case['worker_measurements'])
            log = f"worker-{len(case['worker_measurements']) + 1}-route-{route}.cli.log"
            values = dict(pid=pid, route=route, binary_sha256=identity['binary_sha256'], started_at=when(start), stopped_at=when(stop), exit_code=code, wait_status=9 if code == -1 else code << 8, forced_kill=code == -1, reason='fault' if code else 'completed', terminal_error=terminal if terminal is not None else 'context deadline exceeded' if code == 1 else '', rss_samples=10, rss_failures=0, sampled_peak_rss_bytes=10000, record_cap=1024, byte_cap=128 * 1024 ** 2, peak_uncommitted_records=None if code == -1 else 0 if zero else 5, peak_uncommitted_bytes=None if code == -1 else 0 if zero else 100, progress_observations=1, sampled_max_pending_entries=0 if zero else 5, sampled_max_pending_estimated_bytes=0 if zero else 100, lifetime_highwater_available=code != -1, copy_summary=None, cap_measurement_scope='exact lifetime highwater from final copy summary', log=log)
            progress = dict(msg='progress', submitted=0 if zero else 10, confirmed=0 if zero else 5, pending_entries=0 if zero else 5, pending_estimated_bytes=0 if zero else 100)
            lines = [progress]
            if code != -1:
                values['copy_summary'] = dict(msg='copy summary', submitted=0 if zero else 10, confirmed=0 if zero else 10, peak_uncommitted_records=0 if zero else 5, peak_uncommitted_bytes=0 if zero else 100)
                lines.append(values['copy_summary'])
            (directory / log).write_text(''.join((json.dumps(line) + '\n' for line in lines)) + (values['terminal_error'] + '\n' if code == 1 else ''))
            case['worker_measurements'].append(values)
            if phase:
                values['coordinator_exit_phase'] = phase
            if code == -1:
                values['cap_measurement_scope'] = 'sampled progress only; SIGKILL omits final lifetime highwater'
            return values

        def signal(number, worker, at):
            signal_name = {2: 'interrupt', 9: 'killed', 19: 'stopped (signal)', 18: 'continued'}[number]
            reason = 'SIGKILL during independently observed PreparingRebalance' if number == 9 else 'controlled fixture signal'
            case['events'].append(dict(signal_number=number, signal=signal_name, reason=reason, pid=worker['pid'], route=worker['route'], started=when(at), finished=when(at + 0.05)))

        def window(kind, target, start, injected, healed, injection_kind, injection_target, unavailable=False):
            safety['fault_events'].append(dict(kind=kind, target=target, started_at=start, healed_at=healed, timeout_basis={k: v / 1000 for (k, v) in budget.items()}, recovery_seconds=sum(budget.values()) / 1000, unavailable=unavailable, injections=[dict(at=injected, kind=injection_kind, target=injection_target)]))
        baseline = state('baseline', 5, 6, [1], 10, 1)
        row(3, 'observer', 'METADATA', {}, dict(topics=[dict(name='source', topicId=topic_id, errorCode=0, partitions=[])]))
        if name == 'worker-scale':
            original = worker(1, 2, 71)
            (second, third) = (worker(2, 10, 26), worker(3, 11, 27))
            up = state('scaled_up', 20, 21, [1, 2, 3], 30, 2)
            down = state('scaled_down', 35, 36, [1], 50, 3)
            case['states']['recovered'] = copy.deepcopy(down)
            signal(2, second, 25)
            signal(2, third, 25.1)
            window('scale-up', {'from': 1, 'to': 3}, 8, 12, 22, 'worker-start', dict(routes=[1, 2, 3]))
            window('scale-down', {'from': 3, 'to': 1}, 24, 28, 37, 'worker-stop', dict(routes=[2, 3]))
        else:
            coordinator = name == 'coordinator-kill-rebalance'
            original = worker(1, 2, 14 if restart else 71 if coordinator else 13.3, 1 if restart else 0 if coordinator else -1, terminal=None, phase=None)
            worker(2, 10, 70, 0, zero=False, terminal=None, phase=None)
            if restart:
                worker(1, 19.5, 71)
            signal(19, original, 9)
            case['events'].append(dict(reason='OS confirms held worker', pid=original['pid'], command=['ps', '-o', 'state=', '-p', str(original['pid'])], stdout='T\n', os_process_state='T', observed_at=when(9.2)))
            state('preparing_rebalance', 11, 12, [1, 2], 10, preparing=True)
            at_kill = state('at_kill', 12.1, 13, [1, 2], 10, preparing=True)
            role = dict(kind='coordinator', cluster='source', group='group', partition=0)
            stimulus = dict(held_worker=original['pid'], coordinator_role=role)
            if coordinator:
                targets = [dict(cluster='source', broker=2, service='source-2', container='container2', listener=listener) for listener in ('internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3')]
                fault = dict(role=role, fault=dict(kind='kill'), targets=targets, start=when(12), heal=when(17.5))
                case['events'] += [dict(fault=fault), dict(coordinator_kill_started=when(13.1), coordinator_kill_finished=when(13.2), container='container2'), dict(restart_routes=[1] if restart else [])]
                signal(18, original, 13.3)
                replacement = state('coordinator_replacement', 16, 17, [1, 2], 20, broker=3, preparing=True)
                api_ready = state('broker_api_ready', 17.55, 17.95, [1, 2], 20, broker=3, preparing=True)
                restored = state('broker_restored', 18, 19, [1, 2], 20, broker=3, preparing=True)
                restored['source_topology'] = [dict(Leader=3, ISR=[1, 2, 3]) for _ in range(24)]
                case['events'].append(dict(broker=2, audit_logging_reenabled_at=when(17.97)))
                row(18.5, 'observer', 'METADATA', {}, dict(topics=[dict(name='source', topicId=topic_id, errorCode=0, partitions=[dict(partitionIndex=p, errorCode=0, leaderId=3, isrNodes=[1, 2, 3]) for p in range(24)])]), 3)
                for (running, label, interval) in ((False, 'stopped', replacement), (True, 'restored', restored)):
                    filename = f'coordinator-{label}.inspect.json'
                    (directory / filename).write_text(json.dumps(dict(id='container2', project='fixture', service='source-2', running=running, health='starting' if running else 'unhealthy')))
                    case['events'].append(dict(broker_inspection=filename, container='container2', started=interval['started'], finished=interval['finished']))
                state('recovered', 22, 23, [1, 2], 40, 2, broker=3)
                window('held-worker-rebalance', stimulus, 8, 13.5, 24, 'coordinator-kill', {k: v for (k, v) in fault.items() if k != 'heal'}, True)
            else:
                signal(9, original, 13.1)
                state('recovered', 20, 21, [2], 30, 2)
                window('held-worker-rebalance', stimulus, 8, 13.4, 22, 'worker-sigkill', dict(pid=original['pid'], route=1, state=at_kill))
        safety['writer_observations'] = [dict(pid=w['pid'], exit_code=w['exit_code']) for w in case['worker_measurements']]
        case['exact_lifetime_work_caps_qualified'] = all((w['lifetime_highwater_available'] for w in case['worker_measurements']))
        for (index, worker) in enumerate(case['worker_measurements'], 1):
            if worker['exit_code'] == 0 and (not any((event.get('signal_number') == 2 and event.get('pid') == worker['pid'] for event in case['events']))):
                signal(2, worker, ownership.stamp(worker['stopped_at']) - 0.2)
            worker['lifetime'] = index
            raw = (directory / worker['log']).read_bytes()
            raw_name = f'worker-{index}.exited.cli.log'
            (directory / raw_name).write_bytes(raw)
            intents = [dict(at=when(ownership.stamp(event['started']) + 0.01), signal=event['signal'], reason=event['reason']) for event in case['events'] if 'signal_number' in event and event.get('pid') == worker['pid']]
            code = worker['exit_code']
            witness = dict(lifetime=dict(number=index, pid=worker['pid'], started_at=worker['started_at'], binary_sha256=worker['binary_sha256']), stopped_at=worker['stopped_at'], recorded_at=when(ownership.stamp(worker['stopped_at']) + 0.01), exit_code=code, wait_error='signal: killed' if code == -1 else 'exit status 1' if code == 1 else '', termination_signal='killed' if code == -1 else '', driver_intent_before_wait=intents, terminal_lines=worker['terminal_error'].splitlines(), raw_log=dict(observations=raw_name, sha256=hashlib.sha256(raw).hexdigest()), raw_log_bytes=len(raw), raw_log_error='', active_fault=None)
            name = f'worker-{index}.lifecycle.json'
            (directory / name).write_text(json.dumps(witness))
            worker['lifecycle'] = dict(observations=name, sha256=hashlib.sha256((directory / name).read_bytes()).hexdigest())
        original_fixture = audit_fixture.OwnershipAuditTest()
        original_fixture.setUp()
        self.addCleanup(original_fixture.doCleanups)
        for broker in range(1, 4):
            (directory / f'source-{broker}.log4j2.yaml').write_bytes(original_fixture.config)
            (directory / f'source-{broker}.log4j2.json').write_text(json.dumps(dict(raw_sha256=hashlib.sha256(original_fixture.config).hexdigest(), configuration=original_fixture.configuration)))
            (directory / f'source-{broker}.jvm-logging.txt').write_text('-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml\n-Dkafka.logs.dir=/opt/kafka/logs\n')
            (directory / f'source-{broker}.console.log').write_text('broker started\n')
            first = audit_fixture.record(0.5, 'guard', 'API_VERSIONS', {}, {}, 0)
            final = audit_fixture.record(73.5, 'guard', 'API_VERSIONS', {}, {}, 99999)
            (directory / f'source-{broker}.baseline.requests.log').write_bytes(audit_fixture.encode([first]))
            ordered = rows[broker]
            (directory / f'source-{broker}.final.requests.log').write_bytes(audit_fixture.encode([first] + ordered + [final]))
        (directory / 'ownership-observations.jsonl').write_text(''.join((json.dumps(s) + '\n' for s in case['states'].values())))
        (directory / 'worker-settings.toml').write_text("[worker]\nrecord_cap=1024\npartition_record_cap=128\nbyte_cap=134217728\npartition_byte_cap=8388608\nprogress_interval='100ms'\ndelivery_timeout='1m0s'\nshutdown_timeout='5s'\n")

        def connection(name, properties):
            return 'CONNECTION ' + name + ' AS (' + ','.join(("'" + key + "'='" + str(value) + "'" for (key, value) in properties)) + ')'
        for worker_route in {w['route'] for w in case['worker_measurements']}:
            marker = 'selected-worker' + str(worker_route)
            endpoints = lambda base: ','.join(('127.0.0.1:' + str(base + worker_route * 100 + n) for n in range(3)))
            default = connection('default', [('bootstrap.servers', endpoints(46092)), ('client.id', marker), ('session.timeout.ms', 6000), ('heartbeat.interval.ms', 1000), ('max.poll.interval.ms', 30000), ('socket.timeout.ms', 8000)])
            destination = connection('destination', [('bootstrap.servers', endpoints(47092)), ('client.id', marker)])
            (directory / f'worker-route-{worker_route}.sql').write_text('WITH ' + default + ', ' + destination + " INSERT INTO TOPIC 'destination@destination' SELECT * FROM TOPIC 'source' WITH (GROUP='group', START=EARLIEST)")
        safety_path = root / 'fault-oracle/fresh/case/observations/case.json'
        safety_path.parent.mkdir(parents=True)
        safety_path.write_text(json.dumps(dict(id=id)) + '\n')
        case['safety_case'] = dict(observations=safety_path.relative_to(root).as_posix(), sha256=hashlib.sha256(safety_path.read_bytes()).hexdigest())
        self.save(directory, case)
        return (root, directory, case, safety, identity)

    def save(self,directory,case):
        case['artifacts']=[dict(observations=p.name,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(directory.iterdir()) if p.name != 'replicated-ownership-case.json']
        (directory/'replicated-ownership-case.json').write_text(json.dumps(case))


    def validate(self,fixture):
        root,directory,case,safety,identity=fixture
        self.save(directory,case)
        # Shared JSONL safety/reconciliation has its own complete real fixtures;
        # leave its path/checksum binding real and isolate the exposure parser.
        with patch.object(ownership,'validate_case',return_value=copy.deepcopy(safety)):
            return ownership.validate(root,case['id'],identity)


    def move_birth(self,fixture,worker_index,start,matching_identity=True):
        worker=fixture[2]['worker_measurements'][worker_index]
        worker['started_at']=when(start)
        path=fixture[1]/worker['lifecycle']['observations']
        witness=json.loads(path.read_text())
        if matching_identity:
            witness['lifetime']['started_at']=worker['started_at']
        path.write_text(json.dumps(witness))
        worker['lifecycle']['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()


    def delay_protocol_start(self,fixture,api):
        path=fixture[1]/'source-2.final.requests.log'
        rows=ownership.records(ownership.segments(path))
        edited=0
        for row in rows:
            row['at']=row.pop('completed_at')
            row.pop('received_at')
            if row['requestHeader']['clientId'] == 'kaferry-group-selected-worker2' and row['requestHeader']['requestApiKeyName'] == api:
                row['totalTimeMs']=9500
                edited+=1
        self.assertEqual(1,edited)
        path.write_bytes(audit_fixture.encode(rows))


    def test_all_three_cases_keep_independent_raw_group_generations(self):
        # Given raw broker protocol and retained CLI lifetimes, when their
        # independent observations agree, then all three selections validate.
        for name in ('worker-kill-rebalance','worker-scale','coordinator-kill-rebalance'):
            result=self.validate(self.fixture(name))
            self.assertIn('ownership_exposure',result,result['id'])
        restarted=self.validate(self.fixture('coordinator-kill-rebalance',restart=True))
        self.assertIn('ownership_exposure',restarted)


    def test_selected_members_and_protocol_must_belong_to_retained_lifetimes(self):
        # Given valid raw proof and immutable sidecars, when a claimed peer or
        # replacement is born after its proof, rechecksumming cannot qualify it.
        for name,index,start,restart in (
            ('worker-scale',1,22,False),
            ('worker-scale',2,22,False),
            ('worker-scale',1,19.5,False),
            ('coordinator-kill-rebalance',2,24,True),
        ):
            fixture=self.fixture(name,restart=restart)
            self.move_birth(fixture,index,start)
            with self.subTest(name=name,index=index,start=start),self.assertRaisesRegex(ValueError,'route lifetime'):
                self.validate(fixture)
        # When JOIN, SYNC or commit starts before its retained CLI lifetime,
        # then a later completion and valid membership snapshot cannot repair it.
        for api in ('JOIN_GROUP','SYNC_GROUP','OFFSET_COMMIT'):
            fixture=self.fixture('worker-scale')
            self.delay_protocol_start(fixture,api)
            with self.subTest(api=api),self.assertRaisesRegex(ValueError,'member protocol'):
                self.validate(fixture)
        # Updating only one copy of the lifetime identity remains invalid too.
        fixture=self.fixture('worker-scale')
        self.move_birth(fixture,1,22,matching_identity=False)
        with self.assertRaisesRegex(ValueError,'lifecycle differs'):
            self.validate(fixture)


    def test_retained_dead_member_metadata_is_not_live_ownership_proof(self):
        # Given a failed old CLI still listed by its coordinator until expiry,
        # when metadata proves broker recovery, then that observation is valid.
        fixture=self.fixture('coordinator-kill-rebalance',restart=True)
        self.validate(fixture)
        _,directory,case,_,_=fixture
        rows=ownership.audit(directory,case)
        for name in ('coordinator_replacement','broker_restored'):
            state=case['states'][name]
            self.assertLess(ownership.stamp(case['worker_measurements'][0]['stopped_at']),ownership.stamp(state['started']))
            self.assertEqual(2,len(ownership.group_description(case,state,rows)))
            # Promoting retained broker membership into a claim of live CLI
            # ownership cannot bypass the actual selected lifetime boundary.
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'retained selected route lifetime'):
                ownership.snapshot(case,state,rows)


    def test_missing_altered_and_rechecksummed_invalid_audits_fail(self):
        # Given a valid receipt, when raw accepted commits are removed or their
        # generation changes even with a new checksum, then exposure cannot pass.
        for mode in ('missing','checksum','rechecksummed'):
            fixture=self.fixture('worker-kill-rebalance')
            root,directory,case,safety,identity=fixture
            path=directory/'source-2.final.requests.log'
            if mode == 'missing':
                path.unlink()
            else:
                path.write_text(path.read_text().replace('"generationIdOrMemberEpoch":2','"generationIdOrMemberEpoch":99'))
            if mode != 'checksum':
                self.save(directory,case)
            with patch.object(ownership,'validate_case',return_value=safety),self.subTest(mode=mode),self.assertRaises(ValueError):
                ownership.validate(root,case['id'],identity)


    def test_shared_observer_is_revalidated_and_exact_run_selection_is_enforced(self):
        # Given checksummed ownership exposure, when shared evidence is invalid
        # or another case is selected, then neither summaries nor checksums pass.
        root,directory,case,safety,identity=self.fixture('worker-scale')
        with self.assertRaises(ValueError):
            ownership.validate(root,case['id'],identity)
        with self.assertRaises(ValueError):
            collect(root,['ownership/worker-kill-rebalance'],identity,'fresh')
        with patch.object(ownership,'validate_case',return_value=safety):
            self.assertEqual(case['id'],collect(root,[case['id']],identity,'fresh')[0]['id'])


    def test_independent_generation_assignment_and_commit_chain_reject_self_attestation(self):
        # Given checksummed raw protocol, when a receipt invents a generation
        # or assignment, then raw JOIN/SYNC/commit responses must still agree.
        for mutate in (
            lambda case: case['states']['recovered']['accepted_generations'].update({'kaferry-group-selected-worker2': 99}),
            lambda case: case['states']['recovered']['members'][0]['partitions'].__setitem__(23, 22),
            lambda case: case['worker_measurements'][1].update(binary_sha256='other'),
        ):
            fixture = self.fixture('worker-kill-rebalance')
            mutate(fixture[2])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.validate(fixture)


if __name__ == '__main__':
    unittest.main()
