"""Reject incomplete broker exposures even when independent reconciliation passes."""

import copy
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.suites import REPLICATED_CASES, fault_timeout_catalog
from tools.qualification.replicated_results import (EXACT_SCOPE, NEGOTIATION_EOF,
                                                  SAMPLED_SCOPE, SOURCE_OPERATION_EOFS, broker_case, network_case, collect)
from tools.qualification.broker_lifecycle import FORCED_COPY_PHASES
from tools.qualification.broker_readiness import CRITERION, admin

FORCED_SAVE_EXIT = next(key for key, phase in FORCED_COPY_PHASES.items() if phase == "saving progress")
BINARY = "a"*64
RAW_PROGRESS = dict(msg="progress", submitted=100, confirmed=90, pending_entries=128, pending_estimated_bytes=1024)
PROGRESS_BYTES = (json.dumps(RAW_PROGRESS)+"\n").encode()

def checksum(data):
    return hashlib.sha256(data).hexdigest()

def lifetime(worker):
    return dict(number=worker["lifetime"], pid=worker["pid"], started_at=worker["started_at"], binary_sha256=worker["binary_sha256"])


def stamp(second):
    return datetime.fromtimestamp(second, timezone.utc).isoformat()


def state(second, count):
    return dict(at=stamp(second), saved=[count]*24, destination_ends=[count]*24,
                topology={side: [dict(Leader=1, ISR=[1, 2, 3]) for _ in range(24)] for side in ('source', 'destination')},
                publishing=dict(acknowledged_by_partition=[count]*24), broker_healthy=True,
                cli_progress=dict(msg='progress', submitted=100, confirmed=90, peak_uncommitted_records=0,peak_uncommitted_bytes=0),
                worker=dict(lifetime=dict(number=1,pid=123,started_at=stamp(2),binary_sha256=BINARY),log_prefix_bytes=len(PROGRESS_BYTES),log_prefix_sha256=checksum(PROGRESS_BYTES)))


def readiness(case, observation, targets, at):
    details, probes = [], []
    for target in targets:
        side, broker = target['cluster'], target['broker']
        topic = case['source_topic' if side == 'source' else 'destination_topic']
        response = dict(Version=13, ErrorCode=0, ClusterID=case['cluster_ids'][side],
                        Brokers=[dict(NodeID=n, Host='127.0.0.1', Port=int(admin(side,n).split(':')[1])) for n in (1,2,3)],
                        Topics=[dict(Topic=topic, ErrorCode=0, Partitions=[dict(Partition=n, ErrorCode=0, Leader=p['Leader'], ISR=p['ISR'])
                                                                      for n,p in enumerate(observation['topology'][side])])])
        raw = json.dumps(response)
        probes.append(dict(target=target, client_cluster=side, route='admin', api_key=3, all_topics=True,
                           expected_address=admin(side,broker), transport_broker=broker, transport_address=admin(side,broker),
                           remote_address=admin(side,broker), connected_at=stamp(3), started_at=stamp(at+.1),
                           response_at=stamp(at+.2), finished_at=stamp(at+.3), error='', topic=topic,
                           cluster_id=case['cluster_ids'][side], response_json=raw, response_sha256=checksum(raw.encode()), response_truncated=False))
        details.append(dict(ID=target['container'],Project=case['fixture_project'],Service=target['service'],Running=True,StartedAt=stamp(3),
                            Healthcheck=dict(Test=['CMD','kafka-topics.sh','--list']),
                            Health=dict(Status='starting',FailingStreak=2,Log=[dict(Start=stamp(4),End=stamp(9),ExitCode=-1,Output='Health check exceeded timeout (5s)')]),
                            probe_history_state='retained'))
    observation['broker_readiness'] = [dict(criterion=CRITERION, at=stamp(at),deadline=stamp(at+1.9),
        containers=[target['container'] for target in targets], passed=True,diagnostic_state='complete',details=details,api_probes=probes)]


def proof(id):
    side = 'source' if id in ('broker/source-leader', 'broker/rolling-source') else 'destination'
    rolling, isr = '/rolling-' in id, id.endswith('isr-loss')
    case = dict(id=id, readiness_criterion=CRITERION, fixture_project='owned-project', cluster_ids=dict(source='source-id', destination='destination-id'), source_partitions=24, source_topic='source', destination_topic='destination',
                timeout_milliseconds=fault_timeout_catalog()[id], baseline=state(10, 1), events=[],
                restarts=0, restart_scope='same selected binary, SQL, group and worker1 route', exact_lifetime_work_caps_qualified=True,
                safety_case=dict(observations='fault-oracle/fresh/case/observations/case.json', sha256='checksum'),
                publishing=dict(started=stamp(5), stopped=stamp(499), offered_by_partition=[10]*24, acknowledged_by_partition=[10]*24,max_offer_gap_seconds=0.1),
                worker_measurements=[dict(lifetime=1, unavailable_highwater_phase='', pid=123, binary_sha256=BINARY,worker_route=1,exit_code=0, rss_samples=10, rss_failures=0, sampled_peak_rss_bytes=100,
                    record_cap=1024, byte_cap=128*1024**2, peak_uncommitted_records=1024, peak_uncommitted_bytes=128*1024**2,
                    lifetime_highwater_available=True, cap_measurement_scope=EXACT_SCOPE, progress_observations=10,
                    sampled_max_pending_entries=128, sampled_max_pending_estimated_bytes=1024,
                    copy_summary=dict(msg='copy summary', submitted=100,confirmed=100,peak_uncommitted_records=1024,peak_uncommitted_bytes=128*1024**2),
                    started_at=stamp(2), stopped_at=stamp(500), reason='completed')])
    safety = dict(id=id, execution='candidate CLI', parameters=dict(source_partitions=24, routing='PRESERVE'),
                  source_commit='commit', binary_sha256=BINARY, binary_platform='linux-amd64', execution_platform='linux-amd64',
                  started_at=0, writers_stopped_at=501, fault_events=[], writer_observations=[dict(pid=123, exit_code=0)],
                  independent_reconciliation=dict(partitions=[dict(partition=n, expected=10) for n in range(24)]),
                  safety=dict(observations='samples.jsonl'), bootstrap=dict(observations='bootstrap.jsonl'))
    previous = case['baseline']
    for index in range(3 if rolling else 1):
        at = 20+index*10
        broker = index+1 if rolling else 1
        kind = 'restart' if rolling else 'cut' if isr else 'kill'
        role = dict(kind='broker' if rolling else 'leader', cluster=side, partition=0)
        if rolling:
            role['broker'] = broker
        else:
            role['topic'] = 'destination' if side == 'destination' else 'source'
        if isr:
            role['listener'] = 'internal'
        targets = [dict(cluster=side, broker=broker, container='container', service=f'{side}-{broker}', listener=listener, proxy=f'{side}-{broker}-{listener}')
                   for listener in (['internal'] if isr else ['internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3'])]
        fault = dict(role=role, fault=dict(kind=kind), targets=targets, start=stamp(at), heal=stamp(at+4))
        recovered = state(at+5, 4+index*3)
        event = dict(fault=fault, recovered=recovered, before=copy.deepcopy(previous))
        if isr:
            lost, stalled = state(at+1, 2), state(at+2, 2)
            for row in (lost, stalled):
                row['topology']['destination'][6] = dict(Leader=1, ISR=[1])
            lost['publishing']['acknowledged_by_partition'][6] = 6
            stalled['publishing']['acknowledged_by_partition'][6] = 10
            recovered = state(at+5, 16)
            event.update(isr_partition=6,isr_lost=lost, stalled=stalled,recovered=recovered, min_insync_replicas=2,controller_targets=[dict(cluster='destination',broker=n,service=f'destination-{n}',listener='controller',container=f'controller-{n}',proxy=f'destination-{n}-controller',proxy_container='proxy') for n in (1,2,3)])
            case['publishing'].update(offered_by_partition=[20]*24,acknowledged_by_partition=[20]*24)
            for part in safety['independent_reconciliation']['partitions']:
                part['expected'] = 20
        elif not rolling:
            failover = state(at+2, 4)
            failover['topology'][side][0] = dict(Leader=2, ISR=[2, 3])
            event['failover'] = failover
        readiness(case,event['recovered'],targets[:1],at+4.1)
        if isr:
            readiness(case,event['isr_lost'],event['controller_targets'],at+1.1)
        case['events'].append(event)
        safety['fault_events'].append(dict(target=role, healed_at=at+6, injections=[dict(kind=kind, target={k:v for k,v in fault.items() if k != 'heal'}, at=at+0.5)]))
        previous = recovered
    return case, safety


def failed_worker(case, safety, forced):
    worker = case['worker_measurements'][0]
    replacement = copy.deepcopy(worker)
    replacement.update(lifetime=2, pid=124, started_at=stamp(26))
    worker.update(exit_code=1, stopped_at=stamp(23), terminal_error=FORCED_SAVE_EXIT if forced else NEGOTIATION_EOF+'\n'+NEGOTIATION_EOF)
    fault = case['events'][0]['fault']
    worker['failure_context'] = dict(case=case['id'], recovery_milliseconds=sum(case['timeout_milliseconds'].values()),window=0,role=fault['role'],kind=fault['fault']['kind'], start=fault['start'],heal=fault['heal'],
                                     observed_exit_at=worker['stopped_at'],before=copy.deepcopy(case['baseline']),transport='plaintext')
    if forced:
        worker.update(unavailable_highwater_phase="saving progress",lifetime_highwater_available=False,copy_summary=None,peak_uncommitted_records=None,peak_uncommitted_bytes=None,cap_measurement_scope=SAMPLED_SCOPE)
        case['exact_lifetime_work_caps_qualified'] = False
    else:
        worker['copy_summary']['confirmed'] = 98
    case['worker_measurements'].append(replacement)
    case['restarts'] = 1
    safety['writer_observations'] = [dict(pid=123,exit_code=1),dict(pid=124,exit_code=0)]
    return worker


def labelled_source_worker(case, safety, terminal=SOURCE_OPERATION_EOFS[0]):
    worker = failed_worker(case, safety, False)
    worker['copy_summary']['confirmed'] = worker['copy_summary']['submitted']
    worker['terminal_error'] = terminal
    for target in case['events'][0]['fault']['targets']:
        target['proxy_container'] = 'owned-proxy'
    worker['failure_context'].update(targets=copy.deepcopy(case['events'][0]['fault']['targets']),
                                     timeout_milliseconds=copy.deepcopy(case['timeout_milliseconds']))
    return worker


def write_lifecycles(directory, case):
    for worker in case['worker_measurements']:
        number = worker['lifetime']
        terminal = worker.get('terminal_error', '')
        raw = PROGRESS_BYTES*10
        if worker['copy_summary'] is not None:
            raw += (json.dumps(worker['copy_summary'])+'\n').encode()
        if terminal:
            raw += (terminal+'\n').encode()
        log_name = f'worker-{number}.exited.cli.log'
        (directory/log_name).write_bytes(raw)
        active = copy.deepcopy(worker.get('failure_context'))
        witness = dict(lifetime=lifetime(worker),stopped_at=worker['stopped_at'],recorded_at=worker['stopped_at'],exit_code=worker['exit_code'],
                       wait_error='exit status 1' if worker['exit_code'] else '', termination_signal='',driver_intent_before_wait=[],
                       terminal_lines=terminal.splitlines(), raw_log=dict(observations=log_name,sha256=checksum(raw)),raw_log_bytes=len(raw),raw_log_error='',active_fault=active)
        if active:
            worker['failure_context']['exit_witness'] = witness
        name = f'worker-{number}.lifecycle.json'
        data = json.dumps(witness).encode()
        (directory/name).write_bytes(data)
        worker['lifecycle'] = dict(observations=name,sha256=checksum(data))


def rewrite_witness(edit):
    def artifacts(directory,case):
        rewrite_lifecycle(directory,case['worker_measurements'][0],edit)
    return artifacts


def rewrite_lifecycle(directory, worker, edit):
    path = directory/worker['lifecycle']['observations']
    witness = json.loads(path.read_text())
    edit(witness)
    data = json.dumps(witness).encode()
    path.write_bytes(data)
    worker['lifecycle']['sha256'] = checksum(data)
    if worker.get('failure_context'):
        worker['failure_context']['exit_witness'] = witness


def network_proof(id):
    case, safety = proof(id)
    case.pop('readiness_criterion')
    publishing_start = 5
    name = id.split('/')[1]
    def observed(second, count):
        count -= publishing_start*10
        result = state(second, count)
        result['publishing'].update(started=stamp(publishing_start), observed=stamp(second), offered_by_partition=[count]*24)
        result['broker_healthy'] = False
        result['topology']['destination'] = [dict(Leader=n%3+1, ISR=[1,2,3]) for n in range(24)]
        return result
    case['baseline'] = observed(10,100)
    case['publishing'].update(acknowledged_by_partition=[1000]*24, offered_by_partition=[1000]*24,max_acknowledgement_gap_seconds=.1)
    safety['independent_reconciliation']['partitions'] = [dict(partition=n,expected=1000) for n in range(24)]
    leader, outage = name == 'destination-leaders-outage', name == 'destination-outage'
    role = dict(kind='leader' if leader else 'worker-destination', cluster='destination', worker=1,partition=0)
    if leader: role['topic'] = 'destination'
    targets = [dict(cluster='destination',broker=n,container=f'container-{n}',service=f'destination-{n}',listener='worker1',
                    proxy=f'destination-{n}-worker1',proxy_container='proxy-container') for n in ([1] if leader else [1,2,3])]
    primitive = dict(kind='cut')
    if name in ('latency','bandwidth','slow-close'):
        primitive = dict(kind='slow_close' if name == 'slow-close' else name,stream='downstream' if name == 'slow-close' else 'upstream',
                         value={'latency':500,'bandwidth':32,'slow-close':60000}[name])
    fault = dict(role=role,fault=primitive,targets=targets,start=stamp(18),heal=stamp(87 if outage else 27))
    before, after = observed(20,200), observed(86 if outage else 26,860 if outage else 260)
    affected = [n for n in range(24) if not leader or n%3 == 0]
    for partition in affected:
        after['saved'][partition] = after['destination_ends'][partition] = (240 if name in ('latency','bandwidth') else 200)-publishing_start*10
    event = dict(fault=fault,healthy=dict(before=case['baseline'],after=observed(16,160)),during=dict(before=before,after=after),
                 recovery=dict(before=observed(90 if outage else 30,900 if outage else 300),after=observed(96 if outage else 36,960 if outage else 360)),
                 affected_partitions=affected,unaffected_partitions=sorted(set(range(24))-set(affected)),connection_effect=None)
    window = dict(target=role,unavailable=False,healed_at=97 if outage else 37,
                  injections=[dict(kind=primitive['kind'],target={k:v for k,v in fault.items() if k != 'heal'},at=19)])
    if outage:
        first = case['worker_measurements'][0]
        second = dict(first,lifetime=2,pid=124,exit_code=0,started_at=stamp(89),stopped_at=stamp(500),reason='completed')
        first.update(exit_code=1,terminal_error='context deadline exceeded',stopped_at=stamp(80),reason='observed delivery failure')
        case['worker_measurements'].append(second)
        case['restarts'] = 1
        safety['writer_observations'] = [dict(pid=123,exit_code=1),dict(pid=124,exit_code=0)]
        event['connection_effect'] = dict(exit_observed_at=stamp(80))
        first['failure_context'] = dict(case=id,recovery_milliseconds=sum(case['timeout_milliseconds'].values()),window=0,
                                        role=copy.deepcopy(role),kind='cut',start=fault['start'],heal=fault['heal'],
                                        observed_exit_at=first['stopped_at'],before=copy.deepcopy(event['healthy']['after']),transport='plaintext')
        for boundary in event['recovery'].values():
            boundary['worker']['lifetime'] = lifetime(second)
    if name == 'slow-close':
        stimulus = dict(role=role,fault=dict(kind='timeout',stream='upstream',value=1),targets=targets,start=stamp(19.2),heal=stamp(26.7))
        event['connection_effect'] = dict(stimulus=stimulus,proxy=targets[0]['proxy'],started_at=stamp(19.5),delayed_until=stamp(26.5),
                                        minimum_delay_seconds=6,read_error='i/o timeout',eof_observed_at=stamp(27.1))
        window['injections'].append(dict(kind='timeout',target={k:v for k,v in stimulus.items() if k != 'heal'},at=19.3))
    case['events'], safety['fault_events'] = [event], [window]
    return case, safety


def validate_proof(id, case, safety, validator, artifacts=None):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        family, name = id.split('/')
        directory = root/(family+'-'+name)
        directory.mkdir()
        write_lifecycles(directory,case)
        if artifacts:
            artifacts(directory,case)
        path = root/'fault-oracle/fresh/case/observations/case.json'
        path.parent.mkdir(parents=True)
        path.write_text('{}')
        case['safety_case']['sha256'] = checksum(path.read_bytes())
        (directory/('replicated-'+family+'.json')).write_text(json.dumps(case))
        with patch('tools.qualification.replicated_results.validate_case', return_value=safety):
            return validator(root, id, {key:safety[key] for key in ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')})


class ReplicatedResultsTest(unittest.TestCase):
    def validate(self, id, change=None, artifacts=None):
        case, safety = proof(id)
        if change:
            change(case, safety)
        return validate_proof(id, case, safety, broker_case, artifacts)

    def test_each_broker_exposure_requires_its_actual_observations(self):
        # Given passing independent safety proof, when exposure is complete,
        # then each implemented broker case can be collected.
        for id in REPLICATED_CASES:
            if id.startswith('broker/'):
                with self.subTest(id=id):
                    result = self.validate(id)
                    self.assertEqual(id, result['id'])
                    self.assertIn('fault-oracle/fresh/', result['safety']['observations'])

    def test_rejects_inferred_or_incomplete_fault_exposure(self):
        # Given otherwise passing safety/reconciliation, when a required live
        # observation is absent or contradictory, then collection fails closed.
        mutations = [
            ('broker/source-leader', lambda c,s:c['events'][0]['failover']['topology']['source'][0].update(Leader=1)),
            ('broker/rolling-source', lambda c,s:c['events'][1]['fault']['role'].update(broker=1)),
            ('broker/rolling-destination', lambda c,s:c['events'][0]['recovered']['topology']['destination'][3].update(ISR=[1,2])),
            ('broker/destination-leader', lambda c,s:c['publishing']['offered_by_partition'].__setitem__(0,11)),
            ('broker/destination-leader', lambda c,s:c['publishing'].update(max_offer_gap_seconds=3)),
            ('broker/destination-leader', lambda c,s:c['worker_measurements'][0].update(peak_uncommitted_records=1025)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['isr_lost']['topology']['destination'][6].update(ISR=[1,2])),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['cli_progress'].update(submitted=90)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['saved'].__setitem__(6,3)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['destination_ends'].__setitem__(6,3)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['fault']['targets'][0].update(listener='controller')),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['fault']['targets'][0].update(proxy='destination-2-internal')),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0].pop('isr_partition')),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0].update(isr_partition=0)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['isr_lost']['topology']['destination'][6].update(Leader=2)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['isr_lost']['publishing']['acknowledged_by_partition'].__setitem__(6,5)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['isr_lost']['cli_progress'].update(submitted=90)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled'].update(at=stamp(26))),
        ]
        for id, change in mutations:
            with self.subTest(id=id, change=change), self.assertRaises(ValueError):
                self.validate(id,change)

    def test_readiness_rejects_boolean_cached_peer_wrong_route_and_late_api_evidence(self):
        # Given otherwise passing recovery, when its fresh target API proof is
        # absent, stale, malformed or routed elsewhere, then collection fails.
        def change_probe(case, edit):
            edit(case['events'][0]['recovered']['broker_readiness'][0]['api_probes'][0])
        mutations = [
            lambda c:c.pop('readiness_criterion'),
            lambda c:c['events'][0]['recovered'].pop('broker_readiness'),
            lambda c:c['events'][0]['recovered']['broker_readiness'][0].update(passed=False),
            lambda c:c['events'][0]['recovered']['broker_readiness'][0]['details'][0].update(Running=False),
            lambda c:c['events'][0]['recovered']['broker_readiness'][0]['details'][0].update(Project='other'),
            lambda c:c['events'][0]['recovered']['broker_readiness'][0]['details'][0].update(ID='peer'),
            lambda c:change_probe(c,lambda p:p.update(transport_broker=2)),
            lambda c:change_probe(c,lambda p:p.update(target=dict(p['target'],broker=2))),
            lambda c:change_probe(c,lambda p:p.update(route='internal')),
            lambda c:change_probe(c,lambda p:p.update(remote_address='127.0.0.1:9999')),
            lambda c:change_probe(c,lambda p:p.update(client_cluster='source')),
            lambda c:change_probe(c,lambda p:p.update(all_topics=False)),
            lambda c:change_probe(c,lambda p:p.update(error='context deadline exceeded')),
            lambda c:change_probe(c,lambda p:p.update(response_at=stamp(30))),
            lambda c:change_probe(c,lambda p:p.update(started_at=stamp(1))),
            lambda c:change_probe(c,lambda p:p.update(response_truncated=True)),
            lambda c:change_probe(c,lambda p:p.update(response_json='{}')),
            lambda c:c['events'][0]['recovered']['broker_readiness'][0].update(deadline=stamp(30)),
        ]
        for change in mutations:
            with self.subTest(change=change), self.assertRaises((ValueError,KeyError)):
                self.validate('broker/destination-leader',lambda c,s:change(c))
        def invalid_response(case, edit):
            probe=case['events'][0]['recovered']['broker_readiness'][0]['api_probes'][0]
            response=json.loads(probe['response_json']);edit(response)
            probe['response_json']=json.dumps(response);probe['response_sha256']=checksum(probe['response_json'].encode())
        for edit in (lambda r:r.update(Version=0), lambda r:r.update(ErrorCode=31), lambda r:r.update(ClusterID='source-id'),
                     lambda r:r['Brokers'][0].update(Port=47093),lambda r:r.update(Topics=[]),
                     lambda r:r['Topics'][0]['Partitions'][0].update(ISR=[1])):
            with self.subTest(edit=edit),self.assertRaises(ValueError):
                self.validate('broker/destination-leader',lambda c,s:invalid_response(c,edit))
        for edit in (lambda check:check['api_probes'].pop(),
                     lambda check:check['api_probes'][0].update(finished_at=stamp(24)),
                     lambda check:check['api_probes'][1].update(target=check['api_probes'][0]['target'])):
            with self.subTest(edit=edit),self.assertRaises(ValueError):
                self.validate('broker/destination-isr-loss',lambda c,s:edit(c['events'][0]['isr_lost']['broker_readiness'][0]))

    def test_exact_selection_rejects_missing_or_extra_successful_cases(self):
        # Given selected runtime cases, when evidence omits or adds a case,
        # then a prior successful receipt cannot stand in for this execution.
        with tempfile.TemporaryDirectory() as temporary:
            for selected in ([], ['broker/not-built'], ['broker/source-leader'], ['broker/source-leader']*2):
                with self.subTest(selected=selected), self.assertRaises(ValueError):
                    collect(temporary,selected,{},'fresh')

    def test_forced_copy_phases_bind_current_lifetime_without_claiming_exact_caps(self):
        # Given documented forced-save exit 1, when independent safety passes,
        # then sampled work stays separate and lifetime peaks are explicitly null.
        for id in ('broker/source-leader', 'broker/destination-leader', 'broker/rolling-source', 'broker/rolling-destination'):
            for terminal, phase in FORCED_COPY_PHASES.items():
                def change(case,safety):
                    worker = failed_worker(case,safety,True)
                    worker.update(terminal_error=terminal,unavailable_highwater_phase=phase)
                with self.subTest(id=id,phase=phase):
                    result = self.validate(id,change)
        exposure = result['broker_exposure']
        self.assertFalse(exposure['exact_lifetime_work_caps_qualified'])
        self.assertIsNone(exposure['worker_measurements'][0]['peak_uncommitted_records'])
        for mutate in (
            lambda c,w:w.update(terminal_error='unknown forced exit'),
            lambda c,w:w.update(terminal_error=' '+FORCED_SAVE_EXIT),
            lambda c,w:w.update(terminal_error=FORCED_SAVE_EXIT+'\ncontext deadline exceeded'),
            lambda c,w:w.update(unavailable_highwater_phase='finishing writes'),
            lambda c,w:w['failure_context'].update(case='broker/unknown'),
            lambda c,w:w['failure_context']['before']['worker']['lifetime'].update(pid=999),
            lambda c,w:w['failure_context']['before']['worker']['lifetime'].update(number=2),
            lambda c,w:w['failure_context']['before']['worker']['lifetime'].update(binary_sha256='b'*64),
            lambda c,w:w['failure_context']['before']['worker'].update(log_prefix_sha256='b'*64),
            lambda c,w:w.update(peak_uncommitted_records=w['sampled_max_pending_entries']),
            lambda c,w:w.update(copy_summary=dict(msg='copy summary')),
            lambda c,w:w.update(lifetime_highwater_available=True),
            lambda c,w:w['failure_context'].update(transport='TLS'),
            lambda c,w:w['failure_context'].update(observed_exit_at=stamp(25)),
            lambda c,w:w['failure_context']['before']['cli_progress'].update(confirmed=0),
            lambda c,w:c.update(exact_lifetime_work_caps_qualified=True),
            lambda c,w:w.update(sampled_max_pending_entries=1025),
        ):
            def change(case, safety):
                worker = failed_worker(case,safety,True)
                mutate(case,worker)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.validate('broker/source-leader',change)

    def test_forced_copy_requires_real_raw_and_lifecycle_bindings(self):
        # Given physically retained exit evidence, when bytes, identity, timing,
        # signal intent or the active window contradict eligibility, then hashes
        # and raw prefix checks reject the proof even with independent safety.
        def failed(case,safety):
            failed_worker(case,safety,True)
        mutations = [
            lambda w:w.update(driver_intent_before_wait=[dict(at=stamp(22),signal='interrupt',reason='cleanup')]),
            lambda w:w.update(termination_signal='killed'),
            lambda w:w.update(exit_code=0),
            lambda w:w.update(wait_error=''),
            lambda w:w.update(recorded_at=stamp(22)),
            lambda w:w.update(raw_log_error='unreadable'),
            lambda w:w['lifetime'].update(pid=999),
            lambda w:w.update(active_fault=None),
            lambda w:w['active_fault'].update(window=1),
            lambda w:w.update(terminal_lines=['unknown failure']),
        ]
        for edit in mutations:
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.validate('broker/source-leader',failed,rewrite_witness(edit))
        for name in ('worker-1.lifecycle.json','worker-1.exited.cli.log'):
            def corrupt(directory,case):
                with (directory/name).open('ab') as stream:
                    stream.write(b'corruption')
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.validate('broker/source-leader',failed,corrupt)
        def after_recovery(case,safety):
            worker = failed_worker(case,safety,True)
            worker.update(stopped_at=stamp(144))
            worker['failure_context']['observed_exit_at'] = stamp(144)
            case['worker_measurements'][1]['started_at'] = stamp(145)
        with self.assertRaises(ValueError):
            self.validate('broker/source-leader',after_recovery)
        def recovered_exit(case,safety):
            worker = failed_worker(case,safety,True)
            worker.update(stopped_at=stamp(25))
            worker['failure_context']['observed_exit_at'] = stamp(25)
        self.validate('broker/source-leader',recovered_exit)
        with self.assertRaises(ValueError):
            self.validate('broker/destination-isr-loss',failed)

    def test_destination_eof_requires_exact_unconfirmed_count_and_active_plain_fault(self):
        # Given exact summary highwater and two failed writes, when every terminal
        # line is the pinned diagnostic during destination loss, then restart is
        # collectable; configuration/startup/out-of-window evidence remains fatal.
        self.validate('broker/destination-leader',lambda c,s:failed_worker(c,s,False))
        for mutate in (
            lambda w:w.update(terminal_error=NEGOTIATION_EOF),
            lambda w:w.update(terminal_error=NEGOTIATION_EOF+'\nunknown failure'),
            lambda w:w.update(terminal_error='TLS handshake failed'),
            lambda w:w['failure_context'].update(transport='TLS'),
            lambda w:w['failure_context']['role'].update(cluster='source'),
            lambda w:w['failure_context'].update(observed_exit_at=stamp(19)),
            lambda w:w['failure_context'].update(observed_exit_at=stamp(25)),
            lambda w:w['failure_context']['before']['cli_progress'].update(confirmed=0),
        ):
            def change(case,safety):
                worker = failed_worker(case,safety,False)
                mutate(worker)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                self.validate('broker/destination-leader',change)

    def test_labelled_source_eof_requires_all_confirmed_and_bound_physical_source_fault(self):
        # Given exact all-confirmed output, when the failed operation is labelled,
        # then only its same healthy lifetime inside a source fault is admissible.
        for id in ('broker/source-leader', 'broker/rolling-source'):
            for terminal in SOURCE_OPERATION_EOFS:
                with self.subTest(id=id, terminal=terminal):
                    result = self.validate(id, lambda c,s:labelled_source_worker(c,s,terminal))
                    self.assertTrue(result['broker_exposure']['exact_lifetime_work_caps_qualified'])
        for mutate in (
            lambda c,w:w.update(terminal_error=NEGOTIATION_EOF),
            lambda c,w:w.update(terminal_error=SOURCE_OPERATION_EOFS[0]+' context deadline exceeded'),
            lambda c,w:w.update(terminal_error=SOURCE_OPERATION_EOFS[0]+'\ncontext deadline exceeded'),
            lambda c,w:w.update(terminal_error=' '+SOURCE_OPERATION_EOFS[0]),
            lambda c,w:w.update(terminal_error=SOURCE_OPERATION_EOFS[0]+' '),
            lambda c,w:w.update(terminal_error='poll source: TLS handshake failed'),
            lambda c,w:w['copy_summary'].update(confirmed=99),
            lambda c,w:w.update(copy_summary=None),
            lambda c,w:w['failure_context'].update(transport='TLS'),
            lambda c,w:w['failure_context'].update(case='broker/destination-isr-loss',kind='cut'),
            lambda c,w:w['failure_context'].update(observed_exit_at=stamp(20)),
            lambda c,w:w['failure_context'].update(observed_exit_at=stamp(24)),
            lambda c,w:w['failure_context']['before']['cli_progress'].update(confirmed=0),
            lambda c,w:w['failure_context']['before']['worker']['lifetime'].update(number=2),
            lambda c,w:w['failure_context']['before']['worker'].update(log_prefix_sha256='b'*64),
            lambda c,w:w['failure_context']['targets'].pop(),
            lambda c,w:w['failure_context']['targets'][0].update(listener='worker2'),
            lambda c,w:w['failure_context']['targets'][0].update(container='unowned'),
            lambda c,w:w['failure_context']['timeout_milliseconds'].update(shutdown_timeout=5001),
        ):
            def change(case,safety):
                worker = labelled_source_worker(case,safety)
                mutate(case,worker)
            with self.subTest(mutate=mutate), self.assertRaises((ValueError,KeyError)):
                self.validate('broker/source-leader',change)
        # The physical files and outer references are recreated for each mutation.
        # Changing a sole-Wait witness must still fail with freshly valid hashes.
        for edit in (
            lambda w:w.update(driver_intent_before_wait=[dict(at=stamp(22),signal='',reason='context cancellation')]),
            lambda w:w.update(termination_signal='killed'),
            lambda w:w.update(wait_error=''),
            lambda w:w.update(recorded_at=stamp(22)),
            lambda w:w.update(active_fault=None),
            lambda w:w['active_fault']['targets'][0].update(container='other'),
            lambda w:w['active_fault']['timeout_milliseconds'].update(shutdown_timeout=5001),
        ):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.validate('broker/source-leader',labelled_source_worker,rewrite_witness(edit))
        with self.assertRaises(ValueError):
            self.validate('broker/destination-leader',labelled_source_worker)
        # Given an otherwise consistent exit exactly at or just after physical
        # healing, when worker, proof and sole-Wait times agree, then the strict
        # source boundary rejects it even within the existing recovery budget.
        for stopped in (24,25):
            def at_or_after_heal(case,safety):
                worker = labelled_source_worker(case,safety)
                worker['stopped_at'] = stamp(stopped)
                worker['failure_context']['observed_exit_at'] = worker['stopped_at']
                # validate writes fresh raw/lifecycle files: active exit, sole
                # Wait, recorded time, byte lengths and all hashes agree.
            with self.subTest(stopped=stopped), self.assertRaisesRegex(
                    ValueError, 'labelled source/save EOF lacks a bound all-confirmed active source fault'):
                self.validate('broker/source-leader',at_or_after_heal)

    def test_collection_binds_each_proof_to_this_observer_run(self):
        # Given two selected broker cases from this observer run, when collecting,
        # then exactly those cases pass and a stale reference is rejected.
        selected = ['broker/source-leader', 'broker/destination-leader']
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            def validated(directory, id, identity):
                relative = 'fault-oracle/fresh/'+id.split('/')[1]+'/observations/samples.jsonl'
                return dict(id=id, safety=dict(observations=relative))
            for id in selected:
                path = root/'fault-oracle/fresh'/id.split('/')[1]/'observations/case.json'
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(dict(id=id)))
            with patch('tools.qualification.replicated_results.broker_case', side_effect=validated):
                self.assertEqual(selected, [row['id'] for row in collect(root,selected,{},'fresh')])
            def stale(directory, id, identity):
                row = validated(directory,id,identity)
                row['safety']['observations'] = row['safety']['observations'].replace('/fresh/','/old/')
                return row
            with patch('tools.qualification.replicated_results.broker_case', side_effect=stale), self.assertRaisesRegex(ValueError,'another observer run'):
                collect(root,selected,{},'fresh')


class NetworkResultsTest(unittest.TestCase):
    def validate(self, name, change=None, artifacts=None):
        id = 'network/'+name
        case, safety = network_proof(id)
        if change: change(case,safety)
        return validate_proof(id, case, safety, network_case, artifacts)

    def test_measurement_starts_reject_past_debt_and_output_backlog(self):
        # Given regenerated physical lifecycle/raw checksums and valid spans,
        # when a startpoint retains old debt or output, reject that state itself.
        def lag(state, count, partitions):
            for partition in range(partitions):
                for values in (state['saved'], state['destination_ends'],
                               state['publishing']['acknowledged_by_partition'], state['publishing']['offered_by_partition']):
                    values[partition] -= count
        for phase in ('healthy', 'recovery'):
            for kind in ('source-debt', 'output-backlog'):
                def change(case, safety):
                    state = case['events'][0][phase]['before']
                    if kind == 'source-debt':
                        lag(state, 4, 8)
                    else:
                        state['destination_ends'][0] -= 25
                        state['saved'][0] -= 25
                with self.subTest(phase=phase, kind=kind), self.assertRaisesRegex(ValueError, 'network measurement start'):
                    self.validate('latency', change)

        def inflated(case, safety):
            event = case['events'][0]
            for phase in ('healthy', 'recovery'):
                lag(event[phase]['before'], 14, 8)
                before, after = event[phase]['before'], event[phase]['after']
                rate = (sum(after['destination_ends'])-sum(before['destination_ends']))/6
                self.assertAlmostEqual(258.6666666667, rate)
            self.assertGreater(.95*rate, 240)
        # Both lower-bound checks and the relative recovery bar accept
        # matching catch-up bursts; neither proves a steady measurement start.
        with self.assertRaisesRegex(ValueError, 'network measurement start'):
            self.validate('latency', inflated)

    def test_measurement_start_allows_only_current_slot_and_existing_backlog_limit(self):
        # Given actual publisher timestamps and counters, when only one current
        # slot or at most24output records remain, retain the existing limits.
        for phase in ('healthy', 'recovery'):
            def pending(case, safety):
                state = case['events'][0][phase]['before']
                state['publishing']['acknowledged_by_partition'][0] -= 1
                state['saved'][0] -= 1
                state['destination_ends'][0] -= 1
            def backlog(case, safety):
                state = case['events'][0][phase]['before']
                state['saved'][0] -= 24
                state['destination_ends'][0] -= 24
            for change in (pending, backlog):
                with self.subTest(phase=phase, change=change):
                    self.validate('latency', change)

    def test_measurement_start_requires_the_actual_publisher_clock_and_offers(self):
        # Given otherwise matching receipts and freshly rehashed physical proof,
        # when publisher identity, time or bounded offers contradict the start,
        # then collection must fail before those counts define a reference.
        edits = [lambda p:p.update(started=stamp(1)), lambda p:p.update(observed=stamp(9)),
                 lambda p:p.update(offered_by_partition=[102]*24),
                 lambda p:p.pop('observed'), lambda p:p.pop('offered_by_partition')]
        for edit in edits:
            def change(case, safety):
                edit(case['events'][0]['healthy']['before']['publishing'])
            with self.subTest(edit=edit), self.assertRaises((ValueError, KeyError)):
                self.validate('latency', change)

    def test_measurement_start_preserves_nanoseconds_at_schedule_boundaries(self):
        # Given a publisher origin and snapshots with equal nanosecond tails,
        # when elapsed time is exactly a whole slot, float rounding or datetime
        # truncation must not reject its actual caught-up acknowledgement count.
        def precise(case, safety):
            origin = '1970-01-01T00:00:05.000000009Z'
            case['publishing']['started'] = origin
            for phase in ('healthy', 'during', 'recovery'):
                for state in case['events'][0][phase].values():
                    state['publishing']['started'] = origin
                    state['publishing']['observed'] = state['at'].replace('+00:00', '.000000009Z')
        self.validate('latency', precise)

    def test_outage_sole_wait_and_replacement_lifetimes_bound_observed_windows(self):
        # Given physically regenerated lifecycle/raw files, when the actual
        # first exit or replacement lifetime contradicts the measured window,
        # then a later driver observation cannot qualify those lifetimes.
        self.validate('destination-outage',lambda c,s:c['events'][0]['connection_effect'].update(exit_observed_at=stamp(81)))
        for stopped in (17,18,81):
            def exited(case,safety):
                worker = case['worker_measurements'][0]
                worker['stopped_at'] = worker['failure_context']['observed_exit_at'] = stamp(stopped)
            with self.subTest(stopped=stopped), self.assertRaises(ValueError):
                self.validate('destination-outage',exited)
        for key, second in (('started_at',91),('stopped_at',95)):
            def replacement(case,safety):
                worker = case['worker_measurements'][1]
                worker[key] = stamp(second)
                for boundary in case['events'][0]['recovery'].values():
                    boundary['worker']['lifetime'] = lifetime(worker)
            with self.subTest(replacement=(key,second)), self.assertRaises(ValueError):
                self.validate('destination-outage',replacement)

    def test_outage_rejects_any_driver_intent_signal_or_context_contradiction(self):
        # Given checksummed immutable raw/lifecycle proof with exact summaries,
        # when intent, Wait status or its active fault contradicts fail-fast
        # copying, then rewriting hashes cannot turn it into a fault exit.
        def captured_before_heal(directory,case):
            rewrite_lifecycle(directory,case['worker_measurements'][0],lambda w:w['active_fault'].update(heal='0001-01-01T00:00:00Z'))
        self.validate('destination-outage',artifacts=captured_before_heal)
        edits = [
            lambda w:w.update(driver_intent_before_wait=[dict(at=stamp(79),signal='',reason='context cancellation')]),
            lambda w:w.update(driver_intent_before_wait=[dict(at=stamp(79),signal='interrupt',reason='driver stop')]),
            lambda w:w.update(termination_signal='killed'),
            lambda w:w.update(wait_error=''),
            lambda w:w['active_fault'].update(kind='restart'),
            lambda w:w['active_fault'].update(window=1),
        ]
        for edit in edits:
            def artifacts(directory,case):
                rewrite_lifecycle(directory,case['worker_measurements'][0],edit)
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.validate('destination-outage',artifacts=artifacts)
        for field, value in (('wait_error','exit status 1'),('termination_signal','killed')):
            def replacement_artifacts(directory,case):
                rewrite_lifecycle(directory,case['worker_measurements'][1],lambda w:w.update({field:value}))
            with self.subTest(replacement=(field,value)), self.assertRaises(ValueError):
                self.validate('destination-outage',artifacts=replacement_artifacts)
        with self.assertRaises(ValueError):
            self.validate('destination-outage',lambda c,s:c['worker_measurements'][0].update(unavailable_highwater_phase='finishing writes'))

    def test_each_network_case_requires_its_specific_actual_exposure(self):
        # Given independently validated safety/reconciliation, when every actual
        # exposure and recovered throughput is present, then collect all five IDs.
        for name in ('destination-outage','destination-leaders-outage','latency','bandwidth','slow-close'):
            with self.subTest(name=name):
                self.assertEqual('network/'+name,self.validate(name)['network_exposure']['id'])

    def test_network_evidence_fails_closed_on_missing_or_ineffective_exposures(self):
        # Given otherwise passing independent proof, when a live effect, scope,
        # window, cap or recovery is missing, then qualification must fail.
        mutations = [
            ('destination-outage',lambda c,s:c['events'][0]['during']['after'].update(at=stamp(79))),
            ('destination-outage',lambda c,s:c['events'][0]['connection_effect'].update(exit_observed_at=stamp(17))),
            ('destination-outage',lambda c,s:c['worker_measurements'][1].update(started_at=stamp(85))),
            ('destination-outage',lambda c,s:c['events'][0]['fault']['targets'][0].update(listener='admin')),
            ('destination-leaders-outage',lambda c,s:c['events'][0]['during']['after']['saved'].__setitem__(1,150)),
            ('destination-leaders-outage',lambda c,s:c['events'][0].update(unaffected_partitions=[])),
            ('latency',lambda c,s:c['events'][0]['during']['after'].update(destination_ends=[210]*24)),
            ('latency',lambda c,s:c['events'][0]['fault']['fault'].update(stream='downstream')),
            ('bandwidth',lambda c,s:c['events'][0]['recovery']['after'].update(destination_ends=[251]*24)),
            ('bandwidth',lambda c,s:c['events'][0]['during']['after'].update(destination_ends=[200]*24)),
            ('slow-close',lambda c,s:c['events'][0]['connection_effect'].update(delayed_until=stamp(20))),
            ('slow-close',lambda c,s:c['events'][0]['connection_effect'].pop('stimulus')),
            ('slow-close',lambda c,s:c['events'][0]['connection_effect'].update(eof_observed_at=stamp(26.8))),
            ('slow-close',lambda c,s:s['fault_events'][0]['injections'].pop()),
            ('latency',lambda c,s:s['fault_events'][0].update(unavailable=True)),
            ('latency',lambda c,s:c['publishing'].update(max_acknowledgement_gap_seconds=2.1)),
            ('latency',lambda c,s:c['worker_measurements'][0].update(sampled_peak_rss_bytes=512*1024**2+1)),
        ]
        for name,change in mutations:
            with self.subTest(name=name,change=change),self.assertRaises((ValueError,KeyError)):
                self.validate(name,change)

    def test_network_collection_uses_the_exact_selected_run_and_exposure_validator(self):
        # Given one selected network case, when collecting its current observer
        # run, then use that exposure validator and reject another case's receipt.
        selected = ['network/latency']
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root/'fault-oracle/fresh/latency/observations/case.json'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(dict(id=selected[0])))
            row = dict(id=selected[0],safety=dict(observations=str(path.relative_to(root).with_name('samples.jsonl'))))
            with patch('tools.qualification.replicated_results.network_case',return_value=row) as validate:
                self.assertEqual([row],collect(root,selected,{},'fresh'))
                validate.assert_called_once_with(root,selected[0],{})
            with self.assertRaises(ValueError):
                collect(root,['network/bandwidth'],{},'fresh')

    def test_network_cut_requires_exact_lifetime_peaks_for_both_forced_copy_phases(self):
        # Given a destination network cut with passing independent safety,
        # when its receipt substitutes sampled work for exact lifetime peaks,
        # then neither literal forced-copy diagnostic can qualify it.
        for terminal, phase in FORCED_COPY_PHASES.items():
            def sampled(case, safety):
                worker = case['worker_measurements'][0]
                fault = case['events'][0]['fault']
                worker.update(terminal_error=terminal,unavailable_highwater_phase=phase,lifetime_highwater_available=False,
                              copy_summary=None,peak_uncommitted_records=None,peak_uncommitted_bytes=None,cap_measurement_scope=SAMPLED_SCOPE,
                              failure_context=dict(window=0,role=fault['role'],kind='cut',start=fault['start'],heal=fault['heal'],
                                                   observed_exit_at=worker['stopped_at'],before=case['baseline'],transport='plaintext'))
                case['exact_lifetime_work_caps_qualified'] = False
            with self.subTest(phase=phase), self.assertRaises(ValueError):
                self.validate('destination-outage',sampled)
        with self.assertRaises(ValueError):
            self.validate('latency',lambda c,s:c.update(exact_lifetime_work_caps_qualified=False))


if __name__ == '__main__':
    unittest.main()
