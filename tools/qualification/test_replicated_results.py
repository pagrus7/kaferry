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
                                                  SAMPLED_SCOPE, broker_case, collect)
from tools.qualification.broker_lifecycle import FORCED_COPY_PHASES

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


def proof(id):
    side = 'source' if id in ('broker/source-leader', 'broker/rolling-source') else 'destination'
    rolling, isr = '/rolling-' in id, id.endswith('isr-loss')
    case = dict(id=id, source_partitions=24, source_topic='source', destination_topic='destination',
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
            event.update(isr_partition=6,isr_lost=lost, stalled=stalled,recovered=recovered, min_insync_replicas=2,controller_targets=[dict(cluster='destination',broker=n,listener='controller',container=f'controller-{n}') for n in (1,2,3)])
            case['publishing'].update(offered_by_partition=[20]*24,acknowledged_by_partition=[20]*24)
            for part in safety['independent_reconciliation']['partitions']:
                part['expected'] = 20
        elif not rolling:
            failover = state(at+2, 4)
            failover['topology'][side][0] = dict(Leader=2, ISR=[2, 3])
            event['failover'] = failover
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


class ReplicatedResultsTest(unittest.TestCase):
    def validate(self, id, change=None, artifacts=None):
        case, safety = proof(id)
        if change:
            change(case, safety)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root/('broker-'+id.split('/')[1])
            directory.mkdir()
            write_lifecycles(directory,case)
            if artifacts:
                artifacts(directory,case)
            (directory/'replicated-broker.json').write_text(json.dumps(case))
            path = root/'fault-oracle/fresh/case/observations/case.json'
            path.parent.mkdir(parents=True)
            path.write_text('{}')
            case['safety_case']['sha256'] = checksum(path.read_bytes())
            (directory/'replicated-broker.json').write_text(json.dumps(case))
            with patch('tools.qualification.replicated_results.validate_case', return_value=safety):
                return broker_case(root, id, {key:safety[key] for key in ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')})

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
        def edit_witness(edit):
            def artifacts(directory,case):
                worker = case['worker_measurements'][0]
                path = directory/worker['lifecycle']['observations']
                witness = json.loads(path.read_text())
                edit(witness)
                data = json.dumps(witness).encode()
                path.write_bytes(data)
                worker['lifecycle']['sha256'] = checksum(data)
                worker['failure_context']['exit_witness'] = witness
            return artifacts
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
                self.validate('broker/source-leader',failed,edit_witness(edit))
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


if __name__ == '__main__':
    unittest.main()
