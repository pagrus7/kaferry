"""Reject incomplete broker exposures even when independent reconciliation passes."""

import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.suites import REPLICATED_CASES, fault_timeout_catalog
from tools.qualification.replicated_results import broker_case, collect


def stamp(second):
    return datetime.fromtimestamp(second, timezone.utc).isoformat()


def state(second, count):
    return dict(at=stamp(second), saved=[count]*24, destination_ends=[count]*24,
                topology={side: [dict(Leader=1, ISR=[1, 2, 3]) for _ in range(24)] for side in ('source', 'destination')},
                publishing=dict(acknowledged_by_partition=[count]*24), broker_healthy=True,
                cli_progress=dict(submitted=100, confirmed=90))


def proof(id):
    side = 'source' if id in ('broker/source-leader', 'broker/rolling-source') else 'destination'
    rolling, isr = '/rolling-' in id, id.endswith('isr-loss')
    case = dict(id=id, source_partitions=24, source_topic='source', destination_topic='destination',
                timeout_milliseconds=fault_timeout_catalog()[id], baseline=state(10, 1), events=[],
                restarts=0, restart_scope='same selected binary, SQL, group and worker1 route',
                safety_case=dict(observations='fault-oracle/fresh/case/observations/case.json', sha256='checksum'),
                publishing=dict(started=stamp(5), stopped=stamp(499), offered_by_partition=[10]*24, acknowledged_by_partition=[10]*24,max_offer_gap_seconds=0.1),
                worker_measurements=[dict(pid=123, binary_sha256='binary',worker_route=1,exit_code=0, rss_samples=10, rss_failures=0, sampled_peak_rss_bytes=100,
                    record_cap=1024, byte_cap=128*1024**2, peak_uncommitted_records=1024, peak_uncommitted_bytes=128*1024**2,
                    started_at=stamp(2), stopped_at=stamp(500), reason='completed')])
    safety = dict(id=id, execution='candidate CLI', parameters=dict(source_partitions=24, routing='PRESERVE'),
                  source_commit='commit', binary_sha256='binary', binary_platform='linux-amd64', execution_platform='linux-amd64',
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
        targets = [dict(cluster=side, broker=broker, container='container', service=f'{side}-{broker}', listener=listener)
                   for listener in (['internal'] if isr else ['internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3'])]
        fault = dict(role=role, fault=dict(kind=kind), targets=targets, start=stamp(at), heal=stamp(at+4))
        recovered = state(at+5, 4+index*3)
        event = dict(fault=fault, recovered=recovered, before=copy.deepcopy(previous))
        if isr:
            lost, stalled = state(at+1, 2), state(at+2, 2)
            for row in (lost, stalled):
                row['topology']['destination'][0] = dict(Leader=1, ISR=[1])
            stalled['publishing']['acknowledged_by_partition'][0] = 6
            event.update(isr_lost=lost, stalled=stalled, min_insync_replicas=2,controller_targets=[dict(cluster='destination',broker=n,listener='controller',container=f'controller-{n}') for n in (1,2,3)])
        elif not rolling:
            failover = state(at+2, 4)
            failover['topology'][side][0] = dict(Leader=2, ISR=[2, 3])
            event['failover'] = failover
        case['events'].append(event)
        safety['fault_events'].append(dict(target=role, healed_at=at+6, injections=[dict(kind=kind, target={k:v for k,v in fault.items() if k != 'heal'}, at=at+0.5)]))
        previous = recovered
    return case, safety


class ReplicatedResultsTest(unittest.TestCase):
    def validate(self, id, change=None):
        case, safety = proof(id)
        if change:
            change(case, safety)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root/('broker-'+id.split('/')[1])
            directory.mkdir()
            (directory/'replicated-broker.json').write_text(json.dumps(case))
            path = root/'fault-oracle/fresh/case/observations/case.json'
            with patch('tools.qualification.replicated_results.observation_path', return_value=path), \
                 patch('tools.qualification.replicated_results.validate_case', return_value=safety):
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
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['isr_lost']['topology']['destination'][0].update(ISR=[1,2])),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['cli_progress'].update(submitted=90)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['saved'].__setitem__(0,3)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['stalled']['destination_ends'].__setitem__(0,3)),
            ('broker/destination-isr-loss', lambda c,s:c['events'][0]['fault']['targets'][0].update(listener='controller')),
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
