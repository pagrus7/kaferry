"""Receipt binding complements the Go gates and the independent oracle tests."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.fault_oracle import checksum
from tools.qualification.replicated_results import collect


IDENTITY = dict(source_commit='a'*40, binary_sha256='b'*64,
                binary_platform='linux/amd64', execution_platform='darwin/arm64')


class ReplicatedResultsTest(unittest.TestCase):
    def receipt(self, root, id='network/destination-outage', run='current', precondition=False):
        observer = root/'fault-oracle'/run/id.replace('/', '-')/'observations'
        observer.mkdir(parents=True)
        for name in ('samples.jsonl', 'bootstrap.jsonl'):
            (observer/name).write_text('{}\n')
        safety = dict(IDENTITY, id=id, status='passed', execution='candidate CLI',
                      parameters=dict(source_partitions=24, routing='PRESERVE'),
                      fault_events=[] if precondition else [dict(kind='cut')],
                      safety=dict(observations='samples.jsonl'), bootstrap=dict(observations='bootstrap.jsonl'))
        path = observer/'case.json'
        path.write_text(json.dumps(safety))
        family, name = id.split('/')
        exposure = dict(id=id, oracle_run=run, binary_sha256=IDENTITY['binary_sha256'], events=[] if precondition else [dict(kind='cut')], restarts=0,
                        worker_measurements=[dict(exit_code=0)], exact_lifetime_work_caps_qualified=True,
                        safety_case=dict(observations=path.relative_to(root).as_posix(), sha256=checksum(path)))
        if precondition:
            exposure['precondition'] = dict(status='inconclusive', fault_attempted=False, stage='healthy fixture measurement', detail='rate too low', healthy=dict(before={}, after={}),
                                           max_acknowledgement_gap_seconds=2.28)
        result = root/(family+'-'+name)/('replicated-'+family+'.json')
        result.parent.mkdir(parents=True, exist_ok=True)
        result.write_text(json.dumps(exposure))
        return result, path

    def collect(self, root, selected, run='current'):
        # The independent validator has its own real-record/phase tests. Here it
        # returns a fresh receipt so rewritten references cannot affect another call.
        with patch('tools.qualification.replicated_results.validate_case',
                   side_effect=lambda path, **kwargs: json.loads(path.read_text())) as validator:
            return collect(root, selected, IDENTITY, run), validator

    def test_collects_exact_selection_and_current_run_with_candidate_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            selected = ['broker/source-leader', 'network/destination-outage']
            for id in selected:
                self.receipt(root, id)
            result = root/'broker-source-leader/replicated-broker.json'
            current = result.read_text()
            self.receipt(root, selected[0], run='old')
            result.write_text(current)
            # Given separate Go exposures and current/old independent runs,
            # when collecting, then only the exact selected run is returned.
            cases, validator = self.collect(root, selected)
            self.assertEqual(selected, [case['id'] for case in cases])
            self.assertEqual(2, validator.call_count)
            for case in cases:
                self.assertEqual('passed', case['status'])
                self.assertTrue((root/case['safety']['observations']).is_file())
                self.assertIn('/current/', case['safety']['observations'])
                self.assertEqual(IDENTITY['binary_platform'], case['binary_platform'])
            for invalid in ([], selected+selected[:1], ['network/unknown'], selected[:1]):
                with self.subTest(selected=invalid), self.assertRaises(ValueError):
                    self.collect(root, invalid)
            with self.assertRaises(ValueError):
                self.collect(root, selected, '../current')

    def test_inconclusive_requires_typed_safely_finalized_no_fault_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            id = 'network/latency'
            result, path = self.receipt(root, id, precondition=True)
            original = json.loads(result.read_text())
            # Given a safe no-fault finalization, when Go reports its measured
            # precondition as inconclusive, then only this typed outcome can retry.
            cases, validator = self.collect(root, [id])
            self.assertEqual('inconclusive', cases[0]['status'])
            self.assertEqual(original['precondition'], cases[0]['precondition'])
            self.assertEqual(dict(require_fault=False), validator.call_args.kwargs)
            for change in (dict(oracle_run='old'), dict(binary_sha256='c'*64), dict(precondition={}),
                           *(dict(precondition=dict(original['precondition'], **change)) for change in
                             (dict(fault_attempted=True), dict(fault_attempted=0), dict(stage=''), dict(detail=''))), dict(precondition=dict(status='passed', healthy={})),
                           dict(precondition=dict(status='inconclusive', healthy=dict(before={}, after={}))),
                           *(dict(precondition=dict(original['precondition'], max_acknowledgement_gap_seconds=gap))
                             for gap in (None, '2.28', True, -1, float('inf'), float('nan'))),
                           dict(events=[{}]), dict(restarts=1), dict(worker_measurements=[]),
                           dict(worker_measurements=[dict(exit_code=1)]), dict(exact_lifetime_work_caps_qualified=False)):
                result.write_text(json.dumps(dict(original, **change)))
                with self.subTest(change=change), self.assertRaises(ValueError):
                    self.collect(root, [id])
            result.write_text(json.dumps(original))
            safety = json.loads(path.read_text())
            safety['fault_events'] = [{}]
            path.write_text(json.dumps(safety))
            original['safety_case']['sha256'] = checksum(path)
            result.write_text(json.dumps(original))
            with self.assertRaisesRegex(ValueError, 'no-fault'):
                self.collect(root, [id])

            # Given an earlier readiness failure in any replicated family, when
            # its exact no-fault stage is retained, then no final safety pass is claimed.
            for id in ('ownership/zombie-worker', 'ownership/worker-scale', 'broker/source-leader', 'network/latency'):
                directory = root/id.replace('/', '_')
                result, _ = self.receipt(directory, id, precondition=True)
                exposure = json.loads(result.read_text())
                exposure['precondition'].pop('healthy')
                exposure['precondition'].pop('max_acknowledgement_gap_seconds')
                if id == 'ownership/zombie-worker':
                    result.unlink()
                    result = directory/'replicated-ownership.json'
                elif id.startswith('ownership/'):
                    result = result.rename(result.with_name('replicated-ownership-case.json'))
                result.write_text(json.dumps(exposure))
                cases, validator = self.collect(directory, [id])
                self.assertEqual('inconclusive', cases[0]['status'])
                self.assertNotIn('safety', cases[0])
                validator.assert_not_called()


if __name__ == '__main__':
    unittest.main()
