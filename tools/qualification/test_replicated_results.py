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
        exposure = dict(id=id, events=[] if precondition else [dict(kind='cut')], restarts=0,
                        worker_measurements=[dict(exit_code=0)], exact_lifetime_work_caps_qualified=True,
                        safety_case=dict(observations=path.relative_to(root).as_posix(), sha256=checksum(path)))
        if precondition:
            exposure['precondition'] = dict(status='inconclusive', healthy=dict(before={}, after={}))
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

    def test_stale_foreign_or_modified_receipts_cannot_reuse_a_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            id = 'broker/source-leader'
            result, path = self.receipt(root, id)
            original = json.loads(result.read_text())
            _, stale = self.receipt(root, id, run='old')
            for reference in (dict(observations=stale.relative_to(root).as_posix(), sha256=checksum(stale)),
                              dict(observations='../case.json', sha256=checksum(path)),
                              dict(observations=str(path), sha256=checksum(path)),
                              dict(original['safety_case'], sha256='0'*64)):
                result.write_text(json.dumps(dict(original, safety_case=reference)))
                # When an exposure is redirected or its checksum changes, then
                # it cannot bind to the independent receipt from this run.
                with self.subTest(reference=reference), self.assertRaises(ValueError):
                    self.collect(root, [id])
            result.write_text(json.dumps(original))
            for field in IDENTITY:
                with self.subTest(identity=field), self.assertRaisesRegex(ValueError, 'identity'):
                    with patch.dict(IDENTITY, {field: 'other'}):
                        self.collect(root, [id])
            with patch('tools.qualification.replicated_results.validate_case', side_effect=ValueError('unsafe saved prefix')):
                with self.assertRaisesRegex(ValueError, 'unsafe saved prefix'):
                    collect(root, [id], IDENTITY, 'current')

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
            self.assertEqual(dict(require_fault=False), validator.call_args.kwargs)
            for change in (dict(precondition={}), dict(precondition=dict(status='passed', healthy={})),
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

    def test_ownership_keeps_its_independent_fencing_audit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            id = 'ownership/zombie-worker'
            _, path = self.receipt(root, id)
            safety = json.loads(path.read_text())
            safety['safety']['observations'] = (path.parent/'samples.jsonl').relative_to(root).as_posix()
            # Given the ownership selection, when collecting, then the separate
            # stale-generation audit remains required rather than a Go receipt.
            with patch('tools.qualification.ownership_audit.validate', return_value=safety) as audit:
                self.assertEqual([safety], collect(root, [id], IDENTITY, 'current'))
            audit.assert_called_once_with(root, IDENTITY)


if __name__ == '__main__':
    unittest.main()
