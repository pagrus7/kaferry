import json
from pathlib import Path
import tempfile
import unittest

from tools.qualification.replicated_evidence import retain


class ReplicatedEvidenceTest(unittest.TestCase):
    def test_failed_case_retains_activation_expiry_and_completion_without_final_audit(self):
        # Given a failed case with no successful ownership/audit receipt.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            activation = dict(fault_active_at='2026-11-13T13:38:15.123456789Z',
                              proxy='source-2-worker1', toxic_name='kaferry-fault')
            (root/'brokers.log').write_text('toxiproxy | '+json.dumps(activation)+'\nsource-2 | '
                '[2026-11-13 13:38:20,637] INFO Member old in group copy has failed, removing it from the group.\n')
            observations = root/'oracle'
            observations.mkdir()
            (observations/'fault-events.jsonl').write_text(json.dumps(dict(event='injection-completed', at=1794577100.7239509))+'\n')
            # When teardown retains diagnostics, then all three clock readings
            # survive without creating any successful qualification evidence.
            retain(root)
            report = json.loads((root/'replicated-case-evidence.json').read_text())
            self.assertEqual([activation], report['activations'])
            self.assertEqual('2026-11-13T13:38:20.637000+00:00', report['session_expiries'][0]['at'])
            self.assertEqual(1794577100.7239509, report['completions'][0]['at'])
            self.assertEqual([], report['missing'])
            self.assertTrue(report['diagnostic_only'])
            self.assertFalse((root/'replicated-ownership.json').exists())

    def test_truncated_failure_records_keep_earlier_observations(self):
        # Given a failure interrupting each diagnostic stream after a valid row.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            activation = dict(fault_active_at='2026-11-13T13:38:15.123456789Z')
            completion = dict(event='injection-completed', at=1794577100.7239509)
            (root/'brokers.log').write_text(json.dumps(activation)+'\n{"fault_active_at":\n')
            (root/'fault-events.jsonl').write_text(json.dumps(completion)+'\n{"event":')
            (root/'replicated-fault-window.json').write_text('{"event":')
            # When collected, then corrupt tails remain errors beside the valid
            # records, rather than losing the case artifact or creating proof.
            report = retain(root)
            self.assertEqual([activation], report['activations'])
            self.assertEqual(completion['at'], report['completions'][0]['at'])
            self.assertEqual(3, len(report['errors']))
            self.assertEqual({'brokers.log', 'fault-events.jsonl', 'replicated-fault-window.json'},
                             {row['source'] for row in report['errors']})
            self.assertEqual(['session_expiries'], report['missing'])
            self.assertEqual(report, json.loads((root/'replicated-case-evidence.json').read_text()))
            self.assertFalse((root/'replicated-ownership.json').exists())

    def test_pre_case_failure_reports_missing_observations(self):
        # Given failure before the fixture starts, when retained, then missing
        # evidence stays explicit rather than becoming a successful timing proof.
        with tempfile.TemporaryDirectory() as temporary:
            report = retain(Path(temporary))
            self.assertEqual(['activations', 'session_expiries', 'completions'], report['missing'])
