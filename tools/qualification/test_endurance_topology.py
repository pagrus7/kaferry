"""Replicated endurance changes topology while preserving workload and admission gates."""

import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import (endurance_allowance, main, replicated_admission,
                                         replicated_storage, resolve_run)


class ReplicatedEnduranceTest(unittest.TestCase):
    def settings(self, root, **changes):
        binary = root / 'candidate'
        binary.write_bytes(b'candidate')
        values = dict(suite='endurance', topology='replicated', source='4.4.0', destination='4.4.0',
                      output=root / 'evidence', binary=str(binary), config=None, duration=60, rate=200)
        values.update(changes)
        return argparse.Namespace(**values)

    def test_defaults_select_newest_replicated_images_and_keep_the_same_workload(self):
        # Given a replicated topology request, when resolving, then only fixture settings change.
        with patch('tools.qualification.qualify.run_suite') as run:
            main(['run', 'endurance', '--topology', 'replicated'])
        args = run.call_args.args[0]
        self.assertEqual(('4.4.0', '4.4.0', 86400, 200), (args.source, args.destination, args.duration, args.rate))
        with tempfile.TemporaryDirectory() as temporary:
            settings = self.settings(Path(temporary), broker_volume='private-lab-volume')
            receipt = {}
            with patch('tools.qualification.qualify.capture', return_value='candidate'), \
                 patch('tools.qualification.qualify.identity_platforms', return_value={}):
                spec = resolve_run(settings, receipt, None)
            self.assertIn('src/tests/replicated/compose.yaml', spec.compose)
            self.assertEqual('kaferry-replicated', receipt['fixture_project'])
            workload = {'KAFERRY_SOAK_TOPOLOGY': 'replicated', 'KAFERRY_SOAK_PROFILE': 'application',
                        'KAFERRY_SOAK_RATE': '200', 'KAFERRY_ARCHIVE_PAYLOAD': 'random'}
            self.assertEqual(workload, {key: spec.env[key] for key in workload})
            self.assertIn('-timeout=370s', spec.command)
            self.assertEqual(['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'], spec.expected)
            self.assertEqual('true', spec.env['KAFERRY_REPLICATED_VOLUME_EXTERNAL'])
            self.assertEqual('external-volume', receipt['fixture_storage']['mode'])
            self.assertNotIn('private-lab-volume', json.dumps(receipt))

    def test_rejects_other_suite_topologies_old_images_and_parallel_replicated_selection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for changes, fixture in [({'suite': 'soak'}, None), ({'topology': 'other'}, None),
                                     ({'source': '3.9.2'}, None), ({}, {'project': 'parallel', 'ports': [1, 2]})]:
                # Given unsupported combinations, when resolving, then no Docker or binary executes.
                with self.subTest(changes=changes, fixture=fixture):
                    with patch('tools.qualification.qualify.capture') as execute, self.assertRaises(ValueError):
                        resolve_run(self.settings(root, **changes), {}, fixture)
                    execute.assert_not_called()

    def test_capacity_counts_three_replicas_of_both_clusters_before_starting_brokers(self):
        # Given a full-day workload, the single-topology allowance remains unchanged.
        baseline, replicated = endurance_allowance(86400), endurance_allowance(86400, replicas=3)
        self.assertEqual(17856000, baseline['records'])
        self.assertEqual(17856000 * 5376 * 3 + 4 * 1024**3, baseline['required_broker_bytes'])
        self.assertEqual(17856000 * 5376 * 9 + 4 * 1024**3, replicated['required_broker_bytes'])
        self.assertEqual(baseline['required_evidence_bytes'], replicated['required_evidence_bytes'])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch('tools.qualification.qualify.capture', return_value=json.dumps({'NCPU': 4, 'MemTotal': 16 * 1024**3})), \
                 patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)), \
                 patch('tools.qualification.qualify.socket.socket'):
                capacity = replicated_admission(root, {'KAFERRY_REPLICATED_VOLUME_EXTERNAL': 'false'}, 86400)
            self.assertEqual(replicated['required_broker_bytes'], capacity['required_broker_bytes'])
            def disk(command, output, label, env, **kwargs):
                (output / 'storage-capacity.log').write_text(str(baseline['required_broker_bytes'] // 1024) + '\n')
            # When the volume fits RF=1 but not RF=3, then reject before preparation/startup.
            with patch('tools.qualification.qualify.logged', side_effect=disk) as execute:
                with self.assertRaisesRegex(ValueError, 'broker space'):
                    replicated_storage(root, ['compose'], {}, capacity, None)
            self.assertEqual(1, execute.call_count)
            self.assertEqual('storage-capacity', execute.call_args.args[2])

if __name__ == '__main__':
    unittest.main()
