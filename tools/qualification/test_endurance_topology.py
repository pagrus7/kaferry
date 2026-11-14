"""Replicated endurance changes topology while preserving workload and admission gates."""

import argparse
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification import network
from tools.qualification.qualify import main, resolve_run


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
            settings = self.settings(Path(temporary))
            receipt = {}
            with patch('tools.qualification.qualify.capture', return_value='candidate'), \
                 patch('tools.qualification.qualify.identity_platforms', return_value={}):
                spec = resolve_run(settings, receipt)
            self.assertIn('src/tests/replicated/compose.yaml', spec.compose)
            self.assertEqual('kaferry-replicated', receipt['fixture_project'])
            workload = {'KAFERRY_SOAK_TOPOLOGY': 'replicated', 'KAFERRY_SOAK_PROFILE': 'application',
                        'KAFERRY_SOAK_RATE': '200', 'KAFERRY_ARCHIVE_PAYLOAD': 'random'}
            self.assertEqual(workload, {key: spec.env[key] for key in workload})
            self.assertIn('-timeout=370s', spec.command)
            self.assertEqual(['TestStreamingSoakFidelityCommitLagMemoryAndShutdown'], spec.expected)
            self.assertEqual('disposable-volume', receipt['fixture_storage']['mode'])
            self.assertIn(network.IMAGE, receipt['requested_images'])
            self.assertEqual(network.API, spec.env['KAFERRY_TOXIPROXY_API'])
            for side, key in [('source', 'KAFKA_TEST_BROKERS'), ('destination', 'KAFKA_TEST_DEST_BROKERS')]:
                self.assertEqual(network.endpoints(side), spec.env[key])
                for worker in range(1, 4):
                    self.assertEqual(network.endpoints(side, worker), spec.env[f'KAFERRY_WORKER{worker}_{side.upper()}_BROKERS'])

    def test_rejects_other_suite_topologies_and_old_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for changes in ({'suite': 'soak'}, {'topology': 'other'}, {'source': '3.9.2'}):
                # Given unsupported combinations, when resolving, then no Docker or binary executes.
                with self.subTest(changes=changes):
                    with patch('tools.qualification.qualify.capture') as execute, self.assertRaises(ValueError):
                        resolve_run(self.settings(root, **changes), {})
                    execute.assert_not_called()

if __name__ == '__main__':
    unittest.main()
