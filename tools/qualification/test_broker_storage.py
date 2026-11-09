"""Default replicated endurance preserves storage when cleanup cannot authenticate it."""

import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import run_suite


def suite_args(root, suite='recovery', **changes):
    binary = root / 'candidate'
    binary.write_bytes(b'candidate')
    values = dict(suite=suite, binary=str(binary), output=root / 'evidence', source='3.9.2',
                  destination='3.9.2', config=None, rate=200 if suite == 'endurance' else 50,
                  duration=60, state_contract='resume', previous_binary=binary)
    values.update(changes)
    return argparse.Namespace(**values)



class ReplicatedBrokerStorageTest(unittest.TestCase):
    def test_replicated_endurance_uses_replicated_storage_and_preserves_unverified_data(self):
        # Given replicated endurance with default disposable storage and failed cleanup validation.
        with tempfile.TemporaryDirectory() as temporary:
            root, calls = Path(temporary), []
            args = suite_args(root, 'endurance', topology='replicated', source='4.4.0', destination='4.4.0')
            def command(command, output, label, env, **kwargs):
                self.assertEqual('kaferry-replicated', env['KAFERRY_FIXTURE_PROJECT'])
                calls.append((label, command))
                if label == 'storage-capacity':
                    (output / 'storage-capacity.log').write_text('10000000\n')
                if label == 'storage-clean':
                    raise RuntimeError('ownership mismatch')
            with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                 patch('tools.qualification.qualify.replicated_admission', return_value={'required_broker_bytes': 4096}) as admit, \
                 patch('tools.qualification.qualify.endurance_capacity', side_effect=AssertionError('RF=1 disk allowance used')), \
                 patch('tools.qualification.qualify.capture', return_value='fixture'), \
                 patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                 patch('tools.qualification.qualify.fixture_images', return_value='fixture'), \
                 patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                # When finishing, then the duration reaches RF=3 admission and only enabled proxies are configured.
                with self.assertRaisesRegex(RuntimeError, 'teardown failed'):
                    run_suite(args)
            self.assertEqual(60, admit.call_args.args[1])
            self.assertEqual(['storage-capacity', 'storage-prepare', 'proxy-up', 'proxy-configure',
                              'brokers-up', 'suite', 'brokers-stop', 'storage-clean', 'brokers-down'],
                             [label for label, _ in calls])
            self.assertNotIn('-v', calls[-1][1])
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('failed', receipt['status'])
            self.assertEqual(3, receipt['workload']['replication_factor'])
            self.assertEqual('disposable-volume', receipt['fixture_storage']['mode'])
