"""A fixture cannot qualify without checked image evidence from its resolved environment."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import run_suite


class FixtureImagesTest(unittest.TestCase):
    def test_default_run_requires_checked_image_evidence_before_the_suite(self):
        # Given the default fixture and its resolved environment.
        image_evidence = json.dumps([dict(ID='sha256:' + 'a' * 64, Repository='apache/kafka')])
        outcomes = (image_evidence, '', '[]', '{}', 'null', '[{}]', '[{"ID":""}]', '[{"ID":null}]',
                    'invalid JSON', subprocess.CalledProcessError(1, 'images'), subprocess.TimeoutExpired('images', 10))
        for outcome in outcomes:
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                binary = root / 'candidate'; binary.write_bytes(b'candidate')
                args = argparse.Namespace(suite='broker', source='3.9.2', destination='3.9.2',
                    binary=str(binary), output=root / 'evidence', config=None, duration=60)
                labels, startup_env = [], []
                def command(command, output, label, env, **kwargs):
                    labels.append(label)
                    if label == 'brokers-up':
                        startup_env.append(env)
                def images(command, **kwargs):
                    self.assertIs(startup_env[0], kwargs['env'])
                    self.assertEqual(['images', '--format', 'json'], command[-3:])
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.platform.platform', return_value='fixture-platform'), \
                     patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.check_output', side_effect=images) as inspect, \
                     patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                    # When image collection fails or lacks identities, then the suite cannot run or pass.
                    if outcome == image_evidence:
                        run_suite(args)
                    else:
                        with self.assertRaises((ValueError, subprocess.CalledProcessError, subprocess.TimeoutExpired)):
                            run_suite(args)
                inspect.assert_called_once()
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual('brokers-down', labels[-1])
                if outcome == image_evidence:
                    self.assertIn('suite', labels)
                    self.assertEqual('passed', receipt['status'])
                    self.assertEqual(image_evidence, receipt['broker_images'])
                else:
                    self.assertNotIn('suite', labels)
                    self.assertEqual('failed', receipt['status'])
                    self.assertNotIn('broker_images', receipt)
