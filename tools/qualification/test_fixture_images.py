"""A fixture cannot qualify without checked image evidence from its resolved environment."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import ROOT, fixture_images, run_suite


class FixtureImagesTest(unittest.TestCase):
    def test_default_run_requires_checked_image_evidence_before_the_suite(self):
        # Given the default fixture and its resolved environment.
        image_evidence = json.dumps([dict(ID='sha256:' + 'a' * 64, Repository='apache/kafka')])
        outcomes = (image_evidence, '', '[]', '{}', 'null', '[{}]', '[{"ID":""}]', '[{"ID":null}]',
                    'invalid JSON', subprocess.CalledProcessError(1, 'images'), subprocess.TimeoutExpired('images', 60))
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
                    self.assertEqual(60, kwargs['timeout'])
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.platform.platform', return_value='fixture-platform'), \
                     patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.network.subprocess.check_output', side_effect=images) as inspect, \
                     patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                    # When image collection fails or lacks identities, then the suite cannot run or pass.
                    if outcome == image_evidence:
                        run_suite(args)
                    else:
                        with self.assertRaises(ValueError):
                            run_suite(args)
                self.assertEqual(3 if isinstance(outcome, Exception) else 1, inspect.call_count)
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

    def test_image_query_recovers_on_third_attempt_or_reports_last_failure(self):
        # Given two transient failures, keep the resolved Compose command and environment.
        spec = argparse.Namespace(compose=['docker', 'compose', '-p', 'isolated'], env={'FIXTURE': 'resolved'})
        evidence = '[{"ID":"sha256:fixture"}]'
        command = spec.compose + ['images', '--format', 'json']
        failures = [subprocess.TimeoutExpired(command, 60),
                    subprocess.CalledProcessError(1, command, stderr='daemon busy')]
        for final in (evidence, OSError('last query failure')):
            with self.subTest(final=final), patch('tools.qualification.network.subprocess.check_output',
                    side_effect=failures + [final]) as query:
                # When the third attempt finishes, then return its evidence or name its failure.
                if final == evidence:
                    self.assertEqual(evidence, fixture_images(spec))
                else:
                    with self.assertRaisesRegex(ValueError,
                            'docker compose -p isolated images --format json failed after 3 attempts: could not execute: last query failure'):
                        fixture_images(spec)
                self.assertEqual(3, query.call_count)
                for call in query.call_args_list:
                    self.assertEqual(command, call.args[0])
                    self.assertIs(spec.env, call.kwargs['env'])
                    self.assertEqual(ROOT, call.kwargs['cwd'])
                    self.assertEqual(60, call.kwargs['timeout'])
