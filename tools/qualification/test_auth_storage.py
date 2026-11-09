"""Authentication keeps its default lifecycle and private certificate paths."""

import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.suites import EXPECTED
from tools.qualification.qualify import IMAGES, run_suite
from tools.qualification.test_broker_storage import suite_args

ROOT = Path(__file__).resolve().parents[2]


class AuthRunnerTest(unittest.TestCase):
    def test_auth_runner_checks_live_images_and_preserves_paths_and_original_failures(self):
        # Given actual runner/shell execution with synthetic Docker/Go executables.
        images = json.dumps([dict(ID='sha256:' + 'a' * 64,
                                 ContainerName=f'kaferry-qualification-kafka-secure-{i}-1') for i in range(1, 4)])
        cases = [(images, 0, 0, 0), ('', 0, 0, 0), ('[]', 0, 0, 0), ('[{"ID":""}]', 0, 0, 0),
                 ('invalid JSON', 0, 0, 0), (images, 13, 0, 0), (images, 0, 7, 0),
                 (images, 0, 0, 9), (images, 0, 7, 9)]
        for image_output, image_exit, suite_exit, cleanup_exit in cases:
            with self.subTest(images=image_output, exits=(image_exit, suite_exit, cleanup_exit)), \
                 tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = root / 'src/tests/qualification'; fixture.mkdir(parents=True)
                shutil.copyfile(ROOT / 'src/tests/qualification/run.sh', fixture / 'run.sh')
                (fixture / 'certificates.sh').write_text('mkdir -p "$1"\ntouch "$1/ca.pem" "$1/wrong-ca.pem" "$1/client.pem" "$1/client.key"\n')
                tools = root / 'tools'; tools.mkdir()
                docker = tools / 'docker'
                docker.write_text('#!' + sys.executable + '\nimport json, os, pathlib, sys\n'
                    'cert = pathlib.Path(os.environ["KAFERRY_QUAL_CERT_DIR"])\n'
                    'alive = pathlib.Path(os.environ["ALIVE"])\n'
                    'assert os.environ["KAFERRY_AUTH_IMAGE"] == os.environ["EXPECTED_AUTH_IMAGE"]\n'
                    'with open(os.environ["CALLS"], "a") as log: log.write(json.dumps(dict(args=sys.argv[1:], cert=str(cert))) + "\\n")\n'
                    'if "up" in sys.argv:\n'
                    '    alive.touch()\n'
                    '    assert (cert / "ca.pem").is_file()\n'
                    '    for name in ("client-krb5.conf", "copier.keytab"): (cert / name).touch()\n'
                    'if "images" in sys.argv:\n'
                    '    assert alive.exists() and (cert / "ca.pem").is_file(), "images collected after teardown"\n'
                    '    print(os.environ["IMAGE_OUTPUT"])\n'
                    '    sys.exit(int(os.environ["IMAGE_EXIT"]))\n'
                    'if sys.argv[-1] == "capacity": print("10000000")\n'
                    'if "down" in sys.argv or "stop" in sys.argv: alive.unlink(missing_ok=True)\n'
                    'if "down" in sys.argv and cert != pathlib.Path("/tmp"): sys.exit(int(os.environ["CLEANUP_EXIT"]))\n')
                go = tools / 'go'
                go.write_text('#!' + sys.executable + '\nimport json, os, pathlib, sys\n'
                    'assert pathlib.Path(os.environ["ALIVE"]).exists()\n'
                    'assert "-timeout=10m" in sys.argv\n'
                    'assert (pathlib.Path(os.environ["KAFERRY_REPORT_DIR"]) / "qualification.broker-images.json").is_file()\n'
                    'for name in ("KAFERRY_QUAL_CA", "KAFERRY_QUAL_WRONG_CA", "KAFERRY_QUAL_KRB_CONFIG", "KAFERRY_QUAL_KEYTAB"):\n'
                    '    assert pathlib.Path(os.environ[name]).is_file(), name\n'
                    'for name in ("client.pem", "client.key"):\n'
                    '    assert (pathlib.Path(os.environ["KAFERRY_QUAL_CERT_DIR"]) / name).is_file()\n'
                    'for name in json.loads(os.environ["AUTH_TESTS"]): print(json.dumps(dict(Action="pass", Test=name)))\n'
                    'sys.exit(int(os.environ["SUITE_EXIT"]))\n')
                docker.chmod(0o700); go.chmod(0o700)
                env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'], GO=str(go),
                           CALLS=str(root / 'calls'), ALIVE=str(root / 'alive'), PYTHONPATH=str(ROOT),
                           IMAGE_OUTPUT=image_output, IMAGE_EXIT=str(image_exit), SUITE_EXIT=str(suite_exit),
                           CLEANUP_EXIT=str(cleanup_exit),
                           AUTH_TESTS=json.dumps(EXPECTED['qualification']), EXPECTED_AUTH_IMAGE=IMAGES['3.9.2'])
                args = suite_args(root, 'auth')
                failed = image_exit or suite_exit or cleanup_exit or image_output != images
                with patch.dict(os.environ, env, clear=True), patch('tools.qualification.qualify.ROOT', root), \
                     patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('builtins.print'):
                    # When executing, then image validation occurs before tests and teardown using the resolved environment.
                    if failed:
                        with self.assertRaisesRegex(RuntimeError, f'suite exited {image_exit or suite_exit or cleanup_exit or 1}'):
                            run_suite(args)
                    else:
                        run_suite(args)
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual('failed' if failed else 'passed', receipt['status'])
                if image_output == images and not image_exit:
                    self.assertEqual(images, receipt['broker_images'])
                else:
                    self.assertNotIn('broker_images', receipt)
                    self.assertIn('image_error', receipt)
                self.assertFalse((root / 'alive').exists())
                calls = [json.loads(line) for line in (root / 'calls').read_text().splitlines()]
                self.assertEqual(1 if failed else 0, sum('logs' in call['args'] for call in calls))
                self.assertEqual(bool(failed), (args.output / 'security-brokers.log').exists())
                startup = next(call['args'] for call in calls if 'up' in call['args'])
                self.assertEqual(['up', '-d', '--build', '--wait', '--wait-timeout', '90'], startup[startup.index('up'):])
                for call in calls:
                    if call['cert'] != '/tmp':
                        self.assertFalse(Path(call['cert']).exists(), 'private fixture certificates must be removed')
