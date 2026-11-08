"""Authentication storage keeps its existing lifecycle and private certificate paths."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import external_storage_admission, resolve_run, run_suite
from tools.qualification.test_broker_storage import assert_owned_cleanup, suite_args

ROOT = Path(__file__).resolve().parents[2]


class AuthBrokerStorageTest(unittest.TestCase):
    def test_external_auth_keeps_project_identity_and_never_inherits_a_volume(self):
        # Given an unrelated storage selection in the caller's environment.
        with tempfile.TemporaryDirectory() as temporary, \
             patch('tools.qualification.qualify.capture', return_value='fixture'), \
             patch('tools.qualification.qualify.identity_platforms', return_value={}), \
             patch.dict(os.environ, KAFERRY_BROKER_VOLUME='foreign', KAFERRY_BROKER_RUN_ID='f' * 32):
            root, receipt = Path(temporary), {}
            # When resolved, then only the explicit option selects an owned auth namespace.
            default = resolve_run(suite_args(root, 'auth', broker_volume=None), {}, None)
            spec = resolve_run(suite_args(root, 'auth'), receipt, None)
            self.assertEqual([], default.compose)
            self.assertNotIn('KAFERRY_BROKER_VOLUME', default.env)
            self.assertEqual('kaferry-qualification', receipt['fixture_project'])
            self.assertEqual('auth', receipt['fixture_storage']['layout'])
            self.assertEqual(['127.0.0.1:41092,127.0.0.1:42092,127.0.0.1:43092'] * 2,
                             receipt['fixture_brokers'])
            self.assertEqual([spec.env['KAFERRY_AUTH_IMAGE']], receipt['requested_images'])
            self.assertNotIn('private-volume', json.dumps(receipt))

    def test_auth_capacity_and_volume_label_are_checked_before_storage_mutation(self):
        # Given the three-broker allowance or a CPU, memory, evidence or label shortage.
        cases = [(2, 8, 1024**3, True, True), (1, 8, 1024**3, True, False),
                 (2, 7, 1024**3, True, False), (2, 8, 1024**3 - 1, True, False),
                 (2, 8, 1024**3, False, False)]
        for cpus, gib, free, label, accepted in cases:
            with self.subTest(cpus=cpus, gib=gib, free=free, label=label), tempfile.TemporaryDirectory() as temporary:
                with patch('tools.qualification.qualify.capture', side_effect=[
                        json.dumps(dict(NCPU=cpus, MemTotal=gib * 1024**3)),
                        json.dumps({'kaferry.lab.storage': 'true'} if label else {})]), \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=free)):
                    # When admitting the volume, then the rejection leaves a capacity receipt.
                    if accepted:
                        capacity = external_storage_admission(Path(temporary), {'KAFERRY_BROKER_VOLUME': 'lab'}, False, auth=True)
                        self.assertEqual(4 * 1024**3, capacity['required_broker_bytes'])
                    else:
                        with self.assertRaises(ValueError):
                            external_storage_admission(Path(temporary), {'KAFERRY_BROKER_VOLUME': 'lab'}, False, auth=True)
                self.assertEqual(8 * 1024**3, json.loads((Path(temporary) / 'capacity.json').read_text())['required_runtime_memory_bytes'])

    def test_auth_script_owns_startup_and_shared_cleanup_stops_before_deleting(self):
        # Given normal completion, interruption, broker-space shortage or unverified cleanup.
        for failure in (None, 'suite', 'storage-capacity', 'brokers-stop', 'storage-clean'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root, calls = Path(temporary), []
                args = suite_args(root, 'auth')
                def command(command, output, label, env, **kwargs):
                    calls.append((label, command, env.copy()))
                    if label == 'storage-capacity':
                        (output / 'storage-capacity.log').write_text('1\n' if failure == label else '10000000\n')
                    elif label == failure:
                        if label == 'suite':
                            raise KeyboardInterrupt()
                        raise RuntimeError('unverified storage')
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('tools.qualification.qualify.external_storage_admission', return_value={'required_broker_bytes': 4096}), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                    # When the suite finishes, then no outer startup bypasses certificate generation.
                    if failure:
                        expected = KeyboardInterrupt if failure == 'suite' else ValueError if failure == 'storage-capacity' else RuntimeError
                        with self.assertRaises(expected):
                            run_suite(args)
                    else:
                        run_suite(args)
                labels = [label for label, _, _ in calls]
                expected = ['storage-capacity']
                if failure != 'storage-capacity':
                    expected += ['storage-prepare', 'suite']
                expected += ['brokers-stop'] + ([] if failure == 'brokers-stop' else ['storage-clean']) + ['brokers-down']
                self.assertEqual(expected, labels)
                for label, command, env in calls:
                    if label != 'suite':
                        self.assertEqual('kaferry-qualification', command[command.index('-p') + 1])
                        self.assertIn('src/tests/qualification/broker-storage.compose.yaml', command)
                assert_owned_cleanup(self, args, calls, failure)


class AuthStorageShellTest(unittest.TestCase):
    def test_auth_namespace_ownership_symlinks_and_peer_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, run = Path(temporary), 'a' * 32
            env = dict(os.environ, KAFERRY_STORAGE_ROOT=str(root), KAFERRY_STORAGE_LAYOUT='auth',
                       KAFERRY_FIXTURE_PROJECT='kaferry-qualification', KAFERRY_BROKER_RUN_ID=run,
                       KAFERRY_STORAGE_UID=str(os.getuid()), KAFERRY_STORAGE_GID=str(os.getgid()))
            def storage(operation, **changes):
                return subprocess.run(['/bin/sh', str(ROOT / 'src/tests/replicated/storage.sh'), operation],
                                      env=dict(env, **changes), capture_output=True, text=True)
            # Given a prepared auth run, a peer run and unrelated paired storage.
            self.assertEqual(0, storage('prepare').returncode)
            self.assertEqual(0, storage('prepare', KAFERRY_BROKER_RUN_ID='b' * 32).returncode)
            directory = root / 'kaferry-auth/kaferry-qualification' / run
            self.assertEqual({'.kaferry-owned', 'kafka-secure-1', 'kafka-secure-2', 'kafka-secure-3'},
                             {p.name for p in directory.iterdir()})
            paired = root / 'kaferry-paired'; paired.mkdir()
            (paired / 'keep').write_bytes(b'foreign')
            owner = directory / '.kaferry-owned'
            marker = owner.read_text()
            owner.write_text(marker.replace('kaferry-auth', 'kaferry-paired'))
            # When the marker or a broker path is substituted, then all data is preserved.
            self.assertNotEqual(0, storage('clean').returncode)
            owner.write_text(marker)
            broker = directory / 'kafka-secure-3'; broker.rmdir(); broker.symlink_to(paired, target_is_directory=True)
            self.assertNotEqual(0, storage('clean').returncode)
            self.assertEqual(b'foreign', (paired / 'keep').read_bytes())
            broker.unlink(); broker.mkdir()
            # When ownership is restored, then cleanup removes only this UUID and is idempotent.
            self.assertEqual(0, storage('clean').returncode)
            self.assertEqual(0, storage('clean').returncode)
            self.assertFalse(directory.exists())
            self.assertTrue((directory.parent / ('b' * 32)).is_dir())
            self.assertEqual(b'foreign', (paired / 'keep').read_bytes())
            self.assertNotEqual(0, storage('prepare', KAFERRY_FIXTURE_PROJECT='kaferry-compat').returncode)

    def test_auth_runner_preserves_startup_flags_and_temporary_certificate_paths(self):
        # Given the existing shell entrypoint and synthetic Docker/Go executables.
        storage_modes = ({}, {'KAFERRY_BROKER_VOLUME': 'lab', 'KAFERRY_BROKER_RUN_ID': 'a' * 32})
        for storage_env in storage_modes:
            with self.subTest(storage=storage_env), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = root / 'src/tests/qualification'; fixture.mkdir(parents=True)
                shutil.copyfile(ROOT / 'src/tests/qualification/run.sh', fixture / 'run.sh')
                (fixture / 'certificates.sh').write_text('mkdir -p "$1"\ntouch "$1/ca.pem" "$1/wrong-ca.pem" "$1/client.pem" "$1/client.key"\n')
                tools = root / 'tools'; tools.mkdir()
                docker = tools / 'docker'
                docker.write_text('#!' + sys.executable + '\nimport json, os, pathlib, sys\n'
                    'cert = pathlib.Path(os.environ["KAFERRY_QUAL_CERT_DIR"])\n'
                    'with open(os.environ["CALLS"], "a") as log: log.write(json.dumps(dict(args=sys.argv[1:], cert=str(cert))) + "\\n")\n'
                    'if "up" in sys.argv:\n'
                    '    assert (cert / "ca.pem").is_file()\n'
                    '    for name in ("client-krb5.conf", "copier.keytab"): (cert / name).touch()\n')
                go = tools / 'go'
                go.write_text('#!' + sys.executable + '\nimport os, pathlib\n'
                    'for name in ("KAFERRY_QUAL_CA", "KAFERRY_QUAL_WRONG_CA", "KAFERRY_QUAL_KRB_CONFIG", "KAFERRY_QUAL_KEYTAB"):\n'
                    '    assert pathlib.Path(os.environ[name]).is_file(), name\n'
                    'for name in ("client.pem", "client.key"):\n'
                    '    assert (pathlib.Path(os.environ["KAFERRY_QUAL_CERT_DIR"]) / name).is_file()\n')
                docker.chmod(0o700); go.chmod(0o700)
                env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'], GO=str(go),
                           CALLS=str(root / 'calls'), KAFERRY_REPORT_DIR=str(root / 'evidence'))
                env.pop('KAFERRY_BROKER_VOLUME', None)
                env.update(storage_env)
                # When executed without brokers, then startup waits and all host client paths stay valid.
                result = subprocess.run(['/bin/sh', str(fixture / 'run.sh')], cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                calls = [json.loads(line) for line in (root / 'calls').read_text().splitlines()]
                startup = next(call['args'] for call in calls if 'up' in call['args'])
                self.assertEqual(['up', '-d', '--build', '--wait', '--wait-timeout', '90'], startup[startup.index('up'):])
                for call in calls:
                    self.assertEqual(bool(storage_env), 'src/tests/qualification/broker-storage.compose.yaml' in call['args'])
                    self.assertFalse(Path(call['cert']).exists(), 'private fixture certificates must be removed')
