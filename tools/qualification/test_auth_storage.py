"""Authentication storage keeps its existing lifecycle and private certificate paths."""

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.suites import EXPECTED
from tools.qualification.qualify import IMAGES, external_storage_admission, resolve_run, run_suite, validate_auth_storage
from tools.qualification.test_broker_storage import suite_args

ROOT = Path(__file__).resolve().parents[2]


class AuthComposeConfigTest(unittest.TestCase):
    def configurations(self):
        env = dict(KAFERRY_QUAL_CERT_DIR='/tmp', KAFERRY_BROKER_VOLUME='LAB_VOLUME', KAFERRY_BROKER_RUN_ID='a' * 32)
        certificate = dict(type='bind', source='/tmp', target='/mnt/certs')
        services = {'kdc': dict(volumes=[certificate])}
        for number in range(1, 4):
            services[f'kafka-secure-{number}'] = dict(environment={'KAFKA_HEAP_OPTS': '-Xms128m -Xmx512m'},
                volumes=[dict(certificate, read_only=True)])
        default = dict(services=services)
        external = deepcopy(default)
        external['volumes'] = {'broker-data': dict(name='LAB_VOLUME', external=True)}
        for name, service in external['services'].items():
            if name == 'kdc':
                continue
            directory = '/tmp/kaferry-logs/kaferry-auth/kaferry-qualification/' + 'a' * 32 + '/' + name
            service['environment'].update(KAFKA_LOG_DIRS=directory, KAFKA_METADATA_LOG_DIR=directory)
            service['user'] = '1000:1000'
            service['volumes'].append(dict(type='volume', source='broker-data', target='/tmp/kaferry-logs'))
        return default, external, env

    def test_external_storage_keeps_default_certificate_and_kdc_configuration(self):
        # Given merged default/external configurations, when checked, then only bulk storage differs.
        default, external, env = self.configurations()
        before = deepcopy((default, external))
        validate_auth_storage(default, external, env)
        self.assertEqual(before, (default, external), 'validation must not mutate the rendered configurations')

    def test_rejects_unbacked_data_metadata_paths_and_changed_certificate_mounts(self):
        # Given misplaced log paths, a different volume or changed broker/KDC configuration.
        cases = [('external', ('volumes', 'broker-data', 'name'), 'other-volume'),
                 ('external', ('volumes', 'broker-data', 'external'), False),
                 ('external', ('services', 'kdc', 'volumes', 0, 'source'), '/other-certs'),
                 ('external', ('services', 'kdc', 'volumes', 0, 'read_only'), True),
                 ('default', ('services', 'kdc', 'volumes'), [])]
        for number in range(1, 4):
            broker = ('services', f'kafka-secure-{number}')
            cases += [('external', broker + ('environment', key), '/tmp/kraft-combined-logs')
                      for key in ('KAFKA_LOG_DIRS', 'KAFKA_METADATA_LOG_DIR')]
            cases += [('external', broker + ('volumes', 1, key), value) for key, value in (
                ('source', 'other-volume'), ('target', '/unused-volume'), ('type', 'bind'), ('read_only', True))]
            cases += [('external', broker + ('volumes', 0, 'source'), '/other-certs'),
                      ('external', broker + ('volumes', 0, 'read_only'), False),
                      ('external', broker + ('environment', 'KAFKA_LOG_DIRS'),
                       '/tmp/kaferry-logs/kaferry-auth/kaferry-qualification/' + 'b' * 32 + f'/kafka-secure-{number}'),
                      ('external', broker + ('environment', 'KAFKA_HEAP_OPTS'), '-Xmx2g')]
        for selection, path, value in cases:
            with self.subTest(configuration=selection, path=path, value=value):
                default, external, env = self.configurations()
                changed = default if selection == 'default' else external
                for key in path[:-1]:
                    changed = changed[key]
                changed[path[-1]] = value
                # When the actual-render contract sees the mutation, then qualification cannot continue.
                with self.assertRaises(ValueError):
                    validate_auth_storage(default, external, env)


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

    def test_auth_runner_checks_live_images_and_preserves_paths_and_original_failures(self):
        # Given actual runner/shell execution with synthetic Docker/Go executables.
        storage_modes = ({}, {'KAFERRY_BROKER_VOLUME': 'lab', 'KAFERRY_BROKER_RUN_ID': 'a' * 32})
        images = json.dumps([dict(ID='sha256:' + 'a' * 64,
                                 ContainerName=f'kaferry-qualification-kafka-secure-{i}-1') for i in range(1, 4)])
        cases = [(images, 0, 0, 0), ('', 0, 0, 0), ('[]', 0, 0, 0), ('[{"ID":""}]', 0, 0, 0),
                 ('invalid JSON', 0, 0, 0), (images, 13, 0, 0), (images, 0, 7, 0),
                 (images, 0, 0, 9), (images, 0, 7, 9)]
        for storage_env, (image_output, image_exit, suite_exit, cleanup_exit) in (
                (storage, case) for storage in storage_modes for case in cases):
            with self.subTest(storage=storage_env, images=image_output, exits=(image_exit, suite_exit, cleanup_exit)), \
                 tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fixture = root / 'src/tests/qualification'; fixture.mkdir(parents=True)
                shutil.copyfile(ROOT / 'src/tests/qualification/run.sh', fixture / 'run.sh')
                (fixture / 'certificates.sh').write_text('mkdir -p "$1"\ntouch "$1/ca.pem" "$1/wrong-ca.pem" "$1/client.pem" "$1/client.key"\n')
                storage = root / 'src/tests/replicated'; storage.mkdir()
                shutil.copyfile(ROOT / 'src/tests/replicated/storage.sh', storage / 'storage.sh')
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
                    '    assert bool(os.environ.get("KAFERRY_BROKER_VOLUME")) == bool(int(os.environ["EXTERNAL"]))\n'
                    '    if os.environ["EXTERNAL"] == "1": assert len(os.environ["KAFERRY_BROKER_RUN_ID"]) == 32\n'
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
                           CLEANUP_EXIT=str(cleanup_exit), EXTERNAL=str(int(bool(storage_env))),
                           AUTH_TESTS=json.dumps(EXPECTED['qualification']), EXPECTED_AUTH_IMAGE=IMAGES['3.9.2'])
                env.pop('KAFERRY_BROKER_VOLUME', None)
                env.pop('KAFERRY_BROKER_RUN_ID', None)
                args = suite_args(root, 'auth', broker_volume='lab' if storage_env else None)
                failed = image_exit or suite_exit or cleanup_exit or image_output != images
                with patch.dict(os.environ, env, clear=True), patch('tools.qualification.qualify.ROOT', root), \
                     patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', return_value='fixture'), \
                     patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('tools.qualification.qualify.external_storage_admission', return_value={'required_broker_bytes': 4096}), \
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
                startup = next(call['args'] for call in calls if 'up' in call['args'])
                self.assertEqual(['up', '-d', '--build', '--wait', '--wait-timeout', '90'], startup[startup.index('up'):])
                for call in calls:
                    self.assertEqual(bool(storage_env), 'src/tests/qualification/broker-storage.compose.yaml' in call['args'])
                    if call['cert'] != '/tmp':
                        self.assertFalse(Path(call['cert']).exists(), 'private fixture certificates must be removed')
