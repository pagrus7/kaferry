"""External paired broker storage must stay inside a verified run namespace."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.qualify import (
    main, paired_storage_admission, parallel_run,
    resolve_run, run_suite,
)


def suite_args(root, suite='recovery', **changes):
    binary = root / 'candidate'
    binary.write_bytes(b'candidate')
    values = dict(suite=suite, binary=str(binary), output=root / 'evidence', source='3.9.2',
                  destination='3.9.2', config=None, rate=200 if suite == 'endurance' else 50,
                  duration=60, broker_volume='private-volume', state_contract='resume', previous_binary=binary)
    values.update(changes)
    return argparse.Namespace(**values)


class PairedBrokerStorageTest(unittest.TestCase):
    def setUp(self):
        checker = patch('tools.qualification.fault_results.configure',
                        new=lambda env: env.update(KAFERRY_ORACLE_RUN='fixture'))
        checker.start()
        self.addCleanup(checker.stop)
        for target, value in [('capture', 'fixture'), ('identity_platforms', {
                'binary_platform': 'linux-amd64', 'execution_platform': 'linux-amd64'})]:
            mock = patch('tools.qualification.qualify.' + target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    def test_supported_suites_get_fresh_owned_storage_without_changing_workload(self):
        # Given each existing paired suite and explicit external broker storage.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_ids = set()
            for suite in ('broker', 'fault', 'load', 'soak', 'capture', 'endurance', 'recovery', 'integration', 'migration'):
                with self.subTest(suite=suite):
                    args = suite_args(root, suite)
                    receipt, default = {}, {}
                    # When resolved, then only storage selection differs from the default fixture.
                    external = resolve_run(args, receipt, None)
                    normal = resolve_run(suite_args(root, suite, broker_volume=None), default, None)
                    self.assertEqual(normal.command, external.command)
                    self.assertEqual(normal.timeout, external.timeout)
                    self.assertEqual(normal.expected, external.expected)
                    self.assertIn('src/tests/broker-storage.compose.yaml', external.compose)
                    self.assertNotIn('src/tests/broker-storage.compose.yaml', normal.compose)
                    self.assertNotIn('fixture_storage', default)
                    self.assertEqual('private-volume', external.env['KAFERRY_BROKER_VOLUME'])
                    run_id = receipt['fixture_storage']['run_id']
                    self.assertRegex(run_id, r'^[a-f0-9]{32}$')
                    self.assertNotIn(run_id, run_ids)
                    run_ids.add(run_id)
                    self.assertNotIn('private-volume', json.dumps(receipt))

    def test_invalid_suites_names_and_migration_rejection_never_inspect_docker(self):
        # Given inputs that cannot own a paired broker fixture.
        cases = [(suite, 'lab', 'resume') for suite in ('auth', 'cloud', 'model', 'resources', 'fuzz', 'benchmark', 'smoke')]
        cases += [('migration', 'lab', 'reject')]
        cases += [('broker', name, 'resume') for name in ('', '-foreign', '../volume', '/absolute', 'a/b')]
        with tempfile.TemporaryDirectory() as temporary:
            for suite, volume, contract in cases:
                with self.subTest(suite=suite, volume=volume), patch('tools.qualification.qualify.capture') as execute:
                    # When resolving, then reject before executing any external command.
                    with self.assertRaisesRegex(ValueError, 'broker-volume'):
                        resolve_run(suite_args(Path(temporary), suite, broker_volume=volume, state_contract=contract), {}, None)
                    execute.assert_not_called()

    def test_parallel_propagates_volume_with_separate_projects_and_run_ids(self):
        # Given the public parallel option for either existing companion.
        for companion in ('recovery', 'capture'):
            with self.subTest(companion=companion), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                args = suite_args(root, companion=companion, companion_duration=60, output=root / 'pair')
                observed = []
                def run(case, *, fixture, cancelled):
                    receipt = {}
                    spec = resolve_run(case, receipt, fixture)
                    observed.append((receipt, spec))
                    case.output.mkdir()
                    (case.output / 'manifest.json').write_text(json.dumps(dict(receipt, status='passed')))
                # When both jobs are assigned, then the shared volume has independent ownership identities.
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.run_suite', side_effect=run):
                    parallel_run(args)
                self.assertEqual(2, len({receipt['fixture_project'] for receipt, _ in observed}))
                self.assertEqual(2, len({receipt['fixture_storage']['run_id'] for receipt, _ in observed}))
                self.assertEqual({'private-volume'}, {spec.env['KAFERRY_BROKER_VOLUME'] for _, spec in observed})
                self.assertNotIn('private-volume', (args.output / 'parallel.json').read_text())
        with patch('tools.qualification.qualify.parallel_run') as run:
            main(['parallel', '--broker-volume', 'lab', '--duration', '1m', '--output', 'unused'])
        self.assertEqual('lab', run.call_args.args[0].broker_volume)

    def test_parallel_binary_identity_failure_cannot_start_storage(self):
        # Given a coordinator identity that differs from the selected executable.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = dict(project='kaferry-lab-test-1', ports=[45192, 45193], binary_sha256='a' * 64)
            with patch('tools.qualification.qualify.logged') as execute:
                # When the child resolves, then mismatch fails before storage or brokers run.
                with self.assertRaisesRegex(ValueError, 'binary changed'):
                    run_suite(suite_args(root), fixture=fixture)
            execute.assert_not_called()

    def test_cleanup_stops_writers_and_preserves_data_on_unverified_shutdown_or_owner(self):
        # Given success, interruption, failed shutdown or a changed ownership marker.
        for failure in (None, 'suite', 'brokers-stop', 'storage-clean'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root, calls = Path(temporary), []
                args = suite_args(root)
                def command(command, output, label, env, **kwargs):
                    calls.append((label, command, env.copy()))
                    if label == 'storage-capacity':
                        (output / 'storage-capacity.log').write_text('10000000\n')
                    if label == failure:
                        if label == 'suite':
                            raise KeyboardInterrupt()
                        raise RuntimeError('unverified storage')
                with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.paired_storage_admission', return_value={'required_broker_bytes': 4096}), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                    # When finishing, then only this project stops and cleanup follows a successful stop.
                    if failure:
                        with self.assertRaises(KeyboardInterrupt if failure == 'suite' else RuntimeError):
                            run_suite(args)
                    else:
                        run_suite(args)
                labels = [label for label, _, _ in calls]
                expected = ['storage-capacity', 'storage-prepare', 'brokers-up', 'suite', 'brokers-stop']
                if failure != 'brokers-stop':
                    expected.append('storage-clean')
                self.assertEqual(expected + ['brokers-down'], labels)
                for label, command, env in calls:
                    if label != 'suite':
                        self.assertEqual('kaferry-compat', command[command.index('-p') + 1])
                    self.assertNotEqual(['docker', 'volume', 'rm'], command[:3])
                self.assertEqual(1, len({env['KAFERRY_BROKER_RUN_ID'] for _, _, env in calls}))
                if failure in ('brokers-stop', 'storage-clean'):
                    self.assertNotIn('-v', calls[-1][1])
                receipt = json.loads((args.output / 'manifest.json').read_text())
                self.assertEqual('passed' if failure is None else 'interrupted' if failure == 'suite' else 'failed', receipt['status'])
                self.assertNotIn('private-volume', json.dumps(receipt))

    def test_combined_disk_shortage_prevents_child_workload_from_starting(self):
        # Given a paired child whose broker filesystem cannot hold the coordinator's full allowance.
        with tempfile.TemporaryDirectory() as temporary:
            root, calls = Path(temporary), []
            args = suite_args(root)
            fixture = dict(project='kaferry-lab-test-1', ports=[45192, 45193], reserved_bytes=8192,
                           binary_sha256=hashlib.sha256(Path(args.binary).read_bytes()).hexdigest())
            def command(command, output, label, env, **kwargs):
                calls.append(label)
                if label == 'storage-capacity':
                    (output / 'storage-capacity.log').write_text('10000000\n')
            with patch('tools.qualification.qualify.paired_storage_admission', return_value={'required_broker_bytes': 4096}), \
                 patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)), \
                 patch('tools.qualification.qualify.subprocess.check_output', return_value='fs 0 0 7 0% /tmp/kaferry-logs\n') as disk, \
                 patch('tools.qualification.qualify.subprocess.run'):
                # When admission checks the mounted path, then the suite cannot execute and cleanup still runs.
                with self.assertRaisesRegex(ValueError, 'combined'):
                    run_suite(args, fixture=fixture)
            self.assertNotIn('suite', calls)
            self.assertEqual(['brokers-stop', 'storage-clean', 'brokers-down'], calls[-3:])
            self.assertEqual({'kafka', 'kafka-dr'}, {call.args[0][-4] for call in disk.call_args_list})
            self.assertTrue(all(call.args[0][-1] == '/tmp/kaferry-logs' for call in disk.call_args_list))

    def test_replicated_endurance_uses_replicated_storage_and_preserves_unverified_data(self):
        # Given replicated endurance with explicit external storage and failed cleanup validation.
        with tempfile.TemporaryDirectory() as temporary:
            root, calls = Path(temporary), []
            args = suite_args(root, 'endurance', topology='replicated', source='4.4.0', destination='4.4.0')
            def command(command, output, label, env, **kwargs):
                self.assertEqual('kaferry-replicated', env['KAFERRY_FIXTURE_PROJECT'])
                self.assertNotIn('KAFERRY_BROKER_RUN_ID', env)
                calls.append((label, command))
                if label == 'storage-capacity':
                    (output / 'storage-capacity.log').write_text('10000000\n')
                if label == 'storage-clean':
                    raise RuntimeError('ownership mismatch')
            with patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                 patch('tools.qualification.qualify.replicated_admission', return_value={'required_broker_bytes': 4096}) as admit, \
                 patch('tools.qualification.qualify.paired_storage_admission', side_effect=AssertionError('paired admission used')), \
                 patch('tools.qualification.qualify.endurance_capacity', side_effect=AssertionError('RF=1 disk allowance used')), \
                 patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.subprocess.run'), patch('tools.qualification.qualify.verify_run'):
                # When finishing, then the duration reaches RF=3 admission and only enabled proxies are configured.
                with self.assertRaisesRegex(RuntimeError, 'teardown failed'):
                    run_suite(args)
            self.assertEqual(60, admit.call_args.args[2])
            self.assertEqual(['storage-capacity', 'storage-prepare', 'proxy-up', 'proxy-configure',
                              'brokers-up', 'suite', 'brokers-stop', 'storage-clean', 'brokers-down'],
                             [label for label, _ in calls])
            self.assertNotIn('-v', calls[-1][1])
            receipt = json.loads((args.output / 'manifest.json').read_text())
            self.assertEqual('failed', receipt['status'])
            self.assertEqual(3, receipt['workload']['replication_factor'])
            self.assertNotIn('layout', receipt['fixture_storage'])


class PairedAdmissionTest(unittest.TestCase):
    def test_capacity_requires_combined_cpu_memory_and_evidence_before_volume_use(self):
        # Given capacity that fits one two-broker fixture, but not necessarily both jobs.
        cases = [(False, 2, 4, 1024**3, True), (True, 2, 4, 1024**3, False),
                 (True, 4, 7, 1024**3, False), (True, 3, 8, 1024**3, False),
                 (True, 4, 8, 1024**3 - 1, False), (True, 4, 8, 1024**3, True)]
        for parallel, cpus, gib, free, accepted in cases:
            with self.subTest(parallel=parallel, cpus=cpus, gib=gib, free=free), tempfile.TemporaryDirectory() as temporary:
                runtime = dict(NCPU=cpus, MemTotal=gib * 1024**3)
                with patch('tools.qualification.qualify.capture', side_effect=[json.dumps(runtime), '{"kaferry.lab.storage":"true"}']) as execute, \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=free)):
                    # When admitting, then shortages fail before inspecting or using the volume.
                    if accepted:
                        result = paired_storage_admission(Path(temporary), {'KAFERRY_BROKER_VOLUME': 'lab'}, parallel)
                        self.assertEqual(4 if parallel else 2, result['required_cpus'])
                    else:
                        with self.assertRaisesRegex(ValueError, 'capacity'):
                            paired_storage_admission(Path(temporary), {'KAFERRY_BROKER_VOLUME': 'lab'}, parallel)
                        self.assertEqual(1, execute.call_count)
                saved = json.loads((Path(temporary) / 'capacity.json').read_text())
                self.assertEqual(cpus, saved['runtime_cpus'])
                self.assertEqual((8 if parallel else 4) * 1024**3, saved['required_runtime_memory_bytes'])

    def test_missing_volume_or_label_cannot_start_initializer_or_brokers(self):
        # Given a missing volume or a volume without the explicit lab-storage label.
        for labels in ('null', '{}', '{"kaferry.lab.storage":"false"}', subprocess.CalledProcessError(1, 'inspect')):
            with self.subTest(labels=labels), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                args = suite_args(root, 'broker')
                with patch('tools.qualification.qualify.identity_platforms', return_value={}), \
                     patch('tools.qualification.qualify.LAB_LOCK', root / 'lease'), \
                     patch('tools.qualification.qualify.capture', side_effect=['commit', '', 'go', 'binary',
                         json.dumps({'NCPU': 4, 'MemTotal': 8 * 1024**3}), labels]) as inspect, \
                     patch('tools.qualification.qualify.shutil.disk_usage', return_value=argparse.Namespace(free=10**12)), \
                     patch('tools.qualification.qualify.logged') as execute:
                    # When running, then admission failure preserves the existing volume without cleanup commands.
                    with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                        run_suite(args)
                    execute.assert_not_called()
                    self.assertEqual(6, inspect.call_count)
                    self.assertEqual(['docker', 'volume', 'inspect'], inspect.call_args.args[0][:3])


class PairedStorageShellTest(unittest.TestCase):
    def storage(self, root, run, operation, project='kaferry-compat'):
        script = Path(__file__).resolve().parents[2] / 'src/tests/replicated/storage.sh'
        env = dict(os.environ, KAFERRY_STORAGE_ROOT=str(root), KAFERRY_STORAGE_LAYOUT='paired',
                   KAFERRY_FIXTURE_PROJECT=project, KAFERRY_BROKER_RUN_ID=run,
                   KAFERRY_STORAGE_UID=str(os.getuid()), KAFERRY_STORAGE_GID=str(os.getgid()))
        return subprocess.run(['/bin/sh', str(script), operation], env=env, capture_output=True, text=True)

    def test_cleanup_removes_only_marked_run_among_other_projects_and_layouts(self):
        # Given peer runs, a peer project and unrelated replicated/external volume bytes.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runs = [('kaferry-compat', 'a' * 32), ('kaferry-compat', 'b' * 32), ('kaferry-lab-peer-2', 'a' * 32)]
            for project, run in runs:
                result = self.storage(root, run, 'prepare', project)
                self.assertEqual(0, result.returncode, result.stderr)
                (root / 'kaferry-paired' / project / run / 'kafka/data').write_bytes(b'broker bytes')
            unrelated = root / 'kaferry-replicated'; unrelated.mkdir()
            (unrelated / 'keep').write_bytes(b'foreign bytes')
            # When one run is cleaned, then every peer and unrelated byte remains.
            self.assertEqual(0, self.storage(root, 'a' * 32, 'clean').returncode)
            self.assertFalse((root / 'kaferry-paired/kaferry-compat' / ('a' * 32)).exists())
            for project, run in runs[1:]:
                self.assertEqual(b'broker bytes', (root / 'kaferry-paired' / project / run / 'kafka/data').read_bytes())
            self.assertEqual(b'foreign bytes', (unrelated / 'keep').read_bytes())
            self.assertEqual(0, self.storage(root, 'a' * 32, 'clean').returncode)

    def test_reuse_missing_and_mismatched_ownership_preserve_all_existing_bytes(self):
        for marker in (None, 'kaferry-paired-v1\nkaferry-endurance\n' + 'a' * 32 + '\n',
                       'kaferry-paired-v1\nkaferry-compat\n' + 'b' * 32 + '\n',
                       'kaferry-replicated-v1\nkaferry-compat\n' + 'a' * 32 + '\n'):
            with self.subTest(marker=marker), tempfile.TemporaryDirectory() as temporary:
                root, run = Path(temporary), 'a' * 32
                self.assertEqual(0, self.storage(root, run, 'prepare').returncode)
                directory = root / 'kaferry-paired/kaferry-compat' / run
                data = directory / 'kafka/data'; data.write_bytes(b'preserve')
                # Given existing data, when prepared again, then it cannot be reused.
                self.assertNotEqual(0, self.storage(root, run, 'prepare').returncode)
                owner = directory / '.kaferry-owned'
                if marker is None:
                    owner.unlink()
                else:
                    owner.write_text(marker)
                # When ownership is missing or refers to a different identity, then cleanup refuses.
                self.assertNotEqual(0, self.storage(root, run, 'clean').returncode)
                self.assertEqual(b'preserve', data.read_bytes())

    def test_symlink_substitutions_are_rejected_without_following_foreign_targets(self):
        for target in ('root', 'namespace', 'project', 'run', 'marker', 'broker'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temporary:
                base, run = Path(temporary), 'a' * 32
                root = base / 'volume'; root.mkdir()
                self.assertEqual(0, self.storage(root, run, 'prepare').returncode)
                namespace = root / 'kaferry-paired'
                project = namespace / 'kaferry-compat'
                directory = project / run
                path = {'root': root, 'namespace': namespace, 'project': project, 'run': directory,
                        'marker': directory / '.kaferry-owned', 'broker': directory / 'kafka'}[target]
                foreign = base / 'foreign'; foreign.mkdir()
                (foreign / 'keep').write_bytes(b'preserve')
                moved = base / 'moved'; path.rename(moved)
                path.symlink_to(moved if target == 'marker' else foreign, target_is_directory=target != 'marker')
                # Given any substituted ownership path, when cleaned, then reject and keep foreign bytes.
                self.assertNotEqual(0, self.storage(root, run, 'clean').returncode)
                self.assertEqual(b'preserve', (foreign / 'keep').read_bytes())

    def test_invalid_projects_and_run_ids_cannot_prepare_or_delete_storage(self):
        # Given identities outside the disposable fixture naming contract.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for project, run in [('foreign', 'a' * 32), ('kaferry-lab-../other', 'a' * 32),
                                 ('kaferry-compat', 'a' * 31), ('kaferry-compat', '../' + 'a' * 32)]:
                for operation in ('prepare', 'clean'):
                    with self.subTest(project=project, run=run, operation=operation):
                        # When invoked, then fail before creating or deleting any directory.
                        self.assertNotEqual(0, self.storage(root, run, operation, project).returncode)
                        self.assertEqual([], list(root.iterdir()))
