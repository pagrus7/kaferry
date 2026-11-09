"""Failed startup must retain owned health diagnostics and its original failure."""

from contextlib import nullcontext
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.qualification.qualify import broker_fixture, startup_health


class StartupHealthTest(unittest.TestCase):
    def setUp(self):
        lease = patch('tools.qualification.qualify.lab_lock', nullcontext)
        lease.start()
        self.addCleanup(lease.stop)

    def fixture(self, directory):
        return (SimpleNamespace(suite='broker', output=directory),
                SimpleNamespace(compose=['docker', 'compose', '-p', 'fixture'],
                                env={'KAFERRY_FIXTURE_PROJECT': 'fixture', 'KAFERRY_SOURCE_IMAGE': 'catalog-image'}))

    def failed_startup(self, args, spec, receipt):
        with self.assertRaises(RuntimeError) as raised:
            with broker_fixture(args, spec, receipt, None, None):
                self.fail('suite must not run after failed startup')
        return raised.exception

    def test_failed_startup_records_owned_health_before_teardown(self):
        # Given an owned broker whose health check never succeeded.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            args, spec = self.fixture(output)
            row = dict(id='a'*64, project='fixture', service='kafka', status='running', running=True,
                       exit_code=0, health=dict(Status='unhealthy', Log=[dict(ExitCode=1, Output='fixture health failure')]))
            order, receipt = [], {}
            original = RuntimeError('original startup failure')
            def command(cmd, directory, label, env, **kwargs):
                order.append(label)
                if label == 'brokers-up':
                    self.assertEqual('120', cmd[cmd.index('--wait-timeout')+1])
                    raise original
                self.assertTrue((directory/'startup-health.json').is_file())
            def docker(cmd, **kwargs):
                if cmd[-3:] == ['ps', '--all', '--quiet']:
                    order.append('selected-containers')
                    return subprocess.CompletedProcess(cmd, 0, stdout=row['id']+'\n')
                if cmd[:2] == ['docker', 'inspect']:
                    order.append('owned-health')
                    self.assertEqual([row['id']], cmd[4:])
                    self.assertNotIn('.Config.Env', cmd[3])
                    self.assertNotIn('.Mounts', cmd[3])
                    return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(row)+'\n')
                order.append('broker-logs')
                return subprocess.CompletedProcess(cmd, 0)
            # When startup fails, then health survives teardown without changing
            # its deadline, rerunning startup, or replacing the original error.
            with patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.subprocess.run', side_effect=docker):
                raised = self.failed_startup(args, spec, receipt)
            self.assertIs(original, raised)
            self.assertEqual(['brokers-up', 'selected-containers', 'owned-health', 'broker-logs', 'brokers-down'], order)
            self.assertEqual(dict(project='fixture', containers=[row]), json.loads((output/'startup-health.json').read_text()))
            self.assertEqual(str(original), receipt['suite_error'])

    def test_foreign_container_health_is_not_published(self):
        # Given an inspect result whose project label differs from the selection.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            args, spec = self.fixture(output)
            row = dict(id='a'*64, project='other-project', service='kafka')
            with patch('tools.qualification.qualify.subprocess.run', side_effect=[
                    subprocess.CompletedProcess([], 0, stdout=row['id']+'\n'),
                    subprocess.CompletedProcess([], 0, stdout=json.dumps(row)+'\n')]):
                # When capturing diagnostics, then reject the foreign result and
                # write no health report for it.
                with self.assertRaisesRegex(ValueError, 'ownership'):
                    startup_health(output, spec.compose, spec.env)
            self.assertFalse((output/'startup-health.json').exists())

    def test_diagnostic_failure_keeps_original_error_and_still_tears_down(self):
        # Given startup failure and a diagnostic subprocess that also times out.
        with tempfile.TemporaryDirectory() as temporary:
            args, spec = self.fixture(Path(temporary))
            receipt, labels = {}, []
            original = RuntimeError('original startup failure')
            def command(cmd, directory, label, env, **kwargs):
                labels.append(label)
                if label == 'brokers-up':
                    raise original
            with patch('tools.qualification.qualify.logged', side_effect=command), \
                 patch('tools.qualification.qualify.startup_health', side_effect=subprocess.TimeoutExpired('inspect', 15)), \
                 patch('tools.qualification.qualify.subprocess.run'):
                # When cleanup runs, then record the diagnostic failure without
                # losing the startup exception or leaving the fixture running.
                raised = self.failed_startup(args, spec, receipt)
            self.assertIs(original, raised)
            self.assertEqual(['brokers-up', 'brokers-down'], labels)
            self.assertEqual('startup-health', receipt['cleanup_errors'][0]['phase'])
            self.assertEqual(str(original), receipt['suite_error'])

    def test_successful_startup_does_not_inspect_containers(self):
        # Given a fixture that becomes healthy, when its suite executes, then
        # the new failure diagnostic never inspects the Docker host.
        with tempfile.TemporaryDirectory() as temporary:
            args, spec = self.fixture(Path(temporary))
            with patch('tools.qualification.qualify.logged') as logged, \
                 patch('tools.qualification.qualify.fixture_images', return_value='fixture'), \
                 patch('tools.qualification.qualify.startup_health') as health, \
                 patch('tools.qualification.qualify.subprocess.run'):
                with broker_fixture(args, spec, {}, None, None):
                    pass
            health.assert_not_called()
            self.assertEqual(['brokers-up', 'brokers-down'], [call.args[2] for call in logged.call_args_list])

    def test_no_selected_containers_does_not_expand_inspection_scope(self):
        # Given startup created no containers in its selected Compose project.
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            args, spec = self.fixture(output)
            with patch('tools.qualification.qualify.subprocess.run', return_value=subprocess.CompletedProcess([], 0, stdout='')) as docker:
                # When retaining diagnostics, then record that empty selection
                # without falling back to an inspection of other host containers.
                startup_health(output, spec.compose, spec.env)
            self.assertEqual(1, docker.call_count)
            self.assertEqual(dict(project='fixture', containers=[]), json.loads((output/'startup-health.json').read_text()))

    def test_replicated_startup_stages_capture_health_before_owned_cleanup(self):
        for phase, diagnostic_failure in [('proxy-up', False), ('proxy-configure', False),
                                           ('brokers-up', False), ('brokers-up', True)]:
            with self.subTest(phase=phase, diagnostic_failure=diagnostic_failure), tempfile.TemporaryDirectory() as temporary:
                # Given a failure during proxy startup, route setup or broker readiness.
                args, spec = self.fixture(Path(temporary))
                args.suite = 'replicated'
                receipt, order, downs = {}, [], []
                original = RuntimeError(phase+' failed')
                def command(cmd, directory, label, env, **kwargs):
                    order.append(label)
                    if label == phase:
                        if label == 'brokers-up':
                            self.assertEqual('120', cmd[cmd.index('--wait-timeout')+1])
                        raise original
                    if label == 'brokers-down':
                        downs.append(cmd)
                def health(*unused):
                    order.append('owned-health')
                    if diagnostic_failure:
                        raise subprocess.TimeoutExpired('inspect', 15)
                def logs(cmd, **kwargs):
                    order.append('broker-logs')
                    return subprocess.CompletedProcess(cmd, 0)
                def cleanup(*unused):
                    order.append('storage-cleanup')
                    return []
                with patch('tools.qualification.qualify.replicated_admission', return_value={}), \
                     patch('tools.qualification.qualify.replicated_storage'), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.startup_health', side_effect=health) as capture, \
                     patch('tools.qualification.qualify.cleanup_replicated', side_effect=cleanup), \
                     patch('tools.qualification.qualify.subprocess.run', side_effect=logs):
                    # When startup fails, then capture health before logs/stop/data
                    # cleanup, preserving the original failure even if inspection fails.
                    raised = self.failed_startup(args, spec, receipt)
                self.assertIs(original, raised)
                stages = ['proxy-up', 'proxy-configure', 'brokers-up']
                self.assertEqual(['storage-image'] + stages[:stages.index(phase)+1] +
                                 ['owned-health', 'broker-logs', 'storage-cleanup', 'brokers-down'], order)
                capture.assert_called_once_with(args.output, spec.compose, spec.env)
                self.assertEqual(str(original), receipt['suite_error'])
                if diagnostic_failure:
                    self.assertEqual('startup-health', receipt['cleanup_errors'][0]['phase'])
                    self.assertNotIn('-v', downs[0])
                else:
                    self.assertNotIn('cleanup_errors', receipt)
                    self.assertIn('-v', downs[0])


if __name__ == '__main__':
    unittest.main()
