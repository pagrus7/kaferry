"""Failed fixture startup preserves its error, logs and teardown attempt."""

from contextlib import nullcontext
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.qualification.qualify import broker_fixture


class StartupCleanupTest(unittest.TestCase):
    def test_fixture_collects_bulk_logs_only_after_failure_or_inconclusive_result(self):
        # Given default/replicated startup stages and the possible suite outcomes.
        startup = ('default', 'proxy-up', 'proxy-configure', 'brokers-up')
        for phase in (*startup, 'passed', 'suite-failed', 'inconclusive', 'teardown-failed'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temporary:
                output, order, receipt = Path(temporary), [], {}
                args = SimpleNamespace(suite='replicated' if phase in startup[1:] else 'broker', output=output)
                spec = SimpleNamespace(compose=['docker', 'compose', '-p', 'owned'], env={})
                original = RuntimeError('original failure')
                def command(cmd, directory, label, env, **kwargs):
                    order.append(label)
                    if phase in startup and label == ('brokers-up' if phase == 'default' else phase):
                        raise original
                    if label == 'brokers-down' and phase in (*startup, 'teardown-failed'):
                        raise RuntimeError('cleanup failed')
                def logs(cmd, **kwargs):
                    self.assertEqual(['logs', '--no-color'], cmd[-2:])
                    order.append('logs')
                    kwargs['stdout'].write('broker failure output')
                with patch('tools.qualification.qualify.lab_lock', nullcontext), \
                     patch('tools.qualification.qualify.fixture_images', return_value='fixture'), \
                     patch('tools.qualification.qualify.replicated_admission', return_value={}), \
                     patch('tools.qualification.qualify.replicated_storage'), \
                     patch('tools.qualification.qualify.cleanup_replicated', return_value=[]), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run', side_effect=logs) as capture:
                    # When startup, copying or cleanup fails, then logs and the original error survive.
                    try:
                        with broker_fixture(args, spec, receipt, None):
                            self.assertNotIn(phase, startup, 'suite ran after failed startup')
                            if phase == 'suite-failed':
                                raise original
                            if phase == 'inconclusive':
                                receipt['fault_results'] = {'status': 'inconclusive'}
                    except RuntimeError as error:
                        if phase in (*startup, 'suite-failed'):
                            self.assertIs(original, error)
                            self.assertEqual(str(original), receipt['suite_error'])
                        else:
                            self.assertEqual('teardown-failed', phase)
                    else:
                        self.assertIn(phase, ('passed', 'inconclusive'))
                self.assertEqual(0 if phase == 'passed' else 1, capture.call_count)
                self.assertEqual(1, order.count('brokers-down'))
                if phase != 'passed':
                    self.assertEqual('broker failure output', (output / 'brokers.log').read_text())
                else:
                    self.assertFalse((output / 'brokers.log').exists())
                if phase in (*startup, 'inconclusive', 'suite-failed'):
                    self.assertLess(order.index('logs'), order.index('brokers-down'))
                if phase in (*startup, 'teardown-failed'):
                    self.assertEqual('teardown', receipt['cleanup_errors'][0]['phase'])
