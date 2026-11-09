"""Failed fixture startup preserves its error, logs and teardown attempt."""

from contextlib import nullcontext
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools.qualification.qualify import broker_fixture


class StartupCleanupTest(unittest.TestCase):
    def test_failed_startup_keeps_logs_and_original_failure_through_cleanup(self):
        # Given failure at a default or replicated fixture startup stage.
        for phase in ('default', 'proxy-up', 'proxy-configure', 'brokers-up'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temporary:
                output, order, receipt = Path(temporary), [], {}
                args = SimpleNamespace(suite='broker' if phase == 'default' else 'replicated', output=output)
                spec = SimpleNamespace(compose=['docker', 'compose', '-p', 'owned'], env={})
                original = RuntimeError('startup failed')
                def command(cmd, directory, label, env, **kwargs):
                    order.append(label)
                    if label == ('brokers-up' if phase == 'default' else phase):
                        raise original
                    if label == 'brokers-down':
                        raise RuntimeError('cleanup failed')
                def logs(cmd, **kwargs):
                    order.append('logs')
                    kwargs['stdout'].write('broker failure output')
                with patch('tools.qualification.qualify.lab_lock', nullcontext), \
                     patch('tools.qualification.qualify.replicated_admission', return_value={}), \
                     patch('tools.qualification.qualify.replicated_storage'), \
                     patch('tools.qualification.qualify.cleanup_replicated', return_value=[]), \
                     patch('tools.qualification.qualify.logged', side_effect=command), \
                     patch('tools.qualification.qualify.subprocess.run', side_effect=logs):
                    # When startup and teardown fail, then keep the first error and ordinary logs.
                    with self.assertRaises(RuntimeError) as raised:
                        with broker_fixture(args, spec, receipt, None):
                            self.fail('suite ran after failed startup')
                self.assertIs(original, raised.exception)
                self.assertEqual(['logs', 'brokers-down'], order[-2:])
                self.assertEqual('broker failure output', (output / 'brokers.log').read_text())
                self.assertEqual('startup failed', receipt['suite_error'])
                self.assertEqual('teardown', receipt['cleanup_errors'][0]['phase'])
