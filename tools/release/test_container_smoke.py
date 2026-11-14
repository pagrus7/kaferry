"""Container checks require complete file-copy and concurrent demo results."""

from contextlib import redirect_stdout
from io import StringIO
import json
import subprocess
import threading
import unittest
from unittest.mock import patch

from tools.ci.suites import EXPECTED, PATTERNS
from tools.release.container_smoke import check_demo, check_file_copy


class ContainerWrapperTest(unittest.TestCase):
    def test_file_copy_requires_exact_passes_and_rejects_unexpected_skips(self):
        # Given a successful Go exit with complete, partial or skipped evidence.
        passes = [dict(Action='pass', Test=name) for name in EXPECTED['smoke']]
        cases = [(passes, 0, None), (passes[:-1], 0, ValueError),
                 (passes[:-1] + [dict(Action='skip', Test=EXPECTED['smoke'][-1])], 0, ValueError),
                 (passes + [dict(Action='skip', Test='Unexpected')], 0, ValueError),
                 (passes, 17, subprocess.CalledProcessError)]
        for events, code, error in cases:
            with self.subTest(events=events, code=code):
                log = '\n'.join(json.dumps(event) for event in events)
                result = subprocess.CompletedProcess(['go'], code, stdout=log)
                env = dict(KAFERRY_TEST_BINARY='/wrapper')
                with patch('tools.release.container_smoke.subprocess.run', return_value=result) as run, redirect_stdout(StringIO()):
                    # When the container's wrapper is exercised, then use the
                    # shared selector and never qualify exit zero alone.
                    if error:
                        with self.assertRaises(error):
                            check_file_copy(env)
                    else:
                        check_file_copy(env)
                run.assert_called_once_with(['go', 'test', '-count=1', '-json', '-tags=integration,load', '-run',
                                             PATTERNS['smoke'], './src/tests/system'],
                                            env=env, text=True, stdout=subprocess.PIPE, check=False)

    def test_demo_checks_overlap_and_collect_every_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                # Given checks that cannot finish until all four containers have started.
                barrier = threading.Barrier(4, timeout=5)
                lock = threading.Lock()
                observed = []

                def check(name):
                    def run(image):
                        self.assertEqual('demo:fixture', image)
                        barrier.wait()
                        with lock:
                            observed.append(name)
                        if fail:
                            raise ValueError(name + ' assertion failed')
                    return run

                with patch('tools.release.container_smoke.check_demo_fresh', side_effect=check('fresh')), \
                     patch('tools.release.container_smoke.check_demo_corruption', side_effect=check('corruption')), \
                     patch('tools.release.container_smoke.check_demo_cancellation', side_effect=check('cancellation')):
                    # When checks run together, then success waits for all and failure names every case.
                    if fail:
                        with self.assertRaises(ValueError) as raised:
                            check_demo('demo:fixture')
                        for name in ('fresh run 1', 'fresh run 2', 'corrupted output', 'cancellation'):
                            self.assertIn(name + ':', str(raised.exception))
                        self.assertEqual(4, str(raised.exception).count('assertion failed'))
                    else:
                        check_demo('demo:fixture')
                self.assertCountEqual(['fresh', 'fresh', 'corruption', 'cancellation'], observed)
