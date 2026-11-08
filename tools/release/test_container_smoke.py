"""Container checks preserve wrapper arguments and collect concurrent demo results."""

import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from tools.release.container_smoke import check_demo, write_wrapper


class ContainerWrapperTest(unittest.TestCase):
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

    def test_runtime_working_directory_and_arguments_survive_shell_quoting(self):
        # Given a wrapper and a nested tutorial directory containing relative input files.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name).resolve() / 'checkout with spaces'
            nested = root / 'tutorial files'
            nested.mkdir(parents=True)
            (nested / 'copy.sql').write_text('example')
            tools = root / 'tools'
            tools.mkdir()
            docker = tools / 'docker'
            docker.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$CAPTURE"\n')
            docker.chmod(0o755)
            wrapper = root / 'kaferry'
            write_wrapper(wrapper, root, 'fixture/image:v1')
            capture = root / 'arguments.txt'
            environment = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'], CAPTURE=str(capture))
            # When the caller runs from the nested directory, capture the actual Docker arguments.
            subprocess.run([str(wrapper), 'run', '--file', 'copy.sql'], cwd=nested, env=environment, check=True)
            arguments = capture.read_text().splitlines()
            # Then Docker receives that working directory, the shared mount, and unchanged relative SQL input.
            self.assertEqual(arguments[arguments.index('--workdir') + 1], str(nested))
            self.assertEqual(arguments[arguments.index('--mount') + 1], f'type=bind,src={root},dst={root}')
            self.assertEqual(arguments[-4:], ['fixture/image:v1', 'run', '--file', 'copy.sql'])
