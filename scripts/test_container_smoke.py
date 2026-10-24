"""The container wrapper keeps relative input paths usable from a nested directory."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from container_smoke import write_wrapper


class ContainerWrapperTest(unittest.TestCase):
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
