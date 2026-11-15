"""Container checks require complete file-copy and concurrent demo results."""

from contextlib import redirect_stdout
from io import StringIO
import json
import subprocess
import unittest
from unittest.mock import patch

from tools.ci.suites import EXPECTED, PATTERNS
from tools.release.container_smoke import check_file_copy


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
