"""CI summaries expose expensive tests without counting parent/subtest time twice."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ci import capture, summarize


class CISummaryTest(unittest.TestCase):
    def test_missing_optional_metadata_tool_does_not_stop_the_command(self):
        # Given a developer machine without the optional Docker CLI.
        with patch('ci.subprocess.run', side_effect=FileNotFoundError):
            # When reading metadata, then unavailable data stays empty.
            self.assertEqual(capture(['docker', 'version']), '')

    def test_slowest_tests_use_completed_json_events(self):
        # Given mixed command output and Go test events.
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            events = [{'Action': 'run', 'Test': 'unfinished', 'Elapsed': 100},
                      {'Action': 'pass', 'Test': 'quick', 'Package': 'p', 'Elapsed': .1},
                      {'Action': 'fail', 'Test': 'slow', 'Package': 'p', 'Elapsed': 9}]
            (root / 'unit.log').write_text('not JSON\n{invalid\n' + '\n'.join(map(json.dumps, events)))
            with patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(root / 'step.md')}), redirect_stdout(io.StringIO()):
                # When summarized, then completed slow tests appear first without invented timing.
                summarize(root)
            result = (root / 'summary.md').read_text()
            self.assertLess(result.index('| slow |'), result.index('| quick |'))
            self.assertNotIn('| unfinished |', result)
            self.assertIn('must not be added together', result)
