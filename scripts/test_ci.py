"""Selected release tests must pass; an empty or skipped selection cannot qualify."""

import json
import os
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ci import summarize, verify_tests


class SelectedTestsTest(unittest.TestCase):
    def test_required_test_must_have_a_completed_pass_event(self):
        # Given renamed, skipped, started or failed selections and one actual pass.
        for action, name, valid in [('pass', 'smoke/disabled', True), ('skip', 'smoke/disabled', False),
                                    ('run', 'smoke/disabled', False), ('fail', 'smoke/disabled', False),
                                    ('pass', 'different', False)]:
            with self.subTest(action=action, name=name):
                log = 'not JSON\n' + json.dumps({'Action': action, 'Test': name})
                # When verified, then only the required passing test satisfies the gate.
                if valid:
                    verify_tests(log, ['smoke/disabled'])
                else:
                    with self.assertRaisesRegex(ValueError, 'did not pass'):
                        verify_tests(log, ['smoke/disabled'])
        with self.assertRaisesRegex(ValueError, 'did not pass'):
            verify_tests('', ['smoke/disabled'])

    def test_endurance_summary_distinguishes_zero_missing_from_incomplete_evidence(self):
        # Given complete and partial delivery evidence with measured destination lag.
        for complete in (True, False):
            with self.subTest(complete=complete), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                measurement = dict(test='soak', passed=False, skipped=False, records=12,
                    records_per_second=1, sampled_peak_rss_bytes=1024,
                    peak_uncommitted_records=2, peak_uncommitted_bytes=4096,
                    max_sampled_destination_lag=11, final_destination_lag=0,
                    max_sampled_destination_consumer_lag=5, final_destination_consumer_lag=0,
                    delivery=dict(submitted=12, published=12, received=13, unique_received=12,
                        missing=0 if complete else None, duplicates=1, corrupted=0,
                        unexpected=0, reordered=0, reconciliation_complete=complete,
                        observed=dict(backlog=0, peak_backlog=13, timed_receipts=12 if complete else 0,
                            max_latency_seconds=0.123 if complete else None, unsettled=0)))
                (directory / 'soak.metrics.json').write_text(json.dumps(measurement))

                # When GitHub's summary is rendered from the retained metrics.
                with redirect_stdout(StringIO()), patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': ''}):
                    summarize(directory)
                summary = (directory / 'summary.md').read_text()

                # Then counts, unresolved evidence and the two lag definitions stay visible.
                self.assertIn('Published (acked)', summary)
                self.assertIn('Destination lag sampled max / final (records)', summary)
                self.assertIn('Consumer lag sampled max / final (offsets)', summary)
                self.assertIn(f'| 12 | 12 | 13 | 12 | {0 if complete else "unresolved"} | 1 |', summary)
                self.assertIn('| 11 / 0 | 5 / 0 |', summary)
                self.assertIn('Observed peak backlog (records)', summary)
                self.assertIn('Maximum delivery time (ms)', summary)
                self.assertIn('| soak | 0 | 13 | 12 | 123.000 | 0 |' if complete else
                              '| soak | 0 | 13 | 0 | unavailable | 0 |', summary)
