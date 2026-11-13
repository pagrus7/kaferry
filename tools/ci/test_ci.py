"""Selected release tests must pass; an empty or skipped selection cannot qualify."""

import json
import os
import subprocess
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.ci import capture, summarize, verify_tests


class CaptureTest(unittest.TestCase):
    def test_timeout_returns_empty_metadata(self):
        # Given a metadata command that exceeds its time limit.
        command = ['docker', 'compose', 'images']
        with patch('tools.ci.ci.subprocess.run',
                   side_effect=subprocess.TimeoutExpired(command, 10)) as run:
            # When metadata capture times out, then it returns no metadata.
            self.assertEqual(capture(command), '')
        run.assert_called_once_with(command, capture_output=True, text=True, timeout=10)


class SelectedTestsTest(unittest.TestCase):
    def test_skip_requires_an_explicit_allowance_for_this_suite(self):
        # Given a required pass followed by the ordinary integration opt-out.
        events = [dict(Action='pass', Test='Required'), dict(Action='skip', Test='TestKafkaQuickStart')]
        log = '\n'.join(json.dumps(event) for event in events)
        # When verifying each context, then only the named integration contexts
        # permit that skip; a separate quick-start run still needs its own pass.
        for suite in ('integration', 'kafka'):
            verify_tests(log, ['Required'], suite=suite)
            with self.assertRaisesRegex(ValueError, 'did not pass: TestKafkaQuickStart'):
                verify_tests(log, ['Required', 'TestKafkaQuickStart'], suite=suite)
        for suite in (None, 'smoke', 'ci-integration', 'quickstart', 'replicated-helpers', 'unknown'):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, 'Unexpected skip events: TestKafkaQuickStart'):
                verify_tests(log, ['Required'], suite=suite)

    def test_unexpected_named_and_package_skips_fail_even_with_all_required_passes(self):
        # Given every required pass plus an optional, package-only or unnamed skip.
        skips = [dict(Action='skip', Test='Unexpected'),
                 dict(Action='skip', Package='github.com/pagrus7/kaferry/src/tests/compatibility'),
                 dict(Action='skip')]
        for skipped in skips:
            with self.subTest(skipped=skipped):
                log = '\n'.join(json.dumps(event) for event in [dict(Action='pass', Test='Required'), skipped])
                # When checked in an allowed integration context or a context
                # without allowances, then unrelated skips always reject it.
                for suite in ('kafka', 'smoke'):
                    with self.assertRaisesRegex(ValueError, 'Unexpected skip events'):
                        verify_tests(log, ['Required'], suite=suite)
                with self.assertRaisesRegex(ValueError, 'Unexpected skip events'):
                    verify_tests(json.dumps(skipped), [])

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
                            max_latency_seconds=0.123 if complete else None, unsettled=0,
                            p95_latency_seconds=0.01 if complete else None,
                            p99_latency_seconds=0.02 if complete else None,
                            p999_latency_seconds=0.03 if complete else None),
                        integrity=dict(algorithm='sha256-canonical-records-v1', submitted_records=12,
                            received_records=12, pending_records=0, submitted_sha256='a'*64,
                            received_sha256='a'*64, matched=True if complete else None)))
                measurement['endurance'] = dict(profile='application-24x32-v1', target_records_per_second=200,
                    target_records=12400, submitted_at_publish_end=12399, acknowledged_at_publish_end=12398,
                    acknowledged_after_grace=12400 if complete else 11000, min_acknowledged_window_ratio=.999,
                    load_validated=complete, max_scheduling_debt_records=400)
                (directory / 'soak.metrics.json').write_text(json.dumps(measurement))

                # When GitHub's summary is rendered from the retained metrics.
                with redirect_stdout(StringIO()), patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': ''}):
                    summarize(directory)
                summary = (directory / 'summary.md').read_text()

                # Then counts, unresolved evidence and the two lag definitions stay visible.
                self.assertIn('Published (acked)', summary)
                self.assertIn('### Application endurance load', summary)
                self.assertIn(f'| application-24x32-v1 | 200 | 12400 | 12399 / 12398 | {12400 if complete else 11000} | 0.999 | {complete} | 400 |', summary)
                self.assertIn('Destination lag sampled max / final (records)', summary)
                self.assertIn('Consumer lag sampled max / final (offsets)', summary)
                self.assertIn(f'| 12 | 12 | 13 | 12 | {0 if complete else "unresolved"} | 1 |', summary)
                self.assertIn('| 11 / 0 | 5 / 0 |', summary)
                self.assertIn('Observed peak backlog (records)', summary)
                self.assertIn('Maximum delivery time (ms)', summary)
                self.assertIn('p99.9 (ms)', summary)
                self.assertIn('| soak | 0 | 13 | 12 | 10.000 | 20.000 | 30.000 | 123.000 | 0 |' if complete else
                              '| soak | 0 | 13 | 0 | unavailable | unavailable | unavailable | unavailable | 0 |', summary)
                self.assertIn(f'| soak | {True if complete else "unresolved"} | 12 / 12 | 0 | `{"a"*64}` | `{"a"*64}` |', summary)

                # Older artifacts retain their maximum while new measurements remain unavailable.
                for field in ('p95_latency_seconds', 'p99_latency_seconds', 'p999_latency_seconds'):
                    del measurement['delivery']['observed'][field]
                del measurement['delivery']['integrity']
                (directory / 'soak.metrics.json').write_text(json.dumps(measurement))
                with redirect_stdout(StringIO()), patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': ''}):
                    summarize(directory)
                older_summary = (directory / 'summary.md').read_text()
                self.assertIn('| unavailable | unavailable | unavailable |', older_summary)
                self.assertNotIn('### Key, payload and header integrity', older_summary)
