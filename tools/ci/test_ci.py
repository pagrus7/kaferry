"""Selected release tests must pass; an empty or skipped selection cannot qualify."""

import json
import unittest

from tools.ci.ci import verify_tests


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
