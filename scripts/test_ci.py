"""Selected release tests must pass; an empty or skipped selection cannot qualify."""

import json
import unittest

from ci import verify_tests


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
