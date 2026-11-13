"""Individual evidence must preserve unittest failures, skips and subtests."""

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools.ci.python_tests import main, run


class PythonTestsTest(unittest.TestCase):
    def results(self, fixture):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)/'nested/results.log'
            result = run(unittest.defaultTestLoader.loadTestsFromTestCase(fixture), output, io.StringIO())
            return result, [json.loads(line) for line in output.read_text().splitlines()]

    def test_outcomes_keep_failure_error_skip_and_expected_failure_distinct(self):
        # Given each unittest terminal outcome, including an unexpected success.
        class Fixture(unittest.TestCase):
            def test_success(self):
                pass

            def test_failure(self):
                self.fail('failure')

            def test_error(self):
                raise RuntimeError('error')

            @unittest.skip('unavailable')
            def test_skip(self):
                pass

            @unittest.expectedFailure
            def test_expected_failure(self):
                self.fail('expected')

            @unittest.expectedFailure
            def test_unexpected_success(self):
                pass

        result, events = self.results(Fixture)
        # When results are retained, then only a real success is a pass.
        self.assertFalse(result.wasSuccessful())
        terminal = {e['Test'].rsplit('.', 1)[-1]: e['Action'] for e in events if e['Action'] != 'run'}
        self.assertEqual(dict(test_success='pass', test_failure='fail', test_error='fail',
                              test_skip='skip', test_expected_failure='skip', test_unexpected_success='fail'), terminal)
        self.assertEqual(6, sum(e['Action'] == 'run' for e in events))

    def test_failed_subtest_never_leaves_a_parent_pass(self):
        # Given successful and failed subtests in the same parent test.
        class Fixture(unittest.TestCase):
            def test_subtests(self):
                for value in (1, 2):
                    with self.subTest(value=value):
                        self.assertEqual(1, value)

        result, events = self.results(Fixture)
        # When reporting, then keep exact subtest IDs and the failed parent.
        self.assertFalse(result.wasSuccessful())
        terminal = [(e['Action'], e['Test']) for e in events if e['Action'] != 'run']
        parent = Fixture('test_subtests').id()
        self.assertEqual([('pass', parent+' (value=1)'), ('fail', parent+' (value=2)'), ('fail', parent)], terminal)

    def test_main_returns_the_discovered_suite_result(self):
        # Given a discovered suite, when it fails, then the command exits nonzero.
        with tempfile.TemporaryDirectory() as temporary:
            for valid in (False, True):
                fixture = unittest.FunctionTestCase(lambda: None)
                if not valid:
                    fixture = unittest.FunctionTestCase(lambda: self.fail('fixture failure'))
                with patch('unittest.defaultTestLoader.discover', return_value=unittest.TestSuite([fixture])), \
                     patch('sys.stderr', new_callable=io.StringIO):
                    self.assertEqual(0 if valid else 1, main(['--output', temporary+'/results.log']))


if __name__ == '__main__':
    unittest.main()
