"""Run script tests with retained, individual JSON results."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest


class JSONResult(unittest.TextTestResult):
    def __init__(self, *args, events):
        super().__init__(*args)
        self.events = events
        self.failed_subtests = set()

    def event(self, action, test):
        self.events.write(json.dumps(dict(Time=datetime.now(timezone.utc).isoformat(),
                                         Action=action, Test=test.id())) + '\n')
        self.events.flush()

    def startTest(self, test):
        super().startTest(test)
        self.event('run', test)

    def addSuccess(self, test):
        super().addSuccess(test)
        self.event('pass', test)

    def addFailure(self, test, err):
        self.failure(super().addFailure, test, err)

    def addError(self, test, err):
        self.failure(super().addError, test, err)

    def failure(self, callback, test, err):
        callback(test, err)
        self.failed_subtests.discard(test)
        self.event('fail', test)

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.event('skip', test)

    def addExpectedFailure(self, test, err):
        super().addExpectedFailure(test, err)
        self.event('skip', test)

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.event('fail', test)

    def addSubTest(self, test, subtest, err):
        super().addSubTest(test, subtest, err)
        self.event('fail' if err else 'pass', subtest)
        if err:
            self.failed_subtests.add(test)

    def stopTest(self, test):
        if test in self.failed_subtests:
            self.event('fail', test)
            self.failed_subtests.remove(test)
        super().stopTest(test)


def run(suite, output, stream=None):
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', encoding='utf-8') as events:
        runner = unittest.TextTestRunner(stream=stream,
            resultclass=lambda *args: JSONResult(*args, events=events))
        return runner.run(suite)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('build-results/python-tests.log'))
    args = parser.parse_args(argv)
    suite = unittest.defaultTestLoader.discover('tools', pattern='test_*.py', top_level_dir='.')
    return 0 if run(suite, args.output).wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
