"""Fast checks must execute the tagged helper selection and reject missing passes."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from tools.ci.suites import EXPECTED


ROOT = Path(__file__).resolve().parents[2]
HELPERS = EXPECTED['replicated-helpers']


class ReplicatedHelpersTest(unittest.TestCase):
    def test_shared_fast_target_requires_exact_completed_helper_passes(self):
        # Given a Go command that exits zero with actual, absent or wrong events.
        self.assertTrue(HELPERS)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            fake = directory/'go'
            arguments = directory/'arguments.json'
            fake.write_text(f'''#!{sys.executable}
import json
import os
import sys
from pathlib import Path
entry = json.dumps(dict(args=sys.argv[1:], cgo=os.environ.get("CGO_ENABLED")))
with Path(os.environ["HELPER_ARGUMENTS"]).open("a") as log:
    log.write(entry+"\\n")
for event in json.loads(os.environ["HELPER_EVENTS"]):
    print(json.dumps(event))
''')
            fake.chmod(0o700)
            passes = [dict(Action='pass', Test=name) for name in HELPERS]
            cases = [(passes, None)] + [
                ([dict(Action='pass', Test=name) for name in emitted], 'Required tests did not pass')
                for emitted in ((), HELPERS[3:], HELPERS[4:], HELPERS[5:], HELPERS[:-1], ('TestDifferent',))]
            cases += [(passes[:-1] + [dict(Action=action, Test=HELPERS[-1])], 'Required tests did not pass')
                      for action in ('run', 'skip', 'fail')]
            cases.append((passes + [dict(Action='skip', Test='Unexpected')], 'Unexpected skip events'))
            for events, error in cases:
                with self.subTest(events=events):
                    arguments.unlink(missing_ok=True)
                    env = dict(os.environ, KAFERRY_REPORT_DIR=str(directory/'results'),
                               HELPER_ARGUMENTS=str(arguments), HELPER_EVENTS=json.dumps(events))
                    result = subprocess.run(['make', '--no-print-directory', 'replicated-helpers',
                                             'GO='+str(fake), 'PYTHON='+sys.executable],
                                            cwd=ROOT, env=env, text=True, capture_output=True, timeout=60)
                    # When the shared target verifies JSON, then exit zero alone
                    # cannot qualify an empty, partial or mismatched selection.
                    if error is None:
                        self.assertEqual(0, result.returncode, result.stdout+result.stderr)
                    else:
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn(error, result.stderr)
                    commands = [json.loads(line) for line in arguments.read_text().splitlines()]
                    self.assertEqual(2 if error is None else 1, len(commands))
                    captured = commands[0]
                    self.assertEqual('1', captured['cgo'])
                    args = captured['args']
                    for flag in ('-race', '-count=1', '-json', '-tags=integration,fault,load,harness'):
                        self.assertIn(flag, args)
                    self.assertEqual(['./src/tests/system', './src/tests/system/faults'], args[-2:])
                    pattern = args[args.index('-run')+1]
                    self.assertTrue(all(re.fullmatch(pattern, name) for name in HELPERS))
                    for excluded in ('TestReplicatedZombieWorker', 'TestReplicatedProxyRoutes',
                                     'TestLoadBoundedKafkaCopy', 'TestReplicatedBrokerFaults', HELPERS[0]+'Extra'):
                        self.assertIsNone(re.fullmatch(pattern, excluded))
                    if error is None:
                        coverage = commands[1]
                        self.assertEqual('1', coverage['cgo'])
                        for flag in ('-race', '-count=1', '-json', '-covermode=atomic', '-coverpkg=./...',
                                     '-tags=integration,harness',
                                     '-coverprofile='+str(directory/'results/replicated-helpers.coverage.out')):
                            self.assertIn(flag, coverage['args'])
                        self.assertEqual(args[-2:], coverage['args'][-2:])
                        self.assertEqual(pattern, coverage['args'][coverage['args'].index('-run')+1])


if __name__ == '__main__':
    unittest.main()
