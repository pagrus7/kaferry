"""Suite consolidation must preserve selection, deadlines and execution gates."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import tools.ci.suites as suites
from tools.qualification.qualify import suite_command
from tools.qualification.fault_results import FAULT_CASES


class SuitesTest(unittest.TestCase):
    def test_cli_catalog_and_verification_keep_quickstart_contexts_distinct(self):
        # Given the Kafka job's full required evidence and its intentional opt-out.
        events = [dict(Action='pass', Test=name) for name in suites.EXPECTED['kafka']]
        events.append(dict(Action='skip', Test='TestKafkaQuickStart'))
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary)/'integration.log'
            log.write_text('\n'.join(json.dumps(event) for event in events))
            # When the CLI selects the evidence contract, then the Kafka context
            # permits that opt-out and enabled quickstart still requires a pass.
            for suite, valid in [('kafka', True), ('ci-integration', False)]:
                result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'verify', suite, str(log)],
                                        text=True, capture_output=True)
                self.assertEqual(valid, result.returncode == 0, result.stdout+result.stderr)
                if not valid:
                    self.assertIn('Required tests did not pass: TestKafkaQuickStart', result.stderr)
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'names', 'kafka'],
                                    text=True, capture_output=True, check=True)
            self.assertEqual(suites.EXPECTED['kafka'], result.stdout.splitlines())
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'pattern', 'smoke'],
                                    text=True, capture_output=True, check=True)
            self.assertEqual(suites.PATTERNS['smoke'], result.stdout.strip())

    def test_smoke_make_target_preserves_pattern_and_requires_passes(self):
        # Given a fake Go process that records Make's actual selector and emits
        # complete, incomplete or unexpectedly skipped test results.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            executable = directory/'go'
            arguments = directory/'args.json'
            executable.write_text('#!'+sys.executable+'\nimport json, os, sys\n'
                                  'from pathlib import Path\n'
                                  'Path(os.environ["SMOKE_ARGUMENTS"]).write_text(json.dumps(sys.argv[1:]))\n'
                                  'for event in json.loads(os.environ["SMOKE_EVENTS"]):\n'
                                  '    print(json.dumps(event))\n')
            executable.chmod(0o700)
            passes = [dict(Action='pass', Test=name) for name in suites.EXPECTED['smoke']]
            for events, error in [(passes, None), (passes[:-1], 'Required tests did not pass'),
                                  (passes + [dict(Action='skip', Test='Unexpected')], 'Unexpected skip events')]:
                env = dict(os.environ, KAFERRY_REPORT_DIR=str(directory/'results'),
                           SMOKE_ARGUMENTS=str(arguments), SMOKE_EVENTS=json.dumps(events))
                result = subprocess.run(['make', '--no-print-directory', 'smoke', 'GO='+str(executable),
                                         'PYTHON='+sys.executable, 'KAFERRY_TEST_BINARY=/unused'],
                                        env=env, text=True, capture_output=True, timeout=60)
                # When the actual target runs, then shell expansion preserves
                # the shared selector and its evidence gate rejects non-passes.
                self.assertEqual(error is None, result.returncode == 0, result.stdout+result.stderr)
                if error:
                    self.assertIn(error, result.stderr)
                args = json.loads(arguments.read_text())
                self.assertEqual(suites.PATTERNS['smoke'], args[args.index('-run')+1])
                self.assertIn('-json', args)

    def test_replicated_broker_budgets_are_registered_and_selection_scoped(self):
        # Given five broker recipes and the unchanged ownership recipe, when
        # selecting deadlines, then each uses only its actual registered bounds.
        catalog = suites.fault_timeout_catalog()
        for id in suites.REPLICATED_CASES:
            if id.startswith('broker/'):
                self.assertEqual(119000, sum(catalog[id].values()))
                self.assertEqual(359, suites.replicated_execution_seconds([id]))
        self.assertEqual(240, suites.replicated_execution_seconds(['ownership/zombie-worker']))
        self.assertEqual(4907, suites.replicated_execution_seconds(list(suites.REPLICATED_CASES)))
        for selected in ([], ['broker/not-implemented']):
            with self.assertRaises(ValueError):
                suites.replicated_execution_seconds(selected)

    def test_network_case_budgets_are_exact_and_do_not_extend_legacy_cases(self):
        # Given the five implemented worker network exposures, when selecting
        # cases, then only the healthy reference adds24s; recovery stays89s.
        catalog = suites.fault_timeout_catalog()
        ids = {'network/'+name for name in ('destination-outage', 'destination-leaders-outage', 'latency', 'bandwidth', 'slow-close')}
        self.assertEqual(ids, {id for id in suites.REPLICATED_CASES if id.startswith('network/')})
        for id in ids:
            self.assertEqual(89000, sum(catalog[id].values()))
            self.assertEqual(24, suites.replicated_execution_seconds([id])-(240+sum(catalog[id].values())//1000))
            self.assertEqual(353, suites.replicated_execution_seconds([id]))
        self.assertEqual(706, suites.replicated_execution_seconds(['network/latency','network/bandwidth']))
        self.assertEqual(1073, suites.fault_execution_seconds())
        self.assertEqual(240, suites.replicated_execution_seconds(['ownership/zombie-worker']))
        for id, keys in [('broker/source-leader', ('socket.timeout.ms', 'replica.lag.time.max.ms')),
                         ('network/latency', ('socket.timeout.ms', 'progress_save_timeout'))]:
            for key in keys:
                changed = json.loads(json.dumps(catalog))
                changed[id].pop(key)
                with patch('tools.ci.suites.json.loads', return_value=changed), self.assertRaises(ValueError):
                    suites.fault_timeout_catalog()
        with self.assertRaises(ValueError):
            suites.replicated_execution_seconds(['network/latency']*2)

    def test_ownership_cases_have_exact_budgets_selectors_and_hosted_scope(self):
        # Given all fourteen implemented deterministic cases, when selecting a
        # case or the default suite, then retain every old case and its budget.
        catalog = suites.fault_timeout_catalog()
        expected = {'ownership/'+name:seconds for name,seconds in
                    (('worker-kill-rebalance',359),('coordinator-kill-rebalance',389),('worker-scale',359))}
        self.assertEqual(14,len(suites.REPLICATED_CASES))
        self.assertEqual(4907,suites.replicated_execution_seconds(list(suites.REPLICATED_CASES)))
        for id,seconds in expected.items():
            self.assertEqual('TestReplicatedOwnershipCases/'+id.split('/')[1],suites.REPLICATED_CASES[id])
            self.assertEqual((seconds-240)*1000,sum(catalog[id].values()))
            self.assertEqual(seconds,suites.replicated_execution_seconds([id]))
            self.assertLess(seconds+60,15*60)
            changed=json.loads(json.dumps(catalog))
            changed[id].pop('max.poll.interval.ms')
            with patch('tools.ci.suites.json.loads',return_value=changed),self.assertRaises(ValueError):
                suites.fault_timeout_catalog()
        self.assertIn('^TestReplicatedOwnershipCases$',suites.GO_SUITES['fault-replicated'][1])
        workflow=(Path(__file__).resolve().parents[2]/'.github/workflows/replicated-lab.yml').read_text()
        import re
        hosted = set(suites.REPLICATED_CASES)-{'ownership/zombie-worker', 'ownership/coordinator-kill-rebalance',
                                             'network/latency', 'network/bandwidth', 'network/slow-close'}
        self.assertEqual(hosted,set(re.findall(r'case: ([a-z/-]+)',workflow)))
        self.assertIn('--case ownership/zombie-worker ',workflow)
        self.assertEqual(1073,suites.fault_execution_seconds())
        self.assertEqual(240,suites.replicated_execution_seconds(['ownership/zombie-worker']))

    def test_registered_ownership_bounds_extend_only_the_catalog_key_allowlist(self):
        # Given the new exposure's actual bounds, when loading the catalog,
        # then its31s horizon is exact and legacy eight-case budgets stay473s.
        catalog = suites.fault_timeout_catalog()
        self.assertEqual(dict(zip(('session.timeout.ms', 'socket.timeout.ms', 'progress_save_timeout',
                                  'delivery_timeout', 'shutdown_timeout'), (6000, 8000, 10000, 2000, 5000))),
                         catalog['ownership/zombie-worker'])
        self.assertEqual(31000, sum(catalog['ownership/zombie-worker'].values()))
        self.assertEqual(473000, sum(sum(catalog[id].values()) for id in suites.FAULT_CASES))
        self.assertEqual(1073, suites.fault_execution_seconds())
        # Unknown or invalid fields must still fail closed before any suite.
        for key, value in (('arbitrary_timeout', 1), ('socket.timeout.ms', 0), ('progress_save_timeout', True)):
            with self.subTest(key=key, value=value):
                changed = json.loads(json.dumps(catalog))
                changed['ownership/zombie-worker'][key] = value
                with patch('tools.ci.suites.json.loads', return_value=changed), \
                     self.assertRaisesRegex(ValueError, 'positive milliseconds'):
                    suites.fault_timeout_catalog()
        for key in ('socket.timeout.ms', 'progress_save_timeout'):
            changed = json.loads(json.dumps(catalog))
            changed['fault/churn'][key] = 1000
            with patch('tools.ci.suites.json.loads', return_value=changed), self.assertRaises(ValueError):
                suites.fault_timeout_catalog()

    def test_fault_outer_budget_accounts_for_the_same_catalog_as_windows(self):
        # Given the fixture's actual per-case timeout catalog, with churn's
        # overlapping windows sharing one maximum extension of its case.
        catalog = json.loads((Path(__file__).resolve().parents[2]/'src/tests/qualification/fault-timeouts-ms.json').read_text())
        self.assertLessEqual(FAULT_CASES, set(catalog))
        self.assertEqual(473000, sum(sum(catalog[id].values()) for id in FAULT_CASES))
        # When selecting the suite, then preserve the existing 600s execution
        # allowance plus the mandatory 473s observation work, without changing
        # individual configured timeouts or overriding an explicit caller limit.
        self.assertEqual(1073, suites.fault_execution_seconds())
        self.assertIn('-timeout=1073s', suites.command('fault'))
        self.assertIn('-timeout=12m', suites.command('fault', timeout='12m'))
        catalog['fault/churn']['delivery_timeout'] += 1000
        catalog['another-suite/registered-case'] = {'session.timeout.ms': 6000}
        with patch('tools.ci.suites.json.loads', return_value=catalog):
            self.assertEqual(1074, suites.fault_execution_seconds())
        catalog.pop('fault/crash')
        with patch('tools.ci.suites.json.loads', return_value=catalog):
            with self.assertRaisesRegex(ValueError, 'omitted a selected case'):
                suites.fault_execution_seconds()

    def test_failed_fault_run_replaces_prior_successful_receipt(self):
        # Given an older successful receipt in the reusable CI report directory.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory/'fault-results.json').write_text(json.dumps(dict(status='passed')))
            def configure(env):
                env['KAFERRY_ORACLE_RUN'] = 'fresh'
            # When this execution fails, then retain that failure and never leave
            # the prior success available for evidence collection.
            with patch.dict(os.environ, KAFERRY_REPORT_DIR=temporary), \
                 patch.object(sys, 'argv', ['suites', 'run', 'fault']), \
                 patch('tools.qualification.fault_results.configure', side_effect=configure), \
                 patch('tools.ci.suites.subprocess.run', return_value=subprocess.CompletedProcess([], 17)), \
                 patch('tools.qualification.fault_results.collect') as collect:
                self.assertEqual(17, suites.main())
            collect.assert_not_called()
            self.assertEqual(dict(schema=1, status='failed', oracle_run='fresh'),
                             json.loads((directory/'fault-results.json').read_text()))

    def test_qualification_keeps_shared_selection_and_its_deadlines(self):
        # Given local qualification with a non-default Go executable.
        for name, tags, pattern, timeout in [
            ('load', 'integration,load', '^TestLoad', '20m'),
            ('fault', 'integration,fault', '^TestBlockingCommit|^TestRebalanceChurn|^TestCrashResume|^TestRecoveryBlockedOutput', '1073s'),
            ('soak', 'integration,load', '^TestStreamingSoak', '370s'),
        ]:
            with self.subTest(suite=name):
                args = argparse.Namespace(suite=name, duration=60, rate=50)
                # When selecting the suite, then tags, selectors and caller budgets survive.
                command, expected = suite_command(args, {'GO': '/custom/go'})
                self.assertEqual(['/custom/go', 'test', '-count=1', '-json', '-timeout='+timeout,
                                  '-tags='+tags, '-run', pattern,
                                  './src/tests/compatibility' if name == 'soak' else './src/tests/system'], command)
                self.assertTrue(expected)
        command, _ = suite_command(argparse.Namespace(suite='integration'), {})
        self.assertEqual(['go', 'test', '-count=1', '-json', '-race', '-timeout=20m',
                          '-tags=integration', './src/tests/system'], command)

    def test_ci_keeps_extra_execution_gates_and_command_failures(self):
        profiles = [('pass', 'TestLoadBoundedKafkaCopy/' + name) for name in
                    ('near-limit-v1', 'hot-keys-v1', 'headers-v1', 'tombstones-v1',
                     'source-none-v1', 'source-gzip-v1', 'source-snappy-v1', 'source-lz4-v1', 'source-zstd-v1')]
        ordinary = [('pass', 'TestLoadBoundedKafkaCopy/default'), ('pass', 'TestLoadObservability/round-0/disabled')]
        for code, events, valid in [
            (9, [], False),
            (0, [('pass', 'TestLoadBoundedKafkaCopy/default')], False),
            (0, [('pass', 'TestLoadBoundedKafkaCopy/default'), ('skip', 'TestLoadObservability/round-0/disabled')], False),
            (0, ordinary, False),
            (0, ordinary + profiles[:-1] + [('skip', 'TestLoadBoundedKafkaCopy/source-zstd-v1')], False),
            (0, ordinary + profiles, True),
        ]:
            with self.subTest(code=code, events=events), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                (directory/'load.log').write_text('\n'.join(json.dumps({'Action': action, 'Test': test}) for action, test in events))
                # Given command results independent of the suite's expected-test definitions.
                with patch('tools.ci.suites.Path', return_value=directory), patch('tools.ci.suites.run', return_value=code) as run:
                    # When CI checks evidence, then failure or skipped required tests cannot pass.
                    if code or valid:
                        self.assertEqual(code, suites.ci('load'))
                    else:
                        with self.assertRaisesRegex(ValueError, 'Required tests did not pass'):
                            suites.ci('load')
                    run.assert_called_once_with('load', ['make', 'load', 'KAFERRY_TEST_OUTPUT=-json'], directory)

    def test_cli_forwards_make_overrides_and_exit_status(self):
        # Given an executable that records the invocation and exits unsuccessfully.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root/'go'
            output = root/'args.json'
            executable.write_text(f'#!{sys.executable}\nimport json, os, sys\nopen(os.environ["SUITE_ARGS"], "w").write(json.dumps(sys.argv[1:]))\nsys.exit(17)\n')
            executable.chmod(0o700)
            # When invoked as a Makefile caller, then use its executable, timeout and output flags.
            result = subprocess.run([sys.executable, '-m', 'tools.ci.suites', 'run', 'soak',
                                     '--go='+str(executable), '--timeout=40m', '--output=-json'],
                                    env=dict(os.environ, SUITE_ARGS=str(output)), capture_output=True, text=True)
            self.assertEqual(17, result.returncode, result.stderr)
            self.assertEqual(['test', '-count=1', '-json', '-timeout=40m', '-tags=integration,load',
                              '-run', '^TestStreamingSoak', './src/tests/compatibility'], json.loads(output.read_text()))
