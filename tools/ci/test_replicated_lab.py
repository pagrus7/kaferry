"""Weekly lab selection must execute exactly the three tracked regressions."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


class ReplicatedLabTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[2]
        self.workflow = (self.root/'.github/workflows/replicated-lab.yml').read_text()

    def test_actual_workflow_selector_preserves_full_and_weekly_matrices(self):
        # Given the actual selector command run by the workflow's producer job.
        command = re.search(r'^        run: (python3 -m tools.ci.replicated_suites .*GITHUB_OUTPUT.*)$',
                            self.workflow, re.M).group(1)
        full = {'ownership/worker-kill-rebalance', 'ownership/worker-scale', 'broker/source-leader',
                'broker/destination-leader', 'broker/rolling-source', 'broker/rolling-destination',
                'broker/destination-isr-loss', 'network/destination-outage', 'network/destination-leaders-outage'}
        weekly = {'ownership/worker-scale', 'network/destination-leaders-outage'}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)/'output'
            for selected, expected in (('false', full), ('true', weekly)):
                with self.subTest(weekly=selected):
                    output.write_text('')
                    env = dict(os.environ, WEEKLY=selected, GITHUB_OUTPUT=str(output))
                    # When the checked-in shell command runs, then neither missing
                    # cases nor extra cases can be mistaken for the requested matrix.
                    subprocess.run(['bash', '-ec', command], cwd=self.root, env=env, check=True,
                                   capture_output=True, text=True)
                    name, value = output.read_text().strip().split('=', 1)
                    self.assertEqual('fault_matrix', name)
                    rows = json.loads(value)['include']
                    self.assertEqual(expected, {row['case'] for row in rows})
                    self.assertEqual(len(expected), len(rows))
                    self.assertEqual(len(expected), len({row['name'] for row in rows}))
                    for row in rows:
                        expected_name = ('network-' if row['case'].startswith('network/') else '')+row['case'].split('/')[1]
                        self.assertEqual(expected_name, row['name'])

    def test_invalid_selection_cannot_emit_a_successful_empty_matrix(self):
        # Given an invalid selection, when invoked, then fail without matrix output.
        for invalid in ('', 'TRUE', 'weekly'):
            result = subprocess.run(['python3', '-m', 'tools.ci.replicated_suites'], cwd=self.root,
                                    env=dict(os.environ, WEEKLY=invalid), capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode, result.stdout)
            self.assertEqual('', result.stdout)

    def test_weekly_event_wiring_cannot_run_unselected_suites(self):
        # Given the weekly schedule and its explicit post-merge dispatch equivalent.
        self.assertIn("  schedule:\n    - cron: '23 7 * * 1'", self.workflow)
        self.assertIn("WEEKLY: ${{ github.event_name == 'schedule' || inputs.weekly == true }}", self.workflow)
        self.assertIn('fault_matrix: ${{ steps.cases.outputs.fault_matrix }}', self.workflow)
        self.assertIn('matrix: ${{ fromJSON(needs.smoke.outputs.fault_matrix) }}', self.workflow)
        # When a weekly run starts, then smoke only prepares the binary, zombie
        # runs separately, and full fixture/endurance execution remains disabled.
        guard = "if: ${{ github.event_name != 'schedule' && inputs.weekly != true }}"
        for name in ('Check endurance gates without brokers', 'Copy and independently reconcile the replicated fixture',
                     'Verify fixture teardown'):
            self.assertIn(f'- name: {name}\n        {guard}', self.workflow)
        self.assertIn(f'  endurance:\n    {guard}', self.workflow)
        zombie = self.workflow.split('  zombie:\n', 1)[1].split('  faults:\n', 1)[0]
        self.assertIn('--case ownership/zombie-worker', zombie)
        self.assertNotIn('\n    if:', zombie)
        self.assertIn('ref: ${{ github.sha }}', zombie)
        self.assertEqual({'14'}, set(re.findall(r'retention-days: (\d+)', self.workflow)))
