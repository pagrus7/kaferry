"""Usage totals survive refreshes, attribution changes and incomplete sources."""

import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pr_usage as usage


RATES = {'as_of': '2026-10-31', 'basis': 'Test reference rates', 'source': 'fixture',
         'models': {'model-a': {'input': 10, 'cached_input': 1, 'output': 50}}}


def event(kind, payload, second=0):
    return {'timestamp': f'2026-10-31T00:00:{second:02d}Z', 'type': kind, 'payload': payload}


def counter(input_tokens, cached, output, reasoning=0, second=1, **extra):
    counts = dict(zip(usage.FIELDS, (input_tokens, cached, 0, output, reasoning, input_tokens + output)))
    return event('event_msg', {'type': 'token_count',
                             'info': {'total_token_usage': counts, **extra}}, second)


class UsageTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(patch.stopall)
        patch.object(usage, 'ROOT', self.root).start()
        patch.object(usage, 'now', return_value='2026-10-31T00:00:02+00:00').start()
        self.source = self.root / 'root.jsonl'
        self.write(self.source, [event('session_meta', {'id': 'root', 'session_id': 'root'}),
                                 event('turn_context', {'model': 'model-a'}), counter(100, 40, 10, 3)])
        self.rates = self.root / 'rates.json'
        self.rates.write_text(json.dumps(RATES))

    def write(self, path, events, mode='w'):
        with path.open(mode) as stream:
            for item in events:
                stream.write(json.dumps(item) + '\n')

    def run_cli(self, *args):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            usage.main(list(args))
        return out.getvalue()

    def report(self, work='feature'):
        return json.loads(self.run_cli('report', work, '--rates', str(self.rates), '--json'))

    def mark(self, *args, work='feature', source=None):
        return self.run_cli('mark', work, '--session', str(source or self.source), *args)

    def test_phase_boundaries_repeated_refresh_and_reasoning_are_not_double_counted(self):
        # Given historical usage registered without invented attribution.
        self.mark('--from-start')
        self.mark('--phase', 'implementation', '--purpose', 'code')
        self.write(self.source, [counter(200, 80, 30, 8, second=3),
                                 counter(200, 80, 30, 8, second=4)], 'a')
        # When refreshed twice, then cumulative snapshots and reasoning count once.
        first, second = self.report(), self.report()
        self.assertEqual(first['totals'], second['totals'])
        self.assertEqual(second['totals']['total_tokens'], 230)
        self.assertEqual(second['totals']['output_tokens'], 30)
        self.assertEqual(second['unattributed_tokens'], 110)
        self.assertAlmostEqual(second['totals']['estimated_usd'], .00278)
        self.assertEqual([(r['phase'], r['total_tokens']) for r in second['rows']],
                         [('implementation', 120), ('unattributed', 110)])
        self.assertIsNone(second['actual_billed_usd'])

    def test_registration_stop_and_resume_exclude_other_work(self):
        # Given a session registered after an existing request, then stopped.
        self.mark('--phase', 'implementation', '--purpose', 'tests')
        self.write(self.source, [counter(200, 80, 30, 3, second=3)], 'a')
        with patch.object(usage, 'now', return_value='2026-10-31T00:00:04+00:00'):
            self.run_cli('stop', 'feature', '--session', str(self.source))
        self.write(self.source, [counter(300, 90, 40, 3, second=5)], 'a')
        # When resumed for rework, then only the two active intervals contribute.
        with patch.object(usage, 'now', return_value='2026-10-31T00:00:06+00:00'):
            self.mark('--phase', 'rework', '--purpose', 'fix-review', '--round', '2')
        self.write(self.source, [counter(400, 100, 50, 3, second=7)], 'a')
        report = self.report()
        self.assertEqual(report['totals']['total_tokens'], 230)
        self.assertEqual(report['outside_work_tokens'], 220)
        self.assertEqual([r['round'] for r in report['rows']], [1, 2])

    def test_child_identity_models_and_cross_work_registration(self):
        # Given a child with the parent's session_id but its own thread id.
        self.mark('--from-start')
        child = self.root / 'child.jsonl'
        self.write(child, [event('session_meta', {'id': 'child', 'session_id': 'root',
                                                'source': {'subagent': {'thread_spawn': {'parent_thread_id': 'root'}}}}),
                           event('turn_context', {'model': 'model-a', 'service_tier': 'priority'}),
                           counter(50, 10, 5), event('turn_context', {'model': 'unknown-model'}),
                           counter(100, 20, 10, second=3)])
        # When registered as a reviewer, then it is distinct and unknown rates stay visible.
        self.mark('--from-start', '--phase', 'review', '--purpose', 'docs', source=child)
        self.run_cli('attach', 'feature', '--pr', '123')
        result = self.report()
        self.assertEqual((result['registered_sessions'], result['pr']), (2, 123))
        self.assertEqual(result['totals']['total_tokens'], 220)
        self.assertEqual(result['totals']['unpriced_tokens'], 55)
        self.assertEqual({r['session'] for r in result['rows']}, {'child', 'root'})
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.mark('--from-start', source=child, work='different')
        self.assertFalse((self.root / '.cache/usage/different/ledger.json').exists())

    def test_partial_missing_and_changed_sources_preserve_import_and_report_limits(self):
        # Given imported counters followed by a partially written request.
        self.mark('--from-start')
        with self.source.open('a') as stream:
            stream.write('{"type":')
        # When refreshed, then the last complete data is kept with a visible warning.
        report = self.report()
        self.assertEqual(report['totals']['total_tokens'], 110)
        self.assertTrue(any('unfinished' in w for w in report['warnings']))
        self.source.unlink()
        report = self.report()
        self.assertEqual(report['totals']['total_tokens'], 110)
        self.assertTrue(any('retained prior' in w for w in report['warnings']))
        self.write(self.source, [event('session_meta', {'id': 'root'})])
        self.assertEqual(self.report()['totals']['total_tokens'], 110)
        self.assertTrue(any('retained prior' in w for w in self.report()['warnings']))

    def test_counter_reset_missing_usage_and_inherited_first_counter_are_explicit(self):
        # Given inherited counters, a reset and a request with unavailable accounting.
        inherited = counter(1000, 400, 100, last_token_usage=counter(100, 40, 10)['payload']['info']['total_token_usage'])
        self.write(self.source, [event('session_meta', {'id': 'root'}), inherited,
                                 counter(50, 20, 5, second=2), counter(100, 40, 10, second=3),
                                 event('event_msg', {'type': 'token_count', 'info': None}, second=4)])
        # When imported, then only measurable increments are counted and gaps are disclosed.
        _, rows, warnings = usage.read_rollout(self.source)
        self.assertEqual(sum(r['total_tokens'] for r in rows), 165)
        self.assertTrue(any('initial counter' in w for w in warnings))
        self.assertTrue(any('counter reset' in w for w in warnings))
        self.assertTrue(any('usage unavailable' in w for w in warnings))

    def test_cache_write_and_unknown_model_costs_are_not_zero_price_claims(self):
        # Given a request with cache-write usage whose retention price is unknown.
        item = counter(100, 40, 10)
        item['payload']['info']['total_token_usage']['cache_write_input_tokens'] = 20
        self.write(self.source, [event('session_meta', {'id': 'root'}),
                                 event('turn_context', {'model': 'model-a'}), item])
        self.mark('--from-start')
        # When priced, then all affected tokens are explicitly unpriced.
        result = self.report()
        self.assertEqual(result['totals']['unpriced_tokens'], 110)
        self.assertIn('unpriced tokens: 110', usage.render(result))

    def test_saved_report_contains_no_transcript_and_default_rate_snapshot_is_valid(self):
        # Given secret-like text in prompt and tool events.
        self.write(self.source, [event('response_item', {'text': 'SECRET-PROMPT'}),
                                 event('event_msg', {'type': 'tool', 'arguments': 'SECRET-ARGUMENT'})], 'a')
        self.mark('--from-start')
        # When reported, then only counters and attribution are retained locally.
        self.report()
        for path in (self.root / '.cache/usage/feature').iterdir():
            self.assertNotIn('SECRET-', path.read_text())
        default = usage.load_rates(Path(usage.__file__).with_name('pr_usage_rates.json'))
        self.assertIn('gpt-6-astra', default['models'])

    def test_default_thread_lookup_and_separate_reasoning_counters(self):
        # Given the CLI's thread environment and an output counter excluding reasoning.
        sessions = self.root / 'codex/sessions/2026/10/31'
        sessions.mkdir(parents=True)
        source = sessions / 'rollout-date-root.jsonl'
        item = counter(100, 40, 10, 3)
        item['payload']['info']['total_token_usage']['total_tokens'] = 113
        self.write(source, [event('session_meta', {'id': 'root'}),
                            event('turn_context', {'model': 'model-a'}), item])
        # When registered without --session, then lookup and output normalization work.
        with patch.dict(os.environ, {'CODEX_HOME': str(self.root / 'codex'), 'CODEX_THREAD_ID': 'root'}):
            self.run_cli('mark', 'feature', '--from-start')
        result = self.report()
        self.assertEqual(result['totals']['output_tokens'], 13)
        self.assertEqual(result['totals']['reasoning_output_tokens'], 3)
        self.assertEqual(result['totals']['total_tokens'], 113)
        self.assertAlmostEqual(result['totals']['estimated_usd'], .00129)
        # Given an ambiguous lookup, then the caller must identify the source path.
        self.write(sessions / 'rollout-other-root.jsonl', [])
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            usage.resolve_session('root', sessions)

    def test_invalid_input_does_not_silently_become_zero_usage(self):
        # Given malformed complete JSON and invalid rate values.
        with self.source.open('a') as stream:
            stream.write('not json\n')
        # When parsed, then previous counters survive and the unread suffix is disclosed.
        _, rows, warnings = usage.read_rollout(self.source)
        self.assertEqual(len(rows), 1)
        self.assertTrue(any('invalid record' in w for w in warnings))
        for invalid in (-1, float('nan'), float('inf'), '10'):
            with self.subTest(value=invalid):
                rates = json.loads(json.dumps(RATES))
                rates['models']['model-a']['input'] = invalid
                self.rates.write_text(json.dumps(rates))
                with self.assertRaises(ValueError):
                    usage.load_rates(self.rates)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.mark(work='../escape')
        self.assertFalse((self.root / '.cache/escape.json').exists())


if __name__ == '__main__':
    unittest.main()
