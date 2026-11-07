"""Published result discovery and rendering preserve evidence and failure status."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from endurance_site import api, build, collect, render_run, validate


def report(status='passed', seconds=86400):
    return dict(schema=1, manifest=dict(suite='endurance', status=status, requested_seconds=seconds,
        drain_seconds=300, started_at='2026-11-06T04:00:00+00:00', binary_sha256='a'*64,
        binary_version='kaferry v0.12.0', workload=dict(rate=200, source_partitions=24,
        destination_partitions=32, routing='KEY', replication_factor=1)),
        measurements=[dict(binary_sha256='a'*64, platform='linux/arm64', passed=status == 'passed',
            endurance=dict(load_validated=True, recoveries=1440), rss_samples=86400,
            sampled_peak_rss_bytes=32*2**20, destination_stalls=1440,
            delivery=dict(submitted=17856000, published=17856000, unique_received=17856000,
                verified=17856000, missing=0, duplicates=0, corrupted=0, unexpected=0,
                reordered=0, publish_errors=0, reconciliation_complete=True,
                integrity=dict(matched=True)))])


class SiteTest(unittest.TestCase):
    def setUp(self):
        self.prefix = 'endurance-linux-arm64-202611060400000000'
        self.name = self.prefix + '-endurance-results.json'
        self.checksum_name = self.prefix + '-endurance.SHA256SUMS'
        self.result = report()
        self.content = json.dumps(self.result).encode()
        self.assets = [dict(id=11, name=self.name), dict(id=12, name=self.checksum_name)]
        self.release = dict(id=1, tag_name='v0.12.0', draft=False, prerelease=False)

    def api(self, path):
        if path.endswith('/releases?per_page=100'):
            return [self.release, dict(self.release, id=2, draft=True),
                    dict(self.release, id=3, prerelease=True)]
        self.assertEqual('repos/owner/project/releases/1/assets?per_page=100', path)
        return self.assets

    def download(self, repository, asset):
        self.assertEqual('owner/project', repository)
        if asset['id'] == 11:
            return self.content
        return f'{hashlib.sha256(self.content).hexdigest()}  {self.name}\n'.encode()

    def test_paginated_github_responses_are_not_truncated_to_the_first_page(self):
        # Given more than one API page, when decoded, then every item is retained.
        with patch('endurance_site.subprocess.check_output', return_value=b'[[{"id":1}],[{"id":2}]]') as request:
            self.assertEqual([{'id': 1}, {'id': 2}], api('repos/owner/project/releases?per_page=100'))
        self.assertIn('--paginate', request.call_args.args[0])

    def test_collects_checked_release_json_without_downloading_evidence_archives(self):
        # Given stable, draft and prerelease entries, when collected, then only stable evidence is used.
        self.assets += [dict(id=13, name=self.prefix+'-endurance-evidence.tar.gz')]
        self.result['manifest']['started_at'] = '2026-11-06T05:00:00+01:00'
        self.content = json.dumps(self.result).encode()
        with patch('endurance_site.api', side_effect=self.api), patch('endurance_site.download', side_effect=self.download) as fetch:
            runs = collect('owner/project')
        self.assertEqual(1, len(runs))
        self.assertEqual(self.content, runs[0]['content'])
        self.assertEqual('v0.12.0', runs[0]['release'])
        self.assertEqual([12, 11], [call.args[1]['id'] for call in fetch.call_args_list])
        self.assertIn('Logs and samples', runs[0]['downloads'])
        self.assertEqual(datetime(2026, 11, 6, 4, tzinfo=timezone.utc), runs[0]['started'])

    def test_bad_checksums_or_incomplete_success_never_produce_a_site(self):
        # Given damaged or invalid passing evidence, when built, then deployment input is not produced.
        for case in ('missing-checksums', 'checksum-mismatch', 'missing-metrics', 'unvalidated-load', 'different-binary'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                self.assets = [dict(id=11, name=self.name), dict(id=12, name=self.checksum_name)]
                self.result = report()
                if case == 'missing-checksums': self.assets.pop()
                if case == 'missing-metrics': self.result['measurements'] = []
                if case == 'unvalidated-load': self.result['measurements'][0]['endurance']['load_validated'] = False
                if case == 'different-binary': self.result['measurements'][0]['binary_sha256'] = 'b'*64
                self.content = json.dumps(self.result).encode()
                def damaged(repository, asset):
                    if case == 'checksum-mismatch' and asset['id'] == 12:
                        return f'{"0"*64}  {self.name}\n'.encode()
                    return self.download(repository, asset)
                output = Path(temporary)/'site'
                with patch('endurance_site.api', side_effect=self.api), patch('endurance_site.download', side_effect=damaged):
                    with self.assertRaises(ValueError): build('owner/project', output)
                self.assertFalse(output.exists())

    def test_failed_rehearsal_retains_unknown_counts_and_escapes_report_text(self):
        # Given an incomplete failed rehearsal, when displayed, then it cannot appear as a passing long run.
        result = report('failed', 60)
        m = result['measurements'][0]
        m.update(rss_samples=0, sampled_peak_rss_bytes=0, elapsed_seconds=0)
        m['delivery'].update(missing=None, reconciliation_complete=False, integrity=dict(matched=None))
        result['manifest']['binary_version'] = '<script>alert(1)</script>'
        validate(result)
        page = render_run(dict(prefix=self.prefix, release='v0.12.0', report=result, downloads={}), True)
        self.assertIn('FAILED', page)
        self.assertIn('REHEARSAL', page)
        self.assertIn('does not establish 24 hours', page)
        self.assertIn('Unavailable / 0 / 0', page)
        self.assertIn('Unavailable MiB', page)
        self.assertIn('<dt>Measured test duration</dt><dd>Unavailable</dd>', page)
        m['elapsed_seconds'] = 60
        measured = render_run(dict(prefix=self.prefix, release='v0.12.0', report=result, downloads={}), True)
        self.assertIn('<dt>Measured test duration</dt><dd>1 minute</dd>', measured)
        self.assertIn('&lt;script&gt;', page)
        self.assertNotIn('<script>', page)
        result['manifest']['status'] = 'interrupted'
        self.assertIn('INTERRUPTED', render_run(dict(prefix=self.prefix, release='v0.12.0', report=result, downloads={}), False))

    def test_build_uses_latest_failure_instead_of_hiding_it_behind_an_older_pass(self):
        # Given a later failed run and earlier passing run, when built, then the latest status is visible.
        newer = report('failed')
        newer['manifest']['started_at'] = '2026-11-07T01:00:00+01:00'
        newer['measurements'] = []
        content = json.dumps(newer).encode()
        prefix = self.prefix+'1'
        name = prefix+'-endurance-results.json'
        self.assets += [dict(id=21, name=name), dict(id=22, name=prefix+'-endurance.SHA256SUMS')]
        def download(repository, asset):
            if asset['id'] == 21: return content
            if asset['id'] == 22: return f'{hashlib.sha256(content).hexdigest()}  {name}\n'.encode()
            return self.download(repository, asset)
        with tempfile.TemporaryDirectory() as temporary, patch('endurance_site.api', side_effect=self.api), \
                patch('endurance_site.download', side_effect=download):
            output = Path(temporary)/'site'
            build('owner/project', output)
            page = (output/'index.html').read_text()
            self.assertIn('Failed · 24 hours requested · v0.12.0', page)
            self.assertEqual(self.content, (output/'data'/ (self.prefix+'.json')).read_bytes())
            self.assertLess(page.index('#'+prefix+'"'), page.index('#'+self.prefix+'"'))
            self.assertIn('17,856,000', page)
            self.assertNotIn('{{', page)
            self.assertEqual(1, page.count(' open>'))

    def test_empty_release_history_renders_an_explicit_empty_state(self):
        # Given no published runs, when built, then no success or zero-result claim is invented.
        with tempfile.TemporaryDirectory() as temporary, patch('endurance_site.collect', return_value=[]):
            output = Path(temporary)/'site'
            build('owner/project', output)
            page = (output/'index.html').read_text()
        self.assertIn('No published endurance runs yet', page)
        self.assertNotIn('0 records verified', page)


if __name__ == '__main__':
    unittest.main()
