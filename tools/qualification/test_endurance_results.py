"""Endurance publication preserves failed evidence and binds uploads to the measured executable."""

import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout

from tools.qualification.endurance_results import prepare, publish


class PublicationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.evidence = Path(self.temporary.name)
        self.binary = b'release executable fixture'
        self.receipt = dict(suite='endurance', status='passed', requested_seconds=60,
            drain_seconds=300, started_at='2026-11-06T04:00:00+00:00', elapsed_seconds=370,
            source_commit='a'*40, working_tree_dirty=False, binary_version='kaferry dev',
            binary_sha256=hashlib.sha256(self.binary).hexdigest(), platform='Linux', go='go fixture')
        self.metrics = dict(binary_sha256=self.receipt['binary_sha256'], platform='linux-arm64',
            passed=True, skipped=False, endurance=dict(load_validated=True),
            delivery=dict(reconciliation_complete=True, integrity=dict(matched=True)))
        self.save()

    def save(self):
        (self.evidence / 'manifest.json').write_text(json.dumps(self.receipt))
        (self.evidence / 'soak.metrics.json').write_text(json.dumps(self.metrics))

    def prepare(self):
        # Existing ci summary table tests exercise rendering all metric fields.
        with patch('tools.qualification.endurance_results.summarize', return_value='## Metric tables\n'):
            return prepare(self.evidence, self.evidence / 'publication')

    def test_package_contains_evidence_and_checksums_and_refuses_overwrite(self):
        # Given real report inputs plus an unrelated private file and symlink.
        (self.evidence / 'soak.samples.jsonl').write_text('{}\n')
        (self.evidence / 'suite.log').write_text('test log')
        (self.evidence / 'private.txt').write_text('not evidence')
        (self.evidence / 'linked.log').symlink_to(self.evidence / 'private.txt')
        # When packaged, only declared evidence is retained.
        package = self.prepare()
        with tarfile.open(package / 'endurance-evidence.tar.gz') as archive:
            self.assertEqual({'manifest.json', 'soak.metrics.json', 'soak.samples.jsonl', 'suite.log'}, set(archive.getnames()))
        for line in (package / 'endurance.SHA256SUMS').read_text().splitlines():
            digest, name = line.split('  ')
            self.assertEqual(digest, hashlib.sha256((package / name).read_bytes()).hexdigest())
        self.assertIn('rehearsal', (package / 'endurance-results.md').read_text())
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.prepare()

    def test_failed_run_without_metrics_is_reported_without_claiming_success(self):
        self.receipt['status'] = 'failed'
        self.save()
        (self.evidence / 'soak.metrics.json').unlink()
        package = self.prepare()
        text = (package / 'endurance-results.md').read_text()
        self.assertIn('**FAILED**', text)
        self.assertIn('unresolved', text)
        self.assertEqual([], json.loads((package / 'endurance-results.json').read_text())['measurements'])

    def test_rejects_incomplete_or_mismatched_success_and_unfinished_receipt(self):
        receipt, metrics = self.receipt.copy(), json.dumps(self.metrics)
        for field in ('digest', 'load', 'integrity', 'running', 'missing'):
            with self.subTest(field=field):
                self.receipt, self.metrics = receipt.copy(), json.loads(metrics)
                if field == 'digest': self.metrics['binary_sha256'] = 'b'*64
                if field == 'load': self.metrics['endurance']['load_validated'] = False
                if field == 'integrity': self.metrics['delivery']['integrity']['matched'] = None
                if field == 'running': self.receipt['status'] = 'running'
                self.save()
                if field == 'missing': (self.evidence / 'soak.metrics.json').unlink()
                with self.assertRaises(ValueError): self.prepare()

    def test_upload_requires_matching_release_and_retains_unique_asset_names(self):
        package = self.prepare()
        uploaded = []
        def gh(command, **kwargs):
            if command[2] == 'download':
                target = Path(command[command.index('--dir')+1]) / 'kaferry-v0.12.0-linux-arm64.tar.gz'
                with tarfile.open(target, 'w:gz') as archive:
                    info = tarfile.TarInfo('kaferry'); info.size = len(self.binary)
                    archive.addfile(info, io.BytesIO(self.binary))
            elif command[:3] == ['gh', 'release', 'upload']:
                uploaded.extend(Path(path).name for path in command[command.index('--repo')+2:])
                checksums = Path(command[-1]).read_text()
                for path in command[command.index('--repo')+2:-1]:
                    self.assertIn(f'{hashlib.sha256(Path(path).read_bytes()).hexdigest()}  {Path(path).name}', checksums)
        with patch('tools.qualification.endurance_results.subprocess.run', side_effect=gh) as run:
            publish(package, 'v0.12.0', 'pagrus7/kaferry')
            self.assertEqual(4, len(uploaded))
            self.assertTrue(all(name.startswith('endurance-linux-arm64-202611060400000000-') for name in uploaded))
            self.assertNotIn('--clobber', run.call_args_list[1].args[0])
            self.assertEqual(['gh', 'workflow', 'run', 'endurance-results.yml', '--ref', 'main',
                              '--repo', 'pagrus7/kaferry'], run.call_args.args[0])
            uploaded.clear()
            # When refresh fails after upload, then retry guidance must not repeat the upload.
            def failed_refresh(command, **kwargs):
                if command[:2] == ['gh', 'workflow']:
                    raise subprocess.CalledProcessError(1, command)
                return gh(command, **kwargs)
            with patch('tools.qualification.endurance_results.subprocess.run', side_effect=failed_refresh):
                with self.assertRaisesRegex(ValueError, 'Evidence is uploaded.*Retry the refresh only: gh workflow run'):
                    publish(package, 'v0.12.0', 'pagrus7/kaferry')
            self.assertEqual(4, len(uploaded))
            uploaded.clear()
            self.binary = b'different release'
            with self.assertRaisesRegex(ValueError, 'does not match'):
                publish(package, 'v0.12.0', 'pagrus7/kaferry')
            self.assertFalse(uploaded)

    def test_tampered_package_never_contacts_github(self):
        package = self.prepare()
        (package / 'endurance-results.json').write_text('{}')
        with patch('tools.qualification.endurance_results.subprocess.run') as run:
            with self.assertRaisesRegex(ValueError, 'checksums'):
                publish(package, 'v0.12.0', 'pagrus7/kaferry')
            run.assert_not_called()

    def test_early_failure_renders_unmeasured_resources_as_unavailable(self):
        # Given a failed copy before RSS sampling, shutdown or stall recovery.
        self.receipt['status'] = 'failed'
        self.metrics.update(test='soak', passed=False, records=1, records_per_second=1,
            sampled_peak_rss_bytes=0, child_cpu_seconds=0, peak_uncommitted_records=0,
            peak_uncommitted_bytes=0, endurance=dict(recoveries=0, max_recovery_seconds=0))
        self.metrics.pop('delivery')
        self.metrics['endurance'].update(profile='application-24x32-v1', target_records_per_second=200,
            target_records=12400, submitted_at_publish_end=0, acknowledged_at_publish_end=0,
            acknowledged_after_grace=0, min_acknowledged_window_ratio=1, load_validated=False,
            max_scheduling_debt_records=0)
        self.save()
        # When rendering the real metric tables, then zero defaults are not observations.
        with redirect_stdout(io.StringIO()):
            package = prepare(self.evidence, self.evidence / 'publication')
        text = (package / 'endurance-results.md').read_text()
        self.assertIn('| CLI CPU seconds | unavailable |', text)
        self.assertIn('| Maximum stall recovery seconds | unavailable |', text)
        self.assertIn('| soak | FAIL | 1 | 1 | unavailable |', text)


if __name__ == '__main__':
    unittest.main()
