"""Proxy observations require the pinned source and an intact insertion boundary."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification import proxy_evidence


class ProxyEvidenceTest(unittest.TestCase):
    def test_source_identity_failure_stops_before_patching_or_building(self):
        # Given upstream source with an untrusted revision or module checksum.
        for changed in ('revision', 'checksum'):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temporary:
                module = dict(Sum=proxy_evidence.MODULE_SUM,
                              Origin=dict(Hash=proxy_evidence.REVISION), Dir='unused')
                if changed == 'revision':
                    module['Origin']['Hash'] = 'unexpected'
                else:
                    module['Sum'] = 'untrusted'
                with patch.object(proxy_evidence.subprocess, 'check_output', return_value=json.dumps(module).encode()), \
                     patch.object(proxy_evidence.subprocess, 'run') as run:
                    # When preparing evidence, then no binary from this source runs.
                    with self.assertRaisesRegex(ValueError, 'exact pinned upstream'):
                        proxy_evidence.prepare(Path(temporary), {})
                    run.assert_not_called()

    def test_missing_pipeline_boundary_fails_closed(self):
        # Given source without the pinned pipeline installation boundary.
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source/'toxics').mkdir()
            for name in ('link.go', 'toxics/toxic.go'):
                (source/name).write_text('package changed\n')
            # When patching it, then failure cannot produce an instrumented build.
            with self.assertRaises(subprocess.CalledProcessError):
                proxy_evidence.patch_source(source)
            self.assertFalse((source/'toxics/kaferry_evidence.go').exists())


if __name__ == '__main__':
    unittest.main()
