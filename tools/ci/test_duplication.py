"""Exercise the real local/CI duplication gate with renamed Go and Python copies."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipUnless(os.environ.get('JSCPD'), 'Set JSCPD to the pinned jscpd 5.4.2 binary')
class DuplicationGateTest(unittest.TestCase):
    def test_baseline_new_normalized_tests_and_empty_scan(self):
        repository = Path(__file__).resolve().parents[2]
        executable = str(Path(os.environ['JSCPD']).resolve())
        sources = {
            'src/fixture/one_test.go': '''//go:build integration && qualification
package fixture
func original(seed int) int {
    result := seed
    for index := 0; index < 10; index++ {
        if index % 2 == 0 {
            result += index * seed
        } else {
            result -= index
        }
    }
    if result < 0 {
        return -result
    }
    return result + 10
}
''',
            'tools/fixture/test_one.py': '''def original(seed):
    result = seed
    for index in range(10):
        if index % 2 == 0:
            result += index * seed
        else:
            result -= index
    adjustment = seed * 3
    if result > 100:
        result = result - adjustment
    elif result < 20:
        result = result + adjustment
    else:
        result = result * 2 + adjustment
    if result < 0:
        return -result
    return result + 10
''',
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for filename in ('.jscpd.json', 'Makefile'):
                shutil.copyfile(repository / filename, root / filename)
            for filename, source in sources.items():
                path = root / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source)
                # Given existing duplicates, including renamed identifiers and changed literals.
                path.with_name(path.name.replace('one', 'two')).write_text(
                    source.replace('original', 'existing').replace('seed', 'value').replace('10', '20'))
            for command in (['git', 'init', '--quiet'], ['git', 'add', '.'],
                            ['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                             'commit', '--quiet', '-m', 'baseline']):
                subprocess.run(command, cwd=root, check=True, capture_output=True)
            command = ['make', 'duplicates', 'JSCPD=' + executable, 'DUPLICATION_BASE=HEAD']
            report = root / 'build-results/duplication/jscpd-report.json'
            result = subprocess.run(command, cwd=root, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            statistics = json.loads(report.read_text())['statistics']
            self.assertEqual({'go', 'python'}, set(statistics['formats']))
            baseline = statistics['total']
            self.assertGreater(baseline['clones'], 0)
            self.assertEqual(0, baseline['newClones'])
            for filename, source in sources.items():
                with self.subTest(language=Path(filename).suffix):
                    path = root / filename
                    added = path.with_name(path.name.replace('one', 'three'))
                    added.write_text(source.replace('original', 'added').replace('seed', 'sample').replace('10', '30'))
                    # When a new normalized copy is added, then the real gate fails and retains its findings.
                    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
                    self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
                    scan = json.loads(report.read_text())
                    self.assertGreater(scan['statistics']['total']['newClones'], 0)
                    language = 'go' if path.suffix == '.go' else 'python'
                    self.assertGreater(scan['statistics']['formats'][language]['newClones'], 0)
                    self.assertTrue(any(clone['isNew'] and clone['kind'] == 'renamed'
                                        for clone in scan['duplicates']))
                    self.assertTrue(report.with_suffix('.sarif').is_file())
                    added.unlink()
            for filename in root.glob('src/**/*.go'):
                filename.unlink()
            for filename in root.glob('tools/**/*.py'):
                filename.unlink()
            # Then a misconfigured scan selecting no source files also fails.
            result = subprocess.run(command, cwd=root, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual(0, json.loads(report.read_text())['statistics']['total']['sources'])
