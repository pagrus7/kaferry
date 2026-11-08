"""Report integrity tests: missing or contradictory measurements must not pass."""

import unittest
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

from tools.ci.quality import benchmarks, coverage, maintainability, markdown
from tools.ci.history import comparison


class CoverageTest(unittest.TestCase):
    def test_new_duplication_locations_survive_into_the_summary(self):
        # Given a scan containing an existing match and a new normalized test match.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / 'duplication').mkdir()
            clone = {'firstFile': {'name': str(Path.cwd() / 'tools/test_a.py'), 'start': 10, 'end': 20},
                     'secondFile': {'name': str(Path.cwd() / 'tools/test_b.py'), 'start': 30, 'end': 40},
                     'tokens': 80, 'isNew': True}
            scan = {'statistics': {'total': {'sources': 4, 'clones': 2, 'percentage': 5, 'newClones': 1}},
                    'duplicates': [dict(clone, isNew=False), clone]}
            (directory / 'duplication/jscpd-report.json').write_text(json.dumps(scan))
            with patch.dict('os.environ', {'QUALITY_BASE_SHA': 'a' * 40}):
                result = maintainability(directory)
            (directory / 'quality.json').write_text(json.dumps(result))
            # When rendering, then only new findings require action and both locations are visible.
            self.assertEqual([clone], result['new_clones'])
            summary = '\n'.join(markdown(directory))
            self.assertIn('New clones: **1**', summary)
            self.assertIn('tools/test_a.py:10-20', summary)
            self.assertIn('tools/test_b.py:30-40', summary)
            self.assertIn('New duplication fails CI', summary)

    def test_statement_weights_and_duplicate_blocks(self):
        # Given overlapping profiles with unequal statement counts.
        profile = "mode: atomic\np/a.go:1.1,2.2 8 0\np/a.go:1.1,2.2 8 3\np/b.go:1.1,3.1 2 0\n"
        # When summarizing the profile.
        result = coverage(profile)
        # Then repeated blocks count once and coverage is statement-weighted.
        self.assertEqual((result["covered"], result["statements"], result["percent"]), (8, 10, 80))

    def test_unusable_profiles_fail(self):
        for profile in ("", "mode: wrong", "mode: atomic\n", "mode: set\ninvalid",
                        "mode: set\np/a.go:1.1,2.2 2 1\np/a.go:1.1,2.2 3 1"):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                coverage(profile)

    def test_benchmarks_allow_additional_metrics(self):
        # Given Go benchmarks with default, throughput and custom codec metrics.
        log = "BenchmarkEncode/normal-2 100 42.5 ns/op 120.3 MB/s 8 B/op 1 allocs/op\n" \
              "BenchmarkOther-4 200 10 ns/op 0 B/op 0 allocs/op\n" \
              "BenchmarkEncode/tiny-14 1352 268976 ns/op 12.49 container-B/record 3717804 records/s 750079 B/op 2036 allocs/op\n"
        # When parsing the report, then all allocation measurements survive.
        self.assertEqual(benchmarks(log), {"BenchmarkEncode/normal": (42.5, 8, 1),
                                          "BenchmarkOther": (10.0, 0, 0),
                                          "BenchmarkEncode/tiny": (268976.0, 750079, 2036)})

    def test_history_uses_test_environment_and_image_identity(self):
        # Given the fixture startup environment is identical but actual test images differ.
        with tempfile.TemporaryDirectory() as name:
            current, previous = Path(name) / "current", Path(name) / "previous"
            current.mkdir(); previous.mkdir()
            (previous / "baseline.json").write_text(json.dumps({"suite": "load", "url": "run", "headSha": "abc"}))
            environment = {"go": "go1.26.9", "system": "Linux", "machine": "x86_64", "soak_duration": "30m"}
            for root, image in ((current, "new"), (previous, "old")):
                (root / "broker.environment.json").write_text(json.dumps(environment))
                (root / "load.environment.json").write_text(json.dumps({**environment, "broker_images": json.dumps([{"ID": image}])}))
            # When comparing results, then no numeric comparison is offered.
            self.assertIn("differ; no numeric comparison", "\n".join(comparison(current, previous)))

    def test_history_compares_only_equal_measurement_settings(self):
        # Given otherwise identical load measurements with declared timing and record limits.
        with tempfile.TemporaryDirectory() as name:
            current, previous = Path(name) / "current", Path(name) / "previous"
            environment = {"go": "go1.26.9", "system": "Linux", "machine": "x86_64",
                           "broker_images": [{"ID": "same"}]}
            for root in (current, previous):
                root.mkdir()
                (root / "load.environment.json").write_text(json.dumps(environment))
            (previous / "baseline.json").write_text(json.dumps({"suite": "load", "url": "run", "headSha": "abc"}))
            measurement = {"test": "limited copy", "passed": True, "skipped": False,
                           "worker_stop_after_records": 10, "records_per_second": 100,
                           "measurement_method": "sql_release_to_exit", "fixture_client": "franz-go"}
            (previous / "load.metrics.json").write_text(json.dumps(measurement))
            for changes in ({"worker_stop_after_records": 20}, {}, {"client": "new-version"}, {"measurement_method": None}, {"fixture_client": None}, {"archive_format": "avro"}, {"archive_payload": "random"}):
                with self.subTest(changes=changes):
                    actual = {key: value for key, value in {**measurement, **changes}.items()
                              if value is not None}
                    (current / "load.metrics.json").write_text(json.dumps(actual))
                    # When comparing matching settings, a changed cap, timing method or fixture client.
                    result = "\n".join(comparison(current, previous))
                    # Then only matching settings allow numeric comparison.
                    settings_changed = bool(changes.keys() - {'client'})
                    self.assertEqual("Settings differ" in result, settings_changed)
                    self.assertEqual("records_per_second" in result, not settings_changed)
                    if 'client' in changes:
                        self.assertIn('Client versions: previous unknown; current new-version', result)


if __name__ == "__main__":
    unittest.main()
