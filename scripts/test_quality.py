"""Report integrity tests: missing or contradictory measurements must not pass."""

import unittest
import json
from pathlib import Path
import tempfile

from quality import benchmarks, coverage
from history import comparison


class CoverageTest(unittest.TestCase):
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

    def test_benchmark_includes_optional_throughput(self):
        # Given actual Go benchmark formats with and without b.SetBytes.
        log = "BenchmarkRecordFraming/encode-2 100 42.5 ns/op 120.3 MB/s 8 B/op 1 allocs/op\n" \
              "BenchmarkOther-4 200 10 ns/op 0 B/op 0 allocs/op\n"
        # When parsing the report, then all allocation measurements survive.
        self.assertEqual(benchmarks(log), {"BenchmarkRecordFraming/encode": (42.5, 8, 1),
                                          "BenchmarkOther": (10.0, 0, 0)})

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

    def test_history_compares_only_equal_record_limits(self):
        # Given otherwise identical load measurements with different record limits.
        with tempfile.TemporaryDirectory() as name:
            current, previous = Path(name) / "current", Path(name) / "previous"
            environment = {"go": "go1.26.9", "system": "Linux", "machine": "x86_64",
                           "broker_images": [{"ID": "same"}]}
            for root in (current, previous):
                root.mkdir()
                (root / "load.environment.json").write_text(json.dumps(environment))
            (previous / "baseline.json").write_text(json.dumps({"suite": "load", "url": "run", "headSha": "abc"}))
            measurement = {"test": "limited copy", "passed": True, "skipped": False,
                           "record_limit": 10, "records_per_second": 100}
            (previous / "load.metrics.json").write_text(json.dumps(measurement))
            for limit in (20, 10):
                with self.subTest(limit=limit):
                    (current / "load.metrics.json").write_text(json.dumps({**measurement, "record_limit": limit}))
                    # When comparing the same scenario with a changed or matching cap.
                    result = "\n".join(comparison(current, previous))
                    # Then suppress numbers only when the workload's cap differs.
                    self.assertEqual("Settings differ" in result, limit != 10)
                    self.assertEqual("records_per_second" in result, limit == 10)


if __name__ == "__main__":
    unittest.main()
