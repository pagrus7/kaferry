"""Report integrity tests: missing or contradictory measurements must not pass."""

import unittest
import json

from tools.ci.quality import benchmarks


class QualityTest(unittest.TestCase):
    def test_benchmark_json_preserves_split_output_and_measurements(self):
        # Given Go JSON that splits a benchmark row across output events.
        events = [dict(Action='start', Package='codec'),
                  dict(Action='output', Package='codec', Output='BenchmarkEncode/normal-2\t'),
                  dict(Action='output', Package='codec', Output='100 42.5 ns/op 8 B/op 1 allocs/op\n'),
                  dict(Action='pass', Package='codec')]
        # When parsed, then retain the same numeric result as plain Go output.
        self.assertEqual({'BenchmarkEncode/normal': (42.5, 8, 1)},
                         benchmarks('\n'.join(json.dumps(event) for event in events)))


if __name__ == "__main__":
    unittest.main()
