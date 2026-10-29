import json
from pathlib import Path
import tempfile
import tomllib
import unittest

from benchmark import override_config
from epoch_report import compare, summarize, table
from freeze_build import BENCH_FAST

GENERATED = '''state_dir = "state"
[compaction]
data_rewrite_scope = "all"
[limits]
table_workers = 4
batch_rows = 1024

[[tables]]
target_table = "events_0"
'''


def event(at, name, **fields):
    return json.dumps({"timestamp": f"2026-10-24T00:00:{at:06.3f}Z", "level": "DEBUG",
                       "fields": {"event": name, **fields}, "target": "flow_events"})


class OverrideConfigTest(unittest.TestCase):
    def test_replaces_and_adds_keys_within_their_sections(self):
        text = override_config(GENERATED, "limits", ["batch_rows = 2048", "collapse_memory_bytes=0"])
        text = override_config(text, "compaction", ["delete_files_soft = 16"])
        parsed = tomllib.loads(text)
        self.assertEqual(parsed["limits"], {"table_workers": 4, "batch_rows": 2048,
                                            "collapse_memory_bytes": 0})
        self.assertEqual(parsed["compaction"], {"data_rewrite_scope": "all", "delete_files_soft": 16})
        self.assertEqual(parsed["tables"], [{"target_table": "events_0"}])

    def test_overrides_the_durability_section(self):
        text = override_config(GENERATED.replace("[compaction]", '[durability]\nuri = "s3://b/p"\n[compaction]'),
                               "durability", ["max_writes_per_second = 0.0"])
        self.assertEqual(tomllib.loads(text)["durability"], {"uri": "s3://b/p", "max_writes_per_second": 0.0})

    def test_rejects_an_assignment_without_a_value(self):
        with self.assertRaisesRegex(ValueError, "--limit expects KEY=VALUE"):
            override_config(GENERATED, "limits", ["batch_rows"])


class EpochReportTest(unittest.TestCase):
    def run_directory(self, root, name, scale):
        directory = Path(root) / name
        (directory / "state").mkdir(parents=True)
        lines = []
        for index, start in enumerate((1.0, 5.0)):
            lines += [
                event(start + 1.0 * scale, "epoch_collapsed", operation_id=str(index), elapsed_ms=1000 * scale,
                      transactions=10, collapse_mode="memory" if index else "disk_capacity"),
                event(start + 2.0 * scale, "ingest_prepared", operation_id=str(index),
                      precommit_prepare_ms=1000 * scale),
                event(start + 2.5 * scale, "table_published", operation_id=str(index)),
                event(start + 3.0 * scale, "epoch_completed", operation_id=str(index), transactions=10),
            ]
        (directory / "daemon-1.log").write_text("not json\n" + "\n".join(lines) + "\n")
        (directory / "state" / "metrics.prom").write_text(
            'flow_compaction_publication_stall_seconds_sum{table_id="1"} 0.5\n')
        (directory / "report.json").write_text(json.dumps({
            "correctness_passed": True, "transaction_rows": 100, "failure": None,
            "phases": [{"name": "open-loop-workload", "details": {
                "measured_cohort_catalog_mutations_per_second_including_drain": 1000 / scale,
                "latency": {"commit_to_catalog": {"p50_ms": 500, "p95_ms": 900, "p99_ms": 950}}}}],
        }))
        return directory

    def test_splits_epoch_time_and_compares_medians(self):
        with tempfile.TemporaryDirectory() as root:
            base = summarize(self.run_directory(root, "base-1", 1.0))
            candidate = summarize(self.run_directory(root, "candidate-1", 0.5))
        self.assertEqual(base["epochs"], 2)
        self.assertEqual(base["mutations"], 2000)
        self.assertAlmostEqual(base["cdc_service_seconds"], 6.0)
        self.assertAlmostEqual(base["service_us_per_mutation"], 3000.0)
        self.assertAlmostEqual(base["stage_seconds"]["collapse"], 2.0)
        self.assertAlmostEqual(base["stage_seconds"]["commit"], 1.0)
        self.assertAlmostEqual(base["between_epochs_seconds"], 1.0)
        self.assertEqual(base["collapse_modes"], {"disk_capacity": 1, "memory": 1})
        self.assertEqual(base["compaction_stall_seconds"], 0.5)
        self.assertIn("| base-1 | yes |", table([base]))
        self.assertIn("| CDC service µs/mutation | 3,000.0 | 1,500.0 | -50.0% |", compare([base], [candidate]))


class BenchFastTest(unittest.TestCase):
    def test_bench_fast_is_release_without_lto(self):
        self.assertEqual(BENCH_FAST["CARGO_PROFILE_RELEASE_LTO"], "false")
        self.assertEqual(BENCH_FAST["CARGO_PROFILE_RELEASE_INCREMENTAL"], "true")


if __name__ == "__main__":
    unittest.main()
