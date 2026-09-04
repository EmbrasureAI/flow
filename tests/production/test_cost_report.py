"""Exercise the offline cost CLI using real inputs, evidence and output files."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CostReportTest(unittest.TestCase):
    def test_scoped_arithmetic_partial_evidence_and_invalid_inputs(self):
        command = Path(__file__).with_name("cost_report.py")
        period = ["2026-09-04T00:00:00Z", "2026-09-04T01:00:00Z"]

        def quantity(value, unit, basis):
            return {"value": value, "unit": unit, "window": period.copy(),
                    "evidence": "evidence.txt", "basis": basis}

        complete = {
            "accounting_scope": "Synthetic arithmetic fixture; compute, storage and provider requests only.",
            "window": period,
            "quantities": {
                "committed_mutations": quantity(2_000_000, "mutations", "successful committed mutations"),
                "source_bytes": quantity(2 << 30, "bytes", "decoded logical change payload bytes"),
                "active_table_seconds": quantity(7200, "table-seconds", "two tables active for the entire hour"),
                "object_store_requests": quantity(4000, "requests", "provider request records"),
                "catalog_commits": quantity(100, "commits", "distinct successful operation IDs"),
                "compaction_bytes": quantity(6 << 30, "bytes", "physical input plus output file bytes"),
            },
            "cost_items": {
                "compute": quantity(2, "hours", "two allocated instance-hours"),
                "storage": quantity(10, "GiB-hours", "integrated retained storage"),
                "object_requests": quantity(4000, "requests", "provider request records"),
            },
            "fileio_calls": quantity(8000, "calls", "logical FileIO calls; not provider requests"),
        }
        prices = {"compute": {"usd_per_unit": .5, "unit": "hours"},
                  "storage": {"usd_per_unit": .2, "unit": "GiB-hours"},
                  "object_requests": {"usd_per_unit": .00025, "unit": "requests"}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            evidence = root / "evidence.txt"
            evidence.write_text("Synthetic evidence for CLI arithmetic; not production measurements.\n")

            def invoke(name, measured, rates=prices):
                inputs = root / f"{name}-measurements.json"
                pricing = root / f"{name}-prices.json"
                output = root / f"{name}-output.json"
                inputs.write_text(json.dumps(measured))
                pricing.write_text(json.dumps(rates))
                originals = {path: path.read_bytes() for path in (inputs, pricing, evidence)}
                result = subprocess.run([sys.executable, str(command), "--measurements", str(inputs),
                                         "--prices", str(pricing), "--output", str(output)],
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(originals, {path: path.read_bytes() for path in originals})
                return result, output, originals

            result, output, originals = invoke("complete", complete)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["known_subtotal_usd"], 4)
            self.assertEqual(report["total_usd_within_declared_scope"], 4)
            self.assertEqual(report["accounting_scope"], complete["accounting_scope"])
            self.assertEqual({name: metric["value"] for name, metric in report["metrics"].items()}, {
                "dollars_per_million_mutations": 2, "dollars_per_GiB_ingested": 2,
                "dollars_per_active_table_hour": 2, "object_store_operations_per_million_mutations": 2000,
                "catalog_commits_per_million_mutations": 50, "compaction_bytes_per_source_byte": 3,
            })
            self.assertEqual(report["fileio_diagnostic"]["value"], 4000)
            for identity in (report["inputs"]["measurements"], report["inputs"]["prices"]):
                self.assertEqual(identity["sha256"], hashlib.sha256(originals[Path(identity["path"])]).hexdigest())
            self.assertEqual(report["inputs"]["evidence_sha256"][str(evidence.resolve())],
                             hashlib.sha256(evidence.read_bytes()).hexdigest())
            retained = output.read_bytes()
            result, _, _ = invoke("complete", complete)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(output.read_bytes(), retained, "existing output must remain immutable")

            partial = copy.deepcopy(complete)
            for name in ("source_bytes", "object_store_requests", "compaction_bytes"):
                partial["quantities"][name] = None
            partial["cost_items"]["object_requests"] = None
            result, output, _ = invoke("partial", partial, {"compute": prices["compute"]})
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["known_subtotal_usd"], 1)
            self.assertIsNone(report["total_usd_within_declared_scope"])
            self.assertEqual(set(report["missing_cost_items"]), {"storage", "object_requests"})
            for name, metric in report["metrics"].items():
                if name == "catalog_commits_per_million_mutations":
                    self.assertEqual(metric["value"], 50)
                else:
                    self.assertIsNone(metric["value"])
                    self.assertTrue(metric["reason"])

            zero = copy.deepcopy(complete)
            for name in ("committed_mutations", "source_bytes", "active_table_seconds"):
                zero["quantities"][name]["value"] = 0
            result, output, _ = invoke("zero", zero)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(all(metric["value"] is None and "is zero" in metric["reason"]
                                for metric in json.loads(output.read_text())["metrics"].values()))

            invalid = []
            wrong_window = copy.deepcopy(complete)
            wrong_window["quantities"]["catalog_commits"]["window"][1] = "2026-09-04T02:00:00Z"
            invalid.append(("window", wrong_window, prices))
            wrong_price = copy.deepcopy(prices)
            wrong_price["compute"]["unit"] = "seconds"
            invalid.append(("price-unit", complete, wrong_price))
            fileio_cost = copy.deepcopy(complete)
            fileio_cost["cost_items"]["object_requests"]["unit"] = "calls"
            invalid.append(("fileio-not-billable", fileio_cost, prices))
            for label, value in (("fractional-count", 1.5), ("negative-count", -1)):
                malformed = copy.deepcopy(complete)
                malformed["quantities"]["committed_mutations"]["value"] = value
                invalid.append((label, malformed, prices))
            nonfinite_price = copy.deepcopy(prices)
            nonfinite_price["compute"]["usd_per_unit"] = float("nan")
            invalid.append(("nonfinite-price", complete, nonfinite_price))
            for label, measured, rates in invalid:
                with self.subTest(label=label):
                    result, output, _ = invoke(label, measured, rates)
                    self.assertEqual(result.returncode, 2, result.stdout)
                    self.assertIn("cost report:", result.stderr)
                    self.assertFalse(output.exists(), "invalid inputs must not create a report")


if __name__ == "__main__":
    unittest.main()
