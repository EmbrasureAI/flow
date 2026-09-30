"""Actual-file storage, workload-order and exact attribution regressions."""

import json
import math
from pathlib import Path
import random
import sqlite3
from contextlib import closing
import tempfile
import unittest

from benchmark_store import BenchmarkStore, DenseKeys, PAGE_ROWS, lsn_key, lsn_text, zipf_sample
from reconciliation import scan_reconciliation_events


class BenchmarkStorageTest(unittest.TestCase):
    def test_reconciliation_cursor_requires_exact_complete_event(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "daemon.log"
            expected = {(42, 101)}
            records = [
                {"fields": {"event": "table_published", "table_id": 42, "snapshot_id": 101}},
                {"fields": {"event": "external_snapshot_reconciled", "table_id": 42,
                            "snapshot_id": 100, "sequence_number": 9,
                            "reconciliation_kind": "data-rewrite"}},
            ]
            partial = {"fields": {"event": "external_snapshot_reconciled", "table_id": 42,
                                  "snapshot_id": 101, "sequence_number": 10,
                                  "reconciliation_kind": "data-rewrite"}}
            log.write_bytes(("not-json\nnull\n" + "\n".join(map(json.dumps, records)) + "\n" +
                             json.dumps(partial)).encode())

            offset, acknowledged = scan_reconciliation_events(log, 0, expected)
            self.assertEqual(acknowledged, {})
            self.assertLess(offset, log.stat().st_size)

            with log.open("ab") as output:
                output.write(b"\n")
            offset, acknowledged = scan_reconciliation_events(log, offset, expected)
            self.assertEqual(acknowledged, {(42, 101): {
                "table_id": 42, "snapshot_id": 101, "sequence_number": 10,
                "reconciliation_kind": "data-rewrite",
            }})
            final_offset, acknowledged = scan_reconciliation_events(log, offset, expected)
            self.assertEqual((final_offset, acknowledged), (offset, {}))

    def test_disk_key_slots_preserve_selection_and_swap_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            for distribution in ("uniform", "hot", "zipf"):
                reference = list(range(1, 5000, 4))
                positions = {key: i for i, key in enumerate(reference)}
                old_rng, new_rng = random.Random(20260905), random.Random(20260905)
                keys = DenseKeys(Path(temporary) / distribution, reference)
                try:
                    for transaction in range(80):
                        count = 8
                        if distribution == "zipf":
                            expected = zipf_sample(old_rng, reference, count, .99)
                            indices = zipf_sample(new_rng, range(len(keys)), count, .99)
                        else:
                            population = max(count, len(reference) // 100) if distribution == "hot" else len(reference)
                            expected = old_rng.sample(reference[:population], count)
                            indices = new_rng.sample(range(population), count)
                        self.assertEqual([keys[index] for index in indices], expected)
                        self.assertEqual(old_rng.getstate(), new_rng.getstate())
                        # Exercise removal of a later selected last key as well
                        # as ordinary random key updates/deletes.
                        removals = indices[4:] if transaction else [0, len(keys) - 1, len(keys) - 2]
                        deleted = [keys[index] for index in removals]
                        for key in deleted:
                            position = positions.pop(key)
                            last = reference.pop()
                            if position < len(reference):
                                reference[position] = last
                                positions[last] = position
                        keys.remove_selected(removals)
                        added = list(range(10000 + transaction * 9, 10000 + (transaction + 1) * 9))
                        for key in added:
                            positions[key] = len(reference)
                            reference.append(key)
                        keys.extend(added)
                        self.assertEqual(list(keys), reference)
                    self.assertEqual((Path(temporary) / distribution).stat().st_size, 8 * len(reference))
                finally:
                    keys.close()

    def test_recorder_holes_unsigned_attribution_and_exact_exports(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = BenchmarkStore(directory / "evidence.sqlite3")
            try:
                # More than a queue and a page; a missing low sequence must be
                # revisited even when arrivals extend the current scan round.
                for sequence in range(PAGE_ROWS + 3):
                    store.record({"sequence": sequence, "table": "probes", "measured": False,
                                  "scheduled_at_micros": sequence}, -(sequence + 1))
                store.flush()
                page, cursor = store.pending("probes")
                self.assertEqual(len(page), PAGE_ROWS)
                store.observed([sequence for sequence, _ in page if sequence], 500)
                for sequence in range(PAGE_ROWS + 3, PAGE_ROWS * 2 + 10):
                    store.record({"sequence": sequence, "table": "probes", "measured": False,
                                  "scheduled_at_micros": sequence}, -(sequence + 1))
                store.flush()
                page, cursor = store.pending("probes", cursor)
                self.assertEqual([sequence for sequence, _ in page], list(range(PAGE_ROWS, PAGE_ROWS + 3)))
                store.observed([sequence for sequence, _ in page], 600)
                store.flush()
                page, cursor = store.pending("probes", cursor)
                self.assertEqual(page[0], (0, -1))
                self.assertEqual(len(page), PAGE_ROWS)

                journal, publications = [], []
                expected_corrected, expected_raw, expected_reader = [], [], []
                for i in range(9):
                    sequence = 10000 + i
                    row = {"sequence": sequence, "table": "a" if i % 2 == 0 else "b", "measured": True,
                           "scheduled_at_micros": 1_000_000 + i * 10000}
                    if i == 8:
                        row["missed_arrival"] = True
                        store.record(row)
                        continue
                    row.update(started_at_micros=row["scheduled_at_micros"] + 100,
                               xid=str((1 << 32) + 7 + i))
                    store.record(row, -sequence - 1)
                    if i == 7:  # Attempted source transaction, no commit response.
                        continue
                    commit = row["scheduled_at_micros"] + 3000
                    row.update(commit_response_at_micros=commit - 1000, marker=-sequence - 1,
                               mutations=100, update_rows=70, delete_rows=10, insert_rows=20)
                    # Observation can precede the recorder's commit-response update.
                    if i != 4:
                        store.observed([sequence], commit + 1000)
                    store.record(row, row["marker"])
                    if i == 6:  # Missing journal evidence.
                        continue
                    end = lsn_text(lsn_key((1 << 63) - 16 + i * 32))
                    journal.append({"event": "transaction_journaled", "source_id": "test", "xid": 7 + i,
                                    "end_lsn": end, "commit_timestamp_micros": commit,
                                    "journaled_at_micros": commit - 2000 + i * 100})
                    expected_raw.append((-2000 + i * 100) / 1000)
                    expected_corrected.append((1000 + i * 100) / 1000)
                    if i != 5:  # Missing catalog interval; the next table interval cannot cover this gap.
                        publications.append({"event": "table_published", "operation_id": str(i),
                                             "table_id": 11 if i % 2 == 0 else 12, "first_lsn": end, "last_lsn": end,
                                             "catalog_committed_at_micros": commit + 2000, "already_committed": False})
                        if i != 4:
                            expected_reader.append((commit + 1000 - row["scheduled_at_micros"]) / 1000)
                store.sample("resource", {"at_micros": 1, "metrics": {"flow_journal_bytes": 7}, "processes": []})
                store.sample("reader", {"marker_rows": 1})
                store.flush()
                log = directory / "daemon-1.log"
                events = journal + publications + [journal[0], publications[0]]
                log.write_text("not-json\n" + "\n".join(json.dumps({"fields": event}) for event in reversed(events)))
                store.import_logs([log], "test")
                store.analyze({"a": 11, "b": 12}, 3000)
                for name, expected in (("commit_to_journal", expected_corrected), ("commit_to_journal_raw", expected_raw),
                                       ("scheduled_to_reader", expected_reader)):
                    expected.sort()
                    result = store.distribution(name)
                    self.assertEqual(result["count"], len(expected))
                    for p in (50, 95, 99):
                        self.assertEqual(result[f"p{p}_ms"], expected[math.ceil(len(expected) * p / 100) - 1])
                    self.assertEqual(result["max_ms"], expected[-1])
                self.assertEqual(store.distribution("catalog_to_reader")["max_ms"], -1)
                self.assertEqual(store.distribution("scheduled_to_reader", 9)["p50_ms"], "failed_or_unobserved")
                self.assertFalse(store.all_committed_observed())
                store.export(directory, enriched=True)
                exported = json.loads((directory / "transactions.json").read_text())
                measured = [row for row in exported if row["measured"]]
                self.assertEqual(len(measured), 9)
                self.assertEqual(measured[0]["end_lsn"], "7FFFFFFF/FFFFFFF0")
                self.assertEqual(measured[1]["end_lsn"], "80000000/10")
                self.assertIsNone(measured[4]["reader_observed_at_micros"])
                self.assertNotIn("catalog_committed_at_micros", measured[5])
                self.assertNotIn("end_lsn", measured[6])
                self.assertNotIn("commit_response_at_micros", measured[7])
                self.assertTrue(measured[8]["missed_arrival"])
                self.assertEqual(len(json.loads((directory / "resources.json").read_text())), 1)
                conflict = journal[0] | {"journaled_at_micros": 123}
                with log.open("a") as output:
                    output.write("\n" + json.dumps({"fields": conflict}))
                with self.assertRaisesRegex(ValueError, "conflicting repeated journal"):
                    store.import_logs([log], "test")
                log.write_text("\n".join(json.dumps({"fields": event}) for event in events + [
                    journal[0] | {"end_lsn": "FFFFFFFF/FFFFFFFF"}]))
                with self.assertRaisesRegex(ValueError, "ambiguous wire XID"):
                    store.import_logs([log], "test")
            finally:
                store.close()
            # Final evidence is reusable without loading a JSON array or running
            # another service. SQLite verifies the actual closed file as well.
            with closing(sqlite3.connect(directory / "evidence.sqlite3")) as reopened:
                self.assertEqual(reopened.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(reopened.execute("SELECT count(*) FROM transactions WHERE measured=1").fetchone()[0], 9)

            # Actual database failure must reach producers and shutdown must
            # join the failed recorder before its companion connection closes.
            failed = BenchmarkStore(directory / "failed.sqlite3")
            failed.db.execute("DROP TABLE transactions")
            failed.db.commit()
            failed.record({"sequence": 0, "table": "a", "measured": True, "scheduled_at_micros": 0})
            with self.assertRaisesRegex(RuntimeError, "recorder failed"):
                failed.flush()
            with self.assertRaisesRegex(RuntimeError, "recorder failed"):
                failed.close()
            self.assertFalse(failed.thread.is_alive())

    def test_writer_timing_schema_queue_measurement_and_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = BenchmarkStore(Path(temporary) / "timings.sqlite3")
            try:
                legacy_columns = [
                    "sequence", "table_name", "measured", "scheduled_at_micros", "missed_arrival",
                    "started_at_micros", "xid", "wire_xid", "probe_marker", "commit_response_at_micros",
                    "mutations", "update_rows", "delete_rows", "insert_rows", "marker",
                    "reader_observed_at_micros",
                ]
                columns = [row[1] for row in store.db.execute("PRAGMA table_info(transactions)")]
                self.assertEqual(columns[:len(legacy_columns)], legacy_columns)

                for value in range(1, 6):
                    store.record({
                        "sequence": value,
                        "table": "timed",
                        "measured": True,
                        "scheduled_at_micros": value,
                        "admission_lag_micros": value,
                        "key_prepare_micros": value * 2,
                        "intent_enqueue_wait_micros": value * 3,
                        "postgres_transaction_micros": value * 4,
                        "commit_enqueue_wait_micros": value * 5,
                        "previous_postcommit_keys_micros": value * 6,
                    })
                enqueue_wait = store.record(
                    {"sequence": 10, "table": "timed", "measured": False, "scheduled_at_micros": 10},
                    enqueue_field="intent_enqueue_wait_micros",
                )
                store.sample("writer_terminal", {
                    "worker": 0, "sequence": 5, "measured": True, "postcommit_keys_micros": 37,
                })
                store.sample("writer_terminal", {
                    "worker": 1, "sequence": 0, "measured": False, "postcommit_keys_micros": 999,
                })
                summary = store.writer_timing_summary()

                measured = summary["measured_arrival_cohort_micros"]
                self.assertEqual(measured["admission_lag_micros"], {
                    "count": 5, "total": 15, "p50": 3, "p95": 5, "p99": 5, "max": 5,
                })
                self.assertEqual(measured["previous_postcommit_keys_micros"]["total"], 90)
                self.assertEqual(summary["terminal_postcommit_keys_micros"], {
                    "count": 1, "total": 37, "p50": 37, "p95": 37, "p99": 37, "max": 37,
                })
                persisted = store.db.execute(
                    "SELECT intent_enqueue_wait_micros FROM transactions WHERE sequence=10"
                ).fetchone()[0]
                self.assertEqual(persisted, enqueue_wait)
                self.assertGreaterEqual(enqueue_wait, 0)

                recorder = summary["recorder_queue"]
                self.assertGreaterEqual(recorder["high_water_items"], 1)
                self.assertLessEqual(recorder["high_water_items"], recorder["capacity_items"])
                self.assertEqual(recorder["enqueue_count"],
                                 sum(item["count"] for item in recorder["by_kind"].values()))
                self.assertEqual(recorder["by_kind"]["transaction"]["count"], 6)
                self.assertEqual(recorder["by_kind"]["sample"]["count"], 2)
                with self.assertRaises(ValueError):
                    store.record({"sequence": 11, "table": "timed", "measured": False,
                                  "scheduled_at_micros": 11}, enqueue_field="unknown")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
