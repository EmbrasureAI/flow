"""Disk-backed benchmark evidence and active-key slots; no service dependencies."""

from bisect import bisect_right
from collections.abc import Sequence
import json
import math
import os
from pathlib import Path
import queue
import sqlite3
import struct
import threading
import time


PAGE_ROWS = 1024
QUEUE_ITEMS = 256
LATENCIES = ("commit_to_journal", "commit_to_journal_raw", "journal_to_catalog",
             "commit_to_catalog", "catalog_to_reader", "commit_to_reader",
             "scheduled_to_reader", "client_commit_response")
SOURCE_COLUMNS = ("sequence", "table_name", "measured", "scheduled_at_micros", "missed_arrival",
                  "started_at_micros", "xid", "wire_xid", "probe_marker", "commit_response_at_micros",
                  "mutations", "update_rows", "delete_rows", "insert_rows", "marker",
                  "admission_lag_micros", "key_prepare_micros", "intent_enqueue_wait_micros",
                  "postgres_transaction_micros", "commit_enqueue_wait_micros",
                  "previous_postcommit_keys_micros")
WRITER_TIMINGS = ("admission_lag_micros", "key_prepare_micros", "intent_enqueue_wait_micros",
                  "postgres_transaction_micros", "commit_enqueue_wait_micros",
                  "previous_postcommit_keys_micros")


class _QueueItem:
    __slots__ = ("kind", "value", "started_ns", "wait_field", "wait_micros")

    def __init__(self, kind, value, wait_field):
        self.kind = kind
        self.value = value
        self.started_ns = time.monotonic_ns()
        self.wait_field = wait_field
        self.wait_micros = None


class _EvidenceQueue(queue.Queue):
    """Measure insertion under Queue's existing mutex."""

    def __init__(self, maxsize):
        super().__init__(maxsize)
        self.high_water_mark = 0
        self.enqueue_count = 0
        self.enqueue_wait_ns = 0
        self.max_enqueue_wait_ns = 0
        self.enqueue_by_kind = {}

    def _put(self, item):
        elapsed = time.monotonic_ns() - item.started_ns
        item.wait_micros = elapsed / 1000
        if item.wait_field is not None:
            item.value[item.wait_field] = item.wait_micros
        super()._put(item)
        self.high_water_mark = max(self.high_water_mark, self._qsize())
        self.enqueue_count += 1
        self.enqueue_wait_ns += elapsed
        self.max_enqueue_wait_ns = max(self.max_enqueue_wait_ns, elapsed)
        count, total, maximum = self.enqueue_by_kind.get(item.kind, (0, 0, 0))
        self.enqueue_by_kind[item.kind] = (count + 1, total + elapsed, max(maximum, elapsed))


def lsn_key(value):
    """SQLite signed integers cannot order the entire PostgreSQL u64 LSN domain."""
    if isinstance(value, str):
        high, low = value.split("/")
        high, low = int(high, 16), int(low, 16)
        if not 0 <= high <= 0xffffffff or not 0 <= low <= 0xffffffff:
            raise ValueError("invalid PostgreSQL LSN")
        value = (high << 32) | low
    return value.to_bytes(8, "big")


def lsn_text(value):
    number = int.from_bytes(value, "big")
    return f"{number >> 32:X}/{number & 0xffffffff:X}"



def zipf_sample(rng, population, count, exponent):
    """Successive distinct rank draws with weights r**-exponent, 1 <= r <= N.

    A power-of-two bucket proposes each rank with its bucket's maximum weight;
    rejection corrects that envelope to the finite Zipf law. Previously chosen
    ranks are rejected. O(log N + count) memory; exhaustion fails the workload.
    """
    if not 0 <= count <= len(population) or not math.isfinite(exponent) or not 0 < exponent <= 2:
        raise ValueError("Zipf sampling requires count <= population and 0 < exponent <= 2")
    if not count:
        return []
    buckets, cumulative = [], []
    lower, total = 1, 0.0
    while lower <= len(population):
        upper = min(2 * lower, len(population) + 1)
        total += (upper - lower) * lower ** -exponent
        buckets.append((lower, upper))
        cumulative.append(total)
        lower *= 2
    chosen, seen = [], set()
    attempts = max(1024, 128 * count)
    for _ in range(attempts):
        lower, upper = buckets[bisect_right(cumulative, rng.random() * total)]
        rank = rng.randrange(lower, upper)
        if rank not in seen and rng.random() < (lower / rank) ** exponent:
            seen.add(rank)
            chosen.append(population[rank - 1])
            if len(chosen) == count:
                return chosen
    raise RuntimeError(f"Zipf selection exhausted {attempts} proposals for {count} distinct keys "
                       f"from {len(population)} at exponent {exponent}; reduce transaction size or skew")


class DenseKeys(Sequence):
    """Fixed-width positive keys with exactly the former list's swap-last order.

    One private file per writer/table. Only selected ranks and this transaction's
    deletion-position map live in Python; the file grows by eight bytes per key.
    """

    def __init__(self, path, initial):
        self.fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        self.length = 0
        try:
            batch = []
            for key in initial:
                batch.append(key)
                if len(batch) == PAGE_ROWS:
                    self.extend(batch)
                    batch.clear()
            self.extend(batch)
        except BaseException:
            self.close()
            raise

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        if not isinstance(index, int) or not 0 <= index < self.length:
            raise IndexError(index)
        raw = os.pread(self.fd, 8, index * 8)
        if len(raw) != 8:
            raise IOError("active-key file truncated")
        return struct.unpack("<q", raw)[0]

    def _write(self, offset, data):
        if os.pwrite(self.fd, data, offset) != len(data):
            raise IOError("short active-key write")

    def extend(self, keys):
        if keys:
            self._write(self.length * 8, struct.pack(f"<{len(keys)}q", *keys))
            self.length += len(keys)

    def remove_selected(self, indices):
        if not indices:
            return
        # A removed last key may itself be selected for a later deletion.
        pending = {self[index]: index for index in indices}
        if len(pending) != len(indices):
            raise ValueError("duplicate active-key deletion")
        for key in tuple(pending):
            position = pending.pop(key)
            last = self[self.length - 1]
            self.length -= 1
            if position < self.length:
                self._write(position * 8, struct.pack("<q", last))
                if last in pending:
                    pending[last] = position
        os.ftruncate(self.fd, self.length * 8)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA cache_size=-16384")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA mmap_size=0")
    return db


def write_array(path, rows):
    """Keep the existing JSON artifact contract without materializing the array."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        output.write("[\n")
        separator = ""
        for row in rows:
            output.write(separator)
            json.dump(row, output, allow_nan=False, separators=(",", ":"))
            separator = ",\n"
        output.write("\n]\n")
    temporary.replace(path)


def bounded_lines(path):
    with path.open("rb") as source:
        while line := source.readline((1 << 20) + 1):
            if len(line) > 1 << 20:
                raise ValueError(f"benchmark log line exceeds 1 MiB in {path}")
            yield line.decode("utf-8", errors="replace")


class BenchmarkStore:
    """One batched recorder, bounded producer queue, and file-backed SQL analysis.

    A full queue blocks producers rather than dropping measurements. flush() is
    an evidence visibility barrier, not a per-source-commit filesystem barrier.
    The recorder's errors fail the workload. This is test evidence, not a source
    recovery journal: SQLite uses WAL/NORMAL and flushes on orderly shutdown.
    """

    def __init__(self, path):
        self.path = Path(path)
        if self.path.exists():
            raise FileExistsError(self.path)
        self.db = connect(self.path)
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE transactions (
                sequence INTEGER PRIMARY KEY, table_name TEXT NOT NULL, measured INTEGER NOT NULL,
                scheduled_at_micros INTEGER NOT NULL, missed_arrival INTEGER, started_at_micros INTEGER,
                xid TEXT, wire_xid INTEGER, probe_marker INTEGER, commit_response_at_micros INTEGER,
                mutations INTEGER, update_rows INTEGER, delete_rows INTEGER, insert_rows INTEGER,
                marker INTEGER, reader_observed_at_micros INTEGER,
                admission_lag_micros REAL, key_prepare_micros REAL,
                intent_enqueue_wait_micros REAL, postgres_transaction_micros REAL,
                commit_enqueue_wait_micros REAL, previous_postcommit_keys_micros REAL);
            CREATE INDEX pending_probes ON transactions(table_name,sequence)
                WHERE reader_observed_at_micros IS NULL AND probe_marker IS NOT NULL;
            CREATE TABLE samples (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, record TEXT NOT NULL);
            CREATE TABLE journal (wire_xid INTEGER NOT NULL, end_lsn BLOB NOT NULL,
                commit_at_micros INTEGER NOT NULL, journaled_at_micros INTEGER NOT NULL,
                PRIMARY KEY(wire_xid,end_lsn)) WITHOUT ROWID;
            CREATE UNIQUE INDEX journal_end ON journal(end_lsn);
            CREATE TABLE publications (operation_id TEXT PRIMARY KEY, table_id INTEGER NOT NULL,
                first_lsn BLOB NOT NULL,last_lsn BLOB NOT NULL,catalog_committed_at_micros INTEGER NOT NULL);
            CREATE INDEX publication_interval ON publications(table_id,last_lsn);
        """)
        self.queue = _EvidenceQueue(QUEUE_ITEMS)
        self.error = None
        self.closed = False
        self.thread = threading.Thread(target=self._record, name="benchmark-evidence")
        self.thread.start()

    def _put(self, kind, value, wait_field=None):
        item = _QueueItem(kind, value, wait_field)
        while True:
            self.check()
            if self.closed:
                raise RuntimeError("benchmark recorder is closed")
            try:
                self.queue.put(item, timeout=.1)
                return item.wait_micros
            except queue.Full:
                pass

    def check(self):
        if self.error is not None:
            raise RuntimeError("benchmark evidence recorder failed") from self.error

    def record(self, row, probe_marker=None, enqueue_field=None):
        if enqueue_field not in (None, "intent_enqueue_wait_micros", "commit_enqueue_wait_micros"):
            raise ValueError(enqueue_field)
        values = row | {"table_name": row["table"], "probe_marker": probe_marker,
                        "wire_xid": int(row["xid"]) & 0xffffffff if "xid" in row else None}
        return self._put("transaction", values, enqueue_field)

    def sample(self, kind, row):
        return self._put("sample", (kind, json.dumps(row, allow_nan=False, separators=(",", ":"))))

    def observed(self, sequences, at_micros):
        return self._put("observed", [(at_micros, sequence) for sequence in sequences])

    def _record(self):
        db = None
        barrier = None
        try:
            db = connect(self.path)
            db.execute("PRAGMA synchronous=NORMAL")
            columns = ",".join(SOURCE_COLUMNS)
            upsert = (f"INSERT INTO transactions({columns}) VALUES({','.join('?' for _ in SOURCE_COLUMNS)}) "
                      "ON CONFLICT(sequence) DO UPDATE SET " + ",".join(
                          f"{name}=excluded.{name}" for name in SOURCE_COLUMNS if name != "sequence"))
            while True:
                item = self.queue.get()
                kind, value = item.kind, item.value
                count = 0
                while kind not in ("barrier", "close"):
                    if kind == "transaction":
                        db.execute(upsert, tuple(value.get(name) for name in SOURCE_COLUMNS))
                    elif kind == "sample":
                        db.execute("INSERT INTO samples(kind,record) VALUES(?,?)", value)
                    elif kind == "observed":
                        db.executemany("UPDATE transactions SET reader_observed_at_micros=? "
                                       "WHERE sequence=? AND reader_observed_at_micros IS NULL", value)
                    count += 1
                    if count == QUEUE_ITEMS:
                        break
                    try:
                        item = self.queue.get_nowait()
                        kind, value = item.kind, item.value
                    except queue.Empty:
                        break
                barrier = value if kind == "barrier" else None
                db.commit()
                if barrier is not None:
                    barrier.set()
                    barrier = None
                if kind == "close":
                    break
        except BaseException as error:
            self.error = error
            if barrier is not None:
                barrier.set()
        finally:
            if db is not None:
                db.close()

    def flush(self):
        barrier = threading.Event()
        self._put("barrier", barrier)
        while not barrier.wait(.1):
            self.check()
        self.check()

    def pending(self, table, cursor=None):
        # Freeze a round's upper sequence. New arrivals cannot indefinitely keep
        # the cursor ahead of an older missing probe.
        after, through = cursor if cursor is not None else (-1, None)
        if through is None:
            through = self.db.execute("SELECT max(sequence) FROM transactions WHERE table_name=? "
                                      "AND reader_observed_at_micros IS NULL AND probe_marker IS NOT NULL", (table,)).fetchone()[0]
        query = ("SELECT sequence,probe_marker FROM transactions WHERE table_name=? "
                 "AND reader_observed_at_micros IS NULL AND probe_marker IS NOT NULL "
                 "AND sequence>? AND sequence<=? ORDER BY sequence LIMIT ?")
        rows = self.db.execute(query, (table, after, through, PAGE_ROWS)).fetchall()
        if not rows and cursor is not None:
            return self.pending(table)
        return [(row[0], row[1]) for row in rows], ((rows[-1][0], through) if rows else None)

    def all_committed_observed(self):
        self.flush()
        return self.db.execute("SELECT NOT EXISTS(SELECT 1 FROM transactions WHERE marker IS NOT NULL "
                               "AND probe_marker IS NOT NULL AND reader_observed_at_micros IS NULL)").fetchone()[0]

    def samples(self, kind):
        self.flush()
        for row in self.db.execute("SELECT record FROM samples WHERE kind=? ORDER BY id", (kind,)):
            yield json.loads(row[0])

    def import_logs(self, paths, source_id):
        self.flush()
        self.db.execute("DELETE FROM journal")
        self.db.execute("DELETE FROM publications")
        for path in paths:
            for number, line in enumerate(bounded_lines(path), 1):
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                fields = item.get("fields", item)
                if fields.get("event") == "transaction_journaled" and fields["source_id"] == source_id:
                    values = (fields["xid"], lsn_key(fields["end_lsn"]), fields["commit_timestamp_micros"],
                              fields["journaled_at_micros"])
                    try:
                        self.db.execute("INSERT INTO journal VALUES(?,?,?,?)", values)
                    except sqlite3.IntegrityError:
                        previous = self.db.execute("SELECT * FROM journal WHERE wire_xid=? AND end_lsn=?", values[:2]).fetchone()
                        if previous is None or tuple(previous) != values:
                            raise ValueError(f"conflicting repeated journal event at {path}:{number}") from None
                elif fields.get("event") == "table_published" and not fields["already_committed"]:
                    values = (fields["operation_id"], fields["table_id"], lsn_key(fields["first_lsn"]),
                              lsn_key(fields["last_lsn"]), fields["catalog_committed_at_micros"])
                    if values[2] > values[3]:
                        raise ValueError(f"reversed publication LSN interval at {path}:{number}")
                    try:
                        self.db.execute("INSERT INTO publications VALUES(?,?,?,?,?)", values)
                    except sqlite3.IntegrityError:
                        previous = self.db.execute("SELECT * FROM publications WHERE operation_id=?", values[:1]).fetchone()
                        if previous is None or tuple(previous) != values:
                            raise ValueError(f"conflicting repeated publication event at {path}:{number}") from None
                if number % PAGE_ROWS == 0:
                    self.db.commit()
        self.db.commit()
        # A wire XID alone is insufficient after wrap/reuse. Never guess which
        # source transaction a repeated wire XID belongs to.
        ambiguous = self.db.execute("""
            SELECT wire_xid FROM journal WHERE wire_xid IN
                (SELECT wire_xid FROM transactions WHERE wire_xid IS NOT NULL)
            GROUP BY wire_xid HAVING count(*)>1 LIMIT 1
        """).fetchone()
        reused = self.db.execute("SELECT wire_xid FROM transactions WHERE xid IS NOT NULL "
                                 "GROUP BY wire_xid HAVING count(*)>1 LIMIT 1").fetchone()
        if ambiguous or reused:
            raise ValueError("ambiguous wire XID reuse in benchmark evidence")
        previous = None
        for row in self.db.execute("SELECT table_id,first_lsn,last_lsn FROM publications ORDER BY table_id,first_lsn"):
            if previous is not None and previous[0] == row[0] and previous[2] >= row[1]:
                raise ValueError("overlapping successful publication intervals in benchmark evidence")
            previous = row

    def analyze(self, oids, offset):
        self.db.execute("DROP TABLE IF EXISTS temp.table_ids")
        self.db.execute("CREATE TEMP TABLE table_ids(name TEXT PRIMARY KEY,oid INTEGER)")
        self.db.executemany("INSERT INTO table_ids VALUES(?,?)", oids.items())
        self.db.executescript("""
            DROP TABLE IF EXISTS analysis;
            CREATE TABLE analysis AS SELECT t.sequence,j.commit_at_micros,j.journaled_at_micros,j.end_lsn,
                p.catalog_committed_at_micros
            FROM transactions t LEFT JOIN journal j ON t.wire_xid=j.wire_xid
            LEFT JOIN table_ids i ON t.table_name=i.name
            LEFT JOIN publications p ON p.operation_id=(
                SELECT operation_id FROM publications WHERE table_id=i.oid AND last_lsn>=j.end_lsn
                ORDER BY last_lsn LIMIT 1) AND p.first_lsn<=j.end_lsn;
            CREATE UNIQUE INDEX analysis_sequence ON analysis(sequence);
        """)
        # Readiness gates still use only the measured schedule cohort. Keep all
        # signed differences, including the raw clock-domain measurement.
        expressions = {
            "commit_to_journal": "(a.journaled_at_micros-a.commit_at_micros+?) / 1000.0",
            "commit_to_journal_raw": "(a.journaled_at_micros-a.commit_at_micros) / 1000.0",
            "journal_to_catalog": "(a.catalog_committed_at_micros-a.journaled_at_micros) / 1000.0",
            "commit_to_catalog": "(a.catalog_committed_at_micros-a.commit_at_micros+?) / 1000.0",
            "catalog_to_reader": "(t.reader_observed_at_micros-a.catalog_committed_at_micros) / 1000.0",
            "commit_to_reader": "CASE WHEN a.catalog_committed_at_micros IS NOT NULL THEN "
                                "(t.reader_observed_at_micros-a.commit_at_micros+?) / 1000.0 END",
            "scheduled_to_reader": "CASE WHEN a.catalog_committed_at_micros IS NOT NULL THEN "
                                   "(t.reader_observed_at_micros-t.scheduled_at_micros) / 1000.0 END",
            "client_commit_response": "(t.commit_response_at_micros-t.started_at_micros) / 1000.0",
        }
        self.db.execute("DROP TABLE IF EXISTS latencies")
        self.db.execute("CREATE TABLE latencies AS SELECT t.sequence," + ",".join(
            expression + " AS " + name for name, expression in expressions.items()) +
            " FROM transactions t JOIN analysis a USING(sequence) WHERE t.measured=1 "
            "AND t.commit_response_at_micros IS NOT NULL AND t.missed_arrival IS NULL", (offset, offset, offset))
        self.db.commit()

    def distribution(self, name, requested_count=None):
        if name not in LATENCIES:
            raise ValueError(name)
        count = self.db.execute(f"SELECT count({name}) FROM latencies").fetchone()[0]
        if requested_count is not None:
            ranks = {math.ceil(requested_count * p / 100): f"p{p}_ms" for p in (50, 95, 99)}
            result = {f"p{p}_ms": "failed_or_unobserved" for p in (50, 95, 99)}
        elif not count:
            return {"count": 0}
        else:
            ranks = {math.ceil(count * p / 100): f"p{p}_ms" for p in (50, 95, 99)}
            result = {"count": count}
        # One external sort per metric, consumed in bounded SQLite pages.
        found = {}
        last = None
        for rank, row in enumerate(self.db.execute(
                f"SELECT {name} FROM latencies WHERE {name} IS NOT NULL ORDER BY {name}"), 1):
            last = row[0]
            if rank in ranks:
                found[rank] = last
        denominator = count if requested_count is None else requested_count
        for p in (50, 95, 99):
            rank = math.ceil(denominator * p / 100)
            if rank in found:
                result[f"p{p}_ms"] = found[rank]
        if requested_count is None:
            result["max_ms"] = last
        return result

    def writer_timing_summary(self):
        """Measured arrival-cohort timings plus bounded-recorder pressure."""
        self.flush()
        timings = {}
        for name in WRITER_TIMINGS:
            count, total = self.db.execute(
                f"SELECT count({name}),coalesce(sum({name}),0) FROM transactions "
                f"WHERE measured=1 AND {name} IS NOT NULL"
            ).fetchone()
            if not count:
                timings[name] = {"count": 0}
                continue
            ranks = {math.ceil(count * p / 100) for p in (50, 95, 99)}
            result = {"count": count, "total": total}
            found = {}
            last = None
            for rank, row in enumerate(self.db.execute(
                    f"SELECT {name} FROM transactions WHERE measured=1 AND {name} IS NOT NULL ORDER BY {name}"), 1):
                last = row[0]
                if rank in ranks:
                    found[rank] = last
            for percentile in (50, 95, 99):
                result[f"p{percentile}"] = found[math.ceil(count * percentile / 100)]
            result["max"] = last
            timings[name] = result
        terminals = [json.loads(row[0]) for row in self.db.execute(
            "SELECT record FROM samples WHERE kind='writer_terminal' ORDER BY id"
        )]
        terminal_values = sorted(item["postcommit_keys_micros"] for item in terminals if item["measured"])
        terminal = {"count": len(terminal_values), "total": sum(terminal_values)}
        if terminal_values:
            for percentile in (50, 95, 99):
                terminal[f"p{percentile}"] = terminal_values[math.ceil(len(terminal_values) * percentile / 100) - 1]
            terminal["max"] = terminal_values[-1]
        return {"measured_arrival_cohort_micros": timings,
                "terminal_postcommit_keys_micros": terminal,
                "recorder_queue": self.queue_summary()}

    def queue_summary(self):
        with self.queue.mutex:
            by_kind = dict(self.queue.enqueue_by_kind)
            result = {
                "capacity_items": self.queue.maxsize,
                "high_water_items": self.queue.high_water_mark,
                "enqueue_count": self.queue.enqueue_count,
                "enqueue_wait_micros": {
                    "total": self.queue.enqueue_wait_ns / 1000,
                    "max": self.queue.max_enqueue_wait_ns / 1000,
                },
            }
        result["by_kind"] = {
            kind: {"count": count, "total_wait_micros": total / 1000, "max_wait_micros": maximum / 1000}
            for kind, (count, total, maximum) in sorted(by_kind.items())
        }
        return result

    def transactions(self, enriched=False):
        query = ("SELECT t.*,a.commit_at_micros,a.journaled_at_micros,a.end_lsn,a.catalog_committed_at_micros "
                 "FROM transactions t LEFT JOIN analysis a USING(sequence) ORDER BY sequence") if enriched else (
                    "SELECT * FROM transactions ORDER BY sequence")
        for row in self.db.execute(query):
            record = {name: value for name, value in dict(row).items()
                      if name not in ("wire_xid", "probe_marker", "table_name") and value is not None}
            record["table"] = row["table_name"]
            record["measured"] = bool(record["measured"])
            if "missed_arrival" in record:
                record["missed_arrival"] = bool(record["missed_arrival"])
            if "marker" in record:
                record["reader_observed_at_micros"] = row["reader_observed_at_micros"]
            if record.get("end_lsn") is not None:
                record["end_lsn"] = lsn_text(record["end_lsn"])
            yield record

    def export(self, directory, enriched=False):
        self.flush()
        write_array(directory / "transactions.json", self.transactions(enriched))
        for kind, path in (("resource", "resources.json"), ("reader", "reader-samples.json")):
            write_array(directory / path, self.samples(kind))

    def close(self):
        if not self.closed:
            try:
                self._put("close", None)
            finally:
                # A failed recorder may still be unwinding its own connection.
                self.thread.join()
                self.closed = True
                self.db.close()
            self.check()

def mutation_counts(profile, rows):
    updates = rows * {"mixed": 7, "update90": 9, "append": 0}[profile] // 10
    deletes = rows // 10 if profile == "mixed" else 0
    return updates, deletes, rows - updates - deletes  # includes one immutable probe


def clock_assessment(before, after, max_uncertainty_us):
    change = after["postgres_minus_client_us"] - before["postgres_minus_client_us"]
    # The average correction can err by half the observed drift plus the worst
    # endpoint's RTT/2. This bounds endpoint uncertainty, not arbitrary unseen
    # excursions between samples, which remain an explicit protocol assumption.
    uncertainty = abs(change) / 2 + max(before["roundtrip_us"], after["roundtrip_us"]) / 2
    valid = (all(math.isfinite(v) for sample in (before, after) for v in
                 (sample["postgres_minus_client_us"], sample["roundtrip_us"]))
             and min(before["roundtrip_us"], after["roundtrip_us"]) >= 0
             and uncertainty <= max_uncertainty_us)
    return {"correction_us": (before["postgres_minus_client_us"] + after["postgres_minus_client_us"]) / 2,
            "offset_change_us": change, "uncertainty_us": uncertainty,
            "max_uncertainty_us": max_uncertainty_us, "latency_qualification_available": valid}


def catalog_slo_met(percentiles, clock, p95_ms, p99_ms):
    return clock["latency_qualification_available"] and all(
        percentiles.get(name + "_ms", math.inf) + clock["uncertainty_us"] / 1000 < limit
        for name, limit in (("p95", p95_ms), ("p99", p99_ms)))
