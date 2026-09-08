#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]==3.3.5"]
# ///
"""Measure FULL versus DEFAULT generated WAL on an isolated disposable source."""

import argparse
import hashlib
import json
from pathlib import Path
import random
import signal
import statistics
import subprocess
import sys
import time
import traceback
import uuid

import psycopg
from psycopg import sql
from process_lifecycle import run_supervised, write_report
from container_cleanup import cleanup_container


IMAGE = "postgres:18.6-bookworm"
SEED_ROWS = 1024
TRANSACTIONS = 20
PROFILES = {"ordinary-256": 256, "unchanged-toast-16k": 16384}
PHASES = ("after-checkpoint", "without-new-checkpoint")
SETTINGS = {
    "wal_level": "logical", "fsync": "on", "synchronous_commit": "on",
    "full_page_writes": "on", "wal_compression": "off", "autovacuum": "off",
    "checkpoint_timeout": "1h", "max_wal_size": "4GB", "shared_buffers": "128MB",
    "log_checkpoints": "on", "track_counts": "on",
}
REFERENCES = {
    "wal_and_checkpoint_policy": "https://www.postgresql.org/docs/18/runtime-config-wal.html",
    "counters_and_async_statistics": "https://www.postgresql.org/docs/18/monitoring-stats.html",
    "lsn_positions": "https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-BACKUP",
    "checkpoint_control_data": "https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-CONTROLDATA",
}


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def payload(token, width):
    # Deterministic, poorly compressible hexadecimal text, not random binary data.
    return hashlib.shake_256(token.encode()).hexdigest(width // 2)


def operations(rows, phase):
    """Ordered 70 update / 20 insert / 10 delete transactions; ten updates move PKs."""
    rng = random.Random(20260904 + phase)
    next_id = max(rows) + 1
    for transaction in range(TRANSACTIONS):
        chosen = rng.sample(sorted(rows), 80)
        batch = [("update", key, f"{phase}:{transaction}:{key}") for key in chosen[:60]]
        batch += [("move", key, next_id + offset) for offset, key in enumerate(chosen[60:70])]
        next_id += 10
        batch += [("insert", next_id + offset, f"insert:{next_id + offset}") for offset in range(20)]
        next_id += 20
        batch += [("delete", key, None) for key in chosen[70:]]
        yield batch


def apply_model(rows, batch, width):
    for operation, key, value in batch:
        if operation == "insert":
            if key in rows:
                raise AssertionError("duplicate model insert")
            rows[key] = (0, payload(value, width))
        elif operation == "delete":
            del rows[key]
        else:
            version, previous = rows[key]
            if operation == "move":
                if value in rows:
                    raise AssertionError("duplicate model key move")
                del rows[key]
                rows[value] = (version + 1, previous)
            else:
                rows[key] = (version + 1, previous if width > 256 else payload(value, width))


def execute_batch(pg, batch, width):
    for operation, key, value in batch:
        if operation == "update" and width > 256:
            cursor = pg.execute("UPDATE events SET version=version+1 WHERE id=%s", (key,))
        elif operation == "update":
            cursor = pg.execute("UPDATE events SET version=version+1,payload=%s WHERE id=%s",
                                (payload(value, width), key))
        elif operation == "move":
            cursor = pg.execute("UPDATE events SET id=%s,version=version+1 WHERE id=%s", (value, key))
        elif operation == "insert":
            cursor = pg.execute("INSERT INTO events VALUES (%s,0,%s)", (key, payload(value, width)))
        else:
            cursor = pg.execute("DELETE FROM events WHERE id=%s", (key,))
        if cursor.rowcount != 1:
            raise AssertionError(f"{operation} affected {cursor.rowcount} rows for key {key}")


def snapshot(pg):
    return pg.execute("""
        SELECT jsonb_build_object(
          'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
          'checkpointer', (SELECT to_jsonb(c) FROM pg_stat_checkpointer c),
          'checkpoint', (SELECT to_jsonb(c) FROM pg_control_checkpoint() c),
          'lsn', jsonb_build_object('insert',pg_current_wal_insert_lsn(),
                    'write',pg_current_wal_lsn(),'flush',pg_current_wal_flush_lsn()),
          'table', (SELECT jsonb_build_object('insert',n_tup_ins,'update',n_tup_upd,
                    'delete',n_tup_del,'hot_update',n_tup_hot_upd)
                    FROM pg_stat_user_tables WHERE relid='events'::regclass),
          'other_clients', (SELECT coalesce(jsonb_agg(pid),'[]'::jsonb) FROM pg_stat_activity
                    WHERE backend_type='client backend' AND pid<>pg_backend_pid()))
    """).fetchone()[0]


def settled_snapshot(pg, expected_counts, evidence):
    # No resets or undocumented force-flush functions. Writers have closed; wait
    # for exact table counters and an unchanged WAL/checkpoint snapshot over >1s.
    deadline, unchanged_since, previous = time.monotonic() + 15, None, None
    while time.monotonic() < deadline:
        current = snapshot(pg)
        evidence.append({"at_ns": time.time_ns(), **current})
        if current["other_clients"]:
            raise AssertionError("unexpected client in isolated source cluster")
        signature = {key: current[key] for key in ("wal", "checkpointer", "lsn", "table")}
        signature["checkpoint_lsn"] = current["checkpoint"]["checkpoint_lsn"]
        counts_match = all(current["table"][key] == value for key, value in expected_counts.items())
        if counts_match and signature == previous:
            if unchanged_since is not None and time.monotonic() - unchanged_since >= 1.1:
                return current
        else:
            unchanged_since = time.monotonic() if counts_match else None
        previous = signature
        time.sleep(0.2)
    raise TimeoutError("source statistics did not reach exact counts and a stable WAL boundary")


def verify_rows(pg, expected):
    digest = hashlib.sha256()
    with pg.cursor() as cursor:
        cursor.execute("SELECT id,version,payload FROM events ORDER BY id")
        for key, (version, text) in sorted(expected.items()):
            actual = cursor.fetchone()
            if actual != (key, version, text):
                raise AssertionError(f"row mismatch at id={key}; actual={actual!r}")
            digest.update(json.dumps(actual, separators=(",", ":")).encode() + b"\n")
        if cursor.fetchone() is not None:
            raise AssertionError("unexpected extra source row")
    toast = pg.execute("SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='events'::regclass").fetchone()[0]
    chunks, stored_bytes = pg.execute(sql.SQL("SELECT count(*),coalesce(sum(octet_length(chunk_data)),0) FROM {}")
                                     .format(sql.Identifier(*toast.split(".")))).fetchone()
    return {"rows": len(expected), "sha256": digest.hexdigest(),
            "toast_chunks": chunks, "live_toast_chunk_bytes": int(stored_bytes)}


def check_checkpoint(before, after):
    if (before["checkpoint"]["checkpoint_lsn"] != after["checkpoint"]["checkpoint_lsn"]
            or any(before["checkpointer"][key] != after["checkpointer"][key]
                   for key in ("num_timed", "num_requested", "num_done", "stats_reset"))):
        raise AssertionError("intervening checkpoint invalidated matched phase")


def delta(pg, before, after, mutations):
    if before["wal"]["stats_reset"] != after["wal"]["stats_reset"]:
        raise AssertionError("WAL statistics reset during measurement")
    check_checkpoint(before, after)
    counters = {key: after["wal"][key] - before["wal"][key]
                for key in ("wal_bytes", "wal_records", "wal_fpi", "wal_buffers_full")}
    distances = {key: int(pg.execute("SELECT pg_wal_lsn_diff(%s::pg_lsn,%s::pg_lsn)",
                                     (after["lsn"][key], before["lsn"][key])).fetchone()[0])
                 for key in ("insert", "write", "flush")}
    if counters["wal_bytes"] <= 0 or any(value < 0 for value in (*counters.values(), *distances.values())):
        raise AssertionError("invalid WAL counter or LSN delta")
    return {"generated_wal": counters, "lsn_distance_bytes": distances,
            "generated_wal_bytes_per_mutation": counters["wal_bytes"] / mutations}


def run_case(dsn, record, path, width):
    rows = {key: (0, payload(f"seed:{key}", width)) for key in range(1, SEED_ROWS + 1)}
    counts = {"insert": SEED_ROWS, "update": 0, "delete": 0}
    with psycopg.connect(dsn, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS events")
        setup.execute("CREATE TABLE events(id bigint PRIMARY KEY,version integer NOT NULL,payload text NOT NULL)")
        setup.execute(sql.SQL("ALTER TABLE events REPLICA IDENTITY {}")
                      .format(sql.SQL(record["identity"])))
        with setup.cursor().copy("COPY events FROM STDIN") as copy:
            for key, (version, text) in rows.items():
                copy.write_row((key, version, text))
        record["seed"] = verify_rows(setup, rows)
        if width > 256 and not record["seed"]["toast_chunks"]:
            raise AssertionError("wide fixture did not produce external TOAST values")
        setup.execute("CHECKPOINT")
    with psycopg.connect(dsn, autocommit=True) as observer:
        observer.execute("SET stats_fetch_consistency=none")
        observer.execute("SET default_transaction_read_only=on")
        for phase_number, name in enumerate(PHASES):
            phase = {"name": name, "passed": False, "committed_transactions": 0,
                     "committed_mutations": 0, "before_samples": [], "after_samples": []}
            record["phases"].append(phase)
            phase["before"] = settled_snapshot(observer, counts, phase["before_samples"])
            if phase_number:
                check_checkpoint(record["phases"][phase_number - 1]["after"], phase["before"])
            plan_hash = hashlib.sha256()
            started = time.monotonic()
            try:
                with (path.parent / f"{path.stem}-{name}-operations.jsonl").open("x") as plan:
                    with psycopg.connect(dsn, autocommit=True) as writer:
                        for batch in operations(rows, phase_number):
                            encoded = json.dumps(batch, separators=(",", ":")) + "\n"
                            plan.write(encoded)
                            plan.flush()
                            plan_hash.update(encoded.encode())
                            with writer.transaction():
                                execute_batch(writer, batch, width)
                            phase["committed_transactions"] += 1
                            phase["committed_mutations"] += len(batch)
                            apply_model(rows, batch, width)
                            counts = {key: counts[key] + increment
                                      for key, increment in (("insert", 20), ("update", 70), ("delete", 10))}
                phase["workload_seconds"] = time.monotonic() - started
                phase["operations_sha256"] = plan_hash.hexdigest()
                phase["after"] = settled_snapshot(observer, counts, phase["after_samples"])
                phase.update(delta(observer, phase["before"], phase["after"], phase["committed_mutations"]))
                phase["verification"] = verify_rows(observer, rows)
                phase["committed_by_kind"] = {"insert": TRANSACTIONS * 20, "update": TRANSACTIONS * 70,
                                              "delete": TRANSACTIONS * 10, "key_moves_within_updates": TRANSACTIONS * 10}
                if phase["committed_transactions"] != TRANSACTIONS or phase["committed_mutations"] != TRANSACTIONS * 100:
                    raise AssertionError("incomplete mutation workload")
                phase["passed"] = True
            finally:
                save(path, record)
    record["passed"] = True


def comparisons(cases):
    result = {}
    for profile in PROFILES:
        for name in PHASES:
            pairs = []
            for repetition in range(3):
                pair = {case["identity"]: case for case in cases
                        if case["profile"] == profile and case["repetition"] == repetition}
                if len(pair) != 2 or not all(case["passed"] for case in pair.values()):
                    raise AssertionError("cannot summarize an incomplete pair")
                phases = {identity: next(p for p in case["phases"] if p["name"] == name)
                          for identity, case in pair.items()}
                default, full = phases["DEFAULT"], phases["FULL"]
                for key in ("operations_sha256", "committed_transactions", "committed_mutations"):
                    if default[key] != full[key]:
                        raise AssertionError(f"unmatched {profile}/{name}/{repetition}: {key}")
                if any(default["verification"][key] != full["verification"][key] for key in ("rows", "sha256")):
                    raise AssertionError(f"unmatched final rows for {profile}/{name}/{repetition}")
                a, b = (phases[identity]["generated_wal"]["wal_bytes"] for identity in ("DEFAULT", "FULL"))
                pairs.append({"repetition": repetition, "default_bytes": a, "full_bytes": b,
                              "extra_bytes": b - a, "full_over_default": b / a})
            ratios = [pair["full_over_default"] for pair in pairs]
            result[f"{profile}/{name}"] = {"pairs": pairs, "median_full_over_default": statistics.median(ratios),
                                          "ratio_range": [min(ratios), max(ratios)]}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--container-name", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.supervised_worker:
        if args.artifacts.exists():
            raise FileExistsError(args.artifacts)
        name = "flow-wal-" + uuid.uuid4().hex[:12]
        result = None
        try:
            result = run_supervised([sys.executable, __file__, "--artifacts", str(args.artifacts),
                                     "--supervised-worker", "--container-name", name], args.artifacts, args.timeout)
        finally:
            # Container ownership stays with the deadline owner, so even killing
            # a worker blocked inside libpq cannot bypass fixture removal.
            cleanup = cleanup_container(name)
            cleanup_failed = not cleanup["passed"]
            args.artifacts.mkdir(parents=True, exist_ok=True)
            path = args.artifacts / "report.json"
            report = json.loads(path.read_text()) if path.exists() else {"passed": False}
            report["cleanup"] = cleanup
            if cleanup_failed:
                report["passed"] = False
            write_report(path, report)
        return result.returncode or int(cleanup_failed)
    args.artifacts.mkdir(parents=True, exist_ok=False)
    name = args.container_name
    password = uuid.uuid4().hex
    report = {"passed": False, "container": name, "cases": [], "references": REFERENCES,
              "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "command": sys.argv, "postgres_image_tag": IMAGE, "psycopg": psycopg.__version__,
              "scope": "Isolated PostgreSQL generated WAL only; no daemon, catalog, reader or replication slot.",
              "policy": "Fresh table per case; explicit checkpoint before phase one, none before phase two. "
                        "FPI remains enabled and actual FPI counts are retained; phase two is not assumed FPI-free. "
                        "Setup, checkpoint, validation and statistics-settling time are excluded from workload duration.",
              "limits": "Finite serial source workload, not daemon throughput, wire volume, historical workload "
                        "overhead, steady-state vacuum cost or an end-to-end qualification.",
              "retained_wal_bytes": None, "journal_payload_bytes": None}
    container_id = None
    try:
        command = ["docker", "create", "--name", name, "--cpus=2", "--memory=1g",
                   "--publish", "127.0.0.1:0:5432", "--volume", "/var/lib/postgresql",
                   "--env", "POSTGRES_USER=flow", "--env", f"POSTGRES_PASSWORD={password}",
                   "--env", "POSTGRES_DB=flow", IMAGE, "postgres"]
        for key, value in SETTINGS.items():
            command += ["-c", f"{key}={value}"]
        container_id = subprocess.check_output(command, text=True, timeout=60).strip()
        subprocess.run(["docker", "start", container_id], check=True, capture_output=True, timeout=30)
        inspection = json.loads(subprocess.check_output(["docker", "inspect", container_id], timeout=30))[0]
        report["image_id"] = inspection["Image"]
        port = inspection["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
        report["host_port"] = int(port)
        dsn = f"postgres://flow:{password}@127.0.0.1:{port}/flow?connect_timeout=2&options=-c%20statement_timeout%3D30000"
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(dsn, autocommit=True) as pg:
                    report["version"] = pg.execute("SELECT version()").fetchone()[0]
                    if pg.info.server_version != 180006:
                        raise AssertionError("fixture requires PostgreSQL 18.6")
                    report["settings"] = pg.execute("SELECT name,setting,unit FROM pg_settings WHERE name=ANY(%s) ORDER BY name",
                        (list(SETTINGS) + ["data_checksums", "wal_log_hints", "block_size", "wal_segment_size", "default_toast_compression"],)).fetchall()
                    report["control_init"] = pg.execute("SELECT to_jsonb(c) FROM pg_control_init() c").fetchone()[0]
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("isolated PostgreSQL did not become ready")
                time.sleep(0.25)
        for profile, width in PROFILES.items():
            for repetition in range(3):
                for identity in (("DEFAULT", "FULL") if repetition % 2 == 0 else ("FULL", "DEFAULT")):
                    record = {"profile": profile, "repetition": repetition, "identity": identity,
                              "payload_bytes": width, "phases": [], "passed": False}
                    report["cases"].append(record)
                    path = args.artifacts / f"{profile}-{repetition}-{identity.lower()}.json"
                    try:
                        run_case(dsn, record, path, width)
                    except BaseException as error:
                        record.update(error=f"{type(error).__name__}: {error}", traceback=traceback.format_exc())
                        raise
                    finally:
                        save(path, record)
                        save(args.artifacts / "report.json", report)
        report["comparisons"] = comparisons(report["cases"])
        report["passed"] = True
    except BaseException as error:
        report.update(error=f"{type(error).__name__}: {error}", traceback=traceback.format_exc())
    finally:
        if container_id:
            try:
                if not report["passed"]:
                    with (args.artifacts / "failed-source.sql").open("wb") as source:
                        dumped = subprocess.run(["docker", "exec", container_id, "pg_dump", "-U", "flow",
                                                 "-d", "flow", "--no-owner", "--no-privileges", "-t", "public.events"],
                                                stdout=source, stderr=subprocess.PIPE, timeout=30)
                    report["failure_dump"] = {"returncode": dumped.returncode,
                                              "stderr": dumped.stderr.decode(errors="replace")}
                logs = subprocess.run(["docker", "logs", container_id], capture_output=True, timeout=30)
                (args.artifacts / "postgres.log").write_bytes(logs.stdout + logs.stderr)
            except Exception as error:
                report["collection_error"] = f"{type(error).__name__}: {error}"
                report["passed"] = False
        save(args.artifacts / "report.json", report)
    print(f"{'PASS' if report['passed'] else 'FAIL'}: {args.artifacts / 'report.json'}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(main())
