# Service failure and recovery suite

`faults.py` runs the real ingestion daemon and optionally the separate compactor against one explicitly selected, disposable Compose project. The project must already be running, use fixed loopback ports, and preserve PostgreSQL, catalog, and object-store state across container restarts. It must not contain production or shared development services. The script only accepts project names beginning with `flow-` and never creates, removes, or resets volumes.

From the repository root, after starting the fixture and exporting its credentials and PostgreSQL URL:

```sh
uv run tests/production/faults.py \
  --postgres-url "$FLOW_POSTGRES_URL" \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --compose-project flow-production-local \
  --compose-env-file target/production-local/ports.env \
  --binary target/release/embrasure-flow \
  --compactor-binary target/release/examples/local_compactor \
  --artifacts target/production-faults-01
```

Use the actual selected ports if those defaults were occupied. `--compose-file` defaults to `tests/production/compose.yaml`. `--fault-seconds` defaults to five seconds. The artifact directory must be new. Do not run the benchmark concurrently: this suite deliberately stops services within its named project.

The workload starts with 4,096 typed orders and 16 accounts, verifies the initial COPY/WAL handoff, then exercises:

1. A lost catalog response **after the upstream catalog successfully committed an actual ingestion snapshot**. A loopback HTTP proxy closes the downstream socket without sending response headers. Its trace records the operation ID, snapshot ID, and successful upstream status. Recovery must find exactly one matching logical commit and produce the exact source rows.
   The same commit is then reported as 500, 502 and 504 (commit state unknown), and once its real response is held for `--late-response-seconds` (default 65), beyond the daemon's catalog request timeout. Each variant must also produce exactly one matching logical commit.
2. Catalog HTTP 503 responses before requests reach the upstream catalog. PostgreSQL continues accepting updates, deletes, reinserts, and a transaction spanning both tables. The source ACK must remain behind that unmaterialized transaction.
3. A full catalog container stop/start, preserving table UUIDs and catalog metadata.
4. A full MinIO container stop/start, preserving objects and the same endpoint.
5. A PostgreSQL container restart, preserving its system identifier, logical slot, WAL, and configured endpoint.
6. A `SIGKILL` after a 4,000-row transaction is durable in the local journal while its catalog requests are held. After joining the killed process, the fixture releases the requests and requires a successful upstream commit matching a Prepared operation from this phase, before starting a replacement. Restart must recover that outcome and replay remaining work without duplicate rows or unsafe ACK advancement.
7. The same successful-commit crash boundary followed by removal of the replaceable physical index. The durable control store remains intact. Startup must rebuild the index and correctly apply further updates, deletes and primary-key changes.
8. High-entropy 16 KiB text and 8 KiB binary values, including an update that leaves both TOAST columns unchanged, subsequent changes to the large values, primary-key moves, and deletes. Unsupported unchanged-TOAST handling is a failed functionality test, even if capture safely stops.

Every recovery checkpoint reads current Iceberg metadata directly from the catalog and compares every column of every row using stock DuckDB against PostgreSQL. The final audit also checks retained history, unique logical operation IDs, manifest sequence numbers, and sorted positional deletes. The normal daemon compactor role remains enabled. With `--compactor-binary`, an independent compactor also runs during the outage and row-change phases. The two consecutive forced-success crash phases share one graceful stop and join of that process. Each phase first synchronizes both tables; the compactor restarts after both recoveries succeed. This prevents an unrelated physical rewrite from legitimately invalidating the catalog request whose success the scenario specifically requires.

The script acts as an explicit process supervisor after service restoration. Every daemon or external-compactor exit becomes an `availability_incidents` entry with its exit code and log path. A recovered dataset can pass correctness while `availability_without_supervisor_restart` is false. The report never treats a restart as uninterrupted availability. The daemon is restarted at most once per recovery phase, preventing repeated failure loops from masquerading as successful recovery.

The fixture's JDBC catalog shares the PostgreSQL server. After a PostgreSQL restart, readiness probes load both actual tables because the catalog's configuration endpoint does not exercise its database pool. If table reads still fail after five seconds, the suite explicitly restarts the catalog and records that infrastructure intervention as another availability incident. Neither source nor catalog data is reset.

Artifacts include `report.json`, daemon and compactor logs, a JSONL catalog-proxy request trace, Compose actions, catalog metadata copies, and durable daemon state. Service restoration is attempted in `finally` blocks. Containers and volumes remain available for inspection after the script exits. `proxy.py` exposes no network fault-control API; its control state is available only inside the local test process.

`--format-version 3` runs the same phases against Iceberg v3 targets (deletion vectors; the manifest audit checks one live vector per data file). `--ack-mode journaled` declares independent journal storage and replaces the "ACK behind the barrier" checks with "ACK never past the durable journal"; the recovery and exactly-once checks are unchanged. CI runs both variants on PostgreSQL 18 (journaled also on 14).

## Randomized crash loop

`crash_loop.py` takes the same connection arguments (no Compose project; it never restarts services). Three writers share key ranges and mix updates, upserts, deletes and primary-key moves, with savepoint rollbacks and whole-transaction rollbacks. A bulk writer periodically commits a 12,000-row transaction that exceeds a 4 MB `logical_decoding_work_mem` (PostgreSQL must report it streamed) and contains its own rolled-back savepoint. Nullable column additions land on a FULL-identity and a DEFAULT-identity table at random times. Every cycle the daemon is `SIGKILL`ed after 0.5–10 seconds under a random catalog fault: none, commits held before upstream (released at a random time or after the kill), 503 before upstream, a dropped response after a successful commit, or a successful commit reported as 500/502/504.

After each kill, the harness compares the slot's `confirmed_flush_lsn` with the catalog: every writer transaction advances a per-writer `ticks` row and records WAL positions immediately before and after `COMMIT`, so a transaction that is certainly acknowledged but certainly outside the ticks table's highest published `streaming.last-lsn` fails the run. After each restart, writers pause at a transaction boundary; every column of every row of all four tables must match PostgreSQL once materialization passes a new barrier, and each `flow.operation-id` must map to exactly one snapshot for the whole run. The final phase audits recent manifests.

```sh
uv run tests/production/crash_loop.py --duration 240 \
  --catalog-uri http://127.0.0.1:58181 --s3-endpoint http://127.0.0.1:59000 \
  --binary target/debug/embrasure-flow --artifacts target/crash-loop-01
```

The seed is printed first and recorded with a reproduction command in `report.json`; pass `--seed` to replay the same fault, timing and workload decisions (thread scheduling still varies). CI runs a four-minute loop on every change. `.github/workflows/nightly.yml` runs a one-hour loop (`--duration 3600`) and `soak.py`, which runs the same workload for 30 minutes without faults, samples daemon RSS, state directory and journal bytes and table metadata size, and fails when a resource's medians grow across three post-warm-up windows beyond a tolerance. The soak shortens snapshot retention and orphan grace to two minutes so that metadata and artifact bookkeeping can reach steady state.

## Fail-closed storage-loss guards

`live_storage_loss_guards_fail_closed` (in the daemon's test binary, `--ignored`, `FLOW_POSTGRES_URL` of a disposable superuser session, `--test-threads=1`) runs the real daemon against live PostgreSQL and an in-memory catalog. Each case publishes a change, damages state while the daemon is stopped, and requires startup to refuse with the documented message, publish no snapshot and leave the slot unchanged: restoring an older copy of `state_dir`; dropping and recreating the slot; advancing it past the journal; truncating the journal below a journaled but unpublished transaction; and invalidating the slot by lowering `max_slot_wal_keep_size` and removing its WAL (PostgreSQL 17+ also reports `invalidation_reason = wal_removed`). The last case changes a server setting, so `init.sql` configures `max_slot_wal_keep_size` with `ALTER SYSTEM` rather than on the command line. CI runs it on PostgreSQL 14–18.

These are bounded local correctness and recovery tests. They do not prove multi-node failover, independent-host journal durability, network partitions with real object-store semantics, complete disk-loss recovery, or long-duration availability.

## Schema and lifecycle contracts

`contracts.py` uses the same running services without restarting their containers:

```sh
uv run tests/production/contracts.py \
  --postgres-url "$FLOW_POSTGRES_URL" \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/debug/embrasure-flow \
  --artifacts target/production-contracts-01
```

It verifies idle nullable additions with an actual lost successful catalog
response, queued historical schemas across `SIGKILL`, streamed aborted DDL,
old/new row shapes in one transaction with savepoint rollback, schema history
replayed after downtime, checkpoint retention and index-loss recovery after DDL,
graceful `SIGTERM`, and primary-key drift rejection. Every successful recovery
compares all rows and columns with PostgreSQL using stock DuckDB, including
decimal and timezone-aware timestamp values.

Run three further isolated lineages with `--unsafe-case column-incarnation`,
`--unsafe-case non-null-default`, and `--unsafe-case volatile-default`, each with
a new artifact directory. They prove that dropped/re-added columns, hidden
constant-default backfills, and hidden volatile-default rewrites stop ingestion
without changing published schemas or rows. Each run retains its logs and
durable state for inspection. Do not run these contracts concurrently with
service outages or performance measurements.

`source_versions.py` runs the contracts and interrupted-bootstrap DDL suite
sequentially on PostgreSQL 14.24, 15.19, 16.15 and 17.11. It reuses the selected
REST/S3 fixture, allocates a fresh source container and loopback port for each
version, and removes only those generated containers and their volumes.

```sh
python3 tests/production/source_versions.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/release/embrasure-flow \
  --artifacts target/production-source-versions-01
```

Use a stable binary during this run. The report records its SHA-256, container
image identities, selected ports and suite reports, including failed versions.
The contract suite checks that idle publications release unrelated WAL through
a durably completed heartbeat. Its idle-schema check permits ACK advancement
only when it matches the DDL transaction or a real empty transaction's durable
journal terminal; metadata changes cannot invent source progress.
