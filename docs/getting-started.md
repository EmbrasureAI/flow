# Getting started

For a complete local stack, follow the [Docker demo](../demo/README.md).
The steps below run the native binary against an existing PostgreSQL database,
Iceberg REST catalog and S3-compatible object store.

## Build

Use the pinned Rust toolchain and install a C++ compiler, libclang, CMake,
pkg-config and OpenSSL development headers. On macOS, Xcode Command Line Tools
supply the compiler and libclang.

```sh
cargo build --locked --release -p flow-daemon
cargo run --locked -p flow-daemon -- --config examples/flow.toml check
```

## Prepare PostgreSQL

Flow supports PostgreSQL 14–18. Enable `wal_level = logical` and reserve enough
`max_replication_slots` and `max_wal_senders` for Flow alongside existing
replication. Each Flow source uses a permanent slot and may need one additional
temporary slot during recovery. Server setting changes may require a restart;
see [PostgreSQL's configuration guide](https://www.postgresql.org/docs/18/logical-replication-config.html).

Use a login with `REPLICATION`, database `CONNECT`, schema `USAGE` and `SELECT`
on each configured table. Configure database access in `pg_hba.conf` or the
provider's equivalent. The login must also be able to execute the text overload
of `pg_catalog.pg_logical_emit_message` for idle-source heartbeats. On PostgreSQL
17–18 that function has arguments `(boolean, text, text, boolean)`; on 14–16 it
has `(boolean, text, text)`. Managed services may expose replication privileges
through provider-specific roles. See [PostgreSQL's privilege requirements](https://www.postgresql.org/docs/18/logical-replication-security.html).

As the table owner or administrator, prepare the existing example table before
running `init`:

```sql
-- public.orders must have the stable primary key described in flow.toml.
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION embrasure_flow FOR TABLE public.orders
  WITH (publish = 'insert, update, delete, truncate');
```

Include every configured source table in that publication. It may also contain
other tables, so an administrator-owned `FOR ALL TABLES` or `FOR TABLES IN
SCHEMA` publication works; Flow ignores changes to tables it does not capture,
including their row filters and column lists. Flow validates the publication;
it does not create or alter it. For PostgreSQL 18 stored generated
columns, also set `publish_generated_columns = stored`; see the
[type guide](postgres-types.md#generated-columns). Initial COPY requires full
row visibility: a role subject to row-level security is rejected rather than
silently copying a filtered snapshot. Have the administrator provide appropriate
full-table access for the replication login.

## Configuration

Copy `examples/flow.toml` to `flow.toml` (ignored by Git) and provide the PostgreSQL URL through its named environment variable. Object-storage credentials use the standard storage credential chain or catalog-vended credentials. For REST catalog authentication, configure environment-variable names under `[catalog]`:

```toml
[catalog]
uri = "https://catalog.example.com"
token_env = "FLOW_CATALOG_TOKEN"
# Alternatively, use OAuth client credentials:
# credential_env = "FLOW_CATALOG_CREDENTIAL"
```

Set the named variable in the Flow process environment through your shell,
container or secret manager. `token_env` supplies a bearer token;
`credential_env` supplies `client_id:client_secret` (or a client secret alone,
if the catalog supports it). Flow does not load `.env` files automatically.
Missing, empty or non-Unicode values fail when connecting. `check` and `status`
do not resolve these secrets. Restart Flow to use changed environment values.
Existing literal `token` and `credential` properties remain supported, but do
not set a literal property and its corresponding `_env` reference together.
If both token and OAuth credentials are supplied, the token takes precedence.

Configuration parse errors report a location without source excerpts or input
values. Catalog errors similarly omit response bodies, including OAuth error
descriptions; use the HTTP status and server-side diagnostics when investigating
an authentication failure.

The configured column list defines the initial source schema; use the [type mappings](postgres-types.md) when filling it in. `primary_key` contains zero-based positions in that list. New nullable columns without a non-null backfill are discovered automatically; keep the original prefix in the configuration across restarts. Field IDs are stable destination identities, while PostgreSQL attribute identity is checked separately to detect drop-and-recreate changes.

Configure the REST catalog URI, warehouse and object-store endpoint for your own
services. Flow needs catalog access to load/create tables and commit snapshots,
and object access to read, write, list and delete its files. Use a persistent,
writable `state_dir`; do not share it between running Flow processes.

`check` validates the configuration locally. It does not verify credentials,
source permissions, publication membership or service connectivity; `init`
performs those checks while initializing the pipeline.

Mutable tables require a primary key and `REPLICA IDENTITY FULL`. Publications must include inserts, updates, deletes and truncates, and every configured table with all of its columns and no row filter; they may include other tables. During streaming, TRUNCATE blocks the affected table and requires coordinated resynchronization. Keyless tables are supported only in append-only mode.

An unchanged TOAST value is recovered from the complete old tuple included in that replication event. Missing or unresolved old values block the affected table rather than publishing an incomplete row.

```sh
export FLOW_POSTGRES_URL='postgres://user:password@host/database?sslmode=require'
./target/release/embrasure-flow --config flow.toml init
./target/release/embrasure-flow --config flow.toml run
```

`init` creates a replication slot and copies up to four tables concurrently while capturing CDC into the durable journal. Each table has its own staging journal and publishes reader-sized initial files. Source acknowledgement stays at the initial cut until every table has a published base. Restart reuses completed staging and recopies only unfinished tables from a new temporary snapshot; the original replication slot and source identity stay intact. The service never drops or resets permanent slots automatically.

The default combined roles are `ingest,coordinator,compactor`. `--roles=ingest,coordinator` disables built-in compaction. At a hard reader-debt limit the affected table pauses publication and retries, while capture continues within its disk budget. External maintenance can clear that debt and resume publication without restarting the daemon. An optional `[compaction]` section configures the thresholds. Workers and rebuild facilities are also available as Rust libraries.

`status` reads an atomic status file without locking the index. It reports exact source watermarks, readiness, process identity and freshness while running or stopped. `state_dir/metrics.prom` supports a Prometheus textfile collector. See [observability](observability.md) for latency definitions, reader debt and the distinction between SDK operations and billed requests.

## Common setup errors

| Error or symptom | Action |
| --- | --- |
| Source connection environment variable is missing | Export the variable named by `source.connection_env` in the process running Flow. |
| Publication not found or a configured table is missing | Create the named publication in the source database with every configured table and all four operation flags. It may include other tables. |
| Publication no longer matches the capture contract | A configured table was removed or given a row filter or column list: only that table is blocked with `publication_changed` and needs a resync. A missing publication or unpublished operation stops capture; every start for that slot then fails with "requires resynchronization before capture can resume". Restoring the setting does not clear either; changes may have been skipped. |
| Replica identity or primary-key validation fails | Set FULL replica identity and match the complete primary key in `primary_key`; keyless tables require append-only mode. |
| Source column name or type differs | Match column order, names and the type mappings; check `column_selection` if intentionally excluding columns. |
| Connection, authentication or access denied | Check PostgreSQL login/replication permissions, catalog credentials and object-store permissions. A successful `check` does not validate them. |
| Source slot is missing, lost WAL, or source identity changed | Preserve local state and diagnose the source change. Restoring the slot name alone cannot recover missing changes; coordinated resynchronization is required. |

Inspect `status`, `blocked_tables` and the structured process logs together.
The [observability guide](observability.md) explains readiness and retryable table
failures, including cases where healthy tables continue publishing.

## Recovery and operational limits

`materialized` acknowledgement is the default. `journaled` mode requires an explicit declaration of independently durable storage. Selecting that mode does not replicate a local disk. Disk loss and a process crash are different failure models.

Keep snapshot history covering unfinished prepared operations, reader retention windows and useful checkpoints. The garbage collector protects retained snapshots, checkpoints and in-flight operations, and deletes only registered service-owned artifacts after a grace period. Index loss does not impair reads; startup restores a matching checkpoint or scans standard data and deletes, then atomically activates the rebuilt generation before restoring writes. Source lineage or unexplained external logical changes stop publication.

Automatic snapshot expiration defaults to disabled. Set `limits.snapshot_expiration = true` only when external branch/tag creation and retention-policy changes are excluded or coordinated with this service's expiration. Coordinate catalog retention-policy changes with ingestion and maintenance commits too: a policy-only update does not move a snapshot head. Standard Iceberg REST cannot atomically assert the complete reference set and its policies; assertions on existing reference heads alone do not cover newly created tags. With expiration disabled, retained history and its files accumulate; arrange catalog-side coordinated expiration if automatic cleanup is required. Data compaction, manifest rewriting and collection of unreferenced owned artifacts still run.

Current support boundaries:

- Unpartitioned Iceberg v2 position deletes and v3 deletion vectors. Set `format_version = 3` on a table to create a v3 target; see [v3 configuration and compatibility](iceberg-v3.md). Partitioning remains planned work.
- Mutable tables require a stable primary key and FULL replica identity. Keyless tables support append-only ingestion and equivalent external physical rewrites, including duplicate rows.
- Automatic DDL supports nullable column additions without a non-null backfill and compatible required-to-nullable changes. During streaming, classified table schema/row errors and TRUNCATE durably block that table. Healthy tables can continue within the journal/WAL budgets, but shared acknowledgement cannot pass an incomplete transaction. Source connection/slot/identity failures remain connection-wide. See [table isolation and recovery](table-publication-isolation.md).
- Initial COPY uses up to four workers. Recovery needs capacity for one additional temporary replication slot. Transaction metadata and row payloads spill to disk; configured journal, spool, message and row-size budgets still apply.
- Catalog and object-store transient failures retry with durable prepared-operation recovery. Run the daemon under a supervisor for process failures and startup failures. Local fault tests do not establish independent-host durability.
- Compaction supports unsorted layouts. Z-order-aware compaction, distributed compaction protocols, HA and global autocompaction are outside the early release scope. External compactor reconciliation is included and tested with actual Spark maintenance.

Keep the source publication's operation flags, and the configured tables' membership, column lists and row filters, fixed from initialization through streaming. Other tables may join or leave the publication. Coordinate changes by stopping capture and resynchronizing the affected source before resuming. PostgreSQL may silently omit changes under an altered publication, so restoring its settings or restarting the service cannot prove that no rows were missed. Flow re-checks this contract on every reconnect and about once a minute while running. A change to one configured table blocks only that table (`blocked_tables` code `publication_changed`) and drops its changes while the others keep syncing. A missing publication or unpublished operation stops the process and writes `state_dir/publication-resync-required.json`; later starts for that slot refuse until a resync. Neither clears when the publication is restored. The check is best effort: it cannot see a configured table removed and re-added between checks, and the writes made in that gap are never sent. Coordinate every publication change that affects configured tables with a resync.
