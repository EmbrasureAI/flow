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

Tagged releases also publish Linux binaries for x86-64 and arm64 and a
container image, `ghcr.io/embrasureai/flow`; see the
[releases page](https://github.com/EmbrasureAI/flow/releases).

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

PostgreSQL's default `max_slot_wal_keep_size = -1` lets a stalled slot retain
WAL until the source disk fills. Set a cap the source volume can hold; reaching
it invalidates the slot and requires resynchronization. See
[bounding source WAL retention](operations.md#bound-source-wal-retention).

As the table owner or administrator, prepare the existing example table before
running `init`:

```sql
-- public.orders must have the stable primary key described in flow.toml.
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION embrasure_flow FOR TABLE public.orders
  WITH (publish = 'insert, update, delete, truncate');
```

### Replica identity

Mutable tables need a primary key and one of two replica identities:

- `REPLICA IDENTITY FULL` works for every supported column type. PostgreSQL
  logs the complete old row for each update and delete, so it writes more WAL.
- `REPLICA IDENTITY DEFAULT` (PostgreSQL's default) also works when every
  replicated column has a fixed-width type: `boolean`, `smallint`, `integer`,
  `bigint`, `real`, `double precision`, `date`, `time`, `timestamp`,
  `timestamptz`, `uuid`, an enum, or a domain over one of these. Such values
  cannot be TOASTed, so every update carries the complete new row without the
  extra old-row WAL. A table with any variable-width replicated column (`text`,
  `numeric`, `jsonb`, arrays and so on) needs FULL; excluding such a column with
  explicit `column_selection` does not change this.

`check --source` and `discover` report which identity each table needs. Tables
without a primary key can only be replicated in append-only mode.

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
If both token and OAuth credentials are supplied, the token takes precedence
until the catalog rejects it.

Tokens obtained with OAuth client credentials are renewed before the expiry
the token endpoint reports in `expires_in` (five minutes early, or after nine
tenths of a shorter lifetime). If the catalog rejects a token with HTTP 401 or
419, Flow exchanges the credentials for a new token and retries the request
once. A static `token_env` token cannot be renewed; when it expires, restart Flow
with a new value. Rejected credentials or insufficient permissions (HTTP 401,
403 or 419, or a rejected OAuth token request) block the affected tables with
`blocked_tables` code `catalog_auth` instead of `catalog_unavailable`, so alert
on it separately. Blocked tables keep retrying.

HTTPS catalog and OAuth endpoints are verified against the host's trust store:
the system certificate bundle on Linux and the keychain's trusted roots on
macOS. Setting `SSL_CERT_FILE` (a PEM bundle) or `SSL_CERT_DIR` in the Flow
process environment replaces that store for the catalog client. To trust a
private CA in addition to the system roots, name its PEM bundle:

```toml
[catalog]
uri = "https://catalog.internal.example.com"
tls_ca_file = "/etc/flow/catalog-ca.pem"
```

Flow reads `tls_ca_file` when connecting and does not forward it to the
catalog. Client certificates (mutual TLS) are not supported. Use `https://` for
any catalog that receives a token or credential: Flow logs a warning when
`uri` or `oauth2-server-uri` sends them over plain `http://` to a host other
than loopback, but still connects so existing private-network deployments keep
working. `tls_ca_file` and these environment variables apply to the catalog
client; object-storage connections use their own client.

Configuration parse errors report a location without source excerpts or input
values. Catalog errors similarly omit response bodies, including OAuth error
descriptions; use the HTTP status and server-side diagnostics when investigating
an authentication failure.

Generate the `[[tables]]` blocks instead of writing them by hand. With the
`[source]`, `[catalog]` and `state_dir` settings in place and the publication
created, run:

```sh
./target/release/embrasure-flow --config flow.toml discover >> flow.toml
```

By default `discover` describes the publication's tables that are not yet
configured. Name tables (`discover sales.orders sales.customers`) or pass
`--schema sales` to choose others, and `--target-namespace` to change the Iceberg
namespace, which defaults to the source schema. It is read-only. It maps types
with the same rules as `init`, excludes unsupported columns through explicit
column selection, marks tables without a primary key append-only, and adds a
comment above any table that `init` would still reject. Review the output before
initializing; the column list and field IDs are fixed once `init` runs.

The configured column list defines the initial source schema; see the [type mappings](postgres-types.md). `primary_key` contains zero-based positions in that list. New nullable columns without a non-null backfill are discovered automatically; keep the original prefix in the configuration across restarts. Field IDs are stable destination identities, while PostgreSQL attribute identity is checked separately to detect drop-and-recreate changes.

Configure the REST catalog URI, warehouse and object-store endpoint for your own
services. Flow needs catalog access to load/create tables and commit snapshots,
and object access to read, write, list and delete its files. Use a persistent,
writable `state_dir`; do not share it between running Flow processes. A
relative `state_dir` is resolved against the configuration file's directory. It
holds replicated row data, so Flow creates it and every file in it readable only
by the user running Flow, and removes group and other access from an existing
`state_dir` owned by that user. On shared volumes such as Kubernetes volumes
with `fsGroup`, use a subdirectory like `/data/state`; see
[security](../SECURITY.md#deployment).

`check` validates the configuration locally. `check --source` also connects to
PostgreSQL read-only and reports server settings, slot and sender capacity,
replication and heartbeat permissions, each table's key, replica identity,
access and row-level security, the publication contract, and the slot's WAL
retention; it exits nonzero when a check fails. Neither verifies catalog or
object-store access; `init` performs the remaining checks while initializing
the pipeline. See [operations](operations.md#before-initialization).

Mutable tables require a primary key and a [supported replica identity](#replica-identity). Publications must include inserts, updates, deletes and truncates, and every configured table with all of its columns and no row filter; they may include other tables. During streaming, TRUNCATE blocks the affected table and requires coordinated resynchronization. Keyless tables are supported only in append-only mode.

An unchanged TOAST value is recovered from the complete old tuple included in that replication event. Missing or unresolved old values block the affected table rather than publishing an incomplete row.

```sh
export FLOW_POSTGRES_URL='postgres://user:password@host/database?sslmode=verify-full&sslrootcert=/etc/flow/postgres-ca.pem'
./target/release/embrasure-flow --config flow.toml init
./target/release/embrasure-flow --config flow.toml run
```

The URL follows libpq, including its default `sslmode=prefer`, which silently
falls back to plaintext. Use `sslmode=verify-full` whenever the connection
crosses a network: it verifies the server certificate and hostname, against
`sslrootcert` when set or the system trust store otherwise. `sslmode=require`
encrypts without authenticating the server unless `sslrootcert` is set.
`sslmode=verify-ca` checks the chain but not the hostname, so it requires
`sslrootcert` naming a private CA. `init`, `run` and `check --source` warn when
a non-local connection does not authenticate the server. Client certificates
(`sslcert`/`sslkey`) are not supported.

`init` creates a replication slot and copies up to four tables concurrently while capturing CDC into the durable journal. Each table has its own staging journal and publishes reader-sized initial files. Source acknowledgement stays at the initial cut until every table has a published base. Restart reuses completed staging and recopies only unfinished tables from a new temporary snapshot; the original replication slot and source identity stay intact. The service never drops or resets permanent slots automatically.

The default combined roles are `ingest,coordinator,compactor`. `--roles=ingest,coordinator` disables built-in compaction. At a hard reader-debt limit the affected table pauses publication and retries, while capture continues within its disk budget. External maintenance can clear that debt and resume publication without restarting the daemon. An optional `[compaction]` section configures the thresholds. Workers and rebuild facilities are also available as Rust libraries.

`status` reads an atomic status file without locking the index. It reports exact source watermarks, readiness, process identity and freshness while running or stopped. `state_dir/metrics.prom` supports a Prometheus textfile collector running as the Flow user; the `state_dir` is private to that user (see [security](../SECURITY.md#deployment)). To serve probes and metrics over HTTP instead, add:

```toml
[http]
listen = "0.0.0.0:9464"
```

`init` and `run` then serve `GET /healthz` (liveness: fails only if the running
service's main loop stalls), `GET /readyz` (200 while this process reports
ready, otherwise 503, with the status JSON) and `GET /metrics` (the same
Prometheus text as `metrics.prom`). The listener has no
authentication; bind it to a private interface. See [observability](observability.md) for latency definitions, reader debt and the distinction between SDK operations and billed requests.

## Common setup errors

| Error or symptom | Action |
| --- | --- |
| Source connection environment variable is missing | Export the variable named by `source.connection_env` in the process running Flow. |
| Publication not found or a configured table is missing | Create the named publication in the source database with every configured table and all four operation flags. It may include other tables. |
| Publication no longer matches the capture contract | A configured table was removed or given a row filter or column list: only that table is blocked with `publication_changed` and needs a resync. A missing publication or unpublished operation stops capture; every start for that slot then fails with "requires resynchronization before capture can resume". Restoring the setting does not clear either; changes may have been skipped. |
| Replica identity or primary-key validation fails | Set FULL replica identity, or DEFAULT when every replicated column is [fixed-width](#replica-identity), and match the complete primary key in `primary_key`; keyless tables require append-only mode. `discover` generates matching blocks. |
| Source column name or type differs | Match column order, names and the type mappings; check `column_selection` if intentionally excluding columns. |
| Connection, authentication or access denied | Check PostgreSQL login/replication permissions, catalog credentials and object-store permissions. `check --source` validates the PostgreSQL side; plain `check` validates none of them. Tables blocked with `catalog_auth` need new or broader catalog credentials. |
| Catalog TLS certificate verification fails | Trust the catalog's CA through the system store, `SSL_CERT_FILE`, or `catalog.tls_ca_file`, and connect with a host name the certificate covers. |
| Source slot is missing, lost WAL, or source identity changed | Preserve local state and diagnose the source change. Restoring the slot name alone cannot recover missing changes; coordinated resynchronization is required. |

Inspect `status`, `blocked_tables` and the structured process logs together.
The [observability guide](observability.md) explains readiness and retryable table
failures, including cases where healthy tables continue publishing.

## Recovery and operational limits

`materialized` acknowledgement is the default. `journaled` mode requires an explicit declaration of independently durable storage. Selecting that mode does not replicate a local disk. Disk loss and a process crash are different failure models.

Keep snapshot history covering unfinished prepared operations, reader retention windows and useful checkpoints. The garbage collector protects retained snapshots, checkpoints and in-flight operations, and deletes only registered service-owned artifacts after a grace period. Index loss does not impair reads; startup restores a matching checkpoint or scans standard data and deletes, then atomically activates the rebuilt generation before restoring writes. Source lineage or unexplained external logical changes stop publication.

Automatic snapshot expiration is enabled by default. Once a table has at least 128 snapshots, snapshots older than `limits.snapshot_retention_secs` (one hour by default) are expired, and garbage collection later deletes Flow-owned files that no retained snapshot references after `limits.orphan_grace_secs`. Without expiration every commit stays in table metadata and compaction cannot reclaim the files it replaces, so metadata and storage grow without bound. Expiration retains snapshots referenced by branches and tags, the indexed snapshot, checkpoints and unfinished operations. Raise the retention to cover the time-travel window readers need; each retained snapshot enlarges the metadata file that every commit rewrites. Standard Iceberg REST cannot atomically assert the complete reference set during expiration, so a branch or tag created concurrently on a snapshot being expired may be lost. If external tools create branches or tags, coordinate them with Flow maintenance, or set `limits.snapshot_expiration = false` and run coordinated expiration elsewhere. Coordinate catalog retention-policy changes with ingestion and maintenance commits too: a policy-only update does not move a snapshot head. Data compaction, manifest rewriting and collection of unreferenced owned artifacts run in either mode.

Current support boundaries:

- Unpartitioned Iceberg v2 position deletes and v3 deletion vectors. Set `format_version = 3` on a table to create a v3 target; see [v3 configuration and compatibility](iceberg-v3.md). Partitioning remains planned work.
- Mutable tables require a stable primary key and a [supported replica identity](#replica-identity). Keyless tables support append-only ingestion and equivalent external physical rewrites, including duplicate rows.
- Automatic DDL supports nullable column additions without a non-null backfill and compatible required-to-nullable changes. During streaming, classified table schema/row errors and TRUNCATE durably block that table. Healthy tables can continue within the journal/WAL budgets, but shared acknowledgement cannot pass an incomplete transaction. Source connection/slot/identity failures remain connection-wide. See [table isolation and recovery](table-publication-isolation.md).
- Initial COPY copies up to four tables concurrently; each table is read by one worker. Recovery needs capacity for one additional temporary replication slot. Transaction metadata and row payloads spill to disk; configured journal, spool, message and row-size budgets still apply.
- Maximum row size: one captured row change must encode within `limits.chunk_bytes` (default 4 MiB) less 40 bytes of framing, which also reserves room to quarantine the complete decoded row if its table is later blocked. The encoding is close to the row's decoded value bytes; an UPDATE also carries its old primary key. A larger change durably blocks only its table, with a reason naming `limits.chunk_bytes`. The limit can be raised to just under 64 MiB (the journal and spool segment size); `limits.batch_bytes` and `limits.parquet_row_group_bytes` must be at least as large. Each pgoutput message must also fit `limits.source_message_bytes` (default 32 MiB, at least `chunk_bytes`). Text-mode messages expand bytea to hex and an UPDATE carries both row images, so size it at a few times the largest row. An oversized message is rejected before its table can be identified and stops capture for all tables until the limit is raised.
- An UPDATE or DELETE on an `append_only = true` table durably blocks that table; resynchronize it, or set `append_only = false` (which needs a primary key and replica identity) and resynchronize.
- Capture spools open source transactions on disk. `limits.spool_transactions` (default 1024) bounds concurrently open and streamed transactions, and `limits.spool_subtransactions` (default 1048576, roughly 100 bytes of memory each) bounds savepoints that captured rows within one transaction. Exceeding either, or `limits.spool_bytes`, stops capture with an error naming that setting.
- A journal segment damaged before the final segment, or damage that would discard transactions the source ledger recorded as durable, stops startup without modifying the journal. Only an unsynchronized tail of the final segment is truncated after a crash. Restore the state directory or resynchronize the source.
- Catalog and object-store transient failures retry with durable prepared-operation recovery. Run the daemon under a supervisor (systemd, Kubernetes or similar) that restarts it after a crash or failed start. The `state_dir` must be on persistent storage; Flow does not replicate it to another host.
- Compaction supports unsorted layouts. Z-order-aware compaction, distributed compaction and high availability are not supported yet. Flow reconciles rewrites made by external compactors, and this is tested against Spark's maintenance procedures.

Keep the source publication's operation flags, and the configured tables' membership, column lists and row filters, fixed from initialization through streaming. Other tables may join or leave the publication. [Operations](operations.md) describes resynchronization, adding tables and planned source maintenance. Coordinate changes by stopping capture and resynchronizing the affected source before resuming. PostgreSQL may silently omit changes under an altered publication, so restoring its settings or restarting the service cannot prove that no rows were missed. Flow re-checks this contract on every reconnect and about once a minute while running. A change to one configured table blocks only that table (`blocked_tables` code `publication_changed`) and drops its changes while the others keep syncing. A missing publication or unpublished operation stops the process and writes `state_dir/publication-resync-required.json`; later starts for that slot refuse until a resync. Neither clears when the publication is restored. The check is best effort: it cannot see a configured table removed and re-added between checks, and the writes made in that gap are never sent. Coordinate every publication change that affects configured tables with a resync.
