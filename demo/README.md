# Local service demo

The stack runs the Docker-built Rust daemon, PostgreSQL, a standard Iceberg REST catalog backed by PostgreSQL, MinIO and Trino. No ports are published on the host. Named volumes retain source data, catalog metadata, objects and daemon state. The example credentials are for this local demo.

From a fresh set of demo volumes:

```sh
docker compose -f demo/compose.yaml up --build -d
```

Wait for `initialize` to complete and `flow` to start. Query with the unmodified Trino Iceberg connector:

```sh
docker compose -f demo/compose.yaml exec trino trino --execute 'SELECT * FROM lake.replicated.orders'
docker compose -f demo/compose.yaml exec postgres psql -U flow -d flow -c "UPDATE orders SET status='shipped' WHERE id=1"
docker compose -f demo/compose.yaml exec trino trino --execute 'SELECT * FROM lake.replicated.orders'
```

Publication and reader observation are distinct measurements. Built-in maintenance rewrites the idle L0 tail after its age threshold while preserving the visible rows. The daemon runs as UID 10001, including when creating its control store and row index on the named volume. Services have explicit CPU and memory budgets.

Initialization resumes from durable bootstrap state. Rerunning it on a completed bootstrap preserves the source slot and published tables. To restart only the running daemon, use `docker compose -f demo/compose.yaml restart flow`. SIGTERM waits for the daemon's graceful shutdown, with a 30-second container grace period. Inspect readiness and exact source watermarks with:

```sh
docker compose -f demo/compose.yaml exec flow embrasure-flow --config /etc/flow.toml status
```

To remove this demo and its data, explicitly run `docker compose -f demo/compose.yaml down -v`.

The automated packaging check builds the images, creates a separate project with fresh volumes, verifies Trino rows after inserts, updates, deletes and primary-key changes, checks non-root volume ownership and SIGTERM/status, and restarts with source changes queued during downtime. It rejects an existing project and removes only the project it created; logs, rows and status observations remain in the artifact directory:

```sh
python3 demo/validate.py --artifacts target/production-linux-demo-01
```

This complete Linux container check passed locally. Independent Spark and Trino interoperability, including Spark's three maintenance procedures followed by CDC, is covered by the [reader suite](../tests/production/reader_fixtures/README.md). These are correctness checks; they do not qualify production throughput or latency.
