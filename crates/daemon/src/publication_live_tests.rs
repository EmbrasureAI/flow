//! The real daemon against live PostgreSQL and an in-memory Iceberg catalog:
//! a publication change affecting one configured table blocks only that table,
//! and a streamed transaction blocks a table only if it commits.
use crate::config::Config;
use flow_pg_source::tokio_postgres::{self, Client, NoTls};
use futures::FutureExt;
use iceberg::{
    CatalogBuilder,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

async fn lsn(sql: &Client, query: &str, slot: &str) -> i64 {
    let rows = sql.query(query, &[&slot]).await.unwrap();
    rows.first().and_then(|row| row.get(0)).unwrap_or(0)
}

async fn current(sql: &Client) -> i64 {
    sql.query_one("SELECT (pg_current_wal_lsn() - '0/0')::bigint", &[])
        .await
        .unwrap()
        .get(0)
}

async fn confirmed(sql: &Client, slot: &str) -> i64 {
    lsn(
        sql,
        "SELECT (confirmed_flush_lsn - '0/0')::bigint FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        slot,
    )
    .await
}

fn status(config: &Config) -> serde_json::Value {
    std::fs::read(config.state_dir.join("status.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn materialized(config: &Config, table: &str) -> u64 {
    status(config)["table_progress"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|progress| progress["source_table"] == table)
        .and_then(|progress| progress["materialized_lsn"].as_u64())
        .unwrap_or(0)
}

async fn until<F: Future<Output = bool>>(description: &str, mut check: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {description}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Run `checks` against a live daemon capturing `{name}.orders` and
/// `{name}.items`, then release its slot and drop its objects.
async fn with_daemon<F, Fut>(url: &str, sql: &Client, label: &str, checks: F)
where
    F: FnOnce(Config, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let name = format!(
        "flow_public_block_{label}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sql.batch_execute(&format!(
        "CREATE SCHEMA {name};
         CREATE TABLE {name}.orders (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.orders REPLICA IDENTITY FULL;
         CREATE TABLE {name}.items (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.items REPLICA IDENTITY FULL;
         INSERT INTO {name}.orders VALUES (1, 'seed');
         INSERT INTO {name}.items VALUES (1, 'seed');
         CREATE PUBLICATION {name} FOR TABLE {name}.orders, {name}.items;"
    ))
    .await
    .unwrap();
    // Surface capture warnings (disconnects, deferred checks) in test output.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("warn"))
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir().unwrap();
    let mut config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config.state_dir = root.path().join("state");
    config.source.id = name.clone();
    config.source.connection_env = "FLOW_POSTGRES_URL".into();
    config.source.slot = name.clone();
    config.source.publication = name.clone();
    // Functional tests must not depend on the host's free disk space.
    config.storage.min_free_bytes = 0;
    let uri = format!("memory://{name}");
    config.catalog = HashMap::from([("uri".into(), uri.clone())]);
    let orders = config.tables[0].clone();
    config.tables = ["orders", "items"]
        .into_iter()
        .map(|table| {
            let mut configured = orders.clone();
            configured.source_namespace = name.clone();
            configured.source_table = table.into();
            configured.target_namespace = vec![name.clone()];
            configured.target_table = table.into();
            configured
        })
        .collect();
    let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                &name,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), uri.clone())]),
            )
            .await
            .unwrap(),
    );
    crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .insert(uri.clone(), catalog);
    assert!(url.starts_with("postgres"));

    let body = AssertUnwindSafe(async {
        crate::bootstrap::initialize(config.clone()).await.unwrap();
        let daemon = crate::runtime::run(config.clone(), false);
        tokio::pin!(daemon);
        tokio::select! {
            result = &mut daemon => panic!("daemon stopped: {result:?}"),
            () = checks(config.clone(), name.clone()) => {}
        }
    });
    let result = body.catch_unwind().await;

    // Dropping the daemon closes capture; always release the slot.
    for _ in 0..100 {
        sql.execute(
            "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
            &[&name],
        )
        .await
        .unwrap();
        let _ = sql
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                &[&name],
            )
            .await;
        let remaining: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sql.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS {name}; DROP SCHEMA {name} CASCADE"
    ))
    .await
    .unwrap();
    crate::services::TEST_CATALOGS.lock().unwrap().remove(&uri);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// `change` applies a publication edit to `{name}.items`.
async fn scenario(url: &str, sql: &Client, label: &str, change: &str) {
    with_daemon(url, sql, label, |config, name| async move {
            sql.batch_execute(&format!(
                "BEGIN; INSERT INTO {name}.orders VALUES (2, 'before'); INSERT INTO {name}.items VALUES (2, 'before'); COMMIT"
            ))
            .await
            .unwrap();
            until("both tables publish before the change", || async {
                materialized(&config, "orders") > 0 && materialized(&config, "items") > 0
            })
            .await;

            sql.batch_execute(&change.replace("{name}", &name))
                .await
                .unwrap();
            // Changes that may be skipped or filtered, and a healthy-table write.
            sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (3, 'after')"))
                .await
                .unwrap();
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'after')"))
                .await
                .unwrap();
            until(
                "the changed table is blocked with publication_changed",
                || async {
                    status(&config)["blocked_tables"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|record| record["error_code"] == "publication_changed")
                },
            )
            .await;
            let blocked = status(&config)["blocked_tables"].clone();
            assert_eq!(blocked.as_array().unwrap().len(), 1, "{blocked}");

            // The healthy table keeps publishing and the slot keeps advancing,
            // while the blocked table's changes keep arriving (row filter) or stop.
            let orders_before = materialized(&config, "orders");
            let mark = current(sql).await;
            for id in 10..15 {
                sql.batch_execute(&format!(
                    "INSERT INTO {name}.items VALUES ({id}, 'dropped'); INSERT INTO {name}.orders VALUES ({id}, 'kept')"
                ))
                .await
                .unwrap();
            }
            until("the healthy table keeps publishing", || async {
                materialized(&config, "orders") > orders_before
            })
            .await;
            // The product derives freshness from this global watermark.
            until(
                "status watermarks.materialized_lsn advances past the blocked table",
                || async {
                    status(&config)["watermarks"]["materialized_lsn"]
                        .as_i64()
                        .is_some_and(|lsn| lsn > mark)
                },
            )
            .await;
            until(
                "confirmed_flush_lsn advances past the blocked table",
                || async { confirmed(sql, &name).await > mark },
            )
            .await;

            let status = status(&config);
            assert_ne!(status["source_health"], "publication_changed", "{status}");
            assert!(
                !config
                    .state_dir
                    .join("publication-resync-required.json")
                    .exists()
            );
            let progress = status["table_progress"].as_array().unwrap();
            assert_eq!(progress.len(), 2, "blocked tables stay in table_progress");
            for table in ["orders", "items"] {
                assert!(
                    progress
                        .iter()
                        .any(|record| record["source_namespace"] == name.as_str()
                            && record["source_table"] == table)
                );
            }
            let items = progress
                .iter()
                .find(|record| record["source_table"] == "items")
                .unwrap();
            assert_eq!(blocked[0]["table_id"], items["table_id"]);
            assert_eq!(status["state"], "running");

            // The block is visible to Prometheus alerts without parsing status.
            let series = format!(
                "flow_table_blocked{{table_id=\"{}\",code=\"publication_changed\"}} 1\n",
                items["table_id"]
            );
            until("the blocked table is exported as a gauge", || async {
                std::fs::read_to_string(config.state_dir.join("metrics.prom")).is_ok_and(
                    |metrics| {
                        metrics.contains(&series) && metrics.contains("flow_blocked_tables 1\n")
                    },
                )
            })
            .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_publication_change_blocks_only_the_affected_table() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    // Removed: pgoutput stops sending the table's changes.
    scenario(
        &url,
        &sql,
        "removed",
        "ALTER PUBLICATION {name} SET TABLE {name}.orders",
    )
    .await;
    // Filtered: changes keep arriving, so they must be dropped, not retained.
    scenario(
        &url,
        &sql,
        "filtered",
        "ALTER PUBLICATION {name} SET TABLE {name}.orders, {name}.items WHERE (id > 0)",
    )
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

fn blocked_tables(config: &Config) -> Vec<serde_json::Value> {
    status(config)["blocked_tables"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn published_rows(name: &str, table: &str) -> usize {
    use futures::TryStreamExt;
    let catalog = crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .get(&format!("memory://{name}"))
        .cloned()
        .unwrap();
    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(name.into()),
            table.into(),
        ))
        .await
        .unwrap();
    let mut batches = table.scan().build().unwrap().to_arrow().await.unwrap();
    let mut rows = 0;
    while let Some(batch) = batches.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    rows
}

/// Wait until the slot's walsender decoded all WAL written so far, including
/// an open transaction's changes. A committed message flushes that WAL.
async fn decoded(sql: &Client, slot: &str, config: &Config) {
    sql.batch_execute("SELECT pg_catalog.pg_logical_emit_message(true, 'flow-test', '')")
        .await
        .unwrap();
    let mark = current(sql).await;
    let deadline = Instant::now() + Duration::from_secs(120);
    while lsn(
        sql,
        "SELECT (r.sent_lsn - '0/0')::bigint FROM pg_catalog.pg_stat_replication r JOIN pg_catalog.pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1",
        slot,
    )
    .await
        < mark
    {
        if Instant::now() >= deadline {
            // Tell a disconnected capture from one that stopped reading.
            let diagnostics = sql
                .query(
                    "SELECT format('slot active=%s pid=%s confirmed=%s wal_status=%s; sender state=%s sent=%s write=%s flush=%s; stream_txns=%s spill_txns=%s; mark=%s',
                        s.active, s.active_pid, s.confirmed_flush_lsn, s.wal_status, r.state, r.sent_lsn, r.write_lsn, r.flush_lsn,
                        st.stream_txns, st.spill_txns, pg_catalog.pg_current_wal_lsn())
                     FROM pg_catalog.pg_replication_slots s
                     LEFT JOIN pg_catalog.pg_stat_replication r ON r.pid = s.active_pid
                     LEFT JOIN pg_catalog.pg_stat_replication_slots st ON st.slot_name = s.slot_name
                     WHERE s.slot_name = $1",
                    &[&slot],
                )
                .await
                .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>());
            let sessions = sql
                .query(
                    "SELECT format('%s %s wait=%s:%s xact_age=%s query=%s', backend_type, state, wait_event_type, wait_event,
                        now() - xact_start, left(regexp_replace(query, '\\s+', ' ', 'g'), 160))
                     FROM pg_catalog.pg_stat_activity WHERE pid <> pg_backend_pid() AND state <> 'idle'",
                    &[],
                )
                .await
                .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>());
            let daemon = status(config);
            panic!(
                "timed out: the walsender decodes the open transaction; {diagnostics:?}; sessions: {sessions:?}; daemon state={} source_health={} last_error={} blocked={}",
                daemon["state"], daemon["source_health"], daemon["last_error"], daemon["blocked_tables"]
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn streamed_transactions(sql: &Client, slot: &str) -> i64 {
    lsn(
        sql,
        "SELECT stream_txns FROM pg_catalog.pg_stat_replication_slots WHERE slot_name = $1",
        slot,
    )
    .await
}

/// A long transaction holding ACCESS EXCLUSIVE on a published table must not
/// stall capture: the publication check backs off instead of disconnecting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_locked_table_does_not_stall_capture() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    let held_url = url.clone();
    with_daemon(&url, sql, "locked", |config, name| async move {
        until("orders publishes before the lock", || async {
            materialized(&config, "orders") > 0
        })
        .await;
        let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
        let held_task = tokio::spawn(connection);
        held.batch_execute(&format!(
            "BEGIN; LOCK TABLE {name}.items IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .unwrap();
        // Longer than a source operation deadline and many check intervals:
        // every change to the unlocked table still publishes. Probes are spaced
        // out so tiny commits don't build reader debt (compaction is off here).
        let started = Instant::now();
        let mut id = 100;
        while started.elapsed() < Duration::from_secs(36) {
            let before = materialized(&config, "orders");
            sql.batch_execute(&format!(
                "INSERT INTO {name}.orders VALUES ({id}, 'locked')"
            ))
            .await
            .unwrap();
            id += 1;
            until("orders keeps publishing while items is locked", || async {
                materialized(&config, "orders") > before
            })
            .await;
            let active: bool = sql
                .query_one(
                    "SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&name],
                )
                .await
                .unwrap()
                .get(0);
            assert!(active, "capture disconnected while items was locked");
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
        held.batch_execute("ROLLBACK").await.unwrap();
        drop(held);
        held_task.await.unwrap().unwrap();
        // Reader-debt pauses are expected here (compaction is off); a lock must
        // never latch a publication or source-schema block.
        let blocked = status(&config)["blocked_tables"].clone();
        assert!(
            blocked.as_array().into_iter().flatten().all(|record| {
                record["error_code"] != "publication_changed"
                    && record["error_code"] != "source_schema_incompatible"
            }),
            "a lock must not block a table: {blocked}"
        );
    })
    .await;
    drop(sql_task);
}

/// pgoutput streams a transaction's TRUNCATE, DDL Relation and rows once it
/// exceeds logical_decoding_work_mem, before PostgreSQL commits or aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_rolled_back_streamed_changes_do_not_block_tables() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    // Twice the decoding budget of ~300-byte rows forces streaming.
    let budget: i64 = sql
        .query_one(
            "SELECT setting::bigint * 1024 FROM pg_catalog.pg_settings WHERE name = 'logical_decoding_work_mem'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let rows = budget * 2 / 256 + 100;
    let held_url = url.clone();
    with_daemon(&url, sql, "streamed", |config, name| async move {
        let fill = |from: i64| {
            format!(
                "INSERT INTO {name}.items SELECT g, repeat(md5(g::text), 8) FROM generate_series({from}, {}) g",
                from + rows
            )
        };
        sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (2, 'before')"))
            .await
            .unwrap();
        until("items publishes before the streamed transactions", || async {
            materialized(&config, "items") > 0
        })
        .await;
        let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
        let held_task = tokio::spawn(connection);
        let streamed = streamed_transactions(sql, &name).await;
        for (open, end) in [
            // TRUNCATE is streamed with the rows that follow it.
            (
                format!("BEGIN; TRUNCATE {name}.items; {}", fill(1_000_000)),
                "ROLLBACK".to_owned(),
            ),
            // So is the Relation describing an incompatible column type.
            (
                format!(
                    "BEGIN; ALTER TABLE {name}.items ALTER COLUMN status TYPE varchar(400); {}",
                    fill(1_000_000)
                ),
                "ROLLBACK".to_owned(),
            ),
            // A savepoint rollback discards its TRUNCATE; the rest commits.
            (
                format!(
                    "BEGIN; {}; SAVEPOINT truncated; TRUNCATE {name}.items; {}",
                    fill(2_000_000),
                    fill(3_000_000)
                ),
                format!(
                    "ROLLBACK TO SAVEPOINT truncated; INSERT INTO {name}.items VALUES (3, 'kept'); COMMIT"
                ),
            ),
        ] {
            held.batch_execute(&open).await.unwrap();
            // PostgreSQL skips streaming a transaction already known to have
            // aborted, so end it only after its changes reached the daemon.
            decoded(sql, &name, &config).await;
            held.batch_execute(&end).await.unwrap();
        }
        drop(held);
        held_task.await.unwrap().unwrap();
        sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (4, 'after')"))
            .await
            .unwrap();
        let mark = current(sql).await;
        until("items publishes after the rolled-back streams", || async {
            materialized(&config, "items") as i64 >= mark
        })
        .await;
        until("confirmed_flush_lsn advances past the streams", || async {
            confirmed(sql, &name).await >= mark
        })
        .await;
        assert!(blocked_tables(&config).is_empty(), "{:?}", status(&config));
        until("PostgreSQL reports the streamed transactions", || async {
            streamed_transactions(sql, &name).await >= streamed + 3
        })
        .await;
        // Seed, 2, the committed fill, 3 and 4: nothing from rolled-back work.
        let expected = usize::try_from(rows).unwrap() + 5;
        assert_eq!(published_rows(&name, "items").await, expected);

        // Committed, the same streamed TRUNCATE blocks the table.
        sql.batch_execute(&format!(
            "BEGIN; TRUNCATE {name}.items; {}; COMMIT",
            fill(4_000_000)
        ))
        .await
        .unwrap();
        until("a committed streamed TRUNCATE blocks items", || async {
            blocked_tables(&config)
                .iter()
                .any(|record| record["error_code"] == "source_schema_incompatible")
        })
        .await;
        let blocked = blocked_tables(&config);
        assert_eq!(blocked.len(), 1, "{blocked:?}");
    })
    .await;
    sql_task.abort();
}

/// ORM migrations add a column, then set its default or backfill it and set
/// NOT NULL, usually before the next catalog check. Only defaults that
/// backfill existing rows without row events block the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_column_migrations_publish_optional_fields() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    with_daemon(&url, sql, "migration", |config, name| async move {
        sql.batch_execute(&format!(
            "ALTER TABLE {name}.items ADD COLUMN note text;
             ALTER TABLE {name}.items ALTER COLUMN note SET DEFAULT 'defaulted';
             INSERT INTO {name}.items (id, status) VALUES (2, 'default');
             BEGIN;
             ALTER TABLE {name}.items ADD COLUMN flag integer;
             UPDATE {name}.items SET flag = 1;
             ALTER TABLE {name}.items ALTER COLUMN flag SET NOT NULL;
             ALTER TABLE {name}.items ALTER COLUMN flag SET DEFAULT 0;
             COMMIT;
             INSERT INTO {name}.items (id, status) VALUES (3, 'not null');"
        ))
        .await
        .unwrap();
        let mark = current(sql).await;
        until("items publishes the migrated rows", || async {
            materialized(&config, "items") as i64 >= mark
        })
        .await;
        // Span the five-second catalog refresh as well as commit validation.
        tokio::time::sleep(Duration::from_secs(6)).await;
        sql.batch_execute(&format!(
            "INSERT INTO {name}.items (id, status) VALUES (4, 'after refresh')"
        ))
        .await
        .unwrap();
        let mark = current(sql).await;
        until("items publishes after the catalog refresh", || async {
            materialized(&config, "items") as i64 >= mark
        })
        .await;
        assert!(blocked_tables(&config).is_empty(), "{:?}", status(&config));
        assert_eq!(published_rows(&name, "items").await, 4);
        let catalog = crate::services::TEST_CATALOGS
            .lock()
            .unwrap()
            .get(&format!("memory://{name}"))
            .cloned()
            .unwrap();
        let table = catalog
            .load_table(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new(name.clone()),
                "items".into(),
            ))
            .await
            .unwrap();
        let schema = table.metadata().current_schema();
        for field in ["note", "flag"] {
            // Source NOT NULL still maps to an optional field: Iceberg cannot
            // make an existing optional field required.
            assert!(!schema.field_by_name(field).unwrap().required, "{field}");
        }
    })
    .await;
    sql_task.abort();
}

/// DDL before a savepoint streams its Relation with the savepoint's rows. The
/// table blocks at the commit when those rows survive; when they are rolled
/// back, the committed DDL blocks at the next Relation or catalog refresh.
/// Either way the other table keeps publishing and no row of the new shape
/// reaches the blocked table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_streamed_ddl_blocks_only_when_it_commits() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    let budget: i64 = sql
        .query_one(
            "SELECT setting::bigint * 1024 FROM pg_catalog.pg_settings WHERE name = 'logical_decoding_work_mem'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let rows = budget * 2 / 256 + 100;
    for (label, ddl, column, rolled_back) in [
        ("rename_rb", "RENAME COLUMN status TO label", "label", true),
        ("rename", "RENAME COLUMN status TO label", "label", false),
        (
            "default_rb",
            "ADD COLUMN extra text DEFAULT 'x'",
            "status",
            true,
        ),
        (
            "default",
            "ADD COLUMN extra text DEFAULT 'x'",
            "status",
            false,
        ),
    ] {
        let held_url = url.clone();
        with_daemon(&url, sql, label, |config, name| async move {
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (2, 'before')"))
                .await
                .unwrap();
            let mark = current(sql).await;
            until("both tables publish before the DDL", || async {
                materialized(&config, "orders") as i64 >= mark
                    && materialized(&config, "items") > 0
            })
            .await;
            let items = published_rows(&name, "items").await;
            let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
            let held_task = tokio::spawn(connection);
            held.batch_execute(&format!(
                "BEGIN; ALTER TABLE {name}.items {ddl}; SAVEPOINT s;
                 INSERT INTO {name}.items (id, {column}) SELECT g, repeat(md5(g::text), 8) FROM generate_series(1000, {}) g",
                1000 + rows
            ))
            .await
            .unwrap();
            decoded(sql, &name, &config).await;
            // Another transaction commits while the streamed one is undecided.
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'during')"))
                .await
                .unwrap();
            let mark = current(sql).await;
            until("orders publishes during the streamed DDL", || async {
                materialized(&config, "orders") as i64 >= mark
            })
            .await;
            let end = if rolled_back { "ROLLBACK TO SAVEPOINT s; COMMIT" } else { "COMMIT" };
            held.batch_execute(end).await.unwrap();
            drop(held);
            held_task.await.unwrap().unwrap();
            // A row of the committed new shape.
            sql.batch_execute(&format!(
                "INSERT INTO {name}.items (id, {column}) VALUES (4, 'new shape')"
            ))
            .await
            .unwrap();
            until("the committed DDL blocks items", || async {
                blocked_tables(&config)
                    .iter()
                    .any(|record| record["error_code"] == "source_schema_incompatible")
            })
            .await;
            let blocked = blocked_tables(&config);
            assert_eq!(blocked.len(), 1, "{label}: {blocked:?}");
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (5, 'after')"))
                .await
                .unwrap();
            let mark = current(sql).await;
            until("orders keeps publishing after the block", || async {
                materialized(&config, "orders") as i64 >= mark
            })
            .await;
            assert_eq!(published_rows(&name, "items").await, items, "{label}");
            assert_eq!(published_rows(&name, "orders").await, 4, "{label}");
        })
        .await;
    }
    sql_task.abort();
}
