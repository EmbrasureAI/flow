//! Source failures against live PostgreSQL: a busy slot or full server is
//! retried, and one table's row errors block only that table.
use super::{retry_hint, retryable_connection};
use crate::config::Config;
use flow_model::PgLsn;
use flow_pg_source::{
    PgOutputSource,
    tokio_postgres::{self, Client, NoTls, config::ReplicationMode, error::SqlState},
};
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

fn unique(label: &str) -> String {
    format!(
        "flow_source_{label}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn connect(url: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let (sql, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    (
        sql,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}

async fn replication(url: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let mut config: tokio_postgres::Config = url.parse().unwrap();
    config.replication_mode(ReplicationMode::Logical);
    let (client, connection) = config.connect(NoTls).await.unwrap();
    (
        client,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}

async fn release_slot(sql: &Client, slot: &str) {
    for _ in 0..100 {
        sql.execute(
            "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
            &[&slot],
        )
        .await
        .unwrap();
        let _ = sql
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                &[&slot],
            )
            .await;
        let remaining: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .unwrap()
            .get(0);
        if remaining == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The SQLSTATEs PostgreSQL actually returns are classified as retryable.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_busy_slot_and_connection_limit_are_retryable() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, _sql_task) = connect(&url).await;
    let name = unique("busy");
    for statement in [
        format!("CREATE PUBLICATION {name}"),
        format!("SELECT pg_catalog.pg_create_logical_replication_slot('{name}', 'pgoutput')"),
        format!("CREATE ROLE {name} LOGIN CONNECTION LIMIT 0 PASSWORD 'flow-connection-limit'"),
    ] {
        sql.batch_execute(&statement).await.unwrap();
    }
    let result = AssertUnwindSafe(async {
        let (holder, _holder_task) = replication(&url).await;
        let _held = PgOutputSource::start(&holder, &name, &name, PgLsn(0), 1 << 20)
            .await
            .unwrap();
        let (second, _second_task) = replication(&url).await;
        let Err(flow_pg_source::Error::Postgres(error)) =
            PgOutputSource::start(&second, &name, &name, PgLsn(0), 1 << 20).await
        else {
            panic!("an active slot must refuse a second consumer");
        };
        assert_eq!(error.code(), Some(&SqlState::OBJECT_IN_USE), "{error}");
        let error = anyhow::Error::new(error);
        assert!(retryable_connection(&error), "{error:#}");
        assert!(retry_hint(&error).contains("wal_sender_timeout"));

        let mut limited: tokio_postgres::Config = url.parse().unwrap();
        limited.user(&name).password("flow-connection-limit");
        let error = limited.connect(NoTls).await.err().unwrap();
        assert_eq!(
            error.code(),
            Some(&SqlState::TOO_MANY_CONNECTIONS),
            "{error}"
        );
        let error = anyhow::Error::new(error);
        assert!(retryable_connection(&error), "{error:#}");
        assert!(retry_hint(&error).contains("connection slots"));
    })
    .catch_unwind()
    .await;
    release_slot(&sql, &name).await;
    sql.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS {name}; DROP ROLE IF EXISTS {name}"
    ))
    .await
    .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn status(config: &Config) -> serde_json::Value {
    std::fs::read(config.state_dir.join("status.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn table_id(config: &Config, table: &str) -> Option<serde_json::Value> {
    status(config)["table_progress"]
        .as_array()?
        .iter()
        .find(|progress| progress["source_table"] == table)
        .map(|progress| progress["table_id"].clone())
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

fn blocked(config: &Config) -> Vec<serde_json::Value> {
    status(config)["blocked_tables"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn until<F: Future<Output = bool>>(description: &str, mut check: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {description}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The daemon waits out a slot held by another session, then an UPDATE on an
/// append-only table and a row larger than `limits.chunk_bytes` each block only
/// their own table while the healthy table keeps publishing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_busy_slot_waits_and_row_errors_block_only_their_table() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, _sql_task) = connect(&url).await;
    let name = unique("rows");
    sql.batch_execute(&format!(
        "CREATE SCHEMA {name};
         CREATE TABLE {name}.orders (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.orders REPLICA IDENTITY FULL;
         CREATE TABLE {name}.events (id bigint PRIMARY KEY, status text);
         CREATE TABLE {name}.blobs (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.blobs REPLICA IDENTITY FULL;
         INSERT INTO {name}.orders VALUES (1, 'seed');
         INSERT INTO {name}.events VALUES (1, 'seed');
         INSERT INTO {name}.blobs VALUES (1, 'seed');
         CREATE PUBLICATION {name} FOR TABLE {name}.orders, {name}.events, {name}.blobs;"
    ))
    .await
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config.state_dir = root.path().join("state");
    config.source.id = name.clone();
    config.source.connection_env = "FLOW_POSTGRES_URL".into();
    config.source.slot = name.clone();
    config.source.publication = name.clone();
    config.limits.chunk_bytes = 64 << 10;
    let uri = format!("memory://{name}");
    config.catalog = HashMap::from([("uri".into(), uri.clone())]);
    let template = config.tables[0].clone();
    config.tables = ["orders", "events", "blobs"]
        .into_iter()
        .map(|table| {
            let mut configured = template.clone();
            configured.source_namespace = name.clone();
            configured.source_table = table.into();
            configured.target_namespace = vec![name.clone()];
            configured.target_table = table.into();
            configured.append_only = table == "events";
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

    let body = AssertUnwindSafe(async {
        crate::bootstrap::initialize(config.clone()).await.unwrap();
        // Another session holds the slot, as a previous walsender can after a
        // network partition until wal_sender_timeout.
        let (holder, holder_task) = replication(&url).await;
        let held = PgOutputSource::start(&holder, &name, &name, PgLsn(0), 1 << 20)
            .await
            .unwrap();
        let daemon = crate::runtime::run(config.clone(), false);
        tokio::pin!(daemon);
        let checks = async {
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (2, 'held')"))
                .await
                .unwrap();
            let inserted: i64 = sql
                .query_one("SELECT (pg_current_wal_lsn() - '0/0')::bigint", &[])
                .await
                .unwrap()
                .get(0);
            tokio::time::sleep(Duration::from_secs(5)).await;
            assert!(materialized(&config, "orders") < inserted as u64);
            drop(held);
            drop(holder);
            holder_task.abort();
            until("capture starts once the slot is released", || async {
                materialized(&config, "orders") >= inserted as u64
            })
            .await;

            sql.batch_execute(&format!(
                "UPDATE {name}.events SET status = 'changed' WHERE id = 1"
            ))
            .await
            .unwrap();
            sql.batch_execute(&format!(
                "INSERT INTO {name}.blobs VALUES (2, repeat('x', 100000))"
            ))
            .await
            .unwrap();
            until("both failing tables are blocked", || async {
                blocked(&config).len() == 2
            })
            .await;
            let ids: Vec<_> = blocked(&config)
                .iter()
                .map(|record| record["table_id"].clone())
                .collect();
            for table in ["events", "blobs"] {
                let id = table_id(&config, table).unwrap();
                assert!(ids.contains(&id), "{table} must be blocked: {ids:?}");
            }
            let before = materialized(&config, "orders");
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'after')"))
                .await
                .unwrap();
            until("the healthy table keeps publishing", || async {
                materialized(&config, "orders") > before
            })
            .await;
            assert_eq!(blocked(&config).len(), 2);
        };
        tokio::select! {
            result = &mut daemon => panic!("daemon stopped: {result:?}"),
            () = checks => {}
        }
    });
    let result = body.catch_unwind().await;
    release_slot(&sql, &name).await;
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
