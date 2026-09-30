//! The real daemon against live PostgreSQL and an in-memory Iceberg catalog:
//! a publication change affecting one configured table blocks only that table.
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

/// `change` applies a publication edit to `{name}.items`.
async fn scenario(url: &str, sql: &Client, label: &str, change: &str) {
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
    let root = tempfile::tempdir().unwrap();
    let mut config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config.state_dir = root.path().join("state");
    config.source.id = name.clone();
    config.source.connection_env = "FLOW_POSTGRES_URL".into();
    config.source.slot = name.clone();
    config.source.publication = name.clone();
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
        let checks = async {
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
        };
        tokio::select! {
            result = &mut daemon => panic!("daemon stopped: {result:?}"),
            () = checks => {}
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
