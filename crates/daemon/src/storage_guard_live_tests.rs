//! Fail-closed storage-loss guards: the real daemon against live PostgreSQL
//! and an in-memory Iceberg catalog. Each case damages local state or the
//! slot while the daemon is stopped, then requires startup to refuse with the
//! documented diagnostic, publish nothing and leave the slot's ACK alone.
//!
//! Run serially (`--test-threads=1`): the lost-WAL case lowers the server's
//! `max_slot_wal_keep_size`, which can also invalidate other lagging slots.
use crate::config::Config;
use flow_pg_source::tokio_postgres::{self, Client, NoTls};
use futures::FutureExt;
use iceberg::{
    Catalog, CatalogBuilder, TableIdent,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const LOST_JOURNAL: &[&str] = &[
    // Startup's ledger comparison after a clean truncation.
    "journal lost previously durable transactions",
    // Journal open refusing to truncate a torn tail below the ledger.
    "below its recorded durable position",
];
const AHEAD_OF_JOURNAL: &[&str] =
    &["PostgreSQL slot has acknowledged beyond the local durable journal"];
const RESTORED: &[&str] = &[
    "PostgreSQL slot has acknowledged beyond the local durable journal",
    // A table worker can load the catalog first: the restored index does not
    // know the snapshots published after the backup.
    "paused publication: UnknownServiceOperation",
];
const LOST_WAL: &[&str] = &[
    // Capture's slot validation, or the WAL health monitor if it runs first.
    "replication slot lost required WAL",
    "replication slot lost WAL; resynchronization required",
];

struct Fixture {
    name: String,
    uri: String,
    config: Config,
    catalog: Arc<dyn Catalog>,
    _root: tempfile::TempDir,
}

/// The catalog state a refused start must leave untouched.
#[derive(Debug, PartialEq, Eq)]
struct Published {
    metadata_location: Option<String>,
    snapshots: usize,
    current: Option<i64>,
}

async fn wal(sql: &Client) -> u64 {
    lsn_value(
        sql.query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0),
    )
}

fn lsn_value(text: String) -> u64 {
    text.parse::<flow_model::PgLsn>().unwrap().0
}

async fn slot(sql: &Client, name: &str) -> Option<(Option<u64>, bool, Option<String>)> {
    sql.query_opt(
        "SELECT confirmed_flush_lsn::text, active, wal_status FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&name],
    )
    .await
    .unwrap()
    .map(|row| {
        (
            row.get::<_, Option<String>>(0).map(lsn_value),
            row.get(1),
            row.get(2),
        )
    })
}

async fn confirmed(sql: &Client, name: &str) -> Option<u64> {
    slot(sql, name)
        .await
        .and_then(|(confirmed, _, _)| confirmed)
}

fn status(config: &Config) -> serde_json::Value {
    std::fs::read(config.state_dir.join("status.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn materialized(config: &Config) -> u64 {
    status(config)["watermarks"]["materialized_lsn"]
        .as_u64()
        .unwrap_or(0)
}

async fn until<F: Future<Output = bool>>(description: &str, mut check: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {description}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

impl Fixture {
    async fn new(sql: &Client, label: &str) -> Self {
        let name = format!(
            "flow_guard_{label}_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sql.batch_execute(&format!(
            "CREATE SCHEMA {name};
             CREATE TABLE {name}.items (id bigint PRIMARY KEY, status text);
             ALTER TABLE {name}.items REPLICA IDENTITY FULL;
             INSERT INTO {name}.items SELECT i, 'seed' FROM generate_series(1, 16) i;
             CREATE PUBLICATION {name} FOR TABLE {name}.items;"
        ))
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().join("state");
        config.source.id = name.clone();
        config.source.connection_env = "FLOW_POSTGRES_URL".into();
        config.source.slot = name.clone();
        config.source.publication = name.clone();
        let uri = format!("memory://{name}");
        config.catalog = HashMap::from([("uri".into(), uri.clone())]);
        let table = &mut config.tables[0];
        table.source_namespace = name.clone();
        table.source_table = "items".into();
        table.target_namespace = vec![name.clone()];
        table.target_table = "items".into();
        let catalog: Arc<dyn Catalog> = Arc::new(
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
            .insert(uri.clone(), catalog.clone());
        Self {
            name,
            uri,
            config,
            catalog,
            _root: root,
        }
    }

    async fn published(&self) -> Published {
        let table = self
            .catalog
            .load_table(&TableIdent::from_strs([self.name.as_str(), "items"]).unwrap())
            .await
            .unwrap();
        Published {
            metadata_location: table.metadata_location().map(str::to_owned),
            snapshots: table.metadata().snapshots().count(),
            current: table.metadata().current_snapshot_id(),
        }
    }

    /// Run the daemon until `write` has been published and acknowledged,
    /// then stop it and wait until it released the slot and local state.
    async fn publish(&self, sql: &Client, write: &str) {
        self.run_until(sql, write, async |mark| {
            materialized(&self.config) >= mark
                && confirmed(sql, &self.name).await.unwrap_or(0) >= mark
        })
        .await;
    }

    /// Run the daemon until `write` is durable in the journal but, because it
    /// blocks its table, neither published nor acknowledged.
    async fn journal_unpublished(&self, sql: &Client, write: &str) -> u64 {
        let mark = self
            .run_until(sql, write, async |mark| {
                let status = status(&self.config);
                status["watermarks"]["journal_durable_lsn"]
                    .as_u64()
                    .is_some_and(|durable| durable >= mark)
                    && status["blocked_tables"]
                        .as_array()
                        .is_some_and(|blocked| !blocked.is_empty())
            })
            .await;
        assert!(materialized(&self.config) < mark);
        assert!(confirmed(sql, &self.name).await.unwrap_or(0) < mark);
        mark
    }

    async fn run_until(&self, sql: &Client, write: &str, ready: impl AsyncFn(u64) -> bool) -> u64 {
        let checks = async {
            sql.batch_execute(&write.replace("{name}", &self.name))
                .await
                .unwrap();
            let mark = wal(sql).await;
            until("the daemon reaches the expected state", || ready(mark)).await;
            mark
        };
        let mark = {
            // The pinned runtime future is dropped, and capture closed, at the end of this block.
            let daemon = crate::runtime::run(self.config.clone(), false);
            tokio::pin!(daemon);
            tokio::select! {
                result = &mut daemon => panic!("daemon stopped: {result:?}"),
                mark = checks => mark,
            }
        };
        self.stopped(sql).await;
        mark
    }

    /// Dropping the runtime closes capture on its own thread. Its replication
    /// connection and state stores must be released before the next start.
    async fn stopped(&self, sql: &Client) {
        until("the stopped daemon releases its slot", || async {
            slot(sql, &self.name)
                .await
                .is_none_or(|(_, active, _)| !active)
        })
        .await;
        until("the stopped daemon releases its control store", || async {
            flow_state_store::ControlStore::open(self.config.state_dir.join("control")).is_ok()
        })
        .await;
    }

    /// Start against damaged state; it must refuse without side effects.
    async fn refuses(&self, sql: &Client, expected: &[&str]) -> String {
        let before = self.published().await;
        let ack = slot(sql, &self.name).await;
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            crate::runtime::run(self.config.clone(), false),
        )
        .await
        .expect("daemon kept running on damaged state instead of refusing");
        let error = format!("{:#}", result.expect_err("daemon accepted damaged state"));
        assert!(
            expected.iter().any(|message| error.contains(message)),
            "unexpected refusal: {error}"
        );
        self.stopped(sql).await;
        assert_eq!(
            self.published().await,
            before,
            "a refused start published to the catalog"
        );
        assert_eq!(
            slot(sql, &self.name)
                .await
                .map(|(confirmed, _, status)| (confirmed, status)),
            ack.map(|(confirmed, _, status)| (confirmed, status)),
            "a refused start moved the slot"
        );
        error
    }

    async fn cleanup(&self, sql: &Client) {
        for _ in 0..100 {
            sql.execute(
                "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
                &[&self.name],
            )
            .await
            .unwrap();
            let _ = sql
                .query(
                    "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                    &[&self.name],
                )
                .await;
            if slot(sql, &self.name).await.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        sql.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {0}; DROP SCHEMA IF EXISTS {0} CASCADE",
            self.name
        ))
        .await
        .unwrap();
        crate::services::TEST_CATALOGS
            .lock()
            .unwrap()
            .remove(&self.uri);
    }
}

/// Initialize and publish one change, damage state, then require refusal.
async fn scenario<D>(sql: &Client, label: &str, damage: D) -> String
where
    D: AsyncFnOnce(&Fixture) -> &'static [&'static str],
{
    let fixture = Fixture::new(sql, label).await;
    let result = AssertUnwindSafe(async {
        crate::bootstrap::initialize(fixture.config.clone())
            .await
            .unwrap();
        fixture
            .publish(
                sql,
                "INSERT INTO {name}.items VALUES (100, 'published'); UPDATE {name}.items SET status = 'changed' WHERE id = 1",
            )
            .await;
        let expected = damage(&fixture).await;
        fixture.refuses(sql, expected).await
    })
    .catch_unwind()
    .await;
    fixture.cleanup(sql).await;
    match result {
        Ok(error) => error,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn restored_older_state_dir(sql: &Client) -> String {
    scenario(sql, "restored", async |fixture: &Fixture| {
        let state = &fixture.config.state_dir;
        let backup = state.with_file_name("backup");
        copy_tree(state, &backup);
        // Newer work is published and acknowledged after the backup.
        fixture
            .publish(
                sql,
                "INSERT INTO {name}.items VALUES (200, 'after backup'); DELETE FROM {name}.items WHERE id = 2",
            )
            .await;
        std::fs::rename(state, state.with_file_name("newer")).unwrap();
        std::fs::rename(&backup, state).unwrap();
        RESTORED
    })
    .await
}

async fn recreated_slot(sql: &Client) -> String {
    scenario(sql, "recreated", async |fixture: &Fixture| {
        let name = &fixture.name;
        sql.execute("SELECT pg_catalog.pg_drop_replication_slot($1)", &[name])
            .await
            .unwrap();
        // A change nobody captured, then a fresh slot with the same name.
        sql.batch_execute(&format!(
            "INSERT INTO {name}.items VALUES (300, 'never captured')"
        ))
        .await
        .unwrap();
        sql.execute(
            "SELECT pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[name],
        )
        .await
        .unwrap();
        AHEAD_OF_JOURNAL
    })
    .await
}

async fn advanced_slot(sql: &Client) -> String {
    scenario(sql, "advanced", async |fixture: &Fixture| {
        let name = &fixture.name;
        sql.batch_execute(&format!(
            "INSERT INTO {name}.items VALUES (400, 'skipped by advance')"
        ))
        .await
        .unwrap();
        sql.execute(
            "SELECT pg_catalog.pg_replication_slot_advance($1, pg_catalog.pg_current_wal_lsn())",
            &[name],
        )
        .await
        .unwrap();
        AHEAD_OF_JOURNAL
    })
    .await
}

async fn truncated_journal(sql: &Client) -> String {
    scenario(sql, "journal", async |fixture: &Fixture| {
        // Materialized journal content is reclaimable. Only a durably journaled
        // transaction the ledger still needs makes the truncation a loss.
        fixture
            .journal_unpublished(
                sql,
                "BEGIN; TRUNCATE {name}.items; INSERT INTO {name}.items VALUES (600, 'after truncate'); COMMIT",
            )
            .await;
        let journal = fixture.config.state_dir.join("journal");
        let mut segments: Vec<_> = std::fs::read_dir(&journal)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "segment"))
            .collect();
        segments.sort();
        let last = segments.last().expect("journal has no segment");
        let length = std::fs::metadata(last).unwrap().len();
        assert!(length > 1, "final journal segment is empty");
        // Cut into the final frame: the ledger already recorded it as durable.
        std::fs::OpenOptions::new()
            .write(true)
            .open(last)
            .unwrap()
            .set_len(length - 1)
            .unwrap();
        LOST_JOURNAL
    })
    .await
}

async fn invalidated_slot(sql: &Client) -> String {
    let row = sql
        .query_one(
            "SELECT current_setting('server_version_num')::int, setting, unit, source FROM pg_catalog.pg_settings WHERE name = 'max_slot_wal_keep_size'",
            &[],
        )
        .await
        .unwrap();
    let (version, setting, unit, source): (i32, String, Option<String>, String) =
        (row.get(0), row.get(1), row.get(2), row.get(3));
    // Command-line settings take precedence over ALTER SYSTEM.
    assert_ne!(
        source, "command line",
        "max_slot_wal_keep_size is fixed on the server command line; configure it in postgresql.auto.conf instead"
    );
    let previous = (source != "default").then(|| format!("{setting}{}", unit.unwrap_or_default()));
    let configure = async |value: Option<&str>| {
        sql.batch_execute(&match value {
            Some(value) => format!("ALTER SYSTEM SET max_slot_wal_keep_size = '{value}'"),
            None => "ALTER SYSTEM RESET max_slot_wal_keep_size".into(),
        })
        .await
        .unwrap();
        sql.batch_execute("SELECT pg_catalog.pg_reload_conf()")
            .await
            .unwrap();
    };
    scenario(sql, "invalidated", async |fixture: &Fixture| {
        let name = &fixture.name;
        sql.batch_execute(&format!(
            "INSERT INTO {name}.items VALUES (500, 'in removed WAL')"
        ))
        .await
        .unwrap();
        configure(Some("1MB")).await;
        let mut lost = false;
        for _ in 0..40 {
            for statement in [
                format!(
                    "INSERT INTO {name}.items SELECT i, repeat('w', 512) FROM generate_series(10000, 10099) i ON CONFLICT (id) DO UPDATE SET status = excluded.status || 'x'"
                ),
                "SELECT pg_catalog.pg_switch_wal()".into(),
                "CHECKPOINT".into(),
            ] {
                sql.batch_execute(&statement).await.unwrap();
            }
            if slot(sql, name)
                .await
                .is_some_and(|(_, _, status)| status.as_deref() == Some("lost"))
            {
                lost = true;
                break;
            }
        }
        configure(previous.as_deref()).await;
        assert!(lost, "slot was not invalidated by removed WAL");
        if version >= 170_000 {
            let reason: Option<String> = sql
                .query_one(
                    "SELECT invalidation_reason FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[name],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(reason.as_deref(), Some("wal_removed"));
        }
        LOST_WAL
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL superuser session"]
async fn live_storage_loss_guards_fail_closed() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let mut refusals = Vec::new();
    refusals.push((
        "restored older state_dir",
        restored_older_state_dir(&sql).await,
    ));
    refusals.push(("dropped and recreated slot", recreated_slot(&sql).await));
    refusals.push(("slot advanced past the journal", advanced_slot(&sql).await));
    refusals.push((
        "journal truncated behind the ledger",
        truncated_journal(&sql).await,
    ));
    // Last: lowering the WAL budget can invalidate other lagging slots.
    refusals.push((
        "slot invalidated by removed WAL",
        invalidated_slot(&sql).await,
    ));
    for (case, error) in refusals {
        println!("{case}: {error}");
    }
    drop(sql);
    sql_task.await.unwrap().unwrap();
}
