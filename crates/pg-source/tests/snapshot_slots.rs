//! Slot ownership across snapshot initialization, using disposable PostgreSQL.
use flow_pg_source::{
    Error, export_snapshot, export_temporary_snapshot,
    tokio_postgres::{Client, Config, NoTls, config::ReplicationMode, error::SqlState},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

fn config() -> Config {
    std::env::var("FLOW_POSTGRES_URL")
        .expect("FLOW_POSTGRES_URL")
        .parse()
        .unwrap()
}

async fn connect(config: &Config) -> (Client, JoinHandle<()>) {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let task = tokio::spawn(async move { connection.await.unwrap() });
    (client, task)
}

fn slot_name() -> String {
    format!(
        "snapshot_slots_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn slot_state(client: &Client, slot: &str) -> Option<(bool, bool)> {
    client
        .query_opt(
            "SELECT active, temporary FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .map(|row| (row.get(0), row.get(1)))
}

async fn cleanup(client: &Client, slot: &str) {
    client
        .execute(
            "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
}

fn assert_cross_database_error(error: &Error) {
    let Error::Postgres(error) = error else {
        panic!("expected PostgreSQL import error: {error:?}");
    };
    assert_eq!(error.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED));
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "cannot import a snapshot from a different database"
    );
}

#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to disposable PostgreSQL with access to postgres and template1"]
async fn failed_import_removes_permanent_slot_and_allows_retry() {
    let mut config = config();
    let (sql, sql_task) = connect(&config).await;
    let database: String = sql
        .query_one("SELECT current_database()", &[])
        .await
        .unwrap()
        .get(0);
    let mut other = config.clone();
    other.dbname(if database == "postgres" {
        "template1"
    } else {
        "postgres"
    });
    let (mut importer, importer_task) = connect(&other).await;
    config.replication_mode(ReplicationMode::Logical);
    let (replication, replication_task) = connect(&config).await;
    let slot = slot_name();

    let error = export_snapshot(&replication, &mut importer, &slot)
        .await
        .err()
        .unwrap();
    let before_disconnect = slot_state(&sql, &slot).await;
    drop(replication);
    replication_task.await.unwrap();
    let after_disconnect = slot_state(&sql, &slot).await;

    let (replication, replication_task) = connect(&config).await;
    let retry_error = export_snapshot(&replication, &mut importer, &slot)
        .await
        .err()
        .unwrap();
    drop(replication);
    replication_task.await.unwrap();
    let after_retry = slot_state(&sql, &slot).await;
    // Also clean up on the unfixed implementation before asserting the regression.
    cleanup(&sql, &slot).await;
    drop(importer);
    drop(sql);
    importer_task.await.unwrap();
    sql_task.await.unwrap();

    eprintln!(
        "import={error:?}; before_disconnect={before_disconnect:?}; after_disconnect={after_disconnect:?}; retry={retry_error:?}"
    );
    assert_cross_database_error(&error);
    assert_eq!(
        before_disconnect, None,
        "failed import leaked a permanent slot"
    );
    assert_eq!(after_disconnect, None, "disconnect left a permanent slot");
    assert_cross_database_error(&retry_error);
    assert_eq!(after_retry, None);
}

#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to disposable PostgreSQL"]
async fn successful_import_retains_slot_and_duplicate_attempt_does_not_drop_it() {
    let mut config = config();
    let (sql, sql_task) = connect(&config).await;
    let (mut importer, importer_task) = connect(&config).await;
    config.replication_mode(ReplicationMode::Logical);
    let (replication, replication_task) = connect(&config).await;
    let slot = slot_name();

    let snapshot = export_snapshot(&replication, &mut importer, &slot)
        .await
        .unwrap();
    assert_eq!(snapshot.slot, slot);
    assert!(snapshot.consistent_lsn.0 > 0);
    assert_eq!(slot_state(&sql, &slot).await, Some((false, false)));
    snapshot.finish().await.unwrap();
    drop(replication);
    replication_task.await.unwrap();
    assert_eq!(slot_state(&sql, &slot).await, Some((false, false)));

    let (replication, replication_task) = connect(&config).await;
    let error = export_snapshot(&replication, &mut importer, &slot)
        .await
        .err()
        .unwrap();
    drop(replication);
    replication_task.await.unwrap();
    let retained = slot_state(&sql, &slot).await;
    cleanup(&sql, &slot).await;
    drop(importer);
    drop(sql);
    importer_task.await.unwrap();
    sql_task.await.unwrap();

    let Error::Postgres(error) = error else {
        panic!("expected duplicate slot error")
    };
    assert_eq!(error.code(), Some(&SqlState::DUPLICATE_OBJECT));
    assert_eq!(
        retained,
        Some((false, false)),
        "pre-existing slot was dropped"
    );
}

#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to disposable PostgreSQL with access to postgres and template1"]
async fn failed_import_leaves_temporary_slot_to_session_cleanup() {
    let mut config = config();
    let (sql, sql_task) = connect(&config).await;
    let database: String = sql
        .query_one("SELECT current_database()", &[])
        .await
        .unwrap()
        .get(0);
    let mut other = config.clone();
    other.dbname(if database == "postgres" {
        "template1"
    } else {
        "postgres"
    });
    let (mut importer, importer_task) = connect(&other).await;
    config.replication_mode(ReplicationMode::Logical);
    let (replication, replication_task) = connect(&config).await;
    let slot = slot_name();

    let error = export_temporary_snapshot(&replication, &mut importer, &slot)
        .await
        .err()
        .unwrap();
    let before_disconnect = slot_state(&sql, &slot).await;
    drop(replication);
    replication_task.await.unwrap();
    // A completed client task need not mean the server has processed Terminate.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while slot_state(&sql, &slot).await.is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(importer);
    drop(sql);
    importer_task.await.unwrap();
    sql_task.await.unwrap();

    assert_cross_database_error(&error);
    assert_eq!(before_disconnect, Some((true, true)));
}
