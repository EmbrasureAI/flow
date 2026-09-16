//! A slow snapshot reader must not inherit an application's query time limit.
use flow_model::PgLsn;
use flow_pg_source::{
    SnapshotSession,
    tokio_postgres::{IsolationLevel, NoTls, binary_copy::BinaryCopyOutStream, types::Type},
};
use futures::TryStreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn copy_outlives_source_statement_timeout_without_changing_session_default() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (mut keeper, keeper_connection) = flow_pg_source::tokio_postgres::connect(&url, NoTls)
        .await
        .unwrap();
    let keeper_task = tokio::spawn(keeper_connection);
    let (mut client, connection) = flow_pg_source::tokio_postgres::connect(&url, NoTls)
        .await
        .unwrap();
    let client_task = tokio::spawn(connection);
    let name = format!(
        "snapshot_timeout_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    keeper
        .batch_execute(&format!(
            "CREATE TABLE {name} (id integer PRIMARY KEY, payload text NOT NULL); \
         INSERT INTO {name} SELECT n, repeat(md5(n::text), 2048) FROM generate_series(1, 1024) n"
        ))
        .await
        .unwrap();
    let export = keeper
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .unwrap();
    let snapshot_name: String = export
        .query_one("SELECT pg_export_snapshot()", &[])
        .await
        .unwrap()
        .get(0);
    client
        .batch_execute("SET statement_timeout = '500ms'")
        .await
        .unwrap();
    let snapshot = SnapshotSession::import(&mut client, "unused", PgLsn(0), &snapshot_name)
        .await
        .unwrap();
    let output = snapshot
        .copy_table("public", &name, &["id".into(), "payload".into()])
        .await
        .unwrap();
    let stream = BinaryCopyOutStream::new(output, &[Type::INT4, Type::TEXT]);
    tokio::pin!(stream);
    // 64 MiB exceeds the bounded transport/socket buffers. Stop consuming so
    // PostgreSQL remains in COPY past the inherited 500 ms query deadline.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut rows = 0;
    while let Some(row) = stream.as_mut().try_next().await.unwrap() {
        assert_eq!(row.get::<String>(1).len(), 65536);
        rows += 1;
    }
    assert_eq!(rows, 1024);
    snapshot.finish().await.unwrap();
    assert_eq!(
        client
            .query_one("SHOW statement_timeout", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "500ms"
    );
    let error = client
        .query_one("SELECT pg_sleep(1)", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&flow_pg_source::tokio_postgres::error::SqlState::QUERY_CANCELED)
    );
    export.commit().await.unwrap();
    keeper
        .batch_execute(&format!("DROP TABLE {name}"))
        .await
        .unwrap();
    drop(client);
    drop(keeper);
    client_task.await.unwrap().unwrap();
    keeper_task.await.unwrap().unwrap();
}
