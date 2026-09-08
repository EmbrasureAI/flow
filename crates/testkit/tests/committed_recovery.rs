use flow_coordinator::TablePublisher;
use flow_iceberg_ext::RowDeltaAction;
use flow_materializer::{DataWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, SourceId, Value};
use flow_state_store::{Change, OperationPhase, StateStore, StateStoreOptions};
use flow_testkit::{LostResponseCatalog, catalog, collapse_fixture, scan, schema, table};
use iceberg::spec::{SnapshotReference, SnapshotRetention};
use iceberg::{Catalog, TableCommit, TableUpdate};
use std::sync::Arc;

#[tokio::test]
async fn committed_recovery_requires_current_ancestry_before_applying_the_index() {
    let temp = tempfile::tempdir().unwrap();
    let index_path = temp.path().join("index");
    let store = StateStore::open(&index_path, StateStoreOptions::default()).unwrap();
    let catalog = Arc::new(LostResponseCatalog::new(
        catalog(&temp.path().join("lake")).await,
    ));
    let schema = schema(1);
    let empty = table(catalog.as_ref(), &schema).await;
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let mut head = empty;
    let mut pending = None;
    let mut base = None;
    for lsn in [10, 20] {
        let row = vec![Value::Int64(lsn), Value::String(format!("row-{lsn}"))];
        let (epoch, rows) = collapse_fixture(
            &store,
            &head,
            &schema,
            SourceId("recovery".into()),
            PgLsn(lsn as u64),
            [(schema.encode_key(&row).unwrap(), Change::Insert(row))],
        );
        if lsn == 20 {
            base = head.metadata().current_snapshot_id();
            catalog.lose_next_response_and_disconnect();
            assert!(publisher.publish(&head, &schema, rows).await.is_err());
            catalog.reconnect();
            head = catalog.load_table(head.identifier()).await.unwrap();
            let snapshot = head.metadata().current_snapshot().unwrap();
            store
                .mark_committed(
                    &epoch.id,
                    snapshot.snapshot_id(),
                    snapshot.sequence_number(),
                )
                .unwrap();
            pending = Some(epoch.id);
        } else {
            publisher.publish(&head, &schema, rows).await.unwrap();
            head = catalog.load_table(head.identifier()).await.unwrap();
        }
    }
    let operation = pending.unwrap();
    let committed = head.metadata().current_snapshot_id().unwrap();
    drop(publisher);
    drop(store);
    let store = StateStore::open(&index_path, StateStoreOptions::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let move_head = |snapshot| {
        TableCommit::builder()
            .ident(head.identifier().clone())
            .requirements(vec![])
            .updates(vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".into(),
                reference: SnapshotReference::new(
                    snapshot,
                    SnapshotRetention::branch(None, None, None),
                ),
            }])
            .build()
    };
    let rolled_back = catalog
        .update_table(move_head(base.unwrap()))
        .await
        .unwrap();
    let before = store.table_state(&schema.table_id).unwrap();
    assert!(
        publisher
            .recover(&rolled_back, &operation)
            .await
            .unwrap_err()
            .to_string()
            .contains("current-branch history")
    );
    assert_eq!(
        store.operation(&operation).unwrap().unwrap().phase,
        OperationPhase::Committed
    );
    assert_eq!(store.table_state(&schema.table_id).unwrap(), before);
    assert_eq!(scan(&rolled_back, &schema).await.unwrap().len(), 1);

    // A newer descendant is valid recovery evidence; an exact-head check would reject it.
    let restored = catalog.update_table(move_head(committed)).await.unwrap();
    let mut writer = DataWriter::new(
        restored.file_io().clone(),
        restored.metadata().location(),
        &OperationId("descendant".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer
        .write(
            &[vec![Value::Int64(30), Value::String("later".into())]],
            PgLsn(30),
        )
        .await
        .unwrap();
    let descendant = RowDeltaAction::new(&restored, "descendant")
        .add_data_files(writer.close().await.unwrap())
        .commit(catalog.as_ref(), &restored)
        .await
        .unwrap()
        .table;
    assert_eq!(
        publisher.recover(&descendant, &operation).await.unwrap(),
        Some(committed)
    );
    assert_eq!(
        store
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
    assert_eq!(
        store.operation(&operation).unwrap().unwrap().phase,
        OperationPhase::Applied
    );
}
