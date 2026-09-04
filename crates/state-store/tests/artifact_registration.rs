use flow_model::{OperationId, PgLsn, TableId};
use flow_state_store::{
    ControlStore, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use tempfile::TempDir;

#[test]
fn owned_records_survive_index_loss_and_recovery_writes_fence_the_old_generation() {
    let temp = TempDir::new().unwrap();
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let index = control
        .initialize_index(temp.path().join("index"), StateStoreOptions::default())
        .unwrap();
    let operation = PreparedOperation {
        id: OperationId("upload".into()),
        table_id: TableId(9),
        kind: OperationKind::Ingest,
        base_snapshot_id: None,
        last_lsn: PgLsn(10),
        schema_version: 0,
        artifacts: vec![],
        payload: vec![],
    };
    index
        .begin_prepare_with_record(operation.clone(), Some((b"owned/range", b"reserve-64")))
        .unwrap();
    assert_eq!(
        control
            .source_transaction(b"owned/range")
            .unwrap()
            .as_deref(),
        Some(b"reserve-64".as_slice())
    );
    assert!(
        control
            .register_recovery_record(&operation.id, b"owned/metadata", b"attempt")
            .is_err()
    );
    index
        .seal_prepare_with_record(
            &operation.id,
            vec!["file".into()],
            vec![1],
            Some((b"owned/range", b"final-3")),
        )
        .unwrap();
    assert_eq!(
        control
            .source_transaction(b"owned/range")
            .unwrap()
            .as_deref(),
        Some(b"final-3".as_slice())
    );
    control
        .register_recovery_record(&operation.id, b"owned/metadata", b"attempt")
        .unwrap();
    assert!(index.complete_noop(&TableId(3), PgLsn(11), 0).is_err());
    drop(index);
    std::fs::remove_dir_all(temp.path().join("index")).unwrap();
    control.resolve_operation(&operation.id, None).unwrap();
    assert!(
        control
            .register_recovery_record(&operation.id, b"owned/late", b"unsafe")
            .is_err()
    );
    drop(control);
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    assert_eq!(
        control
            .source_transaction(b"owned/range")
            .unwrap()
            .as_deref(),
        Some(b"final-3".as_slice())
    );
    assert_eq!(
        control
            .source_transaction(b"owned/metadata")
            .unwrap()
            .as_deref(),
        Some(b"attempt".as_slice())
    );
    assert!(control.source_transaction(b"owned/late").unwrap().is_none());
    assert!(
        StateStore::open_with_control(
            temp.path().join("index"),
            StateStoreOptions::default(),
            control
        )
        .is_err()
    );
}
