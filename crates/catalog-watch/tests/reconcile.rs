use flow_catalog_watch::{
    Classification, PauseReason, SnapshotChange, SnapshotOperation, classify, reconcile_rewrite,
    validate_delete_rewrite,
};
use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId};
use flow_state_store::{
    IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use std::collections::BTreeSet;
use tempfile::TempDir;

fn location(file: &str, pos: u64, fingerprint: u8) -> RowLocation {
    RowLocation {
        data_file_id: FileId(file.into()),
        row_position: pos,
        data_sequence_number: 1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(10),
        row_version: 1,
        row_fingerprint: [fingerprint; 16],
    }
}
fn op(id: &str, kind: OperationKind, base: Option<i64>) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TableId(1),
        kind,
        base_snapshot_id: base,
        last_lsn: PgLsn(10),
        schema_version: 1,
        artifacts: vec![],
        payload: vec![],
    }
}
fn populate(store: &StateStore) {
    store
        .prepare(
            op("load", OperationKind::Ingest, None),
            [
                IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: None,
                    replacement: Some(location("old", 0, 1)),
                },
                IndexDelta {
                    key: PrimaryKey(vec![2]),
                    expected: None,
                    replacement: Some(location("old", 1, 2)),
                },
            ],
        )
        .unwrap();
    store
        .mark_committed(&OperationId("load".into()), 1, 1)
        .unwrap();
    store.apply_committed(&OperationId("load".into())).unwrap();
}
#[test]
fn physical_rewrite_can_reorder_rows_and_preserves_logical_versions() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
    populate(&store);
    let added = [
        Ok((PrimaryKey(vec![2]), location("new", 0, 2))),
        Ok((PrimaryKey(vec![1]), location("new", 1, 1))),
    ];
    reconcile_rewrite(
        &store,
        op("rewrite", OperationKind::Reconcile, Some(1)),
        2,
        2,
        &BTreeSet::from([FileId("old".into())]),
        &BTreeSet::from([FileId("new".into())]),
        added,
    )
    .unwrap();
    assert_eq!(
        store.file_rows(&TableId(1), &FileId("old".into())).count(),
        0
    );
    assert_eq!(
        store.file_rows(&TableId(1), &FileId("new".into())).count(),
        2
    );
    let row = store
        .lookup(&TableId(1), &PrimaryKey(vec![1]))
        .unwrap()
        .unwrap();
    assert_eq!(row.row_position, 1);
    assert_eq!(row.source_commit_lsn, PgLsn(10));
    assert_eq!(row.row_version, 1);
}
#[test]
fn incomplete_changed_and_duplicate_external_rows_leave_publication_fenced() {
    for rows in [
        vec![(PrimaryKey(vec![1]), location("new", 0, 1))],
        vec![
            (PrimaryKey(vec![1]), location("new", 0, 9)),
            (PrimaryKey(vec![2]), location("new", 1, 2)),
        ],
        vec![
            (PrimaryKey(vec![1]), location("new", 0, 1)),
            (PrimaryKey(vec![1]), location("new", 1, 1)),
        ],
    ] {
        let temp = TempDir::new().unwrap();
        let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
        populate(&store);
        assert!(
            reconcile_rewrite(
                &store,
                op("bad", OperationKind::Reconcile, Some(1)),
                2,
                2,
                &BTreeSet::from([FileId("old".into())]),
                &BTreeSet::from([FileId("new".into())]),
                rows.into_iter().map(Ok)
            )
            .is_err()
        );
        assert_eq!(store.table_state(&TableId(1)).unwrap().snapshot_id, Some(1));
        assert!(
            store
                .table_state(&TableId(1))
                .unwrap()
                .pending_operation
                .is_some()
        );
        assert_eq!(
            store.file_rows(&TableId(1), &FileId("old".into())).count(),
            2
        );
    }
}
#[test]
fn unknown_snapshots_require_evidence_not_only_replace_label() {
    let mut change = SnapshotChange {
        snapshot_id: 2,
        parent_snapshot_id: Some(1),
        operation: SnapshotOperation::Replace,
        service_operation: None,
        added_data: BTreeSet::new(),
        removed_data: BTreeSet::new(),
        added_deletes: BTreeSet::new(),
        removed_deletes: BTreeSet::new(),
        schema_changed: false,
        spec_changed: false,
    };
    assert_eq!(
        classify(&change, Some(1), false),
        Classification::MetadataOnly
    );
    change.removed_data.insert(FileId("a".into()));
    assert_eq!(
        classify(&change, Some(1), false),
        Classification::ReconcileDataRewrite
    );
    assert_eq!(
        classify(&change, Some(0), false),
        Classification::Pause(PauseReason::MissingHistory)
    );
    change.service_operation = Some(OperationId("someone-else".into()));
    assert_eq!(
        classify(&change, Some(1), false),
        Classification::Pause(PauseReason::UnknownServiceOperation)
    );
}
#[test]
fn delete_maintenance_must_preserve_the_effective_set() {
    let a = (FileId("a".into()), 1);
    let b = (FileId("b".into()), 2);
    validate_delete_rewrite([a.clone(), a.clone(), b.clone()], [a.clone(), b.clone()]).unwrap();
    assert!(validate_delete_rewrite([a.clone()], [a.clone(), b.clone()]).is_err());
    assert!(validate_delete_rewrite([b.clone(), a.clone()], [a, b]).is_err());
}
