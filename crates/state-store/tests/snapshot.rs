use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId};
use flow_state_store::{
    Error, IndexDelta, IndexSnapshot, OperationKind, PreparedOperation, RowIndex, StateStore,
    StateStoreOptions, TableState,
};
use tempfile::TempDir;

const TABLE: TableId = TableId(7);

fn open(path: &std::path::Path) -> StateStore {
    StateStore::open(
        path,
        StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        },
    )
    .unwrap()
}

fn key(value: u8) -> PrimaryKey {
    PrimaryKey(vec![value])
}

fn location(file: &str, position: u64, lsn: u64) -> RowLocation {
    RowLocation {
        data_file_id: FileId(file.into()),
        row_position: position,
        data_sequence_number: -1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(lsn),
        row_version: lsn,
        row_fingerprint: [position as u8; 16],
    }
}

fn operation(id: &str, base: Option<i64>, lsn: u64, schema: u32) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TABLE,
        kind: OperationKind::Ingest,
        base_snapshot_id: base,
        last_lsn: PgLsn(lsn),
        schema_version: schema,
        artifacts: vec![],
        payload: vec![],
    }
}

fn file_rows(index: &impl RowIndex, file: &str) -> Vec<(u64, PrimaryKey)> {
    index
        .file_rows(&TABLE, &FileId(file.into()))
        .collect::<Result<_, _>>()
        .unwrap()
}

fn seed_table(store: &StateStore) -> TableState {
    let initial = operation("seed", None, 10, 1);
    store
        .prepare(
            initial.clone(),
            [IndexDelta {
                key: key(1),
                expected: None,
                replacement: Some(location("a", 0, 10)),
            }],
        )
        .unwrap();
    store.mark_committed(&initial.id, 100, 1).unwrap();
    store.apply_committed(&initial.id).unwrap();
    store.table_state(&TABLE).unwrap()
}

#[test]
fn exact_prepare_fences_the_admitted_table_state_with_its_source_record() {
    let directory = TempDir::new().unwrap();
    let store = open(directory.path());
    let expected = seed_table(&store);
    let mut rewrite = operation("rewrite", Some(100), 10, 1);
    rewrite.kind = OperationKind::Rewrite;

    store
        .begin_prepare_exact(
            rewrite.clone(),
            &expected,
            Some((b"owned/rewrite", b"reservation")),
        )
        .unwrap();

    let mut fenced = expected;
    fenced.pending_operation = Some(rewrite.id.clone());
    assert_eq!(store.table_state(&TABLE).unwrap(), fenced);
    let record = store.operation(&rewrite.id).unwrap().unwrap();
    assert_eq!(record.operation, rewrite);
    assert_eq!(record.phase, flow_state_store::OperationPhase::Building);
    assert_eq!(
        store
            .source_transaction(b"owned/rewrite")
            .unwrap()
            .as_deref(),
        Some(b"reservation".as_slice())
    );
}

#[test]
fn exact_prepare_rejects_a_noop_lsn_advance_without_partial_writes() {
    let directory = TempDir::new().unwrap();
    let store = open(directory.path());
    let expected = seed_table(&store);
    let expected_rows = store.lookup_many(&TABLE, &[key(1)]).unwrap();
    store.complete_noop(&TABLE, PgLsn(20), 1).unwrap();
    let current = store.table_state(&TABLE).unwrap();
    assert_eq!(current.snapshot_id, expected.snapshot_id);
    assert_ne!(current.materialized_lsn, expected.materialized_lsn);

    let mut rewrite = operation("stale-rewrite", Some(100), 10, 1);
    rewrite.kind = OperationKind::Rewrite;
    assert!(matches!(
        store.begin_prepare_exact(
            rewrite.clone(),
            &expected,
            Some((b"owned/stale", b"reservation")),
        ),
        Err(Error::ExactStateMismatch { table }) if table == TABLE
    ));

    assert_eq!(store.table_state(&TABLE).unwrap(), current);
    assert!(store.operation(&rewrite.id).unwrap().is_none());
    assert!(store.source_transaction(b"owned/stale").unwrap().is_none());
    assert_eq!(store.lookup_many(&TABLE, &[key(1)]).unwrap(), expected_rows);
    assert_eq!(file_rows(&store, "a"), [(0, key(1))]);
    assert_eq!(
        store
            .file_live_row_counts(&TABLE, Some(100), &[FileId("a".into())])
            .unwrap(),
        [1]
    );
}

#[test]
fn snapshot_reads_survive_partial_cdc_apply_and_capture_noop_watermarks() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>(value: T) -> T {
        value
    }
    assert_send_sync::<IndexSnapshot<'_>>();

    let directory = TempDir::new().unwrap();
    let final_state;
    let final_rows;
    {
        let store = open(directory.path());
        let initial = operation("initial", None, 10, 1);
        store
            .prepare(
                initial.clone(),
                (1..=3).map(|id| IndexDelta {
                    key: key(id),
                    expected: None,
                    replacement: Some(location("a", u64::from(id - 1), 10)),
                }),
            )
            .unwrap();
        store.mark_committed(&initial.id, 100, 1).unwrap();
        store.apply_committed(&initial.id).unwrap();

        let original = store
            .lookup_many(&TABLE, &[key(1), key(2), key(3)])
            .unwrap();
        let snapshot = store.index_snapshot(&TABLE, Some(100)).unwrap();
        assert_eq!(snapshot.table_id(), TABLE);
        assert_eq!(
            snapshot.table_state(),
            &TableState {
                snapshot_id: Some(100),
                materialized_lsn: PgLsn(10),
                schema_version: 1,
                pending_operation: None,
            }
        );
        assert_eq!(
            snapshot
                .file_live_row_counts(
                    &[FileId("a".into()), FileId("b".into()), FileId("a".into()),]
                )
                .unwrap(),
            [3, 0, 3]
        );
        let old_file = FileId("a".into());
        let mut reverse = assert_send(snapshot.file_rows(&TABLE, &old_file));
        assert_eq!(reverse.next().unwrap().unwrap(), (0, key(1)));

        // One update, one delete and a primary-key move cross two atomic apply
        // batches while the compactor retains its original read view.
        let next = operation("cdc", Some(100), 20, 2);
        store
            .prepare(
                next.clone(),
                [
                    IndexDelta {
                        key: key(1),
                        expected: original[0].clone(),
                        replacement: Some(location("b", 0, 20)),
                    },
                    IndexDelta {
                        key: key(2),
                        expected: original[1].clone(),
                        replacement: None,
                    },
                    IndexDelta {
                        key: key(3),
                        expected: original[2].clone(),
                        replacement: None,
                    },
                    IndexDelta {
                        key: key(4),
                        expected: None,
                        replacement: Some(location("b", 1, 20)),
                    },
                ],
            )
            .unwrap();
        store.mark_committed(&next.id, 200, 2).unwrap();
        assert!(!store.apply_committed_batch(&next.id).unwrap().complete);
        assert_eq!(file_rows(&store, "a"), [(2, key(3))]);
        assert!(matches!(
            store.file_live_row_counts(
                &TABLE,
                Some(100),
                &[FileId("a".into()), FileId("b".into())]
            ),
            Err(Error::SnapshotMismatch {
                table: TABLE,
                expected: Some(100),
                actual: Some(100),
                pending: Some(id),
            }) if id == next.id
        ));
        assert_eq!(
            snapshot
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into())])
                .unwrap(),
            [3, 0]
        );
        assert!(matches!(
            store.index_snapshot(&TABLE, Some(100)),
            Err(Error::SnapshotMismatch { pending: Some(id), .. }) if id == next.id
        ));
        assert_eq!(
            snapshot
                .lookup_many(&TABLE, &[key(3), key(1), key(2), key(1), key(4)])
                .unwrap(),
            [
                original[2].clone(),
                original[0].clone(),
                original[1].clone(),
                original[0].clone(),
                None
            ]
        );
        assert!(file_rows(&snapshot, "b").is_empty());

        assert!(store.apply_committed(&next.id).unwrap().complete);
        assert!(matches!(
            store.index_snapshot(&TABLE, Some(100)),
            Err(Error::SnapshotMismatch {
                actual: Some(200),
                pending: None,
                ..
            })
        ));
        assert_eq!(
            reverse.collect::<Result<Vec<_>, _>>().unwrap(),
            [(1, key(2)), (2, key(3))]
        );
        assert_eq!(
            file_rows(&snapshot, "a"),
            [(0, key(1)), (1, key(2)), (2, key(3))]
        );
        assert_eq!(
            snapshot
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into())])
                .unwrap(),
            [3, 0]
        );
        let current = store.index_snapshot(&TABLE, Some(200)).unwrap();
        assert!(file_rows(&current, "a").is_empty());
        assert_eq!(file_rows(&current, "b"), [(0, key(1)), (1, key(4))]);
        assert_eq!(
            current
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into())])
                .unwrap(),
            [0, 2]
        );

        store.complete_noop(&TABLE, PgLsn(30), 3).unwrap();
        let after_noop = store.index_snapshot(&TABLE, Some(200)).unwrap();
        assert_eq!(snapshot.table_state().materialized_lsn, PgLsn(10));
        assert_eq!(current.table_state().materialized_lsn, PgLsn(20));
        assert_eq!(current.table_state().schema_version, 2);
        assert_eq!(after_noop.table_state().materialized_lsn, PgLsn(30));
        assert_eq!(after_noop.table_state().schema_version, 3);
        assert_eq!(
            snapshot
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into())])
                .unwrap(),
            [3, 0]
        );
        assert_eq!(
            after_noop
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into())])
                .unwrap(),
            [0, 2]
        );
        final_state = after_noop.table_state().clone();
        final_rows = after_noop
            .lookup_many(&TABLE, &[key(1), key(2), key(3), key(4)])
            .unwrap();
    }

    let store = open(directory.path());
    let reopened = store.index_snapshot(&TABLE, Some(200)).unwrap();
    assert_eq!(reopened.table_state(), &final_state);
    assert_eq!(
        reopened
            .lookup_many(&TABLE, &[key(1), key(2), key(3), key(4)])
            .unwrap(),
        final_rows
    );
    assert!(file_rows(&reopened, "a").is_empty());
    assert_eq!(file_rows(&reopened, "b"), [(0, key(1)), (1, key(4))]);
}
