use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId, Value};
use flow_state_store::{
    BufferResult, Change, CollapseBuffer, IndexDelta, OperationKind, OperationPhase,
    OperationRecord, PreparedOperation, StateStore, StateStoreOptions,
};
use tempfile::TempDir;

fn open(path: &std::path::Path) -> StateStore {
    open_with_apply_limits(path, 2, StateStoreOptions::default().apply_lookup_rows)
}

fn open_with_apply_limits(
    path: &std::path::Path,
    batch_rows: usize,
    lookup_rows: usize,
) -> StateStore {
    StateStore::open(
        path,
        StateStoreOptions {
            apply_batch_rows: batch_rows,
            apply_lookup_rows: lookup_rows,
            ..Default::default()
        },
    )
    .unwrap()
}
fn location(path: &str, position: u64, lsn: u64) -> RowLocation {
    RowLocation {
        data_file_id: FileId(path.into()),
        row_position: position,
        data_sequence_number: -1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(lsn),
        row_version: lsn,
        row_fingerprint: [position as u8; 16],
    }
}
fn operation(id: &str, base: Option<i64>, lsn: u64) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TableId(7),
        kind: OperationKind::Ingest,
        base_snapshot_id: base,
        last_lsn: PgLsn(lsn),
        schema_version: 1,
        artifacts: vec!["data.parquet".into()],
        payload: vec![1, 2],
    }
}
fn insert(key: u8, file: &str, pos: u64) -> IndexDelta {
    IndexDelta {
        key: PrimaryKey(vec![key]),
        expected: None,
        replacement: Some(location(file, pos, 10)),
    }
}
fn file_counts(store: &StateStore, snapshot: i64, files: &[&str]) -> Vec<u64> {
    store
        .file_live_row_counts(
            &TableId(7),
            Some(snapshot),
            &files
                .iter()
                .map(|file| FileId((*file).into()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
}

#[test]
fn prepared_delta_stream_owns_rows_and_stays_within_its_operation_after_reopen() {
    let temp = TempDir::new().unwrap();
    let expected = [insert(3, "s3://warehouse/é.parquet", 9), insert(1, "b", 2)];
    {
        let store = open(temp.path());
        for (table, name) in [(7, "op"), (8, "op-next"), (9, "empty")] {
            let mut prepared = operation(name, None, 10);
            prepared.table_id = TableId(table);
            store
                .prepare(
                    prepared,
                    if name == "empty" { &[][..] } else { &expected }
                        .iter()
                        .cloned(),
                )
                .unwrap();
        }
        let mut building = operation("building", None, 10);
        building.table_id = TableId(10);
        store.begin_prepare(building).unwrap();
        assert!(
            store
                .prepared_deltas(&OperationId("building".into()))
                .is_err()
        );
    }
    let rows = {
        let store = open(temp.path());
        let mut empty = store.prepared_deltas(&OperationId("empty".into())).unwrap();
        assert!(empty.next().is_none());
        assert!(empty.next().is_none());
        let mut stream = store.prepared_deltas(&OperationId("op".into())).unwrap();
        let rows = stream.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert!(stream.next().is_none());
        assert!(stream.next().is_none());
        rows
    };
    assert_eq!(rows, expected);
}

#[test]
fn recovery_resumes_atomic_index_batches_and_keeps_table_fenced() {
    let temp = TempDir::new().unwrap();
    let id = OperationId("op".into());
    let file = "s3://warehouse/é.parquet?signature=abc";
    {
        let store = open(temp.path());
        store
            .prepare(
                operation("op", None, 10),
                [
                    insert(1, file, 0),
                    insert(2, file, 2),
                    insert(3, file, 4),
                    insert(4, file, 3),
                    insert(5, "b", 0),
                ],
            )
            .unwrap();
        assert!(
            store
                .lookup(&TableId(7), &PrimaryKey(vec![1]))
                .unwrap()
                .is_none()
        );
        assert!(store.apply_committed(&id).is_err());
        assert!(store.forget_applied(&id).is_err());
    }
    {
        let store = open(temp.path());
        assert_eq!(
            store.pending_operations().unwrap()[0].phase,
            OperationPhase::Prepared
        );
        store.mark_committed(&id, 100, 4).unwrap();
    }
    {
        let store = open(temp.path());
        let result = store.apply_committed_batch(&id).unwrap();
        assert_eq!(result.applied_rows, 2);
        assert!(!result.complete);
        assert_eq!(
            store
                .file_rows(&TableId(7), &FileId(file.into()))
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(store.table_state(&TableId(7)).unwrap().snapshot_id, None);
        assert!(matches!(
            store.file_live_row_counts(&TableId(7), None, &[FileId(file.into())]),
            Err(flow_state_store::Error::SnapshotMismatch { .. })
        ));
        assert!(store.prepare(operation("later", None, 11), []).is_err());
    }
    {
        let store = open(temp.path());
        assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 2);
        // Resume in the same output file; the next positions are above its
        // existing maximum even though the batch itself is out of order.
        assert!(store.apply_committed(&id).unwrap().complete);
        assert_eq!(
            store.table_state(&TableId(7)).unwrap().snapshot_id,
            Some(100)
        );
        assert_eq!(
            store
                .lookup(&TableId(7), &PrimaryKey(vec![1]))
                .unwrap()
                .unwrap()
                .data_sequence_number,
            4
        );
        assert_eq!(store.apply_committed(&id).unwrap().applied_rows, 0);
        assert!(store.pending_operations().unwrap().is_empty());
        store.forget_applied(&id).unwrap();
        assert!(store.operation(&id).unwrap().is_none());
        assert!(
            store
                .lookup(&TableId(7), &PrimaryKey(vec![1]))
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store.file_rows(&TableId(7), &FileId(file.into())).count(),
            4
        );
        assert_eq!(
            file_counts(&store, 100, &[file, "b", file, "missing"]),
            [4, 1, 4, 0]
        );

        // A gap below the maximum is free, and an unchanged physical position
        // remains legal when its existing owner is the same key.
        let id = OperationId("gap".into());
        store
            .prepare(
                operation(&id.0, Some(100), 11),
                [
                    insert(6, file, 1),
                    IndexDelta {
                        key: PrimaryKey(vec![1]),
                        expected: store.lookup(&TableId(7), &PrimaryKey(vec![1])).unwrap(),
                        replacement: Some(location(file, 0, 11)),
                    },
                ],
            )
            .unwrap();
        store.mark_committed(&id, 101, 5).unwrap();
        assert_eq!(store.apply_committed(&id).unwrap().applied_rows, 2);
        assert_eq!(file_counts(&store, 101, &[file, "b"]), [5, 1]);

        // The first target is beyond the maximum, but the second is occupied.
        // Checking the actual minimum must retain whole-batch conflict behavior.
        let id = OperationId("overlap".into());
        store
            .prepare(
                operation(&id.0, Some(101), 12),
                [insert(7, file, 6), insert(8, file, 3)],
            )
            .unwrap();
        store.mark_committed(&id, 102, 6).unwrap();
    }
    {
        let store = open(temp.path());
        let id = OperationId("overlap".into());
        assert!(matches!(store.apply_committed_batch(&id),
            Err(flow_state_store::Error::InvalidState(reason))
            if reason == "two live keys claim the same physical row"));
        assert!(
            store
                .lookup(&TableId(7), &PrimaryKey(vec![7]))
                .unwrap()
                .is_none()
        );
        assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 0);
        assert_eq!(
            store.table_state(&TableId(7)).unwrap().snapshot_id,
            Some(101)
        );
        assert_eq!(
            store.file_rows(&TableId(7), &FileId(file.into())).count(),
            5
        );
    }
}

#[test]
fn applied_operation_retirement_is_bounded_and_atomic() {
    let temp = TempDir::new().unwrap();
    let store = open(temp.path());
    let cases = [
        ("applied-a", None, 100, 10, 1),
        ("applied-b", Some(100), 101, 11, 2),
        ("applied-c", Some(101), 102, 12, 3),
    ];
    for (name, base, snapshot, lsn, key) in cases {
        let id = OperationId(name.into());
        store
            .prepare(operation(name, base, lsn), [insert(key, name, 0)])
            .unwrap();
        store.mark_committed(&id, snapshot, snapshot).unwrap();
        assert!(store.apply_committed(&id).unwrap().complete);
    }

    let first = store.applied_operations(2).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.operation.id.0.as_str())
            .collect::<Vec<_>>(),
        ["applied-a", "applied-b"]
    );
    let all = cases
        .iter()
        .map(|(name, ..)| OperationId((*name).into()))
        .collect::<Vec<_>>();
    assert!(store.forget_applied_batch(&all).is_err());
    assert_eq!(store.applied_operations(3).unwrap().len(), 3);
    store
        .collapse_changes(
            "applied-a",
            [(
                TableId(7),
                PrimaryKey(vec![1]),
                Change::Update(vec![Value::Int32(2)]),
            )],
        )
        .unwrap();
    store.seal_transaction("applied-a").unwrap();
    assert_eq!(
        store.collapsed("applied-a", &TableId(7)).unwrap().count(),
        1
    );

    let first_ids = first
        .into_iter()
        .map(|record| record.operation.id)
        .collect::<Vec<_>>();
    store.forget_applied_batch(&first_ids).unwrap();
    assert!(store.operation(&first_ids[0]).unwrap().is_none());
    assert!(store.operation(&first_ids[1]).unwrap().is_none());
    assert!(store.collapsed("applied-a", &TableId(7)).is_err());
    store
        .prepare(
            operation("applied-a", Some(102), 13),
            [insert(1, "applied-a", 0)],
        )
        .unwrap();
    store.discard_uncommitted(&first_ids[0]).unwrap();
    assert_eq!(
        store.applied_operations(2).unwrap()[0].operation.id,
        OperationId("applied-c".into())
    );
}

#[test]
fn index_conflict_never_partially_applies_a_batch() {
    for reverse_conflict in [false, true] {
        let temp = TempDir::new().unwrap();
        let store = open(temp.path());
        let table = TableId(7);
        let id = OperationId("initial".into());
        store
            .prepare(operation("initial", None, 10), [insert(1, "a", 0)])
            .unwrap();
        store.mark_committed(&id, 1, 1).unwrap();
        store.apply_committed(&id).unwrap();
        let original = store.lookup(&table, &PrimaryKey(vec![1])).unwrap();
        let conflict = if reverse_conflict {
            insert(3, "a", 0)
        } else {
            IndexDelta {
                key: PrimaryKey(vec![1]),
                expected: Some(location("wrong", 0, 10)),
                replacement: None,
            }
        };
        let id = OperationId("conflict".into());
        store
            .prepare(
                operation("conflict", Some(1), 20),
                [insert(2, "b", 0), conflict],
            )
            .unwrap();
        store.mark_committed(&id, 2, 2).unwrap();
        drop(store);
        for _ in 0..2 {
            let store = open(temp.path());
            let error = store.apply_committed_batch(&id).unwrap_err();
            if reverse_conflict {
                assert!(
                    matches!(error, flow_state_store::Error::InvalidState(message)
                    if message == "two live keys claim the same physical row")
                );
            } else {
                assert!(matches!(
                    error,
                    flow_state_store::Error::IndexConflict { .. }
                ));
            }
            // Unsorted keys, duplicate requests and misses preserve input order.
            let keys = [3, 1, 2, 3].map(|key| PrimaryKey(vec![key]));
            assert_eq!(
                store.lookup_many(&table, &keys).unwrap(),
                [None, original.clone(), None, None]
            );
            assert_eq!(
                store
                    .file_rows(&table, &FileId("a".into()))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                [(0, PrimaryKey(vec![1]))]
            );
            assert_eq!(store.file_rows(&table, &FileId("b".into())).count(), 0);
            assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 0);
            let state = store.table_state(&table).unwrap();
            assert_eq!(state.snapshot_id, Some(1));
            assert_eq!(state.materialized_lsn, PgLsn(10));
            assert_eq!(state.pending_operation.as_ref(), Some(&id));
        }
    }
}

#[test]
fn lookup_chunks_advance_one_atomic_apply_cursor() {
    let temp = TempDir::new().unwrap();
    let store = open_with_apply_limits(temp.path(), 5, 2);
    let id = OperationId("chunked-insert".into());
    store
        .prepare(
            operation(&id.0, None, 10),
            (0..5).map(|row| insert(row, "a", row.into())),
        )
        .unwrap();
    store.mark_committed(&id, 1, 1).unwrap();

    let result = store.apply_committed_batch(&id).unwrap();
    assert_eq!(result.applied_rows, 5);
    assert!(result.complete);
    assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 5);
    assert_eq!(file_counts(&store, 1, &["a"]), [5]);
}

#[test]
fn later_lookup_chunk_owner_conflict_rolls_back_the_commit_group() {
    let temp = TempDir::new().unwrap();
    let store = open_with_apply_limits(temp.path(), 2, 1);
    let table = TableId(7);
    let seed = OperationId("owner-seed".into());
    store
        .prepare(operation(&seed.0, None, 10), [insert(1, "a", 0)])
        .unwrap();
    store.mark_committed(&seed, 1, 1).unwrap();
    store.apply_committed(&seed).unwrap();
    let original = store.lookup(&table, &PrimaryKey(vec![1])).unwrap().unwrap();

    let id = OperationId("owner-conflict".into());
    store
        .prepare(
            operation(&id.0, Some(1), 20),
            [
                IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: Some(original.clone()),
                    replacement: Some(location("b", 0, 20)),
                },
                insert(2, "a", 0),
            ],
        )
        .unwrap();
    store.mark_committed(&id, 2, 2).unwrap();

    assert!(matches!(
        store.apply_committed_batch(&id),
        Err(flow_state_store::Error::InvalidState(message))
            if message == "two live keys claim the same physical row"
    ));
    assert_eq!(
        store.lookup(&table, &PrimaryKey(vec![1])).unwrap(),
        Some(original)
    );
    assert_eq!(store.lookup(&table, &PrimaryKey(vec![2])).unwrap(), None);
    assert_eq!(store.file_rows(&table, &FileId("a".into())).count(), 1);
    assert_eq!(store.file_rows(&table, &FileId("b".into())).count(), 0);
    assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 0);
}

#[test]
fn file_count_underflow_is_checked_at_each_lookup_chunk_boundary() {
    let temp = TempDir::new().unwrap();
    let table = TableId(7);
    let seed = OperationId("count-boundary-seed".into());
    let (first, second) = {
        let store = open_with_apply_limits(temp.path(), 3, 2);
        store
            .prepare(
                operation(&seed.0, None, 10),
                [insert(1, "a", 0), insert(2, "a", 1)],
            )
            .unwrap();
        store.mark_committed(&seed, 1, 1).unwrap();
        store.apply_committed(&seed).unwrap();
        (
            store.lookup(&table, &PrimaryKey(vec![1])).unwrap(),
            store.lookup(&table, &PrimaryKey(vec![2])).unwrap(),
        )
    };
    let options = rocksdb::Options::default();
    let families = rocksdb::DB::list_cf(&options, temp.path()).unwrap();
    let db = rocksdb::DB::open_cf(&options, temp.path(), families).unwrap();
    let counts = db.cf_handle("file_live_rows").unwrap();
    let (key, _) = db
        .iterator_cf(&counts, rocksdb::IteratorMode::Start)
        .next()
        .unwrap()
        .unwrap();
    db.put_cf(&counts, key, 1_u64.to_be_bytes()).unwrap();
    drop(counts);
    drop(db);

    let store = open_with_apply_limits(temp.path(), 3, 2);
    let id = OperationId("count-boundary".into());
    store
        .prepare(
            operation(&id.0, Some(1), 20),
            [
                IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: first.clone(),
                    replacement: None,
                },
                IndexDelta {
                    key: PrimaryKey(vec![2]),
                    expected: second.clone(),
                    replacement: None,
                },
                insert(3, "a", 2),
            ],
        )
        .unwrap();
    store.mark_committed(&id, 2, 2).unwrap();

    // The full group nets -1 and would hide the corrupted count. Its first
    // two-row lookup chunk nets -2, so the shadow count detects the underflow.
    assert!(matches!(
        store.apply_committed_batch(&id),
        Err(flow_state_store::Error::RecoveryRequired(_))
    ));
    assert_eq!(store.lookup(&table, &PrimaryKey(vec![1])).unwrap(), first);
    assert_eq!(store.lookup(&table, &PrimaryKey(vec![2])).unwrap(), second);
    assert_eq!(store.lookup(&table, &PrimaryKey(vec![3])).unwrap(), None);
    assert_eq!(store.file_rows(&table, &FileId("a".into())).count(), 2);
    assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 0);
}

#[test]
fn checkpoint_restores_both_index_directions_and_delete_removes_reverse_row() {
    let temp = TempDir::new().unwrap();
    let store = open(&temp.path().join("db"));
    let id = OperationId("initial".into());
    store
        .prepare(operation("initial", None, 10), [insert(1, "a", 0)])
        .unwrap();
    store.mark_committed(&id, 1, 1).unwrap();
    store.apply_committed(&id).unwrap();
    store.checkpoint(temp.path().join("checkpoint")).unwrap();
    let checkpoint = open(&temp.path().join("checkpoint"));
    assert_eq!(
        checkpoint
            .file_rows(&TableId(7), &FileId("a".into()))
            .count(),
        1
    );
    let old = store.lookup(&TableId(7), &PrimaryKey(vec![1])).unwrap();
    let id = OperationId("delete".into());
    store
        .prepare(
            operation("delete", Some(1), 20),
            [IndexDelta {
                key: PrimaryKey(vec![1]),
                expected: old,
                replacement: None,
            }],
        )
        .unwrap();
    store.mark_committed(&id, 2, 2).unwrap();
    store.apply_committed(&id).unwrap();
    assert_eq!(store.file_rows(&TableId(7), &FileId("a".into())).count(), 0);
    assert_eq!(file_counts(&store, 2, &["a"]), [0]);
    assert_eq!(file_counts(&checkpoint, 1, &["a"]), [1]);
    assert!(matches!(
        store.file_live_row_counts(&TableId(7), Some(1), &[]),
        Err(flow_state_store::Error::SnapshotMismatch { .. })
    ));
    assert!(
        store
            .lookup(&TableId(7), &PrimaryKey(vec![1]))
            .unwrap()
            .is_none()
    );
    assert!(
        checkpoint
            .lookup(&TableId(7), &PrimaryKey(vec![1]))
            .unwrap()
            .is_some()
    );
}

#[test]
fn duplicate_keys_and_locations_are_rejected_within_and_between_staging_batches() {
    for staging in [
        "same-batch",
        "later-batch",
        "later-call",
        "after-reopen",
        "monotone-and-out-of-order-after-reopen",
    ] {
        for duplicate_key in [false, true] {
            let temp = TempDir::new().unwrap();
            let mut store = open(temp.path());
            let id = OperationId("dup".into());
            store.begin_prepare(operation("dup", None, 10)).unwrap();
            let duplicate = if duplicate_key {
                insert(1, "b", 0)
            } else {
                insert(6, "a", 0)
            };
            let first = [insert(1, "a", 0), insert(2, "a", 1)];
            let result = match staging {
                "same-batch" => store.stage_deltas(&id, [insert(1, "a", 0), duplicate]),
                "later-batch" => store.stage_deltas(&id, first.into_iter().chain([duplicate])),
                "later-call" => {
                    store.stage_deltas(&id, first).unwrap();
                    store.stage_deltas(&id, [duplicate])
                }
                "after-reopen" => {
                    store.stage_deltas(&id, first).unwrap();
                    drop(store);
                    store = open(temp.path());
                    store.stage_deltas(&id, [duplicate])
                }
                "monotone-and-out-of-order-after-reopen" => {
                    store.stage_deltas(&id, first).unwrap();
                    drop(store);
                    store = open(temp.path());
                    // Both marker families advance beyond their persisted max.
                    store
                        .stage_deltas(&id, [insert(4, "a", 3), insert(5, "a", 4)])
                        .unwrap();
                    // Fresh holes and earlier file paths still need admission;
                    // neither input order nor a high previous max is a fence.
                    store
                        .stage_deltas(&id, [insert(3, "a", 2), insert(0, "0", 0)])
                        .unwrap();
                    // The duplicate lies below the max of its own family while
                    // the other family contains only a fresh, greater marker.
                    store.stage_deltas(&id, [duplicate])
                }
                _ => unreachable!(),
            };
            let expected = if duplicate_key {
                "duplicate primary key in prepared delta"
            } else {
                "duplicate output row location"
            };
            assert!(
                matches!(result, Err(flow_state_store::Error::InvalidState(reason)) if reason == expected),
                "{staging}, duplicate_key={duplicate_key}"
            );
            let operation = store.operation(&id).unwrap().unwrap();
            assert_eq!(operation.phase, OperationPhase::Building);
            assert_eq!(
                operation.delta_count,
                match staging {
                    "same-batch" => 0,
                    "monotone-and-out-of-order-after-reopen" => 6,
                    _ => 2,
                }
            );
            store.discard_uncommitted(&id).unwrap();
            assert!(
                store
                    .table_state(&TableId(7))
                    .unwrap()
                    .pending_operation
                    .is_none()
            );
        }
    }
}

#[test]
fn fallible_staging_retains_completed_batches_and_can_retry_the_failed_chunk() {
    let temp = TempDir::new().unwrap();
    let store = open(temp.path());
    let id = OperationId("fallible".into());
    store.begin_prepare(operation(&id.0, None, 10)).unwrap();
    let result = store.stage_deltas_fallible(
        &id,
        [
            Ok(insert(1, "a", 0)),
            Ok(insert(2, "a", 1)),
            Ok(insert(3, "a", 2)),
            Err(flow_state_store::Error::InvalidState(
                "injected input failure".into(),
            )),
        ],
    );
    assert!(result.is_err());
    assert_eq!(store.operation(&id).unwrap().unwrap().delta_count, 2);
    store.stage_deltas(&id, [insert(3, "a", 2)]).unwrap();
    store.seal_prepare(&id, vec![], vec![]).unwrap();
    store.mark_committed(&id, 1, 1).unwrap();
    assert_eq!(store.apply_committed(&id).unwrap().applied_rows, 3);
    assert_eq!(store.file_rows(&TableId(7), &FileId("a".into())).count(), 3);
}

#[test]
fn spool_collapses_streamed_changes_across_reopen_and_keeps_original_location() {
    let temp = TempDir::new().unwrap();
    let table = TableId(7);
    let key = PrimaryKey(vec![1]);
    {
        let store = open(temp.path());
        store
            .prepare(operation("initial", None, 10), [insert(1, "a", 0)])
            .unwrap();
        store
            .mark_committed(&OperationId("initial".into()), 1, 1)
            .unwrap();
        store
            .apply_committed(&OperationId("initial".into()))
            .unwrap();
        store
            .collapse_changes(
                "txn",
                [(table, key.clone(), Change::Update(vec![Value::Int32(2)]))],
            )
            .unwrap();
    }
    {
        let store = open(temp.path());
        store
            .collapse_changes(
                "txn",
                [
                    (table, key.clone(), Change::Delete),
                    (table, key.clone(), Change::Insert(vec![Value::Int32(3)])),
                ],
            )
            .unwrap();
        assert!(
            store
                .collapse_changes(
                    "txn",
                    [
                        (table, key.clone(), Change::Update(vec![Value::Int32(9)])),
                        (
                            table,
                            PrimaryKey(vec![2]),
                            Change::Update(vec![Value::Int32(8)]),
                        ),
                    ],
                )
                .is_err()
        );
        store.seal_transaction("txn").unwrap();
        let rows = store
            .collapsed("txn", &table)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row, Some(vec![Value::Int32(3)]));
        assert_eq!(
            rows[0].original.as_ref().unwrap().data_file_id,
            FileId("a".into())
        );
        assert!(
            store
                .collapse_changes("txn", [(table, key, Change::Delete)])
                .is_err()
        );
        store.discard_transaction("txn").unwrap();
        assert!(store.collapsed("txn", &table).is_err());
    }
}

#[test]
fn abort_noop_and_position_delete_sort_are_disk_backed() {
    let temp = TempDir::new().unwrap();
    let store = open(temp.path());
    let table = TableId(7);
    store
        .collapse_changes(
            "cancel",
            [
                (
                    table,
                    PrimaryKey(vec![1]),
                    Change::Insert(vec![Value::Int32(1)]),
                ),
                (table, PrimaryKey(vec![1]), Change::Delete),
            ],
        )
        .unwrap();
    store.seal_transaction("cancel").unwrap();
    assert_eq!(store.collapsed("cancel", &table).unwrap().count(), 0);
    store
        .put_position_deletes(
            "delete",
            &table,
            [location("z", 1, 0), location("aa", 2, 0)],
        )
        .unwrap();
    store
        .put_position_deletes(
            "delete",
            &table,
            [location("a", 9, 0), location("aa", 2, 0)],
        )
        .unwrap();
    let locations: Vec<_> = store
        .position_deletes("delete", &table)
        .map(|row| {
            let row = row.unwrap();
            (row.data_file_id.0, row.row_position)
        })
        .collect();
    assert_eq!(
        locations,
        [("a".into(), 9), ("aa".into(), 2), ("z".into(), 1)]
    );
    store.discard_transaction("delete").unwrap();
    assert_eq!(store.position_deletes("delete", &table).count(), 0);
}

#[test]
fn abrupt_process_exit_preserves_synced_prepare_and_apply_cursor() {
    for phase in ["prepared", "committed", "applying", "applied"] {
        let temp = TempDir::new().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env("FLOW_CRASH_DB", temp.path())
            .env("FLOW_CRASH_PHASE", phase)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93));
        let store = open(temp.path());
        let id = OperationId("crash".into());
        let operation = store.operation(&id).unwrap().unwrap();
        match phase {
            "prepared" => assert_eq!(operation.phase, OperationPhase::Prepared),
            "committed" => assert_eq!(operation.applied_count, 0),
            "applying" => assert_eq!(operation.applied_count, 2),
            "applied" => {
                assert_eq!(operation.phase, OperationPhase::Applied);
                assert_eq!(operation.applied_count, 3);
                assert_eq!(
                    store.table_state(&TableId(7)).unwrap().materialized_lsn,
                    PgLsn(10)
                );
            }
            _ => unreachable!(),
        }
        if phase == "prepared" {
            store.mark_committed(&id, 3, 3).unwrap();
        }
        store.apply_committed(&id).unwrap();
        assert_eq!(store.file_rows(&TableId(7), &FileId("a".into())).count(), 3);
        assert_eq!(file_counts(&store, 3, &["a"]), [3]);
        assert!(
            store
                .table_state(&TableId(7))
                .unwrap()
                .pending_operation
                .is_none()
        );
    }
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("FLOW_CRASH_DB") else {
        return;
    };
    let phase = std::env::var("FLOW_CRASH_PHASE").unwrap();
    let store = open(std::path::Path::new(&path));
    let id = OperationId("crash".into());
    store
        .prepare(
            operation("crash", None, 10),
            [insert(1, "a", 0), insert(2, "a", 1), insert(3, "a", 2)],
        )
        .unwrap();
    if phase != "prepared" {
        store.mark_committed(&id, 3, 3).unwrap();
    }
    if phase == "applying" {
        store.apply_committed_batch(&id).unwrap();
    }
    if phase == "applied" {
        store.apply_committed(&id).unwrap();
    }
    // Bypass Drop and RocksDB close; only the already-synced WAL may recover.
    std::process::exit(93);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn disk_spool_matches_a_transaction_after_random_mutations(events in proptest::collection::vec((0u8..8, proptest::bool::ANY), 1..80)) {
        let temp = TempDir::new().unwrap();
        let mut store = open(temp.path());
        let mut memory = CollapseBuffer::new(TableId(7), 32 << 20);
        let mut expected = std::collections::BTreeMap::new();
        for (index, events) in events.chunks(2).enumerate() {
            let changes: Vec<_> = events.iter().enumerate().map(|(offset, &(key, delete))| {
                let exists = expected.contains_key(&key);
                let change = if delete && exists {
                    expected.remove(&key); Change::Delete
                } else {
                    let row = vec![Value::Int32((index * 2 + offset) as i32)];
                    expected.insert(key, row.clone());
                    if exists { Change::Update(row) } else { Change::Insert(row) }
                };
                (TableId(7), PrimaryKey(vec![key]), change)
            }).collect();
            for (table, key, change) in &changes {
                assert_eq!(*table, TableId(7));
                assert!(matches!(memory.push(key.clone(), change.clone()).unwrap(), BufferResult::Ready(())));
            }
            store.collapse_changes("random", changes).unwrap();
            if index % 17 == 0 { drop(store); store = open(temp.path()); }
        }
        store.seal_transaction("random").unwrap();
        let disk: Vec<_> = store.collapsed("random", &TableId(7)).unwrap().collect::<Result<_, _>>().unwrap();
        let BufferResult::Ready(memory) = store.bind_collapse_buffer(memory, &store.table_state(&TableId(7)).unwrap()).unwrap() else { panic!("small fold must fit") };
        assert_eq!(memory.collect::<Vec<_>>(), disk);
        let actual: std::collections::BTreeMap<_, _> = disk.into_iter().map(|entry| {
            assert!(entry.original.is_none());
            (entry.key.0[0], entry.row.unwrap())
        }).collect();
        proptest::prop_assert_eq!(actual, expected);
    }
}

#[test]
fn memory_collapse_keeps_key_move_tombstones_originals_and_full_state_fence() {
    let temp = TempDir::new().unwrap();
    let store = open(temp.path());
    let table = TableId(7);
    store
        .prepare(
            operation("initial", None, 10),
            (1..=3).map(|id| insert(id, "a", u64::from(id))),
        )
        .unwrap();
    store
        .mark_committed(&OperationId("initial".into()), 1, 1)
        .unwrap();
    store
        .apply_committed(&OperationId("initial".into()))
        .unwrap();
    let expected = store.table_state(&table).unwrap();
    let changes = [
        (1, Change::Update(vec![Value::Int32(11)])),
        (2, Change::Delete),
        (3, Change::Delete),
        (5, Change::Insert(vec![Value::Int32(35)])),
        (5, Change::Delete),
        (3, Change::Insert(vec![Value::Int32(33)])),
        (1, Change::Delete),
        (1, Change::Insert(vec![Value::Int32(12)])),
        (4, Change::Insert(vec![Value::Int32(40)])),
        (4, Change::Update(vec![Value::Int32(44)])),
    ];
    let mut memory = CollapseBuffer::new(table, 32 << 20);
    for batch in changes.chunks(2) {
        for (key, change) in batch {
            assert!(matches!(
                memory.push(PrimaryKey(vec![*key]), change.clone()).unwrap(),
                BufferResult::Ready(())
            ));
        }
        store
            .collapse_changes(
                "moves",
                batch
                    .iter()
                    .map(|(key, change)| (table, PrimaryKey(vec![*key]), change.clone())),
            )
            .unwrap();
    }
    store.seal_transaction("moves").unwrap();
    let disk = store
        .collapsed("moves", &table)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let BufferResult::Ready(memory) = store.bind_collapse_buffer(memory, &expected).unwrap() else {
        panic!("small fold must fit")
    };
    assert_eq!(memory.table_state(), &expected);
    assert_eq!(memory.collect::<Vec<_>>(), disk);
    assert_eq!(
        disk.iter().map(|entry| entry.key.0[0]).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    for entry in &disk[..3] {
        assert_eq!(entry.original, store.lookup(&table, &entry.key).unwrap());
    }
    assert!(disk[1].row.is_none());
    assert!(disk[3].original.is_none());
    assert_eq!(store.table_state(&table).unwrap(), expected);

    // Final absent→absent must still reject an initially occupied key.
    let mut invalid = CollapseBuffer::new(table, 32 << 20);
    invalid
        .push(PrimaryKey(vec![1]), Change::Insert(vec![]))
        .unwrap();
    invalid.push(PrimaryKey(vec![1]), Change::Delete).unwrap();
    assert!(store.bind_collapse_buffer(invalid, &expected).is_err());
    let mut invalid = CollapseBuffer::new(table, 32 << 20);
    invalid.push(PrimaryKey(vec![2]), Change::Delete).unwrap();
    assert!(
        invalid
            .push(PrimaryKey(vec![2]), Change::Update(vec![]))
            .is_err()
    );

    // The physical snapshot is unchanged across both watermark-only changes.
    for (lsn, schema) in [(20, 1), (20, 2)] {
        let prior = store.table_state(&table).unwrap();
        store.complete_noop(&table, PgLsn(lsn), schema).unwrap();
        assert_eq!(
            store.table_state(&table).unwrap().snapshot_id,
            prior.snapshot_id
        );
        assert!(
            store
                .bind_collapse_buffer(CollapseBuffer::new(table, 32 << 20), &prior)
                .is_err()
        );
    }
    store
        .begin_prepare(operation("fence", Some(1), 30))
        .unwrap();
    assert!(
        store
            .bind_collapse_buffer(
                CollapseBuffer::new(table, 32 << 20),
                &store.table_state(&table).unwrap()
            )
            .is_err()
    );
}

#[test]
fn memory_collapse_accounts_owned_capacity_and_late_binding_overflow_replays_on_disk() {
    let temp = TempDir::new().unwrap();
    let table = TableId(7);
    let store = open(temp.path());
    let mut wide = location(&"p".repeat(16 << 10), 2, 10);
    // The seed commit materializes sequence1, replacing the writer placeholder.
    wide.data_sequence_number = 1;
    wide.partition = vec![3; 16 << 10];
    store
        .prepare(
            operation("initial", None, 10),
            [
                insert(1, "a", 0),
                insert(2, "a", 1),
                IndexDelta {
                    key: PrimaryKey(vec![3]),
                    expected: None,
                    replacement: Some(wide.clone()),
                },
            ],
        )
        .unwrap();
    store
        .mark_committed(&OperationId("initial".into()), 1, 1)
        .unwrap();
    store
        .apply_committed(&OperationId("initial".into()))
        .unwrap();
    let expected = store.table_state(&table).unwrap();

    let mut reserved_row = Vec::with_capacity(1024);
    reserved_row.push(Value::Null);
    let mut reserved_text = String::with_capacity(32 << 10);
    reserved_text.push('x');
    let mut reserved_binary = Vec::with_capacity(32 << 10);
    reserved_binary.push(1);
    let mut reserved_key = Vec::with_capacity(32 << 10);
    reserved_key.push(9);
    for (key, row) in [
        (PrimaryKey(vec![9]), reserved_row),
        (PrimaryKey(vec![9]), vec![Value::String(reserved_text)]),
        (PrimaryKey(vec![9]), vec![Value::Binary(reserved_binary)]),
        (PrimaryKey(reserved_key), vec![]),
    ] {
        let mut memory = CollapseBuffer::new(table, 16 << 10);
        assert!(matches!(
            memory.push(key, Change::Insert(row)).unwrap(),
            BufferResult::CapacityExceeded
        ));
        // Ignoring overflow cannot turn the discarded prefix into valid input.
        assert!(matches!(
            memory
                .push(PrimaryKey(vec![10]), Change::Insert(vec![]))
                .unwrap(),
            BufferResult::CapacityExceeded
        ));
        assert!(matches!(
            store.bind_collapse_buffer(memory, &expected).unwrap(),
            BufferResult::CapacityExceeded
        ));
    }

    // Replacing an image releases its capacity charge, not just its string length.
    let mut memory = CollapseBuffer::new(table, 16 << 10);
    for key in [9, 10] {
        let mut value = String::with_capacity(6144);
        value.push('x');
        assert!(matches!(
            memory
                .push(
                    PrimaryKey(vec![key]),
                    Change::Insert(vec![Value::String(value)])
                )
                .unwrap(),
            BufferResult::Ready(())
        ));
        if key == 9 {
            assert!(matches!(
                memory
                    .push(PrimaryKey(vec![9]), Change::Update(vec![Value::Int32(1)]))
                    .unwrap(),
                BufferResult::Ready(())
            ));
        }
    }
    let BufferResult::Ready(rows) = store.bind_collapse_buffer(memory, &expected).unwrap() else {
        panic!("replacement must release budget")
    };
    assert!(rows.peak_accounted_bytes() <= 16 << 10);
    assert_eq!(rows.count(), 2);

    let mut memory = CollapseBuffer::new(table, 32 << 10);
    for key in 1..=3 {
        assert!(matches!(
            memory
                .push(
                    PrimaryKey(vec![key]),
                    Change::Update(vec![Value::Int32(99)])
                )
                .unwrap(),
            BufferResult::Ready(())
        ));
    }
    // Batch one binds keys1/2; key3's path AND partition exceed the retained cap.
    assert!(matches!(
        store.bind_collapse_buffer(memory, &expected).unwrap(),
        BufferResult::CapacityExceeded
    ));
    assert_eq!(store.table_state(&table).unwrap(), expected);
    assert_eq!(
        store.lookup(&table, &PrimaryKey(vec![3])).unwrap(),
        Some(wide)
    );
    drop(store);

    let store = open(temp.path());
    assert_eq!(store.table_state(&table).unwrap(), expected);
    for keys in [vec![1, 2], vec![3]] {
        store
            .collapse_changes(
                "replay",
                keys.into_iter().map(|key| {
                    (
                        table,
                        PrimaryKey(vec![key]),
                        Change::Update(vec![Value::Int32(99)]),
                    )
                }),
            )
            .unwrap();
    }
    store.seal_transaction("replay").unwrap();
    let replay = store
        .collapsed("replay", &table)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(replay.len(), 3);
    for entry in replay {
        assert_eq!(entry.original, store.lookup(&table, &entry.key).unwrap());
        assert_eq!(entry.row, Some(vec![Value::Int32(99)]));
    }
    assert_eq!(store.table_state(&table).unwrap(), expected);
}

#[test]
fn live_file_counts_follow_key_moves_rewrites_and_obsolete_cas_across_reopen() {
    let temp = TempDir::new().unwrap();
    let store = open(temp.path());
    let table = TableId(7);
    let id = OperationId("count-initial".into());
    store
        .prepare(
            operation(&id.0, None, 10),
            [insert(1, "a", 0), insert(2, "a", 1), insert(3, "a", 2)],
        )
        .unwrap();
    store.mark_committed(&id, 1, 1).unwrap();
    store.apply_committed(&id).unwrap();
    let original: Vec<_> = (1..=3)
        .map(|key| {
            store
                .lookup(&table, &PrimaryKey(vec![key]))
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(file_counts(&store, 1, &["a", "b"]), [3, 0]);

    let id = OperationId("count-mutations".into());
    store
        .prepare(
            operation(&id.0, Some(1), 20),
            [
                IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: Some(original[0].clone()),
                    replacement: Some(location("b", 0, 20)),
                },
                IndexDelta {
                    key: PrimaryKey(vec![3]),
                    expected: Some(original[2].clone()),
                    replacement: None,
                },
                IndexDelta {
                    key: PrimaryKey(vec![4]),
                    expected: None,
                    replacement: Some(location("b", 1, 20)),
                },
            ],
        )
        .unwrap();
    store.mark_committed(&id, 2, 2).unwrap();
    store.apply_committed(&id).unwrap();
    assert_eq!(file_counts(&store, 2, &["a", "b"]), [1, 2]);

    // Updating the row version in the same file must cancel its count delta.
    let id = OperationId("count-same-file".into());
    store
        .prepare(
            operation(&id.0, Some(2), 30),
            [IndexDelta {
                key: PrimaryKey(vec![1]),
                expected: store.lookup(&table, &PrimaryKey(vec![1])).unwrap(),
                replacement: Some(location("b", 0, 30)),
            }],
        )
        .unwrap();
    store.mark_committed(&id, 3, 3).unwrap();
    store.apply_committed(&id).unwrap();
    assert_eq!(file_counts(&store, 3, &["a", "b"]), [1, 2]);

    let id = OperationId("count-rewrite".into());
    let mut rewrite = operation(&id.0, Some(3), 30);
    rewrite.kind = OperationKind::Rewrite;
    store
        .prepare(
            rewrite,
            [
                IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: Some(original[0].clone()),
                    // The stale row is ignored before checking this position,
                    // which is still owned by key 4 in the pre-batch index.
                    replacement: Some(location("b", 1, 10)),
                },
                IndexDelta {
                    key: PrimaryKey(vec![2]),
                    expected: Some(original[1].clone()),
                    replacement: Some(location("d", 1, 10)),
                },
                IndexDelta {
                    key: PrimaryKey(vec![4]),
                    expected: store.lookup(&table, &PrimaryKey(vec![4])).unwrap(),
                    replacement: Some(location("d", 2, 20)),
                },
            ],
        )
        .unwrap();
    store.mark_committed(&id, 4, 4).unwrap();
    let applied = store.apply_committed(&id).unwrap();
    assert_eq!((applied.applied_rows, applied.obsolete_rows), (2, 1));
    assert_eq!(store.apply_committed(&id).unwrap().applied_rows, 0);
    assert_eq!(file_counts(&store, 4, &["a", "b", "d"]), [0, 1, 2]);
    drop(store);

    let store = open(temp.path());
    assert_eq!(file_counts(&store, 4, &["d", "a", "b", "d"]), [2, 0, 1, 2]);
    for file in ["a", "b", "d"] {
        let rows = store
            .file_rows(&table, &FileId(file.into()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(file_counts(&store, 4, &[file]), [rows.len() as u64]);
    }
}

#[test]
fn malformed_or_overflowing_counts_do_not_partially_apply_index_changes() {
    use flow_state_store::Error;
    for (name, bytes, remove) in [
        ("malformed", vec![1], true),
        ("underflow", 0_u64.to_be_bytes().to_vec(), true),
        ("overflow", u64::MAX.to_be_bytes().to_vec(), false),
    ] {
        let temp = TempDir::new().unwrap();
        let store = open(temp.path());
        let id = OperationId("count-corrupt-initial".into());
        store
            .prepare(operation(&id.0, None, 10), [insert(1, "a", 0)])
            .unwrap();
        store.mark_committed(&id, 1, 1).unwrap();
        store.apply_committed(&id).unwrap();
        let original = store.lookup(&TableId(7), &PrimaryKey(vec![1])).unwrap();
        drop(store);
        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, temp.path()).unwrap();
        let db = rocksdb::DB::open_cf(&options, temp.path(), families).unwrap();
        let cf = db.cf_handle("file_live_rows").unwrap();
        let (key, _) = db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .next()
            .unwrap()
            .unwrap();
        db.put_cf(&cf, key, bytes).unwrap();
        drop(cf);
        drop(db);
        let store = open(temp.path());
        if name == "malformed" {
            assert!(matches!(
                store.file_live_row_counts(&TableId(7), Some(1), &[FileId("a".into())]),
                Err(Error::RecoveryRequired(_))
            ));
        }
        let id = OperationId(format!("count-{name}"));
        let change = if remove {
            IndexDelta {
                key: PrimaryKey(vec![1]),
                expected: original.clone(),
                replacement: None,
            }
        } else {
            insert(2, "a", 1)
        };
        store
            .prepare(operation(&id.0, Some(1), 20), [change])
            .unwrap();
        store.mark_committed(&id, 2, 2).unwrap();
        assert!(matches!(
            store.apply_committed(&id),
            Err(Error::RecoveryRequired(_))
        ));
        assert_eq!(store.operation(&id).unwrap().unwrap().applied_count, 0);
        assert_eq!(
            store.lookup(&TableId(7), &PrimaryKey(vec![1])).unwrap(),
            original
        );
        assert!(
            store
                .lookup(&TableId(7), &PrimaryKey(vec![2]))
                .unwrap()
                .is_none()
        );
        assert_eq!(store.file_rows(&TableId(7), &FileId("a".into())).count(), 1);
    }
}

#[test]
fn corrupt_persisted_apply_cursors_fail_closed_after_reopen() {
    use flow_state_store::Error;

    for (case, applied_count) in [("at-end", 3), ("past-end", 4)] {
        let temp = TempDir::new().unwrap();
        let id = OperationId(format!("corrupt-cursor-{case}"));
        let store = open(temp.path());
        store
            .prepare(
                operation(&id.0, None, 10),
                [insert(1, "a", 0), insert(2, "a", 1), insert(3, "a", 2)],
            )
            .unwrap();
        store.mark_committed(&id, 1, 1).unwrap();
        let table_before = store.table_state(&TableId(7)).unwrap();
        drop(store);

        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, temp.path()).unwrap();
        let db = rocksdb::DB::open_cf(&options, temp.path(), families).unwrap();
        let operations = db.cf_handle("prepared_operations").unwrap();
        let encoded = db.get_cf(&operations, id.0.as_bytes()).unwrap().unwrap();
        let mut record: OperationRecord = bincode::deserialize(&encoded).unwrap();
        assert_eq!(record.delta_count, 3);
        record.applied_count = applied_count;
        db.put_cf(
            &operations,
            id.0.as_bytes(),
            bincode::serialize(&record).unwrap(),
        )
        .unwrap();
        db.flush_wal(true).unwrap();
        drop(operations);
        drop(db);

        let error = match StateStore::open(temp.path(), StateStoreOptions::default()) {
            Ok(_) => panic!("corrupt operation cursor was accepted during reopen"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::RecoveryRequired(_)));

        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, temp.path()).unwrap();
        let db = rocksdb::DB::open_cf(&options, temp.path(), families).unwrap();
        let operations = db.cf_handle("prepared_operations").unwrap();
        let encoded = db.get_cf(&operations, id.0.as_bytes()).unwrap().unwrap();
        let record: OperationRecord = bincode::deserialize(&encoded).unwrap();
        assert_eq!(record.phase, OperationPhase::Committed);
        assert_eq!(record.applied_count, applied_count);
        let table: flow_state_store::TableState = bincode::deserialize(
            &db.get_cf(
                &db.cf_handle("table_state").unwrap(),
                TableId(7).0.to_be_bytes(),
            )
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        assert_eq!(table, table_before);
    }
}

#[test]
fn applied_delta_stream_accepts_complete_payload_then_atomic_pruning() {
    let temp = TempDir::new().unwrap();
    let id = OperationId("pruned-deltas".into());
    let deltas = vec![insert(1, "a", 0), insert(2, "a", 1)];
    let store = open(temp.path());
    store
        .prepare(operation(&id.0, None, 10), deltas.clone())
        .unwrap();
    store.mark_committed(&id, 1, 1).unwrap();
    store.apply_committed(&id).unwrap();
    assert_eq!(
        store
            .prepared_deltas(&id)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        deltas
    );
    store.prune_applied_deltas(&id).unwrap();
    drop(store);
    let store = open(temp.path());
    store.validate_storage().unwrap();
    assert_eq!(store.operation(&id).unwrap().unwrap().delta_count, 2);
    assert!(store.prepared_deltas(&id).unwrap().next().is_none());
}
