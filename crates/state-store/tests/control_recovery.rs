use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId};
use flow_state_store::{
    ControlStore, Error, IndexDelta, OperationKind, OperationPhase, OperationRecord,
    PreparedOperation, StateStore, StateStoreOptions,
};
use std::path::Path;
use tempfile::TempDir;

fn options() -> StateStoreOptions {
    StateStoreOptions {
        apply_batch_rows: 2,
        ..Default::default()
    }
}
fn operation(name: &str, base: Option<i64>, lsn: u64) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(name.into()),
        table_id: TableId(7),
        kind: OperationKind::Ingest,
        base_snapshot_id: base,
        last_lsn: PgLsn(lsn),
        schema_version: 1,
        artifacts: vec![format!("{name}.parquet")],
        payload: vec![42],
    }
}
fn delta(file: &str, key: u8, lsn: u64) -> IndexDelta {
    IndexDelta {
        key: PrimaryKey(vec![key]),
        expected: None,
        replacement: Some(RowLocation {
            data_file_id: FileId(file.into()),
            row_position: u64::from(key),
            data_sequence_number: -1,
            spec_id: 0,
            partition: vec![],
            source_commit_lsn: PgLsn(lsn),
            row_version: lsn,
            row_fingerprint: [key; 16],
        }),
    }
}
fn publish(index: &StateStore, name: &str, snapshot: i64, lsn: u64) {
    let id = OperationId(name.into());
    index
        .prepare(
            operation(name, None, lsn),
            [
                delta(name, 1, lsn),
                delta(name, 2, lsn),
                delta(name, 3, lsn),
            ],
        )
        .unwrap();
    index.mark_committed(&id, snapshot, snapshot).unwrap();
    index.apply_committed(&id).unwrap();
    index.forget_applied(&id).unwrap();
}
fn initialize(root: &Path) -> (ControlStore, StateStore) {
    let control = ControlStore::open(root.join("control")).unwrap();
    let index = control
        .initialize_index(root.join("index"), options())
        .unwrap();
    index
        .put_source_transaction(b"bootstrap", b"source-identity/snapshot/schema")
        .unwrap();
    index
        .update_source_ledger(
            (b"watermark", b"10"),
            Some((
                b"transaction/20".as_slice(),
                b"durable-journal-range".as_slice(),
            )),
            None,
        )
        .unwrap();
    publish(&index, "first", 1, 10);
    index.complete_noop(&TableId(8), PgLsn(10), 1).unwrap();
    (control, index)
}
fn child(root: &Path, phase: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "control_crash_child", "--nocapture"])
        .env("FLOW_CONTROL_TEST_ROOT", root)
        .env("FLOW_CONTROL_TEST_PHASE", phase)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(93));
}

#[test]
fn control_survives_process_exit_and_total_index_loss_at_catalog_boundaries() {
    for phase in ["prepared", "committed", "applying", "applied"] {
        let temp = TempDir::new().unwrap();
        child(temp.path(), phase);
        std::fs::remove_dir_all(temp.path().join("index")).unwrap();
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        assert!(matches!(
            StateStore::open_with_control(temp.path().join("index"), options(), control.clone()),
            Err(Error::RecoveryRequired(_))
        ));
        assert!(!temp.path().join("index").exists());
        assert_eq!(
            control.source_transaction(b"bootstrap").unwrap().unwrap(),
            b"source-identity/snapshot/schema"
        );
        assert_eq!(
            control
                .source_transaction(b"transaction/20")
                .unwrap()
                .unwrap(),
            b"durable-journal-range"
        );
        let expected_lsn = if phase == "prepared" { 10 } else { 20 };
        if phase != "applied" {
            let pending = control.pending_operations().unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].operation.payload, [42]);
            assert_eq!(
                pending[0].phase,
                if phase == "prepared" {
                    OperationPhase::Prepared
                } else {
                    OperationPhase::Committed
                }
            );
            control
                .resolve_operation(
                    &OperationId("second".into()),
                    if phase != "prepared" {
                        Some((2, 2))
                    } else {
                        None
                    },
                )
                .unwrap();
        }
        assert_eq!(
            control
                .table_state(&TableId(7))
                .unwrap()
                .unwrap()
                .materialized_lsn,
            PgLsn(expected_lsn)
        );
        assert!(control.table_state(&TableId(999)).unwrap().is_none());
        let replacement = StateStore::open(temp.path().join("rebuilt"), options()).unwrap();
        publish(
            &replacement,
            "catalog-rows",
            if phase == "prepared" { 1 } else { 2 },
            expected_lsn,
        );
        // Missing a no-op table must not silently substitute watermark zero.
        assert!(control.activate_rebuilt(&replacement).is_err());
        replacement
            .complete_noop(&TableId(8), PgLsn(10), 1)
            .unwrap();
        let selected = control.activate_rebuilt(&replacement).unwrap();
        drop(replacement);
        drop(control);
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        assert_eq!(control.active_generation().unwrap().unwrap(), selected);
        let index = StateStore::open_with_control(&selected.path, options(), control).unwrap();
        assert_eq!(
            index.table_state(&TableId(7)).unwrap().materialized_lsn,
            PgLsn(expected_lsn)
        );
        assert_eq!(
            index.source_transaction(b"watermark").unwrap().unwrap(),
            b"10"
        );
        assert_eq!(
            index
                .file_rows(&TableId(7), &FileId("catalog-rows".into()))
                .count(),
            3
        );
        assert_eq!(
            index
                .file_live_row_counts(
                    &TableId(7),
                    Some(if phase == "prepared" { 1 } else { 2 }),
                    &[FileId("catalog-rows".into())]
                )
                .unwrap(),
            [3]
        );
        index
            .update_source_ledger(
                (b"watermark", b"20"),
                None,
                Some((b"transaction/", b"transaction0")),
            )
            .unwrap();
        assert!(
            index
                .source_transaction(b"transaction/20")
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn incompatible_indexes_cannot_be_used_or_adopted_as_new_authority() {
    for revision_delta in [-1_i64, 0, 1] {
        let temp = TempDir::new().unwrap();
        let (control, index) = initialize(temp.path());
        let revision = control.active_generation().unwrap().unwrap().revision;
        drop(index);
        let path = temp.path().join("index");
        let families = rocksdb::DB::list_cf(&rocksdb::Options::default(), &path).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), &path, families).unwrap();
        let mut sync = rocksdb::WriteOptions::default();
        sync.set_sync(true);
        db.put_opt(
            b"control-revision",
            revision
                .checked_add_signed(revision_delta)
                .unwrap()
                .to_be_bytes(),
            &sync,
        )
        .unwrap();
        if revision_delta == 0 {
            db.put_opt(b"format-version", [1], &sync).unwrap();
        }
        drop(db);
        assert!(matches!(
            StateStore::open_with_control(&path, options(), control.clone()),
            Err(Error::RecoveryRequired(_))
        ));
        assert!(
            control
                .initialize_index(temp.path().join("empty"), options())
                .is_err()
        );
        assert_eq!(
            control
                .table_state(&TableId(7))
                .unwrap()
                .unwrap()
                .materialized_lsn,
            PgLsn(10)
        );
    }
}

#[test]
fn missing_control_cannot_promote_a_replaceable_index_to_authority() {
    let temp = TempDir::new().unwrap();
    let (control, index) = initialize(temp.path());
    drop(index);
    drop(control);
    std::fs::remove_dir_all(temp.path().join("control")).unwrap();
    let empty = ControlStore::open(temp.path().join("control")).unwrap();
    assert!(matches!(
        empty.initialize_index(temp.path().join("index"), options()),
        Err(Error::RecoveryRequired(_))
    ));
    assert!(empty.active_generation().unwrap().is_none());
}

#[test]
fn checkpoint_activation_is_restart_safe_before_and_after_pointer_switch() {
    for phase in ["candidate-ready", "activated"] {
        let temp = TempDir::new().unwrap();
        let (control, index) = initialize(temp.path());
        let checkpoint = control
            .checkpoint(&index, temp.path().join("checkpoint"))
            .unwrap();
        assert_eq!(checkpoint.tables.len(), 2);
        assert!(checkpoint.pending_operations.is_empty());
        let old = control.active_generation().unwrap().unwrap();
        drop(index);
        drop(control);
        child(temp.path(), phase);
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        let active = control.active_generation().unwrap().unwrap();
        assert_eq!(active.path == old.path, phase == "candidate-ready");
        assert!(old.path.join("CURRENT").exists());
        let index =
            StateStore::open_with_control(&active.path, options(), control.clone()).unwrap();
        assert_eq!(
            index
                .file_rows(&TableId(7), &FileId("first".into()))
                .count(),
            3
        );
        assert_eq!(
            index
                .file_live_row_counts(&TableId(7), Some(1), &[FileId("first".into())])
                .unwrap(),
            [3]
        );
        assert_eq!(
            index.source_transaction(b"bootstrap").unwrap().unwrap(),
            b"source-identity/snapshot/schema"
        );
        assert_eq!(
            control.checkpoints().unwrap().as_slice(),
            std::slice::from_ref(&checkpoint)
        );
        control.forget_checkpoint(&checkpoint).unwrap();
        assert!(
            control
                .restore_checkpoint(
                    &checkpoint,
                    temp.path().join("unregistered-restore"),
                    options()
                )
                .is_err()
        );
        assert!(!temp.path().join("unregistered-restore").exists());
    }
}

#[test]
fn control_crash_child() {
    let Ok(root) = std::env::var("FLOW_CONTROL_TEST_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let phase = std::env::var("FLOW_CONTROL_TEST_PHASE").unwrap();
    if phase == "candidate-ready" || phase == "activated" {
        let control = ControlStore::open(root.join("control")).unwrap();
        let checkpoint = control.checkpoints().unwrap().pop().unwrap();
        let replacement = control
            .restore_checkpoint(&checkpoint, root.join("replacement"), options())
            .unwrap();
        if phase == "activated" {
            control.activate_rebuilt(&replacement).unwrap();
        }
        std::mem::forget(replacement);
        std::mem::forget(control);
    } else {
        let (_, index) = initialize(root);
        let id = OperationId("second".into());
        index
            .prepare(
                operation("second", Some(1), 20),
                [
                    delta("second", 4, 20),
                    delta("second", 5, 20),
                    delta("second", 6, 20),
                ],
            )
            .unwrap();
        if phase != "prepared" {
            index.mark_committed(&id, 2, 2).unwrap();
        }
        if phase == "applying" {
            index.apply_committed_batch(&id).unwrap();
        }
        if phase == "applied" {
            index.apply_committed(&id).unwrap();
        }
        // Exit without RocksDB destructors, including the partially applied path.
        std::mem::forget(index);
    }
    std::process::exit(93);
}

#[test]
fn old_index_formats_import_authority_but_require_rebuilt_live_counts() {
    for version in [1, 2] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("legacy");
        let store = StateStore::open(&path, options()).unwrap();
        publish(&store, "legacy-rows", 1, 10);
        drop(store);
        let opts = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&opts, &path).unwrap();
        let db = rocksdb::DB::open_cf(&opts, &path, families).unwrap();
        db.put(b"format-version", [version]).unwrap();
        db.drop_cf("file_live_rows").unwrap();
        drop(db);
        assert!(matches!(
            StateStore::open(&path, options()),
            Err(Error::RecoveryRequired(_))
        ));
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        assert!(matches!(
            control.initialize_index(&path, options()),
            Err(Error::RecoveryRequired(_))
        ));
        assert_eq!(
            control
                .table_state(&TableId(7))
                .unwrap()
                .unwrap()
                .snapshot_id,
            Some(1)
        );
        let rebuilt = StateStore::open(temp.path().join("rebuilt"), options()).unwrap();
        publish(&rebuilt, "verified-catalog-rows", 1, 10);
        let generation = control.activate_rebuilt(&rebuilt).unwrap();
        drop(rebuilt);
        let store = StateStore::open_with_control(&generation.path, options(), control).unwrap();
        assert_eq!(
            store
                .file_live_row_counts(
                    &TableId(7),
                    Some(1),
                    &[FileId("verified-catalog-rows".into())]
                )
                .unwrap(),
            [3]
        );
    }
}

#[test]
fn corrupt_control_operation_stops_before_index_rebuild() {
    let temp = TempDir::new().unwrap();
    let (control, index) = initialize(temp.path());
    let id = OperationId("corrupt-control-cursor".into());
    index
        .prepare(
            operation(&id.0, Some(1), 20),
            [
                delta("second", 4, 20),
                delta("second", 5, 20),
                delta("second", 6, 20),
            ],
        )
        .unwrap();
    index.mark_committed(&id, 2, 2).unwrap();
    drop(index);
    drop(control);

    let control_path = temp.path().join("control");
    let options = rocksdb::Options::default();
    let db = rocksdb::DB::open(&options, &control_path).unwrap();
    let mut key = vec![1];
    key.extend_from_slice(id.0.as_bytes());
    let encoded = db.get(&key).unwrap().unwrap();
    let mut record: OperationRecord = bincode::deserialize(&encoded).unwrap();
    record.applied_count = record.delta_count + 1;
    db.put(&key, bincode::serialize(&record).unwrap()).unwrap();
    db.flush_wal(true).unwrap();
    drop(db);

    assert!(matches!(
        ControlStore::open(&control_path),
        Err(Error::AuthorityCorruption(_))
    ));
}

#[test]
fn missing_sealed_tail_requires_rebuild_and_preserves_control_outcome() {
    let temp = TempDir::new().unwrap();
    let index_path = temp.path().join("index");
    let control_path = temp.path().join("control");
    let id = OperationId("sealed-tail".into());
    {
        let control = ControlStore::open(&control_path).unwrap();
        let index = control.initialize_index(&index_path, options()).unwrap();
        index
            .prepare(
                operation(&id.0, None, 10),
                [delta("a", 1, 10), delta("a", 2, 10)],
            )
            .unwrap();
    }
    {
        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, &index_path).unwrap();
        let db = rocksdb::DB::open_cf(&options, &index_path, families).unwrap();
        let mut key = (id.0.len() as u64).to_be_bytes().to_vec();
        key.extend_from_slice(id.0.as_bytes());
        key.push(0);
        key.extend_from_slice(&1u64.to_be_bytes());
        db.delete_cf(&db.cf_handle("index_deltas").unwrap(), key)
            .unwrap();
    }
    let control = ControlStore::open(control_path).unwrap();
    let error = match StateStore::open_with_control(index_path, options(), control.clone()) {
        Ok(_) => panic!("missing prepared tail was accepted"),
        Err(error) => error,
    };
    assert!(error.requires_index_rebuild());
    let record = control.operation(&id).unwrap().unwrap();
    assert_eq!(record.phase, OperationPhase::Prepared);
    assert_eq!(record.delta_count, 2);
    assert_eq!(record.operation.artifacts, ["sealed-tail.parquet"]);
    // Catalog outcome can still be resolved using the durable control record.
    control.resolve_operation(&id, Some((1, 1))).unwrap();
    assert!(control.operation(&id).unwrap().is_none());
    let table = control.table_state(&TableId(7)).unwrap().unwrap();
    assert_eq!(table.snapshot_id, Some(1));
    assert_eq!(table.materialized_lsn, PgLsn(10));
    assert!(table.pending_operation.is_none());
}
