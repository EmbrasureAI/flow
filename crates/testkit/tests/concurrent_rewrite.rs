#[path = "common/rewrite.rs"]
mod fixture;
#[path = "common/gated_storage.rs"]
mod gated_storage;
#[path = "common/storage_catalog.rs"]
mod storage_catalog;
use fixture::{Fixture, options, row, sorted};
use flow_compactor::Policy;
use flow_coordinator::{
    DeleteRewritePolicy, PreparationWait, ReadyCompaction, TableMaintenance, TablePublisher,
    active_build_protection, resolve_catalog_operation,
};
use flow_iceberg_ext::{CommitBase, ManifestCache, RewriteFilesAction, SnapshotView};
use flow_materializer::{DataWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, Row, SourceId, Value};
use flow_state_store::{Change, OperationKind, OperationPhase, PreparedOperation, StateStore};
use flow_testkit::scan;
use iceberg::{Catalog, spec::DataContentType, table::Table};
use std::time::Duration;

fn compaction_policy() -> Policy {
    Policy {
        l0_soft_files: 2,
        l0_hard_files: 4,
        oldest_l0_soft_ms: 1,
        oldest_l0_hard_ms: 2,
        deleted_rows_percent: 100,
        min_file_age_ms: 0,
        ..Default::default()
    }
}

fn maintenance(f: &Fixture) -> TableMaintenance {
    TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        compaction_policy(),
        WriterConfig::default(),
    )
    .unwrap()
}

async fn cdc(f: &mut Fixture, lsn: u64, changes: Vec<(i64, Change)>) {
    let (epoch, collapsed) = flow_testkit::collapse_fixture(
        &f.index,
        &f.head,
        &f.schema,
        SourceId("fixture".into()),
        PgLsn(lsn),
        changes
            .into_iter()
            .map(|(id, change)| (f.schema.encode_key(&row(id)).unwrap(), change)),
    );
    TablePublisher::new(
        f.index.clone(),
        f.catalog.clone(),
        WriterConfig::default(),
        2,
        1 << 20,
    )
    .unwrap()
    .publish(&f.head, &f.schema, collapsed)
    .await
    .unwrap();
    f.index.forget_applied(&epoch.id).unwrap();
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
}

fn updated(id: i64) -> Row {
    vec![Value::Int64(id), Value::String("late".into())]
}
fn expected() -> Vec<Row> {
    sorted(vec![
        row(3),
        row(4),
        row(5),
        updated(10),
        row(15),
        row(16),
        row(17),
        updated(140),
    ])
}

async fn build_with_late_changes(f: &mut Fixture) -> (ReadyCompaction, Table, Table) {
    // Include an unchanged selected row, in addition to the four live rows of
    // the older L0 file that late CDC will replace or remove.
    cdc(f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let base = f.head.clone();
    let running = maintenance(f)
        .start_compaction(&base, &f.schema, f.scratch("build"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        f.index.pending_operations().unwrap().is_empty(),
        "the expensive build must not fence CDC"
    );
    cdc(
        f,
        100,
        vec![
            (10, Change::Update(updated(10))),
            (13, Change::Delete),
            (14, Change::Delete),
            (140, Change::Insert(updated(140))),
            (15, Change::Delete),
            (17, Change::Insert(row(17))),
        ],
    )
    .await;
    // Reinsert the same bytes under the same key. Fingerprint equality alone
    // cannot authorize replacing its newer source location with the old row.
    cdc(f, 110, vec![(15, Change::Insert(row(15)))]).await;
    let head = f.head.clone();
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), expected());
    let ready = running.wait().await.unwrap();
    assert!(f.index.pending_operations().unwrap().is_empty());
    (ready, base, head)
}

async fn verify(f: &Fixture, base: &Table, late: &Table, committed: &Table, rebuilt: bool) {
    let before = SnapshotView::current(base).await.unwrap();
    let after = SnapshotView::current(committed).await.unwrap();
    let base_path = &f.locations[0].data_file_id.0;
    let minor_path = &f.locations[6].data_file_id.0;
    assert_eq!(after.live_files[base_path], before.live_files[base_path]);
    assert!(!after.live_files.contains_key(minor_path));
    let old_delete = before
        .live_files
        .values()
        .find(|entry| {
            entry.content_type() == DataContentType::PositionDeletes
                && entry.sequence_number == Some(2)
        })
        .unwrap();
    assert_eq!(
        &after.live_files[old_delete.file_path()],
        old_delete,
        "old inapplicable delete records must retain their sequence"
    );
    assert_eq!(
        sorted(scan(committed, &f.schema).await.unwrap()),
        expected()
    );
    assert_eq!(sorted(scan(late, &f.schema).await.unwrap()), expected());
    let mut base_rows = f.expected.clone();
    base_rows.push(row(16));
    assert_eq!(
        sorted(scan(base, &f.schema).await.unwrap()),
        sorted(base_rows)
    );
    let state = f.index.table_state(&f.schema.table_id).unwrap();
    assert_eq!(
        state.snapshot_id,
        committed.metadata().current_snapshot_id()
    );
    assert_eq!(state.materialized_lsn, PgLsn(110));
    assert!(state.pending_operation.is_none());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );
    for (id, lsn) in [
        (3, 80),
        (10, 100),
        (15, 110),
        (16, 90),
        (17, 100),
        (140, 100),
    ] {
        let location = f
            .index
            .lookup(&f.schema.table_id, &f.schema.encode_key(&row(id)).unwrap())
            .unwrap()
            .unwrap();
        // Full reconstruction records the authoritative source prefix for all
        // rows; an intact index preserves each row's original source position.
        assert_eq!(
            location.source_commit_lsn,
            PgLsn(if rebuilt { 110 } else { lsn })
        );
        if id == 3 {
            assert_eq!(location.data_file_id, f.locations[3].data_file_id);
            assert_eq!(location.row_position, f.locations[3].row_position);
            assert_eq!(location.row_fingerprint, f.locations[3].row_fingerprint);
        }
        if id == 16 {
            let previous = SnapshotView::current(late).await.unwrap();
            assert!(!previous.live_files.contains_key(&location.data_file_id.0));
        }
    }
    for id in [13, 14] {
        assert!(
            f.index
                .lookup(&f.schema.table_id, &f.schema.encode_key(&row(id)).unwrap())
                .unwrap()
                .is_none()
        );
    }
    let inventory = maintenance(f)
        .inventory(committed, f.schema.table_id)
        .await
        .unwrap();
    let mut live = 0;
    for file in &inventory.files {
        let indexed = f
            .index
            .file_rows(&f.schema.table_id, &file.id)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len() as u64;
        assert_eq!(file.deleted_rows, file.row_count - indexed);
        live += indexed;
    }
    assert_eq!(live, expected().len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_merges_overlapping_residuals_and_masks_into_bounded_cohorts() {
    let mut f = Fixture::new().await;
    // This A-only delete is outside the selected L0 inputs. An independent
    // rewrite can later widen its path range without changing logical rows.
    cdc(
        &mut f,
        90,
        vec![(16, Change::Insert(row(16))), (3, Change::Delete)],
    )
    .await;
    let base = f.head.clone();
    let before = SnapshotView::current(&base).await.unwrap();
    let a_only = before
        .live_files
        .values()
        .find(|entry| {
            entry.content_type() == DataContentType::PositionDeletes
                && entry.sequence_number == Some(base.metadata().last_sequence_number())
        })
        .unwrap();
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        compaction_policy(),
        WriterConfig {
            row_group_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let running = maintenance
        .start_compaction(&base, &f.schema, f.scratch("overlap-build"))
        .await
        .unwrap()
        .unwrap();
    let shared = fixture::delete(
        &base,
        "external-overlapping-deletes",
        [1, 2, 3, 7, 8]
            .into_iter()
            .map(|i| f.locations[i].clone())
            .collect(),
    )
    .await;
    f.head = RewriteFilesAction::new(&base, "external-overlapping-deletes")
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_delete_files([a_only.file_path().to_owned()])
        .rewrite_position_deletes(shared, a_only.sequence_number.unwrap())
        .commit(f.catalog.as_ref(), &base)
        .await
        .unwrap()
        .table;
    maintenance
        .reconcile(&f.head, &f.schema, f.scratch("overlap-reconcile"))
        .await
        .unwrap();
    let external = f.head.clone();
    cdc(&mut f, 100, vec![(10, Change::Update(updated(10)))]).await;
    let late = f.head.clone();
    let at_head = SnapshotView::current(&late).await.unwrap();
    maintenance
        .finish_compaction(&late, &f.schema, running.wait().await.unwrap())
        .await
        .unwrap()
        .unwrap();
    let committed = f.catalog.load_table(late.identifier()).await.unwrap();
    let after = SnapshotView::current(&committed).await.unwrap();
    let new_deletes = after
        .live_files
        .values()
        .filter(|entry| {
            entry.content_type() == DataContentType::PositionDeletes
                && !at_head.live_files.contains_key(entry.file_path())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        new_deletes.len(),
        2,
        "four unique positions span two bounded cohorts"
    );
    for entry in &new_deletes {
        assert_eq!(entry.data_file.record_count(), 2);
        assert_eq!(
            entry.sequence_number,
            Some(late.metadata().last_sequence_number())
        );
        assert!(entry.data_file.lower_bounds().contains_key(&2147483546));
        assert!(entry.data_file.upper_bounds().contains_key(&2147483546));
    }
    let audit = f.scratch("merged-residual-audit");
    flow_compactor::stage_delete_files(
        &committed,
        &f.schema,
        &after,
        &after
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| flow_model::FileId(entry.file_path().into()))
            .collect(),
        &new_deletes
            .iter()
            .map(|entry| flow_model::FileId(entry.file_path().into()))
            .collect(),
        &audit,
        "output",
        2,
        flow_compactor::DeleteReadLimits {
            max_bytes: 1 << 20,
            max_rows: 4,
        },
        &flow_compactor::ReadLimits::default(),
    )
    .await
    .unwrap();
    let positions = audit
        .position_deletes("output", &f.schema.table_id)
        .collect::<flow_state_store::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        positions.len(),
        4,
        "written record counts equal the distinct effective union"
    );
    let a = &f.locations[0].data_file_id;
    assert_eq!(
        positions
            .iter()
            .filter(|position| &position.data_file_id == a)
            .map(|position| position.row_position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let masks = positions
        .iter()
        .filter(|position| &position.data_file_id != a)
        .collect::<Vec<_>>();
    assert_eq!(masks.len(), 1);
    assert!(!at_head.live_files.contains_key(&masks[0].data_file_id.0));
    assert_eq!(
        after.live_files[&masks[0].data_file_id.0].sequence_number,
        Some(base.metadata().last_sequence_number())
    );
    assert_eq!(after.live_files[&a.0], before.live_files[&a.0]);
    let historical = sorted([4, 5, 10, 13, 14, 15, 16].into_iter().map(row).collect());
    for table in [&base, &external] {
        assert_eq!(sorted(scan(table, &f.schema).await.unwrap()), historical);
    }
    let expected = sorted(vec![
        row(4),
        row(5),
        updated(10),
        row(13),
        row(14),
        row(15),
        row(16),
    ]);
    for table in [&late, &committed] {
        assert_eq!(sorted(scan(table, &f.schema).await.unwrap()), expected);
    }
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(100)
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_build_catches_up_updates_deletes_key_moves_and_identical_reinsert() {
    let mut f = Fixture::new().await;
    let (ready, base, late) = build_with_late_changes(&mut f).await;
    let maintenance = maintenance(&f);
    let prepared = maintenance
        .start_compaction_preparation(&late, &f.schema, ready)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(f.index.pending_operations().unwrap().is_empty());
    maintenance
        .activate_compaction(&late, &f.schema, prepared)
        .await
        .unwrap()
        .unwrap();
    let committed = f.catalog.load_table(late.identifier()).await.unwrap();
    verify(&f, &base, &late, &committed, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_watermark_after_detached_preparation_replans_without_partial_publication() {
    let mut f = Fixture::new().await;
    cdc(&mut f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let head = f.head.clone();
    let snapshot = head.metadata().current_snapshot_id();
    let snapshot_count = head.metadata().snapshots().len();
    let rows = sorted(scan(&head, &f.schema).await.unwrap());
    let maintenance = maintenance(&f);
    let ready = maintenance
        .start_compaction(&head, &f.schema, f.scratch("noop-race-build"))
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    let preparing = maintenance
        .start_compaction_preparation(&head, &f.schema, ready)
        .await
        .unwrap();
    let prepared = preparing.wait().await.unwrap();

    // The completed preparation is anchored to the unfenced row index at LSN
    // 90. Advancing only its source watermark must invalidate activation even
    // though the catalog snapshot and all physical rows remain unchanged.
    f.index
        .complete_noop(&f.schema.table_id, PgLsn(100), f.schema.version)
        .unwrap();
    let operation = prepared.operation_id().clone();
    let rejected = maintenance
        .activate_compaction(&head, &f.schema, prepared)
        .await
        .unwrap_err();
    assert!(
        rejected.is::<flow_coordinator::ReplanRequired>(),
        "{rejected:#}"
    );

    let unchanged = f.catalog.load_table(head.identifier()).await.unwrap();
    assert_eq!(unchanged.metadata().current_snapshot_id(), snapshot);
    assert_eq!(unchanged.metadata().snapshots().len(), snapshot_count);
    assert_eq!(sorted(scan(&unchanged, &f.schema).await.unwrap()), rows);
    let state = f.index.table_state(&f.schema.table_id).unwrap();
    assert_eq!(state.snapshot_id, snapshot);
    assert_eq!(state.materialized_lsn, PgLsn(100));
    assert!(state.pending_operation.is_none());
    assert!(f.index.operation(&operation).unwrap().is_none());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );

    // A following CDC transaction proves that cleanup released the table.
    cdc(&mut f, 110, vec![(10, Change::Update(updated(10)))]).await;
    let after = sorted(scan(&f.head, &f.schema).await.unwrap());
    assert!(after.contains(&updated(10)));
    assert!(!after.contains(&row(10)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_cdc_after_detached_preparation_replans_without_leaking_publication_state() {
    let mut f = Fixture::new().await;
    cdc(&mut f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let base = f.head.clone();
    let maintenance = maintenance(&f);
    let ready = maintenance
        .start_compaction(&base, &f.schema, f.scratch("cdc-race-build"))
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    let prepared = maintenance
        .start_compaction_preparation(&base, &f.schema, ready)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    let operation = prepared.operation_id().clone();

    // Preparation is complete but still owns no publication fence, so a real
    // catalog and index advance must remain available to CDC.
    cdc(
        &mut f,
        100,
        vec![(10, Change::Update(updated(10))), (13, Change::Delete)],
    )
    .await;
    let advanced = f.head.clone();
    let advanced_state = f.index.table_state(&f.schema.table_id).unwrap();
    let advanced_rows = sorted(scan(&advanced, &f.schema).await.unwrap());

    let rejected = maintenance
        .activate_compaction(&advanced, &f.schema, prepared)
        .await
        .unwrap_err();
    assert!(
        rejected.is::<flow_coordinator::ReplanRequired>(),
        "{rejected:#}"
    );

    let unchanged = f.catalog.load_table(advanced.identifier()).await.unwrap();
    assert_eq!(
        unchanged.metadata().current_snapshot_id(),
        advanced.metadata().current_snapshot_id()
    );
    assert_eq!(
        f.index.table_state(&f.schema.table_id).unwrap(),
        advanced_state
    );
    assert_eq!(
        sorted(scan(&unchanged, &f.schema).await.unwrap()),
        advanced_rows
    );
    assert!(f.index.operation(&operation).unwrap().is_none());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );

    // A later source transaction proves the failed activation released every
    // fence and marker needed by the ingest path.
    cdc(&mut f, 110, vec![(14, Change::Update(updated(14)))]).await;
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(110)
    );
    assert!(sorted(scan(&f.head, &f.schema).await.unwrap()).contains(&updated(14)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preparation_deadline_allows_cdc_before_join_and_retires_scratch_ownership() {
    let mut f = Fixture::new().await;
    cdc(&mut f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let base = f.head.clone();
    let maintenance = maintenance(&f);
    let scratch_name = "deadline-preparation-build";
    let scratch_path = f.temp.path().join(scratch_name);
    let scratch = f.scratch(scratch_name);
    let ready = maintenance
        .start_compaction(&base, &f.schema, scratch)
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();

    // Give background preparation a real catalog and index advance to catch up.
    cdc(&mut f, 100, vec![(10, Change::Update(updated(10)))]).await;
    let late = f.head.clone();

    // This delete is read by catch-up only after its immutable index capture.
    // Waiting for the storage gate avoids racing CDC with that initial capture.
    let late_delete = SnapshotView::current(&late)
        .await
        .unwrap()
        .live_files
        .values()
        .find(|entry| {
            entry.content_type() == DataContentType::PositionDeletes
                && entry.sequence_number == Some(late.metadata().last_sequence_number())
        })
        .unwrap()
        .file_path()
        .to_owned();
    let gate = std::sync::Arc::new(gated_storage::Gate::default());
    let file_io =
        iceberg::io::FileIOBuilder::new(std::sync::Arc::new(gated_storage::GatedStorage {
            gate: gate.clone(),
            owner_path: late_delete,
            sibling_path: String::new(),
            fail: false,
        }))
        .build();
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        std::sync::Arc::new(storage_catalog::StorageCatalog {
            inner: f.catalog.clone(),
            file_io,
        }),
        compaction_policy(),
        WriterConfig::default(),
    )
    .unwrap();
    let preparing = maintenance
        .start_compaction_preparation(&late, &f.schema, ready)
        .await
        .unwrap();
    let captured = tokio::time::timeout(Duration::from_secs(5), gate.started.acquire()).await;
    if captured.is_err() {
        gate.release.close();
        if let PreparationWait::Deadline(retiring) =
            preparing.wait_for(Duration::ZERO).await.unwrap()
        {
            let _ = tokio::time::timeout(Duration::from_secs(5), retiring.wait()).await;
        }
        panic!("preparation never reached its read after index capture");
    }
    captured.unwrap().unwrap().forget();
    let retiring = match preparing.wait_for(Duration::from_millis(10)).await.unwrap() {
        PreparationWait::Deadline(retiring) => retiring,
        PreparationWait::Ready(prepared) => {
            gate.release.close();
            prepared.discard().await.unwrap();
            panic!("preparation completed while its storage read was held")
        }
    };
    let operation = retiring.operation_id().clone();

    let continued = tokio::time::timeout(
        Duration::from_secs(5),
        cdc(&mut f, 110, vec![(14, Change::Delete)]),
    )
    .await;
    if continued.is_err() {
        gate.release.close();
        let _ = tokio::time::timeout(Duration::from_secs(5), retiring.wait()).await;
        panic!("CDC waited for background preparation");
    }
    let advanced = f.head.clone();
    let advanced_state = f.index.table_state(&f.schema.table_id).unwrap();
    let advanced_rows = sorted(scan(&advanced, &f.schema).await.unwrap());
    assert!(
        active_build_protection(&f.index, &f.head, f.schema.table_id)
            .unwrap()
            .operations
            .contains(&operation),
        "retirement must retain build ownership until its worker is joined"
    );

    gate.release.close();
    tokio::time::timeout(Duration::from_secs(5), retiring.wait())
        .await
        .unwrap()
        .unwrap();
    let unchanged = f.catalog.load_table(advanced.identifier()).await.unwrap();
    assert_eq!(
        unchanged.metadata().current_snapshot_id(),
        advanced.metadata().current_snapshot_id()
    );
    assert_eq!(
        f.index.table_state(&f.schema.table_id).unwrap(),
        advanced_state
    );
    assert_eq!(
        sorted(scan(&unchanged, &f.schema).await.unwrap()),
        advanced_rows
    );
    assert!(f.index.operation(&operation).unwrap().is_none());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );
    std::fs::remove_dir_all(&scratch_path).unwrap();
    assert!(!scratch_path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_prepared_recovery_survives_lost_response_and_complete_index_loss() {
    for lose_index in [false, true] {
        let mut f = Fixture::new().await;
        let (ready, base, late) = build_with_late_changes(&mut f).await;
        f.catalog.lose_next_response_and_disconnect();
        assert!(
            maintenance(&f)
                .finish_compaction(&late, &f.schema, ready)
                .await
                .is_err()
        );
        let pending = f.index.pending_operations().unwrap().remove(0);
        assert_eq!(pending.phase, OperationPhase::Prepared);
        assert_eq!(pending.operation.last_lsn, PgLsn(110));
        assert!(
            f.index
                .source_transactions_after(b"active-build/", None)
                .next()
                .is_none()
        );
        let scratch = f.scratch("rebuild-scratch");
        drop(f.index);
        f.catalog.reconnect();
        let committed = f.catalog.load_table(late.identifier()).await.unwrap();
        let snapshot_count = committed.metadata().snapshots().len();
        if lose_index {
            std::fs::remove_dir_all(f.temp.path().join("index")).unwrap();
            let result =
                resolve_catalog_operation(&pending, f.catalog.as_ref(), &committed, &f.control)
                    .await
                    .unwrap()
                    .unwrap();
            f.control
                .resolve_operation(&pending.operation.id, Some(result))
                .unwrap();
            let replacement =
                StateStore::open(f.temp.path().join("replacement"), options()).unwrap();
            let builder = TableMaintenance::new(
                replacement.clone(),
                f.catalog.clone(),
                Policy::default(),
                WriterConfig::default(),
            )
            .unwrap();
            let rebuilt = builder
                .rebuild_index(
                    &committed,
                    &f.schema,
                    replacement.clone(),
                    scratch,
                    PgLsn(110),
                    f.schema.version,
                )
                .await
                .unwrap();
            let selected = f.control.activate_rebuilt(&rebuilt).unwrap();
            drop(builder);
            drop(replacement);
            drop(rebuilt);
            f.index =
                StateStore::open_with_control(selected.path, options(), f.control.clone()).unwrap();
        } else {
            f.index = StateStore::open_with_control(
                f.temp.path().join("index"),
                options(),
                f.control.clone(),
            )
            .unwrap();
            maintenance(&f)
                .recover(&committed, &pending.operation.id)
                .await
                .unwrap();
        }
        assert_eq!(
            f.catalog
                .load_table(late.identifier())
                .await
                .unwrap()
                .metadata()
                .snapshots()
                .len(),
            snapshot_count
        );
        verify(&f, &base, &late, &committed, lose_index).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_catch_up_staging_remains_unpublished_across_restart() {
    let mut f = Fixture::new().await;
    cdc(&mut f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let base = f.head.clone();
    let policy = compaction_policy();
    let inventory = maintenance(&f)
        .inventory(&base, f.schema.table_id)
        .await
        .unwrap();
    let plan = policy
        .plan(
            base.metadata().current_snapshot_id().unwrap(),
            base.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )
        .unwrap()
        .unwrap();
    let selected = plan.input_files.iter().cloned().collect::<Vec<_>>();
    let operation = OperationId("partial-catch-up".into());
    let built = flow_compactor::build_data(
        &base,
        f.schema.clone(),
        plan,
        operation.clone(),
        &f.index,
        f.scratch("partial-build"),
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .unwrap();
    cdc(&mut f, 100, vec![(10, Change::Update(updated(10)))]).await;
    let head = f.head.clone();
    let expected = sorted(scan(&head, &f.schema).await.unwrap());
    let counts = selected
        .iter()
        .cloned()
        .zip(
            f.index
                .file_live_row_counts(
                    &f.schema.table_id,
                    head.metadata().current_snapshot_id(),
                    &selected,
                )
                .unwrap(),
        )
        .collect();
    f.index
        .begin_prepare(PreparedOperation {
            id: operation.clone(),
            table_id: f.schema.table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: head.metadata().current_snapshot_id(),
            last_lsn: PgLsn(100),
            schema_version: f.schema.version,
            artifacts: Vec::new(),
            payload: Vec::new(),
        })
        .unwrap();
    // Exercise the real reader while stage_deltas holds the controlled row
    // mutex, then fail the next lookup. Catch-up must join that held write before
    // exposing the failure to the caller that can discard Building.
    struct FailingLookup {
        store: StateStore,
        calls: std::sync::atomic::AtomicUsize,
        failed: std::sync::mpsc::Sender<()>,
        stage_entered: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl flow_state_store::RowIndex for FailingLookup {
        fn lookup_many(
            &self,
            table: &flow_model::TableId,
            keys: &[flow_model::PrimaryKey],
        ) -> flow_state_store::Result<Vec<Option<flow_model::RowLocation>>> {
            let second = self
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                == 1;
            if second {
                self.stage_entered
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .map_err(|_| {
                        flow_state_store::Error::InvalidState(
                            "stage did not enter before second lookup".into(),
                        )
                    })?;
            }
            let rows = self.store.lookup_many(table, keys)?;
            if second {
                let _ = self.failed.send(());
                return Err(flow_state_store::Error::InvalidState(
                    "injected next-batch lookup failure".into(),
                ));
            }
            Ok(rows)
        }

        fn file_rows<'a>(
            &'a self,
            table: &flow_model::TableId,
            file: &flow_model::FileId,
        ) -> impl Iterator<Item = flow_state_store::Result<(u64, flow_model::PrimaryKey)>> + Send + 'a
        {
            self.store.file_rows(table, file)
        }
    }
    let (entered, held) = std::sync::mpsc::channel();
    let (row_locked, row_lock) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let (failed, failure) = std::sync::mpsc::channel();
    let reader = FailingLookup {
        store: f.index.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        failed,
        stage_entered: std::sync::Mutex::new(row_lock),
    };
    let staged_rows = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let staged = staged_rows.clone();
    let store = f.index.clone();
    let stage_operation = operation.clone();
    let catch_up_head = head.clone();
    let schema = f.schema.clone();
    let runtime = tokio::runtime::Handle::current();
    let catching_up = tokio::task::spawn_blocking(move || {
        runtime.block_on(async move {
            flow_compactor::catch_up(
                &catch_up_head,
                &schema,
                &CommitBase::new(&base),
                built,
                &reader,
                &counts,
                &policy,
                WriterConfig::default(),
                &flow_compactor::ReadLimits::default(),
                &ManifestCache::default(),
                move |deltas| {
                    staged.store(deltas.len(), std::sync::atomic::Ordering::Relaxed);
                    let mut deltas = deltas.into_iter();
                    let mut hold = true;
                    store.stage_deltas(
                        &stage_operation,
                        std::iter::from_fn(|| {
                            if hold {
                                hold = false;
                                let _ = entered.send(());
                                let _ = row_locked.send(());
                                let _ = released.recv();
                            }
                            deltas.next()
                        }),
                    )?;
                    Ok(())
                },
            )
            .await
        })
    });
    let stage_entered =
        tokio::task::spawn_blocking(move || held.recv_timeout(std::time::Duration::from_secs(5)))
            .await;
    let next_batch_failed = tokio::task::spawn_blocking(move || {
        failure.recv_timeout(std::time::Duration::from_secs(1))
    })
    .await;
    // Give the caller time to propagate its read error if it incorrectly skips
    // the join. Always release and join before any assertion, including timeouts.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let returned_early = catching_up.is_finished();
    let _ = release.send(());
    let outcome = catching_up.await;
    assert!(
        stage_entered.unwrap().is_ok(),
        "stage did not reach its hold"
    );
    assert!(
        next_batch_failed.unwrap().is_ok(),
        "next lookup did not overlap staging"
    );
    assert!(
        !returned_early,
        "catch-up returned while its index stage was held"
    );
    let error = outcome.unwrap().err().expect("injected lookup must fail");
    assert!(
        error
            .to_string()
            .contains("injected next-batch lookup failure")
    );
    let staged_rows = staged_rows.load(std::sync::atomic::Ordering::Relaxed);
    assert!(staged_rows > 0);
    let pending = f.index.operation(&operation).unwrap().unwrap();
    assert_eq!(pending.phase, OperationPhase::Building);
    assert_eq!(pending.delta_count, staged_rows as u64);
    drop(f.index);
    f.index =
        StateStore::open_with_control(f.temp.path().join("index"), options(), f.control.clone())
            .unwrap();
    assert_eq!(
        f.index.operation(&operation).unwrap().unwrap().phase,
        OperationPhase::Building
    );
    assert!(maintenance(&f).recover(&head, &operation).await.is_err());
    assert_eq!(
        f.catalog
            .load_table(head.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        head.metadata().current_snapshot_id()
    );
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), expected);
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(100)
    );
    f.index.discard_uncommitted(&operation).unwrap();
    cdc(&mut f, 110, vec![(13, Change::Delete)]).await;
    let head = f.head.clone();
    let expected = expected
        .into_iter()
        .filter(|value| value[0] != Value::Int64(13))
        .collect::<Vec<_>>();
    // A retry builds fresh mappings. The partially staged operation is never
    // reused after the head advances and deletes a previously staged row.
    let ready = maintenance(&f)
        .start_compaction(&head, &f.schema, f.scratch("retry-build"))
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    maintenance(&f)
        .finish_compaction(&head, &f.schema, ready)
        .await
        .unwrap()
        .unwrap();
    let final_head = f.catalog.load_table(head.identifier()).await.unwrap();
    assert_eq!(
        sorted(scan(&final_head, &f.schema).await.unwrap()),
        expected
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_input_rewrite_cancels_completed_build_without_resurrecting_rows() {
    let mut f = Fixture::new().await;
    cdc(&mut f, 90, vec![(16, Change::Insert(row(16)))]).await;
    let base = f.head.clone();
    let ready = maintenance(&f)
        .start_compaction(&base, &f.schema, f.scratch("build"))
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    let mut writer = DataWriter::new(
        base.file_io().clone(),
        base.metadata().location(),
        &OperationId("external-input-rewrite".into()),
        f.schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer
        .write(&[row(10), row(13), row(14), row(15)], PgLsn(90))
        .await
        .unwrap();
    let external = RewriteFilesAction::new(&base, "external-input-rewrite")
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files([f.locations[6].data_file_id.0.clone()])
        .add_data_files(writer.close().await.unwrap())
        .commit(f.catalog.as_ref(), &base)
        .await
        .unwrap()
        .table;
    f.maintenance()
        .reconcile(&external, &f.schema, f.scratch("external-reconcile"))
        .await
        .unwrap();
    let rejected = maintenance(&f)
        .finish_compaction(&external, &f.schema, ready)
        .await
        .unwrap_err();
    assert!(
        rejected.is::<flow_coordinator::ReplanRequired>(),
        "{rejected:#}"
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );
    f.head = f.catalog.load_table(external.identifier()).await.unwrap();
    assert_eq!(
        f.head.metadata().current_snapshot_id(),
        external.metadata().current_snapshot_id()
    );
    let mut expected = f.expected.clone();
    expected.push(row(16));
    assert_eq!(
        sorted(scan(&f.head, &f.schema).await.unwrap()),
        sorted(expected.clone())
    );
    assert_eq!(
        sorted(scan(&base, &f.schema).await.unwrap()),
        sorted(expected.clone())
    );
    cdc(&mut f, 100, vec![(10, Change::Delete)]).await;
    expected.retain(|value| value[0] != Value::Int64(10));
    assert_eq!(
        sorted(scan(&f.head, &f.schema).await.unwrap()),
        sorted(expected.clone())
    );

    // A different invalidation: all selected data stays physically identical,
    // but another maintenance commit replaces its original position deletes.
    let delete_base = f.head.clone();
    let selected = SnapshotView::current(&delete_base).await.unwrap();
    let reclaim = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        Policy {
            deleted_rows_percent: 1,
            min_file_age_ms: 0,
            ..Default::default()
        },
        WriterConfig::default(),
    )
    .unwrap();
    let ready = reclaim
        .start_compaction(&delete_base, &f.schema, f.scratch("delete-input-build"))
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    f.maintenance()
        .compact_deletes(
            &delete_base,
            &f.schema,
            f.scratch("independent-delete-consolidation"),
            &DeleteRewritePolicy {
                min_input_files: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    let consolidated = f
        .catalog
        .load_table(delete_base.identifier())
        .await
        .unwrap();
    let after = SnapshotView::current(&consolidated).await.unwrap();
    for (path, entry) in &selected.live_files {
        if entry.content_type() == DataContentType::Data {
            assert_eq!(&after.live_files[path], entry);
        }
    }
    assert!(selected.live_files.values().any(|entry| {
        entry.content_type() == DataContentType::PositionDeletes
            && !after.live_files.contains_key(entry.file_path())
    }));
    let rejected = reclaim
        .finish_compaction(&consolidated, &f.schema, ready)
        .await
        .unwrap_err();
    assert!(
        rejected.is::<flow_coordinator::ReplanRequired>(),
        "{rejected:#}"
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
    assert!(
        f.index
            .source_transactions_after(b"active-build/", None)
            .next()
            .is_none()
    );
    let final_head = f
        .catalog
        .load_table(consolidated.identifier())
        .await
        .unwrap();
    assert_eq!(
        final_head.metadata().current_snapshot_id(),
        consolidated.metadata().current_snapshot_id()
    );
    assert_eq!(
        sorted(scan(&final_head, &f.schema).await.unwrap()),
        sorted(expected.clone())
    );
    assert_eq!(
        sorted(scan(&delete_base, &f.schema).await.unwrap()),
        sorted(expected)
    );
}
