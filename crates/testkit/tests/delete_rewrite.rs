#[path = "common/rewrite.rs"]
mod fixture;
use fixture::{Fixture, delete, options, row, sorted};
use flow_compactor::{DeleteReadLimits, Policy, stage_delete_files};
use flow_coordinator::{
    DeleteRepairCursor, DeleteRewritePolicy, TableMaintenance, resolve_catalog_operation,
};
use flow_iceberg_ext::{
    CommitBase, RewriteFilesAction, RowDeltaAction, SnapshotView, write_artifact_plan,
};
use flow_materializer::{DataWriter, WriterConfig, rows_from_batch};
use flow_model::{FileId, OperationId, PgLsn, Value};
use flow_state_store::{OperationKind, OperationPhase, PreparedOperation, StateStore};
use flow_testkit::scan;
use futures::TryStreamExt;
use iceberg::{
    Catalog,
    spec::{DataContentType, ManifestContentType, ManifestStatus},
};
use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_deletes_preserve_sequences_density_history_and_recover_lost_response() {
    let mut f = Fixture::new().await;
    let before = SnapshotView::current(&f.head).await.unwrap();
    let old_snapshot = before.snapshot_id.unwrap();
    let original_manifests = f
        .head
        .manifest_list_reader(f.head.metadata().current_snapshot().unwrap())
        .load()
        .await
        .unwrap()
        .consume_entries()
        .into_iter()
        .collect::<Vec<_>>();
    let maintenance = f.maintenance();
    let density = maintenance
        .inventory(&f.head, f.schema.table_id)
        .await
        .unwrap();
    assert_eq!(
        density
            .files
            .iter()
            .map(|file| file.deleted_rows)
            .sum::<u64>(),
        5
    );
    for file in &density.files {
        assert_eq!(
            file.deleted_rows,
            if file.id == f.locations[0].data_file_id {
                3
            } else {
                2
            }
        );
    }
    let original = f
        .expected
        .iter()
        .map(|row| {
            f.index
                .lookup(&f.schema.table_id, &f.schema.encode_key(row).unwrap())
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    f.catalog.lose_next_response_and_disconnect();
    assert!(
        maintenance
            .compact_deletes(
                &f.head,
                &f.schema,
                f.scratch("rewrite"),
                &DeleteRewritePolicy {
                    min_input_files: 3,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let pending = f.index.pending_operations().unwrap().remove(0);
    assert_eq!(pending.phase, OperationPhase::Prepared);
    assert_eq!(pending.delta_count, 0);
    drop(maintenance);
    drop(f.index);
    f.catalog.reconnect();
    f.index =
        StateStore::open_with_control(f.temp.path().join("index"), options(), f.control.clone())
            .unwrap();
    let maintenance = f.maintenance();
    let committed = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let snapshot_count = committed.metadata().snapshots().len();
    assert_eq!(
        maintenance
            .recover(&committed, &pending.operation.id)
            .await
            .unwrap(),
        committed.metadata().current_snapshot_id()
    );
    assert_eq!(
        f.catalog
            .load_table(f.head.identifier())
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .len(),
        snapshot_count
    );
    let after = SnapshotView::current(&committed).await.unwrap();
    let snapshot = committed.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.parent_snapshot_id(), Some(old_snapshot));
    assert_eq!(snapshot_count, f.head.metadata().snapshots().len() + 1);
    let manifests = committed
        .manifest_list_reader(snapshot)
        .load()
        .await
        .unwrap()
        .consume_entries()
        .into_iter()
        .collect::<Vec<_>>();
    // Both data manifests are reused; all three removed delete files and the
    // replacement share one new delete manifest in this same publication.
    assert_eq!(manifests.len(), 3);
    for manifest in original_manifests
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
    {
        assert!(manifests.contains(manifest));
    }
    let new_manifests = manifests
        .iter()
        .filter(|manifest| manifest.added_snapshot_id == snapshot.snapshot_id())
        .collect::<Vec<_>>();
    assert_eq!(new_manifests.len(), 1);
    let manifest = new_manifests[0];
    assert_eq!(manifest.content, ManifestContentType::Deletes);
    assert_eq!(
        manifest.partition_spec_id,
        committed.metadata().default_partition_spec_id()
    );
    let loaded = manifest.load_manifest(committed.file_io()).await.unwrap();
    assert_eq!(loaded.metadata().content, manifest.content);
    assert_eq!(
        loaded.metadata().partition_spec.spec_id(),
        manifest.partition_spec_id
    );
    let mut removed = Vec::new();
    for entry in loaded.entries() {
        assert_eq!(entry.content_type(), DataContentType::PositionDeletes);
        assert_eq!(entry.snapshot_id, after.snapshot_id);
        if entry.status == ManifestStatus::Deleted {
            let mut expected = before.live_files[entry.file_path()].as_ref().clone();
            expected.status = ManifestStatus::Deleted;
            expected.snapshot_id = after.snapshot_id;
            assert_eq!(entry.as_ref(), &expected);
            removed.push(entry.file_path().to_owned());
        } else {
            assert_eq!(entry.status, ManifestStatus::Added);
            assert_eq!(entry, &after.live_files[entry.file_path()]);
        }
    }
    removed.sort();
    let expected_removed = before
        .live_files
        .iter()
        .filter(|(_, entry)| entry.content_type() == DataContentType::PositionDeletes)
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    assert_eq!(removed, expected_removed);
    assert_eq!(loaded.entries().len(), removed.len() + 1);
    let deletes = after
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        .collect::<Vec<_>>();
    assert_eq!(deletes.len(), 1);
    assert_eq!(
        deletes[0].data_file.record_count(),
        5,
        "duplicate positions and formerly inapplicable targets must be removed"
    );
    assert_eq!(deletes[0].sequence_number, Some(5));
    assert_eq!(deletes[0].file_sequence_number, Some(6));
    for (path, entry) in &before.live_files {
        if entry.content_type() == DataContentType::Data {
            assert_eq!(&after.live_files[path], entry);
        }
    }
    assert_eq!(
        sorted(scan(&committed, &f.schema).await.unwrap()),
        f.expected
    );
    let mut history = committed
        .scan()
        .snapshot_id(old_snapshot)
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let mut historical_rows = Vec::new();
    while let Some(batch) = history.try_next().await.unwrap() {
        historical_rows.extend(rows_from_batch(&f.schema, &batch).unwrap());
    }
    assert_eq!(sorted(historical_rows), f.expected);
    let density = maintenance
        .inventory(&committed, f.schema.table_id)
        .await
        .unwrap();
    for file in &density.files {
        assert_eq!(
            file.deleted_rows,
            if file.id == f.locations[0].data_file_id {
                3
            } else {
                2
            }
        );
    }
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(80)
    );
    for (row, original) in f.expected.iter().zip(original) {
        assert_eq!(
            f.index
                .lookup(&f.schema.table_id, &f.schema.encode_key(row).unwrap())
                .unwrap(),
            Some(original)
        );
    }
    assert!(
        maintenance
            .compact_deletes(
                &committed,
                &f.schema,
                f.scratch("idle"),
                &DeleteRewritePolicy {
                    min_input_files: 1,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_none()
    );
    // A subsequent delete remains effective alongside the inherited-sequence output.
    let next = delete(&committed, "later-delete", vec![f.locations[6].clone()]).await;
    let head = RowDeltaAction::new(&committed, "later-delete")
        .add_delete_files(next)
        .validate_data_files_exist([f.locations[6].data_file_id.0.clone()])
        .commit(f.catalog.as_ref(), &committed)
        .await
        .unwrap()
        .table;
    assert_eq!(
        sorted(scan(&head, &f.schema).await.unwrap()),
        f.expected
            .into_iter()
            .filter(|value| value[0] != Value::Int64(10))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_replay_survives_index_loss_and_rejects_stale_base_and_physical_budget_overrun() {
    let f = Fixture::new().await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    let data = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| FileId(entry.file_path().into()))
        .collect();
    let deletes = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        .map(|entry| FileId(entry.file_path().into()))
        .collect::<BTreeSet<_>>();
    for limits in [
        DeleteReadLimits {
            max_bytes: 1,
            max_rows: 100,
        },
        DeleteReadLimits {
            max_bytes: 1 << 20,
            max_rows: 1,
        },
    ] {
        assert!(
            stage_delete_files(
                &f.head,
                &f.schema,
                &view,
                &data,
                &deletes,
                &f.scratch(&format!("budget-{}", limits.max_bytes)),
                "budget",
                2,
                limits,
                &flow_compactor::ReadLimits::default(),
            )
            .await
            .is_err()
        );
    }
    let output = delete(
        &f.head,
        "offline-merged",
        [0, 1, 2, 7, 8]
            .into_iter()
            .map(|i| f.locations[i].clone())
            .collect(),
    )
    .await;
    let removed = deletes.into_iter().map(|id| id.0).collect::<BTreeSet<_>>();
    let stale = RewriteFilesAction::new(&f.head, "stale")
        .remove_delete_files(removed.clone())
        .rewrite_position_deletes(output.clone(), 5);
    // Incorrect data-sequence inheritance fails before any catalog update.
    assert!(
        RewriteFilesAction::new(&f.head, "bad-sequence")
            .remove_delete_files(removed.clone())
            .rewrite_position_deletes(output.clone(), 6)
            .commit(f.catalog.as_ref(), &f.head)
            .await
            .is_err()
    );
    let id = OperationId("offline-delete-replay".into());
    let path = format!(
        "{}/metadata/{}-prepared.avro",
        f.head.metadata().location(),
        id.0
    );
    write_artifact_plan(&f.head, &path, &[], &output)
        .await
        .unwrap();
    f.index
        .prepare(
            PreparedOperation {
                id: id.clone(),
                table_id: f.schema.table_id,
                kind: OperationKind::Rewrite,
                base_snapshot_id: view.snapshot_id,
                last_lsn: PgLsn(80),
                schema_version: f.schema.version,
                artifacts: vec![path.clone(), output[0].file_path().into()],
                payload: serde_json::to_vec(&serde_json::json!({
                    "base": CommitBase::new(&f.head), "manifest_list": path, "removed_data": [],
                    "removed_deletes": removed, "delete_sequence": 5, "properties": {}
                }))
                .unwrap(),
            },
            [],
        )
        .unwrap();
    drop(f.index);
    std::fs::remove_dir_all(f.temp.path().join("index")).unwrap();
    let record = f.control.pending_operations().unwrap().remove(0);
    let result = resolve_catalog_operation(&record, f.catalog.as_ref(), &f.head, &f.control)
        .await
        .unwrap()
        .unwrap();
    f.control.resolve_operation(&id, Some(result)).unwrap();
    let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(
        f.control
            .table_state(&f.schema.table_id)
            .unwrap()
            .unwrap()
            .materialized_lsn,
        PgLsn(80)
    );
    assert!(stale.commit(f.catalog.as_ref(), &head).await.is_err());
    let prefix = format!("owned-artifacts/v1/{}/", head.metadata().uuid());
    assert!(
        f.control
            .source_transactions_after(prefix.as_bytes(), None)
            .count()
            > 0,
        "offline replay must register new metadata before upload"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dangling_delete_group_can_commit_without_output_files_or_index_deltas() {
    let mut f = Fixture::new().await;
    f.maintenance()
        .compact_deletes(
            &f.head,
            &f.schema,
            f.scratch("first-merge"),
            &DeleteRewritePolicy {
                min_input_files: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let before = SnapshotView::current(&f.head).await.unwrap();
    let mut writer = DataWriter::new(
        f.head.file_io().clone(),
        f.head.metadata().location(),
        &OperationId("external-data-rewrite".into()),
        f.schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&f.expected, PgLsn(80)).await.unwrap();
    let external = RewriteFilesAction::new(&f.head, "external-data-rewrite")
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files(
            before
                .live_files
                .values()
                .filter(|entry| entry.content_type() == DataContentType::Data)
                .map(|entry| entry.file_path().to_owned()),
        )
        .add_data_files(writer.close().await.unwrap())
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    let maintenance = f.maintenance();
    maintenance
        .reconcile(&external, &f.schema, f.scratch("reconcile"))
        .await
        .unwrap();
    let view = SnapshotView::current(&external).await.unwrap();
    assert_eq!(
        view.live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
            .count(),
        1
    );
    let snapshot = maintenance
        .compact_deletes(
            &external,
            &f.schema,
            f.scratch("dangling"),
            &DeleteRewritePolicy {
                min_input_files: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let after = SnapshotView::current(&head).await.unwrap();
    assert_eq!(after.snapshot_id, Some(snapshot));
    assert!(
        after
            .live_files
            .values()
            .all(|entry| entry.content_type() == DataContentType::Data)
    );
    assert_eq!(after.live_files.len(), 1);
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(80)
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_rewrite_preserves_base_and_residuals_through_lost_response_and_index_loss() {
    for lose_index in [false, true] {
        let mut f = Fixture::new().await;
        let before = SnapshotView::current(&f.head).await.unwrap();
        let base_path = f.locations[0].data_file_id.0.clone();
        let minor_path = f.locations[6].data_file_id.0.clone();
        let old_base_location = f
            .index
            .lookup(&f.schema.table_id, &f.schema.encode_key(&row(3)).unwrap())
            .unwrap()
            .unwrap();
        let policy = Policy {
            l0_soft_files: 2,
            l0_hard_files: 4,
            oldest_l0_soft_ms: 1,
            oldest_l0_hard_ms: 2,
            deleted_rows_percent: 100,
            min_file_age_ms: 0,
            ..Default::default()
        };
        let maintenance = TableMaintenance::new(
            f.index.clone(),
            f.catalog.clone(),
            policy,
            WriterConfig {
                row_group_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        f.catalog.lose_next_response_and_disconnect();
        assert!(
            maintenance
                .compact(&f.head, &f.schema, f.scratch("minor"))
                .await
                .is_err()
        );
        let pending = f.index.pending_operations().unwrap().remove(0);
        assert_eq!(pending.phase, OperationPhase::Prepared);
        assert_eq!(
            pending.delta_count, 4,
            "only live rows from the L0 file should move"
        );
        let payload: serde_json::Value =
            serde_json::from_slice(&pending.operation.payload).unwrap();
        assert_eq!(payload["removed_data"], serde_json::json!([minor_path]));
        assert_eq!(payload["delete_sequence"], 5);
        drop(maintenance);
        drop(f.index);
        f.catalog.reconnect();
        let committed = f.catalog.load_table(f.head.identifier()).await.unwrap();
        let snapshots = committed.metadata().snapshots().len();
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
                    StateStore::open(f.temp.path().join("rebuild-scratch"), options()).unwrap(),
                    PgLsn(80),
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
            f.maintenance()
                .recover(&committed, &pending.operation.id)
                .await
                .unwrap();
        }
        assert_eq!(
            f.catalog
                .load_table(f.head.identifier())
                .await
                .unwrap()
                .metadata()
                .snapshots()
                .len(),
            snapshots
        );
        let after = SnapshotView::current(&committed).await.unwrap();
        assert_eq!(
            after.live_files[&base_path], before.live_files[&base_path],
            "large base must remain physically unchanged"
        );
        assert!(!after.live_files.contains_key(&minor_path));
        // The oldest delete was inapplicable to the newer L0 input. It remains
        // unchanged, including the record for that now-obsolete L0 path.
        let untouched = before
            .live_files
            .values()
            .find(|entry| {
                entry.content_type() == DataContentType::PositionDeletes
                    && entry.sequence_number == Some(2)
            })
            .unwrap();
        assert_eq!(&after.live_files[untouched.file_path()], untouched);
        let residual = after
            .live_files
            .values()
            .filter(|entry| {
                entry.content_type() == DataContentType::PositionDeletes
                    && !before.live_files.contains_key(entry.file_path())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            residual.len(),
            2,
            "the one-row cohort limit splits the sorted residual"
        );
        for entry in &residual {
            assert_eq!(entry.data_file.record_count(), 1);
            assert_eq!(entry.sequence_number, Some(5));
            assert_eq!(entry.file_sequence_number, Some(6));
        }
        assert_eq!(
            sorted(scan(&committed, &f.schema).await.unwrap()),
            f.expected
        );
        assert_eq!(
            sorted(scan(&f.head, &f.schema).await.unwrap()),
            f.expected,
            "retained pre-rewrite snapshot changed"
        );
        let maintenance = f.maintenance();
        let inventory = maintenance
            .inventory(&committed, f.schema.table_id)
            .await
            .unwrap();
        for file in &inventory.files {
            assert_eq!(
                file.deleted_rows,
                if file.id.0 == base_path { 3 } else { 0 }
            );
        }
        assert_eq!(
            f.index
                .lookup(&f.schema.table_id, &f.schema.encode_key(&row(3)).unwrap())
                .unwrap(),
            Some(old_base_location)
        );
        let moved = f
            .index
            .lookup(&f.schema.table_id, &f.schema.encode_key(&row(10)).unwrap())
            .unwrap()
            .unwrap();
        assert_ne!(moved.data_file_id.0, minor_path);
        assert_eq!(moved.source_commit_lsn, PgLsn(80));
        assert_eq!(
            f.index
                .table_state(&f.schema.table_id)
                .unwrap()
                .materialized_lsn,
            PgLsn(80)
        );
        let next = delete(&committed, "after-residual", vec![moved.clone()]).await;
        let latest = RowDeltaAction::new(&committed, "after-residual")
            .add_delete_files(next)
            .validate_data_files_exist([moved.data_file_id.0])
            .commit(f.catalog.as_ref(), &committed)
            .await
            .unwrap()
            .table;
        assert_eq!(
            sorted(scan(&latest, &f.schema).await.unwrap()),
            f.expected
                .into_iter()
                .filter(|value| value[0] != Value::Int64(10))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dependency_repair_unblocks_bounded_data_rewrite_without_changing_rows_or_history() {
    let mut f = Fixture::new().await;
    f.maintenance()
        .compact_deletes(
            &f.head,
            &f.schema,
            f.scratch("initial-delete-union"),
            &DeleteRewritePolicy {
                min_input_files: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    // Five distinct effective positions in one shared file, followed by two
    // distinct late positions: neither data file fits the six-row read budget.
    let late = delete(
        &f.head,
        "late-shared",
        vec![f.locations[3].clone(), f.locations[9].clone()],
    )
    .await;
    let committed = RowDeltaAction::new(&f.head, "late-shared")
        .add_delete_files(late)
        .validate_data_files_exist([
            f.locations[3].data_file_id.0.clone(),
            f.locations[9].data_file_id.0.clone(),
        ])
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap();
    let operation = OperationId("late-shared-index".into());
    f.index
        .prepare(
            PreparedOperation {
                id: operation.clone(),
                table_id: f.schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: f.head.metadata().current_snapshot_id(),
                last_lsn: PgLsn(81),
                schema_version: f.schema.version,
                artifacts: vec![],
                payload: vec![],
            },
            [(3, 3), (13, 9)]
                .into_iter()
                .map(|(id, position)| flow_state_store::IndexDelta {
                    key: f.schema.encode_key(&row(id)).unwrap(),
                    expected: Some(f.locations[position].clone()),
                    replacement: None,
                }),
        )
        .unwrap();
    f.index
        .mark_committed(&operation, committed.snapshot_id, committed.sequence_number)
        .unwrap();
    f.index.apply_committed(&operation).unwrap();
    f.index.forget_applied(&operation).unwrap();
    f.head = committed.table;
    f.expected
        .retain(|value| value[0] != Value::Int64(3) && value[0] != Value::Int64(13));
    let before = SnapshotView::current(&f.head).await.unwrap();
    let policy = Policy {
        max_delete_input_rows: 6,
        min_file_age_ms: 0,
        ..Default::default()
    };
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        policy.clone(),
        WriterConfig::default(),
    )
    .unwrap();
    let inventory = maintenance
        .inventory(&f.head, f.schema.table_id)
        .await
        .unwrap();
    assert_eq!(
        policy.plan(
            before.snapshot_id.unwrap(),
            f.head.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes
        ),
        Err(flow_compactor::Error::DependencyBudget)
    );
    assert_eq!(
        inventory
            .deletes
            .iter()
            .map(|file| file.row_count)
            .sum::<u64>(),
        7
    );

    let too_small = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        Policy {
            max_delete_input_rows: 3,
            ..policy.clone()
        },
        WriterConfig::default(),
    )
    .unwrap();
    assert!(
        too_small
            .repair_delete_dependencies(
                &f.head,
                &f.schema,
                f.scratch("no-progress"),
                &mut DeleteRepairCursor::default()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
    assert_eq!(
        f.catalog
            .load_table(f.head.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        before.snapshot_id
    );
    drop(too_small);

    f.catalog.lose_next_response_and_disconnect();
    assert!(
        maintenance
            .repair_delete_dependencies(
                &f.head,
                &f.schema,
                f.scratch("repair"),
                &mut DeleteRepairCursor::default()
            )
            .await
            .is_err()
    );
    let pending = f.index.pending_operations().unwrap().remove(0);
    assert_eq!(pending.phase, OperationPhase::Prepared);
    assert_eq!(pending.delta_count, 0);
    drop(maintenance);
    drop(f.index);
    f.catalog.reconnect();
    f.index =
        StateStore::open_with_control(f.temp.path().join("index"), options(), f.control.clone())
            .unwrap();
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        policy.clone(),
        WriterConfig::default(),
    )
    .unwrap();
    let repaired = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let snapshot_count = repaired.metadata().snapshots().len();
    assert_eq!(
        resolve_catalog_operation(&pending, f.catalog.as_ref(), &repaired, &f.control)
            .await
            .unwrap()
            .unwrap()
            .0,
        repaired.metadata().current_snapshot_id().unwrap()
    );
    maintenance
        .recover(&repaired, &pending.operation.id)
        .await
        .unwrap();
    f.index.forget_applied(&pending.operation.id).unwrap();
    assert_eq!(
        f.catalog
            .load_table(f.head.identifier())
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .len(),
        snapshot_count
    );
    let after = SnapshotView::current(&repaired).await.unwrap();
    let outputs = after
        .live_files
        .values()
        .filter(|entry| !before.live_files.contains_key(entry.file_path()))
        .collect::<Vec<_>>();
    assert_eq!(
        outputs.len(),
        2,
        "only the two nonempty ordered path ranges are written"
    );
    assert_eq!(
        outputs
            .iter()
            .map(|entry| entry.data_file.record_count())
            .sum::<u64>(),
        5
    );
    for entry in &outputs {
        assert_eq!(entry.content_type(), DataContentType::PositionDeletes);
        assert_eq!(entry.sequence_number, Some(5));
        assert_eq!(entry.file_sequence_number, Some(8));
        assert_eq!(
            entry.data_file.lower_bounds()[&2147483546],
            entry.data_file.upper_bounds()[&2147483546]
        );
    }
    for (path, entry) in &before.live_files {
        if entry.content_type() == DataContentType::Data || entry.sequence_number == Some(7) {
            assert_eq!(&after.live_files[path], entry);
        }
    }
    assert_eq!(
        sorted(scan(&repaired, &f.schema).await.unwrap()),
        f.expected
    );
    assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
    let inventory = maintenance
        .inventory(&repaired, f.schema.table_id)
        .await
        .unwrap();
    assert_eq!(
        inventory
            .files
            .iter()
            .map(|file| file.deleted_rows)
            .sum::<u64>(),
        7
    );
    let plan = policy
        .plan(
            after.snapshot_id.unwrap(),
            repaired.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )
        .unwrap()
        .unwrap();
    assert!(plan.delete_input_rows <= 6);
    assert_eq!(
        plan.input_files.len(),
        1,
        "combining the localized ranges would exceed the read budget"
    );
    assert!(
        maintenance
            .repair_delete_dependencies(
                &repaired,
                &f.schema,
                f.scratch("no-repeat"),
                &mut DeleteRepairCursor::default()
            )
            .await
            .unwrap()
            .is_none()
    );
    maintenance
        .compact(&repaired, &f.schema, f.scratch("bounded-data"))
        .await
        .unwrap()
        .unwrap();
    let final_head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    assert_eq!(
        sorted(scan(&final_head, &f.schema).await.unwrap()),
        f.expected
    );
    assert_eq!(
        sorted(scan(&repaired, &f.schema).await.unwrap()),
        f.expected
    );
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(81)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_consolidation_preserves_locality_after_restart() {
    let mut f = Fixture::new().await;
    let original = f.head.clone();
    let before = SnapshotView::current(&f.head).await.unwrap();
    let mut localized = Vec::new();
    for (name, positions) in [
        ("localized-first", vec![0, 1, 2]),
        ("localized-second", vec![7, 8]),
    ] {
        localized.extend(
            delete(
                &f.head,
                name,
                positions
                    .into_iter()
                    .map(|i| f.locations[i].clone())
                    .collect(),
            )
            .await,
        );
    }
    assert_eq!(localized.len(), 2);
    // Model a completed external repair. Each path-local range fits a four-row
    // read budget; merging the ranges charges all five rows to both data files.
    f.head = RewriteFilesAction::new(&f.head, "external-localization")
        .with_operation_id_key("test.external.operation-id")
        .unwrap()
        .remove_delete_files(
            before
                .live_files
                .values()
                .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
                .map(|entry| entry.file_path().to_owned())
                .collect::<BTreeSet<_>>(),
        )
        .rewrite_position_deletes(localized, 5)
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    f.maintenance()
        .reconcile(&f.head, &f.schema, f.scratch("localization-reconcile"))
        .await
        .unwrap();
    let localized = SnapshotView::current(&f.head).await.unwrap();
    let policy = Policy {
        max_delete_input_rows: 4,
        min_file_age_ms: 0,
        ..Default::default()
    };
    for attempt in 0..2 {
        let maintenance = TableMaintenance::new(
            f.index.clone(),
            f.catalog.clone(),
            policy.clone(),
            WriterConfig::default(),
        )
        .unwrap();
        assert!(
            maintenance
                .compact_deletes(
                    &f.head,
                    &f.schema,
                    f.scratch(&format!("locality-guard-{attempt}")),
                    &DeleteRewritePolicy {
                        min_input_files: 2,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .is_none(),
            "a lower file count must not recreate an oversized dependency union"
        );
        assert!(f.index.pending_operations().unwrap().is_empty());
        let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
        let after = SnapshotView::current(&head).await.unwrap();
        assert_eq!(after.snapshot_id, localized.snapshot_id);
        assert_eq!(after.live_files, localized.live_files);
        drop(maintenance);
        if attempt == 0 {
            drop(f.index);
            f.index = StateStore::open_with_control(
                f.temp.path().join("index"),
                options(),
                f.control.clone(),
            )
            .unwrap();
        }
    }
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        policy.clone(),
        WriterConfig::default(),
    )
    .unwrap();
    let inventory = maintenance
        .inventory(&f.head, f.schema.table_id)
        .await
        .unwrap();
    let plan = policy
        .plan(
            localized.snapshot_id.unwrap(),
            f.head.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )
        .unwrap()
        .unwrap();
    assert_eq!(plan.input_files.len(), 1);
    assert!(plan.delete_input_rows <= 4);
    maintenance
        .compact(&f.head, &f.schema, f.scratch("localized-data"))
        .await
        .unwrap()
        .unwrap();
    let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(
        sorted(scan(&original, &f.schema).await.unwrap()),
        f.expected
    );
    assert_eq!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(80)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_checks_alternate_targets_inputs_and_exhaustion_without_repeating_reads() {
    // Cases respectively require another target in the same staged input,
    // another bounded input on the next pass, and exhausting both inputs.
    for case in 0..3 {
        let mut f = Fixture::new().await;
        let (left, right, left_extra, extra_count, density) = match case {
            0 => (vec![0, 1, 7], vec![2, 8], 2, 4, 30),
            1 => (vec![0, 1, 2, 7], vec![8], 0, 2, 50),
            _ => (vec![0, 1, 7], vec![2, 3, 8], 1, 3, 50),
        };
        let mut writer = DataWriter::new(
            f.head.file_io().clone(),
            f.head.metadata().location(),
            &OperationId("flow-l2-healthy-other".into()),
            f.schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        let mut extra = writer
            .write(&(1000..1100).map(row).collect::<Vec<_>>(), PgLsn(82))
            .await
            .unwrap()
            .locations;
        let data = writer.close().await.unwrap();
        let added_data = RowDeltaAction::new(&f.head, "other-data")
            .add_data_files(data)
            .commit(f.catalog.as_ref(), &f.head)
            .await
            .unwrap();
        extra
            .iter_mut()
            .for_each(|location| location.data_sequence_number = added_data.sequence_number);
        let mut new_deletes = extra[..extra_count].to_vec();
        if case == 2 {
            new_deletes.push(f.locations[3].clone());
        }
        let referenced = new_deletes
            .iter()
            .map(|location| location.data_file_id.0.clone())
            .collect::<BTreeSet<_>>();
        let added = delete(&added_data.table, "other-deletes", new_deletes).await;
        let commit = RowDeltaAction::new(&added_data.table, "other-deletes")
            .add_delete_files(added)
            .validate_data_files_exist(referenced)
            .commit(f.catalog.as_ref(), &added_data.table)
            .await
            .unwrap();
        let mut first_positions = left
            .into_iter()
            .map(|i| f.locations[i].clone())
            .collect::<Vec<_>>();
        first_positions.extend_from_slice(&extra[..left_extra]);
        let mut second_positions = right
            .into_iter()
            .map(|i| f.locations[i].clone())
            .collect::<Vec<_>>();
        second_positions.extend_from_slice(&extra[left_extra..extra_count]);
        let mut inputs = delete(&commit.table, "probe-left", first_positions).await;
        inputs.extend(delete(&commit.table, "probe-right", second_positions).await);
        assert_eq!(inputs.len(), 2);
        let input_paths = inputs
            .iter()
            .map(|file| file.file_path().to_owned())
            .collect::<Vec<_>>();
        let initial = SnapshotView::current(&commit.table).await.unwrap();
        let commit = RewriteFilesAction::new(&commit.table, "external-shared-layout")
            .remove_delete_files(
                initial
                    .live_files
                    .values()
                    .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
                    .map(|entry| entry.file_path().to_owned()),
            )
            .rewrite_position_deletes(inputs, commit.sequence_number)
            .commit(f.catalog.as_ref(), &commit.table)
            .await
            .unwrap();
        let operation = OperationId("other-index".into());
        let mut deltas = extra
            .iter()
            .enumerate()
            .skip(extra_count)
            .map(|(i, location)| flow_state_store::IndexDelta {
                key: f.schema.encode_key(&row(1000 + i as i64)).unwrap(),
                expected: None,
                replacement: Some(location.clone()),
            })
            .collect::<Vec<_>>();
        if case == 2 {
            deltas.push(flow_state_store::IndexDelta {
                key: f.schema.encode_key(&row(3)).unwrap(),
                expected: Some(f.locations[3].clone()),
                replacement: None,
            });
            f.expected.retain(|value| value[0] != Value::Int64(3));
        }
        f.index
            .prepare(
                PreparedOperation {
                    id: operation.clone(),
                    table_id: f.schema.table_id,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: f.head.metadata().current_snapshot_id(),
                    last_lsn: PgLsn(82),
                    schema_version: f.schema.version,
                    artifacts: vec![],
                    payload: vec![],
                },
                deltas,
            )
            .unwrap();
        f.index
            .mark_committed(&operation, commit.snapshot_id, commit.sequence_number)
            .unwrap();
        f.index.apply_committed(&operation).unwrap();
        f.index.forget_applied(&operation).unwrap();
        f.head = commit.table;
        f.expected
            .extend((1000 + extra_count as i64..1100).map(row));
        assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
        let before = SnapshotView::current(&f.head).await.unwrap();
        let policy = Policy {
            max_delete_input_rows: 5,
            deleted_rows_percent: density,
            min_file_age_ms: 0,
            ..Default::default()
        };
        let maintenance = TableMaintenance::new(
            f.index.clone(),
            f.catalog.clone(),
            policy.clone(),
            WriterConfig::default(),
        )
        .unwrap();
        let inventory = maintenance
            .inventory(&f.head, f.schema.table_id)
            .await
            .unwrap();
        assert_eq!(
            policy.plan(
                before.snapshot_id.unwrap(),
                f.head.metadata().current_schema_id(),
                &inventory.files,
                &inventory.deletes
            ),
            Err(flow_compactor::Error::DependencyBudget)
        );
        let mut cursor = DeleteRepairCursor::default();
        let first = maintenance
            .repair_delete_dependencies(&f.head, &f.schema, f.scratch("first-probe"), &mut cursor)
            .await
            .unwrap();
        if case == 0 {
            assert!(
                first.is_some(),
                "the same input must be checked against both dense targets"
            );
        } else {
            assert!(first.is_none());
            assert!(f.index.pending_operations().unwrap().is_empty());
            if case == 1 {
                // The failed larger input remains in catalog metadata but is
                // temporarily unreadable. The second pass must only read its sibling.
                let held = f.temp.path().join("held-first-input");
                std::fs::rename(&input_paths[0], &held).unwrap();
                let second = maintenance
                    .repair_delete_dependencies(
                        &f.head,
                        &f.schema,
                        f.scratch("second-probe"),
                        &mut cursor,
                    )
                    .await
                    .unwrap();
                std::fs::rename(&held, &input_paths[0]).unwrap();
                assert!(second.is_some());
            } else {
                assert!(
                    maintenance
                        .repair_delete_dependencies(
                            &f.head,
                            &f.schema,
                            f.scratch("second-probe"),
                            &mut cursor
                        )
                        .await
                        .unwrap()
                        .is_none()
                );
                let held = [
                    f.temp.path().join("held-left"),
                    f.temp.path().join("held-right"),
                ];
                for (path, held) in input_paths.iter().zip(&held) {
                    std::fs::rename(path, held).unwrap();
                }
                assert!(
                    maintenance
                        .repair_delete_dependencies(
                            &f.head,
                            &f.schema,
                            f.scratch("exhausted"),
                            &mut cursor
                        )
                        .await
                        .unwrap()
                        .is_none()
                );
                for (path, held) in input_paths.iter().zip(&held) {
                    std::fs::rename(held, path).unwrap();
                }
                assert!(f.index.pending_operations().unwrap().is_empty());
            }
        }
        let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
        assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), f.expected);
        assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
        let after = SnapshotView::current(&head).await.unwrap();
        for (path, entry) in &before.live_files {
            if entry.content_type() == DataContentType::Data {
                assert_eq!(&after.live_files[path], entry);
            }
        }
        if case == 2 {
            assert_eq!(head.metadata().current_snapshot_id(), before.snapshot_id);
            assert_eq!(after.live_files, before.live_files);
        } else {
            let inventory = maintenance
                .inventory(&head, f.schema.table_id)
                .await
                .unwrap();
            let plan = policy
                .plan(
                    after.snapshot_id.unwrap(),
                    head.metadata().current_schema_id(),
                    &inventory.files,
                    &inventory.deletes,
                )
                .unwrap()
                .unwrap();
            assert!(plan.delete_input_rows <= 5);
            assert!(
                after
                    .live_files
                    .values()
                    .filter(|entry| !before.live_files.contains_key(entry.file_path()))
                    .count()
                    <= 3
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn byte_failed_target_does_not_exhaust_a_shared_delete_input() {
    use iceberg::spec::{DataFile, DataFileBuilder};
    // Unreferenced padding before the footer leaves every column/page offset
    // intact. Charge the actual resulting size, rather than fabricated metadata.
    fn pad(file: DataFile, size: u64) -> DataFile {
        let mut bytes = std::fs::read(file.file_path()).unwrap();
        assert!(size as usize > bytes.len());
        let footer = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
            as usize;
        let start = bytes.len() - 8 - footer;
        let padding = size as usize - bytes.len();
        bytes.splice(start..start, std::iter::repeat_n(0, padding));
        std::fs::write(file.file_path(), &bytes).unwrap();
        DataFileBuilder::default()
            .content(file.content_type())
            .file_path(file.file_path().to_owned())
            .file_format(file.file_format())
            .partition(file.partition().clone())
            .record_count(file.record_count())
            .file_size_in_bytes(size)
            .lower_bounds(file.lower_bounds().clone())
            .upper_bounds(file.upper_bounds().clone())
            .build()
            .unwrap()
    }
    let mut f = Fixture::new().await;
    f.maintenance()
        .compact_deletes(
            &f.head,
            &f.schema,
            f.scratch("union"),
            &DeleteRewritePolicy {
                min_input_files: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let before = SnapshotView::current(&f.head).await.unwrap();
    let shared = before
        .live_files
        .values()
        .find(|entry| entry.content_type() == DataContentType::PositionDeletes)
        .unwrap();
    let shared_bytes = shared.data_file.file_size_in_bytes();
    let isolated_a = delete(&f.head, "measure-a", f.locations[..3].to_vec()).await;
    let isolated_b = delete(&f.head, "measure-b", f.locations[7..9].to_vec()).await;
    let budget = 64 << 10;
    let a_bytes = isolated_a[0].file_size_in_bytes();
    let b_bytes = isolated_b[0].file_size_in_bytes();
    assert!(shared_bytes > b_bytes + 1);
    let a = pad(
        delete(&f.head, "other-a", vec![f.locations[0].clone()])
            .await
            .remove(0),
        budget - a_bytes + 1,
    );
    let b = pad(
        delete(&f.head, "other-b", vec![f.locations[7].clone()])
            .await
            .remove(0),
        budget - b_bytes - 1,
    );
    let commit = RowDeltaAction::new(&f.head, "duplicate-effective-deletes")
        .add_delete_files(vec![a, b])
        .validate_data_files_exist([
            f.locations[0].data_file_id.0.clone(),
            f.locations[7].data_file_id.0.clone(),
        ])
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap();
    // These extra physical deletes duplicate already deleted positions.
    assert_eq!(
        sorted(scan(&commit.table, &f.schema).await.unwrap()),
        f.expected
    );
    let id = OperationId("unchanged-row-index".into());
    f.index
        .prepare(
            PreparedOperation {
                id: id.clone(),
                table_id: f.schema.table_id,
                kind: OperationKind::Rewrite,
                base_snapshot_id: f.head.metadata().current_snapshot_id(),
                last_lsn: PgLsn(80),
                schema_version: f.schema.version,
                artifacts: vec![],
                payload: vec![],
            },
            [],
        )
        .unwrap();
    f.index
        .mark_committed(&id, commit.snapshot_id, commit.sequence_number)
        .unwrap();
    f.index.apply_committed(&id).unwrap();
    f.index.forget_applied(&id).unwrap();
    f.head = commit.table;
    let policy = Policy {
        max_delete_input_bytes: budget,
        min_file_age_ms: 0,
        ..Default::default()
    };
    let maintenance = TableMaintenance::new(
        f.index.clone(),
        f.catalog.clone(),
        policy.clone(),
        WriterConfig::default(),
    )
    .unwrap();
    let mut cursor = DeleteRepairCursor::default();
    assert!(
        maintenance
            .repair_delete_dependencies(&f.head, &f.schema, f.scratch("first-target"), &mut cursor)
            .await
            .unwrap()
            .is_none()
    );
    assert!(f.index.pending_operations().unwrap().is_empty());
    assert!(
        maintenance
            .repair_delete_dependencies(&f.head, &f.schema, f.scratch("second-target"), &mut cursor)
            .await
            .unwrap()
            .is_some(),
        "the second target must remain eligible after the first output exceeds its byte budget"
    );
    let repaired = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let inventory = maintenance
        .inventory(&repaired, f.schema.table_id)
        .await
        .unwrap();
    let plan = policy
        .plan(
            repaired.metadata().current_snapshot_id().unwrap(),
            repaired.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )
        .unwrap()
        .unwrap();
    assert!(plan.input_files.contains(&f.locations[7].data_file_id));
    assert!(
        maintenance
            .compact(&repaired, &f.schema, f.scratch("unblocked-data"))
            .await
            .unwrap()
            .is_some()
    );
    let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    assert_eq!(sorted(scan(&head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
}
