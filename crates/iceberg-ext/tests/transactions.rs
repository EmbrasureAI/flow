use std::collections::HashMap;
use std::sync::Arc;

use flow_iceberg_ext::{
    RewriteFilesAction, RowDeltaAction, SnapshotView, find_operation, read_artifact_plan,
    write_artifact_plan,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion,
    ManifestContentType, ManifestStatus, NestedField, Operation, PrimitiveType, Schema, Struct,
    Type,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

async fn setup() -> (iceberg::MemoryCatalog, Table) {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                "memory://warehouse".to_owned(),
            )]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("replicated".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".to_owned())
                .schema(schema)
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    (catalog, table)
}

// These fixtures test transaction metadata and Avro round trips. Parquet row
// encoding and reader results are covered by materializer integration tests.
fn data(name: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("memory://warehouse/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .file_size_in_bytes(128)
        .record_count(10)
        .build()
        .unwrap()
}

fn delete(name: &str, target: &DataFile) -> DataFile {
    let bounds = HashMap::from([(
        iceberg::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH,
        Datum::string(target.file_path()),
    )]);
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(format!("memory://warehouse/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .lower_bounds(bounds.clone())
        .upper_bounds(bounds)
        .file_size_in_bytes(64)
        .record_count(1)
        .build()
        .unwrap()
}

#[tokio::test]
async fn row_delta_publishes_data_and_deletes_together_and_recovers_from_history() {
    let (catalog, empty) = setup().await;
    let original = data("original");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![original.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let replacement = data("replacement");
    let tombstone = delete("delete-original", &original);
    let update = RowDeltaAction::new(&initial.table, "update-1")
        .add_data_files(vec![replacement.clone()])
        .add_delete_files(vec![tombstone.clone()])
        .validate_data_files_exist([original.file_path().to_owned()]);
    let committed = update.commit(&catalog, &initial.table).await.unwrap();
    assert_eq!(committed.sequence_number, 2);
    let view = SnapshotView::current(&committed.table).await.unwrap();
    assert_eq!(view.live_files.len(), 3);
    for file in [&replacement, &tombstone] {
        let entry = &view.live_files[file.file_path()];
        assert_eq!(entry.sequence_number, Some(2));
        assert_eq!(entry.file_sequence_number, Some(2));
        assert_eq!(entry.snapshot_id, Some(committed.snapshot_id));
    }
    assert_eq!(
        view.live_files[original.file_path()].sequence_number,
        Some(1)
    );
    assert_eq!(
        view.applicable_deletes(original.file_path()).unwrap().len(),
        1
    );
    assert!(
        view.applicable_deletes(replacement.file_path())
            .unwrap()
            .is_empty()
    );
    let later = RowDeltaAction::new(&committed.table, "later")
        .add_data_files(vec![data("later")])
        .commit(&catalog, &committed.table)
        .await
        .unwrap();
    let recovered = update.commit(&catalog, &initial.table).await.unwrap();
    assert!(recovered.already_committed);
    assert_eq!(recovered.snapshot_id, committed.snapshot_id);
    assert_eq!(
        recovered.table.metadata().current_snapshot_id(),
        Some(later.snapshot_id)
    );
    assert_eq!(recovered.table.metadata().snapshots().len(), 3);
    assert!(find_operation(recovered.table.metadata(), "update-1").is_some());
}

#[tokio::test]
async fn indexed_append_fences_external_rewrites_while_generic_append_can_rebase() {
    let (catalog, empty) = setup().await;
    let original = data("original");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![original.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let prepared =
        RowDeltaAction::new(&initial.table, "new-keys").add_data_files(vec![data("new-keys")]);
    let rewritten = RewriteFilesAction::new(&initial.table, "external-rewrite")
        .remove_data_files([original.file_path().to_owned()])
        .add_data_files(vec![data("compacted")])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    let strict = prepared.clone().require_base_snapshot();
    let error = strict.commit(&catalog, &rewritten.table).await.unwrap_err();
    assert_eq!(error.kind(), iceberg::ErrorKind::PreconditionFailed);
    let committed = prepared.commit(&catalog, &rewritten.table).await.unwrap();
    assert_eq!(committed.sequence_number, 3);
    let recovered = strict.commit(&catalog, &initial.table).await.unwrap();
    assert!(recovered.already_committed);
    assert_eq!(recovered.snapshot_id, committed.snapshot_id);
    assert_eq!(
        SnapshotView::current(&recovered.table)
            .await
            .unwrap()
            .live_files
            .len(),
        2
    );
}

#[tokio::test]
async fn stale_compaction_cannot_resurrect_a_concurrently_deleted_row() {
    let (catalog, empty) = setup().await;
    let original = data("base");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![original.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    // Worker read rows from snapshot S before ingestion deleted one of them.
    let stale = RewriteFilesAction::new(&initial.table, "stale-worker")
        .remove_data_files([original.file_path().to_owned()])
        .add_data_files(vec![data("stale-output")]);
    let deleted = RowDeltaAction::new(&initial.table, "delete-row")
        .add_delete_files(vec![delete("concurrent-delete", &original)])
        .validate_data_files_exist([original.file_path().to_owned()])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    let error = stale.commit(&catalog, &initial.table).await.unwrap_err();
    assert!(error.message().contains("delete state changed"), "{error}");
    let head = catalog.load_table(empty.identifier()).await.unwrap();
    assert_eq!(
        head.metadata().current_snapshot_id(),
        Some(deleted.snapshot_id)
    );
    let view = SnapshotView::current(&head).await.unwrap();
    assert_eq!(
        view.applicable_deletes(original.file_path()).unwrap().len(),
        1
    );
    assert!(
        !view
            .live_files
            .contains_key(data("stale-output").file_path())
    );
}

#[tokio::test]
async fn rewrite_survives_disjoint_append_and_preserves_sequence_numbers() {
    let (catalog, empty) = setup().await;
    let first = data("first");
    let second = data("second");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![first.clone(), second.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    // Two fully consumed manifests of each content, plus the initial mixed
    // manifest (first is removed, second survives).
    let mut base = initial.table.clone();
    let mut removed_data = vec![first.file_path().to_owned()];
    let mut removed_deletes = Vec::new();
    for index in 0..2 {
        let file = data(&format!("consumed-{index}"));
        let tombstone = delete(&format!("consumed-delete-{index}"), &first);
        removed_data.push(file.file_path().to_owned());
        removed_deletes.push(tombstone.file_path().to_owned());
        base = RowDeltaAction::new(&base, format!("input-{index}"))
            .add_data_files(vec![file])
            .add_delete_files(vec![tombstone])
            .validate_data_files_exist([first.file_path().to_owned()])
            .commit(&catalog, &base)
            .await
            .unwrap()
            .table;
    }
    let rewritten = data("compacted");
    let rewrite = RewriteFilesAction::new(&base, "rewrite")
        .with_operation_id_key("other-writer.operation-id")
        .unwrap()
        .remove_data_files(removed_data.clone())
        .remove_delete_files(removed_deletes.clone())
        .add_data_files(vec![rewritten.clone()]);
    let unrelated = RowDeltaAction::new(&base, "unrelated")
        .add_data_files(vec![data("unrelated")])
        .commit(&catalog, &base)
        .await
        .unwrap();
    let before = SnapshotView::current(&unrelated.table).await.unwrap();
    let original_manifests = unrelated
        .table
        .manifest_list_reader(unrelated.table.metadata().current_snapshot().unwrap())
        .load()
        .await
        .unwrap()
        .consume_entries()
        .into_iter()
        .collect::<Vec<_>>();
    let strict = rewrite.clone().require_base_snapshot();
    let strict_attempt = strict
        .commit_with_diagnostics(&catalog, &unrelated.table)
        .await;
    assert!(!strict_attempt.catalog_update_attempted());
    assert_eq!(strict_attempt.already_committed(), None);
    let error = strict_attempt.result.unwrap_err();
    assert_eq!(error.kind(), iceberg::ErrorKind::PreconditionFailed);
    let rewrite_attempt = rewrite
        .commit_with_diagnostics(&catalog, &unrelated.table)
        .await;
    assert!(rewrite_attempt.catalog_update_attempted());
    assert_eq!(rewrite_attempt.already_committed(), Some(false));
    let result = rewrite_attempt.result.unwrap();
    assert_eq!(result.sequence_number, unrelated.sequence_number + 1);
    assert_eq!(
        result
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .operation,
        Operation::Replace
    );
    let view = SnapshotView::current(&result.table).await.unwrap();
    assert!(!view.live_files.contains_key(first.file_path()));
    let output = &view.live_files[rewritten.file_path()];
    assert_eq!(output.status, ManifestStatus::Added);
    assert_eq!(
        output.sequence_number,
        Some(base.metadata().last_sequence_number())
    );
    assert_eq!(output.file_sequence_number, Some(result.sequence_number));
    let survivor = &view.live_files[second.file_path()];
    assert_eq!(survivor.sequence_number, Some(1));
    assert_eq!(survivor.file_sequence_number, Some(1));
    assert_eq!(survivor.snapshot_id, Some(initial.snapshot_id));
    assert_eq!(view.live_files.len(), 3);
    let snapshot = result.table.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot.parent_snapshot_id(), before.snapshot_id);
    assert_eq!(
        result.table.metadata().snapshots().len(),
        unrelated.table.metadata().snapshots().len() + 1
    );
    let manifests = result
        .table
        .manifest_list_reader(snapshot)
        .load()
        .await
        .unwrap()
        .consume_entries()
        .into_iter()
        .collect::<Vec<_>>();
    // One mixed rewrite, one unchanged manifest, and one group per content.
    // The delete group must exist even though this rewrite adds no delete file.
    assert_eq!(manifests.len(), 4);
    let unchanged = original_manifests
        .iter()
        .find(|manifest| manifest.added_snapshot_id == unrelated.snapshot_id)
        .unwrap();
    assert!(manifests.contains(unchanged));
    let mut actual_removed = Vec::new();
    let mut new_contents = [0, 0];
    for manifest in &manifests {
        let loaded = manifest
            .load_manifest(result.table.file_io())
            .await
            .unwrap();
        assert_eq!(loaded.metadata().content, manifest.content);
        assert_eq!(
            loaded.metadata().partition_spec.spec_id(),
            manifest.partition_spec_id
        );
        assert_eq!(
            manifest.partition_spec_id,
            base.metadata().default_partition_spec_id()
        );
        if manifest.added_snapshot_id == result.snapshot_id {
            new_contents[manifest.content as usize] += 1;
        }
        for entry in loaded.entries() {
            assert_eq!(
                entry.content_type() == DataContentType::Data,
                manifest.content == ManifestContentType::Data
            );
            if entry.status == ManifestStatus::Deleted {
                // Standard removedDataFiles/removedDeleteFiles enumerate changes
                // from manifests attributed to this publication, not just summaries.
                assert_eq!(manifest.added_snapshot_id, result.snapshot_id);
                let mut expected = before.live_files[entry.file_path()].as_ref().clone();
                expected.status = ManifestStatus::Deleted;
                expected.snapshot_id = Some(result.snapshot_id);
                assert_eq!(entry.as_ref(), &expected);
                actual_removed.push(entry.file_path().to_owned());
            }
        }
    }
    assert_eq!(new_contents, [2, 1]);
    let mut expected_removed = removed_data;
    expected_removed.extend(removed_deletes);
    expected_removed.sort();
    actual_removed.sort();
    assert_eq!(actual_removed, expected_removed);
    assert_eq!(
        SnapshotView::load(&result.table, before.snapshot_id.unwrap())
            .await
            .unwrap()
            .live_files,
        before.live_files
    );
    let properties = &result
        .table
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties;
    assert_eq!(properties["other-writer.operation-id"], "rewrite");
    assert!(!properties.contains_key("flow.operation-id"));
    assert!(find_operation(result.table.metadata(), "rewrite").is_none());
    // Distinct writer namespaces can use the same ID. Recovery must still find
    // the correct older snapshot after another writer advances the head.
    let later = RowDeltaAction::new(&result.table, "rewrite")
        .add_data_files(vec![data("after-custom-rewrite")])
        .commit(&catalog, &result.table)
        .await
        .unwrap();
    assert_eq!(later.sequence_number, result.sequence_number + 1);
    let retry_attempt = strict
        .commit_with_diagnostics(&catalog, &initial.table)
        .await;
    assert!(!retry_attempt.catalog_update_attempted());
    assert_eq!(retry_attempt.already_committed(), Some(true));
    let retry = retry_attempt.result.unwrap();
    assert!(retry.already_committed);
    assert_eq!(retry.snapshot_id, result.snapshot_id);
}

#[tokio::test]
async fn shared_delete_requires_all_potential_targets_to_be_consumed() {
    let (catalog, empty) = setup().await;
    let first = data("first");
    let second = data("second");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![first.clone(), second.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let shared = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("memory://warehouse/shared-delete.parquet".to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .file_size_in_bytes(64)
        .record_count(2)
        .build()
        .unwrap();
    let deleted = RowDeltaAction::new(&initial.table, "shared-delete")
        .add_delete_files(vec![shared.clone()])
        .validate_data_files_exist([first.file_path().to_owned(), second.file_path().to_owned()])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    let rewrite = RewriteFilesAction::new(&deleted.table, "rewrite")
        .remove_data_files([first.file_path().to_owned()])
        .remove_delete_files([shared.file_path().to_owned()])
        .add_data_files(vec![data("output")]);
    assert!(
        rewrite
            .commit(&catalog, &deleted.table)
            .await
            .unwrap_err()
            .message()
            .contains("shared delete")
    );
    // The worker may retain the shared file. Its path matching prevents it from
    // deleting replacement rows; it continues to protect the surviving input.
    let result = RewriteFilesAction::new(&deleted.table, "retain-shared")
        .remove_data_files([first.file_path().to_owned()])
        .add_data_files(vec![data("output")])
        .commit(&catalog, &deleted.table)
        .await
        .unwrap();
    assert!(
        SnapshotView::current(&result.table)
            .await
            .unwrap()
            .live_files
            .contains_key(shared.file_path())
    );
}

#[tokio::test]
async fn native_prepared_plan_round_trip_preserves_metrics() {
    let (_, table) = setup().await;
    let row = data("data");
    let tombstone = delete("delete", &row);
    let path = "memory://warehouse/prepared/operation.avro";
    write_artifact_plan(
        &table,
        path,
        std::slice::from_ref(&row),
        std::slice::from_ref(&tombstone),
    )
    .await
    .unwrap();
    let loaded = read_artifact_plan(table.file_io(), path).await.unwrap();
    assert_eq!(loaded.added_data, vec![row]);
    assert_eq!(loaded.added_deletes, vec![tombstone]);
    assert!(write_artifact_plan(&table, path, &[], &[]).await.is_err());
}

#[tokio::test]
async fn manifest_rewrite_preserves_data_delete_identities_and_fences_stale_metadata() {
    use flow_iceberg_ext::{ManifestCache, ManifestRewritePolicy, RewriteManifestsAction};
    let (catalog, mut table) = setup().await;
    let mut previous = None;
    for index in 0..4 {
        let file = data(&format!("manifest-data-{index}"));
        let mut action = RowDeltaAction::new(&table, format!("manifest-input-{index}"))
            .add_data_files(vec![file.clone()]);
        if let Some(previous) = &previous {
            action = action
                .add_delete_files(vec![delete(&format!("manifest-delete-{index}"), previous)])
                .validate_data_files_exist([previous.file_path().to_owned()]);
        }
        table = action.commit(&catalog, &table).await.unwrap().table;
        previous = Some(file);
    }
    let before = SnapshotView::current(&table).await.unwrap();
    assert_eq!(before.manifest_count(), 7);
    let policy = ManifestRewritePolicy {
        min_manifest_count: 2,
        ..Default::default()
    };
    let rewrite = RewriteManifestsAction::plan(&table, "merge-manifests", &policy)
        .await
        .unwrap()
        .unwrap();
    let stale = RewriteManifestsAction::plan(&table, "stale-manifests", &policy)
        .await
        .unwrap()
        .unwrap();
    for action in [&rewrite, &stale] {
        for path in action.artifacts() {
            assert!(!table.file_io().exists(&path).await.unwrap());
        }
        action
            .write_artifacts(&table, &ManifestCache::default())
            .await
            .unwrap();
        assert!(
            action
                .write_artifacts(&table, &ManifestCache::default())
                .await
                .is_err()
        );
    }
    let commit_attempt = rewrite.commit_with_diagnostics(&catalog, &table).await;
    assert!(commit_attempt.catalog_update_attempted());
    assert_eq!(commit_attempt.already_committed(), Some(false));
    let committed = commit_attempt.result.unwrap();
    let after = SnapshotView::current(&committed.table).await.unwrap();
    assert_eq!(after.manifest_count(), 2);
    assert_eq!(after.live_files.len(), before.live_files.len());
    for (path, original) in &before.live_files {
        let current = &after.live_files[path];
        assert_eq!(current.data_file, original.data_file);
        assert_eq!(current.snapshot_id, original.snapshot_id);
        assert_eq!(current.sequence_number, original.sequence_number);
        assert_eq!(current.file_sequence_number, original.file_sequence_number);
        if current.content_type() == DataContentType::Data {
            let targets = |view: &SnapshotView| {
                view.applicable_deletes(path)
                    .unwrap()
                    .into_iter()
                    .map(|entry| entry.file_path().to_owned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(targets(&after), targets(&before));
        }
    }
    let snapshot_count = committed.table.metadata().snapshots().len();
    let recovery_attempt = rewrite.commit_with_diagnostics(&catalog, &table).await;
    assert!(!recovery_attempt.catalog_update_attempted());
    assert_eq!(recovery_attempt.already_committed(), Some(true));
    let recovered = recovery_attempt.result.unwrap();
    assert!(recovered.already_committed);
    assert_eq!(recovered.snapshot_id, committed.snapshot_id);
    assert_eq!(recovered.table.metadata().snapshots().len(), snapshot_count);
    let stale_attempt = stale.commit_with_diagnostics(&catalog, &table).await;
    assert!(!stale_attempt.catalog_update_attempted());
    assert_eq!(stale_attempt.already_committed(), None);
    assert!(stale_attempt.result.is_err());
    assert!(
        RewriteManifestsAction::plan(&committed.table, "idle-manifests", &policy)
            .await
            .unwrap()
            .is_none()
    );
    let historical = SnapshotView::load(&committed.table, before.snapshot_id.unwrap())
        .await
        .unwrap();
    assert_eq!(historical.live_files, before.live_files);
}

#[tokio::test]
async fn ingestion_rejects_a_position_delete_after_its_target_was_rewritten() {
    let (catalog, empty) = setup().await;
    let original = data("original");
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![original.clone()])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let pending = RowDeltaAction::new(&initial.table, "pending-update")
        .add_data_files(vec![data("replacement")])
        .add_delete_files(vec![delete("tombstone", &original)])
        .validate_data_files_exist([original.file_path().to_owned()]);
    let compacted = RewriteFilesAction::new(&initial.table, "external-compaction")
        .remove_data_files([original.file_path().to_owned()])
        .add_data_files(vec![data("compacted")])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    assert!(
        pending
            .commit(&catalog, &initial.table)
            .await
            .unwrap_err()
            .message()
            .contains("no longer exists")
    );
    assert_eq!(
        catalog
            .load_table(empty.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        Some(compacted.snapshot_id)
    );
}

#[tokio::test]
async fn expired_prepared_base_requires_recovery_instead_of_blind_replay() {
    let (catalog, empty) = setup().await;
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![data("initial")])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let pending =
        RowDeltaAction::new(&initial.table, "pending").add_data_files(vec![data("pending")]);
    let later = RowDeltaAction::new(&initial.table, "later")
        .add_data_files(vec![data("later")])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    let tx = Transaction::new(&later.table);
    let expired = tx
        .expire_snapshots()
        .expire_snapshot_ids([initial.snapshot_id])
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert!(
        expired
            .metadata()
            .snapshot_by_id(initial.snapshot_id)
            .is_none()
    );
    assert!(
        pending
            .commit(&catalog, &initial.table)
            .await
            .unwrap_err()
            .message()
            .contains("base snapshot expired")
    );
}

#[tokio::test]
async fn schema_changes_invalidate_prepared_files_even_without_a_new_snapshot() {
    let (catalog, empty) = setup().await;
    let pending = RowDeltaAction::new(&empty, "pending").add_data_files(vec![data("pending")]);
    let tx = Transaction::new(&empty);
    let changed = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "note",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert_eq!(changed.metadata().current_snapshot_id(), None);
    assert!(
        pending
            .commit(&catalog, &empty)
            .await
            .unwrap_err()
            .message()
            .contains("schema or partition spec changed")
    );
}

#[tokio::test]
async fn publication_preserves_main_branch_retention() {
    use iceberg::spec::{SnapshotReference, SnapshotRetention};
    use iceberg::{TableCommit, TableUpdate};
    let (catalog, empty) = setup().await;
    let initial = RowDeltaAction::new(&empty, "initial")
        .add_data_files(vec![data("initial")])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let retention = SnapshotRetention::branch(Some(50), Some(86_400_000), None);
    let configured = catalog
        .update_table(
            TableCommit::builder()
                .ident(empty.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_owned(),
                    reference: SnapshotReference::new(initial.snapshot_id, retention.clone()),
                }])
                .build(),
        )
        .await
        .unwrap();
    let published = RowDeltaAction::new(&configured, "next")
        .add_data_files(vec![data("next")])
        .commit(&catalog, &configured)
        .await
        .unwrap();
    assert_eq!(
        published
            .table
            .metadata()
            .snapshot_reference("main")
            .unwrap()
            .retention,
        retention
    );
}

#[tokio::test]
async fn protected_bases_and_reader_windows_override_expiration() {
    let (catalog, empty) = setup().await;
    let first = RowDeltaAction::new(&empty, "first")
        .add_data_files(vec![data("first")])
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let second = RowDeltaAction::new(&first.table, "second")
        .add_data_files(vec![data("second")])
        .commit(&catalog, &first.table)
        .await
        .unwrap();
    let third = RowDeltaAction::new(&second.table, "third")
        .add_data_files(vec![data("third")])
        .commit(&catalog, &second.table)
        .await
        .unwrap();
    let transaction = Transaction::new(&third.table);
    let expired = transaction
        .expire_snapshots()
        .expire_snapshot_ids([first.snapshot_id, second.snapshot_id])
        .protect_snapshots([first.snapshot_id])
        .apply(transaction)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert!(
        expired
            .metadata()
            .snapshot_by_id(first.snapshot_id)
            .is_some()
    );
    assert!(
        expired
            .metadata()
            .snapshot_by_id(second.snapshot_id)
            .is_none()
    );
    let timestamp = expired
        .metadata()
        .snapshot_by_id(first.snapshot_id)
        .unwrap()
        .timestamp_ms();
    let transaction = Transaction::new(&expired);
    let retained = transaction
        .expire_snapshots()
        .expire_snapshot_ids([first.snapshot_id])
        .protect_newer_than_ms(timestamp)
        .apply(transaction)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert!(
        retained
            .metadata()
            .snapshot_by_id(first.snapshot_id)
            .is_some()
    );
    assert_eq!(
        SnapshotView::current(&retained)
            .await
            .unwrap()
            .live_files
            .len(),
        3
    );
}

#[tokio::test]
async fn discarded_data_singletons_leave_budget_for_delete_manifest_merge() {
    use flow_iceberg_ext::{ManifestCache, ManifestRewritePolicy, RewriteManifestsAction};
    let (catalog, mut table) = setup().await;
    for index in 0..2 {
        let mut state = index + 123u64;
        let metadata = (0..8192)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("memory://warehouse/budget-{index}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .file_size_in_bytes(128)
            .record_count(10)
            .key_metadata(Some(metadata))
            .build()
            .unwrap();
        table = RowDeltaAction::new(&table, format!("budget-{index}"))
            .add_data_files(vec![file.clone()])
            .commit(&catalog, &table)
            .await
            .unwrap()
            .table;
        table = RowDeltaAction::new(&table, format!("budget-delete-{index}"))
            .add_delete_files(vec![delete(&format!("budget-delete-{index}"), &file)])
            .validate_data_files_exist([file.file_path().to_owned()])
            .commit(&catalog, &table)
            .await
            .unwrap()
            .table;
    }
    let list = table
        .manifest_list_reader(table.metadata().current_snapshot().unwrap())
        .load()
        .await
        .unwrap();
    let data_sizes = list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Data)
        .map(|m| m.manifest_length as u64)
        .collect::<Vec<_>>();
    let delete_bytes: u64 = list
        .entries()
        .iter()
        .filter(|m| m.content == ManifestContentType::Deletes)
        .map(|m| m.manifest_length as u64)
        .sum();
    let target = data_sizes.iter().max().unwrap() + 1;
    assert!(data_sizes.iter().sum::<u64>() > target && delete_bytes < target);
    let before = SnapshotView::current(&table).await.unwrap();
    let policy = ManifestRewritePolicy {
        min_manifest_count: 2,
        max_input_manifests: 2,
        target_bytes: target,
        ..Default::default()
    };
    let rewrite = RewriteManifestsAction::plan(&table, "refund-singletons", &policy)
        .await
        .unwrap()
        .expect("eligible delete pair must retain its budget");
    rewrite
        .write_artifacts(&table, &ManifestCache::default())
        .await
        .unwrap();
    let result = rewrite.commit(&catalog, &table).await.unwrap();
    let after = SnapshotView::current(&result.table).await.unwrap();
    assert_eq!(after.manifest_count(), 3);
    assert_eq!(after.live_files.len(), before.live_files.len());
    for (path, original) in &before.live_files {
        let current = &after.live_files[path];
        assert_eq!(current.data_file, original.data_file);
        assert_eq!(current.snapshot_id, original.snapshot_id);
        assert_eq!(current.sequence_number, original.sequence_number);
        assert_eq!(current.file_sequence_number, original.file_sequence_number);
    }
}
