use flow_iceberg_ext::{
    ManifestCache, ManifestRewritePolicy, RewriteFilesAction, RewriteManifestsAction,
    RowDeltaAction, SnapshotView, content_file_id, read_artifact_plan, write_artifact_plan,
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
    PrimitiveType, Schema, Struct, Type,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_version(version: FormatVersion) -> (iceberg::MemoryCatalog, Table) {
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
                .format_version(version)
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

fn vector(target: &DataFile, offset: i64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("memory://warehouse/shared.puffin".into())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .file_size_in_bytes(1024)
        .record_count(1)
        .referenced_data_file(Some(target.file_path().to_owned()))
        .content_offset(Some(offset))
        .content_size_in_bytes(Some(64))
        .build()
        .unwrap()
}

#[tokio::test]
async fn v3_vectors_share_objects_and_replace_by_blob_identity() {
    let (catalog, table) = setup_version(FormatVersion::V3).await;
    let a = data("a");
    let b = data("b");
    let table = RowDeltaAction::new(&table, "seed")
        .add_data_files(vec![a.clone(), b.clone()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    assert!(
        RowDeltaAction::new(&table, "invalid-range")
            .add_delete_files(vec![vector(&a, 1000)])
            .validate_data_files_exist([a.file_path().to_owned()])
            .commit(&catalog, &table)
            .await
            .unwrap_err()
            .message()
            .contains("blob range")
    );
    let va = vector(&a, 4);
    let vb = vector(&b, 68);
    write_artifact_plan(
        &table,
        "memory://warehouse/plan",
        &[],
        &[va.clone(), vb.clone()],
    )
    .await
    .unwrap();
    let plan = read_artifact_plan(table.file_io(), "memory://warehouse/plan")
        .await
        .unwrap();
    assert_eq!(plan.added_deletes, vec![va.clone(), vb.clone()]);
    let table = RowDeltaAction::new(&table, "vectors")
        .add_delete_files(plan.added_deletes)
        .validate_data_files_exist([a.file_path().to_owned(), b.file_path().to_owned()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let next = vector(&a, 132);
    assert!(
        RowDeltaAction::new(&table, "duplicate")
            .add_delete_files(vec![next.clone()])
            .validate_data_files_exist([a.file_path().to_owned()])
            .commit(&catalog, &table)
            .await
            .is_err()
    );
    let replacement = RowDeltaAction::new(&table, "replace-vector")
        .remove_delete_files([content_file_id(&va)])
        .add_delete_files(vec![next.clone()])
        .validate_data_files_exist([a.file_path().to_owned()]);
    let table = replacement.commit(&catalog, &table).await.unwrap().table;
    let view = SnapshotView::current(&table).await.unwrap();
    assert!(!view.live_files.contains_key(&content_file_id(&va)));
    assert!(view.live_files.contains_key(&content_file_id(&vb)));
    assert!(view.live_files.contains_key(&content_file_id(&next)));
    let stale = RowDeltaAction::new(&table, "stale-vector")
        .remove_delete_files([content_file_id(&next)])
        .add_delete_files(vec![vector(&a, 196)])
        .validate_data_files_exist([a.file_path().to_owned()]);
    let table = RowDeltaAction::new(&table, "advance")
        .add_data_files(vec![data("c")])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    assert!(
        stale
            .commit(&catalog, &table)
            .await
            .unwrap_err()
            .message()
            .contains("prepared snapshot changed")
    );
    let table = RewriteFilesAction::new(&table, "rewrite-a")
        .remove_data_files([a.file_path().to_owned()])
        .add_data_files(vec![data("rewritten-a")])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let view = SnapshotView::current(&table).await.unwrap();
    assert!(!view.live_files.contains_key(&content_file_id(&next)));
    assert!(view.live_files.contains_key(&content_file_id(&vb)));
}

#[tokio::test]
async fn v3_manifest_maintenance_preserves_inherited_row_ids_and_plan_leaves_them_unassigned() {
    let (catalog, table) = setup_version(FormatVersion::V3).await;
    let cache = ManifestCache::default();
    let a = data("a");
    write_artifact_plan(
        &table,
        "memory://warehouse/data-plan",
        std::slice::from_ref(&a),
        &[],
    )
    .await
    .unwrap();
    let plan = read_artifact_plan(table.file_io(), "memory://warehouse/data-plan")
        .await
        .unwrap();
    assert_eq!(plan.added_data[0].first_row_id(), None);
    let table = RowDeltaAction::new(&table, "a")
        .add_data_files(plan.added_data)
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    assert_eq!(table.metadata().next_row_id(), 10);
    let first = SnapshotView::current_with_cache(&table, &cache)
        .await
        .unwrap();
    assert_eq!(
        first.live_files[a.file_path()].data_file().first_row_id(),
        Some(0)
    );
    let b = data("b");
    let table = RowDeltaAction::new(&table, "b")
        .add_data_files(vec![b.clone()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    assert_eq!(
        table.metadata().current_snapshot().unwrap().row_range(),
        Some((10, 10))
    );
    let action = RewriteManifestsAction::plan(
        &table,
        "maintenance",
        &ManifestRewritePolicy {
            min_manifest_count: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .unwrap();
    action.write_artifacts(&table, &cache).await.unwrap();
    let table = action.commit(&catalog, &table).await.unwrap().table;
    let after = SnapshotView::current_with_cache(&table, &cache)
        .await
        .unwrap();
    assert_eq!(after.manifest_count(), 1);
    assert_eq!(
        after.live_files[a.file_path()].data_file().first_row_id(),
        Some(0)
    );
    assert_eq!(
        after.live_files[b.file_path()].data_file().first_row_id(),
        Some(10)
    );
    assert!(table.metadata().next_row_id() >= 20);
    assert_eq!(
        table.metadata().current_snapshot().unwrap().first_row_id(),
        Some(20)
    );
}

#[tokio::test]
async fn upgrade_assigns_lineage_to_cached_manifests_and_keeps_legacy_deletes_readable() {
    let (catalog, table) = setup_version(FormatVersion::V2).await;
    let a = data("old-a");
    let legacy = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path("memory://warehouse/legacy.parquet".into())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .file_size_in_bytes(128)
        .record_count(1)
        .referenced_data_file(Some(a.file_path().to_owned()))
        .build()
        .unwrap();
    let table = RowDeltaAction::new(&table, "old-a")
        .add_data_files(vec![a.clone()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let table = RowDeltaAction::new(&table, "legacy")
        .add_delete_files(vec![legacy.clone()])
        .validate_data_files_exist([a.file_path().to_owned()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let cache = ManifestCache::default();
    let before = SnapshotView::current_with_cache(&table, &cache)
        .await
        .unwrap();
    assert_eq!(
        before.live_files[a.file_path()].data_file().first_row_id(),
        None
    );
    let prepared =
        RowDeltaAction::new(&table, "prepared-before-upgrade").add_data_files(vec![data("stale")]);
    let mut legacy_base = serde_json::to_value(prepared.base()).unwrap();
    legacy_base
        .as_object_mut()
        .unwrap()
        .remove("format_version");
    let legacy_base: flow_iceberg_ext::CommitBase = serde_json::from_value(legacy_base).unwrap();
    assert_eq!(legacy_base.format_version, FormatVersion::V2);
    let table = catalog
        .update_table(
            iceberg::TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![iceberg::TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3,
                }])
                .build(),
        )
        .await
        .unwrap();
    assert!(
        prepared
            .commit(&catalog, &table)
            .await
            .unwrap_err()
            .to_string()
            .contains("format changed")
    );
    let table = RowDeltaAction::new(&table, "upgraded-append")
        .with_manifest_cache(cache.clone())
        .add_data_files(vec![data("new-b")])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let after = SnapshotView::current_with_cache(&table, &cache)
        .await
        .unwrap();
    assert_eq!(
        after.live_files[a.file_path()].data_file().first_row_id(),
        Some(0)
    );
    assert_eq!(after.applicable_deletes(a.file_path()).unwrap().len(), 1);
    assert_eq!(table.metadata().next_row_id(), 20);
    let dv = vector(&a, 4);
    let table = RowDeltaAction::new(&table, "upgrade-vector")
        .add_delete_files(vec![dv.clone()])
        .validate_data_files_exist([a.file_path().to_owned()])
        .commit(&catalog, &table)
        .await
        .unwrap()
        .table;
    let with_vector = SnapshotView::current(&table).await.unwrap();
    assert!(with_vector.live_files.contains_key(legacy.file_path()));
    let applicable = with_vector.applicable_deletes(a.file_path()).unwrap();
    assert_eq!(applicable.len(), 1);
    assert_eq!(applicable[0].file_format(), DataFileFormat::Puffin);
    let old_snapshot = SnapshotView::load_with_cache(&table, before.snapshot_id.unwrap(), &cache)
        .await
        .unwrap();
    assert_eq!(
        old_snapshot.live_files[a.file_path()]
            .data_file()
            .first_row_id(),
        None
    );
    assert!(
        RowDeltaAction::new(&table, "reject-legacy")
            .add_delete_files(vec![legacy])
            .validate_data_files_exist([a.file_path().to_owned()])
            .commit(&catalog, &table)
            .await
            .is_err()
    );
}
