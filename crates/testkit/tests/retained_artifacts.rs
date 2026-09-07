use flow_iceberg_ext::{ManifestCache, RewriteFilesAction, RowDeltaAction, retained_artifacts};
use iceberg::{
    Runtime,
    io::FileIOBuilder,
    spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct},
    table::Table,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

#[path = "common/gated_storage.rs"]
mod gated_storage;
use gated_storage::{Gate, GatedStorage};

#[tokio::test]
async fn retained_protection_reads_each_shared_manifest_once_even_without_cache_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = flow_testkit::catalog(&temp.path().join("warehouse")).await;
    let mut head = flow_testkit::table(&catalog, &flow_testkit::schema(1)).await;
    let mut data_paths = BTreeSet::new();
    for i in 0..3 {
        let path = format!("{}/external-layout/{i}.parquet", head.metadata().location());
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .file_size_in_bytes(10)
            .record_count(1)
            .build()
            .unwrap();
        data_paths.insert(path);
        head = RowDeltaAction::new(&head, format!("append-{i}"))
            .add_data_files(vec![file])
            .commit(&catalog, &head)
            .await
            .unwrap()
            .table;
    }
    head = RewriteFilesAction::new(&head, "remove-oldest")
        .remove_data_files([data_paths.first().unwrap().clone()])
        .commit(&catalog, &head)
        .await
        .unwrap()
        .table;
    let mut manifest_references = HashMap::<String, usize>::new();
    let mut expected = data_paths;
    for snapshot in head.metadata().snapshots() {
        expected.insert(snapshot.manifest_list().to_owned());
        for manifest in head
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .entries()
        {
            *manifest_references
                .entry(manifest.manifest_path.clone())
                .or_default() += 1;
            expected.insert(manifest.manifest_path.clone());
        }
    }
    assert!(manifest_references.values().any(|count| *count > 1));
    let mut candidates = expected.clone();
    candidates.insert(format!(
        "{}/data/unreferenced-reservation.parquet",
        head.metadata().location()
    ));
    let gate = Arc::new(Gate::default());
    let io = FileIOBuilder::new(Arc::new(GatedStorage {
        gate: gate.clone(),
        ..Default::default()
    }))
    .build();
    let observed = Table::builder()
        .identifier(head.identifier().clone())
        .metadata(head.metadata_ref())
        .file_io(io)
        .runtime(Runtime::current())
        .build()
        .unwrap();
    let protected = retained_artifacts(&observed, &candidates, &ManifestCache::new(1))
        .await
        .unwrap();
    assert_eq!(protected, expected);
    let reads = gate.reads.lock().unwrap();
    for path in manifest_references.keys() {
        assert_eq!(
            reads.get(path),
            Some(&1),
            "immutable manifest {path} must be read once"
        );
    }
}
