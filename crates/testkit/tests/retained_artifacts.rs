use flow_iceberg_ext::{
    ManifestCache, RetainedIndex, RewriteFilesAction, RowDeltaAction, retained_artifacts,
};
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

fn data_file(path: &str) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .file_size_in_bytes(10)
        .record_count(1)
        .build()
        .unwrap()
}

fn gated(head: &Table, gate: &Arc<Gate>) -> Table {
    let io = FileIOBuilder::new(Arc::new(GatedStorage {
        gate: gate.clone(),
        ..Default::default()
    }))
    .build();
    Table::builder()
        .identifier(head.identifier().clone())
        .metadata(head.metadata_ref())
        .file_io(io)
        .runtime(Runtime::current())
        .build()
        .unwrap()
}

#[tokio::test]
async fn retained_index_matches_the_walk_and_reads_each_list_once() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = flow_testkit::catalog(&temp.path().join("warehouse")).await;
    let mut head = flow_testkit::table(&catalog, &flow_testkit::schema(1)).await;
    let mut data_paths = Vec::new();
    for i in 0..3 {
        let path = format!("{}/external-layout/{i}.parquet", head.metadata().location());
        head = RowDeltaAction::new(&head, format!("append-{i}"))
            .add_data_files(vec![data_file(&path)])
            .commit(&catalog, &head)
            .await
            .unwrap()
            .table;
        data_paths.push(path);
    }
    // The removed file stays live in older retained snapshots.
    head = RewriteFilesAction::new(&head, "remove-oldest")
        .remove_data_files([data_paths[0].clone()])
        .commit(&catalog, &head)
        .await
        .unwrap()
        .table;
    let mut candidates: BTreeSet<_> = data_paths.iter().cloned().collect();
    for snapshot in head.metadata().snapshots() {
        candidates.insert(snapshot.manifest_list().to_owned());
        for manifest in head
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .entries()
        {
            candidates.insert(manifest.manifest_path.clone());
        }
    }
    let unreferenced = [
        format!("{}/data/unreferenced.parquet", head.metadata().location()),
        format!(
            "{}/metadata/unreferenced-m0.avro",
            head.metadata().location()
        ),
    ];
    candidates.extend(unreferenced.iter().cloned());
    let expected = retained_artifacts(&head, &candidates, &ManifestCache::new(1))
        .await
        .unwrap();

    let gate = Arc::new(Gate::default());
    let observed = gated(&head, &gate);
    let cache = ManifestCache::new(1);
    let deadline = || std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut index = RetainedIndex::default();
    let progress = index
        .sync(&observed, &cache, deadline(), usize::MAX)
        .await
        .unwrap();
    assert!(progress.complete && index.is_complete());
    assert_eq!(progress.manifest_lists_read, 4);
    for path in &candidates {
        assert_eq!(
            index.contains(path),
            expected.contains(path),
            "index and walk disagree on {path}"
        );
    }
    assert!(unreferenced.iter().all(|path| !index.contains(path)));
    assert!(index.contains(&data_paths[0]));
    {
        let reads = gate.reads.lock().unwrap();
        assert!(reads.values().all(|count| *count == 1), "{reads:?}");
    }
    let again = index
        .sync(&observed, &cache, deadline(), usize::MAX)
        .await
        .unwrap();
    assert!(again.complete);
    assert_eq!((again.manifest_lists_read, again.manifests_read), (0, 0));

    let path = format!("{}/external-layout/3.parquet", head.metadata().location());
    head = RowDeltaAction::new(&head, "append-3")
        .add_data_files(vec![data_file(&path)])
        .commit(&catalog, &head)
        .await
        .unwrap()
        .table;
    let next = index
        .sync(&gated(&head, &gate), &cache, deadline(), usize::MAX)
        .await
        .unwrap();
    assert!(next.complete);
    assert_eq!(next.manifest_lists_read, 1);
    assert!(index.contains(&path));

    // A budget below the history clears the index instead of growing it.
    let mut small = RetainedIndex::default();
    let oversized = small.sync(&observed, &cache, deadline(), 1).await.unwrap();
    assert!(oversized.oversized && !oversized.complete);
    assert!(!small.is_complete() && small.estimated_bytes() == 0);
}
