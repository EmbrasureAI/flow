//! Count HTTP requests beneath FileIO/OpenDAL, not just calls to metadata().
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use flow_compactor::{DeleteReadLimits, ReadLimits, stage_delete_files};
use flow_iceberg_ext::{SnapshotView, content_file_id};
use flow_materializer::{DeleteWriter, WriterConfig, iceberg_schema};
use flow_model::{
    Column, ColumnType, FileId, OperationId, PgLsn, RowLocation, TableId, TableSchema,
};
use flow_state_store::{StateStore, StateStoreOptions};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    io::{
        FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    },
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestEntry,
        ManifestStatus,
    },
    table::Table,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
struct Counts {
    fail_head: AtomicBool,
    heads: AtomicUsize,
    gets: AtomicUsize,
    bytes: AtomicUsize,
}
impl Counts {
    fn reset(&self) {
        self.heads.store(0, Ordering::SeqCst);
        self.gets.store(0, Ordering::SeqCst);
        self.bytes.store(0, Ordering::SeqCst);
    }
    fn requests(&self) -> (usize, usize) {
        (
            self.heads.load(Ordering::SeqCst),
            self.gets.load(Ordering::SeqCst),
        )
    }
}

struct Endpoint {
    url: String,
    counts: Arc<Counts>,
    task: tokio::task::JoinHandle<()>,
    objects: Arc<RwLock<HashMap<String, bytes::Bytes>>>,
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Endpoint {
    async fn start(objects: HashMap<String, bytes::Bytes>, delay: Duration) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let counts = Arc::new(Counts::default());
        let state = counts.clone();
        let objects = Arc::new(RwLock::new(objects));
        let served_objects = objects.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let counts = state.clone();
                        let objects = served_objects.clone();
                        connections.spawn(async move {
                            // Keep connections alive so OpenDAL can reuse its HTTP pool.
                            loop {
                                let mut request = Vec::new();
                                while !request.ends_with(b"\r\n\r\n") {
                                    match socket.read_u8().await {
                                        Ok(byte) => request.push(byte),
                                        Err(_) => return,
                                    }
                                    assert!(request.len() < 16_384);
                                }
                                let request = String::from_utf8(request).unwrap();
                                let mut first = request.lines().next().unwrap().split_whitespace();
                                let method = first.next().unwrap();
                                let path = first.next().unwrap();
                                let object = objects.read().unwrap()[path].clone();
                                if !delay.is_zero() { tokio::time::sleep(delay).await; }
                                let response = if method == "HEAD" {
                                    counts.heads.fetch_add(1, Ordering::SeqCst);
                                    if counts.fail_head.load(Ordering::SeqCst) {
                                        socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n").await.unwrap();
                                        continue;
                                    }
                                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", object.len()).into_bytes()
                                } else {
                                    assert_eq!(method, "GET");
                                    counts.gets.fetch_add(1, Ordering::SeqCst);
                                    let lower = request.to_ascii_lowercase();
                                    let range = lower.lines().find_map(|line| line.strip_prefix("range: bytes=")).unwrap();
                                    let (start, end) = range.split_once('-').unwrap();
                                    let start: usize = start.parse().unwrap();
                                    let end: usize = end.parse().unwrap();
                                    let body = &object[start..=end];
                                    counts.bytes.fetch_add(body.len(), Ordering::SeqCst);
                                    let mut response = format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\n\r\n", body.len(), object.len()).into_bytes();
                                    response.extend_from_slice(body);
                                    response
                                };
                                socket.write_all(&response).await.unwrap();
                            }
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            url,
            counts,
            task,
            objects,
        }
    }
    fn replace(&self, path: &str, bytes: bytes::Bytes) {
        self.objects
            .write()
            .unwrap()
            .insert(path.strip_prefix("s3:/").unwrap().to_owned(), bytes);
    }
    fn io(&self) -> FileIO {
        FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
            .with_prop(S3_ENDPOINT, &self.url)
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(S3_PATH_STYLE_ACCESS, "true")
            .with_prop(S3_ACCESS_KEY_ID, "test-key")
            .with_prop(S3_SECRET_ACCESS_KEY, "test-secret")
            .build()
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    endpoint: Endpoint,
    table: Table,
    source_io: FileIO,
    view: SnapshotView,
    schema: TableSchema,
    store: StateStore,
    inputs: BTreeSet<FileId>,
    deletes: BTreeSet<FileId>,
    expected: Vec<RowLocation>,
}
impl Fixture {
    async fn new(groups: &[usize], cardinality: usize, delay: Duration, corrupt: bool) -> Self {
        let io = FileIO::new_with_memory();
        let schema = TableSchema {
            table_id: TableId(1),
            version: 0,
            columns: vec![Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            }],
            primary_key: vec![0],
            append_only: false,
        };
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "dv-test",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://warehouse".into())]),
            )
            .await
            .unwrap();
        let ns = NamespaceIdent::new("test".into());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let table = catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("dv".into())
                    .schema(iceberg_schema(&schema).unwrap())
                    .format_version(FormatVersion::V3)
                    .build(),
            )
            .await
            .unwrap();
        let mut view = SnapshotView::default();
        let mut expected = Vec::new();
        let mut inputs = BTreeSet::new();
        let mut deletes = BTreeSet::new();
        let mut objects = HashMap::new();
        let mut ordinal = 0;
        for (group, count) in groups.iter().enumerate() {
            let mut writer = DeleteWriter::new_for_version(
                io.clone(),
                "s3://warehouse/table",
                &OperationId(format!("group-{group}")),
                0,
                WriterConfig::default(),
                FormatVersion::V3,
            )
            .unwrap();
            for _ in 0..*count {
                let target = format!("s3://warehouse/data-{ordinal:06}.parquet");
                // Distinct cardinalities give distinct blob lengths as well as offsets.
                let positions = (0..cardinality + ordinal)
                    .map(|p| RowLocation {
                        data_file_id: FileId(target.clone()),
                        row_position: (p * 2) as u64,
                        data_sequence_number: 1,
                        spec_id: 0,
                        partition: vec![],
                        source_commit_lsn: PgLsn(0),
                        row_version: 0,
                        row_fingerprint: [0; 16],
                    })
                    .collect::<Vec<_>>();
                writer.write(&positions).await.unwrap();
                expected.extend(positions);
                inputs.insert(FileId(target.clone()));
                let data = DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(target.clone())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(((cardinality + ordinal) * 2) as u64)
                    .file_size_in_bytes(1)
                    .partition_spec_id(0)
                    .build()
                    .unwrap();
                view.live_files.insert(target, entry(data, 1));
                ordinal += 1;
            }
            let descriptors = writer.close().await.unwrap();
            assert_eq!(descriptors.len(), *count);
            assert!(
                descriptors
                    .iter()
                    .all(|d| d.file_path() == descriptors[0].file_path())
            );
            let path = descriptors[0].file_path();
            let mut object = io.new_input(path).unwrap().read().await.unwrap().to_vec();
            if corrupt {
                object[descriptors[1].content_offset().unwrap() as usize + 8] ^= 1;
            }
            objects.insert(path.strip_prefix("s3:/").unwrap().to_owned(), object.into());
            for descriptor in descriptors {
                let id = content_file_id(&descriptor);
                deletes.insert(FileId(id.clone()));
                view.live_files.insert(id, entry(descriptor, 2));
            }
        }
        let endpoint = Endpoint::start(objects, delay).await;
        let table = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(table.metadata().clone())
            .file_io(endpoint.io())
            .runtime(iceberg::Runtime::current())
            .build()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let store =
            StateStore::open(temp.path().join("state"), StateStoreOptions::default()).unwrap();
        Self {
            _temp: temp,
            endpoint,
            table,
            source_io: io,
            view,
            schema,
            store,
            inputs,
            deletes,
            expected,
        }
    }
    async fn stage(&self) -> anyhow::Result<u64> {
        tokio::time::timeout(
            Duration::from_secs(60),
            stage_delete_files(
                &self.table,
                &self.schema,
                &self.view,
                &self.inputs,
                &self.deletes,
                &self.store,
                "scan",
                1024,
                DeleteReadLimits {
                    max_bytes: u64::MAX,
                    max_rows: u64::MAX,
                },
                &ReadLimits::default(),
            ),
        )
        .await
        .unwrap()
    }
    fn assert_positions(&self) {
        let actual = self
            .store
            .position_deletes("scan", &self.schema.table_id)
            .collect::<flow_state_store::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(actual, self.expected);
    }
}
fn entry(data_file: DataFile, sequence_number: i64) -> Arc<ManifestEntry> {
    Arc::new(
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(sequence_number)
            .data_file(data_file)
            .build(),
    )
}

#[tokio::test]
async fn shared_puffin_counts_backing_requests_and_decodes_every_blob() {
    let f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
    let descriptors = f
        .deletes
        .iter()
        .map(|id| &f.view.live_files[&id.0].data_file)
        .collect::<Vec<_>>();
    assert_eq!(
        descriptors
            .iter()
            .map(|d| d.content_offset())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        descriptors
            .iter()
            .map(|d| d.content_size_in_bytes())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    assert_eq!(f.stage().await.unwrap(), 6);
    f.assert_positions();
    assert_eq!(f.endpoint.counts.requests(), (1, 12));
}

#[tokio::test]
async fn multiple_puffin_files_count_backing_requests() {
    let f = Fixture::new(&[2, 2, 1], 1, Duration::ZERO, false).await;
    f.stage().await.unwrap();
    f.assert_positions();
    assert_eq!(f.endpoint.counts.requests(), (3, 20));
}

#[tokio::test]
async fn every_descriptor_must_match_the_observed_object_size() {
    // Exercise both the first observation and a later descriptor sharing it.
    for index in [0, 1] {
        let mut f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
        let id = f.deletes.iter().nth(index).unwrap().0.clone();
        let old = &f.view.live_files[&id].data_file;
        let bad = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(old.file_path().to_owned())
            .file_format(DataFileFormat::Puffin)
            .record_count(old.record_count())
            .file_size_in_bytes(old.file_size_in_bytes() + 1)
            .partition_spec_id(0)
            .referenced_data_file(old.referenced_data_file())
            .content_offset(old.content_offset())
            .content_size_in_bytes(old.content_size_in_bytes())
            .build()
            .unwrap();
        f.view.live_files.insert(id, entry(bad, 2));
        assert_eq!(
            f.stage().await.unwrap_err().to_string(),
            "delete file size differs from manifest"
        );
    }
}

#[tokio::test]
async fn a_new_scan_rechecks_metadata_and_replaces_scratch_positions() {
    let f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
    for _ in 0..2 {
        f.endpoint.counts.reset();
        f.stage().await.unwrap();
        f.assert_positions();
        assert_eq!(f.endpoint.counts.requests(), (1, 12));
    }
}

#[tokio::test]
async fn metadata_failure_can_be_retried_by_a_new_scan() {
    let f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
    f.endpoint.counts.fail_head.store(true, Ordering::SeqCst);
    assert!(f.stage().await.is_err());
    assert_eq!(f.endpoint.counts.requests(), (1, 0));
    f.endpoint.counts.fail_head.store(false, Ordering::SeqCst);
    f.endpoint.counts.reset();
    f.stage().await.unwrap();
    f.assert_positions();
    assert_eq!(f.endpoint.counts.requests(), (1, 12));
}

#[tokio::test]
async fn corrupt_blob_is_still_rejected() {
    let f = Fixture::new(&[3], 1, Duration::ZERO, true).await;
    assert!(
        f.stage()
            .await
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch")
    );
}

#[tokio::test]
async fn a_new_scan_rejects_a_changed_physical_size() {
    let f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
    f.stage().await.unwrap();
    let path = f.view.live_files[&f.deletes.first().unwrap().0].file_path();
    let original = f.source_io.new_input(path).unwrap().read().await.unwrap();
    let mut changed = original.to_vec();
    changed.push(0);
    f.endpoint.replace(path, changed.into());
    f.endpoint.counts.reset();
    assert_eq!(
        f.stage().await.unwrap_err().to_string(),
        "delete file size differs from manifest"
    );
    assert_eq!(f.endpoint.counts.requests(), (1, 0));
    f.endpoint.replace(path, original);
    f.endpoint.counts.reset();
    f.stage().await.unwrap();
    f.assert_positions();
    assert_eq!(f.endpoint.counts.requests(), (1, 12));
}

#[tokio::test]
async fn retry_after_a_later_blob_failure_discards_partial_scratch_state() {
    let mut f = Fixture::new(&[3], 1, Duration::ZERO, true).await;
    assert!(
        f.stage()
            .await
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch")
    );
    assert_eq!(f.endpoint.counts.requests(), (1, 8));
    assert_eq!(
        f.store.position_deletes("scan", &f.schema.table_id).count(),
        1
    );

    let first = f.deletes.pop_first().unwrap();
    let descriptor = &f.view.live_files[&first.0].data_file;
    let path = descriptor.file_path();
    let original = f.source_io.new_input(path).unwrap().read().await.unwrap();
    f.endpoint.replace(path, original);
    // Retry a smaller selection to prove the previously staged row is removed.
    let target = descriptor.referenced_data_file().unwrap();
    f.expected.retain(|row| row.data_file_id.0 != target);
    f.endpoint.counts.reset();
    f.stage().await.unwrap();
    f.assert_positions();
    assert_eq!(f.endpoint.counts.requests(), (1, 8));
}

#[tokio::test]
async fn observed_size_reader_preserves_validation_and_original_api_metadata() {
    use iceberg::puffin::{
        DeletionVectorLimits, read_deletion_vector, read_deletion_vector_with_size,
    };
    let f = Fixture::new(&[3], 1, Duration::ZERO, false).await;
    let d = &f.view.live_files[&f.deletes.iter().nth(1).unwrap().0].data_file;
    let io = f.table.file_io();
    let input = io.new_input(d.file_path()).unwrap();
    let size = input.metadata().await.unwrap().size;
    let offset = d.content_offset().unwrap() as u64;
    let length = d.content_size_in_bytes().unwrap() as u64;
    let target = d.referenced_data_file().unwrap();
    let cardinality = d.record_count();
    let limits = DeletionVectorLimits::default();
    f.endpoint.counts.reset();
    for _ in 0..2 {
        let decoded = read_deletion_vector(
            io,
            d.file_path(),
            offset,
            length,
            &target,
            cardinality,
            limits,
        )
        .await
        .unwrap();
        assert_eq!(decoded.iter().collect::<Vec<_>>(), [0, 2]);
    }
    assert_eq!(f.endpoint.counts.requests(), (2, 8));
    f.endpoint.counts.reset();
    let decoded =
        read_deletion_vector_with_size(&input, size, offset, length, &target, cardinality, limits)
            .await
            .unwrap();
    assert_eq!(decoded.iter().collect::<Vec<_>>(), [0, 2]);
    assert_eq!(f.endpoint.counts.requests(), (0, 4));

    for (offset, length, target, cardinality, limits, expected) in [
        (
            0,
            length,
            target.as_str(),
            cardinality,
            limits,
            "overlaps Puffin header",
        ),
        (
            u64::MAX,
            length,
            target.as_str(),
            cardinality,
            limits,
            "byte range overflow",
        ),
        (
            size,
            length,
            target.as_str(),
            cardinality,
            limits,
            "exceeds Puffin file",
        ),
        (
            offset,
            length,
            "wrong-target",
            cardinality,
            limits,
            "metadata does not match",
        ),
        (
            offset,
            length,
            target.as_str(),
            cardinality + 1,
            limits,
            "metadata does not match",
        ),
        (
            offset,
            length,
            target.as_str(),
            cardinality,
            DeletionVectorLimits {
                max_blob_bytes: length - 1,
                ..limits
            },
            "blob length exceeds limits",
        ),
        (
            offset,
            length,
            target.as_str(),
            cardinality,
            DeletionVectorLimits {
                max_cardinality: cardinality - 1,
                ..limits
            },
            "cardinality exceeds limit",
        ),
        (
            offset,
            length,
            target.as_str(),
            cardinality,
            DeletionVectorLimits {
                max_footer_bytes: 1,
                ..limits
            },
            "footer length exceeds limit",
        ),
    ] {
        let (heads, gets) = match expected {
            "metadata does not match" => (1, 3),
            "footer length exceeds limit" => (1, 2),
            "exceeds Puffin file" => (1, 0),
            _ => (0, 0), // Argument validation must precede all I/O.
        };
        f.endpoint.counts.reset();
        let original = read_deletion_vector(
            io,
            d.file_path(),
            offset,
            length,
            target,
            cardinality,
            limits,
        )
        .await
        .unwrap_err();
        assert_eq!(f.endpoint.counts.requests(), (heads, gets));
        f.endpoint.counts.reset();
        let observed = read_deletion_vector_with_size(
            &input,
            size,
            offset,
            length,
            target,
            cardinality,
            limits,
        )
        .await
        .unwrap_err();
        assert_eq!(original.to_string(), observed.to_string());
        assert!(observed.to_string().contains(expected), "{observed}");
        assert_eq!(f.endpoint.counts.requests(), (0, gets));
    }
    for size in [0, 19] {
        f.endpoint.counts.reset();
        assert!(
            read_deletion_vector_with_size(
                &input,
                size,
                offset,
                length,
                &target,
                cardinality,
                limits
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("exceeds Puffin file")
        );
        assert_eq!(f.endpoint.counts.requests(), (0, 0));
    }
}

#[tokio::test]
#[ignore = "focused timing workload; run explicitly with --ignored --nocapture"]
async fn shared_puffin_preparation_benchmark() {
    for (cardinality, delay_ms) in [(16, 0), (16, 2), (4096, 0)] {
        let f = Fixture::new(
            &[32, 32],
            cardinality,
            Duration::from_millis(delay_ms),
            false,
        )
        .await;
        let mut samples = Vec::new();
        for iteration in 0..11 {
            f.endpoint.counts.reset();
            let start = Instant::now();
            f.stage().await.unwrap();
            let stage = start.elapsed();
            let counts = f.endpoint.counts.requests();
            // Do not report candidate timings if the intended reader was not used.
            assert_eq!(counts, (2, f.deletes.len() * 4));
            let bytes = f.endpoint.counts.bytes.load(Ordering::SeqCst);
            let start = Instant::now();
            let mut decoded = 0;
            for id in &f.deletes {
                let d = &f.view.live_files[&id.0].data_file;
                decoded += iceberg::puffin::read_deletion_vector(
                    &f.source_io,
                    d.file_path(),
                    d.content_offset().unwrap() as u64,
                    d.content_size_in_bytes().unwrap() as u64,
                    &d.referenced_data_file().unwrap(),
                    d.record_count(),
                    iceberg::puffin::DeletionVectorLimits::default(),
                )
                .await
                .unwrap()
                .len();
            }
            assert_eq!(decoded as usize, f.expected.len());
            let memory_decode = start.elapsed();
            let start = Instant::now();
            f.store.discard_transaction("epoch").unwrap();
            let mut positions = f.store.position_deletes("scan", &f.schema.table_id);
            loop {
                let batch = positions
                    .by_ref()
                    .take(1024)
                    .collect::<flow_state_store::Result<Vec<_>>>()
                    .unwrap();
                if batch.is_empty() {
                    break;
                }
                f.store
                    .put_position_deletes("epoch", &f.schema.table_id, batch)
                    .unwrap();
            }
            let copy = start.elapsed();
            let start = Instant::now();
            let mut writer = DeleteWriter::new_for_version(
                FileIO::new_with_memory(),
                "memory://rewrite",
                &OperationId("epoch".into()),
                0,
                WriterConfig::default(),
                FormatVersion::V3,
            )
            .unwrap();
            let mut positions = f.store.position_deletes("epoch", &f.schema.table_id);
            loop {
                let batch = positions
                    .by_ref()
                    .take(1024)
                    .collect::<flow_state_store::Result<Vec<_>>>()
                    .unwrap();
                if batch.is_empty() {
                    break;
                }
                writer.write(&batch).await.unwrap();
            }
            assert_eq!(writer.close().await.unwrap().len(), 64);
            let rewrite = start.elapsed();
            f.assert_positions();
            if iteration >= 2 {
                samples.push((
                    stage.as_secs_f64() * 1000.,
                    copy.as_secs_f64() * 1000.,
                    rewrite.as_secs_f64() * 1000.,
                    memory_decode.as_secs_f64() * 1000.,
                ));
                println!(
                    "sample cardinality={cardinality} delay_ms={delay_ms} positions={} heads={} ranges={} range_bytes={bytes} stage_ms={:.3} copy_ms={:.3} rewrite_ms={:.3} memory_decode_ms={:.3}",
                    f.expected.len(),
                    counts.0,
                    counts.1,
                    samples.last().unwrap().0,
                    samples.last().unwrap().1,
                    samples.last().unwrap().2,
                    samples.last().unwrap().3
                );
            }
        }
        let median = |index: usize| {
            let mut values = samples
                .iter()
                .map(|s| [s.0, s.1, s.2, s.3][index])
                .collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            values[values.len() / 2]
        };
        println!(
            "median cardinality={cardinality} delay_ms={delay_ms} stage_ms={:.3} copy_ms={:.3} rewrite_ms={:.3} memory_decode_ms={:.3}",
            median(0),
            median(1),
            median(2),
            median(3)
        );
    }
}

#[tokio::test]
#[ignore = "isolated baseline HEAD cost; run explicitly with --ignored --nocapture"]
async fn baseline_metadata_request_cost() {
    let f = Fixture::new(&[32, 32], 16, Duration::ZERO, false).await;
    let mut samples = Vec::new();
    for iteration in 0..11 {
        f.endpoint.counts.reset();
        let start = Instant::now();
        for id in &f.deletes {
            let path = f.view.live_files[&id.0].file_path();
            for _ in 0..2 {
                f.table
                    .file_io()
                    .new_input(path)
                    .unwrap()
                    .metadata()
                    .await
                    .unwrap();
            }
        }
        assert_eq!(f.endpoint.counts.requests(), (128, 0));
        if iteration >= 2 {
            samples.push(start.elapsed().as_secs_f64() * 1000.);
        }
    }
    println!("128 HEADs in isolation, ms: {samples:?}");
    samples.sort_by(f64::total_cmp);
    println!("128 HEADs median_ms={:.3}", samples[4]);
}
