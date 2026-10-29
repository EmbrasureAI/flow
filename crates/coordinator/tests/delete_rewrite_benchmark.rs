//! Focused timing for one position-delete consolidation over local files: the
//! inline merge a table's publication lane waits for. The catalog, row index
//! and scratch store are real. Data files are metadata only, because a
//! delete-only rewrite never reads them. Each iteration rebuilds the table.

use flow_compactor::{DeleteLayout, Policy};
use flow_coordinator::{DeleteRewritePolicy, TableMaintenance};
use flow_iceberg_ext::{RowDeltaAction, SnapshotView};
use flow_materializer::{DeleteWriter, WriterConfig, iceberg_schema};
use flow_model::{
    Column, ColumnType, FileId, OperationId, PgLsn, RowLocation, TableId, TableSchema,
};
use flow_state_store::{OperationKind, PreparedOperation, StateStore, StateStoreOptions};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    io::LocalFsStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, Struct},
    table::Table,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().expect("numeric benchmark setting"))
        .unwrap_or(default)
}

fn schema() -> TableSchema {
    TableSchema {
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
    }
}

struct Settings {
    delete_files: usize,
    positions: usize,
    targets: usize,
    target_rows: u64,
    path_bytes: usize,
    layout: DeleteLayout,
}

struct Fixture {
    _dir: TempDir,
    catalog: Arc<dyn Catalog>,
    index: StateStore,
    scratch: StateStore,
    head: Table,
    input_bytes: u64,
    unique: u64,
}

/// `delete_files` epochs, each deleting about `positions / delete_files`
/// random rows spread over every target, so epochs overlap a little.
async fn fixture(settings: &Settings) -> Fixture {
    let dir = TempDir::new().unwrap();
    let schema = schema();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "delete-rewrite-benchmark",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.into(),
                    dir.path().join("warehouse").display().to_string(),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("bench".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("events".into())
                .schema(iceberg_schema(&schema).unwrap())
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    let prefix = format!("{}/data/", table.metadata().location());
    let targets: Vec<String> = (0..settings.targets)
        .map(|target| {
            let name = format!("-target-{target:05}.parquet");
            let pad = settings
                .path_bytes
                .saturating_sub(prefix.len() + name.len())
                .max(1);
            format!("{prefix}{}{name}", "0".repeat(pad))
        })
        .collect();
    let data = targets
        .iter()
        .map(|path| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path(path.clone())
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(settings.target_rows)
                .file_size_in_bytes(settings.target_rows * 64)
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut head = RowDeltaAction::new(&table, "data")
        .add_data_files(data)
        .commit(catalog.as_ref(), &table)
        .await
        .unwrap()
        .table;
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let per_target = settings.positions / settings.delete_files / settings.targets;
    let mut all = BTreeSet::new();
    let mut input_bytes = 0;
    for file in 0..settings.delete_files {
        let mut positions = Vec::new();
        for target in &targets {
            let rows = (0..per_target)
                .map(|_| next() % settings.target_rows)
                .collect::<BTreeSet<_>>();
            for row in rows {
                all.insert((target.clone(), row));
                positions.push(RowLocation {
                    data_file_id: FileId(target.clone()),
                    row_position: row,
                    data_sequence_number: 1,
                    spec_id: 0,
                    partition: Vec::new(),
                    source_commit_lsn: PgLsn(0),
                    row_version: 0,
                    row_fingerprint: [0; 16],
                });
            }
        }
        let id = format!("epoch-{file}");
        let mut writer = DeleteWriter::new(
            head.file_io().clone(),
            head.metadata().location(),
            &OperationId(id.clone()),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        for batch in positions.chunks(8192) {
            writer.write(batch).await.unwrap();
        }
        let files = writer.close().await.unwrap();
        input_bytes += files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum::<u64>();
        head = RowDeltaAction::new(&head, &id)
            .add_delete_files(files)
            .validate_data_files_exist(targets.iter().cloned())
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap()
            .table;
    }
    let index = StateStore::open(dir.path().join("index"), StateStoreOptions::default()).unwrap();
    let operation = OperationId("seed-index".into());
    index
        .prepare(
            PreparedOperation {
                id: operation.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(1),
                schema_version: 0,
                artifacts: vec![],
                payload: vec![],
            },
            std::iter::empty(),
        )
        .unwrap();
    let snapshot = head.metadata().current_snapshot().unwrap();
    index
        .mark_committed(
            &operation,
            snapshot.snapshot_id(),
            snapshot.sequence_number(),
        )
        .unwrap();
    index.apply_committed(&operation).unwrap();
    index.forget_applied(&operation).unwrap();
    // The daemon's delete-compaction scratch store, at its default batch size.
    let scratch =
        StateStore::open(dir.path().join("scratch"), StateStoreOptions::scratch()).unwrap();
    Fixture {
        _dir: dir,
        catalog,
        index,
        scratch,
        head,
        input_bytes,
        unique: all.len() as u64,
    }
}

async fn run(settings: &Settings) -> (Duration, Fixture) {
    let fixture = fixture(settings).await;
    let maintenance = TableMaintenance::new(
        fixture.index.clone(),
        fixture.catalog.clone(),
        Policy {
            delete_layout: settings.layout,
            ..Policy::default()
        },
        WriterConfig::default(),
    )
    .unwrap();
    let started = Instant::now();
    let snapshot = maintenance
        .compact_deletes(
            &fixture.head,
            &schema(),
            fixture.scratch.clone(),
            &DeleteRewritePolicy {
                min_input_files: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let wall = started.elapsed();
    assert!(snapshot.is_some(), "consolidation must publish");
    let head = fixture
        .catalog
        .load_table(fixture.head.identifier())
        .await
        .unwrap();
    let view = SnapshotView::current(&head).await.unwrap();
    let rows = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        .map(|entry| entry.data_file.record_count())
        .sum::<u64>();
    assert_eq!(rows, fixture.unique, "consolidated positions");
    (wall, fixture)
}

#[test]
#[ignore = "focused timing workload; run explicitly with --release --ignored --nocapture"]
fn delete_consolidation_benchmark() {
    let settings = Settings {
        delete_files: env("FLOW_BENCH_DELETE_FILES", 8),
        positions: env("FLOW_BENCH_POSITIONS", 800_000),
        targets: env("FLOW_BENCH_TARGETS", 64),
        target_rows: env("FLOW_BENCH_TARGET_ROWS", 1 << 20) as u64,
        path_bytes: env("FLOW_BENCH_PATH_BYTES", 120),
        layout: match std::env::var("FLOW_BENCH_LAYOUT").as_deref() {
            Ok("per_target") => DeleteLayout::PerTarget,
            Ok("shared") | Err(_) => DeleteLayout::Shared,
            Ok(other) => panic!("unknown FLOW_BENCH_LAYOUT {other}"),
        },
    };
    let warmups = env("FLOW_BENCH_WARMUPS", 1);
    let iterations = env("FLOW_BENCH_ITERATIONS", 7);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for _ in 0..warmups {
            run(&settings).await;
        }
        let mut wall = Vec::new();
        let mut last = None;
        for _ in 0..iterations {
            let (elapsed, fixture) = run(&settings).await;
            wall.push(elapsed.as_secs_f64() * 1000.0);
            last = Some(fixture);
        }
        wall.sort_by(f64::total_cmp);
        let fixture = last.unwrap();
        let p50 = wall[wall.len() / 2];
        println!(
            "| Layout | Inputs | Input MiB | Unique positions | p50 ms | min ms | max ms | us/position |"
        );
        println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
        println!(
            "| {} | {} | {:.2} | {} | {p50:.0} | {:.0} | {:.0} | {:.2} |",
            settings.layout.as_str(),
            settings.delete_files,
            fixture.input_bytes as f64 / (1 << 20) as f64,
            fixture.unique,
            wall[0],
            wall[wall.len() - 1],
            p50 * 1000.0 / fixture.unique as f64,
        );
    });
}
