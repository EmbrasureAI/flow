//! Focused timing for reconciling one external data rewrite over local files.
//! The catalog, scratch store and row index are real. Each iteration starts
//! from a copy of the same pre-rewrite index and rotates through the scratch
//! memtable sizes in `FLOW_BENCH_SCRATCH_MIB`, so host noise affects every
//! size alike.

use flow_compactor::Policy;
use flow_coordinator::TableMaintenance;
use flow_iceberg_ext::{RewriteFilesAction, RowDeltaAction, SnapshotView};
use flow_materializer::{DataWriter, WriterConfig, iceberg_schema};
use flow_model::{
    Column, ColumnType, OperationId, PgLsn, PrimaryKey, Row, TableId, TableSchema, Value,
};
use flow_state_store::{
    IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    io::LocalFsStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::FormatVersion,
    table::Table,
};
use std::{
    collections::HashMap,
    path::Path,
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

/// The compaction benchmark's ten-column schema.
fn schema() -> TableSchema {
    let column = |field_id, name: &str, data_type, nullable| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable,
    };
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![
            column(1, "id", ColumnType::Int64, false),
            column(2, "account_id", ColumnType::Int64, false),
            column(3, "status", ColumnType::String, false),
            column(4, "payload", ColumnType::String, false),
            column(
                5,
                "amount",
                ColumnType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                false,
            ),
            column(6, "created_at", ColumnType::TimestampTzMicros, false),
            column(7, "score", ColumnType::Float64, true),
            column(8, "active", ColumnType::Bool, false),
            column(9, "note", ColumnType::String, true),
            column(10, "external_id", ColumnType::Uuid, false),
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

/// The compaction benchmark's deterministic, roughly 220-byte rows.
fn row(id: i64) -> Row {
    let mut state = (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    const STATUS: [&str; 6] = ["pending", "active", "settled", "failed", "refunded", "held"];
    let payload_len = 64 + (next() % 96) as usize;
    let seed = next();
    let payload = (0..payload_len)
        .map(|index| char::from(b'a' + ((seed >> (index % 59)) as u8 % 26)))
        .collect();
    let mut uuid = [0; 16];
    uuid[..8].copy_from_slice(&next().to_be_bytes());
    uuid[8..].copy_from_slice(&next().to_be_bytes());
    vec![
        Value::Int64(id),
        Value::Int64((next() % 50_000) as i64),
        Value::String(STATUS[(next() % 6) as usize].into()),
        Value::String(payload),
        Value::Decimal {
            unscaled: (next() % 100_000_000) as i128 - 50_000_000,
            scale: 2,
        },
        Value::TimestampTzMicros(1_700_000_000_000_000 + (next() % 10_000_000_000) as i64),
        if next() % 8 == 0 {
            Value::Null
        } else {
            Value::Float64((next() % 1_000_000) as f64 / 7.0)
        },
        Value::Bool(next() % 2 == 0),
        if next() % 2 == 0 {
            Value::Null
        } else {
            Value::String(format!("note-{}", next() % 1_000))
        },
        Value::Uuid(uuid),
    ]
}

async fn write(table: &Table, operation: &str, rows: &[Row]) -> Vec<iceberg::spec::DataFile> {
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &OperationId(operation.into()),
        schema(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(rows, PgLsn(1)).await.unwrap();
    writer.close().await.unwrap()
}

struct Fixture {
    dir: TempDir,
    catalog: Arc<dyn Catalog>,
    head: Table,
    rows: u64,
    input_bytes: u64,
    sample_keys: Vec<PrimaryKey>,
    original_files: Vec<String>,
}

/// Ingest `files` service-written files, then commit an external rewrite of
/// all of them into one file, as Spark's `rewrite_data_files` would. The
/// pre-rewrite index is left closed in `dir/index` for each iteration to copy.
async fn fixture(files: usize, rows_per_file: usize) -> Fixture {
    let dir = TempDir::new().unwrap();
    let schema = schema();
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "reconcile-benchmark",
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
    let index = StateStore::open(dir.path().join("index"), StateStoreOptions::default()).unwrap();
    let mut data_files = Vec::new();
    let mut deltas = Vec::new();
    for file in 0..files {
        let rows: Vec<Row> = (0..rows_per_file)
            .map(|offset| row((file * rows_per_file + offset) as i64))
            .collect();
        let mut writer = DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(format!("ingest-{file}")),
            schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        let locations = writer.write(&rows, PgLsn(1)).await.unwrap().locations;
        data_files.extend(writer.close().await.unwrap());
        for (row, location) in rows.iter().zip(locations) {
            deltas.push(IndexDelta {
                key: schema.encode_key(row).unwrap(),
                expected: None,
                replacement: Some(location),
            });
        }
    }
    assert_eq!(data_files.len(), files, "one data file per writer");
    let sample_keys = deltas
        .iter()
        .step_by(deltas.len().div_ceil(64))
        .map(|delta| delta.key.clone())
        .collect();
    let ingest = RowDeltaAction::new(&table, "ingest")
        .add_data_files(data_files.clone())
        .commit(catalog.as_ref(), &table)
        .await
        .unwrap();
    let operation = OperationId("ingest".into());
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
            deltas,
        )
        .unwrap();
    index
        .mark_committed(&operation, ingest.snapshot_id, ingest.sequence_number)
        .unwrap();
    index.apply_committed(&operation).unwrap();
    drop(index);

    let head = ingest.table;
    let rows: Vec<Row> = (0..files * rows_per_file)
        .map(|id| row(id as i64))
        .collect();
    let rewritten = write(&head, "external-rewrite", &rows).await;
    let view = SnapshotView::current(&head).await.unwrap();
    let original_files: Vec<String> = view.live_files.keys().cloned().collect();
    let head = RewriteFilesAction::new(&head, "external-rewrite")
        .with_operation_id_key("external.compaction-id")
        .unwrap()
        .remove_data_files(original_files.clone())
        .add_data_files(rewritten)
        .commit(catalog.as_ref(), &head)
        .await
        .unwrap()
        .table;
    Fixture {
        dir,
        catalog,
        head,
        rows: rows.len() as u64,
        input_bytes: data_files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum(),
        sample_keys,
        original_files,
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Process CPU seconds from /proc, so the cost is visible on small hosts.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat.rsplit_once(") ").unwrap().1.split(' ').collect();
    let ticks: f64 = fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap();
    ticks / 100.0
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|value| value.trim().strip_suffix(" kB"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

struct Sample {
    wall: Duration,
    cpu: f64,
    peak_growth_kib: u64,
}

async fn run(fixture: &Fixture, scratch_mib: usize) -> Sample {
    let run_dir = TempDir::new_in(fixture.dir.path()).unwrap();
    copy_dir(
        &fixture.dir.path().join("index"),
        &run_dir.path().join("index"),
    );
    let index =
        StateStore::open(run_dir.path().join("index"), StateStoreOptions::default()).unwrap();
    // The daemon's reconcile scratch options at the default 16 MiB; other
    // sizes vary only the memtables.
    let scratch = StateStore::open(
        run_dir.path().join("scratch"),
        StateStoreOptions {
            write_buffer_bytes: scratch_mib << 20,
            ..Default::default()
        },
    )
    .unwrap();
    let maintenance = TableMaintenance::new(
        index.clone(),
        fixture.catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    // Reset the peak-RSS watermark so it measures this reconcile only.
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
    let rss_before = status_kib("VmRSS:");
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    let snapshot = maintenance
        .reconcile(&fixture.head, &schema(), scratch)
        .await
        .unwrap();
    let wall = started.elapsed();
    let cpu = cpu_seconds() - cpu_before;
    let peak_growth_kib = status_kib("VmHWM:").saturating_sub(rss_before);
    assert_eq!(snapshot, fixture.head.metadata().current_snapshot_id());
    for key in &fixture.sample_keys {
        let location = index.lookup(&TableId(1), key).unwrap().unwrap();
        assert!(
            !fixture.original_files.contains(&location.data_file_id.0),
            "reconcile left a row on a rewritten file"
        );
    }
    Sample {
        wall,
        cpu,
        peak_growth_kib,
    }
}

fn percentile(sorted: &[f64], percent: f64) -> f64 {
    let rank = ((sorted.len() - 1) as f64 * percent / 100.0).round() as usize;
    sorted[rank]
}

#[test]
#[ignore = "focused timing workload; run explicitly with --release --ignored --nocapture"]
fn reconcile_external_rewrite_benchmark() {
    let files = env("FLOW_BENCH_FILES", 16);
    let rows_per_file = env("FLOW_BENCH_ROWS_PER_FILE", 32_768);
    let warmups = env("FLOW_BENCH_WARMUPS", 1);
    let iterations = env("FLOW_BENCH_ITERATIONS", 7);
    let sizes: Vec<usize> = std::env::var("FLOW_BENCH_SCRATCH_MIB")
        .unwrap_or_else(|_| "16,64".into())
        .split(',')
        .map(|size| size.trim().parse().expect("scratch memtable MiB"))
        .collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let fixture = fixture(files, rows_per_file).await;
        for _ in 0..warmups {
            for &size in &sizes {
                run(&fixture, size).await;
            }
        }
        let mut samples: Vec<Vec<Sample>> = sizes.iter().map(|_| Vec::new()).collect();
        for iteration in 0..iterations {
            // Rotate the starting size so none always runs first.
            for offset in 0..sizes.len() {
                let variant = (iteration + offset) % sizes.len();
                samples[variant].push(run(&fixture, sizes[variant]).await);
            }
        }
        println!(
            "| Scratch memtable MiB | Rows | Input MiB | p50 ms | p95 ms | min ms | Rows/s | CPU s | Peak RSS growth MiB |"
        );
        println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- |");
        for (size, samples) in sizes.iter().zip(samples) {
            let mut wall: Vec<_> = samples
                .iter()
                .map(|sample| sample.wall.as_secs_f64() * 1000.0)
                .collect();
            wall.sort_by(f64::total_cmp);
            let mut cpu: Vec<_> = samples.iter().map(|sample| sample.cpu).collect();
            cpu.sort_by(f64::total_cmp);
            let peak = samples
                .iter()
                .map(|sample| sample.peak_growth_kib)
                .max()
                .unwrap();
            let p50 = percentile(&wall, 50.0);
            println!(
                "| {size} | {} | {:.1} | {p50:.0} | {:.0} | {:.0} | {:.0} | {:.2} | {:.0} |",
                fixture.rows,
                fixture.input_bytes as f64 / (1 << 20) as f64,
                percentile(&wall, 95.0),
                wall[0],
                fixture.rows as f64 / (p50 / 1000.0),
                percentile(&cpu, 50.0),
                peak as f64 / 1024.0,
            );
        }
    });
}
