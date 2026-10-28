//! Focused worker timing for one full-table compaction over local files. The
//! catalog, scratch store and row index are real; publication is not timed.

use flow_compactor::{CompactionPlan, Level, ReadLimits, compact};
use flow_iceberg_ext::{ManifestCache, RowDeltaAction, content_file_id, delete_content_size};
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig, iceberg_schema};
use flow_model::{
    Column, ColumnType, FileId, OperationId, PgLsn, Row, RowLocation, TableId, TableSchema, Value,
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

/// Deterministic, roughly 220-byte rows with mixed widths and nulls.
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

fn prepared(id: &str, kind: OperationKind, base: Option<i64>) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TableId(1),
        kind,
        base_snapshot_id: base,
        last_lsn: PgLsn(1),
        schema_version: 0,
        artifacts: vec![],
        payload: vec![],
    }
}

fn apply(index: &StateStore, id: &str, snapshot: i64, sequence: i64) {
    let id = OperationId(id.into());
    index.mark_committed(&id, snapshot, sequence).unwrap();
    index.apply_committed(&id).unwrap();
}

struct Fixture {
    _dir: TempDir,
    table: Table,
    index: StateStore,
    plan: CompactionPlan,
    live_rows: u64,
    input_bytes: u64,
}

/// Commit `files` data files and delete every `delete_every`th row, if any.
async fn fixture(
    version: FormatVersion,
    files: usize,
    rows_per_file: usize,
    delete_every: usize,
) -> Fixture {
    let dir = TempDir::new().unwrap();
    let schema = schema();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "compaction-benchmark",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.into(),
                dir.path().join("warehouse").display().to_string(),
            )]),
        )
        .await
        .unwrap();
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
                .format_version(version)
                .build(),
        )
        .await
        .unwrap();
    let index = StateStore::open(dir.path().join("index"), StateStoreOptions::default()).unwrap();
    let mut data_files = Vec::new();
    let mut deltas = Vec::new();
    let mut deleted = Vec::new();
    for file in 0..files {
        let rows: Vec<Row> = (0..rows_per_file)
            .map(|offset| row((file * rows_per_file + offset) as i64))
            .collect();
        // One writer per file gives the many-small-files shape compaction repairs.
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
            let key = schema.encode_key(row).unwrap();
            if delete_every > 0 && location.row_position as usize % delete_every == 3 {
                deleted.push((key.clone(), location.clone()));
            }
            deltas.push(IndexDelta {
                key,
                expected: None,
                replacement: Some(location),
            });
        }
    }
    assert_eq!(data_files.len(), files, "one data file per writer");
    let ingest = RowDeltaAction::new(&table, "ingest")
        .add_data_files(data_files.clone())
        .commit(&catalog, &table)
        .await
        .unwrap();
    index
        .prepare(prepared("ingest", OperationKind::Ingest, None), deltas)
        .unwrap();
    apply(&index, "ingest", ingest.snapshot_id, ingest.sequence_number);
    let mut table = ingest.table;
    let mut delete_files = Vec::new();
    if !deleted.is_empty() {
        // The index assigns the committed sequence; deletes must expect it.
        for (key, location) in &mut deleted {
            *location = index.lookup(&schema.table_id, key).unwrap().unwrap();
        }
        deleted.sort_by(|a, b| {
            (&a.1.data_file_id, a.1.row_position).cmp(&(&b.1.data_file_id, b.1.row_position))
        });
        let mut writer = DeleteWriter::new_for_version(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId("delete".into()),
            0,
            WriterConfig::default(),
            version,
        )
        .unwrap();
        let positions: Vec<RowLocation> = deleted
            .iter()
            .map(|(_, location)| location.clone())
            .collect();
        writer.write(&positions).await.unwrap();
        delete_files = writer.close().await.unwrap();
        let deletion = RowDeltaAction::new(&table, "delete")
            .add_delete_files(delete_files.clone())
            .validate_data_files_exist(data_files.iter().map(|file| file.file_path().to_owned()))
            .commit(&catalog, &table)
            .await
            .unwrap();
        index
            .prepare(
                prepared("delete", OperationKind::Ingest, Some(ingest.snapshot_id)),
                deleted.into_iter().map(|(key, original)| IndexDelta {
                    key,
                    expected: Some(original),
                    replacement: None,
                }),
            )
            .unwrap();
        apply(
            &index,
            "delete",
            deletion.snapshot_id,
            deletion.sequence_number,
        );
        table = deletion.table;
    }
    let input_bytes = data_files
        .iter()
        .map(|file| file.file_size_in_bytes())
        .sum();
    let delete_rows: u64 = delete_files.iter().map(|file| file.record_count()).sum();
    let plan = CompactionPlan {
        base_snapshot_id: table.metadata().current_snapshot_id().unwrap(),
        schema_id: 0,
        input_files: data_files
            .iter()
            .map(|file| FileId(file.file_path().into()))
            .collect(),
        delete_files: delete_files
            .iter()
            .map(|file| FileId(content_file_id(file)))
            .collect::<BTreeSet<_>>(),
        spec_id: 0,
        partition: vec![],
        output_level: Level::L1,
        target_file_bytes: 128 << 20,
        input_bytes,
        delete_input_bytes: delete_files.iter().map(delete_content_size).sum(),
        delete_input_rows: delete_rows,
        lazy_index_remap: false,
    };
    Fixture {
        _dir: dir,
        table,
        index,
        plan,
        live_rows: (files * rows_per_file) as u64 - delete_rows,
        input_bytes,
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

async fn run(fixture: &Fixture, iteration: usize) -> Sample {
    let scratch_dir = TempDir::new().unwrap();
    let scratch = StateStore::open(scratch_dir.path(), StateStoreOptions::scratch()).unwrap();
    // Reset the peak-RSS watermark so it measures this compaction only.
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
    let rss_before = status_kib("VmRSS:");
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    let output = compact(
        &fixture.table,
        schema(),
        fixture.plan.clone(),
        OperationId(format!("bench-{iteration}")),
        &fixture.index,
        scratch,
        WriterConfig::default(),
        &ReadLimits::default(),
        &ManifestCache::default(),
    )
    .await
    .unwrap();
    let wall = started.elapsed();
    let cpu = cpu_seconds() - cpu_before;
    let peak_growth_kib = status_kib("VmHWM:").saturating_sub(rss_before);
    assert_eq!(
        output
            .data_files
            .iter()
            .map(|file| file.record_count())
            .sum::<u64>(),
        fixture.live_rows
    );
    assert_eq!(
        output.index_deltas().unwrap().count() as u64,
        fixture.live_rows
    );
    for file in &output.data_files {
        fixture
            .table
            .file_io()
            .delete(file.file_path())
            .await
            .unwrap();
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
fn compaction_worker_benchmark() {
    let files = env("FLOW_BENCH_FILES", 16);
    let rows_per_file = env("FLOW_BENCH_ROWS_PER_FILE", 32_768);
    let warmups = env("FLOW_BENCH_WARMUPS", 1);
    let iterations = env("FLOW_BENCH_ITERATIONS", 7);
    // Production runs workers on a dedicated single-threaded I/O runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    println!(
        "host_cpus={} files={files} rows_per_file={rows_per_file} warmups={warmups} iterations={iterations}",
        std::thread::available_parallelism().unwrap()
    );
    println!(
        "| case | live rows | input MiB | p50 ms | p95 ms | min ms | rows/s (p50) | input MiB/s (p50) | CPU s (p50) | peak RSS growth MiB (max) |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for (name, version, delete_every) in [
        ("v2, no deletes", FormatVersion::V2, 0),
        ("v2, 10% position deletes", FormatVersion::V2, 10),
        ("v3, 10% deletion vectors", FormatVersion::V3, 10),
    ] {
        runtime.block_on(async {
            let fixture = fixture(version, files, rows_per_file, delete_every).await;
            for warmup in 0..warmups {
                run(&fixture, usize::MAX - warmup).await;
            }
            let mut samples = Vec::new();
            for iteration in 0..iterations {
                samples.push(run(&fixture, iteration).await);
            }
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
            let input_mib = fixture.input_bytes as f64 / (1 << 20) as f64;
            println!(
                "| {name} | {} | {input_mib:.1} | {p50:.0} | {:.0} | {:.0} | {:.0} | {:.1} | {:.2} | {:.0} |",
                fixture.live_rows,
                percentile(&wall, 95.0),
                wall[0],
                fixture.live_rows as f64 / (p50 / 1000.0),
                input_mib / (p50 / 1000.0),
                percentile(&cpu, 50.0),
                peak as f64 / 1024.0,
            );
        });
    }
}
