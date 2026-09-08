//! Independent maintenance fixture for the local integration suite. It reads
//! ordinary Iceberg files and has no access to the ingestion index or journal.

use anyhow::{Context, Result, ensure};
use clap::Parser;
use flow_iceberg_ext::{RewriteFilesAction, SnapshotView};
use flow_materializer::{DataWriter, WriterConfig, iceberg_schema};
use flow_model::{Column, FileId, OperationId, PgLsn, TableId, TableSchema};
use flow_state_store::{StateStore, StateStoreOptions};
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent, spec::DataContentType,
};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

const MARKER: &str = "local-test.compaction-id";
const BATCH_ROWS: usize = 1024;
const DEFAULT_MAX_INPUT_BYTES: u64 = 512 << 20;
const DEFAULT_MAX_INPUT_FILES: usize = 256;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value_t = 3000)]
    interval_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_INPUT_FILES)]
    max_input_files: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_INPUT_BYTES)]
    max_input_bytes: u64,
}

// Only destination settings are deserialized from the shared configuration.
#[derive(Deserialize)]
struct Config {
    catalog: HashMap<String, String>,
    tables: Vec<Target>,
}

#[derive(Deserialize)]
struct Target {
    target_namespace: Vec<String>,
    target_table: String,
    columns: Vec<Column>,
    primary_key: Vec<usize>,
    #[serde(default)]
    append_only: bool,
}

impl Target {
    fn schema(&self) -> TableSchema {
        TableSchema {
            table_id: TableId(0),
            version: 0,
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
            append_only: self.append_only,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.interval_ms > 0, "interval must be positive");
    ensure!(args.max_input_files > 0, "max input files must be positive");
    ensure!(args.max_input_bytes > 0, "max input bytes must be positive");
    let config: Config = toml::from_str(&std::fs::read_to_string(&args.config)?)?;
    ensure!(!config.tables.is_empty(), "at least one table is required");
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("local-compactor", config.catalog)
        .await?;
    println!(
        "{}",
        json!({
            "event": "compactor_ready",
            "pid": std::process::id(),
            "max_input_files": args.max_input_files,
            "max_input_bytes": args.max_input_bytes,
        })
    );
    let mut interval = tokio::time::interval(Duration::from_millis(args.interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            result = &mut shutdown => return Ok(result?),
            _ = interval.tick() => {}
        }
        for target in &config.tables {
            compact(&catalog, target, args.max_input_files, args.max_input_bytes)
                .await
                .with_context(|| format!("compact {}", target.target_table))?;
        }
    }
}

async fn compact(
    catalog: &dyn Catalog,
    target: &Target,
    max_input_files: usize,
    max_input_bytes: u64,
) -> Result<()> {
    let ident = TableIdent::new(
        NamespaceIdent::from_vec(target.target_namespace.clone())?,
        target.target_table.clone(),
    );
    let table = catalog.load_table(&ident).await?;
    let Some(base_snapshot) = table.metadata().current_snapshot_id() else {
        return Ok(());
    };
    let schema = target.schema();
    schema.validate()?;
    ensure!(
        table.metadata().current_schema().as_ref() == &iceberg_schema(&schema)?,
        "fixture schema differs from Iceberg"
    );
    ensure!(
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty()
            && table.metadata().default_sort_order().fields.is_empty(),
        "fixture requires unpartitioned, unsorted tables"
    );
    let view = SnapshotView::current(&table).await?;
    let mut data = BTreeSet::new();
    let mut deletes = BTreeSet::new();
    let mut physical_files = BTreeSet::new();
    let mut bytes = 0u64;
    for entry in view.live_files.values() {
        match entry.content_type() {
            DataContentType::Data => {
                data.insert(FileId(entry.file_path().to_owned()));
            }
            DataContentType::PositionDeletes => {
                deletes.insert(flow_iceberg_ext::content_file_id(&entry.data_file));
            }
            DataContentType::EqualityDeletes => anyhow::bail!("equality deletes are unsupported"),
        }
        if physical_files.insert(entry.file_path()) {
            bytes = bytes
                .checked_add(entry.data_file.file_size_in_bytes())
                .context("input size overflow")?;
        }
    }
    if data.is_empty() || (data.len() == 1 && deletes.is_empty()) {
        return Ok(());
    }
    ensure!(
        bytes <= max_input_bytes && physical_files.len() <= max_input_files,
        "fixture maintenance budget exceeded: {} files / {} bytes exceeds configured maximum of {} files / {} bytes",
        physical_files.len(),
        bytes,
        max_input_files,
        max_input_bytes,
    );
    let started = Instant::now();
    let operation = OperationId(format!("local-compaction-{}", uuid::Uuid::new_v4()));
    println!(
        "{}",
        json!({
            "event": "compaction_started", "table": target.target_table,
            "operation": operation.0, "base_snapshot": base_snapshot,
            "input_data_files": data.len(), "input_delete_files": deletes.len(), "input_bytes": bytes,
            "max_input_files": max_input_files, "max_input_bytes": max_input_bytes,
        })
    );
    let scratch_dir = tempfile::tempdir()?;
    let scratch = StateStore::open(
        scratch_dir.path().join("scratch"),
        StateStoreOptions::default(),
    )?;
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &operation,
        schema.clone(),
        table.metadata().default_partition_spec_id(),
        WriterConfig {
            target_file_bytes: 128 << 20,
            row_group_rows: 8192,
            ..Default::default()
        },
    )?;
    if table.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
        writer = writer.with_row_lineage()?;
    }
    let mut rows = 0u64;
    flow_compactor::scan_live_files(
        &table,
        &schema,
        &view,
        &data,
        &scratch,
        &operation.0,
        BATCH_ROWS,
        &flow_compactor::ReadLimits::default(),
        async |batch| {
            rows += batch.rows.len() as u64;
            writer
                .write_with_lineage(&batch.rows, batch.lineage.as_deref(), PgLsn(0))
                .await?;
            Ok(())
        },
    )
    .await?;
    let files = writer.close().await?;
    let output_files = files.len();
    let action = RewriteFilesAction::new(&table, operation.0.clone())
        .with_operation_id_key(MARKER)?
        .remove_data_files(data.into_iter().map(|file| file.0))
        .remove_delete_files(deletes)
        .add_data_files(files)
        .with_properties(HashMap::from([
            ("streaming.operation".into(), "compact".into()),
            ("streaming.level".into(), "L1".into()),
            (
                "streaming.writer-id".into(),
                "local-compactor-fixture".into(),
            ),
        ]));
    // A lost commit response must retry the same artifacts and marker before
    // replanning. A proven conflict abandons this attempt; the next tick scans
    // fresh files. Unreferenced output remains for separate orphan cleanup.
    let result = match action.commit(catalog, &table).await {
        Err(error) if error.kind() == ErrorKind::Unexpected => action.commit(catalog, &table).await,
        result => result,
    };
    match result {
        Ok(result) => println!(
            "{}",
            json!({
                "event": "compaction_committed", "table": target.target_table,
                "operation": operation.0, "base_snapshot": base_snapshot,
                "snapshot": result.snapshot_id, "rows": rows, "output_data_files": output_files,
                "elapsed_ms": started.elapsed().as_millis(), "recovered": result.already_committed,
            })
        ),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PreconditionFailed | ErrorKind::CatalogCommitConflicts
            ) =>
        {
            println!(
                "{}",
                json!({
                    "event": "compaction_conflict", "table": target.target_table,
                    "operation": operation.0, "base_snapshot": base_snapshot,
                    "rows": rows, "elapsed_ms": started.elapsed().as_millis(), "error": error.to_string(),
                })
            );
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
