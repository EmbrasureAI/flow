//! Index lifecycle. Durable control is opened before a replaceable row index;
//! source acknowledgement and publication begin only after recovery returns.

use crate::config::Config;
use anyhow::{Context, Result, ensure};
use flow_coordinator::{
    ReplanRequired, SourceSchemaRecord, TableMaintenance, load_control_schema,
    resolve_catalog_operation, same_iceberg_schema, source_schema_prefix,
};
use flow_materializer::iceberg_schema;
use flow_model::{SourceId, TableId, TableSchema};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use iceberg::{Catalog, table::Table};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Instant};

fn options(config: &Config) -> StateStoreOptions {
    StateStoreOptions {
        apply_batch_rows: config.limits.batch_rows,
        ..Default::default()
    }
}

pub(crate) fn open(config: &Config, control: ControlStore) -> Result<StateStore> {
    match control.active_generation()? {
        Some(active) => Ok(StateStore::open_with_control(
            active.path,
            options(config),
            control,
        )?),
        None => Ok(control.initialize_index(config.state_dir.join("index"), options(config))?),
    }
}

/// The caller must validate persisted target UUIDs before invoking this method.
/// A fresh catalog scan preserves control watermarks; it never invents source
/// progress from a row count or a snapshot's current sequence number.
pub(crate) async fn rebuild(
    config: &Config,
    control: ControlStore,
    catalog: Arc<dyn Catalog>,
    schemas: &[TableSchema],
    targets: &BTreeMap<TableId, Table>,
) -> Result<StateStore> {
    let started = Instant::now();
    for operation in control.pending_operations()? {
        let table = targets
            .get(&operation.operation.table_id)
            .context("pending operation references an unconfigured table")?;
        let outcome =
            resolve_catalog_operation(&operation, catalog.as_ref(), table, &control).await?;
        let authority = control.clone();
        tokio::task::spawn_blocking(move || {
            authority.resolve_operation(&operation.operation.id, outcome)
        })
        .await??;
    }
    let mut refreshed_targets = BTreeMap::new();
    for (id, table) in targets {
        let head = catalog.load_table(table.identifier()).await?;
        ensure!(
            head.metadata().uuid() == table.metadata().uuid(),
            "target incarnation changed while resolving catalog operations"
        );
        refreshed_targets.insert(*id, head);
    }
    let targets = &refreshed_targets;
    let source = SourceId(config.source.id.clone());
    let schemas = schemas
        .iter()
        .map(|base| -> Result<TableSchema> {
            let target = targets
                .get(&base.table_id)
                .context("missing target table")?;
            if same_iceberg_schema(target.metadata().current_schema(), &iceberg_schema(base)?) {
                return Ok(base.clone());
            }
            for record in control
                .source_transactions_after(&source_schema_prefix(&source, base.table_id), None)
            {
                let (_, bytes) = record?;
                let record = SourceSchemaRecord::decode(&bytes)?;
                if same_iceberg_schema(
                    target.metadata().current_schema(),
                    &iceberg_schema(&record.schema)?,
                ) {
                    base.validate_successor(&record.schema)?;
                    return Ok(record.schema);
                }
            }
            anyhow::bail!("target schema has no durable source lineage")
        })
        .collect::<Result<Vec<_>>>()?;
    let authority: BTreeMap<_, _> = control.table_states()?.into_iter().collect();
    ensure!(
        authority.len() == schemas.len(),
        "durable control does not cover every source table; recover initialization before rebuilding"
    );
    for schema in &schemas {
        let state = authority
            .get(&schema.table_id)
            .context("source table has no durable watermark")?;
        ensure!(
            state.schema_version <= schema.version,
            "target schema regressed behind durable source progress"
        );
        if state.schema_version < schema.version {
            load_control_schema(&control, &source, schema.table_id, state.schema_version)?
                .validate_successor(schema)?;
        }
    }

    let mut checkpoints = control.checkpoints()?;
    checkpoints.sort_by_key(|checkpoint| std::cmp::Reverse(checkpoint.revision));
    let mut restored_candidate = None;
    for checkpoint in checkpoints {
        let usable = checkpoint.pending_operations.is_empty()
            && checkpoint.tables.len() == authority.len()
            && checkpoint.tables.iter().all(|(id, state)| {
                authority.get(id).is_some_and(|current| {
                    state.materialized_lsn == current.materialized_lsn
                        && state.schema_version == current.schema_version
                        && targets.get(id).is_some_and(|table| {
                            table.metadata().current_snapshot_id() == state.snapshot_id
                        })
                })
            });
        if !usable || !checkpoint.path.join("CURRENT").is_file() {
            continue;
        }
        let directory = candidate_directory(config)?;
        let candidate = directory.clone();
        let authority = control.clone();
        let options = options(config);
        match tokio::task::spawn_blocking(move || {
            authority.restore_checkpoint(&checkpoint, candidate, options)
        })
        .await?
        {
            Ok(index) => {
                restored_candidate = Some((directory, index));
                break;
            }
            Err(error) => {
                tracing::warn!(%error, "checkpoint restore failed; trying another recovery source");
                if directory.exists() {
                    std::fs::remove_dir_all(&directory)?;
                }
            }
        }
    }
    let restored = restored_candidate.is_some();
    let (directory, replacement) = if let Some(candidate) = restored_candidate {
        candidate
    } else {
        let directory = candidate_directory(config)?;
        let candidate = directory.clone();
        let options = options(config);
        let index =
            tokio::task::spawn_blocking(move || StateStore::open(candidate, options)).await??;
        (directory, index)
    };
    if !restored {
        let scratch_directory = directory.with_extension("scratch");
        let scratch = StateStore::open(&scratch_directory, options(config))?;
        let maintenance = TableMaintenance::new(
            replacement.clone(),
            catalog.clone(),
            config.compaction.clone(),
            crate::services::writer_config(config),
        )?
        .with_read_limits(config.parquet_read.clone())?;
        for schema in &schemas {
            let table = targets
                .get(&schema.table_id)
                .context("missing target table")?;
            maintenance
                .rebuild_index(
                    table,
                    schema,
                    replacement.clone(),
                    scratch.clone(),
                    authority
                        .get(&schema.table_id)
                        .expect("validated above")
                        .materialized_lsn,
                    authority[&schema.table_id].schema_version,
                )
                .await?;
        }
        drop(maintenance);
        drop(scratch);
        std::fs::remove_dir_all(scratch_directory)?;
    }
    // An external rewrite can race a multi-table scan. Never select locations
    // from an older head and start emitting position deletes against the new one.
    for schema in &schemas {
        let table = targets
            .get(&schema.table_id)
            .context("missing target table")?;
        let head = catalog.load_table(table.identifier()).await?;
        ensure!(
            head.metadata().uuid() == table.metadata().uuid(),
            "target incarnation changed during index rebuild"
        );
        ensure!(
            same_iceberg_schema(head.metadata().current_schema(), &iceberg_schema(schema)?),
            "target schema changed during index rebuild"
        );
        if head.metadata().current_snapshot_id()
            != replacement.table_state(&schema.table_id)?.snapshot_id
        {
            return Err(ReplanRequired.into());
        }
    }
    let authority = control.clone();
    let active =
        tokio::task::spawn_blocking(move || authority.activate_rebuilt(&replacement)).await??;
    tracing::info!(
        event = "index_generation_activated", tables = schemas.len(), restored_checkpoint = restored,
        elapsed_ms = started.elapsed().as_millis() as u64, path = %active.path.display(),
        "recovered authoritative row index"
    );
    Ok(StateStore::open_with_control(
        active.path,
        options(config),
        control,
    )?)
}

fn candidate_directory(config: &Config) -> Result<PathBuf> {
    let parent = config.state_dir.join("index-generations");
    std::fs::create_dir_all(&parent)?;
    std::fs::File::open(&config.state_dir)?.sync_all()?;
    Ok(parent.join(format!("index-{}", uuid::Uuid::new_v4())))
}
