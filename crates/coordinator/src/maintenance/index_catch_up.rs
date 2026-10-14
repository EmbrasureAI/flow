//! Bring a restored index checkpoint forward to the current catalog head.
//!
//! A checkpoint index exactly describes the live rows of its table snapshot.
//! The current head differs by whole files: data files removed (compaction,
//! external rewrites), data files added, and position deletes added against
//! files that both snapshots share. Catch-up therefore removes every index
//! entry whose physical row is no longer live, then inserts the live rows of
//! added files, without rescanning unchanged data. Removals and insertions are
//! separate operations because a moved key may appear only once per operation.
//! Any disagreement with the index fails; the caller rebuilds from scratch.
use super::{TableMaintenance, index_rows, worker_runtime};
use crate::blocking;
use anyhow::{Context, Result, ensure};
use flow_iceberg_ext::SnapshotView;
use flow_model::{FileId, OperationId, PgLsn, TableSchema};
use flow_state_store::{IndexDelta, OperationKind, PreparedOperation, StateStore};
use iceberg::{spec::DataContentType, table::Table};
use std::collections::BTreeSet;

/// What catch-up changed, for diagnostics.
#[derive(Debug, Default, Clone, Copy)]
pub struct CatchUpStats {
    pub removed_files: usize,
    pub added_files: usize,
    pub changed_files: usize,
    pub removed_rows: u64,
    pub added_rows: u64,
}

fn data_files(view: &SnapshotView) -> BTreeSet<FileId> {
    view.live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect()
}

fn delete_files(view: &SnapshotView) -> BTreeSet<String> {
    view.live_files
        .values()
        .filter(|entry| entry.content_type() != DataContentType::Data)
        .map(|entry| flow_iceberg_ext::content_file_id(&entry.data_file))
        .collect()
}

impl TableMaintenance {
    /// Advance `index`, restored from a checkpoint of `checkpoint_snapshot`,
    /// to `head`. The caller validates `head` and the authority watermarks,
    /// and must discard `index` if this fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn catch_up_index(
        &self,
        head: &Table,
        schema: &TableSchema,
        index: StateStore,
        scratch: StateStore,
        checkpoint_snapshot: Option<i64>,
        materialized_lsn: PgLsn,
        materialized_schema_version: u32,
    ) -> Result<CatchUpStats> {
        let table_id = schema.table_id;
        let state = index.table_state(&table_id)?;
        ensure!(
            state.pending_operation.is_none() && state.snapshot_id == checkpoint_snapshot,
            "checkpoint index does not describe its recorded snapshot"
        );
        ensure!(
            state.materialized_lsn <= materialized_lsn,
            "checkpoint is ahead of durable authority"
        );
        let current = SnapshotView::current_with_cache(head, &self.cache).await?;
        ensure!(
            current
                .live_files
                .values()
                .all(|entry| entry.content_type() != DataContentType::EqualityDeletes),
            "index catch-up requires position-delete state"
        );
        let previous = match checkpoint_snapshot {
            Some(id) => SnapshotView::load_with_cache(head, id, &self.cache)
                .await
                .context("checkpoint snapshot is no longer retained")?,
            None => SnapshotView::default(),
        };
        let before = data_files(&previous);
        let after = data_files(&current);
        let removed: BTreeSet<_> = before.difference(&after).cloned().collect();
        let added: BTreeSet<_> = after.difference(&before).cloned().collect();
        let old_deletes = delete_files(&previous);
        // Shared files can only lose rows, through deletes the checkpoint lacked.
        let mut changed = BTreeSet::new();
        for file in before.intersection(&after) {
            if current.applicable_deletes(&file.0)?.iter().any(|entry| {
                !old_deletes.contains(&flow_iceberg_ext::content_file_id(&entry.data_file))
            }) {
                changed.insert(file.clone());
            }
        }
        let mut stats = CatchUpStats {
            removed_files: removed.len(),
            added_files: added.len(),
            changed_files: changed.len(),
            ..CatchUpStats::default()
        };
        let snapshot = head.metadata().current_snapshot();
        if removed.is_empty() && added.is_empty() && changed.is_empty() {
            ensure!(
                head.metadata().current_snapshot_id() == checkpoint_snapshot,
                "catalog head changed without changing files"
            );
            if state.materialized_lsn != materialized_lsn
                || state.schema_version != materialized_schema_version
            {
                blocking(move || {
                    index.complete_noop(
                        &table_id,
                        materialized_lsn,
                        materialized_schema_version,
                    )?;
                    Ok(())
                })
                .await?;
            }
            return Ok(stats);
        }
        let spec_id = head.metadata().default_partition_spec_id();
        let read_limits = self.read_limits.clone();
        let worker_table = head.clone();
        let worker_schema = schema.clone();
        let worker_index = index.clone();
        let base_sequence = match checkpoint_snapshot {
            Some(id) => head
                .metadata()
                .snapshot_by_id(id)
                .context("checkpoint snapshot is no longer retained")?
                .sequence_number(),
            None => 0,
        };
        let checkpoint_lsn = state.materialized_lsn;
        let checkpoint_schema = state.schema_version;
        let removal = OperationId(format!("catch-up-remove-{}", uuid::Uuid::new_v4()));
        let insertion = OperationId(format!("catch-up-insert-{}", uuid::Uuid::new_v4()));
        let staged = insertion.clone();
        let (removed_rows, added_rows) =
            tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
                let index = worker_index;
                let insertion = staged;
                let batch_rows = index.batch_rows();
                // 1. Remove entries whose rows are no longer live at the head.
                let mut removed_rows = 0u64;
                if let Some(base) = checkpoint_snapshot
                    && !(removed.is_empty() && changed.is_empty())
                {
                    index.begin_prepare(PreparedOperation {
                        id: removal.clone(),
                        table_id,
                        kind: OperationKind::Rebuild,
                        base_snapshot_id: checkpoint_snapshot,
                        last_lsn: checkpoint_lsn,
                        schema_version: checkpoint_schema,
                        artifacts: Vec::new(),
                        payload: Vec::new(),
                    })?;
                    let deletes_scan = format!("{}-deletes", removal.0);
                    worker_runtime()?.block_on(flow_compactor::stage_position_deletes(
                        &worker_table,
                        &worker_schema,
                        &current,
                        &changed,
                        &scratch,
                        &deletes_scan,
                        batch_rows,
                        &read_limits,
                    ))?;
                    let mut pending: Vec<(u64, flow_model::PrimaryKey)> = Vec::new();
                    let mut stage = |pending: &mut Vec<(u64, flow_model::PrimaryKey)>,
                                     file: &FileId|
                     -> Result<()> {
                        if pending.is_empty() {
                            return Ok(());
                        }
                        let keys: Vec<_> = pending.iter().map(|(_, key)| key.clone()).collect();
                        let locations = index.lookup_many(&table_id, &keys)?;
                        let mut deltas = Vec::with_capacity(keys.len());
                        for ((position, key), location) in pending.drain(..).zip(locations) {
                            let location = location.context("reverse index entry has no key")?;
                            ensure!(
                                location.data_file_id == *file && location.row_position == position,
                                "index directions disagree for a checkpoint row"
                            );
                            deltas.push(IndexDelta {
                                key,
                                expected: Some(location),
                                replacement: None,
                            });
                        }
                        removed_rows += deltas.len() as u64;
                        index.stage_deltas(&removal, deltas)?;
                        Ok(())
                    };
                    for file in &removed {
                        for row in index.file_rows(&table_id, file) {
                            pending.push(row?);
                            if pending.len() >= batch_rows {
                                stage(&mut pending, file)?;
                            }
                        }
                        stage(&mut pending, file)?;
                    }
                    for file in &changed {
                        let deleted = scratch
                            .file_position_deletes(&deletes_scan, &table_id, file)
                            .map(|location| location.map(|location| location.row_position))
                            .collect::<flow_state_store::Result<BTreeSet<_>>>()?;
                        for row in index.file_rows(&table_id, file) {
                            let (position, key) = row?;
                            if deleted.contains(&position) {
                                pending.push((position, key));
                                if pending.len() >= batch_rows {
                                    stage(&mut pending, file)?;
                                }
                            }
                        }
                        stage(&mut pending, file)?;
                    }
                    index.seal_prepare(&removal, Vec::new(), Vec::new())?;
                    index.mark_committed(&removal, base, base_sequence)?;
                    index.apply_committed(&removal)?;
                    index.forget_applied(&removal)?;
                }
                // 2. Insert the live rows of added files at the head snapshot.
                let state = index.table_state(&table_id)?;
                index.begin_prepare(PreparedOperation {
                    id: insertion.clone(),
                    table_id,
                    kind: OperationKind::Rebuild,
                    base_snapshot_id: state.snapshot_id,
                    last_lsn: materialized_lsn,
                    schema_version: materialized_schema_version,
                    artifacts: Vec::new(),
                    payload: Vec::new(),
                })?;
                let mut added_rows = 0u64;
                worker_runtime()?.block_on(flow_compactor::scan_live_files(
                    &worker_table,
                    &worker_schema,
                    &current,
                    &added,
                    &scratch,
                    &insertion.0,
                    batch_rows,
                    &read_limits,
                    async |batch| {
                        let deltas = index_rows(
                            &worker_schema,
                            &batch,
                            &current,
                            spec_id,
                            materialized_lsn,
                        )?;
                        added_rows += deltas.len() as u64;
                        index.stage_deltas(&insertion, deltas)?;
                        Ok(())
                    },
                ))?;
                index.seal_prepare(&insertion, Vec::new(), Vec::new())?;
                Ok((removed_rows, added_rows))
            })
            .await??;
        stats.removed_rows = removed_rows;
        stats.added_rows = added_rows;
        let (snapshot_id, sequence) = snapshot
            .map(|snapshot| (snapshot.snapshot_id(), snapshot.sequence_number()))
            .context("catch-up requires a current snapshot")?;
        blocking(move || {
            index.mark_committed(&insertion, snapshot_id, sequence)?;
            index.apply_committed(&insertion)?;
            index.forget_applied(&insertion)?;
            Ok(())
        })
        .await?;
        Ok(stats)
    }
}
