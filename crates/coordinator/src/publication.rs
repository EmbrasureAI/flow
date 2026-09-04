use crate::maintenance::artifacts::{MetadataRegistration, WriterRegistration};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{
    ArtifactTracker, CommitBase, ManifestCache, RowDeltaAction, find_operation, read_artifact_plan,
    write_artifact_plan,
};
use flow_ingress_journal::ChunkReader;
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig, WrittenBatch, iceberg_schema};
use flow_model::{
    Mutation, MutationKind, OperationId, PgLsn, PrimaryKey, SourceId, SourceTransaction, TableId,
    TableSchema,
};
use flow_state_store::{
    BufferResult, Change, CollapseBuffer, CollapsedMutation, ControlStore, IndexDelta, MemoryRows,
    OperationKind, OperationPhase, OperationRecord, PreparedOperation, StateStore, TableState,
};
use iceberg::{Catalog, spec::DataFile, table::Table};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// A complete source prefix for one table. Transactions are never split at a file boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Epoch {
    pub id: OperationId,
    pub source: SourceId,
    pub table: TableId,
    pub first_lsn: PgLsn,
    pub last_lsn: PgLsn,
    pub transaction_count: usize,
    pub initial_snapshot: bool,
    pub oldest_commit_timestamp_micros: i64,
}
impl Epoch {
    pub fn new(
        source: SourceId,
        table: TableId,
        transactions: &[SourceTransaction],
    ) -> Result<Self> {
        let first = transactions
            .first()
            .ok_or_else(|| anyhow::anyhow!("empty epoch"))?;
        let last = transactions.last().expect("nonempty above");
        ensure!(
            transactions
                .iter()
                .all(|t| t.source_id == source && t.affected_tables.contains(&table)),
            "epoch source/table mismatch"
        );
        ensure!(
            transactions.windows(2).all(|w| w[0].end_lsn < w[1].end_lsn),
            "epoch transactions must be strictly ordered"
        );
        Ok(Self {
            id: OperationId::epoch(&source, table, first.end_lsn, last.end_lsn),
            source,
            table,
            first_lsn: first.end_lsn,
            last_lsn: last.end_lsn,
            transaction_count: transactions.len(),
            initial_snapshot: transactions.iter().all(|transaction| transaction.xid == 0),
            oldest_commit_timestamp_micros: transactions
                .iter()
                .map(|transaction| transaction.commit_timestamp_micros)
                .min()
                .expect("nonempty epoch"),
        })
    }
}

/// Separate replay-batch and retained current-epoch memory budgets. Zero memory
/// selects the disk path. These limits do not include reader/writer native memory.
#[derive(Debug, Clone, Copy)]
pub struct CollapseLimits {
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub memory_bytes: usize,
}

/// A single collapsed epoch bound to the exact source schema and physical head.
/// Consume through `publish`; dropping it leaves the journal as replay authority.
/// Its private constructor prevents blessing rows collapsed against another head.
pub struct CollapsedEpoch {
    epoch: Epoch,
    schema: TableSchema,
    base: CommitBase,
    indexed: TableState,
    rows: CollapsedRows,
    mode: &'static str,
    memory_peak_bytes: Option<usize>,
}
impl CollapsedEpoch {
    pub fn epoch(&self) -> &Epoch {
        &self.epoch
    }

    /// The selected path and, for disk, why the buffer was bypassed or abandoned.
    pub fn mode(&self) -> &'static str {
        self.mode
    }

    /// Peak accepted retained buffer payload, excluding separately bounded
    /// decoding, binding batches and writer workspaces. Binding overflow returns
    /// None because its consumed partial buffer is not observed. Not process RSS.
    pub fn memory_peak_bytes(&self) -> Option<usize> {
        self.memory_peak_bytes
    }
}

enum CollapsedRows {
    Memory(MemoryRows),
    Disk,
}

/// Collapse the current immutable source prefix on a blocking table actor.
/// Capacity alone drops the ephemeral attempt and replays the complete prefix
/// through the existing disk map; validation and I/O failures never select fallback.
pub fn collapse_epoch(
    store: &StateStore,
    journal: &impl ChunkReader,
    table: &Table,
    schema: &TableSchema,
    epoch: &Epoch,
    transactions: &[SourceTransaction],
    limits: CollapseLimits,
) -> Result<CollapsedEpoch> {
    ensure!(
        limits.batch_rows > 1 && limits.batch_bytes > 0,
        "collapse batch must fit both halves of a primary-key change"
    );
    ensure!(
        limits.batch_rows <= store.batch_rows(),
        "collapse chunk exceeds state-store batch row budget"
    );
    ensure!(
        epoch == &Epoch::new(epoch.source.clone(), epoch.table, transactions)?,
        "epoch differs from its source transaction prefix"
    );
    ensure!(
        schema.table_id == epoch.table,
        "schema table differs from epoch"
    );
    ensure!(
        crate::same_iceberg_schema(table.metadata().current_schema(), &iceberg_schema(schema)?),
        "source schema does not match current Iceberg schema"
    );
    let base = CommitBase::new(table);
    base.validate(table)?;
    let indexed = store.table_state(&epoch.table)?;
    ensure!(
        indexed.pending_operation.is_none(),
        "recover pending publication before collapsing more source changes"
    );
    if indexed.snapshot_id != base.snapshot_id {
        return Err(ReplanRequired.into());
    }
    // Output position deletes use this prefix even when the input stays in memory.
    store.discard_transaction(&epoch.id.0)?;
    let source_bytes = transactions.iter().fold(0u64, |bytes, transaction| {
        bytes.saturating_add(transaction.mutation_chunks.payload_bytes())
    });
    // Match the current ordinary-prefix boundaries without changing scheduling.
    // Unknown/oversized single transactions never pay a speculative prefix replay.
    let mut mode = if limits.memory_bytes == 0 {
        "disk_disabled"
    } else if epoch.initial_snapshot {
        "disk_initial"
    } else if source_bytes > 32 << 20
        || transactions.iter().any(|transaction| {
            transaction
                .mutation_count(epoch.table)
                .is_none_or(|rows| rows > 10_000)
        })
    {
        "disk_oversize"
    } else {
        "memory"
    };
    let mut memory_peak_bytes = Some(0);
    let rows = if mode == "memory" {
        let mut buffer = CollapseBuffer::new(epoch.table, limits.memory_bytes);
        let folded = replay_epoch(
            store,
            journal,
            schema,
            epoch,
            transactions,
            limits,
            |changes| {
                for (_, key, change) in changes.drain(..) {
                    if let BufferResult::CapacityExceeded = buffer.push(key, change)? {
                        return Ok(BufferResult::CapacityExceeded);
                    }
                }
                Ok(BufferResult::Ready(()))
            },
        )?;
        memory_peak_bytes = Some(buffer.peak_accounted_bytes());
        match folded {
            BufferResult::Ready(()) => match store.bind_collapse_buffer(buffer, &indexed)? {
                BufferResult::Ready(rows) => {
                    memory_peak_bytes = Some(rows.peak_accounted_bytes());
                    Some(CollapsedRows::Memory(rows))
                }
                BufferResult::CapacityExceeded => {
                    memory_peak_bytes = None;
                    None
                }
            },
            BufferResult::CapacityExceeded => {
                drop(buffer);
                None
            }
        }
    } else {
        None
    };
    let rows = match rows {
        Some(rows) => rows,
        None => {
            if mode == "memory" {
                mode = "disk_capacity";
            }
            // A new cursor, ordinal, projection cache and empty input map are used.
            // No memory binding/snapshot survives into this full disk replay.
            replay_epoch(
                store,
                journal,
                schema,
                epoch,
                transactions,
                limits,
                |changes| {
                    store.collapse_changes(&epoch.id.0, changes.drain(..))?;
                    Ok(BufferResult::Ready(()))
                },
            )?;
            store.seal_transaction(&epoch.id.0)?;
            CollapsedRows::Disk
        }
    };
    if store.table_state(&epoch.table)? != indexed {
        return Err(ReplanRequired.into());
    }
    Ok(CollapsedEpoch {
        epoch: epoch.clone(),
        schema: schema.clone(),
        base,
        indexed,
        rows,
        mode,
        memory_peak_bytes,
    })
}

fn replay_epoch(
    store: &StateStore,
    journal: &impl ChunkReader,
    schema: &TableSchema,
    epoch: &Epoch,
    transactions: &[SourceTransaction],
    limits: CollapseLimits,
    mut consume: impl FnMut(&mut Vec<(TableId, PrimaryKey, Change)>) -> Result<BufferResult<()>>,
) -> Result<BufferResult<()>> {
    let mut changes = Vec::with_capacity(limits.batch_rows);
    let mut buffered_bytes = 0usize;
    let mut ordinal = 0u64;
    let mut historical_schema: Option<TableSchema> = None;
    let mut replay = journal.replay_cursor();
    for txn in transactions {
        for bytes in replay.chunks(&txn.mutation_chunks)? {
            let mutations: Vec<Mutation> = bincode::deserialize(&bytes?)?;
            for mutation in mutations {
                if mutation.table_id != epoch.table {
                    continue;
                }
                ensure!(
                    mutation.schema_version <= schema.version,
                    "mutation requires a newer publication schema"
                );
                let source_schema = if mutation.schema_version == schema.version {
                    None
                } else {
                    if historical_schema
                        .as_ref()
                        .is_none_or(|old| old.version != mutation.schema_version)
                    {
                        let old = crate::load_source_schema(
                            store,
                            &txn.source_id,
                            mutation.table_id,
                            mutation.schema_version,
                        )?;
                        old.validate_successor(schema)?;
                        historical_schema = Some(old);
                    }
                    historical_schema.as_ref()
                };
                let project = |row| -> Result<_> {
                    match &source_schema {
                        Some(source) => Ok(source.project_row(row, schema)?),
                        None => {
                            schema.validate_row(&row)?;
                            Ok(row)
                        }
                    }
                };
                let (first, second) = match mutation.kind {
                    MutationKind::Insert { row } => {
                        let row = project(row)?;
                        let key = if schema.primary_key.is_empty() {
                            ensure!(schema.append_only, "mutable table has no key");
                            let mut key = epoch.id.0.as_bytes().to_vec();
                            key.extend(ordinal.to_be_bytes());
                            PrimaryKey(key)
                        } else {
                            schema.encode_key(&row)?
                        };
                        ((key, Change::Insert(row)), None)
                    }
                    MutationKind::Update { old_key, row } => {
                        let row = project(row)?;
                        ensure!(!schema.append_only, "update on append-only table");
                        let key = schema.encode_key(&row)?;
                        if key == old_key {
                            ((key, Change::Update(row)), None)
                        } else {
                            ((old_key, Change::Delete), Some((key, Change::Insert(row))))
                        }
                    }
                    MutationKind::Delete { key } => {
                        ensure!(!schema.append_only, "delete on append-only table");
                        ((key, Change::Delete), None)
                    }
                };
                let size = change_bytes(&first)?
                    .saturating_add(second.as_ref().map(change_bytes).transpose()?.unwrap_or(0));
                ensure!(
                    size <= limits.batch_bytes,
                    "individual mutation exceeds collapse byte limit"
                );
                let count = 1 + usize::from(second.is_some());
                if !changes.is_empty()
                    && (changes.len() + count > limits.batch_rows
                        || buffered_bytes.saturating_add(size) > limits.batch_bytes)
                {
                    if let BufferResult::CapacityExceeded = consume(&mut changes)? {
                        return Ok(BufferResult::CapacityExceeded);
                    }
                    buffered_bytes = 0;
                }
                changes.push((epoch.table, first.0, first.1));
                if let Some((key, change)) = second {
                    changes.push((epoch.table, key, change));
                }
                buffered_bytes += size;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("mutation ordinal overflow"))?;
            }
        }
    }
    if !changes.is_empty() {
        return consume(&mut changes);
    }
    Ok(BufferResult::Ready(()))
}

fn change_bytes((key, change): &(PrimaryKey, Change)) -> Result<usize> {
    let row_bytes = match change {
        Change::Insert(row) | Change::Update(row) => bincode::serialized_size(row)? as usize,
        Change::Delete => 0,
    };
    Ok(std::mem::size_of::<(TableId, PrimaryKey, Change)>()
        .saturating_add(key.0.len())
        .saturating_add(row_bytes))
}

#[derive(Serialize, Deserialize)]
struct RecoveryPlan {
    base: CommitBase,
    manifest_list: String,
    referenced_data: BTreeSet<String>,
    properties: HashMap<String, String>,
}

/// Resolve a persisted catalog action without relying on the replaceable index.
/// `None` proves the operation cannot later publish against its exact base; an
/// ambiguous response at the unchanged base remains an error and retains its fence.
pub async fn resolve_catalog_operation(
    record: &OperationRecord,
    catalog: &dyn Catalog,
    table: &Table,
    control: &ControlStore,
) -> Result<Option<(i64, i64)>> {
    let tracker = Some(MetadataRegistration::recovery(
        control.clone(),
        table,
        record.operation.table_id,
        record.operation.id.clone(),
    )?);
    match record.operation.kind {
        OperationKind::Ingest => Ok(resolve_ingest_catalog_operation(
            record,
            catalog,
            table,
            ManifestCache::default(),
            tracker,
        )
        .await?
        .map(|(snapshot, sequence, _)| (snapshot, sequence))),
        OperationKind::Rewrite | OperationKind::Reconcile | OperationKind::ManifestRewrite => {
            crate::maintenance::resolve_maintenance_catalog_operation(
                record, catalog, table, tracker,
            )
            .await
        }
        OperationKind::Rebuild => {
            anyhow::bail!("offline index rebuild cannot own a catalog publication")
        }
    }
}

async fn resolve_ingest_catalog_operation(
    record: &OperationRecord,
    catalog: &dyn Catalog,
    table: &Table,
    cache: ManifestCache,
    tracker: Option<Arc<dyn ArtifactTracker>>,
) -> Result<Option<(i64, i64, bool)>> {
    if record.phase == OperationPhase::Building {
        return Ok(None);
    }
    let plan: RecoveryPlan = serde_json::from_slice(&record.operation.payload)?;
    let head = catalog.load_table(table.identifier()).await?;
    ensure!(
        head.metadata().uuid() == plan.base.uuid,
        "table incarnation changed during recovery"
    );
    if let Some(snapshot) = find_operation(head.metadata(), &record.operation.id.0) {
        ensure!(
            record
                .snapshot_id
                .is_none_or(|id| id == snapshot.snapshot_id())
                && record
                    .sequence_number
                    .is_none_or(|sequence| sequence == snapshot.sequence_number()),
            "catalog operation marker conflicts with durable commit proof"
        );
        return Ok(Some((
            snapshot.snapshot_id(),
            snapshot.sequence_number(),
            true,
        )));
    }
    ensure!(
        !matches!(
            record.phase,
            OperationPhase::Committed | OperationPhase::Applied
        ),
        "proven committed operation is absent from retained current-branch history"
    );
    plan.base.validate(&head)?;
    if head.metadata().current_snapshot_id() != plan.base.snapshot_id {
        return Ok(None);
    }
    let files = read_artifact_plan(head.file_io(), &plan.manifest_list).await?;
    let outcome = RowDeltaAction::from_base(plan.base.clone(), record.operation.id.0.clone())
        .require_base_snapshot()
        .add_data_files(files.added_data)
        .add_delete_files(files.added_deletes)
        .validate_data_files_exist(plan.referenced_data)
        .with_properties(plan.properties)
        .with_manifest_cache(cache)
        .with_artifact_tracker(tracker)
        .commit(catalog, &head)
        .await;
    match outcome {
        Ok(committed) => Ok(Some((
            committed.snapshot_id,
            committed.sequence_number,
            committed.already_committed,
        ))),
        Err(error) => {
            let refreshed = catalog.load_table(table.identifier()).await?;
            ensure!(
                refreshed.metadata().uuid() == plan.base.uuid,
                "table incarnation changed during recovery"
            );
            if let Some(snapshot) = find_operation(refreshed.metadata(), &record.operation.id.0) {
                return Ok(Some((
                    snapshot.snapshot_id(),
                    snapshot.sequence_number(),
                    true,
                )));
            }
            plan.base.validate(&refreshed)?;
            if refreshed.metadata().current_snapshot_id() != plan.base.snapshot_id {
                Ok(None)
            } else {
                Err(error.into())
            }
        }
    }
}

/// The catalog advanced before publication. Reconcile the index and collapse
/// the original journal transactions again before creating another attempt.
#[derive(Debug, thiserror::Error)]
#[error("table changed during publication; reconcile and replan from the journal")]
pub struct ReplanRequired;

/// One owner per table. Persistent table fencing survives loss of this actor.
pub struct TablePublisher {
    store: StateStore,
    catalog: Arc<dyn Catalog>,
    writer: WriterConfig,
    batch_rows: usize,
    batch_bytes: usize,
    manifest_cache: ManifestCache,
}
impl TablePublisher {
    pub fn new(
        store: StateStore,
        catalog: Arc<dyn Catalog>,
        writer: WriterConfig,
        batch_rows: usize,
        batch_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            batch_rows > 0 && batch_bytes > 0,
            "materializer batch limits must be positive"
        );
        ensure!(
            batch_rows <= store.batch_rows(),
            "materializer batch exceeds state-store batch row budget"
        );
        Ok(Self {
            store,
            catalog,
            writer,
            batch_rows,
            batch_bytes,
            manifest_cache: ManifestCache::default(),
        })
    }
    /// Builds artifacts, durably seals their plan, then commits. Ambiguous
    /// failures retain the fence; a proven stale attempt returns ReplanRequired.
    pub async fn publish(
        &self,
        table: &Table,
        schema: &TableSchema,
        collapsed: CollapsedEpoch,
    ) -> Result<Option<i64>> {
        let CollapsedEpoch {
            epoch,
            schema: captured_schema,
            base,
            indexed: captured_state,
            rows,
            ..
        } = collapsed;
        let epoch = &epoch;
        let prepare_started = Instant::now();
        let mut data_write = Duration::ZERO;
        let mut index_delete_stage = Duration::ZERO;
        let mut data_stage_pair_wall = Duration::ZERO;
        let mut data_stage_overlap = Duration::ZERO;
        let mut data_stage_other = Duration::ZERO;
        let mut delete_write = Duration::ZERO;
        let table = self.catalog.load_table(table.identifier()).await?;
        if schema != &captured_schema
            || base.uuid != table.metadata().uuid()
            || base.snapshot_id != table.metadata().current_snapshot_id()
            || base.sequence_number
                != table
                    .metadata()
                    .current_snapshot()
                    .map_or(0, |snapshot| snapshot.sequence_number())
            || base.schema_id != table.metadata().current_schema_id()
            || base.spec_id != table.metadata().default_partition_spec_id()
        {
            return Err(ReplanRequired.into());
        }
        base.validate(&table)?;
        ensure!(
            crate::same_iceberg_schema(table.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema does not match current Iceberg schema"
        );
        let store = self.store.clone();
        let id = epoch.table;
        let indexed = blocking(move || Ok(store.table_state(&id)?)).await?;
        if indexed != captured_state {
            return Err(ReplanRequired.into());
        }
        let mut batches = match rows {
            CollapsedRows::Disk => CollapsedBatches::Disk(self.collapsed_batches(epoch)),
            CollapsedRows::Memory(rows) => CollapsedBatches::Memory {
                rows: Box::new(rows.peekable()),
                max_rows: self.batch_rows,
                max_bytes: self.batch_bytes,
            },
        };
        let Some(first) = batches.next().await? else {
            let store = self.store.clone();
            let epoch = epoch.clone();
            let version = schema.version;
            blocking(move || {
                store.complete_noop(&epoch.table, epoch.last_lsn, version)?;
                Ok(())
            })
            .await?;
            return Ok(table.metadata().current_snapshot_id());
        };
        // Logical epoch identity is stable; physical attempts never overwrite old files.
        let level = if epoch.initial_snapshot { "l1" } else { "l0" };
        let attempt = OperationId(format!("flow-{level}-{}", uuid::Uuid::new_v4()));
        let ownership = WriterRegistration::new(
            self.store.clone(),
            &table,
            epoch.table,
            epoch.id.clone(),
            &attempt,
        )?;
        let reservation = ownership.record().await?;
        let mut writer_config = self.writer.clone();
        writer_config.artifact_tracker = Some(ownership.clone());
        let operation = PreparedOperation {
            id: epoch.id.clone(),
            table_id: epoch.table,
            kind: OperationKind::Ingest,
            base_snapshot_id: indexed.snapshot_id,
            last_lsn: epoch.last_lsn,
            schema_version: schema.version,
            artifacts: vec![],
            payload: vec![],
        };
        let store = self.store.clone();
        blocking(move || {
            store.begin_prepare_with_record(operation, Some((&reservation.0, &reservation.1)))?;
            Ok(())
        })
        .await?;
        let spec_id = table.metadata().default_partition_spec_id();
        let mut data_writer = Some(DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &attempt,
            schema.clone(),
            spec_id,
            writer_config.clone(),
        )?);
        let mut referenced = BTreeSet::new();
        let mut first = Some(first);
        let mut stage = None;
        let data_files = loop {
            let next = async {
                let mutations = match first.take() {
                    Some(first) => Some(first),
                    None => batches.next().await?,
                };
                let Some(mutations) = mutations else {
                    let started = Instant::now();
                    let files = data_writer
                        .take()
                        .expect("writer closes once")
                        .close()
                        .await?;
                    return Ok((DataOutput::Complete(files), started, Instant::now()));
                };
                let rows = mutations
                    .iter()
                    .filter_map(|m| m.row.as_ref())
                    .collect::<Vec<_>>();
                let started = Instant::now();
                let written = data_writer
                    .as_mut()
                    .expect("writer is open")
                    .write(&rows, epoch.last_lsn)
                    .await?;
                Ok((
                    DataOutput::Batch(mutations, written),
                    started,
                    Instant::now(),
                ))
            };
            // The previous stage was already spawned, even if the next writer
            // call never yields. Receiver, write and close failures all join it.
            let (output, timing) = join_data_stage(stage.take(), next).await?;
            data_write += timing.data;
            index_delete_stage += timing.stage;
            data_stage_pair_wall += timing.wall;
            data_stage_overlap += timing.overlap;
            data_stage_other += timing.other;
            let (mutations, written) = match output {
                DataOutput::Batch(mutations, written) => (mutations, written),
                DataOutput::Complete(files) => break files,
            };
            let mut locations = written.locations.into_iter();
            let mut deltas = Vec::with_capacity(mutations.len());
            let mut deletes = Vec::new();
            for mutation in mutations {
                if let Some(old) = &mutation.original {
                    if !referenced.contains(&old.data_file_id.0) {
                        referenced.insert(old.data_file_id.0.clone());
                    }
                    deletes.push(old.clone());
                }
                let replacement = if mutation.row.is_some() {
                    let mut location = locations
                        .next()
                        .expect("writer returns one location per row");
                    location.row_version = mutation
                        .original
                        .as_ref()
                        .map_or(Some(1), |old| old.row_version.checked_add(1))
                        .ok_or_else(|| anyhow::anyhow!("row version overflow"))?;
                    Some(location)
                } else {
                    None
                };
                deltas.push(IndexDelta {
                    key: mutation.key,
                    expected: mutation.original,
                    replacement,
                });
            }
            let store = self.store.clone();
            let epoch = epoch.clone();
            stage = Some(DataStage::spawn(move || {
                store.stage_deltas(&epoch.id, deltas)?;
                store.put_position_deletes(&epoch.id.0, &epoch.table, deletes)?;
                Ok(())
            }));
        };
        let mut delete_writer = DeleteWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &attempt,
            spec_id,
            writer_config.clone(),
        )?;
        let mut deletes = self.delete_batches(epoch);
        while let Some(batch) = deletes.recv().await.transpose()? {
            let write_started = Instant::now();
            delete_writer.write(&batch).await?;
            delete_write += write_started.elapsed();
        }
        let write_started = Instant::now();
        let delete_files = delete_writer.close().await?;
        delete_write += write_started.elapsed();
        for (kind, files) in [("data", &data_files), ("delete", &delete_files)] {
            metrics::counter!("flow_artifact_files_written_total", "table_id" => epoch.table.0.to_string(), "kind" => kind).increment(files.len() as u64);
            metrics::counter!("flow_artifact_bytes_written_total", "table_id" => epoch.table.0.to_string(), "kind" => kind)
                .increment(files.iter().map(|file| file.file_size_in_bytes()).sum());
        }
        let path = format!(
            "{}/metadata/{}-prepared.avro",
            table.metadata().location().trim_end_matches('/'),
            attempt.0
        );
        write_artifact_plan(&table, &path, &data_files, &delete_files).await?;
        let properties = HashMap::from([
            ("streaming.writer-id".into(), "embrasure-flow".into()),
            ("streaming.source-id".into(), epoch.source.0.clone()),
            ("streaming.operation".into(), "ingest".into()),
            ("streaming.level".into(), level.into()),
            ("streaming.epoch-id".into(), epoch.id.0.clone()),
            ("streaming.first-lsn".into(), epoch.first_lsn.to_string()),
            ("streaming.last-lsn".into(), epoch.last_lsn.to_string()),
            (
                "streaming.oldest-commit-micros".into(),
                epoch.oldest_commit_timestamp_micros.to_string(),
            ),
            (
                "streaming.initial-snapshot".into(),
                epoch.initial_snapshot.to_string(),
            ),
            (
                "streaming.transaction-count".into(),
                epoch.transaction_count.to_string(),
            ),
            (
                "streaming.schema-version".into(),
                schema.version.to_string(),
            ),
        ]);
        let plan = RecoveryPlan {
            base: CommitBase::new(&table),
            manifest_list: path.clone(),
            referenced_data: referenced,
            properties,
        };
        let artifacts = data_files
            .iter()
            .chain(&delete_files)
            .map(|f| f.file_path().to_owned())
            .chain(std::iter::once(path))
            .collect();
        let payload = serde_json::to_vec(&plan)?;
        let reservation = ownership
            .finish(data_files.len(), delete_files.len())
            .await?;
        let store = self.store.clone();
        let op = epoch.id.clone();
        blocking(move || {
            store.seal_prepare_with_record(
                &op,
                artifacts,
                payload,
                Some((&reservation.0, &reservation.1)),
            )?;
            Ok(())
        })
        .await?;
        let preparation = prepare_started.elapsed();
        let precommit_prepare_ms = preparation.as_secs_f64() * 1000.0;
        let other_prepare = preparation.saturating_sub(data_stage_pair_wall + delete_write);
        metrics::histogram!("flow_ingest_prepare_seconds", "table_id" => epoch.table.0.to_string())
            .record(precommit_prepare_ms / 1000.0);
        tracing::info!(
            event = "ingest_prepared",
            operation_id = %epoch.id.0,
            table_id = epoch.table.0,
            precommit_prepare_ms,
            data_write_ms = data_write.as_secs_f64() * 1000.0,
            index_delete_stage_ms = index_delete_stage.as_secs_f64() * 1000.0,
            data_stage_pair_wall_ms = data_stage_pair_wall.as_secs_f64() * 1000.0,
            data_stage_overlap_ms = data_stage_overlap.as_secs_f64() * 1000.0,
            data_stage_other_ms = data_stage_other.as_secs_f64() * 1000.0,
            delete_write_ms = delete_write.as_secs_f64() * 1000.0,
            other_prepare_ms = other_prepare.as_secs_f64() * 1000.0,
            "ingest artifacts and index deltas prepared"
        );
        self.recover(&table, &epoch.id).await
    }

    /// Resume Prepared/Committed operations. Building outputs were never eligible
    /// for publication and require deliberate discard/rebuild with new artifact IDs.
    pub async fn recover(&self, table: &Table, id: &OperationId) -> Result<Option<i64>> {
        let store = self.store.clone();
        let op = id.clone();
        let record = blocking(move || Ok(store.operation(&op)?))
            .await?
            .ok_or_else(|| anyhow::anyhow!("unknown prepared operation"))?;
        match record.phase {
            OperationPhase::Building => anyhow::bail!(
                "incomplete artifact build {}; discard its uncommitted state and regenerate with a fresh attempt ID",
                id.0
            ),
            OperationPhase::Applied => return Ok(record.snapshot_id),
            OperationPhase::Committed => {}
            OperationPhase::Prepared => {
                ensure!(
                    record.operation.kind == OperationKind::Ingest,
                    "rewrite recovery requires the rewrite coordinator"
                );
                let plan: RecoveryPlan = serde_json::from_slice(&record.operation.payload)?;
                let first_lsn = plan
                    .properties
                    .get("streaming.first-lsn")
                    .cloned()
                    .unwrap_or_default();
                let commit_started = Instant::now();
                metrics::counter!("flow_catalog_commit_attempts_total", "table_id" => record.operation.table_id.0.to_string()).increment(1);
                let Some((snapshot, sequence, already_committed)) =
                    resolve_ingest_catalog_operation(
                        &record,
                        self.catalog.as_ref(),
                        table,
                        self.manifest_cache.clone(),
                        Some(MetadataRegistration::live(
                            self.store.clone(),
                            table,
                            record.operation.table_id,
                            record.operation.id.clone(),
                        )?),
                    )
                    .await?
                else {
                    let store = self.store.clone();
                    let op = id.clone();
                    blocking(move || {
                        store.discard_uncommitted(&op)?;
                        Ok(())
                    })
                    .await?;
                    return Err(ReplanRequired.into());
                };
                // Observe catalog visibility separately from the durable index
                // application below. Recovered markers are not latency samples.
                let catalog_committed_at_micros = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                let table_label = record.operation.table_id.0.to_string();
                if already_committed {
                    metrics::counter!("flow_catalog_recovered_commits_total", "table_id" => table_label.clone()).increment(1);
                } else {
                    metrics::histogram!("flow_catalog_commit_seconds", "table_id" => table_label.clone()).record(commit_started.elapsed().as_secs_f64());
                    metrics::counter!("flow_catalog_commits_total", "table_id" => table_label.clone()).increment(1);
                    if plan
                        .properties
                        .get("streaming.initial-snapshot")
                        .is_some_and(|value| value == "false")
                        && let Some(committed) = plan
                            .properties
                            .get("streaming.oldest-commit-micros")
                            .and_then(|value| value.parse::<u64>().ok())
                        && catalog_committed_at_micros >= committed
                    {
                        metrics::histogram!("flow_epoch_oldest_commit_to_catalog_seconds", "table_id" => table_label)
                            .record((catalog_committed_at_micros - committed) as f64 / 1_000_000.0);
                    }
                }
                tracing::info!(
                    event = "table_published",
                    operation_id = %id.0,
                    table_id = record.operation.table_id.0,
                    first_lsn,
                    last_lsn = %record.operation.last_lsn,
                    catalog_committed_at_micros,
                    already_committed,
                    "table epoch published"
                );
                let store = self.store.clone();
                let op = id.clone();
                blocking(move || {
                    store.mark_committed(&op, snapshot, sequence)?;
                    Ok(())
                })
                .await?;
            }
        }
        let apply_started = Instant::now();
        let table_label = record.operation.table_id.0.to_string();
        let store = self.store.clone();
        let op = id.clone();
        let snapshot = blocking(move || {
            store.apply_committed(&op)?;
            Ok(store.operation(&op)?.and_then(|o| o.snapshot_id))
        })
        .await?;
        metrics::histogram!("flow_index_apply_seconds", "table_id" => table_label)
            .record(apply_started.elapsed().as_secs_f64());
        Ok(snapshot)
    }

    fn collapsed_batches(
        &self,
        epoch: &Epoch,
    ) -> tokio::sync::mpsc::Receiver<Result<Vec<CollapsedMutation>>> {
        let store = self.store.clone();
        let epoch = epoch.clone();
        let max_rows = self.batch_rows;
        let max_bytes = self.batch_bytes;
        let (send, receive) = tokio::sync::mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<()> {
                let mut batch = Vec::with_capacity(max_rows);
                let mut bytes = 0usize;
                for mutation in store.collapsed(&epoch.id.0, &epoch.table)? {
                    let mutation = mutation?;
                    let size = bincode::serialized_size(&mutation)? as usize;
                    if !batch.is_empty()
                        && (batch.len() >= max_rows || bytes.saturating_add(size) > max_bytes)
                    {
                        if send
                            .blocking_send(Ok(std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(max_rows),
                            )))
                            .is_err()
                        {
                            return Ok(());
                        }
                        bytes = 0;
                    }
                    ensure!(
                        size <= max_bytes,
                        "individual decoded row exceeds materializer batch byte limit"
                    );
                    bytes += size;
                    batch.push(mutation);
                }
                if !batch.is_empty() {
                    let _ = send.blocking_send(Ok(batch));
                }
                Ok(())
            })();
            if let Err(error) = result {
                let _ = send.blocking_send(Err(error));
            }
        });
        receive
    }
    fn delete_batches(
        &self,
        epoch: &Epoch,
    ) -> tokio::sync::mpsc::Receiver<Result<Vec<flow_model::RowLocation>>> {
        let store = self.store.clone();
        let epoch = epoch.clone();
        let max = self.batch_rows;
        let (send, receive) = tokio::sync::mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<()> {
                let mut batch = Vec::with_capacity(max);
                for location in store.position_deletes(&epoch.id.0, &epoch.table) {
                    batch.push(location?);
                    if batch.len() == max
                        && send
                            .blocking_send(Ok(std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(max),
                            )))
                            .is_err()
                    {
                        return Ok(());
                    }
                }
                if !batch.is_empty() {
                    let _ = send.blocking_send(Ok(batch));
                }
                Ok(())
            })();
            if let Err(error) = result {
                let _ = send.blocking_send(Err(error));
            }
        });
        receive
    }
}
// Memory input is consumed directly. In particular it does not start the disk
// producer or layer that producer's two queued row batches over the whole map.
enum CollapsedBatches {
    Disk(tokio::sync::mpsc::Receiver<Result<Vec<CollapsedMutation>>>),
    Memory {
        rows: Box<std::iter::Peekable<MemoryRows>>,
        max_rows: usize,
        max_bytes: usize,
    },
}
impl CollapsedBatches {
    async fn next(&mut self) -> Result<Option<Vec<CollapsedMutation>>> {
        match self {
            Self::Disk(receive) => receive.recv().await.transpose(),
            Self::Memory {
                rows,
                max_rows,
                max_bytes,
            } => {
                let mut batch = Vec::with_capacity(*max_rows);
                let mut bytes = 0usize;
                while let Some(mutation) = rows.peek() {
                    let size = bincode::serialized_size(mutation)? as usize;
                    ensure!(
                        size <= *max_bytes,
                        "individual decoded row exceeds materializer batch byte limit"
                    );
                    if !batch.is_empty()
                        && (batch.len() >= *max_rows || bytes.saturating_add(size) > *max_bytes)
                    {
                        break;
                    }
                    bytes += size;
                    batch.push(rows.next().expect("peeked row exists"));
                }
                Ok((!batch.is_empty()).then_some(batch))
            }
        }
    }
}

enum DataOutput {
    Batch(Vec<CollapsedMutation>, WrittenBatch),
    Complete(Vec<DataFile>),
}

struct DataStage {
    spawned: Instant,
    task: tokio::task::JoinHandle<Result<(Instant, Instant)>>,
}
impl DataStage {
    fn spawn(work: impl FnOnce() -> Result<()> + Send + 'static) -> Self {
        let spawned = Instant::now();
        let task = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            work()?;
            Ok((started, Instant::now()))
        });
        Self { spawned, task }
    }
}

struct DataStageTiming {
    wall: Duration,
    data: Duration,
    stage: Duration,
    overlap: Duration,
    other: Duration,
}

/// Await both sides before propagating either error. Dropping an unfinished
/// blocking task does not stop its writes, so a short-circuiting join would let
/// the caller discard/retry Building while its previous stage still runs.
async fn join_data_stage<T>(
    stage: Option<DataStage>,
    data: impl Future<Output = Result<(T, Instant, Instant)>>,
) -> Result<(T, DataStageTiming)> {
    let started = stage
        .as_ref()
        .map_or_else(Instant::now, |stage| stage.spawned);
    let staged = async {
        match stage {
            Some(stage) => Ok::<_, anyhow::Error>(Some(stage.task.await??)),
            None => Ok(None),
        }
    };
    let (staged, data) = tokio::join!(staged, data);
    let wall = started.elapsed();
    let staged = staged?;
    let (value, data_start, data_end) = data?;
    let data = data_end.duration_since(data_start);
    let (stage, overlap) = staged.map_or((Duration::ZERO, Duration::ZERO), |(start, end)| {
        (
            end.duration_since(start),
            end.min(data_end)
                .saturating_duration_since(start.max(data_start)),
        )
    });
    Ok((
        value,
        DataStageTiming {
            wall,
            data,
            stage,
            overlap,
            other: wall.saturating_sub(data + stage - overlap),
        },
    ))
}

pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f).await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::{Column, ColumnType, Value};
    use flow_state_store::StateStoreOptions;
    use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_data_write_joins_held_index_stage_before_building_can_be_discarded() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("index");
        let store = StateStore::open(&path, StateStoreOptions::default()).unwrap();
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
        let id = OperationId("held-ingest-stage".into());
        store
            .begin_prepare(PreparedOperation {
                id: id.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: schema.version,
                artifacts: vec![],
                payload: vec![],
            })
            .unwrap();
        let mut writer = DataWriter::new(
            FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build(),
            temp.path().to_str().unwrap(),
            &id,
            schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        let row = vec![Value::Int64(1)];
        let written = writer.write(&[&row], PgLsn(10)).await.unwrap();
        let key = schema.encode_key(&row).unwrap();
        let delta = IndexDelta {
            key: key.clone(),
            expected: None,
            replacement: written.locations.into_iter().next(),
        };
        let (entered_send, entered) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let (finished_send, finished) = tokio::sync::oneshot::channel();
        let staged_store = store.clone();
        let staged_id = id.clone();
        let stage = DataStage::spawn(move || {
            let result = staged_store.stage_deltas_fallible(
                &staged_id,
                std::iter::once_with(move || {
                    let _ = entered_send.send(());
                    held.recv_timeout(Duration::from_secs(15))
                        .map_err(|error| {
                            flow_state_store::Error::InvalidState(error.to_string())
                        })?;
                    Ok(delta)
                }),
            );
            drop(staged_store);
            let _ = finished_send.send(());
            Ok(result?)
        });
        let (failed_send, failed) = tokio::sync::oneshot::channel();
        let mut pair = tokio::spawn(async move {
            let result = join_data_stage(Some(stage), async {
                let started = Instant::now();
                let result = writer.write(&[vec![Value::Null]], PgLsn(10)).await;
                let _ = failed_send.send(result.is_err());
                result.map(|written| (written, started, Instant::now()))
            })
            .await;
            (result, writer)
        });
        let entered = tokio::time::timeout(Duration::from_secs(5), entered).await;
        let failed = tokio::time::timeout(Duration::from_secs(5), failed).await;
        // Give the join task time to propagate the writer error if it incorrectly
        // short-circuits. Release and finish the held worker before any assertion.
        let early = tokio::time::timeout(Duration::from_millis(50), &mut pair).await;
        let returned_early = early.is_ok();
        let _ = release.send(());
        let joined = match early {
            Ok(joined) => joined,
            Err(_) => pair.await,
        };
        let finished = tokio::time::timeout(Duration::from_secs(5), finished).await;
        assert!(matches!(entered, Ok(Ok(()))));
        assert!(matches!(failed, Ok(Ok(true))));
        assert!(matches!(finished, Ok(Ok(()))));
        assert!(
            !returned_early,
            "writer failure detached the held index stage"
        );
        let (result, writer) = joined.unwrap();
        assert!(result.is_err());
        let files = writer.close().await.unwrap();
        assert_eq!(files.len(), 1);
        let bytes = std::fs::read(files[0].file_path()).unwrap();
        assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
        drop(store);

        let store = StateStore::open(&path, StateStoreOptions::default()).unwrap();
        let record = store.operation(&id).unwrap().unwrap();
        assert_eq!(record.phase, OperationPhase::Building);
        assert_eq!(record.delta_count, 1);
        assert!(store.lookup(&schema.table_id, &key).unwrap().is_none());
        store.discard_uncommitted(&id).unwrap();
        assert!(
            store
                .table_state(&schema.table_id)
                .unwrap()
                .pending_operation
                .is_none()
        );
    }
}
