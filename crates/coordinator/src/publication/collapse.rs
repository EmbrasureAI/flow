//! Replay complete journal prefixes into rows bound to an exact table head.
//!
//! [`collapse_epoch`] validates the source schema and index before producing a
//! [`CollapsedEpoch`]; capacity exhaustion alone permits a full replay on disk.

use super::ReplanRequired;
use anyhow::{Result, ensure};
use bincode::Options;
use flow_iceberg_ext::CommitBase;
use flow_ingress_journal::ChunkReader;
use flow_materializer::iceberg_schema;
use flow_model::{
    Mutation, MutationKind, OperationId, PgLsn, PrimaryKey, SourceId, SourceTransaction, TableId,
    TableSchema,
};
use flow_state_store::{BufferResult, Change, CollapseBuffer, MemoryRows, StateStore, TableState};
use iceberg::table::Table;

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
/// Consume through [`TablePublisher::publish`](super::TablePublisher::publish);
/// dropping it leaves the journal as replay authority.
/// Its private constructor prevents blessing rows collapsed against another head.
pub struct CollapsedEpoch {
    pub(super) epoch: Epoch,
    pub(super) schema: TableSchema,
    pub(super) base: CommitBase,
    pub(super) indexed: TableState,
    pub(super) rows: CollapsedRows,
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

pub(super) enum CollapsedRows {
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
        let first_ordinal = ordinal;
        let affected: std::collections::BTreeSet<_> = txn.affected_tables.iter().copied().collect();
        for bytes in replay.chunks(&txn.mutation_chunks)? {
            let bytes = bytes?;
            let mutations: Vec<Mutation> = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(bytes.len() as u64)
                .reject_trailing_bytes()
                .deserialize(&bytes)?;
            for mutation in mutations {
                ensure!(
                    affected.contains(&mutation.table_id),
                    "journal mutation references an undeclared table"
                );
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
        if let Some(expected) = txn.mutation_count(epoch.table) {
            ensure!(
                ordinal - first_ordinal == expected,
                "journal mutation count differs from transaction descriptor for table {}: expected {expected}, decoded {}",
                epoch.table.0,
                ordinal - first_ordinal
            );
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
