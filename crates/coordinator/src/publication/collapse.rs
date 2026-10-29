//! Replay complete journal prefixes into rows bound to an exact table head.
//!
//! [`collapse_epoch`] validates the source schema and index before producing a
//! [`CollapsedEpoch`]. An epoch folds in memory; if it outgrows its budget the
//! fold spills to the disk map and the same pass continues there.
//!
//! Folding reads only the journal and source schemas. A [`Prefold`] folds an
//! epoch's leading transactions while the table's previous epoch is still
//! publishing; [`collapse_epoch_prefolded`] continues it and binds against the
//! index with every check a cold collapse makes.

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
use flow_state_store::{
    Bound, BufferResult, Change, CollapseBuffer, MemoryRows, StateStore, TableState,
};
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
    prefolded_transactions: usize,
}
impl CollapsedEpoch {
    pub fn epoch(&self) -> &Epoch {
        &self.epoch
    }

    /// The selected path and, for disk, why the buffer was bypassed or spilled
    /// (`disk_capacity`).
    pub fn mode(&self) -> &'static str {
        self.mode
    }

    /// Peak accepted buffer payload, within the limit, excluding separately
    /// bounded decoding, binding batches and writer workspaces. None when the
    /// epoch never used the buffer. Not process RSS.
    pub fn memory_peak_bytes(&self) -> Option<usize> {
        self.memory_peak_bytes
    }

    /// Leading transactions folded by a [`Prefold`] before this collapse.
    pub fn prefolded_transactions(&self) -> usize {
        self.prefolded_transactions
    }
}

/// Replay state carried across the transactions of one epoch.
#[derive(Debug, Default)]
struct ReplayState {
    ordinal: u64,
}

/// An epoch's leading transactions folded ahead of time, for a table with a
/// primary key, within the table's collapse budget. It holds no index state:
/// the previous epoch may still be preparing, committing or applying.
///
/// [`collapse_epoch_prefolded`] uses it only if the epoch's source, table,
/// schema and memory budget are unchanged and its transactions begin with
/// exactly the folded ones; otherwise it is dropped and the epoch collapses
/// from the journal, which remains the authority.
#[derive(Debug)]
pub struct Prefold {
    source: SourceId,
    schema: TableSchema,
    memory_bytes: usize,
    ends: Vec<PgLsn>,
    buffer: CollapseBuffer,
    state: ReplayState,
}

impl Prefold {
    /// None where a cold collapse would not fold in memory: keyless tables
    /// derive keys from the epoch's identity, and a zero budget selects disk.
    pub fn new(source: SourceId, schema: TableSchema, limits: CollapseLimits) -> Option<Self> {
        (!schema.primary_key.is_empty() && limits.memory_bytes > 0).then(|| Self {
            source,
            buffer: CollapseBuffer::new(schema.table_id, limits.memory_bytes),
            schema,
            memory_bytes: limits.memory_bytes,
            ends: Vec::new(),
            state: ReplayState::default(),
        })
    }

    /// The publication schema the fold projected rows to.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// End positions of the folded transactions, in source order.
    pub fn transactions(&self) -> &[PgLsn] {
        &self.ends
    }

    /// Fold the next transactions. Returns None, discarding the fold, when
    /// they would exceed the collapse budget or need the cold path: initial
    /// copies, or transactions this fold cannot continue. An error is the
    /// journal's or the schema's; the epoch's own collapse reports it again.
    pub fn extend(
        mut self,
        store: &StateStore,
        journal: &impl ChunkReader,
        transactions: &[SourceTransaction],
        limits: CollapseLimits,
    ) -> Result<Option<Self>> {
        ensure!(
            limits.batch_rows > 1 && limits.batch_bytes > 0,
            "collapse batch must fit both halves of a primary-key change"
        );
        let table = self.schema.table_id;
        let mut last = self.ends.last().copied();
        for transaction in transactions {
            if transaction.xid == 0
                || transaction.source_id != self.source
                || !transaction.affected_tables.contains(&table)
                || last.is_some_and(|last| transaction.end_lsn <= last)
            {
                return Ok(None);
            }
            last = Some(transaction.end_lsn);
        }
        if limits.memory_bytes != self.memory_bytes {
            return Ok(None);
        }
        let buffer = &mut self.buffer;
        let replayed = replay_epoch(
            store,
            journal,
            &self.schema,
            table,
            None,
            transactions,
            limits,
            &mut self.state,
            |changes| {
                for (_, key, change) in changes.drain(..) {
                    if let BufferResult::CapacityExceeded = buffer.push(key, change)? {
                        return Ok(BufferResult::CapacityExceeded);
                    }
                }
                Ok(BufferResult::Ready(()))
            },
        )?;
        if let BufferResult::CapacityExceeded = replayed {
            return Ok(None);
        }
        self.ends
            .extend(transactions.iter().map(|transaction| transaction.end_lsn));
        Ok(Some(self))
    }

    /// Whether `transactions` of `epoch` begin with this fold, unchanged.
    fn continues(
        &self,
        epoch: &Epoch,
        schema: &TableSchema,
        transactions: &[SourceTransaction],
        limits: CollapseLimits,
    ) -> bool {
        !epoch.initial_snapshot
            && self.source == epoch.source
            && self.schema.table_id == epoch.table
            && &self.schema == schema
            && self.memory_bytes == limits.memory_bytes
            && self.ends.len() <= transactions.len()
            && self
                .ends
                .iter()
                .zip(transactions)
                .all(|(end, transaction)| *end == transaction.end_lsn)
    }
}

pub(super) enum CollapsedRows {
    Memory(MemoryRows),
    Disk,
}

/// Collapse the current immutable source prefix on a blocking table actor.
/// Capacity alone moves the fold to the existing disk map, without replaying
/// what was already folded; validation and I/O failures never select fallback.
pub fn collapse_epoch(
    store: &StateStore,
    journal: &impl ChunkReader,
    table: &Table,
    schema: &TableSchema,
    epoch: &Epoch,
    transactions: &[SourceTransaction],
    limits: CollapseLimits,
) -> Result<CollapsedEpoch> {
    collapse_epoch_prefolded(
        store,
        journal,
        table,
        schema,
        epoch,
        transactions,
        limits,
        None,
    )
}

/// [`collapse_epoch`], continuing `prefold` when it folded the leading
/// transactions of this exact epoch. Binding, validation and spilling are
/// unchanged; a fold that does not continue is dropped.
#[allow(clippy::too_many_arguments)]
pub fn collapse_epoch_prefolded(
    store: &StateStore,
    journal: &impl ChunkReader,
    table: &Table,
    schema: &TableSchema,
    epoch: &Epoch,
    transactions: &[SourceTransaction],
    limits: CollapseLimits,
    prefold: Option<Prefold>,
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
    // Initial snapshots are copied into the spool directly; everything else folds
    // in memory first and spills only if it outgrows the budget.
    let mut mode = if limits.memory_bytes == 0 {
        "disk_disabled"
    } else if epoch.initial_snapshot {
        "disk_initial"
    } else {
        "memory"
    };
    let mut memory_peak_bytes = None;
    let mut prefolded_transactions = 0;
    let disk_fold = |changes: &mut Vec<(TableId, PrimaryKey, Change)>| -> Result<()> {
        store.collapse_changes(&epoch.id.0, changes.drain(..))?;
        Ok(())
    };
    let rows = if mode == "memory" {
        let (buffer, mut state) = match prefold
            .filter(|prefold| prefold.continues(epoch, schema, transactions, limits))
        {
            Some(prefold) => {
                prefolded_transactions = prefold.ends.len();
                (prefold.buffer, prefold.state)
            }
            None => (
                CollapseBuffer::new(epoch.table, limits.memory_bytes),
                ReplayState::default(),
            ),
        };
        let mut buffer = Some(buffer);
        replay_epoch(
            store,
            journal,
            schema,
            epoch.table,
            Some(&epoch.id),
            &transactions[prefolded_transactions..],
            limits,
            &mut state,
            |changes| {
                let Some(fold) = buffer.as_mut() else {
                    disk_fold(changes)?;
                    return Ok(BufferResult::Ready(()));
                };
                let mut changes = changes.drain(..);
                while let Some((_, key, change)) = changes.next() {
                    if let BufferResult::CapacityExceeded = fold.push(key, change)? {
                        let full = buffer.take().expect("buffer present above");
                        memory_peak_bytes = Some(full.peak_accounted_bytes());
                        store.spill_collapse_buffer(&epoch.id.0, full)?;
                        store.collapse_changes(&epoch.id.0, changes)?;
                        break;
                    }
                }
                Ok(BufferResult::Ready(()))
            },
        )?;
        match buffer {
            Some(buffer) => {
                memory_peak_bytes = Some(buffer.peak_accounted_bytes());
                match store.bind_collapse_buffer(buffer, &indexed)? {
                    Bound::Ready(rows) => {
                        memory_peak_bytes = Some(rows.peak_accounted_bytes());
                        Some(CollapsedRows::Memory(rows))
                    }
                    Bound::Overflow(buffer) => {
                        store.spill_collapse_buffer(&epoch.id.0, buffer)?;
                        None
                    }
                }
            }
            None => None,
        }
    } else {
        replay_epoch(
            store,
            journal,
            schema,
            epoch.table,
            Some(&epoch.id),
            transactions,
            limits,
            &mut ReplayState::default(),
            |changes| {
                disk_fold(changes)?;
                Ok(BufferResult::Ready(()))
            },
        )?;
        None
    };
    let rows = match rows {
        Some(rows) => rows,
        None => {
            if mode == "memory" {
                mode = "disk_capacity";
            }
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
        prefolded_transactions,
    })
}

/// Fold `transactions` for `table`, continuing `state`. Keyless inserts take
/// their keys from `epoch` and its mutation ordinal.
#[allow(clippy::too_many_arguments)]
fn replay_epoch(
    store: &StateStore,
    journal: &impl ChunkReader,
    schema: &TableSchema,
    table: TableId,
    epoch: Option<&OperationId>,
    transactions: &[SourceTransaction],
    limits: CollapseLimits,
    state: &mut ReplayState,
    mut consume: impl FnMut(&mut Vec<(TableId, PrimaryKey, Change)>) -> Result<BufferResult<()>>,
) -> Result<BufferResult<()>> {
    let mut changes = Vec::with_capacity(limits.batch_rows);
    let mut buffered_bytes = 0usize;
    let ReplayState { ordinal } = state;
    let mut historical_schema: Option<TableSchema> = None;
    let mut replay = journal.replay_cursor();
    for txn in transactions {
        let first_ordinal = *ordinal;
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
                if mutation.table_id != table {
                    continue;
                }
                if matches!(&mutation.kind, MutationKind::Quarantined { .. }) {
                    return Err(flow_model::SourceTableBlocked.into());
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
                // Only an update may omit unchanged values. Its key is always
                // present, and a key change would need another key's image.
                let project_update = |row: flow_model::Row| -> Result<_> {
                    if !flow_model::row_has_unchanged(&row) {
                        return project(row);
                    }
                    match &source_schema {
                        Some(source) => Ok(source.project_partial_row(row, schema)?),
                        None => {
                            schema.validate_partial_row(&row)?;
                            Ok(row)
                        }
                    }
                };
                let (first, second) = match mutation.kind {
                    MutationKind::Quarantined { .. } => {
                        return Err(flow_model::SourceTableBlocked.into());
                    }
                    MutationKind::Insert { row } => {
                        let row = project(row)?;
                        let key = if schema.primary_key.is_empty() {
                            ensure!(schema.append_only, "mutable table has no key");
                            let epoch = epoch.ok_or_else(|| {
                                anyhow::anyhow!("a keyless insert needs its epoch's identity")
                            })?;
                            let mut key = epoch.0.as_bytes().to_vec();
                            key.extend(ordinal.to_be_bytes());
                            PrimaryKey(key)
                        } else {
                            schema.encode_key(&row)?
                        };
                        ((key, Change::Insert(row)), None)
                    }
                    MutationKind::Update { old_key, row } => {
                        ensure!(!schema.append_only, "update on append-only table");
                        let row = project_update(row)?;
                        let key = schema.encode_partial_key(&row)?;
                        ensure!(
                            key == old_key || !flow_model::row_has_unchanged(&row),
                            "a primary-key change cannot omit unchanged values"
                        );
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
                changes.push((table, first.0, first.1));
                if let Some((key, change)) = second {
                    changes.push((table, key, change));
                }
                buffered_bytes += size;
                *ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("mutation ordinal overflow"))?;
            }
        }
        if let Some(expected) = txn.mutation_count(table) {
            ensure!(
                *ordinal - first_ordinal == expected,
                "journal mutation count differs from transaction descriptor for table {}: expected {expected}, decoded {}",
                table.0,
                *ordinal - first_ordinal
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
