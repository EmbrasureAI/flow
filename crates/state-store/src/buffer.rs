//! Ephemeral input for one current epoch. On capacity failure the fold is
//! spilled to the transaction spool and the epoch continues on disk; nothing
//! already folded is replayed.

use crate::{Change, CollapsedMutation, Error, Result, StateStore, TableState, spool::PendingRow};
use flow_model::{FileId, PgLsn, PrimaryKey, Row, RowLocation, TableId, Value};
use std::collections::{BTreeMap, HashMap, btree_map};

// Originals exist only after binding, and only for keys already indexed. Boxing
// keeps the inline entry small, which the per-entry allowance below multiplies.
type Entry = (PendingRow, Option<Box<Original>>);
const MAP_ALLOWANCE: usize = 4096;
// One interned path or partition: its hash-map bucket and index at up to twice
// their occupied size. The value itself is charged once, by capacity.
const INTERN_ALLOWANCE: usize = 2 * (size_of::<Vec<u8>>() + size_of::<u32>() + 8) + 32;

/// A bound original. An epoch's keys share few data files, so the path and
/// partition are interned per buffer instead of copied into every key.
#[derive(Debug)]
struct Original {
    file: u32,
    partition: u32,
    row_position: u64,
    data_sequence_number: i64,
    spec_id: i32,
    source_commit_lsn: PgLsn,
    row_version: u64,
    row_fingerprint: [u8; 16],
}

/// Paths and partitions seen while binding, each stored once.
#[derive(Debug, Default)]
struct Interned {
    files: HashMap<FileId, u32>,
    partitions: HashMap<Vec<u8>, u32>,
}

/// Interned values indexed by id, for expanding originals after binding.
#[derive(Debug, Default)]
struct Expanded {
    files: Vec<FileId>,
    partitions: Vec<Vec<u8>>,
}

fn intern<T: std::hash::Hash + Eq>(
    ids: &mut HashMap<T, u32>,
    value: T,
    bytes: usize,
) -> Result<(u32, usize)> {
    let next = u32::try_from(ids.len())
        .map_err(|_| Error::InvalidState("too many interned original locations".into()))?;
    match ids.entry(value) {
        std::collections::hash_map::Entry::Occupied(entry) => Ok((*entry.get(), 0)),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(next);
            Ok((next, bytes + INTERN_ALLOWANCE))
        }
    }
}

fn by_id<T>(ids: HashMap<T, u32>) -> Vec<T> {
    let mut values: Vec<_> = ids.into_iter().collect();
    values.sort_unstable_by_key(|(_, id)| *id);
    values.into_iter().map(|(value, _)| value).collect()
}

impl Interned {
    /// The compact original and the bytes newly retained to bind it.
    fn bind(&mut self, location: RowLocation) -> Result<(Original, usize)> {
        let file_bytes = location.data_file_id.0.capacity();
        let (file, file_added) = intern(&mut self.files, location.data_file_id, file_bytes)?;
        let partition_bytes = location.partition.capacity();
        let (partition, partition_added) =
            intern(&mut self.partitions, location.partition, partition_bytes)?;
        Ok((
            Original {
                file,
                partition,
                row_position: location.row_position,
                data_sequence_number: location.data_sequence_number,
                spec_id: location.spec_id,
                source_commit_lsn: location.source_commit_lsn,
                row_version: location.row_version,
                row_fingerprint: location.row_fingerprint,
            },
            file_added + partition_added + size_of::<Original>(),
        ))
    }

    fn finish(self) -> Expanded {
        Expanded {
            files: by_id(self.files),
            partitions: by_id(self.partitions),
        }
    }
}

impl Expanded {
    fn location(&self, original: &Original) -> RowLocation {
        RowLocation {
            data_file_id: self.files[original.file as usize].clone(),
            row_position: original.row_position,
            data_sequence_number: original.data_sequence_number,
            spec_id: original.spec_id,
            partition: self.partitions[original.partition as usize].clone(),
            source_commit_lsn: original.source_commit_lsn,
            row_version: original.row_version,
            row_fingerprint: original.row_fingerprint,
        }
    }
}

// Account conservatively for partially occupied tree nodes and their links.
// This is an owned-working-set allowance, not an allocator or process RSS limit.
const ENTRY_ALLOWANCE: usize = 3 * size_of::<(PrimaryKey, Entry)>() + 64;

#[derive(Debug)]
pub enum BufferResult<T> {
    Ready(T),
    CapacityExceeded,
}

/// The outcome of binding a fold against the index.
#[derive(Debug)]
pub enum Bound {
    Ready(MemoryRows),
    /// Binding would exceed the limit; spill the returned buffer instead.
    Overflow(CollapseBuffer),
}

/// A table-local, capacity-accounted fold. No source or index state is written.
#[derive(Debug)]
pub struct CollapseBuffer {
    table: TableId,
    rows: BTreeMap<PrimaryKey, Entry>,
    limit: usize,
    bytes: usize,
    peak: usize,
    exhausted: bool,
}

impl CollapseBuffer {
    pub fn new(table: TableId, memory_bytes: usize) -> Self {
        let exhausted = memory_bytes < MAP_ALLOWANCE;
        let bytes = if exhausted { 0 } else { MAP_ALLOWANCE };
        Self {
            table,
            rows: BTreeMap::new(),
            limit: memory_bytes,
            bytes,
            peak: bytes,
            exhausted,
        }
    }

    /// Largest accepted working set. The incoming source batch is separate.
    pub fn peak_accounted_bytes(&self) -> usize {
        self.peak
    }

    /// Applies the change, then reports whether the fold is over its limit. An
    /// over-limit fold keeps every change it accepted, including this one: the
    /// caller spills it with [`StateStore::spill_collapse_buffer`].
    pub fn push(&mut self, key: PrimaryKey, change: Change) -> Result<BufferResult<()>> {
        if let Some((pending, _)) = self.rows.get_mut(&key) {
            let before = row_bytes(pending.row.as_ref()).expect("accepted row accounting");
            pending.push(change)?;
            let after = row_bytes(pending.row.as_ref());
            let next = after.and_then(|after| (self.bytes - before).checked_add(after));
            Ok(self.account(next))
        } else {
            let pending = PendingRow::new(change)?;
            let next = row_bytes(pending.row.as_ref())
                .and_then(|row| row.checked_add(key.0.capacity()))
                .and_then(|heap| heap.checked_add(ENTRY_ALLOWANCE))
                .and_then(|entry| self.bytes.checked_add(entry));
            self.rows.insert(key, (pending, None));
            Ok(self.account(next))
        }
    }

    /// The peak records only states within the limit. The change that crosses
    /// it was already resident as decoded input and is spilled straight away.
    fn account(&mut self, next: Option<usize>) -> BufferResult<()> {
        self.bytes = next.unwrap_or(usize::MAX);
        if self.exhausted || self.bytes > self.limit {
            self.exhausted = true;
            BufferResult::CapacityExceeded
        } else {
            self.peak = self.peak.max(self.bytes);
            BufferResult::Ready(())
        }
    }
}

/// Bound, validated rows in canonical key order. No snapshot or DB borrow escapes.
#[derive(Debug)]
pub struct MemoryRows {
    table: TableId,
    state: TableState,
    rows: btree_map::IntoIter<PrimaryKey, Entry>,
    interned: Expanded,
    peak: usize,
}

impl MemoryRows {
    pub fn table_state(&self) -> &TableState {
        &self.state
    }

    pub fn peak_accounted_bytes(&self) -> usize {
        self.peak
    }
}

impl Iterator for MemoryRows {
    type Item = CollapsedMutation;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.find_map(|(key, (pending, original))| {
            // Initial absence was proved before any row became publishable.
            if original.is_none() && pending.row.is_none() {
                None
            } else {
                let original = original.map(|original| self.interned.location(&original));
                Some(pending.into_mutation(self.table, key, original))
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.rows.len()))
    }
}

impl StateStore {
    /// Bind against one complete H, including no-op/schema watermarks. All rows
    /// must validate and fit before the caller can consume or publish any of them.
    /// On overflow the unbound buffer is returned for [`Self::spill_collapse_buffer`].
    pub fn bind_collapse_buffer(
        &self,
        mut buffer: CollapseBuffer,
        expected: &TableState,
    ) -> Result<Bound> {
        if buffer.exhausted {
            return Ok(Bound::Overflow(buffer));
        }
        let snapshot = self.index_snapshot(&buffer.table, expected.snapshot_id)?;
        if snapshot.table_state() != expected {
            return Err(Error::InvalidState(
                "table state changed before binding collapsed source rows".into(),
            ));
        }
        let mut interned = Interned::default();
        let overflow = 'bind: {
            let mut entries = buffer.rows.iter_mut();
            loop {
                let batch: Vec<_> = entries.by_ref().take(self.batch_rows()).collect();
                if batch.is_empty() {
                    break 'bind false;
                }
                let originals =
                    snapshot.lookup_keys(&buffer.table, batch.iter().map(|(key, _)| *key))?;
                for ((_, (pending, bound)), original) in batch.into_iter().zip(originals) {
                    pending.validate_initial(original.is_some())?;
                    let (original, extra) = match original {
                        Some(location) => {
                            let (original, extra) = interned.bind(location)?;
                            (Some(Box::new(original)), extra)
                        }
                        None => (None, 0),
                    };
                    let Some(bytes) = buffer
                        .bytes
                        .checked_add(extra)
                        .filter(|bytes| *bytes <= buffer.limit)
                    else {
                        break 'bind true;
                    };
                    buffer.bytes = bytes;
                    buffer.peak = buffer.peak.max(bytes);
                    *bound = original;
                }
            }
        };
        if overflow {
            // Spilling looks up every original again under the row lock.
            buffer.exhausted = true;
            return Ok(Bound::Overflow(buffer));
        }
        Ok(Bound::Ready(MemoryRows {
            table: buffer.table,
            state: snapshot.table_state().clone(),
            rows: buffer.rows.into_iter(),
            interned: interned.finish(),
            peak: buffer.peak,
        }))
    }
}

impl StateStore {
    /// Move an over-limit fold into the transaction spool, bound against the
    /// current index exactly as [`Self::collapse_changes`] binds a disk chunk.
    /// The table's spool must be empty; later chunks then fold on disk.
    pub fn spill_collapse_buffer(&self, transaction: &str, buffer: CollapseBuffer) -> Result<()> {
        let table = buffer.table;
        self.ensure_spool_empty(transaction, &table)?;
        let mut rows = buffer.rows.into_iter();
        loop {
            let batch: Vec<_> = rows
                .by_ref()
                .take(self.batch_rows())
                .map(|(key, (pending, _))| (key, pending))
                .collect();
            if batch.is_empty() {
                return Ok(());
            }
            self.spill_folded(transaction, &table, batch)?;
        }
    }
}

fn row_bytes(row: Option<&Row>) -> Option<usize> {
    let Some(row) = row else { return Some(0) };
    row.iter().try_fold(
        row.capacity().checked_mul(size_of::<Value>())?,
        |sum, value| {
            sum.checked_add(match value {
                Value::String(value) => value.capacity(),
                Value::Binary(value) => value.capacity(),
                _ => 0,
            })
        },
    )
}
