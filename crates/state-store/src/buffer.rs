//! Ephemeral input for one current epoch. On capacity failure the fold is
//! spilled to the transaction spool and the epoch continues on disk; nothing
//! already folded is replayed.

use crate::{Change, CollapsedMutation, Error, Result, StateStore, TableState, spool::PendingRow};
use flow_model::{PrimaryKey, Row, RowLocation, TableId, Value};
use std::collections::{BTreeMap, btree_map};

// Originals exist only after binding, and only for keys already indexed. Boxing
// keeps the inline entry small, which the per-entry allowance below multiplies.
type Entry = (PendingRow, Option<Box<RowLocation>>);
const MAP_ALLOWANCE: usize = 4096;
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
                Some(pending.into_mutation(self.table, key, original.map(|original| *original)))
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
                    let extra = original.as_ref().map_or(Some(0), |location| {
                        location
                            .data_file_id
                            .0
                            .capacity()
                            .checked_add(location.partition.capacity())
                            .and_then(|heap| heap.checked_add(size_of::<RowLocation>()))
                    });
                    let Some(bytes) = extra
                        .and_then(|extra| buffer.bytes.checked_add(extra))
                        .filter(|bytes| *bytes <= buffer.limit)
                    else {
                        break 'bind true;
                    };
                    buffer.bytes = bytes;
                    buffer.peak = buffer.peak.max(bytes);
                    *bound = original.map(Box::new);
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
