//! Ephemeral input for one current epoch. Capacity failure requires full replay.

use crate::{
    Change, CollapsedMutation, Error, Result, RowIndex, StateStore, TableState, spool::PendingRow,
};
use flow_model::{PrimaryKey, Row, RowLocation, TableId, Value};
use std::collections::{BTreeMap, btree_map};

type Entry = (PendingRow, Option<RowLocation>);
const MAP_ALLOWANCE: usize = 4096;
// Account conservatively for partially occupied tree nodes and their links.
// This is an owned-working-set allowance, not an allocator or process RSS limit.
const ENTRY_ALLOWANCE: usize = 3 * size_of::<(PrimaryKey, Entry)>() + 64;

#[derive(Debug)]
pub enum BufferResult<T> {
    Ready(T),
    CapacityExceeded,
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

    /// On overflow the entire fold is discarded and cannot later be bound.
    pub fn push(&mut self, key: PrimaryKey, change: Change) -> Result<BufferResult<()>> {
        if self.exhausted {
            return Ok(BufferResult::CapacityExceeded);
        }
        if let Some((pending, _)) = self.rows.get_mut(&key) {
            let before = row_bytes(pending.row.as_ref()).expect("accepted row accounting");
            pending.push(change)?;
            let after = row_bytes(pending.row.as_ref());
            let next = after.and_then(|after| (self.bytes - before).checked_add(after));
            if !self.accept(next) {
                return Ok(BufferResult::CapacityExceeded);
            }
        } else {
            let pending = PendingRow::new(change)?;
            let next = row_bytes(pending.row.as_ref())
                .and_then(|row| row.checked_add(key.0.capacity()))
                .and_then(|heap| heap.checked_add(ENTRY_ALLOWANCE))
                .and_then(|entry| self.bytes.checked_add(entry));
            if !self.accept(next) {
                return Ok(BufferResult::CapacityExceeded);
            }
            self.rows.insert(key, (pending, None));
        }
        Ok(BufferResult::Ready(()))
    }

    fn accept(&mut self, next: Option<usize>) -> bool {
        if let Some(bytes) = next.filter(|bytes| *bytes <= self.limit) {
            self.bytes = bytes;
            self.peak = self.peak.max(bytes);
            true
        } else {
            self.rows.clear();
            self.bytes = 0;
            self.exhausted = true;
            false
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
    pub fn bind_collapse_buffer(
        &self,
        mut buffer: CollapseBuffer,
        expected: &TableState,
    ) -> Result<BufferResult<MemoryRows>> {
        if buffer.exhausted {
            return Ok(BufferResult::CapacityExceeded);
        }
        let snapshot = self.index_snapshot(&buffer.table, expected.snapshot_id)?;
        if snapshot.table_state() != expected {
            return Err(Error::InvalidState(
                "table state changed before binding collapsed source rows".into(),
            ));
        }
        let mut entries = buffer.rows.iter_mut();
        loop {
            let batch: Vec<_> = entries.by_ref().take(self.batch_rows()).collect();
            if batch.is_empty() {
                break;
            }
            let keys: Vec<_> = batch.iter().map(|(key, _)| (*key).clone()).collect();
            let originals = snapshot.lookup_many(&buffer.table, &keys)?;
            for ((_, (pending, bound)), original) in batch.into_iter().zip(originals) {
                pending.validate_initial(original.is_some())?;
                let extra = original.as_ref().map_or(Some(0), |location| {
                    location
                        .data_file_id
                        .0
                        .capacity()
                        .checked_add(location.partition.capacity())
                });
                let Some(bytes) = extra
                    .and_then(|extra| buffer.bytes.checked_add(extra))
                    .filter(|bytes| *bytes <= buffer.limit)
                else {
                    // Returning drops partial bindings, the map and this snapshot.
                    return Ok(BufferResult::CapacityExceeded);
                };
                buffer.bytes = bytes;
                buffer.peak = buffer.peak.max(bytes);
                *bound = original;
            }
        }
        Ok(BufferResult::Ready(MemoryRows {
            table: buffer.table,
            state: snapshot.table_state().clone(),
            rows: buffer.rows.into_iter(),
            peak: buffer.peak,
        }))
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
