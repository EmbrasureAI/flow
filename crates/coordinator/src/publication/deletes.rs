//! An epoch's position deletes. They are sorted in memory while they fit a
//! budget and staged in the transaction spool beyond it; both yield the
//! globally (path, position)-sorted, duplicate-free stream delete writers need.

use anyhow::Result;
use flow_model::{RowLocation, TableId};
use flow_state_store::StateStore;
use std::collections::HashMap;

// A file's interned path, its first location and its hash-map bucket.
const FILE_ALLOWANCE: usize = size_of::<RowLocation>() + 2 * size_of::<(String, u32)>() + 64;

pub(super) struct EpochDeletes {
    ids: HashMap<String, u32>,
    /// One original per interned file. Delete writers read only its path,
    /// spec and partition, which every row of one data file shares.
    files: Vec<RowLocation>,
    positions: Vec<(u32, u64)>,
    bytes: usize,
    limit: usize,
}

impl EpochDeletes {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            ids: HashMap::new(),
            files: Vec::new(),
            positions: Vec::new(),
            bytes: 0,
            limit,
        }
    }

    /// Record one deleted original. Returns false once the set outgrows its
    /// budget; the caller then spills it with [`Self::spill`].
    pub(super) fn push(&mut self, location: &RowLocation) -> bool {
        let id = match self.ids.get(location.data_file_id.0.as_str()) {
            Some(id) => *id,
            None => {
                let id = self.files.len() as u32;
                self.ids.insert(location.data_file_id.0.clone(), id);
                self.files.push(location.clone());
                self.bytes += FILE_ALLOWANCE
                    + 2 * location.data_file_id.0.capacity()
                    + location.partition.capacity();
                id
            }
        };
        self.positions.push((id, location.row_position));
        self.bytes += size_of::<(u32, u64)>();
        self.bytes <= self.limit && self.files.len() < u32::MAX as usize
    }

    pub(super) fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Batches of at most `batch_rows`, sorted by path and position.
    pub(super) fn into_sorted(self, batch_rows: usize) -> impl Iterator<Item = Vec<RowLocation>> {
        let Self {
            files,
            mut positions,
            ..
        } = self;
        // Rank interned files by path so a position sort matches path order.
        let mut by_path: Vec<u32> = (0..files.len() as u32).collect();
        by_path.sort_unstable_by(|a, b| {
            files[*a as usize]
                .data_file_id
                .cmp(&files[*b as usize].data_file_id)
        });
        let mut rank = vec![0u32; files.len()];
        for (order, id) in by_path.into_iter().enumerate() {
            rank[id as usize] = order as u32;
        }
        positions.sort_unstable_by_key(|(id, position)| (rank[*id as usize], *position));
        positions.dedup();
        let mut positions = positions.into_iter().peekable();
        std::iter::from_fn(move || {
            positions.peek()?;
            Some(
                positions
                    .by_ref()
                    .take(batch_rows)
                    .map(|(id, position)| RowLocation {
                        row_position: position,
                        ..files[id as usize].clone()
                    })
                    .collect(),
            )
        })
    }

    /// Stage every position in the spool and leave this set empty.
    pub(super) fn spill(
        &mut self,
        store: &StateStore,
        transaction: &str,
        table: &TableId,
        batch_rows: usize,
    ) -> Result<()> {
        let deletes = std::mem::replace(self, Self::new(self.limit));
        for batch in deletes.into_sorted(batch_rows) {
            store.put_position_deletes(transaction, table, batch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::{FileId, PgLsn};

    fn location(path: &str, position: u64) -> RowLocation {
        RowLocation {
            data_file_id: FileId(path.into()),
            row_position: position,
            data_sequence_number: 3,
            spec_id: 0,
            partition: vec![],
            source_commit_lsn: PgLsn(9),
            row_version: 2,
            row_fingerprint: [1; 16],
        }
    }

    #[test]
    fn memory_and_spool_yield_the_same_sorted_unique_stream() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = StateStore::open(temp.path(), Default::default()).unwrap();
        let table = TableId(7);
        // Paths chosen so first-seen order, byte order and an embedded NUL
        // (escaped in spool keys) all disagree.
        let input = [
            location("s3://b/z.parquet", 5),
            location("s3://b/a\0x.parquet", 2),
            location("s3://b/a.parquet", 9),
            location("s3://b/z.parquet", 1),
            location("s3://b/a.parquet", 9),
            location("s3://b/a.parquet", 0),
        ];
        let mut memory = EpochDeletes::new(1 << 20);
        for location in &input {
            assert!(memory.push(location));
        }
        let sorted: Vec<_> = memory.into_sorted(2).flatten().collect();
        let mut spilled = EpochDeletes::new(1 << 20);
        for location in &input {
            spilled.push(location);
        }
        spilled.spill(&store, "epoch", &table, 2).unwrap();
        let staged: Vec<_> = store
            .position_deletes("epoch", &table)
            .collect::<flow_state_store::Result<_>>()
            .unwrap();
        let key = |l: &RowLocation| (l.data_file_id.0.clone(), l.row_position);
        assert_eq!(
            sorted.iter().map(key).collect::<Vec<_>>(),
            staged.iter().map(key).collect::<Vec<_>>()
        );
        assert_eq!(sorted.len(), 5);
        assert!(sorted.windows(2).all(|w| key(&w[0]) < key(&w[1])));
    }

    #[test]
    fn the_budget_is_reported_once_exceeded() {
        let mut deletes = EpochDeletes::new(FILE_ALLOWANCE + 64);
        assert!(deletes.push(&location("f", 0)));
        let mut position = 1;
        while deletes.push(&location("f", position)) {
            position += 1;
        }
        assert!(position < 64);
    }
}
