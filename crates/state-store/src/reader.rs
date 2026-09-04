//! Current and snapshot index reads for bounded compaction workers.

use crate::{
    Error, FILE_COUNTS, PK, REVERSE, Result, StateStore, TABLES, TableState, decode,
    decode_file_count, file_prefix, pk_key, prefix_end,
};
use flow_model::{FileId, PrimaryKey, RowLocation, TableId};
use rocksdb::{DB, Direction, IteratorMode, ReadOptions, SnapshotWithThreadMode};

/// The worker's read-only index surface. Scratch writes remain separate.
pub trait RowIndex: Sync {
    fn lookup_many(&self, table: &TableId, keys: &[PrimaryKey])
    -> Result<Vec<Option<RowLocation>>>;

    fn file_rows<'a>(
        &'a self,
        table: &TableId,
        file: &FileId,
    ) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a;
}

/// A borrowed RocksDB snapshot with an admission proof for one table.
///
/// Reads share one immutable database sequence. The captured table state was
/// unfenced and matched the requested catalog snapshot when this view was made.
/// The owner must retain the StateStore and any catalog/artifact pins until the
/// worker finishes; this view does not own those external recovery protections.
pub struct IndexSnapshot<'db> {
    db: &'db DB,
    snapshot: SnapshotWithThreadMode<'db, DB>,
    table: TableId,
    state: TableState,
}

impl IndexSnapshot<'_> {
    pub fn table_id(&self) -> TableId {
        self.table
    }

    /// Includes the materialized LSN and schema version. A no-op CDC commit can
    /// advance these without changing the catalog snapshot ID.
    pub fn table_state(&self) -> &TableState {
        &self.state
    }

    /// Exact live-row counts in input order at the captured table version.
    pub fn file_live_row_counts(&self, files: &[FileId]) -> Result<Vec<u64>> {
        let cf = self
            .db
            .cf_handle(FILE_COUNTS)
            .expect("opened column family");
        files
            .iter()
            .map(|file| {
                self.snapshot
                    .get_cf(&cf, file_prefix(&self.table, file))?
                    .map(|bytes| decode_file_count(&bytes))
                    .transpose()
                    .map(|count| count.unwrap_or(0))
            })
            .collect()
    }
}

impl StateStore {
    /// Capture a complete table version while excluding partial index applies.
    /// The lock is released before returning; later CDC does not block on reads.
    pub fn index_snapshot(
        &self,
        table: &TableId,
        expected_snapshot: Option<i64>,
    ) -> Result<IndexSnapshot<'_>> {
        let _guard = self.lock()?;
        let snapshot = self.0.db.snapshot();
        let cf = self.0.db.cf_handle(TABLES).expect("opened column family");
        let state: TableState = snapshot
            .get_cf(&cf, table.0.to_be_bytes())?
            .map(|bytes| decode(&bytes))
            .transpose()?
            .unwrap_or_default();
        if state.pending_operation.is_some() || state.snapshot_id != expected_snapshot {
            return Err(Error::SnapshotMismatch {
                table: *table,
                expected: expected_snapshot,
                actual: state.snapshot_id,
                pending: state.pending_operation,
            });
        }
        Ok(IndexSnapshot {
            db: &self.0.db,
            snapshot,
            table: *table,
            state,
        })
    }

    /// Batch the reads and decode pinned values without copying their encoded bytes.
    pub fn lookup_many(
        &self,
        table: &TableId,
        keys: &[PrimaryKey],
    ) -> Result<Vec<Option<RowLocation>>> {
        lookup_many(&self.0.db, table, keys, &ReadOptions::default())
    }

    /// Ordered reverse scan, bounded by the caller's consumption. Deleted rows
    /// are removed from this index atomically with their primary-key mapping.
    pub fn file_rows<'a>(
        &'a self,
        table: &TableId,
        file: &FileId,
    ) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a {
        file_rows(&self.0.db, table, file, ReadOptions::default())
    }
}

impl RowIndex for StateStore {
    fn lookup_many(
        &self,
        table: &TableId,
        keys: &[PrimaryKey],
    ) -> Result<Vec<Option<RowLocation>>> {
        self.lookup_many(table, keys)
    }

    fn file_rows<'a>(
        &'a self,
        table: &TableId,
        file: &FileId,
    ) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a {
        self.file_rows(table, file)
    }
}

impl RowIndex for IndexSnapshot<'_> {
    fn lookup_many(
        &self,
        table: &TableId,
        keys: &[PrimaryKey],
    ) -> Result<Vec<Option<RowLocation>>> {
        let mut options = ReadOptions::default();
        options.set_snapshot(&self.snapshot);
        lookup_many(self.db, table, keys, &options)
    }

    fn file_rows<'a>(
        &'a self,
        table: &TableId,
        file: &FileId,
    ) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a {
        let mut options = ReadOptions::default();
        options.set_snapshot(&self.snapshot);
        file_rows(self.db, table, file, options)
    }
}

fn lookup_many(
    db: &DB,
    table: &TableId,
    keys: &[PrimaryKey],
    options: &ReadOptions,
) -> Result<Vec<Option<RowLocation>>> {
    let cf = db.cf_handle(PK).expect("opened column family");
    let encoded: Vec<_> = keys.iter().map(|key| pk_key(table, key)).collect();
    db.batched_multi_get_cf_opt(&cf, encoded.iter(), false, options)
        .into_iter()
        .map(|item| {
            item.map_err(Error::from)?
                .map(|value| decode(&value))
                .transpose()
        })
        .collect()
}

fn file_rows<'a>(
    db: &'a DB,
    table: &TableId,
    file: &FileId,
    mut options: ReadOptions,
) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a + use<'a> {
    let prefix = file_prefix(table, file);
    options.fill_cache(false);
    if let Some(end) = prefix_end(&prefix) {
        options.set_iterate_upper_bound(end);
    }
    let cf = db.cf_handle(REVERSE).expect("opened column family");
    db.iterator_cf_opt(
        &cf,
        options,
        IteratorMode::From(&prefix, Direction::Forward),
    )
    .take_while(move |item| {
        item.as_ref()
            .map(|(key, _)| key.starts_with(&prefix))
            .unwrap_or(true)
    })
    .map(|item| {
        let (key, value) = item?;
        let bytes: [u8; 8] = key[key.len() - 8..]
            .try_into()
            .map_err(|_| Error::InvalidState("invalid reverse index key".into()))?;
        Ok((u64::from_be_bytes(bytes), decode(&value)?))
    })
}
