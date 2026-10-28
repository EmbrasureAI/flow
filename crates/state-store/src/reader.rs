//! Current and snapshot index reads for bounded compaction workers.

use crate::{
    Error, FILE_COUNTS, PK, REVERSE, Result, StateStore, TABLES, TableState, decode,
    decode_file_count, file_prefix, prefix_end,
    remap::{RemapCache, Resolver},
};
use flow_model::{FileId, PgLsn, PrimaryKey, RowLocation, TableId};
use rocksdb::{DB, DBPinnableSlice, Direction, IteratorMode, ReadOptions, SnapshotWithThreadMode};
use serde::Deserialize;

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
    /// Lazy remaps visible in this snapshot may exist.
    remaps: Option<&'db RemapCache>,
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

    /// [`RowIndex::lookup_many`] over borrowed keys.
    pub(crate) fn lookup_keys<'k>(
        &self,
        table: &TableId,
        keys: impl IntoIterator<Item = &'k PrimaryKey>,
    ) -> Result<Vec<Option<RowLocation>>> {
        let mut options = ReadOptions::default();
        options.set_snapshot(&self.snapshot);
        lookup_many(self.db, table, keys, &options, self.remaps)
    }

    /// Like [`Self::lookup_keys`], but passes each original in key order to
    /// `visit`, borrowed from the pinned value when no lazy remap can apply.
    /// Every key is read and decoded before the first call.
    pub(crate) fn visit_keys<'k>(
        &self,
        table: &TableId,
        keys: impl IntoIterator<Item = &'k PrimaryKey>,
        visit: impl FnMut(Option<LocationRef<'_>>) -> Result<()>,
    ) -> Result<()> {
        let mut options = ReadOptions::default();
        options.set_snapshot(&self.snapshot);
        if self.remaps.is_some() {
            return lookup_many(self.db, table, keys, &options, self.remaps)?
                .iter()
                .map(|location| location.as_ref().map(LocationRef::from))
                .try_for_each(visit);
        }
        // The pinned values outlive the borrowed decodes; errors keep key order.
        let values = multi_get(self.db, table, keys, &options);
        values
            .iter()
            .map(|item| match item {
                Ok(value) => value.as_deref().map(decode_ref).transpose(),
                Err(error) => Err(error.clone().into()),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .try_for_each(visit)
    }
}

/// A [`RowLocation`] decoded in place, borrowing its path and partition.
/// Bincode encodes a string or byte vector and its borrowed form identically.
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct LocationRef<'a> {
    pub data_file_id: &'a str,
    pub row_position: u64,
    pub data_sequence_number: i64,
    pub spec_id: i32,
    pub partition: &'a [u8],
    pub source_commit_lsn: PgLsn,
    pub row_version: u64,
    pub row_fingerprint: [u8; 16],
}

impl<'a> From<&'a RowLocation> for LocationRef<'a> {
    fn from(location: &'a RowLocation) -> Self {
        Self {
            data_file_id: &location.data_file_id.0,
            row_position: location.row_position,
            data_sequence_number: location.data_sequence_number,
            spec_id: location.spec_id,
            partition: &location.partition,
            source_commit_lsn: location.source_commit_lsn,
            row_version: location.row_version,
            row_fingerprint: location.row_fingerprint,
        }
    }
}

fn decode_ref(value: &[u8]) -> Result<LocationRef<'_>> {
    Ok(bincode::deserialize(value)?)
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
            remaps: self.remaps_possible().then_some(&self.0.remap_cache),
        })
    }

    /// Batch the reads and decode pinned values without copying their encoded bytes.
    pub fn lookup_many(
        &self,
        table: &TableId,
        keys: &[PrimaryKey],
    ) -> Result<Vec<Option<RowLocation>>> {
        if !self.remaps_possible() {
            return lookup_many(&self.0.db, table, keys, &ReadOptions::default(), None);
        }
        // Read raw entries and remaps from one view, so a concurrent
        // materialization cannot hide a remap from an entry read before it.
        let snapshot = self.0.db.snapshot();
        let mut options = ReadOptions::default();
        options.set_snapshot(&snapshot);
        lookup_many(&self.0.db, table, keys, &options, Some(&self.0.remap_cache))
    }

    /// Ordered reverse scan, bounded by the caller's consumption. Deleted rows
    /// are removed from this index atomically with their primary-key mapping.
    /// Rows moved by a lazy remap stay under their retired file until
    /// [`StateStore::materialize_remaps`]; call it before relying on this scan
    /// for a table where [`StateStore::has_remaps`] holds.
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
        self.lookup_keys(table, keys)
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

fn lookup_many<'k>(
    db: &DB,
    table: &TableId,
    keys: impl IntoIterator<Item = &'k PrimaryKey>,
    options: &ReadOptions,
    remaps: Option<&RemapCache>,
) -> Result<Vec<Option<RowLocation>>> {
    let mut locations = multi_get(db, table, keys, options)
        .into_iter()
        .map(|item| {
            item.map_err(Error::from)?
                .map(|value| decode(&value))
                .transpose()
        })
        .collect::<Result<Vec<Option<RowLocation>>>>()?;
    if let Some(cache) = remaps {
        let mut resolver = Resolver::new(db, cache, *table, options);
        for location in locations.iter_mut().flatten() {
            resolver.resolve_in_place(location)?;
        }
    }
    Ok(locations)
}

/// Raw primary-key entries in input order. The encoded keys share one buffer.
fn multi_get<'db, 'k>(
    db: &'db DB,
    table: &TableId,
    keys: impl IntoIterator<Item = &'k PrimaryKey>,
    options: &ReadOptions,
) -> Vec<std::result::Result<Option<DBPinnableSlice<'db>>, rocksdb::Error>> {
    let cf = db.cf_handle(PK).expect("opened column family");
    let mut encoded = Vec::new();
    let mut ends = Vec::new();
    for key in keys {
        encoded.extend_from_slice(&table.0.to_be_bytes());
        encoded.extend_from_slice(&key.0);
        ends.push(encoded.len());
    }
    let keys: Vec<&[u8]> = std::iter::once(0)
        .chain(ends.iter().copied())
        .zip(&ends)
        .map(|(start, end)| &encoded[start..*end])
        .collect();
    // Callers usually pass canonical key order; RocksDB then skips its own sort.
    let sorted = keys.is_sorted();
    db.batched_multi_get_cf_opt(&cf, keys, sorted, options)
}

/// Raw reverse entries, without lazy-remap translation.
pub(crate) fn file_rows_raw<'a>(
    db: &'a DB,
    table: &TableId,
    file: &FileId,
    options: ReadOptions,
) -> impl Iterator<Item = Result<(u64, PrimaryKey)>> + Send + 'a + use<'a> {
    file_rows(db, table, file, options)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_borrowed_location_decodes_the_stored_encoding() {
        let location = RowLocation {
            data_file_id: FileId("s3://bucket/δ/data.parquet".into()),
            row_position: u64::MAX - 1,
            data_sequence_number: -7,
            spec_id: -3,
            partition: vec![0, 255, 1, 128],
            source_commit_lsn: PgLsn(u64::MAX - 2),
            row_version: u64::MAX - 3,
            row_fingerprint: [0xa5; 16],
        };
        let stored = bincode::serialize(&location).unwrap();
        assert_eq!(decode_ref(&stored).unwrap(), LocationRef::from(&location));
        assert!(decode_ref(&stored[..stored.len() - 1]).is_err());
    }
}
