//! Current and snapshot index reads for bounded compaction workers.

use crate::{
    Error, FILE_COUNTS, PK, REVERSE, Result, SnapshotIter, StateStore, TABLES, TableState, decode,
    decode_file_count, file_prefix,
    files::{self, FileOrdinals, NO_FILE, PathReader, StoredLocation},
    prefix_end,
    remap::{RemapCache, Resolver},
};
use flow_model::{FileId, PgLsn, PrimaryKey, RowLocation, TableId};
use rocksdb::{DB, DBPinnableSlice, Direction, IteratorMode, ReadOptions, SnapshotWithThreadMode};
use std::sync::Arc;

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
    files: &'db FileOrdinals,
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
        lookup_many(self.db, self.files, table, keys, &options, self.remaps)?
            .ok_or_else(files::unknown_file)
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
            return lookup_many(self.db, self.files, table, keys, &options, self.remaps)?
                .ok_or_else(files::unknown_file)?
                .iter()
                .map(|location| location.as_ref().map(LocationRef::from))
                .try_for_each(visit);
        }
        // The pinned values outlive the borrowed decodes; errors keep key order.
        // Ordinals resolve in this snapshot, to paths shared with the cache.
        let values = multi_get(self.db, table, keys, &options);
        let mut paths = PathReader::new(self.files, self.db, Some(&options));
        let decoded = values
            .iter()
            .map(|item| match item {
                Ok(Some(value)) => {
                    let stored = files::decode_location(value)?;
                    let path = paths
                        .path(*table, stored.file)?
                        .ok_or_else(files::unknown_file)?;
                    Ok(Some((stored, path)))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error.clone().into()),
            })
            .collect::<Result<Vec<Option<(StoredLocation<'_>, Arc<str>)>>>>()?;
        decoded
            .iter()
            .map(|entry| {
                entry
                    .as_ref()
                    .map(|(stored, path)| LocationRef::stored(stored, path))
            })
            .try_for_each(visit)
    }
}

/// A [`RowLocation`] that borrows its path and partition, from the cached
/// path and the pinned value.
#[derive(Debug, PartialEq, Eq)]
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

impl<'a> LocationRef<'a> {
    fn stored(stored: &'a StoredLocation<'_>, path: &'a str) -> Self {
        // Exhaustive, so a new stored field cannot be silently dropped.
        let StoredLocation {
            file: _,
            row_position,
            data_sequence_number,
            spec_id,
            partition,
            source_commit_lsn,
            row_version,
            row_fingerprint,
        } = stored;
        Self {
            data_file_id: path,
            row_position: *row_position,
            data_sequence_number: *data_sequence_number,
            spec_id: *spec_id,
            partition,
            source_commit_lsn: *source_commit_lsn,
            row_version: *row_version,
            row_fingerprint: *row_fingerprint,
        }
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
            remaps: self.remaps_possible().then_some(&self.0.remap_cache),
            files: &self.0.files,
        })
    }

    /// Batch the reads and decode pinned values without copying their encoded bytes.
    pub fn lookup_many(
        &self,
        table: &TableId,
        keys: &[PrimaryKey],
    ) -> Result<Vec<Option<RowLocation>>> {
        if !self.remaps_possible()
            && let Some(locations) = lookup_many(
                &self.0.db,
                &self.0.files,
                table,
                keys,
                &ReadOptions::default(),
                None,
            )?
        {
            return Ok(locations);
        }
        // Read raw entries and remaps from one view, so a concurrent
        // materialization cannot hide a remap from an entry read before it.
        // The same view also resolves an ordinal released after an entry
        // read without one.
        let snapshot = self.0.db.snapshot();
        let mut options = ReadOptions::default();
        options.set_snapshot(&snapshot);
        let remaps = self.remaps_possible().then_some(&self.0.remap_cache);
        lookup_many(&self.0.db, &self.0.files, table, keys, &options, remaps)?
            .ok_or_else(files::unknown_file)
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
        // The file's ordinal and its reverse entries come from one view: a
        // release and re-allocation in between would scan the wrong ordinal.
        let snapshot = self.0.db.snapshot();
        let mut options = ReadOptions::default();
        options.set_snapshot(&snapshot);
        SnapshotIter {
            rows: file_rows(&self.0.db, table, file, options),
            _snapshot: snapshot,
        }
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

/// `None` when an entry names an ordinal absent from `options`' view, which
/// a reader without a snapshot can see if the file was released in between.
fn lookup_many<'k>(
    db: &DB,
    files: &FileOrdinals,
    table: &TableId,
    keys: impl IntoIterator<Item = &'k PrimaryKey>,
    options: &ReadOptions,
    remaps: Option<&RemapCache>,
) -> Result<Option<Vec<Option<RowLocation>>>> {
    let mut paths = PathReader::new(files, db, Some(options));
    let values = multi_get(db, table, keys, options);
    let mut locations = Vec::with_capacity(values.len());
    for item in values {
        let Some(value) = item? else {
            locations.push(None);
            continue;
        };
        match paths.location(*table, &files::decode_location(&value)?)? {
            Some(location) => locations.push(Some(location)),
            None => return Ok(None),
        }
    }
    if let Some(cache) = remaps {
        let mut resolver = Resolver::new(db, cache, *table, options);
        for location in locations.iter_mut().flatten() {
            resolver.resolve_in_place(location)?;
        }
    }
    Ok(Some(locations))
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
    // The ordinal is read in the scan's own view. A file without one has no
    // reverse entries, and no ordinal has the reserved value.
    let (ordinal, error) = match files::ordinal_in_view(db, &options, table, file) {
        Ok(ordinal) => (ordinal.unwrap_or(NO_FILE), None),
        Err(error) => (NO_FILE, Some(error)),
    };
    let prefix = files::reverse_prefix(table, ordinal);
    options.fill_cache(false);
    if let Some(end) = prefix_end(&prefix) {
        options.set_iterate_upper_bound(end);
    }
    let cf = db.cf_handle(REVERSE).expect("opened column family");
    error.map(Err).into_iter().chain(
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
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_borrowed_location_matches_its_stored_encoding() {
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
        let mut encoded = Vec::new();
        files::encode_location(&mut encoded, &StoredLocation::new(9, &location)).unwrap();
        let stored = files::decode_location(&encoded).unwrap();
        assert_eq!(
            LocationRef::stored(&stored, &location.data_file_id.0),
            LocationRef::from(&location)
        );
    }
}
