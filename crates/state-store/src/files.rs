//! Data-file ordinals and the compact row-location encodings that use them.
//!
//! Primary-key values, reverse keys and staged deltas name a data file by a
//! per-table `u64` ordinal instead of its path, which keeps those records a
//! fixed small size however long object-store paths are. The `FILES` family
//! maps path to ordinal and back, plus a durable next-ordinal counter. Public
//! APIs still exchange [`RowLocation`]s with paths: ordinals never leave one
//! index generation, so a scratch store and the live index allocate their own.
//!
//! Allocation happens under the row lock, in the same write batch as the first
//! record that names the new ordinal, so a lost batch loses both. The cache
//! holds every durable entry and is updated only after its batch was written;
//! a failed write that touched the dictionary fences the store, because its
//! entries may be durable even though the write reported an error.
//!
//! An ordinal is released only when nothing can name it: its file has no live
//! count, no lazy remap and no reverse entry, and no remaining operation pins
//! it. Staging writes one pin per operation and ordinal its deltas name, under
//! the operation's staged records, so pins go with those records. Candidates
//! are marked when an ordinal is allocated, when its file's live count reaches
//! zero and when its remap is dropped; [`StateStore::forget_applied_batch`]
//! sweeps them, also while later operations of the table are in flight.
//! Readers of a RocksDB snapshot still resolve released ordinals from that
//! snapshot, and ordinals are never reused, so a cached ordinal never changes
//! meaning.

use crate::{
    Error, FILE_COUNTS, REMAPS, REVERSE, Result, StateBatch, StateStore, file_prefix, prefix_end,
};
use bincode::Options as _;
use flow_model::{FileId, PgLsn, RowLocation, TableId};
use rocksdb::{DB, Direction, IteratorMode, ReadOptions};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    hash::{BuildHasherDefault, Hasher},
    sync::{
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        atomic::{AtomicU64, Ordering},
    },
};

pub(crate) const FILES: &str = "file_ordinals";

// Keys of the FILES family. The counter sorts before every prefixed record.
const NEXT_ORDINAL: &[u8] = &[0];
const BY_PATH: u8 = 1;
const BY_ORDINAL: u8 = 2;
const RELEASED: u8 = 3;

/// Never allocated. It names a file that has no ordinal, so it compares
/// unequal to every stored location and its reverse prefix is always empty.
pub(crate) const NO_FILE: u64 = u64::MAX;

/// A row location as the index stores it. All fields of [`RowLocation`] are
/// kept; only the path is replaced by its ordinal. Equality is exhaustive and
/// is the apply compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredLocation<'a> {
    pub(crate) file: u64,
    pub(crate) row_position: u64,
    pub(crate) data_sequence_number: i64,
    pub(crate) spec_id: i32,
    #[serde(borrow)]
    pub(crate) partition: Cow<'a, [u8]>,
    pub(crate) source_commit_lsn: PgLsn,
    pub(crate) row_version: u64,
    pub(crate) row_fingerprint: [u8; 16],
}

impl<'a> StoredLocation<'a> {
    pub(crate) fn new(file: u64, location: &'a RowLocation) -> Self {
        // Exhaustive, so a new RowLocation field cannot be silently dropped
        // from the stored form and from the compare-and-swap.
        let RowLocation {
            data_file_id: _,
            row_position,
            data_sequence_number,
            spec_id,
            partition,
            source_commit_lsn,
            row_version,
            row_fingerprint,
        } = location;
        Self {
            file,
            row_position: *row_position,
            data_sequence_number: *data_sequence_number,
            spec_id: *spec_id,
            partition: Cow::Borrowed(partition),
            source_commit_lsn: *source_commit_lsn,
            row_version: *row_version,
            row_fingerprint: *row_fingerprint,
        }
    }

    pub(crate) fn to_location(&self, path: &str) -> RowLocation {
        RowLocation {
            data_file_id: FileId(path.to_owned()),
            row_position: self.row_position,
            data_sequence_number: self.data_sequence_number,
            spec_id: self.spec_id,
            partition: self.partition.to_vec(),
            source_commit_lsn: self.source_commit_lsn,
            row_version: self.row_version,
            row_fingerprint: self.row_fingerprint,
        }
    }

    pub(crate) fn into_owned(self) -> StoredLocation<'static> {
        StoredLocation {
            partition: Cow::Owned(self.partition.into_owned()),
            ..self
        }
    }
}

/// A staged [`crate::IndexDelta`] in compact form. Its ordinals are allocated
/// in or before the batch that stages it.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredDelta<'a> {
    #[serde(borrow)]
    pub(crate) key: Cow<'a, [u8]>,
    #[serde(borrow)]
    pub(crate) expected: Option<StoredLocation<'a>>,
    #[serde(borrow)]
    pub(crate) replacement: Option<StoredLocation<'a>>,
}

impl StoredDelta<'_> {
    pub(crate) fn into_owned(self) -> StoredDelta<'static> {
        StoredDelta {
            key: Cow::Owned(self.key.into_owned()),
            expected: self.expected.map(StoredLocation::into_owned),
            replacement: self.replacement.map(StoredLocation::into_owned),
        }
    }
}

// Variable-length integers and no trailing bytes: a small position, sequence
// or ordinal takes one or two bytes, and a damaged value fails to decode.
fn codec() -> impl bincode::Options + Copy {
    bincode::DefaultOptions::new()
}

pub(crate) fn encode_location(out: &mut Vec<u8>, location: &StoredLocation<'_>) -> Result<()> {
    out.clear();
    codec().serialize_into(&mut *out, location)?;
    Ok(())
}
pub(crate) fn decode_location(bytes: &[u8]) -> Result<StoredLocation<'_>> {
    Ok(codec().deserialize(bytes)?)
}
pub(crate) fn encode_delta(out: &mut Vec<u8>, delta: &StoredDelta<'_>) -> Result<()> {
    out.clear();
    codec().serialize_into(&mut *out, delta)?;
    Ok(())
}
pub(crate) fn decode_delta(bytes: &[u8]) -> Result<StoredDelta<'_>> {
    Ok(codec().deserialize(bytes)?)
}

/// Reverse entries of one file: table, then the file's ordinal.
pub(crate) fn reverse_prefix(table: &TableId, file: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(size_of::<u32>() + 2 * size_of::<u64>());
    key.extend_from_slice(&table.0.to_be_bytes());
    key.extend_from_slice(&file.to_be_bytes());
    key
}
/// The reverse entry of one physical row: [`reverse_prefix`] and its position.
pub(crate) fn reverse_key(table: &TableId, file: u64, position: u64) -> Vec<u8> {
    let mut key = reverse_prefix(table, file);
    key.extend_from_slice(&position.to_be_bytes());
    key
}

fn path_key(table: &TableId, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + size_of::<u32>() + path.len());
    key.push(BY_PATH);
    key.extend_from_slice(&table.0.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}
fn ordinal_key(marker: u8, table: &TableId, ordinal: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + size_of::<u32>() + size_of::<u64>());
    key.push(marker);
    key.extend_from_slice(&table.0.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}
fn table_marker_prefix(marker: u8, table: &TableId) -> Vec<u8> {
    let mut key = vec![marker];
    key.extend_from_slice(&table.0.to_be_bytes());
    key
}

fn corrupt(what: &str) -> Error {
    Error::RecoveryRequired(format!(
        "data-file ordinal dictionary {what}; rebuild the index"
    ))
}
pub(crate) fn unknown_file() -> Error {
    Error::RecoveryRequired(
        "row index names an unknown data-file ordinal; rebuild the index".into(),
    )
}
fn decode_ordinal(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| corrupt("has a malformed ordinal"))?,
    ))
}
fn parse_ordinal_key(key: &[u8]) -> Result<(TableId, u64)> {
    if key.len() != 1 + size_of::<u32>() + size_of::<u64>() {
        return Err(corrupt("has a malformed key"));
    }
    let table = TableId(u32::from_be_bytes(key[1..5].try_into().expect("length")));
    Ok((
        table,
        u64::from_be_bytes(key[5..].try_into().expect("length")),
    ))
}

/// FxHash-style mixing: ordinals are dense integers and paths are hashed on
/// every staged delta, so SipHash's DoS resistance buys nothing here.
#[derive(Default, Clone, Copy)]
struct FastHasher(u64);
impl FastHasher {
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}
impl Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut words = bytes.chunks_exact(size_of::<u64>());
        for word in &mut words {
            self.add(u64::from_le_bytes(word.try_into().expect("word")));
        }
        let rest = words.remainder();
        if !rest.is_empty() {
            let mut word = [0; size_of::<u64>()];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word));
        }
    }
    fn write_u8(&mut self, value: u8) {
        self.add(u64::from(value));
    }
    fn write_u32(&mut self, value: u32) {
        self.add(u64::from(value));
    }
    fn write_u64(&mut self, value: u64) {
        self.add(value);
    }
    fn write_usize(&mut self, value: usize) {
        self.add(value as u64);
    }
}
type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;

#[derive(Default)]
struct Maps {
    by_path: FastMap<TableId, FastMap<Arc<str>, u64>>,
    by_ordinal: FastMap<u64, (TableId, Arc<str>)>,
}

/// Every durable dictionary entry, in memory. Writers read it under the row
/// lock, where it equals durable state; lock-free readers fall back to their
/// RocksDB view on a miss.
#[derive(Default)]
pub(crate) struct FileOrdinals {
    maps: RwLock<Maps>,
    next: AtomicU64,
}

/// Dictionary changes of one write batch, published once it is written.
#[derive(Default)]
pub(crate) struct FileChanges {
    /// Looked up on every memo miss while a batch is staged.
    added: FastMap<TableId, FastMap<Arc<str>, u64>>,
    released: Vec<(TableId, u64)>,
}

impl FileChanges {
    pub(crate) fn is_empty(&self) -> bool {
        self.added.is_empty() && self.released.is_empty()
    }
}

impl FileOrdinals {
    /// Load and cross-check the whole dictionary. It holds one entry per data
    /// file the index can still name.
    pub(crate) fn load(db: &DB) -> Result<Self> {
        let cf = db.cf_handle(FILES).expect("opened column family");
        let next = db
            .get_cf(&cf, NEXT_ORDINAL)?
            .map(|bytes| decode_ordinal(&bytes))
            .transpose()?
            .unwrap_or(0);
        let mut maps = Maps::default();
        for entry in scan_marker(db, &[BY_ORDINAL], None) {
            let (key, value) = entry?;
            let (table, ordinal) = parse_ordinal_key(&key)?;
            if ordinal >= next {
                return Err(corrupt("has an ordinal beyond its counter"));
            }
            let path: Arc<str> = std::str::from_utf8(&value)
                .map_err(|_| corrupt("has a malformed path"))?
                .into();
            maps.by_ordinal.insert(ordinal, (table, path.clone()));
            if maps
                .by_path
                .entry(table)
                .or_default()
                .insert(path, ordinal)
                .is_some()
            {
                return Err(corrupt("names one file twice"));
            }
        }
        let mut paths = 0usize;
        for entry in scan_marker(db, &[BY_PATH], None) {
            let (key, value) = entry?;
            let table = key
                .get(1..5)
                .map(|bytes| TableId(u32::from_be_bytes(bytes.try_into().expect("length"))))
                .ok_or_else(|| corrupt("has a malformed key"))?;
            let path =
                std::str::from_utf8(&key[5..]).map_err(|_| corrupt("has a malformed path"))?;
            let ordinal = decode_ordinal(&value)?;
            if maps.by_path.get(&table).and_then(|paths| paths.get(path)) != Some(&ordinal) {
                return Err(corrupt("directions disagree"));
            }
            paths += 1;
        }
        if paths != maps.by_ordinal.len() {
            return Err(corrupt("directions disagree"));
        }
        Ok(Self {
            maps: RwLock::new(maps),
            next: AtomicU64::new(next),
        })
    }

    fn read(&self) -> RwLockReadGuard<'_, Maps> {
        // Holders never panic while holding the lock; the maps stay valid.
        self.maps.read().unwrap_or_else(|error| error.into_inner())
    }
    fn write(&self) -> RwLockWriteGuard<'_, Maps> {
        self.maps.write().unwrap_or_else(|error| error.into_inner())
    }

    /// The cached ordinal of a path. Exact for callers holding the row lock.
    pub(crate) fn ordinal(&self, table: TableId, path: &str) -> Option<u64> {
        self.read()
            .by_path
            .get(&table)
            .and_then(|paths| paths.get(path))
            .copied()
    }

    /// The cached path of an ordinal. Exact for callers holding the row lock.
    pub(crate) fn path(&self, table: TableId, ordinal: u64) -> Option<Arc<str>> {
        self.read()
            .by_ordinal
            .get(&ordinal)
            .filter(|(owner, _)| *owner == table)
            .map(|(_, path)| path.clone())
    }

    /// Number of cached entries, for tests and observation.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.read().by_ordinal.len()
    }

    fn publish(&self, changes: FileChanges) {
        let mut maps = self.write();
        for (table, added) in changes.added {
            let paths = maps.by_path.entry(table).or_default();
            let mut ordinals = Vec::with_capacity(added.len());
            for (path, ordinal) in added {
                paths.insert(path.clone(), ordinal);
                ordinals.push((ordinal, path));
            }
            for (ordinal, path) in ordinals {
                maps.by_ordinal.insert(ordinal, (table, path));
            }
        }
        for (table, ordinal) in changes.released {
            if let Some((_, path)) = maps.by_ordinal.remove(&ordinal)
                && let Some(paths) = maps.by_path.get_mut(&table)
            {
                paths.remove(&path);
                if paths.is_empty() {
                    maps.by_path.remove(&table);
                }
            }
        }
    }
}

fn scan_marker<'a>(
    db: &'a DB,
    prefix: &[u8],
    options: Option<ReadOptions>,
) -> impl Iterator<Item = Result<crate::RawEntry>> + 'a {
    let cf = db.cf_handle(FILES).expect("opened column family");
    let mut options = options.unwrap_or_default();
    options.fill_cache(false);
    options.set_iterate_upper_bound(prefix_end(prefix).expect("bounded dictionary prefix"));
    db.iterator_cf_opt(&cf, options, IteratorMode::From(prefix, Direction::Forward))
        .map(|entry| entry.map_err(Error::from))
}

/// The ordinal of `file` in a RocksDB view, for reverse scans. A snapshot
/// reader must see the entry of its snapshot, not the current one.
pub(crate) fn ordinal_in_view(
    db: &DB,
    options: &ReadOptions,
    table: &TableId,
    file: &FileId,
) -> Result<Option<u64>> {
    let cf = db.cf_handle(FILES).expect("opened column family");
    db.get_pinned_cf_opt(&cf, path_key(table, &file.0), options)?
        .map(|bytes| decode_ordinal(&bytes))
        .transpose()
}

/// Resolves ordinals to paths for decoding, from the cache or, for an
/// ordinal released after the caller's view was taken, from that view.
pub(crate) struct PathReader<'a> {
    files: &'a FileOrdinals,
    db: &'a DB,
    view: View<'a>,
    last: Option<(TableId, u64, Arc<str>)>,
}

enum View<'a> {
    Current,
    Borrowed(&'a ReadOptions),
    /// For readers that outlive their caller's frame, such as a stream over
    /// a snapshot it owns.
    Owned(ReadOptions),
}

impl<'a> PathReader<'a> {
    pub(crate) fn new(
        files: &'a FileOrdinals,
        db: &'a DB,
        options: Option<&'a ReadOptions>,
    ) -> Self {
        Self {
            files,
            db,
            view: options.map_or(View::Current, View::Borrowed),
            last: None,
        }
    }

    pub(crate) fn owned(files: &'a FileOrdinals, db: &'a DB, options: ReadOptions) -> Self {
        Self {
            files,
            db,
            view: View::Owned(options),
            last: None,
        }
    }

    /// `None` when the view has no such ordinal.
    pub(crate) fn path(&mut self, table: TableId, ordinal: u64) -> Result<Option<Arc<str>>> {
        if let Some((cached_table, cached, path)) = &self.last
            && *cached_table == table
            && *cached == ordinal
        {
            return Ok(Some(path.clone()));
        }
        let path = match self.files.path(table, ordinal) {
            Some(path) => path,
            None => {
                let cf = self.db.cf_handle(FILES).expect("opened column family");
                let key = ordinal_key(BY_ORDINAL, &table, ordinal);
                let value = match &self.view {
                    View::Current => self.db.get_pinned_cf(&cf, key)?,
                    View::Borrowed(options) => self.db.get_pinned_cf_opt(&cf, key, options)?,
                    View::Owned(options) => self.db.get_pinned_cf_opt(&cf, key, options)?,
                };
                let Some(value) = value else {
                    return Ok(None);
                };
                std::str::from_utf8(&value)
                    .map_err(|_| corrupt("has a malformed path"))?
                    .into()
            }
        };
        self.last = Some((table, ordinal, path.clone()));
        Ok(Some(path))
    }

    /// `None` when the view has no such ordinal.
    pub(crate) fn location(
        &mut self,
        table: TableId,
        stored: &StoredLocation<'_>,
    ) -> Result<Option<RowLocation>> {
        Ok(self
            .path(table, stored.file)?
            .map(|path| stored.to_location(&path)))
    }

    /// Decode a primary-key value whose ordinal must resolve.
    pub(crate) fn pk_value(&mut self, table: TableId, bytes: &[u8]) -> Result<RowLocation> {
        self.location(table, &decode_location(bytes)?)?
            .ok_or_else(unknown_file)
    }
}

/// Recently used ordinals of one table, so staging a batch of deltas that
/// share a few files hashes each path once.
#[derive(Default)]
pub(crate) struct OrdinalMemo {
    recent: [Option<(TableId, Arc<str>, u64)>; 4],
    next: usize,
}

impl OrdinalMemo {
    fn get(&self, table: TableId, path: &str) -> Option<u64> {
        self.recent
            .iter()
            .flatten()
            .find_map(|(owner, cached, ordinal)| {
                (*owner == table && **cached == *path).then_some(*ordinal)
            })
    }
    fn insert(&mut self, table: TableId, path: Arc<str>, ordinal: u64) {
        self.recent[self.next] = Some((table, path, ordinal));
        self.next = (self.next + 1) % self.recent.len();
    }
}

impl StateStore {
    /// The ordinal of `file`, allocated in `batch` if the file has none. The
    /// caller holds the row lock and writes `batch` through `write` or
    /// `write_staged`, which publish the allocation. `memo` must not outlive
    /// a failed write.
    pub(crate) fn file_ordinal(
        &self,
        batch: &mut StateBatch,
        memo: &mut OrdinalMemo,
        table: TableId,
        file: &FileId,
    ) -> Result<u64> {
        if let Some(ordinal) = memo.get(table, &file.0) {
            return Ok(ordinal);
        }
        let known = batch
            .files
            .added
            .get(&table)
            .and_then(|added| added.get_key_value(file.0.as_str()))
            .map(|(path, ordinal)| (path.clone(), *ordinal));
        let (path, ordinal) = match known {
            Some(known) => known,
            None => {
                let path: Arc<str> = file.0.as_str().into();
                match self.0.files.ordinal(table, &path) {
                    Some(ordinal) => (path, ordinal),
                    None => (path.clone(), self.allocate_ordinal(batch, table, path)?),
                }
            }
        };
        memo.insert(table, path, ordinal);
        Ok(ordinal)
    }

    fn allocate_ordinal(
        &self,
        batch: &mut StateBatch,
        table: TableId,
        path: Arc<str>,
    ) -> Result<u64> {
        // Reserved even if the batch is never written: gaps are harmless and
        // an ordinal is never handed out twice.
        let ordinal = self.0.files.next.fetch_add(1, Ordering::Relaxed);
        if ordinal >= NO_FILE - 1 {
            return Err(Error::InvalidState("data-file ordinals exhausted".into()));
        }
        let cf = self.0.db.cf_handle(FILES).expect("opened column family");
        batch.put_cf(&cf, path_key(&table, &path), ordinal.to_be_bytes());
        batch.put_cf(
            &cf,
            ordinal_key(BY_ORDINAL, &table, ordinal),
            path.as_bytes(),
        );
        batch.put_cf(&cf, NEXT_ORDINAL, (ordinal + 1).to_be_bytes());
        // A file that is staged but never applied, as after a discarded
        // publication, is reclaimed by the next sweep.
        batch.put_cf(&cf, ordinal_key(RELEASED, &table, ordinal), []);
        batch
            .files
            .added
            .entry(table)
            .or_default()
            .insert(path, ordinal);
        Ok(ordinal)
    }

    /// Mark `file` for the next sweep, e.g. once it holds no live row. The
    /// caller holds the row lock.
    pub(crate) fn mark_file_released(&self, batch: &mut StateBatch, table: TableId, file: &FileId) {
        if let Some(ordinal) = self.0.files.ordinal(table, &file.0) {
            let cf = self.0.db.cf_handle(FILES).expect("opened column family");
            batch.put_cf(&cf, ordinal_key(RELEASED, &table, ordinal), []);
        }
    }

    /// Release up to one batch of marked ordinals that nothing names. The
    /// caller holds the row lock and passes every ordinal of `table` that an
    /// operation remaining after `batch` pins. Pinned candidates stay marked,
    /// so they are checked again once their operation is gone.
    pub(crate) fn sweep_released_files(
        &self,
        batch: &mut StateBatch,
        table: TableId,
        pinned: &BTreeSet<u64>,
    ) -> Result<()> {
        let db = &self.0.db;
        let files_cf = db.cf_handle(FILES).expect("opened column family");
        let counts_cf = db.cf_handle(FILE_COUNTS).expect("opened column family");
        let remaps_cf = db.cf_handle(REMAPS).expect("opened column family");
        let reverse_cf = db.cf_handle(REVERSE).expect("opened column family");
        // Pinned candidates are skipped without counting, so they cannot
        // starve the others; there are at most as many as there are pins.
        let mut marked = Vec::new();
        for entry in scan_marker(db, &table_marker_prefix(RELEASED, &table), None) {
            let (_, ordinal) = parse_ordinal_key(&entry?.0)?;
            if pinned.contains(&ordinal) {
                continue;
            }
            marked.push(ordinal);
            if marked.len() == self.0.batch_rows {
                break;
            }
        }
        for ordinal in marked {
            batch.delete_cf(&files_cf, ordinal_key(RELEASED, &table, ordinal));
            let Some(path) = self.0.files.path(table, ordinal) else {
                continue;
            };
            let file = FileId(path.to_string());
            // Zero counts are deleted, so any count names live rows. A
            // retired file keeps raw entries until its remap is dropped,
            // which marks it again.
            if db
                .get_pinned_cf(&counts_cf, file_prefix(&table, &file))?
                .is_some()
                || db
                    .get_pinned_cf(&remaps_cf, file_prefix(&table, &file))?
                    .is_some()
            {
                continue;
            }
            // Counts are derived; a reverse entry is the row itself.
            let prefix = reverse_prefix(&table, ordinal);
            let mut options = ReadOptions::default();
            options.set_iterate_upper_bound(prefix_end(&prefix).expect("bounded reverse prefix"));
            let mut rows = db.raw_iterator_cf_opt(&reverse_cf, options);
            rows.seek(&prefix);
            rows.status()?;
            if rows.valid() {
                continue;
            }
            batch.delete_cf(&files_cf, path_key(&table, &path));
            batch.delete_cf(&files_cf, ordinal_key(BY_ORDINAL, &table, ordinal));
            batch.files.released.push((table, ordinal));
        }
        Ok(())
    }

    /// Publish a written batch's dictionary changes, or fence the store if
    /// the write failed: its entries may be durable despite the error.
    pub(crate) fn settle_file_changes(&self, changes: FileChanges, written: bool) {
        if changes.is_empty() {
            return;
        }
        if written {
            self.0.files.publish(changes);
        } else {
            self.0.failed.store(true, Ordering::Release);
        }
    }

    /// A RocksDB-view path reader for decoding stored locations.
    pub(crate) fn path_reader<'a>(&'a self, options: Option<&'a ReadOptions>) -> PathReader<'a> {
        PathReader::new(&self.0.files, &self.0.db, options)
    }

    #[cfg(test)]
    pub(crate) fn cached_ordinal(&self, table: TableId, file: &str) -> Option<u64> {
        self.0.files.ordinal(table, file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        IndexDelta, OperationKind, PK, PreparedOperation, StateStoreOptions,
        tests::SIMULATE_POWER_LOSS,
    };
    use flow_model::{OperationId, PrimaryKey};

    const TABLE: TableId = TableId(4);

    fn long(name: &str) -> String {
        format!(
            "s3://warehouse/{}/data/{name}.parquet",
            "nested-directory/".repeat(12)
        )
    }

    fn location(file: &str, position: u64, version: u64) -> RowLocation {
        RowLocation {
            data_file_id: FileId(file.into()),
            row_position: position,
            data_sequence_number: version as i64,
            spec_id: 0,
            partition: Vec::new(),
            source_commit_lsn: PgLsn(version),
            row_version: version,
            row_fingerprint: [version as u8; 16],
        }
    }

    fn key(value: u8) -> PrimaryKey {
        PrimaryKey(vec![value])
    }

    fn operation(store: &StateStore, table: TableId, name: &str) -> PreparedOperation {
        let state = store.table_state(&table).unwrap();
        PreparedOperation {
            id: OperationId(name.into()),
            table_id: table,
            kind: OperationKind::Ingest,
            base_snapshot_id: state.snapshot_id,
            last_lsn: PgLsn(state.snapshot_id.unwrap_or(0) as u64 + 1),
            schema_version: 1,
            artifacts: Vec::new(),
            payload: Vec::new(),
        }
    }

    /// Publish, apply and retire one operation, as an ingest epoch does.
    fn commit(store: &StateStore, table: TableId, name: &str, deltas: Vec<IndexDelta>) {
        let operation = operation(store, table, name);
        let snapshot = operation.base_snapshot_id.unwrap_or(0) + 1;
        let id = operation.id.clone();
        store.prepare(operation, deltas).unwrap();
        store.mark_committed(&id, snapshot, snapshot).unwrap();
        assert!(store.apply_committed(&id).unwrap().complete);
        store.forget_applied(&id).unwrap();
    }

    fn insert(value: u8, at: RowLocation) -> IndexDelta {
        IndexDelta {
            key: key(value),
            expected: None,
            replacement: Some(at),
        }
    }

    fn rows(store: &StateStore, table: TableId, file: &str) -> Vec<(u64, PrimaryKey)> {
        store
            .file_rows(&table, &FileId(file.into()))
            .collect::<Result<_>>()
            .unwrap()
    }

    fn durable_ordinal(store: &StateStore, table: TableId, file: &str) -> Option<u64> {
        let cf = store.0.db.cf_handle(FILES).unwrap();
        store
            .0
            .db
            .get_cf(&cf, path_key(&table, file))
            .unwrap()
            .map(|bytes| decode_ordinal(&bytes).unwrap())
    }

    #[test]
    fn ordinals_are_compact_durable_and_survive_reopen_and_power_loss() {
        let directory = tempfile::tempdir().unwrap();
        let (a, b, c) = (long("a"), long("b"), long("c"));
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            TABLE,
            "a",
            (0..3)
                .map(|value| insert(value, location(&a, value.into(), 1)))
                .collect(),
        );
        let first = store.cached_ordinal(TABLE, &a).unwrap();
        assert_eq!(durable_ordinal(&store, TABLE, &a), Some(first));
        // The row value no longer carries the path.
        let pk = store.0.db.cf_handle(PK).unwrap();
        let value = store
            .0
            .db
            .get_cf(&pk, crate::pk_key(&TABLE, &key(1)))
            .unwrap()
            .unwrap();
        assert!(a.len() > 200 && value.len() < 40, "{} bytes", value.len());
        drop(pk);
        drop(store);

        SIMULATE_POWER_LOSS.set(true);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(store.cached_ordinal(TABLE, &a), Some(first));
        assert_eq!(
            store.lookup(&TABLE, &key(2)).unwrap(),
            Some(location(&a, 2, 1))
        );
        // A later file never reuses a durable ordinal.
        commit(&store, TABLE, "b", vec![insert(7, location(&b, 0, 2))]);
        let second = store.cached_ordinal(TABLE, &b).unwrap();
        assert!(second > first);
        // Staging allocates without a sync; power loss drops the allocation
        // together with the deltas that named it.
        let staged = operation(&store, TABLE, "c");
        let id = staged.id.clone();
        store.begin_prepare(staged).unwrap();
        store
            .stage_deltas(&id, [insert(8, location(&c, 0, 3))])
            .unwrap();
        assert!(store.cached_ordinal(TABLE, &c).is_some());
        let image = tempfile::tempdir().unwrap();
        std::fs::remove_dir(image.path()).unwrap();
        crate::control::copy_directory_tree(directory.path(), image.path()).unwrap();
        drop(store);
        SIMULATE_POWER_LOSS.set(false);

        let store = StateStore::open(image.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(store.cached_ordinal(TABLE, &c), None);
        assert_eq!(durable_ordinal(&store, TABLE, &c), None);
        assert_eq!(store.operation(&id).unwrap().unwrap().delta_count, 0);
        store
            .stage_deltas(&id, [insert(8, location(&c, 0, 3))])
            .unwrap();
        store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            store
                .prepared_deltas(&id)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            [insert(8, location(&c, 0, 3))]
        );
        store.mark_committed(&id, 3, 3).unwrap();
        store.apply_committed(&id).unwrap();
        assert!(store.cached_ordinal(TABLE, &c).unwrap() > first);
        assert_eq!(rows(&store, TABLE, &a).len(), 3);
        assert_eq!(rows(&store, TABLE, &b), [(0, key(7))]);
        assert_eq!(rows(&store, TABLE, &c), [(0, key(8))]);
        assert_eq!(
            store.lookup(&TABLE, &key(8)).unwrap(),
            Some(location(&c, 0, 3))
        );
        store.validate_storage().unwrap();
    }

    #[test]
    fn reverse_scans_cover_exactly_one_file_of_one_table() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        // Paths that are prefixes of each other, shared by two tables.
        let files = ["a", "ab", "a\0", "b"];
        let value = |index: usize, position: u64| {
            (index * 8) as u8
                + match position {
                    9 => 0,
                    0 => 1,
                    300 => 2,
                    _ => 3,
                }
        };
        for table in [TableId(1), TableId(2)] {
            let mut deltas = Vec::new();
            for (index, file) in files.iter().enumerate() {
                for position in [9, 0, 300, 2] {
                    deltas.push(insert(value(index, position), location(file, position, 1)));
                }
            }
            commit(&store, table, &format!("seed-{}", table.0), deltas);
        }
        let snapshot = store.index_snapshot(&TableId(1), Some(1)).unwrap();
        // A later change to the second table is invisible to the first's scans.
        commit(
            &store,
            TableId(2),
            "more",
            vec![insert(200, location("a", 1, 2))],
        );
        for (index, file) in files.iter().enumerate() {
            let expected: Vec<_> = [0u64, 2, 9, 300]
                .into_iter()
                .map(|position| (position, key(value(index, position))))
                .collect();
            assert_eq!(rows(&store, TableId(1), file), expected, "{file:?}");
            assert_eq!(
                crate::RowIndex::file_rows(&snapshot, &TableId(1), &FileId((*file).into()))
                    .collect::<Result<Vec<_>>>()
                    .unwrap(),
                expected
            );
        }
        assert_eq!(rows(&store, TableId(2), "a").len(), 5);
        assert!(rows(&store, TableId(1), "missing").is_empty());
        assert!(rows(&store, TableId(3), "a").is_empty());
    }

    #[test]
    fn ordinals_are_released_only_when_nothing_names_them() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            TABLE,
            "seed",
            vec![
                insert(0, location("a", 0, 1)),
                insert(1, location("a", 1, 1)),
            ],
        );
        let old = store.cached_ordinal(TABLE, "a").unwrap();
        let before = store.index_snapshot(&TABLE, Some(1)).unwrap();

        // Every row of `a` moves to `b`; `a` has no live row afterwards.
        let moved = |value: u8| IndexDelta {
            key: key(value),
            expected: Some(location("a", value.into(), 1)),
            replacement: Some(location("b", value.into(), 2)),
        };
        let relocation = operation(&store, TABLE, "move");
        let move_id = relocation.id.clone();
        store.prepare(relocation, [moved(0), moved(1)]).unwrap();
        store.mark_committed(&move_id, 2, 2).unwrap();
        store.apply_committed(&move_id).unwrap();
        // A pending operation pins only the ordinals its staged deltas name:
        // `d` has no row yet, while `a` is released despite the pending record.
        let pending = operation(&store, TABLE, "pending");
        let pending_id = pending.id.clone();
        store.begin_prepare(pending).unwrap();
        store
            .stage_deltas(&pending_id, [insert(5, location("d", 0, 3))])
            .unwrap();
        store.forget_applied(&move_id).unwrap();
        assert_eq!(store.cached_ordinal(TABLE, "a"), None);
        assert_eq!(durable_ordinal(&store, TABLE, "a"), None);
        assert!(store.cached_ordinal(TABLE, "d").is_some());
        store.discard_uncommitted(&pending_id).unwrap();

        // Discarding drops the pin; the next retirement sweeps `d`.
        commit(&store, TABLE, "later", vec![insert(9, location("c", 0, 3))]);
        assert_eq!(store.cached_ordinal(TABLE, "d"), None);
        assert_eq!(durable_ordinal(&store, TABLE, "d"), None);
        assert!(store.cached_ordinal(TABLE, "b").is_some());
        assert!(store.cached_ordinal(TABLE, "c").is_some());
        assert_eq!(store.0.files.len(), 2);
        assert_eq!(
            store
                .lookup_many(&TABLE, &[key(0), key(1), key(9)])
                .unwrap(),
            [
                Some(location("b", 0, 2)),
                Some(location("b", 1, 2)),
                Some(location("c", 0, 3))
            ]
        );
        // A snapshot from before the release still resolves the old ordinal.
        assert_eq!(
            before.lookup_keys(&TABLE, [&key(0), &key(1)]).unwrap(),
            [Some(location("a", 0, 1)), Some(location("a", 1, 1))]
        );
        assert_eq!(
            crate::RowIndex::file_rows(&before, &TABLE, &FileId("a".into()))
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            [(0, key(0)), (1, key(1))]
        );
        drop(before);

        // A released path that returns gets a fresh ordinal.
        commit(&store, TABLE, "again", vec![insert(4, location("a", 0, 4))]);
        let new = store.cached_ordinal(TABLE, "a").unwrap();
        assert!(new > old);
        assert_eq!(rows(&store, TABLE, "a"), [(0, key(4))]);
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(store.cached_ordinal(TABLE, "a"), Some(new));
        assert_eq!(store.0.files.len(), 3);
        assert_eq!(
            store.lookup(&TABLE, &key(4)).unwrap(),
            Some(location("a", 0, 4))
        );
    }

    /// Epochs overlap as in the daemon: each operation is retired while the
    /// next one's record and staged deltas exist. Release must still happen.
    #[test]
    fn release_keeps_up_while_operation_records_overlap() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let file = |epoch: u64| long(&format!("epoch-{epoch}"));
        let moved = |value: u8, epoch: u64| IndexDelta {
            key: key(value),
            expected: Some(location(&file(epoch - 1), value.into(), epoch)),
            replacement: Some(location(&file(epoch), value.into(), epoch + 1)),
        };
        commit(
            &store,
            TABLE,
            "seed",
            vec![
                insert(0, location(&file(0), 0, 1)),
                insert(1, location(&file(0), 1, 1)),
            ],
        );
        let prepare = |epoch: u64| {
            let operation = operation(&store, TABLE, &format!("epoch-{epoch}"));
            let id = operation.id.clone();
            store
                .prepare(operation, [moved(0, epoch), moved(1, epoch)])
                .unwrap();
            id
        };
        let mut applied = None;
        for epoch in 1..=6 {
            // The next epoch is staged before the previous one is retired.
            let id = prepare(epoch);
            if let Some(previous) = applied.take() {
                store.forget_applied(&previous).unwrap();
                // Nothing names the file emptied by the retired epoch; the
                // staged deltas pin the current file and the new, empty one.
                assert_eq!(store.cached_ordinal(TABLE, &file(epoch - 2)), None);
                assert_eq!(durable_ordinal(&store, TABLE, &file(epoch - 2)), None);
                assert!(store.cached_ordinal(TABLE, &file(epoch - 1)).is_some());
                assert!(store.cached_ordinal(TABLE, &file(epoch)).is_some());
                assert_eq!(store.0.files.len(), 2);
            }
            let snapshot = epoch as i64 + 1;
            store.mark_committed(&id, snapshot, snapshot).unwrap();
            assert!(store.apply_committed(&id).unwrap().complete);
            applied = Some(id);
        }
        store.forget_applied(&applied.unwrap()).unwrap();
        assert_eq!(store.0.files.len(), 1);
        assert_eq!(
            store.lookup_many(&TABLE, &[key(0), key(1)]).unwrap(),
            [
                Some(location(&file(6), 0, 7)),
                Some(location(&file(6), 1, 7))
            ]
        );
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(store.0.files.len(), 1);
        assert_eq!(rows(&store, TABLE, &file(6)), [(0, key(0)), (1, key(1))]);
        store.validate_storage().unwrap();
    }

    #[test]
    fn a_prepared_delta_without_its_pin_requires_a_rebuild() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let operation = operation(&store, TABLE, "pinned");
        let id = operation.id.clone();
        store
            .prepare(operation, vec![insert(0, location("a", 0, 1))])
            .unwrap();
        let ordinal = store.cached_ordinal(TABLE, "a").unwrap();
        let cf = store.0.db.cf_handle(crate::DELTAS).unwrap();
        store
            .0
            .db
            .delete_cf(&cf, crate::file_pin_key(&id, ordinal))
            .unwrap();
        drop(cf);
        drop(store);
        let Err(error) = StateStore::open(directory.path(), StateStoreOptions::default()) else {
            panic!("a staged ordinal without a pin was accepted");
        };
        assert!(error.requires_index_rebuild(), "{error}");
    }

    #[test]
    fn an_inconsistent_dictionary_requires_a_rebuild() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(&store, TABLE, "seed", vec![insert(0, location("a", 0, 1))]);
        let ordinal = store.cached_ordinal(TABLE, "a").unwrap();
        let cf = store.0.db.cf_handle(FILES).unwrap();
        store
            .0
            .db
            .delete_cf(&cf, ordinal_key(BY_ORDINAL, &TABLE, ordinal))
            .unwrap();
        drop(cf);
        drop(store);
        let Err(error) = StateStore::open(directory.path(), StateStoreOptions::default()) else {
            panic!("a one-sided dictionary entry was accepted");
        };
        assert!(error.requires_index_rebuild(), "{error}");
    }
}
