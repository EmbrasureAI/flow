//! Lazy row-index remaps for data rewrites (experimental, `lazy_index_remap`).
//!
//! A rewrite that writes the survivors of each input file in input order can
//! describe every moved row with O(files + deleted positions) runs instead of
//! one index delta per row. The final apply batch of such a rewrite retires
//! each input file by storing its [`FileRemap`] and moves the per-file live
//! counts arithmetically; primary-key and reverse entries keep their original
//! (raw) location.
//!
//! Every forward read (`lookup`, `lookup_many`, snapshot lookups, collapse
//! binding and the apply compare-and-swap) translates a raw location whose
//! file is retired, following chained remaps, to the location the per-row apply
//! would have stored. Snapshot readers consult only remaps visible in their
//! RocksDB snapshot. Reverse entries stay at the raw location; callers that need
//! an exact reverse scan of a file first call [`StateStore::materialize_remaps`],
//! which rewrites the remaining raw entries per row and drops the remaps.

use crate::{
    DELTAS, Error, FILE_COUNTS, OUTPUT_COUNT_MARKER, OperationKind, OperationPhase, PK,
    REMAP_MARKER, REMAPS, REVERSE, Result, StateBatch, StateStore, apply::FileCountShadow, decode,
    file_prefix, operation_prefix, pk_key, reverse_key,
};
use flow_model::{FileId, OperationId, PrimaryKey, RowLocation, TableId};
use rocksdb::{DB, IteratorMode, ReadOptions};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// Bound chained translation; each hop is one completed rewrite of a file
/// whose row was never touched by CDC since.
const MAX_REMAP_HOPS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemapTarget {
    pub data_file_id: FileId,
    pub data_sequence_number: i64,
    pub spec_id: i32,
    pub partition: Vec<u8>,
}

/// `len` consecutive old positions starting at `old_start` became consecutive
/// positions starting at `new_start` of `targets[target]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemapRun {
    pub old_start: u64,
    pub new_start: u64,
    pub len: u64,
    pub target: u32,
}

/// Where every surviving row of one retired data file now lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRemap {
    pub source: FileId,
    pub targets: Vec<RemapTarget>,
    /// Sorted by `old_start`, non-overlapping.
    pub runs: Vec<RemapRun>,
}

impl FileRemap {
    pub fn new(source: FileId) -> Self {
        Self {
            source,
            targets: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// Append the next surviving row in input order. The replacement's data
    /// sequence is the output's committed one, supplied separately.
    pub fn push(
        &mut self,
        old_position: u64,
        replacement: &RowLocation,
        data_sequence_number: i64,
    ) -> Result<()> {
        if let Some(last) = self.runs.last()
            && old_position < last.old_start + last.len
        {
            return Err(Error::InvalidState(
                "lazy remap rows must arrive in input order".into(),
            ));
        }
        let target = match self
            .targets
            .iter()
            .rposition(|target| target.data_file_id == replacement.data_file_id)
        {
            Some(index) => index,
            None => {
                self.targets.push(RemapTarget {
                    data_file_id: replacement.data_file_id.clone(),
                    data_sequence_number,
                    spec_id: replacement.spec_id,
                    partition: replacement.partition.clone(),
                });
                self.targets.len() - 1
            }
        } as u32;
        if let Some(last) = self.runs.last_mut()
            && last.target == target
            && last.old_start + last.len == old_position
            && last.new_start + last.len == replacement.row_position
        {
            last.len += 1;
        } else {
            self.runs.push(RemapRun {
                old_start: old_position,
                new_start: replacement.row_position,
                len: 1,
                target,
            });
        }
        Ok(())
    }

    /// Number of rows this remap moves, i.e. survivors at the rewrite base.
    pub fn survivors(&self) -> u64 {
        self.runs.iter().map(|run| run.len).sum()
    }

    /// Output target and position of an old position, if it survived.
    pub fn translate_position(&self, position: u64) -> Option<(&RemapTarget, u64)> {
        let index = self
            .runs
            .partition_point(|run| run.old_start <= position)
            .checked_sub(1)?;
        let run = &self.runs[index];
        (position < run.old_start + run.len).then(|| {
            (
                &self.targets[run.target as usize],
                run.new_start + (position - run.old_start),
            )
        })
    }

    /// The location a per-row apply would have stored for this raw location.
    pub fn translate(&self, location: &RowLocation) -> Option<RowLocation> {
        let (target, position) = self.translate_position(location.row_position)?;
        Some(RowLocation {
            data_file_id: target.data_file_id.clone(),
            row_position: position,
            data_sequence_number: target.data_sequence_number,
            spec_id: target.spec_id,
            partition: target.partition.clone(),
            source_commit_lsn: location.source_commit_lsn,
            row_version: location.row_version,
            row_fingerprint: location.row_fingerprint,
        })
    }

    fn validate(&self) -> Result<()> {
        let ordered = self
            .runs
            .windows(2)
            .all(|pair| pair[0].old_start + pair[0].len <= pair[1].old_start);
        if !ordered
            || self
                .runs
                .iter()
                .any(|run| run.len == 0 || run.target as usize >= self.targets.len())
        {
            return Err(Error::InvalidState("malformed lazy remap".into()));
        }
        Ok(())
    }
}

/// Everything a lazy rewrite stages instead of per-row deltas.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LazyRemap {
    pub files: Vec<FileRemap>,
    /// Live rows of each output file at the commit head (written minus masked).
    pub output_live_rows: Vec<(FileId, u64)>,
}

/// Decoded remaps are immutable once committed. Visibility is still decided
/// by a read in the caller's view; the cache only avoids decoding again.
#[derive(Default)]
pub(crate) struct RemapCache(Mutex<HashMap<(TableId, FileId), Arc<FileRemap>>>);

/// One reader's view of retired files, memoized per call.
pub(crate) struct Resolver<'a> {
    db: &'a DB,
    cache: &'a RemapCache,
    table: TableId,
    options: &'a ReadOptions,
    memo: HashMap<FileId, Option<Arc<FileRemap>>>,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(
        db: &'a DB,
        cache: &'a RemapCache,
        table: TableId,
        options: &'a ReadOptions,
    ) -> Self {
        Self {
            db,
            cache,
            table,
            options,
            memo: HashMap::new(),
        }
    }

    fn remap(&mut self, file: &FileId) -> Result<Option<Arc<FileRemap>>> {
        if let Some(remap) = self.memo.get(file) {
            return Ok(remap.clone());
        }
        let cf = self.db.cf_handle(REMAPS).expect("opened column family");
        let remap =
            match self
                .db
                .get_pinned_cf_opt(&cf, file_prefix(&self.table, file), self.options)?
            {
                None => None,
                Some(bytes) => {
                    let key = (self.table, file.clone());
                    let cached = self
                        .cache
                        .0
                        .lock()
                        .map_err(|_| Error::Poisoned)?
                        .get(&key)
                        .cloned();
                    Some(match cached {
                        Some(remap) => remap,
                        None => {
                            let remap = Arc::new(decode::<FileRemap>(&bytes)?);
                            self.cache
                                .0
                                .lock()
                                .map_err(|_| Error::Poisoned)?
                                .insert(key, remap.clone());
                            remap
                        }
                    })
                }
            };
        self.memo.insert(file.clone(), remap.clone());
        Ok(remap)
    }

    /// Translate through every visible remap. `None` means the raw file is live.
    pub(crate) fn resolve(&mut self, raw: &RowLocation) -> Result<Option<RowLocation>> {
        let mut current: Option<RowLocation> = None;
        for _ in 0..MAX_REMAP_HOPS {
            let location = current.as_ref().unwrap_or(raw);
            let Some(remap) = self.remap(&location.data_file_id)? else {
                return Ok(current);
            };
            let next = remap.translate(location).ok_or_else(|| {
                Error::RecoveryRequired(
                    "row index points at a row a lazy remap did not move; rebuild the index".into(),
                )
            })?;
            current = Some(next);
        }
        Err(Error::RecoveryRequired(
            "lazy remap chain exceeds its hop limit; rebuild the index".into(),
        ))
    }

    /// Translate in place, returning whether the location changed.
    pub(crate) fn resolve_in_place(&mut self, location: &mut RowLocation) -> Result<bool> {
        match self.resolve(location)? {
            Some(translated) => {
                *location = translated;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

fn staged_key(id: &OperationId, marker: u8, ordinal: u64) -> Vec<u8> {
    let mut key = operation_prefix(id);
    key.push(marker);
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn staged_prefix(id: &OperationId, marker: u8) -> Vec<u8> {
    let mut key = operation_prefix(id);
    key.push(marker);
    key
}

impl StateStore {
    /// Whether any committed lazy remap may exist in this generation.
    pub(crate) fn remaps_possible(&self) -> bool {
        self.0.remaps.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether the table currently has committed lazy remaps.
    pub fn has_remaps(&self, table: &TableId) -> Result<bool> {
        if !self.remaps_possible() {
            return Ok(false);
        }
        Ok(self
            .scan(REMAPS, table.0.to_be_bytes().to_vec())
            .next()
            .transpose()?
            .is_some())
    }

    /// Stage a lazy remap in place of row deltas for a Building rewrite. The
    /// final apply batch commits it atomically with the table transition.
    pub fn stage_remap(&self, id: &OperationId, remap: &LazyRemap) -> Result<()> {
        let _guard = self.lock()?;
        let record = self.require_operation(id)?;
        if record.phase != OperationPhase::Building
            || record.operation.kind != OperationKind::Rewrite
        {
            return Err(Error::InvalidState(
                "a lazy remap requires a Building rewrite".into(),
            ));
        }
        if self
            .scan(DELTAS, staged_prefix(id, REMAP_MARKER))
            .next()
            .is_some()
        {
            return Err(Error::InvalidState("lazy remap is already staged".into()));
        }
        let mut sources = std::collections::BTreeSet::new();
        for file in &remap.files {
            file.validate()?;
            if !sources.insert(&file.source) {
                return Err(Error::InvalidState("duplicate lazy remap source".into()));
            }
        }
        let cf = self.0.db.cf_handle(DELTAS).expect("opened column family");
        let mut batch = StateBatch::default();
        for (ordinal, file) in remap.files.iter().enumerate() {
            batch.put_cf(
                &cf,
                staged_key(id, REMAP_MARKER, ordinal as u64),
                bincode::serialize(file)?,
            );
        }
        batch.put_cf(
            &cf,
            staged_key(id, OUTPUT_COUNT_MARKER, 0),
            bincode::serialize(&remap.output_live_rows)?,
        );
        self.write_staged(batch)
    }

    /// Staged lazy remap of an operation, if any.
    pub(crate) fn staged_remap(&self, id: &OperationId) -> Result<Option<LazyRemap>> {
        let mut files = Vec::new();
        for entry in self.scan(DELTAS, staged_prefix(id, REMAP_MARKER)) {
            let (_, value) = entry?;
            files.push(decode::<FileRemap>(&value)?);
        }
        let counts = self
            .scan(DELTAS, staged_prefix(id, OUTPUT_COUNT_MARKER))
            .next()
            .transpose()?;
        match counts {
            None if files.is_empty() => Ok(None),
            None => Err(Error::RecoveryRequired(
                "staged lazy remap lacks output counts".into(),
            )),
            Some((_, value)) => Ok(Some(LazyRemap {
                files,
                output_live_rows: decode(&value)?,
            })),
        }
    }

    /// Commit a staged remap into `batch`: retire each input file and move its
    /// live-row count to the outputs. The input counts at this exact fenced
    /// head must equal the output counts the caller derived at the same head.
    pub(crate) fn commit_remap(
        &self,
        batch: &mut StateBatch,
        table: &TableId,
        remap: &LazyRemap,
        counts: &mut BTreeMap<FileId, FileCountShadow>,
    ) -> Result<()> {
        let counts_cf = self
            .0
            .db
            .cf_handle(FILE_COUNTS)
            .expect("opened column family");
        let remaps_cf = self.0.db.cf_handle(REMAPS).expect("opened column family");
        let current = |file: &FileId, counts: &mut BTreeMap<FileId, FileCountShadow>| {
            if let Some(count) = counts.get(file) {
                return Ok::<_, Error>(count.current);
            }
            let value = self
                .0
                .db
                .get_cf(&counts_cf, file_prefix(table, file))?
                .map(|bytes| crate::decode_file_count(&bytes))
                .transpose()?
                .unwrap_or(0);
            counts.insert(
                file.clone(),
                FileCountShadow {
                    initial: value,
                    current: value,
                },
            );
            Ok(value)
        };
        let mut moved = 0u64;
        for file in &remap.files {
            let existing = self
                .0
                .db
                .get_pinned_cf(&remaps_cf, file_prefix(table, &file.source))?;
            if existing.is_some() {
                return Err(Error::InvalidState("data file is already remapped".into()));
            }
            moved = moved
                .checked_add(current(&file.source, counts)?)
                .ok_or_else(|| Error::InvalidState("live count overflow".into()))?;
            counts.get_mut(&file.source).expect("loaded count").current = 0;
            batch.put_cf(
                &remaps_cf,
                file_prefix(table, &file.source),
                bincode::serialize(file)?,
            );
        }
        let mut added = 0u64;
        for (file, rows) in &remap.output_live_rows {
            let previous = current(file, counts)?;
            added = added
                .checked_add(*rows)
                .ok_or_else(|| Error::InvalidState("live count overflow".into()))?;
            counts.get_mut(file).expect("loaded count").current = previous
                .checked_add(*rows)
                .ok_or_else(|| Error::InvalidState("live count overflow".into()))?;
        }
        if moved != added {
            return Err(Error::RecoveryRequired(format!(
                "lazy remap moves {moved} indexed rows but outputs hold {added}; rebuild the index"
            )));
        }
        // Publish before the write so a snapshot captured afterwards consults remaps.
        self.0
            .remaps
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Rewrite every raw entry of the table's retired files to its translated
    /// location and drop the table's remaps, restoring exact reverse scans.
    /// Progress is crash-safe: each batch is a self-consistent index rewrite and
    /// remaps are deleted only after no raw entry needs them.
    pub fn materialize_remaps(&self, table: &TableId) -> Result<u64> {
        if !self.has_remaps(table)? {
            return Ok(0);
        }
        let mut retired = Vec::new();
        for entry in self.scan(REMAPS, table.0.to_be_bytes().to_vec()) {
            let (key, value) = entry?;
            retired.push((key, decode::<FileRemap>(&value)?.source));
        }
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let reverse_cf = self.0.db.cf_handle(REVERSE).expect("opened column family");
        let mut rewritten = 0u64;
        for (_, file) in &retired {
            loop {
                // Scan under the row lock so concurrent applies cannot move a row
                // between this read and the rewrite below.
                let _guard = self.lock()?;
                let rows: Vec<(u64, PrimaryKey)> =
                    crate::reader::file_rows_raw(&self.0.db, table, file, ReadOptions::default())
                        .take(self.0.batch_rows)
                        .collect::<Result<_>>()?;
                if rows.is_empty() {
                    break;
                }
                let keys: Vec<_> = rows.iter().map(|(_, key)| pk_key(table, key)).collect();
                let current = self.0.db.batched_multi_get_cf(&pk_cf, keys.iter(), false);
                let options = ReadOptions::default();
                let mut resolver = Resolver::new(&self.0.db, &self.0.remap_cache, *table, &options);
                let mut batch = StateBatch::default();
                for (((position, key), pk), current) in rows.iter().zip(&keys).zip(current) {
                    let raw = reverse_key(
                        table,
                        &RowLocation {
                            data_file_id: file.clone(),
                            row_position: *position,
                            data_sequence_number: 0,
                            spec_id: 0,
                            partition: Vec::new(),
                            source_commit_lsn: Default::default(),
                            row_version: 0,
                            row_fingerprint: [0; 16],
                        },
                    );
                    let Some(current) = current? else {
                        return Err(Error::RecoveryRequired(
                            "reverse index has no forward entry; rebuild the index".into(),
                        ));
                    };
                    let location: RowLocation = decode(&current)?;
                    if &location.data_file_id != file || location.row_position != *position {
                        return Err(Error::RecoveryRequired(
                            "forward and reverse index disagree; rebuild the index".into(),
                        ));
                    }
                    let translated = resolver
                        .resolve(&location)?
                        .ok_or_else(|| Error::InvalidState("retired file lost its remap".into()))?;
                    batch.delete_cf(&reverse_cf, raw);
                    batch.put_cf(&pk_cf, pk, bincode::serialize(&translated)?);
                    batch.put_cf(
                        &reverse_cf,
                        reverse_key(table, &translated),
                        bincode::serialize(key)?,
                    );
                    rewritten += 1;
                }
                self.write_staged(batch)?;
            }
        }
        let _guard = self.lock()?;
        let remaps_cf = self.0.db.cf_handle(REMAPS).expect("opened column family");
        let mut batch = StateBatch::default();
        for (key, _) in &retired {
            batch.delete_cf(&remaps_cf, key);
        }
        self.write(batch)?;
        let mut cache = self.0.remap_cache.0.lock().map_err(|_| Error::Poisoned)?;
        for (_, file) in retired {
            cache.remove(&(*table, file));
        }
        Ok(rewritten)
    }
}

/// Whether the REMAPS family has any record, for reopening a generation.
pub(crate) fn any_remaps(db: &DB) -> Result<bool> {
    let cf = db.cf_handle(REMAPS).expect("opened column family");
    let mut iterator = db.iterator_cf(&cf, IteratorMode::Start);
    Ok(iterator.next().transpose()?.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexDelta, PreparedOperation, StateStoreOptions};
    use flow_model::PgLsn;

    const TABLE: TableId = TableId(9);

    fn location(file: &str, position: u64, sequence: i64, version: u64) -> RowLocation {
        RowLocation {
            data_file_id: FileId(file.into()),
            row_position: position,
            data_sequence_number: sequence,
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

    fn operation(store: &StateStore, name: &str, kind: OperationKind) -> PreparedOperation {
        let state = store.table_state(&TABLE).unwrap();
        PreparedOperation {
            id: OperationId(name.into()),
            table_id: TABLE,
            kind,
            base_snapshot_id: state.snapshot_id,
            last_lsn: PgLsn(state.snapshot_id.unwrap_or(0) as u64 + 1),
            schema_version: 1,
            artifacts: Vec::new(),
            payload: Vec::new(),
        }
    }

    /// Commit one operation at the next snapshot, mirroring publication.
    fn commit(
        store: &StateStore,
        name: &str,
        kind: OperationKind,
        deltas: Vec<IndexDelta>,
        remap: Option<&LazyRemap>,
    ) {
        let operation = operation(store, name, kind);
        let snapshot = operation.base_snapshot_id.unwrap_or(0) + 1;
        let id = operation.id.clone();
        store.begin_prepare(operation).unwrap();
        store.stage_deltas(&id, deltas).unwrap();
        if let Some(remap) = remap {
            store.stage_remap(&id, remap).unwrap();
        }
        store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
        store.mark_committed(&id, snapshot, snapshot).unwrap();
        assert!(store.apply_committed(&id).unwrap().complete);
        store.forget_applied(&id).unwrap();
    }

    /// Survivors of `source` in order, written from position 0 of `target`
    /// with a rotation to `rotated` after `split` rows.
    fn remap_of(
        source: &str,
        survivors: &[u64],
        target: &str,
        rotated: &str,
        split: usize,
        sequence: i64,
    ) -> FileRemap {
        let mut remap = FileRemap::new(FileId(source.into()));
        for (index, position) in survivors.iter().enumerate() {
            let replacement = if index < split {
                location(target, index as u64, -5, 0)
            } else {
                location(rotated, (index - split) as u64, -5, 0)
            };
            remap.push(*position, &replacement, sequence).unwrap();
        }
        remap
    }

    #[test]
    fn translation_skips_base_deletes_and_follows_output_rotation() {
        let remap = remap_of("a", &[0, 1, 2, 4, 5, 8], "b", "c", 4, 7);
        // Runs break at the deleted positions 3, 6, 7 and at the rotation.
        assert_eq!(remap.runs.len(), 4);
        assert_eq!(remap.survivors(), 6);
        let at = |position| {
            remap.translate(&location("a", position, 1, 3)).map(|row| {
                (
                    row.data_file_id.0,
                    row.row_position,
                    row.data_sequence_number,
                )
            })
        };
        assert_eq!(at(0), Some(("b".into(), 0, 7)));
        assert_eq!(at(2), Some(("b".into(), 2, 7)));
        assert_eq!(at(3), None);
        assert_eq!(at(4), Some(("b".into(), 3, 7)));
        assert_eq!(at(5), Some(("c".into(), 0, 7)));
        assert_eq!(at(6), None);
        assert_eq!(at(8), Some(("c".into(), 1, 7)));
        assert_eq!(at(9), None);
        let translated = remap.translate(&location("a", 4, 1, 3)).unwrap();
        assert_eq!(translated.row_version, 3);
        assert_eq!(translated.source_commit_lsn, PgLsn(3));
        assert_eq!(translated.row_fingerprint, [3; 16]);
        let mut unordered = FileRemap::new(FileId("a".into()));
        unordered.push(5, &location("b", 0, 0, 0), 1).unwrap();
        assert!(unordered.push(5, &location("b", 1, 0, 0), 1).is_err());
    }

    #[test]
    fn per_row_rewrite_with_dense_markers_is_not_a_lazy_remap() {
        // An unordered second batch switches the operation to dense staging
        // markers. Their flag must not read as a staged remap at apply.
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            "insert",
            OperationKind::Ingest,
            (0..4)
                .map(|position| IndexDelta {
                    key: key(position as u8),
                    expected: None,
                    replacement: Some(location("a", position, 1, 1)),
                })
                .collect(),
            None,
        );
        let moved = |position: u64| IndexDelta {
            key: key(position as u8),
            expected: Some(location("a", position, 1, 1)),
            replacement: Some(location("b", position, 2, 1)),
        };
        let operation = operation(&store, "rewrite", OperationKind::Rewrite);
        let id = operation.id.clone();
        store.begin_prepare(operation).unwrap();
        store.stage_deltas(&id, vec![moved(2), moved(3)]).unwrap();
        store.stage_deltas(&id, vec![moved(0), moved(1)]).unwrap();
        assert!(store.staged_remap(&id).unwrap().is_none());
        store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
        store.mark_committed(&id, 2, 2).unwrap();
        assert!(store.apply_committed(&id).unwrap().complete);
        store.forget_applied(&id).unwrap();
        assert!(!store.has_remaps(&TABLE).unwrap());
        for position in 0..4 {
            assert_eq!(
                store.lookup(&TABLE, &key(position as u8)).unwrap(),
                Some(location("b", position, 2, 1))
            );
        }
    }

    #[test]
    fn remap_commit_rejects_count_disagreement() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            "insert",
            OperationKind::Ingest,
            (0..3)
                .map(|position| IndexDelta {
                    key: key(position as u8),
                    expected: None,
                    replacement: Some(location("a", position, 1, 1)),
                })
                .collect(),
            None,
        );
        let remap = LazyRemap {
            files: vec![remap_of("a", &[0, 1, 2], "b", "b", usize::MAX, 2)],
            output_live_rows: vec![(FileId("b".into()), 2)],
        };
        let operation = operation(&store, "rewrite", OperationKind::Rewrite);
        let id = operation.id.clone();
        store.begin_prepare(operation).unwrap();
        store.stage_remap(&id, &remap).unwrap();
        assert!(store.stage_remap(&id, &remap).is_err(), "staged twice");
        store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
        store.mark_committed(&id, 2, 2).unwrap();
        assert!(
            store
                .apply_committed(&id)
                .unwrap_err()
                .requires_index_rebuild()
        );
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_eq!(
            store.lookup(&TABLE, &key(1)).unwrap(),
            Some(location("a", 1, 1, 1))
        );
    }

    #[test]
    fn chained_remaps_translate_reads_cas_and_materialize() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        // Keys 0..10 at a/0..10, except the deleted position 3.
        let positions: Vec<u64> = (0..10).filter(|position| *position != 3).collect();
        commit(
            &store,
            "insert",
            OperationKind::Ingest,
            positions
                .iter()
                .map(|position| IndexDelta {
                    key: key(*position as u8),
                    expected: None,
                    replacement: Some(location("a", *position, 1, 1)),
                })
                .collect(),
            None,
        );
        assert!(!store.has_remaps(&TABLE).unwrap());
        let before = store.index_snapshot(&TABLE, Some(1)).unwrap();

        // a -> b (6 rows) + c (3 rows), at data sequence 2.
        let first = LazyRemap {
            files: vec![remap_of("a", &positions, "b", "c", 6, 2)],
            output_live_rows: vec![(FileId("b".into()), 6), (FileId("c".into()), 3)],
        };
        commit(
            &store,
            "rewrite-1",
            OperationKind::Rewrite,
            vec![],
            Some(&first),
        );
        assert!(store.has_remaps(&TABLE).unwrap());
        assert_eq!(
            store.lookup(&TABLE, &key(4)).unwrap(),
            Some(location("b", 3, 2, 1))
        );
        // A snapshot from before the remap still sees the original rows.
        assert_eq!(
            before.lookup_keys(&TABLE, [&key(4)]).unwrap(),
            vec![Some(location("a", 4, 1, 1))]
        );
        drop(before);
        let snapshot = store.index_snapshot(&TABLE, Some(2)).unwrap();
        assert_eq!(
            snapshot.lookup_keys(&TABLE, [&key(8)]).unwrap(),
            vec![Some(location("c", 1, 2, 1))]
        );
        assert_eq!(
            snapshot
                .file_live_row_counts(&[FileId("a".into()), FileId("b".into()), FileId("c".into())])
                .unwrap(),
            [0, 6, 3]
        );
        drop(snapshot);

        // A CDC update of key 9 (now c/2) compares against the translation.
        commit(
            &store,
            "update",
            OperationKind::Ingest,
            vec![IndexDelta {
                key: key(9),
                expected: Some(location("c", 2, 2, 1)),
                replacement: Some(location("n", 0, 3, 3)),
            }],
            None,
        );
        assert_eq!(
            store.lookup(&TABLE, &key(9)).unwrap(),
            Some(location("n", 0, 3, 3))
        );
        assert!(
            store
                .file_rows(&TABLE, &FileId("a".into()))
                .all(|row| row.unwrap().1 != key(9)),
            "the raw reverse entry moved with the update"
        );

        // b -> d, after key 1 (b/1) was deleted before the second base.
        commit(
            &store,
            "delete",
            OperationKind::Ingest,
            vec![IndexDelta {
                key: key(1),
                expected: Some(location("b", 1, 2, 1)),
                replacement: None,
            }],
            None,
        );
        let survivors: Vec<u64> = (0..6).filter(|position| *position != 1).collect();
        let second = LazyRemap {
            files: vec![remap_of("b", &survivors, "d", "unused", usize::MAX, 4)],
            output_live_rows: vec![(FileId("d".into()), 5)],
        };
        commit(
            &store,
            "rewrite-2",
            OperationKind::Rewrite,
            vec![],
            Some(&second),
        );
        let expected = |value: u8| -> Option<RowLocation> {
            let position = u64::from(value);
            match value {
                0 | 2 => Some(location("d", position.saturating_sub(1), 4, 1)),
                4..=6 => Some(location("d", position - 2, 4, 1)),
                7 | 8 => Some(location("c", position - 7, 2, 1)),
                9 => Some(location("n", 0, 3, 3)),
                _ => None,
            }
        };
        let keys: Vec<_> = (0..10).map(key).collect();
        let all: Vec<_> = (0..10).map(expected).collect();
        assert_eq!(store.lookup_many(&TABLE, &keys).unwrap(), all);
        assert_eq!(
            store
                .file_live_row_counts(
                    &TABLE,
                    Some(5),
                    &[FileId("b".into()), FileId("c".into()), FileId("d".into())]
                )
                .unwrap(),
            [0, 2, 5]
        );

        // Remaps are durable index state.
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(store.lookup_many(&TABLE, &keys).unwrap(), all);

        // Materializing restores exact reverse scans and drops the remaps.
        assert_eq!(store.materialize_remaps(&TABLE).unwrap(), 7);
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_eq!(store.lookup_many(&TABLE, &keys).unwrap(), all);
        let rows = |file: &str| {
            store
                .file_rows(&TABLE, &FileId(file.into()))
                .map(|row| row.unwrap())
                .collect::<Vec<_>>()
        };
        assert!(rows("a").is_empty() && rows("b").is_empty());
        assert_eq!(
            rows("d"),
            vec![
                (0, key(0)),
                (1, key(2)),
                (2, key(4)),
                (3, key(5)),
                (4, key(6))
            ]
        );
        assert_eq!(rows("c"), vec![(0, key(7)), (1, key(8))]);

        // A stale expectation is still rejected by the compare-and-swap.
        let stale = operation(&store, "stale", OperationKind::Ingest);
        let id = stale.id.clone();
        store
            .prepare(
                stale,
                [IndexDelta {
                    key: key(4),
                    expected: Some(location("a", 4, 1, 1)),
                    replacement: None,
                }],
            )
            .unwrap();
        store.mark_committed(&id, 6, 6).unwrap();
        assert!(matches!(
            store.apply_committed(&id),
            Err(Error::IndexConflict { .. })
        ));
    }

    #[test]
    fn committed_remap_applies_after_reopen_and_discard_drops_staging() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            "insert",
            OperationKind::Ingest,
            vec![IndexDelta {
                key: key(0),
                expected: None,
                replacement: Some(location("a", 0, 1, 1)),
            }],
            None,
        );
        let remap = LazyRemap {
            files: vec![remap_of("a", &[0], "b", "b", usize::MAX, 2)],
            output_live_rows: vec![(FileId("b".into()), 1)],
        };
        // A discarded attempt leaves no staged remap behind.
        let discarded = operation(&store, "discarded", OperationKind::Rewrite);
        let id = discarded.id.clone();
        store.begin_prepare(discarded).unwrap();
        store.stage_remap(&id, &remap).unwrap();
        store.discard_uncommitted(&id).unwrap();
        assert_eq!(store.staged_remap(&id).unwrap(), None);

        let rewrite = operation(&store, "rewrite", OperationKind::Rewrite);
        let id = rewrite.id.clone();
        store.begin_prepare(rewrite).unwrap();
        store.stage_remap(&id, &remap).unwrap();
        store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
        store.mark_committed(&id, 2, 2).unwrap();
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_eq!(
            store.lookup(&TABLE, &key(0)).unwrap(),
            Some(location("a", 0, 1, 1)),
            "nothing is translated before the remap is applied"
        );
        assert!(store.apply_committed(&id).unwrap().complete);
        assert_eq!(
            store.lookup(&TABLE, &key(0)).unwrap(),
            Some(location("b", 0, 2, 1))
        );
        assert_eq!(store.table_state(&TABLE).unwrap().snapshot_id, Some(2));
    }

    #[test]
    fn translated_cas_rejects_stale_raw_expectations() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        commit(
            &store,
            "insert",
            OperationKind::Ingest,
            vec![IndexDelta {
                key: key(0),
                expected: None,
                replacement: Some(location("a", 0, 1, 1)),
            }],
            None,
        );
        let remap = LazyRemap {
            files: vec![remap_of("a", &[0], "b", "b", usize::MAX, 2)],
            output_live_rows: vec![(FileId("b".into()), 1)],
        };
        commit(
            &store,
            "rewrite",
            OperationKind::Rewrite,
            vec![],
            Some(&remap),
        );
        let stale = operation(&store, "stale", OperationKind::Ingest);
        let id = stale.id.clone();
        store
            .prepare(
                stale,
                [IndexDelta {
                    key: key(0),
                    expected: Some(location("a", 0, 1, 1)),
                    replacement: None,
                }],
            )
            .unwrap();
        store.mark_committed(&id, 3, 3).unwrap();
        assert!(matches!(
            store.apply_committed(&id),
            Err(Error::IndexConflict { .. })
        ));
    }
}
