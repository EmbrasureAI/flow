//! Lazy row-index remaps for data rewrites (`lazy_index_remap`, on by default).
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
//! Periodic maintenance bounds the remaps with [`StateStore::clean_remaps`]:
//! it drops remaps nothing needs and materializes the longest chains in
//! bounded batches while a table exceeds its [`RemapLimits`].

use crate::{
    DELTAS, Error, FILE_COUNTS, OUTPUT_COUNT_MARKER, OperationKind, OperationPhase, PK,
    REMAP_MARKER, REMAPS, REVERSE, Result, StateBatch, StateStore,
    apply::FileCountShadow,
    decode, file_prefix,
    files::{self, OrdinalMemo, StoredLocation, reverse_key},
    operation_prefix, pk_key,
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
pub(crate) struct RemapCache {
    decoded: Mutex<HashMap<(TableId, FileId), Arc<FileRemap>>>,
    /// Per-table graph sizes, computed and invalidated under the row lock.
    stats: Mutex<HashMap<TableId, RemapStats>>,
}

impl RemapCache {
    /// Decode `bytes`, the committed remap of `file`, at most once.
    fn decoded(&self, table: TableId, file: &FileId, bytes: &[u8]) -> Result<Arc<FileRemap>> {
        let key = (table, file.clone());
        if let Some(remap) = self.decoded.lock().map_err(|_| Error::Poisoned)?.get(&key) {
            return Ok(remap.clone());
        }
        let remap = Arc::new(decode::<FileRemap>(bytes)?);
        self.decoded
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(key, remap.clone());
        Ok(remap)
    }

    pub(crate) fn forget_stats(&self, table: &TableId) -> Result<()> {
        self.stats
            .lock()
            .map_err(|_| Error::Poisoned)?
            .remove(table);
        Ok(())
    }
}

/// Size of one table's lazy remaps, which bounds forward-read translation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemapStats {
    /// Retired data files with a remap record.
    pub files: u64,
    /// Longest translation chain, in hops, from any retired file.
    pub max_depth: u64,
}

/// Bounds of one [`StateStore::clean_remaps`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemapLimits {
    /// Materialize while more retired files than this keep a remap.
    pub max_files: u64,
    /// Materialize while a translation chain is longer than this.
    pub max_depth: u64,
    /// Raw entries one pass may rewrite before it yields.
    pub max_rows: u64,
}

/// What one cleanup pass did and what remains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemapCleanup {
    /// Remap records dropped because no raw entry or other remap needs them.
    pub dropped_files: u64,
    /// Raw entries rewritten to their translated location.
    pub rewritten_rows: u64,
    /// The table's remaps after the pass.
    pub stats: RemapStats,
    /// The pass stopped at `max_rows` while still over its limits.
    pub continuation: bool,
}

impl RemapStats {
    pub fn exceeds(&self, limits: &RemapLimits) -> bool {
        self.files > limits.max_files || self.max_depth > limits.max_depth
    }
}

/// The table's retired files and the remaps that translate into them. Remap
/// targets are fresh outputs, so a committed remap never gains a referrer and
/// a retired file never gains raw entries; only roots (files no other remap
/// targets) can be dropped, and dropping one changes no other depth.
struct RemapGraph {
    nodes: BTreeMap<FileId, RemapNode>,
}

struct RemapNode {
    key: Vec<u8>,
    /// Retired files this remap translates into.
    retired_targets: Vec<FileId>,
    referrers: u64,
    depth: u64,
}

impl RemapGraph {
    fn stats(&self) -> RemapStats {
        RemapStats {
            files: self.nodes.len() as u64,
            max_depth: self
                .nodes
                .values()
                .map(|node| node.depth)
                .max()
                .unwrap_or(0),
        }
    }

    fn roots(&self) -> Vec<FileId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.referrers == 0)
            .map(|(file, _)| file.clone())
            .collect()
    }

    /// The root on the longest chain; deterministic so passes resume it.
    fn deepest_root(&self) -> Option<FileId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.referrers == 0)
            .max_by_key(|(_, node)| node.depth)
            .map(|(file, _)| file.clone())
    }

    fn remove(&mut self, file: &FileId) -> Option<RemapNode> {
        let node = self.nodes.remove(file)?;
        for target in &node.retired_targets {
            if let Some(target) = self.nodes.get_mut(target) {
                target.referrers -= 1;
            }
        }
        Some(node)
    }
}

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
                Some(bytes) => Some(self.cache.decoded(self.table, file, &bytes)?),
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
    /// a remap is deleted only after no raw entry or other remap needs it.
    pub fn materialize_remaps(&self, table: &TableId) -> Result<u64> {
        let cleanup = self.clean_remaps(
            table,
            &RemapLimits {
                max_files: 0,
                max_depth: 0,
                max_rows: u64::MAX,
            },
        )?;
        if cleanup.stats.files != 0 {
            return Err(Error::InvalidState(
                "lazy remaps remain after materialization".into(),
            ));
        }
        Ok(cleanup.rewritten_rows)
    }

    /// Size of the table's lazy remaps. Cached until a remap commits or a
    /// cleanup pass changes them.
    pub fn remap_stats(&self, table: &TableId) -> Result<RemapStats> {
        if !self.remaps_possible() {
            return Ok(RemapStats::default());
        }
        let cached = self
            .0
            .remap_cache
            .stats
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(table)
            .copied();
        if let Some(stats) = cached {
            return Ok(stats);
        }
        // Under the row lock, so no remap commit lands between this read and
        // caching its result.
        let _guard = self.lock()?;
        let stats = self.remap_graph(table)?.stats();
        self.0
            .remap_cache
            .stats
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(*table, stats);
        Ok(stats)
    }

    /// Bounded, crash-safe cleanup. First drops every remap that no raw entry
    /// or other remap needs, which is free. While the table still exceeds
    /// `limits`, it then rewrites the raw entries of the root on the longest
    /// chain, in row-lock batches like an apply, and drops that root once it is
    /// empty, until `max_rows` entries were rewritten. No table fence is held;
    /// translated reads, compare-and-swap and live counts are unchanged.
    pub fn clean_remaps(&self, table: &TableId, limits: &RemapLimits) -> Result<RemapCleanup> {
        let mut cleanup = RemapCleanup::default();
        // Sizes are recomputed under the row lock on the next read, so a remap
        // committed during this pass is never hidden by a stale cache entry.
        self.0.remap_cache.forget_stats(table)?;
        if !self.has_remaps(table)? {
            return Ok(cleanup);
        }
        let mut graph = {
            let _guard = self.lock()?;
            self.remap_graph(table)?
        };
        let mut budget = limits.max_rows;
        loop {
            cleanup.dropped_files += self.drop_unneeded_remaps(table, &mut graph)?;
            if !graph.stats().exceeds(limits) || budget == 0 {
                break;
            }
            let root = graph.deepest_root().ok_or_else(|| {
                Error::RecoveryRequired("lazy remaps form a cycle; rebuild the index".into())
            })?;
            let (rewritten, drained) = self.drain_retired_file(table, &root, budget)?;
            cleanup.rewritten_rows += rewritten;
            budget = budget.saturating_sub(rewritten);
            if !drained && budget != 0 {
                return Err(Error::InvalidState(
                    "remap drain stopped before its budget".into(),
                ));
            }
        }
        cleanup.stats = graph.stats();
        cleanup.continuation = cleanup.stats.exceeds(limits);
        self.0.remap_cache.forget_stats(table)?;
        Ok(cleanup)
    }

    /// Every committed remap of the table. The caller holds the row lock or
    /// tolerates a graph that misses remaps committed after this read, which is
    /// conservative: new remaps only add roots.
    fn remap_graph(&self, table: &TableId) -> Result<RemapGraph> {
        let prefix_len = size_of::<u32>() + size_of::<u64>();
        let mut nodes = BTreeMap::new();
        let mut targets = Vec::new();
        for entry in self.scan(REMAPS, table.0.to_be_bytes().to_vec()) {
            let (key, value) = entry?;
            let file = key
                .get(prefix_len..)
                .and_then(|name| String::from_utf8(name.to_vec()).ok())
                .map(FileId)
                .ok_or_else(|| Error::InvalidState("malformed lazy remap key".into()))?;
            let remap = self.0.remap_cache.decoded(*table, &file, &value)?;
            if remap.source != file {
                return Err(Error::InvalidState(
                    "lazy remap key and source differ".into(),
                ));
            }
            targets.push((
                file.clone(),
                remap
                    .targets
                    .iter()
                    .map(|target| target.data_file_id.clone())
                    .collect::<Vec<_>>(),
            ));
            nodes.insert(
                file,
                RemapNode {
                    key: key.to_vec(),
                    retired_targets: Vec::new(),
                    referrers: 0,
                    depth: 0,
                },
            );
        }
        for (file, files) in targets {
            let retired: Vec<_> = files
                .into_iter()
                .filter(|target| nodes.contains_key(target))
                .collect();
            for target in &retired {
                nodes.get_mut(target).expect("retired target").referrers += 1;
            }
            nodes.get_mut(&file).expect("loaded node").retired_targets = retired;
        }
        // Depth is one hop plus the deepest retired target. Remaps only
        // translate into newer files, so this terminates unless the records are
        // corrupt, which the visit bound reports.
        let mut depths = HashMap::<FileId, u64>::new();
        for file in nodes.keys() {
            let mut stack = vec![(file.clone(), false)];
            let mut visits = 0usize;
            while let Some((current, expanded)) = stack.pop() {
                if depths.contains_key(&current) {
                    continue;
                }
                visits += 1;
                if visits > nodes.len().saturating_mul(2) + MAX_REMAP_HOPS {
                    return Err(Error::RecoveryRequired(
                        "lazy remaps form a cycle; rebuild the index".into(),
                    ));
                }
                let node = &nodes[&current];
                if expanded {
                    let depth = 1 + node
                        .retired_targets
                        .iter()
                        .map(|target| depths[target])
                        .max()
                        .unwrap_or(0);
                    depths.insert(current, depth);
                } else {
                    stack.push((current.clone(), true));
                    for target in &node.retired_targets {
                        if !depths.contains_key(target) {
                            stack.push((target.clone(), false));
                        }
                    }
                }
            }
        }
        for (file, node) in &mut nodes {
            node.depth = depths[file];
        }
        Ok(RemapGraph { nodes })
    }

    /// Drop remaps of roots without raw entries until none remains; each drop
    /// may expose its targets as new roots.
    fn drop_unneeded_remaps(&self, table: &TableId, graph: &mut RemapGraph) -> Result<u64> {
        let remaps_cf = self.0.db.cf_handle(REMAPS).expect("opened column family");
        let mut dropped = 0u64;
        let mut candidates = graph.roots();
        while !candidates.is_empty() {
            let _guard = self.lock()?;
            let mut batch = StateBatch::default();
            let mut removed = Vec::new();
            for file in candidates.drain(..) {
                let raw =
                    crate::reader::file_rows_raw(&self.0.db, table, &file, ReadOptions::default())
                        .next()
                        .transpose()?;
                if raw.is_none() {
                    let node = graph.remove(&file).expect("candidate root");
                    batch.delete_cf(&remaps_cf, &node.key);
                    // No raw entry names the retired file any more.
                    self.mark_file_released(&mut batch, *table, &file);
                    removed.push((file, node));
                }
            }
            if removed.is_empty() {
                break;
            }
            self.write(batch)?;
            let mut cache = self
                .0
                .remap_cache
                .decoded
                .lock()
                .map_err(|_| Error::Poisoned)?;
            for (file, node) in removed {
                dropped += 1;
                cache.remove(&(*table, file));
                candidates.extend(
                    node.retired_targets
                        .into_iter()
                        .filter(|target| graph.nodes.get(target).is_some_and(|n| n.referrers == 0)),
                );
            }
            candidates.sort();
            candidates.dedup();
        }
        Ok(dropped)
    }

    /// Rewrite up to `budget` raw entries of one retired file to their
    /// translated locations. Returns the rows rewritten and whether the file
    /// has no raw entry left.
    fn drain_retired_file(
        &self,
        table: &TableId,
        file: &FileId,
        budget: u64,
    ) -> Result<(u64, bool)> {
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let reverse_cf = self.0.db.cf_handle(REVERSE).expect("opened column family");
        let mut rewritten = 0u64;
        while rewritten < budget {
            let remaining = budget - rewritten;
            // Scan under the row lock so concurrent applies cannot move a row
            // between this read and the rewrite below.
            let _guard = self.lock()?;
            let rows: Vec<(u64, PrimaryKey)> =
                crate::reader::file_rows_raw(&self.0.db, table, file, ReadOptions::default())
                    .take(
                        self.0
                            .batch_rows
                            .min(usize::try_from(remaining).unwrap_or(usize::MAX)),
                    )
                    .collect::<Result<_>>()?;
            if rows.is_empty() {
                return Ok((rewritten, true));
            }
            let keys: Vec<_> = rows.iter().map(|(_, key)| pk_key(table, key)).collect();
            let current = self.0.db.batched_multi_get_cf(&pk_cf, keys.iter(), false);
            let options = ReadOptions::default();
            let mut resolver = Resolver::new(&self.0.db, &self.0.remap_cache, *table, &options);
            // The rows were found under this file's ordinal.
            let ordinal = self
                .0
                .files
                .ordinal(*table, &file.0)
                .ok_or_else(files::unknown_file)?;
            let mut batch = StateBatch::default();
            // Only valid while this lock is held.
            let mut memo = OrdinalMemo::default();
            let mut encoded = Vec::new();
            for (((position, key), pk), current) in rows.iter().zip(&keys).zip(current) {
                let Some(current) = current? else {
                    return Err(Error::RecoveryRequired(
                        "reverse index has no forward entry; rebuild the index".into(),
                    ));
                };
                let stored = files::decode_location(&current)?;
                if stored.file != ordinal || stored.row_position != *position {
                    return Err(Error::RecoveryRequired(
                        "forward and reverse index disagree; rebuild the index".into(),
                    ));
                }
                let translated = resolver
                    .resolve(&stored.to_location(&file.0))?
                    .ok_or_else(|| Error::InvalidState("retired file lost its remap".into()))?;
                // A lazy output may have had no row of its own until now.
                let target =
                    self.file_ordinal(&mut batch, &mut memo, *table, &translated.data_file_id)?;
                batch.delete_cf(&reverse_cf, reverse_key(table, ordinal, *position));
                files::encode_location(&mut encoded, &StoredLocation::new(target, &translated))?;
                batch.put_cf(&pk_cf, pk, &encoded);
                batch.put_cf(
                    &reverse_cf,
                    reverse_key(table, target, translated.row_position),
                    bincode::serialize(key)?,
                );
                rewritten += 1;
            }
            self.write_staged(batch)?;
        }
        // The budget ran out exactly at a batch boundary; check for more rows.
        let drained = crate::reader::file_rows_raw(&self.0.db, table, file, ReadOptions::default())
            .next()
            .transpose()?
            .is_none();
        Ok((rewritten, drained))
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

    const UNBOUNDED: RemapLimits = RemapLimits {
        max_files: u64::MAX,
        max_depth: u64::MAX,
        max_rows: 0,
    };

    fn next_sequence(store: &StateStore) -> i64 {
        store.table_state(&TABLE).unwrap().snapshot_id.unwrap_or(0) + 1
    }

    /// Index `keys` at consecutive positions of `file`, returning the model.
    fn insert(store: &StateStore, file: &str, keys: &[u8]) -> BTreeMap<u8, RowLocation> {
        let sequence = next_sequence(store);
        let model: BTreeMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(position, value)| (*value, location(file, position as u64, sequence, 1)))
            .collect();
        commit(
            store,
            &format!("insert-{file}"),
            OperationKind::Ingest,
            model
                .iter()
                .map(|(value, location)| IndexDelta {
                    key: key(*value),
                    expected: None,
                    replacement: Some(location.clone()),
                })
                .collect(),
            None,
        );
        model
    }

    /// Lazily rewrite every live file of the model into `output`, in physical
    /// order, as a background build would.
    fn rewrite_all(store: &StateStore, model: &mut BTreeMap<u8, RowLocation>, output: &str) {
        let sequence = next_sequence(store);
        let mut rows: Vec<_> = model
            .iter()
            .map(|(value, location)| (location.data_file_id.clone(), location.row_position, *value))
            .collect();
        rows.sort();
        let mut files = BTreeMap::<FileId, FileRemap>::new();
        for (position, (file, old, value)) in rows.iter().enumerate() {
            let replacement = location(output, position as u64, sequence, 0);
            files
                .entry(file.clone())
                .or_insert_with(|| FileRemap::new(file.clone()))
                .push(*old, &replacement, sequence)
                .unwrap();
            let moved = model.get_mut(value).unwrap();
            moved.data_file_id = replacement.data_file_id;
            moved.row_position = replacement.row_position;
            moved.data_sequence_number = sequence;
        }
        let remap = LazyRemap {
            files: files.into_values().collect(),
            output_live_rows: vec![(FileId(output.into()), rows.len() as u64)],
        };
        commit(
            store,
            &format!("rewrite-{output}"),
            OperationKind::Rewrite,
            vec![],
            Some(&remap),
        );
    }

    fn update(store: &StateStore, model: &mut BTreeMap<u8, RowLocation>, value: u8, file: &str) {
        let sequence = next_sequence(store);
        let replacement = location(file, 0, sequence, sequence as u64);
        commit(
            store,
            &format!("update-{file}"),
            OperationKind::Ingest,
            vec![IndexDelta {
                key: key(value),
                expected: model.get(&value).cloned(),
                replacement: Some(replacement.clone()),
            }],
            None,
        );
        model.insert(value, replacement);
    }

    fn assert_model(store: &StateStore, model: &BTreeMap<u8, RowLocation>) {
        let keys: Vec<_> = model.keys().map(|value| key(*value)).collect();
        assert_eq!(
            store.lookup_many(&TABLE, &keys).unwrap(),
            model.values().cloned().map(Some).collect::<Vec<_>>()
        );
    }

    /// Reverse entries of drained files are exact; the rest stay raw.
    fn raw_rows(store: &StateStore, file: &str) -> usize {
        store.file_rows(&TABLE, &FileId(file.into())).count()
    }

    #[test]
    fn binaries_without_the_remap_family_refuse_to_open_the_index() {
        // Every index this binary opens has the family, with or without
        // remaps. A binary that predates it lists only the older families and
        // RocksDB refuses the open, so it can never ignore remaps and misread
        // raw entries in retired files.
        let directory = tempfile::tempdir().unwrap();
        drop(StateStore::open(directory.path(), StateStoreOptions::default()).unwrap());
        let older: Vec<_> = crate::STATE_COLUMN_FAMILIES
            .into_iter()
            .filter(|name| *name != REMAPS)
            .collect();
        assert_eq!(older.len(), crate::STATE_COLUMN_FAMILIES.len() - 1);
        let Err(error) = DB::open_cf(&rocksdb::Options::default(), directory.path(), older) else {
            panic!("an older family list must not open the index");
        };
        assert!(error.to_string().contains(REMAPS), "{error}");
    }

    #[test]
    fn cleanup_drops_unneeded_remaps_but_keeps_referenced_ones() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let mut model = insert(&store, "a", &[0, 1, 2, 3]);
        rewrite_all(&store, &mut model, "b");
        // CDC replaces every row of `b` that came from `a`; `a` is then free.
        for value in 0..4 {
            update(&store, &mut model, value, &format!("n{value}"));
        }
        let cleanup = store.clean_remaps(&TABLE, &UNBOUNDED).unwrap();
        assert_eq!(
            (cleanup.dropped_files, cleanup.rewritten_rows),
            (1, 0),
            "a remap without raw entries is dropped for free"
        );
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_model(&store, &model);

        // a2 -> c -> d: c has no raw entries, but a2's remap translates into it.
        model.extend(insert(&store, "a2", &[10, 11, 12]));
        rewrite_all(&store, &mut model, "c");
        rewrite_all(&store, &mut model, "d");
        assert_eq!(
            store.remap_stats(&TABLE).unwrap(),
            RemapStats {
                // n0..n3 and a2 retired into c, c into d.
                files: 6,
                max_depth: 2,
            }
        );
        let cleanup = store.clean_remaps(&TABLE, &UNBOUNDED).unwrap();
        assert_eq!(cleanup.dropped_files, 0, "n* and a2 hold raw entries");
        assert_eq!(cleanup.stats, store.remap_stats(&TABLE).unwrap());
        assert_model(&store, &model);

        // Drain at most two rows: progress, continuation, nothing dropped.
        let limits = RemapLimits {
            max_files: 0,
            max_depth: 0,
            max_rows: 2,
        };
        let cleanup = store.clean_remaps(&TABLE, &limits).unwrap();
        assert_eq!(cleanup.rewritten_rows, 2);
        assert!(cleanup.continuation);
        assert_model(&store, &model);
        // Finish: every raw entry moves to `d`, and every remap is dropped
        // once nothing needs it, the referenced `c` last.
        let cleanup = store
            .clean_remaps(
                &TABLE,
                &RemapLimits {
                    max_rows: u64::MAX,
                    ..limits
                },
            )
            .unwrap();
        assert_eq!(cleanup.rewritten_rows, 5);
        assert_eq!(cleanup.stats, RemapStats::default());
        assert!(!cleanup.continuation);
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_eq!(store.remap_stats(&TABLE).unwrap(), RemapStats::default());
        assert_model(&store, &model);
        assert_eq!(raw_rows(&store, "d"), 7);
        assert_eq!(raw_rows(&store, "a2") + raw_rows(&store, "n0"), 0);
    }

    #[test]
    fn retired_files_keep_their_ordinal_until_their_remap_is_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let mut model = insert(&store, "a", &[0, 1]);
        rewrite_all(&store, &mut model, "b");
        // Raw entries still name `a`; a lazy output has no ordinal of its own.
        assert!(store.cached_ordinal(TABLE, "a").is_some());
        assert!(store.cached_ordinal(TABLE, "b").is_none());
        assert_model(&store, &model);
        // Expectations in `b` allocate its ordinal at staging.
        update(&store, &mut model, 0, "n0");
        assert!(store.cached_ordinal(TABLE, "b").is_some());
        update(&store, &mut model, 1, "n1");
        assert!(store.cached_ordinal(TABLE, "a").is_some(), "remap retained");
        assert_eq!(
            store
                .clean_remaps(&TABLE, &UNBOUNDED)
                .unwrap()
                .dropped_files,
            1
        );
        // The next retirement releases both files; nothing names them.
        update(&store, &mut model, 0, "n2");
        assert!(store.cached_ordinal(TABLE, "a").is_none());
        assert!(store.cached_ordinal(TABLE, "b").is_none());
        assert!(store.cached_ordinal(TABLE, "n0").is_none());
        assert!(store.cached_ordinal(TABLE, "n1").is_some());
        assert_model(&store, &model);
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_model(&store, &model);
        assert_eq!(raw_rows(&store, "n1") + raw_rows(&store, "n2"), 2);
    }

    #[test]
    fn cleanup_bounds_remaps_over_many_rewrites() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(
            directory.path(),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap();
        // Cold keys are never updated; hot keys are updated between rewrites.
        let mut model = insert(&store, "base", &(0..32).collect::<Vec<_>>());
        let limits = RemapLimits {
            max_files: 4,
            max_depth: 2,
            max_rows: 8,
        };
        let (mut dropped, mut rewritten) = (0, 0);
        for round in 0..24u8 {
            // Each round retires the previous output and one hot file.
            update(&store, &mut model, round % 4, &format!("hot-{round}"));
            rewrite_all(&store, &mut model, &format!("out-{round}"));
            // One bounded pass per rewrite, like a periodic visit.
            let mut cleanup = store.clean_remaps(&TABLE, &limits).unwrap();
            assert!(cleanup.rewritten_rows <= limits.max_rows);
            // Continuations run until the table is within its limits.
            loop {
                dropped += cleanup.dropped_files;
                rewritten += cleanup.rewritten_rows;
                if !cleanup.continuation {
                    break;
                }
                cleanup = store.clean_remaps(&TABLE, &limits).unwrap();
            }
            let stats = store.remap_stats(&TABLE).unwrap();
            assert_eq!(stats, cleanup.stats);
            assert!(
                stats.files <= limits.max_files && stats.max_depth <= limits.max_depth,
                "round {round}: {stats:?}"
            );
            assert_model(&store, &model);
        }
        // 48 retired files without cleanup; most are dropped for free. Cold rows
        // are rewritten once per `max_depth + 1` rewrites instead of each one.
        assert!(dropped >= 40, "{dropped}");
        assert!(
            rewritten <= 32 * (24 / (limits.max_depth + 1) + 1),
            "{rewritten}"
        );
        // The hop limit was never approached, and translation stays exact
        // after a reopen.
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_model(&store, &model);
        store.materialize_remaps(&TABLE).unwrap();
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_model(&store, &model);
        assert_eq!(raw_rows(&store, "out-23"), 32);
    }

    #[test]
    fn crash_during_cleanup_keeps_a_consistent_translatable_index() {
        use crate::tests::SIMULATE_POWER_LOSS;
        let crash_image = |path: &std::path::Path| {
            let image = tempfile::tempdir().unwrap();
            std::fs::remove_dir(image.path()).unwrap();
            crate::control::copy_directory_tree(path, image.path()).unwrap();
            image
        };
        SIMULATE_POWER_LOSS.set(true);
        let directory = tempfile::tempdir().unwrap();
        let options = StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        };
        let store = StateStore::open(directory.path(), options.clone()).unwrap();
        let mut model = insert(&store, "a", &(0..9).collect::<Vec<_>>());
        rewrite_all(&store, &mut model, "b");
        rewrite_all(&store, &mut model, "c");
        let drain = RemapLimits {
            max_files: 0,
            max_depth: 0,
            max_rows: 5,
        };
        // Unsynced drain batches are lost by the crash.
        assert_eq!(
            store.clean_remaps(&TABLE, &drain).unwrap().rewritten_rows,
            5
        );
        let lost = crash_image(directory.path());
        // A later synced write makes the drained prefix durable.
        update(&store, &mut model, 8, "n");
        let kept = crash_image(directory.path());
        drop(store);
        SIMULATE_POWER_LOSS.set(false);
        for (image, drained) in [(lost, 0), (kept, 5)] {
            let store = StateStore::open(image.path(), options.clone()).unwrap();
            let mut expected = model.clone();
            if drained == 0 {
                // The update after the image is not part of it.
                expected.insert(8, location("c", 8, 3, 1));
            }
            assert_model(&store, &expected);
            assert!(store.has_remaps(&TABLE).unwrap());
            assert_eq!(raw_rows(&store, "c"), drained);
            assert_eq!(
                store.remap_stats(&TABLE).unwrap(),
                RemapStats {
                    files: 2,
                    max_depth: 2
                }
            );
            // Cleanup resumes and finishes from either state.
            store.materialize_remaps(&TABLE).unwrap();
            assert!(!store.has_remaps(&TABLE).unwrap());
            assert_model(&store, &expected);
            assert_eq!(raw_rows(&store, "a") + raw_rows(&store, "b"), 0);
            assert_eq!(
                raw_rows(&store, "c") + raw_rows(&store, "n"),
                expected.len()
            );
        }
    }
}
