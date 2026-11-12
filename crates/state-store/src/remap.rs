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
    REMAP_CHUNKS, REMAP_MARKER, REMAPS, REVERSE, Result, StateBatch, StateStore,
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

/// Runs per encoded chunk. A lookup scans at most one chunk, so this bounds
/// the work of one translation; a rewrite that keeps inputs contiguous needs
/// a single chunk per file.
pub(crate) const CHUNK_RUNS: usize = 512;

/// Leading bytes of every chunked record. A record written before chunking is
/// a bincode `LegacyFileRemap`, which starts with its source path's length as
/// a little-endian u64 and so can never start with `u64::MAX`.
const RECORD_TAG: [u8; 8] = [0xff; 8];
const RECORD_VERSION: u8 = 1;

/// Encoded chunks kept across reads, in bytes. At about two bytes per run
/// this holds some 30 million runs.
const CHUNK_CACHE_BYTES: usize = 64 << 20;

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

impl RemapRun {
    fn end(&self) -> u64 {
        self.old_start + self.len
    }
}

/// Up to [`CHUNK_RUNS`] consecutive runs, varint-encoded relative to each
/// other. A run costs two bytes when it follows the previous run of the same
/// output closely, which is what a merge of sorted inputs produces. Restart
/// points every [`RESTART_RUNS`] runs let a lookup scan at most that many.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RemapChunk {
    /// First old position of the first run.
    old_start: u64,
    /// One past the last old position of the last run.
    old_end: u64,
    runs: u32,
    bytes: Vec<u8>,
    /// Decoder state before runs `RESTART_RUNS`, `2 * RESTART_RUNS`, ...
    restarts: Vec<Restart>,
}

/// Runs between restart points; a lookup decodes at most this many.
const RESTART_RUNS: usize = 32;

/// Decoder state at a run boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Restart {
    /// Byte offset of the next run.
    at: u32,
    /// One past the previous run's old positions.
    old_end: u64,
    /// One past the previous run's new positions.
    new_end: u64,
    /// The previous run's target.
    target: u32,
}

/// Decodes runs one at a time from a restart point.
struct RunCursor<'a> {
    bytes: &'a [u8],
    at: usize,
    old_end: u64,
    new_end: u64,
    target: Option<u32>,
}

impl<'a> RunCursor<'a> {
    fn start(chunk: &'a RemapChunk) -> Self {
        Self {
            bytes: &chunk.bytes,
            at: 0,
            old_end: chunk.old_start,
            new_end: 0,
            target: None,
        }
    }

    fn resume(chunk: &'a RemapChunk, restart: &Restart) -> Self {
        Self {
            bytes: &chunk.bytes,
            at: restart.at as usize,
            old_end: restart.old_end,
            new_end: restart.new_end,
            target: Some(restart.target),
        }
    }

    fn restart(&self) -> Result<Restart> {
        Ok(Restart {
            at: u32::try_from(self.at).map_err(|_| malformed())?,
            old_end: self.old_end,
            new_end: self.new_end,
            target: self.target.ok_or_else(malformed)?,
        })
    }

    fn next_run(&mut self) -> Result<RemapRun> {
        let head = get_varint(self.bytes, &mut self.at)?;
        if head & 1 == 1 {
            let index = get_varint(self.bytes, &mut self.at)?;
            self.target = Some(u32::try_from(index).map_err(|_| malformed())?);
            self.new_end = 0;
        }
        let target = self.target.ok_or_else(malformed)?;
        let zigzag = i128::from(get_varint(self.bytes, &mut self.at)?);
        let delta = if zigzag % 2 == 0 {
            zigzag / 2
        } else {
            -(zigzag + 1) / 2
        };
        let new_start = u64::try_from(i128::from(self.new_end) + delta).map_err(|_| malformed())?;
        let len = if head & 2 == 2 {
            get_varint(self.bytes, &mut self.at)?
                .checked_add(2)
                .ok_or_else(malformed)?
        } else {
            1
        };
        let old_start = self.old_end.checked_add(head >> 2).ok_or_else(malformed)?;
        self.old_end = old_start.checked_add(len).ok_or_else(malformed)?;
        self.new_end = new_start.checked_add(len).ok_or_else(malformed)?;
        Ok(RemapRun {
            old_start,
            new_start,
            len,
            target,
        })
    }
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push((value as u8) | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

fn get_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at).ok_or_else(malformed)?;
        *at += 1;
        value |= u64::from(byte & 0x7f)
            .checked_shl(shift)
            .filter(|part| part >> shift == u64::from(byte & 0x7f))
            .ok_or_else(malformed)?;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(malformed())
}

fn malformed() -> Error {
    Error::RecoveryRequired("malformed lazy remap; rebuild the index".into())
}

impl RemapChunk {
    /// Encode runs that are ordered and non-overlapping by old position.
    fn encode(runs: &[RemapRun]) -> Result<Self> {
        let first = runs.first().ok_or_else(malformed)?;
        let mut bytes = Vec::with_capacity(runs.len() * 2);
        let mut restarts = Vec::with_capacity(runs.len() / RESTART_RUNS);
        let (mut old_end, mut new_end, mut target) = (first.old_start, 0u64, None);
        for (index, run) in runs.iter().enumerate() {
            if index > 0 && index % RESTART_RUNS == 0 {
                restarts.push(Restart {
                    at: u32::try_from(bytes.len()).map_err(|_| malformed())?,
                    old_end,
                    new_end,
                    target: target.ok_or_else(malformed)?,
                });
            }
            let gap = run.old_start.checked_sub(old_end).ok_or_else(malformed)?;
            if run.len == 0 || gap > u64::MAX >> 2 {
                return Err(malformed());
            }
            let changed = target != Some(run.target);
            put_varint(
                &mut bytes,
                (gap << 2) | (u64::from(run.len > 1) << 1) | u64::from(changed),
            );
            if changed {
                put_varint(&mut bytes, u64::from(run.target));
                new_end = 0;
            }
            // Zigzag: a merge's next row is usually just after the previous one.
            let delta = i128::from(run.new_start) - i128::from(new_end);
            let zigzag = if delta >= 0 {
                delta * 2
            } else {
                -delta * 2 - 1
            };
            put_varint(&mut bytes, u64::try_from(zigzag).map_err(|_| malformed())?);
            if run.len > 1 {
                put_varint(&mut bytes, run.len - 2);
            }
            old_end = run.old_start.checked_add(run.len).ok_or_else(malformed)?;
            new_end = run.new_start.checked_add(run.len).ok_or_else(malformed)?;
            target = Some(run.target);
        }
        Ok(Self {
            old_start: first.old_start,
            old_end,
            runs: u32::try_from(runs.len()).map_err(|_| malformed())?,
            bytes,
            restarts,
        })
    }

    /// Every run, checking that the encoding and its restart points agree.
    fn decode(&self) -> Result<Vec<RemapRun>> {
        let mut runs = Vec::with_capacity(self.runs as usize);
        let mut cursor = RunCursor::start(self);
        for index in 0..self.runs as usize {
            if index > 0 && index % RESTART_RUNS == 0 {
                let restart = self.restarts.get(index / RESTART_RUNS - 1);
                if restart != Some(&cursor.restart()?) {
                    return Err(malformed());
                }
            }
            runs.push(cursor.next_run()?);
        }
        if cursor.at != self.bytes.len()
            || cursor.old_end != self.old_end
            || self.restarts.len() != (self.runs as usize).saturating_sub(1) / RESTART_RUNS
        {
            return Err(malformed());
        }
        Ok(runs)
    }

    /// Target index and new position of `position`, scanning from the last
    /// restart point at or before it, without allocating.
    fn translate(&self, position: u64) -> Result<Option<(u32, u64)>> {
        if position < self.old_start || position >= self.old_end {
            return Ok(None);
        }
        let restart = self
            .restarts
            .partition_point(|restart| restart.old_end <= position);
        let (mut cursor, first) = match restart.checked_sub(1) {
            None => (RunCursor::start(self), 0),
            Some(index) => (
                RunCursor::resume(self, &self.restarts[index]),
                (index + 1) * RESTART_RUNS,
            ),
        };
        for _ in first..(self.runs as usize).min(first + RESTART_RUNS) {
            let run = cursor.next_run()?;
            if position < run.old_start {
                return Ok(None);
            }
            if position < run.end() {
                return Ok(Some((
                    run.target,
                    run.new_start + (position - run.old_start),
                )));
            }
        }
        Ok(None)
    }

    /// Bytes this chunk adds to its record.
    fn stored_bytes(&self) -> u64 {
        (self.bytes.len() + self.restarts.len() * size_of::<Restart>()) as u64
            + 2 * size_of::<u64>() as u64
            + size_of::<u32>() as u64
    }
}

/// Output target and position of `position` among ordered runs.
fn translate_in(runs: &[RemapRun], position: u64) -> Option<(u32, u64)> {
    let index = runs
        .partition_point(|run| run.old_start <= position)
        .checked_sub(1)?;
    let run = &runs[index];
    (position < run.end()).then(|| (run.target, run.new_start + (position - run.old_start)))
}

fn translated(target: &RemapTarget, position: u64, raw: &RowLocation) -> RowLocation {
    RowLocation {
        data_file_id: target.data_file_id.clone(),
        row_position: position,
        data_sequence_number: target.data_sequence_number,
        spec_id: target.spec_id,
        partition: target.partition.clone(),
        source_commit_lsn: raw.source_commit_lsn,
        row_version: raw.row_version,
        row_fingerprint: raw.row_fingerprint,
    }
}

/// Where every surviving row of one retired data file now lives. Runs are
/// kept encoded in chunks as they are pushed, so a large interleaved remap
/// costs about two bytes per run in memory, staging and the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRemap {
    pub source: FileId,
    pub targets: Vec<RemapTarget>,
    chunks: Vec<RemapChunk>,
    /// Runs not yet encoded; at most [`CHUNK_RUNS`].
    open: Vec<RemapRun>,
    survivors: u64,
    runs: u64,
}

/// The pre-chunking record of one retired file, still read from indexes and
/// staged operations written by earlier releases.
#[derive(Deserialize)]
struct LegacyFileRemap {
    source: FileId,
    targets: Vec<RemapTarget>,
    runs: Vec<RemapRun>,
}

/// The staged form of a [`FileRemap`], with every chunk inline.
#[derive(Serialize, Deserialize)]
struct StagedFileRemap {
    source: FileId,
    targets: Vec<RemapTarget>,
    survivors: u64,
    runs: u64,
    chunks: Vec<RemapChunk>,
}

impl FileRemap {
    pub fn new(source: FileId) -> Self {
        Self {
            source,
            targets: Vec::new(),
            chunks: Vec::new(),
            open: Vec::new(),
            survivors: 0,
            runs: 0,
        }
    }

    /// Append the next surviving row in input order. The replacement's data
    /// sequence is the output's committed one, supplied separately. Output
    /// positions may be in any order; a merge interleaves inputs.
    pub fn push(
        &mut self,
        old_position: u64,
        replacement: &RowLocation,
        data_sequence_number: i64,
    ) -> Result<()> {
        let previous_end = self
            .open
            .last()
            .map(RemapRun::end)
            .or_else(|| self.chunks.last().map(|chunk| chunk.old_end));
        if previous_end.is_some_and(|end| old_position < end) {
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
        self.survivors += 1;
        if let Some(last) = self.open.last_mut()
            && last.target == target
            && last.end() == old_position
            && last.new_start + last.len == replacement.row_position
        {
            last.len += 1;
            return Ok(());
        }
        if self.open.len() == CHUNK_RUNS {
            self.seal()?;
        }
        self.open.push(RemapRun {
            old_start: old_position,
            new_start: replacement.row_position,
            len: 1,
            target,
        });
        self.runs += 1;
        Ok(())
    }

    fn seal(&mut self) -> Result<()> {
        if !self.open.is_empty() {
            self.chunks.push(RemapChunk::encode(&self.open)?);
            self.open.clear();
        }
        Ok(())
    }

    /// Number of rows this remap moves, i.e. survivors at the rewrite base.
    pub fn survivors(&self) -> u64 {
        self.survivors
    }

    /// Number of runs: one per contiguous block of moved rows.
    pub fn runs(&self) -> u64 {
        self.runs
    }

    /// Encoded size, as staged and stored in the index.
    pub fn encoded_bytes(&self) -> u64 {
        let open = if self.open.is_empty() {
            0
        } else {
            RemapChunk::encode(&self.open).map_or(0, |chunk| chunk.stored_bytes())
        };
        self.chunks
            .iter()
            .map(RemapChunk::stored_bytes)
            .sum::<u64>()
            + open
    }

    /// A translator over this remap's chunks; each translation scans at most
    /// one chunk.
    pub fn translator(&self) -> RemapTranslator<'_> {
        RemapTranslator { remap: self }
    }

    /// The location a per-row apply would have stored for this raw location.
    pub fn translate(&self, location: &RowLocation) -> Result<Option<RowLocation>> {
        self.translator().translate(location)
    }

    fn validate(&self) -> Result<()> {
        let mut end = None::<u64>;
        for chunk in &self.chunks {
            let runs = chunk.decode()?;
            if end.is_some_and(|end| chunk.old_start < end) || runs.len() != chunk.runs as usize {
                return Err(Error::InvalidState("malformed lazy remap".into()));
            }
            check_runs(&runs, self.targets.len())?;
            end = Some(chunk.old_end);
        }
        if let Some(first) = self.open.first()
            && end.is_some_and(|end| first.old_start < end)
        {
            return Err(Error::InvalidState("malformed lazy remap".into()));
        }
        check_runs(&self.open, self.targets.len())
    }

    fn staged(&self) -> Result<StagedFileRemap> {
        let mut chunks = self.chunks.clone();
        if !self.open.is_empty() {
            chunks.push(RemapChunk::encode(&self.open)?);
        }
        Ok(StagedFileRemap {
            source: self.source.clone(),
            targets: self.targets.clone(),
            survivors: self.survivors,
            runs: self.runs,
            chunks,
        })
    }

    fn from_staged(staged: StagedFileRemap) -> Self {
        Self {
            source: staged.source,
            targets: staged.targets,
            chunks: staged.chunks,
            open: Vec::new(),
            survivors: staged.survivors,
            runs: staged.runs,
        }
    }

    fn from_runs(source: FileId, targets: Vec<RemapTarget>, runs: &[RemapRun]) -> Result<Self> {
        check_runs(runs, targets.len())?;
        Ok(Self {
            source,
            targets,
            chunks: runs
                .chunks(CHUNK_RUNS)
                .map(RemapChunk::encode)
                .collect::<Result<_>>()?,
            open: Vec::new(),
            survivors: runs.iter().map(|run| run.len).sum(),
            runs: runs.len() as u64,
        })
    }

    /// Encode for staging, tagged so it is never read as a legacy record.
    fn encode_staged(&self) -> Result<Vec<u8>> {
        tagged(&self.staged()?)
    }

    fn decode_staged(bytes: &[u8]) -> Result<Self> {
        match untag(bytes) {
            Some(record) => Ok(Self::from_staged(decode(record)?)),
            None => {
                let legacy: LegacyFileRemap = decode(bytes)?;
                Self::from_runs(legacy.source, legacy.targets, &legacy.runs)
            }
        }
    }
}

fn check_runs(runs: &[RemapRun], targets: usize) -> Result<()> {
    let ordered = runs
        .windows(2)
        .all(|pair| pair[0].end() <= pair[1].old_start);
    if !ordered
        || runs
            .iter()
            .any(|run| run.len == 0 || run.target as usize >= targets)
    {
        return Err(Error::InvalidState("malformed lazy remap".into()));
    }
    Ok(())
}

fn tagged(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = RECORD_TAG.to_vec();
    bytes.push(RECORD_VERSION);
    bincode::serialize_into(&mut bytes, value)?;
    Ok(bytes)
}

/// The record after its tag, or `None` for a legacy record.
fn untag(bytes: &[u8]) -> Option<&[u8]> {
    let record = bytes.strip_prefix(&RECORD_TAG)?;
    match record.split_first() {
        Some((&RECORD_VERSION, rest)) => Some(rest),
        // A newer record version; decoding it as legacy would fail anyway.
        _ => Some(&[]),
    }
}

/// Translates through one in-memory [`FileRemap`].
pub struct RemapTranslator<'a> {
    remap: &'a FileRemap,
}

impl RemapTranslator<'_> {
    pub fn translate(&mut self, location: &RowLocation) -> Result<Option<RowLocation>> {
        let position = location.row_position;
        let remap = self.remap;
        let found = if remap
            .open
            .first()
            .is_some_and(|run| run.old_start <= position)
        {
            translate_in(&remap.open, position)
        } else {
            let Some(index) = remap
                .chunks
                .partition_point(|chunk| chunk.old_start <= position)
                .checked_sub(1)
            else {
                return Ok(None);
            };
            remap.chunks[index].translate(position)?
        };
        Ok(found.map(|(target, position)| {
            translated(&remap.targets[target as usize], position, location)
        }))
    }
}

/// Everything a lazy rewrite stages instead of per-row deltas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LazyRemap {
    pub files: Vec<FileRemap>,
    /// Live rows of each output file at the commit head (written minus masked).
    pub output_live_rows: Vec<(FileId, u64)>,
}

/// The committed record of one retired file in REMAPS. Its chunks are stored
/// under the same key followed by each chunk's first old position, in
/// REMAP_CHUNKS, and are written in the same batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemapHeader {
    source: FileId,
    targets: Vec<RemapTarget>,
    survivors: u64,
    runs: u64,
    /// Each chunk's first and one-past-last old position, ascending.
    chunks: Vec<(u64, u64)>,
    /// Encoded size of the chunks.
    chunk_bytes: u64,
}

/// A committed remap as read: chunked, or a legacy record whose runs are
/// held inline after decoding it once.
#[derive(Debug)]
pub(crate) struct CommittedRemap {
    header: RemapHeader,
    inline: Option<Vec<Arc<RemapChunk>>>,
    /// Bytes the record and its chunks occupy.
    stored_bytes: u64,
}

impl CommittedRemap {
    fn decode(bytes: &[u8]) -> Result<Self> {
        match untag(bytes) {
            Some(record) => {
                let header: RemapHeader = decode(record)?;
                let stored_bytes = bytes.len() as u64 + header.chunk_bytes;
                Ok(Self {
                    header,
                    inline: None,
                    stored_bytes,
                })
            }
            None => {
                let legacy: LegacyFileRemap = decode(bytes)?;
                check_runs(&legacy.runs, legacy.targets.len())?;
                let chunks: Vec<_> = legacy
                    .runs
                    .chunks(CHUNK_RUNS)
                    .map(|runs| RemapChunk::encode(runs).map(Arc::new))
                    .collect::<Result<_>>()?;
                Ok(Self {
                    header: RemapHeader {
                        survivors: legacy.runs.iter().map(|run| run.len).sum(),
                        runs: legacy.runs.len() as u64,
                        chunks: chunks
                            .iter()
                            .map(|chunk| (chunk.old_start, chunk.old_end))
                            .collect(),
                        chunk_bytes: 0,
                        source: legacy.source,
                        targets: legacy.targets,
                    },
                    inline: Some(chunks),
                    stored_bytes: bytes.len() as u64,
                })
            }
        }
    }

    /// The chunk that may hold `position`.
    fn chunk_index(&self, position: u64) -> Option<usize> {
        let index = self
            .header
            .chunks
            .partition_point(|(start, _)| *start <= position)
            .checked_sub(1)?;
        (position < self.header.chunks[index].1).then_some(index)
    }

    pub(crate) fn targets(&self) -> &[RemapTarget] {
        &self.header.targets
    }
}

/// The REMAP_CHUNKS key of a chunk.
fn chunk_key(table: &TableId, file: &FileId, old_start: u64) -> Vec<u8> {
    let mut key = file_prefix(table, file);
    key.extend_from_slice(&old_start.to_be_bytes());
    key
}

/// Encoded chunks by (table, file, first old position), bounded in bytes.
/// Chunks are immutable once committed and a retired file is never retired
/// again, so a cached chunk is valid for every reader that sees its header.
struct ChunkCache {
    chunks: HashMap<(TableId, FileId, u64), Arc<RemapChunk>>,
    order: std::collections::VecDeque<(TableId, FileId, u64)>,
    bytes: usize,
    limit: usize,
}

impl Default for ChunkCache {
    fn default() -> Self {
        Self {
            chunks: HashMap::new(),
            order: std::collections::VecDeque::new(),
            bytes: 0,
            limit: CHUNK_CACHE_BYTES,
        }
    }
}

fn cached_bytes(chunk: &RemapChunk) -> usize {
    chunk.bytes.len() + chunk.restarts.len() * size_of::<Restart>() + 96
}

impl ChunkCache {
    fn get(&self, key: &(TableId, FileId, u64)) -> Option<Arc<RemapChunk>> {
        self.chunks.get(key).cloned()
    }

    fn insert(&mut self, key: (TableId, FileId, u64), chunk: Arc<RemapChunk>) {
        let bytes = cached_bytes(&chunk);
        if bytes > self.limit || self.chunks.contains_key(&key) {
            return;
        }
        while self.bytes + bytes > self.limit {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.chunks.remove(&oldest) {
                self.bytes -= cached_bytes(&evicted);
            }
        }
        self.bytes += bytes;
        self.order.push_back(key.clone());
        self.chunks.insert(key, chunk);
    }

    fn forget_file(&mut self, table: TableId, file: &FileId) {
        let mut freed = 0;
        self.chunks.retain(|(owner, name, _), chunk| {
            let keep = *owner != table || name != file;
            if !keep {
                freed += cached_bytes(chunk);
            }
            keep
        });
        self.bytes -= freed;
        self.order
            .retain(|(owner, name, _)| *owner != table || name != file);
    }
}

/// Decoded remaps are immutable once committed. Visibility is still decided
/// by a read in the caller's view; the cache only avoids decoding again.
#[derive(Default)]
pub(crate) struct RemapCache {
    /// Committed headers: small, one per retired file.
    decoded: Mutex<HashMap<(TableId, FileId), Arc<CommittedRemap>>>,
    chunks: Mutex<ChunkCache>,
    /// Per-table graph sizes, computed and invalidated under the row lock.
    stats: Mutex<HashMap<TableId, RemapStats>>,
}

impl RemapCache {
    /// Decode `bytes`, the committed remap of `file`, at most once.
    fn decoded(&self, table: TableId, file: &FileId, bytes: &[u8]) -> Result<Arc<CommittedRemap>> {
        let key = (table, file.clone());
        if let Some(remap) = self.decoded.lock().map_err(|_| Error::Poisoned)?.get(&key) {
            return Ok(remap.clone());
        }
        let remap = Arc::new(CommittedRemap::decode(bytes)?);
        self.decoded
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(key, remap.clone());
        Ok(remap)
    }

    fn forget(&self, table: TableId, file: &FileId) -> Result<()> {
        self.decoded
            .lock()
            .map_err(|_| Error::Poisoned)?
            .remove(&(table, file.clone()));
        self.chunks
            .lock()
            .map_err(|_| Error::Poisoned)?
            .forget_file(table, file);
        Ok(())
    }

    /// Bytes of cached chunks held.
    pub(crate) fn chunk_bytes(&self) -> Result<usize> {
        Ok(self.chunks.lock().map_err(|_| Error::Poisoned)?.bytes)
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
    /// Bytes of the table's remap records and chunks.
    pub bytes: u64,
}

/// Bounds of one [`StateStore::clean_remaps`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemapLimits {
    /// Materialize while more retired files than this keep a remap.
    pub max_files: u64,
    /// Materialize while a translation chain is longer than this.
    pub max_depth: u64,
    /// Materialize while the remaps take more bytes than this.
    pub max_bytes: u64,
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
        self.files > limits.max_files
            || self.max_depth > limits.max_depth
            || self.bytes > limits.max_bytes
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
    /// REMAP_CHUNKS keys of the record's chunks.
    chunk_keys: Vec<Vec<u8>>,
    bytes: u64,
    /// Retired files this remap translates into.
    retired_targets: Vec<FileId>,
    referrers: u64,
    depth: u64,
}

impl RemapGraph {
    fn stats(&self) -> RemapStats {
        RemapStats {
            files: self.nodes.len() as u64,
            bytes: self.nodes.values().map(|node| node.bytes).sum(),
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
    memo: HashMap<FileId, Option<Arc<CommittedRemap>>>,
    /// Chunks read in this call, by header address and index. The memo holds
    /// each header for the whole call, so an address cannot be reused here.
    chunks: HashMap<(usize, usize), Arc<RemapChunk>>,
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
            chunks: HashMap::new(),
        }
    }

    fn remap(&mut self, file: &FileId) -> Result<Option<Arc<CommittedRemap>>> {
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

    /// Chunk `index` of `remap`, read in this view.
    fn chunk(
        &mut self,
        file: &FileId,
        remap: &CommittedRemap,
        index: usize,
    ) -> Result<Arc<RemapChunk>> {
        if let Some(inline) = &remap.inline {
            return Ok(inline[index].clone());
        }
        let (start, end) = remap.header.chunks[index];
        let key = (self.table, file.clone(), start);
        let cached = self
            .cache
            .chunks
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(&key);
        if let Some(chunk) = cached {
            return Ok(chunk);
        }
        let cf = self
            .db
            .cf_handle(REMAP_CHUNKS)
            .expect("opened column family");
        let bytes = self
            .db
            .get_pinned_cf_opt(&cf, chunk_key(&self.table, file, start), self.options)?
            .ok_or_else(|| {
                Error::RecoveryRequired("lazy remap chunk is missing; rebuild the index".into())
            })?;
        let chunk: RemapChunk = decode(&bytes)?;
        if chunk.old_start != start || chunk.old_end != end {
            return Err(malformed());
        }
        let chunk = Arc::new(chunk);
        self.cache
            .chunks
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(key, chunk.clone());
        Ok(chunk)
    }

    /// Target index and new position of `position` in chunk `index`.
    fn translate_chunk(
        &mut self,
        file: &FileId,
        remap: &Arc<CommittedRemap>,
        index: usize,
        position: u64,
    ) -> Result<Option<(u32, u64)>> {
        let key = (Arc::as_ptr(remap) as usize, index);
        let chunk = match self.chunks.get(&key) {
            Some(chunk) => chunk.clone(),
            None => {
                let chunk = self.chunk(file, remap, index)?;
                self.chunks.insert(key, chunk.clone());
                chunk
            }
        };
        chunk.translate(position)
    }

    /// Translate through every visible remap. `None` means the raw file is live.
    pub(crate) fn resolve(&mut self, raw: &RowLocation) -> Result<Option<RowLocation>> {
        let mut current: Option<RowLocation> = None;
        for _ in 0..MAX_REMAP_HOPS {
            let location = current.as_ref().unwrap_or(raw);
            let Some(remap) = self.remap(&location.data_file_id)? else {
                return Ok(current);
            };
            let missed = || {
                Error::RecoveryRequired(
                    "row index points at a row a lazy remap did not move; rebuild the index".into(),
                )
            };
            let index = remap
                .chunk_index(location.row_position)
                .ok_or_else(missed)?;
            let (target, position) = self
                .translate_chunk(&location.data_file_id, &remap, index, location.row_position)?
                .ok_or_else(missed)?;
            let target = remap
                .header
                .targets
                .get(target as usize)
                .ok_or_else(malformed)?;
            current = Some(translated(target, position, location));
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
                file.encode_staged()?,
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
            files.push(FileRemap::decode_staged(&value)?);
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
        let chunks_cf = self
            .0
            .db
            .cf_handle(REMAP_CHUNKS)
            .expect("opened column family");
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
            // The header and its chunks commit together; a reader that sees
            // the header sees every chunk.
            let staged = file.staged()?;
            let header = RemapHeader {
                source: staged.source,
                targets: staged.targets,
                survivors: staged.survivors,
                runs: staged.runs,
                chunks: staged
                    .chunks
                    .iter()
                    .map(|chunk| (chunk.old_start, chunk.old_end))
                    .collect(),
                chunk_bytes: staged.chunks.iter().map(RemapChunk::stored_bytes).sum(),
            };
            batch.put_cf(
                &remaps_cf,
                file_prefix(table, &file.source),
                tagged(&header)?,
            );
            for chunk in &staged.chunks {
                batch.put_cf(
                    &chunks_cf,
                    chunk_key(table, &file.source, chunk.old_start),
                    bincode::serialize(chunk)?,
                );
            }
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
                max_bytes: 0,
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
            if remap.header.source != file {
                return Err(Error::InvalidState(
                    "lazy remap key and source differ".into(),
                ));
            }
            targets.push((
                file.clone(),
                remap
                    .targets()
                    .iter()
                    .map(|target| target.data_file_id.clone())
                    .collect::<Vec<_>>(),
            ));
            let chunk_keys = if remap.inline.is_some() {
                Vec::new()
            } else {
                remap
                    .header
                    .chunks
                    .iter()
                    .map(|(start, _)| chunk_key(table, &file, *start))
                    .collect()
            };
            nodes.insert(
                file,
                RemapNode {
                    key: key.to_vec(),
                    chunk_keys,
                    bytes: remap.stored_bytes,
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
        let chunks_cf = self
            .0
            .db
            .cf_handle(REMAP_CHUNKS)
            .expect("opened column family");
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
                    for key in &node.chunk_keys {
                        batch.delete_cf(&chunks_cf, key);
                    }
                    // No raw entry names the retired file any more.
                    self.mark_file_released(&mut batch, *table, &file);
                    removed.push((file, node));
                }
            }
            if removed.is_empty() {
                break;
            }
            self.write(batch)?;
            for (file, node) in removed {
                dropped += 1;
                self.0.remap_cache.forget(*table, &file)?;
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
        assert_eq!(remap.runs(), 4);
        assert_eq!(remap.survivors(), 6);
        let at = |position| {
            remap
                .translate(&location("a", position, 1, 3))
                .unwrap()
                .map(|row| {
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
        let translated = remap.translate(&location("a", 4, 1, 3)).unwrap().unwrap();
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
        max_bytes: u64::MAX,
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
    fn binaries_without_the_remap_families_refuse_to_open_the_index() {
        // Every index this binary opens has both families, with or without
        // remaps. A binary that predates either lists only the older families
        // and RocksDB refuses the open, so it can never ignore remaps, or read
        // a chunked record as the single-value form, and misread raw entries
        // in retired files.
        let directory = tempfile::tempdir().unwrap();
        drop(StateStore::open(directory.path(), StateStoreOptions::default()).unwrap());
        for missing in [REMAPS, REMAP_CHUNKS] {
            let older: Vec<_> = crate::STATE_COLUMN_FAMILIES
                .into_iter()
                .filter(|name| *name != missing)
                .collect();
            assert_eq!(older.len(), crate::STATE_COLUMN_FAMILIES.len() - 1);
            let Err(error) = DB::open_cf(&rocksdb::Options::default(), directory.path(), older)
            else {
                panic!("an older family list must not open the index");
            };
            assert!(error.to_string().contains(missing), "{error}");
        }
    }

    /// Deterministic pseudo-random numbers for encoding tests.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }
    }

    /// Ordered runs with every shape the encoding distinguishes: adjacent and
    /// gapped old positions, forward and backward new positions, target
    /// switches, long runs and positions near the top of the range.
    fn random_runs(seed: u64, count: usize, targets: u32) -> Vec<RemapRun> {
        let mut rng = Lcg(seed);
        let mut runs = Vec::with_capacity(count);
        let mut old = rng.next() % 3;
        for _ in 0..count {
            let len = match rng.next() % 8 {
                0 => 1 + rng.next() % 100_000,
                1 | 2 => 2 + rng.next() % 5,
                _ => 1,
            };
            let new_start = match rng.next() % 6 {
                0 => rng.next() * 1_000_003,
                1 => (1 << 50) + rng.next(),
                _ => rng.next() % 4096,
            };
            runs.push(RemapRun {
                old_start: old,
                new_start,
                len,
                target: (rng.next() % u64::from(targets)) as u32,
            });
            old += len
                + match rng.next() % 5 {
                    0 => rng.next() % 50,
                    1 => rng.next() * 7_919,
                    _ => 0,
                };
        }
        runs
    }

    fn targets(count: u32) -> Vec<RemapTarget> {
        (0..count)
            .map(|index| RemapTarget {
                data_file_id: FileId(format!("out-{index}")),
                data_sequence_number: 3,
                spec_id: 0,
                partition: Vec::new(),
            })
            .collect()
    }

    #[test]
    fn chunks_round_trip_any_ordered_runs_and_translate_like_the_run_list() {
        for seed in 0..20 {
            let runs = random_runs(seed, 1 + (seed as usize * 997) % 9_000, 1 + seed as u32 % 4);
            let first = &runs[..runs.len().min(CHUNK_RUNS)];
            let chunk = RemapChunk::encode(first).unwrap();
            assert_eq!(chunk.decode().unwrap(), first);
            assert_eq!(chunk.restarts.len(), (first.len() - 1) / RESTART_RUNS);
            // Scanning from restart points agrees with the decoded runs at
            // every boundary and in every gap.
            for run in first {
                for position in [
                    run.old_start,
                    run.end() - 1,
                    run.end(),
                    run.old_start.saturating_sub(1),
                ] {
                    assert_eq!(
                        chunk.translate(position).unwrap(),
                        translate_in(first, position)
                    );
                }
            }
            let remap = FileRemap::from_runs(FileId("in".into()), targets(4), &runs).unwrap();
            assert_eq!(remap.runs(), runs.len() as u64);
            assert_eq!(
                remap.survivors(),
                runs.iter().map(|run| run.len).sum::<u64>()
            );
            remap.validate().unwrap();
            let decoded = FileRemap::decode_staged(&remap.encode_staged().unwrap()).unwrap();
            assert_eq!(decoded, remap);
            // Translation agrees with a search of the whole run list, through
            // one translator in ascending order and fresh ones out of order.
            let mut rng = Lcg(seed + 100);
            let end = runs.last().unwrap().end();
            let expected = |position: u64| {
                translate_in(&runs, position)
                    .map(|(target, position)| (FileId(format!("out-{target}")), position))
            };
            // A fresh translator decodes a chunk per call; sample a few.
            for _ in 0..200 {
                let position = rng.next() % (end + 10);
                let got = remap
                    .translate(&location("in", position, 1, 1))
                    .unwrap()
                    .map(|row| (row.data_file_id, row.row_position));
                assert_eq!(got, expected(position), "seed {seed} position {position}");
            }
            let mut probes: Vec<u64> = (0..2_000).map(|_| rng.next() % (end + 10)).collect();
            probes.extend(
                runs.iter()
                    .flat_map(|run| [run.old_start, run.end() - 1, run.end()]),
            );
            probes.sort();
            let mut translator = remap.translator();
            for position in &probes {
                let got = translator
                    .translate(&location("in", *position, 1, 1))
                    .unwrap()
                    .map(|row| (row.data_file_id, row.row_position));
                assert_eq!(got, expected(*position));
            }
        }
    }

    #[test]
    fn corrupt_chunks_are_rejected() {
        let runs = random_runs(7, 50, 2);
        let chunk = RemapChunk::encode(&runs).unwrap();
        for cut in [0, 1, chunk.bytes.len() / 2, chunk.bytes.len() - 1] {
            let mut truncated = chunk.clone();
            truncated.bytes.truncate(cut);
            assert!(truncated.decode().is_err());
        }
        let mut extra = chunk.clone();
        extra.bytes.push(0);
        assert!(extra.decode().is_err());
        let mut wrong_end = chunk.clone();
        wrong_end.old_end += 1;
        assert!(wrong_end.decode().is_err());
        let mut restart = chunk.clone();
        restart.restarts[0].new_end += 1;
        assert!(restart.decode().is_err());
        let mut missing = chunk.clone();
        missing.restarts.pop();
        assert!(missing.decode().is_err());
        assert!(get_varint(&[0xff; 11], &mut 0).is_err());
    }

    /// A merge of three inputs interleaves them in the output, one run per
    /// row, about two bytes each: the shape merge compaction produces.
    fn interleaved(inputs: usize, rows: u64, output: &str, sequence: i64) -> Vec<FileRemap> {
        (0..inputs)
            .map(|input| {
                let mut remap = FileRemap::new(FileId(format!("in-{input}")));
                for row in 0..rows {
                    let position = row * inputs as u64 + input as u64;
                    remap
                        .push(row, &location(output, position, -1, 0), sequence)
                        .unwrap();
                }
                remap
            })
            .collect()
    }

    fn wide_key(input: usize, row: u64) -> PrimaryKey {
        let mut key = vec![input as u8];
        key.extend_from_slice(&row.to_be_bytes());
        PrimaryKey(key)
    }

    #[test]
    fn interleaved_remaps_commit_in_chunks_translate_and_clean_up() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let (inputs, rows) = (3usize, 10_000u64);
        for input in 0..inputs {
            let sequence = next_sequence(&store);
            commit(
                &store,
                &format!("insert-{input}"),
                OperationKind::Ingest,
                (0..rows)
                    .map(|row| IndexDelta {
                        key: wide_key(input, row),
                        expected: None,
                        replacement: Some(location(&format!("in-{input}"), row, sequence, 1)),
                    })
                    .collect(),
                None,
            );
        }
        let sequence = next_sequence(&store);
        let files = interleaved(inputs, rows, "merged", sequence);
        assert!(files.iter().all(|file| file.runs() == rows));
        // About two bytes per run, against 28 for the single-value form.
        let bytes: u64 = files.iter().map(FileRemap::encoded_bytes).sum();
        assert!(bytes <= 3 * rows * inputs as u64, "{bytes} bytes");
        let remap = LazyRemap {
            files,
            output_live_rows: vec![(FileId("merged".into()), rows * inputs as u64)],
        };
        commit(
            &store,
            "merge",
            OperationKind::Rewrite,
            vec![],
            Some(&remap),
        );

        let chunk_cf = store.0.db.cf_handle(REMAP_CHUNKS).unwrap();
        let chunks = store
            .0
            .db
            .iterator_cf(&chunk_cf, IteratorMode::Start)
            .count();
        assert_eq!(chunks, inputs * (rows as usize).div_ceil(CHUNK_RUNS));
        let stats = store.remap_stats(&TABLE).unwrap();
        assert_eq!(stats.files, inputs as u64);
        assert!(
            stats.bytes >= bytes && stats.bytes < bytes + 4096,
            "{stats:?}"
        );

        let keys: Vec<_> = (0..inputs)
            .flat_map(|input| (0..rows).map(move |row| wide_key(input, row)))
            .collect();
        let check = |store: &StateStore| {
            for (key, found) in keys.iter().zip(store.lookup_many(&TABLE, &keys).unwrap()) {
                let found = found.unwrap();
                let (input, row) = (
                    key.0[0] as u64,
                    u64::from_be_bytes(key.0[1..].try_into().unwrap()),
                );
                assert_eq!(found.data_file_id, FileId("merged".into()));
                assert_eq!(found.row_position, row * inputs as u64 + input);
                assert_eq!(found.data_sequence_number, sequence);
            }
        };
        check(&store);
        assert!(store.memory_usage().unwrap().remap_cache_bytes > 0);
        // A fresh reader finds the same rows without a warm cache.
        drop(chunk_cf);
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        check(&store);

        // The byte budget alone makes cleanup materialize, and dropping a
        // remap removes every chunk.
        let limits = RemapLimits {
            max_files: u64::MAX,
            max_depth: u64::MAX,
            max_bytes: 0,
            max_rows: u64::MAX,
        };
        let cleanup = store.clean_remaps(&TABLE, &limits).unwrap();
        assert_eq!(cleanup.rewritten_rows, rows * inputs as u64);
        assert_eq!(cleanup.stats, RemapStats::default());
        assert!(!store.has_remaps(&TABLE).unwrap());
        let chunk_cf = store.0.db.cf_handle(REMAP_CHUNKS).unwrap();
        assert_eq!(
            store
                .0
                .db
                .iterator_cf(&chunk_cf, IteratorMode::Start)
                .count(),
            0
        );
        assert_eq!(store.memory_usage().unwrap().remap_cache_bytes, 0);
        check(&store);
    }

    /// The pre-chunking record, as earlier releases wrote it.
    #[derive(Serialize)]
    struct LegacyRecord<'a> {
        source: &'a FileId,
        targets: &'a [RemapTarget],
        runs: &'a [RemapRun],
    }

    #[test]
    fn legacy_records_still_translate_stage_and_clean_up() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let mut model = insert(&store, "a", &[1, 2, 3, 4]);
        rewrite_all(&store, &mut model, "b");
        // Replace the committed record with its single-value legacy form.
        let remaps_cf = store.0.db.cf_handle(REMAPS).unwrap();
        let chunk_cf = store.0.db.cf_handle(REMAP_CHUNKS).unwrap();
        let runs = vec![RemapRun {
            old_start: 0,
            new_start: 0,
            len: 4,
            target: 0,
        }];
        let sequence = model[&1].data_sequence_number;
        let target = [RemapTarget {
            data_file_id: FileId("b".into()),
            data_sequence_number: sequence,
            spec_id: 0,
            partition: Vec::new(),
        }];
        let legacy = bincode::serialize(&LegacyRecord {
            source: &FileId("a".into()),
            targets: &target,
            runs: &runs,
        })
        .unwrap();
        store
            .0
            .db
            .put_cf(
                &remaps_cf,
                file_prefix(&TABLE, &FileId("a".into())),
                &legacy,
            )
            .unwrap();
        for entry in store.0.db.iterator_cf(&chunk_cf, IteratorMode::Start) {
            store.0.db.delete_cf(&chunk_cf, entry.unwrap().0).unwrap();
        }
        drop((remaps_cf, chunk_cf));
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        assert_model(&store, &model);
        assert_eq!(
            store.remap_stats(&TABLE).unwrap().bytes,
            legacy.len() as u64
        );
        // A legacy staged record decodes to the same remap.
        let staged = FileRemap::decode_staged(&legacy).unwrap();
        assert_eq!(
            staged,
            FileRemap::from_runs(FileId("a".into()), target.to_vec(), &runs).unwrap()
        );
        assert_eq!(store.materialize_remaps(&TABLE).unwrap(), 4);
        assert!(!store.has_remaps(&TABLE).unwrap());
        assert_model(&store, &model);
    }

    /// Lookups through a large interleaved remap: chunked records against the
    /// same remaps stored as single values, as earlier releases did. Run with
    /// `cargo test --release -p flow-state-store --lib remap_lookup_benchmark
    /// -- --ignored --nocapture`; `REMAP_BENCH_ROWS` sets rows per input.
    #[test]
    #[ignore]
    fn remap_lookup_benchmark() {
        let rows: u64 = std::env::var("REMAP_BENCH_ROWS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(500_000);
        let inputs = 2usize;
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        for input in 0..inputs {
            let sequence = next_sequence(&store);
            commit(
                &store,
                &format!("insert-{input}"),
                OperationKind::Ingest,
                (0..rows)
                    .map(|row| IndexDelta {
                        key: wide_key(input, row),
                        expected: None,
                        replacement: Some(location(&format!("in-{input}"), row, sequence, 1)),
                    })
                    .collect(),
                None,
            );
        }
        let sequence = next_sequence(&store);
        let files = interleaved(inputs, rows, "merged", sequence);
        let runs: u64 = files.iter().map(FileRemap::runs).sum();
        let legacy: Vec<(FileId, Vec<u8>)> = files
            .iter()
            .map(|file| {
                let staged = file.staged().unwrap();
                let runs: Vec<RemapRun> = staged
                    .chunks
                    .iter()
                    .flat_map(|chunk| chunk.decode().unwrap())
                    .collect();
                let bytes = bincode::serialize(&LegacyRecord {
                    source: &file.source,
                    targets: &file.targets,
                    runs: &runs,
                })
                .unwrap();
                (file.source.clone(), bytes)
            })
            .collect();
        let remap = LazyRemap {
            files,
            output_live_rows: vec![(FileId("merged".into()), rows * inputs as u64)],
        };
        commit(
            &store,
            "merge",
            OperationKind::Rewrite,
            vec![],
            Some(&remap),
        );
        let chunked_bytes = store.remap_stats(&TABLE).unwrap().bytes;
        let mut rng = Lcg(5);
        let batches: Vec<Vec<PrimaryKey>> = (0..200)
            .map(|_| {
                (0..1024)
                    .map(|_| wide_key((rng.next() % inputs as u64) as usize, rng.next() % rows))
                    .collect()
            })
            .collect();
        let time = |store: &StateStore| {
            let started = std::time::Instant::now();
            for batch in &batches {
                for found in store.lookup_many(&TABLE, batch).unwrap() {
                    assert_eq!(found.unwrap().data_file_id, FileId("merged".into()));
                }
            }
            started.elapsed().as_secs_f64() * 1e6 / (batches.len() * 1024) as f64
        };
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let chunked_cold = time(&store);
        let chunked_warm = time(&store);
        let cache = store.memory_usage().unwrap().remap_cache_bytes;
        {
            let remaps_cf = store.0.db.cf_handle(REMAPS).unwrap();
            let chunk_cf = store.0.db.cf_handle(REMAP_CHUNKS).unwrap();
            for (file, bytes) in &legacy {
                store
                    .0
                    .db
                    .put_cf(&remaps_cf, file_prefix(&TABLE, file), bytes)
                    .unwrap();
            }
            for entry in store.0.db.iterator_cf(&chunk_cf, IteratorMode::Start) {
                store.0.db.delete_cf(&chunk_cf, entry.unwrap().0).unwrap();
            }
        }
        drop(store);
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let legacy_bytes: usize = legacy.iter().map(|(_, bytes)| bytes.len()).sum();
        let legacy_cold = time(&store);
        let legacy_warm = time(&store);
        // Without remaps: every entry rewritten to its output location.
        store.materialize_remaps(&TABLE).unwrap();
        let direct = time(&store);
        println!("no remap (materialized): {direct:.2} us/key");
        println!(
            "rows {} runs {runs}: chunked {chunked_bytes} bytes ({:.2}/run), cold {chunked_cold:.2} us/key, warm {chunked_warm:.2} us/key, chunk cache {cache} bytes; single-value {legacy_bytes} bytes ({:.2}/run), cold {legacy_cold:.2} us/key, warm {legacy_warm:.2} us/key",
            rows * inputs as u64,
            chunked_bytes as f64 / runs as f64,
            legacy_bytes as f64 / runs as f64,
        );
    }

    #[test]
    fn the_chunk_cache_stays_within_its_byte_limit() {
        let chunk = Arc::new(RemapChunk::encode(&random_runs(3, 100, 2)).unwrap());
        let mut cache = ChunkCache {
            limit: 10 * cached_bytes(&chunk),
            ..ChunkCache::default()
        };
        for start in 0..50 {
            cache.insert((TABLE, FileId("f".into()), start), chunk.clone());
            assert!(cache.bytes <= cache.limit);
        }
        assert_eq!(cache.chunks.len(), 10);
        assert!(cache.get(&(TABLE, FileId("f".into()), 49)).is_some());
        assert!(cache.get(&(TABLE, FileId("f".into()), 0)).is_none());
        cache.forget_file(TABLE, &FileId("f".into()));
        assert_eq!(
            (cache.bytes, cache.chunks.len(), cache.order.len()),
            (0, 0, 0)
        );
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
        let stats = store.remap_stats(&TABLE).unwrap();
        // n0..n3 and a2 retired into c, c into d.
        assert_eq!((stats.files, stats.max_depth), (6, 2));
        assert!(stats.bytes > 0);
        let cleanup = store.clean_remaps(&TABLE, &UNBOUNDED).unwrap();
        assert_eq!(cleanup.dropped_files, 0, "n* and a2 hold raw entries");
        assert_eq!(cleanup.stats, store.remap_stats(&TABLE).unwrap());
        assert_model(&store, &model);

        // Drain at most two rows: progress, continuation, nothing dropped.
        let limits = RemapLimits {
            max_files: 0,
            max_depth: 0,
            max_bytes: u64::MAX,
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
            max_bytes: u64::MAX,
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
            max_bytes: u64::MAX,
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
            let stats = store.remap_stats(&TABLE).unwrap();
            assert_eq!((stats.files, stats.max_depth), (2, 2));
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
