//! A single-writer, segmented transaction journal. A returned `commit` or
//! `flush_commits` has passed `sync_data`; staging alone never advances the
//! durable transaction watermark or exposes terminals to readers.
//!
//! Methods perform blocking disk IO. Run this owner on a dedicated blocking
//! actor, rather than holding a Tokio worker while the disk flushes.

use flow_model::{JournalChunkRef, JournalChunks, PgLsn, SourceTransaction};

mod chunks;
mod frame;
pub mod legacy;
pub use chunks::{ChunkIter, ReplayChunks, ReplayCursor};
mod transactions;
use frame::{
    ABORT, BOUNDED_COMMIT, CHUNK, COMMIT, HEADER, LEGACY_COMMIT, decode_transaction, read_frame,
};
use fs2::FileExt;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};
use thiserror::Error;
pub use transactions::{TransactionIter, TransactionLog};

#[derive(Clone, Debug)]
pub struct JournalConfig {
    pub segment_bytes: u64,
    pub quota_bytes: u64,
    pub max_frame_bytes: u32,
    pub max_open_transactions: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            quota_bytes: 16 << 30,
            max_frame_bytes: 4 << 20,
            max_open_transactions: 1024,
        }
    }
}

impl JournalConfig {
    /// Validate frame, segment, quota, and transaction limits without opening storage.
    pub fn validate(&self) -> Result<()> {
        if self.max_frame_bytes == 0
            || u64::from(self.max_frame_bytes) + HEADER as u64 > self.segment_bytes
            || self.segment_bytes > self.quota_bytes
            || self.max_open_transactions == 0
        {
            return Err(Error::Config(
                "frame must fit a segment, segment must fit quota, and transaction limits must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("journal IO: {0}")]
    Io(#[from] io::Error),
    #[error("journal serialization: {0}")]
    Codec(#[from] bincode::Error),
    #[error("invalid journal configuration: {0}")]
    Config(&'static str),
    #[error(
        "journal quota exhausted: {used} bytes in use, {requested} requested, {quota} quota; materialize pending transactions or increase quota"
    )]
    Quota {
        used: u64,
        requested: u64,
        quota: u64,
    },
    #[error("journal transaction limit exceeded: {0}")]
    Limit(&'static str),
    #[error("invalid journal transaction: {0}")]
    Transaction(&'static str),
    #[error(
        "unsupported journal frame version {0}; use a compatible binary without modifying this journal"
    )]
    Version(u16),
    #[error(
        "unsupported journal record kind {0}; use a compatible binary without modifying this journal"
    )]
    RecordKind(u8),
    #[error(
        "stored journal frame has {length} bytes, exceeding configured limit {limit}; increase the frame limit to reopen this journal"
    )]
    FrameLimit { length: u32, limit: u32 },
    #[error("journal frame is corrupt")]
    Corrupt,
    #[error(
        "journal segment {segment} is damaged at byte {offset} ({reason}); segments are synchronized before rollover, so damage before the final segment is storage corruption, not a torn write. No segment file was modified; restore the state directory or resynchronize the source"
    )]
    SegmentCorrupt {
        segment: u64,
        offset: u64,
        reason: &'static str,
    },
    #[error(
        "journal tail damage in segment {segment} at byte {offset} ({reason}) would leave the journal at {recovered}, below its recorded durable position {floor}. No segment file was modified; restore the state directory or resynchronize the source"
    )]
    DurableTail {
        segment: u64,
        offset: u64,
        reason: &'static str,
        recovered: PgLsn,
        floor: PgLsn,
    },
    #[error("journal writer experienced an IO failure; reopen it to resolve the durable prefix")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait ChunkReader {
    fn chunks(&self, chunks: &JournalChunks) -> Result<ChunkIter>;
    fn read_chunk(&self, reference: &JournalChunkRef) -> Result<Vec<u8>>;
    /// Create a reader-local file cursor for sequential transaction replay.
    fn replay_cursor(&self) -> ReplayCursor;
}

/// Immutable reads do not borrow or lock the writer. The coordinator must
/// retain the corresponding source transaction until its readers are done.
#[derive(Clone)]
pub struct JournalReader {
    catalog: transactions::SharedCatalog,
    root: PathBuf,
    max_frame_bytes: u32,
}

impl JournalReader {
    pub fn transactions_after(&self, lsn: PgLsn) -> Result<TransactionIter> {
        TransactionLog::retained(self.clone()).after(lsn)
    }
}

impl ChunkReader for JournalReader {
    fn replay_cursor(&self) -> ReplayCursor {
        ReplayCursor::new(self.clone())
    }
    fn chunks(&self, chunks: &JournalChunks) -> Result<ChunkIter> {
        ChunkIter::open(self.clone(), chunks)
    }
    fn read_chunk(&self, reference: &JournalChunkRef) -> Result<Vec<u8>> {
        let mut file = File::open(segment_path(&self.root, reference.segment))?;
        file.seek(SeekFrom::Start(reference.offset))?;
        let frame = read_frame(&mut file, self.max_frame_bytes)?;
        if frame.kind != CHUNK || frame.payload.len() != reference.length as usize {
            return Err(Error::Corrupt);
        }
        Ok(frame.payload)
    }
}

impl ChunkReader for Journal {
    fn replay_cursor(&self) -> ReplayCursor {
        self.reader().replay_cursor()
    }
    fn chunks(&self, chunks: &JournalChunks) -> Result<ChunkIter> {
        self.reader().chunks(chunks)
    }
    fn read_chunk(&self, reference: &JournalChunkRef) -> Result<Vec<u8>> {
        self.reader().read_chunk(reference)
    }
}

pub struct Recovery {
    /// Ordered complete transactions above the durable reclamation watermark.
    pub transactions: TransactionLog,
    /// Torn-tail bytes discarded from the final segment.
    pub truncated_bytes: u64,
    pub reclaimed_lsn: PgLsn,
}

pub struct Journal {
    root: PathBuf,
    config: JournalConfig,
    _lock: File,
    writer: File,
    segment: u64,
    offset: u64,
    sequence: u64,
    bytes: u64,
    segments: BTreeMap<u64, u64>,
    pending: HashMap<u32, JournalChunks>,
    staged_xids: HashSet<u32>,
    staged_segments: BTreeSet<u64>,
    segment_pins: BTreeMap<u64, u64>,
    terminal_writer: transactions::Writer,
    catalog: transactions::SharedCatalog,
    durable_lsn: PgLsn,
    staged_lsn: PgLsn,
    reclaimed_lsn: PgLsn,
    poisoned: bool,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>, config: JournalConfig) -> Result<(Self, Recovery)> {
        Self::open_with_floor(path, config, PgLsn(0))
    }

    /// Open and recover the journal. Only the final segment can hold a torn
    /// write, since rollover synchronizes a segment before closing it; damage
    /// in an earlier segment is corruption and fails without modifying any
    /// file. A torn tail is truncated only if every transaction through
    /// `durable_floor`, a durable position the caller recorded independently
    /// (such as its source ledger's), survives; otherwise open fails too.
    pub fn open_with_floor(
        path: impl AsRef<Path>,
        config: JournalConfig,
        durable_floor: PgLsn,
    ) -> Result<(Self, Recovery)> {
        config.validate()?;
        let root = path.as_ref().to_path_buf();
        create_dir_all_durable(&root, &mut sync_dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("writer.lock"))?;
        lock.try_lock_exclusive()?;
        let reclaimed_lsn = read_checkpoint(&root)?;
        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(id) = name
                .to_str()
                .and_then(|n| n.strip_suffix(".segment"))
                .and_then(|n| n.parse::<u64>().ok())
            {
                segments.insert(id, entry.metadata()?.len());
            }
        }
        transactions::reset(&root)?;
        let catalog = Arc::new(Mutex::new(transactions::Catalog::default()));
        let mut terminal_writer = transactions::Writer::new(root.clone());
        let mut truncated_bytes = 0;
        let mut pending: HashMap<u32, JournalChunks> = HashMap::new();
        let mut sequence = None;
        let mut durable_lsn = reclaimed_lsn;
        let mut segment_pins = BTreeMap::new();
        let mut truncated = false;
        let mut repaired = BTreeMap::new();
        let last = segments.last_key_value().map(|(&id, _)| id);
        for (&id, &size) in &segments {
            let path = segment_path(&root, id);
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let mut offset = 0;
            let mut damage = None;
            while offset < size {
                let frame = match read_frame(&mut file, config.max_frame_bytes) {
                    Ok(frame) => frame,
                    Err(Error::Io(e)) if e.kind() != io::ErrorKind::UnexpectedEof => {
                        return Err(e.into());
                    }
                    Err(
                        error @ (Error::Version(_)
                        | Error::RecordKind(_)
                        | Error::FrameLimit { .. }),
                    ) => return Err(error),
                    Err(Error::Io(_)) => {
                        damage = Some("incomplete frame");
                        break;
                    }
                    Err(_) => {
                        damage = Some("frame header or checksum mismatch");
                        break;
                    }
                };
                if sequence.is_some_and(|s| frame.sequence != s) {
                    damage = Some("frame sequence gap");
                    break;
                }
                let reference = JournalChunkRef {
                    segment: id,
                    offset,
                    length: frame.payload.len() as u32,
                };
                let accepted = match frame.kind {
                    CHUNK => {
                        if pending.len() >= config.max_open_transactions
                            && !pending.contains_key(&frame.xid)
                        {
                            return Err(Error::Limit("recovery needs more open transactions"));
                        }
                        chunks::include(pending.entry(frame.xid).or_default(), &reference)?;
                        true
                    }
                    LEGACY_COMMIT | BOUNDED_COMMIT | COMMIT => {
                        let decoded =
                            decode_transaction(frame.kind, &frame.payload, config.max_frame_bytes);
                        match decoded {
                            Ok(txn)
                                if txn.xid == frame.xid
                                    && validate_transaction(&txn).is_ok()
                                    && (txn.end_lsn <= reclaimed_lsn
                                        || (txn.end_lsn > durable_lsn
                                            && pending
                                                .get(&frame.xid)
                                                .cloned()
                                                .unwrap_or_default()
                                                == txn.mutation_chunks)) =>
                            {
                                pending.remove(&frame.xid);
                                if txn.end_lsn > reclaimed_lsn {
                                    durable_lsn = txn.end_lsn;
                                    terminal_writer.append(&reference, &txn)?;
                                    let first = txn
                                        .mutation_chunks
                                        .first
                                        .as_ref()
                                        .map_or(id, |r| r.segment);
                                    *segment_pins.entry(first).or_insert(0) += 1;
                                    catalog
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .count += 1;
                                }
                                true
                            }
                            _ => false,
                        }
                    }
                    ABORT if frame.payload.is_empty() => {
                        pending.remove(&frame.xid);
                        true
                    }
                    ABORT => false,
                    kind => return Err(Error::RecordKind(kind)),
                };
                if !accepted {
                    damage = Some("invalid transaction record");
                    break;
                }
                sequence = Some(frame.sequence.checked_add(1).ok_or(Error::Corrupt)?);
                offset += HEADER as u64 + frame.payload.len() as u64;
            }
            if let Some(reason) = damage {
                // Refuse before modifying anything: later segments hold
                // transactions that were synchronized after this damage.
                if Some(id) != last {
                    return Err(Error::SegmentCorrupt {
                        segment: id,
                        offset,
                        reason,
                    });
                }
                if durable_lsn < durable_floor {
                    return Err(Error::DurableTail {
                        segment: id,
                        offset,
                        reason,
                        recovered: durable_lsn,
                        floor: durable_floor,
                    });
                }
                truncated_bytes += size - offset;
                file.set_len(offset)?;
                file.sync_all()?;
                truncated = true;
            }
            repaired.insert(id, offset);
        }
        if truncated {
            sync_dir(&root)?;
        }
        // Incomplete transactions have no terminal marker and must be resent by
        // the source. Do not retain their XIDs across connection lifetimes.
        let segment = repaired.last_key_value().map(|(&id, _)| id).unwrap_or(0);
        let offset = repaired.get(&segment).copied().unwrap_or(0);
        let writer = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .open(segment_path(&root, segment))?;
        repaired.entry(segment).or_insert(0);
        sync_dir(&root)?;
        terminal_writer.flush()?;
        let index_bytes = terminal_writer
            .segments()
            .values()
            .map(|segment| segment.bytes)
            .sum::<u64>();
        let bytes = repaired.values().sum::<u64>() + index_bytes;
        if bytes > config.quota_bytes {
            return Err(Error::Quota {
                used: bytes,
                requested: 0,
                quota: config.quota_bytes,
            });
        }
        {
            let mut catalog = catalog
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            catalog.segments = terminal_writer.segments().clone();
            catalog.durable_lsn = durable_lsn;
            catalog.reclaimed_lsn = reclaimed_lsn;
        }
        let mut journal = Self {
            root,
            config,
            _lock: lock,
            writer,
            segment,
            offset,
            sequence: sequence.unwrap_or(0),
            bytes,
            segments: repaired,
            pending,
            staged_xids: HashSet::new(),
            staged_segments: BTreeSet::new(),
            segment_pins,
            terminal_writer,
            catalog,
            durable_lsn,
            staged_lsn: durable_lsn,
            reclaimed_lsn,
            poisoned: false,
        };
        // Terminal aborts separate orphan chunks from a future transaction that
        // reuses the same XID. Without them the next recovery could join both.
        let abandoned: Vec<_> = journal.pending.keys().copied().collect();
        for xid in abandoned {
            journal.abort(xid)?;
        }
        journal.writer.sync_data()?;
        let recovery = Recovery {
            transactions: journal.transactions(),
            truncated_bytes,
            reclaimed_lsn,
        };
        Ok((journal, recovery))
    }

    pub fn durable_lsn(&self) -> PgLsn {
        self.durable_lsn
    }
    /// Last complete terminal written by this owner, possibly not yet durable.
    /// This is an ordering/replay guard, never an acknowledgement frontier.
    pub fn staged_lsn(&self) -> PgLsn {
        self.staged_lsn
    }
    pub fn bytes_used(&self) -> u64 {
        self.bytes
    }
    pub fn transactions(&self) -> TransactionLog {
        TransactionLog::retained(self.reader())
    }
    pub fn reader(&self) -> JournalReader {
        JournalReader {
            catalog: self.catalog.clone(),
            root: self.root.clone(),
            max_frame_bytes: self.config.max_frame_bytes,
        }
    }

    pub fn append_chunk(&mut self, xid: u32, payload: &[u8]) -> Result<JournalChunkRef> {
        if self.staged_xids.contains(&xid) {
            return Err(Error::Transaction(
                "XID has a staged terminal; flush before reuse",
            ));
        }
        if !self.pending.contains_key(&xid)
            && self.pending.len() + self.staged_xids.len() >= self.config.max_open_transactions
        {
            return Err(Error::Limit("open transactions"));
        }
        let reference = self.append(CHUNK, xid, payload)?;
        chunks::include(self.pending.entry(xid).or_default(), &reference)?;
        Ok(reference)
    }

    pub fn transaction_chunks(&self, xid: u32) -> JournalChunks {
        self.pending.get(&xid).cloned().unwrap_or_default()
    }

    /// Flushes the terminal commit and every referenced chunk before advancing
    /// `durable_lsn`. The caller owns policy for reporting this LSN to Postgres.
    pub fn commit(&mut self, txn: SourceTransaction) -> Result<()> {
        self.stage_commit(txn)?;
        self.flush_commits()
    }

    /// Append a complete terminal without exposing it to journal readers.
    /// Open and staged XIDs share the configured transaction limit. Call
    /// `flush_commits` before reporting durability or releasing source spools.
    pub fn stage_commit(&mut self, txn: SourceTransaction) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        validate_transaction(&txn)?;
        if txn.table_mutation_counts.is_none() {
            return Err(Error::Transaction(
                "new terminals require per-table mutation counts",
            ));
        }
        if txn.end_lsn <= self.staged_lsn || txn.end_lsn <= txn.commit_lsn {
            return Err(Error::Transaction(
                "end LSN must advance and follow commit LSN",
            ));
        }
        if self.staged_xids.contains(&txn.xid) {
            return Err(Error::Transaction("XID already has a staged terminal"));
        }
        if !self.pending.contains_key(&txn.xid)
            && self.pending.len() + self.staged_xids.len() >= self.config.max_open_transactions
        {
            return Err(Error::Limit("open transactions"));
        }
        if self.transaction_chunks(txn.xid) != txn.mutation_chunks {
            return Err(Error::Transaction(
                "terminal references must cover exactly the transaction chunks in order",
            ));
        }
        let payload = bincode::serialize(&txn)?;
        let reference = self.append(COMMIT, txn.xid, &payload)?;
        if let Err(error) = self.terminal_writer.append(&reference, &txn) {
            self.poisoned = true;
            return Err(error);
        }
        self.bytes += transactions::ENTRY_BYTES;
        self.staged_lsn = txn.end_lsn;
        self.staged_xids.insert(txn.xid);
        self.staged_segments.insert(reference.segment);
        let first = txn
            .mutation_chunks
            .first
            .as_ref()
            .map_or(reference.segment, |r| r.segment);
        // Reclamation must retain staged chunks even though readers cannot yet
        // discover their terminal. The pin survives the durability barrier.
        *self.segment_pins.entry(first).or_insert(0) += 1;
        self.pending.remove(&txn.xid);
        Ok(())
    }

    /// Durably seal all staged transactions and publish their index extents as
    /// one reader-visible frontier. Rollover synchronizes every previous journal
    /// segment before closing it; this barrier synchronizes the active segment.
    pub fn flush_commits(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.staged_xids.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.terminal_writer.flush() {
            self.poisoned = true;
            return Err(error);
        }
        let started = Instant::now();
        let synced = self.writer.sync_data();
        metrics::histogram!(
            "flow_capture_phase_seconds",
            "phase" => "journal_sync",
            "result" => if synced.is_ok() { "success" } else { "error" }
        )
        .record(started.elapsed().as_secs_f64());
        if let Err(error) = synced {
            self.poisoned = true;
            return Err(error.into());
        }
        self.durable_lsn = self.staged_lsn;
        let mut catalog = self
            .catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Publish both the index extent and terminal frontier under one lock,
        // after the index write and the journal sync. A concurrent reader must
        // never binary-search bytes still held in the writer's buffer.
        for segment in &self.staged_segments {
            catalog
                .segments
                .insert(*segment, self.terminal_writer.segments()[segment].clone());
        }
        catalog.durable_lsn = self.durable_lsn;
        catalog.count += self.staged_xids.len() as u64;
        drop(catalog);
        metrics::histogram!("flow_journal_commit_group_transactions")
            .record(self.staged_xids.len() as f64);
        self.staged_xids.clear();
        self.staged_segments.clear();
        Ok(())
    }

    pub fn abort(&mut self, xid: u32) -> Result<()> {
        if self.staged_xids.contains(&xid) {
            return Err(Error::Transaction("cannot abort a staged commit"));
        }
        self.append(ABORT, xid, &[])?;
        self.pending.remove(&xid);
        Ok(())
    }

    pub fn read_chunk(&self, reference: &JournalChunkRef) -> Result<Vec<u8>> {
        self.reader().read_chunk(reference)
    }

    /// Delete only whole segments with no live references. Persist the supplied
    /// all-tables materialization watermark before removing any segment.
    pub fn reclaim(&mut self, materialized_lsn: PgLsn) -> Result<u64> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if materialized_lsn < self.reclaimed_lsn || materialized_lsn > self.durable_lsn {
            return Err(Error::Transaction(
                "reclaim watermark outside durable range",
            ));
        }
        let mut completed = 0;
        let mut released = BTreeMap::<u64, u64>::new();
        // Terminal indexes seek beyond the last reclaimed LSN. Each completed
        // transaction is visited once, independent of the remaining backlog.
        for transaction in self.reader().transactions_after(self.reclaimed_lsn)? {
            let transaction = transaction?;
            if transaction.end_lsn > materialized_lsn {
                break;
            }
            let first = transaction
                .mutation_chunks
                .first
                .as_ref()
                .map(|r| r.segment);
            // Empty transactions pin only their terminal segment. Find it from
            // the bounded segment catalog's first terminal above this LSN.
            let first = match first {
                Some(first) => first,
                None => self
                    .catalog
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .segments
                    .iter()
                    .find(|(_, segment)| segment.last_lsn >= transaction.end_lsn)
                    .map(|(id, _)| *id)
                    .ok_or(Error::Corrupt)?,
            };
            *released.entry(first).or_default() += 1;
            completed += 1;
        }
        for (segment, count) in &released {
            if self.segment_pins.get(segment).copied().unwrap_or(0) < *count {
                return Err(Error::Corrupt);
            }
        }
        if materialized_lsn > self.reclaimed_lsn {
            write_checkpoint(&self.root, materialized_lsn)?;
        }
        for (segment, released) in released {
            let count = self.segment_pins.get_mut(&segment).ok_or(Error::Corrupt)?;
            *count -= released;
            if *count == 0 {
                self.segment_pins.remove(&segment);
            }
        }
        self.reclaimed_lsn = materialized_lsn;
        {
            let mut catalog = self
                .catalog
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            catalog.reclaimed_lsn = materialized_lsn;
            catalog.count = catalog.count.checked_sub(completed).ok_or(Error::Corrupt)?;
        }
        let oldest = self
            .segment_pins
            .keys()
            .copied()
            .chain(
                self.pending
                    .values()
                    .filter_map(|chunks| chunks.first.as_ref().map(|r| r.segment)),
            )
            .min()
            .unwrap_or(self.segment);
        let removable: Vec<_> = self
            .segments
            .range(..oldest)
            .map(|(&id, &size)| (id, size))
            .collect();
        let mut removed = 0;
        for (id, size) in removable {
            transactions::remove(&self.root, id)?;
            let index_bytes = self
                .catalog
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .segments
                .remove(&id)
                .map_or(0, |segment| segment.bytes);
            self.bytes -= index_bytes;
            removed += index_bytes;
            self.terminal_writer.remove_segment(id);
            fs::remove_file(segment_path(&self.root, id))?;
            // Persist removals oldest first. Recovery rejects a sequence gap
            // before the final segment, which an older segment reappearing
            // after a crash, next to a removed newer one, would create.
            sync_dir(&self.root)?;
            self.segments.remove(&id);
            self.bytes -= size;
            removed += size;
        }
        Ok(removed)
    }

    fn append(&mut self, kind: u8, xid: u32, payload: &[u8]) -> Result<JournalChunkRef> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if payload.len() > self.config.max_frame_bytes as usize {
            return Err(Error::Limit(
                "frame bytes; split the payload into bounded chunks",
            ));
        }
        let length = HEADER as u64 + payload.len() as u64;
        let index_bytes = if kind == COMMIT {
            transactions::ENTRY_BYTES
        } else {
            0
        };
        // Reserve one maximal terminal record and recovery aborts. A full
        // payload budget must not prevent sealing the transaction or reopening.
        let reserve = if kind == CHUNK {
            u64::from(self.config.max_frame_bytes)
                + HEADER as u64 * (self.pending.len() as u64 + 2)
                + transactions::ENTRY_BYTES
        } else {
            // A terminal releases its own pending-XID reserve, but must leave
            // enough room to abort every other orphan during recovery.
            HEADER as u64
                * (self.pending.len() - usize::from(self.pending.contains_key(&xid))) as u64
        };
        if self
            .bytes
            .saturating_add(length)
            .saturating_add(reserve)
            .saturating_add(index_bytes)
            > self.config.quota_bytes
        {
            return Err(Error::Quota {
                used: self.bytes,
                requested: length + reserve + index_bytes,
                quota: self.config.quota_bytes,
            });
        }
        let result = self.append_inner(kind, xid, payload, length);
        if matches!(result, Err(Error::Io(_))) {
            self.poisoned = true;
        }
        result
    }

    fn append_inner(
        &mut self,
        kind: u8,
        xid: u32,
        payload: &[u8],
        length: u64,
    ) -> Result<JournalChunkRef> {
        if self.offset + length > self.config.segment_bytes {
            self.writer.sync_data()?;
            self.segment = self.segment.checked_add(1).ok_or(Error::Corrupt)?;
            self.writer = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(segment_path(&self.root, self.segment))?;
            sync_dir(&self.root)?;
            self.offset = 0;
            self.segments.insert(self.segment, 0);
        }
        let header = frame::encode_header(kind, xid, self.sequence, payload);
        self.writer.write_all(&header)?;
        self.writer.write_all(payload)?;
        let reference = JournalChunkRef {
            segment: self.segment,
            offset: self.offset,
            length: payload.len() as u32,
        };
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Corrupt)?;
        self.offset += length;
        self.bytes += length;
        self.segments.insert(self.segment, self.offset);
        Ok(reference)
    }
}

fn segment_path(root: &Path, id: u64) -> PathBuf {
    root.join(format!("{id:020}.segment"))
}
// Persist each new directory's entry in its parent before creating descendants.
fn create_dir_all_durable(
    path: &Path,
    sync: &mut impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            // A previous attempt may have created this entry but failed its
            // parent barrier. Existing paths must retry that barrier too.
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            return sync(parent);
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "journal path is not a directory",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_dir_all_durable(parent, sync)?;
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(error),
    }
    sync(parent)
}

fn sync_dir(root: &Path) -> io::Result<()> {
    File::open(root)?.sync_all()
}

fn validate_transaction(txn: &SourceTransaction) -> Result<()> {
    use std::collections::HashSet;
    txn.validate_mutation_counts()
        .map_err(|_| Error::Transaction("invalid per-table mutation counts"))?;
    if txn.source_id.0.is_empty() || txn.begin_lsn > txn.commit_lsn || txn.end_lsn <= txn.commit_lsn
    {
        return Err(Error::Transaction(
            "invalid source identity or transaction LSNs",
        ));
    }
    let affected: HashSet<_> = txn.affected_tables.iter().collect();
    let schemas: HashSet<_> = txn.schema_versions.iter().map(|s| &s.table_id).collect();
    if affected.len() != txn.affected_tables.len()
        || schemas.len() != txn.schema_versions.len()
        || affected != schemas
        || (!txn.mutation_chunks.is_empty() && affected.is_empty())
    {
        return Err(Error::Transaction(
            "affected tables and schema versions must be unique and agree",
        ));
    }
    Ok(())
}

fn read_checkpoint(root: &Path) -> Result<PgLsn> {
    match fs::read(root.join("reclaimed")) {
        Ok(bytes)
            if bytes.len() == 12
                && crc32fast::hash(&bytes[..8])
                    == u32::from_le_bytes(bytes[8..].try_into().unwrap()) =>
        {
            Ok(PgLsn(u64::from_le_bytes(bytes[..8].try_into().unwrap())))
        }
        Ok(_) => Err(Error::Corrupt),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(PgLsn(0)),
        Err(e) => Err(e.into()),
    }
}

fn write_checkpoint(root: &Path, lsn: PgLsn) -> Result<()> {
    let mut bytes = lsn.0.to_le_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let mut file = File::create(root.join("reclaimed.tmp"))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(root.join("reclaimed.tmp"), root.join("reclaimed"))?;
    sync_dir(root)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod sync_failure_tests {
    use super::*;
    use flow_model::SourceId;

    #[test]
    fn new_nested_directories_sync_each_parent_and_propagate_failure() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let nested = first.join("journal");
        let mut synced = Vec::new();
        create_dir_all_durable(&nested, &mut |path| {
            // The child entry exists at its parent barrier; descendants have
            // not yet been created at the outer barrier.
            if path == root.path() {
                assert!(first.is_dir());
                assert!(!nested.exists());
            } else if path == first {
                assert!(nested.is_dir());
            }
            sync_dir(path)?;
            synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            &synced[synced.len() - 2..],
            [root.path().to_path_buf(), first]
        );
        let failed = root.path().join("failed");
        assert!(
            create_dir_all_durable(&failed.join("journal"), &mut |path| {
                if path == root.path() {
                    Err(io::Error::other("directory sync failed"))
                } else {
                    sync_dir(path)
                }
            })
            .is_err()
        );
        assert!(failed.is_dir());
        assert!(!failed.join("journal").exists());
        let mut retry = Vec::new();
        create_dir_all_durable(&failed.join("journal"), &mut |path| {
            sync_dir(path)?;
            retry.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert!(
            retry.contains(&root.path().to_path_buf()),
            "retry must persist the existing parent entry"
        );
        let mut existing = Vec::new();
        create_dir_all_durable(&failed.join("journal"), &mut |path| {
            existing.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(existing, [failed]);
    }

    #[test]
    fn failed_group_sync_keeps_reader_frontier_private_and_poisoned_until_reopen() {
        let root = tempfile::tempdir().unwrap();
        let (mut journal, _) = Journal::open(root.path(), JournalConfig::default()).unwrap();
        for xid in 1..=3 {
            journal
                .stage_commit(SourceTransaction {
                    source_id: SourceId("source".into()),
                    xid,
                    begin_lsn: PgLsn(1),
                    commit_lsn: PgLsn(u64::from(xid) * 10),
                    end_lsn: PgLsn(u64::from(xid) * 10 + 1),
                    commit_timestamp_micros: 0,
                    affected_tables: vec![],
                    schema_versions: vec![],
                    mutation_chunks: JournalChunks::default(),
                    table_mutation_counts: Some(Vec::new()),
                })
                .unwrap();
        }
        // Keep real terminal/index writes, but fail the actual OS sync syscall.
        // A character device cannot establish durability for a journal file.
        journal.writer = File::open("/dev/null").unwrap();
        assert!(matches!(journal.flush_commits(), Err(Error::Io(_))));
        assert_eq!(journal.durable_lsn(), PgLsn(0));
        assert_eq!(journal.staged_lsn(), PgLsn(31));
        assert!(journal.transactions().is_empty());
        assert_eq!(
            journal
                .reader()
                .transactions_after(PgLsn(0))
                .unwrap()
                .count(),
            0
        );
        assert!(matches!(journal.flush_commits(), Err(Error::Poisoned)));
        assert!(matches!(
            journal.append_chunk(4, b"later"),
            Err(Error::Poisoned)
        ));
        assert!(matches!(journal.reclaim(PgLsn(0)), Err(Error::Poisoned)));
        drop(journal);
        let (journal, recovered) = Journal::open(root.path(), JournalConfig::default()).unwrap();
        assert_eq!(recovered.transactions.len(), 3);
        assert_eq!(journal.durable_lsn(), PgLsn(31));
    }
}
