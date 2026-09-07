//! Rebuildable terminal indexes, one file per journal segment. They contain no
//! rows and require no separate sync: only the journal establishes durability.
//! The index avoids rescanning large transaction payloads during acknowledgement
//! and keeps retained-transaction memory proportional to segment count.

use crate::frame::{HEADER, decode_transaction, read_frame};
use crate::{Error, JournalReader, Result, segment_path};
use flow_model::{JournalChunkRef, PgLsn, SourceTransaction};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(crate) const ENTRY_BYTES: u64 = 32;
const SUFFIX: &str = ".commits-v1";

#[derive(Clone, Default)]
pub(crate) struct Segment {
    pub(crate) bytes: u64,
    pub(crate) last_lsn: PgLsn,
}
#[derive(Default)]
pub(crate) struct Catalog {
    pub(crate) segments: BTreeMap<u64, Segment>,
    pub(crate) durable_lsn: PgLsn,
    pub(crate) reclaimed_lsn: PgLsn,
    pub(crate) count: u64,
}
pub(crate) type SharedCatalog = Arc<Mutex<Catalog>>;

fn path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("{segment:020}{SUFFIX}"))
}
pub(crate) fn reset(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_suffix(SUFFIX))
            .is_some_and(|n| n.parse::<u64>().is_ok())
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
pub(crate) fn remove(root: &Path, segment: u64) -> Result<()> {
    match fs::remove_file(path(root, segment)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) struct Writer {
    root: PathBuf,
    segments: BTreeMap<u64, Segment>,
    file: Option<(u64, BufWriter<File>)>,
}
impl Writer {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            segments: BTreeMap::new(),
            file: None,
        }
    }
    pub(crate) fn segments(&self) -> &BTreeMap<u64, Segment> {
        &self.segments
    }
    pub(crate) fn remove_segment(&mut self, id: u64) {
        self.segments.remove(&id);
    }
    pub(crate) fn append(
        &mut self,
        reference: &JournalChunkRef,
        txn: &SourceTransaction,
    ) -> Result<()> {
        if self.file.as_ref().map(|(id, _)| *id) != Some(reference.segment) {
            self.flush()?;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path(&self.root, reference.segment))?;
            self.file = Some((reference.segment, BufWriter::with_capacity(64 << 10, file)));
        }
        let mut bytes = [0; ENTRY_BYTES as usize];
        bytes[..8].copy_from_slice(&txn.end_lsn.0.to_le_bytes());
        bytes[8..16].copy_from_slice(&reference.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&reference.length.to_le_bytes());
        let first_segment = txn
            .mutation_chunks
            .first
            .as_ref()
            .map_or(reference.segment, |r| r.segment);
        bytes[20..28].copy_from_slice(&first_segment.to_le_bytes());
        let checksum = crc32fast::hash(&bytes[..28]);
        bytes[28..].copy_from_slice(&checksum.to_le_bytes());
        self.file
            .as_mut()
            .ok_or(Error::Corrupt)?
            .1
            .write_all(&bytes)?;
        let segment = self.segments.entry(reference.segment).or_default();
        segment.bytes = segment
            .bytes
            .checked_add(ENTRY_BYTES)
            .ok_or(Error::Corrupt)?;
        segment.last_lsn = txn.end_lsn;
        Ok(())
    }
    pub(crate) fn flush(&mut self) -> Result<()> {
        if let Some((_, file)) = &mut self.file {
            file.flush()?;
        }
        Ok(())
    }
}

/// A snapshot of the retained terminal range, not a transaction collection.
/// Iteration holds one decoded descriptor and two bounded file buffers.
#[derive(Clone)]
pub struct TransactionLog {
    reader: JournalReader,
    segments: Vec<(u64, Segment)>,
    after: PgLsn,
    through: PgLsn,
    count: u64,
}
impl TransactionLog {
    pub(crate) fn retained(reader: JournalReader) -> Self {
        let catalog = reader
            .catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = Self {
            reader: reader.clone(),
            segments: catalog
                .segments
                .iter()
                .map(|(id, segment)| (*id, segment.clone()))
                .collect(),
            after: catalog.reclaimed_lsn,
            through: catalog.durable_lsn,
            count: catalog.count,
        };
        drop(catalog);
        result
    }
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn iter(&self) -> Result<TransactionIter> {
        self.after(self.after)
    }
    pub(crate) fn after(&self, lsn: PgLsn) -> Result<TransactionIter> {
        Ok(TransactionIter {
            reader: self.reader.clone(),
            segments: self
                .segments
                .iter()
                .filter(|(_, s)| s.last_lsn > lsn)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter(),
            after: lsn.max(self.after),
            through: self.through,
            current: None,
            failed: false,
        })
    }
}

struct CurrentSegment {
    id: u64,
    index: BufReader<File>,
    journal: BufReader<File>,
    journal_offset: u64,
    remaining: u64,
}
pub struct TransactionIter {
    reader: JournalReader,
    segments: std::vec::IntoIter<(u64, Segment)>,
    after: PgLsn,
    through: PgLsn,
    current: Option<CurrentSegment>,
    failed: bool,
}
impl TransactionIter {
    fn open_next_segment(&mut self) -> Result<bool> {
        let Some((id, segment)) = self.segments.next() else {
            return Ok(false);
        };
        let mut index =
            BufReader::with_capacity(64 << 10, File::open(path(&self.reader.root, id))?);
        // Find the first terminal beyond the cursor without reading row frames.
        let mut low = 0;
        let mut high = segment.bytes / ENTRY_BYTES;
        while low < high {
            let mid = low + (high - low) / 2;
            index.seek(SeekFrom::Start(mid * ENTRY_BYTES))?;
            let record = read_record(&mut index)?;
            if record.end_lsn <= self.after {
                low = mid + 1
            } else {
                high = mid
            }
        }
        index.seek(SeekFrom::Start(low * ENTRY_BYTES))?;
        self.current = Some(CurrentSegment {
            id,
            index,
            journal: BufReader::with_capacity(
                64 << 10,
                File::open(segment_path(&self.reader.root, id))?,
            ),
            journal_offset: 0,
            remaining: segment.bytes / ENTRY_BYTES - low,
        });
        Ok(true)
    }
    fn read(&mut self) -> Result<Option<SourceTransaction>> {
        loop {
            if self
                .current
                .as_ref()
                .is_none_or(|segment| segment.remaining == 0)
                && !self.open_next_segment()?
            {
                return Ok(None);
            }
            let current = self.current.as_mut().ok_or(Error::Corrupt)?;
            if current.remaining == 0 {
                continue;
            }
            let record = read_record(&mut current.index)?;
            current.remaining -= 1;
            if record.end_lsn > self.through {
                return Ok(None);
            }
            let delta = i128::from(record.offset) - i128::from(current.journal_offset);
            if let Ok(delta) = i64::try_from(delta) {
                current.journal.seek_relative(delta)?;
            } else {
                current.journal.seek(SeekFrom::Start(record.offset))?;
            }
            let frame = read_frame(&mut current.journal, self.reader.max_frame_bytes)?;
            if frame.payload.len() != record.length as usize {
                return Err(Error::Corrupt);
            }
            current.journal_offset = record
                .offset
                .checked_add(HEADER as u64 + frame.payload.len() as u64)
                .ok_or(Error::Corrupt)?;
            let txn = decode_transaction(frame.kind, &frame.payload, self.reader.max_frame_bytes)?;
            if txn.xid != frame.xid
                || txn.end_lsn != record.end_lsn
                || txn
                    .mutation_chunks
                    .first
                    .as_ref()
                    .map_or(current.id, |r| r.segment)
                    != record.first_segment
            {
                return Err(Error::Corrupt);
            }
            self.after = txn.end_lsn;
            return Ok(Some(txn));
        }
    }
}
impl Iterator for TransactionIter {
    type Item = Result<SourceTransaction>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.read() {
            Ok(Some(txn)) => Some(Ok(txn)),
            Ok(None) => {
                self.failed = true;
                None
            }
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}
impl std::iter::FusedIterator for TransactionIter {}

struct Record {
    end_lsn: PgLsn,
    offset: u64,
    length: u32,
    first_segment: u64,
}
fn read_record(reader: &mut impl Read) -> Result<Record> {
    let mut bytes = [0; ENTRY_BYTES as usize];
    reader.read_exact(&mut bytes)?;
    if crc32fast::hash(&bytes[..28]) != u32::from_le_bytes(bytes[28..].try_into().unwrap()) {
        return Err(Error::Corrupt);
    }
    Ok(Record {
        end_lsn: PgLsn(u64::from_le_bytes(bytes[..8].try_into().unwrap())),
        offset: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        length: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        first_segment: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Journal, JournalConfig};
    use flow_model::{JournalChunks, SourceId};

    #[test]
    fn adjacent_terminals_reuse_read_ahead() {
        let directory = tempfile::tempdir().unwrap();
        let (mut journal, _) = Journal::open(directory.path(), JournalConfig::default()).unwrap();
        for xid in 1..=1000 {
            journal
                .stage_commit(SourceTransaction {
                    source_id: SourceId("buffered-terminals".into()),
                    xid,
                    begin_lsn: PgLsn(u64::from(xid)),
                    commit_lsn: PgLsn(u64::from(xid)),
                    end_lsn: PgLsn(u64::from(xid) + 1),
                    commit_timestamp_micros: 0,
                    affected_tables: vec![],
                    schema_versions: vec![],
                    mutation_chunks: JournalChunks::default(),
                    table_mutation_counts: Some(vec![]),
                })
                .unwrap();
        }
        journal.flush_commits().unwrap();
        let mut transactions = journal.reader().transactions_after(PgLsn(0)).unwrap();
        assert_eq!(transactions.next().unwrap().unwrap().xid, 1);
        let position = transactions
            .current
            .as_mut()
            .unwrap()
            .journal
            .get_mut()
            .stream_position()
            .unwrap();
        assert_eq!(position, 64 << 10);
        for xid in 2..=100 {
            assert_eq!(transactions.next().unwrap().unwrap().xid, xid);
            assert_eq!(
                transactions
                    .current
                    .as_mut()
                    .unwrap()
                    .journal
                    .get_mut()
                    .stream_position()
                    .unwrap(),
                position
            );
        }
        assert_eq!(transactions.collect::<Result<Vec<_>>>().unwrap().len(), 900);
    }
}
