//! Constant-size range descriptors over the checksummed journal frames.
//! Capture appends each surviving spool transaction consecutively at commit,
//! making replay sequential without a second index or one file per transaction.

use crate::frame::{CHUNK, HEADER, read_frame};
use crate::{Error, JournalReader, Result, segment_path};
use flow_model::{JournalChunkRef, JournalChunks};
use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom},
};

pub(crate) fn include(chunks: &mut JournalChunks, reference: &JournalChunkRef) -> Result<()> {
    chunks.count = chunks.count.checked_add(1).ok_or(Error::Corrupt)?;
    chunks.payload_bytes = chunks
        .payload_bytes
        .checked_add(u64::from(reference.length))
        .ok_or(Error::Corrupt)?;
    chunks.first.get_or_insert_with(|| reference.clone());
    chunks.last = Some(reference.clone());
    let mut bytes = [0; 20];
    bytes[..8].copy_from_slice(&reference.segment.to_le_bytes());
    bytes[8..16].copy_from_slice(&reference.offset.to_le_bytes());
    bytes[16..].copy_from_slice(&reference.length.to_le_bytes());
    let mut checksum = crc32fast::Hasher::new_with_initial(chunks.checksum);
    checksum.update(&bytes);
    chunks.checksum = checksum.finalize();
    Ok(())
}

/// A private, reusable file cursor for replaying retained transactions. It owns
/// at most one file and 64 KiB read buffer; it never shares an OS cursor with
/// another reader. Segment contents must remain retained while replay is active.
pub struct ReplayCursor {
    reader: JournalReader,
    segment: Option<Segment>,
}

struct Segment {
    reader: BufReader<File>,
    id: u64,
    offset: u64,
    bytes: u64,
}

impl ReplayCursor {
    pub(crate) fn new(reader: JournalReader) -> Self {
        Self {
            reader,
            segment: None,
        }
    }

    /// Replay one descriptor, preserving buffered bytes for the next call.
    /// Exhaust the iterator to validate its complete range/count/checksum.
    /// Dropping an iterator early is safe, but does not validate its unread tail.
    pub fn chunks<'a>(&'a mut self, chunks: &JournalChunks) -> Result<ReplayChunks<'a>> {
        let state = ChunkState::new(chunks)?;
        Ok(ReplayChunks {
            cursor: self,
            state,
        })
    }

    fn position(&mut self, id: u64, offset: u64) -> Result<()> {
        if let Some(segment) = &mut self.segment
            && segment.id == id
        {
            // Unlike seek(Current), seek_relative can stay inside the existing
            // buffer. Track the logical offset without a per-frame seek syscall.
            let delta = i128::from(offset) - i128::from(segment.offset);
            if let Ok(delta) = i64::try_from(delta) {
                segment.reader.seek_relative(delta)?;
            } else {
                segment.reader.seek(SeekFrom::Start(offset))?;
            }
            segment.offset = offset;
            return Ok(());
        }
        let file = File::open(segment_path(&self.reader.root, id))?;
        let bytes = file.metadata()?.len();
        if let Some(segment) = &mut self.segment {
            // An absolute seek discards the old segment's buffered bytes while
            // retaining the allocation for the new file.
            *segment.reader.get_mut() = file;
            segment.reader.seek(SeekFrom::Start(offset))?;
            segment.id = id;
            segment.offset = offset;
            segment.bytes = bytes;
            return Ok(());
        }
        let mut reader = BufReader::with_capacity(64 << 10, file);
        reader.seek(SeekFrom::Start(offset))?;
        self.segment = Some(Segment {
            reader,
            id,
            offset,
            bytes,
        });
        Ok(())
    }
}

/// Independently owned transaction replay. Use [`ReplayCursor`] when replaying
/// multiple descriptors in sequence to reuse the file and buffer between them.
pub struct ChunkIter {
    cursor: ReplayCursor,
    state: ChunkState,
}
impl ChunkIter {
    pub(crate) fn open(reader: JournalReader, chunks: &JournalChunks) -> Result<Self> {
        Ok(Self {
            cursor: ReplayCursor::new(reader),
            state: ChunkState::new(chunks)?,
        })
    }
}

/// A transaction iterator borrowing its epoch-local replay cursor.
pub struct ReplayChunks<'a> {
    cursor: &'a mut ReplayCursor,
    state: ChunkState,
}

struct ChunkState {
    expected: JournalChunks,
    observed: JournalChunks,
    xid: Option<u32>,
    started: bool,
    failed: bool,
}
impl ChunkState {
    fn new(chunks: &JournalChunks) -> Result<Self> {
        match (&chunks.first, &chunks.last) {
            (Some(first), Some(last))
                if chunks.count > 0
                    && (first.segment, first.offset) <= (last.segment, last.offset) => {}
            (None, None) if *chunks == JournalChunks::default() => {}
            _ => return Err(Error::Corrupt),
        }
        Ok(Self {
            expected: chunks.clone(),
            observed: JournalChunks::default(),
            xid: None,
            started: false,
            failed: false,
        })
    }

    fn read(&mut self, cursor: &mut ReplayCursor) -> Result<Vec<u8>> {
        let last = self.expected.last.as_ref().ok_or(Error::Corrupt)?;
        if !self.started {
            let first = self.expected.first.as_ref().ok_or(Error::Corrupt)?;
            cursor.position(first.segment, first.offset)?;
            self.started = true;
        }
        loop {
            let segment = cursor.segment.as_mut().ok_or(Error::Corrupt)?;
            if segment.offset >= segment.bytes && segment.id < last.segment {
                // A previously open tail can grow before the writer rolls it.
                // Refresh at this boundary; never skip newly appended frames
                // using the extent observed by an earlier transaction.
                segment.bytes = segment.reader.get_ref().metadata()?.len();
                if segment.offset > segment.bytes {
                    return Err(Error::Corrupt);
                }
                if segment.offset == segment.bytes {
                    let next = segment.id.checked_add(1).ok_or(Error::Corrupt)?;
                    cursor.position(next, 0)?;
                    continue;
                }
            }
            if (segment.id, segment.offset) > (last.segment, last.offset) {
                return Err(Error::Corrupt);
            }
            let frame = read_frame(&mut segment.reader, cursor.reader.max_frame_bytes)?;
            let reference = JournalChunkRef {
                segment: segment.id,
                offset: segment.offset,
                length: frame.payload.len() as u32,
            };
            segment.offset = segment
                .offset
                .checked_add(HEADER as u64 + u64::from(reference.length))
                .ok_or(Error::Corrupt)?;
            if self.xid.is_none() {
                if frame.kind != CHUNK {
                    return Err(Error::Corrupt);
                }
                self.xid = Some(frame.xid);
            }
            if Some(frame.xid) != self.xid {
                continue;
            }
            if frame.kind != CHUNK {
                // A terminal record ends this XID incarnation. Never join a
                // later transaction that happens to reuse the same XID.
                return Err(Error::Corrupt);
            }
            include(&mut self.observed, &reference)?;
            if self.observed.count == self.expected.count && self.observed != self.expected {
                return Err(Error::Corrupt);
            }
            return Ok(frame.payload);
        }
    }

    fn next(&mut self, cursor: &mut ReplayCursor) -> Option<Result<Vec<u8>>> {
        if self.failed || self.observed.count == self.expected.count {
            return None;
        }
        let result = self.read(cursor);
        self.failed = result.is_err();
        if self.failed {
            // A failed frame may have consumed only part of its header/payload.
            // Discard any buffered bytes and reopen at the next descriptor.
            cursor.segment = None;
        }
        Some(result)
    }
}
impl Iterator for ChunkIter {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.state.next(&mut self.cursor)
    }
}
impl Iterator for ReplayChunks<'_> {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.state.next(self.cursor)
    }
}
impl std::iter::FusedIterator for ChunkIter {}
impl std::iter::FusedIterator for ReplayChunks<'_> {}
