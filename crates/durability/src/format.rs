//! Versioned object layout. Objects are immutable once written. Owner claims
//! and log batches are created conditionally, so a replaced owner can never
//! overwrite a record written by its successor.
//!
//! ```text
//! <prefix>/format.json                         identity of the source using this prefix
//! <prefix>/owners/<epoch>.json                 one claim per process start
//! <prefix>/log/<seq>.batch                     ordered records, contiguous from the last checkpoint
//! <prefix>/checkpoints/<replay>-<ordinal>-<epoch>-<count>.json state as of a log position
//! <prefix>/control/<ordinal>-<epoch>-<crc>.snapshot control store image named by a checkpoint
//! <prefix>/journals/<name>/<segment>-<start>-<end>-<crc>.part journal bytes named by a checkpoint
//! ```
//!
//! Row-index checkpoints live below `index/`; see the `index` module.
use anyhow::{Context, Result, ensure};
use bincode::Options;
use flow_state_store::RawEntry;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const FORMAT: &str = "embrasure-flow-durability";
pub(crate) const VERSION: u32 = 1;
pub(crate) const IDENTITY_KEY: &str = "format.json";
pub(crate) const OWNERS: &str = "owners/";
pub(crate) const LOG: &str = "log/";
pub(crate) const CHECKPOINTS: &str = "checkpoints/";
pub(crate) const CONTROL: &str = "control/";
pub(crate) const JOURNALS: &str = "journals/";
const BATCH_MAGIC: &[u8; 4] = b"FRDB";
const SNAPSHOT_MAGIC: &[u8; 4] = b"FRDC";
/// Largest object this format reads back into memory.
pub(crate) const MAX_OBJECT_BYTES: u64 = 256 << 20;

pub(crate) fn owner_key(epoch: u64) -> String {
    format!("{OWNERS}{epoch:020}.json")
}
pub(crate) fn log_key(seq: u64) -> String {
    format!("{LOG}{seq:020}.batch")
}
/// Keys sort by log position, then owner epoch and the owner's checkpoint
/// count, so the newest checkpoint sorts last even when two cover the same
/// log position with different journal or file images.
pub(crate) fn checkpoint_key(replay_from: u64, ordinal: u64, epoch: u64, count: u64) -> String {
    format!("{CHECKPOINTS}{replay_from:020}-{ordinal:020}-{epoch:020}-{count:020}.json")
}
/// Names a snapshot by its contents and writer, so no owner can replace an
/// image another checkpoint names.
pub(crate) fn control_key(ordinal: u64, epoch: u64, crc32: u32) -> String {
    format!("{CONTROL}{ordinal:020}-{epoch:020}-{crc32:08x}.snapshot")
}
pub(crate) fn part_key(journal: &str, segment: u64, start: u64, end: u64) -> String {
    format!("{JOURNALS}{journal}/{segment:020}-{start:020}-{end:020}.part")
}

/// Parse the number that starts the final path component of a listed key.
pub(crate) fn key_number(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let digits = name.split(['.', '-']).next()?;
    (digits.len() == 20).then(|| digits.parse().ok()).flatten()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub format: String,
    pub version: u32,
    pub source_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Owner {
    pub epoch: u64,
    pub host: String,
    pub pid: u32,
    pub started_unix_ms: u64,
    /// The local lock this owner held for its lifetime, if it can identify it.
    #[serde(default)]
    pub lock: Option<LockIdentity>,
}

/// A running kernel and the exclusive lock file an owner held. A successor
/// that acquires the same lock under the same kernel proves the owner exited.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LockIdentity {
    pub boot_id: String,
    pub device: u64,
    pub inode: u64,
}

/// One replicated change. Ordinals are implicit: record `i` of a batch has
/// ordinal `first_ordinal + i`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Record {
    /// A RocksDB write batch committed to the control store.
    Control(Vec<u8>),
    /// Bytes written at `offset` of a journal segment; the segment ends there.
    Extent {
        journal: String,
        segment: u64,
        offset: u64,
        data: Vec<u8>,
    },
    /// The journal shortened a segment while recovering a torn tail.
    Truncate {
        journal: String,
        segment: u64,
        length: u64,
    },
    /// The journal persisted `reclaimed` and deletes segments below `first_retained`.
    Reclaim {
        journal: String,
        reclaimed: u64,
        first_retained: u64,
    },
    /// A small authority file below the state directory.
    File { name: String, data: Vec<u8> },
    /// First record of an owner. Later batches of older epochs are stale.
    EpochStart { epoch: u64 },
    /// The owner stopped cleanly after this record and wrote nothing else.
    Release { epoch: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Batch {
    pub epoch: u64,
    pub seq: u64,
    pub first_ordinal: u64,
    pub records: Vec<Record>,
}

impl Batch {
    pub(crate) fn last_ordinal(&self) -> u64 {
        self.first_ordinal + self.records.len() as u64 - 1
    }
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
        .with_limit(MAX_OBJECT_BYTES)
}

/// Encode the body directly after a reserved header, so large batches are
/// not copied a second time to prepend it.
fn seal(
    magic: &[u8; 4],
    capacity: usize,
    body: impl FnOnce(&mut Vec<u8>) -> Result<()>,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(capacity + 12);
    out.extend_from_slice(magic);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    body(&mut out)?;
    let checksum = crc32fast::hash(&out[12..]);
    out[8..12].copy_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

fn open<'a>(magic: &[u8; 4], bytes: &'a [u8], what: &str) -> Result<&'a [u8]> {
    ensure!(
        bytes.len() >= 12 && &bytes[..4] == magic,
        "{what} is not a Flow durability object"
    );
    let version = u32::from_le_bytes(bytes[4..8].try_into()?);
    ensure!(
        version == VERSION,
        "{what} has unsupported format version {version}; use a compatible binary"
    );
    let checksum = u32::from_le_bytes(bytes[8..12].try_into()?);
    ensure!(
        crc32fast::hash(&bytes[12..]) == checksum,
        "{what} checksum mismatch"
    );
    Ok(&bytes[12..])
}

pub(crate) fn encode_batch(batch: &Batch) -> Result<Vec<u8>> {
    ensure!(!batch.records.is_empty(), "empty durability batch");
    let size = usize::try_from(codec().serialized_size(batch)?)?;
    seal(BATCH_MAGIC, size, |out| {
        Ok(codec().serialize_into(out, batch)?)
    })
}

pub(crate) fn decode_batch(bytes: &[u8]) -> Result<Batch> {
    let body = open(BATCH_MAGIC, bytes, "log batch")?;
    let batch: Batch = codec()
        .reject_trailing_bytes()
        .deserialize(body)
        .context("decode log batch")?;
    ensure!(!batch.records.is_empty(), "empty log batch");
    Ok(batch)
}

/// Encode a sorted control-store image. Compression keeps periodic full
/// snapshots of the small authority store cheap.
pub(crate) fn encode_snapshot(entries: &[RawEntry]) -> Result<Vec<u8>> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (key, value) in entries {
        raw.extend_from_slice(&(key.len() as u32).to_le_bytes());
        raw.extend_from_slice(key);
        raw.extend_from_slice(&(value.len() as u32).to_le_bytes());
        raw.extend_from_slice(value);
    }
    let compressed = zstd::bulk::compress(&raw, 3)?;
    seal(SNAPSHOT_MAGIC, compressed.len(), |out| {
        out.extend_from_slice(&compressed);
        Ok(())
    })
}

pub(crate) fn decode_snapshot(bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let body = open(SNAPSHOT_MAGIC, bytes, "control snapshot")?;
    let raw = zstd::stream::decode_all(body).context("decompress control snapshot")?;
    let mut cursor = raw.as_slice();
    let mut take = |len: usize| -> Result<&[u8]> {
        ensure!(cursor.len() >= len, "truncated control snapshot");
        let (head, tail) = cursor.split_at(len);
        cursor = tail;
        Ok(head)
    };
    let count = u64::from_le_bytes(take(8)?.try_into()?);
    let mut entries = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let key_len = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let key = take(key_len)?.to_vec();
        let value_len = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        let value = take(value_len)?.to_vec();
        entries.push((key, value));
    }
    ensure!(take(0)?.is_empty(), "trailing bytes in control snapshot");
    Ok(entries)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Part {
    pub object: String,
    pub start: u64,
    pub end: u64,
    pub crc32: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SegmentImage {
    pub length: u64,
    /// Contiguous parts covering `[0, length)`.
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalImage {
    pub reclaimed: u64,
    pub segments: BTreeMap<u64, SegmentImage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ControlImage {
    pub object: String,
    pub entries: u64,
    pub bytes: u64,
}

/// State as of log ordinal `ordinal`. Recovery loads it and replays every
/// record with a greater ordinal from batch `replay_from` onwards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub version: u32,
    pub epoch: u64,
    pub replay_from: u64,
    /// Every record with an ordinal at or below this is reflected.
    pub ordinal: u64,
    /// Control batches at or below this ordinal are in `control`.
    pub control_ordinal: u64,
    pub control: Option<ControlImage>,
    pub journals: BTreeMap<String, JournalImage>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub created_unix_ms: u64,
}

impl Checkpoint {
    pub(crate) fn objects(&self) -> impl Iterator<Item = &str> {
        self.control
            .iter()
            .map(|control| control.object.as_str())
            .chain(self.journals.values().flat_map(|journal| {
                journal
                    .segments
                    .values()
                    .flat_map(|segment| segment.parts.iter().map(|part| part.object.as_str()))
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_and_snapshots_round_trip_and_reject_damage() {
        let batch = Batch {
            epoch: 3,
            seq: 7,
            first_ordinal: 40,
            records: vec![
                Record::EpochStart { epoch: 3 },
                Record::Control(b"batch".to_vec()),
                Record::Extent {
                    journal: "journal".into(),
                    segment: 1,
                    offset: 10,
                    data: b"bytes".to_vec(),
                },
            ],
        };
        let mut encoded = encode_batch(&batch).unwrap();
        assert_eq!(decode_batch(&encoded).unwrap(), batch);
        assert_eq!(batch.last_ordinal(), 42);
        *encoded.last_mut().unwrap() ^= 1;
        assert!(decode_batch(&encoded).is_err());
        encoded[0] = b'X';
        assert!(decode_batch(&encoded).is_err());
        assert!(decode_batch(b"FRDB").is_err());

        let entries: Vec<RawEntry> = vec![
            (b"a".to_vec().into(), b"1".to_vec().into()),
            (b"b".to_vec().into(), Vec::new().into()),
        ];
        let snapshot = encode_snapshot(&entries).unwrap();
        assert_eq!(
            decode_snapshot(&snapshot).unwrap(),
            vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), Vec::new())]
        );
        assert!(decode_batch(&snapshot).is_err());
    }

    #[test]
    fn listed_keys_parse_their_sequence() {
        assert_eq!(key_number(&log_key(12)), Some(12));
        assert_eq!(key_number(&checkpoint_key(5, 9, 2, 1)), Some(5));
        assert!(checkpoint_key(5, 9, 2, 1) < checkpoint_key(5, 9, 2, 10));
        assert!(checkpoint_key(5, 9, 3, 1) > checkpoint_key(5, 9, 2, 10));
        assert_eq!(key_number(&owner_key(2)), Some(2));
        assert_eq!(key_number(&part_key("journal", 4, 0, 10)), Some(4));
        assert_eq!(key_number("log/bad.batch"), None);
    }
}
