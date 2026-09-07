//! Versioned journal frames and bounded decoding of transaction terminals.
//! Header bytes, checksums and legacy terminal compatibility live together here.

use crate::{Error, Result, legacy};
use bincode::Options;
use flow_model::SourceTransaction;
use std::io::Read;

const MAGIC: [u8; 4] = *b"FLJ1";
pub(super) const HEADER: usize = 28;
pub(super) const CHUNK: u8 = 1;
pub(super) const LEGACY_COMMIT: u8 = 2;
pub(super) const BOUNDED_COMMIT: u8 = 4;
pub(super) const COMMIT: u8 = 5;
pub(super) const ABORT: u8 = 3;

pub(super) fn encode_header(kind: u8, xid: u32, sequence: u64, payload: &[u8]) -> [u8; HEADER] {
    let mut header = [0u8; HEADER];
    header[..4].copy_from_slice(&MAGIC);
    header[4..6].copy_from_slice(&1u16.to_le_bytes());
    header[6] = kind;
    header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[12..20].copy_from_slice(&sequence.to_le_bytes());
    header[20..24].copy_from_slice(&xid.to_le_bytes());
    let mut crc = crc32fast::Hasher::new();
    crc.update(&header[..24]);
    crc.update(payload);
    header[24..].copy_from_slice(&crc.finalize().to_le_bytes());
    header
}

pub(super) struct Frame {
    pub(super) kind: u8,
    pub(super) xid: u32,
    pub(super) sequence: u64,
    pub(super) payload: Vec<u8>,
}

pub(super) fn read_frame(file: &mut impl Read, max_bytes: u32) -> Result<Frame> {
    let mut header = [0; HEADER];
    file.read_exact(&mut header)?;
    if header[..4] != MAGIC || header[7] != 0 {
        return Err(Error::Corrupt);
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != 1 {
        return Err(Error::Version(version));
    }
    let length = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if length > max_bytes {
        return Err(Error::FrameLimit {
            length,
            limit: max_bytes,
        });
    }
    let mut payload = vec![0; length as usize];
    file.read_exact(&mut payload)?;
    let mut crc = crc32fast::Hasher::new();
    crc.update(&header[..24]);
    crc.update(&payload);
    if crc.finalize() != u32::from_le_bytes(header[24..].try_into().unwrap()) {
        return Err(Error::Corrupt);
    }
    if !matches!(
        header[6],
        CHUNK | COMMIT | BOUNDED_COMMIT | LEGACY_COMMIT | ABORT
    ) {
        return Err(Error::RecordKind(header[6]));
    }
    Ok(Frame {
        kind: header[6],
        xid: u32::from_le_bytes(header[20..24].try_into().unwrap()),
        sequence: u64::from_le_bytes(header[12..20].try_into().unwrap()),
        payload,
    })
}

pub(super) fn decode_transaction(
    kind: u8,
    payload: &[u8],
    max_bytes: u32,
) -> Result<SourceTransaction> {
    let codec = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(u64::from(max_bytes))
        .reject_trailing_bytes();
    let txn = match kind {
        COMMIT => codec.deserialize(payload)?,
        BOUNDED_COMMIT => codec
            .deserialize::<legacy::BoundedSourceTransaction>(payload)?
            .into_current()?,
        LEGACY_COMMIT => codec
            .deserialize::<legacy::SourceTransaction>(payload)?
            .into_current()?,
        _ => return Err(Error::RecordKind(kind)),
    };
    if kind == COMMIT && txn.table_mutation_counts.is_none() {
        return Err(Error::Transaction("counted terminal lacks mutation counts"));
    }
    txn.validate_mutation_counts()
        .map_err(|_| Error::Transaction("invalid per-table mutation counts"))?;
    Ok(txn)
}
