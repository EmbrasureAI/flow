// Added by Embrasure Flow; see LOCAL_CHANGES.md. Not part of upstream
// Apache Iceberg Rust.
//
// Copyright 2026 The Embrasure Flow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::io::{Cursor, Read};

use roaring::{RoaringBitmap, RoaringTreemap};

use super::{CompressionCodec, DELETION_VECTOR_V1, FileMetadata};
use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::{Error, ErrorKind, Result};

const MAGIC: [u8; 4] = [0xd1, 0xd3, 0x39, 0x64];
const MIN_BLOB_BYTES: u64 = 20;

/// Resource limits applied before reading or decoding deletion vectors.
#[derive(Debug, Clone, Copy)]
pub struct DeletionVectorLimits {
    /// Maximum complete blob size, including length, magic, and checksum.
    pub max_blob_bytes: u64,
    /// Maximum stored and decompressed Puffin footer payload size.
    pub max_footer_bytes: u64,
    /// Maximum number of deleted row positions.
    pub max_cardinality: u64,
}

impl Default for DeletionVectorLimits {
    fn default() -> Self {
        Self {
            max_blob_bytes: 64 * 1024 * 1024,
            max_footer_bytes: 1024 * 1024,
            max_cardinality: 100_000_000,
        }
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

fn check_limits(length: u64, cardinality: u64, limits: DeletionVectorLimits) -> Result<()> {
    if !(MIN_BLOB_BYTES..=limits.max_blob_bytes).contains(&length) {
        return Err(invalid(
            "Deletion vector blob length exceeds limits or is too short",
        ));
    }
    if cardinality > limits.max_cardinality {
        return Err(invalid("Deletion vector cardinality exceeds limit"));
    }
    Ok(())
}

/// Encode an Iceberg `deletion-vector-v1` blob, enforcing size and cardinality limits.
///
/// Positions must fit in a nonnegative signed 64-bit integer. The returned bytes
/// include the big-endian payload length and CRC-32 framing required by Puffin.
pub fn encode_deletion_vector(
    vector: &DeleteVector,
    limits: DeletionVectorLimits,
) -> Result<Vec<u8>> {
    let length = vector.serialized_size();
    check_limits(length as u64, vector.len(), limits)?;
    if vector
        .bitmap()
        .max()
        .is_some_and(|pos| pos > i64::MAX as u64)
    {
        return Err(invalid(
            "Deletion vector position exceeds signed 64-bit range",
        ));
    }
    let payload_length = u32::try_from(length - 8)
        .map_err(|_| invalid("Deletion vector payload exceeds 32-bit length"))?;
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&payload_length.to_be_bytes());
    bytes.extend_from_slice(&MAGIC);
    vector.bitmap().serialize_into(&mut bytes)?;
    let checksum = crc32fast::hash(&bytes[4..]);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn read_array<const N: usize>(cursor: &mut Cursor<&[u8]>) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    cursor
        .read_exact(&mut bytes)
        .map_err(|err| invalid("Truncated deletion vector bitmap").with_source(err))?;
    Ok(bytes)
}

// Check the portable bitmap descriptors before its decoder allocates containers.
// In particular a compact run container can describe billions of positions.
fn bitmap_cardinality(bytes: &[u8]) -> Result<u64> {
    let mut cursor = Cursor::new(bytes);
    let cookie = u32::from_le_bytes(read_array(&mut cursor)?);
    let (count, run_header) = if cookie == 12346 {
        (u32::from_le_bytes(read_array(&mut cursor)?) as usize, false)
    } else if cookie as u16 == 12347 {
        (((cookie >> 16) + 1) as usize, true)
    } else {
        return Err(invalid("Invalid deletion vector Roaring cookie"));
    };
    if count > 65536 {
        return Err(invalid("Invalid deletion vector container count"));
    }
    if run_header {
        cursor.set_position(cursor.position() + count.div_ceil(8) as u64);
    }
    let mut cardinality = 0;
    for _ in 0..count {
        let descriptor = read_array::<4>(&mut cursor)?;
        cardinality += u64::from(u16::from_le_bytes([descriptor[2], descriptor[3]])) + 1;
    }
    Ok(cardinality)
}

/// Decode a complete Iceberg `deletion-vector-v1` blob.
///
/// Validates framing, checksum, ordered bitmap keys, signed positions, portable
/// bitmap contents, and the expected cardinality. Limits are checked before
/// decoding each bitmap, without expanding the vector into individual positions.
pub fn decode_deletion_vector(
    bytes: &[u8],
    expected_cardinality: u64,
    limits: DeletionVectorLimits,
) -> Result<DeleteVector> {
    check_limits(bytes.len() as u64, expected_cardinality, limits)?;
    let payload_length = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    if payload_length != bytes.len() - 8 {
        return Err(invalid("Deletion vector payload length mismatch"));
    }
    if bytes[4..8] != MAGIC {
        return Err(invalid("Invalid deletion vector magic"));
    }
    let checksum_offset = bytes.len() - 4;
    let checksum = u32::from_be_bytes(bytes[checksum_offset..].try_into().unwrap());
    if crc32fast::hash(&bytes[4..checksum_offset]) != checksum {
        return Err(invalid("Deletion vector checksum mismatch"));
    }
    let bitmap_bytes = &bytes[8..checksum_offset];
    let mut cursor = Cursor::new(bitmap_bytes);
    let bitmap_count = u64::from_le_bytes(read_array(&mut cursor)?);
    // Even an empty 32-bit bitmap needs a key and an eight-byte header.
    if bitmap_count > (bitmap_bytes.len() as u64 - 8) / 12 {
        return Err(invalid("Invalid deletion vector bitmap count"));
    }
    let mut bitmaps = Vec::new();
    let mut previous_key = None;
    let mut cardinality = 0_u64;
    for _ in 0..bitmap_count {
        let key = u32::from_le_bytes(read_array(&mut cursor)?);
        if key > i32::MAX as u32 || previous_key.is_some_and(|previous| key <= previous) {
            return Err(invalid(
                "Deletion vector keys must be ordered and nonnegative signed integers",
            ));
        }
        previous_key = Some(key);
        let declared = bitmap_cardinality(&bitmap_bytes[cursor.position() as usize..])?;
        cardinality = cardinality
            .checked_add(declared)
            .ok_or_else(|| invalid("Deletion vector cardinality overflow"))?;
        if cardinality > expected_cardinality || cardinality > limits.max_cardinality {
            return Err(invalid(
                "Deletion vector cardinality exceeds expected value or limit",
            ));
        }
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
            .map_err(|err| invalid("Invalid deletion vector Roaring bitmap").with_source(err))?;
        if bitmap.len() != declared {
            return Err(invalid("Deletion vector bitmap cardinality mismatch"));
        }
        bitmaps.push((key, bitmap));
    }
    if cursor.position() != bitmap_bytes.len() as u64 {
        return Err(invalid("Trailing bytes in deletion vector bitmap"));
    }
    if cardinality != expected_cardinality {
        return Err(invalid("Deletion vector cardinality mismatch"));
    }
    Ok(DeleteVector::new(RoaringTreemap::from_bitmaps(bitmaps)))
}

/// Read one deletion vector from a Puffin file with bounded range reads.
///
/// The footer must identify exactly one blob at `offset` with `length`, matching
/// the referenced data file and cardinality. Blob compression is forbidden by
/// Iceberg. Both plain and LZ4-compressed Puffin footers are supported, with
/// the footer size limit applied to stored and decompressed bytes.
#[allow(clippy::too_many_arguments)]
pub async fn read_deletion_vector(
    file_io: &FileIO,
    path: &str,
    offset: u64,
    length: u64,
    referenced_data_file: &str,
    expected_cardinality: u64,
    limits: DeletionVectorLimits,
) -> Result<DeleteVector> {
    check_limits(length, expected_cardinality, limits)?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("Deletion vector byte range overflow"))?;
    if offset < 4 {
        return Err(invalid("Deletion vector overlaps Puffin header"));
    }
    let input = file_io.new_input(path)?;
    let size = input.metadata().await?.size;
    if size < 20 || end > size - 16 {
        return Err(invalid("Deletion vector byte range exceeds Puffin file"));
    }
    let reader = input.reader().await?;
    let header = reader.read(0..4).await?;
    let tail = reader.read(size - 12..size).await?;
    if header.as_ref() != FileMetadata::MAGIC
        || tail.len() != 12
        || tail[8..] != FileMetadata::MAGIC
    {
        return Err(invalid("Invalid Puffin header or footer magic"));
    }
    let compressed_footer = tail[4..8] == [1, 0, 0, 0];
    if !compressed_footer && tail[4..8] != [0; 4] {
        return Err(invalid("Invalid Puffin footer flags"));
    }
    let footer_length = u64::from(u32::from_le_bytes(tail[..4].try_into().unwrap()));
    if footer_length > limits.max_footer_bytes || footer_length > size - 20 {
        return Err(invalid("Puffin footer length exceeds limit or file bounds"));
    }
    let footer_start = size - 16 - footer_length;
    if end > footer_start {
        return Err(invalid("Deletion vector overlaps Puffin footer"));
    }
    let footer = reader.read(footer_start..size - 12).await?;
    if footer.len() as u64 != footer_length + 4 || footer[..4] != FileMetadata::MAGIC {
        return Err(invalid("Invalid Puffin footer length or magic"));
    }
    let payload = if compressed_footer {
        let mut decoded = Vec::new();
        lz4_flex::frame::FrameDecoder::new(&footer[4..])
            .take(limits.max_footer_bytes.saturating_add(1))
            .read_to_end(&mut decoded)
            .map_err(|err| invalid("Invalid compressed Puffin footer").with_source(err))?;
        if decoded.len() as u64 > limits.max_footer_bytes {
            return Err(invalid("Decompressed Puffin footer exceeds limit"));
        }
        Cow::Owned(decoded)
    } else {
        Cow::Borrowed(&footer[4..])
    };
    let metadata: FileMetadata = serde_json::from_slice(&payload)
        .map_err(|err| invalid("Invalid Puffin deletion vector metadata").with_source(err))?;
    let mut matches = metadata.blobs.iter().filter(|blob| blob.offset == offset);
    let blob = matches
        .next()
        .ok_or_else(|| invalid("Deletion vector missing from Puffin footer"))?;
    if matches.next().is_some()
        || blob.length != length
        || blob.r#type != DELETION_VECTOR_V1
        || blob.compression_codec != CompressionCodec::None
        || blob.snapshot_id != -1
        || blob.sequence_number != -1
        || blob
            .properties
            .get("referenced-data-file")
            .map(String::as_str)
            != Some(referenced_data_file)
        || blob
            .properties
            .get("cardinality")
            .and_then(|value| value.parse::<u64>().ok())
            != Some(expected_cardinality)
    {
        return Err(invalid(
            "Deletion vector metadata does not match requested blob",
        ));
    }
    let bytes = reader.read(offset..end).await?;
    if bytes.len() as u64 != length {
        return Err(invalid("Truncated deletion vector blob range"));
    }
    decode_deletion_vector(&bytes, expected_cardinality, limits)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::puffin::{Blob, PuffinWriter};

    fn vector(positions: &[u64]) -> DeleteVector {
        DeleteVector::new(positions.iter().copied().collect())
    }

    fn frame(bitmap: &[u8]) -> Vec<u8> {
        let mut bytes = ((bitmap.len() + 4) as u32).to_be_bytes().to_vec();
        bytes.extend(MAGIC);
        bytes.extend(bitmap);
        bytes.extend(crc32fast::hash(&bytes[4..]).to_be_bytes());
        bytes
    }

    #[test]
    fn portable_golden_bytes_and_signed_position_boundaries() {
        // Independent portable fixture: one partition, array container {0, 1}.
        let portable = [
            1, 0, 0, 0, 0, 0, 0, 0, // partition count
            0, 0, 0, 0, // partition key
            58, 48, 0, 0, 1, 0, 0, 0, // no-run cookie, container count
            0, 0, 1, 0, // container key, cardinality minus one
            16, 0, 0, 0, // container offset
            0, 0, 1, 0, // array values
        ];
        let encoded =
            encode_deletion_vector(&vector(&[0, 1]), DeletionVectorLimits::default()).unwrap();
        assert_eq!(encoded, frame(&portable));
        let positions = [0, u32::MAX as u64, 1 << 32, i64::MAX as u64];
        let encoded =
            encode_deletion_vector(&vector(&positions), DeletionVectorLimits::default()).unwrap();
        assert_eq!(
            decode_deletion_vector(&encoded, 4, DeletionVectorLimits::default())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            positions
        );
        assert!(
            encode_deletion_vector(&vector(&[1 << 63]), DeletionVectorLimits::default()).is_err()
        );
        let empty = encode_deletion_vector(&vector(&[]), DeletionVectorLimits::default()).unwrap();
        assert_eq!(empty.len(), 20);
        assert_eq!(
            decode_deletion_vector(&empty, 0, DeletionVectorLimits::default())
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn rejects_truncation_corruption_and_cardinality_limits() {
        let limits = DeletionVectorLimits::default();
        let encoded = encode_deletion_vector(&vector(&[0, 3, 1 << 32]), limits).unwrap();
        for length in 0..encoded.len() {
            assert!(
                decode_deletion_vector(&encoded[..length], 3, limits).is_err(),
                "length {length}"
            );
        }
        for index in [0, 4, 12, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[index] ^= 1;
            assert!(decode_deletion_vector(&corrupt, 3, limits).is_err());
        }
        assert!(decode_deletion_vector(&encoded, 2, limits).is_err());
        assert!(decode_deletion_vector(&encoded, 4, limits).is_err());
        assert!(
            decode_deletion_vector(
                &encoded,
                3,
                DeletionVectorLimits {
                    max_blob_bytes: encoded.len() as u64 - 1,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(
            decode_deletion_vector(
                &encoded,
                3,
                DeletionVectorLimits {
                    max_cardinality: 2,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(
            encode_deletion_vector(
                &vector(&[1, 2, 3]),
                DeletionVectorLimits {
                    max_cardinality: 2,
                    ..limits
                }
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_malformed_portable_payload_even_with_valid_checksum() {
        let limits = DeletionVectorLimits::default();
        let encoded = encode_deletion_vector(&vector(&[1, 1 << 32]), limits).unwrap();
        let original = &encoded[8..encoded.len() - 4];
        let mut duplicate_key = original.to_vec();
        let second_key = 8 + 4 + 18;
        duplicate_key[second_key..second_key + 4].copy_from_slice(&0_u32.to_le_bytes());
        assert!(decode_deletion_vector(&frame(&duplicate_key), 2, limits).is_err());
        let mut negative_position = original.to_vec();
        negative_position[8..12].copy_from_slice(&0x80000000_u32.to_le_bytes());
        assert!(decode_deletion_vector(&frame(&negative_position), 2, limits).is_err());
        let mut bad_cookie = original.to_vec();
        bad_cookie[12..16].fill(0);
        assert!(decode_deletion_vector(&frame(&bad_cookie), 2, limits).is_err());
        let mut bad_count = original.to_vec();
        bad_count[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_deletion_vector(&frame(&bad_count), 2, limits).is_err());
        let mut trailing = original.to_vec();
        trailing.push(0);
        assert!(decode_deletion_vector(&frame(&trailing), 2, limits).is_err());
    }

    #[test]
    fn reads_portable_run_container_and_checks_declared_cardinality() {
        // One run container for positions 10..=19, encoded without offsets.
        let mut portable = vec![
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 48, 0, 0, 1, 0, 0, 9, 0, 1, 0, 10, 0, 9, 0,
        ];
        let limits = DeletionVectorLimits::default();
        let decoded = decode_deletion_vector(&frame(&portable), 10, limits).unwrap();
        assert_eq!(
            decoded.iter().collect::<Vec<_>>(),
            (10..20).collect::<Vec<_>>()
        );
        portable[19] = 8;
        assert!(decode_deletion_vector(&frame(&portable), 9, limits).is_err());
    }

    async fn write_file(io: &FileIO, path: &str, positions: &[u64], target: &str) -> u64 {
        let encoded =
            encode_deletion_vector(&vector(positions), DeletionVectorLimits::default()).unwrap();
        let length = encoded.len() as u64;
        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.into())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(encoded)
            .properties(HashMap::from([
                ("referenced-data-file".into(), target.into()),
                ("cardinality".into(), positions.len().to_string()),
            ]))
            .build();
        let mut writer = PuffinWriter::new(&io.new_output(path).unwrap(), HashMap::new(), false)
            .await
            .unwrap();
        writer.add(blob, CompressionCodec::None).await.unwrap();
        writer.close().await.unwrap();
        length
    }

    #[tokio::test]
    async fn bounded_puffin_read_checks_target_metadata_and_ranges() {
        let io = FileIO::new_with_memory();
        let path = "memory:///deletes.puffin";
        let target = "memory:///data.parquet";
        let limits = DeletionVectorLimits::default();
        let length = write_file(&io, path, &[3, 7], target).await;
        assert_eq!(
            read_deletion_vector(&io, path, 4, length, target, 2, limits)
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [3, 7]
        );
        for (offset, len, data_file, cardinality) in [
            (4, length, "other.parquet", 2),
            (4, length, target, 3),
            (5, length, target, 2),
            (4, length + 1, target, 2),
            (0, length, target, 2),
            (u64::MAX, length, target, 2),
        ] {
            assert!(
                read_deletion_vector(&io, path, offset, len, data_file, cardinality, limits)
                    .await
                    .is_err()
            );
        }
        assert!(
            read_deletion_vector(
                &io,
                path,
                4,
                length,
                target,
                2,
                DeletionVectorLimits {
                    max_footer_bytes: 1,
                    ..limits
                }
            )
            .await
            .is_err()
        );
        // A forged footer length must fail before attempting a large range read.
        let mut bytes = io.new_input(path).unwrap().read().await.unwrap().to_vec();
        let size = bytes.len();
        bytes[size - 12..size - 8].copy_from_slice(&u32::MAX.to_le_bytes());
        io.new_output(path)
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        assert!(
            read_deletion_vector(&io, path, 4, length, target, 2, limits)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn reads_lz4_footer_and_bounds_decompressed_size() {
        use lz4_flex::frame::{FrameEncoder, FrameInfo};
        use std::io::Write;

        let io = FileIO::new_with_memory();
        let path = "memory:///compressed-footer.puffin";
        let target = "memory:///data.parquet";
        let length = write_file(&io, path, &[1, 3], target).await;
        let original = io.new_input(path).unwrap().read().await.unwrap();
        let end = original.len();
        let payload_length =
            u32::from_le_bytes(original[end - 12..end - 8].try_into().unwrap()) as usize;
        let payload_start = end - 12 - payload_length;
        let mut payload = original[payload_start..end - 12].to_vec();
        // JSON whitespace creates a valid large footer that compresses to a small frame.
        payload.extend(std::iter::repeat_n(b' ', 4096));
        let mut encoder = FrameEncoder::with_frame_info(
            FrameInfo::new().content_size(Some(payload.len() as u64)),
            Vec::new(),
        );
        encoder.write_all(&payload).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut bytes = original[..payload_start].to_vec();
        bytes.extend_from_slice(&compressed);
        bytes.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&[1, 0, 0, 0]);
        bytes.extend_from_slice(&FileMetadata::MAGIC);
        io.new_output(path)
            .unwrap()
            .write(bytes.into())
            .await
            .unwrap();
        let decoded = read_deletion_vector(
            &io,
            path,
            4,
            length,
            target,
            2,
            DeletionVectorLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(decoded.iter().collect::<Vec<_>>(), [1, 3]);
        let error = read_deletion_vector(
            &io,
            path,
            4,
            length,
            target,
            2,
            DeletionVectorLimits {
                max_footer_bytes: 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Decompressed Puffin footer exceeds limit")
        );
    }
}
