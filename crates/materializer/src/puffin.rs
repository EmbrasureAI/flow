//! Deletion-vector reads that parse each shared Puffin footer once.
//!
//! Validation matches `iceberg::puffin::read_deletion_vector_with_size`: magic,
//! footer flags, footer length and LZ4 decompression limits, exactly one blob at
//! the requested offset with the requested length, type, uncompressed codec,
//! unassigned snapshot and sequence, referenced data file and cardinality, then
//! framing, CRC and bitmap decoding. Only the requests differ: one size request
//! and one tail read per object (plus header and footer reads when the object
//! is larger than the tail), then one range read per blob outside the tail.

use std::{borrow::Cow, collections::HashMap, io::Read};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesOrdered};
use iceberg::{
    Error, ErrorKind,
    io::{FileIO, FileRead},
    puffin::{
        BlobMetadata, CompressionCodec, DELETION_VECTOR_V1, DeleteVector, DeletionVectorLimits,
        FileMetadata, decode_deletion_vector,
    },
    spec::DataFile,
};

const MAGIC: [u8; 4] = [0x50, 0x46, 0x41, 0x31];
const MIN_BLOB_BYTES: u64 = 20;
/// Read together with the footer. Objects up to this size need one GET.
const TAIL_BYTES: u64 = 64 * 1024;

/// One deletion-vector descriptor to read.
#[derive(Debug, Clone)]
pub struct VectorRead {
    pub path: String,
    /// The manifest's object size. The observed size must equal it.
    pub file_size: u64,
    pub offset: u64,
    pub length: u64,
    pub referenced_data_file: String,
    pub cardinality: u64,
    pub limits: DeletionVectorLimits,
}

impl VectorRead {
    pub fn new(file: &DataFile, limits: DeletionVectorLimits) -> Result<Self> {
        Ok(Self {
            path: file.file_path().to_owned(),
            file_size: file.file_size_in_bytes(),
            offset: u64::try_from(file.content_offset().context("DV lacks offset")?)?,
            length: u64::try_from(file.content_size_in_bytes().context("DV lacks length")?)?,
            referenced_data_file: file.referenced_data_file().context("DV lacks target")?,
            cardinality: file.record_count(),
            limits,
        })
    }
}

/// Bounds one [`read_vectors`] call.
#[derive(Debug, Clone, Copy)]
pub struct VectorReadLimits {
    /// Stored and decompressed Puffin footer limit.
    pub max_footer_bytes: u64,
    /// Objects opened, and blobs fetched, at once.
    pub concurrency: usize,
    /// Blob and tail bytes held by one batch. A larger single blob is read alone.
    pub max_batch_bytes: u64,
}

/// Read every vector, calling `visit` with its request index. Requests are
/// grouped by object in first-appearance order; visits follow that order. Each
/// object's size is observed once and must match every descriptor.
pub async fn read_vectors(
    file_io: &FileIO,
    requests: &[VectorRead],
    limits: VectorReadLimits,
    mut visit: impl FnMut(usize, DeleteVector) -> Result<()>,
) -> Result<()> {
    // Argument validation precedes all I/O, as in the vendored reader.
    for request in requests {
        checked_blob_end(request)?;
    }
    let mut slots: HashMap<&str, usize> = HashMap::new();
    let mut objects: Vec<Vec<usize>> = Vec::new();
    for (index, request) in requests.iter().enumerate() {
        let slot = *slots.entry(&request.path).or_insert_with(|| {
            objects.push(Vec::new());
            objects.len() - 1
        });
        objects[slot].push(index);
    }
    let order: Vec<usize> = objects.iter().flatten().copied().collect();
    let concurrency = limits.concurrency.max(1);
    let mut opened: HashMap<&str, PuffinObject> = HashMap::new();
    let mut start = 0;
    while start < order.len() {
        // Charge each newly opened object's tail as well as every blob.
        let mut end = start;
        let mut bytes = 0u64;
        let mut fresh: Vec<&str> = Vec::new();
        while end < order.len() {
            let request = &requests[order[end]];
            let new = !opened.contains_key(request.path.as_str())
                && fresh.last() != Some(&request.path.as_str());
            let cost = request
                .length
                .saturating_add(if new { TAIL_BYTES } else { 0 });
            if end > start && bytes.saturating_add(cost) > limits.max_batch_bytes {
                break;
            }
            bytes = bytes.saturating_add(cost);
            if new {
                fresh.push(&request.path);
            }
            end += 1;
        }
        // Futures are queued directly rather than through `Stream::map`
        // closures, which would make callers' futures not `Send`.
        let mut paths = fresh.into_iter();
        let mut opens = FuturesOrdered::new();
        loop {
            while opens.len() < concurrency
                && let Some(path) = paths.next()
            {
                let members = objects[slots[path]].as_slice();
                opens.push_back(open_object(file_io, requests, members, path, limits));
            }
            let Some(result) = opens.next().await else {
                break;
            };
            let (path, object) = result?;
            opened.insert(path, object);
        }
        let mut indexes = order[start..end].iter();
        let mut vectors = FuturesOrdered::new();
        loop {
            while vectors.len() < concurrency
                && let Some(&index) = indexes.next()
            {
                let request = &requests[index];
                let object = &opened[request.path.as_str()];
                vectors.push_back(async move { (index, object.read(request).await) });
            }
            let Some((index, vector)) = vectors.next().await else {
                break;
            };
            visit(index, vector?)?;
        }
        drop(vectors);
        // Requests of one object are contiguous; only the last can continue.
        let next = order.get(end).map(|&index| requests[index].path.as_str());
        opened.retain(|path, _| Some(*path) == next);
        start = end;
    }
    Ok(())
}

async fn open_object<'a>(
    file_io: &FileIO,
    requests: &[VectorRead],
    members: &[usize],
    path: &'a str,
    limits: VectorReadLimits,
) -> Result<(&'a str, PuffinObject)> {
    let input = file_io.new_input(path)?;
    let size = input.metadata().await?.size;
    for &index in members {
        ensure!(
            requests[index].file_size == size,
            "delete file size differs from manifest"
        );
        check_range(&requests[index], size)?;
    }
    let object = PuffinObject::open(
        input.reader().await?,
        size,
        limits.max_footer_bytes,
        members.iter().map(|&index| requests[index].offset),
        TAIL_BYTES,
    )
    .await?;
    Ok((path, object))
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

fn checked_blob_end(request: &VectorRead) -> iceberg::Result<u64> {
    let limits = request.limits;
    if !(MIN_BLOB_BYTES..=limits.max_blob_bytes).contains(&request.length) {
        return Err(invalid(
            "Deletion vector blob length exceeds limits or is too short",
        ));
    }
    if request.cardinality > limits.max_cardinality {
        return Err(invalid("Deletion vector cardinality exceeds limit"));
    }
    let end = request
        .offset
        .checked_add(request.length)
        .ok_or_else(|| invalid("Deletion vector byte range overflow"))?;
    if request.offset < 4 {
        return Err(invalid("Deletion vector overlaps Puffin header"));
    }
    Ok(end)
}

fn check_range(request: &VectorRead, size: u64) -> iceberg::Result<u64> {
    let end = checked_blob_end(request)?;
    if size < 20 || end > size - 16 {
        return Err(invalid("Deletion vector byte range exceeds Puffin file"));
    }
    Ok(end)
}

/// One Puffin object with its validated footer, retaining only requested blobs.
struct PuffinObject {
    reader: Box<dyn FileRead>,
    size: u64,
    tail_start: u64,
    tail: Bytes,
    footer_start: u64,
    // None marks an offset shared by several blobs.
    blobs: HashMap<u64, Option<BlobMetadata>>,
}

impl PuffinObject {
    async fn open(
        reader: Box<dyn FileRead>,
        size: u64,
        max_footer_bytes: u64,
        offsets: impl Iterator<Item = u64>,
        tail_bytes: u64,
    ) -> iceberg::Result<Self> {
        if size < 20 {
            return Err(invalid("Deletion vector byte range exceeds Puffin file"));
        }
        let tail_start = size.saturating_sub(tail_bytes.max(12));
        let (header, tail) = if tail_start == 0 {
            let tail = reader.read(0..size).await?;
            (tail.slice(..4.min(tail.len())), tail)
        } else {
            futures::try_join!(reader.read(0..4), reader.read(tail_start..size))?
        };
        if header.as_ref() != MAGIC
            || tail.len() as u64 != size - tail_start
            || tail[tail.len() - 4..] != MAGIC
        {
            return Err(invalid("Invalid Puffin header or footer magic"));
        }
        let fixed = &tail[tail.len() - 12..];
        let compressed_footer = fixed[4..8] == [1, 0, 0, 0];
        if !compressed_footer && fixed[4..8] != [0; 4] {
            return Err(invalid("Invalid Puffin footer flags"));
        }
        let footer_length = u64::from(u32::from_le_bytes(fixed[..4].try_into().unwrap()));
        if footer_length > max_footer_bytes || footer_length > size - 20 {
            return Err(invalid("Puffin footer length exceeds limit or file bounds"));
        }
        let footer_start = size - 16 - footer_length;
        let footer = if footer_start >= tail_start {
            let start = (footer_start - tail_start) as usize;
            tail.slice(start..tail.len() - 12)
        } else {
            reader.read(footer_start..size - 12).await?
        };
        if footer.len() as u64 != footer_length + 4 || footer[..4] != MAGIC {
            return Err(invalid("Invalid Puffin footer length or magic"));
        }
        let payload = if compressed_footer {
            let mut decoded = Vec::new();
            lz4_flex::frame::FrameDecoder::new(&footer[4..])
                .take(max_footer_bytes.saturating_add(1))
                .read_to_end(&mut decoded)
                .map_err(|err| invalid("Invalid compressed Puffin footer").with_source(err))?;
            if decoded.len() as u64 > max_footer_bytes {
                return Err(invalid("Decompressed Puffin footer exceeds limit"));
            }
            Cow::Owned(decoded)
        } else {
            Cow::Borrowed(&footer[4..])
        };
        let metadata: FileMetadata = serde_json::from_slice(&payload)
            .map_err(|err| invalid("Invalid Puffin deletion vector metadata").with_source(err))?;
        let mut blobs: HashMap<u64, Option<BlobMetadata>> =
            offsets.map(|offset| (offset, None)).collect();
        let mut seen = HashMap::with_capacity(blobs.len());
        for blob in metadata.blobs() {
            if let Some(slot) = blobs.get_mut(&blob.offset()) {
                let count = seen.entry(blob.offset()).or_insert(0usize);
                *count += 1;
                *slot = (*count == 1).then(|| blob.clone());
            }
        }
        // Requested offsets absent from the footer are reported per request.
        blobs.retain(|offset, _| seen.contains_key(offset));
        Ok(Self {
            reader,
            size,
            tail_start,
            tail,
            footer_start,
            blobs,
        })
    }

    async fn read(&self, request: &VectorRead) -> iceberg::Result<DeleteVector> {
        let end = check_range(request, self.size)?;
        if end > self.footer_start {
            return Err(invalid("Deletion vector overlaps Puffin footer"));
        }
        let blob = self
            .blobs
            .get(&request.offset)
            .ok_or_else(|| invalid("Deletion vector missing from Puffin footer"))?;
        if !blob.as_ref().is_some_and(|blob| {
            blob.length() == request.length
                && blob.blob_type() == DELETION_VECTOR_V1
                && blob.compression_codec() == CompressionCodec::None
                && blob.snapshot_id() == -1
                && blob.sequence_number() == -1
                && blob
                    .properties()
                    .get("referenced-data-file")
                    .map(String::as_str)
                    == Some(request.referenced_data_file.as_str())
                && blob
                    .properties()
                    .get("cardinality")
                    .and_then(|value| value.parse::<u64>().ok())
                    == Some(request.cardinality)
        }) {
            return Err(invalid(
                "Deletion vector metadata does not match requested blob",
            ));
        }
        let bytes = if request.offset >= self.tail_start {
            let start = (request.offset - self.tail_start) as usize;
            self.tail.slice(start..start + request.length as usize)
        } else {
            self.reader.read(request.offset..end).await?
        };
        if bytes.len() as u64 != request.length {
            return Err(invalid("Truncated deletion vector blob range"));
        }
        decode_deletion_vector(&bytes, request.cardinality, request.limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::{FileId, OperationId, PgLsn, RowLocation};
    use iceberg::{puffin::read_deletion_vector_with_size, spec::FormatVersion};

    use crate::{DeleteWriter, WriterConfig};

    async fn write(io: &FileIO, location: &str, targets: &[(&str, &[u64])]) -> Vec<DataFile> {
        let mut writer = DeleteWriter::new_for_version(
            io.clone(),
            location,
            &OperationId("vectors".into()),
            0,
            WriterConfig::default(),
            FormatVersion::V3,
        )
        .unwrap();
        for (target, positions) in targets {
            let rows: Vec<_> = positions
                .iter()
                .map(|&row_position| RowLocation {
                    data_file_id: FileId((*target).into()),
                    row_position,
                    data_sequence_number: 1,
                    spec_id: 0,
                    partition: vec![],
                    source_commit_lsn: PgLsn(1),
                    row_version: 0,
                    row_fingerprint: [0; 16],
                })
                .collect();
            writer.write(&rows).await.unwrap();
        }
        writer.close().await.unwrap()
    }

    fn limits() -> VectorReadLimits {
        VectorReadLimits {
            max_footer_bytes: DeletionVectorLimits::default().max_footer_bytes,
            concurrency: 4,
            max_batch_bytes: 1 << 20,
        }
    }

    async fn read_all(io: &FileIO, requests: &[VectorRead]) -> Result<Vec<Vec<u64>>> {
        let mut vectors = vec![Vec::new(); requests.len()];
        read_vectors(io, requests, limits(), |index, vector| {
            vectors[index] = vector.iter().collect();
            Ok(())
        })
        .await?;
        Ok(vectors)
    }

    // Each candidate object is compared against the vendored reader for every
    // descriptor, with the tail covering the whole object or only its end.
    async fn assert_parity(io: &FileIO, path: &str, requests: &[VectorRead]) {
        let input = io.new_input(path).unwrap();
        let size = input.metadata().await.unwrap().size;
        for tail in [12, 24, 64, TAIL_BYTES] {
            let object = PuffinObject::open(
                input.reader().await.unwrap(),
                size,
                DeletionVectorLimits::default().max_footer_bytes,
                requests.iter().map(|request| request.offset),
                tail,
            )
            .await;
            for request in requests {
                let expected = read_deletion_vector_with_size(
                    &input,
                    size,
                    request.offset,
                    request.length,
                    &request.referenced_data_file,
                    request.cardinality,
                    request.limits,
                )
                .await
                .map(|vector| vector.iter().collect::<Vec<_>>());
                let actual = match &object {
                    Ok(object) => object
                        .read(request)
                        .await
                        .map(|vector| vector.iter().collect::<Vec<_>>()),
                    Err(error) => Err(invalid(&error.to_string())),
                };
                match (&expected, &actual) {
                    (Ok(expected), Ok(actual)) => assert_eq!(expected, actual),
                    (Err(_), Err(_)) => {}
                    _ => panic!("tail {tail}: vendored {expected:?}, footer-once {actual:?}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn every_single_byte_corruption_matches_the_vendored_reader() {
        let io = FileIO::new_with_memory();
        let files = write(
            &io,
            "memory:///table",
            &[("a", &[0, 3]), ("b", &[1 << 32]), ("c", &[7, 8, 9])],
        )
        .await;
        let path = files[0].file_path().to_owned();
        assert!(files.iter().all(|file| file.file_path() == path));
        let requests: Vec<_> = files
            .iter()
            .map(|file| VectorRead::new(file, DeletionVectorLimits::default()).unwrap())
            .collect();
        assert_eq!(
            read_all(&io, &requests).await.unwrap(),
            [vec![0, 3], vec![1 << 32], vec![7, 8, 9]]
        );
        let original = io.new_input(&path).unwrap().read().await.unwrap();
        assert_parity(&io, &path, &requests).await;
        for index in 0..original.len() {
            for flip in [1u8, 0x80] {
                let mut corrupt = original.to_vec();
                corrupt[index] ^= flip;
                io.new_output(&path)
                    .unwrap()
                    .write(corrupt.into())
                    .await
                    .unwrap();
                assert_parity(&io, &path, &requests).await;
            }
        }
        // Truncation and extension change the observed size.
        for size in [0, 19, 20, original.len() - 1, original.len() + 1] {
            let mut changed = original.to_vec();
            changed.resize(size, 0);
            io.new_output(&path)
                .unwrap()
                .write(changed.into())
                .await
                .unwrap();
            assert_parity(&io, &path, &requests).await;
        }
    }

    #[tokio::test]
    async fn request_mismatches_and_limits_match_the_vendored_reader() {
        let io = FileIO::new_with_memory();
        let files = write(&io, "memory:///table", &[("a", &[0, 3]), ("b", &[5])]).await;
        let path = files[0].file_path().to_owned();
        let good = VectorRead::new(&files[1], DeletionVectorLimits::default()).unwrap();
        let variants = [
            VectorRead {
                referenced_data_file: "other".into(),
                ..good.clone()
            },
            VectorRead {
                cardinality: 2,
                ..good.clone()
            },
            VectorRead {
                offset: good.offset + 1,
                ..good.clone()
            },
            VectorRead {
                length: good.length + 1,
                ..good.clone()
            },
            VectorRead {
                offset: 0,
                ..good.clone()
            },
            VectorRead {
                offset: u64::MAX,
                ..good.clone()
            },
            VectorRead {
                limits: DeletionVectorLimits {
                    max_blob_bytes: good.length - 1,
                    ..good.limits
                },
                ..good.clone()
            },
            VectorRead {
                limits: DeletionVectorLimits {
                    max_cardinality: 0,
                    ..good.limits
                },
                ..good.clone()
            },
        ];
        assert_parity(&io, &path, &variants).await;
        for variant in variants {
            assert!(read_all(&io, &[good.clone(), variant]).await.is_err());
        }
        let resized = VectorRead {
            file_size: good.file_size + 1,
            ..good.clone()
        };
        assert_eq!(
            read_all(&io, &[good.clone(), resized])
                .await
                .unwrap_err()
                .to_string(),
            "delete file size differs from manifest"
        );
        // The footer limit applies once per object.
        let mut tight = limits();
        tight.max_footer_bytes = 1;
        let error = read_vectors(&io, &[good], tight, |_, _| Ok(()))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("footer length exceeds limit"));
    }

    #[tokio::test]
    async fn duplicate_offsets_and_compressed_footers_match_the_vendored_reader() {
        use lz4_flex::frame::{FrameEncoder, FrameInfo};
        use std::io::Write;

        let io = FileIO::new_with_memory();
        let files = write(&io, "memory:///table", &[("a", &[1, 3]), ("b", &[4])]).await;
        let path = files[0].file_path().to_owned();
        let requests: Vec<_> = files
            .iter()
            .map(|file| VectorRead::new(file, DeletionVectorLimits::default()).unwrap())
            .collect();
        let original = io.new_input(&path).unwrap().read().await.unwrap();
        let end = original.len();
        let footer_length = u32::from_le_bytes(original[end - 12..end - 8].try_into().unwrap());
        let payload_start = end - 12 - footer_length as usize;
        let payload = original[payload_start..end - 12].to_vec();
        let replace = |payload: Vec<u8>, compressed: bool| {
            let mut bytes = original[..payload_start].to_vec();
            let payload = if compressed {
                let mut encoder = FrameEncoder::with_frame_info(
                    FrameInfo::new().content_size(Some(payload.len() as u64)),
                    Vec::new(),
                );
                encoder.write_all(&payload).unwrap();
                encoder.finish().unwrap()
            } else {
                payload
            };
            bytes.extend_from_slice(&payload);
            bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&[u8::from(compressed), 0, 0, 0]);
            bytes.extend_from_slice(&MAGIC);
            bytes
        };
        // A large whitespace-padded footer compresses to a small frame.
        let mut padded = payload.clone();
        padded.extend(std::iter::repeat_n(b' ', 4096));
        let compressed = replace(padded.clone(), true);
        let sized = |size: usize| -> Vec<VectorRead> {
            requests
                .iter()
                .map(|request| VectorRead {
                    file_size: size as u64,
                    ..request.clone()
                })
                .collect()
        };
        let requests = sized(compressed.len());
        io.new_output(&path)
            .unwrap()
            .write(compressed.into())
            .await
            .unwrap();
        assert_parity(&io, &path, &requests).await;
        assert_eq!(
            read_all(&io, &requests).await.unwrap(),
            [vec![1, 3], vec![4]]
        );
        let mut tight = limits();
        tight.max_footer_bytes = 1024;
        let error = read_vectors(&io, &requests, tight, |_, _| Ok(()))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Decompressed Puffin footer exceeds limit")
        );
        // Duplicate the first blob's metadata entry: its offset is ambiguous.
        let mut metadata: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let blobs = metadata["blobs"].as_array_mut().unwrap();
        blobs.push(blobs[0].clone());
        let duplicated = serde_json::to_vec(&metadata).unwrap();
        for compressed in [false, true] {
            let object = replace(duplicated.clone(), compressed);
            let requests = sized(object.len());
            io.new_output(&path)
                .unwrap()
                .write(object.into())
                .await
                .unwrap();
            assert_parity(&io, &path, &requests).await;
            assert!(read_all(&io, &requests).await.is_err());
            assert_eq!(read_all(&io, &requests[1..]).await.unwrap(), [vec![4]]);
        }
    }

    #[tokio::test]
    async fn shared_objects_and_batches_return_every_vector_in_object_order() {
        let io = FileIO::new_with_memory();
        let mut requests = Vec::new();
        let mut expected = Vec::new();
        for object in 0..3 {
            let targets: Vec<(String, Vec<u64>)> = (0..4)
                .map(|target| {
                    (
                        format!("data-{object}-{target}"),
                        (0..=(object * 4 + target)).map(|p| p * 3).collect(),
                    )
                })
                .collect();
            let borrowed: Vec<(&str, &[u64])> = targets
                .iter()
                .map(|(target, positions)| (target.as_str(), positions.as_slice()))
                .collect();
            for file in write(&io, &format!("memory:///table-{object}"), &borrowed).await {
                requests.push(VectorRead::new(&file, DeletionVectorLimits::default()).unwrap());
            }
            expected.extend(targets.into_iter().map(|(_, positions)| positions));
        }
        // Interleave objects; visits are grouped by first appearance.
        let interleaved: Vec<_> = (0..4)
            .flat_map(|target| (0..3).map(move |object| object * 4 + target))
            .collect();
        let shuffled: Vec<_> = interleaved.iter().map(|&i| requests[i].clone()).collect();
        for batch in [1, 64, 1 << 20] {
            let mut visited = Vec::new();
            read_vectors(
                &io,
                &shuffled,
                VectorReadLimits {
                    max_batch_bytes: batch,
                    ..limits()
                },
                |index, vector| {
                    visited.push(index);
                    assert_eq!(
                        vector.iter().collect::<Vec<_>>(),
                        expected[interleaved[index]]
                    );
                    Ok(())
                },
            )
            .await
            .unwrap();
            let grouped: Vec<_> = (0..3)
                .flat_map(|object| (0..4).map(move |target| target * 3 + object))
                .collect();
            assert_eq!(visited, grouped);
        }
    }
}
