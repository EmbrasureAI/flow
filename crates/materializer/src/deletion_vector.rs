use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use flow_iceberg_ext::ArtifactSet;
use flow_model::{OperationId, RowLocation};
use iceberg::{
    compression::CompressionCodec,
    io::FileIO,
    puffin::{Blob, DeleteVector, DeletionVectorLimits, PuffinWriter, encode_deletion_vector},
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Struct},
};

use crate::{WriterConfig, writer::ParquetDeleteWriter};

/// Writes sorted, unique positions in the table's format. V3 keeps one vector
/// per target, even when batches or requested file rotations split that target.
pub struct DeleteWriter {
    inner: Writer,
}

enum Writer {
    Parquet(Box<ParquetDeleteWriter>),
    Puffin(Box<VectorWriter>),
}

impl DeleteWriter {
    pub fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        spec_id: i32,
        config: WriterConfig,
    ) -> Result<Self> {
        Self::new_for_version(
            file_io,
            location,
            operation,
            spec_id,
            config,
            FormatVersion::V2,
        )
    }

    pub fn new_for_version(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        spec_id: i32,
        config: WriterConfig,
        version: FormatVersion,
    ) -> Result<Self> {
        let inner = match version {
            FormatVersion::V2 => Writer::Parquet(Box::new(ParquetDeleteWriter::new(
                file_io, location, operation, spec_id, config,
            )?)),
            FormatVersion::V3 => {
                ensure!(
                    spec_id >= 0
                        && config.target_file_bytes > 0
                        && config.row_group_bytes >= 20
                        && config.row_group_rows > 0,
                    "invalid deletion-vector writer limits"
                );
                ensure!(
                    !operation.0.is_empty()
                        && operation
                            .0
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                    "invalid operation ID"
                );
                Writer::Puffin(Box::new(VectorWriter {
                    file_io,
                    prefix: format!("{}/data/{}", location.trim_end_matches('/'), operation.0),
                    spec_id,
                    config,
                    target: None,
                    positions: DeleteVector::default(),
                    last_position: None,
                    current: None,
                    descriptors: Vec::new(),
                    files: Vec::new(),
                    offset: 4,
                    ordinal: 0,
                    rotate: false,
                    footer_bytes: 0,
                }))
            }
            _ => anyhow::bail!("delete writer requires Iceberg v2 or v3"),
        };
        Ok(Self { inner })
    }

    pub async fn write(&mut self, positions: &[RowLocation]) -> Result<()> {
        match &mut self.inner {
            Writer::Parquet(writer) => writer.write(positions).await,
            Writer::Puffin(writer) => writer.write(positions).await,
        }
    }

    /// V3 defers rotation until the next target, preventing split vectors.
    pub async fn finish_file(&mut self) -> Result<()> {
        match &mut self.inner {
            Writer::Parquet(writer) => writer.finish_file().await,
            Writer::Puffin(writer) => {
                writer.rotate = true;
                Ok(())
            }
        }
    }

    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self.inner {
            Writer::Parquet(writer) => writer.close().await,
            Writer::Puffin(mut writer) => {
                writer.finish_target().await?;
                writer.finish_object().await?;
                Ok(writer.files)
            }
        }
    }
}

struct VectorWriter {
    file_io: FileIO,
    prefix: String,
    spec_id: i32,
    config: WriterConfig,
    target: Option<String>,
    positions: DeleteVector,
    last_position: Option<u64>,
    current: Option<PuffinWriter>,
    descriptors: Vec<(String, u64, u64, u64)>,
    files: Vec<DataFile>,
    offset: u64,
    ordinal: usize,
    rotate: bool,
    footer_bytes: usize,
}

impl VectorWriter {
    fn path(&self) -> String {
        format!("{}-delete-{:06}.puffin", self.prefix, self.ordinal)
    }

    async fn write(&mut self, positions: &[RowLocation]) -> Result<()> {
        let limit = self
            .config
            .row_group_bytes
            .min(DeletionVectorLimits::default().max_blob_bytes as usize);
        for chunk in positions.chunks(self.config.row_group_rows) {
            // A new high-key bitmap costs at most 32 encoded bytes per inserted
            // position. Reserve that headroom once per batch; near the limit,
            // exact checks retain the same admission boundary.
            let check_each = chunk.len().saturating_mul(32)
                > limit.saturating_sub(self.positions.serialized_size());
            for position in chunk {
                let target = &position.data_file_id.0;
                ensure!(
                    position.spec_id == self.spec_id
                        && position.partition.is_empty()
                        && position.row_position <= i64::MAX as u64,
                    "invalid deletion-vector position"
                );
                if let Some(previous) = &self.target {
                    ensure!(
                        target > previous
                            || (target == previous
                                && self
                                    .last_position
                                    .is_none_or(|last| position.row_position > last)),
                        "delete positions must be sorted and unique"
                    );
                }
                if self.target.as_ref() != Some(target) {
                    self.finish_target().await?;
                    if self.rotate || self.offset >= self.config.target_file_bytes as u64 {
                        self.finish_object().await?;
                    }
                    self.target = Some(target.clone());
                    self.last_position = None;
                }
                self.positions.insert(position.row_position);
                self.last_position = Some(position.row_position);
                ensure!(
                    !check_each || self.positions.serialized_size() <= limit,
                    "deletion vector exceeds writer memory budget"
                );
            }
        }
        Ok(())
    }

    async fn finish_target(&mut self) -> Result<()> {
        if self.positions.len() == 0 {
            return Ok(());
        }
        let target = self
            .target
            .as_ref()
            .context("vector has no target")?
            .clone();
        // JSON can escape each path byte to six bytes; leave room for all
        // fixed blob fields and the outer container. Bound metadata separately
        // because many tiny vectors otherwise produce a very large footer.
        let metadata_bytes = target
            .len()
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(512))
            .context("Puffin metadata size overflow")?;
        let footer_limit = DeletionVectorLimits::default().max_footer_bytes as usize / 2;
        ensure!(
            metadata_bytes <= footer_limit,
            "delete target exceeds Puffin metadata budget"
        );
        if self.footer_bytes > footer_limit - metadata_bytes {
            self.finish_object().await?;
        }
        self.footer_bytes += metadata_bytes;
        let limits = DeletionVectorLimits {
            max_blob_bytes: (self.config.row_group_bytes as u64)
                .min(DeletionVectorLimits::default().max_blob_bytes),
            ..Default::default()
        };
        let data = encode_deletion_vector(&self.positions, limits)?;
        let length = data.len() as u64;
        let cardinality = self.positions.len();
        if self.current.is_none() {
            let path = self.path();
            if let Some(tracker) = &self.config.artifact_tracker {
                tracker
                    .register(ArtifactSet {
                        paths: vec![path.clone()],
                        ranges: Vec::new(),
                    })
                    .await?;
            }
            ensure!(
                !self.file_io.exists(&path).await?,
                "refusing to overwrite prepared delete artifact"
            );
            self.current = Some(
                PuffinWriter::new(&self.file_io.new_output(&path)?, HashMap::new(), false).await?,
            );
        }
        let blob = Blob::builder()
            .r#type("deletion-vector-v1".to_owned())
            .fields(Vec::new())
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(HashMap::from([
                ("referenced-data-file".into(), target.clone()),
                ("cardinality".into(), cardinality.to_string()),
            ]))
            .build();
        self.current
            .as_mut()
            .expect("opened writer")
            .add(blob, CompressionCodec::None)
            .await?;
        self.descriptors
            .push((target, self.offset, length, cardinality));
        self.offset = self
            .offset
            .checked_add(length)
            .context("Puffin size overflow")?;
        self.positions = DeleteVector::default();
        Ok(())
    }

    async fn finish_object(&mut self) -> Result<()> {
        if let Some(writer) = self.current.take() {
            writer.close().await?;
            let path = self.path();
            let size = self.file_io.new_input(&path)?.metadata().await?.size;
            for (target, offset, length, cardinality) in self.descriptors.drain(..) {
                self.files.push(
                    DataFileBuilder::default()
                        .content(DataContentType::PositionDeletes)
                        .file_path(path.clone())
                        .file_format(DataFileFormat::Puffin)
                        .partition(Struct::empty())
                        .partition_spec_id(self.spec_id)
                        .record_count(cardinality)
                        .file_size_in_bytes(size)
                        .referenced_data_file(Some(target))
                        .content_offset(Some(i64::try_from(offset)?))
                        .content_size_in_bytes(Some(i64::try_from(length)?))
                        .build()?,
                );
            }
            self.ordinal += 1;
        }
        self.offset = 4;
        self.footer_bytes = 0;
        self.rotate = false;
        Ok(())
    }
}
