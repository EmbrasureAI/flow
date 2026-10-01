use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use iceberg::Result;
use iceberg::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH;
use iceberg::spec::{
    DataContentType, DataFileFormat, Datum, Manifest, ManifestEntry, ManifestEntryRef,
    ManifestFile, PrimitiveLiteral,
};
use iceberg::table::Table;
use moka::future::Cache;
use uuid::Uuid;

use crate::{content_file_id, invalid};

/// Manifests unread for this long leave the cache; a later read reloads them.
/// Publication and inventory read every manifest of a table's current
/// snapshot, so those stay cached. Manifests superseded by commits, compaction
/// or manifest rewrites are read only by work on retained snapshots, if at
/// all; without expiry they accumulate until the budget is full. Twice the
/// default garbage interval keeps quiet tables' periodic maintenance warm.
const MANIFEST_IDLE: std::time::Duration = std::time::Duration::from_secs(600);

/// Bounded, shared cache of immutable parsed manifests. A table coordinator can
/// reuse this across actions so publication reads only new manifest objects.
/// The budget is a decoded-size estimate, not an allocator quota.
#[derive(Debug, Clone)]
pub struct ManifestCache {
    entries: Cache<ManifestCacheKey, Arc<Manifest>>,
}

type ManifestCacheKey = (Uuid, String, i64, i64, Option<u64>);

fn data_file_weight(file: &iceberg::spec::DataFile) -> usize {
    let mut bytes = 512_usize
        .saturating_add(file.file_path().len())
        .saturating_add(
            (file.column_sizes().len()
                + file.value_counts().len()
                + file.null_value_counts().len()
                + file.nan_value_counts().len())
            .saturating_mul(48),
        )
        .saturating_add(file.key_metadata().map_or(0, <[u8]>::len))
        .saturating_add(
            file.split_offsets()
                .map_or(0, |v| v.len().saturating_mul(8)),
        )
        .saturating_add(file.equality_ids().map_or(0, |v| v.len().saturating_mul(4)))
        .saturating_add(file.referenced_data_file().as_ref().map_or(0, String::len));
    for bound in file
        .lower_bounds()
        .values()
        .chain(file.upper_bounds().values())
    {
        let payload = match bound.literal() {
            PrimitiveLiteral::String(value) => value.capacity(),
            PrimitiveLiteral::Binary(value) => value.capacity(),
            _ => 0,
        };
        bytes = bytes.saturating_add(128).saturating_add(payload);
    }
    bytes
}

impl ManifestCache {
    pub fn new(estimated_bytes: u64) -> Self {
        Self::with_idle(estimated_bytes, MANIFEST_IDLE)
    }

    fn with_idle(estimated_bytes: u64, idle: std::time::Duration) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(estimated_bytes)
                .time_to_idle(idle)
                .weigher(|_, manifest: &Arc<Manifest>| {
                    let bytes = manifest.entries().iter().fold(4096_usize, |bytes, entry| {
                        bytes.saturating_add(data_file_weight(entry.data_file()))
                    });
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }

    /// Estimated bytes charged against the budget. Maintenance runs lazily,
    /// so recent insertions and evictions may not be reflected yet.
    pub fn weighted_bytes(&self) -> u64 {
        self.entries.weighted_size()
    }

    pub fn entry_count(&self) -> u64 {
        self.entries.entry_count()
    }

    pub(crate) async fn load(
        &self,
        table: &Table,
        manifest: &ManifestFile,
    ) -> Result<Arc<Manifest>> {
        let key = (
            table.metadata().uuid(),
            manifest.manifest_path.clone(),
            manifest.sequence_number,
            manifest.added_snapshot_id,
            manifest.first_row_id,
        );
        self.entries
            .try_get_with(key, async {
                manifest.load_manifest(table.file_io()).await.map(Arc::new)
            })
            .await
            .map_err(|error| iceberg::Error::new(error.kind(), error.message()).with_source(error))
    }

    /// Reuse a cached manifest, but do not admit one read for background
    /// history scans. Those reads would evict manifests of the current snapshot
    /// that publication reuses on every commit.
    pub(crate) async fn peek_or_read(
        &self,
        table: &Table,
        manifest: &ManifestFile,
    ) -> Result<Arc<Manifest>> {
        let key = (
            table.metadata().uuid(),
            manifest.manifest_path.clone(),
            manifest.sequence_number,
            manifest.added_snapshot_id,
            manifest.first_row_id,
        );
        match self.entries.get(&key).await {
            Some(cached) => Ok(cached),
            None => manifest.load_manifest(table.file_io()).await.map(Arc::new),
        }
    }
}

impl Default for ManifestCache {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024)
    }
}

/// A manifest-backed view of one immutable snapshot. It contains physical files,
/// not row data. The manifest list is retained so unchanged manifests can be
/// reused on publication without merging the full table on the hot path.
#[derive(Debug, Clone, Default)]
pub struct SnapshotView {
    pub snapshot_id: Option<i64>,
    pub live_files: BTreeMap<String, ManifestEntryRef>,
    pub(crate) manifests: Vec<ManifestFile>,
    pub(crate) entries_by_manifest: BTreeMap<String, Vec<ManifestEntryRef>>,
    deletes_by_target: HashMap<String, Vec<ManifestEntryRef>>,
    shared_deletes: Vec<ManifestEntryRef>,
}

impl SnapshotView {
    pub fn manifest_count(&self) -> usize {
        self.manifests.len()
    }

    /// Includes deleted entries still carried by the current manifest list.
    pub fn manifest_entries(&self) -> usize {
        self.entries_by_manifest.values().map(Vec::len).sum()
    }

    /// Load a snapshot's manifests with bounded I/O concurrency.
    pub async fn load(table: &Table, snapshot_id: i64) -> Result<Self> {
        Self::load_with_cache(table, snapshot_id, &ManifestCache::default()).await
    }

    pub async fn load_with_cache(
        table: &Table,
        snapshot_id: i64,
        cache: &ManifestCache,
    ) -> Result<Self> {
        if !matches!(
            table.metadata().format_version(),
            iceberg::spec::FormatVersion::V2 | iceberg::spec::FormatVersion::V3
        ) {
            return Err(invalid("snapshot views currently support Iceberg v2 or v3"));
        }
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| invalid(format!("snapshot {snapshot_id} is not retained")))?;
        let manifests: Vec<_> = table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .consume_entries()
            .into_iter()
            .collect();
        if !table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty()
            || manifests.iter().any(|manifest| {
                manifest.partition_spec_id != table.metadata().default_partition_spec_id()
            })
        {
            return Err(invalid("only the current unpartitioned spec is supported"));
        }
        let loaded: Vec<_> = stream::iter(manifests.iter().cloned())
            .map(|manifest| async move {
                let entries = cache.load(table, &manifest).await?.entries().to_vec();
                Ok::<_, iceberg::Error>((manifest.manifest_path, entries))
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        let mut live_files = BTreeMap::new();
        let mut entries_by_manifest = BTreeMap::new();
        for (path, entries) in loaded {
            for entry in entries.iter().filter(|entry| entry.is_alive()) {
                if live_files
                    .insert(content_file_id(entry.data_file()), entry.clone())
                    .is_some()
                {
                    return Err(invalid(format!(
                        "duplicate live file {}",
                        entry.file_path()
                    )));
                }
                if entry.sequence_number.is_none() || entry.file_sequence_number.is_none() {
                    return Err(invalid("manifest entry lacks inherited sequence numbers"));
                }
            }
            entries_by_manifest.insert(path, entries);
        }
        crate::action::validate_live_vectors(live_files.values().map(|entry| entry.data_file()))?;
        let mut deletes_by_target: HashMap<String, Vec<ManifestEntryRef>> = HashMap::new();
        let mut shared_deletes = Vec::new();
        for entry in live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        {
            let file = entry.data_file();
            let exact_target = file.referenced_data_file().or_else(|| {
                let lower = file
                    .lower_bounds()
                    .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
                let upper = file
                    .upper_bounds()
                    .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)?;
                match lower.literal() {
                    PrimitiveLiteral::String(path) if lower == upper => Some(path.clone()),
                    _ => None,
                }
            });
            if let Some(path) = exact_target {
                deletes_by_target
                    .entry(path)
                    .or_default()
                    .push(entry.clone());
            } else {
                shared_deletes.push(entry.clone());
            }
        }
        Ok(Self {
            snapshot_id: Some(snapshot_id),
            live_files,
            manifests,
            entries_by_manifest,
            deletes_by_target,
            shared_deletes,
        })
    }

    pub async fn current(table: &Table) -> Result<Self> {
        Self::current_with_cache(table, &ManifestCache::default()).await
    }

    pub async fn current_with_cache(table: &Table, cache: &ManifestCache) -> Result<Self> {
        match table.metadata().current_snapshot_id() {
            Some(id) => Self::load_with_cache(table, id, cache).await,
            None => Ok(Self::default()),
        }
    }

    /// Conservative applicability: missing path bounds mean the delete may
    /// apply. Workers still filter its rows by the exact referenced file path.
    pub fn applicable_deletes<'a>(&'a self, data_path: &str) -> Result<Vec<&'a ManifestEntry>> {
        let data = self
            .live_files
            .get(data_path)
            .ok_or_else(|| invalid(format!("data file is not live: {data_path}")))?;
        if data.content_type() != DataContentType::Data {
            return Err(invalid(format!("not a data file: {data_path}")));
        }
        let mut deletes: Vec<_> = self
            .deletes_by_target
            .get(data_path)
            .into_iter()
            .flatten()
            .chain(&self.shared_deletes)
            .map(AsRef::as_ref)
            .filter(|delete| position_delete_may_apply(delete, data))
            .collect();
        // A cumulative vector supersedes legacy position deletes for its target.
        if deletes
            .iter()
            .any(|entry| entry.file_format() == DataFileFormat::Puffin)
        {
            deletes.retain(|entry| entry.file_format() == DataFileFormat::Puffin);
        }
        deletes.sort_unstable_by_key(|entry| content_file_id(entry.data_file()));
        Ok(deletes)
    }
}

/// The v2 position-delete applicability rules for the supported single spec: matching partition, an
/// equal or newer data sequence, and matching file path where known. Bounds
/// are only used for exclusion, so truncated bounds remain conservative.
pub fn position_delete_may_apply(delete: &ManifestEntry, data: &ManifestEntry) -> bool {
    if delete.content_type() != DataContentType::PositionDeletes
        || data.content_type() != DataContentType::Data
        || delete.data_file.partition() != data.data_file.partition()
        || matches!((delete.sequence_number, data.sequence_number), (Some(d), Some(a)) if d < a)
    {
        return false;
    }
    if let Some(referenced) = delete.data_file.referenced_data_file() {
        return referenced == data.file_path();
    }
    let path = Datum::string(data.file_path());
    !delete
        .data_file
        .lower_bounds()
        .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)
        .is_some_and(|lower| lower > &path)
        && !delete
            .data_file
            .upper_bounds()
            .get(&RESERVED_FIELD_ID_DELETE_FILE_PATH)
            .is_some_and(|upper| upper < &path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion,
        ManifestContentType, ManifestMetadata, ManifestStatus, NestedField, PartitionSpec,
        PrimitiveType, Schema, Struct, Type,
    };

    fn bounded_file(size: usize) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/data/metrics.parquet".into())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .file_size_in_bytes(1)
            .record_count(1)
            .lower_bounds(HashMap::from([
                (1, Datum::string("x".repeat(size))),
                (2, Datum::binary(vec![0; size])),
            ]))
            .upper_bounds(HashMap::from([
                (1, Datum::string("z".repeat(size))),
                (2, Datum::binary(vec![255; size])),
            ]))
            .key_metadata(Some(vec![0; size]))
            .split_offsets(Some(vec![0; size]))
            .equality_ids(Some(vec![1; size]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn cache_releases_manifests_that_are_no_longer_read() {
        let manifest = Arc::new(Manifest::new(
            ManifestMetadata {
                schema: Arc::new(
                    Schema::builder()
                        .with_fields(vec![Arc::new(NestedField::required(
                            1,
                            "value",
                            Type::Primitive(PrimitiveType::String),
                        ))])
                        .build()
                        .unwrap(),
                ),
                schema_id: 0,
                partition_spec: PartitionSpec::unpartition_spec(),
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            },
            Vec::new(),
        ));
        let cache = ManifestCache::with_idle(1 << 20, std::time::Duration::from_secs(1));
        let current = (Uuid::nil(), "current".into(), 1, 1, None);
        let superseded = (Uuid::nil(), "superseded".into(), 1, 1, None);
        cache
            .entries
            .insert(current.clone(), manifest.clone())
            .await;
        cache.entries.insert(superseded.clone(), manifest).await;
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert!(cache.entries.get(&current).await.is_some());
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        cache.entries.run_pending_tasks().await;
        assert!(cache.entries.get(&current).await.is_some());
        assert!(cache.entries.get(&superseded).await.is_none());
        assert_eq!(cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn cache_charges_variable_metric_payloads_and_evicts_oversized_manifests() {
        let small = bounded_file(1);
        let large = bounded_file(16_384);
        assert!(data_file_weight(&large) - data_file_weight(&small) >= (16_384 - 1) * 17);
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "value",
                    Type::Primitive(PrimitiveType::String),
                ))])
                .build()
                .unwrap(),
        );
        let manifest = |file| {
            Arc::new(Manifest::new(
                ManifestMetadata {
                    schema: schema.clone(),
                    schema_id: 0,
                    partition_spec: PartitionSpec::unpartition_spec(),
                    format_version: FormatVersion::V2,
                    content: ManifestContentType::Data,
                },
                vec![
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(file)
                        .build(),
                ],
            ))
        };
        let cache = ManifestCache::new(32_768);
        let small_key = (Uuid::nil(), "small".into(), 1, 1, None);
        let large_key = (Uuid::nil(), "large".into(), 1, 1, None);
        cache
            .entries
            .insert(small_key.clone(), manifest(small))
            .await;
        cache.entries.run_pending_tasks().await;
        assert!(cache.entries.get(&small_key).await.is_some());
        cache
            .entries
            .insert(large_key.clone(), manifest(large))
            .await;
        cache.entries.run_pending_tasks().await;
        assert!(cache.entries.get(&large_key).await.is_none());
        assert!(cache.entries.weighted_size() <= 32_768);
    }
}
