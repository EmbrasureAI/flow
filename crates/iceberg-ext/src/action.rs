use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestListWriter, ManifestWriter, ManifestWriterBuilder, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Summary,
};
use iceberg::table::Table;
use iceberg::{Catalog, Result, TableCommit, TableRequirement, TableUpdate};
use uuid::Uuid;

use crate::validation::{CommitBase, find_operation_with_key, validate_inputs, validate_rewrite};
use crate::{
    ArtifactSet, ArtifactTracker, ManifestCache, SnapshotView, conflict, content_file_id, invalid,
};

pub const OPERATION_ID_KEY: &str = "flow.operation-id";

/// Flow owns, and garbage collection may delete, only flat children of the
/// table's `metadata/` directory. A catalog that writes its JSON elsewhere
/// (for example under `write.metadata.path`) keeps ownership of that JSON.
pub fn owned_metadata_path(location: &str, path: &str) -> bool {
    path.strip_prefix(location.trim_end_matches('/'))
        .and_then(|rest| rest.strip_prefix("/metadata/"))
        .is_some_and(|name| {
            !name.is_empty()
                && name != "."
                && name != ".."
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
}

/// A successful publication, or the prior publication found during recovery.
#[derive(Debug)]
pub struct CommitResult {
    pub table: Table,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub already_committed: bool,
}

/// The result of a commit together with the exact catalog update boundary.
///
/// `catalog_update_elapsed` is `None` when validation, recovery, or artifact
/// preparation completed without calling [`Catalog::update_table`]. It remains
/// available when that call returns an error so ambiguous outcomes can be
/// diagnosed without timing unrelated manifest work.
#[derive(Debug)]
pub struct CommitAttempt {
    pub result: Result<CommitResult>,
    pub catalog_update_elapsed: Option<Duration>,
}

impl CommitAttempt {
    pub fn catalog_update_attempted(&self) -> bool {
        self.catalog_update_elapsed.is_some()
    }

    pub fn already_committed(&self) -> Option<bool> {
        self.result
            .as_ref()
            .ok()
            .map(|result| result.already_committed)
    }
}

/// Atomically add data and position-delete files in one ordinary v2 or v3 snapshot.
/// All files must be immutable, complete, and durable before calling commit.
#[derive(Debug, Clone)]
pub struct RowDeltaAction {
    base: CommitBase,
    require_base_snapshot: bool,
    operation_id: String,
    added_data: Vec<DataFile>,
    added_deletes: Vec<DataFile>,
    removed_deletes: BTreeSet<String>,
    referenced_data: BTreeSet<String>,
    properties: HashMap<String, String>,
    cache: ManifestCache,
    artifact_tracker: Option<Arc<dyn ArtifactTracker>>,
}

impl RowDeltaAction {
    pub fn new(table: &Table, operation_id: impl Into<String>) -> Self {
        Self::from_base(CommitBase::new(table), operation_id)
    }
    pub fn from_base(base: CommitBase, operation_id: impl Into<String>) -> Self {
        Self {
            base,
            require_base_snapshot: false,
            operation_id: operation_id.into(),
            added_data: Vec::new(),
            added_deletes: Vec::new(),
            removed_deletes: BTreeSet::new(),
            referenced_data: BTreeSet::new(),
            properties: HashMap::new(),
            cache: ManifestCache::default(),
            artifact_tracker: None,
        }
    }
    pub fn base(&self) -> &CommitBase {
        &self.base
    }
    /// Fence publication to the prepared snapshot. Coordinators with a
    /// physical row index must reconcile any intervening snapshot first.
    pub fn require_base_snapshot(mut self) -> Self {
        self.require_base_snapshot = true;
        self
    }
    pub fn with_artifact_tracker(mut self, tracker: Option<Arc<dyn ArtifactTracker>>) -> Self {
        self.artifact_tracker = tracker;
        self
    }
    pub fn with_manifest_cache(mut self, cache: ManifestCache) -> Self {
        self.cache = cache;
        self
    }

    pub fn add_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.added_data.extend(files);
        self
    }
    pub fn add_delete_files(mut self, files: Vec<DataFile>) -> Self {
        self.added_deletes.extend(files);
        self
    }
    /// Replace delete contents by their logical content_file_id. Replacements
    /// must commit against the exact snapshot whose deletes were read.
    pub fn remove_delete_files(mut self, files: impl IntoIterator<Item = String>) -> Self {
        self.removed_deletes.extend(files);
        if !self.removed_deletes.is_empty() {
            self.require_base_snapshot = true;
        }
        self
    }
    pub fn validate_data_files_exist(mut self, files: impl IntoIterator<Item = String>) -> Self {
        self.referenced_data.extend(files);
        self
    }
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Refresh and validate, then perform exactly one compare-and-swap catalog
    /// update. On ambiguous failure, retry this same prepared action; its marker
    /// is searched before any new files or snapshots are published.
    pub async fn commit(&self, catalog: &dyn Catalog, table: &Table) -> Result<CommitResult> {
        let head = catalog.load_table(table.identifier()).await?;
        if head.metadata().uuid() != self.base.uuid {
            return Err(conflict("table identity changed"));
        }
        if let Some(committed) = recovered(&head, &self.operation_id, OPERATION_ID_KEY) {
            return Ok(committed);
        }
        self.base.validate(&head)?;
        if self.require_base_snapshot
            && head.metadata().current_snapshot_id() != self.base.snapshot_id
        {
            return Err(conflict(
                "prepared snapshot changed; reconcile before publication",
            ));
        }
        if self.added_data.is_empty()
            && self.added_deletes.is_empty()
            && self.removed_deletes.is_empty()
        {
            return Err(invalid("empty row delta"));
        }
        if !self.added_deletes.is_empty() && self.referenced_data.is_empty() {
            return Err(invalid(
                "position deletes require referenced-data-file validation",
            ));
        }
        let view = SnapshotView::current_with_cache(&head, &self.cache).await?;
        validate_inputs(&view, &self.referenced_data, DataContentType::Data)?;
        if let Some(base_id) = self
            .base
            .snapshot_id
            .filter(|id| Some(*id) != view.snapshot_id)
            && !self.referenced_data.is_empty()
        {
            let original = SnapshotView::load_with_cache(&head, base_id, &self.cache).await?;
            validate_inputs(&original, &self.referenced_data, DataContentType::Data)?;
            for path in &self.referenced_data {
                let before = &original.live_files[path];
                let after = &view.live_files[path];
                if before.data_file != after.data_file
                    || before.sequence_number != after.sequence_number
                    || before.file_sequence_number != after.file_sequence_number
                {
                    return Err(conflict(format!("position-delete target changed: {path}")));
                }
            }
        }
        validate_inputs(
            &view,
            &self.removed_deletes,
            DataContentType::PositionDeletes,
        )?;
        validate_additions(
            &head,
            &view,
            &self.added_data,
            &self.added_deletes,
            &self.removed_deletes,
        )?;
        for file in &self.added_deletes {
            if let Some(path) = file.referenced_data_file()
                && !self.referenced_data.contains(&path)
            {
                return Err(invalid(format!("delete target was not validated: {path}")));
            }
        }
        let operation = if self.added_deletes.is_empty() && self.removed_deletes.is_empty() {
            Operation::Append
        } else if self.added_data.is_empty() {
            Operation::Delete
        } else {
            Operation::Overwrite
        };
        publish(
            catalog,
            &head,
            &view,
            Publication {
                artifact_tracker: self.artifact_tracker.as_deref(),
                operation_id: &self.operation_id,
                operation_id_key: OPERATION_ID_KEY,
                properties: &self.properties,
                operation,
                added_data: &self.added_data,
                added_deletes: &self.added_deletes,
                removed: &self.removed_deletes,
                data_sequence: -1,
                delete_sequence: -1,
            },
            None,
        )
        .await
    }
}

/// Replace physical files after validating the exact delete state the worker
/// read. The worker must apply all applicable deletes before constructing its
/// output; retaining an unconsumed shared delete is allowed.
#[derive(Debug, Clone)]
pub struct RewriteFilesAction {
    base: CommitBase,
    require_base_snapshot: bool,
    operation_id: String,
    operation_id_key: String,
    removed_data: BTreeSet<String>,
    removed_deletes: BTreeSet<String>,
    added_data: Vec<DataFile>,
    added_deletes: Vec<DataFile>,
    delete_sequence: Option<i64>,
    residual_deletes: bool,
    output_data_sequence: Option<i64>,
    properties: HashMap<String, String>,
    cache: ManifestCache,
    artifact_tracker: Option<Arc<dyn ArtifactTracker>>,
}

impl RewriteFilesAction {
    pub fn new(table: &Table, operation_id: impl Into<String>) -> Self {
        Self::from_base(CommitBase::new(table), operation_id)
    }
    pub fn from_base(base: CommitBase, operation_id: impl Into<String>) -> Self {
        Self {
            base,
            require_base_snapshot: false,
            operation_id: operation_id.into(),
            operation_id_key: OPERATION_ID_KEY.to_owned(),
            removed_data: BTreeSet::new(),
            removed_deletes: BTreeSet::new(),
            added_data: Vec::new(),
            added_deletes: Vec::new(),
            delete_sequence: None,
            residual_deletes: false,
            output_data_sequence: None,
            properties: HashMap::new(),
            cache: ManifestCache::default(),
            artifact_tracker: None,
        }
    }
    pub fn base(&self) -> &CommitBase {
        &self.base
    }
    /// Fence publication to the prepared snapshot. Coordinators with a
    /// physical row index must reconcile any intervening snapshot first.
    pub fn require_base_snapshot(mut self) -> Self {
        self.require_base_snapshot = true;
        self
    }
    /// Select a namespaced snapshot property for idempotency. Independent
    /// maintenance services should use their own key so the ingestion
    /// coordinator verifies the rewrite as external maintenance.
    pub fn with_operation_id_key(mut self, key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if !key.contains('.') || key.chars().any(char::is_whitespace) {
            return Err(invalid("operation ID property must be a namespaced key"));
        }
        self.operation_id_key = key;
        Ok(self)
    }
    pub fn with_artifact_tracker(mut self, tracker: Option<Arc<dyn ArtifactTracker>>) -> Self {
        self.artifact_tracker = tracker;
        self
    }
    pub fn with_manifest_cache(mut self, cache: ManifestCache) -> Self {
        self.cache = cache;
        self
    }
    pub fn remove_data_files(mut self, files: impl IntoIterator<Item = String>) -> Self {
        self.removed_data.extend(files);
        self
    }
    pub fn remove_delete_files(mut self, files: impl IntoIterator<Item = String>) -> Self {
        self.removed_deletes.extend(files);
        self
    }
    pub fn add_data_files(mut self, files: Vec<DataFile>) -> Self {
        self.added_data.extend(files);
        self
    }
    /// Replace position deletes without changing data files. The caller must
    /// preserve the effective union after applying each input's original data
    /// sequence, partition and path constraints. The output inherits the maximum
    /// input data sequence; the new snapshot supplies its file sequence.
    pub fn rewrite_position_deletes(mut self, files: Vec<DataFile>, sequence: i64) -> Self {
        self.added_deletes = files;
        self.delete_sequence = Some(sequence);
        self.residual_deletes = false;
        self.require_base_snapshot = true;
        self
    }
    /// Replace selected data and its delete files together. The worker must
    /// apply all input deletes to rewritten rows and preserve their effective
    /// positions for every surviving file in these residual delete outputs.
    /// Publication is fenced to the exact input snapshot.
    pub fn with_residual_deletes(mut self, files: Vec<DataFile>, sequence: i64) -> Self {
        self.added_deletes = files;
        self.delete_sequence = Some(sequence);
        self.residual_deletes = true;
        self.require_base_snapshot = true;
        self
    }
    /// Preserve the data sequence of a speculative build after validating its
    /// replacement rows and catch-up deletes at the exact publication head.
    /// This is not a rebase: the supplied base must describe the final plan.
    pub fn with_output_data_sequence(mut self, sequence: i64) -> Self {
        self.output_data_sequence = Some(sequence);
        self.require_base_snapshot = true;
        self
    }
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    pub async fn commit(&self, catalog: &dyn Catalog, table: &Table) -> Result<CommitResult> {
        self.commit_with_diagnostics(catalog, table).await.result
    }

    /// Commit while retaining whether and how long the catalog update itself ran.
    pub async fn commit_with_diagnostics(
        &self,
        catalog: &dyn Catalog,
        table: &Table,
    ) -> CommitAttempt {
        let mut catalog_update_elapsed = None;
        let result = self
            .commit_inner(catalog, table, &mut catalog_update_elapsed)
            .await;
        CommitAttempt {
            result,
            catalog_update_elapsed,
        }
    }

    async fn commit_inner(
        &self,
        catalog: &dyn Catalog,
        table: &Table,
        catalog_update_elapsed: &mut Option<Duration>,
    ) -> Result<CommitResult> {
        let head = catalog.load_table(table.identifier()).await?;
        if head.metadata().uuid() != self.base.uuid {
            return Err(conflict("table identity changed"));
        }
        if let Some(committed) = recovered(&head, &self.operation_id, &self.operation_id_key) {
            return Ok(committed);
        }
        self.base.validate(&head)?;
        if self.require_base_snapshot
            && head.metadata().current_snapshot_id() != self.base.snapshot_id
        {
            return Err(conflict(
                "prepared snapshot changed; reconcile before publication",
            ));
        }
        if let Some(sequence) = self.output_data_sequence
            && (self.removed_data.is_empty()
                || !(0..=self.base.sequence_number).contains(&sequence))
        {
            return Err(invalid(
                "output data sequence must belong to the final rewrite base",
            ));
        }
        let base_id = self
            .base
            .snapshot_id
            .ok_or_else(|| invalid("cannot rewrite an empty table"))?;
        if self.removed_data.is_empty() && self.delete_sequence.is_none() {
            return Err(invalid("a data rewrite requires input files"));
        }
        let view = SnapshotView::current_with_cache(&head, &self.cache).await?;
        let base = if view.snapshot_id == Some(base_id) {
            view.clone()
        } else {
            SnapshotView::load_with_cache(&head, base_id, &self.cache).await?
        };
        let mut removed: BTreeSet<_> = self
            .removed_data
            .union(&self.removed_deletes)
            .cloned()
            .collect();
        // Deletion vectors cannot outlive their referenced data file.
        removed.extend(
            view.live_files
                .iter()
                .filter(|(_, entry)| {
                    entry.file_format() == DataFileFormat::Puffin
                        && entry
                            .data_file()
                            .referenced_data_file()
                            .is_some_and(|path| self.removed_data.contains(&path))
                })
                .map(|(id, _)| id.clone()),
        );
        validate_additions(
            &head,
            &view,
            &self.added_data,
            &self.added_deletes,
            &removed,
        )?;
        if let Some(sequence) = self.delete_sequence {
            if self.removed_deletes.is_empty()
                || !(0..=self.base.sequence_number).contains(&sequence)
            {
                return Err(invalid(
                    "delete rewrite requires inputs with a sequence from the base snapshot",
                ));
            }
            if self.residual_deletes {
                if self.removed_data.is_empty() {
                    return Err(invalid("residual delete rewrite requires data inputs"));
                }
                // The exact-head worker supplies residuals instead of expanding
                // data inputs to every survivor covered by a shared delete.
                validate_rewrite(&base, &view, &self.removed_data, &BTreeSet::new())?;
            } else if !self.removed_data.is_empty() || !self.added_data.is_empty() {
                return Err(invalid(
                    "standalone delete rewrite cannot change data files",
                ));
            }
            validate_inputs(
                &view,
                &self.removed_deletes,
                DataContentType::PositionDeletes,
            )?;
            let maximum = self
                .removed_deletes
                .iter()
                .try_fold(None, |maximum, path| {
                    let sequence = view.live_files[path]
                        .sequence_number
                        .ok_or_else(|| invalid("delete input lacks data sequence"))?;
                    Ok::<_, iceberg::Error>(Some(
                        maximum.map_or(sequence, |value: i64| value.max(sequence)),
                    ))
                })?;
            if maximum != Some(sequence) {
                return Err(invalid(
                    "delete rewrite must preserve maximum input data sequence",
                ));
            }
        } else {
            validate_rewrite(&base, &view, &self.removed_data, &self.removed_deletes)?;
        }
        // Do not miss a delete or removal which appeared and disappeared again
        // between the worker's read and the current head.
        let mut cursor = if view.snapshot_id == Some(base_id) {
            None
        } else {
            head.metadata()
                .current_snapshot()
                .and_then(|snapshot| snapshot.parent_snapshot_id())
        };
        while let Some(id) = cursor {
            if id == base_id {
                break;
            }
            let intermediate = SnapshotView::load_with_cache(&head, id, &self.cache).await?;
            validate_rewrite(
                &base,
                &intermediate,
                &self.removed_data,
                &self.removed_deletes,
            )?;
            cursor = head
                .metadata()
                .snapshot_by_id(id)
                .and_then(|snapshot| snapshot.parent_snapshot_id());
        }
        publish(
            catalog,
            &head,
            &view,
            Publication {
                artifact_tracker: self.artifact_tracker.as_deref(),
                operation_id: &self.operation_id,
                operation_id_key: &self.operation_id_key,
                properties: &self.properties,
                operation: Operation::Replace,
                added_data: &self.added_data,
                added_deletes: &self.added_deletes,
                removed: &removed,
                data_sequence: self
                    .output_data_sequence
                    .unwrap_or(self.base.sequence_number),
                delete_sequence: self.delete_sequence.unwrap_or(-1),
            },
            Some(catalog_update_elapsed),
        )
        .await
    }
}

fn recovered(table: &Table, operation_id: &str, property: &str) -> Option<CommitResult> {
    find_operation_with_key(table.metadata(), operation_id, property).map(|snapshot| CommitResult {
        table: table.clone(),
        snapshot_id: snapshot.snapshot_id(),
        sequence_number: snapshot.sequence_number(),
        already_committed: true,
    })
}

/// Validate the resulting snapshot, including independent blobs in shared objects.
pub(crate) fn validate_live_vectors<'a>(
    files: impl IntoIterator<Item = &'a DataFile>,
) -> Result<()> {
    let files: Vec<_> = files.into_iter().collect();
    let data: HashMap<_, _> = files
        .iter()
        .filter(|file| file.content_type() == DataContentType::Data)
        .map(|file| (file.file_path(), *file))
        .collect();
    let mut targets = BTreeSet::new();
    for file in files
        .iter()
        .filter(|file| file.file_format() == DataFileFormat::Puffin)
    {
        let target = file
            .referenced_data_file()
            .ok_or_else(|| invalid("deletion vector lacks referenced data file"))?;
        let valid_range = file
            .content_offset()
            .zip(file.content_size_in_bytes())
            .is_some_and(|(offset, size)| {
                offset >= 4
                    && size > 0
                    && offset
                        .checked_add(size)
                        .is_some_and(|end| end as u64 <= file.file_size_in_bytes())
            });
        if file.content_type() != DataContentType::PositionDeletes
            || !valid_range
            || file.record_count() == 0
        {
            return Err(invalid("invalid deletion vector content or blob range"));
        }
        if !targets.insert(target.clone()) {
            return Err(invalid(format!(
                "multiple live deletion vectors for {target}"
            )));
        }
        let input = data
            .get(target.as_str())
            .ok_or_else(|| invalid(format!("deletion vector target is not live: {target}")))?;
        if input.partition() != file.partition() {
            return Err(invalid("deletion vector partition differs from target"));
        }
    }
    Ok(())
}

fn validate_additions(
    table: &Table,
    view: &SnapshotView,
    data: &[DataFile],
    deletes: &[DataFile],
    removed: &BTreeSet<String>,
) -> Result<()> {
    let mut paths = BTreeSet::new();
    for (files, content) in [
        (data, DataContentType::Data),
        (deletes, DataContentType::PositionDeletes),
    ] {
        let format = if content == DataContentType::PositionDeletes
            && table.metadata().format_version() == FormatVersion::V3
        {
            DataFileFormat::Puffin
        } else {
            DataFileFormat::Parquet
        };
        for file in files {
            if file.content_type() != content
                || file.file_format() != format
                || !file.partition().fields().is_empty()
                || file.record_count() == 0
                || file.file_size_in_bytes() == 0
                || (content == DataContentType::Data && file.first_row_id().is_some())
            {
                return Err(invalid(format!(
                    "invalid added content artifact: {}",
                    file.file_path()
                )));
            }
            let id = content_file_id(file);
            if !paths.insert(id.clone()) || view.live_files.contains_key(&id) {
                return Err(invalid(format!(
                    "duplicate added file: {}",
                    file.file_path()
                )));
            }
        }
    }
    if view
        .live_files
        .values()
        .any(|entry| entry.content_type() == DataContentType::EqualityDeletes)
    {
        return Err(conflict(
            "equality deletes require external-maintenance reconciliation",
        ));
    }
    validate_live_vectors(
        view.live_files
            .iter()
            .filter(|(id, _)| !removed.contains(*id))
            .map(|(_, entry)| entry.data_file())
            .chain(data)
            .chain(deletes),
    )
}

struct Publication<'a> {
    artifact_tracker: Option<&'a dyn ArtifactTracker>,
    operation_id: &'a str,
    operation_id_key: &'a str,
    properties: &'a HashMap<String, String>,
    operation: Operation,
    added_data: &'a [DataFile],
    added_deletes: &'a [DataFile],
    removed: &'a BTreeSet<String>,
    data_sequence: i64,
    delete_sequence: i64,
}

async fn publish(
    catalog: &dyn Catalog,
    table: &Table,
    view: &SnapshotView,
    publication: Publication<'_>,
    catalog_update_elapsed: Option<&mut Option<Duration>>,
) -> Result<CommitResult> {
    if publication.operation_id.is_empty() {
        return Err(invalid("operation ID must not be empty"));
    }
    let metadata = table.metadata();
    let nonce = Uuid::new_v4();
    let snapshot_id = (nonce.as_u64_pair().0 & i64::MAX as u64) as i64;
    if metadata.snapshot_by_id(snapshot_id).is_some() {
        return Err(invalid("snapshot ID collision; retry"));
    }
    let sequence_number = metadata.next_sequence_number();
    let root = format!(
        "{}/metadata/{nonce}",
        metadata.location().trim_end_matches('/')
    );
    let mut manifests = Vec::with_capacity(view.manifests.len() + 2);
    let mut rewritten = Vec::new();
    let mut removed_data = Vec::new();
    let mut removed_deletes = Vec::new();
    for manifest in &view.manifests {
        let entries = &view.entries_by_manifest[&manifest.manifest_path];
        let live = entries.iter().filter(|entry| entry.is_alive());
        let has_removed = live.clone().any(|entry| {
            publication
                .removed
                .contains(&content_file_id(entry.data_file()))
        });
        let has_existing = live.clone().any(|entry| {
            !publication
                .removed
                .contains(&content_file_id(entry.data_file()))
        });
        if !has_removed {
            if has_existing {
                manifests.push(manifest.clone());
            }
        } else if has_existing {
            rewritten.push((manifest, entries));
        } else {
            // Keep this snapshot's deletions in its new content manifest,
            // avoiding a separate deletion-only manifest for each input.
            // SnapshotView already requires one current, unpartitioned spec.
            match manifest.content {
                ManifestContentType::Data => removed_data.extend(live),
                ManifestContentType::Deletes => removed_deletes.extend(live),
            }
        }
    }
    if let Some(tracker) = publication.artifact_tracker {
        let count = rewritten.len()
            + usize::from(!publication.added_data.is_empty() || !removed_data.is_empty())
            + usize::from(!publication.added_deletes.is_empty() || !removed_deletes.is_empty());
        let mut artifacts = ArtifactSet::metadata(&root, count, format!("{root}-list.avro"));
        // The catalog writes the next JSON, but its current pointer is already
        // authoritative. Own it before superseding it, including lost responses
        // and retries. REST catalogs need not maintain a previous-metadata log.
        artifacts.paths.extend(
            table
                .metadata_location()
                .filter(|path| owned_metadata_path(metadata.location(), path))
                .map(str::to_owned),
        );
        tracker.register(artifacts).await?;
    }
    let mut manifest_number = 0;
    for (manifest, entries) in rewritten {
        let mut writer =
            manifest_writer(table, snapshot_id, &root, manifest_number, manifest.content)?;
        manifest_number += 1;
        for entry in entries.iter().filter(|entry| entry.is_alive()) {
            let sequence = entry
                .sequence_number
                .ok_or_else(|| invalid("missing data sequence number"))?;
            if publication
                .removed
                .contains(&content_file_id(entry.data_file()))
            {
                writer.add_delete_file(
                    entry.data_file.clone(),
                    sequence,
                    entry.file_sequence_number,
                )?;
            } else {
                writer.add_existing_file(
                    entry.data_file.clone(),
                    entry
                        .snapshot_id
                        .ok_or_else(|| invalid("missing snapshot ID"))?,
                    sequence,
                    entry.file_sequence_number,
                )?;
            }
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    for (files, removed, content, sequence) in [
        (
            publication.added_data,
            removed_data,
            ManifestContentType::Data,
            publication.data_sequence,
        ),
        (
            publication.added_deletes,
            removed_deletes,
            ManifestContentType::Deletes,
            publication.delete_sequence,
        ),
    ] {
        if files.is_empty() && removed.is_empty() {
            continue;
        }
        let mut writer = manifest_writer(table, snapshot_id, &root, manifest_number, content)?;
        manifest_number += 1;
        for file in files {
            writer.add_file(file.clone(), sequence)?;
        }
        for entry in removed {
            writer.add_delete_file(
                entry.data_file.clone(),
                entry
                    .sequence_number
                    .ok_or_else(|| invalid("missing data sequence number"))?,
                entry.file_sequence_number,
            )?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let manifest_list = format!("{root}-list.avro");
    let mut writer = manifest_list_writer(
        table,
        &manifest_list,
        snapshot_id,
        metadata.current_snapshot_id(),
        sequence_number,
    )
    .await?;
    writer.add_manifests(manifests.into_iter())?;
    let added_rows = writer
        .next_row_id()
        .map(|next| next - metadata.next_row_id());
    writer.close().await?;
    let mut collector = SnapshotSummaryCollector::default();
    for file in publication
        .added_data
        .iter()
        .chain(publication.added_deletes)
    {
        collector.add_file(
            file,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
        );
    }
    for path in publication.removed {
        collector.remove_file(
            &view.live_files[path].data_file,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
        );
    }
    let mut properties = publication.properties.clone();
    properties.insert(
        publication.operation_id_key.to_owned(),
        publication.operation_id.to_owned(),
    );
    properties.extend(collector.build());
    // Upstream's total-summary helper is private and does not accept Replace.
    // Compute physical-file totals from the validated view rather than trusting
    // optional summaries left by other writers.
    let mut totals = [0_u64; 6];
    for file in view
        .live_files
        .values()
        .filter(|entry| {
            !publication
                .removed
                .contains(&content_file_id(entry.data_file()))
        })
        .map(|entry| &entry.data_file)
        .chain(publication.added_data)
        .chain(publication.added_deletes)
    {
        let values = match file.content_type() {
            DataContentType::Data => [1, 0, file.record_count(), 0, 0, file.file_size_in_bytes()],
            DataContentType::PositionDeletes => {
                [0, 1, 0, file.record_count(), 0, file.file_size_in_bytes()]
            }
            DataContentType::EqualityDeletes => {
                [0, 1, 0, 0, file.record_count(), file.file_size_in_bytes()]
            }
        };
        for (total, value) in totals.iter_mut().zip(values) {
            *total = total
                .checked_add(value)
                .ok_or_else(|| invalid("snapshot summary overflow"))?;
        }
    }
    for (key, total) in [
        "total-data-files",
        "total-delete-files",
        "total-records",
        "total-position-deletes",
        "total-equality-deletes",
        "total-files-size",
    ]
    .into_iter()
    .zip(totals)
    {
        properties.insert(key.to_owned(), total.to_string());
    }
    let summary = Summary {
        operation: publication.operation,
        additional_properties: properties,
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before Unix epoch"))?
        .as_millis();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(i64::try_from(now).map_err(|_| invalid("timestamp overflow"))?)
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(manifest_list)
        .with_summary(summary);
    let snapshot = if let Some(added_rows) = added_rows {
        snapshot
            .with_row_range(metadata.next_row_id(), added_rows)
            .build()
    } else {
        snapshot.build()
    };
    commit_snapshot(catalog, table, snapshot, catalog_update_elapsed).await
}

pub(crate) async fn commit_snapshot(
    catalog: &dyn Catalog,
    table: &Table,
    snapshot: Snapshot,
    catalog_update_elapsed: Option<&mut Option<Duration>>,
) -> Result<CommitResult> {
    let metadata = table.metadata();
    let snapshot_id = snapshot.snapshot_id();
    let sequence_number = snapshot.sequence_number();
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_owned(),
                snapshot_id: metadata.current_snapshot_id(),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
        ])
        .updates(vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_owned(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    metadata
                        .snapshot_reference(MAIN_BRANCH)
                        .map(|reference| reference.retention.clone())
                        .unwrap_or_else(|| SnapshotRetention::branch(None, None, None)),
                ),
            },
        ])
        .build();
    let catalog_update_started = Instant::now();
    let updated = catalog.update_table(commit).await;
    if let Some(elapsed) = catalog_update_elapsed {
        *elapsed = Some(catalog_update_started.elapsed());
    }
    let table = updated?;
    Ok(CommitResult {
        table,
        snapshot_id,
        sequence_number,
        already_committed: false,
    })
}

pub(crate) fn manifest_writer(
    table: &Table,
    snapshot_id: i64,
    root: &str,
    number: usize,
    content: ManifestContentType,
) -> Result<ManifestWriter> {
    let builder = ManifestWriterBuilder::new(
        table
            .file_io()
            .new_output(format!("{root}-m{number}.avro"))?,
        Some(snapshot_id),
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    );
    Ok(match (table.metadata().format_version(), content) {
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        (_, ManifestContentType::Data) => builder.build_v2_data(),
        (_, ManifestContentType::Deletes) => builder.build_v2_deletes(),
    })
}

/// Snapshot lists assign lineage only at the publication boundary.
pub(crate) async fn manifest_list_writer(
    table: &Table,
    path: &str,
    snapshot_id: i64,
    parent: Option<i64>,
    sequence: i64,
) -> Result<ManifestListWriter> {
    let output = table.file_io().new_output(path)?.writer().await?;
    Ok(if table.metadata().format_version() == FormatVersion::V3 {
        ManifestListWriter::v3(
            output,
            snapshot_id,
            parent,
            sequence,
            Some(table.metadata().next_row_id()),
        )
    } else {
        ManifestListWriter::v2(output, snapshot_id, parent, sequence)
    })
}

#[cfg(test)]
mod tests {
    use super::owned_metadata_path;

    #[test]
    fn only_flat_children_of_the_table_metadata_directory_are_owned() {
        let location = "s3://bucket/warehouse/db/table/";
        assert!(owned_metadata_path(
            location,
            "s3://bucket/warehouse/db/table/metadata/00001-a.metadata.json"
        ));
        for path in [
            "s3://bucket/custom-metadata/00001-a.metadata.json",
            "s3://bucket/warehouse/db/table/metadata/nested/00001-a.metadata.json",
            "s3://bucket/warehouse/db/table/metadata/",
            "s3://bucket/warehouse/db/table/metadata/..",
            "s3://bucket/warehouse/db/table-other/metadata/00001-a.metadata.json",
            "s3://bucket/warehouse/db/table/data/00001-a.metadata.json",
        ] {
            assert!(!owned_metadata_path(location, path), "{path}");
        }
    }
}
