use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::spec::{
    FormatVersion, ManifestContentType, ManifestList, Operation, Snapshot, Summary,
};
use iceberg::table::Table;
use iceberg::{Catalog, Result};
use serde::{Deserialize, Serialize};

use crate::action::{commit_snapshot, manifest_list_writer, manifest_writer};
use crate::{
    CommitAttempt, CommitBase, CommitResult, ManifestCache, OPERATION_ID_KEY, conflict,
    find_operation, invalid,
};

/// Limits one metadata-maintenance operation. Output size is estimated from
/// input Avro lengths; input bytes, entries, and manifest count are hard bounds.
#[derive(Debug, Clone)]
pub struct ManifestRewritePolicy {
    pub min_manifest_count: usize,
    pub target_bytes: u64,
    pub max_input_bytes: u64,
    pub max_input_manifests: usize,
    pub max_input_entries: u64,
}

impl Default for ManifestRewritePolicy {
    fn default() -> Self {
        Self {
            min_manifest_count: 64,
            target_bytes: 8 << 20,
            max_input_bytes: 32 << 20,
            max_input_manifests: 64,
            max_input_entries: 100_000,
        }
    }
}

impl ManifestRewritePolicy {
    pub fn validate(&self) -> Result<()> {
        if self.min_manifest_count < 2
            || self.max_input_manifests < 2
            || self.target_bytes == 0
            || self.max_input_bytes < self.target_bytes
            || self.max_input_entries == 0
        {
            return Err(invalid("invalid manifest rewrite budgets"));
        }
        Ok(())
    }
}

/// An exact-base metadata rewrite. Planning performs reads only. Persist this
/// plan and register `artifacts()` before `write_artifacts`, then seal the
/// prepared operation before `commit`. Prepared replay never rewrites objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewriteManifestsAction {
    base: CommitBase,
    operation_id: String,
    snapshot_id: i64,
    sequence_number: i64,
    root: String,
    groups: Vec<Vec<String>>,
    max_input_entries: u64,
}

impl RewriteManifestsAction {
    pub async fn plan(
        table: &Table,
        operation_id: impl Into<String>,
        policy: &ManifestRewritePolicy,
    ) -> Result<Option<Self>> {
        policy.validate()?;
        let base = CommitBase::new(table);
        base.validate(table)?;
        let operation_id = operation_id.into();
        if operation_id.is_empty()
            || !operation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(invalid("invalid manifest operation ID"));
        }
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(None);
        };
        let manifests: Vec<_> = table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .consume_entries()
            .into_iter()
            .collect();
        if manifests.len() < policy.min_manifest_count {
            return Ok(None);
        }
        let mut groups = Vec::new();
        let mut input_count = 0;
        let mut input_bytes = 0_u64;
        let mut input_entries = 0_u64;
        for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
            let mut candidates: Vec<_> = manifests
                .iter()
                .filter(|manifest| manifest.content == content)
                .collect();
            candidates.sort_by(|a, b| {
                a.manifest_length
                    .cmp(&b.manifest_length)
                    .then_with(|| a.manifest_path.cmp(&b.manifest_path))
            });
            let mut group = Vec::new();
            let mut group_bytes = 0_u64;
            let mut group_entries = 0_u64;
            for manifest in candidates {
                if manifest.partition_spec_id != base.spec_id {
                    return Err(invalid(
                        "manifest rewrite requires the current unpartitioned spec",
                    ));
                }
                let bytes = u64::try_from(manifest.manifest_length)
                    .map_err(|_| invalid("negative manifest length"))?;
                let entries = manifest
                    .added_files_count
                    .zip(manifest.existing_files_count)
                    .zip(manifest.deleted_files_count)
                    .map(|((added, existing), deleted)| {
                        u64::from(added) + u64::from(existing) + u64::from(deleted)
                    })
                    .ok_or_else(|| {
                        invalid("manifest entry counts are required to bound maintenance")
                    })?;
                if bytes >= policy.target_bytes {
                    continue;
                }
                if group_bytes.saturating_add(bytes) > policy.target_bytes {
                    if group.len() > 1 {
                        groups.push(std::mem::take(&mut group));
                    } else {
                        input_count -= group.len();
                        input_bytes -= group_bytes;
                        input_entries -= group_entries;
                        group.clear();
                    }
                    group_bytes = 0;
                    group_entries = 0;
                }
                if input_count == policy.max_input_manifests
                    || input_bytes.saturating_add(bytes) > policy.max_input_bytes
                    || input_entries.saturating_add(entries) > policy.max_input_entries
                {
                    break;
                }
                group.push(manifest.manifest_path.clone());
                group_bytes += bytes;
                group_entries += entries;
                input_count += 1;
                input_bytes += bytes;
                input_entries += entries;
            }
            if group.len() > 1 {
                groups.push(group);
            } else {
                input_count -= group.len();
                input_bytes -= group_bytes;
                input_entries -= group_entries;
            }
        }
        if groups.is_empty() {
            return Ok(None);
        }
        let root = format!(
            "{}/metadata/{operation_id}",
            table.metadata().location().trim_end_matches('/')
        );
        Ok(Some(Self {
            base,
            operation_id,
            root,
            groups,
            snapshot_id: (uuid::Uuid::new_v4().as_u64_pair().0 & i64::MAX as u64) as i64,
            sequence_number: table.metadata().next_sequence_number(),
            max_input_entries: policy.max_input_entries,
        }))
    }

    pub fn base(&self) -> &CommitBase {
        &self.base
    }

    pub fn artifacts(&self) -> Vec<String> {
        (0..self.groups.len())
            .map(|index| format!("{}-m{index}.avro", self.root))
            .chain(std::iter::once(self.manifest_list()))
            .collect()
    }

    fn manifest_list(&self) -> String {
        format!("{}-list.avro", self.root)
    }

    fn validate_base(&self, table: &Table) -> Result<()> {
        self.base.validate(table)?;
        if table.metadata().current_snapshot_id() != self.base.snapshot_id
            || table.metadata().next_sequence_number() != self.sequence_number
        {
            return Err(conflict(
                "table advanced since manifest rewrite preparation",
            ));
        }
        Ok(())
    }

    pub async fn write_artifacts(&self, table: &Table, cache: &ManifestCache) -> Result<()> {
        self.validate_base(table)?;
        for path in self.artifacts() {
            if table.file_io().exists(&path).await? {
                return Err(invalid(format!(
                    "refusing to overwrite prepared artifact {path}"
                )));
            }
        }
        let snapshot = table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| invalid("manifest rewrite needs a snapshot"))?;
        let manifests: Vec<_> = table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .consume_entries()
            .into_iter()
            .collect();
        let by_path: BTreeMap<_, _> = manifests
            .iter()
            .map(|manifest| (manifest.manifest_path.as_str(), manifest))
            .collect();
        let selected: BTreeSet<_> = self.groups.iter().flatten().map(String::as_str).collect();
        if selected.len() != self.groups.iter().map(Vec::len).sum::<usize>() {
            return Err(invalid("manifest rewrite repeats an input"));
        }
        let mut output = Vec::with_capacity(self.groups.len());
        let mut entries = 0_u64;
        for (index, group) in self.groups.iter().enumerate() {
            let first = group
                .first()
                .and_then(|path| by_path.get(path.as_str()))
                .ok_or_else(|| invalid("manifest rewrite input disappeared"))?;
            let mut writer =
                manifest_writer(table, self.snapshot_id, &self.root, index, first.content)?;
            let mut live_entries = 0_u64;
            for path in group {
                let manifest = by_path
                    .get(path.as_str())
                    .ok_or_else(|| invalid("manifest rewrite input disappeared"))?;
                if manifest.content != first.content
                    || manifest.partition_spec_id != self.base.spec_id
                {
                    return Err(invalid("manifest group mixes content or partition specs"));
                }
                let loaded = cache.load(table, manifest).await?;
                entries = entries
                    .checked_add(loaded.entries().len() as u64)
                    .ok_or_else(|| invalid("manifest entry count overflow"))?;
                if entries > self.max_input_entries {
                    return Err(invalid("manifest entry budget exceeded"));
                }
                for entry in loaded.entries().iter().filter(|entry| entry.is_alive()) {
                    writer.add_existing_file(
                        entry.data_file.clone(),
                        entry
                            .snapshot_id
                            .ok_or_else(|| invalid("live manifest entry lacks a snapshot ID"))?,
                        entry
                            .sequence_number
                            .ok_or_else(|| invalid("live manifest entry lacks a data sequence"))?,
                        entry.file_sequence_number,
                    )?;
                    live_entries += 1;
                }
            }
            if live_entries > 0 {
                output.push(writer.write_manifest_file().await?);
            }
        }
        let mut writer = manifest_list_writer(
            table,
            &self.manifest_list(),
            self.snapshot_id,
            self.base.snapshot_id,
            self.sequence_number,
        )
        .await?;
        writer.add_manifests(
            manifests
                .into_iter()
                .filter(|manifest| !selected.contains(manifest.manifest_path.as_str())),
        )?;
        writer.add_manifests(output.into_iter())?;
        writer.close().await
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
        let table = &head;
        if table.metadata().uuid() != self.base.uuid {
            return Err(conflict("table identity changed"));
        }
        if let Some(snapshot) = find_operation(table.metadata(), &self.operation_id) {
            return Ok(CommitResult {
                table: table.clone(),
                snapshot_id: snapshot.snapshot_id(),
                sequence_number: snapshot.sequence_number(),
                already_committed: true,
            });
        }
        self.validate_base(table)?;
        if !table.file_io().exists(&self.manifest_list()).await? {
            return Err(invalid("prepared manifest list is missing"));
        }
        let mut properties: HashMap<String, String> = table
            .metadata()
            .current_snapshot()
            .into_iter()
            .flat_map(|snapshot| snapshot.summary().additional_properties.iter())
            .filter(|(key, _)| key.starts_with("total-"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        properties.insert(OPERATION_ID_KEY.into(), self.operation_id.clone());
        properties.insert("streaming.operation".into(), "rewrite-manifests".into());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid("clock is before Unix epoch"))?
            .as_millis();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.base.snapshot_id)
            .with_sequence_number(self.sequence_number)
            .with_timestamp_ms(i64::try_from(now).map_err(|_| invalid("timestamp overflow"))?)
            .with_schema_id(self.base.schema_id)
            .with_manifest_list(self.manifest_list())
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: properties,
            });
        let snapshot = if table.metadata().format_version() == FormatVersion::V3 {
            let first = table.metadata().next_row_id();
            let bytes = table
                .file_io()
                .new_input(self.manifest_list())?
                .read()
                .await?;
            let list = ManifestList::parse_with_version(&bytes, FormatVersion::V3)?;
            let mut next = first;
            for manifest in list.entries() {
                if let Some(start) = manifest.first_row_id.filter(|start| *start >= first) {
                    let end = start
                        .checked_add(
                            manifest
                                .added_rows_count
                                .ok_or_else(|| invalid("missing added row count"))?,
                        )
                        .and_then(|value| value.checked_add(manifest.existing_rows_count?))
                        .ok_or_else(|| {
                            invalid("manifest lineage range overflow or missing row count")
                        })?;
                    next = next.max(end);
                }
            }
            snapshot.with_row_range(first, next - first).build()
        } else {
            snapshot.build()
        };
        commit_snapshot(catalog, table, snapshot, Some(catalog_update_elapsed)).await
    }
}
