use std::collections::BTreeSet;

use iceberg::Result;
use iceberg::spec::{DataContentType, FormatVersion, SnapshotRef, TableMetadata};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{OPERATION_ID_KEY, SnapshotView, conflict, content_file_id, invalid};

/// Find a committed operation on the current branch, including retained parent
/// snapshots. A marker on a detached branch is not a successful publication.
/// Absence is inconclusive when the action's base has expired; commit validation
/// separately rejects that case rather than blindly replaying the operation.
pub fn find_operation(metadata: &TableMetadata, operation_id: &str) -> Option<SnapshotRef> {
    find_operation_with_key(metadata, operation_id, OPERATION_ID_KEY)
}

pub(crate) fn find_operation_with_key(
    metadata: &TableMetadata,
    operation_id: &str,
    property: &str,
) -> Option<SnapshotRef> {
    let mut cursor = metadata.current_snapshot_id();
    let mut seen = BTreeSet::new();
    while let Some(id) = cursor {
        if !seen.insert(id) {
            return None;
        }
        let snapshot = metadata.snapshot_by_id(id)?;
        if snapshot
            .summary()
            .additional_properties
            .get(property)
            .is_some_and(|id| id == operation_id)
        {
            return Some(snapshot.clone());
        }
        cursor = snapshot.parent_snapshot_id();
    }
    None
}

/// Durable validation context captured before reading input rows or creating
/// output artifacts. Persist this alongside a prepared artifact plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitBase {
    pub uuid: Uuid,
    #[serde(default = "legacy_format_version")]
    pub format_version: FormatVersion,
    pub snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub schema_id: i32,
    pub spec_id: i32,
}

fn legacy_format_version() -> FormatVersion {
    FormatVersion::V2
}

impl CommitBase {
    pub fn new(table: &Table) -> Self {
        let metadata = table.metadata();
        Self {
            uuid: metadata.uuid(),
            format_version: metadata.format_version(),
            snapshot_id: metadata.current_snapshot_id(),
            sequence_number: metadata
                .current_snapshot()
                .map_or(0, |snapshot| snapshot.sequence_number()),
            schema_id: metadata.current_schema_id(),
            spec_id: metadata.default_partition_spec_id(),
        }
    }

    pub fn validate(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        if metadata.uuid() != self.uuid {
            return Err(conflict("table identity changed"));
        }
        if metadata.format_version() != self.format_version {
            return Err(conflict("table format changed since files were prepared"));
        }
        if !matches!(
            metadata.format_version(),
            FormatVersion::V2 | FormatVersion::V3
        ) {
            return Err(invalid("row deltas and rewrites require Iceberg v2 or v3"));
        }
        if !metadata.default_partition_spec().fields().is_empty() {
            return Err(invalid("only unpartitioned tables are supported"));
        }
        if metadata.table_properties()?.encryption_key_id.is_some() {
            return Err(invalid("encrypted writes are not supported"));
        }
        if metadata.current_schema_id() != self.schema_id
            || metadata.default_partition_spec_id() != self.spec_id
        {
            return Err(conflict(
                "schema or partition spec changed since files were prepared",
            ));
        }
        if self
            .snapshot_id
            .is_some_and(|id| metadata.snapshot_by_id(id).is_none())
        {
            return Err(conflict(
                "base snapshot expired; cannot safely replay prepared operation",
            ));
        }
        let mut cursor = metadata.current_snapshot_id();
        let mut seen = BTreeSet::new();
        loop {
            if cursor == self.snapshot_id {
                return Ok(());
            }
            let Some(id) = cursor else {
                return Err(conflict("base snapshot is not an ancestor of current head"));
            };
            if !seen.insert(id) {
                return Err(invalid("cycle in snapshot ancestry"));
            }
            cursor = metadata
                .snapshot_by_id(id)
                .ok_or_else(|| conflict("snapshot history expired; cannot prove base ancestry"))?
                .parent_snapshot_id();
        }
    }
}

pub(crate) fn validate_inputs(
    view: &SnapshotView,
    files: &BTreeSet<String>,
    content: DataContentType,
) -> Result<()> {
    for path in files {
        let entry = view
            .live_files
            .get(path)
            .ok_or_else(|| conflict(format!("input file no longer exists: {path}")))?;
        if entry.content_type() != content {
            return Err(invalid(format!("incorrect input content type: {path}")));
        }
    }
    Ok(())
}

pub(crate) fn validate_rewrite(
    base: &SnapshotView,
    head: &SnapshotView,
    data: &BTreeSet<String>,
    deletes: &BTreeSet<String>,
) -> Result<()> {
    validate_inputs(base, data, DataContentType::Data)?;
    validate_inputs(head, data, DataContentType::Data)?;
    validate_inputs(base, deletes, DataContentType::PositionDeletes)?;
    validate_inputs(head, deletes, DataContentType::PositionDeletes)?;

    // A new delete is unsafe even if its file was removed again before the
    // current head. The ancestry check in the action additionally inspects
    // intermediate snapshots; here we check the exact currently live state.
    for path in data {
        let old = &base.live_files[path];
        let current = &head.live_files[path];
        if old.data_file != current.data_file
            || old.sequence_number != current.sequence_number
            || old.file_sequence_number != current.file_sequence_number
        {
            return Err(conflict(format!("rewrite input changed: {path}")));
        }
        let old_deletes = base.applicable_deletes(path)?;
        let current_deletes = head.applicable_deletes(path)?;
        if old_deletes.len() != current_deletes.len()
            || old_deletes.iter().zip(current_deletes).any(|(old, new)| {
                old.data_file != new.data_file
                    || old.sequence_number != new.sequence_number
                    || old.file_sequence_number != new.file_sequence_number
            })
        {
            return Err(conflict(format!(
                "applicable delete state changed for {path}; replan rewrite"
            )));
        }
    }
    if deletes.is_empty() {
        return Ok(());
    }
    for input in head.live_files.values().filter(|input| {
        input.content_type() == DataContentType::Data && !data.contains(input.file_path())
    }) {
        // A shared legacy delete may be removed once every surviving target
        // is covered by its cumulative vector.
        for delete in head.applicable_deletes(input.file_path())? {
            let id = content_file_id(delete.data_file());
            if deletes.contains(&id) {
                return Err(conflict(format!(
                    "shared delete still covers a surviving data file: {id}"
                )));
            }
        }
    }
    Ok(())
}
