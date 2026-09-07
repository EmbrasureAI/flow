//! Classification and row-identity checks for external Iceberg snapshot changes.
//!
//! Callers poll the catalog and pass changes to [`classify`]; this crate does not
//! run a watcher. Unknown snapshots fence publication until their effect on
//! physical row identities is understood. Operation labels are hints, never proof.

use flow_model::{FileId, OperationId, PrimaryKey, RowLocation};
use flow_state_store::{IndexDelta, OperationKind, PreparedOperation, StateStore};
use std::collections::BTreeSet;
use thiserror::Error;

mod multiset;
pub use multiset::match_append_only_rows;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOperation {
    Append,
    Overwrite,
    Replace,
    Delete,
}

#[derive(Debug, Clone)]
pub struct SnapshotChange {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub operation: SnapshotOperation,
    pub service_operation: Option<OperationId>,
    pub added_data: BTreeSet<FileId>,
    pub removed_data: BTreeSet<FileId>,
    pub added_deletes: BTreeSet<FileId>,
    pub removed_deletes: BTreeSet<FileId>,
    pub schema_changed: bool,
    pub spec_changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    /// A known prepared operation still requires its durable index transition.
    ServiceOperation,
    MetadataOnly,
    VerifyDeleteRewrite,
    ReconcileDataRewrite,
    Pause(PauseReason),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseReason {
    MissingHistory,
    SchemaOrSpecChanged,
    ExternalLogicalMutation,
    UnknownServiceOperation,
}

pub fn classify(
    change: &SnapshotChange,
    expected_parent: Option<i64>,
    known_service_operation: bool,
) -> Classification {
    if change.parent_snapshot_id != expected_parent {
        return Classification::Pause(PauseReason::MissingHistory);
    }
    if change.schema_changed || change.spec_changed {
        return Classification::Pause(PauseReason::SchemaOrSpecChanged);
    }
    if change.service_operation.is_some() {
        return if known_service_operation {
            Classification::ServiceOperation
        } else {
            Classification::Pause(PauseReason::UnknownServiceOperation)
        };
    }
    let data_changed = !change.added_data.is_empty() || !change.removed_data.is_empty();
    let deletes_changed = !change.added_deletes.is_empty() || !change.removed_deletes.is_empty();
    if !data_changed {
        return if !deletes_changed {
            Classification::MetadataOnly
        } else if change.operation == SnapshotOperation::Replace {
            Classification::VerifyDeleteRewrite
        } else {
            Classification::Pause(PauseReason::ExternalLogicalMutation)
        };
    }
    if change.operation == SnapshotOperation::Replace && !change.removed_data.is_empty() {
        // All-deleted input files may legitimately produce zero output files.
        Classification::ReconcileDataRewrite
    } else {
        Classification::Pause(PauseReason::ExternalLogicalMutation)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    State(#[from] flow_state_store::Error),
    #[error("external rewrite changed logical data: {0}")]
    LogicalMutation(&'static str),
    #[error("invalid reconciliation request: {0}")]
    Invalid(&'static str),
}
pub type Result<T> = std::result::Result<T, Error>;

/// Compare canonical, sorted effective delete sets. The caller must apply each
/// delete file's sequence/partition rules before producing this stream. Adjacent
/// duplicate positions are harmless and ignored; unsorted input is rejected.
pub fn validate_delete_rewrite(
    before: impl IntoIterator<Item = (FileId, u64)>,
    after: impl IntoIterator<Item = (FileId, u64)>,
) -> Result<()> {
    validate_delete_rewrite_fallible(before.into_iter().map(Ok), after.into_iter().map(Ok))
}

pub fn validate_delete_rewrite_fallible(
    before: impl IntoIterator<Item = flow_state_store::Result<(FileId, u64)>>,
    after: impl IntoIterator<Item = flow_state_store::Result<(FileId, u64)>>,
) -> Result<()> {
    let mut before = SortedSet::new(before.into_iter());
    let mut after = SortedSet::new(after.into_iter());
    loop {
        let left = before.next()?;
        let right = after.next()?;
        if left != right {
            return Err(Error::LogicalMutation(
                "effective position-delete set differs",
            ));
        }
        if left.is_none() {
            return Ok(());
        }
    }
}

struct SortedSet<I> {
    iter: I,
    previous: Option<(FileId, u64)>,
}
impl<I: Iterator<Item = flow_state_store::Result<(FileId, u64)>>> SortedSet<I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            previous: None,
        }
    }
    fn next(&mut self) -> Result<Option<(FileId, u64)>> {
        for item in self.iter.by_ref() {
            let item = item?;
            if let Some(previous) = &self.previous {
                if &item < previous {
                    return Err(Error::Invalid("delete positions are not sorted"));
                }
                if &item == previous {
                    continue;
                }
            }
            self.previous = Some(item.clone());
            return Ok(Some(item));
        }
        Ok(None)
    }
}

/// A disk-backed validation join. Added rows are streamed through the existing
/// forward index, with uniqueness checked by prepared delta keys. Nothing is
/// applied until cardinality, key membership, and fingerprints all agree.
///
/// `added_rows` must contain only effective live rows, with all currently
/// applicable position deletes applied. The caller refreshes the catalog again
/// before resuming publication. Any error intentionally leaves the table fenced.
pub fn reconcile_rewrite(
    store: &StateStore,
    operation: PreparedOperation,
    committed_snapshot_id: i64,
    committed_sequence_number: i64,
    removed_files: &BTreeSet<FileId>,
    added_files: &BTreeSet<FileId>,
    added_rows: impl IntoIterator<Item = flow_state_store::Result<(PrimaryKey, RowLocation)>>,
) -> Result<()> {
    if operation.kind != OperationKind::Reconcile || removed_files.is_empty() {
        return Err(Error::Invalid(
            "reconciliation requires removed data files and a Reconcile operation",
        ));
    }
    let id = operation.id.clone();
    let table_id = operation.table_id;
    let expected_lsn = store.table_state(&table_id)?.materialized_lsn;
    if operation.last_lsn != expected_lsn {
        return Err(Error::Invalid("physical rewrite cannot advance source LSN"));
    }
    let artifacts = operation.artifacts.clone();
    let payload = operation.payload.clone();
    store.begin_prepare(operation)?;
    let mut expected_rows = 0u64;
    for file in removed_files {
        for item in store.file_rows(&table_id, file) {
            let (position, key) = item?;
            let current = store
                .lookup(&table_id, &key)?
                .ok_or(Error::LogicalMutation("reverse index has no live key"))?;
            if &current.data_file_id != file || current.row_position != position {
                return Err(Error::LogicalMutation("forward and reverse index disagree"));
            }
            expected_rows += 1;
        }
    }
    let mut seen_rows = 0u64;
    let mut validation_error = None;
    let deltas = added_rows.into_iter().map(|item| {
        let (key, mut replacement) = item?;
        let expected = store.lookup(&table_id, &key)?.ok_or_else(|| {
            flow_state_store::Error::InvalidState("external rewrite introduced a new key".into())
        })?;
        if !removed_files.contains(&expected.data_file_id)
            || !added_files.contains(&replacement.data_file_id)
            || replacement.row_fingerprint != expected.row_fingerprint
        {
            validation_error = Some("added key, output file, or row fingerprint differs");
            return Err(flow_state_store::Error::InvalidState(
                "external rewrite changed logical state".into(),
            ));
        }
        replacement.source_commit_lsn = expected.source_commit_lsn;
        replacement.row_version = expected.row_version;
        seen_rows += 1;
        Ok(IndexDelta {
            key,
            expected: Some(expected),
            replacement: Some(replacement),
        })
    });
    let staged = store.stage_deltas_fallible(&id, deltas);
    if let Some(reason) = validation_error {
        return Err(Error::LogicalMutation(reason));
    }
    staged?;
    if expected_rows != seen_rows {
        return Err(Error::LogicalMutation("live key cardinality differs"));
    }
    store.seal_prepare(&id, artifacts, payload)?;
    store.mark_committed(&id, committed_snapshot_id, committed_sequence_number)?;
    store.apply_committed(&id)?;
    Ok(())
}
