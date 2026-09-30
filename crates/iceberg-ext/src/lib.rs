//! Validated Iceberg v2/v3 row deltas and physical rewrites.
//!
//! All artifacts and catalog updates use Apache Iceberg's public Rust types.
//! The extension is separate because upstream 0.10.1's transaction action trait
//! is private. Catalog conflicts are returned to the coordinator: an action
//! must be validated again against a refreshed head before another attempt.

mod action;
mod artifacts;
mod identity;
mod manifests;
mod plan;
mod snapshot;
mod validation;

pub use identity::{content_file_id, delete_content_size, physical_file_stats};

pub use action::{
    CommitAttempt, CommitResult, OPERATION_ID_KEY, RewriteFilesAction, RowDeltaAction,
    owned_metadata_path,
};
pub use artifacts::{ArtifactSet, ArtifactTracker, NumberedArtifacts, retained_artifacts};
pub use manifests::{ManifestRewritePolicy, RewriteManifestsAction};
pub use plan::{ArtifactPlan, read_artifact_plan, write_artifact_plan};
pub use snapshot::{ManifestCache, SnapshotView, position_delete_may_apply};
pub use validation::{CommitBase, find_operation};

fn invalid(message: impl Into<String>) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, message)
}

fn conflict(message: impl Into<String>) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::PreconditionFailed, message)
}
