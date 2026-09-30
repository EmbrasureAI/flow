//! Source-wide acknowledgement and per-table publication coordination.
//!
//! [`SourceLedger`] advances acknowledgements over durable source prefixes.
//! [`collapse_epoch`] binds a table prefix to its schema and index head before
//! [`TablePublisher`] publishes it. [`TableMaintenance`] manages native rewrites
//! and reconciles external catalog changes against that same index.
//!
//! Callers must serialize publication, reconciliation, and maintenance activation
//! for each table. Speculative compaction workers retain explicit ownership until
//! the table actor joins them; a timeout alone does not release their artifacts.

use anyhow::Result;

mod artifacts;
mod ledger;
mod maintenance;
mod publication;
mod scheduler;
mod schema;

pub use artifacts::{import_catalog_metadata, register_catalog_metadata};
pub use ledger::{AckMode, JournalDurability, SourceLedger, Watermarks};
pub use maintenance::{
    BuildRegistration, DeleteRepairCursor, DeleteRewritePolicy, GarbagePolicy, GarbageProtection,
    GarbageReport, HistoryPolicy, Inventory, PreparationWait, PreparedCompaction, ReadyCompaction,
    RetiringCompactionPreparation, RunningCompaction, RunningCompactionPreparation,
    TableMaintenance, active_build_protection, discard_abandoned_builds,
};
pub use publication::{
    CollapseLimits, CollapsedEpoch, Epoch, ReplanRequired, TablePublisher, collapse_epoch,
    resolve_catalog_operation,
};
pub use scheduler::{Priority, Scheduler, SourceHealth, WalPressure};
pub use schema::{
    SourceSchemaRecord, latest_source_schema, load_control_schema, load_source_schema,
    reconcile_index_schema, same_iceberg_schema, source_schema_key, source_schema_prefix,
    store_source_schema,
};

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await?
}
