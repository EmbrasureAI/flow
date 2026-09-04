//! Source-wide acknowledgement and per-table publication coordination.
mod ledger;
mod scheduler;
pub use ledger::{AckMode, JournalDurability, SourceLedger, Watermarks};
pub use scheduler::{Priority, Scheduler, SourceHealth, WalPressure};
mod publication;
pub use publication::{
    CollapseLimits, CollapsedEpoch, Epoch, ReplanRequired, TablePublisher, collapse_epoch,
    resolve_catalog_operation,
};

mod maintenance;
pub use maintenance::{
    BuildRegistration, DeleteRepairCursor, DeleteRewritePolicy, GarbagePolicy, GarbageProtection,
    GarbageReport, Inventory, PreparationWait, PreparedCompaction, ReadyCompaction,
    RetiringCompactionPreparation, RunningCompaction, RunningCompactionPreparation,
    TableMaintenance, active_build_protection, discard_abandoned_builds,
};

mod schema;
pub use schema::{
    SourceSchemaRecord, latest_source_schema, load_control_schema, load_source_schema,
    reconcile_index_schema, same_iceberg_schema, source_schema_key, source_schema_prefix,
    store_source_schema,
};
