use super::{ReconcileRecovery, RewriteRecovery};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{
    ArtifactTracker, CommitBase, ManifestCache, RewriteFilesAction, RewriteManifestsAction,
    find_operation, read_artifact_plan,
};
use flow_model::OperationId;
use flow_state_store::{OperationKind, OperationPhase, OperationRecord};
use iceberg::{Catalog, table::Table};
use std::{sync::Arc, time::Duration};

#[derive(Debug)]
pub(super) struct MaintenanceResolution {
    pub outcome: Option<(i64, i64)>,
    pub path: &'static str,
    pub catalog_already_committed: bool,
}

impl MaintenanceResolution {
    fn new(
        outcome: Option<(i64, i64)>,
        path: &'static str,
        catalog_already_committed: bool,
    ) -> Self {
        Self {
            outcome,
            path,
            catalog_already_committed,
        }
    }
}

#[derive(Debug)]
pub(super) struct MaintenanceResolutionAttempt {
    pub result: Result<MaintenanceResolution>,
    pub catalog_update_elapsed: Option<Duration>,
}

enum RewritePlan {
    Data(RewriteRecovery),
    Manifests(RewriteManifestsAction),
}

impl RewritePlan {
    fn base(&self) -> &CommitBase {
        match self {
            Self::Data(plan) => &plan.base,
            Self::Manifests(plan) => plan.base(),
        }
    }

    async fn commit(
        self,
        catalog: &dyn Catalog,
        head: &Table,
        id: &OperationId,
        cache: ManifestCache,
        tracker: Option<Arc<dyn ArtifactTracker>>,
        elapsed: &mut Option<Duration>,
    ) -> Result<flow_iceberg_ext::CommitResult> {
        match self {
            Self::Manifests(plan) => {
                let attempt = plan.commit_with_diagnostics(catalog, head).await;
                *elapsed = attempt.catalog_update_elapsed;
                Ok(attempt.result?)
            }
            Self::Data(plan) => {
                let files = read_artifact_plan(head.file_io(), &plan.manifest_list).await?;
                let rewrites_data = !plan.removed_data.is_empty();
                let mut action = RewriteFilesAction::from_base(plan.base, id.0.clone())
                    .require_base_snapshot()
                    .remove_data_files(plan.removed_data)
                    .remove_delete_files(plan.removed_deletes)
                    .add_data_files(files.added_data)
                    .with_properties(plan.properties)
                    .with_manifest_cache(cache)
                    .with_artifact_tracker(tracker);
                if let Some(sequence) = plan.output_data_sequence {
                    action = action.with_output_data_sequence(sequence);
                }
                if let Some(sequence) = plan.delete_sequence {
                    action = if rewrites_data {
                        action.with_residual_deletes(files.added_deletes, sequence)
                    } else {
                        action.rewrite_position_deletes(files.added_deletes, sequence)
                    };
                } else {
                    ensure!(
                        files.added_deletes.is_empty(),
                        "data rewrite output contains deletes"
                    );
                }
                let attempt = action.commit_with_diagnostics(catalog, head).await;
                *elapsed = attempt.catalog_update_elapsed;
                Ok(attempt.result?)
            }
        }
    }
}

/// Resolve catalog authority without relying on a surviving row-index generation.
/// An advanced exact base rules out a late commit; an unchanged ambiguous base
/// remains fenced until replay proves the outcome.
pub(crate) async fn resolve_maintenance_catalog_operation(
    record: &OperationRecord,
    catalog: &dyn Catalog,
    table: &Table,
    tracker: Option<Arc<dyn ArtifactTracker>>,
) -> Result<Option<(i64, i64)>> {
    Ok(
        resolve_maintenance_operation(record, catalog, table, ManifestCache::default(), tracker)
            .await
            .result?
            .outcome,
    )
}

pub(super) async fn resolve_maintenance_operation(
    record: &OperationRecord,
    catalog: &dyn Catalog,
    table: &Table,
    cache: ManifestCache,
    tracker: Option<Arc<dyn ArtifactTracker>>,
) -> MaintenanceResolutionAttempt {
    let mut catalog_update_elapsed = None;
    let result = resolve_maintenance_operation_inner(
        record,
        catalog,
        table,
        cache,
        tracker,
        &mut catalog_update_elapsed,
    )
    .await;
    MaintenanceResolutionAttempt {
        result,
        catalog_update_elapsed,
    }
}

async fn resolve_maintenance_operation_inner(
    record: &OperationRecord,
    catalog: &dyn Catalog,
    table: &Table,
    cache: ManifestCache,
    tracker: Option<Arc<dyn ArtifactTracker>>,
    catalog_update_elapsed: &mut Option<Duration>,
) -> Result<MaintenanceResolution> {
    ensure!(
        matches!(
            record.operation.kind,
            OperationKind::Rewrite | OperationKind::Reconcile | OperationKind::ManifestRewrite
        ),
        "operation is not maintenance"
    );
    if record.phase == OperationPhase::Building {
        return Ok(MaintenanceResolution::new(None, "building", false));
    }
    let head = catalog.load_table(table.identifier()).await?;
    if record.operation.kind == OperationKind::Reconcile {
        let plan: ReconcileRecovery = serde_json::from_slice(&record.operation.payload)?;
        if matches!(
            record.phase,
            OperationPhase::Committed | OperationPhase::Applied
        ) {
            ensure!(
                record.snapshot_id == Some(plan.snapshot_id)
                    && record.sequence_number == Some(plan.sequence_number),
                "committed reconciliation differs from its verified target"
            );
        }
        verify_snapshot(
            &head,
            plan.table_uuid,
            plan.snapshot_id,
            plan.sequence_number,
        )?;
        return Ok(MaintenanceResolution::new(
            Some((plan.snapshot_id, plan.sequence_number)),
            "reconcile_verified",
            true,
        ));
    }
    let plan = if record.operation.kind == OperationKind::ManifestRewrite {
        RewritePlan::Manifests(serde_json::from_slice(&record.operation.payload)?)
    } else {
        RewritePlan::Data(serde_json::from_slice(&record.operation.payload)?)
    };
    let base = plan.base().clone();
    ensure!(
        head.metadata().uuid() == base.uuid,
        "table identity changed during rewrite recovery"
    );
    if matches!(
        record.phase,
        OperationPhase::Committed | OperationPhase::Applied
    ) {
        let snapshot = record
            .snapshot_id
            .ok_or_else(|| anyhow::anyhow!("committed operation lacks a snapshot"))?;
        let sequence = record
            .sequence_number
            .ok_or_else(|| anyhow::anyhow!("committed operation lacks a sequence number"))?;
        verify_snapshot(&head, base.uuid, snapshot, sequence)?;
        return Ok(MaintenanceResolution::new(
            Some((snapshot, sequence)),
            "committed_verified",
            true,
        ));
    }
    if let Some(snapshot) = find_operation(head.metadata(), &record.operation.id.0) {
        return Ok(MaintenanceResolution::new(
            Some((snapshot.snapshot_id(), snapshot.sequence_number())),
            "operation_found",
            true,
        ));
    }
    if head.metadata().format_version() != base.format_version {
        return Ok(MaintenanceResolution::new(None, "format_changed", false));
    }
    base.validate(&head)?;
    if head.metadata().current_snapshot_id() != base.snapshot_id {
        return Ok(MaintenanceResolution::new(None, "base_changed", false));
    }
    match plan
        .commit(
            catalog,
            &head,
            &record.operation.id,
            cache,
            tracker,
            catalog_update_elapsed,
        )
        .await
    {
        Ok(committed) => Ok(MaintenanceResolution {
            outcome: Some((committed.snapshot_id, committed.sequence_number)),
            path: if committed.already_committed {
                "action_operation_found"
            } else {
                "catalog_updated"
            },
            catalog_already_committed: committed.already_committed,
        }),
        Err(error) => {
            let refreshed = catalog.load_table(table.identifier()).await?;
            ensure!(
                refreshed.metadata().uuid() == base.uuid,
                "table identity changed during rewrite recovery"
            );
            if let Some(snapshot) = find_operation(refreshed.metadata(), &record.operation.id.0) {
                return Ok(MaintenanceResolution {
                    outcome: Some((snapshot.snapshot_id(), snapshot.sequence_number())),
                    path: "commit_error_operation_found",
                    catalog_already_committed: true,
                });
            }
            if refreshed.metadata().format_version() != base.format_version {
                return Ok(MaintenanceResolution::new(
                    None,
                    "commit_error_format_changed",
                    false,
                ));
            }
            base.validate(&refreshed)?;
            if refreshed.metadata().current_snapshot_id() != base.snapshot_id {
                Ok(MaintenanceResolution {
                    outcome: None,
                    path: "commit_error_base_changed",
                    catalog_already_committed: false,
                })
            } else {
                Err(error)
            }
        }
    }
}

fn verify_snapshot(table: &Table, uuid: uuid::Uuid, snapshot_id: i64, sequence: i64) -> Result<()> {
    ensure!(
        table.metadata().uuid() == uuid,
        "table identity changed during maintenance recovery"
    );
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| {
            anyhow::anyhow!("maintenance snapshot expired; cannot prove its ancestry")
        })?;
    ensure!(
        snapshot.sequence_number() == sequence,
        "maintenance snapshot sequence changed"
    );
    let mut base = CommitBase::new(table);
    base.snapshot_id = Some(snapshot_id);
    base.sequence_number = sequence;
    base.validate(table)?;
    Ok(())
}
