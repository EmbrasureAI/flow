use super::TableMaintenance;
use crate::artifacts::OwnedArtifacts;
use crate::{blocking, publication::ReplanRequired};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{ArtifactSet, ManifestRewritePolicy, RewriteManifestsAction};
use flow_model::{OperationId, TableId};
use flow_state_store::{OperationKind, PreparedOperation};
use iceberg::table::Table;

impl TableMaintenance {
    /// Consolidate bounded manifest groups while preserving all row identities.
    /// The durable Building record owns every output path before the first PUT.
    pub async fn rewrite_manifests(
        &self,
        table: &Table,
        table_id: TableId,
        policy: &ManifestRewritePolicy,
    ) -> Result<Option<i64>> {
        let head = self.catalog.load_table(table.identifier()).await?;
        let store = self.store.clone();
        let indexed = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            indexed.pending_operation.is_none(),
            "recover pending operation before rewriting manifests"
        );
        if indexed.snapshot_id != head.metadata().current_snapshot_id() {
            return Err(ReplanRequired.into());
        }
        let id = OperationId(format!("manifest-rewrite-{}", uuid::Uuid::new_v4()));
        let Some(action) = RewriteManifestsAction::plan(&head, &id.0, policy).await? else {
            return Ok(None);
        };
        let operation = PreparedOperation {
            id: id.clone(),
            table_id,
            kind: OperationKind::ManifestRewrite,
            base_snapshot_id: indexed.snapshot_id,
            last_lsn: indexed.materialized_lsn,
            schema_version: indexed.schema_version,
            artifacts: action.artifacts(),
            payload: serde_json::to_vec(&action)?,
        };
        let ownership = OwnedArtifacts::new(
            &head,
            table_id,
            id.clone(),
            ArtifactSet {
                paths: operation.artifacts.clone(),
                ranges: Vec::new(),
            },
        )?;
        let key = ownership.key();
        let value = bincode::serialize(&ownership)?;
        let store = self.store.clone();
        let owner = operation.clone();
        blocking(move || Ok(store.begin_prepare_with_record(owner, Some((&key, &value)))?)).await?;
        action.write_artifacts(&head, &self.cache).await?;
        let store = self.store.clone();
        blocking(move || {
            Ok(store.seal_prepare(&operation.id, operation.artifacts, operation.payload)?)
        })
        .await?;
        let snapshot = self.recover(&head, &id).await?;
        let store = self.store.clone();
        blocking(move || Ok(store.forget_applied(&id)?)).await?;
        tracing::info!(
            event = "manifest_rewrite_completed",
            table_id = table_id.0,
            snapshot,
            "table manifests consolidated"
        );
        Ok(snapshot)
    }
}
