//! Local speculative data compaction. The actor captures the base and later
//! finalizes completed work; it never waits for a running worker under its fence.

use super::{BuildRegistration, RewriteCommit, TableMaintenance};
use crate::publication::{ReplanRequired, blocking};
use anyhow::{Result, ensure};
use flow_compactor::{BuiltData, CatchUpRejected, Level};
use flow_iceberg_ext::CommitBase;
use flow_materializer::iceberg_schema;
use flow_model::{OperationId, TableSchema};
use flow_state_store::{OperationKind, OperationPhase, PreparedOperation, StateStore, TableState};
use iceberg::table::Table;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const MAX_BUILD_TIME: Duration = Duration::from_secs(30);

/// Await outside the table actor's busy slot. Dropping this handle intentionally
/// leaves durable ownership for restart cleanup; it cannot stop blocking I/O.
#[must_use = "join the worker before releasing its scratch directory"]
pub struct RunningCompaction {
    worker: tokio::task::JoinHandle<Result<BuiltData>>,
    ownership: BuildRegistration,
    cancelled: Arc<AtomicBool>,
    base: CommitBase,
    indexed: TableState,
    started: Instant,
}

impl RunningCompaction {
    pub async fn wait(mut self) -> Result<ReadyCompaction> {
        let remaining = MAX_BUILD_TIME.saturating_sub(self.started.elapsed());
        let outcome = match tokio::time::timeout(remaining, &mut self.worker).await {
            Ok(outcome) => outcome
                .map_err(anyhow::Error::from)
                .and_then(|result| result),
            Err(_) => {
                self.cancelled.store(true, Ordering::Relaxed);
                let ownership = self.ownership.verify_owned().await;
                tracing::info!(event = "compaction_build_cancellation_requested",
                    operation_id = %self.ownership.operation_id().0,
                    elapsed_ms = self.started.elapsed().as_millis(),
                    durable_ownership_retained = ownership.is_ok(),
                    "compaction deadline reached; waiting for the worker to join");
                // A timeout is not cancellation of spawn_blocking. Keep the
                // snapshot and every output protected until the actual join.
                // Even a failed ownership read must not skip this barrier.
                let _ = self.worker.await;
                ownership.and(Err(ReplanRequired.into()))
            }
        };
        match outcome {
            Ok(built) => Ok(ReadyCompaction {
                built,
                ownership: self.ownership,
                base: self.base,
                indexed: self.indexed,
                worker_ms: self.started.elapsed().as_secs_f64() * 1000.0,
            }),
            Err(error) => {
                self.ownership.release_after_join().await?;
                tracing::info!(event = "compaction_build_retired",
                    operation_id = %self.ownership.operation_id().0,
                    elapsed_ms = self.started.elapsed().as_millis(),
                    "joined compaction worker and released durable build ownership");
                Err(replan_error(error))
            }
        }
    }
}

/// A joined worker, safe to finalize or discard while the table actor is idle.
#[must_use = "finalize or discard the completed compaction"]
pub struct ReadyCompaction {
    built: BuiltData,
    ownership: BuildRegistration,
    base: CommitBase,
    indexed: TableState,
    worker_ms: f64,
}

impl ReadyCompaction {
    pub fn operation_id(&self) -> &OperationId {
        self.ownership.operation_id()
    }

    pub async fn discard(self) -> Result<()> {
        self.ownership.release_after_join().await
    }
}

impl TableMaintenance {
    /// Plan at an indexed head and return only after the worker has acquired its
    /// frozen index read view. Subsequent CDC may then advance the table.
    pub async fn start_compaction(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
    ) -> Result<Option<RunningCompaction>> {
        let table = self.catalog.load_table(table.identifier()).await?;
        ensure!(
            crate::same_iceberg_schema(table.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema differs from Iceberg"
        );
        let Some(snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(None);
        };
        let inventory = self.inventory(&table, schema.table_id).await?;
        let Some(plan) = self.policy.plan(
            snapshot,
            table.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )?
        else {
            return Ok(None);
        };
        let level = match plan.output_level {
            Level::L0 => "l0",
            Level::L1 => "l1",
            Level::L2 => "l2",
        };
        let operation = OperationId(format!("flow-{level}-rewrite-{}", uuid::Uuid::new_v4()));
        let base = CommitBase::new(&table);
        let ownership = BuildRegistration::begin(
            self.store.clone(),
            &table,
            schema.table_id,
            operation.clone(),
        )
        .await?;
        let store = self.store.clone();
        let schema = schema.clone();
        let mut writer = self.writer.clone();
        writer.artifact_tracker = Some(ownership.artifact_tracker());
        let limits = self.read_limits.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = cancelled.clone();
        let (captured, ready) = tokio::sync::oneshot::channel();
        let started = Instant::now();
        let worker = tokio::task::spawn_blocking(move || {
            let index = store.index_snapshot(&schema.table_id, Some(snapshot))?;
            ensure!(
                index.table_state().schema_version == schema.version,
                "build index schema does not match source schema"
            );
            captured
                .send(index.table_state().clone())
                .map_err(|_| ReplanRequired)?;
            super::worker_runtime()?.block_on(flow_compactor::build_data(
                &table,
                schema,
                plan,
                operation,
                &index,
                scratch,
                writer,
                &limits,
                &worker_cancelled,
            ))
        });
        match ready.await {
            Ok(indexed) => {
                tracing::info!(event = "compaction_build_started", operation_id = %ownership.operation_id().0,
                    base_snapshot_id = snapshot, materialized_lsn = %indexed.materialized_lsn,
                    "captured speculative compaction index snapshot");
                Ok(Some(RunningCompaction {
                    worker,
                    ownership,
                    cancelled,
                    base,
                    indexed,
                    started,
                }))
            }
            Err(_) => {
                let result = worker.await;
                ownership.release_after_join().await?;
                match result {
                    Ok(Err(error)) => Err(replan_error(error)),
                    Err(error) => Err(error.into()),
                    Ok(Ok(_)) => Err(anyhow::anyhow!(
                        "worker omitted snapshot capture acknowledgement"
                    )),
                }
            }
        }
    }

    /// Finish only an already joined worker while the owning table actor is
    /// idle. Catch-up uses the ordinary durable fence and exact-head recovery.
    pub async fn finish_compaction(
        &self,
        table: &Table,
        schema: &TableSchema,
        ready: ReadyCompaction,
    ) -> Result<Option<i64>> {
        let ownership = ready.ownership.clone();
        let id = ownership.operation_id().clone();
        let result = self.finish_compaction_inner(table, schema, ready).await;
        // No catalog POST exists before Prepared. Discarding Building here is
        // therefore safe; all later phases retain the existing recovery proof.
        let store = self.store.clone();
        let saved_id = id.clone();
        let promoted = blocking(move || {
            let Some(record) = store.operation(&saved_id)? else {
                return Ok(false);
            };
            if record.phase == OperationPhase::Building {
                store.discard_uncommitted(&saved_id)?;
                Ok(false)
            } else {
                Ok(true)
            }
        })
        .await?;
        if promoted {
            ownership.promoted().await?;
        } else {
            ownership.release_after_join().await?;
        }
        if result.is_ok() {
            self.forget_rewrite(&id).await?;
        }
        result.map_err(replan_error)
    }

    async fn finish_compaction_inner(
        &self,
        table: &Table,
        schema: &TableSchema,
        ready: ReadyCompaction,
    ) -> Result<Option<i64>> {
        let prepare_started = Instant::now();
        let head = self.catalog.load_table(table.identifier()).await?;
        ready.base.validate(&head).map_err(|_| ReplanRequired)?;
        ensure!(
            schema.table_id == ready.built.table_id()
                && schema.version == ready.indexed.schema_version,
            ReplanRequired
        );
        let table_id = schema.table_id;
        let snapshot = head.metadata().current_snapshot_id();
        let files = ready
            .built
            .plan()
            .input_files
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let store = self.store.clone();
        let (indexed, counts) = blocking(move || {
            let indexed = store.table_state(&table_id)?;
            let counts = store.file_live_row_counts(&table_id, snapshot, &files)?;
            Ok((
                indexed,
                files.into_iter().zip(counts).collect::<BTreeMap<_, _>>(),
            ))
        })
        .await?;
        ensure!(
            indexed.materialized_lsn >= ready.indexed.materialized_lsn
                && indexed.schema_version == schema.version,
            ReplanRequired
        );
        let operation = PreparedOperation {
            id: ready.ownership.operation_id().clone(),
            table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: indexed.snapshot_id,
            last_lsn: indexed.materialized_lsn,
            schema_version: schema.version,
            artifacts: Vec::new(),
            payload: Vec::new(),
        };
        let ownership = ready.ownership.writer();
        let reservation = ownership.record().await?;
        let store = self.store.clone();
        blocking(move || {
            Ok(store
                .begin_prepare_with_record(operation, Some((&reservation.0, &reservation.1)))?)
        })
        .await?;
        let output_sequence = ready.base.sequence_number;
        let build_snapshot_id = ready.base.snapshot_id;
        let worker_table = head.clone();
        let worker_schema = schema.clone();
        let store = self.store.clone();
        let policy = self.policy.clone();
        let limits = self.read_limits.clone();
        let cache = self.cache.clone();
        let mut writer = self.writer.clone();
        writer.artifact_tracker = Some(ownership.clone());
        let operation_id = ready.built.operation_id().clone();
        let catch_up_started = Instant::now();
        let output = tokio::task::spawn_blocking(move || {
            super::worker_runtime()?.block_on(flow_compactor::catch_up(
                &worker_table,
                &worker_schema,
                &ready.base,
                ready.built,
                &store,
                &counts,
                &policy,
                writer,
                &limits,
                &cache,
                |deltas| {
                    store.stage_deltas(&operation_id, deltas)?;
                    Ok(())
                },
            ))
        })
        .await??;
        let plan_seal_started = Instant::now();
        self.publish_rewrite(
            &head,
            schema,
            output,
            RewriteCommit {
                base: CommitBase::new(&head),
                ownership,
                output_data_sequence: Some(output_sequence),
                build_snapshot_id,
                deltas_staged: true,
                worker_ms: ready.worker_ms,
                prepare_started,
                prepare_preflight_ms: catch_up_started
                    .duration_since(prepare_started)
                    .as_secs_f64()
                    * 1000.0,
                catch_up_ms: plan_seal_started
                    .duration_since(catch_up_started)
                    .as_secs_f64()
                    * 1000.0,
                plan_seal_started,
            },
        )
        .await
    }
}

fn replan_error(error: anyhow::Error) -> anyhow::Error {
    if error.is::<CatchUpRejected>()
        || matches!(
            error.downcast_ref::<flow_state_store::Error>(),
            Some(flow_state_store::Error::SnapshotMismatch { .. })
        )
    {
        error.context(ReplanRequired)
    } else {
        error
    }
}
