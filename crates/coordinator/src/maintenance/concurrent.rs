//! Local speculative data compaction. The actor captures the base and later
//! finalizes completed work; it never waits for a running worker under its fence.

use super::{BuildRegistration, RewriteCommit, TableMaintenance};
use crate::{blocking, publication::ReplanRequired};
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

struct PreparedData {
    output: flow_compactor::WorkerOutput,
    head: Table,
    indexed: TableState,
    build_base: CommitBase,
    catch_up_ms: f64,
}

/// Catch-up running against one immutable catalog and row-index head. It owns no
/// table publication fence and cannot submit a catalog commit.
#[must_use = "join preparation before releasing its scratch directory"]
pub struct RunningCompactionPreparation {
    worker: tokio::task::JoinHandle<Result<PreparedData>>,
    ownership: BuildRegistration,
    started: Instant,
    worker_ms: f64,
}

/// Result of waiting for detached catch-up without sacrificing its ownership
/// barrier when the caller's publication-stall budget expires.
pub enum PreparationWait {
    Ready(Box<PreparedCompaction>),
    Deadline(RetiringCompactionPreparation),
}

/// A timed-out preparation whose blocking worker still owns scratch artifacts.
/// CDC may resume while this handle joins and retires that work, or transfers it
/// back to a running preparation for later exact-head activation.
#[must_use = "join the worker before releasing its scratch directory"]
pub struct RetiringCompactionPreparation {
    worker: tokio::task::JoinHandle<Result<PreparedData>>,
    ownership: BuildRegistration,
    started: Instant,
    worker_ms: f64,
}

impl RetiringCompactionPreparation {
    /// Continue the same immutable preparation after releasing the table lane.
    /// Activation still requires its exact captured catalog and index head.
    pub fn into_running(self) -> RunningCompactionPreparation {
        RunningCompactionPreparation {
            worker: self.worker,
            ownership: self.ownership,
            started: self.started,
            worker_ms: self.worker_ms,
        }
    }

    pub fn operation_id(&self) -> &OperationId {
        self.ownership.operation_id()
    }

    pub async fn wait(self) -> Result<()> {
        let joined = self.worker.await;
        self.ownership.release_after_join().await?;
        tracing::info!(
            event = "compaction_preparation_retired",
            operation_id = %self.ownership.operation_id().0,
            elapsed_ms = self.started.elapsed().as_secs_f64() * 1000.0,
            "joined timed-out compaction preparation and released build ownership"
        );
        match joined {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(replan_error(error)),
            Err(error) => Err(error.into()),
        }
    }
}

impl RunningCompactionPreparation {
    pub async fn wait(self) -> Result<PreparedCompaction> {
        match self.wait_for(MAX_BUILD_TIME).await? {
            PreparationWait::Ready(prepared) => Ok(*prepared),
            PreparationWait::Deadline(retiring) => {
                retiring.wait().await?;
                Err(ReplanRequired.into())
            }
        }
    }

    /// Wait only through the caller's stall budget. On expiry the returned
    /// retirement handle keeps BUILD ownership until the blocking worker joins.
    pub async fn wait_for(mut self, deadline: Duration) -> Result<PreparationWait> {
        if deadline.is_zero() {
            return Ok(PreparationWait::Deadline(RetiringCompactionPreparation {
                worker: self.worker,
                ownership: self.ownership,
                started: self.started,
                worker_ms: self.worker_ms,
            }));
        }
        let outcome = match tokio::time::timeout(deadline, &mut self.worker).await {
            Ok(outcome) => outcome
                .map_err(anyhow::Error::from)
                .and_then(|result| result),
            Err(_) => {
                return Ok(PreparationWait::Deadline(RetiringCompactionPreparation {
                    worker: self.worker,
                    ownership: self.ownership,
                    started: self.started,
                    worker_ms: self.worker_ms,
                }));
            }
        };
        match outcome {
            Ok(prepared) => {
                tracing::info!(
                    event = "compaction_preparation_ready",
                    operation_id = %self.ownership.operation_id().0,
                    head_snapshot_id = prepared.head.metadata().current_snapshot_id(),
                    materialized_lsn = %prepared.indexed.materialized_lsn,
                    elapsed_ms = self.started.elapsed().as_secs_f64() * 1000.0,
                    catch_up_ms = prepared.catch_up_ms,
                    "compaction catch-up prepared outside the table actor lane"
                );
                Ok(PreparationWait::Ready(Box::new(PreparedCompaction {
                    output: prepared.output,
                    ownership: self.ownership,
                    head: prepared.head,
                    indexed: prepared.indexed,
                    build_base: prepared.build_base,
                    worker_ms: self.worker_ms,
                    catch_up_ms: prepared.catch_up_ms,
                })))
            }
            Err(error) => {
                self.ownership.release_after_join().await?;
                Err(replan_error(error))
            }
        }
    }
}

/// A fully checked rewrite candidate anchored to an exact catalog and row-index
/// head. Activation must install the ordinary durable fence against that same
/// `TableState` before importing any mapping.
#[must_use = "activate or discard the prepared compaction"]
pub struct PreparedCompaction {
    output: flow_compactor::WorkerOutput,
    ownership: BuildRegistration,
    head: Table,
    indexed: TableState,
    build_base: CommitBase,
    worker_ms: f64,
    catch_up_ms: f64,
}

impl PreparedCompaction {
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
            ReplanRequired
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

    /// Compatibility path for serialized library callers. The daemon uses the
    /// split prepare/activate API so catch-up does not occupy its table lane.
    pub async fn finish_compaction(
        &self,
        table: &Table,
        schema: &TableSchema,
        ready: ReadyCompaction,
    ) -> Result<Option<i64>> {
        let prepared = self
            .start_compaction_preparation(table, schema, ready)
            .await?
            .wait()
            .await?;
        self.activate_compaction(table, schema, prepared).await
    }

    /// Capture an exact unfenced index head, then run all late-delete and mapping
    /// checks on its immutable RocksDB snapshot. No main-store operation exists
    /// and no catalog commit is possible until `activate_compaction` succeeds.
    pub async fn start_compaction_preparation(
        &self,
        table: &Table,
        schema: &TableSchema,
        ready: ReadyCompaction,
    ) -> Result<RunningCompactionPreparation> {
        let identifier = table.identifier().clone();
        let catalog = self.catalog.clone();
        let store = self.store.clone();
        let worker_schema = schema.clone();
        let base = ready.base;
        let built = ready.built;
        let build_indexed = ready.indexed;
        let policy = self.policy.clone();
        let limits = self.read_limits.clone();
        let cache = self.cache.clone();
        let ownership = ready.ownership;
        let worker_ownership = ownership.clone();
        let mut writer = self.writer.clone();
        writer.artifact_tracker = Some(worker_ownership.writer());
        let started = Instant::now();
        let worker = tokio::spawn(async move {
            let head = catalog.load_table(&identifier).await?;
            base.validate(&head).map_err(|_| ReplanRequired)?;
            worker_ownership
                .validate_table(&head, worker_schema.table_id)
                .map_err(|_| ReplanRequired)?;
            ensure!(
                worker_schema.table_id == built.table_id()
                    && worker_schema.version == build_indexed.schema_version
                    && crate::same_iceberg_schema(
                        head.metadata().current_schema(),
                        &iceberg_schema(&worker_schema)?
                    ),
                ReplanRequired
            );
            let table_id = worker_schema.table_id;
            let snapshot_id = head.metadata().current_snapshot_id();
            let files = built.plan().input_files.iter().cloned().collect::<Vec<_>>();
            let worker_table = head.clone();
            let prepared = tokio::task::spawn_blocking(move || {
                let index = store.index_snapshot(&table_id, snapshot_id)?;
                let indexed = index.table_state().clone();
                ensure!(
                    indexed.schema_version == worker_schema.version
                        && indexed.materialized_lsn >= build_indexed.materialized_lsn,
                    ReplanRequired
                );
                let counts = files
                    .iter()
                    .cloned()
                    .zip(index.file_live_row_counts(&files)?)
                    .collect::<BTreeMap<_, _>>();
                let catch_up_started = Instant::now();
                let output = super::worker_runtime()?.block_on(flow_compactor::catch_up(
                    &worker_table,
                    &worker_schema,
                    &base,
                    built,
                    &index,
                    &counts,
                    &policy,
                    writer,
                    &limits,
                    &cache,
                    |_| Ok(()),
                ))?;
                Ok(PreparedData {
                    output,
                    head: worker_table,
                    indexed,
                    build_base: base,
                    catch_up_ms: catch_up_started.elapsed().as_secs_f64() * 1000.0,
                })
            })
            .await??;
            Ok(prepared)
        });
        Ok(RunningCompactionPreparation {
            worker,
            ownership,
            started,
            worker_ms: ready.worker_ms,
        })
    }

    /// Atomically admit an already prepared candidate at its exact captured
    /// catalog/index head, then use the ordinary recoverable publication path.
    pub async fn activate_compaction(
        &self,
        table: &Table,
        schema: &TableSchema,
        prepared: PreparedCompaction,
    ) -> Result<Option<i64>> {
        let ownership = prepared.ownership.clone();
        let id = ownership.operation_id().clone();
        let result = self
            .activate_compaction_inner(table, schema, prepared)
            .await;
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

    async fn activate_compaction_inner(
        &self,
        table: &Table,
        schema: &TableSchema,
        prepared: PreparedCompaction,
    ) -> Result<Option<i64>> {
        let prepare_started = Instant::now();
        let head = self.catalog.load_table(table.identifier()).await?;
        let anchor = CommitBase::new(&prepared.head);
        anchor.validate(&head).map_err(|_| ReplanRequired)?;
        ensure!(
            head.metadata().location() == prepared.head.metadata().location()
                && head.metadata().current_snapshot_id() == anchor.snapshot_id
                && head.metadata().current_schema_id() == anchor.schema_id
                && head.metadata().default_partition_spec_id() == anchor.spec_id
                && schema.table_id == prepared.output.table_id()
                && schema.version == prepared.indexed.schema_version,
            ReplanRequired
        );
        let table_id = schema.table_id;
        let operation = PreparedOperation {
            id: prepared.ownership.operation_id().clone(),
            table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: prepared.indexed.snapshot_id,
            last_lsn: prepared.indexed.materialized_lsn,
            schema_version: schema.version,
            artifacts: Vec::new(),
            payload: Vec::new(),
        };
        let ownership = prepared.ownership.writer();
        let reservation = ownership.record().await?;
        let store = self.store.clone();
        let expected = prepared.indexed.clone();
        blocking(move || {
            Ok(store.begin_prepare_exact(
                operation,
                &expected,
                Some((&reservation.0, &reservation.1)),
            )?)
        })
        .await?;
        let plan_seal_started = Instant::now();
        self.publish_rewrite(
            &head,
            schema,
            prepared.output,
            RewriteCommit {
                base: CommitBase::new(&head),
                ownership,
                output_data_sequence: Some(prepared.build_base.sequence_number),
                build_snapshot_id: prepared.build_base.snapshot_id,
                deltas_staged: false,
                worker_ms: prepared.worker_ms,
                prepare_started,
                prepare_preflight_ms: plan_seal_started
                    .duration_since(prepare_started)
                    .as_secs_f64()
                    * 1000.0,
                catch_up_ms: prepared.catch_up_ms,
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
            Some(
                flow_state_store::Error::SnapshotMismatch { .. }
                    | flow_state_store::Error::ExactStateMismatch { .. }
            )
        )
    {
        error.context(ReplanRequired)
    } else {
        error
    }
}
