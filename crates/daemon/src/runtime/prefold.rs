//! Fold a busy table's next epoch while its current epoch commits and applies.
//!
//! A table's epochs run one at a time: the next epoch's collapse starts only
//! after the previous epoch has applied its index deltas. Folding needs only
//! the journal and the source schema, though, so once the running epoch has
//! consumed its collapsed rows (its preparation is sealed), the queued
//! transactions the next epoch must begin with are folded into a [`Prefold`]
//! on a blocking task, and extended as more arrive. The next dispatch hands
//! the fold to its collapse, which binds it against the index with every check
//! a cold collapse makes, and uses it only if the epoch begins with exactly the
//! folded transactions. Any other dispatch or outcome discards it; the journal
//! remains the authority.
//!
//! The fold starts only after the previous epoch's rows are released, so a
//! table's collapse state stays within one collapse budget. A fold that would
//! exceed the budget is discarded instead of spilled.

use super::pending::PendingWork;
use anyhow::Result;
use flow_coordinator::{CollapseLimits, Prefold};
use flow_ingress_journal::JournalReader;
use flow_model::{PgLsn, SourceId, TableId, TableSchema};
use flow_state_store::StateStore;
use futures::FutureExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

type Fold = JoinHandle<Result<Option<Prefold>>>;

/// A dispatched epoch's fold: ready, or still extending on a blocking task.
pub(super) enum PrefoldTicket {
    Ready(Prefold),
    Folding(Fold),
}

impl PrefoldTicket {
    /// The fold, or None if it failed or gave up; the epoch then collapses
    /// from the journal alone.
    pub(super) async fn resolve(self) -> Option<Prefold> {
        match self {
            Self::Ready(prefold) => Some(prefold),
            Self::Folding(task) => match task.await {
                Ok(Ok(prefold)) => prefold,
                Ok(Err(error)) => {
                    tracing::debug!(event = "prefold_discarded", reason = "fold_failed", %error,
                        "epoch prefold failed; collapsing from the journal");
                    None
                }
                Err(error) => {
                    tracing::debug!(event = "prefold_discarded", reason = "task_failed", %error,
                        "epoch prefold task failed; collapsing from the journal");
                    None
                }
            },
        }
    }
}

/// The fold for one busy table's next epoch.
struct Slot {
    /// Sent by the running epoch once its collapsed rows are consumed.
    released: oneshot::Receiver<()>,
    armed: bool,
    folded: Option<Prefold>,
    task: Option<Fold>,
    /// End positions folded or being folded, in order.
    requested: Vec<PgLsn>,
}

pub(super) struct Prefolds {
    source: SourceId,
    store: StateStore,
    reader: JournalReader,
    limits: CollapseLimits,
    enabled: bool,
    slots: HashMap<TableId, Slot>,
    wake: mpsc::UnboundedSender<()>,
    woken: mpsc::UnboundedReceiver<()>,
}

impl Prefolds {
    pub(super) fn new(
        source: SourceId,
        store: StateStore,
        reader: JournalReader,
        limits: CollapseLimits,
        enabled: bool,
    ) -> Self {
        let (wake, woken) = mpsc::unbounded_channel();
        Self {
            source,
            store,
            reader,
            limits,
            enabled,
            slots: HashMap::new(),
            wake,
            woken,
        }
    }

    /// Resolves when a running epoch released its rows or a fold step ended,
    /// so the runtime loop can [`Self::advance`].
    pub(super) async fn woken(&mut self) {
        let _ = self.woken.recv().await;
        while self.woken.try_recv().is_ok() {}
    }

    /// Track the table's newly dispatched CDC epoch. The returned callback is
    /// called once its collapsed rows are consumed.
    pub(super) fn dispatched(&mut self, table: TableId) -> Option<impl FnOnce() + Send + 'static> {
        if !self.enabled {
            return None;
        }
        let (release, released) = oneshot::channel();
        self.slots.insert(
            table,
            Slot {
                released,
                armed: false,
                folded: None,
                task: None,
                requested: Vec::new(),
            },
        );
        let wake = self.wake.clone();
        Some(move || {
            let _ = release.send(());
            let _ = wake.send(());
        })
    }

    /// The fold for the table's next CDC epoch, if one was started.
    pub(super) fn take(&mut self, table: TableId) -> Option<PrefoldTicket> {
        let slot = self.slots.remove(&table)?;
        match (slot.task, slot.folded) {
            (Some(task), _) => Some(PrefoldTicket::Folding(task)),
            (None, Some(prefold)) if !prefold.transactions().is_empty() => {
                Some(PrefoldTicket::Ready(prefold))
            }
            _ => None,
        }
    }

    /// Drop the table's fold: its epoch failed, replanned or was not CDC.
    pub(super) fn discard(&mut self, table: TableId, reason: &'static str) {
        if let Some(slot) = self.slots.remove(&table)
            && !slot.requested.is_empty()
        {
            metrics::counter!("flow_collapse_prefolds_total", "table_id" => table.0.to_string(),
                "result" => "discarded")
            .increment(1);
            tracing::debug!(
                event = "prefold_discarded",
                table_id = table.0,
                reason,
                transactions = slot.requested.len(),
                "epoch prefold discarded"
            );
        }
    }

    /// Start or extend folds for tables whose running CDC epoch released its
    /// rows, over the queued prefix their next epoch must begin with. `busy`
    /// holds tables with running work; only CDC dispatches keep a slot.
    pub(super) fn advance(
        &mut self,
        pending: &PendingWork,
        busy: &HashSet<TableId>,
        schemas: &BTreeMap<TableId, TableSchema>,
    ) {
        let mut dead = Vec::new();
        for (table, slot) in &mut self.slots {
            if !busy.contains(table) {
                continue;
            }
            if !slot.armed {
                match slot.released.try_recv() {
                    Ok(()) => slot.armed = true,
                    Err(oneshot::error::TryRecvError::Empty) => continue,
                    // The epoch ended without consuming rows, such as a no-op.
                    Err(oneshot::error::TryRecvError::Closed) => {
                        dead.push((*table, "not_released"));
                        continue;
                    }
                }
            }
            if let Some(task) = slot.task.take_if(|task| task.is_finished()) {
                match finished(task) {
                    Some(prefold) => slot.folded = Some(prefold),
                    None => {
                        dead.push((*table, "over_budget_or_failed"));
                        continue;
                    }
                }
            }
            if slot.task.is_some() {
                continue;
            }
            let Some(schema) = schemas.get(table) else {
                continue;
            };
            let mut prefix = pending.epoch_prefix(*table);
            if !slot.requested.iter().all(|end| {
                prefix
                    .next()
                    .is_some_and(|transaction| transaction.end_lsn == *end)
            }) {
                dead.push((*table, "queue_changed"));
                continue;
            }
            let next: Vec<_> = prefix.cloned().collect();
            if next.is_empty() {
                continue;
            }
            slot.requested
                .extend(next.iter().map(|transaction| transaction.end_lsn));
            let (source, store, reader, limits, wake, id) = (
                self.source.clone(),
                self.store.clone(),
                self.reader.clone(),
                self.limits,
                self.wake.clone(),
                *table,
            );
            let (folded, boot_schema) = (slot.folded.take(), schema.clone());
            slot.task = Some(tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                // Fold with the schema the epoch's collapse will read.
                let schema =
                    crate::schema::latest_schema(&store, &source, id)?.unwrap_or(boot_schema);
                let prefold = match folded {
                    Some(prefold) if prefold.schema() == &schema => prefold,
                    Some(_) => return Ok(None),
                    None => match Prefold::new(source, schema, limits) {
                        Some(prefold) => prefold,
                        None => return Ok(None),
                    },
                };
                let result = prefold.extend(&store, &reader, &next, limits);
                metrics::histogram!("flow_collapse_prefold_seconds", "table_id" => id.0.to_string())
                    .record(started.elapsed().as_secs_f64());
                let _ = wake.send(());
                result
            }));
        }
        for (table, reason) in dead {
            self.discard(table, reason);
        }
    }
}

/// The result of a finished fold task; None if it failed or gave up.
fn finished(task: Fold) -> Option<Prefold> {
    match task.now_or_never()? {
        Ok(Ok(prefold)) => prefold,
        _ => None,
    }
}
