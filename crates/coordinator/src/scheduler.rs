//! Event-driven deadlines and explicit limits on catalog pressure and reader debt.
use flow_model::TableId;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

#[derive(Debug, Default, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    #[default]
    Realtime,
    Balanced,
    Efficient,
}
impl Priority {
    fn budget(self) -> Duration {
        match self {
            Self::Realtime => Duration::from_millis(900),
            Self::Balanced => Duration::from_secs(3),
            Self::Efficient => Duration::from_secs(15),
        }
    }
}
#[derive(Debug, Clone)]
struct Pending {
    first: Instant,
    ready_at: Instant,
    rows: u64,
    bytes: u64,
    priority: Priority,
    stalled: bool,
}
/// Only complete transactions enter this scheduler. A deadline cannot split one.
pub struct Scheduler {
    pending: BTreeMap<TableId, Pending>,
    max_rows: u64,
    max_bytes: u64,
    min_commit_interval: Duration,
    next_permit: Instant,
    service_times: BTreeMap<TableId, VecDeque<Duration>>,
}
impl Scheduler {
    pub fn new(
        max_rows: u64,
        max_bytes: u64,
        commits_per_second: u32,
        now: Instant,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_rows > 0 && max_bytes > 0 && commits_per_second > 0,
            "scheduler limits must be positive"
        );
        Ok(Self {
            pending: BTreeMap::new(),
            max_rows,
            max_bytes,
            min_commit_interval: Duration::from_secs_f64(1.0 / f64::from(commits_per_second)),
            next_permit: now,
            service_times: BTreeMap::new(),
        })
    }
    pub fn push(
        &mut self,
        table: TableId,
        rows: Option<u64>,
        bytes: u64,
        priority: Priority,
        source_age: Duration,
        now: Instant,
    ) {
        // None is legacy work with unknown size: publish it promptly rather
        // than inventing a row count. Zero-row transactions still need a deadline.
        let mut samples = self
            .service_times
            .get(&table)
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        samples.sort_unstable();
        let estimate = if samples.is_empty() {
            Duration::from_millis(300)
        } else {
            samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)]
        };
        let delay = priority
            .budget()
            .saturating_sub(estimate)
            .saturating_sub(source_age)
            .min(match priority {
                Priority::Realtime => Duration::from_millis(400),
                _ => priority.budget(),
            });
        let pending = self.pending.entry(table).or_insert(Pending {
            first: now,
            ready_at: now + delay,
            rows: 0,
            bytes: 0,
            priority,
            stalled: false,
        });
        pending.rows = pending.rows.saturating_add(rows.unwrap_or(0));
        pending.bytes = pending.bytes.saturating_add(bytes);
        pending.ready_at = pending.ready_at.min(pending.first + delay);
        if rows.is_none() || pending.rows >= self.max_rows || pending.bytes >= self.max_bytes {
            pending.ready_at = pending.ready_at.min(now);
        }
    }
    /// Include artifact preparation and local maintenance in the estimate: both
    /// occupy this table's publication lane and consume its latency budget.
    pub fn record_completion(&mut self, table: TableId, elapsed: Duration) {
        let samples = self.service_times.entry(table).or_default();
        if samples.len() == 32 {
            samples.pop_front();
        }
        samples.push_back(elapsed);
    }
    pub fn stall(&mut self, table: TableId, stalled: bool) {
        if let Some(p) = self.pending.get_mut(&table) {
            p.stalled = stalled;
        }
    }
    /// Evict only volatile admission; the source ledger retains table work.
    pub fn remove(&mut self, table: TableId) {
        self.pending.remove(&table);
    }
    pub fn force(&mut self, table: TableId, now: Instant) {
        if let Some(p) = self.pending.get_mut(&table) {
            p.ready_at = p.ready_at.min(now);
        }
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter(|p| !p.stalled)
            .map(|p| p.ready_at.max(self.next_permit))
            .min()
    }
    pub fn take_ready(&mut self, now: Instant) -> Option<TableId> {
        if now < self.next_permit {
            return None;
        }
        let id = self
            .pending
            .iter()
            .filter(|(_, p)| !p.stalled && p.ready_at <= now)
            .min_by_key(|(_, p)| (p.ready_at, p.priority))
            .map(|(id, _)| *id)?;
        self.pending.remove(&id);
        self.next_permit = now + self.min_commit_interval;
        Some(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHealth {
    Healthy,
    Warning,
    AtRisk,
    SlotLost,
}
#[derive(Debug, Clone, Copy)]
pub struct WalPressure {
    pub retained_bytes: u64,
    pub journal_bytes: u64,
    pub safe_wal_bytes: Option<u64>,
    pub slot_lost: bool,
}
impl WalPressure {
    pub fn health(&self, wal_soft: u64, wal_hard: u64, journal_quota: u64) -> SourceHealth {
        // PostgreSQL's actual retention budget can be smaller than our configured
        // thresholds. React with headroom remaining, not only after it reaches zero.
        let headroom = self.safe_wal_bytes.map(|safe| {
            let budget = u128::from(self.retained_bytes) + u128::from(safe);
            (u128::from(safe), budget)
        });
        if self.slot_lost {
            SourceHealth::SlotLost
        } else if self.retained_bytes >= wal_hard
            || self.journal_bytes >= journal_quota
            || self.safe_wal_bytes == Some(0)
            || headroom.is_some_and(|(safe, budget)| safe * 10 <= budget)
        {
            SourceHealth::AtRisk
        } else if self.retained_bytes >= wal_soft
            || self.journal_bytes >= journal_quota.saturating_mul(4) / 5
            || headroom.is_some_and(|(safe, budget)| safe * 4 <= budget)
        {
            SourceHealth::Warning
        } else {
            SourceHealth::Healthy
        }
    }
}
