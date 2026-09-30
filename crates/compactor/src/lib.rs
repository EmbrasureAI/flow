//! Bounded maintenance planning. Workers may execute concurrently, but only the
//! table coordinator can validate and publish their output.

use flow_model::FileId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Level {
    L0,
    L1,
    L2,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataRewriteScope {
    Disabled,
    L0Only,
    #[default]
    All,
}

#[derive(Debug, Clone)]
pub struct FileCandidate {
    pub id: FileId,
    pub level: Level,
    pub size_bytes: u64,
    pub row_count: u64,
    pub deleted_rows: u64,
    pub delete_file_count: usize,
    pub age_ms: u64,
    pub spec_id: i32,
    pub partition: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DeleteDependency {
    pub id: FileId,
    /// Complete set of live data files to which this delete file applies.
    pub targets: BTreeSet<FileId>,
    pub size_bytes: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub data_rewrite_scope: DataRewriteScope,
    pub l0_soft_files: usize,
    pub l0_hard_files: usize,
    pub stable_small_soft_files: usize,
    pub stable_small_hard_files: usize,
    pub l0_soft_bytes: u64,
    pub l0_hard_bytes: u64,
    pub oldest_l0_soft_ms: u64,
    pub oldest_l0_hard_ms: u64,
    pub delete_files_soft: usize,
    pub delete_files_hard: usize,
    pub deleted_rows_percent: u8,
    pub min_group_files: usize,
    pub max_group_files: usize,
    pub max_input_bytes: u64,
    pub max_delete_input_files: usize,
    pub max_delete_input_bytes: u64,
    pub max_delete_input_rows: u64,
    pub l1_target_bytes: u64,
    pub l2_target_bytes: u64,
    pub min_file_age_ms: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            data_rewrite_scope: DataRewriteScope::All,
            l0_soft_files: 24,
            l0_hard_files: 32,
            stable_small_soft_files: 24,
            stable_small_hard_files: 32,
            l0_soft_bytes: 128 << 20,
            l0_hard_bytes: 256 << 20,
            oldest_l0_soft_ms: 10_000,
            oldest_l0_hard_ms: 30_000,
            delete_files_soft: 8,
            delete_files_hard: 16,
            deleted_rows_percent: 30,
            min_group_files: 2,
            max_group_files: 32,
            max_input_bytes: 1 << 30,
            max_delete_input_files: 32,
            max_delete_input_bytes: 32 << 20,
            max_delete_input_rows: 1_000_000,
            l1_target_bytes: 128 << 20,
            l2_target_bytes: 512 << 20,
            min_file_age_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationPressure {
    Healthy,
    Delay,
    Pause,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Debt {
    pub l0_files: usize,
    /// Undersized files outside L0, including external rewrite output. Level
    /// hints cannot exempt a fragmented table from file-count maintenance.
    pub small_files: usize,
    pub l0_bytes: u64,
    pub oldest_l0_ms: u64,
    pub max_delete_files: usize,
    pub reclaimable_bytes: u64,
    pub pressure: PublicationPressure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub base_snapshot_id: i64,
    pub schema_id: i32,
    pub input_files: BTreeSet<FileId>,
    pub delete_files: BTreeSet<FileId>,
    pub spec_id: i32,
    pub partition: Vec<u8>,
    pub output_level: Level,
    pub target_file_bytes: u64,
    pub input_bytes: u64,
    pub delete_input_bytes: u64,
    pub delete_input_rows: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("reader-debt limits require maintenance before publication can continue")]
    MaintenanceRequired,
    #[error("invalid compaction policy: {0}")]
    InvalidPolicy(&'static str),
    #[error("invalid snapshot inventory: {0}")]
    InvalidInventory(&'static str),
    #[error("no useful data compaction group fits the delete-work limits")]
    DependencyBudget,
}
pub type Result<T> = std::result::Result<T, Error>;

impl Policy {
    pub fn needs_delete_reclamation(&self, file: &FileCandidate) -> bool {
        file.row_count > 0
            && file.deleted_rows as u128 * 100
                >= file.row_count as u128 * self.deleted_rows_percent as u128
    }

    pub fn validate(&self) -> Result<()> {
        if self.l0_soft_files == 0
            || self.l0_soft_files > self.l0_hard_files
            || self.stable_small_soft_files == 0
            || self.stable_small_soft_files > self.stable_small_hard_files
            || self.l0_soft_bytes == 0
            || self.l0_soft_bytes > self.l0_hard_bytes
            || self.oldest_l0_soft_ms == 0
            || self.oldest_l0_soft_ms > self.oldest_l0_hard_ms
            || self.delete_files_soft == 0
            || self.delete_files_soft > self.delete_files_hard
        {
            return Err(Error::InvalidPolicy(
                "soft limits must be positive and at most hard limits",
            ));
        }
        if self.min_group_files < 2
            || self.min_group_files > self.max_group_files
            || self.l1_target_bytes == 0
            || self.l1_target_bytes > self.l2_target_bytes
            || self.max_input_bytes < self.l2_target_bytes
            || self.max_delete_input_files == 0
            || self.max_delete_input_bytes == 0
            || self.max_delete_input_rows == 0
            || !(1..=100).contains(&self.deleted_rows_percent)
        {
            return Err(Error::InvalidPolicy(
                "invalid group, output-size, or deletion-density limits",
            ));
        }
        Ok(())
    }

    pub fn debt(&self, files: &[FileCandidate]) -> Result<Debt> {
        self.validate()?;
        let mut debt = Debt {
            l0_files: 0,
            small_files: 0,
            l0_bytes: 0,
            oldest_l0_ms: 0,
            max_delete_files: 0,
            reclaimable_bytes: 0,
            pressure: PublicationPressure::Healthy,
        };
        let mut identities = BTreeSet::new();
        let mut dense_deletes = false;
        for file in files {
            if !identities.insert(&file.id) || file.deleted_rows > file.row_count {
                return Err(Error::InvalidInventory(
                    "duplicate file or deleted rows exceed physical rows",
                ));
            }
            if file.level == Level::L0 {
                debt.l0_files += 1;
                debt.l0_bytes = debt.l0_bytes.saturating_add(file.size_bytes);
                debt.oldest_l0_ms = debt.oldest_l0_ms.max(file.age_ms);
            }
            if file.level != Level::L0 && file.size_bytes < self.l1_target_bytes {
                debt.small_files += 1;
            }
            debt.max_delete_files = debt.max_delete_files.max(file.delete_file_count);
            if file.row_count > 0 {
                dense_deletes |= self.needs_delete_reclamation(file);
                let reclaimable = (file.size_bytes as u128 * file.deleted_rows as u128
                    / file.row_count as u128) as u64;
                debt.reclaimable_bytes = debt.reclaimable_bytes.saturating_add(reclaimable);
            }
        }
        if debt.l0_files >= self.l0_soft_files
            || debt.small_files >= self.stable_small_soft_files
            || debt.l0_bytes >= self.l0_soft_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_soft_ms
            || debt.max_delete_files >= self.delete_files_soft
            || dense_deletes
        {
            debt.pressure = PublicationPressure::Delay;
        }
        if debt.l0_files >= self.l0_hard_files
            || debt.small_files >= self.stable_small_hard_files
            || debt.l0_bytes >= self.l0_hard_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_hard_ms
            || debt.max_delete_files >= self.delete_files_hard
        {
            debt.pressure = PublicationPressure::Pause;
        }
        Ok(debt)
    }

    /// Return at most one partition-local group. Prioritizes deletion debt, then
    /// old L0 files, then full-sized L1 groups. Singleton rewrites are permitted
    /// for delete debt; old singleton L0 files become L1 to retire age debt.
    pub fn plan(
        &self,
        base_snapshot_id: i64,
        schema_id: i32,
        files: &[FileCandidate],
        deletes: &[DeleteDependency],
    ) -> Result<Option<CompactionPlan>> {
        let debt = self.debt(files)?;
        let by_id: BTreeMap<_, _> = files.iter().map(|file| (&file.id, file)).collect();
        let mut by_target = BTreeMap::<&FileId, Vec<usize>>::new();
        let mut delete_ids = BTreeSet::new();
        for (index, delete) in deletes.iter().enumerate() {
            if !delete_ids.insert(&delete.id) {
                return Err(Error::InvalidInventory(
                    "delete dependencies must be unique and target only current live files",
                ));
            }
            for target in &delete.targets {
                if !by_id.contains_key(target) {
                    return Err(Error::InvalidInventory(
                        "delete dependencies must be unique and target only current live files",
                    ));
                }
                by_target.entry(target).or_default().push(index);
            }
        }
        let needs_delete_rewrite = |f: &FileCandidate| {
            f.delete_file_count >= self.delete_files_soft || self.needs_delete_reclamation(f)
        };
        let needs_small_file_rewrite = |file: &FileCandidate| {
            debt.small_files >= self.stable_small_soft_files
                && file.level != Level::L0
                && file.size_bytes < self.l1_target_bytes
        };
        // Under delete pressure, retiring effective positions can remove more
        // residual files than another small L0 rewrite. Hard L0 limits retain
        // their existing ordering so reclamation cannot postpone that debt.
        let hard_l0_debt = debt.l0_files >= self.l0_hard_files
            || debt.l0_bytes >= self.l0_hard_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_hard_ms;
        let prioritize_reclamation = (deletes.len() >= self.delete_files_soft
            || debt.max_delete_files >= self.delete_files_soft)
            && !hard_l0_debt;
        let mut eligible: Vec<_> = files
            .iter()
            .filter(|file| {
                let in_scope = match self.data_rewrite_scope {
                    DataRewriteScope::Disabled => false,
                    DataRewriteScope::L0Only => file.level == Level::L0,
                    DataRewriteScope::All => true,
                };
                in_scope
                    && (file.age_ms >= self.min_file_age_ms
                        || debt.pressure == PublicationPressure::Pause)
                    && match file.level {
                        Level::L0 => {
                            debt.pressure != PublicationPressure::Healthy
                                || needs_delete_rewrite(file)
                        }
                        Level::L1 => true,
                        Level::L2 => needs_delete_rewrite(file) || needs_small_file_rewrite(file),
                    }
            })
            .collect();
        eligible.sort_by(|a, b| {
            (hard_l0_debt && b.level == Level::L0)
                .cmp(&(hard_l0_debt && a.level == Level::L0))
                .then_with(|| needs_delete_rewrite(b).cmp(&needs_delete_rewrite(a)))
                .then_with(|| needs_small_file_rewrite(b).cmp(&needs_small_file_rewrite(a)))
                .then_with(|| {
                    if prioritize_reclamation && needs_delete_rewrite(a) && needs_delete_rewrite(b)
                    {
                        b.deleted_rows.cmp(&a.deleted_rows)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .then_with(|| a.level.cmp(&b.level))
                .then_with(|| b.age_ms.cmp(&a.age_ms))
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut budget_blocked = false;
        for seed in &eligible {
            let output_level = if seed.level == Level::L0 {
                Level::L1
            } else {
                Level::L2
            };
            let target_file_bytes = if output_level == Level::L1 {
                self.l1_target_bytes
            } else {
                self.l2_target_bytes
            };
            let forced = needs_delete_rewrite(seed)
                || (seed.level == Level::L0 && debt.pressure != PublicationPressure::Healthy);
            let small_group = needs_small_file_rewrite(seed);
            let useful = |count, bytes| {
                forced
                    || count >= self.min_group_files
                        && (seed.level != Level::L1 || bytes >= target_file_bytes || small_group)
            };
            let companions = eligible.iter().copied().filter(|file| {
                if file.id == seed.id
                    || small_group && !needs_small_file_rewrite(file)
                    || (file.level != seed.level
                        && !(small_group && needs_small_file_rewrite(file)))
                    || file.spec_id != seed.spec_id
                    || file.partition != seed.partition
                {
                    return false;
                }
                // Urgency permits a singleton, not unrelated large companions.
                // File-count consolidation admits mixed small sizes.
                let smallest = seed.size_bytes.min(file.size_bytes).max(1);
                small_group
                    || u128::from(seed.size_bytes.max(file.size_bytes)) <= u128::from(smallest) * 4
            });
            let mut delete_files = BTreeSet::new();
            let mut delete_input_bytes = 0u64;
            let mut delete_input_rows = 0u64;
            // Include the full applicable delete union; the worker preserves
            // its residual positions for data files outside this group.
            let mut admit_deletes = |file: &FileCandidate| {
                let inputs = by_target.get(&file.id).map(Vec::as_slice).unwrap_or(&[]);
                let mut count = delete_files.len();
                let mut bytes = delete_input_bytes;
                let mut rows = delete_input_rows;
                for &index in inputs {
                    if delete_files.contains(&index) {
                        continue;
                    }
                    let delete = &deletes[index];
                    let (Some(next_bytes), Some(next_rows)) = (
                        bytes.checked_add(delete.size_bytes),
                        rows.checked_add(delete.row_count),
                    ) else {
                        return false;
                    };
                    count += 1;
                    if count > self.max_delete_input_files
                        || next_bytes > self.max_delete_input_bytes
                        || next_rows > self.max_delete_input_rows
                    {
                        return false;
                    }
                    bytes = next_bytes;
                    rows = next_rows;
                }
                delete_files.extend(inputs.iter().copied());
                delete_input_bytes = bytes;
                delete_input_rows = rows;
                true
            };
            let mut selected = BTreeSet::new();
            let mut total = seed.size_bytes;
            let mut limited = total > self.max_input_bytes || !admit_deletes(seed);
            if !limited {
                selected.insert(seed.id.clone());
                for file in companions.clone() {
                    if selected.len() >= self.max_group_files
                        || total >= target_file_bytes && useful(selected.len(), total)
                    {
                        break;
                    }
                    let Some(next_total) = total
                        .checked_add(file.size_bytes)
                        .filter(|bytes| *bytes <= self.max_input_bytes)
                    else {
                        continue;
                    };
                    if !admit_deletes(file) {
                        limited = true;
                        continue;
                    }
                    selected.insert(file.id.clone());
                    total = next_total;
                }
                if useful(selected.len(), total) {
                    return Ok(Some(CompactionPlan {
                        base_snapshot_id,
                        schema_id,
                        input_files: selected,
                        delete_files: delete_files
                            .into_iter()
                            .map(|index| deletes[index].id.clone())
                            .collect(),
                        spec_id: seed.spec_id,
                        partition: seed.partition.clone(),
                        output_level,
                        target_file_bytes,
                        input_bytes: total,
                        delete_input_bytes,
                        delete_input_rows,
                    }));
                }
            }
            if limited {
                // Distinguish budget-blocked useful work from an ordinary
                // undersized group. This pass needs only candidate metadata.
                let mut count = 1;
                let mut bytes = seed.size_bytes;
                for file in companions {
                    if count >= self.max_group_files
                        || bytes >= target_file_bytes && useful(count, bytes)
                    {
                        break;
                    }
                    if let Some(next) = bytes
                        .checked_add(file.size_bytes)
                        .filter(|bytes| *bytes <= self.max_input_bytes)
                    {
                        count += 1;
                        bytes = next;
                    }
                }
                budget_blocked |= useful(count, bytes);
            }
        }
        if budget_blocked {
            Err(Error::DependencyBudget)
        } else {
            Ok(None)
        }
    }
}

mod parquet_scan;
pub use parquet_scan::ReadLimits;
mod catchup;
pub use catchup::{CatchUpRejected, catch_up};
mod worker;
pub use worker::{
    BuiltData, DeleteReadLimits, LiveBatch, WorkerOutput, build_data, compact, scan_live_files,
    stage_delete_files, stage_position_deletes,
};
