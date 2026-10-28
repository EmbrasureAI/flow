//! Bounded maintenance planning. Workers may execute concurrently, but only the
//! table coordinator can validate and publish their output.

use flow_model::FileId;
use iceberg::spec::FormatVersion;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
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

/// How delete-file count debt is paid.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeleteDebt {
    /// The previous behavior: a data file with `delete_files_soft` applicable
    /// delete files is a data rewrite candidate, and delete-only consolidation
    /// runs only when no file needs delete reclamation.
    Current,
    /// Delete-file count debt, soft or hard, is paid first by a delete-only
    /// consolidation. Data rewrites for deletes select only files whose own
    /// deleted fraction reaches `deleted_rows_percent`. Format v3 tables use
    /// [`DeleteDebt::Current`]; see [`Policy::for_format_version`].
    #[default]
    Consolidate,
}

/// How Flow lays out the format v2 position-delete files it rewrites.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteLayout {
    /// Delete-only consolidation merges its inputs into shared files and data
    /// rewrites keep residual positions in at most eight shared ranges.
    #[default]
    Shared,
    /// Delete-only consolidation and data-rewrite residuals write one file per
    /// full-size target data file, with exact single-path bounds, and pooled
    /// ranges for L0 and smaller ones (see [`TargetRanges`]); consolidation
    /// merges a target's existing files by size tier. CDC epochs still write
    /// one shared file. Format v3 tables keep one deletion vector per data file
    /// and use [`DeleteLayout::Shared`]; see [`Policy::for_format_version`].
    #[serde(alias = "per-target")]
    PerTarget,
}

impl DeleteLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::PerTarget => "per_target",
        }
    }
}

/// Why the planner selected a data rewrite group, from its seed file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanReason {
    /// The seed's own deleted fraction reached `deleted_rows_percent`.
    DeletedRows,
    /// The seed had at least `delete_files_soft` applicable delete files.
    DeleteFileCount,
    /// Files outside L0 below `l1_target_bytes` reached their soft limit.
    SmallFiles,
    /// An L0 file under publication pressure.
    L0Debt,
    /// An ordinary full-size group promoted to the next level.
    Promotion,
    /// Small files outside L0 carrying single-target delete files, once those
    /// reach `small_file_target_deletes` table-wide with
    /// [`DeleteLayout::PerTarget`].
    TargetDeletes,
}

impl PlanReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeletedRows => "deleted_rows",
            Self::DeleteFileCount => "delete_file_count",
            Self::SmallFiles => "small_files",
            Self::L0Debt => "l0_debt",
            Self::Promotion => "promotion",
            Self::TargetDeletes => "target_deletes",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileCandidate {
    pub id: FileId,
    pub level: Level,
    pub size_bytes: u64,
    pub row_count: u64,
    pub deleted_rows: u64,
    pub delete_file_count: usize,
    /// Applicable delete files that apply to no other live data file, except
    /// those too large to merge. See [`Policy::settled_target_delete`].
    pub target_delete_files: usize,
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

/// A single-target position-delete file considered by
/// [`Policy::select_target_merges`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetDeleteFile {
    pub id: FileId,
    pub rows: u64,
    pub bytes: u64,
}

/// One live data file's delete files in a per-target consolidation.
#[derive(Debug, Clone)]
pub struct TargetDeletes {
    pub target: FileId,
    /// Existing files whose every position targets this data file.
    pub files: Vec<TargetDeleteFile>,
    /// Effective positions for this target staged from the selected shared inputs.
    pub new_rows: u64,
    /// Applicable shared delete files the consolidation leaves in place.
    pub other_files: usize,
    /// Whether the target is full-size, so its single-target files count
    /// toward `target_delete_files_max`. See [`Policy::full_size_target`].
    pub full_size: bool,
}

/// How [`DeleteLayout::PerTarget`] groups rewritten positions into files.
///
/// Only full-size data files get one delete file each. Positions on L0 and
/// small data files are pooled into shared range files, as with
/// [`DeleteLayout::Shared`]: those files are rewritten soon, and the next
/// rewrite or consolidation consumes a range, while one file per small data
/// file would add a delete file per epoch-sized output under skew. A range
/// ends before any full-size data file between its targets by path, so no
/// full-size file is charged for it, unless there are more than
/// [`TargetRanges::MAX_BREAKS`] full-size files; then ranges may span them,
/// as shared files do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetRanges {
    /// Live L0 and small data files.
    pub pooled: BTreeSet<FileId>,
    /// Live full-size data files.
    pub breaks: BTreeSet<FileId>,
    /// Data files at least this large are full-size:
    /// `target_delete_min_file_bytes`.
    pub full_size_bytes: u64,
}

impl TargetRanges {
    /// At most this many full-size files split a pooled range, so a pooled
    /// rewrite writes at most one more range than this.
    pub const MAX_BREAKS: usize = 7;

    /// Classify a data file a rewrite adds, such as its output.
    pub fn add(&mut self, id: FileId, bytes: u64) {
        if bytes >= self.full_size_bytes {
            self.breaks.insert(id);
        } else {
            self.pooled.insert(id);
        }
    }

    /// Whether positions on `next` may join the range that holds `previous`.
    pub fn continues(&self, previous: &FileId, next: &FileId) -> bool {
        use std::ops::Bound::Excluded;
        self.pooled.contains(previous)
            && self.pooled.contains(next)
            && previous < next
            && (self.breaks.len() > Self::MAX_BREAKS
                || self
                    .breaks
                    .range((Excluded(previous), Excluded(next)))
                    .next()
                    .is_none())
    }
}

/// Input a consolidation may still read after its shared inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeBudget {
    pub files: usize,
    pub bytes: u64,
    pub rows: u64,
}

impl MergeBudget {
    /// The budget left after reading `file`, if it fits.
    fn charge(self, file: &TargetDeleteFile) -> Option<Self> {
        Some(Self {
            files: self.files.checked_sub(1)?,
            bytes: self.bytes.checked_sub(file.bytes)?,
            rows: self.rows.checked_sub(file.rows)?,
        })
    }
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
    /// Data rewrites of keyed tables commit one remap per input file instead
    /// of rewriting every moved row's index entries.
    pub lazy_index_remap: bool,
    /// How delete-file count debt is paid. See [`DeleteDebt`].
    pub delete_debt: DeleteDebt,
    /// How rewritten v2 position deletes are laid out. See [`DeleteLayout`].
    pub delete_layout: DeleteLayout,
    /// With [`DeleteLayout::PerTarget`], rewrite small data files outside L0
    /// once this many single-target delete files apply to them table-wide.
    /// Such a rewrite moves few rows and removes their delete files instead
    /// of keeping one per small file.
    pub small_file_target_deletes: usize,
    /// With [`DeleteLayout::PerTarget`], the most single-target delete files
    /// full-size data files keep table-wide while one has more than one. At
    /// this count, delete-only consolidation merges the smallest of each such
    /// file's delete files until at most half as many remain. Files too large
    /// to merge do not count; see [`Policy::settled_target_delete`].
    pub target_delete_files_max: usize,
    /// With [`DeleteLayout::PerTarget`], data files outside L0 at least this
    /// large are full-size: they keep one delete file each. Positions on
    /// smaller and L0 files share range files; see [`TargetRanges`].
    pub target_delete_min_file_bytes: u64,
    /// Periodic maintenance materializes a table's remaps while more retired
    /// files than this keep one.
    pub remap_max_files: u64,
    /// ... or while a translation chain has more hops than this.
    pub remap_max_depth: u64,
    /// Index entries one periodic visit may rewrite while materializing.
    pub remap_cleanup_rows: u64,
}
/// Well below the state store's translation hop limit.
const MAX_REMAP_DEPTH: u64 = 1024;

impl Default for Policy {
    fn default() -> Self {
        Self {
            data_rewrite_scope: DataRewriteScope::All,
            // Start small-file compaction early but pause publication late:
            // soft debt only schedules background work, and fewer files keep
            // merge-on-read scans close to a compacted layout.
            l0_soft_files: 6,
            l0_hard_files: 32,
            stable_small_soft_files: 6,
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
            lazy_index_remap: true,
            delete_debt: DeleteDebt::Consolidate,
            delete_layout: DeleteLayout::Shared,
            small_file_target_deletes: 8,
            target_delete_files_max: 16,
            target_delete_min_file_bytes: 128 << 20,
            remap_max_files: 512,
            remap_max_depth: 8,
            remap_cleanup_rows: 32_768,
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
    /// Table-wide position-delete files other than deletion vectors. Shared
    /// and dangling files add planning work across targets.
    pub shared_delete_files: usize,
    /// Data files whose own deleted fraction reaches `deleted_rows_percent`.
    pub dense_delete_files: usize,
    /// With [`DeleteLayout::PerTarget`], single-target delete files on small
    /// data files outside L0. Reaching `small_file_target_deletes` is soft
    /// debt that a data rewrite of those files pays.
    pub small_target_delete_files: usize,
    /// With [`DeleteLayout::PerTarget`], delete files that apply to exactly
    /// one live full-size data file, table-wide ...
    pub target_delete_files: usize,
    /// ... and how many of them are not their data file's only one.
    pub excess_target_delete_files: usize,
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
    /// Build a lazy index remap instead of per-row mappings (keyed tables).
    pub lazy_index_remap: bool,
    /// Write residual position deletes one file per target data file
    /// ([`DeleteLayout::PerTarget`]) instead of in shared ranges.
    pub per_target_deletes: bool,
    /// With `per_target_deletes`, which residual targets share ranges.
    pub target_ranges: TargetRanges,
    /// Why this group was selected, for attribution only.
    pub reason: PlanReason,
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
            || self.small_file_target_deletes == 0
            || self.target_delete_files_max == 0
            || self.target_delete_min_file_bytes == 0
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
        if self.remap_max_depth == 0
            || self.remap_max_depth > MAX_REMAP_DEPTH
            || self.remap_cleanup_rows == 0
        {
            return Err(Error::InvalidPolicy(
                "remap_max_depth must be 1..=1024 and remap_cleanup_rows positive",
            ));
        }
        Ok(())
    }

    /// Bounds of one periodic remap cleanup pass.
    pub fn remap_limits(&self) -> flow_state_store::RemapLimits {
        flow_state_store::RemapLimits {
            max_files: self.remap_max_files,
            max_depth: self.remap_max_depth,
            max_rows: self.remap_cleanup_rows,
        }
    }

    pub fn debt(&self, files: &[FileCandidate]) -> Result<Debt> {
        self.validate()?;
        let mut debt = Debt {
            l0_files: 0,
            small_files: 0,
            l0_bytes: 0,
            oldest_l0_ms: 0,
            max_delete_files: 0,
            shared_delete_files: 0,
            dense_delete_files: 0,
            small_target_delete_files: 0,
            target_delete_files: 0,
            excess_target_delete_files: 0,
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
            if self.delete_layout == DeleteLayout::PerTarget
                && file.level != Level::L0
                && !self.full_size_target(file)
            {
                debt.small_target_delete_files += file.target_delete_files;
            }
            debt.max_delete_files = debt.max_delete_files.max(file.delete_file_count);
            if self.delete_layout == DeleteLayout::PerTarget && self.full_size_target(file) {
                debt.target_delete_files += file.target_delete_files;
                debt.excess_target_delete_files += file.target_delete_files.saturating_sub(1);
            }
            if file.row_count > 0 {
                let dense = self.needs_delete_reclamation(file);
                dense_deletes |= dense;
                debt.dense_delete_files += usize::from(dense);
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
            || self.small_target_debt(&debt)
            || self.over_target_budget(&debt)
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

    /// [`Self::debt`] including the table-wide count of shared (non-vector)
    /// position-delete files, to which the same soft and hard limits apply.
    pub fn debt_with_shared_deletes(
        &self,
        files: &[FileCandidate],
        shared_delete_files: usize,
    ) -> Result<Debt> {
        let mut debt = self.debt(files)?;
        debt.shared_delete_files = shared_delete_files;
        if shared_delete_files >= self.delete_files_hard {
            debt.pressure = PublicationPressure::Pause;
        } else if shared_delete_files >= self.delete_files_soft {
            debt.pressure = debt.pressure.max(PublicationPressure::Delay);
        }
        Ok(debt)
    }

    /// Whether single-target delete files on small data files outside L0 are
    /// due for a data rewrite of those files. See [`PlanReason::TargetDeletes`].
    pub fn small_target_debt(&self, debt: &Debt) -> bool {
        self.delete_layout == DeleteLayout::PerTarget
            && debt.small_target_delete_files >= self.small_file_target_deletes
    }

    /// Whether a data file is full-size: outside L0 and at least
    /// `target_delete_min_file_bytes`. Rewriting it costs too much to pay its
    /// delete-file count, so with [`DeleteLayout::PerTarget`] it keeps its
    /// own delete files, they count toward `target_delete_files_max`, and
    /// consolidation merges them. Smaller files share ranges and are
    /// rewritten instead; see [`TargetRanges`] and [`PlanReason::TargetDeletes`].
    pub fn full_size_target(&self, file: &FileCandidate) -> bool {
        file.level != Level::L0 && file.size_bytes >= self.target_delete_min_file_bytes
    }

    /// Whether a single-target delete file is too large to count toward the
    /// per-target limits: above half of `max_delete_input_rows` or
    /// `max_delete_input_bytes`, so two would not fit one consolidation.
    pub fn settled_target_delete(&self, rows: u64, bytes: u64) -> bool {
        rows > self.max_delete_input_rows / 2 || bytes > self.max_delete_input_bytes / 2
    }

    /// Whether full-size data files keep `target_delete_files_max`
    /// single-target delete files while one has more than one, so that
    /// consolidation can lower the count by merging a target's files.
    pub fn over_target_budget(&self, debt: &Debt) -> bool {
        self.delete_layout == DeleteLayout::PerTarget
            && debt.target_delete_files >= self.target_delete_files_max
            && debt.excess_target_delete_files > 0
    }

    /// Pressure from delete-file count alone: the most delete files applying
    /// to one data file, or the table-wide shared delete-file count. With
    /// [`DeleteLayout::PerTarget`], [`Self::over_target_budget`] is soft.
    pub fn delete_file_pressure(&self, debt: &Debt) -> PublicationPressure {
        let count = debt.max_delete_files.max(debt.shared_delete_files);
        if count >= self.delete_files_hard {
            PublicationPressure::Pause
        } else if count >= self.delete_files_soft || self.over_target_budget(debt) {
            PublicationPressure::Delay
        } else {
            PublicationPressure::Healthy
        }
    }

    /// Pressure from L0 and small-file limits alone, excluding delete debt.
    pub fn layout_pressure(&self, debt: &Debt) -> PublicationPressure {
        if debt.l0_files >= self.l0_hard_files
            || debt.small_files >= self.stable_small_hard_files
            || debt.l0_bytes >= self.l0_hard_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_hard_ms
        {
            PublicationPressure::Pause
        } else if debt.l0_files >= self.l0_soft_files
            || debt.small_files >= self.stable_small_soft_files
            || debt.l0_bytes >= self.l0_soft_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_soft_ms
        {
            PublicationPressure::Delay
        } else {
            PublicationPressure::Healthy
        }
    }

    /// The policy for a table of `version`. A v3 table keeps at most one
    /// deletion vector per data file, merged at publication, so a delete-only
    /// consolidation can repack Puffin objects but never lowers a file's
    /// delete-file count. Only a data rewrite pays that debt, so v3 tables
    /// keep [`DeleteDebt::Current`]. Their vectors are already per target, so
    /// they also keep [`DeleteLayout::Shared`] for legacy position deletes.
    pub fn for_format_version(&self, version: FormatVersion) -> Cow<'_, Self> {
        if version >= FormatVersion::V3
            && (self.delete_debt != DeleteDebt::Current
                || self.delete_layout != DeleteLayout::Shared)
        {
            Cow::Owned(Self {
                delete_debt: DeleteDebt::Current,
                delete_layout: DeleteLayout::Shared,
                ..self.clone()
            })
        } else {
            Cow::Borrowed(self)
        }
    }

    /// Whether a delete-only consolidation should pay this debt before any
    /// data rewrite: only with [`DeleteDebt::Consolidate`], once delete-file
    /// count reaches its soft limit.
    pub fn consolidates_delete_debt(&self, debt: &Debt) -> bool {
        self.delete_debt == DeleteDebt::Consolidate
            && self.delete_file_pressure(debt) != PublicationPressure::Healthy
    }

    /// The [`TargetRanges`] of a [`DeleteLayout::PerTarget`] rewrite of
    /// `selected`, over the live data files outside it.
    pub fn target_ranges(
        &self,
        files: &[FileCandidate],
        selected: &BTreeSet<FileId>,
    ) -> TargetRanges {
        let mut ranges = TargetRanges {
            full_size_bytes: self.target_delete_min_file_bytes,
            ..TargetRanges::default()
        };
        for file in files.iter().filter(|file| !selected.contains(&file.id)) {
            if self.full_size_target(file) {
                ranges.breaks.insert(file.id.clone());
            } else {
                ranges.pooled.insert(file.id.clone());
            }
        }
        ranges
    }

    /// The limits that set `debt.pressure`, for attribution. Empty when healthy.
    pub fn pressure_reasons(&self, debt: &Debt) -> Vec<&'static str> {
        let hard = match debt.pressure {
            PublicationPressure::Healthy => return Vec::new(),
            PublicationPressure::Delay => false,
            PublicationPressure::Pause => true,
        };
        let deletes = self.delete_file_pressure(debt);
        [
            (
                "l0_files",
                debt.l0_files >= self.l0_soft_files,
                debt.l0_files >= self.l0_hard_files,
            ),
            (
                "l0_bytes",
                debt.l0_bytes >= self.l0_soft_bytes,
                debt.l0_bytes >= self.l0_hard_bytes,
            ),
            (
                "oldest_l0",
                debt.oldest_l0_ms >= self.oldest_l0_soft_ms,
                debt.oldest_l0_ms >= self.oldest_l0_hard_ms,
            ),
            (
                "small_files",
                debt.small_files >= self.stable_small_soft_files,
                debt.small_files >= self.stable_small_hard_files,
            ),
            (
                "delete_file_count",
                deletes != PublicationPressure::Healthy,
                deletes == PublicationPressure::Pause,
            ),
            ("deleted_rows", debt.dense_delete_files > 0, false),
            ("target_deletes", self.small_target_debt(debt), false),
        ]
        .into_iter()
        .filter(|&(_, soft, hard_limit)| if hard { hard_limit } else { soft })
        .map(|(reason, ..)| reason)
        .collect()
    }

    /// Return at most one partition-local group. Prioritizes deletion debt, then
    /// old L0 files, then full-sized L1 groups. Singleton rewrites are permitted
    /// for delete debt; old singleton L0 files become L1 to retire age debt.
    ///
    /// With [`DeleteDebt::Consolidate`], delete-file count alone does not make
    /// a file a rewrite candidate; the delete-only consolidation pays it. Only
    /// when no other group exists while one file or the dependency set is at
    /// the hard delete-file limit does planning fall back to count-driven
    /// selection, so debt that consolidation cannot reduce still makes progress.
    pub fn plan(
        &self,
        base_snapshot_id: i64,
        schema_id: i32,
        files: &[FileCandidate],
        deletes: &[DeleteDependency],
    ) -> Result<Option<CompactionPlan>> {
        let count_driven = self.delete_debt == DeleteDebt::Current;
        let plan = self.plan_with(base_snapshot_id, schema_id, files, deletes, count_driven)?;
        if plan.is_none()
            && !count_driven
            && self
                .debt(files)?
                .max_delete_files
                .max(self.table_delete_files(deletes))
                >= self.delete_files_hard
        {
            return self.plan_with(base_snapshot_id, schema_id, files, deletes, true);
        }
        Ok(plan)
    }

    /// Delete files charged table-wide by planning: every dependency with the
    /// shared layout, but only those applying to more than one data file with
    /// [`DeleteLayout::PerTarget`], whose single-target files are charged to
    /// their target's own delete-file count instead.
    fn table_delete_files(&self, deletes: &[DeleteDependency]) -> usize {
        match self.delete_layout {
            DeleteLayout::Shared => deletes.len(),
            DeleteLayout::PerTarget => deletes
                .iter()
                .filter(|delete| delete.targets.len() > 1)
                .count(),
        }
    }

    /// Most delete files one data file keeps after a per-target
    /// consolidation, its new output included: half of `delete_files_soft`,
    /// so CDC can add as many shared files again before the next one is due.
    pub fn target_delete_files(&self) -> usize {
        (self.delete_files_soft / 2).max(1)
    }

    /// Choose which existing single-target files a per-target consolidation
    /// merges into each target's new output, within `budget`. `targets` lists
    /// every data file that has single-target files or new rows.
    ///
    /// Per target, files are taken smallest first while their rows total at
    /// most the target's new rows, so the rewrite stays proportional to new
    /// deletes: a large accumulated file is merged only once new deletes reach
    /// its size. Then, while the target would keep more than
    /// [`Self::target_delete_files`], the next smallest are taken too. A lone
    /// file without new rows is never rewritten, since that would not lower
    /// its target's count. Targets with the most applicable files are served
    /// first; a file that exceeds the remaining budget ends its target's
    /// selection.
    ///
    /// Finally, if full-size targets would keep `target_delete_files_max` or
    /// more single-target files, the merge that removes one more of their
    /// files for the fewest rows read is added until at most half that many
    /// remain or the budget is spent, so the smallest files go first and the
    /// next runs need not merge again.
    pub fn select_target_merges(
        &self,
        targets: &[TargetDeletes],
        mut budget: MergeBudget,
    ) -> BTreeSet<FileId> {
        let cap = self.target_delete_files();
        let mut order = targets.iter().collect::<Vec<_>>();
        order.sort_by(|a, b| {
            (b.files.len() + b.other_files)
                .cmp(&(a.files.len() + a.other_files))
                .then_with(|| a.target.cmp(&b.target))
        });
        // Each target's files, smallest first, and how many are merged.
        let mut plans = Vec::with_capacity(order.len());
        for target in order {
            let mut files = target.files.iter().collect::<Vec<_>>();
            files.sort_by(|a, b| a.rows.cmp(&b.rows).then_with(|| a.id.cmp(&b.id)));
            let mut take = 0;
            let mut merged = 0u64;
            while let Some(file) = files.get(take) {
                match merged.checked_add(file.rows) {
                    Some(rows) if rows <= target.new_rows => {
                        merged = rows;
                        take += 1;
                    }
                    _ => break,
                }
            }
            let remaining = |take: usize| {
                files.len() - take
                    + usize::from(target.new_rows > 0 || take > 0)
                    + target.other_files
            };
            while take < files.len() && remaining(take) > cap {
                take += 1;
            }
            let mut next = budget;
            let mut admitted = 0;
            for file in &files[..take] {
                let Some(charged) = next.charge(file) else {
                    break;
                };
                next = charged;
                admitted += 1;
            }
            if admitted == 1 && target.new_rows == 0 {
                admitted = 0;
            } else {
                budget = next;
            }
            plans.push((target, files, admitted));
        }
        // Single-target files left once the selected merges are written.
        // Settled files neither count nor merge for the table-wide limit.
        let settled = |file: &TargetDeleteFile| self.settled_target_delete(file.rows, file.bytes);
        let kept = |target: &TargetDeletes, files: &[&TargetDeleteFile], take: usize| {
            files[take..].iter().filter(|file| !settled(file)).count()
                + usize::from(target.new_rows > 0 || take > 0)
        };
        let mut total = plans
            .iter()
            .filter(|(target, ..)| target.full_size)
            .map(|(target, files, take)| kept(target, files, *take))
            .sum::<usize>();
        // The files that remove one more of a target's files: the next one
        // once it has an output, else its two smallest.
        let next_merge = |target: &TargetDeletes, files: &[&TargetDeleteFile], take: usize| {
            let count = if target.new_rows > 0 || take > 0 {
                1
            } else {
                2
            };
            let next = files.get(take..take + count)?;
            if next.iter().any(|file| settled(file)) {
                return None;
            }
            let rows = next
                .iter()
                .map(|file| file.rows)
                .fold(0u64, u64::saturating_add);
            Some((rows, count))
        };
        let mut queue = std::collections::BinaryHeap::new();
        for (index, (target, files, take)) in plans.iter().enumerate() {
            if let Some((rows, count)) = next_merge(target, files, *take)
                && target.full_size
            {
                queue.push(std::cmp::Reverse((rows, index, count)));
            }
        }
        let goal = if total >= self.target_delete_files_max {
            self.target_delete_files_max / 2
        } else {
            total
        };
        while total > goal {
            let Some(std::cmp::Reverse((_, index, count))) = queue.pop() else {
                break;
            };
            let (target, files, take) = &mut plans[index];
            let Some(charged) = files[*take..*take + count]
                .iter()
                .try_fold(budget, |budget, file| budget.charge(file))
            else {
                continue;
            };
            budget = charged;
            *take += count;
            total -= 1;
            if let Some((rows, count)) = next_merge(target, files, *take) {
                queue.push(std::cmp::Reverse((rows, index, count)));
            }
        }
        plans
            .into_iter()
            .flat_map(|(_, files, take)| files.into_iter().take(take))
            .map(|file| file.id.clone())
            .collect()
    }

    fn plan_with(
        &self,
        base_snapshot_id: i64,
        schema_id: i32,
        files: &[FileCandidate],
        deletes: &[DeleteDependency],
        count_driven: bool,
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
            (count_driven && f.delete_file_count >= self.delete_files_soft)
                || self.needs_delete_reclamation(f)
        };
        let needs_small_file_rewrite = |file: &FileCandidate| {
            debt.small_files >= self.stable_small_soft_files
                && file.level != Level::L0
                && file.size_bytes < self.l1_target_bytes
        };
        // With the per-target layout, a small file's single-target deletes are
        // cheaper to remove by rewriting its few rows than to keep in one more
        // file per small file. L0 files are already rewritten for L0 debt.
        let target_debt = self.small_target_debt(&debt);
        let per_target = self.delete_layout == DeleteLayout::PerTarget;
        let needs_target_rewrite = |file: &FileCandidate| {
            target_debt
                && file.level != Level::L0
                && !self.full_size_target(file)
                && file.target_delete_files > 0
        };
        // Under delete pressure, retiring effective positions can remove more
        // residual files than another small L0 rewrite. Hard L0 limits retain
        // their existing ordering so reclamation cannot postpone that debt.
        let hard_l0_debt = debt.l0_files >= self.l0_hard_files
            || debt.l0_bytes >= self.l0_hard_bytes
            || debt.oldest_l0_ms >= self.oldest_l0_hard_ms;
        let prioritize_reclamation = (self.table_delete_files(deletes) >= self.delete_files_soft
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
                        Level::L2 => {
                            needs_delete_rewrite(file)
                                || needs_small_file_rewrite(file)
                                || needs_target_rewrite(file)
                        }
                    }
            })
            .collect();
        eligible.sort_by(|a, b| {
            (hard_l0_debt && b.level == Level::L0)
                .cmp(&(hard_l0_debt && a.level == Level::L0))
                .then_with(|| needs_delete_rewrite(b).cmp(&needs_delete_rewrite(a)))
                .then_with(|| needs_small_file_rewrite(b).cmp(&needs_small_file_rewrite(a)))
                .then_with(|| needs_target_rewrite(b).cmp(&needs_target_rewrite(a)))
                .then_with(|| {
                    if needs_target_rewrite(a) && needs_target_rewrite(b) {
                        b.target_delete_files.cmp(&a.target_delete_files)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
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
            let small_group = needs_small_file_rewrite(seed);
            // Any carrier may be rewritten alone: its delete files go away.
            let target_group =
                !small_group && !needs_delete_rewrite(seed) && needs_target_rewrite(seed);
            let forced = needs_delete_rewrite(seed)
                || target_group
                || (seed.level == Level::L0 && debt.pressure != PublicationPressure::Healthy);
            let reason = if self.needs_delete_reclamation(seed) {
                PlanReason::DeletedRows
            } else if needs_delete_rewrite(seed) {
                PlanReason::DeleteFileCount
            } else if small_group {
                PlanReason::SmallFiles
            } else if target_group {
                PlanReason::TargetDeletes
            } else if forced {
                PlanReason::L0Debt
            } else {
                PlanReason::Promotion
            };
            // A stable file selected for its own deleted rows must not pull in
            // mostly-live companions. L0 debt keeps its existing grouping.
            let dense_only =
                !count_driven && reason == PlanReason::DeletedRows && seed.level != Level::L0;
            let useful = |count, bytes| {
                forced
                    || count >= self.min_group_files
                        && (seed.level != Level::L1 || bytes >= target_file_bytes || small_group)
            };
            // Small-file and target-delete groups take only their own kind,
            // across levels outside L0 and of mixed small sizes.
            let member = |file: &FileCandidate| {
                small_group && needs_small_file_rewrite(file)
                    || target_group && needs_target_rewrite(file)
            };
            let companions = eligible.iter().copied().filter(|file| {
                if file.id == seed.id
                    || dense_only && !self.needs_delete_reclamation(file)
                    || (small_group || target_group) && !member(file)
                    || (file.level != seed.level && !member(file))
                    || file.spec_id != seed.spec_id
                    || file.partition != seed.partition
                {
                    return false;
                }
                // Urgency permits a singleton, not unrelated large companions.
                // File-count consolidation admits mixed small sizes.
                let smallest = seed.size_bytes.min(file.size_bytes).max(1);
                small_group
                    || target_group
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
                    let target_ranges = if per_target {
                        self.target_ranges(files, &selected)
                    } else {
                        TargetRanges::default()
                    };
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
                        lazy_index_remap: self.lazy_index_remap,
                        per_target_deletes: self.delete_layout == DeleteLayout::PerTarget,
                        target_ranges,
                        reason,
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

mod columnar;
mod parquet_scan;
pub use parquet_scan::{ReadLimits, read_positions};
mod catchup;
pub use catchup::{CatchUpRejected, catch_up};
mod worker;
pub use worker::{
    BuiltData, DeleteReadLimits, LiveBatch, WorkerOutput, build_data, compact, scan_live_files,
    stage_delete_files, stage_position_deletes,
};
