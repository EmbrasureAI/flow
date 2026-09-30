//! Local state volume safety: the free-space budget, the capture watermark, and
//! RocksDB file descriptor limits.
use crate::config::Config;
use std::{
    collections::BTreeSet,
    fs, io,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// Free bytes on the volume holding a path. Tests inject fixed values.
pub(crate) type SpaceProbe = dyn Fn(&Path) -> io::Result<u64> + Send + Sync;

/// A rebuild writes a complete new index generation beside the active one.
const INDEX_HEADROOM_MIN: u64 = 1 << 30;
const GIB: f64 = (1u64 << 30) as f64;
const FULL_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Free bytes available to this process on the volume holding `path`, or on
/// its nearest existing ancestor before initialization creates it.
pub(crate) fn available_space(path: &Path) -> io::Result<u64> {
    let mut probe = path;
    loop {
        match fs2::available_space(probe) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                probe = match probe.parent() {
                    Some(parent) if !parent.as_os_str().is_empty() => parent,
                    _ => Path::new("."),
                };
            }
            result => return result,
        }
    }
}

/// Space the configured state still needs beyond what it already occupies.
pub(crate) struct Budget {
    pub available: u64,
    /// Unused `limits.journal_bytes` quota.
    pub journal: u64,
    /// Unused `limits.spool_bytes` quota.
    pub spool: u64,
    /// A rebuild's second copy of the largest index generation.
    pub index: u64,
    /// `storage.min_free_bytes`, below which capture pauses.
    pub reserve: u64,
}

impl Budget {
    pub fn measure(config: &Config, probe: &SpaceProbe) -> io::Result<Self> {
        let state = &config.state_dir;
        let largest_generation = std::iter::once(state.join("index"))
            .chain(
                fs::read_dir(state.join("index-generations"))
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok().map(|entry| entry.path())),
            )
            .map(|path| directory_bytes(&path))
            .max()
            .unwrap_or(0);
        Ok(Self {
            available: probe(state)?,
            journal: config
                .limits
                .journal_bytes
                .saturating_sub(directory_bytes(&state.join("journal"))),
            spool: config
                .limits
                .spool_bytes
                .saturating_sub(directory_bytes(&state.join("spool"))),
            index: largest_generation.max(INDEX_HEADROOM_MIN),
            reserve: config.storage.min_free_bytes,
        })
    }

    pub fn required(&self) -> u64 {
        self.journal
            .saturating_add(self.spool)
            .saturating_add(self.index)
            .saturating_add(self.reserve)
    }

    pub fn sufficient(&self) -> bool {
        self.available >= self.required()
    }

    pub fn describe(&self) -> String {
        format!(
            "state volume has {:.1} GiB free; unused journal quota {:.1} GiB + spool quota {:.1} GiB + index rebuild headroom {:.1} GiB + storage.min_free_bytes {:.1} GiB = {:.1} GiB",
            self.available as f64 / GIB,
            self.journal as f64 / GIB,
            self.spool as f64 / GIB,
            self.index as f64 / GIB,
            self.reserve as f64 / GIB,
            self.required() as f64 / GIB,
        )
    }
}

/// Startup warning only: quotas are ceilings, and capture pauses before the
/// volume fills. Measurement failures never prevent a start.
pub(crate) fn warn_if_insufficient(config: &Config) {
    match Budget::measure(config, &available_space) {
        Ok(budget) => {
            metrics::gauge!("flow_state_volume_available_bytes").set(budget.available as f64);
            if budget.sufficient() {
                tracing::info!(event = "state_volume_budget", "{}", budget.describe());
            } else {
                tracing::warn!(
                    event = "state_volume_budget",
                    "{}; the volume may fill before the journal or spool quota is reached",
                    budget.describe()
                );
            }
        }
        Err(error) => tracing::warn!(%error, "could not measure free space on the state volume"),
    }
}

/// `check`: report the budget. A shortfall is a warning, not a failure.
pub(crate) fn check(config: &Config) {
    match Budget::measure(config, &available_space) {
        Ok(budget) if budget.sufficient() => println!("{}", budget.describe()),
        Ok(budget) => println!(
            "warning: {}; the volume may fill before the journal or spool quota is reached",
            budget.describe()
        ),
        Err(error) => {
            println!("warning: could not measure free space on the state volume: {error}")
        }
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_bytes(&entry.path()),
            Ok(kind) if kind.is_file() => entry.metadata().map_or(0, |metadata| metadata.len()),
            _ => 0,
        })
        .sum()
}

/// Whether capture must pause: free space is below the configured watermark.
/// A failed measurement does not pause capture.
pub(crate) fn capture_should_pause(probe: &SpaceProbe, state_dir: &Path, min_free: u64) -> bool {
    if min_free == 0 {
        return false;
    }
    let Some(available) = measure(probe, state_dir) else {
        return false;
    };
    if available < min_free {
        tracing::warn!(
            available_bytes = available,
            min_free_bytes = min_free,
            "state volume free space is below storage.min_free_bytes; pausing capture"
        );
    }
    available < min_free
}

/// Free bytes on the state volume, also exported as a gauge.
pub(crate) fn measure(probe: &SpaceProbe, state_dir: &Path) -> Option<u64> {
    match probe(state_dir) {
        Ok(available) => {
            metrics::gauge!("flow_state_volume_available_bytes").set(available as f64);
            Some(available)
        }
        Err(error) => {
            tracing::warn!(%error, "could not measure free space on the state volume");
            None
        }
    }
}

/// Resume a quarter above the watermark so capture does not flap around it.
pub(crate) fn resume_bytes(min_free: u64) -> u64 {
    min_free.saturating_add(min_free / 4)
}

static SPACE_PAUSED: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// Held while capture for a state directory is paused for free space. Optional
/// maintenance and checkpoints must not consume the remaining reserve.
pub(crate) struct SpacePause(PathBuf);

impl SpacePause {
    pub fn start(state_dir: &Path) -> Self {
        let state_dir = state_dir.to_owned();
        SPACE_PAUSED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(state_dir.clone());
        metrics::gauge!("flow_capture_disk_low").set(1.0);
        Self(state_dir)
    }
}

impl Drop for SpacePause {
    fn drop(&mut self) {
        SPACE_PAUSED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
        metrics::gauge!("flow_capture_disk_low").set(0.0);
    }
}

pub(crate) fn capture_paused_for_space(state_dir: &Path) -> bool {
    SPACE_PAUSED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(state_dir)
}

/// A full volume, as opposed to a failure that indicates damaged state.
pub(crate) fn is_storage_full(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
            )
        })
    })
}

/// Rate-limited warning for skipped non-authoritative observation files.
pub(crate) fn warn_observation_skipped(error: &anyhow::Error) {
    static LAST: Mutex<Option<Instant>> = Mutex::new(None);
    metrics::counter!("flow_observation_write_skipped_total").increment(1);
    let Ok(mut last) = LAST.lock() else { return };
    if last.is_none_or(|at| at.elapsed() >= FULL_WARNING_INTERVAL) {
        *last = Some(Instant::now());
        tracing::warn!(error = %format!("{error:#}"), "state volume is full; status.json/metrics.prom not updated");
    }
}

/// RocksDB descriptor cap for each row index store, derived once per process
/// after raising the soft `RLIMIT_NOFILE` toward its hard limit. The cap is a
/// cache bound, not a reservation: the active index (or a rebuild candidate and
/// its scratch store) shares the limit with short-lived per-table compaction and
/// reconcile scratch stores, which stay small, and with control (256), journal
/// segments and network sockets.
pub(crate) fn index_max_open_files() -> i32 {
    static BUDGET: OnceLock<i32> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        let limit = raise_descriptor_limit();
        let budget = limit.map_or(4096, |limit| (limit / 4).clamp(64, 8192));
        tracing::info!(
            event = "file_descriptor_budget",
            nofile_soft_limit = limit,
            index_max_open_files = budget,
            "RocksDB file descriptor budget"
        );
        i32::try_from(budget).expect("clamped descriptor budget")
    })
}

#[cfg(unix)]
fn raise_descriptor_limit() -> Option<u64> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    // Beyond this, a larger limit only costs kernel memory.
    const TARGET: u64 = 1 << 20;
    // macOS rejects soft limits above OPEN_MAX even when the hard limit is
    // unlimited; its default soft limit is 256.
    const PORTABLE: u64 = 10_240;
    let limit = getrlimit(Resource::Nofile);
    let current = limit.current.unwrap_or(u64::MAX);
    let target = limit.maximum.unwrap_or(u64::MAX).min(TARGET);
    if current >= target {
        return Some(current);
    }
    for candidate in [target, target.min(PORTABLE)] {
        if candidate <= current {
            break;
        }
        let raised = Rlimit {
            current: Some(candidate),
            maximum: limit.maximum,
        };
        if setrlimit(Resource::Nofile, raised).is_ok() {
            tracing::info!(
                from = current,
                to = candidate,
                "raised the soft open-file limit"
            );
            return Some(candidate);
        }
    }
    tracing::warn!(
        soft_limit = current,
        hard_limit = limit.maximum,
        "could not raise the soft open-file limit; large indexes reopen table files more often"
    );
    Some(current)
}

#[cfg(not(unix))]
fn raise_descriptor_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(state_dir: PathBuf) -> Config {
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = state_dir;
        config.limits.journal_bytes = 10 << 30;
        config.limits.spool_bytes = 5 << 30;
        config.storage.min_free_bytes = 2 << 30;
        config
    }

    #[test]
    fn budget_counts_unused_quotas_rebuild_headroom_and_reserve() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path().join("state"));
        let empty = Budget::measure(&config, &|_: &Path| Ok(18 << 30)).unwrap();
        assert_eq!(empty.required(), (10 + 5 + 1 + 2) << 30);
        assert!(empty.sufficient());

        let generation = config.state_dir.join("index-generations").join("index-a");
        fs::create_dir_all(&generation).unwrap();
        fs::write(generation.join("000001.sst"), vec![0; 4096]).unwrap();
        fs::create_dir_all(config.state_dir.join("journal")).unwrap();
        fs::write(config.state_dir.join("journal").join("segment"), [0; 100]).unwrap();
        let used = Budget::measure(&config, &|_: &Path| Ok(18 << 30)).unwrap();
        assert_eq!(used.journal, (10 << 30) - 100);
        assert_eq!(used.index, INDEX_HEADROOM_MIN);
        assert!(used.sufficient());
        let short = Budget::measure(&config, &|_: &Path| Ok(17 << 30)).unwrap();
        assert!(!short.sufficient());
        assert!(short.describe().contains("17.0 GiB free"));
    }

    #[test]
    fn available_space_uses_the_nearest_existing_ancestor() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("not").join("created");
        available_space(&missing).unwrap();
        available_space(Path::new("relative-missing-state")).unwrap();
    }

    #[test]
    fn watermark_pauses_below_and_resumes_a_quarter_above() {
        let dir = Path::new("/state");
        let low = |_: &Path| Ok(99);
        let at = |_: &Path| Ok(100);
        let resumed = |_: &Path| Ok(125);
        let failed = |_: &Path| Err(io::Error::other("statvfs"));
        assert!(capture_should_pause(&low, dir, 100));
        assert!(!capture_should_pause(&at, dir, 100));
        assert!(!capture_should_pause(&low, dir, 0));
        assert!(!capture_should_pause(&failed, dir, 100));
        assert_eq!(measure(&resumed, dir), Some(125));
        assert_eq!(measure(&failed, dir), None);
        assert_eq!(resume_bytes(100), 125);
        let other = Path::new("/other-state");
        let pause = SpacePause::start(dir);
        assert!(capture_paused_for_space(dir));
        assert!(!capture_paused_for_space(other));
        drop(pause);
        assert!(!capture_paused_for_space(dir));
    }

    #[test]
    fn only_full_volumes_are_classified_as_storage_full() {
        let full = anyhow::Error::new(io::Error::from(io::ErrorKind::StorageFull))
            .context("write status.json");
        assert!(is_storage_full(&full));
        let quota = anyhow::Error::new(io::Error::from(io::ErrorKind::QuotaExceeded));
        assert!(is_storage_full(&quota));
        let denied = anyhow::Error::new(io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(!is_storage_full(&denied));
    }

    #[test]
    fn descriptor_budget_is_bounded() {
        let budget = index_max_open_files();
        assert!((64..=8192).contains(&budget));
    }
}
