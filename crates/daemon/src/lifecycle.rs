use crate::{config::Config, runtime::blocked::BlockedTable};
use anyhow::{Context, Result};
use flow_coordinator::{SourceHealth, SourceLedger, Watermarks};
use flow_model::{PgLsn, TableId};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A local observation, never recovery authority. Readers need no database lock.
#[derive(Serialize, Deserialize)]
pub(crate) struct Status {
    source_id: String,
    process_id: u32,
    ready: bool,
    updated_at_ms: u64,
    watermarks: Watermarks,
    pending_transactions: usize,
    #[serde(default)]
    captured_durable_lsn: PgLsn,
    #[serde(default)]
    source_health: SourceHealthStatus,
    #[serde(default)]
    blocked_tables: Vec<BlockedTable>,
    #[serde(default)]
    table_progress: Vec<TableProgress>,
    #[serde(default)]
    state: ProcessState,
    /// Why the last process stopped; kept until a later process is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_error: Option<LastError>,
}

impl Status {
    /// Readiness written by this process, not a previous one that shared the
    /// state directory and exited within the expiry window.
    pub(crate) fn ready_in(&self, process_id: u32) -> bool {
        self.ready && self.process_id == process_id
    }

    pub(crate) fn ready(&self) -> bool {
        self.ready
    }
}

/// `unknown` identifies an observation written before this field existed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProcessState {
    #[default]
    Unknown,
    /// Recovery, index rebuild or source validation before the run loop.
    Starting,
    Running,
    Stopped,
}

/// A fatal error. Only Flow-generated operator-action messages are recorded;
/// other error chains can contain URLs or row values and stay in the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LastError {
    pub(crate) class: String,
    pub(crate) exit_code: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
    pub(crate) at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableProgress {
    pub(crate) table_id: TableId,
    pub(crate) materialized_lsn: PgLsn,
    #[serde(default)]
    pub(crate) source_namespace: Option<String>,
    #[serde(default)]
    pub(crate) source_table: Option<String>,
    /// The table's oldest registered, unpublished transaction, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) oldest_unpublished_lsn: Option<PgLsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) oldest_unpublished_commit_micros: Option<i64>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceHealthStatus {
    #[default]
    Unknown,
    Healthy,
    Warning,
    AtRisk,
    Unavailable,
    SlotLost,
    /// The publication no longer covers the capture contract; changes may
    /// have been skipped, so the process exits for resynchronization.
    PublicationChanged,
}

impl SourceHealthStatus {
    pub(crate) fn permits_readiness(self) -> bool {
        !matches!(
            self,
            Self::AtRisk | Self::Unavailable | Self::SlotLost | Self::PublicationChanged
        )
    }

    pub(crate) fn check_available(self) -> bool {
        !matches!(self, Self::Unknown | Self::Unavailable)
    }

    pub(crate) fn at_risk(self) -> bool {
        matches!(
            self,
            Self::AtRisk | Self::SlotLost | Self::PublicationChanged
        )
    }
}

impl From<SourceHealth> for SourceHealthStatus {
    fn from(health: SourceHealth) -> Self {
        match health {
            SourceHealth::Healthy => Self::Healthy,
            SourceHealth::Warning => Self::Warning,
            SourceHealth::AtRisk => Self::AtRisk,
            SourceHealth::SlotLost => Self::SlotLost,
        }
    }
}

pub(crate) struct Lifecycle {
    config: Config,
    source_health: SourceHealthStatus,
}
impl Lifecycle {
    /// A lost slot stays reported, and readiness withheld, until a later WAL
    /// check proves otherwise. The previous exit reason stays visible until
    /// this process is running.
    pub(crate) fn start(config: &Config) -> Result<Self> {
        let mut source_health = SourceHealthStatus::Unknown;
        update(config, |status| {
            if status.source_health == SourceHealthStatus::SlotLost {
                source_health = SourceHealthStatus::SlotLost;
            }
            status.ready = false;
            status.state = ProcessState::Starting;
            status.source_health = source_health;
        })?;
        Ok(Self {
            config: config.clone(),
            source_health,
        })
    }

    pub(crate) fn source_health(&self) -> SourceHealthStatus {
        self.source_health
    }
}
impl Drop for Lifecycle {
    fn drop(&mut self) {
        RUN_LOOP_PROGRESS_MS.store(0, Ordering::Relaxed);
        if let Err(error) = update(&self.config, |status| {
            status.ready = false;
            status.state = ProcessState::Stopped;
        }) {
            tracing::warn!(%error, "could not mark local status stopped");
        }
    }
}

/// Set once this process holds the state directory's database lock. A
/// process refused by the lock never writes an exit reason over the
/// observation of the process that holds it.
static STATE_LOCK_HELD: AtomicBool = AtomicBool::new(false);

pub(crate) fn state_lock_acquired() {
    STATE_LOCK_HELD.store(true, Ordering::Relaxed);
}

/// Record why `init` or `run` failed, after cleanup marked it stopped. Best
/// effort: an unwritable state directory keeps only the log.
pub(crate) fn record_exit(config: &Config, error: &anyhow::Error) {
    record_exit_as(config, error, STATE_LOCK_HELD.load(Ordering::Relaxed));
}

fn record_exit_as(config: &Config, error: &anyhow::Error, lock_held: bool) {
    if !lock_held {
        return;
    }
    let (class, message) = crate::exit::classify(error);
    let result = now_ms().and_then(|at_ms| {
        update(config, |status| {
            status.ready = false;
            status.state = ProcessState::Stopped;
            status.last_error = Some(LastError {
                class: class.name().to_owned(),
                exit_code: class.code(),
                message,
                at_ms,
            });
        })
    });
    if let Err(error) = result {
        tracing::warn!(%error, "could not record the exit reason in local status");
    }
}

/// A clean exit, such as a completed `init`, replaces an earlier failure.
pub(crate) fn record_clean_exit(config: &Config) {
    record_clean_exit_as(config, STATE_LOCK_HELD.load(Ordering::Relaxed));
}

fn record_clean_exit_as(config: &Config, lock_held: bool) {
    if !lock_held || !config.state_dir.join("status.json").exists() {
        return;
    }
    if let Err(error) = update(config, |status| {
        status.ready = false;
        status.state = ProcessState::Stopped;
        status.last_error = None;
    }) {
        tracing::warn!(%error, "could not record the clean exit in local status");
    }
}

/// Monotonic milliseconds of the run loop's latest iteration; zero outside
/// the run loop, so recovery, index rebuild and initial COPY never fail liveness.
static RUN_LOOP_PROGRESS_MS: AtomicU64 = AtomicU64::new(0);

/// Wall-clock steps must not fail liveness.
fn monotonic_ms() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    u64::try_from(
        BASE.get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
    .saturating_add(1)
}

pub(crate) fn run_loop_progress() {
    RUN_LOOP_PROGRESS_MS.store(monotonic_ms(), Ordering::Relaxed);
}

/// Serializes tests that start or drop a `Lifecycle`, which resets liveness.
#[cfg(test)]
pub(crate) static LIVENESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `Err` carries how long the run loop has not progressed beyond `timeout`.
pub(crate) fn liveness(timeout: Duration) -> std::result::Result<(), Duration> {
    let progress = RUN_LOOP_PROGRESS_MS.load(Ordering::Relaxed);
    stalled(progress, monotonic_ms(), timeout)
}

fn stalled(progress_ms: u64, now_ms: u64, timeout: Duration) -> std::result::Result<(), Duration> {
    let stalled = Duration::from_millis(now_ms.saturating_sub(progress_ms));
    if progress_ms == 0 || timeout.is_zero() || stalled <= timeout {
        Ok(())
    } else {
        Err(stalled)
    }
}

/// Persist a definite publication contract violation found before the capture
/// loop, like a lost slot, so a supervisor can identify it after the exit.
pub(crate) fn record_publication_changed<T>(config: &Config, result: Result<T>) -> Result<T> {
    if let Err(error) = &result
        && error.is::<crate::source::PublicationChanged>()
    {
        require_resync(config, &format!("{error:#}"));
        if let Err(status) = emit(
            config,
            None,
            None,
            false,
            Some(SourceHealthStatus::PublicationChanged),
            None,
        ) {
            tracing::warn!(error = %status, "could not record the publication contract violation");
        }
    }
    result
}

const RESYNC_MARKER: &str = "publication-resync-required.json";

/// Durable proof that this slot's capture may have skipped changes. Restoring
/// the publication cannot recover them, so only a resync may replace it.
#[derive(Serialize, Deserialize)]
struct ResyncRequired {
    source_id: String,
    slot: String,
    publication: String,
    reason: String,
}

fn resync_marker(config: &Config) -> Result<Option<ResyncRequired>> {
    match std::fs::read(config.state_dir.join(RESYNC_MARKER)) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("invalid resync-required marker")?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("resync-required marker is unreadable"),
    }
}

/// Write the marker before the fatal exit, on the journal's volume. The first
/// reason for a slot is kept. A failed write is logged; the exit still happens.
pub(crate) fn require_resync(config: &Config, reason: &str) {
    let result = (|| -> Result<()> {
        let postgres = &config.source;
        if resync_marker(config)?.is_some_and(|marker| marker.slot == postgres.slot) {
            return Ok(());
        }
        let marker = ResyncRequired {
            source_id: config.source.id.clone(),
            slot: postgres.slot.clone(),
            publication: postgres.publication.clone(),
            reason: reason.to_owned(),
        };
        let path = config.state_dir.join(RESYNC_MARKER);
        let temporary = path.with_extension("json.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&marker)?)?;
        file.sync_all()?;
        std::fs::rename(temporary, path)?;
        std::fs::File::open(&config.state_dir)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::error!(%error, "could not persist the resync-required marker");
    }
}

/// Refuse to resume a slot whose capture may have skipped changes, even if the
/// publication now validates. A resync uses a new slot, so only a marker for
/// the configured slot blocks. The marker is never cleared automatically.
pub(crate) fn refuse_if_resync_required(config: &Config) -> Result<()> {
    let postgres = &config.source;
    let Some(marker) = resync_marker(config)? else {
        return Ok(());
    };
    if marker.slot != postgres.slot {
        return Ok(());
    }
    if let Err(status) = emit(
        config,
        None,
        None,
        false,
        Some(SourceHealthStatus::PublicationChanged),
        None,
    ) {
        tracing::warn!(error = %status, "could not record the pending resynchronization");
    }
    Err(crate::exit::resync(format!(
        "replication slot {:?} requires resynchronization before capture can resume; a previous run stopped with: {}",
        marker.slot, marker.reason
    )))
}

pub(crate) fn emit(
    config: &Config,
    ledger: Option<&SourceLedger>,
    captured: Option<PgLsn>,
    ready: bool,
    source_health: Option<SourceHealthStatus>,
    tables: Option<(&[BlockedTable], &[TableProgress])>,
) -> Result<()> {
    update(config, |status| {
        status.ready = ready;
        if let Some(ledger) = ledger {
            status.watermarks = ledger.watermarks().clone();
            status.pending_transactions = ledger.pending_count();
        }
        if let Some(captured) = captured {
            status.captured_durable_lsn = captured;
        }
        if let Some(source_health) = source_health {
            status.source_health = source_health;
        }
        if let Some((blocked, progress)) = tables {
            status.blocked_tables = blocked.to_vec();
            status.table_progress = progress.to_vec();
        }
    })
}

/// The run loop's observation: the process is serving, so an earlier exit
/// reason no longer describes it.
pub(crate) fn emit_running(
    config: &Config,
    ledger: &SourceLedger,
    captured: PgLsn,
    ready: bool,
    source_health: SourceHealthStatus,
    tables: (&[BlockedTable], &[TableProgress]),
) -> Result<()> {
    update(config, |status| {
        status.ready = ready;
        status.watermarks = ledger.watermarks().clone();
        status.pending_transactions = ledger.pending_count();
        status.captured_durable_lsn = captured;
        status.source_health = source_health;
        status.blocked_tables = tables.0.to_vec();
        status.table_progress = tables.1.to_vec();
        status.state = ProcessState::Running;
        status.last_error = None;
    })
}

fn update(config: &Config, change: impl FnOnce(&mut Status)) -> Result<()> {
    let mut status = read(config).unwrap_or_else(|_| Status {
        source_id: config.source.id.clone(),
        process_id: std::process::id(),
        ready: false,
        updated_at_ms: 0,
        watermarks: Watermarks::default(),
        pending_transactions: 0,
        captured_durable_lsn: PgLsn(0),
        source_health: SourceHealthStatus::Unknown,
        blocked_tables: Vec::new(),
        table_progress: Vec::new(),
        state: ProcessState::Unknown,
        last_error: None,
    });
    change(&mut status);
    status.process_id = std::process::id();
    status.updated_at_ms = now_ms()?;
    write_observation(
        &config.state_dir.join("status.json"),
        serde_json::to_vec_pretty(&status)?,
    )
}

pub(crate) fn read(config: &Config) -> Result<Status> {
    let bytes = std::fs::read(config.state_dir.join("status.json"))
        .context("no local status observation; start the service first")?;
    let mut status: Status = serde_json::from_slice(&bytes)?;
    // A SIGKILL cannot clear readiness. Expire the observation after three
    // health intervals, retaining its timestamp and last measured progress.
    status.ready &= now_ms()?.saturating_sub(status.updated_at_ms) <= 15_000;
    Ok(status)
}

/// Atomic replacement. The temporary name is unique to this write, so a
/// concurrent writer (another process, even one with the same PID in another
/// container) cannot rename or truncate it.
pub(crate) fn write_observation(path: &Path, bytes: impl AsRef<[u8]>) -> Result<()> {
    let temporary = path.with_extension(format!(
        "{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

pub(crate) async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod status_tests {
    use super::*;

    fn config() -> (tempfile::TempDir, Config) {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().to_owned();
        (root, config)
    }

    fn status(config: &Config) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(config.state_dir.join("status.json")).unwrap())
            .unwrap()
    }

    #[test]
    fn liveness_judges_only_a_running_loop_past_its_timeout() {
        let timeout = Duration::from_secs(300);
        assert_eq!(stalled(0, 10_000_000, timeout), Ok(()));
        assert_eq!(stalled(1_000, 301_000, timeout), Ok(()));
        assert_eq!(
            stalled(1_000, 302_000, timeout),
            Err(Duration::from_secs(301))
        );
        assert_eq!(stalled(1_000, 10_000_000, Duration::ZERO), Ok(()));
        // A concurrent later progress is not a stall.
        assert_eq!(stalled(5_000, 1_000, timeout), Ok(()));
    }

    #[test]
    fn exit_reason_and_lost_slot_survive_restart_until_running() {
        let _serial = LIVENESS_TEST_LOCK.blocking_lock();
        let (_root, config) = config();
        let lifecycle = Lifecycle::start(&config).unwrap();
        assert_eq!(lifecycle.source_health(), SourceHealthStatus::Unknown);
        assert_eq!(status(&config)["state"], "starting");
        emit(
            &config,
            None,
            None,
            true,
            Some(SourceHealthStatus::SlotLost),
            None,
        )
        .unwrap();
        drop(lifecycle);
        record_exit_as(
            &config,
            &crate::exit::resync("replication slot lost WAL; resynchronization required")
                .context("health check"),
            true,
        );
        let stopped = status(&config);
        assert_eq!(stopped["state"], "stopped");
        assert_eq!(stopped["ready"], false);
        assert_eq!(stopped["source_health"], "slot_lost");
        assert_eq!(stopped["last_error"]["class"], "resync_required");
        assert_eq!(stopped["last_error"]["exit_code"], 79);
        assert_eq!(
            stopped["last_error"]["message"],
            "replication slot lost WAL; resynchronization required"
        );
        assert!(stopped["last_error"]["at_ms"].as_u64().unwrap() > 0);

        // A restart keeps the lost slot and the reason visible while starting.
        let lifecycle = Lifecycle::start(&config).unwrap();
        assert_eq!(lifecycle.source_health(), SourceHealthStatus::SlotLost);
        let starting = status(&config);
        assert_eq!(starting["state"], "starting");
        assert_eq!(starting["source_health"], "slot_lost");
        assert_eq!(starting["last_error"]["exit_code"], 79);

        // Unclassified errors never persist their text.
        record_exit_as(
            &config,
            &anyhow::anyhow!("postgres://flow:secret@db failed"),
            true,
        );
        let failed = status(&config);
        assert_eq!(failed["last_error"]["class"], "failure");
        assert_eq!(failed["last_error"]["exit_code"], 1);
        assert!(failed["last_error"].get("message").is_none());
        assert!(!serde_json::to_string(&failed).unwrap().contains("secret"));
        drop(lifecycle);

        // A healthy check clears the lost slot and a running loop the reason.
        let _lifecycle = Lifecycle::start(&config).unwrap();
        let store = flow_state_store::StateStore::open(
            config.state_dir.join("index"),
            flow_state_store::StateStoreOptions::default(),
        )
        .unwrap();
        let ledger = SourceLedger::open(
            store,
            flow_model::SourceId(config.source.id.clone()),
            flow_coordinator::AckMode::Materialized,
            flow_coordinator::JournalDurability::LocalDisk,
        )
        .unwrap();
        emit_running(
            &config,
            &ledger,
            PgLsn(1),
            true,
            SourceHealthStatus::Healthy,
            (&[], &[]),
        )
        .unwrap();
        let running = status(&config);
        assert_eq!(running["state"], "running");
        assert_eq!(running["ready"], true);
        assert!(running.get("last_error").is_none());
        assert!(read(&config).unwrap().ready());
    }

    #[test]
    fn a_process_that_never_held_the_state_lock_writes_no_exit() {
        let _serial = LIVENESS_TEST_LOCK.blocking_lock();
        let (_root, config) = config();
        // The owner is starting (for example, a long index rebuild).
        let _owner = Lifecycle::start(&config).unwrap();
        let before = std::fs::read(config.state_dir.join("status.json")).unwrap();
        record_exit_as(&config, &crate::exit::config("refused by the lock"), false);
        record_clean_exit_as(&config, false);
        assert_eq!(
            std::fs::read(config.state_dir.join("status.json")).unwrap(),
            before
        );
    }

    #[test]
    fn a_clean_exit_clears_an_earlier_failure() {
        let (_root, config) = config();
        record_clean_exit_as(&config, true);
        assert!(!config.state_dir.join("status.json").exists());
        record_exit_as(&config, &crate::exit::config("init failed"), true);
        assert_eq!(status(&config)["last_error"]["exit_code"], 78);
        record_clean_exit_as(&config, true);
        let clean = status(&config);
        assert_eq!(clean["state"], "stopped");
        assert!(clean.get("last_error").is_none());
    }

    #[test]
    fn concurrent_observation_writers_never_share_a_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("status.json");
        let writers = (0..4)
            .map(|writer| {
                let path = path.clone();
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        write_observation(&path, format!("{writer}")).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    }
}

#[cfg(test)]
mod publication_marker_tests {
    use super::*;

    #[test]
    fn pre_isolation_publication_marker_still_blocks_the_original_slot() {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().to_owned();
        let marker = serde_json::to_vec(&serde_json::json!({
            "source_id": config.source.id,
            "slot": config.source.slot,
            "publication": config.source.publication,
            "reason": "configured table removed",
            "recorded_at_ms": 123456789u64,
        }))
        .unwrap();
        let path = root.path().join("publication-resync-required.json");
        std::fs::write(&path, &marker).unwrap();
        let error = refuse_if_resync_required(&config).unwrap_err();
        assert!(error.to_string().contains("configured table removed"));
        require_resync(&config, "later violation");
        assert_eq!(std::fs::read(&path).unwrap(), marker);
        config.source.slot.push_str("_resync");
        refuse_if_resync_required(&config).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), marker);
    }
}
