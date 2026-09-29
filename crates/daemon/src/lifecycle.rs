use crate::{config::Config, runtime::blocked::BlockedTable};
use anyhow::{Context, Result};
use flow_coordinator::{SourceHealth, SourceLedger, Watermarks};
use flow_model::{PgLsn, TableId};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
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
}

impl Status {
    /// Readiness written by this process, not a previous one that shared the
    /// state directory and exited within the expiry window.
    pub(crate) fn ready_in(&self, process_id: u32) -> bool {
        self.ready && self.process_id == process_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableProgress {
    pub(crate) table_id: TableId,
    pub(crate) materialized_lsn: PgLsn,
    #[serde(default)]
    pub(crate) source_namespace: Option<String>,
    #[serde(default)]
    pub(crate) source_table: Option<String>,
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

pub(crate) struct Lifecycle(Config);
impl Lifecycle {
    pub(crate) fn start(config: &Config) -> Result<Self> {
        emit(
            config,
            None,
            None,
            false,
            Some(SourceHealthStatus::Unknown),
            None,
        )?;
        Ok(Self(config.clone()))
    }
}
impl Drop for Lifecycle {
    fn drop(&mut self) {
        if let Err(error) = emit(&self.0, None, None, false, None, None) {
            tracing::warn!(%error, "could not mark local status stopped");
        }
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
    anyhow::bail!(
        "replication slot {:?} requires resynchronization before capture can resume; a previous run stopped with: {}",
        marker.slot,
        marker.reason
    )
}

pub(crate) fn emit(
    config: &Config,
    ledger: Option<&SourceLedger>,
    captured: Option<PgLsn>,
    ready: bool,
    source_health: Option<SourceHealthStatus>,
    tables: Option<(&[BlockedTable], &[TableProgress])>,
) -> Result<()> {
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
    });
    status.process_id = std::process::id();
    status.ready = ready;
    status.updated_at_ms = now_ms()?;
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

pub(crate) fn write_observation(path: &Path, bytes: impl AsRef<[u8]>) -> Result<()> {
    let temporary = path.with_extension("tmp");
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
