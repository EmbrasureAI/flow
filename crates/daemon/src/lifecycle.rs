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
}

impl SourceHealthStatus {
    pub(crate) fn permits_readiness(self) -> bool {
        !matches!(self, Self::AtRisk | Self::Unavailable | Self::SlotLost)
    }

    pub(crate) fn check_available(self) -> bool {
        !matches!(self, Self::Unknown | Self::Unavailable)
    }

    pub(crate) fn at_risk(self) -> bool {
        matches!(self, Self::AtRisk | Self::SlotLost)
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
