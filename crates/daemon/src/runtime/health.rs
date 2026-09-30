//! PostgreSQL WAL retention observations. The
//! publication contract is checked on capture's identity-proven session.

use crate::{
    config::Config,
    lifecycle::SourceHealthStatus,
    observation::Observation,
    source::{connect_owned, retryable_connection},
};
use anyhow::{Context, Result};
use flow_coordinator::{SourceHealth, SourceLedger, WalPressure};
use flow_model::PgLsn;
use flow_pg_source::tokio_postgres::Client;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
/// Unchanged storage pressure is repeated at most this often; metrics and
/// status carry every five-second check.
const PRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(300);

pub(super) struct HealthConnection {
    client: Client,
    _connection: crate::source::ConnectionTask,
}

pub(super) struct HealthCheckResult {
    pub(super) connection: Option<HealthConnection>,
    pub(super) source_health: SourceHealthStatus,
}

pub(super) async fn check_health(
    config: Arc<Config>,
    client: Option<HealthConnection>,
) -> Result<HealthCheckResult> {
    let result: Result<(HealthConnection, SourceHealth)> = async {
        let client = match client {
            Some(client) => client,
            None => {
                let (client, connection) =
                    tokio::time::timeout(HEALTH_TIMEOUT, connect_owned(&config, false))
                        .await
                        .context("WAL health connection timed out")??;
                HealthConnection {
                    client,
                    _connection: connection,
                }
            }
        };
        let source_health = check_wal(&client.client, &config).await?;
        Ok((client, source_health))
    }
    .await;
    match result {
        Ok((client, source_health)) => Ok(HealthCheckResult {
            connection: Some(client),
            source_health: source_health.into(),
        }),
        Err(error) if retryable_connection(&error) || error.is::<tokio::time::error::Elapsed>() => {
            // Dropping the owned driver closes even an unanswered query.
            tracing::warn!(%error, "WAL health check interrupted; reconnecting on next check");
            Ok(HealthCheckResult {
                connection: None,
                source_health: SourceHealthStatus::Unavailable,
            })
        }
        Err(error) => Err(error),
    }
}

/// Record one completed check. A lost slot is persisted before the runtime
/// exits with it.
pub(super) fn observe_health(
    result: Result<HealthCheckResult>,
    observation: &mut Observation,
    config: &Config,
    ledger: &SourceLedger,
    captured: PgLsn,
) -> Result<HealthCheckResult> {
    let result = result?;
    observation.record_source_health(result.source_health);
    observation.write(config, ledger, captured, true)?;
    if result.source_health == SourceHealthStatus::SlotLost {
        return Err(crate::exit::resync(
            "replication slot lost WAL; resynchronization required",
        ));
    }
    Ok(result)
}

/// The durable resync marker blocks the next start; the status value clears
/// readiness and survives the exit, like `slot_lost`. A failed write must not
/// hide the violation itself.
pub(super) fn record_publication_changed(
    observation: &mut Observation,
    config: &Config,
    ledger: &SourceLedger,
    captured: PgLsn,
    error: &anyhow::Error,
) {
    crate::lifecycle::require_resync(config, &format!("{error:#}"));
    observation.record_source_health(SourceHealthStatus::PublicationChanged);
    if let Err(error) = observation.write(config, ledger, captured, true) {
        tracing::warn!(%error, "could not record the publication contract violation");
    }
}

async fn check_wal(client: &Client, config: &Config) -> Result<SourceHealth> {
    let row = tokio::time::timeout(HEALTH_TIMEOUT, client.query_opt(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, wal_status, safe_wal_size FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
        &[&config.source.slot],
    )).await.context("WAL health query timed out")??;
    let Some(row) = row else {
        return Ok(SourceHealth::SlotLost);
    };
    let retained: Option<i64> = row.get(0);
    let status: Option<String> = row.get(1);
    let safe: Option<i64> = row.get(2);
    let directory = config.state_dir.join("journal");
    let journal_bytes = tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|s| s == "segment") {
                match entry.metadata() {
                    Ok(metadata) => bytes = bytes.saturating_add(metadata.len()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {} // reclaimed concurrently
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(bytes)
    })
    .await??;
    let pressure = WalPressure {
        retained_bytes: retained.unwrap_or(0).max(0) as u64,
        journal_bytes,
        safe_wal_bytes: safe.map(|s| s.max(0) as u64),
        slot_lost: status.as_deref() == Some("lost"),
    };
    metrics::gauge!("flow_source_retained_wal_bytes").set(pressure.retained_bytes as f64);
    metrics::gauge!("flow_journal_bytes").set(pressure.journal_bytes as f64);
    if let Some(bytes) = pressure.safe_wal_bytes {
        metrics::gauge!("flow_source_safe_wal_bytes").set(bytes as f64);
    }
    let health = pressure.health(
        config.limits.wal_soft_bytes,
        config.limits.wal_hard_bytes,
        config.limits.journal_bytes,
    );
    let now = Instant::now();
    let previous = {
        let mut logged = PRESSURE_LOGGED
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if !pressure_log_due(*logged, health, now) {
            return Ok(health);
        }
        let previous = logged.map(|(health, _)| health);
        *logged = (health != SourceHealth::Healthy).then_some((health, now));
        previous
    };
    match health {
        SourceHealth::SlotLost => {}
        SourceHealth::Healthy => tracing::info!(
            ?previous,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure cleared"
        ),
        SourceHealth::Warning => tracing::warn!(
            ?health,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure; increase capacity or drain publication lag"
        ),
        SourceHealth::AtRisk => tracing::error!(
            ?health,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure; increase capacity or drain publication lag"
        ),
    }
    Ok(health)
}

/// The last logged pressure state and when it was logged.
static PRESSURE_LOGGED: Mutex<Option<(SourceHealth, Instant)>> = Mutex::new(None);

/// Log pressure on every state change and repeat an unchanged state only
/// after `PRESSURE_LOG_INTERVAL`. A healthy result is logged only as recovery.
fn pressure_log_due(
    logged: Option<(SourceHealth, Instant)>,
    health: SourceHealth,
    now: Instant,
) -> bool {
    match logged {
        None => health != SourceHealth::Healthy,
        Some((previous, at)) => {
            previous != health || now.duration_since(at) >= PRESSURE_LOG_INTERVAL
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_pressure_is_rate_limited_but_changes_are_logged() {
        let start = Instant::now();
        let later = |seconds| start + Duration::from_secs(seconds);
        assert!(!pressure_log_due(None, SourceHealth::Healthy, start));
        assert!(pressure_log_due(None, SourceHealth::Warning, start));
        let warned = Some((SourceHealth::Warning, start));
        assert!(!pressure_log_due(warned, SourceHealth::Warning, later(5)));
        assert!(!pressure_log_due(warned, SourceHealth::Warning, later(299)));
        assert!(pressure_log_due(warned, SourceHealth::Warning, later(300)));
        assert!(pressure_log_due(warned, SourceHealth::AtRisk, later(5)));
        assert!(pressure_log_due(warned, SourceHealth::Healthy, later(5)));
    }
}
