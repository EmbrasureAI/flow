//! PostgreSQL WAL retention observations. The
//! publication contract is checked on capture's identity-proven session.

use crate::{
    config::Config,
    lifecycle::SourceHealthStatus,
    observation::Observation,
    source::{connect_owned, retryable_connection},
};
use anyhow::{Context, Result, bail};
use flow_coordinator::{SourceHealth, SourceLedger, WalPressure};
use flow_model::PgLsn;
use flow_pg_source::tokio_postgres::Client;
use std::{sync::Arc, time::Duration};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

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
        bail!("replication slot lost WAL; resynchronization required");
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
    match health {
        SourceHealth::Healthy | SourceHealth::SlotLost => {}
        health => tracing::error!(
            ?health,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure; increase capacity or drain publication lag"
        ),
    }
    Ok(health)
}
