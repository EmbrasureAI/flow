//! WAL retention and publication contract checks with an independently owned
//! source connection.

use crate::{
    config::Config,
    lifecycle::SourceHealthStatus,
    source::{
        PublicationViolation, connect_owned, retryable_connection, validate_publication_membership,
    },
};
use anyhow::{Context, Result};
use flow_coordinator::{SourceHealth, WalPressure};
use flow_model::TableSchema;
use flow_pg_source::tokio_postgres::Client;
use std::{sync::Arc, time::Duration};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
// Several catalog reads; a large FOR ALL TABLES publication may take longer.
const PUBLICATION_TIMEOUT: Duration = Duration::from_secs(15);
/// pgoutput silently omits changes after some publication DDL, so startup
/// validation alone cannot keep proving a long-running capture complete.
pub(super) const PUBLICATION_CHECK_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct HealthConnection {
    client: Client,
    _connection: crate::source::ConnectionTask,
}

pub(super) struct HealthCheckResult {
    pub(super) connection: Option<HealthConnection>,
    pub(super) source_health: SourceHealthStatus,
    /// A configured table's publication contract changed while running.
    pub(super) publication_violation: Option<anyhow::Error>,
}

/// `publication` carries the configured schemas, in configuration order, when
/// the publication contract check is due. It reuses the WAL health connection.
pub(super) async fn check_health(
    config: Arc<Config>,
    client: Option<HealthConnection>,
    publication: Option<Arc<[TableSchema]>>,
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
    let (mut client, source_health) = match result {
        Ok(checked) => checked,
        Err(error) if retryable_connection(&error) || error.is::<tokio::time::error::Elapsed>() => {
            // Dropping the owned driver closes even an unanswered query.
            tracing::warn!(%error, "WAL health check interrupted; reconnecting on next check");
            return Ok(HealthCheckResult {
                connection: None,
                source_health: SourceHealthStatus::Unavailable,
                publication_violation: None,
            });
        }
        Err(error) => return Err(error),
    };
    let Some(schemas) = publication else {
        return Ok(HealthCheckResult {
            connection: Some(client),
            source_health: source_health.into(),
            publication_violation: None,
        });
    };
    let checked = tokio::time::timeout(
        PUBLICATION_TIMEOUT,
        validate_publication_membership(&mut client.client, &config, &schemas),
    )
    .await
    .context("publication check timed out")
    .and_then(|checked| checked);
    let (connection, publication_violation) = match classify_publication_check(checked)? {
        PublicationCheck::Satisfied => (Some(client), None),
        PublicationCheck::Violated(error) => (Some(client), Some(error)),
        PublicationCheck::Interrupted(error) => {
            // Not evidence of a contract change, and WAL health remains valid.
            // Dropping the owned driver closes a possibly unanswered query.
            tracing::warn!(%error, "publication check interrupted; retrying on a later check");
            (None, None)
        }
    };
    Ok(HealthCheckResult {
        connection,
        source_health: source_health.into(),
        publication_violation,
    })
}

#[derive(Debug)]
enum PublicationCheck {
    Satisfied,
    Violated(anyhow::Error),
    Interrupted(anyhow::Error),
}

/// Only a definite catalog answer is a violation. Connection loss and timeouts
/// use the source's retryable classification; other query failures stay fatal.
fn classify_publication_check(result: Result<()>) -> Result<PublicationCheck> {
    match result {
        Ok(()) => Ok(PublicationCheck::Satisfied),
        Err(error)
            if error
                .chain()
                .any(|cause| cause.is::<PublicationViolation>()) =>
        {
            Ok(PublicationCheck::Violated(error))
        }
        Err(error) if retryable_connection(&error) => Ok(PublicationCheck::Interrupted(error)),
        Err(error) => Err(error),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn only_a_definite_publication_answer_stops_capture() {
        assert!(matches!(
            classify_publication_check(Ok(())).unwrap(),
            PublicationCheck::Satisfied
        ));
        let changed = Err::<(), _>(anyhow::Error::new(PublicationViolation(
            "publication not found".into(),
        )))
        .context("running check");
        assert!(matches!(
            classify_publication_check(changed).unwrap(),
            PublicationCheck::Violated(error) if error.root_cause().to_string() == "publication not found"
        ));
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .unwrap_err();
        let timed_out =
            Err::<(), _>(anyhow::Error::new(elapsed)).context("publication check timed out");
        assert!(matches!(
            classify_publication_check(timed_out).unwrap(),
            PublicationCheck::Interrupted(_)
        ));
        // A failed catalog query is neither a contract change nor retryable.
        let failed = Err(anyhow::anyhow!(
            "permission denied for view pg_publication_tables"
        ));
        assert!(classify_publication_check(failed).is_err());
    }
}
