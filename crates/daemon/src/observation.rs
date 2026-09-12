//! Prometheus textfile output and bounded-cardinality service observations.
use crate::{
    config::Config,
    lifecycle::{SourceHealthStatus, TableProgress},
    runtime::blocked::{BlockedTable, BlockedTables},
};
use anyhow::Result;
use flow_coordinator::{Inventory, SourceLedger};
use flow_model::{PgLsn, TableId};
use flow_state_store::StateStore;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::{
    fmt::Write,
    time::{Duration, Instant},
};

const EXPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct Observation {
    handle: PrometheusHandle,
    last_export: Option<(Instant, bool)>,
    source_health: SourceHealthStatus,
    blocked_tables: Vec<BlockedTable>,
    table_progress: Vec<TableProgress>,
    #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
    allocator_sample: Option<(Instant, Option<crate::allocator::MemoryUsage>)>,
}
impl Observation {
    pub(crate) fn install() -> Result<Self> {
        Ok(Self {
            handle: PrometheusBuilder::new()
                .set_buckets(&[
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 120.0,
                ])?
                .set_buckets_for_metric(
                    Matcher::Full("flow_capture_phase_seconds".into()),
                    &[
                        0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.003, 0.004, 0.005,
                        0.0075, 0.01, 0.015, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 30.0,
                    ],
                )?
                .set_buckets_for_metric(
                    Matcher::Full("flow_journal_commit_group_transactions".into()),
                    &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0],
                )?
                .install_recorder()?,
            last_export: None,
            source_health: SourceHealthStatus::Unknown,
            blocked_tables: Vec::new(),
            table_progress: Vec::new(),
            #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
            allocator_sample: None,
        })
    }

    pub(crate) fn record_source_health(&mut self, health: impl Into<SourceHealthStatus>) {
        self.source_health = health.into();
    }

    /// Refresh only changed tables; unrelated progress stays cached. The runtime
    /// supplies all tables at startup and during its periodic health observation.
    pub(crate) fn table_states(
        &mut self,
        store: &StateStore,
        blocked: &BlockedTables,
        ids: impl IntoIterator<Item = TableId>,
    ) -> Result<()> {
        let progress = ids
            .into_iter()
            .map(|table_id| {
                Ok(TableProgress {
                    table_id,
                    materialized_lsn: store.table_state(&table_id)?.materialized_lsn,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for progress in progress {
            match self
                .table_progress
                .binary_search_by_key(&progress.table_id, |table| table.table_id)
            {
                Ok(index) => self.table_progress[index] = progress,
                Err(index) => self.table_progress.insert(index, progress),
            }
        }
        self.blocked_tables = blocked.records().cloned().collect();
        Ok(())
    }

    /// Flush process counters even when initialization failed before a source
    /// ledger existed. The next process replaces this metrics snapshot.
    pub(crate) fn flush(&self, config: &Config) -> Result<()> {
        crate::lifecycle::write_observation(
            &config.state_dir.join("metrics.prom"),
            self.handle.render(),
        )
    }

    pub(crate) fn write(
        &mut self,
        config: &Config,
        ledger: &SourceLedger,
        captured: PgLsn,
        ready: bool,
    ) -> Result<bool> {
        self.write_at(config, ledger, captured, ready, Instant::now())
    }

    fn write_at(
        &mut self,
        config: &Config,
        ledger: &SourceLedger,
        captured: PgLsn,
        ready: bool,
        now: Instant,
    ) -> Result<bool> {
        let ready = ready && self.source_health.permits_readiness();
        crate::lifecycle::emit(
            config,
            Some(ledger),
            Some(captured),
            ready,
            Some(self.source_health),
            Some((&self.blocked_tables, &self.table_progress)),
        )?;
        if self.last_export.is_some_and(|(last, was_ready)| {
            ready == was_ready && now.duration_since(last) < EXPORT_INTERVAL
        }) {
            return Ok(false);
        }
        let watermarks = ledger.watermarks();
        let mut text = self.handle.render();
        #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
        {
            // Allocator fields overlap and are distinct from operating-system RSS.
            // Sampling all arenas is bounded to every 15 seconds, including retries.
            let sample = self
                .allocator_sample
                .get_or_insert_with(|| (now - Duration::from_secs(15), None));
            if now.duration_since(sample.0) >= Duration::from_secs(15) {
                *sample = (now, crate::allocator::memory_usage().ok());
            }
            if let Some(usage) = &sample.1 {
                for (name, value) in [
                    ("flow_allocator_allocated_bytes", usage.allocated_bytes),
                    ("flow_allocator_active_bytes", usage.active_bytes),
                    ("flow_allocator_resident_bytes", usage.resident_bytes),
                ] {
                    writeln!(text, "# TYPE {name} gauge\n{name} {value}")?;
                }
            }
        }
        // Keep exact integer text for local inspection. Prometheus itself uses
        // floating-point samples; status.json is the exact watermark interface.
        for (name, value) in [
            (
                "flow_source_received_lsn",
                captured.max(watermarks.received_lsn).0,
            ),
            ("flow_journal_durable_lsn", captured.0),
            (
                "flow_ledger_registered_lsn",
                watermarks.journal_durable_lsn.0,
            ),
            ("flow_materialized_lsn", watermarks.materialized_lsn.0),
            ("flow_pending_transactions", ledger.pending_count() as u64),
            (
                "flow_source_health_check_available",
                u64::from(self.source_health.check_available()),
            ),
            (
                "flow_source_at_risk",
                u64::from(self.source_health.at_risk()),
            ),
        ] {
            writeln!(text, "# TYPE {name} gauge\n{name} {value}")?;
        }
        crate::lifecycle::write_observation(&config.state_dir.join("metrics.prom"), text)?;
        // Failed exports remain due. Readiness transitions bypass the cadence.
        self.last_export = Some((now, ready));
        Ok(true)
    }
}

pub(crate) fn table_inventory(table: TableId, inventory: &Inventory) {
    let table = table.0.to_string();
    let debt = &inventory.debt;
    for (name, value) in [
        ("flow_table_l0_files", debt.l0_files as f64),
        ("flow_table_small_files", debt.small_files as f64),
        ("flow_table_l0_bytes", debt.l0_bytes as f64),
        (
            "flow_table_oldest_l0_seconds",
            debt.oldest_l0_ms as f64 / 1000.0,
        ),
        ("flow_table_max_delete_files", debt.max_delete_files as f64),
        (
            "flow_table_delete_files",
            inventory.delete_file_count as f64,
        ),
        (
            "flow_table_reclaimable_bytes",
            debt.reclaimable_bytes as f64,
        ),
        ("flow_table_manifests", inventory.manifest_count as f64),
        (
            "flow_table_manifest_entries",
            inventory.manifest_entries as f64,
        ),
        (
            "flow_table_publication_pressure",
            debt.pressure as u8 as f64,
        ),
    ] {
        metrics::gauge!(name, "table_id" => table.clone()).set(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability, SourceHealth};
    use flow_model::SourceId;
    use flow_state_store::{StateStore, StateStoreOptions};
    use tempfile::TempDir;

    #[test]
    fn export_cadence_keeps_status_current_and_retries_failed_writes() {
        let temp = TempDir::new().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = temp.path().to_owned();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let ledger = SourceLedger::open(
            store.clone(),
            SourceId(config.source.id.clone()),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut observation = Observation {
            handle: PrometheusBuilder::new().build_recorder().handle(),
            last_export: None,
            source_health: SourceHealthStatus::Unknown,
            blocked_tables: Vec::new(),
            table_progress: Vec::new(),
            #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
            allocator_sample: None,
        };
        let metrics = temp.path().join("metrics.prom");
        let status = temp.path().join("status.json");
        let read_status = || -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(&status).unwrap()).unwrap()
        };
        let now = Instant::now();
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(10), true, now)
                .unwrap()
        );
        assert_eq!(read_status()["source_health"], "unknown");
        assert_eq!(read_status()["ready"], true);
        let mut blocked = BlockedTables::load(&store, &SourceId(config.source.id.clone())).unwrap();
        store.complete_noop(&TableId(1), PgLsn(7), 1).unwrap();
        store.complete_noop(&TableId(2), PgLsn(8), 1).unwrap();
        blocked
            .record(TableId(1), "catalog_unavailable", None)
            .unwrap();
        observation
            .table_states(&store, &blocked, [TableId(2)])
            .unwrap();
        observation
            .table_states(&store, &blocked, [TableId(1)])
            .unwrap();
        observation.record_source_health(SourceHealth::Healthy);
        assert!(
            !observation
                .write_at(&config, &ledger, PgLsn(10), true, now)
                .unwrap()
        );
        assert_eq!(read_status()["source_health"], "healthy");
        assert_eq!(read_status()["ready"], true);
        assert_eq!(read_status()["blocked_tables"][0]["table_id"], 1);
        assert_eq!(
            read_status()["blocked_tables"][0]["error_code"],
            "catalog_unavailable"
        );
        assert_eq!(read_status()["table_progress"].as_array().unwrap().len(), 2);
        assert_eq!(read_status()["table_progress"][0]["materialized_lsn"], 7);
        assert_eq!(read_status()["table_progress"][1]["materialized_lsn"], 8);
        blocked.clear(TableId(1)).unwrap();
        store.complete_noop(&TableId(1), PgLsn(9), 1).unwrap();
        observation
            .table_states(&store, &blocked, [TableId(1)])
            .unwrap();
        let first = std::fs::read(&metrics).unwrap();
        assert!(
            !observation
                .write_at(
                    &config,
                    &ledger,
                    PgLsn(20),
                    true,
                    now + Duration::from_millis(999),
                )
                .unwrap()
        );
        assert_eq!(std::fs::read(&metrics).unwrap(), first);
        assert_eq!(read_status()["captured_durable_lsn"], 20);
        assert_eq!(read_status()["blocked_tables"], serde_json::json!([]));
        assert_eq!(read_status()["table_progress"][0]["materialized_lsn"], 9);
        assert_eq!(read_status()["table_progress"][1]["materialized_lsn"], 8);
        assert_eq!(read_status()["ready"], true);
        assert_eq!(read_status()["source_health"], "healthy");
        let due = now + EXPORT_INTERVAL;
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(30), true, due)
                .unwrap()
        );
        assert!(
            std::fs::read_to_string(&metrics)
                .unwrap()
                .contains("flow_journal_durable_lsn 30\n")
        );

        // A failed scheduled export must not suppress its immediate retry.
        std::fs::remove_file(&metrics).unwrap();
        std::fs::create_dir(&metrics).unwrap();
        let due = due + EXPORT_INTERVAL;
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(40), true, due)
                .is_err()
        );
        assert_eq!(read_status()["ready"], true);
        assert_eq!(read_status()["captured_durable_lsn"], 40);
        std::fs::remove_dir(&metrics).unwrap();
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(50), true, due)
                .unwrap()
        );
        assert!(
            std::fs::read_to_string(&metrics)
                .unwrap()
                .contains("flow_journal_durable_lsn 50\n")
        );
        let stopped = due + Duration::from_millis(1);
        observation.record_source_health(SourceHealth::AtRisk);
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(60), true, stopped)
                .unwrap()
        );
        assert_eq!(read_status()["ready"], false);
        assert_eq!(read_status()["source_health"], "at_risk");
        assert!(
            std::fs::read_to_string(&metrics)
                .unwrap()
                .contains("flow_source_at_risk 1\n")
        );
        observation.record_source_health(SourceHealth::Warning);
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(70), true, stopped)
                .unwrap()
        );
        assert_eq!(read_status()["ready"], true);
        assert_eq!(read_status()["source_health"], "warning");
        observation.record_source_health(SourceHealthStatus::Unavailable);
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(70), true, stopped)
                .unwrap()
        );
        assert_eq!(read_status()["ready"], false);
        assert_eq!(read_status()["source_health"], "unavailable");
        assert!(
            std::fs::read_to_string(&metrics)
                .unwrap()
                .contains("flow_source_health_check_available 0\n")
        );

        // Status failure leaves the last exported metrics and cadence intact.
        let before = std::fs::read(&metrics).unwrap();
        std::fs::remove_file(&status).unwrap();
        std::fs::create_dir(&status).unwrap();
        let due = stopped + EXPORT_INTERVAL;
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(80), true, due)
                .is_err()
        );
        assert_eq!(std::fs::read(&metrics).unwrap(), before);
        std::fs::remove_dir(&status).unwrap();
        assert!(
            observation
                .write_at(&config, &ledger, PgLsn(90), true, due)
                .unwrap()
        );
        assert_eq!(read_status()["captured_durable_lsn"], 90);
    }
}
