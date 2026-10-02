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
use futures::FutureExt;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::{
    fmt::Write,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const EXPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct Observation {
    handle: PrometheusHandle,
    last_export: Option<(Instant, bool)>,
    source_health: SourceHealthStatus,
    blocked_tables: Vec<BlockedTable>,
    table_progress: Vec<TableProgress>,
    table_sources: std::collections::BTreeMap<TableId, (String, String)>,
    started_at: SystemTime,
    memory_warned_at: Option<Instant>,
    memory_sample: Option<tokio::task::JoinHandle<Vec<String>>>,
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
                .install_recorder()
                // Test-only seam: `init` and `run` share one test process.
                .or_else(|error| {
                    if cfg!(test) {
                        Ok(PrometheusBuilder::new().build_recorder().handle())
                    } else {
                        Err(error)
                    }
                })?,
            last_export: None,
            source_health: SourceHealthStatus::Unknown,
            blocked_tables: Vec::new(),
            table_progress: Vec::new(),
            table_sources: Default::default(),
            started_at: SystemTime::now(),
            memory_warned_at: None,
            memory_sample: None,
            #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
            allocator_sample: None,
        })
    }

    pub(crate) fn table_sources(&mut self, config: &Config, schemas: &[flow_model::TableSchema]) {
        self.table_sources = schemas
            .iter()
            .zip(&config.tables)
            .map(|(schema, table)| {
                (
                    schema.table_id,
                    (table.source_namespace.clone(), table.source_table.clone()),
                )
            })
            .collect();
    }

    pub(crate) fn record_source_health(&mut self, health: impl Into<SourceHealthStatus>) {
        self.source_health = health.into();
    }

    /// Refresh only changed tables; unrelated progress stays cached. The runtime
    /// supplies all tables at startup and during its periodic health observation,
    /// so newly registered work appears in table lag within one health interval.
    pub(crate) fn table_states(
        &mut self,
        store: &StateStore,
        ledger: &SourceLedger,
        blocked: &BlockedTables,
        ids: impl IntoIterator<Item = TableId>,
    ) -> Result<()> {
        let progress = ids
            .into_iter()
            .map(|table_id| {
                // Table admission references are ordered by end LSN.
                let oldest = ledger
                    .pending_table_transactions_after(table_id, PgLsn(0))
                    .next()
                    .transpose()?;
                Ok(TableProgress {
                    table_id,
                    materialized_lsn: store.table_state(&table_id)?.materialized_lsn,
                    source_namespace: self.table_sources.get(&table_id).map(|name| name.0.clone()),
                    source_table: self.table_sources.get(&table_id).map(|name| name.1.clone()),
                    oldest_unpublished_lsn: oldest
                        .as_ref()
                        .map(|transaction| transaction.begin_lsn),
                    oldest_unpublished_commit_micros: oldest
                        .map(|transaction| transaction.commit_timestamp_micros),
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

    /// Start a sample of the process-wide memory budgets (see
    /// [`sample_memory_budgets`]). It runs on a blocking thread with at most one
    /// sample outstanding, so RocksDB property reads, whose table-reader
    /// estimate walks every table file, never delay the caller. Diagnostics must
    /// not stop ingestion: a failed read keeps that gauge's previous value and
    /// is logged at most every ten minutes.
    pub(crate) fn memory_budgets(
        &mut self,
        store: &StateStore,
        publisher: &Arc<flow_coordinator::TablePublisher>,
        maintenance: &Arc<flow_coordinator::TableMaintenance>,
    ) {
        if let Some(sample) = self.memory_sample.take_if(|sample| sample.is_finished()) {
            let failures = match sample.now_or_never() {
                Some(Ok(failures)) => failures,
                Some(Err(error)) => vec![format!("sampler: {error}")],
                None => Vec::new(),
            };
            if !failures.is_empty()
                && self
                    .memory_warned_at
                    .is_none_or(|warned| warned.elapsed() >= Duration::from_secs(600))
            {
                self.memory_warned_at = Some(Instant::now());
                tracing::warn!(
                    event = "memory_observation_failed",
                    failures = failures.join("; "),
                    "memory budget gauges are stale"
                );
            }
        }
        if self.memory_sample.is_none() {
            let (store, publisher, maintenance) =
                (store.clone(), publisher.clone(), maintenance.clone());
            self.memory_sample = Some(tokio::task::spawn_blocking(move || {
                sample_memory_budgets(&store, &publisher, &maintenance)
            }));
        }
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
        // These files are observations, never recovery authority. A full
        // volume must not stop the service; capture pauses on its own watermark.
        match self.write_at(config, ledger, captured, ready, Instant::now()) {
            Err(error) if crate::disk::is_storage_full(&error) => {
                crate::disk::warn_observation_skipped(&error);
                Ok(false)
            }
            result => result,
        }
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
        crate::lifecycle::emit_running(
            config,
            ledger,
            captured,
            ready,
            self.source_health,
            (&self.blocked_tables, &self.table_progress),
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
        self.render_process_and_tables(&mut text, captured, ready)?;
        crate::lifecycle::write_observation(&config.state_dir.join("metrics.prom"), text)?;
        // Failed exports remain due. Readiness transitions bypass the cadence.
        self.last_export = Some((now, ready));
        Ok(true)
    }
}

impl Observation {
    /// Stall signals rendered from the current observation, so a cleared block
    /// or a changed code leaves no stale series. Labels are configured table
    /// IDs and fixed error codes: cardinality is bounded by the table count.
    fn render_process_and_tables(
        &self,
        text: &mut String,
        captured: PgLsn,
        ready: bool,
    ) -> Result<()> {
        let now = SystemTime::now();
        let seconds = |time: SystemTime| {
            time.duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        };
        let now_micros = i64::try_from(
            now.duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
        )
        .unwrap_or(i64::MAX);
        for (name, value) in [
            ("flow_up", 1.0),
            ("flow_ready", f64::from(u8::from(ready))),
            ("flow_process_start_time_seconds", seconds(self.started_at)),
            ("flow_last_update_timestamp_seconds", seconds(now)),
            ("flow_blocked_tables", self.blocked_tables.len() as f64),
        ] {
            writeln!(text, "# TYPE {name} gauge\n{name} {value}")?;
        }
        writeln!(text, "# TYPE flow_table_blocked gauge")?;
        for record in &self.blocked_tables {
            writeln!(
                text,
                "flow_table_blocked{{table_id=\"{}\",code=\"{}\"}} 1",
                record.table_id.0, record.error_code
            )?;
        }
        writeln!(
            text,
            "# TYPE flow_table_blocked_since_timestamp_seconds gauge"
        )?;
        for record in &self.blocked_tables {
            writeln!(
                text,
                "flow_table_blocked_since_timestamp_seconds{{table_id=\"{}\",code=\"{}\"}} {}",
                record.table_id.0,
                record.error_code,
                record.blocked_at_ms as f64 / 1000.0
            )?;
        }
        writeln!(text, "# TYPE flow_table_materialized_lsn gauge")?;
        for table in &self.table_progress {
            writeln!(
                text,
                "flow_table_materialized_lsn{{table_id=\"{}\"}} {}",
                table.table_id.0, table.materialized_lsn.0
            )?;
        }
        writeln!(text, "# TYPE flow_table_lag_bytes gauge")?;
        for table in &self.table_progress {
            writeln!(
                text,
                "flow_table_lag_bytes{{table_id=\"{}\"}} {}",
                table.table_id.0,
                table_lag_bytes(table, captured)
            )?;
        }
        writeln!(text, "# TYPE flow_table_lag_seconds gauge")?;
        for table in &self.table_progress {
            writeln!(
                text,
                "flow_table_lag_seconds{{table_id=\"{}\"}} {}",
                table.table_id.0,
                table_lag_seconds(table, now_micros)
            )?;
        }
        Ok(())
    }
}

/// Captured WAL from the start of the table's oldest unpublished transaction;
/// zero when the table has no registered, unpublished work.
fn table_lag_bytes(table: &TableProgress, captured: PgLsn) -> u64 {
    table.oldest_unpublished_lsn.map_or(0, |begin| {
        captured
            .0
            .saturating_sub(begin.max(table.materialized_lsn).0)
    })
}

/// Age of the oldest unpublished source commit by the source's clock.
fn table_lag_seconds(table: &TableProgress, now_micros: i64) -> f64 {
    table
        .oldest_unpublished_commit_micros
        .filter(|commit| *commit > 0)
        .map_or(0.0, |commit| {
            now_micros.saturating_sub(commit).max(0) as f64 / 1_000_000.0
        })
}

/// Usage of the process-wide memory budgets: the row index's RocksDB block
/// cache and memtables, the shared publication and maintenance manifest caches,
/// and the garbage reachability indexes. These are the estimates each budget
/// evicts against. They overlap one another and the allocator gauges and are
/// not a decomposition of RSS. Returns the reads that failed.
fn sample_memory_budgets(
    store: &StateStore,
    publisher: &flow_coordinator::TablePublisher,
    maintenance: &flow_coordinator::TableMaintenance,
) -> Vec<String> {
    let mut failures = Vec::new();
    match store.memory_usage() {
        Ok(index) => {
            for (name, value) in [
                (
                    "flow_memory_index_block_cache_bytes",
                    index.block_cache_bytes,
                ),
                (
                    "flow_memory_index_block_cache_pinned_bytes",
                    index.block_cache_pinned_bytes,
                ),
                ("flow_memory_index_memtable_bytes", index.memtable_bytes),
                (
                    "flow_memory_index_table_reader_bytes",
                    index.table_reader_bytes,
                ),
            ] {
                metrics::gauge!(name).set(value as f64);
            }
        }
        Err(error) => failures.push(format!("row index: {error}")),
    }
    match maintenance.retained_index_bytes() {
        Ok(bytes) => metrics::gauge!("flow_memory_retained_index_bytes").set(bytes as f64),
        Err(error) => failures.push(format!("retained indexes: {error}")),
    }
    for (cache, manifests) in [
        ("publication", publisher.manifest_cache()),
        ("maintenance", maintenance.manifest_cache()),
    ] {
        metrics::gauge!("flow_memory_manifest_cache_bytes", "cache" => cache)
            .set(manifests.weighted_bytes() as f64);
        metrics::gauge!("flow_memory_manifest_cache_entries", "cache" => cache)
            .set(manifests.entry_count() as f64);
    }
    failures
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
    fn stall_gauges_report_blocked_tables_and_table_lag() {
        let temp = TempDir::new().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = temp.path().to_owned();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let source = SourceId(config.source.id.clone());
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        store.complete_noop(&TableId(1), PgLsn(90), 1).unwrap();
        store.complete_noop(&TableId(2), PgLsn(80), 1).unwrap();
        let committed = SystemTime::now() - Duration::from_secs(120);
        let commit_micros = committed.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        ledger
            .journaled(flow_model::SourceTransaction {
                source_id: source.clone(),
                xid: 7,
                begin_lsn: PgLsn(100),
                commit_lsn: PgLsn(140),
                end_lsn: PgLsn(150),
                commit_timestamp_micros: commit_micros,
                schema_versions: vec![],
                affected_tables: vec![TableId(1)],
                mutation_chunks: Default::default(),
                table_mutation_counts: Some(vec![flow_model::TableMutationCount {
                    table_id: TableId(1),
                    mutations: 1,
                }]),
            })
            .unwrap();
        let mut blocked = BlockedTables::load(&store, &source).unwrap();
        blocked
            .record(TableId(1), "catalog_unavailable", None)
            .unwrap();
        let mut observation = Observation::install().unwrap();
        observation
            .table_states(&store, &ledger, &blocked, [TableId(1), TableId(2)])
            .unwrap();
        observation
            .write_at(&config, &ledger, PgLsn(400), true, Instant::now())
            .unwrap();
        let metrics = std::fs::read_to_string(temp.path().join("metrics.prom")).unwrap();
        for line in [
            "flow_up 1\n",
            "flow_ready 1\n",
            "flow_blocked_tables 1\n",
            "flow_table_blocked{table_id=\"1\",code=\"catalog_unavailable\"} 1\n",
            "flow_table_materialized_lsn{table_id=\"1\"} 90\n",
            "flow_table_materialized_lsn{table_id=\"2\"} 80\n",
            // Captured WAL since the oldest unpublished transaction began.
            "flow_table_lag_bytes{table_id=\"1\"} 300\n",
            "flow_table_lag_bytes{table_id=\"2\"} 0\n",
            "flow_table_lag_seconds{table_id=\"2\"} 0\n",
        ] {
            assert!(metrics.contains(line), "{line}{metrics}");
        }
        let lag = metrics
            .lines()
            .find_map(|line| line.strip_prefix("flow_table_lag_seconds{table_id=\"1\"} "))
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!((120.0..180.0).contains(&lag), "{lag}");
        assert!(metrics.contains("flow_table_blocked_since_timestamp_seconds{table_id=\"1\""));
        let updated = metrics
            .lines()
            .find_map(|line| line.strip_prefix("flow_last_update_timestamp_seconds "))
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!(updated > commit_micros as f64 / 1e6);
        let status: serde_json::Value =
            serde_json::from_slice(&std::fs::read(temp.path().join("status.json")).unwrap())
                .unwrap();
        assert_eq!(status["state"], "running");
        assert_eq!(status["table_progress"][0]["oldest_unpublished_lsn"], 100);
        assert!(
            status["table_progress"][1]
                .get("oldest_unpublished_lsn")
                .is_none()
        );

        // A cleared block and completed work leave no stale blocked series.
        blocked.clear(TableId(1)).unwrap();
        ledger
            .table_materialized(PgLsn(150), TableId(1), 1)
            .unwrap();
        observation
            .table_states(&store, &ledger, &blocked, [TableId(1)])
            .unwrap();
        observation
            .write_at(
                &config,
                &ledger,
                PgLsn(400),
                true,
                Instant::now() + EXPORT_INTERVAL,
            )
            .unwrap();
        let metrics = std::fs::read_to_string(temp.path().join("metrics.prom")).unwrap();
        assert!(metrics.contains("flow_blocked_tables 0\n"), "{metrics}");
        assert!(!metrics.contains("flow_table_blocked{"), "{metrics}");
        assert!(
            metrics.contains("flow_table_lag_bytes{table_id=\"1\"} 0\n"),
            "{metrics}"
        );
    }

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
            table_sources: Default::default(),
            started_at: SystemTime::now(),
            memory_warned_at: None,
            memory_sample: None,
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
            .table_states(&store, &ledger, &blocked, [TableId(2)])
            .unwrap();
        observation
            .table_states(&store, &ledger, &blocked, [TableId(1)])
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
            .table_states(&store, &ledger, &blocked, [TableId(1)])
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
