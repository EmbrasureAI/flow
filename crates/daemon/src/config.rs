use anyhow::{Context, Result, ensure};
use flow_coordinator::{AckMode, JournalDurability, Priority};
use flow_ingress_journal::JournalConfig;
use flow_model::{Column, TableId, TableSchema};
use flow_pg_source::SpoolConfig;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub source: Source,
    pub catalog: HashMap<String, String>,
    /// Required except by `discover`, which generates these blocks.
    #[serde(default)]
    pub tables: Vec<Table>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub compaction: flow_compactor::Policy,
    #[serde(default)]
    pub parquet_read: flow_compactor::ReadLimits,
    /// Optional HTTP endpoints for probes and Prometheus scraping.
    #[serde(default)]
    pub http: Option<Http>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
    /// For example `0.0.0.0:9464`; serves /healthz, /readyz and /metrics.
    pub listen: std::net::SocketAddr,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Resolve the URL at runtime so checked-in configuration contains no secrets.
    pub connection_env: String,
    pub id: String,
    pub slot: String,
    pub publication: String,
    #[serde(default)]
    pub ack_mode: AckMode,
    #[serde(default)]
    pub journal_durability: JournalDurability,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    /// Explicit selection freezes configured columns; all_current allows safe nullable additions.
    #[serde(default)]
    pub column_selection: ColumnSelection,
    pub source_namespace: String,
    pub source_table: String,
    pub target_namespace: Vec<String>,
    pub target_table: String,
    /// Format for newly created targets; existing targets retain their format.
    #[serde(default = "default_format_version")]
    pub format_version: iceberg::spec::FormatVersion,
    pub columns: Vec<Column>,
    pub primary_key: Vec<usize>,
    #[serde(default)]
    pub append_only: bool,
    #[serde(default)]
    pub priority: Priority,
}
#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnSelection {
    #[default]
    AllCurrent,
    Explicit,
}

fn default_format_version() -> iceberg::spec::FormatVersion {
    iceberg::spec::FormatVersion::V2
}

impl Table {
    pub fn projection(&self) -> Option<Vec<String>> {
        (self.column_selection == ColumnSelection::Explicit).then(|| {
            self.columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        })
    }
    pub fn schema(&self, id: u32) -> TableSchema {
        TableSchema {
            table_id: TableId(id),
            version: 0,
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
            append_only: self.append_only,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub journal_bytes: u64,
    pub spool_bytes: u64,
    pub chunk_bytes: u32,
    /// A pgoutput UPDATE includes both row images and text bytea expansion.
    pub source_message_bytes: usize,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    /// Additional retained collapse state per table worker; zero uses disk only.
    pub collapse_memory_bytes: usize,
    pub parquet_row_group_bytes: usize,
    pub pending_transactions: usize,
    pub table_workers: usize,
    pub commits_per_second: u32,
    pub wal_soft_bytes: u64,
    pub wal_hard_bytes: u64,
    pub snapshot_retention_secs: u64,
    /// Without expiration, table metadata and replaced files grow without bound.
    /// Disable only when another coordinated process expires snapshots.
    pub snapshot_expiration: bool,
    pub checkpoint_interval_secs: u64,
    pub retained_checkpoints: usize,
    pub manifest_max_count: usize,
    pub garbage_interval_secs: u64,
    pub orphan_grace_secs: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            journal_bytes: 64 << 30,
            spool_bytes: 32 << 30,
            chunk_bytes: 4 << 20,
            source_message_bytes: 32 << 20,
            batch_rows: 1024,
            batch_bytes: 8 << 20,
            collapse_memory_bytes: 32 << 20,
            parquet_row_group_bytes: 32 << 20,
            pending_transactions: 256,
            table_workers: 4,
            commits_per_second: 100,
            wal_soft_bytes: 16 << 30,
            wal_hard_bytes: 32 << 30,
            snapshot_retention_secs: 3600,
            snapshot_expiration: true,
            checkpoint_interval_secs: 300,
            retained_checkpoints: 2,
            manifest_max_count: 64,
            garbage_interval_secs: 300,
            orphan_grace_secs: 86400,
        }
    }
}
impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let config = Self::load_without_tables(path)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate everything except the table list, for `discover`.
    pub fn load_without_tables(path: &std::path::Path) -> Result<Self> {
        let input = std::fs::read_to_string(path).context("read configuration")?;
        let config: Self = toml::from_str(&input).map_err(|error: toml::de::Error| {
            // Both source excerpts and serde messages can contain secret values.
            // Retain the location without chaining the original error.
            let offset = error.span().map_or(0, |span| span.start).min(input.len());
            let prefix = &input[..input.floor_char_boundary(offset)];
            let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
            let column = prefix
                .rsplit('\n')
                .next()
                .unwrap_or_default()
                .chars()
                .count()
                + 1;
            anyhow::anyhow!("invalid configuration at line {line}, column {column}")
        })?;
        config.validate_source()?;
        Ok(config)
    }

    /// Resolve catalog secrets only when connecting, not during `check` or `status`.
    pub fn catalog_properties(&self) -> Result<HashMap<String, String>> {
        self.resolve_catalog_properties(|name| std::env::var(name).ok())
    }

    fn resolve_catalog_properties(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<HashMap<String, String>> {
        let mut properties = self.catalog.clone();
        for key in ["token", "credential"] {
            if let Some(name) = properties.remove(&format!("{key}_env")) {
                // EnvVarError::NotUnicode can include the secret in its Debug output.
                let value = lookup(&name).ok_or_else(|| {
                    anyhow::anyhow!(
                        "catalog.{key}_env must name a set, Unicode environment variable"
                    )
                })?;
                ensure!(
                    !value.is_empty(),
                    "catalog.{key}_env resolved to an empty value"
                );
                properties.insert(key.to_owned(), value);
            }
        }
        Ok(properties)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.tables.is_empty(), "source and tables are required");
        self.validate_source()
    }

    fn validate_source(&self) -> Result<()> {
        ensure!(
            !self.source.id.is_empty()
                && !self.source.slot.is_empty()
                && !self.source.publication.is_empty(),
            "source and tables are required"
        );
        ensure!(
            self.source.ack_mode != AckMode::Journaled
                || self.source.journal_durability == JournalDurability::IndependentStorage,
            "journaled acknowledgement requires independently durable storage"
        );
        ensure!(self.catalog.contains_key("uri"), "catalog.uri is required");
        for key in ["token", "credential"] {
            if let Some(name) = self.catalog.get(&format!("{key}_env")) {
                ensure!(
                    !self.catalog.contains_key(key),
                    "configure only one of catalog.{key} and catalog.{key}_env"
                );
                ensure!(
                    !name.is_empty() && !name.contains(['=', '\0']),
                    "catalog.{key}_env must be an environment variable name"
                );
            }
        }
        self.compaction.validate()?;
        self.parquet_read.validate()?;
        let l = &self.limits;
        JournalConfig {
            quota_bytes: l.journal_bytes,
            max_frame_bytes: l.chunk_bytes,
            ..JournalConfig::default()
        }
        .validate()
        .context("invalid journal limits")?;
        SpoolConfig {
            quota_bytes: l.spool_bytes,
            max_chunk_bytes: l.chunk_bytes,
            ..SpoolConfig::default()
        }
        .validate()
        .context("invalid spool limits")?;
        ensure!(
            l.chunk_bytes >= 64
                && l.source_message_bytes >= l.chunk_bytes as usize
                && l.batch_rows >= 2
                && l.batch_bytes >= l.chunk_bytes as usize
                && l.parquet_row_group_bytes >= l.batch_bytes
                && l.batch_bytes <= i32::MAX as usize
                && l.pending_transactions > 0
                && l.table_workers > 0
                && l.commits_per_second > 0,
            "invalid batching limits"
        );
        ensure!(
            l.wal_soft_bytes > 0 && l.wal_soft_bytes < l.wal_hard_bytes,
            "WAL soft threshold must be below hard threshold"
        );
        ensure!(
            l.snapshot_retention_secs > 0
                && l.checkpoint_interval_secs > 0
                && l.retained_checkpoints > 0
                && l.manifest_max_count >= 2,
            "invalid history, checkpoint, or manifest maintenance limits"
        );
        ensure!(
            l.garbage_interval_secs > 0 && l.orphan_grace_secs > 0,
            "garbage interval and orphan grace must be positive"
        );
        let mut sources = BTreeMap::new();
        let mut targets = BTreeMap::new();
        for table in &self.tables {
            ensure!(
                matches!(
                    table.format_version,
                    iceberg::spec::FormatVersion::V2 | iceberg::spec::FormatVersion::V3
                ),
                "target format_version must be 2 or 3"
            );
            ensure!(
                !table.source_namespace.is_empty()
                    && !table.source_table.is_empty()
                    && !table.target_namespace.is_empty()
                    && !table.target_table.is_empty(),
                "table names are required"
            );
            ensure!(
                sources
                    .insert((&table.source_namespace, &table.source_table), ())
                    .is_none(),
                "duplicate source table"
            );
            ensure!(
                targets
                    .insert((&table.target_namespace, &table.target_table), ())
                    .is_none(),
                "duplicate target table"
            );
            table.schema(0).validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        toml::from_str(include_str!("../../../examples/flow.toml")).unwrap()
    }

    #[test]
    fn catalog_secret_references_resolve_without_changing_literal_properties() {
        let mut config = valid_config();
        let original = config.catalog.clone();
        assert_eq!(
            config
                .resolve_catalog_properties(|_| unreachable!())
                .unwrap(),
            original
        );
        for key in ["token", "credential"] {
            config
                .catalog
                .insert(format!("{key}_env"), format!("FLOW_{key}"));
        }
        config.validate().unwrap();
        let resolved = config
            .resolve_catalog_properties(|name| match name {
                "FLOW_token" => Some("test-token".into()),
                "FLOW_credential" => Some("client:test-secret".into()),
                _ => unreachable!(),
            })
            .unwrap();
        let mut expected = original;
        expected.insert("token".into(), "test-token".into());
        expected.insert("credential".into(), "client:test-secret".into());
        assert_eq!(resolved, expected);
        assert!(config.catalog.contains_key("token_env"));
        for key in ["token", "credential"] {
            config.catalog.insert(key.into(), "test-secret".into());
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("configure only one")
            );
            config.catalog.remove(key);
        }
    }

    #[test]
    fn rejects_limits_that_downstream_fixed_configuration_cannot_use() {
        type Invalidate = fn(&mut Limits);
        type Case = (&'static str, Invalidate, &'static str);

        valid_config().validate().unwrap();
        let cases: [Case; 5] = [
            (
                "journal quota below one segment",
                |limits| {
                    limits.journal_bytes = JournalConfig::default().segment_bytes - 1;
                },
                "invalid journal limits",
            ),
            (
                "spool quota below one segment",
                |limits| {
                    limits.spool_bytes = SpoolConfig::default().segment_bytes - 1;
                },
                "invalid spool limits",
            ),
            (
                "chunk larger than a journal frame",
                |limits| {
                    limits.chunk_bytes =
                        u32::try_from(JournalConfig::default().segment_bytes).unwrap();
                    limits.source_message_bytes = limits.chunk_bytes as usize;
                    limits.batch_bytes = limits.chunk_bytes as usize;
                    limits.parquet_row_group_bytes = limits.batch_bytes;
                },
                "invalid journal limits",
            ),
            (
                "zero garbage interval",
                |limits| {
                    limits.garbage_interval_secs = 0;
                },
                "garbage interval and orphan grace must be positive",
            ),
            (
                "zero orphan grace",
                |limits| {
                    limits.orphan_grace_secs = 0;
                },
                "garbage interval and orphan grace must be positive",
            ),
        ];

        for (case, invalidate, expected) in cases {
            let mut config = valid_config();
            invalidate(&mut config.limits);
            let error = config.validate().expect_err(case);
            assert!(
                format!("{error:#}").contains(expected),
                "{case} returned {error:#}"
            );
        }
    }
}
