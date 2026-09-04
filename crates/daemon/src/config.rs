use anyhow::{Context, Result, ensure};
use flow_coordinator::{AckMode, JournalDurability, Priority};
use flow_model::{Column, TableId, TableSchema};
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
    pub tables: Vec<Table>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub compaction: flow_compactor::Policy,
    #[serde(default)]
    pub parquet_read: flow_compactor::ReadLimits,
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
    pub source_namespace: String,
    pub source_table: String,
    pub target_namespace: Vec<String>,
    pub target_table: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<usize>,
    #[serde(default)]
    pub append_only: bool,
    #[serde(default)]
    pub priority: Priority,
}
impl Table {
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
        let config: Self =
            toml::from_str(&std::fs::read_to_string(path).context("read configuration")?)
                .context("parse configuration")?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.tables.is_empty()
                && !self.source.id.is_empty()
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
        self.compaction.validate()?;
        self.parquet_read.validate()?;
        let l = &self.limits;
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
        let mut sources = BTreeMap::new();
        let mut targets = BTreeMap::new();
        for table in &self.tables {
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
