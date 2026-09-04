//! Durable source-schema records shared by capture, replay and index recovery.

use anyhow::{Context, Result, ensure};
use bincode::Options;
use flow_model::{SourceId, TableId, TableSchema};
use flow_pg_source::{Relation, same_wire_schema};
use flow_state_store::{ControlStore, StateStore};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct SourceSchemaRecord {
    pub format: u32,
    pub storage_id: u32,
    pub attribute_numbers: Vec<i16>,
    pub schema: TableSchema,
    pub relation: Relation,
}
impl SourceSchemaRecord {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let format = u32::from_le_bytes(
            bytes
                .get(..4)
                .context("truncated source schema registry record")?
                .try_into()?,
        );
        ensure!(
            format == 2,
            "source schema registry format lacks column-incarnation proof or is unsupported; initialize a new verified source"
        );
        let record: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes()
            .deserialize(bytes)?;
        ensure!(
            record.attribute_numbers.len() == record.schema.columns.len()
                && record.attribute_numbers.iter().all(|number| *number > 0),
            "invalid source column incarnation proof"
        );
        ensure!(
            record.relation.id == record.schema.table_id.0,
            "source schema relation identity mismatch"
        );
        record.schema.validate()?;
        record.relation.validate_schema(&record.schema)?;
        Ok(record)
    }
}
pub fn source_schema_prefix(source: &SourceId, table: TableId) -> Vec<u8> {
    let mut key = b"flow-schema/v1/".to_vec();
    key.extend((source.0.len() as u64).to_be_bytes());
    key.extend(source.0.as_bytes());
    key.extend(table.0.to_be_bytes());
    key
}
pub fn source_schema_key(source: &SourceId, table: TableId, version: u32) -> Vec<u8> {
    let mut key = source_schema_prefix(source, table);
    key.extend(version.to_be_bytes());
    key
}
pub fn store_source_schema(
    store: &StateStore,
    source: &SourceId,
    record: &SourceSchemaRecord,
) -> Result<()> {
    ensure!(
        record.format == 2
            && record.attribute_numbers.len() == record.schema.columns.len()
            && record.attribute_numbers.iter().all(|number| *number > 0),
        "cannot persist an unverified source schema"
    );
    let key = source_schema_key(source, record.schema.table_id, record.schema.version);
    if let Some(old) = store.source_transaction(&key)? {
        let old = SourceSchemaRecord::decode(&old)?;
        ensure!(
            old.schema == record.schema
                && old.storage_id == record.storage_id
                && old.attribute_numbers == record.attribute_numbers
                && same_wire_schema(&old.relation, &record.relation),
            "source schema version was reused with different content"
        );
        return Ok(());
    }
    store.put_source_transaction(&key, &bincode::serialize(record)?)?;
    Ok(())
}
fn checked_schema(bytes: &[u8], table: TableId, version: u32) -> Result<TableSchema> {
    let record = SourceSchemaRecord::decode(bytes)?;
    ensure!(
        record.schema.table_id == table && record.schema.version == version,
        "source schema registry key and payload disagree"
    );
    Ok(record.schema)
}
pub fn load_source_schema(
    store: &StateStore,
    source: &SourceId,
    table: TableId,
    version: u32,
) -> Result<TableSchema> {
    let bytes = store
        .source_transaction(&source_schema_key(source, table, version))?
        .context("durable source schema version is missing")?;
    checked_schema(&bytes, table, version)
}
pub fn load_control_schema(
    control: &ControlStore,
    source: &SourceId,
    table: TableId,
    version: u32,
) -> Result<TableSchema> {
    let bytes = control
        .source_transaction(&source_schema_key(source, table, version))?
        .context("durable source schema version is missing")?;
    checked_schema(&bytes, table, version)
}

/// Reconcile an unchanged physical index with a verified nullable schema suffix.
/// The caller serializes table work and resolves pending/catalog transitions first.
pub fn reconcile_index_schema(
    store: &StateStore,
    source: &SourceId,
    table: &iceberg::table::Table,
    target: &TableSchema,
) -> Result<()> {
    let indexed = store.table_state(&target.table_id)?;
    ensure!(
        indexed.pending_operation.is_none()
            && indexed.snapshot_id == table.metadata().current_snapshot_id(),
        "resolve the indexed snapshot before reconciling its schema"
    );
    ensure!(
        same_iceberg_schema(
            table.metadata().current_schema(),
            &flow_materializer::iceberg_schema(target)?
        ) && load_source_schema(store, source, target.table_id, target.version)? == *target,
        "catalog schema differs from the durable source schema"
    );
    if indexed.schema_version != target.version {
        let previous = load_source_schema(store, source, target.table_id, indexed.schema_version)?;
        previous.validate_successor(target)?;
        // Old files project the appended fields as NULL. Canonical fingerprints
        // omit trailing NULLs, so row locations, keys and versions remain valid.
        // Keep the published LSN exactly unchanged; schema metadata is not CDC.
        store.complete_noop(&target.table_id, indexed.materialized_lsn, target.version)?;
    }
    Ok(())
}

pub fn same_iceberg_schema(left: &iceberg::spec::Schema, right: &iceberg::spec::Schema) -> bool {
    left.as_struct() == right.as_struct()
        && left
            .identifier_field_ids()
            .collect::<std::collections::BTreeSet<_>>()
            == right.identifier_field_ids().collect()
}

pub fn latest_source_schema(
    store: &StateStore,
    source: &SourceId,
    table: TableId,
) -> Result<Option<TableSchema>> {
    let mut latest = None;
    for entry in store.source_transactions_after(&source_schema_prefix(source, table), None) {
        let (_, bytes) = entry?;
        latest = Some(SourceSchemaRecord::decode(&bytes)?.schema);
    }
    Ok(latest)
}
