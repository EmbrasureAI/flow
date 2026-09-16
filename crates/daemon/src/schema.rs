//! Durable source schema versions and the serialized Iceberg schema barrier.
//! Relation messages choose a decoder; only surviving committed source changes
//! authorize a public schema transition.

use crate::config::Table as ConfiguredTable;
use anyhow::{Context, Result, ensure};
use flow_coordinator::{
    SourceSchemaRecord as SchemaRecord, same_iceberg_schema, source_schema_key as schema_key,
    source_schema_prefix as schema_prefix, store_source_schema,
};
use flow_materializer::iceberg_schema;
use flow_model::{SourceId, TableId, TableSchema};
use flow_pg_source::{
    CaptureAssembler, Relation, TypeRegistry, fetch_table_metadata_selected,
    nullable_successor_with_types, same_wire_schema,
    tokio_postgres::{Client, GenericClient},
    validate_schema_metadata,
};
use flow_state_store::StateStore;
use iceberg::{
    Catalog,
    table::Table,
    transaction::{AddColumn, ApplyTransactionAction, Transaction},
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct SchemaRegistry {
    store: StateStore,
    source: SourceId,
    bases: BTreeMap<TableId, TableSchema>,
    candidates: BTreeMap<(TableId, u32), SchemaRecord>,
    dirty: BTreeSet<TableId>,
    types: TypeRegistry,
    projections: BTreeMap<TableId, Vec<String>>,
    identities: BTreeMap<(String, String), TableId>,
    blocked: BTreeSet<TableId>,
}
impl SchemaRegistry {
    pub(crate) fn new(store: StateStore, source: SourceId, bases: &[TableSchema]) -> Result<Self> {
        let mut initial = BTreeMap::new();
        for schema in bases {
            schema.validate()?;
            ensure!(
                initial.insert(schema.table_id, schema.clone()).is_none(),
                "duplicate source table"
            );
        }
        let mut identities = BTreeMap::new();
        let mut blocked = BTreeSet::new();
        for id in initial.keys().copied() {
            if let Some(entry) = store
                .source_transactions_after(&schema_prefix(&source, id), None)
                .next()
            {
                let (_, bytes) = entry?;
                let record = SchemaRecord::decode(&bytes)?;
                identities.insert((record.relation.namespace, record.relation.name), id);
            }
            if capture_blocked(&store, &source, id)? {
                blocked.insert(id);
            }
        }
        Ok(Self {
            identities,
            blocked,
            store,
            source,
            bases: initial,
            candidates: BTreeMap::new(),
            dirty: BTreeSet::new(),
            types: TypeRegistry::default(),
            projections: BTreeMap::new(),
        })
    }

    /// Keep configuration as the bootstrap prefix contract. A restarted decoder
    /// can select the current catalog shape while replaying older Relation
    /// messages against their persisted historical versions.
    pub(crate) async fn initialize(
        &mut self,
        client: &(impl GenericClient + Sync),
        configured: &[ConfiguredTable],
    ) -> Result<Vec<TableSchema>> {
        ensure!(
            configured.len() == self.bases.len(),
            "schema/config table count differs"
        );
        let mut result = Vec::with_capacity(self.bases.len());
        // Configuration order need not be table-OID order.
        for configured in configured {
            // Only established table identities can be isolated. Bootstrap and
            // missing/corrupt authority still require coordinated recovery.
            let identity = (
                configured.source_namespace.clone(),
                configured.source_table.clone(),
            );
            let known = self
                .identities
                .get(&identity)
                .map(|id| self.latest(*id).map(|record| record.schema))
                .transpose()?;
            if let Some(schema) = &known {
                if let Some(selected) = configured.projection() {
                    self.projections.insert(schema.table_id, selected);
                }
                if self.is_blocked(schema.table_id) {
                    result.push(schema.clone());
                    continue;
                }
            }
            match self.initialize_table(client, configured).await {
                Ok(schema) => {
                    self.identities.insert(identity, schema.table_id);
                    result.push(schema);
                }
                Err(error) if known.is_some() && table_schema_error(&error) => {
                    let schema = known.expect("checked above");
                    self.block(schema.table_id)?;
                    result.push(schema);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }

    async fn initialize_table(
        &mut self,
        client: &(impl GenericClient + Sync),
        configured: &ConfiguredTable,
    ) -> Result<TableSchema> {
        let projection = configured.projection();
        let metadata = fetch_table_metadata_selected(
            client,
            &configured.source_namespace,
            &configured.source_table,
            projection.as_deref(),
        )
        .await?;
        if let Some(selected) = projection {
            self.projections
                .insert(TableId(metadata.relation.id), selected);
        }
        self.types.extend(metadata.types.clone());
        let base = self
            .bases
            .get(&TableId(metadata.relation.id))
            .context(SchemaChange(
                "source table was replaced; resynchronization is required",
            ))?
            .clone();
        if self.record(base.table_id, base.version)?.is_none() {
            ensure!(
                metadata.relation.columns.len() >= base.columns.len(),
                SchemaChange("source dropped bootstrap columns")
            );
            let mut relation = metadata.relation.clone();
            relation.columns.truncate(base.columns.len());
            self.types.validate_relation(&base, &relation)?;
            validate_schema_metadata(&base, &base, &relation, &metadata)?;
            self.persist(&SchemaRecord {
                format: 2,
                storage_id: metadata.storage_id,
                attribute_numbers: metadata.attribute_numbers[..base.columns.len()].to_vec(),
                schema: base.clone(),
                relation,
            })?;
        }
        let mut record = self.select(&metadata.relation)?;
        let parent = if self
            .persisted(record.schema.table_id, record.schema.version)?
            .is_some()
        {
            record.schema.clone()
        } else {
            self.latest(record.schema.table_id)?.schema
        };
        let latest = self.latest(record.schema.table_id)?;
        if latest.schema != record.schema {
            latest.schema.validate_successor(&record.schema)?;
        }
        record = self.relax(record, &metadata)?;
        verify_storage(&record, &metadata)?;
        validate_schema_metadata(&parent, &record.schema, &record.relation, &metadata)?;
        // The SQL catalog sees only committed DDL. This also makes idle
        // nullable additions visible without inventing a source LSN.
        record.attribute_numbers =
            metadata.attribute_numbers[..record.schema.columns.len()].to_vec();
        self.persist(&record)?;
        self.candidates
            .remove(&(record.schema.table_id, record.schema.version));
        Ok(record.schema)
    }

    pub(crate) fn saved_relation(&self, table: TableId) -> Result<Relation> {
        Ok(self.latest(table)?.relation)
    }

    pub(crate) fn is_blocked(&self, table: TableId) -> bool {
        self.blocked.contains(&table)
    }

    pub(crate) fn block_decoder(
        &self,
        table: TableId,
        assembler: &mut CaptureAssembler,
    ) -> Result<()> {
        if assembler.is_blocked(table) {
            return Ok(());
        }
        assembler.set_schema(self.latest(table)?.schema)?;
        assembler.block_table(table)?;
        Ok(())
    }

    pub(crate) fn block(&mut self, table: TableId) -> Result<()> {
        ensure!(
            self.bases.contains_key(&table),
            "unconfigured capture block"
        );
        // Prove a recoverable schema before persisting a table-scoped failure.
        self.latest(table)?;
        self.store
            .put_source_transaction(&capture_block_key(&self.source, table), b"1")?;
        self.blocked.insert(table);
        self.candidates.retain(|(id, _), _| *id != table);
        self.dirty.remove(&table);
        tracing::warn!(
            event = "source_table_blocked",
            table_id = table.0,
            error_code = "source_schema_incompatible",
            "source table requires schema repair or full resync"
        );
        Ok(())
    }

    pub(crate) async fn observe_relation(
        &mut self,
        client: &Client,
        relation: &Relation,
        assembler: &mut CaptureAssembler,
    ) -> Result<()> {
        // Only relation changes perform catalog I/O. Rows use the cached resolver.
        self.types
            .extend(TypeRegistry::fetch(client, relation).await?);
        let record = self.select(relation)?;
        self.types.validate_relation(&record.schema, relation)?;
        assembler.set_types(self.types.clone());
        self.dirty.insert(record.schema.table_id);
        assembler.set_schema(record.schema)?;
        Ok(())
    }

    /// A streamed DROP NOT NULL can send NULL rows before its catalog change
    /// is visible to our SQL session. Stage a decoder only; COMMIT still proves
    /// and persists the version before any journal terminal/publication.
    pub(crate) fn observe_nulls(
        &mut self,
        event: &flow_pg_source::SourceEvent,
        assembler: &mut CaptureAssembler,
    ) -> Result<()> {
        use flow_pg_source::{Cell, SourceEvent};
        let (id, row) = match event {
            SourceEvent::Insert { relation, row, .. }
            | SourceEvent::Update { relation, row, .. } => (TableId(*relation), row),
            _ => return Ok(()),
        };
        let current = assembler
            .schema(id)
            .context("row for unconfigured source table")?;
        if row.len() != current.columns.len() {
            return Ok(());
        }
        let relaxed: Vec<_> = current
            .columns
            .iter()
            .zip(row)
            .enumerate()
            .filter_map(|(index, (column, cell))| {
                (!column.nullable
                    && matches!(cell, Cell::Null)
                    && !current.primary_key.contains(&index))
                .then_some(index)
            })
            .collect();
        if relaxed.is_empty() {
            return Ok(());
        }
        let mut record = self
            .record(id, current.version)?
            .context("decoder schema proof missing")?;
        for index in relaxed {
            record.schema.columns[index].nullable = true;
        }
        record.schema.version = self
            .latest(id)?
            .schema
            .version
            .max(current.version)
            .checked_add(1)
            .context("source schema version overflow")?;
        current.validate_successor(&record.schema)?;
        self.candidates
            .insert((id, record.schema.version), record.clone());
        self.dirty.insert(id);
        assembler.set_schema(record.schema)?;
        Ok(())
    }

    /// Let capture seal an earlier group before a schema proof may await SQL.
    pub(crate) fn validation_may_query(&self) -> bool {
        !self.dirty.is_empty() || !self.candidates.is_empty()
    }

    /// Called before CaptureAssembler receives COMMIT, so every schema referenced
    /// by the resulting durable journal terminal is already recoverable.
    pub(crate) async fn validate_commit(
        &mut self,
        client: &Client,
        xid: u32,
        assembler: &mut CaptureAssembler,
    ) -> Result<()> {
        let hints = assembler.schema_hints(xid)?;
        for hint in &hints {
            if self.is_blocked(hint.table_id) {
                self.block_decoder(hint.table_id, assembler)?;
            }
        }
        let needs_validation = hints.iter().any(|hint| {
            self.dirty.contains(&hint.table_id)
                || self.candidates.contains_key(&(hint.table_id, hint.version))
        });
        if !needs_validation {
            return Ok(());
        }
        let versions = assembler.surviving_schema_versions(xid)?;
        let mut metadata = BTreeMap::new();
        let mut publish = Vec::new();
        for version in versions {
            if self.is_blocked(version.table_id) {
                self.block_decoder(version.table_id, assembler)?;
                continue;
            }
            let validated: Result<()> = async {
                let mut record = self
                    .record(version.table_id, version.version)?
                    .context("transaction references an unknown schema version")?;
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    metadata.entry(version.table_id)
                {
                    entry.insert(
                        fetch_table_metadata_selected(
                            client,
                            &record.relation.namespace,
                            &record.relation.name,
                            self.projections.get(&version.table_id).map(Vec::as_slice),
                        )
                        .await?,
                    );
                }
                let known = self.persisted(version.table_id, version.version)?.is_some();
                let parent = if known {
                    record.schema.clone()
                } else {
                    self.latest(version.table_id)?.schema
                };
                if !known && parent != record.schema {
                    if parent.version < record.schema.version {
                        parent.validate_successor(&record.schema)?;
                    } else {
                        record.schema.validate_successor(&parent)?;
                    }
                }
                verify_storage(&record, &metadata[&version.table_id])?;
                let mut prefix = metadata[&version.table_id].relation.clone();
                let latest = self.latest(version.table_id)?;
                ensure!(
                    prefix.columns.len() >= latest.relation.columns.len(),
                    SchemaChange("source dropped committed columns")
                );
                prefix.columns.truncate(latest.relation.columns.len());
                ensure!(
                    same_wire_schema(&prefix, &latest.relation),
                    SchemaChange("source changed a committed column's type or name")
                );
                let validation_base = if parent.version > record.schema.version {
                    &record.schema
                } else {
                    &parent
                };
                validate_schema_metadata(
                    validation_base,
                    &record.schema,
                    &record.relation,
                    &metadata[&version.table_id],
                )?;
                if !known {
                    record.attribute_numbers = metadata[&version.table_id].attribute_numbers
                        [..record.schema.columns.len()]
                        .to_vec();
                    publish.push(record);
                }
                Ok(())
            }
            .await;
            if let Err(error) = validated {
                if !table_schema_error(&error) {
                    return Err(error);
                }
                self.block(version.table_id)?;
                self.block_decoder(version.table_id, assembler)?;
                publish.retain(|record| record.schema.table_id != version.table_id);
            }
        }
        for record in publish {
            self.persist(&record)?;
            self.candidates
                .remove(&(record.schema.table_id, record.schema.version));
        }
        for table in metadata.keys() {
            self.dirty.remove(table);
        }
        Ok(())
    }

    fn relax(
        &mut self,
        mut record: SchemaRecord,
        metadata: &flow_pg_source::TableMetadata,
    ) -> Result<SchemaRecord> {
        let old = record.schema.clone();
        for (column, attributes) in record.schema.columns.iter_mut().zip(&metadata.columns) {
            column.nullable |= attributes.nullable;
        }
        if record.schema.columns != old.columns {
            record.schema.version = self
                .latest(old.table_id)?
                .schema
                .version
                .checked_add(1)
                .context("source schema version overflow")?
                .max(
                    old.version
                        .checked_add(1)
                        .context("source schema version overflow")?,
                );
            old.validate_successor(&record.schema)?;
            self.candidates
                .insert((old.table_id, record.schema.version), record.clone());
        }
        Ok(record)
    }

    fn select(&mut self, relation: &Relation) -> Result<SchemaRecord> {
        let table = TableId(relation.id);
        ensure!(
            self.bases.contains_key(&table),
            "publication contains an unconfigured relation"
        );
        let mut matching = None;
        for record in self.records(table) {
            let record = record?;
            if same_wire_schema(&record.relation, relation) {
                matching = Some(record);
            }
        }
        if let Some(record) = matching {
            return Ok(record);
        }
        if let Some(record) = self
            .candidates
            .values()
            .find(|record| same_wire_schema(&record.relation, relation))
        {
            return Ok(record.clone());
        }
        let latest = self.latest(table)?;
        let base = &self.bases[&table];
        let added = relation
            .columns
            .len()
            .checked_sub(base.columns.len())
            .context(SchemaChange("source dropped bootstrap columns"))?;
        let version = if relation.columns.len() < latest.relation.columns.len() {
            base.version.checked_add(u32::try_from(added)?)
        } else {
            latest.schema.version.checked_add(
                u32::try_from(relation.columns.len() - latest.relation.columns.len())?.max(1),
            )
        }
        .context("source schema version overflow")?;
        let schema = if relation.columns.len() < latest.relation.columns.len() {
            let mut historical_wire = latest.relation.clone();
            historical_wire.columns.truncate(relation.columns.len());
            ensure!(
                same_wire_schema(&historical_wire, relation),
                SchemaChange("historical relation is not a verified nullable prefix")
            );
            let mut historical = latest.schema.clone();
            historical.version = version;
            historical.columns.truncate(relation.columns.len());
            historical.validate_successor(&latest.schema)?;
            historical
        } else {
            nullable_successor_with_types(
                &latest.schema,
                &latest.relation,
                relation,
                version,
                &self.types,
            )?
        };
        let mut attribute_numbers = latest.attribute_numbers;
        attribute_numbers.resize(schema.columns.len(), 0);
        let record = SchemaRecord {
            format: 2,
            storage_id: latest.storage_id,
            attribute_numbers,
            schema,
            relation: relation.clone(),
        };
        self.candidates.insert((table, version), record.clone());
        Ok(record)
    }
    fn persisted(&self, table: TableId, version: u32) -> Result<Option<SchemaRecord>> {
        self.store
            .source_transaction(&schema_key(&self.source, table, version))?
            .map(|bytes| SchemaRecord::decode(&bytes))
            .transpose()
    }
    fn record(&self, table: TableId, version: u32) -> Result<Option<SchemaRecord>> {
        match self.candidates.get(&(table, version)) {
            Some(record) => Ok(Some(record.clone())),
            None => self.persisted(table, version),
        }
    }
    fn records(&self, table: TableId) -> impl Iterator<Item = Result<SchemaRecord>> + '_ {
        self.store
            .source_transactions_after(&schema_prefix(&self.source, table), None)
            .map(|item| {
                let (_, value) = item?;
                SchemaRecord::decode(&value)
            })
    }
    fn latest(&self, table: TableId) -> Result<SchemaRecord> {
        let mut latest = None;
        for record in self.records(table) {
            latest = Some(record?);
        }
        latest.context("source schema baseline is missing")
    }
    fn persist(&self, record: &SchemaRecord) -> Result<()> {
        store_source_schema(&self.store, &self.source, record)
    }
}

#[derive(Debug)]
struct SchemaChange(&'static str);
impl std::fmt::Display for SchemaChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for SchemaChange {}

/// Explicit source-table validation failures only. Shared storage, transport,
/// corrupted proofs and unknown errors must never be turned into quarantine.
pub(crate) fn table_schema_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.is::<SchemaChange>()
            || matches!(
                cause.downcast_ref::<flow_pg_source::Error>(),
                Some(
                    flow_pg_source::Error::Config(_)
                        | flow_pg_source::Error::ReplicaIdentity(_)
                        | flow_pg_source::Error::UnchangedToast(_)
                        | flow_pg_source::Error::Value(_)
                        | flow_pg_source::Error::Row(_)
                )
            )
    })
}

fn capture_block_key(source: &SourceId, table: TableId) -> Vec<u8> {
    let mut key = b"flow-capture-block/v1/".to_vec();
    key.extend((source.0.len() as u64).to_be_bytes());
    key.extend(source.0.as_bytes());
    key.extend(table.0.to_be_bytes());
    key
}
pub(crate) fn capture_blocked(
    store: &StateStore,
    source: &SourceId,
    table: TableId,
) -> Result<bool> {
    match store.source_transaction(&capture_block_key(source, table))? {
        None => Ok(false),
        Some(bytes) => {
            ensure!(bytes == b"1", "invalid durable capture block");
            Ok(true)
        }
    }
}

pub(crate) use flow_coordinator::latest_source_schema as latest_schema;

/// Reload a target without adopting a replacement under the same catalog name.
pub(crate) async fn refresh_table(catalog: &dyn Catalog, table: &Table) -> Result<Table> {
    let refreshed = catalog.load_table(table.identifier()).await?;
    ensure!(
        refreshed.metadata().uuid() == table.metadata().uuid(),
        "Iceberg target UUID changed; resynchronization is required"
    );
    Ok(refreshed)
}

/// Add only the exact nullable suffix, checking upstream-assigned field IDs.
/// A lost response is resolved by reloading the exact resulting public schema.
pub(crate) async fn ensure_table_schema(
    catalog: &dyn Catalog,
    table: &Table,
    target: &TableSchema,
) -> Result<Table> {
    let expected = iceberg_schema(target)?;
    let current = table.metadata().current_schema();
    if same_iceberg_schema(current, &expected) {
        return Ok(table.clone());
    }
    let old_fields = current.as_struct().fields();
    let fields = expected.as_struct().fields();
    ensure!(
        fields.len() >= old_fields.len()
            && old_fields.iter().zip(fields).all(|(old, new)| {
                let mut relaxed = old.as_ref().clone();
                relaxed.required = new.required;
                (!new.required || old.required) && relaxed == **new
            })
            && current.identifier_field_ids().collect::<BTreeSet<_>>()
                == expected.identifier_field_ids().collect(),
        "Iceberg schema diverged from the source nullable-column lineage"
    );
    let mut next_id = table.metadata().last_column_id();
    let transaction = Transaction::new(table);
    let mut action = transaction.update_schema();
    for (old, new) in old_fields.iter().zip(fields) {
        if old.required && !new.required {
            action = action.make_column_optional(old.id);
        }
    }
    for field in &fields[old_fields.len()..] {
        next_id = next_id
            .checked_add(1)
            .context("Iceberg field ID overflow")?;
        ensure!(
            !field.required && field.id == next_id,
            "Iceberg field ID allocation differs from durable source schema"
        );
        action = action.add_column(AddColumn::optional(
            &field.name,
            field.field_type.as_ref().clone(),
        ));
    }
    let updated = match action.apply(transaction)?.commit(catalog).await {
        Ok(table) => table,
        Err(error) => {
            let refreshed = refresh_table(catalog, table).await?;
            if !same_iceberg_schema(refreshed.metadata().current_schema(), &expected) {
                return Err(error.into());
            }
            refreshed
        }
    };
    ensure!(
        updated.metadata().uuid() == table.metadata().uuid(),
        "Iceberg target UUID changed during schema update"
    );
    ensure!(
        same_iceberg_schema(updated.metadata().current_schema(), &expected),
        "catalog schema differs from the validated source schema"
    );
    Ok(updated)
}

fn verify_storage(record: &SchemaRecord, metadata: &flow_pg_source::TableMetadata) -> Result<()> {
    ensure!(
        record.storage_id == metadata.storage_id,
        SchemaChange("source heap was rewritten; resynchronization is required")
    );
    ensure!(
        record.attribute_numbers.len() == record.schema.columns.len()
            && metadata.attribute_numbers.len() >= record.attribute_numbers.len()
            && record
                .attribute_numbers
                .iter()
                .zip(&metadata.attribute_numbers)
                .all(|(expected, actual)| *expected == 0 || expected == actual),
        SchemaChange("source column was dropped or replaced; resynchronization is required")
    );
    Ok(())
}

#[cfg(test)]
mod target_identity_tests {
    use super::*;
    use flow_model::{Column, ColumnType};
    use iceberg::{
        CatalogBuilder, NamespaceIdent, TableCreation,
        memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
        spec::FormatVersion,
    };
    use std::collections::HashMap;

    #[test]
    fn only_classified_source_errors_are_table_scoped() {
        assert!(table_schema_error(
            &SchemaChange("source heap changed").into()
        ));
        assert!(table_schema_error(
            &flow_pg_source::Error::Config("selected column disappeared").into()
        ));
        assert!(!table_schema_error(&anyhow::anyhow!("unknown invariant")));
        assert!(!table_schema_error(
            &std::io::Error::other("local disk failed").into()
        ));
        assert!(!table_schema_error(
            &flow_pg_source::Error::Protocol("malformed frame").into()
        ));
    }

    #[tokio::test]
    async fn nullable_catalog_transition_keeps_field_ids_and_rejects_identifier_relaxation() {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "nullable",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://nullable".into())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("test".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = TableSchema {
            table_id: TableId(1),
            version: 0,
            primary_key: vec![0],
            append_only: false,
            columns: vec![
                Column {
                    field_id: 1,
                    name: "id".into(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                },
                Column {
                    field_id: 2,
                    name: "value".into(),
                    data_type: ColumnType::String,
                    nullable: false,
                },
            ],
        };
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".into())
                    .format_version(FormatVersion::V2)
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let mut next = schema.clone();
        next.version = 1;
        next.columns[1].nullable = true;
        let updated = ensure_table_schema(&catalog, &table, &next).await.unwrap();
        assert_eq!(updated.metadata().uuid(), table.metadata().uuid());
        assert_eq!(updated.metadata().last_column_id(), 2);
        assert!(same_iceberg_schema(
            updated.metadata().current_schema(),
            &iceberg_schema(&next).unwrap()
        ));
        assert_eq!(
            ensure_table_schema(&catalog, &updated, &next)
                .await
                .unwrap()
                .metadata(),
            updated.metadata()
        );
        assert!(
            Transaction::new(&updated)
                .update_schema()
                .make_column_optional(1)
                .apply(Transaction::new(&updated))
                .unwrap()
                .commit(&catalog)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn replacement_schema_does_not_resolve_an_original_tables_failed_commit() {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "identity",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://identity".into())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("test".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let mut schema = TableSchema {
            table_id: TableId(1),
            version: 0,
            columns: vec![Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            }],
            primary_key: vec![0],
            append_only: false,
        };
        let original = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".into())
                    .format_version(FormatVersion::V2)
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        schema.version = 1;
        schema.columns.push(Column {
            field_id: 2,
            name: "value".into(),
            data_type: ColumnType::String,
            nullable: true,
        });
        catalog.drop_table(original.identifier()).await.unwrap();
        let replacement = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("rows".into())
                    .format_version(FormatVersion::V2)
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        assert_ne!(replacement.metadata().uuid(), original.metadata().uuid());
        assert!(
            refresh_table(&catalog, &original)
                .await
                .unwrap_err()
                .to_string()
                .contains("UUID changed")
        );
        // The replacement already has the expected schema, but is no proof of
        // the failed original table's schema transaction.
        assert!(
            ensure_table_schema(&catalog, &original, &schema)
                .await
                .unwrap_err()
                .to_string()
                .contains("UUID changed")
        );
        let after = catalog.load_table(original.identifier()).await.unwrap();
        assert_eq!(after.metadata(), replacement.metadata());
    }
}
