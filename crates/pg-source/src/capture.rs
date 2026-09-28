use crate::{Cell, Error, Relation, Result, SourceEvent, TransactionSpool, Tuple};
use chrono::{NaiveTime, Timelike};
use flow_ingress_journal::Journal;
use flow_model::{
    ColumnType, Mutation, MutationKind, PgLsn, Row, SourceId, SourceTransaction, TableId,
    TableMutationCount, TableSchema, TableSchemaVersion, Value,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Instant,
};

struct Buffer {
    xid: u32,
    subxid: u32,
    table_id: TableId,
    schema_version: u32,
    mutations: Vec<Mutation>,
    bytes: u64,
}

struct PendingTransaction {
    begin_lsn: PgLsn,
    // A hint only: subtransaction rollback can remove some referenced versions.
    schemas: BTreeMap<TableId, BTreeSet<u32>>,
}

/// Bounded source-wide transaction assembly. Only one pgoutput segment is
/// active at a time, so a single mutation buffer serves all streamed XIDs.
/// On any error stop capture and reopen the spool/journal before reconnecting.
pub struct CaptureAssembler {
    source: SourceId,
    types: crate::TypeRegistry,
    spool: TransactionSpool,
    schemas: HashMap<TableId, TableSchema>,
    relations: HashMap<u32, Relation>,
    transactions: HashMap<u32, PendingTransaction>,
    buffer: Option<Buffer>,
    chunk_bytes: u64,
    pending_commits: Vec<SourceTransaction>,
    pending_commit_bytes: u64,
    pending_commit_limit: usize,
    blocked: BTreeSet<TableId>,
}

impl CaptureAssembler {
    pub fn new(
        source: SourceId,
        spool: TransactionSpool,
        schemas: impl IntoIterator<Item = TableSchema>,
        chunk_bytes: u32,
    ) -> Result<Self> {
        if chunk_bytes < 64 || chunk_bytes > spool.max_chunk_bytes() {
            return Err(Error::Config(
                "capture chunks must be at least 64 bytes and fit the spool frame limit",
            ));
        }
        let mut configured = HashMap::new();
        for schema in schemas {
            schema.validate()?;
            if configured.insert(schema.table_id, schema).is_some() {
                return Err(Error::Config("duplicate configured source table"));
            }
        }
        Ok(Self {
            source,
            types: crate::TypeRegistry::default(),
            blocked: BTreeSet::new(),
            spool,
            schemas: configured,
            relations: HashMap::new(),
            transactions: HashMap::new(),
            buffer: None,
            chunk_bytes: u64::from(chunk_bytes),
            pending_commits: Vec::new(),
            pending_commit_bytes: 0,
            pending_commit_limit: 32,
        })
    }

    pub fn set_types(&mut self, types: crate::TypeRegistry) {
        self.types = types;
    }

    /// Bound terminal descriptors waiting for one journal durability barrier.
    /// Row payloads remain in the disk spool and journal, independent of this
    /// limit. The caller chooses when to flush by count, payload bytes or age.
    pub fn with_pending_commit_limit(mut self, limit: usize) -> Result<Self> {
        if limit == 0 || !self.pending_commits.is_empty() {
            return Err(Error::Config(
                "pending commit limit must be positive and set before capture",
            ));
        }
        self.pending_commit_limit = limit;
        Ok(self)
    }

    pub fn schema(&self, table: TableId) -> Option<&TableSchema> {
        self.schemas.get(&table)
    }

    /// Whether a source relation belongs to this capture set. Other tables in
    /// the publication are ignored rather than treated as source errors.
    pub fn is_captured(&self, table: TableId) -> bool {
        self.schemas.contains_key(&table)
    }

    pub fn is_blocked(&self, table: TableId) -> bool {
        self.blocked.contains(&table)
    }

    pub fn block_table(&mut self, table: TableId) -> Result<()> {
        if !self.schemas.contains_key(&table) {
            return Err(Error::Config("unconfigured blocked table"));
        }
        self.blocked.insert(table);
        Ok(())
    }

    /// Retain only projected cells, together with their wire relation. The
    /// normal spool owns rollback/quotas and the journal owns committed payloads.
    pub fn quarantine(&mut self, event: SourceEvent, relation: &Relation) -> Result<()> {
        let (xid, subxid, table) = match &event {
            SourceEvent::Insert {
                xid,
                subxid,
                relation,
                ..
            }
            | SourceEvent::Update {
                xid,
                subxid,
                relation,
                ..
            }
            | SourceEvent::Delete {
                xid,
                subxid,
                relation,
                ..
            } => (*xid, *subxid, TableId(*relation)),
            SourceEvent::Truncate {
                xid,
                subxid,
                relations,
                ..
            } if relations.len() == 1 => (*xid, *subxid, TableId(relations[0])),
            _ => {
                return Err(Error::Config(
                    "only single-table source events can be quarantined",
                ));
            }
        };
        if relation.id != table.0 {
            return Err(Error::Protocol("quarantine relation identity mismatch"));
        }
        self.block_table(table)?;
        let version = self.schemas[&table].version;
        self.append(
            xid,
            subxid,
            Mutation {
                table_id: table,
                schema_version: version,
                kind: MutationKind::Quarantined {
                    format: flow_model::QuarantineFormat::PostgresEventV1,
                    payload: bincode::serialize(&(relation, event))?,
                },
            },
        )
    }

    pub fn pending_commit_count(&self) -> usize {
        self.pending_commits.len()
    }

    pub fn pending_commit_bytes(&self) -> u64 {
        self.pending_commit_bytes
    }

    /// Release source spools only after every staged terminal is durable.
    /// The returned descriptors are ordered by source commit position. On an
    /// error, stop and reopen: journal recovery resolves any completed barrier.
    pub fn flush_commits(&mut self, journal: &mut Journal) -> Result<Vec<SourceTransaction>> {
        journal.flush_commits()?;
        for transaction in &self.pending_commits {
            self.spool.discard(transaction.xid)?;
        }
        self.pending_commit_bytes = 0;
        Ok(std::mem::take(&mut self.pending_commits))
    }

    /// Select a registry-validated current or historical decoder before its
    /// Relation event. The registry owns lineage and source-catalog validation.
    pub fn set_schema(&mut self, schema: TableSchema) -> Result<()> {
        schema.validate()?;
        let current = self
            .schemas
            .get(&schema.table_id)
            .ok_or(Error::Config("schema for an unconfigured relation"))?;
        if current.primary_key != schema.primary_key || current.append_only != schema.append_only {
            return Err(Error::Config(
                "schema transition changed the table's key contract",
            ));
        }
        self.flush()?;
        self.schemas.insert(schema.table_id, schema);
        Ok(())
    }

    /// Cheap version hints for ordinary commits. Only a transaction containing
    /// an unverified schema needs the surviving-spool scan below.
    pub fn schema_hints(&mut self, xid: u32) -> Result<Vec<TableSchemaVersion>> {
        self.flush()?;
        let transaction = self
            .transactions
            .get(&xid)
            .ok_or(Error::Protocol("schema lookup without transaction"))?;
        Ok(transaction
            .schemas
            .iter()
            .flat_map(|(table, versions)| {
                versions.iter().map(|version| TableSchemaVersion {
                    table_id: *table,
                    version: *version,
                })
            })
            .collect())
    }

    /// Resolve versions after streamed subtransaction rollback. The ordinary
    /// row path does not pay for this extra spool read.
    pub fn surviving_schema_versions(&mut self, xid: u32) -> Result<Vec<TableSchemaVersion>> {
        self.flush()?;
        let mut versions = BTreeMap::<TableId, BTreeSet<u32>>::new();
        self.spool.replay(xid, |bytes| {
            if bytes.len() < 16 {
                return Err(Error::Protocol("truncated capture chunk"));
            }
            let table = TableId(u32::from_le_bytes(bytes[..4].try_into().unwrap()));
            let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
            versions.entry(table).or_default().insert(version);
            Ok(())
        })?;
        Ok(versions
            .into_iter()
            .flat_map(|(table_id, versions)| {
                versions
                    .into_iter()
                    .map(move |version| TableSchemaVersion { table_id, version })
            })
            .collect())
    }

    pub fn push(
        &mut self,
        event: SourceEvent,
        journal: &mut Journal,
    ) -> Result<Option<SourceTransaction>> {
        self.push_at(event, PgLsn(0), journal)
    }

    /// `received_lsn` is the XLogData start position. pgoutput Begin's final LSN
    /// is not a begin position. `push` records zero when no envelope is supplied.
    pub fn push_at(
        &mut self,
        event: SourceEvent,
        received_lsn: PgLsn,
        journal: &mut Journal,
    ) -> Result<Option<SourceTransaction>> {
        if !self.pending_commits.is_empty() {
            return Err(Error::Protocol(
                "flush buffered commits before using synchronous capture",
            ));
        }
        self.push_buffered_at(event, received_lsn, journal)?;
        Ok(self.flush_commits(journal)?.pop())
    }

    /// Process one source event without a durability barrier. A successful
    /// Commit only stages its terminal; call `flush_commits` before publishing
    /// transaction descriptors or acknowledging their source position.
    pub fn push_buffered_at(
        &mut self,
        event: SourceEvent,
        received_lsn: PgLsn,
        journal: &mut Journal,
    ) -> Result<()> {
        let change_xid = match &event {
            SourceEvent::Insert { xid, .. }
            | SourceEvent::Update { xid, .. }
            | SourceEvent::Delete { xid, .. }
            | SourceEvent::Truncate { xid, .. } => Some(*xid),
            _ => None,
        };
        let Some(event) = event.retain_relations(|id| self.schemas.contains_key(&TableId(id)))
        else {
            // Other published tables never reach the spool or journal. Their
            // transaction still commits, exactly like one without row changes.
            if change_xid.is_some_and(|xid| !self.transactions.contains_key(&xid)) {
                return Err(Error::Protocol("mutation outside capture transaction"));
            }
            return Ok(());
        };
        match event {
            SourceEvent::Begin { xid, .. } | SourceEvent::StreamStart { xid, first: true } => {
                self.flush()?;
                if self.transactions.contains_key(&xid)
                    || self.pending_commits.iter().any(|txn| txn.xid == xid)
                {
                    return Err(Error::Protocol("duplicate transaction begin"));
                }
                self.spool.begin(xid)?;
                self.transactions.insert(
                    xid,
                    PendingTransaction {
                        begin_lsn: received_lsn,
                        schemas: BTreeMap::new(),
                    },
                );
            }
            SourceEvent::StreamStart { xid, first: false } => {
                self.flush()?;
                if !self.transactions.contains_key(&xid) {
                    return Err(Error::Protocol(
                        "continuation of unknown capture transaction",
                    ));
                }
            }
            SourceEvent::StreamStop => self.flush()?,
            SourceEvent::Relation(relation) => {
                let schema = self
                    .schemas
                    .get(&TableId(relation.id))
                    .ok_or(Error::Config("relation outside the capture set"))?;
                self.types.validate_relation(schema, &relation)?;
                self.relations.insert(relation.id, relation);
            }
            SourceEvent::Insert {
                xid,
                subxid,
                relation,
                row,
            } => {
                let (schema, metadata) = self.table(relation)?;
                let mutation = Mutation {
                    table_id: schema.table_id,
                    schema_version: schema.version,
                    kind: MutationKind::Insert {
                        row: decode_row_with_types(schema, metadata, &row, &self.types)?,
                    },
                };
                self.append(xid, subxid, mutation)?;
            }
            SourceEvent::Update {
                xid,
                subxid,
                relation,
                old,
                old_is_key,
                mut row,
            } => {
                let (schema, metadata) = self.table(relation)?;
                if schema.append_only {
                    return Err(Error::Config("UPDATE on an append-only table"));
                }
                if row.iter().any(|cell| matches!(cell, Cell::UnchangedToast)) {
                    // pgoutput omits unchanged TOAST values from the new tuple,
                    // even with FULL identity. Its complete old tuple supplies
                    // those values without consulting mutable source state.
                    if metadata.replica_identity != b'f' || old_is_key {
                        return Err(Error::UnchangedToast(relation));
                    }
                    let previous = old.as_ref().ok_or(Error::UnchangedToast(relation))?;
                    metadata.validate_row(previous)?;
                    if row.len() != previous.len() {
                        return Err(Error::Protocol("tuple column count differs from relation"));
                    }
                    for (cell, previous) in row.iter_mut().zip(previous) {
                        if matches!(cell, Cell::UnchangedToast) {
                            *cell = previous.clone();
                        }
                    }
                }
                let row = decode_row_with_types(schema, metadata, &row, &self.types)?;
                let old_key = if let Some(old) = old {
                    decode_key(schema, metadata, &old, &self.types)?
                } else {
                    schema.encode_key(&row)?
                };
                let mutation = Mutation {
                    table_id: schema.table_id,
                    schema_version: schema.version,
                    kind: MutationKind::Update { old_key, row },
                };
                self.append(xid, subxid, mutation)?;
            }
            SourceEvent::Delete {
                xid,
                subxid,
                relation,
                old,
                old_is_key: _,
            } => {
                let (schema, metadata) = self.table(relation)?;
                if schema.append_only {
                    return Err(Error::Config("DELETE on an append-only table"));
                }
                let key = decode_key(schema, metadata, &old, &self.types)?;
                let mutation = Mutation {
                    table_id: schema.table_id,
                    schema_version: schema.version,
                    kind: MutationKind::Delete { key },
                };
                self.append(xid, subxid, mutation)?;
            }
            SourceEvent::Abort { xid, subxid } => {
                if self.pending_commits.iter().any(|txn| txn.xid == xid) {
                    return Err(Error::Protocol("cannot abort a staged capture commit"));
                }
                self.flush()?;
                self.spool.abort(xid, subxid)?;
                if xid == subxid {
                    self.transactions.remove(&xid);
                }
            }
            SourceEvent::Commit {
                xid,
                commit_lsn,
                end_lsn,
                commit_timestamp_micros,
            } => {
                self.flush()?;
                let begin_lsn = self
                    .transactions
                    .get(&xid)
                    .ok_or(Error::Protocol("capture commit without begin"))?
                    .begin_lsn;
                if end_lsn <= journal.staged_lsn() {
                    // PostgreSQL slots can resend their most recent durable
                    // transactions after a server restart. Do not append twice.
                    self.spool.discard(xid)?;
                    self.transactions.remove(&xid);
                    return Ok(());
                }
                if self.pending_commits.len() >= self.pending_commit_limit {
                    return Err(Error::Config(
                        "pending commit limit reached; flush the journal commit group",
                    ));
                }
                let mut tables = BTreeMap::<TableId, (u32, u64)>::new();
                let started = Instant::now();
                let replayed = self.spool.replay(xid, |bytes| {
                    if bytes.len() < 16 {
                        return Err(Error::Protocol("truncated capture chunk"));
                    }
                    let table_id = TableId(u32::from_le_bytes(bytes[..4].try_into().unwrap()));
                    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
                    // The fixed-width bincode Vec length follows the spool-only
                    // table/schema header. Count only chunks surviving rollback.
                    let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
                    if count == 0 {
                        return Err(Error::Protocol("empty capture mutation chunk"));
                    }
                    let entry = tables.entry(table_id).or_insert((version, 0));
                    entry.0 = entry.0.max(version);
                    entry.1 = entry
                        .1
                        .checked_add(count)
                        .ok_or(Error::Protocol("transaction mutation count overflow"))?;
                    // Each chunk contains one table/schema. Its tiny spool-only
                    // header lets commit avoid decoding and reallocating rows.
                    if self.blocked.contains(&table_id) {
                        use bincode::Options;
                        let mut mutations: Vec<Mutation> = bincode::DefaultOptions::new()
                            .with_fixint_encoding()
                            .with_limit(bytes.len() as u64)
                            .reject_trailing_bytes()
                            .deserialize(&bytes[8..])?;
                        for mutation in &mut mutations {
                            if !matches!(&mutation.kind, MutationKind::Quarantined { .. }) {
                                mutation.kind = MutationKind::Quarantined {
                                    format: flow_model::QuarantineFormat::DecodedMutationV1,
                                    payload: bincode::serialize(mutation)?,
                                };
                            }
                            mutation.schema_version = self.schemas[&table_id].version;
                        }
                        journal.append_chunk(xid, &bincode::serialize(&mutations)?)?;
                        entry.0 = self.schemas[&table_id].version;
                    } else {
                        journal.append_chunk(xid, &bytes[8..])?;
                    }
                    Ok(())
                });
                metrics::histogram!(
                    "flow_capture_phase_seconds",
                    "phase" => "spool_replay_append",
                    "result" => if replayed.is_ok() { "success" } else { "error" }
                )
                .record(started.elapsed().as_secs_f64());
                replayed?;
                let txn = SourceTransaction {
                    source_id: self.source.clone(),
                    xid,
                    begin_lsn,
                    commit_lsn,
                    end_lsn,
                    commit_timestamp_micros,
                    affected_tables: tables.keys().copied().collect(),
                    table_mutation_counts: Some(
                        tables
                            .iter()
                            .map(|(&table_id, &(_, mutations))| TableMutationCount {
                                table_id,
                                mutations,
                            })
                            .collect(),
                    ),
                    schema_versions: tables
                        .into_iter()
                        .map(|(table_id, (version, _))| TableSchemaVersion { table_id, version })
                        .collect(),
                    mutation_chunks: journal.transaction_chunks(xid),
                };
                let pending_bytes = self
                    .pending_commit_bytes
                    .checked_add(txn.mutation_chunks.payload_bytes)
                    .ok_or(Error::Config("pending commit payload byte count overflow"))?;
                journal.stage_commit(txn.clone())?;
                self.pending_commits.push(txn);
                self.pending_commit_bytes = pending_bytes;
                self.transactions.remove(&xid);
            }
            SourceEvent::Truncate { .. } => {
                return Err(Error::Config(
                    "TRUNCATE requires coordinated table replacement and is unsupported in V1; capture paused",
                ));
            }
            SourceEvent::Metadata => {}
        }
        Ok(())
    }

    fn table(&self, id: u32) -> Result<(&TableSchema, &Relation)> {
        let schema = self
            .schemas
            .get(&TableId(id))
            .ok_or(Error::Config("unconfigured source relation"))?;
        let relation = self
            .relations
            .get(&id)
            .ok_or(Error::Protocol("source row before relation metadata"))?;
        Ok((schema, relation))
    }

    fn append(&mut self, xid: u32, subxid: u32, mutation: Mutation) -> Result<()> {
        if !self.transactions.contains_key(&xid) {
            return Err(Error::Protocol("mutation outside capture transaction"));
        }
        let bytes = bincode::serialized_size(&mutation)?;
        if bytes + 16 > self.chunk_bytes {
            return Err(Error::Config(
                "single mutation exceeds capture chunk limit; increase row/chunk limits",
            ));
        }
        if self.buffer.as_ref().is_some_and(|b| {
            b.xid != xid
                || b.subxid != subxid
                || b.table_id != mutation.table_id
                || b.schema_version != mutation.schema_version
                || b.bytes + bytes > self.chunk_bytes
        }) {
            self.flush()?;
        }
        let buffer = self.buffer.get_or_insert_with(|| Buffer {
            xid,
            subxid,
            table_id: mutation.table_id,
            schema_version: mutation.schema_version,
            mutations: Vec::new(),
            bytes: 16,
        });
        buffer.mutations.push(mutation);
        buffer.bytes += bytes;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(buffer) = self.buffer.take() {
            let mut bytes = Vec::with_capacity(buffer.bytes as usize);
            bytes.extend_from_slice(&buffer.table_id.0.to_le_bytes());
            bytes.extend_from_slice(&buffer.schema_version.to_le_bytes());
            bincode::serialize_into(&mut bytes, &buffer.mutations)?;
            self.spool.append(buffer.xid, buffer.subxid, &bytes)?;
            self.transactions
                .get_mut(&buffer.xid)
                .ok_or(Error::Protocol("buffer without transaction"))?
                .schemas
                .entry(buffer.table_id)
                .or_default()
                .insert(buffer.schema_version);
        }
        Ok(())
    }
}

pub(crate) fn validate_relation(schema: &TableSchema, relation: &Relation) -> Result<()> {
    crate::TypeRegistry::default().validate_relation(schema, relation)
}

/// pgoutput key tuples retain relation positions, with NULL placeholders for
/// non-key columns. Decode only actual primary-key fields, in configured order.
fn decode_key(
    schema: &TableSchema,
    relation: &Relation,
    tuple: &Tuple,
    types: &crate::TypeRegistry,
) -> Result<flow_model::PrimaryKey> {
    if tuple.len() != relation.columns.len() {
        return Err(Error::Protocol(
            "key tuple column count differs from relation",
        ));
    }
    let mut key_schema = schema.clone();
    key_schema.columns = schema
        .primary_key
        .iter()
        .map(|&i| schema.columns[i].clone())
        .collect();
    key_schema.primary_key = (0..key_schema.columns.len()).collect();
    let mut values = Vec::with_capacity(schema.primary_key.len());
    for &index in &schema.primary_key {
        let column = &schema.columns[index];
        let pg = &relation.columns[index];
        values.push(match &tuple[index] {
            Cell::Null => return Err(Error::Protocol("NULL primary key in old tuple")),
            Cell::UnchangedToast => return Err(Error::UnchangedToast(relation.id)),
            Cell::Text(bytes) => types.decode(&column.data_type, pg.type_oid, bytes, false)?,
            Cell::Binary(bytes) => types.decode(&column.data_type, pg.type_oid, bytes, true)?,
        });
    }
    Ok(key_schema.encode_key(&values)?)
}

pub fn decode_row(schema: &TableSchema, relation: &Relation, tuple: &Tuple) -> Result<Row> {
    decode_row_with_types(schema, relation, tuple, &crate::TypeRegistry::default())
}
pub fn decode_row_with_types(
    schema: &TableSchema,
    relation: &Relation,
    tuple: &Tuple,
    types: &crate::TypeRegistry,
) -> Result<Row> {
    types.validate_relation(schema, relation)?;
    relation.validate_row(tuple)?;
    let mut row = Vec::with_capacity(tuple.len());
    for ((cell, column), pg) in tuple.iter().zip(&schema.columns).zip(&relation.columns) {
        row.push(match cell {
            Cell::Null => Value::Null,
            Cell::UnchangedToast => return Err(Error::UnchangedToast(relation.id)),
            Cell::Text(bytes) => types.decode(&column.data_type, pg.type_oid, bytes, false)?,
            Cell::Binary(bytes) => types.decode(&column.data_type, pg.type_oid, bytes, true)?,
        });
    }
    schema.validate_row(&row)?;
    Ok(row)
}

fn invalid(kind: &ColumnType) -> Error {
    Error::Value(format!("invalid or unsupported {kind:?} representation"))
}

fn postgres_era(text: &str) -> (&str, bool) {
    text.strip_suffix(" BC")
        .map_or((text, false), |date| (date, true))
}

// PostgreSQL emits unsigned wide years and a BC suffix. Integer Gregorian
// arithmetic covers its full finite range, beyond chrono's calendar range.
fn postgres_days(date: &str, bc: bool) -> Option<i64> {
    let mut parts = date.split('-');
    let year = parts.next()?;
    if !year.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let year = year.parse::<i32>().ok()?;
    if year <= 0 {
        return None;
    }
    let year = if bc {
        1 - i64::from(year)
    } else {
        i64::from(year)
    };
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => return None,
    };
    if day == 0 || day > max_day || parts.next().is_some() {
        return None;
    }
    // March-based Gregorian eras; subtract the March-based day of 2000-01-01.
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month + 2) / 5 + i64::from(day) - 1;
    Some(
        era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year
            - 730_425,
    )
}

fn postgres_offset(offset: &str) -> Option<i64> {
    let sign = match offset.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let mut parts = offset[1..].split(':');
    let hours = parts.next()?.parse::<u32>().ok()?;
    let minutes = parts
        .next()
        .map(str::parse::<u32>)
        .transpose()
        .ok()?
        .unwrap_or(0);
    let seconds = parts
        .next()
        .map(str::parse::<u32>)
        .transpose()
        .ok()?
        .unwrap_or(0);
    if hours > 15 || minutes > 59 || seconds > 59 || parts.next().is_some() {
        return None;
    }
    Some(sign * i64::from(hours * 3600 + minutes * 60 + seconds))
}

// These are PostgreSQL's IS_VALID_DATE / IS_VALID_TIMESTAMP finite bounds.
// Both wire formats also require that conversion to the model epoch fits.
fn date_value(days: i64, kind: &ColumnType) -> Result<Value> {
    if !(-2_451_545..2_145_031_949).contains(&days) {
        return Err(invalid(kind));
    }
    Ok(Value::Date(
        i32::try_from(days + 10_957).map_err(|_| invalid(kind))?,
    ))
}

fn timestamp_value(micros: i64, kind: &ColumnType) -> Result<Value> {
    if !(-211_813_488_000_000_000..9_223_371_331_200_000_000).contains(&micros) {
        return Err(invalid(kind));
    }
    let timestamp = micros
        .checked_add(946_684_800_000_000)
        .ok_or_else(|| invalid(kind))?;
    Ok(if matches!(kind, ColumnType::TimestampTzMicros) {
        Value::TimestampTzMicros(timestamp)
    } else {
        Value::TimestampMicros(timestamp)
    })
}

pub(crate) fn decode_text(kind: &ColumnType, oid: u32, bytes: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(bytes).map_err(|_| invalid(kind))?;
    Ok(match kind {
        ColumnType::Bool => Value::Bool(match text {
            "t" => true,
            "f" => false,
            _ => return Err(invalid(kind)),
        }),
        ColumnType::Int32 => {
            let value = text.parse::<i32>().map_err(|_| invalid(kind))?;
            if oid == 21 && i16::try_from(value).is_err() {
                return Err(invalid(kind));
            }
            Value::Int32(value)
        }
        ColumnType::Int64 => Value::Int64(text.parse().map_err(|_| invalid(kind))?),
        ColumnType::Float64 => Value::Float64(if oid == 700 {
            f64::from(text.parse::<f32>().map_err(|_| invalid(kind))?)
        } else {
            text.parse().map_err(|_| invalid(kind))?
        }),
        ColumnType::String => Value::String(text.to_owned()),
        ColumnType::Binary => {
            let hex = text.strip_prefix("\\x").ok_or_else(|| invalid(kind))?;
            if hex.len() % 2 != 0 {
                return Err(invalid(kind));
            }
            let mut bytes = Vec::with_capacity(hex.len() / 2);
            for pair in hex.as_bytes().chunks_exact(2) {
                let a = (pair[0] as char)
                    .to_digit(16)
                    .ok_or_else(|| invalid(kind))?;
                let b = (pair[1] as char)
                    .to_digit(16)
                    .ok_or_else(|| invalid(kind))?;
                bytes.push(((a << 4) | b) as u8);
            }
            Value::Binary(bytes)
        }
        ColumnType::Date => {
            let (text, bc) = postgres_era(text);
            date_value(postgres_days(text, bc).ok_or_else(|| invalid(kind))?, kind)?
        }
        ColumnType::TimestampMicros | ColumnType::TimestampTzMicros => {
            let (text, bc) = postgres_era(text);
            let (date, time) = text.split_once(' ').ok_or_else(|| invalid(kind))?;
            let days = postgres_days(date, bc).ok_or_else(|| invalid(kind))?;
            let (time, offset_seconds) = if matches!(kind, ColumnType::TimestampTzMicros) {
                let index = time.find(['+', '-']).ok_or_else(|| invalid(kind))?;
                let (time, offset) = time.split_at(index);
                (time, postgres_offset(offset).ok_or_else(|| invalid(kind))?)
            } else {
                (time, 0)
            };
            let time = NaiveTime::parse_from_str(time, "%H:%M:%S%.f").map_err(|_| invalid(kind))?;
            if time.nanosecond() >= 1_000_000_000 || time.nanosecond() % 1_000 != 0 {
                return Err(invalid(kind));
            }
            let micros = i128::from(days) * 86_400_000_000
                + i128::from(i64::from(time.num_seconds_from_midnight()) - offset_seconds)
                    * 1_000_000
                + i128::from(time.nanosecond() / 1_000);
            timestamp_value(i64::try_from(micros).map_err(|_| invalid(kind))?, kind)?
        }
        ColumnType::Uuid => Value::Uuid(
            *uuid::Uuid::parse_str(text)
                .map_err(|_| invalid(kind))?
                .as_bytes(),
        ),
        ColumnType::Decimal { scale, .. } => {
            let negative = text.starts_with('-');
            let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
            let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
            if whole.is_empty()
                || !whole
                    .bytes()
                    .chain(fraction.bytes())
                    .all(|b| b.is_ascii_digit())
            {
                return Err(invalid(kind));
            }
            let fractional = fraction.as_bytes();
            if fractional
                .get(usize::from(*scale)..)
                .is_some_and(|s| s.iter().any(|b| *b != b'0'))
            {
                return Err(invalid(kind));
            }
            let mut unscaled: i128 = 0;
            for digit in whole
                .bytes()
                .chain(fraction.bytes().take(usize::from(*scale)))
                .chain(std::iter::repeat_n(
                    b'0',
                    usize::from(*scale).saturating_sub(fraction.len()),
                ))
            {
                unscaled = unscaled
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(i128::from(digit - b'0')))
                    .ok_or_else(|| invalid(kind))?;
            }
            Value::Decimal {
                unscaled: if negative { -unscaled } else { unscaled },
                scale: *scale,
            }
        }
    })
}

pub(crate) fn decode_binary(kind: &ColumnType, oid: u32, bytes: &[u8]) -> Result<Value> {
    Ok(match (kind, oid, bytes.len()) {
        (ColumnType::Bool, _, 1) if bytes[0] <= 1 => Value::Bool(bytes[0] == 1),
        (ColumnType::Int32, 21, 2) => {
            Value::Int32(i16::from_be_bytes(bytes.try_into().unwrap()).into())
        }
        (ColumnType::Int32, 23, 4) => Value::Int32(i32::from_be_bytes(bytes.try_into().unwrap())),
        (ColumnType::Int64, _, 8) => Value::Int64(i64::from_be_bytes(bytes.try_into().unwrap())),
        (ColumnType::Float64, 700, 4) => {
            Value::Float64(f64::from(f32::from_be_bytes(bytes.try_into().unwrap())))
        }
        (ColumnType::Float64, 701, 8) => {
            Value::Float64(f64::from_be_bytes(bytes.try_into().unwrap()))
        }
        (ColumnType::String, _, _) => Value::String(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid(kind))?
                .to_owned(),
        ),
        (ColumnType::Binary, _, _) => Value::Binary(bytes.to_vec()),
        (ColumnType::Date, _, 4) => date_value(
            i64::from(i32::from_be_bytes(bytes.try_into().unwrap())),
            kind,
        )?,
        (ColumnType::TimestampMicros | ColumnType::TimestampTzMicros, _, 8) => {
            timestamp_value(i64::from_be_bytes(bytes.try_into().unwrap()), kind)?
        }
        (ColumnType::Uuid, _, 16) => Value::Uuid(bytes.try_into().unwrap()),
        (ColumnType::Decimal { scale, .. }, _, length) if length >= 8 && length % 2 == 0 => {
            let count = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
            let weight = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
            let sign = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
            if count * 2 + 8 != length || !matches!(sign, 0 | 0x4000) {
                return Err(invalid(kind));
            }
            let mut unscaled = 0i128;
            for (i, pair) in bytes[8..].chunks_exact(2).enumerate() {
                let digit = u16::from_be_bytes(pair.try_into().unwrap());
                if digit >= 10_000 {
                    return Err(invalid(kind));
                }
                if digit == 0 {
                    continue;
                }
                let power = (i32::from(weight) - i as i32) * 4 + i32::from(*scale);
                let value = if power >= 0 {
                    i128::from(digit)
                        .checked_mul(
                            10i128
                                .checked_pow(power as u32)
                                .ok_or_else(|| invalid(kind))?,
                        )
                        .ok_or_else(|| invalid(kind))?
                } else {
                    let divisor = 10i128
                        .checked_pow((-power) as u32)
                        .ok_or_else(|| invalid(kind))?;
                    if i128::from(digit) % divisor != 0 {
                        return Err(invalid(kind));
                    }
                    i128::from(digit) / divisor
                };
                unscaled = unscaled.checked_add(value).ok_or_else(|| invalid(kind))?;
            }
            Value::Decimal {
                unscaled: if sign == 0x4000 { -unscaled } else { unscaled },
                scale: *scale,
            }
        }
        _ => return Err(invalid(kind)),
    })
}
