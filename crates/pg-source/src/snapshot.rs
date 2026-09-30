use crate::{Column, Error, Relation, Result, quote_identifier, quote_literal};
use flow_model::PgLsn;
use flow_model::{Row, TableSchema, Value};
use tokio_postgres::{
    Client, CopyOutStream, GenericClient, IsolationLevel, SimpleQueryMessage, Transaction,
    binary_copy::BinaryCopyOutRow,
    types::{FromSql, Type},
};

#[derive(Debug)]
pub struct TablePreflight {
    pub relation_oid: u32,
    pub replica_identity: u8,
    /// Actual primary-key attribute numbers, not pgoutput replica-identity
    /// flags (FULL identity can describe more columns than the primary key).
    pub primary_key_attributes: Vec<i16>,
}

pub async fn preflight_table(
    client: &(impl GenericClient + Sync),
    namespace: &str,
    table: &str,
    mutable: bool,
) -> Result<TablePreflight> {
    let row = client.query_opt("SELECT c.oid, c.relkind::text, c.relreplident::text, ARRAY(SELECT k.attnum::smallint FROM pg_catalog.pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinal) WHERE i.indrelid = c.oid AND i.indisprimary AND k.ordinal <= i.indnkeyatts ORDER BY k.ordinal) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')", &[&namespace, &table]).await?.ok_or(Error::Config("source table not found"))?;
    let relation_oid: u32 = row.get(0);
    let relation_kind: String = row.get(1);
    if relation_kind != "r" {
        return Err(Error::Config(
            "partitioned PostgreSQL source roots are unsupported; replicate a regular table",
        ));
    }
    let identity: String = row.get(2);
    let replica_identity = identity
        .as_bytes()
        .first()
        .copied()
        .ok_or(Error::Protocol("empty replica identity"))?;
    let primary_key_attributes = row.get::<_, Vec<i16>>(3);
    if mutable && primary_key_attributes.is_empty() {
        return Err(Error::Config(
            "mutable replicated tables require a primary key",
        ));
    }
    if mutable && !matches!(replica_identity, b'f' | b'd') {
        return Err(Error::ReplicaIdentity(relation_oid));
    }
    Ok(TablePreflight {
        relation_oid,
        replica_identity,
        primary_key_attributes,
    })
}

/// Full physical-column metadata for the initial unfiltered publication profile.
/// The caller must compare its configured projection and primary-key columns
/// against this catalog result before starting snapshot copy or CDC.
pub async fn fetch_relation(
    client: &(impl GenericClient + Sync),
    namespace: &str,
    table: &str,
    mutable: bool,
) -> Result<(Relation, TablePreflight)> {
    let preflight = preflight_table(client, namespace, table, mutable).await?;
    let rows = client.query("SELECT attname, atttypid, atttypmod, attnum FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum", &[&preflight.relation_oid]).await?;
    let columns = rows
        .into_iter()
        .map(|row| Column {
            name: row.get(0),
            type_oid: row.get(1),
            type_modifier: row.get(2),
            identity: preflight.primary_key_attributes.contains(&row.get(3)),
        })
        .collect();
    let relation = Relation {
        id: preflight.relation_oid,
        namespace: namespace.to_owned(),
        name: table.to_owned(),
        replica_identity: preflight.replica_identity,
        columns,
    };
    Ok((relation, preflight))
}

pub struct SnapshotSession<'a> {
    transaction: Transaction<'a>,
    pub slot: String,
    pub consistent_lsn: PgLsn,
    pub snapshot_name: String,
}

/// Creates a NEW permanent slot and imports its exported consistent snapshot on
/// a distinct SQL connection before returning. Existing slots are never reset.
/// Keep the SQL session open until every table has been copied; CDC starts at
/// `consistent_lsn` and remains unacknowledged until initial publication.
pub async fn export_snapshot<'a>(
    replication: &Client,
    snapshot_client: &'a mut Client,
    slot: &str,
) -> Result<SnapshotSession<'a>> {
    export_slot_snapshot(replication, snapshot_client, slot, false).await
}

/// A disposable slot establishes a fresh COPY cut while the original permanent
/// source slot retains CDC. The server removes this slot when its session closes.
pub async fn export_temporary_snapshot<'a>(
    replication: &Client,
    snapshot_client: &'a mut Client,
    slot: &str,
) -> Result<SnapshotSession<'a>> {
    export_slot_snapshot(replication, snapshot_client, slot, true).await
}

async fn export_slot_snapshot<'a>(
    replication: &Client,
    snapshot_client: &'a mut Client,
    slot: &str,
    temporary: bool,
) -> Result<SnapshotSession<'a>> {
    if slot.is_empty()
        || !slot
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(Error::Config("invalid replication slot name"));
    }
    let lifetime = if temporary { " TEMPORARY" } else { "" };
    let result = replication
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT {slot}{lifetime} LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let row = result
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .ok_or(Error::Protocol("slot creation returned no snapshot"))?;
    let lsn = row
        .get("consistent_point")
        .ok_or(Error::Protocol("missing consistent point"))?;
    let (hi, lo) = lsn
        .split_once('/')
        .ok_or(Error::Protocol("invalid snapshot LSN"))?;
    let hi = u32::from_str_radix(hi, 16).map_err(|_| Error::Protocol("invalid snapshot LSN"))?;
    let lo = u32::from_str_radix(lo, 16).map_err(|_| Error::Protocol("invalid snapshot LSN"))?;
    let consistent_lsn = PgLsn(u64::from(hi) << 32 | u64::from(lo));
    let snapshot_name = row
        .get("snapshot_name")
        .ok_or(Error::Protocol("slot did not export a snapshot"))?
        .to_owned();
    SnapshotSession::import(snapshot_client, slot, consistent_lsn, &snapshot_name).await
}

impl<'a> SnapshotSession<'a> {
    /// Import before the first query in a read-only repeatable-read transaction.
    pub async fn import(
        client: &'a mut Client,
        slot: &str,
        consistent_lsn: PgLsn,
        snapshot_name: &str,
    ) -> Result<Self> {
        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        // The keeper sits idle in this transaction while other sessions copy,
        // possibly for hours. Server or role session limits must not end it;
        // PostgreSQL 17's transaction_timeout also covers active COPY.
        transaction
            .batch_execute(&format!(
                "SET TRANSACTION SNAPSHOT {}; SET LOCAL row_security = off; \
                 SET LOCAL idle_in_transaction_session_timeout = 0; \
                 SELECT pg_catalog.set_config(name, '0', true) FROM pg_catalog.pg_settings \
                 WHERE name = 'transaction_timeout'",
                quote_literal(snapshot_name)
            ))
            .await?;
        Ok(Self {
            transaction,
            slot: slot.to_owned(),
            consistent_lsn,
            snapshot_name: snapshot_name.to_owned(),
        })
    }

    pub fn transaction(&self) -> &Transaction<'a> {
        &self.transaction
    }

    /// Re-export from the SQL keeper so logical replication can start without
    /// invalidating the snapshot that later COPY workers still need to import.
    pub async fn reexport(&self) -> Result<String> {
        Ok(self
            .transaction
            .query_one("SELECT pg_catalog.pg_export_snapshot()", &[])
            .await?
            .get(0))
    }

    /// Hold the table definition stable while DML and logical decoding continue.
    pub async fn lock_table(&self, namespace: &str, table: &str) -> Result<()> {
        self.transaction
            .batch_execute(&format!(
                "LOCK TABLE ONLY {}.{} IN ACCESS SHARE MODE",
                quote_identifier(namespace),
                quote_identifier(table)
            ))
            .await?;
        Ok(())
    }

    pub async fn relation(
        &self,
        namespace: &str,
        table: &str,
        mutable: bool,
    ) -> Result<(Relation, TablePreflight)> {
        fetch_relation(&self.transaction, namespace, table, mutable).await
    }

    /// Binary COPY is streamed by the transport; the caller chooses a bounded
    /// decoder and must use the exact same publication column projection.
    /// Publication row filters are deliberately unsupported in this V1 helper.
    pub async fn copy_table(
        &self,
        namespace: &str,
        table: &str,
        columns: &[String],
    ) -> Result<CopyOutStream> {
        if columns.is_empty() {
            return Err(Error::Config("snapshot projection cannot be empty"));
        }
        let projection = columns
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "COPY (SELECT {projection} FROM ONLY {}.{}) TO STDOUT (FORMAT BINARY)",
            quote_identifier(namespace),
            quote_identifier(table)
        );
        // A streamed snapshot can outlive an application's query deadline,
        // especially while the bounded reader applies backpressure. Keep this
        // override local to the read-only snapshot transaction.
        self.transaction
            .batch_execute("SET LOCAL statement_timeout = 0")
            .await?;
        Ok(self.transaction.copy_out(&query).await?)
    }

    pub async fn finish(self) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
    pub async fn cancel(self) -> Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

/// Reuse the pinned transport's binary COPY framing and our same row/type
/// conversion as CDC. This keeps initial-copy and update encodings identical.
pub fn decode_copy_row(
    schema: &TableSchema,
    relation: &Relation,
    row: &BinaryCopyOutRow,
) -> Result<Row> {
    decode_copy_row_with_types(schema, relation, row, &crate::TypeRegistry::default())
}
pub fn decode_copy_row_with_types(
    schema: &TableSchema,
    relation: &Relation,
    row: &BinaryCopyOutRow,
    types: &crate::TypeRegistry,
) -> Result<Row> {
    types.validate_relation(schema, relation)?;
    let mut values = Vec::with_capacity(schema.columns.len());
    for (index, (column, pg)) in schema.columns.iter().zip(&relation.columns).enumerate() {
        values.push(match row.try_get::<Option<RawColumn<'_>>>(index)? {
            Some(raw) => types.decode(&column.data_type, pg.type_oid, raw.0, true)?,
            None => Value::Null,
        });
    }
    schema.validate_row(&values)?;
    Ok(values)
}

struct RawColumn<'a>(&'a [u8]);
impl<'a> FromSql<'a> for RawColumn<'a> {
    fn from_sql(
        _: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}
