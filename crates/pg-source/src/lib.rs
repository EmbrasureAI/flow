//! PostgreSQL capture through a pinned rust-postgres transport and a strict
//! pgoutput protocol-2 decoder. Transport authentication/TLS is supplied by the
//! caller's `tokio_postgres::Client`; ACK policy belongs to the coordinator.

mod types;
pub use types::TypeRegistry;
mod capture;
mod projection;
mod protocol;
pub use projection::{EventProjector, project_relation};
mod schema;
mod snapshot;
pub use schema::{
    ColumnMetadata, TABLE_METADATA_BATCH_SIZE, TableMetadata, TableMetadataRequest,
    fetch_table_metadata, fetch_table_metadata_batch, fetch_table_metadata_selected,
    nullable_successor, nullable_successor_with_types, same_wire_schema, validate_schema_metadata,
};
mod spool;
pub use capture::{CaptureAssembler, QUARANTINE_WRAP_BYTES, decode_row, decode_row_with_types};
pub use protocol::{Cell, Column, Decoder, Relation, SourceEvent, Tuple};
pub use snapshot::{
    SnapshotSession, TablePreflight, decode_copy_row, decode_copy_row_with_types, export_snapshot,
    export_temporary_snapshot, fetch_relation, preflight_table,
};
pub use spool::{SpoolConfig, TransactionSpool};
pub use tokio_postgres;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use flow_model::PgLsn;
use futures::{SinkExt, StreamExt};
use std::{
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio_postgres::{Client, CopyBothDuplex};

pub const TRANSPORT_REVISION: &str = "c4b8de06aaa99f71800126bedcca6e623d368357";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres transport: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("invalid pgoutput message: {0}")]
    Protocol(&'static str),
    #[error("unsupported pgoutput message tag: {0:#x}")]
    Unsupported(u8),
    #[error("table {0} requires REPLICA IDENTITY FULL for mutable replication")]
    ReplicaIdentity(u32),
    #[error("{0}")]
    DefaultIdentity(String),
    #[error(
        "table {0} contains an unchanged TOAST value; pause publication until a complete row image is available"
    )]
    UnchangedToast(u32),
    #[error("source spool IO: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "source spool quota exhausted: {used} bytes in use, {requested} requested, limit {quota} (limits.spool_bytes); open source transactions hold their rows here until they commit"
    )]
    SpoolQuota {
        used: u64,
        requested: u64,
        quota: u64,
    },
    #[error(
        "more than {limit} source transactions are open in the capture spool; increase limits.spool_transactions"
    )]
    SpoolTransactions { limit: usize },
    #[error(
        "source transaction {xid} captured rows in more than {limit} subtransactions (savepoints); increase limits.spool_subtransactions"
    )]
    SpoolSubtransactions { xid: u32, limit: usize },
    /// Table-scoped: PostgreSQL changed an existing row of an insert-only target.
    #[error(
        "append-only table {table} received {operation}; resynchronization or append_only = false is required"
    )]
    AppendOnly { table: u32, operation: &'static str },
    /// Table-scoped: one row change cannot fit a journal chunk.
    #[error(
        "a row change on table {table} needs {bytes} bytes including 40 bytes of chunk framing, exceeding limits.chunk_bytes ({limit} bytes); raise limits.chunk_bytes and resynchronize the table"
    )]
    RowLimit { table: u32, bytes: u64, limit: u64 },
    #[error("ingress journal: {0}")]
    Journal(#[from] flow_ingress_journal::Error),
    #[error("mutation encoding: {0}")]
    Codec(#[from] bincode::Error),
    #[error("source row: {0}")]
    Row(#[from] flow_model::ModelError),
    #[error("source value: {0}")]
    Value(String),
    #[error("source configuration: {0}")]
    Config(&'static str),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Default)]
pub struct Acknowledgement {
    pub received: PgLsn,
    pub durable: PgLsn,
    pub materialized: PgLsn,
}

#[async_trait]
pub trait PostgresSource: Send {
    async fn next(&mut self) -> Result<Option<SourceEvent>>;
    /// The coordinator supplies a proven contiguous prefix. `durable` is the
    /// chosen flush ACK (materialized by default), never the server's WAL end.
    async fn acknowledge(&mut self, progress: Acknowledgement) -> Result<()>;
}

pub struct PgOutputSource {
    stream: Pin<Box<CopyBothDuplex<Bytes>>>,
    decoder: Decoder,
    acknowledgement: Acknowledgement,
    pub received_lsn: PgLsn,
}

impl PgOutputSource {
    /// Start/restart an existing slot. Never creates, drops, or advances a slot.
    /// The caller must retain the client connection task for the stream's life.
    pub async fn start(
        client: &Client,
        slot: &str,
        publication: &str,
        resume_lsn: PgLsn,
        max_message_bytes: usize,
    ) -> Result<Self> {
        if slot.is_empty()
            || !slot
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(Error::Config(
                "slot names must contain lowercase letters, digits, or underscores",
            ));
        }
        if publication.is_empty() || max_message_bytes == 0 {
            return Err(Error::Config("publication and message limit are required"));
        }
        // Quote the publication as a SQL identifier inside pgoutput's identifier
        // list, then quote that entire list as a SQL literal.
        let publications = quote_literal(&quote_identifier(publication));
        client.batch_execute("SET DateStyle TO 'ISO, YMD'; SET TimeZone TO 'UTC'; SET bytea_output TO 'hex'; SET extra_float_digits TO 3").await?;
        let query = format!(
            "START_REPLICATION SLOT {slot} LOGICAL {:X}/{:X} (proto_version '2', publication_names {publications}, streaming 'on', binary 'true', messages 'true')",
            resume_lsn.0 >> 32,
            resume_lsn.0 & 0xffff_ffff
        );
        let stream = Box::pin(client.copy_both_simple::<Bytes>(&query).await?);
        // A requested replay position proves neither local materialization nor
        // the configured ACK durability policy. Keepalives must stay at zero
        // until the coordinator explicitly supplies its proven watermarks.
        Ok(Self {
            stream,
            decoder: Decoder::new(max_message_bytes),
            acknowledgement: Acknowledgement::default(),
            received_lsn: resume_lsn,
        })
    }

    /// Bound concurrently streamed transactions to the capture spool's limit.
    pub fn set_max_streamed_transactions(&mut self, limit: usize) {
        self.decoder.set_max_streamed_transactions(limit);
    }

    async fn feedback(&mut self, progress: Acknowledgement) -> Result<()> {
        let unix_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Protocol("system clock precedes Unix epoch"))?
            .as_micros();
        let postgres_micros = i64::try_from(unix_micros)
            .map_err(|_| Error::Protocol("system clock overflow"))?
            - 946_684_800_000_000;
        let mut bytes = BytesMut::with_capacity(34);
        bytes.put_u8(b'r');
        bytes.put_u64(progress.received.0);
        bytes.put_u64(progress.durable.0);
        bytes.put_u64(progress.materialized.0);
        bytes.put_i64(postgres_micros);
        bytes.put_u8(0);
        self.stream.as_mut().send(bytes.freeze()).await?;
        Ok(())
    }
}

#[async_trait]
impl PostgresSource for PgOutputSource {
    async fn next(&mut self) -> Result<Option<SourceEvent>> {
        loop {
            let Some(bytes) = self.stream.as_mut().next().await.transpose()? else {
                return Ok(None);
            };
            match bytes.first() {
                Some(b'w') if bytes.len() >= 25 => {
                    let start = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
                    // wal_end is the server's current WAL high-water mark, not
                    // evidence that its transactions have reached this client.
                    self.received_lsn = self.received_lsn.max(PgLsn(start));
                    return self.decoder.decode(bytes.slice(25..)).map(Some);
                }
                Some(b'k') if bytes.len() == 18 => {
                    if bytes[17] == 1 {
                        self.feedback(self.acknowledgement).await?;
                    } else if bytes[17] != 0 {
                        return Err(Error::Protocol("invalid keepalive reply flag"));
                    }
                }
                _ => return Err(Error::Protocol("invalid replication envelope")),
            }
        }
    }

    async fn acknowledge(&mut self, progress: Acknowledgement) -> Result<()> {
        if progress.materialized > progress.durable
            || progress.durable > progress.received
            || progress.durable < self.acknowledgement.durable
            || progress.materialized < self.acknowledgement.materialized
        {
            return Err(Error::Protocol(
                "acknowledgement watermarks must be ordered and monotonic",
            ));
        }
        self.feedback(progress).await?;
        self.acknowledgement = progress;
        Ok(())
    }
}

/// Widen a 32-bit XID to a full XID near `reference`, a full XID that
/// PostgreSQL has assigned or is about to assign. An XID still in use lies
/// within 2^31 of every other, so the nearest full XID with these low 32 bits
/// is the one PostgreSQL assigned, even when `reference` is in the next or
/// previous epoch. `None` if that would precede the first normal XID.
pub fn widen_xid(reference: u64, xid: u32) -> Option<u64> {
    let distance = i64::from(xid.wrapping_sub(reference as u32) as i32);
    let full = i64::try_from(reference).ok()?.checked_add(distance)?;
    u64::try_from(full).ok().filter(|&full| full >= 3)
}

/// Of these subtransaction XIDs of a transaction whose commit has been
/// received, those PostgreSQL rolled back, from its commit log. The commit
/// record is flushed before the commit log and process array record it, so a
/// status may still read as in progress briefly; it is retried until
/// `deadline`. A status PostgreSQL no longer keeps, or one still in progress
/// at the deadline, is an error: changes are never published on a guess.
pub async fn rolled_back_subtransactions(
    client: &Client,
    xids: &std::collections::BTreeSet<u32>,
    deadline: std::time::Duration,
) -> Result<std::collections::BTreeSet<u32>> {
    let started = std::time::Instant::now();
    let mut delay = std::time::Duration::from_millis(10);
    loop {
        // One past the latest completed XID: at most a few XIDs behind any
        // assigned XID of a transaction whose commit is being recorded.
        let next = client
            .query_one(
                "SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text",
                &[],
            )
            .await?
            .get::<_, String>(0)
            .parse::<u64>()
            .map_err(|_| Error::Protocol("unparseable snapshot xmax"))?;
        let full = xids
            .iter()
            .map(|&xid| {
                widen_xid(next, xid)
                    .map(|full| (full, xid))
                    .ok_or(Error::Protocol(
                        "subtransaction XID precedes the first normal XID",
                    ))
            })
            .collect::<Result<std::collections::BTreeMap<u64, u32>>>()?;
        let keys = full
            .keys()
            .map(|&full| {
                i64::try_from(full).map_err(|_| Error::Protocol("subtransaction XID out of range"))
            })
            .collect::<Result<Vec<i64>>>()?;
        let rows = client
            .query(
                "SELECT x, pg_catalog.pg_xact_status(x::text::pg_catalog.xid8)
                 FROM unnest($1::bigint[]) AS x",
                &[&keys],
            )
            .await?;
        let mut rolled_back = std::collections::BTreeSet::new();
        let mut in_progress = false;
        for row in rows {
            let key: i64 = row.get(0);
            let xid = u64::try_from(key)
                .ok()
                .and_then(|key| full.get(&key))
                .copied()
                .ok_or(Error::Protocol("unexpected subtransaction status row"))?;
            match row.get::<_, Option<String>>(1).as_deref() {
                Some("committed") => {}
                Some("aborted") => {
                    rolled_back.insert(xid);
                }
                Some(_) => in_progress = true,
                None => {
                    return Err(Error::Protocol(
                        "subtransaction status is no longer available",
                    ));
                }
            }
        }
        if !in_progress {
            return Ok(rolled_back);
        }
        if started.elapsed() >= deadline {
            return Err(Error::Protocol(
                "subtransaction of a received commit is still in progress",
            ));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(std::time::Duration::from_millis(500));
    }
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod xid_tests {
    use super::widen_xid;

    #[test]
    fn widening_picks_the_nearest_epoch_across_a_wrap() {
        const EPOCH: u64 = 1 << 32;
        // Same epoch, behind and slightly ahead of the reference: a commit
        // can be decoded before its XIDs count as completed.
        assert_eq!(widen_xid(EPOCH + 1000, 900), Some(EPOCH + 900));
        assert_eq!(widen_xid(EPOCH + 1000, 1005), Some(EPOCH + 1005));
        // The reference has wrapped into the next epoch; the XID has not.
        assert_eq!(widen_xid(EPOCH + 5, u32::MAX - 10), Some(EPOCH - 11));
        // The XID has wrapped; the reference, just behind it, has not.
        assert_eq!(widen_xid(2 * EPOCH - 3, 4), Some(2 * EPOCH + 4));
        // Nothing precedes the first epoch, nor the first normal XID.
        assert_eq!(widen_xid(100, u32::MAX - 10), None);
        assert_eq!(widen_xid(100, 2), None);
        assert_eq!(widen_xid(100, 3), Some(3));
    }
}
