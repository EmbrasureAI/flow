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

/// Subtransaction statuses read per query, bounding each query's parameter,
/// response and duration however many subtransactions a transaction has.
const STATUS_BATCH: usize = 1024;

/// Of these subtransaction XIDs of a transaction whose commit has been
/// received, those PostgreSQL rolled back (sorted), from its commit log. The
/// commit record is flushed before the commit log and process array record
/// it, so a status may still read as in progress briefly; only those are
/// re-read. One `deadline` bounds every query and retry. A status PostgreSQL
/// no longer keeps, one still in progress at the deadline, or a query that
/// does not finish in time is an error: changes are never published on a
/// guess.
pub async fn rolled_back_subtransactions(
    client: &Client,
    xids: &[u32],
    deadline: std::time::Duration,
) -> Result<Vec<u32>> {
    resolve_statuses(
        xids,
        deadline,
        || async move {
            client
                .query_one(
                    "SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text",
                    &[],
                )
                .await?
                .get::<_, String>(0)
                .parse::<u64>()
                .map_err(|_| Error::Protocol("unparseable snapshot xmax"))
        },
        |keys: Vec<i64>| async move {
            Ok::<Vec<(i64, Option<String>)>, Error>(
                client
                    .query(
                        "SELECT x, pg_catalog.pg_xact_status(x::text::pg_catalog.xid8)
                     FROM pg_catalog.unnest($1::bigint[]) AS x",
                        &[&keys],
                    )
                    .await?
                    .into_iter()
                    .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<String>>(1)))
                    .collect(),
            )
        },
    )
    .await
}

/// [`rolled_back_subtransactions`] over injected queries: `next` reads one
/// past the latest completed full XID, `status` reads the statuses of up to
/// [`STATUS_BATCH`] full XIDs. Memory is bounded by the XID list itself, the
/// in-progress remainder and one batch.
async fn resolve_statuses<N, NF, S, SF>(
    xids: &[u32],
    deadline: std::time::Duration,
    mut next: N,
    mut status: S,
) -> Result<Vec<u32>>
where
    N: FnMut() -> NF,
    NF: std::future::Future<Output = Result<u64>>,
    S: FnMut(Vec<i64>) -> SF,
    SF: std::future::Future<Output = Result<Vec<(i64, Option<String>)>>>,
{
    let expires = tokio::time::Instant::now() + deadline;
    let mut delay = std::time::Duration::from_millis(10);
    let mut rolled_back = Vec::new();
    let mut pending: Option<Vec<u32>> = None;
    loop {
        // One past the latest completed XID. Every XID of the transaction
        // lies within 2^31 of it, which is all widening needs.
        let reference = within(expires, next()).await?;
        let mut in_progress = Vec::new();
        for batch in pending.as_deref().unwrap_or(xids).chunks(STATUS_BATCH) {
            let mut full = std::collections::BTreeMap::new();
            for &xid in batch {
                let widened = widen_xid(reference, xid).ok_or(Error::Protocol(
                    "subtransaction XID precedes the first normal XID",
                ))?;
                full.insert(
                    i64::try_from(widened)
                        .map_err(|_| Error::Protocol("subtransaction XID out of range"))?,
                    xid,
                );
            }
            let rows = within(expires, status(full.keys().copied().collect())).await?;
            if rows.len() != full.len() {
                return Err(Error::Protocol("missing subtransaction status rows"));
            }
            for (key, state) in rows {
                // Consume each expected key once: equal row counts alone
                // do not rule out a duplicate hiding an omitted status.
                let xid = full.remove(&key).ok_or(Error::Protocol(
                    "unexpected or duplicate subtransaction status row",
                ))?;
                match state.as_deref() {
                    Some("committed") => {}
                    Some("aborted") => rolled_back.push(xid),
                    Some(_) => in_progress.push(xid),
                    None => {
                        return Err(Error::Protocol(
                            "subtransaction status is no longer available",
                        ));
                    }
                }
            }
        }
        if in_progress.is_empty() {
            rolled_back.sort_unstable();
            return Ok(rolled_back);
        }
        pending = Some(in_progress);
        if tokio::time::Instant::now() + delay >= expires {
            return Err(Error::Protocol(
                "subtransaction of a received commit is still in progress",
            ));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(std::time::Duration::from_millis(500));
    }
}

/// `operation`, unless `expires` passes first.
async fn within<T>(
    expires: tokio::time::Instant,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout_at(expires, operation)
        .await
        .map_err(|_| Error::Protocol("subtransaction status check timed out"))?
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

    use super::{Error, STATUS_BATCH, resolve_statuses};
    use std::{cell::RefCell, time::Duration};

    const EPOCH: u64 = 5 << 32;

    /// Statuses for full XIDs in epoch 5: multiples of 3 rolled back, the
    /// rest committed, multiples of 7 in progress for the first `lagging`
    /// reads of each.
    struct Commitlog {
        lagging: usize,
        reads: RefCell<std::collections::HashMap<i64, usize>>,
        batches: RefCell<Vec<usize>>,
    }

    impl Commitlog {
        fn status(&self, keys: Vec<i64>) -> Vec<(i64, Option<String>)> {
            self.batches.borrow_mut().push(keys.len());
            keys.into_iter()
                .map(|key| {
                    let xid = key - EPOCH as i64;
                    let mut reads = self.reads.borrow_mut();
                    let read = reads.entry(key).or_default();
                    *read += 1;
                    let state = if xid % 7 == 0 && *read <= self.lagging {
                        "in progress"
                    } else if xid % 3 == 0 {
                        "aborted"
                    } else {
                        "committed"
                    };
                    (key, Some(state.to_owned()))
                })
                .collect()
        }
    }

    async fn resolve(log: &Commitlog, xids: &[u32], deadline: Duration) -> super::Result<Vec<u32>> {
        resolve_statuses(
            xids,
            deadline,
            || async { Ok::<u64, Error>(EPOCH + 10_000) },
            |keys| async move { Ok::<_, Error>(log.status(keys)) },
        )
        .await
    }

    #[tokio::test]
    async fn many_subtransactions_resolve_in_bounded_batches_re_reading_only_those_in_progress() {
        let xids: Vec<u32> = (3..5003).collect();
        let log = Commitlog {
            lagging: 2,
            reads: RefCell::default(),
            batches: RefCell::default(),
        };
        let rolled_back = resolve(&log, &xids, Duration::from_secs(10)).await.unwrap();
        assert_eq!(
            rolled_back,
            xids.iter()
                .copied()
                .filter(|xid| xid % 3 == 0)
                .collect::<Vec<_>>()
        );
        let batches = log.batches.borrow();
        assert!(
            batches.iter().all(|&batch| batch <= STATUS_BATCH),
            "{batches:?}"
        );
        // Five batches for every XID, then two retries of only the 714 that
        // were still in progress.
        let in_progress = xids.iter().filter(|xid| *xid % 7 == 0).count();
        assert_eq!(batches.iter().sum::<usize>(), xids.len() + 2 * in_progress);
        assert!(
            log.reads.borrow().iter().all(|(key, &reads)| {
                reads == (if (key - EPOCH as i64) % 7 == 0 { 3 } else { 1 })
            })
        );
    }

    #[tokio::test]
    async fn duplicate_status_rows_cannot_hide_a_missing_subtransaction() {
        let result = resolve_statuses(
            &[7, 8],
            Duration::from_secs(10),
            || async { Ok::<u64, Error>(EPOCH + 10_000) },
            |keys: Vec<i64>| async move {
                // A same-length response repeats the committed child and
                // omits the other child, whose status is unknown.
                Ok::<Vec<(i64, Option<String>)>, Error>(vec![
                    (keys[0], Some("committed".to_owned())),
                    (keys[0], Some("committed".to_owned())),
                ])
            },
        )
        .await;
        assert!(matches!(result, Err(Error::Protocol(message)) if message.contains("duplicate")));
    }

    #[tokio::test]
    async fn statuses_fail_closed() {
        let lagging = Commitlog {
            lagging: usize::MAX,
            reads: RefCell::default(),
            batches: RefCell::default(),
        };
        let still = resolve(&lagging, &[7, 8], Duration::from_millis(100)).await;
        assert!(matches!(still, Err(Error::Protocol(message)) if message.contains("in progress")));

        let forgotten = resolve_statuses(
            &[7],
            Duration::from_secs(10),
            || async { Ok::<u64, Error>(EPOCH + 10_000) },
            |keys: Vec<i64>| async move {
                Ok::<Vec<(i64, Option<String>)>, Error>(
                    keys.into_iter().map(|key| (key, None)).collect(),
                )
            },
        )
        .await;
        assert!(
            matches!(forgotten, Err(Error::Protocol(message)) if message.contains("no longer available"))
        );

        let hung = resolve_statuses(
            &[7],
            Duration::from_millis(100),
            || async { Ok::<u64, Error>(EPOCH + 10_000) },
            |_: Vec<i64>| std::future::pending::<super::Result<Vec<(i64, Option<String>)>>>(),
        )
        .await;
        assert!(matches!(hung, Err(Error::Protocol(message)) if message.contains("timed out")));
    }
}
