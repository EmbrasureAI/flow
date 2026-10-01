//! The crash loop's large transaction: replace a key range, update part of it,
//! then delete part of it inside a savepoint that is rolled back. PostgreSQL
//! streams whatever part of the savepoint's changes exceeded its memory limit
//! before the rollback, in one or more stream blocks, and then aborts the
//! subtransaction. pgoutput messages pass through the decoder, which takes the
//! top-level XID from StreamStart and each change's own XID as its subxid,
//! into capture and the journal. No streamed change of the rolled-back
//! savepoint may be journaled, wherever stream block, capture chunk or spool
//! segment boundaries fall inside it.
//!
//! PostgreSQL can also roll back a streamed savepoint without sending its
//! abort; capture then excludes it at the commit once PostgreSQL's commit log
//! reports it rolled back. Both cases are covered.
//!
//! This covers only the stream shapes supplied here, not reconnect, recovery
//! or what PostgreSQL sends in a particular run.

use bytes::{BufMut, Bytes, BytesMut};
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, SourceId, SourceTransaction, TableId, TableSchema,
    Value,
};
use flow_pg_source::{CaptureAssembler, Decoder, SpoolConfig, TransactionSpool};
use std::collections::BTreeSet;

const TABLE: u32 = 11;
const XID: u32 = 500;
const SAVEPOINT: u32 = 501;
/// A savepoint released after the rollback: a committed child.
const RELEASED: u32 = 502;
const START: i64 = 11_000_000;
const ROWS: i64 = 12_000;
/// Rows the rolled-back savepoint deletes: `start ..= start + rows / 2`.
const SAVEPOINT_ROWS: i64 = ROWS / 2 + 1;
/// Rows updated before the savepoint (`start ..= start + rows / 10`) plus one
/// update by a child savepoint released after the rollback.
const UPDATES: i64 = ROWS / 10 + 2;
/// The crash loop's `limits.chunk_bytes`.
const CHUNK_BYTES: u32 = 65_536;

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(TABLE),
        version: 1,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "payload".into(),
                data_type: ColumnType::String,
                nullable: false,
            },
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

fn message(tag: u8, body: impl FnOnce(&mut BytesMut)) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u8(tag);
    body(&mut bytes);
    bytes.freeze()
}

fn tuple(bytes: &mut BytesMut, id: i64, payload: &str) {
    bytes.put_u16(2);
    for value in [id.to_string().as_bytes(), payload.as_bytes()] {
        bytes.put_u8(b't');
        bytes.put_u32(value.len() as u32);
        bytes.extend_from_slice(value);
    }
}

fn stream_start(first: bool) -> Bytes {
    message(b'S', |b| {
        b.put_u32(XID);
        b.put_u8(u8::from(first));
    })
}

fn stream_stop() -> Bytes {
    message(b'E', |_| {})
}

/// Streamed changes carry the XID of the (sub)transaction that made them.
fn delete(xid: u32, id: i64, payload: &str) -> Bytes {
    message(b'D', |b| {
        b.put_u32(xid);
        b.put_u32(TABLE);
        b.put_u8(b'O');
        tuple(b, id, payload);
    })
}

fn insert(id: i64) -> Bytes {
    message(b'I', |b| {
        b.put_u32(XID);
        b.put_u32(TABLE);
        b.put_u8(b'N');
        tuple(b, id, "new");
    })
}

fn update(id: i64, payload: &str) -> Bytes {
    update_in(XID, id, payload)
}

fn update_in(xid: u32, id: i64, payload: &str) -> Bytes {
    message(b'U', |b| {
        b.put_u32(xid);
        b.put_u32(TABLE);
        b.put_u8(b'O');
        tuple(b, id, "new");
        b.put_u8(b'N');
        tuple(b, id, payload);
    })
}

/// Decode and capture the transaction, sending the savepoint's deletes in
/// stream blocks of the given sizes before its rollback. Without `abort`,
/// PostgreSQL sends no abort for the rolled-back savepoint, and capture
/// excludes it as the daemon does when PostgreSQL's commit log reports it
/// rolled back.
fn journaled(savepoint_blocks: &[i64], abort: bool) -> (SourceTransaction, Vec<Mutation>) {
    let root = tempfile::tempdir().unwrap();
    let (mut journal, _) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    // Small segments so the savepoint spans several of them.
    let spool = TransactionSpool::open(
        root.path().join("spool"),
        SpoolConfig {
            segment_bytes: 2 * u64::from(CHUNK_BYTES),
            max_chunk_bytes: CHUNK_BYTES,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    let mut assembler =
        CaptureAssembler::new(SourceId("source".into()), spool, [schema()], CHUNK_BYTES).unwrap();
    let mut decoder = Decoder::new(1 << 20);
    let mut feed = |bytes| {
        let event = decoder.decode(bytes).unwrap();
        assembler.push(event, &mut journal).unwrap()
    };
    feed(stream_start(true));
    feed(message(b'R', |b| {
        b.put_u32(XID);
        b.put_u32(TABLE);
        b.extend_from_slice(b"public\0orders\0f");
        b.put_u16(2);
        for (name, oid) in [(b"id\0".as_slice(), 20), (b"payload\0".as_slice(), 25)] {
            b.put_u8(1);
            b.extend_from_slice(name);
            b.put_u32(oid);
            b.put_i32(-1);
        }
    }));
    for id in START..START + ROWS {
        feed(delete(XID, id, "old"));
    }
    for id in START..START + ROWS {
        feed(insert(id));
    }
    feed(stream_stop());
    feed(stream_start(false));
    for id in START..=START + ROWS / 10 {
        feed(update(id, "updated"));
    }
    feed(stream_stop());
    let mut next = START;
    for &rows in savepoint_blocks {
        feed(stream_start(false));
        for id in next..next + rows {
            feed(delete(SAVEPOINT, id, "new"));
        }
        next += rows;
        feed(stream_stop());
    }
    assert!(next - START <= SAVEPOINT_ROWS);
    if abort {
        feed(message(b'A', |b| {
            b.put_u32(XID);
            b.put_u32(SAVEPOINT);
        }));
    }
    feed(stream_start(false));
    feed(update_in(RELEASED, START + ROWS - 1, "released child"));
    feed(stream_stop());
    let commit = decoder
        .decode(message(b'c', |b| {
            b.put_u32(XID);
            b.put_u8(0);
            b.put_u64(100);
            b.put_u64(108);
            b.put_i64(0);
        }))
        .unwrap();
    let subtransactions = assembler.subtransactions(XID).unwrap();
    if abort {
        // The received abort already truncated the savepoint's changes.
        assert_eq!(subtransactions, BTreeSet::from([RELEASED]));
    } else {
        assert_eq!(subtransactions, BTreeSet::from([SAVEPOINT, RELEASED]));
        // As PostgreSQL's commit log reports: only the savepoint rolled back.
        assembler
            .exclude_rolled_back(XID, &BTreeSet::from([SAVEPOINT]))
            .unwrap();
    }
    let txn = assembler
        .push(commit, &mut journal)
        .unwrap()
        .expect("staged commit");
    let mutations = journal
        .chunks(&txn.mutation_chunks)
        .unwrap()
        .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
        .collect();
    (txn, mutations)
}

#[test]
fn a_delete_mutation_of_the_crash_loop_key_encodes_in_thirty_bytes() {
    let key = schema()
        .encode_key(&vec![Value::Int64(START), Value::String("x".into())])
        .unwrap();
    let delete = Mutation {
        table_id: TableId(TABLE),
        schema_version: 1,
        kind: MutationKind::Delete { key },
    };
    // Capture flushes a chunk only when the next mutation would take its
    // estimate (a 16-byte header plus the mutations) past the limit, so a
    // 64 KiB chunk holds 2184 such deletes. A savepoint's deletes start a new
    // chunk and reach chunk boundaries after 2184 and 4368 rows.
    assert_eq!(bincode::serialized_size(&delete).unwrap(), 30);
    let per_chunk = (u64::from(CHUNK_BYTES) - 16) / 30;
    assert_eq!(per_chunk, 2184);
    assert!(16 + 30 * per_chunk <= u64::from(CHUNK_BYTES));
    assert!(16 + 30 * (per_chunk + 1) > u64::from(CHUNK_BYTES));
}

#[test]
fn no_streamed_change_of_a_rolled_back_savepoint_is_journaled() {
    let start_key = schema()
        .encode_key(&vec![Value::Int64(START), Value::String(String::new())])
        .unwrap();
    let cases: &[&[i64]] = &[
        // One stream block on both sides of each chunk boundary, at the
        // nightly failure's 5039 surviving deletes and the whole savepoint.
        &[1],
        &[2183],
        &[2184],
        &[2185],
        &[4367],
        &[4368],
        &[4369],
        &[5039],
        &[SAVEPOINT_ROWS],
        // One savepoint spanning several stream blocks.
        &[2184, 2184, 671],
        &[2183, 2, 2183, 2],
        &[1, 5038],
        &[5039, 962],
        &[3000, 3001],
    ];
    for &blocks in cases {
        for abort in [true, false] {
            let (txn, mutations) = journaled(blocks, abort);
            let count = |pick: fn(&MutationKind) -> bool| {
                mutations
                    .iter()
                    .filter(|mutation| pick(&mutation.kind))
                    .count() as i64
            };
            // 12,000 deletes and 12,000 inserts of the replaced range, 1,202 updates.
            assert_eq!(
                mutations.len() as i64,
                2 * ROWS + UPDATES,
                "{blocks:?} abort {abort}"
            );
            assert_eq!(mutations.len(), 25_202, "{blocks:?} abort {abort}");
            assert_eq!(
                txn.mutation_count(TableId(TABLE)),
                Some(25_202),
                "{blocks:?} abort {abort}"
            );
            assert_eq!(
                count(|kind| matches!(kind, MutationKind::Delete { .. })),
                ROWS,
                "{blocks:?} abort {abort}"
            );
            assert_eq!(
                count(|kind| matches!(kind, MutationKind::Insert { .. })),
                ROWS
            );
            assert_eq!(
                count(|kind| matches!(kind, MutationKind::Update { .. })),
                UPDATES
            );
            // Every delete precedes the inserts: none is the savepoint's.
            let first_insert = mutations
                .iter()
                .position(|mutation| matches!(mutation.kind, MutationKind::Insert { .. }))
                .unwrap();
            assert!(
                mutations[first_insert..]
                    .iter()
                    .all(|mutation| !matches!(mutation.kind, MutationKind::Delete { .. })),
                "{blocks:?} abort {abort}"
            );
            assert!(
                matches!(&mutations[0].kind, MutationKind::Delete { key } if *key == start_key)
            );
        }
    }
}
