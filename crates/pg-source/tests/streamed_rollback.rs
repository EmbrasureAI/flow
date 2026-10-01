//! The crash loop's large transaction: replace a key range, update part of it,
//! then delete part of it inside a savepoint that is rolled back. PostgreSQL
//! streams whatever part of the savepoint's changes exceeded its memory limit
//! before the rollback and then aborts the subtransaction. No streamed change
//! of the rolled-back savepoint may reach the journal, wherever the stream
//! block, capture chunk or spool segment boundaries fall inside it.

use bytes::Bytes;
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, SourceId, TableId, TableSchema, Value,
};
use flow_pg_source::{
    CaptureAssembler, Cell, Column as PgColumn, Relation, SourceEvent, SpoolConfig,
    TransactionSpool,
};

const TABLE: u32 = 11;
const START: i64 = 11_000_000;
const ROWS: i64 = 12_000;
/// Rows the rolled-back savepoint deletes: `start ..= start + rows / 2`.
const SAVEPOINT_ROWS: i64 = ROWS / 2 + 1;
/// Rows the transaction updates before the savepoint: `start ..= start + rows / 10`.
const UPDATED_ROWS: i64 = ROWS / 10 + 1;
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

fn relation() -> Relation {
    Relation {
        id: TABLE,
        namespace: "public".into(),
        name: "orders".into(),
        replica_identity: b'f',
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
                identity: true,
            },
            PgColumn {
                name: "payload".into(),
                type_oid: 25,
                type_modifier: -1,
                identity: true,
            },
        ],
    }
}

fn tuple(id: i64, payload: &str) -> Vec<Cell> {
    vec![
        Cell::Text(Bytes::from(id.to_string())),
        Cell::Text(Bytes::from(payload.to_owned())),
    ]
}

/// Push the transaction with the first `streamed` savepoint deletes sent in a
/// stream block before the rollback, and return the journaled mutations.
fn journaled(streamed: i64) -> Vec<Mutation> {
    const XID: u32 = 500;
    const SAVEPOINT: u32 = 501;
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
    let mut push = |event| assembler.push(event, &mut journal).unwrap();
    push(SourceEvent::Relation(relation()));
    let segment = |first| SourceEvent::StreamStart { xid: XID, first };
    push(segment(true));
    for id in START..START + ROWS {
        push(SourceEvent::Delete {
            xid: XID,
            subxid: XID,
            relation: TABLE,
            old: tuple(id, "old"),
            old_is_key: false,
        });
    }
    for id in START..START + ROWS {
        push(SourceEvent::Insert {
            xid: XID,
            subxid: XID,
            relation: TABLE,
            row: tuple(id, "new"),
        });
    }
    push(SourceEvent::StreamStop);
    push(segment(false));
    for id in START..START + UPDATED_ROWS {
        push(SourceEvent::Update {
            xid: XID,
            subxid: XID,
            relation: TABLE,
            old: Some(tuple(id, "new")),
            old_is_key: false,
            row: tuple(id, "updated"),
        });
    }
    for id in START..START + streamed {
        push(SourceEvent::Delete {
            xid: XID,
            subxid: SAVEPOINT,
            relation: TABLE,
            old: tuple(id, "new"),
            old_is_key: false,
        });
    }
    push(SourceEvent::StreamStop);
    push(SourceEvent::Abort {
        xid: XID,
        subxid: SAVEPOINT,
    });
    push(segment(false));
    push(SourceEvent::Update {
        xid: XID,
        subxid: XID,
        relation: TABLE,
        old: Some(tuple(START + ROWS - 1, "new")),
        old_is_key: false,
        row: tuple(START + ROWS - 1, "after rollback"),
    });
    push(SourceEvent::StreamStop);
    let txn = push(SourceEvent::Commit {
        xid: XID,
        commit_lsn: PgLsn(100),
        end_lsn: PgLsn(108),
        commit_timestamp_micros: 1,
    })
    .expect("staged commit");
    journal
        .chunks(&txn.mutation_chunks)
        .unwrap()
        .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
        .collect()
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
    let encode = |id| {
        schema()
            .encode_key(&vec![Value::Int64(id), Value::String(String::new())])
            .unwrap()
    };
    // Stream boundaries inside, at and after the chunk boundaries, and at the
    // nightly failure's 5039 surviving deletes.
    for streamed in [1, 2184, 2185, 4368, 4369, 5039, SAVEPOINT_ROWS] {
        let mutations = journaled(streamed);
        let count = |pick: fn(&MutationKind) -> bool| {
            mutations
                .iter()
                .filter(|mutation| pick(&mutation.kind))
                .count() as i64
        };
        assert_eq!(
            count(|kind| matches!(kind, MutationKind::Delete { .. })),
            ROWS,
            "{streamed} streamed savepoint deletes"
        );
        assert_eq!(
            count(|kind| matches!(kind, MutationKind::Insert { .. })),
            ROWS
        );
        assert_eq!(
            count(|kind| matches!(kind, MutationKind::Update { .. })),
            UPDATED_ROWS + 1
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
            "{streamed} streamed savepoint deletes"
        );
        assert!(
            matches!(&mutations[0].kind, MutationKind::Delete { key } if *key == encode(START))
        );
    }
}
