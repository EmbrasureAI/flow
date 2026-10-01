//! pgoutput streams in-progress transactions. A quarantine decided inside one
//! (TRUNCATE, an undecodable Relation or row) must not affect other
//! transactions, and must disappear with a rolled-back (sub)transaction.
use bytes::Bytes;
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, QuarantineFormat, SourceId,
    SourceTransaction, TableId, TableSchema, Value,
};
use flow_pg_source::{
    CaptureAssembler, Cell, Column as PgColumn, Error, Relation, SourceEvent, SpoolConfig,
    TransactionSpool,
};
use std::collections::BTreeSet;
use tempfile::TempDir;

const TABLE: u32 = 11;
const TRUNCATED: &str = "source table was truncated; resynchronization is required";

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(TABLE),
        version: 1,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int32,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "body".into(),
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
        name: "items".into(),
        replica_identity: b'f',
        columns: ["id", "body"]
            .into_iter()
            .zip([23, 25])
            .map(|(name, type_oid)| PgColumn {
                name: name.into(),
                type_oid,
                type_modifier: -1,
                identity: true,
            })
            .collect(),
    }
}

fn insert(xid: u32, subxid: u32, id: &'static str) -> SourceEvent {
    SourceEvent::Insert {
        xid,
        subxid,
        relation: TABLE,
        row: vec![
            Cell::Text(Bytes::from_static(id.as_bytes())),
            Cell::Text(Bytes::from_static(b"body")),
        ],
    }
}

fn truncate(xid: u32, subxid: u32) -> SourceEvent {
    SourceEvent::Truncate {
        xid,
        subxid,
        relations: vec![TABLE],
        cascade: false,
        restart_identity: false,
    }
}

fn start(xid: u32, first: bool) -> SourceEvent {
    SourceEvent::StreamStart { xid, first }
}

fn commit(xid: u32, lsn: u64) -> SourceEvent {
    SourceEvent::Commit {
        xid,
        commit_lsn: PgLsn(lsn),
        end_lsn: PgLsn(lsn + 1),
        commit_timestamp_micros: 0,
    }
}

struct Capture {
    _root: TempDir,
    journal: Journal,
    assembler: CaptureAssembler,
}

impl Capture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let (journal, _) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        let spool =
            TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
        let assembler =
            CaptureAssembler::new(SourceId("source".into()), spool, [schema()], 256).unwrap();
        let mut capture = Self {
            _root: root,
            journal,
            assembler,
        };
        capture.push(SourceEvent::Relation(relation()));
        capture
    }

    fn push(&mut self, event: SourceEvent) -> Option<SourceTransaction> {
        self.assembler.push(event, &mut self.journal).unwrap()
    }

    /// What the daemon does with a change: quarantine it provisionally while
    /// its streamed transaction has a standing decision for the table.
    fn change(&mut self, event: SourceEvent) {
        let xid = match &event {
            SourceEvent::Insert { xid, .. } => *xid,
            _ => unreachable!(),
        };
        match self.assembler.provisional_block(xid, TableId(TABLE)) {
            Some(reason) => {
                let reason = reason.to_owned();
                self.assembler
                    .quarantine_provisionally(event, &relation(), &reason)
                    .unwrap()
            }
            None => assert!(self.push(event).is_none()),
        }
    }

    fn truncate(&mut self, xid: u32, subxid: u32) {
        self.assembler
            .quarantine_provisionally(truncate(xid, subxid), &relation(), TRUNCATED)
            .unwrap();
    }

    fn mutations(&self, transaction: &SourceTransaction) -> Vec<Mutation> {
        self.journal
            .chunks(&transaction.mutation_chunks)
            .unwrap()
            .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
            .collect()
    }
}

fn inserted(mutations: &[Mutation]) -> Vec<Value> {
    mutations
        .iter()
        .map(|mutation| match &mutation.kind {
            MutationKind::Insert { row } => row[0].clone(),
            _ => panic!("unexpected quarantined mutation"),
        })
        .collect()
}

fn quarantine_formats(mutations: &[Mutation]) -> Vec<QuarantineFormat> {
    mutations
        .iter()
        .map(|mutation| match &mutation.kind {
            MutationKind::Quarantined { format, .. } => *format,
            _ => panic!("unquarantined change of a blocked table"),
        })
        .collect()
}

#[test]
fn rolled_back_streamed_truncate_leaves_the_table_publishing() {
    let mut capture = Capture::new();
    capture.push(start(42, true));
    capture.change(insert(42, 42, "1"));
    capture.truncate(42, 42);
    capture.change(insert(42, 42, "2"));
    assert_eq!(
        capture.assembler.provisional_block(42, TableId(TABLE)),
        Some(TRUNCATED)
    );
    capture.push(SourceEvent::StreamStop);

    // A transaction committing while 42 is undecided publishes normally.
    capture.push(SourceEvent::Begin {
        xid: 50,
        final_lsn: PgLsn(100),
        commit_timestamp_micros: 0,
    });
    assert_eq!(
        capture.assembler.provisional_block(50, TableId(TABLE)),
        None
    );
    capture.change(insert(50, 50, "3"));
    let interleaved = capture.push(commit(50, 100)).unwrap();
    assert_eq!(
        inserted(&capture.mutations(&interleaved)),
        [Value::Int32(3)]
    );

    assert_eq!(
        capture.assembler.provisional_blocks(42),
        [(TableId(TABLE), TRUNCATED.to_owned())]
    );
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 42,
    });
    assert!(capture.assembler.provisional_blocks(42).is_empty());
    assert!(!capture.assembler.is_blocked(TableId(TABLE)));

    capture.push(start(60, true));
    capture.change(insert(60, 60, "4"));
    capture.push(SourceEvent::StreamStop);
    let after = capture.push(commit(60, 200)).unwrap();
    assert_eq!(inserted(&capture.mutations(&after)), [Value::Int32(4)]);
}

#[test]
fn subtransaction_rollback_discards_its_decision_and_descendants() {
    let mut capture = Capture::new();
    capture.push(start(42, true));
    capture.change(insert(42, 42, "1"));
    // SAVEPOINT a; TRUNCATE; SAVEPOINT b; INSERT: b's change is quarantined
    // by a's standing decision, and both go when a rolls back.
    capture.truncate(42, 43);
    capture.change(insert(42, 44, "2"));
    capture.push(SourceEvent::StreamStop);
    // An unrelated rollback of a subtransaction without changes keeps it.
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 45,
    });
    assert_eq!(capture.assembler.provisional_blocks(42).len(), 1);
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 44,
    });
    assert_eq!(capture.assembler.provisional_blocks(42).len(), 1);
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 43,
    });
    assert!(capture.assembler.provisional_blocks(42).is_empty());

    capture.push(start(42, false));
    assert_eq!(
        capture.assembler.provisional_block(42, TableId(TABLE)),
        None
    );
    capture.change(insert(42, 42, "3"));
    capture.push(SourceEvent::StreamStop);
    let committed = capture.push(commit(42, 100)).unwrap();
    assert!(!capture.assembler.is_blocked(TableId(TABLE)));
    assert_eq!(
        inserted(&capture.mutations(&committed)),
        [Value::Int32(1), Value::Int32(3)]
    );
}

#[test]
fn committed_streamed_decision_quarantines_the_whole_transaction() {
    let mut capture = Capture::new();
    capture.push(start(42, true));
    capture.change(insert(42, 42, "1"));
    capture.push(SourceEvent::StreamStop);
    // A rolled-back sibling before the decision does not remove it.
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 43,
    });
    capture.push(start(42, false));
    capture.truncate(42, 42);
    capture.change(insert(42, 44, "2"));
    capture.push(SourceEvent::StreamStop);
    assert_eq!(
        capture.assembler.provisional_blocks(42),
        [(TableId(TABLE), TRUNCATED.to_owned())]
    );

    // Fail closed: the caller must block the table before Commit.
    assert!(matches!(
        capture.assembler.push(commit(42, 100), &mut capture.journal),
        Err(Error::Protocol(message)) if message.contains("not blocked")
    ));

    let mut capture = Capture::new();
    capture.push(start(42, true));
    capture.change(insert(42, 42, "1"));
    capture.truncate(42, 42);
    capture.change(insert(42, 42, "2"));
    capture.push(SourceEvent::StreamStop);
    capture.assembler.block_table(TableId(TABLE)).unwrap();
    let committed = capture.push(commit(42, 100)).unwrap();
    assert_eq!(committed.mutation_count(TableId(TABLE)), Some(3));
    assert_eq!(
        quarantine_formats(&capture.mutations(&committed)),
        [
            QuarantineFormat::DecodedMutationV1,
            QuarantineFormat::PostgresEventV1,
            QuarantineFormat::PostgresEventV1,
        ]
    );
}

#[test]
fn undecodable_relation_is_scoped_to_its_streamed_transaction() {
    let mut capture = Capture::new();
    assert!(matches!(
        capture
            .assembler
            .set_undecodable(TableId(TABLE), Some("type changed")),
        Err(Error::Protocol(_))
    ));
    capture.push(start(42, true));
    capture
        .assembler
        .set_undecodable(TableId(TABLE), Some("type changed"))
        .unwrap();
    assert!(capture.assembler.is_undecodable(TableId(TABLE)));
    capture.change(insert(42, 42, "1"));
    capture.push(SourceEvent::StreamStop);
    // Other transactions receive their own Relation and decode normally.
    assert!(!capture.assembler.is_undecodable(TableId(TABLE)));
    assert_eq!(
        capture.assembler.provisional_block(42, TableId(TABLE)),
        Some("type changed")
    );
    capture.push(SourceEvent::Begin {
        xid: 50,
        final_lsn: PgLsn(100),
        commit_timestamp_micros: 0,
    });
    capture.change(insert(50, 50, "2"));
    let interleaved = capture.push(commit(50, 100)).unwrap();
    assert_eq!(
        inserted(&capture.mutations(&interleaved)),
        [Value::Int32(2)]
    );

    // A later decodable Relation in 42 clears its undecodable shape; the row
    // quarantined under it still stands.
    capture.push(start(42, false));
    capture
        .assembler
        .set_undecodable(TableId(TABLE), None)
        .unwrap();
    assert_eq!(
        capture.assembler.provisional_blocks(42),
        [(TableId(TABLE), "type changed".to_owned())]
    );
    capture.push(SourceEvent::StreamStop);
    capture.push(SourceEvent::Abort {
        xid: 42,
        subxid: 42,
    });
    assert!(!capture.assembler.is_blocked(TableId(TABLE)));
}

/// PostgreSQL can roll back a streamed subtransaction without sending its
/// abort; capture then excludes it at the commit once PostgreSQL's commit log
/// reports the rollback. Its changes go, but a provisional decision it made is
/// kept, so the table is blocked although the rolled-back TRUNCATE never
/// committed. That is a conservative availability limit, not the semantics of
/// a received rollback: the decision never lets through a change it stops.
#[test]
fn rollback_excluded_at_commit_keeps_its_provisional_decision() {
    let mut capture = Capture::new();
    capture.push(start(42, true));
    capture.change(insert(42, 42, "1"));
    capture.truncate(42, 43);
    capture.push(SourceEvent::StreamStop);
    assert_eq!(
        capture.assembler.subtransactions(42).unwrap(),
        BTreeSet::from([43])
    );
    capture
        .assembler
        .exclude_rolled_back(42, &BTreeSet::from([43]))
        .unwrap();
    assert_eq!(
        capture.assembler.provisional_blocks(42),
        [(TableId(TABLE), TRUNCATED.to_owned())]
    );
    assert_eq!(
        capture.assembler.provisional_block(42, TableId(TABLE)),
        Some(TRUNCATED)
    );
}
