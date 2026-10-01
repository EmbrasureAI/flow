//! A publication may contain tables outside the capture set. Their metadata and
//! changes never reach the spool or journal, and never hold back a commit.
use bytes::Bytes;
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, SourceId, SourceTransaction, TableId,
    TableSchema, Value,
};
use flow_pg_source::{
    CaptureAssembler, Cell, Column as PgColumn, Error, Relation, SourceEvent, SpoolConfig,
    TransactionSpool,
};
use tempfile::TempDir;

const CAPTURED: u32 = 11;
const OTHER: u32 = 99;

fn schema(id: u32) -> TableSchema {
    TableSchema {
        table_id: TableId(id),
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

// The other table deliberately violates the captured-table contract (DEFAULT
// identity, an unsupported column type). Its metadata must not be validated.
fn relation(id: u32) -> Relation {
    let other = id != CAPTURED;
    Relation {
        id,
        namespace: "public".into(),
        name: format!("table_{id}"),
        replica_identity: if other { b'd' } else { b'f' },
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                identity: true,
            },
            PgColumn {
                name: if other { "payload" } else { "body" }.into(),
                type_oid: if other { 600 } else { 25 },
                type_modifier: -1,
                identity: !other,
            },
        ],
    }
}

fn row(id: &'static str) -> Vec<Cell> {
    vec![
        Cell::Text(Bytes::from_static(id.as_bytes())),
        Cell::Text(Bytes::from_static(b"body")),
    ]
}

fn insert(xid: u32, subxid: u32, relation: u32, id: &'static str) -> SourceEvent {
    SourceEvent::Insert {
        xid,
        subxid,
        relation,
        row: row(id),
    }
}

fn update(xid: u32, subxid: u32, relation: u32) -> SourceEvent {
    SourceEvent::Update {
        xid,
        subxid,
        relation,
        old: Some(row("1")),
        old_is_key: false,
        row: vec![Cell::Text(Bytes::from_static(b"1")), Cell::UnchangedToast],
    }
}

fn delete(xid: u32, subxid: u32, relation: u32) -> SourceEvent {
    SourceEvent::Delete {
        xid,
        subxid,
        relation,
        old: row("1"),
        old_is_key: false,
    }
}

fn truncate(xid: u32, relations: Vec<u32>) -> SourceEvent {
    SourceEvent::Truncate {
        xid,
        subxid: xid,
        relations,
        cascade: false,
        restart_identity: false,
    }
}

fn begin(xid: u32, lsn: u64) -> SourceEvent {
    SourceEvent::Begin {
        xid,
        final_lsn: PgLsn(lsn),
        commit_timestamp_micros: 0,
    }
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
            CaptureAssembler::new(SourceId("source".into()), spool, [schema(CAPTURED)], 256)
                .unwrap();
        Self {
            _root: root,
            journal,
            assembler,
        }
    }

    fn push(&mut self, event: SourceEvent) -> Option<SourceTransaction> {
        self.assembler.push(event, &mut self.journal).unwrap()
    }

    fn mutations(&self, transaction: &SourceTransaction) -> Vec<Mutation> {
        self.journal
            .chunks(&transaction.mutation_chunks)
            .unwrap()
            .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
            .collect()
    }
}

fn inserted_ids(mutations: &[Mutation]) -> Vec<Value> {
    mutations
        .iter()
        .map(|mutation| {
            assert_eq!(mutation.table_id, TableId(CAPTURED));
            let MutationKind::Insert { row } = &mutation.kind else {
                panic!("unexpected captured mutation");
            };
            row[0].clone()
        })
        .collect()
}

fn assert_empty(transaction: &SourceTransaction) {
    assert!(transaction.affected_tables.is_empty());
    assert!(transaction.schema_versions.is_empty());
    assert_eq!(transaction.table_mutation_counts, Some(vec![]));
    assert_eq!(transaction.mutation_chunks.payload_bytes(), 0);
}

#[test]
fn other_published_tables_are_ignored_and_their_transactions_commit_empty() {
    let mut capture = Capture::new();
    for id in [CAPTURED, OTHER] {
        assert!(capture.push(SourceEvent::Relation(relation(id))).is_none());
    }

    // A transaction without row changes is the baseline for an ignored one.
    let journal_bytes = capture.journal.bytes_used();
    capture.push(begin(1, 100));
    let empty = capture.push(commit(1, 100)).unwrap();
    assert_empty(&empty);
    let empty_bytes = capture.journal.bytes_used() - journal_bytes;

    let journal_bytes = capture.journal.bytes_used();
    capture.push(begin(2, 200));
    for event in [
        insert(2, 2, OTHER, "1"),
        update(2, 2, OTHER),
        delete(2, 2, OTHER),
    ] {
        assert!(capture.push(event).is_none());
    }
    let ignored = capture.push(commit(2, 200)).unwrap();
    assert_empty(&ignored);
    assert_eq!(capture.journal.bytes_used() - journal_bytes, empty_bytes);
    assert_eq!(capture.journal.durable_lsn(), PgLsn(201));
    assert!(capture.mutations(&ignored).is_empty());

    capture.push(begin(3, 300));
    for event in [
        insert(3, 3, CAPTURED, "1"),
        insert(3, 3, OTHER, "2"),
        update(3, 3, OTHER),
        SourceEvent::Relation(relation(OTHER)),
        delete(3, 3, OTHER),
        insert(3, 3, CAPTURED, "2"),
    ] {
        capture.push(event);
    }
    let mixed = capture.push(commit(3, 300)).unwrap();
    assert_eq!(mixed.affected_tables, [TableId(CAPTURED)]);
    assert_eq!(mixed.mutation_count(TableId(CAPTURED)), Some(2));
    assert_eq!(
        inserted_ids(&capture.mutations(&mixed)),
        [Value::Int32(1), Value::Int32(2)]
    );
    assert_eq!(capture.journal.transactions().len(), 3);

    // Ignoring a change never relaxes the transaction protocol itself.
    assert!(matches!(
        capture
            .assembler
            .push(insert(4, 4, OTHER, "3"), &mut capture.journal),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn truncate_stops_capture_only_when_it_includes_a_captured_table() {
    assert_eq!(
        truncate(1, vec![OTHER, CAPTURED, OTHER + 1]).retain_relations(|id| id == CAPTURED),
        Some(truncate(1, vec![CAPTURED]))
    );
    assert_eq!(
        truncate(1, vec![OTHER]).retain_relations(|id| id == CAPTURED),
        None
    );

    let mut capture = Capture::new();
    capture.push(SourceEvent::Relation(relation(CAPTURED)));
    capture.push(SourceEvent::Relation(relation(OTHER)));
    capture.push(begin(1, 100));
    assert!(capture.push(truncate(1, vec![OTHER])).is_none());
    assert_empty(&capture.push(commit(1, 100)).unwrap());

    capture.push(begin(2, 200));
    assert!(matches!(
        capture
            .assembler
            .push(truncate(2, vec![OTHER, CAPTURED]), &mut capture.journal),
        Err(Error::Config(message)) if message.contains("TRUNCATE")
    ));
}

#[test]
fn streamed_transactions_skip_interleaved_changes_to_other_tables() {
    let mut capture = Capture::new();
    capture.push(SourceEvent::Relation(relation(CAPTURED)));
    capture.push(SourceEvent::Relation(relation(OTHER)));
    let segment = |xid, first| SourceEvent::StreamStart { xid, first };
    for event in [
        segment(42, true),
        insert(42, 42, CAPTURED, "1"),
        insert(42, 42, OTHER, "1"),
        SourceEvent::StreamStop,
        // A streamed transaction with only ignored changes interleaves here.
        segment(50, true),
        SourceEvent::Relation(relation(OTHER)),
        insert(50, 50, OTHER, "2"),
        SourceEvent::StreamStop,
        segment(42, false),
        // Subtransaction 43 contains only ignored changes; 45 contains both.
        update(42, 43, OTHER),
        insert(42, 44, CAPTURED, "2"),
        delete(42, 44, OTHER),
        insert(42, 45, CAPTURED, "3"),
        insert(42, 45, OTHER, "3"),
        SourceEvent::StreamStop,
        SourceEvent::Abort {
            xid: 42,
            subxid: 43,
        },
        SourceEvent::Abort {
            xid: 42,
            subxid: 45,
        },
        segment(42, false),
        SourceEvent::Relation(relation(OTHER)),
        delete(42, 42, OTHER),
        SourceEvent::StreamStop,
    ] {
        assert!(capture.push(event).is_none());
    }
    assert_empty(&capture.push(commit(50, 100)).unwrap());
    let streamed = capture.push(commit(42, 200)).unwrap();
    assert_eq!(streamed.affected_tables, [TableId(CAPTURED)]);
    assert_eq!(
        inserted_ids(&capture.mutations(&streamed)),
        [Value::Int32(1), Value::Int32(2)]
    );
    assert_eq!(capture.journal.durable_lsn(), PgLsn(201));
}

/// Real pgoutput against a publication that also contains another table: its
/// relation metadata, rows, streamed segments and TRUNCATE are all ignored.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_superset_publication_captures_only_configured_tables() {
    use flow_pg_source::{
        PgOutputSource, PostgresSource,
        tokio_postgres::{self, NoTls, config::ReplicationMode},
    };
    use futures::FutureExt;
    use std::{
        panic::AssertUnwindSafe,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let name = format!(
        "flow_superset_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sql.batch_execute(&format!(
        "CREATE SCHEMA {name};
         CREATE TABLE {name}.captured (id integer PRIMARY KEY, body text NOT NULL);
         ALTER TABLE {name}.captured REPLICA IDENTITY FULL;
         CREATE TABLE {name}.other (id integer PRIMARY KEY, payload text NOT NULL);
         CREATE PUBLICATION {name} FOR TABLE {name}.captured, {name}.other;"
    ))
    .await
    .unwrap();
    let captured: u32 = sql
        .query_one(
            "SELECT to_regclass($1)::oid",
            &[&format!("{name}.captured")],
        )
        .await
        .unwrap()
        .get(0);
    let start: PgLsn = sql
        .query_one(
            "SELECT lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&name],
        )
        .await
        .unwrap()
        .get::<_, String>(0)
        .parse()
        .unwrap();

    let body = AssertUnwindSafe(async {
        // One simple query per source transaction. In a multi-statement query,
        // BEGIN converts the implicit block holding the preceding statements
        // into its transaction, and the statements after COMMIT share another.
        for transaction in [
            format!("INSERT INTO {name}.other VALUES (1, 'other only')"),
            format!(
                "BEGIN;
                 INSERT INTO {name}.captured VALUES (1, 'kept');
                 INSERT INTO {name}.other VALUES (2, 'ignored');
                 UPDATE {name}.other SET payload = 'ignored update' WHERE id = 1;
                 DELETE FROM {name}.other WHERE id = 2;
                 COMMIT"
            ),
            format!(
                "DO $$ BEGIN
                   FOR i IN 1..600 LOOP
                     INSERT INTO {name}.captured VALUES (1000 + i, repeat('c', 200));
                     INSERT INTO {name}.other VALUES (1000 + i, repeat('o', 200));
                   END LOOP;
                 END $$"
            ),
            format!("TRUNCATE {name}.other"),
            format!("INSERT INTO {name}.captured VALUES (-1, 'end')"),
            format!("TRUNCATE {name}.captured, {name}.other"),
        ] {
            sql.batch_execute(&transaction).await.unwrap();
        }

        // A small decoding budget makes the walsender stream the large
        // interleaved transaction before it commits.
        let mut config: tokio_postgres::Config = url.parse().unwrap();
        config
            .replication_mode(ReplicationMode::Logical)
            .options("-c logical_decoding_work_mem=64kB");
        let (replication, connection) = config.connect(NoTls).await.unwrap();
        let replication_task = tokio::spawn(connection);
        let mut source = PgOutputSource::start(&replication, &name, &name, start, 1 << 20)
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let (mut journal, _) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        let spool =
            TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
        let mut assembler = CaptureAssembler::new(
            SourceId("source".into()),
            spool,
            [schema(captured)],
            1 << 16,
        )
        .unwrap();

        let mut committed = Vec::new();
        let mut streamed = false;
        let mut other_relation = false;
        let truncated = loop {
            let event = tokio::time::timeout(Duration::from_secs(30), source.next())
                .await
                .expect("pgoutput event")
                .unwrap()
                .expect("replication stream ended");
            streamed |= matches!(event, SourceEvent::StreamStart { .. });
            other_relation |=
                matches!(&event, SourceEvent::Relation(relation) if relation.id != captured);
            match assembler.push(event, &mut journal) {
                Ok(Some(transaction)) => committed.push(transaction),
                Ok(None) => {}
                Err(error) => break error,
            }
        };
        assert!(
            matches!(&truncated, Error::Config(message) if message.contains("TRUNCATE")),
            "{truncated}"
        );
        assert!(streamed, "the interleaved transaction was not streamed");
        assert!(other_relation, "pgoutput did not describe the other table");
        let counts = committed
            .iter()
            .map(|transaction| transaction.mutation_count(TableId(captured)))
            .collect::<Vec<_>>();
        // Other-only insert, mixed, streamed, TRUNCATE of the other table, marker.
        assert_eq!(counts, [None, Some(1), Some(600), None, Some(1)]);
        for transaction in [&committed[0], &committed[3]] {
            assert_empty(transaction);
        }
        assert_eq!(journal.durable_lsn(), committed[4].end_lsn);
        drop(source);
        drop(replication);
        replication_task.abort();
    });
    let result = body.catch_unwind().await;

    // Always release the slot, including after a failed assertion.
    for _ in 0..50 {
        sql.execute(
            "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
            &[&name],
        )
        .await
        .unwrap();
        let dropped = sql
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                &[&name],
            )
            .await;
        let remaining: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        if dropped.is_ok() && remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sql.batch_execute(&format!(
        "DROP PUBLICATION {name}; DROP SCHEMA {name} CASCADE"
    ))
    .await
    .unwrap();
    drop(sql);
    sql_task.await.unwrap().unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

// The compatibility suite runs this binary's live tests against every
// supported PostgreSQL version, so the streamed-rollback probe below lives
// here although it concerns rollback rather than publication membership.

/// What Flow observed of one savepoint rollback in a streamed transaction.
#[derive(Debug, Default)]
struct RollbackProbe {
    streamed: bool,
    /// The rolled-back savepoint's XID, as carried by its streamed deletes.
    savepoint: Option<u32>,
    savepoint_deletes_received: u64,
    aborted_subtransactions: Vec<u32>,
    /// Deletes in the journaled transaction; PostgreSQL committed none.
    journaled_deletes: u64,
}

impl RollbackProbe {
    fn savepoint_aborted(&self) -> bool {
        self.savepoint
            .is_some_and(|savepoint| self.aborted_subtransactions.contains(&savepoint))
    }
}

/// Delete `deleted` rows in savepoint `s1` of a transaction that stays open,
/// then make every later change in a child savepoint `s2`. With `spill`, the
/// slot is first advanced past the deletes, so PostgreSQL re-decodes them
/// before its confirmed position, where it may not stream and spills the
/// transaction instead; `s2`'s changes then stream it from the spill. Only
/// after Flow's pgoutput source has received all of `s1`'s deletes in a
/// streamed block does the transaction roll back to `s1` and commit, as when
/// a savepoint is rolled back during live replication.
async fn rollback_probe(deleted: i32, spill: bool) -> RollbackProbe {
    use flow_pg_source::{
        PgOutputSource, PostgresSource,
        tokio_postgres::{self, NoTls, config::ReplicationMode},
    };
    use futures::FutureExt;
    use std::{
        panic::AssertUnwindSafe,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let name = format!(
        "flow_rollback_{deleted}_{}_{}",
        u8::from(spill),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sql.batch_execute(&format!(
        "CREATE SCHEMA {name};
         CREATE TABLE {name}.probe (id integer PRIMARY KEY, body text NOT NULL);
         ALTER TABLE {name}.probe REPLICA IDENTITY FULL;
         INSERT INTO {name}.probe SELECT i, 'row ' || i FROM generate_series(1, {deleted}) i;
         CREATE PUBLICATION {name} FOR TABLE {name}.probe;"
    ))
    .await
    .unwrap();
    let probe: u32 = sql
        .query_one("SELECT to_regclass($1)::oid", &[&format!("{name}.probe")])
        .await
        .unwrap()
        .get(0);
    let start: PgLsn = sql
        .query_one(
            "SELECT lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&name],
        )
        .await
        .unwrap()
        .get::<_, String>(0)
        .parse()
        .unwrap();
    // A committed message flushes WAL, including the open transaction's
    // records so far, so the walsender can read them.
    let flush = "SELECT pg_catalog.pg_logical_emit_message(true, 'flow-rollback-probe', 'flush')";

    let body = AssertUnwindSafe(async {
        let (holder, holder_connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        let holder_task = tokio::spawn(holder_connection);
        holder
            .batch_execute(&format!(
                "BEGIN;
                 INSERT INTO {name}.probe VALUES (-1, 'before the savepoint');
                 SAVEPOINT s1;
                 DELETE FROM {name}.probe WHERE id BETWEEN 1 AND {deleted}"
            ))
            .await
            .unwrap();
        let xid = holder
            .query_one("SELECT txid_current()::text", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
            .parse::<u64>()
            .unwrap() as u32;
        if spill {
            sql.batch_execute(flush).await.unwrap();
            sql.execute(
                "SELECT pg_catalog.pg_replication_slot_advance($1, pg_catalog.pg_current_wal_lsn())",
                &[&name],
            )
            .await
            .unwrap();
        }
        holder
            .batch_execute(&format!(
                "SAVEPOINT s2;
                 INSERT INTO {name}.probe
                   SELECT 100000 + i, repeat('x', 200) FROM generate_series(1, 2000) i"
            ))
            .await
            .unwrap();
        sql.batch_execute(flush).await.unwrap();

        let mut config: tokio_postgres::Config = url.parse().unwrap();
        config
            .replication_mode(ReplicationMode::Logical)
            .options("-c logical_decoding_work_mem=64kB");
        let (replication, connection) = config.connect(NoTls).await.unwrap();
        let replication_task = tokio::spawn(connection);
        let mut source = PgOutputSource::start(&replication, &name, &name, start, 1 << 20)
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let (mut journal, _) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        let spool =
            TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
        let mut assembler =
            CaptureAssembler::new(SourceId("source".into()), spool, [schema(probe)], 1 << 16)
                .unwrap();
        let mut outcome = RollbackProbe::default();
        let mut rolled_back = false;
        let transaction = loop {
            let event = tokio::time::timeout(Duration::from_secs(60), source.next())
                .await
                .unwrap_or_else(|_| panic!("no pgoutput event within 60s: {outcome:?}"))
                .unwrap()
                .expect("replication stream ended");
            match &event {
                SourceEvent::StreamStart { xid: top, .. } if *top == xid => {
                    outcome.streamed = true;
                }
                SourceEvent::Delete {
                    xid: top, subxid, ..
                } if *top == xid && *subxid != xid => {
                    outcome.savepoint = Some(*subxid);
                    outcome.savepoint_deletes_received += 1;
                }
                SourceEvent::Abort { xid: top, subxid } if *top == xid && *subxid != xid => {
                    outcome.aborted_subtransactions.push(*subxid);
                }
                _ => {}
            }
            // Barrier: roll back only once every delete of the still-open
            // savepoint has been streamed, and only at a block boundary.
            let block_ended = matches!(event, SourceEvent::StreamStop);
            if let Some(transaction) = assembler.push(event, &mut journal).unwrap()
                && transaction.xid == xid
            {
                break transaction;
            }
            if !rolled_back
                && block_ended
                && outcome.savepoint_deletes_received >= u64::from(deleted.unsigned_abs())
            {
                rolled_back = true;
                holder
                    .batch_execute(&format!(
                        "ROLLBACK TO SAVEPOINT s1;
                         INSERT INTO {name}.probe VALUES (-2, 'after the rollback');
                         COMMIT"
                    ))
                    .await
                    .unwrap();
            }
        };
        assert!(rolled_back, "committed before the barrier: {outcome:?}");
        drop(holder);
        holder_task.await.unwrap().unwrap();
        outcome.journaled_deletes = journal
            .chunks(&transaction.mutation_chunks)
            .unwrap()
            .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
            .filter(|mutation| matches!(mutation.kind, MutationKind::Delete { .. }))
            .count() as u64;
        drop(source);
        drop(replication);
        replication_task.abort();
        outcome
    });
    let result = body.catch_unwind().await;

    // Always release the slot, including after a failed assertion.
    for _ in 0..50 {
        sql.execute(
            "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
            &[&name],
        )
        .await
        .unwrap();
        let dropped = sql
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                &[&name],
            )
            .await;
        let remaining: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        if dropped.is_ok() && remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sql.batch_execute(&format!(
        "DROP PUBLICATION {name}; DROP SCHEMA {name} CASCADE"
    ))
    .await
    .unwrap();
    drop(sql);
    sql_task.await.unwrap().unwrap();
    match result {
        Ok(outcome) => outcome,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// Control: streamed from memory, the rolled-back savepoint is aborted.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_rollback_of_a_savepoint_streamed_from_memory_is_aborted() {
    let outcome = rollback_probe(5039, false).await;
    assert!(outcome.streamed, "{outcome:?}");
    assert_eq!(outcome.savepoint_deletes_received, 5039, "{outcome:?}");
    assert!(outcome.savepoint_aborted(), "{outcome:?}");
    assert_eq!(outcome.journaled_deletes, 0, "{outcome:?}");
}

/// Control: spilled with fewer changes than PostgreSQL restores at once, the
/// rolled-back savepoint is aborted.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_rollback_of_a_spilled_savepoint_within_one_restore_batch_is_aborted() {
    let outcome = rollback_probe(1000, true).await;
    assert!(outcome.streamed, "{outcome:?}");
    assert_eq!(outcome.savepoint_deletes_received, 1000, "{outcome:?}");
    assert!(outcome.savepoint_aborted(), "{outcome:?}");
    assert_eq!(outcome.journaled_deletes, 0, "{outcome:?}");
}

/// Diagnostic evidence for the cycle-61 hypothesis, NOT desired behaviour.
/// Reading PostgreSQL's source, it restores a spilled transaction's changes
/// in batches of 4096 and marks a subtransaction as streamed only if changes
/// remain in memory afterwards, so a spilled savepoint with more changes
/// would be streamed but its rollback would send no abort. This asserts that
/// prediction, including the resulting wrong journal output, so CI shows
/// whether the mechanism actually occurs on each PostgreSQL version. It does
/// not accept that output: replace it with a correctness assertion once
/// capture handles the rollback.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_rollback_of_a_spilled_savepoint_over_one_restore_batch_sends_no_abort() {
    let outcome = rollback_probe(5039, true).await;
    assert!(outcome.streamed, "{outcome:?}");
    assert_eq!(outcome.savepoint_deletes_received, 5039, "{outcome:?}");
    assert!(!outcome.savepoint_aborted(), "{outcome:?}");
    assert_eq!(outcome.journaled_deletes, 5039, "{outcome:?}");
}
