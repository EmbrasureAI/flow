use bytes::Bytes;
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, SourceId, TableId, TableSchema, Value,
};
use flow_pg_source::{
    CaptureAssembler, Cell, Column as PgColumn, Error, Relation, SourceEvent, SpoolConfig,
    TransactionSpool,
};

fn fixture() -> (TableSchema, Relation) {
    let schema = TableSchema {
        table_id: TableId(11),
        version: 1,
        append_only: false,
        columns: ["tenant", "body", "id"]
            .into_iter()
            .enumerate()
            .map(|(i, name)| Column {
                field_id: i as i32 + 1,
                name: name.into(),
                data_type: ColumnType::Int32,
                nullable: i == 1,
            })
            .collect(),
        // Deliberately different from source attribute order.
        primary_key: vec![2, 0],
    };
    let relation = Relation {
        id: 11,
        namespace: "public".into(),
        name: "items".into(),
        replica_identity: b'd',
        columns: schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| PgColumn {
                name: c.name.clone(),
                type_oid: 23,
                type_modifier: -1,
                identity: i != 1,
            })
            .collect(),
    };
    (schema, relation)
}
fn tuple(values: [Option<i32>; 3]) -> Vec<Cell> {
    values
        .into_iter()
        .map(|v| v.map_or(Cell::Null, |v| Cell::Text(Bytes::from(v.to_string()))))
        .collect()
}

#[test]
fn default_composite_key_updates_deletes_and_replay_preserve_canonical_keys() {
    let root = tempfile::tempdir().unwrap();
    let (schema, relation) = fixture();
    let (mut journal, _) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    let spool = TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
    let mut capture =
        CaptureAssembler::new(SourceId("s".into()), spool, [schema.clone()], 256).unwrap();
    capture
        .push(SourceEvent::Relation(relation), &mut journal)
        .unwrap();
    capture
        .push(
            SourceEvent::Begin {
                xid: 1,
                final_lsn: PgLsn(100),
                commit_timestamp_micros: 0,
            },
            &mut journal,
        )
        .unwrap();
    let initial = tuple([Some(7), Some(10), Some(1)]);
    capture
        .push(
            SourceEvent::Insert {
                xid: 1,
                subxid: 1,
                relation: 11,
                row: initial,
            },
            &mut journal,
        )
        .unwrap();
    capture
        .push(
            SourceEvent::Update {
                xid: 1,
                subxid: 1,
                relation: 11,
                old: None,
                old_is_key: false,
                row: tuple([Some(7), None, Some(1)]),
            },
            &mut journal,
        )
        .unwrap();
    capture
        .push(
            SourceEvent::Update {
                xid: 1,
                subxid: 1,
                relation: 11,
                old: Some(tuple([Some(7), None, Some(1)])),
                old_is_key: true,
                row: tuple([Some(8), Some(20), Some(2)]),
            },
            &mut journal,
        )
        .unwrap();
    capture
        .push(
            SourceEvent::Delete {
                xid: 1,
                subxid: 1,
                relation: 11,
                old: tuple([Some(8), None, Some(2)]),
                old_is_key: true,
            },
            &mut journal,
        )
        .unwrap();
    let txn = capture
        .push(
            SourceEvent::Commit {
                xid: 1,
                commit_lsn: PgLsn(100),
                end_lsn: PgLsn(108),
                commit_timestamp_micros: 0,
            },
            &mut journal,
        )
        .unwrap()
        .unwrap();
    drop(journal);
    let (mut journal, _) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    let mutations: Vec<Mutation> = journal
        .chunks(&txn.mutation_chunks)
        .unwrap()
        .flat_map(|b| bincode::deserialize::<Vec<Mutation>>(&b.unwrap()).unwrap())
        .collect();
    let key = |tenant, id| {
        schema
            .encode_key(&vec![Value::Int32(tenant), Value::Null, Value::Int32(id)])
            .unwrap()
    };
    assert_eq!(mutations.len(), 4);
    assert_eq!(
        mutations[1].kind,
        MutationKind::Update {
            old_key: key(7, 1),
            row: vec![Value::Int32(7), Value::Null, Value::Int32(1)]
        }
    );
    assert_eq!(
        mutations[2].kind,
        MutationKind::Update {
            old_key: key(7, 1),
            row: vec![Value::Int32(8), Value::Int32(20), Value::Int32(2)]
        }
    );
    assert_eq!(mutations[3].kind, MutationKind::Delete { key: key(8, 2) });
    assert_eq!(journal.durable_lsn(), PgLsn(108));
    // Duplicate WAL delivery after reopen must not add a second terminal or
    // reintroduce a row removed by the already durable transaction.
    capture
        .push(
            SourceEvent::Begin {
                xid: 1,
                final_lsn: PgLsn(100),
                commit_timestamp_micros: 0,
            },
            &mut journal,
        )
        .unwrap();
    capture
        .push(
            SourceEvent::Insert {
                xid: 1,
                subxid: 1,
                relation: 11,
                row: tuple([Some(7), Some(10), Some(1)]),
            },
            &mut journal,
        )
        .unwrap();
    assert!(
        capture
            .push(
                SourceEvent::Commit {
                    xid: 1,
                    commit_lsn: PgLsn(100),
                    end_lsn: PgLsn(108),
                    commit_timestamp_micros: 0
                },
                &mut journal
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(journal.transactions().len(), 1);
}

#[test]
fn default_never_repairs_unchanged_toast_from_key_null_placeholders() {
    for old_is_key in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let (schema, relation) = fixture();
        let (mut journal, _) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        let spool =
            TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
        let mut capture =
            CaptureAssembler::new(SourceId("s".into()), spool, [schema], 256).unwrap();
        capture
            .push(SourceEvent::Relation(relation), &mut journal)
            .unwrap();
        capture
            .push(
                SourceEvent::Begin {
                    xid: 1,
                    final_lsn: PgLsn(100),
                    commit_timestamp_micros: 0,
                },
                &mut journal,
            )
            .unwrap();
        let mut row = tuple([Some(7), None, Some(1)]);
        row[1] = Cell::UnchangedToast;
        assert!(matches!(
            capture.push(
                SourceEvent::Update {
                    xid: 1,
                    subxid: 1,
                    relation: 11,
                    old: Some(tuple([Some(7), None, Some(1)])),
                    old_is_key,
                    row
                },
                &mut journal
            ),
            Err(Error::UnchangedToast(11))
        ));
        assert_eq!(journal.durable_lsn(), PgLsn(0));
        drop(capture);
        drop(journal);
        let (journal, recovered) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        assert_eq!(journal.durable_lsn(), PgLsn(0));
        assert_eq!(recovered.transactions.iter().unwrap().count(), 0);
    }
}

#[test]
fn default_eligibility_rejects_variable_width_and_other_identities() {
    let (mut schema, mut relation) = fixture();
    for identity in *b"ni" {
        relation.replica_identity = identity;
        assert!(relation.validate_schema(&schema).is_err());
    }
    relation.replica_identity = b'd';
    for (oid, kind) in [
        (25, ColumnType::String),
        (1043, ColumnType::String),
        (1700, ColumnType::String),
        (17, ColumnType::Binary),
    ] {
        relation.columns[1].type_oid = oid;
        schema.columns[1].data_type = kind;
        let error = relation.validate_schema(&schema).unwrap_err().to_string();
        assert!(
            error.contains("public.items column body") && error.contains("REPLICA IDENTITY FULL"),
            "{error}"
        );
    }
    relation.replica_identity = b'f';
    relation.validate_schema(&schema).unwrap();
    schema.append_only = true;
    relation.replica_identity = b'n';
    relation.validate_schema(&schema).unwrap();
}

#[test]
fn key_only_delete_ignores_non_key_not_null_but_rejects_missing_key_values() {
    for key_cell in [
        Cell::Text(Bytes::from_static(b"7")),
        Cell::Null,
        Cell::UnchangedToast,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut schema, relation) = fixture();
        schema.columns[1].nullable = false;
        let (mut journal, _) =
            Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
        let spool =
            TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
        let mut capture =
            CaptureAssembler::new(SourceId("s".into()), spool, [schema], 256).unwrap();
        capture
            .push(SourceEvent::Relation(relation), &mut journal)
            .unwrap();
        capture
            .push(
                SourceEvent::Begin {
                    xid: 1,
                    final_lsn: PgLsn(100),
                    commit_timestamp_micros: 0,
                },
                &mut journal,
            )
            .unwrap();
        let valid = matches!(key_cell, Cell::Text(_));
        let result = capture.push(
            SourceEvent::Delete {
                xid: 1,
                subxid: 1,
                relation: 11,
                old: vec![key_cell, Cell::Null, Cell::Text(Bytes::from_static(b"1"))],
                old_is_key: true,
            },
            &mut journal,
        );
        assert_eq!(result.is_ok(), valid);
        assert_eq!(journal.durable_lsn(), PgLsn(0));
    }
}
