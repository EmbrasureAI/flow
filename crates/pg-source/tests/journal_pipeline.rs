use bytes::Bytes;
use flow_ingress_journal::{ChunkReader, Journal, JournalConfig};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, SourceId, TableId, TableSchema, Value,
};
use flow_pg_source::{
    CaptureAssembler, Cell, Column as PgColumn, Relation, SourceEvent, SpoolConfig,
    TransactionSpool, decode_row,
};

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
fn relation(id: u32) -> Relation {
    Relation {
        id,
        namespace: "public".into(),
        name: "items".into(),
        replica_identity: b'f',
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
                identity: true,
            },
            PgColumn {
                name: "body".into(),
                type_oid: 25,
                type_modifier: -1,
                identity: true,
            },
        ],
    }
}
fn row(id: &'static str, body: &'static str) -> Vec<Cell> {
    vec![
        Cell::Text(Bytes::from_static(id.as_bytes())),
        Cell::Text(Bytes::from_static(body.as_bytes())),
    ]
}

#[test]
fn capture_spill_abort_durable_journal_restart_and_replay_are_one_pipeline() {
    let root = tempfile::tempdir().unwrap();
    let (mut journal, _) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    let spool = TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
    let mut assembler = CaptureAssembler::new(
        SourceId("source".into()),
        spool,
        [schema(11), schema(12)],
        256,
    )
    .unwrap();
    for id in [11, 12] {
        assembler
            .push(SourceEvent::Relation(relation(id)), &mut journal)
            .unwrap();
    }
    assembler
        .push(
            SourceEvent::StreamStart {
                xid: 42,
                first: true,
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Insert {
                xid: 42,
                subxid: 42,
                relation: 11,
                row: row("1", "keep"),
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Insert {
                xid: 42,
                subxid: 43,
                relation: 12,
                row: row("9", "rollback parent"),
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Insert {
                xid: 42,
                subxid: 44,
                relation: 11,
                row: row("8", "rollback child"),
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(SourceEvent::StreamStop, &mut journal)
        .unwrap();
    assembler
        .push(
            SourceEvent::Abort {
                xid: 42,
                subxid: 43,
            },
            &mut journal,
        )
        .unwrap();
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    assembler
        .push(
            SourceEvent::StreamStart {
                xid: 42,
                first: false,
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Update {
                xid: 42,
                subxid: 42,
                relation: 11,
                old: Some(row("1", "keep")),
                old_is_key: false,
                row: vec![Cell::Text(Bytes::from_static(b"2")), Cell::UnchangedToast],
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Insert {
                xid: 42,
                subxid: 42,
                relation: 12,
                row: row("3", "second table"),
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(SourceEvent::StreamStop, &mut journal)
        .unwrap();
    let commit = SourceEvent::Commit {
        xid: 42,
        commit_lsn: PgLsn(100),
        end_lsn: PgLsn(108),
        commit_timestamp_micros: 1,
    };
    let txn = assembler
        .push(commit.clone(), &mut journal)
        .unwrap()
        .unwrap();
    assert_eq!(txn.affected_tables, [TableId(11), TableId(12)]);
    // The aborted parent and descendant are absent; a key-changing UPDATE is
    // one source event, independently of its two physical index changes.
    assert_eq!(txn.mutation_count(TableId(11)), Some(2));
    assert_eq!(txn.mutation_count(TableId(12)), Some(1));
    drop(journal);
    let (mut journal, recovered) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        std::slice::from_ref(&txn)
    );
    let mutations: Vec<Mutation> = journal
        .chunks(&txn.mutation_chunks)
        .unwrap()
        .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
        .collect();
    assert_eq!(mutations.len(), 3);
    let MutationKind::Update { old_key, row } = &mutations[1].kind else {
        panic!("PK change lost");
    };
    assert_eq!(
        *old_key,
        schema(11)
            .encode_key(&vec![Value::Int32(1), Value::String("keep".into())])
            .unwrap()
    );
    assert_eq!(row[0], Value::Int32(2));
    assert_eq!(row[1], Value::String("keep".into()));
    assembler
        .push(
            SourceEvent::Begin {
                xid: 42,
                final_lsn: PgLsn(100),
                commit_timestamp_micros: 1,
            },
            &mut journal,
        )
        .unwrap();
    assembler
        .push(
            SourceEvent::Insert {
                xid: 42,
                subxid: 42,
                relation: 11,
                row: self::row("1", "duplicate"),
            },
            &mut journal,
        )
        .unwrap();
    assert!(assembler.push(commit, &mut journal).unwrap().is_none());
    assert_eq!(journal.transactions().len(), 1);
}

#[test]
fn exact_source_types_agree_between_copy_binary_and_cdc_text() {
    let types = [
        (
            "n",
            ColumnType::Decimal {
                precision: 20,
                scale: 4,
            },
            1700,
        ),
        ("d", ColumnType::Date, 1082),
        ("t", ColumnType::TimestampTzMicros, 1184),
        ("id", ColumnType::Uuid, 2950),
        ("b", ColumnType::Binary, 17),
        ("f", ColumnType::Float64, 700),
        ("ts", ColumnType::TimestampMicros, 1114),
    ];
    let schema = TableSchema {
        table_id: TableId(1),
        version: 1,
        primary_key: vec![3],
        append_only: false,
        columns: types
            .iter()
            .enumerate()
            .map(|(i, (name, data_type, _))| Column {
                field_id: i as i32 + 1,
                name: (*name).into(),
                data_type: data_type.clone(),
                nullable: false,
            })
            .collect(),
    };
    let relation = Relation {
        id: 1,
        namespace: "public".into(),
        name: "types".into(),
        replica_identity: b'f',
        columns: types
            .iter()
            .map(|(name, data_type, oid)| PgColumn {
                name: (*name).into(),
                type_oid: *oid,
                type_modifier: if matches!(data_type, ColumnType::Decimal { .. }) {
                    4 + (20 << 16) + 4
                } else {
                    -1
                },
                identity: true,
            })
            .collect(),
    };
    let text: Vec<_> = [
        "-12345.6700",
        "2000-01-01",
        "2000-01-01 00:00:00.000001+00",
        "00000000-0000-0000-0000-000000000001",
        "\\x00aaff",
        "0.1",
        "2000-01-01 00:00:00.000001",
    ]
    .into_iter()
    .map(|s| Cell::Text(Bytes::from_static(s.as_bytes())))
    .collect();
    // NUMERIC: three base-10000 digits [1,2345,6700], weight 1, negative.
    let binary = vec![
        Cell::Binary(Bytes::from_static(&[
            0, 3, 0, 1, 0x40, 0, 0, 4, 0, 1, 9, 41, 26, 44,
        ])),
        Cell::Binary(Bytes::copy_from_slice(&0i32.to_be_bytes())),
        Cell::Binary(Bytes::copy_from_slice(&1i64.to_be_bytes())),
        Cell::Binary(Bytes::copy_from_slice(&1u128.to_be_bytes())),
        Cell::Binary(Bytes::from_static(&[0, 170, 255])),
        Cell::Binary(Bytes::copy_from_slice(&0.1f32.to_be_bytes())),
        Cell::Binary(Bytes::copy_from_slice(&1i64.to_be_bytes())),
    ];
    let expected = decode_row(&schema, &relation, &text).unwrap();
    assert_eq!(
        expected[0],
        Value::Decimal {
            unscaled: -123456700,
            scale: 4
        }
    );
    assert_eq!(expected[2], Value::TimestampTzMicros(946_684_800_000_001));
    assert_eq!(expected[5], Value::Float64(f64::from(0.1f32)));
    assert_eq!(decode_row(&schema, &relation, &binary).unwrap(), expected);
    for (date, year, month, day, bc) in [
        ("10000-01-01", 10000, 1, 1, false),
        ("0001-01-01", 0, 1, 1, true),
        ("2024-02-29", 2024, 2, 29, false),
    ] {
        let era = if bc { " BC" } else { "" };
        let civil = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
        let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let pg_days = i32::try_from(civil.signed_duration_since(epoch).num_days()).unwrap();
        let pg_micros = i64::from(pg_days) * 86_400_000_000 + 1;
        let mut wide_text = text.clone();
        wide_text[1] = Cell::Text(Bytes::from(format!("{date}{era}")));
        wide_text[2] = Cell::Text(Bytes::from(format!("{date} 00:00:00.000001+00{era}")));
        wide_text[6] = Cell::Text(Bytes::from(format!("{date} 00:00:00.000001{era}")));
        let mut wide_binary = binary.clone();
        wide_binary[1] = Cell::Binary(Bytes::copy_from_slice(&pg_days.to_be_bytes()));
        wide_binary[2] = Cell::Binary(Bytes::copy_from_slice(&pg_micros.to_be_bytes()));
        wide_binary[6] = wide_binary[2].clone();
        assert_eq!(
            decode_row(&schema, &relation, &wide_text).unwrap(),
            decode_row(&schema, &relation, &wide_binary).unwrap(),
            "{date}{era}"
        );
    }
    // The finite PostgreSQL date range extends well beyond chrono's calendar.
    for (date, pg_days) in [
        ("4714-11-24 BC", -2_451_545i32),
        ("5874897-12-31", 2_145_031_948),
    ] {
        let mut wide_text = text.clone();
        wide_text[1] = Cell::Text(Bytes::from(date));
        let mut wide_binary = binary.clone();
        wide_binary[1] = Cell::Binary(Bytes::copy_from_slice(&pg_days.to_be_bytes()));
        assert_eq!(
            decode_row(&schema, &relation, &wide_text).unwrap(),
            decode_row(&schema, &relation, &wide_binary).unwrap()
        );
    }
    // A finite timestamp beyond chrono's maximum year must also survive CDC.
    let mut far_text = text.clone();
    far_text[2] = Cell::Text(Bytes::from_static(b"280000-01-01 00:00:00+00"));
    far_text[6] = Cell::Text(Bytes::from_static(b"280000-01-01 00:00:00"));
    let mut far_binary = binary.clone();
    let pg_micros = 101_537_415i64 * 86_400_000_000;
    far_binary[2] = Cell::Binary(Bytes::copy_from_slice(&pg_micros.to_be_bytes()));
    far_binary[6] = far_binary[2].clone();
    assert_eq!(
        decode_row(&schema, &relation, &far_text).unwrap(),
        decode_row(&schema, &relation, &far_binary).unwrap()
    );
    // Infinity and finite timestamps whose Unix conversion overflows must be
    // rejected in both representations, even though PostgreSQL can store them.
    for (date, pg_micros) in [
        ("infinity", i64::MAX),
        ("294276-12-31 23:59:59.999999", 9_223_371_331_199_999_999),
    ] {
        let mut bad_text = text.clone();
        bad_text[6] = Cell::Text(Bytes::from(date));
        let mut bad_binary = binary.clone();
        bad_binary[6] = Cell::Binary(Bytes::copy_from_slice(&pg_micros.to_be_bytes()));
        assert!(decode_row(&schema, &relation, &bad_text).is_err());
        assert!(decode_row(&schema, &relation, &bad_binary).is_err());
    }
    let mut incompatible_numeric = relation.clone();
    incompatible_numeric.columns[0].type_modifier = 4 + (20 << 16) + 5;
    assert!(decode_row(&schema, &incompatible_numeric, &text).is_err());
    incompatible_numeric.columns[0].type_modifier = -1;
    assert!(decode_row(&schema, &incompatible_numeric, &text).is_err());
    let mut wrong_scale = text;
    wrong_scale[0] = Cell::Text(Bytes::from_static(b"1.00001"));
    assert!(decode_row(&schema, &relation, &wrong_scale).is_err());
}

#[test]
fn unresolved_toast_stops_capture_before_any_terminal_record_is_written() {
    let root = tempfile::tempdir().unwrap();
    let (mut journal, _) =
        Journal::open(root.path().join("journal"), JournalConfig::default()).unwrap();
    let spool = TransactionSpool::open(root.path().join("spool"), SpoolConfig::default()).unwrap();
    let mut assembler =
        CaptureAssembler::new(SourceId("source".into()), spool, [schema(11)], 256).unwrap();
    assembler
        .push(SourceEvent::Relation(relation(11)), &mut journal)
        .unwrap();
    assembler
        .push(
            SourceEvent::Begin {
                xid: 1,
                final_lsn: PgLsn(100),
                commit_timestamp_micros: 0,
            },
            &mut journal,
        )
        .unwrap();
    for (old, old_is_key) in [
        (None, false),
        (Some(vec![Cell::Text(Bytes::from_static(b"1"))]), false),
        (
            Some(vec![
                Cell::Text(Bytes::from_static(b"1")),
                Cell::UnchangedToast,
            ]),
            false,
        ),
        (Some(row("1", "not a full old image")), true),
    ] {
        assert!(
            assembler
                .push(
                    SourceEvent::Update {
                        xid: 1,
                        subxid: 1,
                        relation: 11,
                        old,
                        old_is_key,
                        row: vec![Cell::Text(Bytes::from_static(b"1")), Cell::UnchangedToast]
                    },
                    &mut journal
                )
                .is_err()
        );
    }
    assert!(journal.transactions().is_empty());
    assert_eq!(journal.durable_lsn(), PgLsn(0));
}

#[test]
fn thousands_of_stream_segments_replay_without_an_aggregate_commit_frame() {
    let root = tempfile::tempdir().unwrap();
    let config = JournalConfig {
        max_frame_bytes: 512,
        segment_bytes: 64 << 10,
        ..JournalConfig::default()
    };
    let (mut journal, _) = Journal::open(root.path().join("journal"), config.clone()).unwrap();
    let spool = TransactionSpool::open(
        root.path().join("spool"),
        SpoolConfig {
            segment_bytes: 64 << 10,
            max_chunk_bytes: 512,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    let mut assembler =
        CaptureAssembler::new(SourceId("source".into()), spool, [schema(11)], 256).unwrap();
    assembler
        .push(SourceEvent::Relation(relation(11)), &mut journal)
        .unwrap();
    for id in 0..5000 {
        assembler
            .push(
                SourceEvent::StreamStart {
                    xid: 42,
                    first: id == 0,
                },
                &mut journal,
            )
            .unwrap();
        assembler
            .push(
                SourceEvent::Insert {
                    xid: 42,
                    subxid: 42,
                    relation: 11,
                    row: vec![
                        Cell::Text(Bytes::from(id.to_string())),
                        Cell::Text(Bytes::from_static(b"payload")),
                    ],
                },
                &mut journal,
            )
            .unwrap();
        assembler
            .push(SourceEvent::StreamStop, &mut journal)
            .unwrap();
    }
    let txn = assembler
        .push(
            SourceEvent::Commit {
                xid: 42,
                commit_lsn: PgLsn(100),
                end_lsn: PgLsn(108),
                commit_timestamp_micros: 1,
            },
            &mut journal,
        )
        .unwrap()
        .unwrap();
    assert_eq!(txn.mutation_chunks.len(), 5000);
    assert!(bincode::serialized_size(&txn).unwrap() < 512);
    drop(assembler);
    drop(journal);
    let (journal, recovery) = Journal::open(root.path().join("journal"), config).unwrap();
    assert_eq!(
        recovery
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        std::slice::from_ref(&txn)
    );
    let mut count = 0;
    for bytes in journal.chunks(&txn.mutation_chunks).unwrap() {
        let rows: Vec<Mutation> = bincode::deserialize(&bytes.unwrap()).unwrap();
        assert_eq!(rows.len(), 1);
        let MutationKind::Insert { row } = &rows[0].kind else {
            panic!("wrong mutation")
        };
        assert_eq!(row[0], Value::Int32(count));
        count += 1;
    }
    assert_eq!(count, 5000);
}

#[test]
fn buffered_capture_preserves_spools_and_mixed_schema_transactions_until_group_sync() {
    let root = tempfile::tempdir().unwrap();
    let spool_path = root.path().join("spool");
    let cfg = JournalConfig {
        segment_bytes: 1024,
        max_frame_bytes: 512,
        ..Default::default()
    };
    let (mut journal, _) = Journal::open(root.path().join("journal"), cfg.clone()).unwrap();
    let spool = TransactionSpool::open(&spool_path, SpoolConfig::default()).unwrap();
    let mut capture = CaptureAssembler::new(SourceId("source".into()), spool, [schema(11)], 128)
        .unwrap()
        .with_pending_commit_limit(2)
        .unwrap();
    let mut send = |capture: &mut CaptureAssembler, event| {
        capture
            .push_buffered_at(event, PgLsn(1), &mut journal)
            .unwrap();
    };
    send(&mut capture, SourceEvent::Relation(relation(11)));
    send(
        &mut capture,
        SourceEvent::StreamStart {
            xid: 1,
            first: true,
        },
    );
    send(
        &mut capture,
        SourceEvent::Insert {
            xid: 1,
            subxid: 1,
            relation: 11,
            row: row("1", "keep"),
        },
    );
    send(
        &mut capture,
        SourceEvent::Insert {
            xid: 1,
            subxid: 2,
            relation: 11,
            row: row("2", "rollback"),
        },
    );
    send(&mut capture, SourceEvent::Abort { xid: 1, subxid: 2 });
    let mut successor = schema(11);
    successor.version = 2;
    successor.columns.push(Column {
        field_id: 3,
        name: "extra".into(),
        data_type: ColumnType::String,
        nullable: true,
    });
    capture.set_schema(successor).unwrap();
    let mut changed_relation = relation(11);
    changed_relation.columns.push(PgColumn {
        name: "extra".into(),
        type_oid: 25,
        type_modifier: -1,
        identity: true,
    });
    send(&mut capture, SourceEvent::Relation(changed_relation));
    let mut old = row("1", "keep");
    old.push(Cell::Null);
    send(
        &mut capture,
        SourceEvent::Update {
            xid: 1,
            subxid: 1,
            relation: 11,
            old: Some(old),
            old_is_key: false,
            row: vec![
                Cell::Text(Bytes::from_static(b"3")),
                Cell::UnchangedToast,
                Cell::Text(Bytes::from_static(b"new")),
            ],
        },
    );
    send(
        &mut capture,
        SourceEvent::Commit {
            xid: 1,
            commit_lsn: PgLsn(10),
            end_lsn: PgLsn(11),
            commit_timestamp_micros: 1,
        },
    );
    send(
        &mut capture,
        SourceEvent::Begin {
            xid: 4,
            final_lsn: PgLsn(20),
            commit_timestamp_micros: 2,
        },
    );
    send(
        &mut capture,
        SourceEvent::Delete {
            xid: 4,
            subxid: 4,
            relation: 11,
            old: vec![
                Cell::Text(Bytes::from_static(b"3")),
                Cell::Text(Bytes::from_static(b"keep")),
                Cell::Text(Bytes::from_static(b"new")),
            ],
            old_is_key: false,
        },
    );
    send(
        &mut capture,
        SourceEvent::Commit {
            xid: 4,
            commit_lsn: PgLsn(20),
            end_lsn: PgLsn(21),
            commit_timestamp_micros: 2,
        },
    );
    assert_eq!(capture.pending_commit_count(), 2);
    assert!(
        capture.pending_commit_bytes() > 128,
        "group payload is a flush threshold, not a single-TX limit"
    );
    assert_eq!(journal.staged_lsn(), PgLsn(21));
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    assert!(journal.transactions().is_empty());
    assert!(spool_path.join("txn-1").is_dir());
    assert!(spool_path.join("txn-4").is_dir());
    assert!(capture.push(SourceEvent::Metadata, &mut journal).is_err());
    assert!(
        capture
            .push_buffered_at(
                SourceEvent::Abort { xid: 1, subxid: 1 },
                PgLsn(1),
                &mut journal
            )
            .is_err()
    );
    assert!(
        capture
            .push_buffered_at(
                SourceEvent::Begin {
                    xid: 1,
                    final_lsn: PgLsn(30),
                    commit_timestamp_micros: 0
                },
                PgLsn(1),
                &mut journal
            )
            .is_err()
    );
    capture
        .push_buffered_at(
            SourceEvent::Begin {
                xid: 5,
                final_lsn: PgLsn(30),
                commit_timestamp_micros: 3,
            },
            PgLsn(22),
            &mut journal,
        )
        .unwrap();
    assert!(
        capture
            .push_buffered_at(
                SourceEvent::Commit {
                    xid: 5,
                    commit_lsn: PgLsn(30),
                    end_lsn: PgLsn(31),
                    commit_timestamp_micros: 3,
                },
                PgLsn(22),
                &mut journal,
            )
            .is_err()
    );
    assert_eq!(capture.pending_commit_count(), 2);
    assert_eq!(journal.staged_lsn(), PgLsn(21));
    let transactions = capture.flush_commits(&mut journal).unwrap();
    assert_eq!(
        transactions.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
        [1, 4]
    );
    assert_eq!(transactions[0].schema_versions[0].version, 2);
    assert_eq!(capture.pending_commit_count(), 0);
    assert_eq!(capture.pending_commit_bytes(), 0);
    assert!(!spool_path.join("txn-1").exists());
    assert!(!spool_path.join("txn-4").exists());
    assert!(spool_path.join("txn-5").is_dir());
    assert_eq!(journal.durable_lsn(), PgLsn(21));
    drop(capture);
    drop(journal);
    let (journal, recovery) = Journal::open(root.path().join("journal"), cfg).unwrap();
    assert_eq!(
        recovery
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        transactions
    );
    let mutations: Vec<Mutation> = journal
        .chunks(&transactions[0].mutation_chunks)
        .unwrap()
        .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
        .collect();
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].schema_version, 1);
    assert_eq!(mutations[1].schema_version, 2);
    let MutationKind::Update { row, .. } = &mutations[1].kind else {
        panic!("update lost");
    };
    assert_eq!(
        row,
        &vec![
            Value::Int32(3),
            Value::String("keep".into()),
            Value::String("new".into())
        ]
    );
    let deleted: Vec<Mutation> = journal
        .chunks(&transactions[1].mutation_chunks)
        .unwrap()
        .flat_map(|bytes| bincode::deserialize::<Vec<Mutation>>(&bytes.unwrap()).unwrap())
        .collect();
    assert!(matches!(deleted[0].kind, MutationKind::Delete { .. }));
}
