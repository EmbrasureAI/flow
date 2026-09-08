use flow_ingress_journal::{Error, Journal, JournalConfig};
use flow_model::{
    JournalChunkRef, PgLsn, SourceId, SourceTransaction, TableId, TableMutationCount,
    TableSchemaVersion,
};
use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
};

fn config() -> JournalConfig {
    JournalConfig {
        segment_bytes: 600,
        quota_bytes: 16_000,
        max_frame_bytes: 400,
        max_open_transactions: 8,
    }
}
fn transaction(xid: u32, lsn: u64, chunks: Vec<JournalChunkRef>) -> SourceTransaction {
    let count = chunks.len() as u64;
    let mut transaction = flow_ingress_journal::legacy::SourceTransaction {
        source_id: SourceId("source-a".into()),
        xid,
        begin_lsn: PgLsn(lsn - 2),
        commit_lsn: PgLsn(lsn),
        end_lsn: PgLsn(lsn + 1),
        commit_timestamp_micros: 1_700_000_000_000_000,
        schema_versions: vec![
            TableSchemaVersion {
                table_id: TableId(1),
                version: 1,
            },
            TableSchemaVersion {
                table_id: TableId(2),
                version: 1,
            },
        ],
        affected_tables: vec![TableId(1), TableId(2)],
        mutation_chunks: chunks,
    }
    .into_current()
    .unwrap();
    // Opaque test chunks represent one event each; the second table has no rows.
    transaction.table_mutation_counts = Some(vec![
        TableMutationCount {
            table_id: TableId(1),
            mutations: count,
        },
        TableMutationCount {
            table_id: TableId(2),
            mutations: 0,
        },
    ]);
    transaction
}

#[test]
fn interleaved_transactions_recover_only_terminal_commits_and_reuse_xids() {
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
    let a = journal.append_chunk(7, &[1; 300]).unwrap();
    journal
        .append_chunk(8, b"abandoned streamed transaction")
        .unwrap();
    let b = journal.append_chunk(7, &[2; 300]).unwrap();
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    journal
        .commit(transaction(7, 100, vec![a.clone(), b.clone()]))
        .unwrap();
    assert_eq!(journal.durable_lsn(), PgLsn(101));
    drop(journal);

    let (mut journal, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(recovered.transactions.len(), 1);
    assert_eq!(journal.read_chunk(&a).unwrap(), vec![1; 300]);
    assert_eq!(journal.read_chunk(&b).unwrap(), vec![2; 300]);
    let c = journal.append_chunk(8, b"replayed transaction").unwrap();
    journal.commit(transaction(8, 200, vec![c])).unwrap();
    drop(journal);
    let (_, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .map(|t| t.unwrap().xid)
            .collect::<Vec<_>>(),
        [7, 8]
    );
    assert_eq!(recovered.truncated_bytes, 0);
}

#[test]
fn torn_and_corrupt_suffix_is_removed_before_accepting_new_commits() {
    for corrupt_crc in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
        let first = journal.append_chunk(1, b"complete").unwrap();
        journal.commit(transaction(1, 10, vec![first])).unwrap();
        let bad = journal.append_chunk(2, &[9; 300]).unwrap();
        journal
            .commit(transaction(2, 20, vec![bad.clone()]))
            .unwrap();
        drop(journal);
        let path = dir.path().join(format!("{:020}.segment", bad.segment));
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        if corrupt_crc {
            file.seek(SeekFrom::Start(bad.offset + 28)).unwrap();
            file.write_all(&[8]).unwrap();
        } else {
            file.set_len(bad.offset + 31).unwrap();
        }
        drop(file);
        let (mut journal, recovered) = Journal::open(dir.path(), config()).unwrap();
        assert!(recovered.truncated_bytes > 0);
        assert_eq!(recovered.transactions.len(), 1);
        let replacement = journal.append_chunk(2, b"resend").unwrap();
        journal
            .commit(transaction(2, 20, vec![replacement]))
            .unwrap();
        drop(journal);
        let (_, recovered) = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(recovered.transactions.len(), 2);
    }
}

#[test]
fn reclamation_preserves_slow_transactions_and_recovery_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
    for xid in 1..=6 {
        let reference = journal.append_chunk(xid, &[xid as u8; 300]).unwrap();
        journal
            .commit(transaction(xid, u64::from(xid) * 10, vec![reference]))
            .unwrap();
    }
    let retained = journal
        .transactions()
        .iter()
        .unwrap()
        .nth(4)
        .unwrap()
        .unwrap()
        .mutation_chunks
        .first
        .clone()
        .unwrap();
    assert!(journal.reclaim(PgLsn(41)).unwrap() > 0);
    assert_eq!(journal.read_chunk(&retained).unwrap(), vec![5; 300]);
    drop(journal);
    let (journal, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(recovered.reclaimed_lsn, PgLsn(41));
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .map(|t| t.unwrap().xid)
            .collect::<Vec<_>>(),
        [5, 6]
    );
    assert_eq!(journal.durable_lsn(), PgLsn(61));
}

#[test]
fn quota_and_exclusive_ownership_do_not_advance_durability() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.quota_bytes = 1200;
    let (mut journal, _) = Journal::open(dir.path(), cfg.clone()).unwrap();
    assert!(Journal::open(dir.path(), cfg).is_err());
    let reference = journal.append_chunk(1, &[0; 300]).unwrap();
    assert!(journal.append_chunk(1, &[0; 300]).is_ok());
    assert!(matches!(
        journal.append_chunk(1, &[0; 300]),
        Err(Error::Quota { .. })
    ));
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    assert!(journal.commit(transaction(1, 10, vec![reference])).is_err());
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    assert!(
        fs::metadata(dir.path().join("00000000000000000000.segment"))
            .unwrap()
            .len()
            > 0
    );
}

#[test]
fn large_transactions_stream_across_segments_with_constant_terminal_size() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig {
        segment_bytes: 64 << 10,
        quota_bytes: 16 << 20,
        max_frame_bytes: 400,
        max_open_transactions: 8,
    };
    let (mut journal, _) = Journal::open(dir.path(), cfg.clone()).unwrap();
    let mut terminal = transaction(1, 10, vec![]);
    let mut small_terminal_size = 0;
    for ordinal in 0..150_000u64 {
        journal.append_chunk(1, &ordinal.to_le_bytes()).unwrap();
        if ordinal == 0 {
            terminal.mutation_chunks = journal.transaction_chunks(1);
            small_terminal_size = bincode::serialized_size(&terminal).unwrap();
        }
        if ordinal == 1000 {
            let other = journal.append_chunk(2, b"interleaved").unwrap();
            journal.commit(transaction(2, 5, vec![other])).unwrap();
        }
    }
    terminal.mutation_chunks = journal.transaction_chunks(1);
    assert_eq!(terminal.mutation_chunks.len(), 150_000);
    terminal.table_mutation_counts.as_mut().unwrap()[0].mutations = 150_000;
    assert_eq!(
        bincode::serialized_size(&terminal).unwrap(),
        small_terminal_size
    );
    assert!(small_terminal_size < u64::from(cfg.max_frame_bytes));
    journal.commit(terminal.clone()).unwrap();
    let bytes = journal.bytes_used();
    drop(journal);
    let (mut journal, recovered) = Journal::open(dir.path(), cfg).unwrap();
    assert_eq!(recovered.truncated_bytes, 0);
    assert_eq!(journal.bytes_used(), bytes);
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .last()
            .unwrap()
            .unwrap(),
        terminal
    );
    for (ordinal, payload) in journal
        .chunks(&terminal.mutation_chunks)
        .unwrap()
        .enumerate()
    {
        assert_eq!(payload.unwrap(), (ordinal as u64).to_le_bytes());
    }
    // A completed interleaved transaction cannot reclaim the older chunks of
    // the still-retained large transaction.
    assert_eq!(journal.reclaim(PgLsn(6)).unwrap(), 0);
    assert_eq!(
        journal.chunks(&terminal.mutation_chunks).unwrap().count(),
        150_000
    );
    assert!(journal.reclaim(terminal.end_lsn).unwrap() > 0);
    assert!(fs::read_dir(dir.path()).unwrap().all(|entry| {
        let name = entry.unwrap().file_name();
        let name = name.to_str().unwrap();
        name.ends_with(".segment")
            || name.ends_with(".commits-v1")
            || name == "writer.lock"
            || name == "reclaimed"
    }));
}

#[test]
fn torn_terminal_does_not_seal_a_range_and_reused_xid_starts_fresh() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
    journal
        .append_chunk(7, b"not committed after tear")
        .unwrap();
    let mut txn = transaction(7, 100, vec![]);
    txn.mutation_chunks = journal.transaction_chunks(7);
    journal.commit(txn.clone()).unwrap();
    let last = txn.mutation_chunks.last.as_ref().unwrap();
    let terminal_offset = last.offset + 28 + u64::from(last.length);
    drop(journal);
    // This small fixture keeps the terminal in the same segment as its chunk.
    OpenOptions::new()
        .write(true)
        .open(dir.path().join(format!("{:020}.segment", last.segment)))
        .unwrap()
        .set_len(terminal_offset + 13)
        .unwrap();
    let (mut journal, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(recovered.truncated_bytes, 13);
    assert!(recovered.transactions.is_empty());
    assert_eq!(journal.durable_lsn(), PgLsn(0));
    journal.append_chunk(7, b"replayed").unwrap();
    txn.mutation_chunks = journal.transaction_chunks(7);
    journal.commit(txn.clone()).unwrap();
    drop(journal);
    let (journal, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        std::slice::from_ref(&txn)
    );
    let chunks: Vec<_> = journal
        .chunks(&txn.mutation_chunks)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(chunks, [b"replayed".to_vec()]);
}

#[test]
fn legacy_terminal_migrates_and_future_versions_are_not_truncated() {
    use flow_ingress_journal::ChunkReader;
    fn frame(kind: u8, sequence: u64, payload: &[u8]) -> Vec<u8> {
        let mut bytes = [0u8; 28].to_vec();
        bytes[..4].copy_from_slice(b"FLJ1");
        bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
        bytes[6] = kind;
        bytes[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[12..20].copy_from_slice(&sequence.to_le_bytes());
        bytes[20..24].copy_from_slice(&7u32.to_le_bytes());
        let mut hash = crc32fast::Hasher::new();
        hash.update(&bytes[..24]);
        hash.update(payload);
        bytes[24..].copy_from_slice(&hash.finalize().to_le_bytes());
        bytes.extend(payload);
        bytes
    }
    let dir = tempfile::tempdir().unwrap();
    let reference = JournalChunkRef {
        segment: 0,
        offset: 0,
        length: 6,
    };
    let legacy = flow_ingress_journal::legacy::SourceTransaction {
        source_id: SourceId("old".into()),
        xid: 7,
        begin_lsn: PgLsn(1),
        commit_lsn: PgLsn(10),
        end_lsn: PgLsn(11),
        commit_timestamp_micros: 0,
        schema_versions: vec![TableSchemaVersion {
            table_id: TableId(1),
            version: 0,
        }],
        affected_tables: vec![TableId(1)],
        mutation_chunks: vec![reference],
    };
    let mut bytes = frame(1, 0, b"legacy");
    bytes.extend(frame(2, 1, &bincode::serialize(&legacy).unwrap()));
    let bounded = flow_ingress_journal::legacy::BoundedSourceTransaction {
        source_id: SourceId("old".into()),
        xid: 7,
        begin_lsn: PgLsn(12),
        commit_lsn: PgLsn(20),
        end_lsn: PgLsn(21),
        commit_timestamp_micros: 0,
        schema_versions: vec![TableSchemaVersion {
            table_id: TableId(1),
            version: 0,
        }],
        affected_tables: vec![TableId(1)],
        mutation_chunks: transaction(
            7,
            20,
            vec![JournalChunkRef {
                segment: 0,
                offset: bytes.len() as u64,
                length: 7,
            }],
        )
        .mutation_chunks,
    };
    bytes.extend(frame(1, 2, b"bounded"));
    bytes.extend(frame(4, 3, &bincode::serialize(&bounded).unwrap()));
    let path = dir.path().join("00000000000000000000.segment");
    fs::write(&path, &bytes).unwrap();
    let (mut journal, recovery) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(
        recovery
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [
            legacy.into_current().unwrap(),
            bounded.into_current().unwrap()
        ]
    );
    assert_eq!(
        journal
            .chunks(
                &recovery
                    .transactions
                    .iter()
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .mutation_chunks
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        b"legacy"
    );
    journal.append_chunk(8, b"current").unwrap();
    let mut current = transaction(8, 30, vec![]);
    current.mutation_chunks = journal.transaction_chunks(8);
    current.table_mutation_counts.as_mut().unwrap()[0].mutations = 1;
    journal.commit(current.clone()).unwrap();
    drop(journal);
    let (_, recovery) = Journal::open(dir.path(), config()).unwrap();
    let recovered = recovery
        .transactions
        .iter()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(recovered.len(), 3);
    assert!(
        recovered[..2]
            .iter()
            .all(|txn| txn.table_mutation_counts.is_none())
    );
    assert_eq!(recovered[2], current);

    // A new binary must report version incompatibility instead of treating a
    // checksummed future record as a damaged suffix and deleting it.
    bytes = frame(99, 0, b"future");
    let future = tempfile::tempdir().unwrap();
    let path = future.path().join("00000000000000000000.segment");
    fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        Journal::open(future.path(), config()),
        Err(Error::RecordKind(99))
    ));
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn terminal_cursor_is_stable_during_append_and_rebuilds_a_torn_index() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let dir = tempfile::tempdir().unwrap();
    let cfg = JournalConfig {
        segment_bytes: 64 << 10,
        quota_bytes: 16 << 20,
        max_frame_bytes: 400,
        max_open_transactions: 8,
    };
    let (mut journal, _) = Journal::open(dir.path(), cfg.clone()).unwrap();
    let reader = journal.reader();
    let finished = Arc::new(AtomicBool::new(false));
    let done = finished.clone();
    let task = std::thread::spawn(move || {
        let mut after = PgLsn(0);
        let mut seen = 0;
        loop {
            let mut group = 0;
            for txn in reader.transactions_after(after).unwrap() {
                let txn = txn.unwrap();
                assert!(txn.end_lsn > after);
                assert_eq!(txn.xid, seen + 1);
                after = txn.end_lsn;
                seen += 1;
                group += 1;
            }
            assert_eq!(group % 5, 0, "reader observed a partial commit group");
            if done.load(Ordering::Acquire) && seen == 500 {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(seen, 500);
    });
    for xid in 1..=500 {
        let chunks = if xid % 7 == 0 {
            Vec::new()
        } else {
            vec![journal.append_chunk(xid, b"row").unwrap()]
        };
        journal
            .stage_commit(transaction(xid, u64::from(xid) * 10, chunks))
            .unwrap();
        if xid % 5 == 0 {
            journal.flush_commits().unwrap();
        }
    }
    finished.store(true, Ordering::Release);
    task.join().unwrap();
    assert_eq!(journal.transactions().len(), 500);
    let first_page = journal
        .reader()
        .transactions_after(PgLsn(2001))
        .unwrap()
        .take(17)
        .map(|txn| txn.unwrap().xid)
        .collect::<Vec<_>>();
    assert_eq!(first_page, (201..=217).collect::<Vec<_>>());
    drop(journal);
    // Derived indexes may be absent or torn after a crash. Recovery validates
    // primary frames and reconstructs every terminal, with no lost ACK prefix.
    for entry in fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap().ends_with(".commits-v1") {
            OpenOptions::new()
                .write(true)
                .open(entry.path())
                .unwrap()
                .set_len(3)
                .unwrap();
        }
    }
    let (mut journal, recovered) = Journal::open(dir.path(), cfg).unwrap();
    assert_eq!(recovered.truncated_bytes, 0);
    assert_eq!(recovered.transactions.len(), 500);
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        500
    );
    journal.reclaim(PgLsn(4001)).unwrap();
    assert_eq!(journal.transactions().len(), 100);
    drop(journal);
    let (_, recovered) = Journal::open(
        dir.path(),
        JournalConfig {
            segment_bytes: 64 << 10,
            quota_bytes: 16 << 20,
            max_frame_bytes: 400,
            max_open_transactions: 8,
        },
    )
    .unwrap();
    assert_eq!(recovered.transactions.len(), 100);
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .xid,
        401
    );
}

#[test]
fn staged_group_crosses_segments_without_exposure_or_premature_reclamation() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
    // The oldest payload belongs to a later commit, and must survive reclaim
    // even after that transaction leaves the open-XID map for the staged group.
    let old = journal.append_chunk(2, &[2; 300]).unwrap();
    let first = journal.append_chunk(1, &[1; 300]).unwrap();
    journal.commit(transaction(1, 10, vec![first])).unwrap();
    let before = journal.transactions();
    let second = transaction(2, 20, vec![old]);
    journal.stage_commit(second.clone()).unwrap();
    let third = journal.append_chunk(3, &[3; 300]).unwrap();
    let third = transaction(3, 30, vec![third]);
    journal.stage_commit(third.clone()).unwrap();
    assert_eq!(journal.staged_lsn(), PgLsn(31));
    assert_eq!(journal.durable_lsn(), PgLsn(11));
    assert_eq!(journal.transactions().len(), 1);
    assert_eq!(
        journal
            .reader()
            .transactions_after(PgLsn(11))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(journal.reclaim(PgLsn(11)).unwrap(), 0);
    journal.flush_commits().unwrap();
    assert_eq!(journal.durable_lsn(), PgLsn(31));
    assert_eq!(before.iter().unwrap().count(), 1);
    assert_eq!(journal.transactions().len(), 2);
    for txn in [&second, &third] {
        assert_eq!(
            journal
                .chunks(&txn.mutation_chunks)
                .unwrap()
                .next()
                .unwrap()
                .unwrap(),
            vec![txn.xid as u8; 300]
        );
    }
    drop(journal);
    let (_, recovery) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(recovery.reclaimed_lsn, PgLsn(11));
    assert_eq!(
        recovery
            .transactions
            .iter()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [second, third]
    );
}

#[test]
fn staged_order_and_limits_reject_invalid_transitions_without_losing_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_open_transactions = 2;
    let (mut journal, _) = Journal::open(dir.path(), cfg).unwrap();
    for counts in [
        None,
        Some(vec![TableMutationCount {
            table_id: TableId(1),
            mutations: 1,
        }]),
        Some(vec![
            TableMutationCount {
                table_id: TableId(1),
                mutations: 1,
            },
            TableMutationCount {
                table_id: TableId(1),
                mutations: 1,
            },
        ]),
        Some(vec![
            TableMutationCount {
                table_id: TableId(1),
                mutations: u64::MAX,
            },
            TableMutationCount {
                table_id: TableId(2),
                mutations: 1,
            },
        ]),
    ] {
        let mut invalid = transaction(1, 10, vec![]);
        invalid.table_mutation_counts = counts;
        assert!(matches!(
            journal.stage_commit(invalid),
            Err(Error::Transaction(_))
        ));
        assert_eq!(journal.staged_lsn(), PgLsn(0));
    }
    journal.stage_commit(transaction(1, 10, vec![])).unwrap();
    assert!(matches!(
        journal.append_chunk(1, b"late"),
        Err(Error::Transaction(_))
    ));
    assert!(matches!(journal.abort(1), Err(Error::Transaction(_))));
    assert!(matches!(
        journal.stage_commit(transaction(1, 20, vec![])),
        Err(Error::Transaction(_))
    ));
    assert!(matches!(
        journal.stage_commit(transaction(2, 5, vec![])),
        Err(Error::Transaction(_))
    ));
    let second = journal.append_chunk(2, b"second").unwrap();
    assert!(matches!(
        journal.stage_commit(transaction(3, 30, vec![])),
        Err(Error::Limit(_))
    ));
    journal
        .stage_commit(transaction(2, 20, vec![second]))
        .unwrap();
    assert!(matches!(
        journal.append_chunk(3, b"third"),
        Err(Error::Limit(_))
    ));
    journal.flush_commits().unwrap();
    assert_eq!(journal.transactions().len(), 2);
    // XID reuse is permitted after the terminal is durable.
    journal.commit(transaction(1, 30, vec![])).unwrap();
    assert_eq!(journal.durable_lsn(), PgLsn(31));
}

#[test]
fn restart_recovers_only_complete_transactions_from_an_unflushed_group() {
    for tear in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
        let first = journal.append_chunk(1, &[1; 300]).unwrap();
        journal
            .stage_commit(transaction(1, 10, vec![first]))
            .unwrap();
        let second = journal.append_chunk(2, &[2; 300]).unwrap();
        journal
            .stage_commit(transaction(2, 20, vec![second]))
            .unwrap();
        assert_eq!(journal.durable_lsn(), PgLsn(0));
        drop(journal);
        if tear {
            let last = fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "segment"))
                .max()
                .unwrap();
            let length = fs::metadata(&last).unwrap().len();
            OpenOptions::new()
                .write(true)
                .open(last)
                .unwrap()
                .set_len(length - 1)
                .unwrap();
        }
        // Reopening is the durability boundary after an uncertain shutdown.
        // Independent source transactions may recover as a valid prefix.
        let (journal, recovery) = Journal::open(dir.path(), config()).unwrap();
        assert_eq!(recovery.transactions.len(), if tear { 1 } else { 2 });
        assert_eq!(journal.durable_lsn(), PgLsn(if tear { 11 } else { 21 }));
        assert_eq!(journal.staged_lsn(), journal.durable_lsn());
    }
}

#[test]
fn replay_cursor_repositions_after_partial_ranges_and_rejects_xid_reuse() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_bytes = 1 << 20;
    cfg.quota_bytes = 2 << 20;
    cfg.max_frame_bytes = 128 << 10;
    let (mut journal, _) = Journal::open(dir.path(), cfg).unwrap();
    let a = journal.append_chunk(1, b"first").unwrap();
    let interleaved = journal
        .append_chunk(77, b"filtered but checksummed")
        .unwrap();
    journal.abort(77).unwrap();
    let b = journal.append_chunk(1, b"second").unwrap();
    let first = transaction(1, 10, vec![a.clone(), b]);
    journal.commit(first.clone()).unwrap();
    let c = journal.append_chunk(2, b"next transaction").unwrap();
    let next = transaction(2, 20, vec![c]);
    journal.commit(next.clone()).unwrap();
    let d = journal.append_chunk(1, &[9; 70_000]).unwrap();
    let reused = transaction(1, 30, vec![d.clone()]);
    journal.commit(reused.clone()).unwrap();
    let reader = journal.reader();
    let mut cursor = reader.replay_cursor();
    {
        let mut partial = cursor.chunks(&first.mutation_chunks).unwrap();
        assert_eq!(partial.next().unwrap().unwrap(), b"first");
    }
    assert_eq!(
        cursor
            .chunks(&next.mutation_chunks)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [b"next transaction".to_vec()]
    );
    assert_eq!(
        cursor
            .chunks(&first.mutation_chunks)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(
        cursor
            .chunks(&reused.mutation_chunks)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        [vec![9; 70_000]]
    );
    // A forged descriptor joining two incarnations must encounter the first
    // COMMIT, not silently accept the later chunk with the same XID.
    let forged = transaction(
        1,
        30,
        vec![a, first.mutation_chunks.last.as_ref().unwrap().clone(), d],
    );
    {
        let mut range = cursor.chunks(&forged.mutation_chunks).unwrap();
        assert_eq!(range.next().unwrap().unwrap(), b"first");
        assert_eq!(range.next().unwrap().unwrap(), b"second");
        assert!(range.next().unwrap().is_err());
        assert!(range.next().is_none());
    }
    let mut wrong = first.mutation_chunks.clone();
    wrong.checksum ^= 1;
    assert!(
        cursor
            .chunks(&wrong)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .is_err()
    );
    assert_eq!(
        cursor
            .chunks(&next.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        b"next transaction"
    );
    // Corruption in a skipped XID must still fail CRC validation. A new reader
    // observes the injected damage; successful subsequent ranges reopen cleanly.
    let path = dir
        .path()
        .join(format!("{:020}.segment", interleaved.segment));
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(interleaved.offset + 28)).unwrap();
    file.write_all(b"X").unwrap();
    let mut damaged = reader.replay_cursor();
    assert!(
        damaged
            .chunks(&first.mutation_chunks)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .is_err()
    );
    assert_eq!(
        damaged
            .chunks(&next.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        b"next transaction"
    );
}

#[test]
fn replay_cursor_refreshes_a_growing_tail_before_crossing_segments() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_bytes = 4096;
    cfg.quota_bytes = 64 << 10;
    cfg.max_frame_bytes = 2048;
    let (mut journal, _) = Journal::open(dir.path(), cfg.clone()).unwrap();
    let a = journal.append_chunk(1, b"initial tail").unwrap();
    let first = transaction(1, 10, vec![a]);
    journal.commit(first.clone()).unwrap();
    let mut cursor = journal.replay_cursor();
    assert_eq!(
        cursor
            .chunks(&first.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        b"initial tail"
    );
    let chunks = (0..8)
        .map(|i| journal.append_chunk(2, &[i; 600]).unwrap())
        .collect();
    let second = transaction(2, 20, chunks);
    assert_eq!(
        first.mutation_chunks.first.as_ref().unwrap().segment,
        second.mutation_chunks.first.as_ref().unwrap().segment
    );
    assert!(
        second.mutation_chunks.first.as_ref().unwrap().segment
            < second.mutation_chunks.last.as_ref().unwrap().segment
    );
    journal.commit(second.clone()).unwrap();
    let expected: Vec<_> = (0..8).map(|i| vec![i; 600]).collect();
    assert_eq!(
        cursor
            .chunks(&second.mutation_chunks)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        expected
    );
    let c = journal.append_chunk(3, &[42; 700]).unwrap();
    let third = transaction(3, 30, vec![c]);
    journal.commit(third.clone()).unwrap();
    assert_eq!(
        cursor
            .chunks(&third.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        vec![42; 700]
    );
    // Cursor owners share metadata only, never seek positions. One can move
    // backwards while another independently traverses the same physical files.
    let reader = journal.reader();
    let range = second.mutation_chunks.clone();
    let other = std::thread::spawn(move || {
        reader
            .replay_cursor()
            .chunks(&range)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    });
    assert_eq!(
        cursor
            .chunks(&first.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        b"initial tail"
    );
    assert_eq!(other.join().unwrap(), expected);
    drop(cursor);
    drop(journal);
    let (journal, recovered) = Journal::open(dir.path(), cfg).unwrap();
    assert_eq!(recovered.truncated_bytes, 0);
    let mut cursor = journal.replay_cursor();
    for transaction in recovered.transactions.iter().unwrap() {
        let transaction = transaction.unwrap();
        assert_eq!(
            cursor
                .chunks(&transaction.mutation_chunks)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len() as u64,
            transaction.mutation_chunks.len()
        );
    }
}

#[test]
fn replay_cursor_reopens_after_a_partial_frame_read_error() {
    use flow_ingress_journal::ChunkReader;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_bytes = 1 << 20;
    cfg.quota_bytes = 2 << 20;
    cfg.max_frame_bytes = 128 << 10;
    let (mut journal, _) = Journal::open(dir.path(), cfg).unwrap();
    let a = journal.append_chunk(1, &[7; 70_000]).unwrap();
    let transaction = transaction(1, 10, vec![a.clone()]);
    journal.commit(transaction.clone()).unwrap();
    let path = dir.path().join(format!("{:020}.segment", a.segment));
    let intact = fs::read(&path).unwrap();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(a.offset + 28 + 65_537)
        .unwrap();
    let mut cursor = journal.replay_cursor();
    assert!(
        cursor
            .chunks(&transaction.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .is_err()
    );
    fs::write(&path, intact).unwrap();
    assert_eq!(
        cursor
            .chunks(&transaction.mutation_chunks)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        vec![7; 70_000]
    );
}

#[test]
fn reduced_frame_limit_leaves_retained_segments_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(dir.path(), config()).unwrap();
    let chunk = journal.append_chunk(1, &[7; 300]).unwrap();
    let txn = transaction(1, 10, vec![chunk.clone()]);
    journal.commit(txn.clone()).unwrap();
    journal.commit(transaction(2, 20, vec![])).unwrap();
    drop(journal);
    let segments = || {
        let mut files = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "segment"))
            .map(|path| {
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(path).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    };
    let before = segments();
    assert!(before.len() > 1);
    let mut smaller = config();
    smaller.max_frame_bytes = 250;
    assert!(matches!(
        Journal::open(dir.path(), smaller),
        Err(Error::FrameLimit {
            length: 300,
            limit: 250
        })
    ));
    assert_eq!(segments(), before);
    let (journal, recovered) = Journal::open(dir.path(), config()).unwrap();
    assert_eq!(recovered.transactions.len(), 2);
    assert_eq!(
        recovered
            .transactions
            .iter()
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        txn
    );
    assert_eq!(journal.read_chunk(&chunk).unwrap(), vec![7; 300]);
}

#[test]
fn empty_commits_preserve_other_transactions_recovery_abort_space() {
    let dir = tempfile::tempdir().unwrap();
    let mut limited = config();
    limited.quota_bytes = 1050;
    let (mut journal, _) = Journal::open(dir.path(), limited.clone()).unwrap();
    journal.append_chunk(1, &[0; 100]).unwrap();
    let mut accepted = 0;
    for xid in 2..=7 {
        let mut txn = transaction(xid, u64::from(xid) * 10, vec![]);
        txn.source_id = SourceId("s".into());
        txn.affected_tables.clear();
        txn.schema_versions.clear();
        txn.table_mutation_counts = Some(vec![]);
        match journal.commit(txn) {
            Ok(()) => accepted += 1,
            Err(Error::Quota { .. }) => break,
            Err(error) => panic!("unexpected commit failure: {error}"),
        }
    }
    assert_eq!(accepted, 5);
    drop(journal);
    let (journal, recovered) = Journal::open(dir.path(), limited).unwrap();
    assert_eq!(recovered.transactions.len(), accepted);
    assert!(journal.transaction_chunks(1).is_empty());
}

#[test]
fn commit_can_spend_its_own_pending_abort_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let txn = transaction(
        1,
        100,
        vec![JournalChunkRef {
            segment: 0,
            offset: 0,
            length: 100,
        }],
    );
    let mut empty = transaction(2, 20, vec![]);
    empty.source_id = SourceId("s".into());
    empty.affected_tables.clear();
    empty.schema_versions.clear();
    empty.table_mutation_counts = Some(vec![]);
    let mut limited = config();
    limited.quota_bytes = 128
        + 3 * (28 + bincode::serialized_size(&empty).unwrap() + 32)
        + 28
        + bincode::serialized_size(&txn).unwrap()
        + 32;
    let (mut journal, _) = Journal::open(dir.path(), limited.clone()).unwrap();
    let chunk = journal.append_chunk(1, &[0; 100]).unwrap();
    assert_eq!(transaction(1, 100, vec![chunk]), txn);
    for xid in 2..=4 {
        empty.xid = xid;
        empty.commit_lsn = PgLsn(u64::from(xid) * 10);
        empty.end_lsn = PgLsn(empty.commit_lsn.0 + 1);
        journal.commit(empty.clone()).unwrap();
    }
    journal.commit(txn).unwrap();
    assert_eq!(journal.bytes_used(), limited.quota_bytes);
    drop(journal);
    let (_, recovered) = Journal::open(dir.path(), limited).unwrap();
    assert_eq!(recovered.transactions.len(), 4);
}
