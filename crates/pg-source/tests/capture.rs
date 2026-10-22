use bytes::{BufMut, Bytes, BytesMut};
use flow_model::PgLsn;
use flow_pg_source::{Cell, Decoder, Error, SourceEvent, SpoolConfig, TransactionSpool};

fn message(tag: u8, body: impl FnOnce(&mut BytesMut)) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(tag);
    body(&mut b);
    b.freeze()
}
fn relation(id: u32) -> Bytes {
    message(b'R', |b| {
        b.put_u32(id);
        b.extend_from_slice(b"public\0items\0f");
        b.put_u16(2);
        for (name, oid) in [(b"id\0".as_slice(), 23), (b"body\0".as_slice(), 25)] {
            b.put_u8(1);
            b.extend_from_slice(name);
            b.put_u32(oid);
            b.put_i32(-1);
        }
    })
}
fn tuple(b: &mut BytesMut, id: &[u8], body: Option<&[u8]>) {
    b.put_u16(2);
    b.put_u8(b't');
    b.put_u32(id.len() as u32);
    b.extend_from_slice(id);
    if let Some(body) = body {
        b.put_u8(b't');
        b.put_u32(body.len() as u32);
        b.extend_from_slice(body);
    } else {
        b.put_u8(b'u');
    }
}

#[test]
fn wire_transaction_boundaries_multitable_pk_change_and_toast_survive_decode() {
    let mut decoder = Decoder::new(1 << 20);
    decoder.decode(relation(11)).unwrap();
    decoder.decode(relation(12)).unwrap();
    decoder
        .decode(message(b'B', |b| {
            b.put_u64(100);
            b.put_i64(0);
            b.put_u32(42);
        }))
        .unwrap();
    let insert = decoder
        .decode(message(b'I', |b| {
            b.put_u32(11);
            b.put_u8(b'N');
            tuple(b, b"1", Some(b"old"));
        }))
        .unwrap();
    assert!(matches!(
        insert,
        SourceEvent::Insert {
            xid: 42,
            subxid: 42,
            relation: 11,
            ..
        }
    ));
    let update = decoder
        .decode(message(b'U', |b| {
            b.put_u32(12);
            b.put_u8(b'O');
            tuple(b, b"1", Some(b"old"));
            b.put_u8(b'N');
            tuple(b, b"2", None);
        }))
        .unwrap();
    let SourceEvent::Update {
        old: Some(old),
        old_is_key,
        row,
        ..
    } = update
    else {
        panic!("update missing");
    };
    assert!(!old_is_key);
    assert_eq!(old[0], Cell::Text(Bytes::from_static(b"1")));
    assert_eq!(row[0], Cell::Text(Bytes::from_static(b"2")));
    assert!(matches!(
        decoder.relation(12).unwrap().validate_row(&row),
        Err(Error::UnchangedToast(12))
    ));
    let commit = decoder
        .decode(message(b'C', |b| {
            b.put_u8(0);
            b.put_u64(100);
            b.put_u64(108);
            b.put_i64(123);
        }))
        .unwrap();
    assert_eq!(
        commit,
        SourceEvent::Commit {
            xid: 42,
            commit_lsn: PgLsn(100),
            end_lsn: PgLsn(108),
            commit_timestamp_micros: 946_684_800_000_123
        }
    );
}

#[test]
fn streamed_nested_abort_truncates_descendants_and_spills_with_bounded_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let config = SpoolConfig {
        segment_bytes: 80,
        quota_bytes: 2000,
        max_chunk_bytes: 64,
        max_transactions: 8,
        max_subtransactions: 8,
        ..SpoolConfig::default()
    };
    let mut spool = TransactionSpool::open(dir.path(), config.clone()).unwrap();
    let mut decoder = Decoder::new(1024);
    decoder.decode(relation(11)).unwrap();
    for xid in [42, 99] {
        spool.begin(xid).unwrap();
    }
    decoder
        .decode(message(b'S', |b| {
            b.put_u32(42);
            b.put_u8(1);
        }))
        .unwrap();
    for (subxid, data) in [(42, b'a'), (43, b'b'), (44, b'c')] {
        let event = decoder
            .decode(message(b'I', |b| {
                b.put_u32(subxid);
                b.put_u32(11);
                b.put_u8(b'N');
                tuple(b, b"1", Some(b"x"));
            }))
            .unwrap();
        let SourceEvent::Insert { xid, subxid, .. } = event else {
            panic!();
        };
        spool.append(xid, subxid, &[data; 64]).unwrap();
    }
    decoder.decode(message(b'E', |_| {})).unwrap();
    spool.append(99, 99, b"other table transaction").unwrap();
    let abort = decoder
        .decode(message(b'A', |b| {
            b.put_u32(42);
            b.put_u32(43);
        }))
        .unwrap();
    let SourceEvent::Abort { xid, subxid } = abort else {
        panic!();
    };
    spool.abort(xid, subxid).unwrap();
    decoder
        .decode(message(b'S', |b| {
            b.put_u32(42);
            b.put_u8(0);
        }))
        .unwrap();
    spool.append(42, 42, b"survives").unwrap();
    decoder.decode(message(b'E', |_| {})).unwrap();
    assert!(matches!(
        decoder
            .decode(message(b'c', |b| {
                b.put_u32(42);
                b.put_u8(0);
                b.put_u64(100);
                b.put_u64(110);
                b.put_i64(0);
            }))
            .unwrap(),
        SourceEvent::Commit { xid: 42, .. }
    ));
    let mut rows = Vec::new();
    spool
        .replay(42, |bytes| {
            rows.push(bytes.to_vec());
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, vec![vec![b'a'; 64], b"survives".to_vec()]);
    spool.discard(42).unwrap();
    spool.abort(99, 99).unwrap();
    assert_eq!(spool.bytes_used(), 0);
    spool.begin(7).unwrap();
    spool
        .append(7, 7, b"uncommitted before disconnect")
        .unwrap();
    drop(spool);
    let mut restarted = TransactionSpool::open(dir.path(), config).unwrap();
    assert_eq!(restarted.bytes_used(), 0);
    restarted.begin(7).unwrap();
}

#[test]
fn malformed_wire_input_never_panics_or_creates_complete_transactions() {
    let valid = relation(11);
    for end in 0..valid.len() {
        assert!(Decoder::new(1024).decode(valid.slice(..end)).is_err());
    }
    assert!(Decoder::new(10).decode(valid).is_err());
    assert!(Decoder::new(1024).decode(message(b'C', |_| {})).is_err());
}

#[test]
fn logical_heartbeats_preserve_transaction_boundaries_and_reject_bad_lengths() {
    let logical = |streamed: bool, flags: u8| {
        message(b'M', |b| {
            if streamed {
                b.put_u32(42);
            }
            b.put_u8(flags);
            b.put_u64(900);
            b.extend_from_slice(b"embrasure-flow-heartbeat\0");
            b.put_u32(0);
        })
    };
    let mut decoder = Decoder::new(1024);
    assert_eq!(
        decoder.decode(logical(false, 0)).unwrap(),
        SourceEvent::Metadata
    );
    assert!(decoder.decode(logical(false, 1)).is_err());
    assert!(decoder.decode(logical(false, 2)).is_err());
    decoder
        .decode(message(b'B', |b| {
            b.put_u64(1000);
            b.put_i64(0);
            b.put_u32(42);
        }))
        .unwrap();
    assert_eq!(
        decoder.decode(logical(false, 1)).unwrap(),
        SourceEvent::Metadata
    );
    assert!(matches!(
        decoder
            .decode(message(b'C', |b| {
                b.put_u8(0);
                b.put_u64(1000);
                b.put_u64(1010);
                b.put_i64(0);
            }))
            .unwrap(),
        SourceEvent::Commit {
            xid: 42,
            end_lsn: PgLsn(1010),
            ..
        }
    ));
    decoder
        .decode(message(b'S', |b| {
            b.put_u32(42);
            b.put_u8(1);
        }))
        .unwrap();
    assert_eq!(
        decoder.decode(logical(true, 1)).unwrap(),
        SourceEvent::Metadata
    );
    decoder.decode(message(b'E', |_| {})).unwrap();
    assert_eq!(
        decoder
            .decode(message(b'A', |b| {
                b.put_u32(42);
                b.put_u32(42);
            }))
            .unwrap(),
        SourceEvent::Abort {
            xid: 42,
            subxid: 42
        }
    );
    let valid = logical(false, 0);
    for end in 0..valid.len() {
        assert!(Decoder::new(1024).decode(valid.slice(..end)).is_err());
    }
}

#[test]
fn each_spool_and_stream_limit_reports_its_own_setting() {
    let dir = tempfile::tempdir().unwrap();
    let config = SpoolConfig {
        segment_bytes: 80,
        quota_bytes: 100,
        max_chunk_bytes: 64,
        max_transactions: 2,
        max_subtransactions: 2,
        ..SpoolConfig::default()
    };
    let mut spool = TransactionSpool::open(dir.path(), config).unwrap();
    spool.begin(1).unwrap();
    spool.begin(2).unwrap();
    let error = spool.begin(3).unwrap_err();
    assert!(
        matches!(error, Error::SpoolTransactions { limit: 2 }),
        "{error}"
    );
    assert!(error.to_string().contains("limits.spool_transactions"));
    // Only subtransactions that captured rows are tracked.
    spool.append(1, 1, b"top level").unwrap();
    spool.append(1, 10, b"a").unwrap();
    spool.append(1, 11, b"b").unwrap();
    spool.append(1, 11, b"same savepoint").unwrap();
    let error = spool.append(1, 12, b"c").unwrap_err();
    assert!(
        matches!(error, Error::SpoolSubtransactions { xid: 1, limit: 2 }),
        "{error}"
    );
    assert!(error.to_string().contains("limits.spool_subtransactions"));
    let error = spool.append(2, 2, &[0; 64]).unwrap_err();
    assert!(
        matches!(error, Error::SpoolQuota { quota: 100, .. }),
        "{error}"
    );
    assert!(error.to_string().contains("limits.spool_bytes"));
    assert!(matches!(
        spool.append(2, 2, &[0; 65]).unwrap_err(),
        Error::Protocol(_)
    ));
    // The default leaves room for large savepoint-heavy transactions.
    assert!(SpoolConfig::default().max_subtransactions >= 1 << 20);

    let mut decoder = Decoder::new(1024);
    decoder.set_max_streamed_transactions(2);
    let start = |xid: u32| {
        message(b'S', |b| {
            b.put_u32(xid);
            b.put_u8(1);
        })
    };
    for xid in [1, 2] {
        decoder.decode(start(xid)).unwrap();
        decoder.decode(message(b'E', |_| {})).unwrap();
    }
    let Err(Error::Protocol(reason)) = decoder.decode(start(3)) else {
        panic!("third concurrent stream must be refused");
    };
    assert!(reason.contains("limits.spool_transactions"));
}

#[test]
fn spool_frames_carry_their_subtransaction_and_exclusion_skips_only_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(
        dir.path(),
        SpoolConfig {
            segment_bytes: 256,
            quota_bytes: 1 << 20,
            max_chunk_bytes: 16,
            max_transactions: 4,
            max_subtransactions: 8,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    spool.begin(7).unwrap();
    // Many tiny frames switching between the transaction and two
    // subtransactions, across many segments. Only the distinct
    // subtransactions are tracked in memory; each frame's owner is on disk.
    for index in 0..3000_u32 {
        let owner = [7, 8, 9][(index % 3) as usize];
        spool.append(7, owner, &index.to_le_bytes()).unwrap();
    }
    assert_eq!(spool.subtransactions(7).unwrap(), [8, 9]);
    let replayed = |spool: &TransactionSpool| {
        let mut kept = Vec::new();
        spool
            .replay(7, |bytes| {
                kept.push(u32::from_le_bytes(bytes.try_into().unwrap()));
                Ok(())
            })
            .unwrap();
        kept
    };
    // PostgreSQL's commit log reports 8 rolled back: skip only its frames.
    spool.exclude(7, &[8]).unwrap();
    assert_eq!(
        replayed(&spool),
        (0..3000).filter(|index| index % 3 != 1).collect::<Vec<_>>()
    );
    // A received rollback of 9 still truncates at its first frame.
    spool.abort(7, 9).unwrap();
    assert_eq!(spool.subtransactions(7).unwrap(), [8]);
    assert_eq!(replayed(&spool), [0]);
}

#[test]
fn a_damaged_spool_subtransaction_tag_fails_its_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(
        dir.path(),
        SpoolConfig {
            memory_transaction_bytes: 0,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    spool.begin(7).unwrap();
    spool.append(7, 8, b"rolled back").unwrap();
    spool.append(7, 7, b"kept").unwrap();
    // Retag the first frame as the transaction's own: excluding 8 must not
    // then silently keep it.
    let path = dir.path().join("txn-7/0");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&7_u32.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    spool.exclude(7, &[8]).unwrap();
    let error = spool.replay(7, |_| Ok(())).unwrap_err();
    assert!(
        matches!(&error, Error::Protocol(message) if message.contains("checksum")),
        "{error}"
    );
}

#[test]
fn stream_aborts_are_checked_against_this_connections_streams() {
    let start = |xid: u32, first: bool| {
        message(b'S', |b| {
            b.put_u32(xid);
            b.put_u8(u8::from(first));
        })
    };
    let abort = |xid: u32, subxid: u32| {
        message(b'A', |b| {
            b.put_u32(xid);
            b.put_u32(subxid);
        })
    };
    let mut decoder = Decoder::new(1024);
    // A rolled-back subtransaction of a transaction never streamed here has
    // nothing to discard, as in PostgreSQL's apply worker.
    assert_eq!(
        decoder.decode(abort(42, 43)).unwrap(),
        SourceEvent::Metadata
    );
    // A top-level abort of a stream this connection never started is not.
    let unknown = decoder.decode(abort(42, 42)).unwrap_err();
    assert!(
        matches!(&unknown, Error::StreamAbort(message) if message.contains("42") && message.contains("not started")),
        "{unknown}"
    );
    // Nor is an abort inside an open stream block.
    decoder.decode(start(42, true)).unwrap();
    let inside = decoder.decode(abort(42, 43)).unwrap_err();
    assert!(
        matches!(&inside, Error::StreamAbort(message) if message.contains("inside open streamed transaction 42")),
        "{inside}"
    );
    decoder.decode(message(b'E', |_| {})).unwrap();
    assert_eq!(
        decoder.decode(abort(42, 43)).unwrap(),
        SourceEvent::Abort {
            xid: 42,
            subxid: 43
        }
    );
    assert_eq!(
        decoder.decode(abort(42, 42)).unwrap(),
        SourceEvent::Abort {
            xid: 42,
            subxid: 42
        }
    );
}

fn replay_all(spool: &TransactionSpool, xid: u32) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    spool
        .replay(xid, |bytes| {
            frames.push(bytes.to_vec());
            Ok(())
        })
        .unwrap();
    frames
}

#[test]
fn small_transactions_stay_in_memory_and_never_touch_the_spool_directory() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(dir.path(), SpoolConfig::default()).unwrap();
    spool.begin(7).unwrap();
    spool.append(7, 7, b"first").unwrap();
    spool.append(7, 8, b"rolled back").unwrap();
    spool.append(7, 8, b"also rolled back").unwrap();
    assert!(spool.is_in_memory(7));
    assert!(!dir.path().join("txn-7").exists());
    // Memory is charged by allocation, which grows geometrically.
    let charged = spool.memory_bytes_used();
    assert!(charged >= spool.bytes_used() && charged < 2 * spool.bytes_used());
    assert_eq!(spool.savepoint(7, 8), Some(12 + 5));
    spool.abort(7, 8).unwrap();
    // A rollback returns the truncated allocation.
    assert!(spool.memory_bytes_used() < charged);
    spool.append(7, 9, b"excluded").unwrap();
    spool.append(7, 7, b"last").unwrap();
    assert_eq!(spool.subtransactions(7).unwrap(), [9]);
    spool.exclude(7, &[9]).unwrap();
    assert_eq!(replay_all(&spool, 7), [b"first".to_vec(), b"last".to_vec()]);
    assert_eq!(spool.bytes_used(), 3 * 12 + 5 + 8 + 4);
    assert!(spool.memory_bytes_used() >= spool.bytes_used());
    spool.discard(7).unwrap();
    assert_eq!((spool.bytes_used(), spool.memory_bytes_used()), (0, 0));
    assert!(!dir.path().join("txn-7").exists());
    // A whole-transaction rollback releases its memory too.
    spool.begin(8).unwrap();
    spool.append(8, 8, b"gone").unwrap();
    spool.abort(8, 8).unwrap();
    assert_eq!((spool.bytes_used(), spool.memory_bytes_used()), (0, 0));
}

#[test]
fn a_transaction_past_its_memory_bound_spills_with_identical_frames_and_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(
        dir.path(),
        SpoolConfig {
            segment_bytes: 256,
            max_chunk_bytes: 64,
            memory_transaction_bytes: 112,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    spool.begin(7).unwrap();
    // Four 28-byte frames fit 112 bytes; savepoint 8 starts in memory.
    for (index, subxid) in [7, 7, 8, 7].into_iter().enumerate() {
        spool.append(7, subxid, &[index as u8; 16]).unwrap();
        assert!(spool.is_in_memory(7));
    }
    assert_eq!(spool.memory_bytes_used(), 4 * 28);
    // Later frames go to disk, across several segments.
    for index in 4..40_u8 {
        spool
            .append(7, if index == 20 { 9 } else { 7 }, &[index; 16])
            .unwrap();
    }
    assert!(!spool.is_in_memory(7));
    assert_eq!(spool.memory_bytes_used(), 0);
    assert_eq!(spool.bytes_used(), 40 * 28);
    assert!(dir.path().join("txn-7/0").is_file());
    assert!(dir.path().join("txn-7/4").is_file());
    let expected: Vec<Vec<u8>> = (0..40_u8).map(|index| vec![index; 16]).collect();
    assert_eq!(replay_all(&spool, 7), expected);
    // Excluding the subtransaction that began in memory skips only its frame.
    spool.exclude(7, &[8]).unwrap();
    let mut kept = expected.clone();
    kept.remove(2);
    assert_eq!(replay_all(&spool, 7), kept);
    // A received rollback of a savepoint recorded before the spill truncates
    // the spilled first segment at its in-memory offset.
    spool.abort(7, 8).unwrap();
    assert_eq!(replay_all(&spool, 7), expected[..2].to_vec());
    assert_eq!(spool.bytes_used(), 2 * 28);
    assert!(!dir.path().join("txn-7/1").exists());
    spool.append(7, 7, b"after rollback").unwrap();
    assert!(!spool.is_in_memory(7));
    assert_eq!(replay_all(&spool, 7).last().unwrap(), b"after rollback");
    spool.discard(7).unwrap();
    assert!(!dir.path().join("txn-7").exists());
    assert_eq!(spool.bytes_used(), 0);
}

#[test]
fn the_global_memory_bound_spills_the_transaction_that_would_exceed_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(
        dir.path(),
        SpoolConfig {
            memory_bytes: 100,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    for xid in [1, 2, 3] {
        spool.begin(xid).unwrap();
    }
    spool.append(1, 1, &[1; 48]).unwrap();
    spool.append(2, 2, &[2; 30]).unwrap();
    assert_eq!(spool.memory_bytes_used(), 60);
    assert!(spool.is_in_memory(1) && !spool.is_in_memory(2));
    // Transaction 1 keeps its memory; others still fit beside it.
    spool.append(3, 3, &[3; 20]).unwrap();
    assert!(spool.is_in_memory(3));
    assert_eq!(spool.memory_bytes_used(), 60 + 32);
    spool.discard(1).unwrap();
    assert_eq!(spool.memory_bytes_used(), 32);
    // Once spilled, a transaction stays on disk even after memory frees up.
    spool.append(2, 2, &[4; 4]).unwrap();
    assert!(!spool.is_in_memory(2));
    assert_eq!(replay_all(&spool, 2), [vec![2; 30], vec![4; 4]]);
    assert_eq!(replay_all(&spool, 3), [vec![3; 20]]);
    assert_eq!(spool.bytes_used(), 42 + 16 + 32);
}

#[test]
fn a_failed_spill_is_an_error_and_releases_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = TransactionSpool::open(
        dir.path(),
        SpoolConfig {
            memory_transaction_bytes: 40,
            ..SpoolConfig::default()
        },
    )
    .unwrap();
    spool.begin(7).unwrap();
    spool.append(7, 7, b"in memory").unwrap();
    // A non-directory entry where the spill directory belongs.
    std::fs::write(dir.path().join("txn-7"), b"").unwrap();
    assert!(matches!(
        spool.append(7, 7, &[0; 32]).unwrap_err(),
        Error::Io(_)
    ));
    assert!(spool.is_in_memory(7));
    assert_eq!(spool.memory_bytes_used(), 21);
    assert_eq!(spool.bytes_used(), 21);
    assert_eq!(replay_all(&spool, 7), [b"in memory".to_vec()]);
}
