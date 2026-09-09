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
