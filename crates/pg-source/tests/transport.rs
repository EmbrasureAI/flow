//! Exercise the real pinned PostgreSQL transport against a tiny wire peer.
//! This is a protocol fixture, not a live PostgreSQL or catalog dependency.
use bytes::{BufMut, BytesMut};
use flow_model::PgLsn;
use flow_pg_source::{
    Acknowledgement, PgOutputSource, PostgresSource, SourceEvent,
    tokio_postgres::{
        Config, NoTls,
        config::{ReplicationMode, SslMode},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn backend(socket: &mut TcpStream, tag: u8, payload: &[u8]) {
    socket.write_u8(tag).await.unwrap();
    socket.write_u32(payload.len() as u32 + 4).await.unwrap();
    socket.write_all(payload).await.unwrap();
}
async fn frontend(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = socket.read_u8().await.unwrap();
    let len = socket.read_u32().await.unwrap();
    assert!((4..1 << 20).contains(&len));
    let mut payload = vec![0; (len - 4) as usize];
    socket.read_exact(&mut payload).await.unwrap();
    (tag, payload)
}

#[tokio::test]
async fn keepalive_cannot_ack_requested_replay_position_and_disconnect_reconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let length = socket.read_u32().await.unwrap();
            let mut startup = vec![0; (length - 4) as usize];
            socket.read_exact(&mut startup).await.unwrap();
            backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
            backend(&mut socket, b'S', b"client_encoding\0UTF8\0").await;
            backend(&mut socket, b'S', b"server_version\x0018.0\0").await;
            backend(&mut socket, b'Z', b"I").await;
            let (tag, query) = frontend(&mut socket).await;
            assert_eq!(tag, b'Q');
            assert!(query.starts_with(b"SET DateStyle"));
            backend(&mut socket, b'C', b"SET\0").await;
            backend(&mut socket, b'Z', b"I").await;
            let (tag, query) = frontend(&mut socket).await;
            assert_eq!(tag, b'Q');
            let query = std::str::from_utf8(&query).unwrap();
            assert!(query.contains("proto_version '2'"));
            assert!(query.contains("streaming 'on'"));
            assert!(query.contains("LOGICAL 0/64"));
            backend(&mut socket, b'W', &[0, 0, 0]).await;

            let mut keepalive = BytesMut::new();
            keepalive.put_u8(b'k');
            keepalive.put_u64(999_999);
            keepalive.put_i64(0);
            keepalive.put_u8(1);
            backend(&mut socket, b'd', &keepalive).await;
            let (tag, feedback) = frontend(&mut socket).await;
            assert_eq!(tag, b'd');
            assert_eq!(feedback[0], b'r');
            assert_eq!(
                &feedback[1..25],
                &[0; 24],
                "requested start LSN is not an ACK proof"
            );

            let mut wal = BytesMut::new();
            wal.put_u8(b'w');
            wal.put_u64(100);
            wal.put_u64(999_999);
            wal.put_i64(0);
            wal.put_u8(b'B');
            wal.put_u64(200);
            wal.put_i64(0);
            wal.put_u32(42);
            backend(&mut socket, b'd', &wal).await;
            let (tag, feedback) = frontend(&mut socket).await;
            assert_eq!(tag, b'd');
            assert_eq!(u64::from_be_bytes(feedback[9..17].try_into().unwrap()), 99);
            assert_eq!(u64::from_be_bytes(feedback[17..25].try_into().unwrap()), 98);
            // Simulate a network loss in the middle of an uncommitted source
            // transaction. The next connection must explicitly resume again.
        }
    });
    for _ in 0..2 {
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(port)
            .user("replication")
            .dbname("source")
            .replication_mode(ReplicationMode::Logical)
            .ssl_mode(SslMode::Disable);
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let driver = tokio::spawn(connection);
        let mut source = PgOutputSource::start(&client, "slot", "publication", PgLsn(100), 1024)
            .await
            .unwrap();
        assert!(matches!(
            source.next().await.unwrap(),
            Some(SourceEvent::Begin { xid: 42, .. })
        ));
        assert_eq!(source.received_lsn, PgLsn(100));
        source
            .acknowledge(Acknowledgement {
                received: PgLsn(100),
                durable: PgLsn(99),
                materialized: PgLsn(98),
            })
            .await
            .unwrap();
        assert!(matches!(source.next().await, Ok(None) | Err(_)));
        let _ = driver.await;
    }
    peer.await.unwrap();
}

// Send only an oversized advertised length. A post-delivery check would hang
// waiting for the missing body; admission in the transport must fail promptly.
async fn oversized_advertised_frame(replication: bool) {
    use futures::StreamExt;
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (admit, admitted) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        let mut startup = vec![0; (length - 4) as usize];
        socket.read_exact(&mut startup).await.unwrap();
        backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
        backend(&mut socket, b'S', b"client_encoding\0UTF8\0").await;
        backend(&mut socket, b'S', b"server_version\x0018.0\0").await;
        backend(&mut socket, b'Z', b"I").await;
        let (tag, _) = frontend(&mut socket).await;
        assert_eq!(tag, b'Q');
        if replication {
            backend(&mut socket, b'C', b"SET\0").await;
            backend(&mut socket, b'Z', b"I").await;
            let (tag, _) = frontend(&mut socket).await;
            assert_eq!(tag, b'Q');
        }
        backend(
            &mut socket,
            if replication { b'W' } else { b'H' },
            &[0, 0, 0],
        )
        .await;
        let mut payload = BytesMut::new();
        if replication {
            payload.put_u8(b'w');
            payload.put_u64(100);
            payload.put_u64(100);
            payload.put_i64(0);
            payload.put_u8(b'B');
            payload.put_u64(100);
            payload.put_i64(0);
            payload.put_u32(42);
        } else {
            payload.resize(46, b'x');
        }
        assert_eq!(payload.len(), 46);
        backend(&mut socket, b'd', &payload).await;
        admitted.await.unwrap();
        socket.write_u8(b'd').await.unwrap();
        socket.write_u32(1 << 30).await.unwrap();
        // Keep the peer open until the client rejects the header and closes it.
        let mut tail = Vec::new();
        socket.read_to_end(&mut tail).await.unwrap();
    });
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("source")
        .dbname("source")
        .ssl_mode(SslMode::Disable)
        .max_backend_message_bytes(46);
    if replication {
        config.replication_mode(ReplicationMode::Logical);
    }
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    if replication {
        let mut source = PgOutputSource::start(&client, "slot", "publication", PgLsn(100), 21)
            .await
            .unwrap();
        assert!(matches!(
            source.next().await.unwrap(),
            Some(SourceEvent::Begin { xid: 42, .. })
        ));
        admit.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), source.next())
            .await
            .unwrap()
            .unwrap_err();
        let flow_pg_source::Error::Postgres(error) = error else {
            panic!("admission error must preserve transport InvalidData");
        };
        assert_admission_error(&error);
        let error = source
            .acknowledge(Acknowledgement::default())
            .await
            .unwrap_err();
        let flow_pg_source::Error::Postgres(error) = error else {
            panic!("feedback must preserve transport InvalidData too");
        };
        assert_admission_error(&error);
    } else {
        let stream = client
            .copy_out_simple("COPY fixture TO STDOUT")
            .await
            .unwrap();
        futures::pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap().len(), 46);
        admit.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_admission_error(&error);
    }
    let error = tokio::time::timeout(Duration::from_secs(2), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_admission_error(&error);
    peer.await.unwrap();
}

fn assert_admission_error(error: &flow_pg_source::tokio_postgres::Error) {
    let cause = std::error::Error::source(error).unwrap();
    let io = cause.downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(io.kind(), std::io::ErrorKind::InvalidData);
    assert!(io.to_string().contains("exceeds configured byte limit"));
}

#[tokio::test]
async fn copy_rejects_oversized_header_without_reading_body() {
    oversized_advertised_frame(false).await;
}

#[tokio::test]
async fn replication_rejects_oversized_header_without_reading_body() {
    oversized_advertised_frame(true).await;
}
