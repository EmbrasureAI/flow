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
