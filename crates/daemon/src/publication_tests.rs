//! The publication contract against a scripted PostgreSQL wire peer: scope,
//! one-snapshot reads, confirmation and identity. Live catalogs are covered by
//! the ignored tests in `source.rs`.
use super::*;
use bytes::{BufMut, BytesMut};
use flow_pg_source::tokio_postgres::{Config as PgConfig, NoTls, config::SslMode};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

/// Source catalog served by the scripted peer; tests edit it between checks.
struct Catalog {
    operations: [bool; 4],
    members: Vec<u32>,
    filtered: Vec<u32>,
    /// Drop the session at the membership read, inside the transaction.
    disconnect: bool,
    database: &'static str,
    /// Membership reads that still miss the configured table, as when a
    /// concurrent ALTER PUBLICATION skews one read.
    skewed: usize,
    /// Executed statements, in order.
    log: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
enum Query {
    Wal,
    Identity,
    Version,
    Publication,
    Members,
}

const BOOL: u32 = 16;
const NAME: u32 = 19;
const INT8: u32 = 20;
const INT4: u32 = 23;
const TEXT: u32 = 25;
const OID: u32 = 26;
const NAME_ARRAY: u32 = 1003;
const OID_ARRAY: u32 = 1028;
const SYSTEM: &str = "7000000000000000001";

impl Query {
    fn parse(sql: &str) -> Self {
        if sql.contains("pg_replication_slots") {
            Self::Wal
        } else if sql.contains("pg_control_system") {
            Self::Identity
        } else if sql.contains("server_version_num") {
            Self::Version
        } else if sql.contains("pubinsert") {
            Self::Publication
        } else if sql.contains("pg_publication_tables") && sql.contains("rowfilter") {
            Self::Members
        } else {
            panic!("unexpected source query: {sql}")
        }
    }

    /// Parameter and result types PostgreSQL infers for each statement.
    fn types(self) -> (&'static [u32], &'static [u32]) {
        match self {
            Self::Wal => (&[NAME], &[INT8, TEXT, INT8]),
            Self::Identity => (&[], &[TEXT, TEXT]),
            Self::Version => (&[], &[INT4]),
            Self::Publication => (&[NAME], &[BOOL; 5]),
            Self::Members => (
                &[NAME, OID_ARRAY, BOOL],
                &[OID, BOOL, BOOL, NAME_ARRAY, NAME_ARRAY, NAME_ARRAY],
            ),
        }
    }

    fn rows(self, catalog: &mut Catalog) -> Vec<Vec<Option<Vec<u8>>>> {
        let flag = |value: bool| Some(vec![u8::from(value)]);
        let names = |names: &[&str]| {
            let mut array = BytesMut::new();
            array.put_i32(i32::from(!names.is_empty()));
            array.put_i32(0);
            array.put_u32(NAME);
            if !names.is_empty() {
                array.put_i32(names.len() as i32);
                array.put_i32(1);
            }
            for name in names {
                array.put_i32(name.len() as i32);
                array.put_slice(name.as_bytes());
            }
            Some(array.to_vec())
        };
        match self {
            Self::Wal => vec![vec![
                Some(0i64.to_be_bytes().to_vec()),
                Some(b"reserved".to_vec()),
                None,
            ]],
            Self::Identity => vec![vec![
                Some(SYSTEM.as_bytes().to_vec()),
                Some(catalog.database.as_bytes().to_vec()),
            ]],
            Self::Version => vec![vec![Some(170000i32.to_be_bytes().to_vec())]],
            Self::Publication => vec![
                catalog
                    .operations
                    .iter()
                    .map(|published| flag(*published))
                    .chain([flag(false)])
                    .collect(),
            ],
            Self::Members => {
                let skewed = catalog.skewed > 0;
                catalog.skewed = catalog.skewed.saturating_sub(1);
                catalog
                    .members
                    .iter()
                    .filter(|id| !skewed || **id != 11)
                    .map(|id| {
                        vec![
                            Some(id.to_be_bytes().to_vec()),
                            flag(catalog.filtered.contains(id)),
                            flag(true),
                            names(&["id", "status"]),
                            names(&["id", "status"]),
                            names(&[]),
                        ]
                    })
                    .collect()
            }
        }
    }
}

async fn backend(socket: &mut TcpStream, tag: u8, payload: &[u8]) {
    socket.write_u8(tag).await.unwrap();
    socket.write_u32(payload.len() as u32 + 4).await.unwrap();
    socket.write_all(payload).await.unwrap();
}

/// Answer tokio-postgres's extended protocol: named Parse/Describe/Sync,
/// then Bind/Execute/Sync, then Close/Sync when the statement drops.
async fn serve(mut socket: TcpStream, catalog: Arc<Mutex<Catalog>>) {
    let length = socket.read_u32().await.unwrap();
    let mut startup = vec![0; (length - 4) as usize];
    socket.read_exact(&mut startup).await.unwrap();
    backend(&mut socket, b'R', &0u32.to_be_bytes()).await;
    backend(&mut socket, b'Z', b"I").await;
    let mut statements = HashMap::new();
    let mut portal = None;
    while let Ok(tag) = socket.read_u8().await {
        let length = socket.read_u32().await.unwrap();
        let mut payload = vec![0; (length - 4) as usize];
        socket.read_exact(&mut payload).await.unwrap();
        let mut fields = payload.split(|byte| *byte == 0);
        match tag {
            b'P' => {
                let name = fields.next().unwrap().to_vec();
                let query = Query::parse(std::str::from_utf8(fields.next().unwrap()).unwrap());
                if matches!(query, Query::Members) && catalog.lock().unwrap().disconnect {
                    return;
                }
                statements.insert(name, query);
                backend(&mut socket, b'1', &[]).await;
            }
            b'D' => {
                let (parameters, columns) = statements[&payload[1..payload.len() - 1]].types();
                let mut description = BytesMut::new();
                description.put_i16(parameters.len() as i16);
                parameters.iter().for_each(|oid| description.put_u32(*oid));
                backend(&mut socket, b't', &description).await;
                let mut description = BytesMut::new();
                description.put_i16(columns.len() as i16);
                for oid in columns {
                    description.put_slice(b"column\0");
                    description.put_u32(0);
                    description.put_i16(0);
                    description.put_u32(*oid);
                    description.put_i16(-1);
                    description.put_i32(-1);
                    description.put_i16(0);
                }
                backend(&mut socket, b'T', &description).await;
            }
            b'B' => {
                fields.next();
                let query = statements[fields.next().unwrap()];
                catalog.lock().unwrap().log.push(format!("{query:?}"));
                portal = Some(query);
                backend(&mut socket, b'2', &[]).await;
            }
            b'E' => {
                let rows = portal.take().unwrap().rows(&mut catalog.lock().unwrap());
                for row in &rows {
                    let mut data = BytesMut::new();
                    data.put_i16(row.len() as i16);
                    for value in row {
                        match value {
                            Some(value) => {
                                data.put_i32(value.len() as i32);
                                data.put_slice(value);
                            }
                            None => data.put_i32(-1),
                        }
                    }
                    backend(&mut socket, b'D', &data).await;
                }
                let complete = format!("SELECT {}\0", rows.len());
                backend(&mut socket, b'C', complete.as_bytes()).await;
            }
            b'C' => backend(&mut socket, b'3', &[]).await,
            b'Q' => {
                let sql = std::str::from_utf8(fields.next().unwrap())
                    .unwrap()
                    .to_owned();
                let tag = format!("{}\0", sql.split_whitespace().next().unwrap());
                catalog.lock().unwrap().log.push(sql);
                backend(&mut socket, b'C', tag.as_bytes()).await;
                backend(&mut socket, b'Z', b"I").await;
            }
            b'S' => backend(&mut socket, b'Z', b"I").await,
            b'X' => return,
            tag => panic!("unexpected frontend message {tag}"),
        }
    }
}

#[tokio::test]
async fn publication_check_scopes_violations_to_tables_and_reads_one_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // An admin-owned superset may filter tables other consumers use.
    let healthy = || Catalog {
        operations: [true; 4],
        members: vec![11, 99],
        filtered: vec![99],
        disconnect: false,
        database: "source",
        skewed: 0,
        log: Vec::new(),
    };
    let catalog = Arc::new(Mutex::new(healthy()));
    let served = catalog.clone();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(serve(socket, served.clone()));
        }
    });
    let root = tempfile::tempdir().unwrap();
    let mut config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config.state_dir = root.path().to_owned();
    std::fs::write(
        root.path().join("source-identity.json"),
        serde_json::json!({
            "source_id": config.source.id, "slot": "embrasure_flow",
            "system_identifier": SYSTEM, "database": "source", "timeline": 1,
        })
        .to_string(),
    )
    .unwrap();
    let schemas = [config.tables[0].schema(11)];
    let connect = move || async move {
        let mut source = PgConfig::new();
        source
            .host("127.0.0.1")
            .port(port)
            .user("flow")
            .dbname("source")
            .ssl_mode(SslMode::Disable);
        let (client, connection) = source.connect(NoTls).await.unwrap();
        (client, ConnectionTask::spawn(connection))
    };
    let reset = |change: fn(&mut Catalog)| {
        let mut catalog = catalog.lock().unwrap();
        *catalog = healthy();
        change(&mut catalog);
    };
    let log = || std::mem::take(&mut catalog.lock().unwrap().log);
    let snapshot = [
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL lock_timeout = '1s'",
        "Version",
        "Publication",
        "Members",
        "COMMIT",
    ];
    let (client, _connection) = connect().await;

    assert!(source_identity_matches(&client, &config).await.unwrap());
    log();
    assert_eq!(
        verify_publication(&client, &config, &schemas)
            .await
            .unwrap(),
        []
    );
    assert_eq!(log(), snapshot, "every catalog read shares one snapshot");

    // A concurrent ALTER PUBLICATION can skew one read; a fresh snapshot that
    // satisfies the contract means nothing is definite.
    reset(|catalog| catalog.skewed = 1);
    assert_eq!(
        verify_publication(&client, &config, &schemas)
            .await
            .unwrap(),
        []
    );
    assert_eq!(log(), [snapshot, snapshot].concat());

    // Table-scoped violations are confirmed, then returned for a block.
    for (change, reason) in [
        (
            (|catalog: &mut Catalog| catalog.members = vec![99]) as fn(&mut Catalog),
            "publication must include every configured source table; missing: public.orders",
        ),
        (
            |catalog| catalog.filtered = vec![11],
            "publication row filters are not supported by initial COPY: public.orders",
        ),
    ] {
        reset(change);
        assert_eq!(
            verify_publication(&client, &config, &schemas)
                .await
                .unwrap(),
            [TableViolation {
                table: TableId(11),
                reason: reason.into()
            }]
        );
        assert_eq!(log(), [snapshot, snapshot].concat());
    }

    // An unpublished operation affects every table and stays connection-wide.
    reset(|catalog| catalog.operations[2] = false);
    let error = verify_publication(&client, &config, &schemas)
        .await
        .unwrap_err();
    assert!(error.is::<PublicationChanged>() && !retryable_connection(&error));
    assert!(format!("{error:#}").ends_with("silently skipped; missing DELETE"));

    // A lost session inside the snapshot is transient, never a violation.
    reset(|catalog| catalog.disconnect = true);
    let error = verify_publication(&client, &config, &schemas)
        .await
        .unwrap_err();
    assert!(
        !error.is::<PublicationChanged>() && retryable_connection(&error),
        "{error:#}"
    );
    reset(|_| {});
    let (client, _connection) = connect().await;
    assert_eq!(
        verify_publication(&client, &config, &schemas)
            .await
            .unwrap(),
        []
    );

    // A session that reached another database is an identity error.
    reset(|catalog| catalog.database = "other");
    let error = source_identity_matches(&client, &config).await.unwrap_err();
    assert!(!error.is::<PublicationChanged>());
    assert!(error.to_string().contains("system or database changed"));
}
