//! S3 file I/O must share rotating credentials across table reloads.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::join_all;
use iceberg::io::{FileIOBuilder, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION};
use reqsign_core::time::Timestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalResolvingStorageFactory, ProvideCredential,
};

#[derive(Debug)]
struct Credentials {
    calls: Arc<AtomicUsize>,
    expire_first: bool,
}

impl ProvideCredential for Credentials {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<AwsCredential>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        // Exercise overlapping cold requests and refreshes.
        tokio::task::yield_now().await;
        Ok(Some(AwsCredential {
            access_key_id: format!("test-key-{call}"),
            secret_access_key: "test-secret".into(),
            session_token: Some("test-token".into()),
            expires_in: Some(
                Timestamp::now()
                    + Duration::from_secs(if self.expire_first && call == 0 {
                        30
                    } else {
                        3600
                    }),
            ),
        }))
    }
}

fn loader(calls: &Arc<AtomicUsize>, expire_first: bool) -> CustomAwsCredentialLoader {
    CustomAwsCredentialLoader::new(Credentials {
        calls: calls.clone(),
        expire_first,
    })
}

async fn endpoint() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0; 1024];
                while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                    let read = stream.read(&mut buf).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..read]);
                }
                assert!(String::from_utf8_lossy(&request).contains("Credential=test-key-"));
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
            });
        }
    });
    (format!("http://{address}"), server)
}

fn file_io(
    factory: Arc<OpenDalResolvingStorageFactory>,
    endpoint: &str,
    region: &str,
) -> iceberg::io::FileIO {
    FileIOBuilder::new(factory)
        .with_prop(S3_ENDPOINT, endpoint)
        .with_prop(S3_PATH_STYLE_ACCESS, "true")
        .with_prop(S3_REGION, region)
        .build()
}

#[tokio::test]
async fn many_table_loads_and_file_operations_share_one_credential_fetch() {
    let (endpoint, server) = endpoint().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(
        OpenDalResolvingStorageFactory::new().with_s3_credential_loader(loader(&calls, false)),
    );
    let operations = (0..512).map(|index| {
        // REST load/update returns a fresh FileIO for each table each time.
        let io = file_io(factory.clone(), &endpoint, "us-east-1");
        async move {
            let path = format!("s3://warehouse/table-{index}/metadata.json");
            assert!(io.exists(&path).await.unwrap());
            assert_eq!(
                io.new_input(&path).unwrap().metadata().await.unwrap().size,
                0
            );
        }
    });
    join_all(operations).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn refresh_is_shared_and_configs_buckets_and_factories_are_isolated() {
    let (endpoint, server) = endpoint().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(
        OpenDalResolvingStorageFactory::new().with_s3_credential_loader(loader(&calls, true)),
    );
    let io = file_io(factory.clone(), &endpoint, "us-east-1");
    assert!(io.exists("s3://warehouse/first").await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // First credentials are still usable but inside reqsign's refresh margin.
    join_all((0..32).map(|_| io.exists("s3a://warehouse/next")))
        .await
        .into_iter()
        .for_each(|result| assert!(result.unwrap()));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        file_io(factory.clone(), &endpoint, "us-west-2")
            .exists("s3://warehouse/other-region")
            .await
            .unwrap()
    );
    assert!(
        io.exists("s3://another-warehouse/other-bucket")
            .await
            .unwrap()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);

    let other_calls = Arc::new(AtomicUsize::new(0));
    let other_factory = Arc::new(
        factory
            .as_ref()
            .clone()
            .with_s3_credential_loader(loader(&other_calls, false)),
    );
    assert!(
        file_io(other_factory, &endpoint, "us-east-1")
            .exists("s3://warehouse/other-identity")
            .await
            .unwrap()
    );
    assert_eq!(other_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    server.abort();
}

#[tokio::test]
async fn rotating_configurations_evict_old_operators_after_the_cache_limit() {
    let (endpoint, server) = endpoint().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(
        OpenDalResolvingStorageFactory::new().with_s3_credential_loader(loader(&calls, false)),
    );
    let io_for_scope = |scope| {
        FileIOBuilder::new(factory.clone())
            .with_prop(S3_ENDPOINT, &endpoint)
            .with_prop(S3_PATH_STYLE_ACCESS, "true")
            .with_prop(S3_REGION, "us-east-1")
            .with_prop("s3.session-token", format!("vended-token-{scope}"))
            .build()
    };
    for scope in 0..129 {
        assert!(
            io_for_scope(scope)
                .exists("s3://warehouse/key")
                .await
                .unwrap()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 129);
    assert!(
        io_for_scope(128)
            .exists("s3://warehouse/key")
            .await
            .unwrap()
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        129,
        "most recent scope stays cached"
    );
    assert!(io_for_scope(0).exists("s3://warehouse/key").await.unwrap());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        130,
        "oldest scope was evicted"
    );
    server.abort();
}
