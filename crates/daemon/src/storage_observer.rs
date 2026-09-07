//! Logical FileIO operations, not object-store HTTP requests or multipart parts.
//! Read bytes are returned bytes; write bytes were accepted by the writer and
//! become durable only after a successful close. Paths never become labels.
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::{
    Result,
    io::{
        FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
        StorageFactory,
    },
};
use serde::{Deserialize, Serialize};
use std::{future::Future, ops::Range, sync::Arc, time::Instant};

pub(crate) fn observe(factory: Arc<dyn StorageFactory>) -> Arc<dyn StorageFactory> {
    Arc::new(ObservedFactory { inner: factory })
}

#[derive(Debug, Serialize, Deserialize)]
struct ObservedFactory {
    inner: Arc<dyn StorageFactory>,
}
#[typetag::serde(name = "flow.observed-storage-factory.v1")]
impl StorageFactory for ObservedFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(ObservedStorage {
            inner: self.inner.build(config)?,
            metrics: Arc::default(),
        }))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ObservedStorage {
    inner: Arc<dyn Storage>,
    #[serde(skip)]
    metrics: Arc<StorageMetrics>,
}
impl std::fmt::Debug for ObservedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservedStorage").finish_non_exhaustive()
    }
}

struct StorageMetrics {
    exists: OperationMetrics,
    metadata: OperationMetrics,
    read: OperationMetrics,
    reader_open: OperationMetrics,
    read_range: OperationMetrics,
    write: OperationMetrics,
    writer_open: OperationMetrics,
    write_chunk: OperationMetrics,
    writer_close: OperationMetrics,
    delete: OperationMetrics,
    delete_prefix: OperationMetrics,
    delete_stream: OperationMetrics,
}
impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            exists: OperationMetrics::new("exists"),
            metadata: OperationMetrics::new("metadata"),
            read: OperationMetrics::new("read"),
            reader_open: OperationMetrics::new("reader_open"),
            read_range: OperationMetrics::new("read_range"),
            write: OperationMetrics::new("write"),
            writer_open: OperationMetrics::new("writer_open"),
            write_chunk: OperationMetrics::new("write_chunk"),
            writer_close: OperationMetrics::new("writer_close"),
            delete: OperationMetrics::new("delete"),
            delete_prefix: OperationMetrics::new("delete_prefix"),
            delete_stream: OperationMetrics::new("delete_stream"),
        }
    }
}
struct OperationMetrics {
    started: metrics::Counter,
    success: metrics::Counter,
    error: metrics::Counter,
    duration: metrics::Histogram,
    bytes: metrics::Counter,
}
impl OperationMetrics {
    fn new(operation: &'static str) -> Self {
        Self {
            started: metrics::counter!("flow_storage_operations_started_total", "operation" => operation),
            success: metrics::counter!("flow_storage_operations_total", "operation" => operation, "outcome" => "success"),
            error: metrics::counter!("flow_storage_operations_total", "operation" => operation, "outcome" => "error"),
            duration: metrics::histogram!("flow_storage_operation_seconds", "operation" => operation),
            bytes: metrics::counter!("flow_storage_bytes_total", "operation" => operation),
        }
    }
    async fn record<T>(
        &self,
        future: impl Future<Output = Result<T>>,
        bytes: impl FnOnce(&T) -> u64,
    ) -> Result<T> {
        self.started.increment(1);
        let start = Instant::now();
        let result = future.await;
        self.duration.record(start.elapsed().as_secs_f64());
        match &result {
            Ok(value) => {
                self.success.increment(1);
                self.bytes.increment(bytes(value));
            }
            Err(_) => self.error.increment(1),
        }
        result
    }
}

#[async_trait]
#[typetag::serde(name = "flow.observed-storage.v1")]
impl Storage for ObservedStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.metrics
            .exists
            .record(self.inner.exists(path), |_| 0)
            .await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.metrics
            .metadata
            .record(self.inner.metadata(path), |_| 0)
            .await
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.metrics
            .read
            .record(self.inner.read(path), |bytes| bytes.len() as u64)
            .await
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let inner = self
            .metrics
            .reader_open
            .record(self.inner.reader(path), |_| 0)
            .await?;
        Ok(Box::new(ObservedReader {
            inner,
            metrics: self.metrics.clone(),
        }))
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        let length = bytes.len() as u64;
        self.metrics
            .write
            .record(self.inner.write(path, bytes), |_| length)
            .await
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let inner = self
            .metrics
            .writer_open
            .record(self.inner.writer(path), |_| 0)
            .await?;
        Ok(Box::new(ObservedWriter {
            inner,
            metrics: self.metrics.clone(),
        }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.metrics
            .delete
            .record(self.inner.delete(path), |_| 0)
            .await
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.metrics
            .delete_prefix
            .record(self.inner.delete_prefix(path), |_| 0)
            .await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.metrics
            .delete_stream
            .record(self.inner.delete_stream(paths), |_| 0)
            .await
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        let file = self.inner.new_input(path)?;
        Ok(InputFile::new(
            Arc::new(self.clone()),
            file.location().to_owned(),
        ))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        let file = self.inner.new_output(path)?;
        Ok(OutputFile::new(
            Arc::new(self.clone()),
            file.location().to_owned(),
        ))
    }
}
struct ObservedReader {
    inner: Box<dyn FileRead>,
    metrics: Arc<StorageMetrics>,
}
#[async_trait]
impl FileRead for ObservedReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.metrics
            .read_range
            .record(self.inner.read(range), |bytes| bytes.len() as u64)
            .await
    }
}
struct ObservedWriter {
    inner: Box<dyn FileWrite>,
    metrics: Arc<StorageMetrics>,
}
#[async_trait]
impl FileWrite for ObservedWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        let length = bytes.len() as u64;
        self.metrics
            .write_chunk
            .record(self.inner.write(bytes), |_| length)
            .await
    }
    async fn close(&mut self) -> Result<()> {
        self.metrics
            .writer_close
            .record(self.inner.close(), |_| 0)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use iceberg::io::FileIOBuilder;
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
    use metrics_exporter_prometheus::PrometheusBuilder;
    #[tokio::test]
    async fn real_file_io_preserves_streams_errors_and_factory_serialization() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let temp = tempfile::TempDir::new().unwrap();
        let factory = observe(Arc::new(OpenDalResolvingStorageFactory::new()));
        let encoded = serde_json::to_vec(&factory).unwrap();
        let factory: Arc<dyn StorageFactory> = serde_json::from_slice(&encoded).unwrap();
        let io = FileIOBuilder::new(factory).build();
        let path = format!("file://{}", temp.path().join("secret-table-data").display());
        let output = io.new_output(&path).unwrap();
        output.write(Bytes::from_static(b"first")).await.unwrap();
        let input = output.to_input_file();
        assert!(input.exists().await.unwrap());
        assert_eq!(input.metadata().await.unwrap().size, 5);
        assert_eq!(input.read().await.unwrap(), b"first"[..]);
        let reader = input.reader().await.unwrap();
        assert_eq!(reader.read(1..4).await.unwrap(), b"irs"[..]);
        let mut writer = io.new_output(&path).unwrap().writer().await.unwrap();
        writer.write(Bytes::from_static(b"abc")).await.unwrap();
        writer.write(Bytes::from_static(b"defg")).await.unwrap();
        writer.close().await.unwrap();
        assert_eq!(
            io.new_input(&path).unwrap().read().await.unwrap(),
            b"abcdefg"[..]
        );
        let missing = format!("file://{}", temp.path().join("missing").display());
        assert!(io.new_input(missing).unwrap().read().await.is_err());
        io.delete_stream(futures::stream::iter(vec![path.clone()]).boxed())
            .await
            .unwrap();
        assert!(!io.exists(&path).await.unwrap());
        let text = handle.render();
        assert!(
            text.contains("flow_storage_operations_total{operation=\"read\",outcome=\"error\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("flow_storage_bytes_total{operation=\"read\"} 12"),
            "{text}"
        );
        assert!(
            text.contains("flow_storage_bytes_total{operation=\"read_range\"} 3"),
            "{text}"
        );
        assert!(
            text.contains("flow_storage_bytes_total{operation=\"write_chunk\"} 7"),
            "{text}"
        );
        assert!(
            text.contains(
                "flow_storage_operations_total{operation=\"delete_stream\",outcome=\"success\"} 1"
            ),
            "{text}"
        );
        assert!(!text.contains("secret-table-data"));
    }
}

#[cfg(test)]
mod rest_tests {
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
    use iceberg_catalog_rest::RestCatalogBuilder;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::{collections::HashMap, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn rest_metrics_observe_oauth_status_errors_and_transport_failures() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let header_end = loop {
                    let mut chunk = [0; 1024];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let line = headers.lines().next().unwrap().to_owned();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|length| length.parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                while request.len() < header_end + length {
                    let mut chunk = [0; 1024];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                let (status, body) = if line.starts_with("POST /v1/oauth/tokens ") {
                    (
                        "200 OK",
                        r#"{"access_token":"private-token","token_type":"bearer"}"#,
                    )
                } else if line.starts_with("GET /v1/config ") {
                    ("200 OK", r#"{"defaults":{},"overrides":{}}"#)
                } else if line.starts_with("GET /v1/namespaces ") {
                    ("200 OK", r#"{"namespaces":[]}"#)
                } else if line.starts_with("HEAD /v1/namespaces/private-name ") {
                    ("404 Not Found", "")
                } else {
                    panic!("unexpected request: {line}");
                };
                stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        let catalog = RestCatalogBuilder::default()
            .with_client(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(3))
                    .build()
                    .unwrap(),
            )
            .load(
                "diagnostics",
                HashMap::from([
                    ("uri".into(), format!("http://{address}")),
                    ("credential".into(), "private-client:private-secret".into()),
                ]),
            )
            .await
            .unwrap();
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());
        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new("private-name".into()))
                .await
                .unwrap()
        );
        server.await.unwrap();
        assert!(catalog.list_namespaces(None).await.is_err());
        let text = handle.render();
        assert!(
            text.contains("flow_catalog_http_requests_total{endpoint=\"oauth\",method=\"POST\"} 1"),
            "{text}"
        );
        assert!(
            text.contains(
                "flow_catalog_http_requests_total{endpoint=\"namespaces\",method=\"GET\"} 2"
            ),
            "{text}"
        );
        assert!(text.contains("flow_catalog_http_results_total{endpoint=\"namespace\",method=\"HEAD\",outcome=\"client_error\"} 1"), "{text}");
        assert!(text.contains("flow_catalog_http_results_total{endpoint=\"namespaces\",method=\"GET\",outcome=\"transport_error\"} 1"), "{text}");
        assert!(
            text.contains(
                "flow_catalog_response_body_bytes_total{endpoint=\"namespaces\",method=\"GET\"} 17"
            ),
            "{text}"
        );
        assert!(!text.contains("private-") && !text.contains(&address.to_string()));
    }
    #[tokio::test]
    async fn oauth_transient_status_survives_json_and_proxy_errors() {
        for (status, body, transient) in [
            (
                "429 Too Many Requests",
                r#"{"error":{"message":"busy","type":"Busy","code":429}}"#,
                true,
            ),
            ("503 Service Unavailable", "<html>unavailable</html>", true),
            (
                "400 Bad Request",
                r#"{"error":{"message":"invalid credential","type":"BadRequest","code":400}}"#,
                false,
            ),
            ("401 Unauthorized", "", false),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let read = stream.read(&mut request).await.unwrap();
                assert!(
                    String::from_utf8_lossy(&request[..read]).starts_with("POST /v1/oauth/tokens ")
                );
                stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            });
            let catalog = RestCatalogBuilder::default()
                .with_client(
                    reqwest::Client::builder()
                        .timeout(Duration::from_secs(3))
                        .build()
                        .unwrap(),
                )
                .load(
                    "test",
                    HashMap::from([
                        ("uri".into(), format!("http://{address}")),
                        ("credential".into(), "client:secret".into()),
                    ]),
                )
                .await
                .unwrap();
            let error = catalog.list_namespaces(None).await.unwrap_err();
            assert_eq!(
                crate::retry::transient(&anyhow::Error::new(error)),
                transient,
                "{status}"
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn rest_endpoint_labels_ignore_reserved_words_in_identifiers() {
        use iceberg::TableIdent;
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                let (status, body) = if request.starts_with("GET /v1/config ") {
                    (
                        "200 OK",
                        r#"{"defaults":{},"overrides":{"prefix":"warehouse"}}"#,
                    )
                } else {
                    assert!(
                        request.starts_with("HEAD /v1/warehouse/namespaces/"),
                        "{request}"
                    );
                    ("404 Not Found", "")
                };
                stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        let catalog = RestCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([("uri".into(), format!("http://{address}"))]),
            )
            .await
            .unwrap();
        for (namespace, table) in [
            ("db", "config"),
            ("db", "namespaces"),
            ("namespaces", "config"),
        ] {
            assert!(
                !catalog
                    .table_exists(&TableIdent::new(
                        NamespaceIdent::new(namespace.into()),
                        table.into()
                    ))
                    .await
                    .unwrap()
            );
        }
        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new("namespaces".into()))
                .await
                .unwrap()
        );
        server.await.unwrap();
        let text = handle.render();
        assert!(
            text.contains("flow_catalog_http_requests_total{endpoint=\"table\",method=\"HEAD\"} 3"),
            "{text}"
        );
        assert!(
            text.contains(
                "flow_catalog_http_requests_total{endpoint=\"namespace\",method=\"HEAD\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("flow_catalog_http_requests_total{endpoint=\"config\",method=\"GET\"} 1"),
            "{text}"
        );
    }
}
