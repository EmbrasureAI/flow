//! Shared service construction and durable source-ledger adapters.
use crate::config::Config;
use anyhow::{Context, Result, ensure};
use flow_coordinator::SourceLedger;
use flow_ingress_journal::JournalConfig;
use flow_model::SourceId;
use flow_state_store::{ControlStore, StateStore};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use std::{collections::HashMap, net::IpAddr, path::Path, sync::Arc, time::Duration};

/// Flow-owned catalog property naming a PEM bundle of additional trusted CAs.
/// It configures the HTTP client and is not forwarded to the catalog.
const CATALOG_CA_FILE: &str = "tls_ca_file";
/// Catalog properties whose URLs receive the bearer token or OAuth credential.
const CATALOG_AUTH_URLS: [&str; 2] = ["uri", "oauth2-server-uri"];

pub(crate) fn state(config: &Config) -> Result<StateStore> {
    std::fs::create_dir_all(&config.state_dir)?;
    crate::generation::open(
        config,
        ControlStore::open(config.state_dir.join("control"))?,
    )
}
pub(crate) fn journal_config(config: &Config) -> JournalConfig {
    JournalConfig {
        quota_bytes: config.limits.journal_bytes,
        max_frame_bytes: config.limits.chunk_bytes,
        ..Default::default()
    }
}
pub(crate) fn writer_config(config: &Config) -> flow_materializer::WriterConfig {
    flow_materializer::WriterConfig {
        batch_bytes: config.limits.batch_bytes,
        row_group_bytes: config.limits.parquet_row_group_bytes,
        ..Default::default()
    }
}
/// Test-only seam: in-process tests run `init` and `run` against an in-memory
/// catalog registered under the configured catalog `uri`.
#[cfg(test)]
pub(crate) static TEST_CATALOGS: std::sync::Mutex<
    std::collections::BTreeMap<String, Arc<dyn Catalog>>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

pub(crate) async fn catalog(config: &Config) -> Result<Arc<dyn Catalog>> {
    #[cfg(test)]
    if let Some(catalog) = config
        .catalog
        .get("uri")
        .and_then(|uri| TEST_CATALOGS.lock().unwrap().get(uri).cloned())
    {
        return Ok(catalog);
    }
    rest_catalog(config.catalog_properties()?).await
}

async fn rest_catalog(mut properties: HashMap<String, String>) -> Result<Arc<dyn Catalog>> {
    for property in plaintext_credential_urls(&properties) {
        tracing::warn!(
            property = %format!("catalog.{property}"),
            "catalog credentials are sent over plaintext HTTP to a non-loopback host; use https"
        );
    }
    let ca_file = properties.remove(CATALOG_CA_FILE);
    Ok(Arc::new(
        RestCatalogBuilder::default()
            .with_client(catalog_http_client(ca_file.as_deref().map(Path::new))?)
            .with_storage_factory(crate::storage_observer::observe(Arc::new(
                OpenDalResolvingStorageFactory::new(),
            )))
            .load("destination", properties)
            .await?,
    ))
}

/// HTTPS uses rustls with the platform trust store, which `SSL_CERT_FILE` and
/// `SSL_CERT_DIR` replace when set, plus any CAs in `catalog.tls_ca_file`.
fn catalog_http_client(ca_file: Option<&Path>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(60));
    if let Some(path) = ca_file {
        // Parse errors may quote file content; report only the property.
        let pem = std::fs::read(path)
            .map_err(|_| anyhow::anyhow!("cannot read catalog.{CATALOG_CA_FILE}"))?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|_| {
            anyhow::anyhow!("catalog.{CATALOG_CA_FILE} must contain PEM certificates")
        })?;
        ensure!(
            !certificates.is_empty(),
            "catalog.{CATALOG_CA_FILE} contains no PEM certificates"
        );
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    builder
        .build()
        .context("cannot configure the catalog HTTP client")
}

/// Catalog URL properties that would send a configured token or credential in
/// plaintext to another host. Loopback endpoints, such as a local proxy, are exempt.
fn plaintext_credential_urls(properties: &HashMap<String, String>) -> Vec<&'static str> {
    if !properties.contains_key("token") && !properties.contains_key("credential") {
        return Vec::new();
    }
    CATALOG_AUTH_URLS
        .into_iter()
        .filter(|key| {
            let Some(url) = properties
                .get(*key)
                .and_then(|url| reqwest::Url::parse(url).ok())
            else {
                return false;
            };
            let host = url.host_str().unwrap_or_default();
            let loopback = host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            url.scheme() == "http" && !loopback
        })
        .collect()
}
pub(crate) fn ledger(store: &StateStore, config: &Config) -> Result<SourceLedger> {
    SourceLedger::open(
        store.clone(),
        SourceId(config.source.id.clone()),
        config.source.ack_mode,
        config.source.journal_durability,
    )
}
pub(crate) fn target(namespace: &[String], name: &str) -> Result<TableIdent> {
    Ok(TableIdent::new(
        NamespaceIdent::from_vec(namespace.to_vec())?,
        name.to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::TcpListener,
    };

    /// One scripted request and its response. Every response closes the connection.
    struct Exchange {
        request: &'static str,
        authorization: Option<&'static str>,
        status: &'static str,
        body: &'static str,
    }

    const CONFIG: Exchange = Exchange {
        request: "GET /v1/config ",
        authorization: None,
        status: "200 OK",
        body: r#"{"defaults":{},"overrides":{}}"#,
    };
    const NAMESPACES: Exchange = Exchange {
        request: "GET /v1/namespaces ",
        authorization: None,
        status: "200 OK",
        body: r#"{"namespaces":[]}"#,
    };

    fn with(exchange: Exchange, authorization: &'static str, status: &'static str) -> Exchange {
        Exchange {
            authorization: Some(authorization),
            status,
            ..exchange
        }
    }

    fn token(access_token: &'static str) -> Exchange {
        Exchange {
            request: "POST /v1/oauth/tokens ",
            authorization: None,
            status: "200 OK",
            body: access_token,
        }
    }

    async fn respond(mut stream: impl AsyncRead + AsyncWrite + Unpin, exchange: &Exchange) {
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 4096];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0 && request.len() < 16384);
            request.extend_from_slice(&buffer[..read]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]).into_owned();
                let header = |name: &str| {
                    headers.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case(name)
                            .then(|| value.trim().to_owned())
                    })
                };
                let length = header("content-length").map_or(0, |value| value.parse().unwrap());
                if request.len() < end + 4 + length {
                    continue;
                }
                assert!(headers.starts_with(exchange.request), "{headers}");
                if let Some(expected) = exchange.authorization {
                    assert_eq!(header("authorization").as_deref(), Some(expected));
                }
                break;
            }
        }
        let body = if exchange.request.starts_with("POST /v1/oauth/tokens ") {
            format!(
                r#"{{"access_token":"{}","token_type":"bearer","expires_in":3600}}"#,
                exchange.body
            )
        } else {
            exchange.body.to_owned()
        };
        let response = format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            exchange.status,
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    async fn serve(
        exchanges: Vec<Exchange>,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for exchange in &exchanges {
                let (stream, _) = listener.accept().await.unwrap();
                match &tls {
                    Some(tls) => respond(tls.accept(stream).await.unwrap(), exchange).await,
                    None => respond(stream, exchange).await,
                }
            }
        });
        (address, server)
    }

    /// A short-lived CA and a `localhost` server certificate it signed.
    fn certificates() -> (String, tokio_rustls::TlsAcceptor) {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
            KeyPair, KeyUsagePurpose,
        };
        let now = time::OffsetDateTime::now_utc();
        let validity = |mut params: CertificateParams| {
            // Short validity keeps platform verifiers' certificate policies satisfied.
            params.not_before = now - time::Duration::days(1);
            params.not_after = now + time::Duration::days(30);
            params
        };
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = validity(CertificateParams::new(Vec::<String>::new()).unwrap());
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.distinguished_name
            .push(DnType::CommonName, "Flow catalog test CA");
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_certificate = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let key = KeyPair::generate().unwrap();
        let mut leaf = validity(
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()]).unwrap(),
        );
        leaf.distinguished_name
            .push(DnType::CommonName, "localhost");
        leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let leaf = leaf.signed_by(&key, &issuer).unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(ca_certificate.der());
        let lines = encoded
            .as_bytes()
            .chunks(64)
            .map(|line| std::str::from_utf8(line).unwrap());
        let pem = std::iter::once("-----BEGIN CERTIFICATE-----")
            .chain(lines)
            .chain(["-----END CERTIFICATE-----", ""])
            .collect::<Vec<_>>()
            .join("\n");
        (pem, tokio_rustls::TlsAcceptor::from(Arc::new(server)))
    }

    #[tokio::test]
    async fn https_catalog_verifies_the_configured_ca() {
        let (ca, acceptor) = certificates();
        let root = tempfile::tempdir().unwrap();
        let ca_file = root.path().join("catalog-ca.pem");
        std::fs::write(&ca_file, ca).unwrap();
        let (address, server) = serve(vec![CONFIG, NAMESPACES], Some(acceptor.clone())).await;
        let catalog = rest_catalog(HashMap::from([
            (
                "uri".into(),
                format!("https://localhost:{}", address.port()),
            ),
            (CATALOG_CA_FILE.into(), ca_file.display().to_string()),
        ]))
        .await
        .unwrap();
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());
        server.await.unwrap();

        // Without the private CA, the handshake fails certificate verification.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            assert!(acceptor.accept(stream).await.is_err());
        });
        let catalog = rest_catalog(HashMap::from([(
            "uri".into(),
            format!("https://localhost:{}", address.port()),
        )]))
        .await
        .unwrap();
        let error = anyhow::Error::new(catalog.list_namespaces(None).await.unwrap_err());
        let diagnostics = format!("{error:?}");
        assert!(
            error.chain().any(|cause| cause
                .downcast_ref::<reqwest::Error>()
                .is_some_and(reqwest::Error::is_connect)),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("certificate"), "{diagnostics}");
        assert!(!diagnostics.contains("scheme"), "{diagnostics}");
        server.await.unwrap();
    }

    #[test]
    fn catalog_ca_file_errors_omit_file_content() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing.pem");
        let error = catalog_http_client(Some(&missing)).unwrap_err();
        assert_eq!(error.to_string(), "cannot read catalog.tls_ca_file");
        let empty = root.path().join("empty.pem");
        std::fs::write(&empty, "FAKE_SECRET_CONTENT").unwrap();
        let error = catalog_http_client(Some(&empty)).unwrap_err();
        assert!(!format!("{error:?}").contains("FAKE_SECRET"));
        assert!(error.to_string().contains("catalog.tls_ca_file"), "{error}");
    }

    #[test]
    fn plaintext_credentials_are_reported_only_for_remote_hosts() {
        let urls = |pairs: &[(&str, &str)]| {
            plaintext_credential_urls(
                &pairs
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            )
        };
        assert_eq!(
            urls(&[("uri", "http://catalog.internal:8181"), ("token", "t")]),
            ["uri"]
        );
        assert_eq!(
            urls(&[
                ("uri", "https://catalog.example.com"),
                ("oauth2-server-uri", "http://10.0.0.5/token"),
                ("credential", "id:secret"),
            ]),
            ["oauth2-server-uri"]
        );
        assert!(urls(&[("uri", "http://catalog.internal:8181")]).is_empty());
        for uri in [
            "http://localhost:8181",
            "http://LOCALHOST:8181",
            "http://127.0.0.1:8181",
            "http://[::1]:8181",
            "https://catalog.example.com",
        ] {
            assert!(urls(&[("uri", uri), ("token", "t")]).is_empty(), "{uri}");
        }
    }

    #[tokio::test]
    async fn rejected_oauth_tokens_are_replaced_and_static_rejections_are_catalog_auth() {
        let (address, server) = serve(
            vec![
                token("first-token"),
                with(CONFIG, "Bearer first-token", "200 OK"),
                with(NAMESPACES, "Bearer first-token", "401 Unauthorized"),
                token("second-token"),
                with(NAMESPACES, "Bearer second-token", "200 OK"),
            ],
            None,
        )
        .await;
        let catalog = rest_catalog(HashMap::from([
            ("uri".into(), format!("http://{address}")),
            ("credential".into(), "client:secret".into()),
        ]))
        .await
        .unwrap();
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());
        server.await.unwrap();

        for (status, code) in [
            ("401 Unauthorized", "catalog_auth"),
            ("403 Forbidden", "catalog_auth"),
            ("503 Service Unavailable", "catalog_unavailable"),
        ] {
            let (address, server) = serve(
                vec![
                    with(CONFIG, "Bearer static-token", "200 OK"),
                    with(NAMESPACES, "Bearer static-token", status),
                ],
                None,
            )
            .await;
            let catalog = rest_catalog(HashMap::from([
                ("uri".into(), format!("http://{address}")),
                ("token".into(), "static-token".into()),
            ]))
            .await
            .unwrap();
            let error = anyhow::Error::new(catalog.list_namespaces(None).await.unwrap_err());
            assert_eq!(
                crate::runtime::blocked::publication_error_code(&error),
                Some(code),
                "{status}"
            );
            server.await.unwrap();
        }

        // Revoked client credentials are an authentication failure too.
        let (address, server) = serve(
            vec![Exchange {
                status: "401 Unauthorized",
                body: "",
                ..token("unused")
            }],
            None,
        )
        .await;
        let catalog = rest_catalog(HashMap::from([
            ("uri".into(), format!("http://{address}")),
            ("credential".into(), "client:revoked".into()),
        ]))
        .await
        .unwrap();
        let error = anyhow::Error::new(catalog.list_namespaces(None).await.unwrap_err());
        assert_eq!(
            crate::runtime::blocked::publication_error_code(&error),
            Some("catalog_auth")
        );
        server.await.unwrap();
    }
}
