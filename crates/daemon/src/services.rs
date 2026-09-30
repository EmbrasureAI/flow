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
            "catalog credentials or custom headers are sent over plaintext HTTP to a non-loopback host; use https"
        );
    }
    let ca_file = properties.remove(CATALOG_CA_FILE);
    let https = CATALOG_AUTH_URLS.iter().any(|key| {
        properties
            .get(*key)
            .and_then(|url| url.get(..8))
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
    });
    if https && ca_file.is_none() {
        let native = rustls_native_certs::load_native_certs();
        if native.certs.is_empty() {
            // reqwest accepts an empty store; every HTTPS handshake would then fail.
            tracing::warn!(
                load_errors = native.errors.len(),
                "no trusted CA certificates found for the HTTPS catalog; install the system CA bundle, set SSL_CERT_FILE, or set catalog.tls_ca_file"
            );
        }
    }
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
        let pem = std::fs::read(path).map_err(|error| {
            anyhow::anyhow!(
                "cannot read catalog.{CATALOG_CA_FILE} {}: {}",
                path.display(),
                error.kind()
            )
        })?;
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

/// Catalog URL properties that would send a configured token, credential or
/// custom header (often an API key) in plaintext to another host. Loopback
/// endpoints, such as a local proxy, are exempt.
fn plaintext_credential_urls(properties: &HashMap<String, String>) -> Vec<&'static str> {
    if !properties.contains_key("token")
        && !properties.contains_key("credential")
        && !properties.keys().any(|key| key.starts_with("header."))
    {
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
    use iceberg::{TableCommit, TableRequirement};
    use std::sync::Mutex;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN: &str = "POST /v1/oauth/tokens ";
    const CONFIG: &str = "GET /v1/config ";
    const NAMESPACES: &str = "GET /v1/namespaces ";
    const COMMIT: &str = "POST /v1/namespaces/ns/tables/t ";

    /// A request received by the test catalog.
    struct Seen {
        line: String,
        authorization: Option<String>,
        body: String,
    }

    impl Seen {
        fn is(&self, request: &str) -> bool {
            self.line.starts_with(request)
        }
    }

    struct Reply {
        status: &'static str,
        body: String,
        delay: Duration,
    }

    fn reply(status: &'static str, body: impl Into<String>) -> Reply {
        Reply {
            status,
            body: body.into(),
            delay: Duration::ZERO,
        }
    }

    fn token(value: &str) -> Reply {
        reply(
            "200 OK",
            format!(r#"{{"access_token":"{value}","token_type":"bearer","expires_in":3600}}"#),
        )
    }

    fn catalog_reply(request: &Seen) -> Reply {
        if request.is(CONFIG) {
            reply("200 OK", r#"{"defaults":{},"overrides":{}}"#)
        } else if request.is(NAMESPACES) {
            reply("200 OK", r#"{"namespaces":[]}"#)
        } else {
            panic!("unexpected request: {}", request.line)
        }
    }

    fn count(log: &[Seen], request: &str) -> usize {
        log.iter().filter(|seen| seen.is(request)).count()
    }

    type Log = Arc<Mutex<Vec<Seen>>>;
    type Route = Arc<dyn Fn(&Seen, &[Seen]) -> Reply + Send + Sync>;

    /// Method, path and bearer header of every request so far.
    fn trace(log: &Log) -> Vec<String> {
        log.lock()
            .unwrap()
            .iter()
            .map(|seen| {
                let request = seen.line.split(' ').take(2).collect::<Vec<_>>().join(" ");
                format!("{request} {}", seen.authorization.as_deref().unwrap_or("-"))
            })
            .collect()
    }

    /// Serves each connection concurrently; `route` sees the request and all earlier ones.
    async fn serve(
        tls: Option<tokio_rustls::TlsAcceptor>,
        route: impl Fn(&Seen, &[Seen]) -> Reply + Send + Sync + 'static,
    ) -> (std::net::SocketAddr, Log) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let log = Log::default();
        let route: Route = Arc::new(route);
        let server_log = log.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (route, log, tls) = (route.clone(), server_log.clone(), tls.clone());
                tokio::spawn(async move {
                    match tls {
                        Some(tls) => {
                            // Untrusting clients abort the handshake.
                            if let Ok(stream) = tls.accept(stream).await {
                                respond(stream, &route, &log).await;
                            }
                        }
                        None => respond(stream, &route, &log).await,
                    }
                });
            }
        });
        (address, log)
    }

    async fn respond(mut stream: impl AsyncRead + AsyncWrite + Unpin, route: &Route, log: &Log) {
        let mut request = Vec::new();
        let seen = loop {
            let mut buffer = [0; 4096];
            let Ok(read) = stream.read(&mut buffer).await else {
                return;
            };
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
            let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..end]).into_owned();
            let header = |name: &str| {
                headers.lines().find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case(name)
                        .then(|| value.trim().to_owned())
                })
            };
            let length: usize = header("content-length").map_or(0, |value| value.parse().unwrap());
            if request.len() >= end + 4 + length {
                break Seen {
                    line: headers.lines().next().unwrap_or_default().to_owned(),
                    authorization: header("authorization"),
                    body: String::from_utf8_lossy(&request[end + 4..end + 4 + length]).into_owned(),
                };
            }
        };
        let reply = {
            let mut log = log.lock().unwrap();
            let reply = route(&seen, &log);
            log.push(seen);
            reply
        };
        tokio::time::sleep(reply.delay).await;
        let response = format!(
            "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            reply.status,
            reply.body.len(),
            reply.body
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    async fn oauth_catalog(address: std::net::SocketAddr) -> Arc<dyn Catalog> {
        rest_catalog(HashMap::from([
            ("uri".into(), format!("http://{address}")),
            ("credential".into(), "client:secret".into()),
        ]))
        .await
        .unwrap()
    }

    /// Moves the runtime clock forward, which the REST client's token expiry uses.
    async fn advance(duration: Duration) {
        tokio::time::pause();
        tokio::time::advance(duration).await;
        tokio::time::resume();
    }

    fn code(error: iceberg::Error) -> Option<&'static str> {
        crate::runtime::blocked::publication_error_code(&anyhow::Error::new(error))
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
        let (address, log) = serve(Some(acceptor), |request, _| catalog_reply(request)).await;
        let uri = format!("https://localhost:{}", address.port());
        let catalog = rest_catalog(HashMap::from([
            ("uri".into(), uri.clone()),
            (CATALOG_CA_FILE.into(), ca_file.display().to_string()),
        ]))
        .await
        .unwrap();
        assert!(catalog.list_namespaces(None).await.unwrap().is_empty());
        assert_eq!(count(&log.lock().unwrap(), NAMESPACES), 1);

        // Without the private CA, the handshake fails certificate verification.
        let catalog = rest_catalog(HashMap::from([("uri".into(), uri)]))
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
        assert_eq!(log.lock().unwrap().len(), 2);
    }

    #[test]
    fn catalog_ca_file_errors_name_the_file_but_omit_its_content() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing.pem");
        let error = catalog_http_client(Some(&missing)).unwrap_err().to_string();
        assert!(
            error.starts_with("cannot read catalog.tls_ca_file"),
            "{error}"
        );
        assert!(error.contains(&missing.display().to_string()), "{error}");
        assert!(error.contains("not found"), "{error}");
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
                ("uri", "http://catalog.internal:8181"),
                ("header.x-api-key", "key"),
            ]),
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
    async fn oauth_tokens_are_renewed_before_the_reported_expiry() {
        let (address, log) = serve(None, |request, prior| {
            if !request.is(TOKEN) {
                return catalog_reply(request);
            }
            token(["first", "second"][count(prior, TOKEN)])
        })
        .await;
        let catalog = oauth_catalog(address).await;
        catalog.list_namespaces(None).await.unwrap();
        advance(Duration::from_secs(3299)).await;
        catalog.list_namespaces(None).await.unwrap();
        // Five minutes before the one-hour expiry, the token is renewed.
        advance(Duration::from_secs(1)).await;
        catalog.list_namespaces(None).await.unwrap();
        assert_eq!(
            trace(&log),
            [
                "POST /v1/oauth/tokens -",
                "GET /v1/config Bearer first",
                "GET /v1/namespaces Bearer first",
                "GET /v1/namespaces Bearer first",
                "POST /v1/oauth/tokens -",
                "GET /v1/namespaces Bearer second",
            ]
        );
    }

    #[tokio::test]
    async fn failed_renewal_neither_blocks_requests_nor_repeats_immediately() {
        let (address, log) = serve(None, |request, prior| {
            if !request.is(TOKEN) {
                return catalog_reply(request);
            }
            match count(prior, TOKEN) {
                0 => token("first"),
                // A slow, failing token endpoint during the early renewal.
                1 => Reply {
                    delay: Duration::from_secs(2),
                    ..reply("503 Service Unavailable", "")
                },
                _ => token("second"),
            }
        })
        .await;
        let catalog = oauth_catalog(address).await;
        catalog.list_namespaces(None).await.unwrap();
        advance(Duration::from_secs(3300)).await;

        let started = std::time::Instant::now();
        let results =
            futures::future::join_all((0..5).map(|_| catalog.list_namespaces(None))).await;
        assert!(results.iter().all(Result::is_ok));
        // Only the renewing request waits for the token endpoint.
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "{:?}",
            started.elapsed()
        );
        // Within the backoff, requests use the unexpired token without renewing.
        catalog.list_namespaces(None).await.unwrap();
        assert_eq!(count(&log.lock().unwrap(), TOKEN), 2);
        assert_eq!(
            trace(&log)
                .iter()
                .filter(|request| request.as_str() == "GET /v1/namespaces Bearer first")
                .count(),
            7
        );

        // The backoff is at most 30 seconds.
        advance(Duration::from_secs(31)).await;
        catalog.list_namespaces(None).await.unwrap();
        assert_eq!(
            trace(&log)[10..],
            [
                "POST /v1/oauth/tokens -",
                "GET /v1/namespaces Bearer second",
            ]
        );
    }

    #[tokio::test]
    async fn rejected_oauth_tokens_are_replaced_once() {
        for accept_second in [true, false] {
            let (address, log) = serve(None, move |request, prior| {
                if request.is(TOKEN) {
                    return token(["first", "second"][count(prior, TOKEN)]);
                }
                let rejected =
                    request.authorization.as_deref() == Some("Bearer first") || !accept_second;
                if request.is(NAMESPACES) && rejected {
                    return reply("401 Unauthorized", "");
                }
                catalog_reply(request)
            })
            .await;
            let catalog = oauth_catalog(address).await;
            let result = catalog.list_namespaces(None).await;
            if accept_second {
                assert!(result.unwrap().is_empty());
            } else {
                assert_eq!(code(result.unwrap_err()), Some("catalog_auth"));
            }
            assert_eq!(
                trace(&log),
                [
                    "POST /v1/oauth/tokens -",
                    "GET /v1/config Bearer first",
                    "GET /v1/namespaces Bearer first",
                    "POST /v1/oauth/tokens -",
                    "GET /v1/namespaces Bearer second",
                ]
            );
        }
    }

    #[tokio::test]
    async fn static_token_rejections_are_catalog_auth_and_not_retried() {
        for (status, expected) in [
            ("401 Unauthorized", "catalog_auth"),
            ("403 Forbidden", "catalog_auth"),
            ("503 Service Unavailable", "catalog_unavailable"),
        ] {
            let (address, log) = serve(None, move |request, _| {
                if request.is(NAMESPACES) {
                    return reply(status, "");
                }
                catalog_reply(request)
            })
            .await;
            let catalog = rest_catalog(HashMap::from([
                ("uri".into(), format!("http://{address}")),
                ("token".into(), "static-token".into()),
            ]))
            .await
            .unwrap();
            let error = catalog.list_namespaces(None).await.unwrap_err();
            assert_eq!(code(error), Some(expected), "{status}");
            assert_eq!(
                trace(&log),
                [
                    "GET /v1/config Bearer static-token",
                    "GET /v1/namespaces Bearer static-token",
                ]
            );
        }

        // Revoked client credentials are an authentication failure too.
        let (address, log) = serve(None, |_, _| reply("401 Unauthorized", "")).await;
        let catalog = oauth_catalog(address).await;
        let error = catalog.list_namespaces(None).await.unwrap_err();
        assert_eq!(code(error), Some("catalog_auth"));
        assert_eq!(trace(&log), ["POST /v1/oauth/tokens -"]);
    }

    /// A commit rejected after it was applied (for example by a proxy) is resent with
    /// the same base assertion, so the catalog answers 409 instead of applying it
    /// twice. Publication recovery then finds the applied commit by its marker.
    #[tokio::test]
    async fn rejected_commit_is_resent_with_its_original_base_assertion() {
        let (address, log) = serve(None, |request, prior| {
            if request.is(TOKEN) {
                return token(["first", "second"][count(prior, TOKEN)]);
            }
            if request.is(COMMIT) {
                return match request.authorization.as_deref() {
                    Some("Bearer first") => reply("401 Unauthorized", ""),
                    _ => reply("409 Conflict", ""),
                };
            }
            catalog_reply(request)
        })
        .await;
        let catalog = oauth_catalog(address).await;
        let commit = TableCommit::builder()
            .ident(TableIdent::from_strs(["ns", "t"]).unwrap())
            .requirements(vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: Some(7),
            }])
            .updates(Vec::new())
            .build();
        let error = catalog.update_table(commit).await.unwrap_err();
        assert_eq!(code(error), Some("catalog_conflict"));
        let log = log.lock().unwrap();
        let commits: Vec<_> = log.iter().filter(|seen| seen.is(COMMIT)).collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].body, commits[1].body);
        assert!(
            commits[0].body.contains(r#""snapshot-id":7"#),
            "{}",
            commits[0].body
        );
    }
}
