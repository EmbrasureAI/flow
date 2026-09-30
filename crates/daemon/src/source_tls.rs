//! Apply PostgreSQL connection TLS policy to the native TLS transport.
use anyhow::{Context, Result, bail, ensure};
use flow_pg_source::tokio_postgres::{
    Config,
    config::{Host, SslMode},
};
use native_tls::{Certificate, TlsConnector};

/// Describe how the configured TLS mode falls short of authenticating the
/// server, or `None` when it does. Connections that never leave the host
/// (Unix sockets and loopback addresses) are not flagged.
pub(crate) fn weakness(pg: &Config) -> Option<&'static str> {
    let local = !(pg.get_hosts().is_empty() && pg.get_hostaddrs().is_empty())
        && pg.get_hosts().iter().all(|host| match host {
            Host::Tcp(name) => {
                name.eq_ignore_ascii_case("localhost")
                    || name
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            }
            #[cfg(unix)]
            Host::Unix(_) => true,
        })
        && pg
            .get_hostaddrs()
            .iter()
            .all(|address| address.is_loopback());
    if local {
        return None;
    }
    match pg.get_ssl_mode() {
        SslMode::Disable => {
            Some("sslmode=disable sends credentials and replicated rows in plaintext")
        }
        SslMode::Prefer => Some(
            "sslmode=prefer (the default when unset) silently falls back to plaintext and does not authenticate the server",
        ),
        SslMode::Require if pg.get_ssl_root_cert().is_none() => Some(
            "sslmode=require without sslrootcert encrypts but does not authenticate the server, so an on-path attacker can intercept the connection",
        ),
        _ => None,
    }
}

/// Operator guidance appended to [`weakness`] warnings.
pub(crate) const RECOMMENDATION: &str =
    "use sslmode=verify-full with sslrootcert set to the server's CA bundle";

/// The configured source connection settings, when they can be read. A missing
/// or invalid URL is reported by the connection attempt itself.
pub(crate) fn configured(config: &crate::config::Config) -> Option<Config> {
    std::env::var(&config.source.connection_env)
        .ok()?
        .parse()
        .ok()
}

/// Warn once at startup when the source connection does not authenticate the
/// server. libpq-compatible defaults are kept; this only makes them visible.
pub(crate) fn warn_if_unauthenticated(config: &crate::config::Config) {
    if let Some(weakness) = configured(config).as_ref().and_then(weakness) {
        tracing::warn!(
            recommendation = RECOMMENDATION,
            "PostgreSQL source TLS is not authenticated: {weakness}"
        );
    }
}

pub(crate) fn connector(pg: &Config) -> Result<TlsConnector> {
    // These parameters are parsed by tokio-postgres but are not applied by its
    // MakeTlsConnector. Never silently ignore a requested client identity.
    ensure!(
        pg.get_ssl_cert().is_none() && pg.get_ssl_key().is_none(),
        "PostgreSQL client TLS certificates (sslcert/sslkey) are not supported"
    );
    let roots = pg.get_ssl_root_cert();
    let (verify_certificate, verify_hostname) = match pg.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (false, false),
        // Match libpq: require encrypts without authentication unless a CA was
        // configured, in which case it has verify-ca semantics.
        SslMode::Require => (roots.is_some(), false),
        // libpq refuses verify-ca without a root certificate. Chain checks
        // against the public system roots without a hostname check would
        // accept any publicly trusted certificate issued for any name.
        SslMode::VerifyCa => {
            ensure!(
                roots.is_some(),
                "sslmode=verify-ca requires sslrootcert: it checks the certificate chain but not the server hostname, so it is only meaningful against a private CA. Set sslrootcert to the CA bundle that issued the server certificate, or use sslmode=verify-full to also verify the hostname"
            );
            (true, false)
        }
        SslMode::VerifyFull => (true, true),
        _ => bail!("unsupported PostgreSQL TLS mode"),
    };
    let mut builder = TlsConnector::builder();
    builder
        .danger_accept_invalid_certs(!verify_certificate)
        .danger_accept_invalid_hostnames(!verify_hostname);
    if let Some(roots) = roots {
        // An explicit CA bundle replaces ambient system trust. Load every PEM
        // certificate, including bundles used during managed-database rotation.
        builder.disable_built_in_roots(true);
        let pem = std::str::from_utf8(roots).context("sslrootcert must be a PEM CA bundle")?;
        let mut count = 0;
        let mut certificate = None::<String>;
        for line in pem.lines().map(str::trim) {
            match line {
                "-----BEGIN CERTIFICATE-----" => {
                    ensure!(
                        certificate.is_none(),
                        "nested certificate in PostgreSQL sslrootcert"
                    );
                    certificate = Some(format!("{line}\n"));
                }
                "-----END CERTIFICATE-----" => {
                    let mut block = certificate
                        .take()
                        .context("unexpected certificate end in PostgreSQL sslrootcert")?;
                    block.push_str(line);
                    builder.add_root_certificate(
                        Certificate::from_pem(block.as_bytes())
                            .context("invalid certificate in PostgreSQL sslrootcert")?,
                    );
                    count += 1;
                }
                _ => {
                    ensure!(
                        !line.starts_with("-----BEGIN") && !line.starts_with("-----END"),
                        "invalid PEM boundary in PostgreSQL sslrootcert"
                    );
                    if let Some(block) = &mut certificate {
                        block.push_str(line);
                        block.push('\n');
                    }
                    // PEM bundles may carry issuer labels and comments outside
                    // certificate blocks, including after the final certificate.
                }
            }
        }
        ensure!(
            certificate.is_none(),
            "unterminated certificate in PostgreSQL sslrootcert"
        );
        ensure!(count > 0, "PostgreSQL sslrootcert contains no certificates");
    }
    builder.build().context("configure PostgreSQL TLS")
}

#[cfg(test)]
mod tests {
    use super::*;
    use native_tls::{Identity, TlsAcceptor};
    use std::{
        net::{TcpListener, TcpStream},
        path::Path,
        process::Command,
        time::Duration,
    };

    fn openssl(dir: &Path, args: &[&str]) {
        let output = Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "openssl: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn certificate(dir: &Path, name: &str) -> Vec<u8> {
        openssl(
            dir,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "extendedKeyUsage=serverAuth",
                "-keyout",
                &format!("{name}.key"),
                "-out",
                &format!("{name}.crt"),
            ],
        );
        std::fs::read(dir.join(format!("{name}.crt"))).unwrap()
    }

    fn handshake(pg: &Config, hostname: &str, identity: &Identity) -> bool {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = TlsAcceptor::new(identity.clone()).unwrap();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            acceptor.accept(socket).is_ok()
        });
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let result = connector(pg).unwrap().connect(hostname, socket);
        if let Err(error) = &result {
            eprintln!("TLS handshake: {error}");
        }
        let connected = result.is_ok();
        let _ = server.join().unwrap();
        connected
    }

    #[test]
    fn postgres_tls_modes_enforce_certificate_and_hostname_policy() {
        let dir = tempfile::tempdir().unwrap();
        let trusted = certificate(dir.path(), "server");
        let untrusted = certificate(dir.path(), "unrelated");
        openssl(
            dir.path(),
            &[
                "pkcs12",
                "-export",
                "-inkey",
                "server.key",
                "-in",
                "server.crt",
                "-out",
                "server.p12",
                "-passout",
                "pass:fixture",
            ],
        );
        let identity = Identity::from_pkcs12(
            &std::fs::read(dir.path().join("server.p12")).unwrap(),
            "fixture",
        )
        .unwrap();
        for (mode, root, hostname, expected) in [
            ("prefer", None, "localhost", true),
            ("require", None, "localhost", true),
            ("require", None, "wrong.invalid", true),
            ("require", Some(&trusted), "wrong.invalid", true),
            ("require", Some(&untrusted), "localhost", false),
            ("verify-ca", Some(&trusted), "wrong.invalid", true),
            ("verify-ca", Some(&untrusted), "localhost", false),
            // Rejected before connecting: public roots without a hostname check.
            ("verify-ca", None, "localhost", false),
            ("verify-full", None, "localhost", false),
            ("verify-full", Some(&trusted), "localhost", true),
            ("verify-full", Some(&untrusted), "localhost", false),
            ("verify-full", Some(&trusted), "wrong.invalid", false),
        ] {
            let mut pg: Config = format!("host=localhost sslmode={mode}").parse().unwrap();
            if let Some(root) = root {
                pg.ssl_root_cert(root);
            }
            let connected = match connector(&pg) {
                Ok(_) => handshake(&pg, hostname, &identity),
                Err(error) => {
                    assert!(format!("{error:#}").contains("requires sslrootcert"));
                    false
                }
            };
            assert_eq!(
                connected,
                expected,
                "mode={mode}, configured_root={}, hostname={hostname}",
                root.is_some()
            );
        }
        // Exercise the actual file-backed URL parser and multi-CA rotation bundles.
        let bundle = [
            b"# issuer labels and leading comments\n".as_slice(),
            &untrusted,
            b"\n# rotation CA\n",
            &trusted,
            b"\n# trailing comments are valid PEM bundle metadata\n",
        ]
        .concat();
        let bundle_path = dir.path().join("bundle.crt");
        std::fs::write(&bundle_path, bundle).unwrap();
        let pg: Config = format!(
            "host=localhost sslmode=verify-full sslrootcert={}",
            bundle_path.display()
        )
        .parse()
        .unwrap();
        assert!(handshake(&pg, "localhost", &identity));
        assert!(!handshake(&pg, "wrong.invalid", &identity));

        // A separate issuer CA must also validate a signed server leaf.
        openssl(
            dir.path(),
            &[
                "req",
                "-new",
                "-key",
                "server.key",
                "-subj",
                "/CN=localhost",
                "-out",
                "leaf.csr",
            ],
        );
        std::fs::write(dir.path().join("leaf.ext"),
            "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n").unwrap();
        openssl(
            dir.path(),
            &[
                "x509",
                "-req",
                "-in",
                "leaf.csr",
                "-CA",
                "unrelated.crt",
                "-CAkey",
                "unrelated.key",
                "-CAcreateserial",
                "-days",
                "1",
                "-extfile",
                "leaf.ext",
                "-out",
                "leaf.crt",
            ],
        );
        openssl(
            dir.path(),
            &[
                "pkcs12",
                "-export",
                "-inkey",
                "server.key",
                "-in",
                "leaf.crt",
                "-out",
                "leaf.p12",
                "-passout",
                "pass:fixture",
            ],
        );
        let leaf = Identity::from_pkcs12(
            &std::fs::read(dir.path().join("leaf.p12")).unwrap(),
            "fixture",
        )
        .unwrap();
        let mut pg: Config = "host=localhost sslmode=verify-full".parse().unwrap();
        pg.ssl_root_cert(&std::fs::read(dir.path().join("unrelated.crt")).unwrap());
        assert!(handshake(&pg, "localhost", &leaf));
        assert!(!handshake(&pg, "wrong.invalid", &leaf));
    }

    #[test]
    fn weak_tls_policies_are_reported_for_network_connections() {
        let root = b"configured CA".as_slice();
        for (settings, root, weak) in [
            ("host=db.example.com", None, true),
            ("host=db.example.com sslmode=disable", None, true),
            ("host=db.example.com sslmode=prefer", None, true),
            ("host=db.example.com sslmode=require", None, true),
            ("host=db.example.com sslmode=require", Some(root), false),
            ("host=db.example.com sslmode=verify-ca", Some(root), false),
            ("host=db.example.com sslmode=verify-full", None, false),
            ("host=db.example.com sslmode=verify-full", Some(root), false),
            ("host=10.0.0.5 sslmode=require", None, true),
            ("host=localhost,db.example.com sslmode=disable", None, true),
            (
                "host=localhost hostaddr=10.0.0.5 sslmode=disable",
                None,
                true,
            ),
            ("hostaddr=10.0.0.5", None, true),
            // Traffic that never leaves the host cannot be intercepted on the network.
            ("host=localhost sslmode=disable", None, false),
            ("host=127.0.0.1", None, false),
            ("host=::1 sslmode=prefer", None, false),
            ("host=/var/run/postgresql", None, false),
            ("hostaddr=127.0.0.1", None, false),
        ] {
            let mut pg: Config = settings.parse().unwrap();
            if let Some(root) = root {
                pg.ssl_root_cert(root);
            }
            assert_eq!(weakness(&pg).is_some(), weak, "{settings}");
        }
    }

    #[test]
    fn malformed_roots_and_unsupported_client_identity_fail_closed() {
        for mode in ["require", "verify-ca", "verify-full"] {
            for root in [
                b"".as_slice(), b"not a certificate",
                b"-----BEGIN CERTIFICATE-----\ntruncated",
                b"-----END CERTIFICATE-----",
                b"-----BEGIN CERTIFICATE----\nmalformed boundary",
                b"-----BEGIN CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----",
            ] {
                let mut pg: Config = format!("sslmode={mode}").parse().unwrap();
                pg.ssl_root_cert(root);
                assert!(connector(&pg).is_err());
            }
        }
        for (cert, key) in [(true, false), (false, true), (true, true)] {
            let mut pg = Config::new();
            if cert {
                pg.ssl_cert(b"client certificate");
            }
            if key {
                pg.ssl_key(b"private key");
            }
            assert!(
                connector(&pg)
                    .unwrap_err()
                    .to_string()
                    .contains("not supported")
            );
        }
    }
}
