//! Apply PostgreSQL connection TLS policy to the native TLS transport.
use anyhow::{Context, Result, bail, ensure};
use flow_pg_source::tokio_postgres::{Config, config::SslMode};
use native_tls::{Certificate, TlsConnector};

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
        SslMode::VerifyCa => (true, false),
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
            ("verify-full", None, "localhost", false),
            ("verify-full", Some(&trusted), "localhost", true),
            ("verify-full", Some(&untrusted), "localhost", false),
            ("verify-full", Some(&trusted), "wrong.invalid", false),
        ] {
            let mut pg: Config = format!("host=localhost sslmode={mode}").parse().unwrap();
            if let Some(root) = root {
                pg.ssl_root_cert(root);
            }
            assert_eq!(
                handshake(&pg, hostname, &identity),
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
