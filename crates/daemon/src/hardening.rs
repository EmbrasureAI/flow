//! Process-wide protections for local state and diagnostics. The state
//! directory holds replicated row data (journal, spool, index), so it and
//! everything Flow creates inside it are private to the service user.
use anyhow::{Context, Result};
use std::path::Path;
use tracing_subscriber::EnvFilter;

/// Log targets capped at INFO whatever `RUST_LOG` requests. reqsign's DEBUG
/// records print credential providers with `{:?}`, and its static AWS provider
/// derives `Debug` over the secret access key. A `RUST_LOG` directive for the
/// same target is replaced, so each crate and the leaking module are listed.
const LOG_CAPS: &[&str] = &[
    "reqsign=info",
    "reqsign_core=info",
    "reqsign_core::api=info",
    "reqsign_aws_core=info",
    "reqsign_aws_v4=info",
    "reqsign_file_read_tokio=info",
];

/// Apply [`LOG_CAPS`] after the operator's filter so they take precedence.
pub(crate) fn cap_secret_logs(mut filter: EnvFilter) -> EnvFilter {
    for directive in LOG_CAPS {
        filter = filter.add_directive(directive.parse().expect("static log directive"));
    }
    filter
}

/// Create every file and directory owner-only (0600/0700), including RocksDB
/// files and journal segments whose creation mode Flow does not control.
/// Call before any threads or files are created.
pub(crate) fn restrict_umask() {
    #[cfg(unix)]
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
}

/// Create the state directory owner-only and tighten one this user owns that
/// is still group- or world-accessible from an earlier release. Files created
/// by earlier releases stay readable only through that directory.
pub(crate) fn secure_state_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .with_context(|| format!("create state_dir {}", path.display()))?;
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("inspect state_dir {}", path.display()))?;
        let mode = metadata.mode() & 0o7777;
        if mode & 0o077 == 0 {
            return Ok(());
        }
        let path = path.display().to_string();
        if metadata.uid() != rustix::process::geteuid().as_raw() {
            tracing::warn!(
                state_dir = path,
                mode = format!("{mode:04o}"),
                "state_dir holds replicated row data but is accessible to other users and owned by a different user; restrict it to the Flow service user"
            );
            return Ok(());
        }
        // Tightening is a protection, not a precondition: an unsupported
        // filesystem must not stop an existing deployment.
        match std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode & !0o077)) {
            Ok(()) => tracing::warn!(
                state_dir = path,
                previous_mode = format!("{mode:04o}"),
                mode = format!("{:04o}", mode & !0o077),
                "removed group and other access from state_dir, which holds replicated row data"
            ),
            Err(error) => tracing::warn!(
                state_dir = path,
                mode = format!("{mode:04o}"),
                %error,
                "state_dir holds replicated row data but is accessible to other users and could not be restricted"
            ),
        }
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(path)
        .with_context(|| format!("create state_dir {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn debug_logging_never_enables_credential_provider_records() {
        for requested in [
            "debug",
            "trace",
            "info,reqsign=trace",
            "info,reqsign_core=debug",
            "info,reqsign_core::api=trace",
            "info,reqsign_core::api=debug,reqsign_aws_core=trace",
        ] {
            let captured = Captured::default();
            let writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(cap_secret_logs(EnvFilter::new(requested)))
                .with_writer(move || writer.clone())
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                tracing::debug!(target: "reqsign_core::api", "provider secret-access-key");
                tracing::trace!(target: "reqsign_aws_core::provide_credential", "secret process");
                tracing::info!(target: "reqsign_core::api", "provider info kept");
                tracing::info!(target: "flow_daemon", "flow info kept");
            });
            let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
            assert!(!output.contains("secret"), "{requested}: {output}");
            assert!(output.contains("provider info kept"), "{requested}");
            assert!(output.contains("flow info kept"), "{requested}");
        }
        // Other targets still honor the operator's level.
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(cap_secret_logs(EnvFilter::new("debug")))
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "flow_daemon", "flow debug kept");
        });
        assert!(String::from_utf8_lossy(&captured.0.lock().unwrap()).contains("flow debug kept"));
    }

    #[tokio::test]
    async fn catalog_and_storage_debug_output_omits_property_values() {
        use iceberg::CatalogBuilder;
        use iceberg::io::{FileIOBuilder, StorageConfig, StorageFactory};
        let secrets = [
            ("credential", "client:TEST_SECRET_CREDENTIAL"),
            ("token", "TEST_SECRET_TOKEN"),
            ("s3.secret-access-key", "TEST_SECRET_ACCESS_KEY"),
            ("s3.session-token", "TEST_SECRET_SESSION"),
        ];
        let props: std::collections::HashMap<_, _> = secrets
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .chain([("uri".into(), "http://127.0.0.1:1".into())])
            .collect();
        let factory =
            std::sync::Arc::new(iceberg_storage_opendal::OpenDalResolvingStorageFactory::new());
        let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
            .load("destination", props.clone())
            .await
            .unwrap();
        let file_io = FileIOBuilder::new(factory.clone())
            .with_props(props.clone())
            .build();
        let storage = factory
            .build(&StorageConfig::from_props(props.clone()))
            .unwrap();
        for debug in [
            format!("{catalog:?}"),
            format!("{file_io:?}"),
            format!("{storage:?}"),
        ] {
            for (key, value) in secrets {
                assert!(debug.contains(key), "{debug}");
                assert!(!debug.contains(value), "{debug}");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn state_dir_is_created_private_and_existing_access_is_removed() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o7777;
        let temp = tempfile::tempdir().unwrap();
        let created = temp.path().join("new/state");
        secure_state_dir(&created).unwrap();
        assert_eq!(mode(&created), 0o700);

        let existing = temp.path().join("existing");
        std::fs::create_dir(&existing).unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(existing.join("control"), "kept").unwrap();
        secure_state_dir(&existing).unwrap();
        assert_eq!(mode(&existing), 0o700);
        assert_eq!(
            std::fs::read_to_string(existing.join("control")).unwrap(),
            "kept"
        );
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o750)).unwrap();
        secure_state_dir(&existing).unwrap();
        assert_eq!(mode(&existing), 0o700);
        // Private owner permissions are left as configured.
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o500)).unwrap();
        secure_state_dir(&existing).unwrap();
        assert_eq!(mode(&existing), 0o500);
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}
