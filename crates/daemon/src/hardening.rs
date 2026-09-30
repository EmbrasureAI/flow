//! Process-wide protections for local state and diagnostics. The state
//! directory holds replicated row data (journal, spool, index), so it and
//! everything Flow creates inside it are private to the service user.
use anyhow::{Context, Result};
use std::path::Path;
use tracing::{Level, Metadata};
use tracing_subscriber::filter::{FilterFn, filter_fn};

/// Global filter layer that drops reqsign records above INFO whatever
/// `RUST_LOG` requests, including span and field directives. reqsign's DEBUG
/// records print credential providers with `{:?}`, and its static AWS provider
/// derives `Debug` over the secret access key. Add it to the subscriber with
/// `SubscriberExt::with`; other targets keep the operator's filter.
pub(crate) fn secret_log_filter() -> FilterFn<fn(&Metadata<'_>) -> bool> {
    fn allowed(metadata: &Metadata<'_>) -> bool {
        !(metadata.target().starts_with("reqsign") && *metadata.level() > Level::INFO)
    }
    filter_fn(allowed as fn(&Metadata<'_>) -> bool)
}

/// Create every file and directory owner-only (0600/0700), including RocksDB
/// files and journal segments whose creation mode Flow does not control.
/// Call before any threads or files are created.
pub(crate) fn restrict_umask() {
    #[cfg(unix)]
    rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
}

/// What to do about an existing state directory's permissions.
#[derive(Debug, PartialEq, Eq)]
enum Access {
    Private,
    /// Owned by this user: remove group and other access.
    Tighten,
    /// Owned by root, as with orchestrator volumes shared through a group
    /// (for example Kubernetes `fsGroup` mounts): report but continue.
    Warn,
    /// Another user can replace Flow's state: refuse to start.
    Refuse,
}

fn access(mode: u32, owner: u32, euid: u32) -> Access {
    if mode & 0o077 == 0 {
        Access::Private
    } else if owner == euid {
        Access::Tighten
    } else if owner != 0 && mode & 0o022 != 0 {
        Access::Refuse
    } else {
        Access::Warn
    }
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
        // Inspect and change the directory through one descriptor (fstat and
        // fchmod), so a concurrent rename cannot redirect the change. A
        // symlinked state_dir is followed, as every other state access does.
        let directory = std::fs::File::open(path)
            .with_context(|| format!("open state_dir {}", path.display()))?;
        let metadata = directory
            .metadata()
            .with_context(|| format!("inspect state_dir {}", path.display()))?;
        anyhow::ensure!(
            metadata.is_dir(),
            "state_dir {} is not a directory",
            path.display()
        );
        let mode = metadata.mode() & 0o7777;
        let path = path.display().to_string();
        match access(mode, metadata.uid(), rustix::process::geteuid().as_raw()) {
            Access::Private => {}
            Access::Refuse => anyhow::bail!(
                "state_dir {path} is owned by uid {} and writable by other users (mode {mode:04o}), so another user could replace Flow's state; make it owned by the Flow service user, or set state_dir to a subdirectory Flow creates, such as /data/state",
                metadata.uid()
            ),
            Access::Warn => tracing::warn!(
                state_dir = path,
                mode = format!("{mode:04o}"),
                "state_dir holds replicated row data but is accessible to other users and owned by a different user; restrict it, or set state_dir to a subdirectory Flow creates, such as /data/state"
            ),
            // Tightening is a protection, not a precondition: an unsupported
            // filesystem must not stop an existing deployment.
            Access::Tighten => {
                let tightened = mode & !0o077;
                match directory.set_permissions(std::fs::Permissions::from_mode(tightened)) {
                    Ok(()) => tracing::warn!(
                        state_dir = path,
                        previous_mode = format!("{mode:04o}"),
                        mode = format!("{tightened:04o}"),
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

    fn capture(requested: &str, emit: impl FnOnce()) -> String {
        use tracing_subscriber::layer::SubscriberExt;
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(requested))
            .with_writer(move || writer.clone())
            .finish()
            .with(secret_log_filter());
        tracing::subscriber::with_default(subscriber, emit);
        String::from_utf8(captured.0.lock().unwrap().clone()).unwrap()
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
            "info,reqsign_aws_core::provide_credential::process=trace",
            // Span and field directives are matched separately from targets.
            "info,[outer]=trace",
            "info,[outer{secret}]=trace",
        ] {
            let output = capture(requested, || {
                let span = tracing::info_span!("outer", secret = true);
                let _entered = span.enter();
                tracing::debug!(target: "reqsign_core::api", "provider secret-access-key");
                tracing::trace!(
                    target: "reqsign_aws_core::provide_credential::process",
                    "secret process"
                );
                tracing::info!(target: "reqsign_core::api", "provider info kept");
                tracing::info!(target: "flow_daemon", "flow info kept");
            });
            assert!(
                !output.contains("secret-access-key"),
                "{requested}: {output}"
            );
            assert!(!output.contains("secret process"), "{requested}: {output}");
            assert!(output.contains("provider info kept"), "{requested}");
            assert!(output.contains("flow info kept"), "{requested}");
        }
        // Other targets still honor the operator's level.
        let output = capture("debug", || {
            tracing::debug!(target: "flow_daemon", "flow debug kept");
        });
        assert!(output.contains("flow debug kept"));
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

    #[test]
    fn foreign_writable_state_dirs_are_refused_but_shared_root_mounts_are_not() {
        const FLOW: u32 = 1000;
        const OTHER: u32 = 1001;
        assert_eq!(access(0o700, FLOW, FLOW), Access::Private);
        assert_eq!(access(0o700, OTHER, FLOW), Access::Private);
        assert_eq!(access(0o755, FLOW, FLOW), Access::Tighten);
        assert_eq!(access(0o777, FLOW, FLOW), Access::Tighten);
        assert_eq!(access(0o755, OTHER, FLOW), Access::Warn);
        assert_eq!(access(0o775, OTHER, FLOW), Access::Refuse);
        assert_eq!(access(0o757, OTHER, FLOW), Access::Refuse);
        // Root-owned group volumes, such as Kubernetes fsGroup mounts.
        assert_eq!(access(0o2775, 0, FLOW), Access::Warn);
        assert_eq!(access(0o777, 0, FLOW), Access::Warn);
        // Running as root owns root-owned directories.
        assert_eq!(access(0o755, 0, 0), Access::Tighten);
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
