#[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
mod allocator;
mod bootstrap;
mod config;
mod discover;
mod disk;
mod exit;
mod generation;
mod hardening;
mod http;
mod lifecycle;
mod metadata_import;
mod observation;
mod preflight;
mod retry;
mod runtime;
mod schema;
mod services;
mod source;
mod source_tls;
mod storage_observer;
use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::{io::IsTerminal, path::PathBuf, process::ExitCode};

#[derive(Parser)]
#[command(
    name = "embrasure-flow",
    version,
    about = "PostgreSQL streaming materialization into ordinary Iceberg v2/v3 tables"
)]
struct Cli {
    #[arg(long, default_value = "flow.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Validate configuration and the state volume's free space; with --source,
    /// also run read-only PostgreSQL readiness checks (settings, permissions,
    /// tables, publication and slot).
    Check {
        /// Connect to the source and report its readiness without changing it.
        #[arg(long)]
        source: bool,
        /// Verify every row index checksum. Stop the service first.
        #[arg(long)]
        storage: bool,
    },
    /// Print [[tables]] configuration for existing source tables (read-only).
    /// Defaults to the configured publication's tables not yet configured.
    Discover {
        /// Tables as schema.table; a bare name means public.
        tables: Vec<String>,
        /// Discover every table in this schema instead of the publication.
        #[arg(long, conflicts_with = "tables")]
        schema: Option<String>,
        /// Iceberg namespace for generated targets; defaults to the source schema.
        #[arg(long)]
        target_namespace: Option<String>,
    },
    /// Create a logical slot and copy a consistent initial snapshot, then exit.
    Init {
        /// Adopt and initialize newly added tables in configuration without resynchronizing existing tables.
        #[arg(long)]
        add_tables: bool,
        /// Re-snapshot a specific table (as schema.table) without affecting other tables.
        #[arg(long)]
        resnapshot: Option<String>,
    },
    /// Adopt and initialize newly configured tables without resynchronizing existing tables.
    AddTable,
    /// Re-snapshot an existing table (as schema.table) without affecting other tables.
    Resnapshot {
        /// Table to re-snapshot as schema.table; a bare name means public.
        table: String,
    },
    /// Recover durable work and continuously capture and publish changes.
    Run {
        #[arg(
            long,
            default_value = "ingest,coordinator,compactor",
            value_delimiter = ','
        )]
        roles: Vec<String>,
    },
    /// Read the latest local service status without opening its state database.
    /// Exits 0 when ready and 3 when not ready.
    Status,
    /// Adopt catalog metadata JSON that Flow did not register into grace-delayed GC; run while the daemon is stopped.
    MetadataImport {
        /// NDJSON entries containing table_uuid and path; inventory is operator supplied.
        #[arg(long)]
        inventory: PathBuf,
        /// Default is validation only. No objects are deleted by this command.
        #[arg(long)]
        apply: bool,
    },
}
fn main() -> ExitCode {
    hardening::restrict_umask();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::from(exit::FAILURE);
        }
    };
    let result = runtime.block_on(run_cli());
    // A started blocking compactor can still own network I/O after its async
    // handle is dropped. Keep BUILD durable until process exit; startup retires
    // it. Runtime shutdown must not wait indefinitely or claim that worker joined.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn run_cli() -> ExitCode {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .finish()
        .with(hardening::secret_log_filter())
        .init();
    let cli = Cli::parse();
    // Services log only structured events; command-line tools also print the
    // plain error, as does any command attached to a terminal.
    let plain = !matches!(
        cli.command,
        Command::Init { .. } | Command::AddTable | Command::Resnapshot { .. } | Command::Run { .. }
    ) || std::io::stderr().is_terminal();
    command(cli)
        .await
        .unwrap_or_else(|error| fatal(&error, plain))
}

/// One structured event, in the same JSON stream as every other log line.
fn fatal(error: &anyhow::Error, plain: bool) -> ExitCode {
    let (class, _) = exit::classify(error);
    tracing::error!(
        event = "fatal",
        exit_code = class.code(),
        class = class.name(),
        error = %format!("{error:#}"),
        "embrasure-flow stopped"
    );
    // A RUST_LOG that filters out this target must not hide why it stopped.
    if plain || !tracing::enabled!(tracing::Level::ERROR) {
        eprintln!("error: {error:#}");
    }
    ExitCode::from(class.code())
}

/// Keep why `init` or `run` stopped in `status`, after its cleanup ran; a
/// clean exit replaces an earlier failure.
fn record_exit(config: &config::Config, result: Result<()>) -> Result<ExitCode> {
    match &result {
        Ok(()) => lifecycle::record_clean_exit(config),
        Err(error) => lifecycle::record_exit(config, error),
    }
    result.map(|()| ExitCode::SUCCESS)
}

async fn command(cli: Cli) -> Result<ExitCode> {
    let config = if matches!(cli.command, Command::Discover { .. }) {
        config::Config::load_without_tables(&cli.config)
    } else {
        config::Config::load(&cli.config)
    }
    .map_err(|error| exit::config(format!("{error:#}")))?;
    #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
    if matches!(
        &cli.command,
        Command::Init { .. } | Command::AddTable | Command::Resnapshot { .. } | Command::Run { .. }
    ) {
        allocator::initialize();
    }
    if matches!(
        &cli.command,
        Command::Init { .. } | Command::AddTable | Command::Resnapshot { .. } | Command::Run { .. }
    ) {
        tracing::info!(
            config = %cli.config.display(),
            state_dir = %config.state_dir.display(),
            "starting"
        );
    }
    match cli.command {
        Command::Check { source, storage } => {
            println!("configuration valid: {} tables", config.tables.len());
            disk::check(&config);
            if storage {
                generation::check_storage(&config)?;
            }
            if source {
                preflight::check_source(&config).await?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Discover {
            tables,
            schema,
            target_namespace,
        } => {
            discover::discover(
                &config,
                &tables,
                schema.as_deref(),
                target_namespace.as_deref(),
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Init {
            add_tables,
            resnapshot,
        } => {
            hardening::secure_state_dir(&config.state_dir)?;
            source_tls::warn_if_unauthenticated(&config);
            let _http = http::start(&config).await?;
            let options = bootstrap::InitOptions {
                add_tables,
                resnapshot,
            };
            record_exit(
                &config,
                runtime::initialize_with_options(config.clone(), options).await,
            )
        }
        Command::AddTable => {
            hardening::secure_state_dir(&config.state_dir)?;
            source_tls::warn_if_unauthenticated(&config);
            let _http = http::start(&config).await?;
            let options = bootstrap::InitOptions {
                add_tables: true,
                resnapshot: None,
            };
            record_exit(
                &config,
                runtime::initialize_with_options(config.clone(), options).await,
            )
        }
        Command::Resnapshot { table } => {
            hardening::secure_state_dir(&config.state_dir)?;
            source_tls::warn_if_unauthenticated(&config);
            let _http = http::start(&config).await?;
            let options = bootstrap::InitOptions {
                add_tables: false,
                resnapshot: Some(table),
            };
            record_exit(
                &config,
                runtime::initialize_with_options(config.clone(), options).await,
            )
        }
        Command::Run { roles } => {
            ensure!(
                roles
                    .iter()
                    .all(|r| ["ingest", "coordinator", "compactor"].contains(&r.as_str())),
                exit::config("unknown role")
            );
            ensure!(
                roles.iter().any(|r| r == "ingest") && roles.iter().any(|r| r == "coordinator"),
                exit::config(
                    "this binary currently requires ingest and coordinator together; compactor workers are exposed through the library API"
                )
            );
            hardening::secure_state_dir(&config.state_dir)?;
            source_tls::warn_if_unauthenticated(&config);
            let _http = http::start(&config).await?;
            let compaction = roles.iter().any(|r| r == "compactor");
            record_exit(&config, runtime::run(config.clone(), compaction).await)
        }
        Command::Status => Ok(if runtime::status(config)? {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(exit::NOT_READY)
        }),
        Command::MetadataImport { inventory, apply } => {
            metadata_import::run(config, &inventory, apply).await?;
            Ok(ExitCode::SUCCESS)
        }
    }
}
