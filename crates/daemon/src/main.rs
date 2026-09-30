#[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
mod allocator;
mod bootstrap;
mod config;
mod discover;
mod disk;
mod generation;
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
use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    Init,
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
    Status,
    /// Adopt legacy catalog JSON into grace-delayed GC while the source is paused.
    MetadataImport {
        /// NDJSON entries containing table_uuid and path; inventory is operator supplied.
        #[arg(long)]
        inventory: PathBuf,
        /// Default is validation only. No objects are deleted by this command.
        #[arg(long)]
        apply: bool,
    },
}
fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run_cli());
    // A started blocking compactor can still own network I/O after its async
    // handle is dropped. Keep BUILD durable until process exit; startup retires
    // it. Runtime shutdown must not wait indefinitely or claim that worker joined.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn run_cli() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();
    let cli = Cli::parse();
    let config = if matches!(cli.command, Command::Discover { .. }) {
        config::Config::load_without_tables(&cli.config)?
    } else {
        config::Config::load(&cli.config)?
    };
    #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
    if matches!(&cli.command, Command::Init | Command::Run { .. }) {
        allocator::initialize();
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
            Ok(())
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
            .await
        }
        Command::Init => {
            let _http = http::start(&config).await?;
            runtime::initialize(config).await
        }
        Command::Run { roles } => {
            anyhow::ensure!(
                roles
                    .iter()
                    .all(|r| ["ingest", "coordinator", "compactor"].contains(&r.as_str())),
                "unknown role"
            );
            anyhow::ensure!(
                roles.iter().any(|r| r == "ingest") && roles.iter().any(|r| r == "coordinator"),
                "this binary currently requires ingest and coordinator together; compactor workers are exposed through the library API"
            );
            let _http = http::start(&config).await?;
            runtime::run(config, roles.iter().any(|r| r == "compactor")).await
        }
        Command::Status => runtime::status(config),
        Command::MetadataImport { inventory, apply } => {
            metadata_import::run(config, &inventory, apply).await
        }
    }
}
