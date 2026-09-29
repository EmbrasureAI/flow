#[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
mod allocator;
mod bootstrap;
mod config;
mod generation;
mod lifecycle;
mod metadata_import;
mod observation;
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
    /// Validate configuration without contacting services.
    Check,
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
    let config = config::Config::load(&cli.config)?;
    #[cfg(all(feature = "jemalloc", target_os = "linux", target_env = "gnu"))]
    if matches!(&cli.command, Command::Init | Command::Run { .. }) {
        allocator::initialize();
    }
    match cli.command {
        Command::Check => {
            println!("configuration valid: {} tables", config.tables.len());
            Ok(())
        }
        Command::Init => runtime::initialize(config).await,
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
            runtime::run(config, roles.iter().any(|r| r == "compactor")).await
        }
        Command::Status => runtime::status(config),
        Command::MetadataImport { inventory, apply } => {
            metadata_import::run(config, &inventory, apply).await
        }
    }
}
