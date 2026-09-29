//! Shared service construction and durable source-ledger adapters.
use crate::config::Config;
use anyhow::Result;
use flow_coordinator::SourceLedger;
use flow_ingress_journal::JournalConfig;
use flow_model::SourceId;
use flow_state_store::{ControlStore, StateStore};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use std::{sync::Arc, time::Duration};

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
    Ok(Arc::new(
        RestCatalogBuilder::default()
            .with_client(
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .read_timeout(Duration::from_secs(30))
                    .timeout(Duration::from_secs(60))
                    .build()?,
            )
            .with_storage_factory(crate::storage_observer::observe(Arc::new(
                OpenDalResolvingStorageFactory::new(),
            )))
            .load("destination", config.catalog_properties()?)
            .await?,
    ))
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
