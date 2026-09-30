//! Operator adoption of pre-registry catalog JSON. Normal GC owns deletion.
use crate::{bootstrap, config::Config, services};
use anyhow::{Context, Result, ensure};
use flow_state_store::ControlStore;
use futures::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    table_uuid: uuid::Uuid,
    path: String,
}

pub(crate) async fn run(config: Config, inventory: &Path, apply: bool) -> Result<()> {
    ensure!(
        config.state_dir.join("control/CURRENT").is_file(),
        "metadata import requires initialized state"
    );
    // RocksDB rejects a concurrent daemon/importer. Pause via the supervisor
    // first; do not change its desired state or start source capture here.
    let control = ControlStore::open(config.state_dir.join("control"))?;
    let boot = bootstrap::bootstrap(&control)?;
    bootstrap::validate_identity(&config, &boot)?;
    ensure!(
        boot.copied
            && boot.target_uuids.len() == boot.schemas.len()
            && boot.targets.len() == boot.schemas.len(),
        "metadata import requires completed, identified targets"
    );
    ensure!(
        control.active_generation()?.is_some(),
        "metadata import requires an existing index"
    );
    // The shared open consumes the clean-shutdown marker, verifies the index
    // after an unclean exit, and applies the descriptor budget.
    let store = crate::generation::open(&config, control)?;
    let targets: BTreeMap<_, _> = boot
        .target_uuids
        .iter()
        .copied()
        .zip(boot.schemas.iter().zip(&boot.targets))
        .collect();
    let catalog = services::catalog(&config).await?;
    let mut reader = BufReader::new(File::open(inventory)?);
    let mut line = Vec::new();
    let mut cached: Option<(uuid::Uuid, iceberg::table::Table)> = None;
    let mut checked = 0_u64;
    let mut queued = 0_u64;
    // Bound object reads and memory while avoiding one network round trip at a
    // time during the operator's paused-source window. Each file still passes
    // the same ownership validation before its durable registration.
    let mut pending = FuturesUnordered::new();
    loop {
        line.clear();
        if (&mut reader)
            .take(16 * 1024 + 1)
            .read_until(b'\n', &mut line)?
            == 0
        {
            break;
        }
        ensure!(
            line.len() <= 16 * 1024,
            "metadata inventory line exceeds budget"
        );
        let entry: Entry =
            serde_json::from_slice(&line).context("invalid metadata inventory line")?;
        let (schema, (namespace, name)) = targets
            .get(&entry.table_uuid)
            .context("metadata inventory table is not owned by this source generation")?;
        if cached
            .as_ref()
            .is_none_or(|(id, _)| *id != entry.table_uuid)
        {
            let table = catalog
                .load_table(&services::target(namespace, name)?)
                .await?;
            ensure!(
                table.metadata().uuid() == entry.table_uuid,
                "metadata inventory target was replaced"
            );
            cached = Some((entry.table_uuid, table));
        }
        let store = store.clone();
        let table = cached.as_ref().expect("loaded table").1.clone();
        let table_id = schema.table_id;
        queued += 1;
        let ordinal = queued;
        pending.push(async move {
            flow_coordinator::import_catalog_metadata(&store, &table, table_id, &entry.path, apply)
                .await
                .with_context(|| format!("metadata inventory entry {ordinal} failed"))
        });
        if pending.len() == 8 {
            pending.next().await.expect("pending imports")?;
            checked += 1;
            if checked.is_multiple_of(1000) {
                tracing::info!(
                    event = "metadata_import_progress",
                    checked,
                    apply,
                    "catalog JSON inventory checked"
                );
            }
        }
    }
    while let Some(result) = pending.next().await {
        result?;
        checked += 1;
    }
    println!(
        "{}",
        serde_json::json!({"checked": checked, "applied": apply, "deleted": 0})
    );
    drop(pending);
    drop(store);
    crate::generation::record_clean_shutdown(&config.state_dir);
    Ok(())
}
