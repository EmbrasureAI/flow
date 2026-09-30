//! Index lifecycle. Durable control is opened before a replaceable row index;
//! source acknowledgement and publication begin only after recovery returns.

use crate::config::Config;
use anyhow::{Context, Result, ensure};
use flow_coordinator::{
    ReplanRequired, SourceSchemaRecord, TableMaintenance, load_control_schema,
    resolve_catalog_operation, same_iceberg_schema, source_schema_prefix,
};
use flow_materializer::iceberg_schema;
use flow_model::{SourceId, TableId, TableSchema};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use iceberg::{Catalog, table::Table};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

struct CleanupDirectory {
    path: PathBuf,
    armed: bool,
}

impl CleanupDirectory {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn retain(&mut self) {
        self.armed = false;
    }

    fn remove(&mut self) -> Result<()> {
        remove_directory_and_sync(&self.path)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for CleanupDirectory {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = remove_directory_and_sync(&self.path)
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove abandoned index rebuild directory"
            );
        }
    }
}

// Field order is intentional: close RocksDB before removing its directory.
struct RebuildCandidate {
    store: Option<StateStore>,
    directory: CleanupDirectory,
}

impl RebuildCandidate {
    fn new(store: StateStore, directory: CleanupDirectory) -> Self {
        Self {
            store: Some(store),
            directory,
        }
    }

    fn store(&self) -> &StateStore {
        self.store.as_ref().expect("candidate store is open")
    }

    fn close(&mut self) {
        drop(self.store.take());
    }
}

fn options(config: &Config) -> StateStoreOptions {
    StateStoreOptions {
        apply_batch_rows: config.limits.batch_rows,
        max_open_files: crate::disk::index_max_open_files(),
        ..Default::default()
    }
}

/// Present only between a clean exit and the next open. Its absence means the
/// previous process crashed, lost power, or stopped with an error.
const CLEAN_SHUTDOWN: &str = "index-clean-shutdown";

pub(crate) fn open(config: &Config, control: ControlStore) -> Result<StateStore> {
    crate::disk::warn_if_insufficient(config);
    // Consume the marker first: any exit that does not record a new one makes
    // the next start verify the complete index again.
    let clean = take_clean_shutdown(&config.state_dir)?;
    let store = match control.active_generation()? {
        Some(active) => StateStore::open_with_control(active.path, options(config), control)?,
        None => control.initialize_index(config.state_dir.join("index"), options(config))?,
    };
    if config.storage.verify_index_on_start || !clean {
        // RocksDB opens metadata lazily and may not touch a damaged SST until
        // its first point lookup. After an unclean exit (including one caused
        // by a corrupt read), verify the complete replaceable index so a
        // supervisor restart deterministically enters the rebuild path.
        let started = Instant::now();
        store.validate_storage()?;
        tracing::info!(
            event = "index_storage_verified",
            after_clean_shutdown = clean,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "verified every row index checksum"
        );
    } else {
        // Reads still verify block checksums; a corrupt read stops the process
        // without a clean marker, so the next start runs the full scan.
        tracing::info!("previous exit was clean; skipping the full index checksum scan");
    }
    Ok(store)
}

/// Record that this process stopped without an error, so the next start may
/// skip the full index scan. Failure only costs that next start a full scan.
pub(crate) fn record_clean_shutdown(state_dir: &Path) {
    let recorded = fs::File::create(state_dir.join(CLEAN_SHUTDOWN))
        .and_then(|marker| marker.sync_all())
        .and_then(|()| fs::File::open(state_dir)?.sync_all());
    if let Err(error) = recorded {
        tracing::warn!(%error, "could not record a clean shutdown; the next start verifies the full index");
    }
}

fn take_clean_shutdown(state_dir: &Path) -> Result<bool> {
    match fs::remove_file(state_dir.join(CLEAN_SHUTDOWN)) {
        Ok(()) => {
            fs::File::open(state_dir)?.sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// `check --storage`: verify every checksum and the index/control revision of
/// the selected generation while the service is stopped. A generation that
/// requires rebuilding is rebuilt by the next start.
pub(crate) fn check_storage(config: &Config) -> Result<()> {
    let control_path = config.state_dir.join("control");
    ensure!(
        control_path.is_dir(),
        "{} has no control store; run `init` first",
        config.state_dir.display()
    );
    let control = ControlStore::open(&control_path)
        .context("open the control store; stop the service before `check --storage`")?;
    let active = control
        .active_generation()?
        .context("the control store has not selected an index generation")?;
    let started = Instant::now();
    let verified = StateStore::open_with_control(&active.path, options(config), control)
        .and_then(|store| store.validate_storage());
    match verified {
        Ok(()) => {
            println!(
                "index storage verified in {:.1}s: {}",
                started.elapsed().as_secs_f64(),
                active.path.display()
            );
            Ok(())
        }
        Err(error) if error.requires_index_rebuild() => {
            take_clean_shutdown(&config.state_dir)?;
            anyhow::bail!(
                "index storage check failed: {error}; the next start rebuilds the index from control and Iceberg"
            )
        }
        Err(error) => Err(error.into()),
    }
}

/// The caller must validate persisted target UUIDs before invoking this method.
/// A fresh catalog scan preserves control watermarks; it never invents source
/// progress from a row count or a snapshot's current sequence number.
pub(crate) async fn rebuild(
    config: &Config,
    control: ControlStore,
    catalog: Arc<dyn Catalog>,
    schemas: &[TableSchema],
    targets: &BTreeMap<TableId, Table>,
) -> Result<StateStore> {
    let started = Instant::now();
    let previous = control.active_generation()?;
    // A rebuild writes a complete new generation. Reclaim what interrupted
    // attempts left first; the selected generation is retained for diagnosis.
    if let Err(error) = sweep_abandoned_generations(
        &config.state_dir,
        previous
            .as_ref()
            .map(|generation| generation.path.as_path()),
    ) {
        tracing::warn!(%error, "failed to remove abandoned index rebuild directories");
    }
    for operation in control.pending_operations()? {
        let table = targets
            .get(&operation.operation.table_id)
            .context("pending operation references an unconfigured table")?;
        let outcome =
            resolve_catalog_operation(&operation, catalog.as_ref(), table, &control).await?;
        let authority = control.clone();
        tokio::task::spawn_blocking(move || {
            authority.resolve_operation(&operation.operation.id, outcome)
        })
        .await??;
    }
    let mut refreshed_targets = BTreeMap::new();
    for (id, table) in targets {
        let head = catalog.load_table(table.identifier()).await?;
        ensure!(
            head.metadata().uuid() == table.metadata().uuid(),
            "target incarnation changed while resolving catalog operations"
        );
        refreshed_targets.insert(*id, head);
    }
    let targets = &refreshed_targets;
    let source = SourceId(config.source.id.clone());
    let schemas = schemas
        .iter()
        .map(|base| -> Result<TableSchema> {
            let target = targets
                .get(&base.table_id)
                .context("missing target table")?;
            if same_iceberg_schema(target.metadata().current_schema(), &iceberg_schema(base)?) {
                return Ok(base.clone());
            }
            for record in control
                .source_transactions_after(&source_schema_prefix(&source, base.table_id), None)
            {
                let (_, bytes) = record?;
                let record = SourceSchemaRecord::decode(&bytes)?;
                if same_iceberg_schema(
                    target.metadata().current_schema(),
                    &iceberg_schema(&record.schema)?,
                ) {
                    base.validate_successor(&record.schema)?;
                    return Ok(record.schema);
                }
            }
            anyhow::bail!("target schema has no durable source lineage")
        })
        .collect::<Result<Vec<_>>>()?;
    let authority: BTreeMap<_, _> = control.table_states()?.into_iter().collect();
    ensure!(
        authority.len() == schemas.len(),
        "durable control does not cover every source table; recover initialization before rebuilding"
    );
    for schema in &schemas {
        let state = authority
            .get(&schema.table_id)
            .context("source table has no durable watermark")?;
        ensure!(
            state.schema_version <= schema.version,
            "target schema regressed behind durable source progress"
        );
        if state.schema_version < schema.version {
            load_control_schema(&control, &source, schema.table_id, state.schema_version)?
                .validate_successor(schema)?;
        }
    }

    let mut checkpoints = control.checkpoints()?;
    checkpoints.sort_by_key(|checkpoint| std::cmp::Reverse(checkpoint.revision));
    let mut restored_candidate = None;
    for checkpoint in checkpoints {
        let usable = checkpoint.pending_operations.is_empty()
            && checkpoint.tables.len() == authority.len()
            && checkpoint.tables.iter().all(|(id, state)| {
                authority.get(id).is_some_and(|current| {
                    state.materialized_lsn == current.materialized_lsn
                        && state.schema_version == current.schema_version
                        && targets.get(id).is_some_and(|table| {
                            table.metadata().current_snapshot_id() == state.snapshot_id
                        })
                })
            });
        if !usable || !checkpoint.path.join("CURRENT").is_file() {
            continue;
        }
        let directory = candidate_directory(&config.state_dir)?;
        let candidate = directory.path().to_owned();
        let authority = control.clone();
        let options = options(config);
        match tokio::task::spawn_blocking(move || {
            let index = authority.restore_checkpoint(&checkpoint, candidate, options)?;
            index.validate_storage()?;
            Ok::<_, flow_state_store::Error>(index)
        })
        .await?
        {
            Ok(index) => {
                restored_candidate = Some(RebuildCandidate::new(index, directory));
                break;
            }
            Err(error) => {
                tracing::warn!(%error, "checkpoint restore failed; trying another recovery source");
            }
        }
    }
    let restored = restored_candidate.is_some();
    let mut candidate = if let Some(candidate) = restored_candidate {
        candidate
    } else {
        let directory = candidate_directory(&config.state_dir)?;
        let path = directory.path().to_owned();
        let options = options(config);
        let index = tokio::task::spawn_blocking(move || StateStore::open(path, options)).await??;
        RebuildCandidate::new(index, directory)
    };
    if !restored {
        let mut scratch_directory =
            CleanupDirectory::new(candidate.directory.path().with_extension("scratch"));
        let scratch = StateStore::open(scratch_directory.path(), options(config))?;
        let maintenance = TableMaintenance::new(
            candidate.store().clone(),
            catalog.clone(),
            config.compaction.clone(),
            crate::services::writer_config(config),
        )?
        .with_read_limits(config.parquet_read.clone())?;
        for schema in &schemas {
            let table = targets
                .get(&schema.table_id)
                .context("missing target table")?;
            maintenance
                .rebuild_index(
                    table,
                    schema,
                    candidate.store().clone(),
                    scratch.clone(),
                    authority
                        .get(&schema.table_id)
                        .expect("validated above")
                        .materialized_lsn,
                    authority[&schema.table_id].schema_version,
                )
                .await?;
        }
        drop(maintenance);
        drop(scratch);
        scratch_directory.remove()?;
    }
    // An external rewrite can race a multi-table scan. Never select locations
    // from an older head and start emitting position deletes against the new one.
    for schema in &schemas {
        let table = targets
            .get(&schema.table_id)
            .context("missing target table")?;
        let head = catalog.load_table(table.identifier()).await?;
        ensure!(
            head.metadata().uuid() == table.metadata().uuid(),
            "target incarnation changed during index rebuild"
        );
        ensure!(
            same_iceberg_schema(head.metadata().current_schema(), &iceberg_schema(schema)?),
            "target schema changed during index rebuild"
        );
        if head.metadata().current_snapshot_id()
            != candidate.store().table_state(&schema.table_id)?.snapshot_id
        {
            return Err(ReplanRequired.into());
        }
    }
    let authority = control.clone();
    let replacement = candidate.store().clone();
    let active =
        tokio::task::spawn_blocking(move || authority.activate_rebuilt(&replacement)).await??;
    // From this point the control store names this directory. Never let error
    // cleanup remove the authoritative generation, even if reopening fails.
    candidate.directory.retain();
    candidate.close();
    let store = StateStore::open_with_control(&active.path, options(config), control)?;
    if let Err(error) = retire_superseded_generations(
        &config.state_dir,
        &active.path,
        previous
            .as_ref()
            .map(|generation| generation.path.as_path()),
    ) {
        // The new generation is already authoritative and reopen-validated.
        // Cleanup failure must not trigger another rebuild and another directory.
        tracing::warn!(%error, "failed to retire superseded index generations");
    }
    tracing::info!(
        event = "index_generation_activated", tables = schemas.len(), restored_checkpoint = restored,
        elapsed_ms = started.elapsed().as_millis() as u64, path = %active.path.display(),
        "recovered authoritative row index"
    );
    Ok(store)
}

fn candidate_directory(state_dir: &Path) -> Result<CleanupDirectory> {
    let root = fs::canonicalize(state_dir)?;
    let requested_parent = state_dir.join("index-generations");
    fs::create_dir_all(&requested_parent)?;
    let kind = fs::symlink_metadata(&requested_parent)?.file_type();
    ensure!(
        kind.is_dir() && !kind.is_symlink(),
        "index generation directory must be a real directory"
    );
    let parent = fs::canonicalize(&requested_parent)?;
    ensure!(
        parent.parent() == Some(root.as_path()),
        "index generation directory escapes the configured state directory"
    );
    fs::File::open(&root)?.sync_all()?;
    let path = parent.join(format!("index-{}", uuid::Uuid::new_v4()));
    ensure!(
        !path.exists(),
        "new index generation path unexpectedly exists"
    );
    Ok(CleanupDirectory::new(path))
}

fn remove_directory_and_sync(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn retire_superseded_generations(
    state_dir: &Path,
    active: &Path,
    previous: Option<&Path>,
) -> Result<()> {
    let active = fs::canonicalize(active)?;
    let previous = previous.and_then(|path| fs::canonicalize(path).ok());
    let managed = managed_generation_directories(state_dir)?;
    ensure!(
        managed.contains(&active),
        "active index is outside the managed state directories"
    );
    let keep = [Some(active), previous]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let mut synced = BTreeSet::new();
    let mut first_error = None;
    for path in managed {
        if keep.contains(&path) {
            continue;
        }
        match fs::remove_dir_all(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    synced.insert(parent.to_owned());
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                first_error.get_or_insert_with(|| {
                    anyhow::anyhow!(
                        "remove superseded index generation {}: {error}",
                        path.display()
                    )
                });
            }
        }
    }
    for parent in synced {
        if let Err(error) = fs::File::open(&parent).and_then(|directory| directory.sync_all()) {
            first_error.get_or_insert_with(|| {
                anyhow::anyhow!(
                    "sync index generation parent {} after removal: {error}",
                    parent.display()
                )
            });
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(())
    }
}

/// Remove every managed generation except the selected one: candidates and
/// scratch stores of interrupted rebuilds, and generations a completed rebuild
/// would retire anyway. Only one process owns the state directory.
fn sweep_abandoned_generations(state_dir: &Path, active: Option<&Path>) -> Result<usize> {
    let mut removed = 0;
    for path in managed_generation_directories(state_dir)? {
        if Some(path.as_path()) == active {
            continue;
        }
        remove_directory_and_sync(&path)?;
        tracing::info!(path = %path.display(), "removed abandoned index generation");
        removed += 1;
    }
    Ok(removed)
}

fn managed_generation_directories(state_dir: &Path) -> Result<Vec<PathBuf>> {
    let root = fs::canonicalize(state_dir)?;
    let mut managed = Vec::new();
    let legacy = state_dir.join("index");
    if is_real_directory(&legacy)? {
        let legacy = fs::canonicalize(legacy)?;
        ensure!(
            legacy.parent() == Some(root.as_path()),
            "legacy index directory escapes the configured state directory"
        );
        managed.push(legacy);
    }

    let requested_parent = state_dir.join("index-generations");
    if !is_real_directory(&requested_parent)? {
        return Ok(managed);
    }
    let parent = fs::canonicalize(requested_parent)?;
    ensure!(
        parent.parent() == Some(root.as_path()),
        "index generation directory escapes the configured state directory"
    );
    for entry in fs::read_dir(&parent)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || !is_managed_generation_name(&entry.file_name()) {
            continue;
        }
        let path = fs::canonicalize(entry.path())?;
        ensure!(
            path.parent() == Some(parent.as_path()),
            "index generation escapes its managed directory"
        );
        managed.push(path);
    }
    Ok(managed)
}

fn is_real_directory(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn is_managed_generation_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let name = name.strip_suffix(".scratch").unwrap_or(name);
    name.strip_prefix("index-")
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fail_after_candidate_build(state_dir: &Path) -> Result<()> {
        let directory = candidate_directory(state_dir)?;
        let replacement = StateStore::open(directory.path(), StateStoreOptions::default())?;
        let candidate = RebuildCandidate::new(replacement, directory);
        let scratch_directory =
            CleanupDirectory::new(candidate.directory.path().with_extension("scratch"));
        let scratch = StateStore::open(scratch_directory.path(), StateStoreOptions::default())?;
        ensure!(
            candidate.store().index_is_empty(&TableId(1))?
                && scratch.index_is_empty(&TableId(1))?,
            "fresh rebuild stores must be empty"
        );
        Err(ReplanRequired.into())
    }

    fn test_config(state_dir: &Path) -> Config {
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = state_dir.to_owned();
        config
    }

    /// Flip a byte in the first data block of the SOURCE family's table file.
    /// Opening reads only metadata; iterating the family reads this block.
    fn corrupt_source_table_file(index: &Path) {
        let mut corrupted = 0;
        for entry in fs::read_dir(index).unwrap() {
            let path = entry.unwrap().path();
            if path.extension() != Some(OsStr::new("sst")) {
                continue;
            }
            let mut bytes = fs::read(&path).unwrap();
            if bytes
                .windows(b"source_transactions".len())
                .any(|window| window == b"source_transactions")
            {
                bytes[10] ^= 0xff;
                fs::write(&path, bytes).unwrap();
                corrupted += 1;
            }
        }
        assert_eq!(corrupted, 1);
    }

    fn requires_rebuild(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<flow_state_store::Error>()
            .is_some_and(flow_state_store::Error::requires_index_rebuild)
    }

    #[test]
    fn full_index_scan_runs_after_an_unclean_exit_on_request_or_when_configured() {
        let root = tempdir().unwrap();
        let state_dir = root.path().join("state");
        fs::create_dir(&state_dir).unwrap();
        let mut config = test_config(&state_dir);
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        let store = open(&config, control.clone()).unwrap();
        store
            .update_source_ledger((b"ledger", &[7; 512]), [], None)
            .unwrap();
        // A checkpoint flushes every memtable into table files.
        store.checkpoint(root.path().join("flushed")).unwrap();
        let active = control.active_generation().unwrap().unwrap().path;
        drop(store);
        drop(control);
        corrupt_source_table_file(&active);
        let marker = state_dir.join(CLEAN_SHUTDOWN);

        // After a clean exit, reads verify block checksums; startup is fast.
        record_clean_shutdown(&state_dir);
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        drop(open(&config, control).unwrap());
        assert!(!marker.exists(), "an open consumes the clean marker");

        // Without a new clean marker, the next start verifies everything.
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        assert!(requires_rebuild(&open(&config, control).err().unwrap()));

        // `check --storage` finds it offline and discards a clean marker.
        record_clean_shutdown(&state_dir);
        let error = check_storage(&config).unwrap_err();
        assert!(format!("{error:#}").contains("the next start rebuilds the index"));
        assert!(!marker.exists());

        // Configuration forces the scan even after a clean exit.
        record_clean_shutdown(&state_dir);
        config.storage.verify_index_on_start = true;
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        assert!(requires_rebuild(&open(&config, control).err().unwrap()));
    }

    #[test]
    fn check_storage_verifies_an_intact_index_and_requires_initialization() {
        let root = tempdir().unwrap();
        let state_dir = root.path().join("state");
        fs::create_dir(&state_dir).unwrap();
        let config = test_config(&state_dir);
        let error = check_storage(&config).unwrap_err();
        assert!(error.to_string().contains("run `init` first"));
        assert!(!state_dir.join("control").exists());
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        drop(open(&config, control).unwrap());
        check_storage(&config).unwrap();
    }

    #[test]
    fn rebuild_start_sweeps_interrupted_candidates_but_keeps_the_selected_generation() {
        let root = tempdir().unwrap();
        let state_dir = root.path().join("state");
        fs::create_dir(&state_dir).unwrap();
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        drop(
            control
                .initialize_index(state_dir.join("index"), StateStoreOptions::default())
                .unwrap(),
        );
        // A crash cannot run the candidate's Drop cleanup.
        let mut interrupted = candidate_directory(&state_dir).unwrap();
        interrupted.retain();
        drop(StateStore::open(interrupted.path(), StateStoreOptions::default()).unwrap());
        let scratch = interrupted.path().with_extension("scratch");
        drop(StateStore::open(&scratch, StateStoreOptions::default()).unwrap());
        let unrelated = state_dir.join("index-generations").join("notes");
        fs::create_dir(&unrelated).unwrap();

        let active = control.active_generation().unwrap().unwrap().path;
        assert_eq!(
            sweep_abandoned_generations(&state_dir, Some(&active)).unwrap(),
            2
        );
        assert_eq!(
            managed_generation_directories(&state_dir).unwrap(),
            std::slice::from_ref(&active)
        );
        assert!(unrelated.exists());
        drop(
            StateStore::open_with_control(&active, StateStoreOptions::default(), control).unwrap(),
        );
    }

    #[test]
    fn repeated_head_races_remove_candidate_and_scratch_directories() {
        let root = tempdir().unwrap();
        let state_dir = root.path().join("state");
        fs::create_dir(&state_dir).unwrap();

        for _ in 0..4 {
            let error = fail_after_candidate_build(&state_dir).unwrap_err();
            assert!(error.downcast_ref::<ReplanRequired>().is_some());
            assert!(
                managed_generation_directories(&state_dir)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                fs::read_dir(state_dir.join("index-generations"))
                    .unwrap()
                    .count(),
                0
            );
        }
    }

    #[test]
    fn repeated_success_retains_only_active_and_previous_generation() {
        let root = tempdir().unwrap();
        let state_dir = root.path().join("state");
        fs::create_dir(&state_dir).unwrap();
        let control = ControlStore::open(state_dir.join("control")).unwrap();
        let legacy_path = state_dir.join("index");
        drop(
            control
                .initialize_index(&legacy_path, StateStoreOptions::default())
                .unwrap(),
        );
        let legacy_path = fs::canonicalize(&legacy_path).unwrap();

        for attempt in 0..5 {
            let previous = control.active_generation().unwrap().unwrap();
            let directory = candidate_directory(&state_dir).unwrap();
            let replacement =
                StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
            let mut candidate = RebuildCandidate::new(replacement, directory);
            let active = control.activate_rebuilt(candidate.store()).unwrap();
            candidate.directory.retain();
            candidate.close();
            drop(
                StateStore::open_with_control(
                    &active.path,
                    StateStoreOptions::default(),
                    control.clone(),
                )
                .unwrap(),
            );

            let stale_scratch = state_dir
                .join("index-generations")
                .join(format!("index-{}.scratch", uuid::Uuid::new_v4()));
            fs::create_dir(&stale_scratch).unwrap();
            retire_superseded_generations(&state_dir, &active.path, Some(&previous.path)).unwrap();

            let retained = managed_generation_directories(&state_dir).unwrap();
            assert_eq!(retained.len(), 2);
            assert!(retained.contains(&active.path));
            assert!(retained.contains(&previous.path));
            assert!(!stale_scratch.exists());
            if attempt == 0 {
                assert!(legacy_path.exists());
            } else {
                assert!(!legacy_path.exists());
            }
        }
    }
}
