//! Replication through real journal and control stores over in-memory object
//! storage: total local loss, replaced owners, clean release and retention.
use flow_durability::{Config, Durability, Layout, Mode};
use flow_ingress_journal::{Journal, JournalConfig};
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId, TableMutationCount};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use opendal::{Operator, services::Memory};
use std::{
    path::Path,
    time::{Duration, Instant},
};

/// Injects failures into object writes, as a flaky or unavailable store would.
mod faults {
    use opendal::raw::{
        Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
    };
    use opendal::{Error, ErrorKind, Result};
    use std::sync::{Arc, Mutex};

    /// Fails writes below a prefix a number of times, or until cleared.
    #[derive(Clone, Debug, Default)]
    pub struct Faults(Arc<Mutex<Vec<(String, usize)>>>);

    impl Faults {
        pub fn fail(&self, prefix: &str, times: usize) {
            self.0.lock().unwrap().push((prefix.to_owned(), times));
        }

        pub fn clear(&self) {
            self.0.lock().unwrap().clear();
        }

        pub fn remaining(&self) -> usize {
            self.0.lock().unwrap().iter().map(|(_, times)| times).sum()
        }

        fn hit(&self, path: &str) -> bool {
            let path = path.trim_start_matches('/');
            let mut faults = self.0.lock().unwrap();
            let Some((_, times)) = faults
                .iter_mut()
                .find(|(prefix, times)| *times > 0 && path.starts_with(prefix.as_str()))
            else {
                return false;
            };
            *times -= 1;
            true
        }
    }

    impl<A: Access> Layer<A> for Faults {
        type LayeredAccess = Faulty<A>;

        fn layer(&self, inner: A) -> Faulty<A> {
            Faulty {
                inner,
                faults: self.clone(),
            }
        }
    }

    #[derive(Debug)]
    pub struct Faulty<A> {
        inner: A,
        faults: Faults,
    }

    impl<A: Access> LayeredAccess for Faulty<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;
        type Copier = A::Copier;

        fn inner(&self) -> &A {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, A::Reader)> {
            self.inner.read(path, args).await
        }

        async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, A::Writer)> {
            if self.faults.hit(path) {
                return Err(Error::new(ErrorKind::Unexpected, "injected failure").set_temporary());
            }
            self.inner.write(path, args).await
        }

        async fn delete(&self) -> Result<(RpDelete, A::Deleter)> {
            self.inner.delete().await
        }

        async fn list(&self, path: &str, args: OpList) -> Result<(RpList, A::Lister)> {
            self.inner.list(path, args).await
        }
    }
}

fn config() -> Config {
    Config {
        lease_secs: 4,
        request_timeout_secs: 1,
        checkpoint_interval_secs: 3600,
        stall_warning_secs: 5,
        ..Config::new("memory://test")
    }
}

fn layout() -> Layout {
    Layout {
        control: "control".into(),
        journals: vec!["journal".into()],
        files: vec!["source-identity.json".into()],
        derived: vec![
            "index".into(),
            "index-generations".into(),
            "checkpoints".into(),
        ],
    }
}

async fn open(operator: &Operator, state: &Path) -> Durability {
    Durability::open_with_operator(&config(), operator.clone(), state, "source", layout())
        .await
        .unwrap()
}

/// Open local state the way the daemon does: control first, then a
/// baseline, then the index and journal.
async fn attach(durability: &Durability, state: &Path) -> (ControlStore, StateStore, Journal) {
    let control = ControlStore::open(state.join("control")).unwrap();
    durability.attach_control(&control).await.unwrap();
    durability.baseline().await.unwrap();
    let index = match control.active_generation().unwrap() {
        Some(active) if active.path.exists() => StateStore::open_with_control(
            active.path,
            StateStoreOptions::default(),
            control.clone(),
        )
        .unwrap(),
        _ => control
            .initialize_index(
                state.join(format!("index-{}", uuid_like())),
                StateStoreOptions::default(),
            )
            .unwrap(),
    };
    let (mut journal, _) = Journal::open(state.join("journal"), JournalConfig::default()).unwrap();
    durability.attach_journal("journal", &mut journal).unwrap();
    (control, index, journal)
}

fn uuid_like() -> u128 {
    Instant::now().elapsed().as_nanos() ^ u128::from(std::process::id())
}

fn commit(journal: &mut Journal, xid: u32, payload: &[u8]) {
    journal.append_chunk(xid, payload).unwrap();
    let lsn = u64::from(xid) * 100;
    journal
        .commit(SourceTransaction {
            source_id: SourceId("source".into()),
            xid,
            begin_lsn: PgLsn(lsn),
            commit_lsn: PgLsn(lsn + 1),
            end_lsn: PgLsn(lsn + 2),
            commit_timestamp_micros: 0,
            affected_tables: vec![TableId(1)],
            schema_versions: vec![flow_model::TableSchemaVersion {
                table_id: TableId(1),
                version: 1,
            }],
            mutation_chunks: journal.transaction_chunks(xid),
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id: TableId(1),
                mutations: 1,
            }]),
        })
        .unwrap();
}

fn transactions(journal: &Journal) -> Vec<(u32, Vec<u8>)> {
    use flow_ingress_journal::ChunkReader;
    journal
        .transactions()
        .iter()
        .unwrap()
        .map(|transaction| {
            let transaction = transaction.unwrap();
            let payload = journal
                .chunks(&transaction.mutation_chunks)
                .unwrap()
                .map(|chunk| chunk.unwrap())
                .collect::<Vec<_>>()
                .concat();
            (transaction.xid, payload)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_state_survives_losing_the_whole_state_directory() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let first = tempfile::tempdir().unwrap();
    let durability = open(&operator, first.path()).await;
    assert_eq!(durability.mode(), Mode::Fresh);
    let (_control, index, mut journal) = attach(&durability, first.path()).await;
    std::fs::write(first.path().join("source-identity.json"), b"{\"slot\":1}").unwrap();
    durability
        .write_file("source-identity.json", b"{\"slot\":1}")
        .unwrap();
    for xid in 1..=20 {
        commit(&mut journal, xid, format!("payload-{xid}").as_bytes());
        index
            .put_source_transaction(format!("ledger-{xid}").as_bytes(), &xid.to_be_bytes())
            .unwrap();
    }
    // Reclaim the first transactions: the restored journal must not need them.
    journal.reclaim(PgLsn(502)).unwrap();
    durability.barrier().await.unwrap();
    // Written after the barrier: may or may not survive, but never partially.
    commit(&mut journal, 21, b"after-barrier");
    let expected = transactions(&journal);
    durability.barrier().await.unwrap();
    // Crash without a release, then continue on another volume. The old
    // directory outlives the new owner's start: if its lock file were deleted,
    // the filesystem could hand its inode to the new lock file, which would
    // rightly prove the old owner stopped and skip the wait under test.
    durability.stop();
    drop((index, journal, _control));

    let second = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let durability = open(&operator, second.path()).await;
    drop(first);
    assert_eq!(durability.mode(), Mode::Restored);
    assert!(
        started.elapsed().as_secs_f64() >= 4.0,
        "an unclean predecessor must be waited for"
    );
    assert_eq!(
        std::fs::read(second.path().join("source-identity.json")).unwrap(),
        b"{\"slot\":1}"
    );
    let control = ControlStore::open(second.path().join("control")).unwrap();
    for xid in 1..=20u32 {
        assert_eq!(
            control
                .source_transaction(format!("ledger-{xid}").as_bytes())
                .unwrap(),
            Some(xid.to_be_bytes().to_vec())
        );
    }
    drop(control);
    let (journal, recovery) =
        Journal::open(second.path().join("journal"), JournalConfig::default()).unwrap();
    assert_eq!(recovery.reclaimed_lsn, PgLsn(502));
    assert_eq!(transactions(&journal), expected);
    assert!(expected.first().is_some_and(|(xid, _)| *xid == 6));
    drop(journal);
    durability.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repaired_tail_is_replicated_again_even_when_rewritten_longer() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    // An orphan chunk makes the next open append an abort record.
    journal.append_chunk(2, b"orphan").unwrap();
    commit(&mut journal, 1, b"first");
    durability.barrier().await.unwrap();
    durability.stop();
    drop((control, index, journal));
    // A torn write shorter than that abort record. The next baseline copies
    // it before the journal opens and cuts it.
    let segment = state
        .path()
        .join("journal")
        .join(flow_ingress_journal::segment_file_name(0));
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap();
    std::io::Write::write_all(&mut file, &[0xff; 5]).unwrap();
    drop(file);

    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.mode(), Mode::Local);
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    assert!(journal.layout().repaired.is_some());
    commit(&mut journal, 3, b"third");
    let expected = transactions(&journal);
    durability.barrier().await.unwrap();
    durability.stop();
    drop((control, index, journal));

    let restored = tempfile::tempdir().unwrap();
    let durability = open(&operator, restored.path()).await;
    assert_eq!(durability.mode(), Mode::Restored);
    let (journal, recovery) =
        Journal::open(restored.path().join("journal"), JournalConfig::default()).unwrap();
    assert_eq!(
        recovery.truncated_bytes, 0,
        "the replica holds no torn bytes"
    );
    assert_eq!(transactions(&journal), expected);
    assert_eq!(expected.len(), 2);
    drop(journal);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_owner_fails_its_barriers_and_cannot_append() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let old_dir = tempfile::tempdir().unwrap();
    let old = open(&operator, old_dir.path()).await;
    let (_control, index, mut journal) = attach(&old, old_dir.path()).await;
    commit(&mut journal, 1, b"before");
    index.put_source_transaction(b"k", b"1").unwrap();
    old.barrier().await.unwrap();

    // A successor starts elsewhere while the old process keeps running.
    let new_dir = tempfile::tempdir().unwrap();
    let successor = tokio::spawn({
        let operator = operator.clone();
        let path = new_dir.path().to_path_buf();
        async move { open(&operator, &path).await }
    });
    // The old owner notices at its next renewal, well within one lease.
    let deadline = Instant::now() + std::time::Duration::from_secs(4);
    while old.failure().is_none() && Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(old.failure().is_some(), "old owner was not fenced");
    assert!(old.barrier().await.is_err());
    assert!(index.put_source_transaction(b"k", b"2").is_err());
    let new = successor.await.unwrap();
    assert_eq!(new.mode(), Mode::Restored);
    let control = ControlStore::open(new_dir.path().join("control")).unwrap();
    assert_eq!(
        control.source_transaction(b"k").unwrap(),
        Some(b"1".to_vec())
    );
    drop(control);
    new.stop();
    old.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_release_keeps_local_state_and_skips_the_lease_wait() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    for xid in 1..=5 {
        commit(&mut journal, xid, b"payload");
    }
    index.put_source_transaction(b"k", b"v").unwrap();
    let expected = transactions(&journal);
    durability.baseline().await.unwrap();
    drop((control, index, journal));
    durability.release().await.unwrap();

    let parts = |operator: Operator| async move {
        operator
            .list_with("journals/")
            .recursive(true)
            .await
            .unwrap()
            .into_iter()
            .filter(|entry| entry.path().ends_with(".part"))
            .map(|entry| entry.path().to_owned())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let before = parts(operator.clone()).await;
    let started = Instant::now();
    let durability = open(&operator, state.path()).await;
    assert!(
        started.elapsed().as_secs_f64() < 3.0,
        "released owner was waited for"
    );
    assert_eq!(durability.mode(), Mode::Local);
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    assert_eq!(transactions(&journal), expected);
    // Unchanged journal bytes are not uploaded again by the restart baseline.
    assert!(parts(operator.clone()).await.is_subset(&before));
    assert_eq!(
        control.source_transaction(b"k").unwrap(),
        Some(b"v".to_vec())
    );
    commit(&mut journal, 6, b"more");
    durability.barrier().await.unwrap();
    drop((control, index, journal));
    durability.release().await.unwrap();

    // Disabling replication on a replicated directory is refused.
    assert!(flow_durability::ensure_not_replicated(state.path()).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_local_state_is_migrated_and_old_history_is_retired() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    {
        // Local-only state from before replication was enabled.
        let control = ControlStore::open(state.path().join("control")).unwrap();
        let index = control
            .initialize_index(state.path().join("index"), StateStoreOptions::default())
            .unwrap();
        index.put_source_transaction(b"legacy", b"1").unwrap();
        let (mut journal, _) =
            Journal::open(state.path().join("journal"), JournalConfig::default()).unwrap();
        commit(&mut journal, 1, b"legacy");
    }
    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.mode(), Mode::Migrated);
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    for round in 0..4u32 {
        for xid in 0..10 {
            commit(&mut journal, 10 + round * 10 + xid, b"tail");
        }
        index
            .put_source_transaction(b"round", &round.to_be_bytes())
            .unwrap();
        durability.baseline().await.unwrap();
    }
    let log = operator.list("log/").await.unwrap();
    assert!(
        log.len() < 10,
        "retention left {} log batches after four checkpoints",
        log.len()
    );
    let expected = transactions(&journal);
    drop((control, index, journal));
    durability.stop();

    let restored = tempfile::tempdir().unwrap();
    let durability = open(&operator, restored.path()).await;
    assert_eq!(durability.mode(), Mode::Restored);
    let control = ControlStore::open(restored.path().join("control")).unwrap();
    assert_eq!(
        control.source_transaction(b"legacy").unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        control.source_transaction(b"round").unwrap(),
        Some(3u32.to_be_bytes().to_vec())
    );
    let (journal, _) =
        Journal::open(restored.path().join("journal"), JournalConfig::default()).unwrap();
    assert_eq!(transactions(&journal), expected);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_restart_on_the_same_volume_proves_the_old_owner_exited() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    commit(&mut journal, 1, b"payload");
    index.put_source_transaction(b"k", b"v").unwrap();
    durability.barrier().await.unwrap();
    let expected = transactions(&journal);
    // Crash: no release record. The process lock is released with it.
    durability.stop();
    drop((control, index, journal));

    let started = Instant::now();
    let durability = open(&operator, state.path()).await;
    assert!(
        started.elapsed().as_secs_f64() < 3.0,
        "the same lock under the same kernel proves the old owner stopped"
    );
    assert_eq!(durability.mode(), Mode::Local);
    let (control, index, journal) = attach(&durability, state.path()).await;
    assert_eq!(transactions(&journal), expected);
    assert_eq!(
        control.source_transaction(b"k").unwrap(),
        Some(b"v".to_vec())
    );
    assert!(!durability.status().waited_for_previous_owner);
    durability.stop();
    drop((control, index, journal));

    // A claimant with this lock that died before starting its epoch proves
    // nothing about the owner before it, which may still hold a lease.
    let claim = operator
        .read(&format!("owners/{:020}.json", 2))
        .await
        .unwrap()
        .to_vec();
    operator
        .write(&format!("owners/{:020}.json", 3), claim)
        .await
        .unwrap();
    let started = Instant::now();
    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.epoch(), 4);
    assert!(durability.status().waited_for_previous_owner);
    assert!(started.elapsed().as_secs() >= 4);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn release_and_stop_return_promptly() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, journal) = attach(&durability, state.path()).await;
    index.put_source_transaction(b"k", b"v").unwrap();
    drop((control, index, journal));
    // An unchanged state needs no second checkpoint; asking for one succeeds.
    durability.baseline().await.unwrap();
    durability.baseline().await.unwrap();
    let started = Instant::now();
    durability.release().await.unwrap();
    assert!(
        started.elapsed().as_secs_f64() < 2.0,
        "release took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn index_checkpoints_upload_only_new_files_and_restore_exactly() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (_control, _index, _journal) = attach(&durability, state.path()).await;
    let checkpoints = state.path().join("checkpoints");
    let first = checkpoints.join("first");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::write(first.join("000001.sst"), vec![1u8; 4096]).unwrap();
    std::fs::write(first.join("MANIFEST-000002"), b"manifest one").unwrap();
    std::fs::write(first.join("CURRENT"), b"MANIFEST-000002\n").unwrap();
    let second = checkpoints.join("second");
    std::fs::create_dir_all(&second).unwrap();
    // RocksDB checkpoints hard-link unchanged SST files.
    std::fs::hard_link(first.join("000001.sst"), second.join("000001.sst")).unwrap();
    std::fs::write(second.join("000003.sst"), vec![3u8; 2048]).unwrap();
    // Larger than one upload piece.
    let large: Vec<u8> = (0..9u32 << 20).map(|i| (i % 251) as u8).collect();
    std::fs::write(second.join("000005.sst"), &large).unwrap();
    std::fs::write(second.join("MANIFEST-000004"), b"manifest two").unwrap();
    std::fs::write(second.join("CURRENT"), b"MANIFEST-000004\n").unwrap();

    let uploader = durability.clone();
    let (first_dir, second_dir) = (first.clone(), second.clone());
    let shared_sst = first.join("000001.sst");
    let (one, two) = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::PermissionsExt;
        let one = uploader.upload_index_checkpoint(&first_dir).unwrap();
        // The linked SST's hash is remembered, so it is not read again.
        let permissions = |mode| std::fs::Permissions::from_mode(mode);
        std::fs::set_permissions(&shared_sst, permissions(0o000)).unwrap();
        let two = uploader.upload_index_checkpoint(&second_dir).unwrap();
        std::fs::set_permissions(&shared_sst, permissions(0o644)).unwrap();
        (one, two)
    })
    .await
    .unwrap();
    assert_eq!(one.0, one.1, "the first checkpoint uploads everything");
    // The shared SST is not uploaded again.
    assert_eq!(two.1, two.0 - 4096);

    let restored = tempfile::tempdir().unwrap();
    let target = restored.path().join("second");
    assert!(
        durability
            .download_index_checkpoint("second", &target)
            .await
            .unwrap()
    );
    for name in [
        "000001.sst",
        "000003.sst",
        "000005.sst",
        "MANIFEST-000004",
        "CURRENT",
    ] {
        assert_eq!(
            std::fs::read(target.join(name)).unwrap(),
            std::fs::read(second.join(name)).unwrap()
        );
    }
    assert!(
        !durability
            .download_index_checkpoint("missing", &restored.path().join("missing"))
            .await
            .unwrap()
    );
    let retainer = durability.clone();
    let deleted = tokio::task::spawn_blocking(move || {
        retainer
            .retain_index_checkpoints(&["second".to_owned()])
            .unwrap()
    })
    .await
    .unwrap();
    // The first manifest and its two unshared files (MANIFEST and CURRENT) go.
    assert_eq!(deleted, 3);
    assert!(
        !durability
            .download_index_checkpoint("first", &restored.path().join("first"))
            .await
            .unwrap()
    );
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn baselines_write_only_new_state_but_always_cover_new_files() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, journal) = attach(&durability, state.path()).await;
    // Cover the index and journal records written while attaching.
    durability.baseline().await.unwrap();
    let after_first = durability.status().stats.checkpoints;
    durability.baseline().await.unwrap();
    assert_eq!(
        durability.status().stats.checkpoints,
        after_first,
        "an unchanged state is not checkpointed again"
    );
    // A file that appears without any log record must still be covered.
    std::fs::write(state.path().join("source-identity.json"), b"{\"late\":1}").unwrap();
    durability.baseline().await.unwrap();
    assert_eq!(durability.status().stats.checkpoints, after_first + 1);
    durability.baseline().await.unwrap();
    assert_eq!(durability.status().stats.checkpoints, after_first + 1);
    drop((control, index, journal));
    durability.stop();

    let restored = tempfile::tempdir().unwrap();
    let durability = open(&operator, restored.path()).await;
    assert_eq!(durability.mode(), Mode::Restored);
    assert_eq!(
        std::fs::read(restored.path().join("source-identity.json")).unwrap(),
        b"{\"late\":1}"
    );
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_journal_of_many_segments_restores_exactly() {
    let small = JournalConfig {
        segment_bytes: 4096,
        max_frame_bytes: 1024,
        ..JournalConfig::default()
    };
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    {
        let (mut journal, _) = Journal::open(state.path().join("journal"), small.clone()).unwrap();
        for xid in 1..=40 {
            commit(&mut journal, xid, &[xid as u8; 600]);
        }
    }
    let durability = open(&operator, state.path()).await;
    let control = ControlStore::open(state.path().join("control")).unwrap();
    durability.attach_control(&control).await.unwrap();
    durability.baseline().await.unwrap();
    let (journal, _) = Journal::open(state.path().join("journal"), small.clone()).unwrap();
    let expected = transactions(&journal);
    let segments = journal.layout().segments.len();
    assert!(segments > 4, "only {segments} segments");
    drop((journal, control));
    durability.release().await.unwrap();

    let restored = tempfile::tempdir().unwrap();
    let durability = open(&operator, restored.path()).await;
    assert_eq!(durability.mode(), Mode::Restored);
    let (journal, recovery) = Journal::open(restored.path().join("journal"), small).unwrap();
    assert_eq!(recovery.truncated_bytes, 0);
    assert_eq!(journal.layout().segments.len(), segments);
    assert_eq!(transactions(&journal), expected);
    drop(journal);
    durability.stop();
}

fn faulty() -> (faults::Faults, Operator) {
    let faults = faults::Faults::default();
    let operator = Operator::new(Memory::default())
        .unwrap()
        .finish()
        .layer(faults.clone());
    (faults, operator)
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_writes_retry_transient_failures() {
    let (faults, operator) = faulty();
    for prefix in ["format.json", "owners/", "log/"] {
        faults.fail(prefix, 2);
    }
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.mode(), Mode::Fresh);
    assert_eq!(faults.remaining(), 0);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn baseline_checkpoints_retry_transient_write_failures() {
    let (faults, operator) = faulty();
    let state = tempfile::tempdir().unwrap();
    {
        // Local state to migrate, so the baseline uploads journal parts.
        let (mut journal, _) =
            Journal::open(state.path().join("journal"), JournalConfig::default()).unwrap();
        commit(&mut journal, 1, b"legacy");
    }
    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.mode(), Mode::Migrated);
    for prefix in ["control/", "journals/", "checkpoints/"] {
        faults.fail(prefix, 2);
    }
    let (control, index, mut journal) = attach(&durability, state.path()).await;
    assert_eq!(faults.remaining(), 0);
    assert!(durability.status().stats.retries >= 6);
    commit(&mut journal, 2, b"after");
    let expected = transactions(&journal);
    durability.barrier().await.unwrap();
    drop((control, index, journal));
    durability.stop();

    let restored = tempfile::tempdir().unwrap();
    let durability = open(&operator, restored.path()).await;
    assert_eq!(durability.mode(), Mode::Restored);
    let (journal, _) =
        Journal::open(restored.path().join("journal"), JournalConfig::default()).unwrap();
    assert_eq!(transactions(&journal), expected);
    drop(journal);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_baseline_waits_like_a_barrier_and_fails_once_replaced() {
    let (faults, operator) = faulty();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    faults.fail("control/", usize::MAX);
    let control = ControlStore::open(state.path().join("control")).unwrap();
    durability.attach_control(&control).await.unwrap();
    let baseline = tokio::spawn({
        let durability = durability.clone();
        async move { durability.baseline().await }
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !baseline.is_finished(),
        "a transient failure must not fail the baseline"
    );
    // A successor claims the state: the waiting baseline now fails.
    operator
        .write(
            &format!("owners/{:020}.json", durability.epoch() + 1),
            b"{}".to_vec(),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), baseline)
        .await
        .expect("a replaced owner stops waiting")
        .unwrap();
    assert!(result.is_err());
    assert!(durability.failure().is_some());
    drop(control);
    durability.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn flush_and_release_give_up_after_one_lease_while_storage_is_unavailable() {
    let (faults, operator) = faulty();
    let state = tempfile::tempdir().unwrap();
    let durability = open(&operator, state.path()).await;
    let (control, index, journal) = attach(&durability, state.path()).await;
    faults.fail("", usize::MAX);
    index.put_source_transaction(b"k", b"v").unwrap();
    let started = Instant::now();
    assert!(durability.flush().await.is_err());
    assert!(started.elapsed() < Duration::from_secs(8));
    // Once storage is back, the same records flush.
    faults.clear();
    durability.flush().await.unwrap();

    faults.fail("", usize::MAX);
    index.put_source_transaction(b"k", b"w").unwrap();
    drop((control, index, journal));
    let started = Instant::now();
    let released = tokio::time::timeout(Duration::from_secs(20), durability.release())
        .await
        .expect("release must not hang while storage is unavailable");
    assert!(released.is_err());
    durability.stop();
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "shutdown took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_baseline_that_cannot_read_local_state_fails_instead_of_hanging() {
    let operator = Operator::new(Memory::default()).unwrap().finish();
    let state = tempfile::tempdir().unwrap();
    {
        let (mut journal, _) =
            Journal::open(state.path().join("journal"), JournalConfig::default()).unwrap();
        commit(&mut journal, 1, b"legacy");
    }
    // A segment the baseline lists but cannot read.
    let segment = state
        .path()
        .join("journal")
        .join(flow_ingress_journal::segment_file_name(0));
    std::fs::remove_file(&segment).unwrap();
    std::os::unix::fs::symlink(state.path().join("missing"), &segment).unwrap();
    let durability = open(&operator, state.path()).await;
    assert_eq!(durability.mode(), Mode::Migrated);
    let control = ControlStore::open(state.path().join("control")).unwrap();
    durability.attach_control(&control).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), durability.baseline())
        .await
        .expect("a local error must fail the baseline, not hang it");
    assert!(result.is_err());
    // Only the request failed; replication itself keeps running.
    assert!(durability.failure().is_none());
    drop(control);
    durability.stop();
}
