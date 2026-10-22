//! Object storage access through OpenDAL. Any backend with conditional
//! creation works; S3, a local filesystem and memory are built in.
use crate::format::{MAX_OBJECT_BYTES, key_number};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use opendal::{ErrorKind, Operator, layers::TimeoutLayer, services};
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

/// Staging directory for filesystem creates, outside every listed prefix.
const STAGING: &str = ".staging";

/// Build an operator rooted at the configured prefix. `endpoint` selects an
/// S3-compatible service instead of AWS, using path-style addressing.
pub fn operator(
    uri: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    timeout: Duration,
) -> Result<Operator> {
    let (scheme, rest) = uri
        .split_once("://")
        .context("durability uri needs a scheme, such as s3://bucket/prefix")?;
    let operator = match scheme {
        "s3" => {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            let prefix = prefix.trim_matches('/');
            ensure!(!bucket.is_empty(), "durability uri names no bucket");
            ensure!(
                !prefix.is_empty(),
                "durability uri needs a dedicated prefix below the bucket"
            );
            let mut builder = services::S3::default()
                .bucket(bucket)
                .root(&format!("/{prefix}/"));
            let region = region
                .map(str::to_owned)
                .or_else(|| std::env::var("AWS_REGION").ok())
                .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok());
            if let Some(region) = region {
                builder = builder.region(&region);
            }
            if let Some(endpoint) = endpoint {
                builder = builder.endpoint(endpoint);
            }
            Operator::new(builder)?.finish()
        }
        "file" => {
            ensure!(
                rest.starts_with('/'),
                "file durability uri must be absolute, such as file:///mnt/flow"
            );
            Operator::new(services::Fs::default().root(rest))?.finish()
        }
        "memory" => Operator::new(services::Memory::default())?.finish(),
        other => bail!("unsupported durability uri scheme {other}; use s3, file or memory"),
    };
    Ok(operator.layer(
        TimeoutLayer::new()
            .with_timeout(timeout)
            .with_io_timeout(timeout),
    ))
}

/// The key already exists. S3 reports a conditional write racing another
/// one as a temporary 409 conflict; that one is retried instead.
fn conflict(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
    ) && !error.is_temporary()
}

/// Whether a failed request may succeed when retried.
pub(crate) fn temporary(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<opendal::Error>()
        .is_some_and(opendal::Error::is_temporary)
}

pub(crate) enum Created {
    New,
    /// The key exists with exactly these bytes, as after a lost response.
    Same,
    /// Another writer created the key first.
    Different(Bytes),
}

/// Create `key` only if it is absent. A conflict is resolved by reading the
/// existing object, so a retried request whose first response was lost is
/// still recognised as our own write.
pub(crate) async fn create(operator: &Operator, key: &str, bytes: Bytes) -> Result<Created> {
    if operator.info().scheme() == "fs" {
        let root = PathBuf::from(operator.info().root());
        let (path, data) = (key.to_owned(), bytes.clone());
        let created = tokio::task::spawn_blocking(move || create_file(&root, &path, &data))
            .await
            .context("create task")?
            .with_context(|| format!("create {key}"))?;
        if created {
            return Ok(Created::New);
        }
    } else {
        match operator
            .write_with(key, bytes.clone())
            .if_not_exists(true)
            .await
        {
            Ok(_) => return Ok(Created::New),
            Err(error) if conflict(&error) => {}
            Err(error) => return Err(error).with_context(|| format!("create {key}")),
        }
    }
    let existing = read(operator, key).await?;
    Ok(if existing == bytes {
        Created::Same
    } else {
        Created::Different(existing)
    })
}

/// Create `key` below a filesystem root only if it is absent, never exposing
/// a partial object: OpenDAL writes in place, so a failed write would leave a
/// truncated object that a retry then mistakes for another owner's. The
/// bytes are synced in a private staging file, which is then hard-linked
/// into place; linking fails if the key exists. Returns false then.
fn create_file(root: &Path, key: &str, bytes: &[u8]) -> std::io::Result<bool> {
    let target = root.join(key);
    let parent = target.parent().unwrap_or(root);
    std::fs::create_dir_all(parent)?;
    let staging = root.join(STAGING);
    std::fs::create_dir_all(&staging)?;
    let temporary = staging.join(uuid::Uuid::new_v4().to_string());
    let result = (|| {
        let mut file = std::fs::File::create_new(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match std::fs::hard_link(&temporary, &target) {
            Ok(()) => {
                std::fs::File::open(parent)?.sync_all()?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        }
    })();
    let _ = std::fs::remove_file(&temporary);
    result
}

pub(crate) async fn read(operator: &Operator, key: &str) -> Result<Bytes> {
    let bytes = operator
        .read(key)
        .await
        .with_context(|| format!("read {key}"))?
        .to_bytes();
    ensure!(
        bytes.len() as u64 <= MAX_OBJECT_BYTES,
        "{key} exceeds the durability object limit"
    );
    Ok(bytes)
}

pub(crate) async fn read_optional(operator: &Operator, key: &str) -> Result<Option<Bytes>> {
    match operator.read(key).await {
        Ok(buffer) => Ok(Some(buffer.to_bytes())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {key}")),
    }
}

/// Numbered objects directly below `directory`, keyed by their number.
/// Uses `start_after` where the backend supports it.
pub(crate) async fn list_numbered(
    operator: &Operator,
    directory: &str,
    after: Option<u64>,
) -> Result<BTreeMap<u64, String>> {
    let mut listing = operator.list_with(directory);
    let start = after.map(|number| format!("{directory}{number:020}~"));
    if let Some(start) = &start
        && operator.info().full_capability().list_with_start_after
    {
        listing = listing.start_after(start);
    }
    let entries = match listing.await {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error).with_context(|| format!("list {directory}")),
    };
    let mut numbered = BTreeMap::new();
    for entry in entries {
        if entry.metadata().is_dir() {
            continue;
        }
        let path = entry.path();
        let key = path.trim_start_matches('/');
        if let Some(number) = key_number(key)
            && after.is_none_or(|after| number > after)
        {
            numbered.insert(number, key.to_owned());
        }
    }
    Ok(numbered)
}

/// Every object below `directory`, at any depth.
pub(crate) async fn list_recursive(operator: &Operator, directory: &str) -> Result<Vec<String>> {
    match operator.list_with(directory).recursive(true).await {
        Ok(entries) => Ok(entries
            .into_iter()
            .filter(|entry| !entry.metadata().is_dir())
            .map(|entry| entry.path().trim_start_matches('/').to_owned())
            .collect()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error).with_context(|| format!("list {directory}")),
    }
}

pub(crate) async fn delete_all(operator: &Operator, keys: Vec<String>) -> Result<usize> {
    let count = keys.len();
    if count > 0 {
        operator
            .delete_iter(keys)
            .await
            .context("delete retired durability objects")?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filesystem_creates_are_exclusive_and_never_partial() {
        let root = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", root.path().display());
        let operator = super::operator(&uri, None, None, Duration::from_secs(4)).unwrap();
        let ours = Bytes::from_static(b"ours");
        assert!(matches!(
            create(&operator, "log/a.batch", ours.clone())
                .await
                .unwrap(),
            Created::New
        ));
        assert!(matches!(
            create(&operator, "log/a.batch", ours).await.unwrap(),
            Created::Same
        ));
        assert!(matches!(
            create(&operator, "log/a.batch", Bytes::from_static(b"theirs"))
                .await
                .unwrap(),
            Created::Different(_)
        ));
        // Racing creates of one key: exactly one wins, the rest see it whole.
        let racers = (0..8u8).map(|index| {
            let operator = operator.clone();
            tokio::spawn(async move {
                create(&operator, "log/b.batch", Bytes::from(vec![index; 1 << 16])).await
            })
        });
        let mut created = 0;
        for racer in racers {
            match racer.await.unwrap().unwrap() {
                Created::New => created += 1,
                Created::Same => {}
                Created::Different(existing) => assert_eq!(existing.len(), 1 << 16),
            }
        }
        assert_eq!(created, 1);
        assert_eq!(read(&operator, "log/b.batch").await.unwrap().len(), 1 << 16);
        // Staging files stay outside every listed prefix and are removed.
        assert_eq!(list_recursive(&operator, "log/").await.unwrap().len(), 2);
        assert_eq!(
            std::fs::read_dir(root.path().join(STAGING))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn a_racing_conditional_write_is_retried_not_resolved() {
        let existing = opendal::Error::new(ErrorKind::ConditionNotMatch, "precondition failed");
        assert!(conflict(&existing));
        // S3 409 ConditionalRequestConflict: the other write may not exist yet.
        let racing = opendal::Error::new(ErrorKind::ConditionNotMatch, "conflict").set_temporary();
        assert!(!conflict(&racing));
        let error = Err::<(), _>(racing).context("create key").unwrap_err();
        assert!(temporary(&error));
    }
}
