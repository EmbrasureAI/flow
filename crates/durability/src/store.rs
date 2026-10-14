//! Object storage access through OpenDAL. Any backend with conditional
//! creation works; S3, a local filesystem and memory are built in.
use crate::format::{MAX_OBJECT_BYTES, key_number};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use opendal::{ErrorKind, Operator, layers::TimeoutLayer, services};
use std::{collections::BTreeMap, time::Duration};

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

pub(crate) fn conflict(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
    )
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
    match operator
        .write_with(key, bytes.clone())
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(Created::New),
        Err(error) if conflict(&error) => {
            let existing = read(operator, key).await?;
            Ok(if existing == bytes {
                Created::Same
            } else {
                Created::Different(existing)
            })
        }
        Err(error) => Err(error).with_context(|| format!("create {key}")),
    }
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
