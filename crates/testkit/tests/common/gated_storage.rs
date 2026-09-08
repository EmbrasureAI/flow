use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug)]
pub struct Gate {
    pub reads: std::sync::Mutex<std::collections::HashMap<String, usize>>,
    pub active: AtomicUsize,
    pub started: Semaphore,
    pub release: Semaphore,
    pub finished: Notify,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            reads: Default::default(),
            active: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            finished: Notify::new(),
        }
    }
}
struct ActiveRead(Arc<Gate>);
impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.finished.notify_one();
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GatedStorage {
    #[serde(skip)]
    pub gate: Arc<Gate>,
    pub owner_path: String,
    pub sibling_path: String,
    pub fail: bool,
}
#[typetag::serde]
impl StorageFactory for GatedStorage {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
#[typetag::serde]
impl Storage for GatedStorage {
    async fn exists(&self, p: &str) -> Result<bool> {
        LocalFsStorage.exists(p).await
    }
    async fn metadata(&self, p: &str) -> Result<FileMetadata> {
        LocalFsStorage.metadata(p).await
    }
    async fn read(&self, p: &str) -> Result<Bytes> {
        *self
            .gate
            .reads
            .lock()
            .unwrap()
            .entry(p.to_owned())
            .or_default() += 1;
        LocalFsStorage.read(p).await
    }
    async fn reader(&self, p: &str) -> Result<Box<dyn FileRead>> {
        if (p == self.owner_path || p == self.sibling_path) && !self.gate.release.is_closed() {
            self.gate.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveRead(self.gate.clone());
            self.gate.started.add_permits(1);
            // In the sibling case, D stays pending while E fails and cancels D.
            if p == self.owner_path && !self.sibling_path.is_empty() {
                futures::future::pending::<()>().await;
            } else {
                // The watchdog bounds failures in callers that never release.
                if let Ok(Ok(permit)) = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    self.gate.release.acquire(),
                )
                .await
                {
                    permit.forget();
                }
            }
            if self.fail {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected delete read failure",
                ));
            }
        }
        LocalFsStorage.reader(p).await
    }
    async fn write(&self, p: &str, b: Bytes) -> Result<()> {
        LocalFsStorage.write(p, b).await
    }
    async fn writer(&self, p: &str) -> Result<Box<dyn FileWrite>> {
        LocalFsStorage.writer(p).await
    }
    async fn delete(&self, p: &str) -> Result<()> {
        LocalFsStorage.delete(p).await
    }
    async fn delete_prefix(&self, p: &str) -> Result<()> {
        LocalFsStorage.delete_prefix(p).await
    }
    async fn delete_stream(&self, p: BoxStream<'static, String>) -> Result<()> {
        LocalFsStorage.delete_stream(p).await
    }
    fn new_input(&self, p: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), p.into()))
    }
    fn new_output(&self, p: &str) -> Result<OutputFile> {
        LocalFsStorage.new_output(p)
    }
}
