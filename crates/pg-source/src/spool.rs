//! Transaction-local sequential spill. It is intentionally not an ACK durability
//! boundary: after a disconnect, start fresh and replay from the journal LSN.

use crate::{Error, Result};
use fs2::FileExt;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

const HEADER: u64 = 8;

#[derive(Clone, Debug)]
pub struct SpoolConfig {
    pub segment_bytes: u64,
    pub quota_bytes: u64,
    pub max_chunk_bytes: u32,
    pub max_transactions: usize,
    pub max_subtransactions: usize,
}
impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            quota_bytes: 16 << 30,
            max_chunk_bytes: 4 << 20,
            max_transactions: 1024,
            // Tracked per open transaction, only for subtransactions that
            // captured rows: roughly 100 bytes of memory each.
            max_subtransactions: 1 << 20,
        }
    }
}

impl SpoolConfig {
    /// Validate frame, segment, quota, and transaction limits without opening storage.
    pub fn validate(&self) -> Result<()> {
        if self.max_chunk_bytes == 0
            || u64::from(self.max_chunk_bytes) + HEADER > self.segment_bytes
            || self.segment_bytes > self.quota_bytes
            || self.max_transactions == 0
            || self.max_subtransactions == 0
        {
            return Err(Error::Config(
                "invalid spool frame, segment, quota, or transaction limits",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Savepoint {
    xid: u32,
    segment: u64,
    offset: u64,
    bytes: u64,
}

struct Transaction {
    file: File,
    segment: u64,
    offset: u64,
    bytes: u64,
    savepoints: Vec<Savepoint>,
    subtransactions: HashMap<u32, usize>,
}

pub struct TransactionSpool {
    root: PathBuf,
    config: SpoolConfig,
    _lock: File,
    transactions: HashMap<u32, Transaction>,
    bytes: u64,
}

impl TransactionSpool {
    /// `root` must be a dedicated spool directory. Old `txn-*` directories are
    /// discarded because only terminal transactions in the journal are durable.
    pub fn open(root: impl AsRef<Path>, config: SpoolConfig) -> Result<Self> {
        config.validate()?;
        let root = root.as_ref().to_owned();
        fs::create_dir_all(&root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("writer.lock"))?;
        lock.try_lock_exclusive()?;
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && entry.file_name().to_str().is_some_and(|n| {
                    n.strip_prefix("txn-")
                        .is_some_and(|x| x.parse::<u32>().is_ok())
                })
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(Self {
            root,
            config,
            _lock: lock,
            transactions: HashMap::new(),
            bytes: 0,
        })
    }

    pub fn begin(&mut self, xid: u32) -> Result<()> {
        if self.transactions.contains_key(&xid) {
            return Err(Error::Protocol("duplicate transaction in spool"));
        }
        if self.transactions.len() >= self.config.max_transactions {
            return Err(Error::SpoolTransactions {
                limit: self.config.max_transactions,
            });
        }
        let path = self.root.join(format!("txn-{xid}"));
        let started = Instant::now();
        let opened = (|| {
            fs::create_dir(&path)?;
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path.join("0"))
        })();
        metrics::histogram!(
            "flow_capture_phase_seconds",
            "phase" => "spool_begin",
            "result" => if opened.is_ok() { "success" } else { "error" }
        )
        .record(started.elapsed().as_secs_f64());
        let file = opened?;
        self.transactions.insert(
            xid,
            Transaction {
                file,
                segment: 0,
                offset: 0,
                bytes: 0,
                savepoints: Vec::new(),
                subtransactions: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn bytes_used(&self) -> u64 {
        self.bytes
    }
    pub fn max_chunk_bytes(&self) -> u32 {
        self.config.max_chunk_bytes
    }

    /// Payloads should contain complete encoded mutation chunks. No row payload
    /// is retained in memory after this call returns.
    pub fn append(&mut self, xid: u32, subxid: u32, payload: &[u8]) -> Result<()> {
        if payload.len() > self.config.max_chunk_bytes as usize {
            return Err(Error::Protocol(
                "capture chunk exceeds the spool frame limit",
            ));
        }
        let length = payload.len() as u64 + HEADER;
        if self.bytes + length > self.config.quota_bytes {
            return Err(Error::SpoolQuota {
                used: self.bytes,
                requested: length,
                quota: self.config.quota_bytes,
            });
        }
        let txn = self
            .transactions
            .get_mut(&xid)
            .ok_or(Error::Protocol("spool append without begin"))?;
        if subxid != xid && !txn.subtransactions.contains_key(&subxid) {
            if txn.savepoints.len() >= self.config.max_subtransactions {
                return Err(Error::SpoolSubtransactions {
                    xid,
                    limit: self.config.max_subtransactions,
                });
            }
            txn.subtransactions.insert(subxid, txn.savepoints.len());
            txn.savepoints.push(Savepoint {
                xid: subxid,
                segment: txn.segment,
                offset: txn.offset,
                bytes: txn.bytes,
            });
        }
        if txn.offset + length > self.config.segment_bytes {
            txn.segment += 1;
            txn.file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(self.root.join(format!("txn-{xid}/{}", txn.segment)))?;
            txn.offset = 0;
        }
        let mut header = [0; HEADER as usize];
        header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[4..].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
        txn.file.write_all(&header)?;
        txn.file.write_all(payload)?;
        txn.offset += length;
        txn.bytes += length;
        self.bytes += length;
        Ok(())
    }

    /// Bytes this transaction has spooled: the position of its next chunk.
    pub fn position(&self, xid: u32) -> Result<u64> {
        Ok(self
            .transactions
            .get(&xid)
            .ok_or(Error::Protocol("position of unknown spool transaction"))?
            .bytes)
    }

    /// The position `abort(xid, subxid)` truncates to, if the subtransaction
    /// spooled anything: chunks at or after it do not survive that rollback.
    pub fn savepoint(&self, xid: u32, subxid: u32) -> Option<u64> {
        let txn = self.transactions.get(&xid)?;
        let &index = txn.subtransactions.get(&subxid)?;
        Some(txn.savepoints[index].bytes)
    }

    /// Match PostgreSQL's serial streamed-apply rollback: truncate to the first
    /// change of the aborted subtransaction, also removing its descendants.
    /// Filtering only rows whose XID equals `subxid` would retain child changes.
    pub fn abort(&mut self, xid: u32, subxid: u32) -> Result<()> {
        if xid == subxid {
            return self.discard(xid);
        }
        let txn = self
            .transactions
            .get_mut(&xid)
            .ok_or(Error::Protocol("abort of unknown spool transaction"))?;
        let Some(&index) = txn.subtransactions.get(&subxid) else {
            return Ok(());
        };
        let start = txn.savepoints[index];
        for segment in start.segment + 1..=txn.segment {
            fs::remove_file(self.root.join(format!("txn-{xid}/{segment}")))?;
        }
        txn.file = OpenOptions::new()
            .append(true)
            .open(self.root.join(format!("txn-{xid}/{}", start.segment)))?;
        txn.file.set_len(start.offset)?;
        txn.segment = start.segment;
        txn.offset = start.offset;
        self.bytes -= txn.bytes - start.bytes;
        txn.bytes = start.bytes;
        for savepoint in txn.savepoints.drain(index..) {
            txn.subtransactions.remove(&savepoint.xid);
        }
        Ok(())
    }

    /// Read surviving chunks in order using one bounded reusable buffer. Retain
    /// the spool until the destination journal terminal commit is durable.
    pub fn replay(&self, xid: u32, mut consume: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let txn = self
            .transactions
            .get(&xid)
            .ok_or(Error::Protocol("replay of unknown spool transaction"))?;
        let mut buffer = Vec::new();
        for segment in 0..=txn.segment {
            let mut file = File::open(self.root.join(format!("txn-{xid}/{segment}")))?;
            let size = file.metadata()?.len();
            let mut offset = 0;
            while offset < size {
                let mut header = [0; HEADER as usize];
                file.read_exact(&mut header)?;
                let len = u32::from_le_bytes(header[..4].try_into().unwrap());
                if len > self.config.max_chunk_bytes || offset + HEADER + u64::from(len) > size {
                    return Err(Error::Protocol("corrupt transaction spool frame"));
                }
                buffer.resize(len as usize, 0);
                file.read_exact(&mut buffer)?;
                if crc32fast::hash(&buffer) != u32::from_le_bytes(header[4..].try_into().unwrap()) {
                    return Err(Error::Protocol("transaction spool checksum mismatch"));
                }
                consume(&buffer)?;
                offset += HEADER + u64::from(len);
            }
        }
        Ok(())
    }

    pub fn discard(&mut self, xid: u32) -> Result<()> {
        let txn = self
            .transactions
            .remove(&xid)
            .ok_or(Error::Protocol("discard of unknown spool transaction"))?;
        let started = Instant::now();
        let discarded = fs::remove_dir_all(self.root.join(format!("txn-{xid}")));
        metrics::histogram!(
            "flow_capture_phase_seconds",
            "phase" => "spool_disposal",
            "result" => if discarded.is_ok() { "success" } else { "error" }
        )
        .record(started.elapsed().as_secs_f64());
        discarded?;
        self.bytes -= txn.bytes;
        Ok(())
    }
}
