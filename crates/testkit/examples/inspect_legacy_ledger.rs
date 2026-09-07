//! Inspect a stopped fixture's copied control directory using existing storage APIs.
//! The caller supplies only the bounded backlog identities it needs to prove.
use anyhow::{Context, Result, ensure};
use bincode::Options;
use flow_model::{SourceTransaction, TableId};
use flow_state_store::ControlStore;
use std::{collections::BTreeMap, path::PathBuf};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(args.next().context("copied control directory required")?);
    let source = args.next().context("source ID required")?;
    let ends = args
        .map(|arg| arg.parse::<u64>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        !ends.is_empty() && ends.len() <= 32,
        "expected 1..32 backlog LSNs"
    );
    ensure!(
        path.join("CURRENT").is_file() && path.join("IDENTITY").is_file(),
        "copied RocksDB is missing"
    );
    let store = ControlStore::open(path)?;
    let mut entries = Vec::new();
    for end in ends {
        let mut key = b"flow-ledger/v1/".to_vec();
        key.extend((source.len() as u64).to_be_bytes());
        key.extend(source.as_bytes());
        key.extend(b"/txn/");
        key.extend(end.to_be_bytes());
        let bytes = store
            .source_transaction(&key)?
            .context("expected backlog ledger entry missing")?;
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes();
        let (format, transaction) = if let Some(payload) = bytes.strip_prefix(b"FLLEDG02") {
            let (transaction, _): (
                flow_ingress_journal::legacy::BoundedSourceTransaction,
                BTreeMap<TableId, i64>,
            ) = codec.deserialize(payload)?;
            ("FLLEDG02", transaction.into_current()?)
        } else if let Some(payload) = bytes.strip_prefix(b"FLLEDG03") {
            let (transaction, _): (SourceTransaction, BTreeMap<TableId, i64>) =
                codec.deserialize(payload)?;
            ("FLLEDG03", transaction)
        } else {
            anyhow::bail!("unrecognized ledger envelope");
        };
        ensure!(
            transaction.source_id.0 == source && transaction.end_lsn.0 == end,
            "ledger key and payload identity disagree"
        );
        entries.push(serde_json::json!({"format": format, "xid": transaction.xid,
            "end_lsn": end, "source_id": source, "encoded_bytes": bytes.len()}));
    }
    println!("{}", serde_json::to_string(&entries)?);
    Ok(())
}
