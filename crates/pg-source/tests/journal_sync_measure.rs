//! Measures how much of the capture loop's time the journal sync takes.
//! Results and method: docs/benchmarks/journal-sync.md.
//!
//! Drains a logical slot through the real pgoutput decoder, capture assembler
//! and journal, flushing with the daemon's group policy (`FLOW_MEASURE_GROUP`
//! transactions, default 256; 4 MiB; or 5 ms after the first pending commit).
//! Reports wall time, time blocked in the flush, the capture thread's CPU time
//! and the WAL sender's CPU time. `FLOW_MEASURE_SYNC_DELAY_US` adds a blocking
//! sleep to every flush to model a slower disk. Schema registration,
//! projection and validation are not run, so real capture spends more CPU per
//! transaction and a smaller share in the sync.
//!
//! The database needs `CREATE TABLE bench (id bigint GENERATED ALWAYS AS
//! IDENTITY PRIMARY KEY, payload text NOT NULL)`, `CREATE PUBLICATION measure
//! FOR TABLE bench` and a `measure` pgoutput slot created before the load. The
//! slot is never acknowledged, so every run replays the same backlog:
//!
//! FLOW_MEASURE_PG=... FLOW_MEASURE_DIR=... FLOW_MEASURE_TRANSACTIONS=...
//! cargo test --release -p flow-pg-source --test journal_sync_measure --
//! --ignored --nocapture

use flow_ingress_journal::{Journal, JournalConfig};
use flow_model::{Column, ColumnType, PgLsn, SourceId, TableId, TableSchema};
use flow_pg_source::{
    CaptureAssembler, PgOutputSource, PostgresSource, SourceEvent, SpoolConfig, TransactionSpool,
};
use std::time::{Duration, Instant};

const GROUP_BYTES: u64 = 4 << 20;
const GROUP_DELAY: Duration = Duration::from_millis(5);

fn ticks(path: &str) -> f64 {
    let stat = std::fs::read_to_string(path).unwrap();
    let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
    // utime and stime are fields 14 and 15; after the comm they are 11 and 12.
    let total: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    total as f64 / 100.0
}

#[test]
#[ignore = "needs a PostgreSQL slot with a WAL backlog"]
fn journal_sync_share_of_capture() {
    let url = std::env::var("FLOW_MEASURE_PG").expect("FLOW_MEASURE_PG");
    let dir = std::env::var("FLOW_MEASURE_DIR").expect("FLOW_MEASURE_DIR");
    let group: usize = std::env::var("FLOW_MEASURE_GROUP").map_or(256, |v| v.parse().unwrap());
    let delay = Duration::from_micros(
        std::env::var("FLOW_MEASURE_SYNC_DELAY_US").map_or(0, |v| v.parse().unwrap()),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    // block_on runs the capture loop on this thread; the connection tasks run
    // on the workers, as in the daemon.
    runtime.block_on(async move {
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        // WAL after the last commit (checkpoints, for example) never reaches
        // the stream, so stop after the expected number of commits.
        let expected: u64 = std::env::var("FLOW_MEASURE_TRANSACTIONS")
            .expect("FLOW_MEASURE_TRANSACTIONS")
            .parse()
            .unwrap();
        let mut config: tokio_postgres::Config = url.parse().unwrap();
        config.replication_mode(tokio_postgres::config::ReplicationMode::Logical);
        let (replication, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(connection);
        let mut source = PgOutputSource::start(&replication, "measure", "measure", PgLsn(0), 64 << 20)
            .await
            .unwrap();

        let root = std::path::Path::new(&dir);
        let _ = std::fs::remove_dir_all(root);
        let (mut journal, _) = Journal::open(
            root.join("journal"),
            JournalConfig { max_open_transactions: (2 * group).max(1024), ..JournalConfig::default() },
        ).unwrap();
        let spool = TransactionSpool::open(root.join("spool"), SpoolConfig::default()).unwrap();
        let schema = TableSchema {
            table_id: TableId(0),
            version: 1,
            columns: vec![
                Column { field_id: 1, name: "id".into(), data_type: ColumnType::Int64, nullable: false },
                Column { field_id: 2, name: "payload".into(), data_type: ColumnType::String, nullable: false },
            ],
            primary_key: vec![0],
            append_only: false,
        };
        let relid: u32 = sql
            .query_one("SELECT 'bench'::regclass::oid", &[])
            .await
            .unwrap()
            .get(0);
        let schema = TableSchema { table_id: TableId(relid), ..schema };
        let mut assembler = CaptureAssembler::new(SourceId("measure".into()), spool, [schema], 4 << 20)
            .unwrap()
            .with_pending_commit_limit(group)
            .unwrap();
        let walsender: i32 = sql
            .query_one("SELECT active_pid FROM pg_replication_slots WHERE slot_name = 'measure'", &[])
            .await
            .unwrap()
            .get(0);
        let walsender = format!("/proc/{walsender}/stat");

        let (mut transactions, mut rows, mut flushes) = (0u64, 0u64, 0u64);
        let mut flush_time = Duration::ZERO;
        let mut flush = |assembler: &mut CaptureAssembler, journal: &mut Journal, flushes: &mut u64| {
            let started = Instant::now();
            let flushed = assembler.flush_commits(journal).unwrap();
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            flush_time += started.elapsed();
            *flushes += 1;
            flushed.len() as u64
        };
        let (cpu0, wal0, started) = (ticks("/proc/thread-self/stat"), ticks(&walsender), Instant::now());
        let mut deadline: Option<tokio::time::Instant> = None;
        let mut commits = 0u64;
        while commits < expected {
            let event = match deadline {
                Some(at) => match tokio::time::timeout_at(at, source.next()).await {
                    Ok(event) => event,
                    Err(_) => {
                        transactions += flush(&mut assembler, &mut journal, &mut flushes);
                        deadline = None;
                        continue;
                    }
                },
                None => tokio::time::timeout(Duration::from_secs(10), source.next())
                    .await
                    .unwrap_or_else(|_| panic!("idle after {commits} commits")),
            };
            let event = event.unwrap().expect("stream ended");
            if matches!(event, SourceEvent::Commit { .. }) {
                commits += 1;
            }
            if matches!(event, SourceEvent::Insert { .. }) {
                rows += 1;
            }
            assembler.push_buffered_at(event, source.received_lsn, &mut journal).unwrap();
            if assembler.pending_commit_count() > 0 {
                deadline.get_or_insert_with(|| tokio::time::Instant::now() + GROUP_DELAY);
                if assembler.pending_commit_count() >= group
                    || assembler.pending_commit_bytes() >= GROUP_BYTES
                {
                    transactions += flush(&mut assembler, &mut journal, &mut flushes);
                    deadline = None;
                }
            }
        }
        transactions += flush(&mut assembler, &mut journal, &mut flushes);
        let wall = started.elapsed().as_secs_f64();
        let cpu = ticks("/proc/thread-self/stat") - cpu0;
        let wal = ticks(&walsender) - wal0;
        let flush_s = flush_time.as_secs_f64();
        println!(
            "{{\"group\":{group},\"delay_us\":{},\"transactions\":{transactions},\"rows\":{rows},\"flushes\":{flushes},\"wall_s\":{wall:.3},\"txn_per_s\":{:.0},\"flush_s\":{flush_s:.3},\"flush_share\":{:.3},\"capture_cpu_s\":{cpu:.2},\"capture_cpu_share\":{:.3},\"walsender_cpu_s\":{wal:.2},\"walsender_cpu_share\":{:.3}}}",
            delay.as_micros(),
            transactions as f64 / wall,
            flush_s / wall,
            cpu / wall,
            wal / wall,
        );
    });
}
