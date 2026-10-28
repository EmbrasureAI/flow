# Journal sync on the capture thread

Measured 2026-10-26 to decide whether to move the journal `sync_data` off the
PostgreSQL capture thread. Conclusion: not now. The sync only limits catch-up
throughput on backlogs of small transactions, and a background sync does not
shorten acknowledgement, which waits for the sync either way. A larger commit
group recovers much of the loss on slow disks with far less risk, but needs its
own limit design first.

## Method

`crates/pg-source/tests/journal_sync_measure.rs` drains a logical slot through
the real pgoutput decoder, `CaptureAssembler` and `Journal`, flushing like the
daemon: at 256 pending commits, 4 MiB, or 5 ms after the first pending commit.
It reports wall time, time blocked in `flush_commits`, the capture thread's
CPU time and the WAL sender's CPU time. Schema registration, projection and
validation are not run, so the daemon spends more CPU per transaction than the
harness and a smaller share of its time in the sync: these results bound the
benefit from above.

- Host: 8 vCPU, 16 GiB, Amazon Linux 2023, kernel 6.18; PostgreSQL 17.11 on
  the same host. The journal disk's `fdatasync` after a 64 KiB append takes
  0.18 ms at p50 and 0.26 ms at p99, like local NVMe.
- Backlog: 1,000,000 single-row transactions (an identity key and a 96-byte
  text payload, 265 MB of WAL), loaded with pgbench before the run. The slot
  is never acknowledged, so every run replays the same backlog.
- Slower disks are modelled by sleeping inside each flush
  (`FLOW_MEASURE_SYNC_DELAY_US`). The sync blocks without using CPU, so from
  the capture thread a longer sync and a sleep look the same. EBS gp3 syncs
  usually take around a millisecond.
- tmpfs makes the sync free and gives the ceiling. It also makes journal
  writes cheaper, so it overstates what overlapping the sync could gain.

## Results

Throughput in transactions per second, median of two or three runs. Flush
share is the fraction of wall time blocked in `flush_commits`.

| Sync | Transactions/s | Flush share | WAL sender busy |
| --- | ---: | ---: | ---: |
| tmpfs (free) | 198,000 | 1% | 79% |
| disk, 0.18 ms | 150,000 | 13–17% | 60% |
| + 0.5 ms | 107,000 | 34% | 44% |
| + 1 ms | 87,000 | 45% | 35% |
| + 2 ms | 65,000 | 60% | 26% |
| + 5 ms | 35,000 | 76% | 15% |

The capture thread, not PostgreSQL, limits catch-up here: the WAL sender is
never fully busy. The capture thread needs about 6 µs of CPU per transaction
and each flush about 0.25 ms besides the sync, so throughput follows
`group / (group × 6 µs + 0.25 ms + sync)`. A background sync would instead
reach `min(1 / 6 µs, group / sync)`: about 165,000 at 1 ms but still about
50,000 at 5 ms with 256-transaction groups.

Larger groups pay for each sync with more transactions:

| Added sync latency | 256 | 512 | 768 | 1,024¹ |
| --- | ---: | ---: | ---: | ---: |
| 0 | 150,000 | 150,000 | 150,000 | 140,000 |
| 1 ms | 87,000 | 111,000 | 124,000 | 129,000 |
| 2 ms | 65,000 | 90,000 | 94,000 | 110,000 |
| 5 ms | 35,000 | 55,000 | 65,000 | 82,000 |

Three runs where the WAL sender was fully busy and groups flushed on the 5 ms
deadline instead of filling (one at 512 with no added latency, two at 1,024 with
1 and 2 ms) are left out: PostgreSQL, not the sync, limited them.

¹ With the journal's `max_open_transactions` raised above its 1,024 default.
2,048 fails: pending commits keep their spool slots until the flush, and the
spool allows `limits.spool_transactions` (1,024) open transactions.

## What would change the decision

- Measured EBS sync latency well above a millisecond, together with
  deployments that must catch up quickly on small-transaction backlogs or
  sustain more than about 90,000 transactions per second.
- A larger group is the cheaper first step. Pending commits share the spool's
  `limits.spool_transactions` and the journal's open-transaction limit with
  in-progress transactions, and the decoder already allows as many streamed
  transactions as the spool has slots, so the group limit has to be derived
  from those limits rather than raised alone.
