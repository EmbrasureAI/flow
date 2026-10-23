# Collapse memory limit

October 23, 2026. Raising the default `limits.collapse_memory_bytes` from 32 MiB
to 128 MiB keeps full-size epochs on the in-memory collapse path. The previous
limit sent most epochs through the disk path whenever a single table fell behind.

## Summary

- An epoch admits up to 32 MiB of serialized payload. For the benchmark's
  256-byte rows, collapse accounting charges about 880 bytes per mutation, so a
  32 MiB map holds about 380 of the 100-row transactions. A backlogged epoch
  holds up to about 1,130.
- With 32 MiB, 17–18 of 25–27 epochs per run overflowed (`disk_capacity`). Disk
  collapse averaged 1.73–1.80 s per epoch. With 128 MiB, every epoch stayed in
  memory; collapse averaged 0.43–0.48 s and peaked at 90 MiB of accounted retention.
- At 30,000 offered mutations/s, with about 2 million mutations per run (the
  accepted cohorts differ by about 5%), CDC service time fell from 72.9 s to
  59.4 s and collapse time from 33.1 s to 13.4 s.
- At 50,000 offered mutations/s, drain throughput rose from 18,254–19,888/s to
  21,684–22,316/s, and commit-to-catalog p95 fell from 29–30 s to 18–20 s.
- No run met the publication latency targets. The source generator missed
  arrivals in every run on this host.

## Environment

- One 8-vCPU (Intel Xeon, 2.5 GHz), 16 GiB x86-64 Linux 6.18 VM.
- PostgreSQL 18.6, Iceberg REST 1.10.1 and MinIO ran from
  `tests/production/compose.yaml` in Docker. The VM's cgroup configuration
  rejected the fixture's CPU and memory limits, so the services ran unbounded.
- The daemon, the benchmark writers and DuckDB shared the host with the services.
- Both daemons were `profiling` builds of `cdd8e54`. The new binary differs only
  in this default. Their SHA-256 prefixes are `2ff96a251c46d219` (32 MiB) and
  `08a2ee7372b7433e` (128 MiB).
- The harness's post-drain acknowledgement check reads `status.json` and then
  the slot. A slot that confirms an idle position ahead of a stale status file
  stops the run before row verification. For these runs, that case re-polled
  until the status caught up, and every run then matched exactly.

## Workload

`benchmark.py` default mixed workload: one table with 100,000 initial rows,
256-byte payloads, and 100 mutations per transaction (70 updates, 20 inserts and
10 deletes). Four writers and four table workers ran default maintenance with
10 seconds of warmup and 60 measured seconds. Runs alternated between binaries.
Every run passed the full source/target row comparison and exact
acknowledgement check.

CDC service time is the sum of each epoch's time from collapse start to source
ledger completion. It removes queueing from the comparison, so it varies less
between runs than drain throughput.

## Results

| Offered | Limit | Runs | Drain mutations/s | Commit→catalog p95 | CDC service time | Collapse time | Disk epochs | Peak RSS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 30,000/s | 32 MiB | 1 | 21,070 | 22.9 s | 72.9 s | 33.1 s | 18 | 721 MiB |
| 30,000/s | 128 MiB | 1 | 22,512 | 15.3 s | 59.4 s | 13.4 s | 0 | 655 MiB |
| 50,000/s | 32 MiB | 2 | 18,254–19,888 | 29.1–30.2 s | 70.5–71.8 s | 32.2 s | 17–18 | 707–712 MiB |
| 50,000/s | 128 MiB | 2 | 21,684–22,316 | 17.9–20.3 s | 57.5–59.5 s | 13.5–13.8 s | 0 | 654–694 MiB |

The writers committed 27,217–28,770 mutations/s in every run. They missed
765–1,669 scheduled transactions at 30,000/s and 12,724–13,690 at 50,000/s, so
both rates offered about the same load. The daemon drained more slowly than the
writers in every run.

Peak RSS is sampled once per second. Disk collapse also uses memory, for RocksDB
memtables, so peak RSS did not rise here. With four busy tables, the accounted
collapse bound is now 512 MiB instead of 128 MiB; this one-table benchmark does
not measure that.

## Reproduction

Start the [production fixture](../../tests/production/README.md), freeze both
binaries, and run each configuration serially with fresh artifact directories:

```sh
uv run tests/production/benchmark.py \
  --catalog-uri "$FLOW_REST_URL" --s3-endpoint "$FLOW_S3_URL" \
  --binary target/qualification-binaries/current/embrasure-flow \
  --build-manifest target/qualification-binaries/current/build-manifest.json \
  --artifacts target/collapse-memory-50k-01 --rate 50000 --duration 60 --warmup 10
```

Collapse modes come from the `epoch_collapsed` events in `daemon-1.log`.
