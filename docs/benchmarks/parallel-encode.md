# Parallel Parquet column encoding

Measured on 2026-11-13. Parquet writers encode the top-level fields of a large
batch on several threads when the batch's work is spread across fields and
encode threads are free. Output files are byte-identical to sequential
encoding. Narrow tables, whose work is dominated by one field, stay
sequential. These are single-host fixture results.

## Encoding alone

A micro-benchmark encoded batches with Flow's writer properties (ZSTD, page
statistics, 8,192-row groups) sequentially and with the columns of each row
group spread over four threads. Mixed columns cycle through `bigint`,
`integer`, nullable `bigint` and `double`, `boolean`, a five-value status, a
32-character token and nullable 40- and 120-character text. Another session
was compiling on the same 8-core host during both runs (load average 5–15),
which understates the parallel speedups.

| Shape | 10K rows | 200K rows |
|---|---:|---:|
| 3 columns, one 256-byte payload | 1.0× | 1.0–1.1× |
| 12 mixed columns | 1.2–2.5× | 1.2–1.7× |
| 30 mixed columns | 1.3–1.4× | 1.5–1.9× |
| 60 mixed columns | 2.1–2.2× | 1.5× |
| 100 mixed columns | 1.6× | 1.4–1.6× |

The largest field bounds the speedup: parquet-rs encodes one column's pages in
order, so a table whose payload column holds most of the bytes cannot gain.

## Service

Base is `main` before this change; the candidate adds it with the default
budget (`limits.parquet_encode_threads` unset: half the visible cores, at most
four; three here). Release ThinLTO with jemalloc, PostgreSQL 18.6, local REST
catalog and MinIO, 8 vCPU. Runs alternate base and candidate.

```sh
tests/production/ab.py --base origin/main --candidate . --profile release --runs 3 \
  --args "--profile mixed --initial-rows 100000 --extra-columns 60 --rate 10000 --duration 60 --warmup 10"
tests/production/ab.py --base origin/main --candidate . --profile release --runs 2 \
  --args "--profile mixed --initial-rows 100000 --rate 30000 --duration 60 --warmup 10"
```

`--extra-columns 60` adds deterministic `bigint`, `integer`, `boolean` and text
columns (64 in total) that every update rewrites; full-row verification covers
them. All ten runs passed verification, admitted the offered rate with no
missed arrivals and passed the arrival and publication-latency gates.

### 64 columns, 10,000 mutations/s, 3 runs per side

| measure (median) | base | candidate | change |
|---|---:|---:|---:|
| CDC Parquet data write s | 10.37 | 9.16 | -11.7% |
| CDC prepare s | 16.2 | 14.9 | -8.0% |
| merge build µs/input row | 18.26 | 14.64 | -19.8% |
| key-order build µs/input row | 14.77 | 13.23 | -10.4% |
| CDC service µs/mutation | 45.5 | 44.0 | -3.2% |
| commit→catalog p95 ms | 567 | 559 | -1.5% |
| peak RSS MiB | 578 | 569 | -1.5% |

The candidate encoded 2,075–2,120 batches per run on more than one thread
(`flow_parquet_parallel_batches_total`). CDC service ranges overlap (base
44.4–45.6, candidate 42.0–46.1 µs/mutation; paired −2.6%): encoding is only
part of an epoch's data write, which also converts rows, sorts and uploads.

### Narrow control, 3 columns, 30,000 mutations/s, 2 runs per side

No batch fanned out (`flow_parquet_parallel_batches_total` stayed 0), so the
candidate encoded exactly as the base did. CDC service medians differed by
+5.6% from one fast base run (pairs +11.5% and +0.5%; ranges 17.4–20.4 and
19.4–20.5 µs/mutation), which is run-to-run variation.
