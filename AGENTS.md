# Agent notes

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the [code guide](docs/code-guide.md)
first. This file covers how to build, test and measure quickly.

## Environment

Before benchmarking on a new machine, run `tests/production/cloud_setup.sh` once
(about 3 minutes on a bare dnf-based VM; its log is `target/cloud-setup.log`). It
installs anything missing, starts Docker and the benchmark fixture, and warms a
`bench-fast` build of `origin/main` in the background. It installs `uv` in
`~/.local/bin` and Cargo in `~/.cargo/bin`. If the fixture is down, run
`tests/production/fixture.sh up`; it restarts Docker when needed.

## Testing standard

1. While iterating, run the touched crates' tests: `cargo test --locked -p flow-<crate>`.
2. Before a pull request, run `cargo fmt --all --check`, the workspace clippy
   command from CONTRIBUTING.md and `cargo test --locked --workspace`. CI also runs
   the PostgreSQL service matrix.
3. For any performance change, compare against the base with
   `tests/production/ab.py --base origin/main --candidate .`. It takes about
   5 minutes after the build and verifies every row. Report its median
   comparison table, led by CDC service time per mutation.
4. Before publishing numbers, repeat the comparison with `--profile release` or
   `profiling`, longer runs (`--args "--rate 30000 --duration 60 --warmup 10"`)
   and at least three runs per side. Include the environment, workload, missed
   arrivals and failed gates, as CONTRIBUTING.md requires.

## Measuring

- Configuration experiments need no code change. Use
  `--candidate-args "--limit KEY=VALUE --compaction KEY=VALUE"` with the same
  revision on both sides.
- `tests/production/epoch_report.py DIR...` breaks any benchmark run into collapse,
  prepare, commit, apply, time between epochs and compaction stall.
- The PostgreSQL benchmark source tops out at roughly 50,000 mutations/s on one
  host. Measure ingest capacity beyond that, or a single stage, with
  `cargo test --release -p flow-testkit --test ingest_benchmark -- --ignored
  --nocapture`; `INGEST_BENCH_*` variables change its shape.
- `bench-fast` and `profiling` builds keep frame pointers, so `perf record -g
  --call-graph fp -p <daemon pid>` works.
- Only one benchmark may run on a machine at a time. Use separate machines for
  parallel experiments. When disk runs low, run `tests/production/fixture.sh
  reset` and remove old `target/ab/runs` directories.
