# Release35 maintenance fairness evidence

Release35 addresses soft compaction starvation under continuously ready CDC.
The evidence below establishes a behavioral regression and its correction on
both debug and optimized builds. Performance qualification is separate;
this record does not establish production readiness, 50,000 mutations/s, or
24-hour endurance.

## Frozen artifact

The optimized build uses clean commit
`73e07320cb03811ce2507fbfd9ccb328f94e074b`, tree
`cfe4d5e16ca37594f5a94f295632e457568cc98d`, Rust 1.97.1, and
`aarch64-apple-darwin`. Its complete build command and toolchain are recorded in
`target/qualification-binaries/release35/build-manifest.json`.

| Artifact | SHA-256 |
| --- | --- |
| Daemon | `694d4dfd7e93d4a4c59bd3a158a4ee92afd4c0cad8e8d6ca0fee0bc3861c12ed` |
| External compactor | `930c928c398fb145a0e3c0c06f034ee50b9c88f68b768f7b73c1b847fbd02595` |
| Cargo.lock | `2d84ac4f5caa3d832aa41c8020efcb083513c12e936309bd9d9259f8021aa8af` |

## Change and regression

Previously, ready CDC always preceded overdue optional maintenance. A sustained
backlog could therefore defer soft builds until the backlog drained or hard
reader debt forced a synchronous rewrite. After the existing one-second delay,
the scheduler now admits a bounded background-build capture ahead of another
CDC epoch. The reservation counts against the existing worker limit and permits
one background build; a single free slot is sufficient when another actor is
already running. Running actors are not preempted.

The capture checks the current catalog head, index head, source schema and
pending operation before proceeding. It starts only a soft data rewrite and
preserves the existing preference for cheaper delete consolidation. Recovery,
schema changes, hard debt and synchronous maintenance remain ordinary table
work. A transient or stale capture yields and retries after the normal delay.
A catalog schema change between validation and fresh build capture now returns
`ReplanRequired`; the schema integration test verifies that it leaves the table
index unchanged.

The service fixture queues 1,025 source transactions with the daemon stopped,
then registers the durable backlog behind a held ingest POST. Its 16-transaction
pages are older than the scheduling delay before native restart. The diagnostic
10,000 commits/s limit avoids introducing token-permit gaps. Soft/hard L0 limits
of 2/128 isolate soft debt in a finite workload; raised delete/stable-file limits
are recorded in each report and are not proposed production defaults.

`target/release34-fairness-negative-02/report.json` is the valid old-binary
negative control: the one-hot-table case fails because the orders build starts
only after orders' own backlog drains. The earlier two-table attempt,
`target/release34-fairness-negative-01`, passed on release34 and did not reproduce
starvation. A bounded shared page can temporarily empty one table's queue while
the other finishes. That run remains retained and is not counted as proof of
the regression.

Both debug positive cases used daemon SHA-256
`85d1ac8e50f4ca70c89ee84dccd227a22be7a29125560b53a43292bd3cdff909`:

| Case and report directory under `target/` | Build admission | Pending transactions |
| --- | ---: | ---: |
| One hot table: `release35-fairness-debug-01` | 1.865 s | 721 |
| Two hot tables: `release35-fairness-debug-two-tables-01` | 1.863 s | 817 |

In both cases orders' own snapshot LSN remained behind its final source barrier.
With an actual input Parquet read held, CDC and PostgreSQL ACK advanced. After
release, the admitted build committed and DuckDB matched all 5,118 orders and
17 accounts. The initial 4,096-order snapshot and pre-backlog 4,098-order snapshot
remained unchanged. Both cases passed clean shutdown and worker-panic checks.
These timings include deliberate fixture coordination and are not throughput
or latency benchmarks. The debug artifact is distinct from the optimized
release35 artifact listed above.

## Optimized regression

Both five-phase service cases pass on the frozen optimized binary in
`target/release35-campaign`. Build admission occurs at 1.546 seconds with
720 transactions pending in the one-table case and 816 in the two-table case.
Both retain orders work at admission, advance CDC and PostgreSQL ACK while the
input read is held, publish the admitted build, verify exact current and
historical rows, and stop cleanly without worker panics. Complete command times
and arguments are recorded in `summary.json`. These ten phases validate the
scheduling boundary; they do not establish the workload performance targets.

## Repository checks

Clippy with warnings denied and nine Python harness checks passed. The final
full workspace Rust gate exited successfully on the frozen source tree:
154 tests passed across 40 test binaries, with four manual profiles ignored
and no failures. Results are recorded in
`target/release35-final-workspace-tests.log`, `target/release35-final-clippy.log`
and `target/release35-python-tests.log`. Raw reports and logs are ignored local artifacts; this tracked summary
preserves their identities and the limits of the conclusions.
