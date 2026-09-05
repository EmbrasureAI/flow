# Finish preparation under hard publication pressure

The [release38 50k follow-up](release38.md#full-50k-follow-up) exposes redundant
synchronous rebuilding. Two concurrent L1 candidates finish their data writes,
then exceed the optional 750 ms preparation stall budget while the table is
already paused by hard reader debt. Their completed work is discarded and the
same accumulated L0 debt is rebuilt synchronously.

In the retained log, `flow-l1-rewrite-f74906cf` has 26 inputs and 249,317 output
rows. Its valid catch-up takes 1.673 seconds, but preparation expires first.
The subsequent 32-file synchronous rewrite holds the table for about 8.20 seconds.
An earlier discarded candidate similarly precedes a 4.37-second replacement.
For both candidates the table was already waiting for its build before
preparation handoff. The detailed operation IDs and timestamps remain in
`target/release38-performance/serial-50k/daemon-1.log`.

The scheduler now distinguishes this existing hard-pressure state at handoff.
Hard-paused tables wait for preparation within the remaining original 30-second
build age. Optional preparations retain their 750 ms budget. No later slowness
promotes optional work to the hard path. Timeout ownership, worker joining,
scratch cleanup, and exact-head validation before activation are unchanged.

The existing real-service hard-debt test now delays catalog requests by 1.25
seconds. It requires the same operation to report completed preparation after
one second, without deadline retirement, then verifies its compaction snapshot,
exact current rows and source ACK. The other phases retain late-delete
translation, history, build cancellation, SIGTERM and restart checks.

The frozen release38 negative control fails at the intended hard-preparation
assertion and logs deadline retirement after a 1,262.85 ms publication stall.
Its report is `target/release39-negative-control/report.json`, SHA-256
`bfbd9f92490cac91b4f08dd32743de718219bd29005a82853cc5da58efe3089f`.

The default workspace passes 154 active tests across 40 binaries, with one
manual profile ignored (`target/release39-workspace-tests.log`). Formatting and
strict Clippy (`target/release39-clippy.log`) pass. The frozen optimized service
gate below passes; performance qualification remains incomplete.

The optimized artifact is frozen from clean commit
`2856bd82c613e92dccfa2e9b6965c51d812ee752`, tree
`8d4a5bbd4189d51dbee6892048d07c0768c932d4`. Daemon SHA-256 is
`720b6b1e7e9fcc4838bce9a2f13d8848c5a4bd72c9db38de2869117fa9c62f37`;
the independent compactor is unchanged from release38 at
`776710a6edb2cdee3f5fff225cb90cb21594266fad817b8af7167706d633cbce`.

The first optimized service run passes the new hard-preparation check, then fails
an existing restart assertion that assumed exactly one abandoned BUILD. Logs
show that the accounts table also started maintenance before SIGTERM, and
restart correctly discarded two builds. `target/release39-campaign` retains this
failed run (report SHA-256
`56eb2e824576f31c632fc8cdf1ba4473dbec03d4faeb860cfd10a04fc7df7219`).
The corrected fixture permits the scheduler's bounded one or two
abandoned builds, snapshots every remaining scratch directory after process
exit, and requires all of them to be removed on restart. The dedicated parallel
fixture separately requires exactly two held owners and two recovered BUILDs.
The corrected service campaign uses the same frozen binary.

## Corrected optimized service gate

All 30 phases pass in `target/release39-campaign-02/summary.json`. The serial
campaign verifies frozen binary, manifest and harness hashes before every case,
without competing builds or workloads. The artifact uses Rust 1.97.1 with the
release profile for `aarch64-apple-darwin`; its manifest is
`target/qualification-binaries/release39/build-manifest.json`.

| Case | Phases | Elapsed | Report SHA-256 |
| --- | ---: | ---: | --- |
| Concurrent compaction and recovery | 7 | 179.750 s | `397b7edb14c6856873a71fc8ebd6fa83c32773c9458898168d95b7986e77d7d3` |
| Two held table builds | 6 | 71.930 s | `a44f914029b673519983c1e6a875d34cc2480289bbb48e5f11950d606cb066fa` |
| Periodic cleanup with two hot tables | 5 | 97.940 s | `cf9527e77bc08f50660a85dea546fab5d3b6567b0fe52cf0e2ab41f0c3258c80` |
| Durable service faults | 12 | 74.250 s | `1050322e8d2920a3be989391c909fc92e1939340a65909984e64d39a86015ecd` |

The hard-pressure candidate completes preparation in 1,258.817 ms, survives the
old 750 ms bound and publishes without retirement. Exact current rows and source
ACK pass. The remaining concurrent phases verify translated deletes, retained
history, the unchanged 30-second build cancellation fence, scratch reclamation
after joining, graceful SIGTERM and restart recovery. Parallel, periodic and
fault cases preserve their separate coverage, including external reconciliation.

The independent compactor requires three supervisor restarts during injected
outages. These are passing recovery checks, not uninterrupted availability.
Independent reader and full 50k follow-ups are running separately; their results
are not yet part of this gate. The original fixture failure remains retained,
and these service results do not qualify the 50k, reader-health or 24-hour targets.
