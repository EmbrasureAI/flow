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
strict Clippy (`target/release39-clippy.log`) pass. Frozen optimized service and
performance validation remain required.
