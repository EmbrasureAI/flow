# Bounded parallel compaction experiment

This candidate allows up to two distinct-table background build lifecycles,
bounded by `table_workers - 1`. One- and two-worker configurations retain their
previous behavior. It is being evaluated on `codex/bounded-parallel-compaction`;
no performance improvement or production qualification is claimed yet.

The release36 eight-worker diagnostic spends most of its slowest transactions'
pre-collapse interval in worker congestion involving synchronous maintenance.
This experiment tests whether a second background build reduces that debt.
Reservations cover capture, build, preparation, ready candidates and finalization.
New admission waits for every retiring worker to join, and explicit table checks
prevent duplicate builds. Existing actor capacity and exact activation fences
remain in force. There is no new public tuning option.

## Initial verification

The default-feature workspace gate passes 154 tests across 40 binaries, with one
manual profile ignored. Strict Clippy and the daemon build pass. The release36
155-test count additionally enabled the optional apply/SST profile modules;
those unchanged modules are not part of this default-feature count. Logs are
`target/release37-workspace-tests.log`, `target/release37-clippy.log`, and
`target/release37-debug-build.log`.

The new real-service fixture passes all six phases against PostgreSQL 18.6,
REST and MinIO in `target/release37-debug-campaign/candidate/report.json`:

- Both table builds have actual GET requests held concurrently; each pair is
  admitted within 0.8 seconds. CDC reaches source ACK while both remain held.
- Updates, deletes, key moves and delete/reinsert produce eight exact translated
  positions per table. Current and retained historical rows match independently.
- Both workers retain ownership and scratch past the real 30-second deadline,
  then join and retire without publishing canceled work.
- SIGTERM exits zero with both builds held. Restart discards exactly two abandoned
  BUILD records, removes both UUID scratch directories and preserves exact rows
  and source ACK. Final counts are 36,091 orders and 8,203 accounts.

The debug daemon SHA-256 is
`47448079170b3b60352a0c979ccd0c3dbb5dd76f64f1e6ef2e1c284016968250`.
The same fixture on frozen release36 fails precisely at second-table build
admission, before the first build's deadline, in
`target/release37-debug-campaign/baseline36/report.json`.

The remaining acceptance gate is the frozen optimized service campaign followed
by the same eight-worker, 100-table diagnostic used for release36. Require complete
source admission, exact rows/ACK and matching workload/provenance before comparing
latency, reader amplification, strict maintenance work and memory. Retain the
candidate only if those results justify the additional concurrent resources.
