# Bounded parallel compaction experiment

This candidate allows up to two distinct-table background build lifecycles,
bounded by `table_workers - 1`. One- and two-worker configurations retain their
previous behavior. It is being evaluated on `codex/bounded-parallel-compaction`;
the matched diagnostic below improves ingestion latency, but reader performance
still fails qualification.

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

## Frozen optimized service gate

The release artifact was built from clean commit
`b5ddcb17eba3352890c67417a61f9e80a5ce19f3`, tree
`b9757bf623e7c0476d2c114a5004fb50d01c7c1d`, with Rust 1.97.1, thin LTO and
the release profile for `aarch64-apple-darwin`. Its manifest is
`target/qualification-binaries/release37/build-manifest.json`.

- Daemon SHA-256: `dd0cb76cdfab7ef33cbf5dc9f82462d21b2066559316eed7ca59bdc3db8e012b`.
- External compactor SHA-256: `930c928c398fb145a0e3c0c06f034ee50b9c88f68b768f7b73c1b847fbd02595` (unchanged).
- Cargo.lock SHA-256: `2d84ac4f5caa3d832aa41c8020efcb083513c12e936309bd9d9259f8021aa8af` (unchanged).

All 30 phases pass in `target/release37-campaign/summary.json`. The runner verifies
frozen binaries, the build manifest and unchanged service harness hashes before
each case. No builds or competing workloads ran during this serial campaign.

| Case | Phases | Elapsed | Report SHA-256 |
| --- | ---: | ---: | --- |
| Two held table builds | 6 | 72.507 s | `fdc43242204cfa36b8419291e7ce128c38238e37ee1ac3411b4e61e34ebf07ac` |
| Periodic cleanup with two hot tables | 5 | 103.504 s | `7322fb42f4a802eb71820ff372fa8f5461a2c6473a5aa8c242eb3efc37c481a0` |
| Concurrent compaction and recovery | 7 | 174.950 s | `d6b4afab331913ca74411b0d62f9f3c43bf7b4266beb04eaea3f3ba5c42004f5` |
| Durable service faults | 12 | 79.284 s | `9c77fcfc2a776ebe0c9cf151bed9545ec655b768f307437a87dad8a94221ec07` |

The fault case includes lost successful commit responses, PostgreSQL/REST/MinIO
restarts, crash replay, physical index loss, unchanged TOAST, reader checks and
external compactor reconciliation. As in release36, the external compactor needs
three supervisor restarts across the outages; this is recovery evidence.
The Python harness gate also passes all 16 checks.

## Matched eight-worker diagnostic

`target/release37-source-diagnostic` repeats the instrumented release36 eight-worker
case with the new frozen daemon. Both admit all 1,000 warmup and 6,000 measured
transactions and verify 1,070,000 final rows across 100 tables with exact source ACK.
The normalized configuration/command, six harness hashes, sampler wrapper,
external compactor, build settings and all 100 final row digests match.
`verify_identity.py` reproduces `comparison-identity.json`; `compare.py` reproduces
the phase comparison. Both diagnostics exit nonzero because reader qualification
fails, while their correctness and diagnostic-completion checks pass.

| Measure | Release36, one build | Release37, two builds |
| --- | ---: | ---: |
| Catalog p95 / p99 | 1,580.898 / 1,899.653 ms | 708.200 / 822.698 ms |
| Drain after measured interval | 1.250 s | 0.474 s |
| Sampled process RSS peak | 530,300,928 bytes | 517,996,544 bytes |
| Maximum reader ratio | 1.392 | 1.684 |
| Tables above the 1.25 reader bound | 12 | 21 |
| Background / synchronous rewrites through ingestion | 161 / 178 | 315 / 73 |

Catalog p95 improves 55.2% and meets the one-second target in this finite case.
Both runs publish 1,750 four-transaction epochs. In the slowest 5% of transactions,
mean journal-to-collapse time falls from 1,698.3 to 544.6 ms. Known synchronous
maintenance occupancy during that interval falls from 3.64 to 0.63 workers.
These approximate log-derived intervals include I/O waits, not CPU time.

The reader result remains a failure. Across tables, median native scan time
increases from 2.818 to 3.890 ms and the compacted reference increases from 2.490
to 3.433 ms. Median paired ratio rises from 1.082 to 1.117. For the worst new
table, `events_25`, native/reference medians are 5.998/3.561 ms, a 1.684 ratio.
Twenty alternating pairs against pinned metadata exclude REST GET time.
Earlier inventory precedes the reader pins while maintenance is active; the
exact-pin follow-up below replaces it for layout attribution.

The source report SHA-256 is
`74dfd216d1220f86f68897d191e409c27ac97d0bb739282b2eca8c333bd15f30`;
the baseline is `df4d4b465c977b944abc2168293b76c1be003a2bf3e931840a95a1fdf778652a`.
This is a sampled diagnostic, not a repeat of the failed uninstrumented control
or a 50k/24-hour qualification. The additional resource reservation remains
bounded; the experiment's latency result does not waive the reader target.

## Exact pinned reader follow-up

`target/release37-reader-assessment/results/assessment.json` verifies metadata
hashes, snapshot IDs and inventory for all 100 original/compacted pairs from each
release. Exact original-pin totals increase from 840 to 1,026 manifests, 395 to
423 data files, 184 to 190 delete files, and 81,467 to 97,511 deleted physical
rows. Both compacted references have 100 data files and no deletes.

With ingestion, compaction and builds stopped, the existing diagnostic profiles
`events_25` against those immutable pins. Release37 full/selective wall ratios
are 1.783/1.855 and planning ratios are 1.911/1.892; aggregates match, including
86 selected rows from 10,720. The separate 2.0 planning bound passes while the
1.25 reader bound fails. Full-query planning is 3.074 versus 1.609 ms; the largest
disjoint phase difference is optimizer work, approximately 1.4 ms. Profiled
operator timings are not physical I/O or CPU measurements. Release36's same-pin
wall ratios are 1.022/1.094 in this follow-up.

The new original snapshot `7394109237433549026` contains six L0 files, no L1 files,
two L2 files, one positional-delete file and 18 manifests. Nine manifests contain
only current-snapshot deletion entries. These entries preserve standard snapshot
removal metadata and cannot simply be omitted. The existing planner compacts the
six L0s about nine seconds later, so this pin does not establish indefinitely
stranded stable files. `snapshot-files.json` retains every live entry and source
manifest hash. The next code review concerns packing the required deletion
metadata into fewer manifests, while preserving the existing commit and history
semantics. A latency gain remains evidence for the bounded scheduler candidate;
reader layout and sustained qualification remain open.
