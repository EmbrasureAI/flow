# Module review fixes and validation

The 35 confirmed findings from the initial review were corrected, then the current
source received a second independent review. Each of the eleven Rust crates had
its own new reviewer with no inherited conversation context. Five additional
reviewers covered Iceberg patches, the PostgreSQL transport patch, benchmarks,
service fixtures and repository tooling. Reviewers read complete assigned files,
traced callers and reported concrete defects without a finding quota. They did
not modify source or run services. The second pass found 20 actionable issues;
those were also corrected and validated.

Baseline: `ed5b6ca47cb49e79988bc39a302f7143f2863e0f`. Scope excludes HA,
distributed/global compaction scheduling and Z-order. External compactor
reconciliation remains covered. These finite checks do not establish production
performance qualification or certify the absence of further defects.

## Initial findings

| Area | Changes |
| --- | --- |
| Source and bootstrap | Physical-table-only COPY; fail-closed RLS; consistent finite date/timestamp domains; bounded setup and feedback; owned connection drivers; initial ACK reclamation; advertised frame admission before payload buffering. |
| Journal | Nondestructive rejection of lower configured frame limits; abort quota reservations preserved across other transactions' commits; durable creation of directory entries. |
| Recovery and state | Committed ingestion must still appear in current catalog ancestry before index application/ACK; sealed index deltas require exact ordinal/count coverage; controlled index recovery retains authoritative control records. |
| Scheduling and compaction | Deadline ordering across priorities; target-byte sizing cannot stop a group before its required minimum; coalesce conservative decoded batches within actual row/byte limits. |
| Iceberg interoperability | Correct decimal scale/timezone interpretation; UUID logical annotations; cache weights include variable metadata; retained shared manifests visited once; failed/canceled delete loads wake consumers and cancel owned I/O. |
| Catalog retention and REST | Explicit snapshot protection wins; expiration checks existing reference heads and defaults off because REST cannot fence newly created references or retention-only changes; transient OAuth responses retain retryability; metric routes distinguish identifiers from endpoints. |
| Benchmarks and services | Exact integer workload mixtures; clock uncertainty/drift affect qualification; finite process lifetimes with retained failure evidence; meaningful held-I/O recovery boundaries; protected external delete-rewrite inputs; actual legacy format preconditions; persistent PostgreSQL volumes and cleanup independent of log collection. |
| Model and tests | Linear affected-table membership validation; unused dependency removed; bounded deterministic preparation-deadline fixture. |

## Second independent pass

| Review unit | Findings | Resolution |
| --- | ---: | --- |
| catalog-watch | 0 | No verified actionable finding. |
| compactor | 1 | Hard L0 pressure takes precedence over stable-file reclamation. |
| coordinator | 2 | Continued arrivals/force preserve overdue deadlines; failed delete repair records a target attempt without exhausting other targets of the same input. |
| daemon | 1 | Reloads and ambiguous schema responses preserve the initialized target UUID. |
| iceberg-ext | 2 | Discarded singleton manifest groups refund budgets; retained-file protection supports paths outside the default data directory. |
| ingress-journal | 2 | Retry parent-directory sync after a failed creation barrier; preserve read-ahead while traversing nearby terminal records. |
| materializer | 1 | Inexact/missing row-group bounds invalidate the corresponding file bound, preventing incorrect predicate pruning. |
| model | 0 | No verified actionable finding. |
| pg-source | 0 | No verified actionable finding. |
| state-store | 0 | No verified actionable finding. |
| testkit | 0 | No verified actionable finding. |
| Iceberg/REST patches | 2 | UUID metadata survives list/map children; the schema patch carries a modification notice. |
| PostgreSQL transport | 0 | No verified actionable regression. |
| Benchmark harness | 3 | SIGTERM terminates owned worker groups; WAL container ownership stays with the supervisor; Docker/SQL operations and overall execution are bounded. |
| Service fixtures | 4 | Container removal survives log failures; stress writers cancel/join under a process deadline; joined-process panic checking is shared; target-replacement tests read the actual PostgreSQL ACK. |
| Repository tooling | 2 | Corrected a moved qualification link and the local fixture's durability description. |

Additional validation caught a transport error-propagation defect, two Clippy
failures, test-only SQLite handles left open, and missing Python fixture
dependencies in CI. These were corrected before the final gates.

## Evidence

- **Rust:** 188 tests passed, zero failed. One manual performance profile remains
  ignored by the normal suite. `cargo fmt`, strict workspace Clippy, Rust 1.94
  workspace/all-target checks, and example configuration validation passed.
- **Python:** 37 fixture regression tests passed, including real subprocess
  deadline/SIGTERM controls and cancellation, evidence retention and cleanup
  failure controls. Four license-policy tests passed.
- **Dependencies:** all vendored files are reproduced by the documented patches
  with zero fuzz: 208 Iceberg files, 16 REST files and 38 PostgreSQL transport
  files. Provenance-only files are recorded separately. Distribution packaging
  collected 436 dependency notices for the local macOS target.
- **Live PostgreSQL/REST/MinIO:** inheritance and restricted-RLS controls passed.
  Same-name target replacement was rejected before publication and before the
  source slot acknowledged the rejected transaction. PostgreSQL 17 and 18
  container recreation preserved rows, logical slots and catalog data in their
  owned volumes.
- **Crash recovery:** interrupted COPY, durable staging, a nullable schema change,
  retained completed tables and complete index loss recovered correctly. The
  legacy-upgrade negative control rejected current-format inputs; it is not
  evidence of a successful old-format migration.
- **Final local stress:** 20 active seconds with three writers, updates/deletes,
  primary-key moves, savepoint/transaction rollback, SIGKILL, native compaction
  and an external compactor. Exact final rows, 16 live snapshot checks and nine
  retained snapshots passed. Observed overlapping active intervals contained
  84 ingestion commits and 30 compaction commits, including eight from the
  external service.
  This small functional run is not a throughput or latency benchmark.
- **Independent readers:** Spark, Trino and DuckDB checks cover native compaction,
  external delete/data/manifest rewrites, continued CDC, keyless duplicates,
  retained history and restart. The final-binary rerun passed all 17 phases,
  with no worker panics and successful container cleanup. Its report is retained
  at `target/module-fixes/live-final-readers/report.json`.

Performance evidence is intentionally narrower than a speedup claim. For an
external Parquet fixture missing decoded-size statistics, at least 32 decoder
batches become four index lookups under the row budget, or two under the byte
budget, with physical delete positions preserved. A journal regression reads
100 adjacent terminals without advancing the underlying file beyond its first
64 KiB read. Shared retained manifests are read once per protection pass even
with a one-byte cache. No new sustained throughput or freshness result is
inferred from these operation counts.

## Operational boundaries

Automatic snapshot expiration is opt-in. Coordinate external reference creation
and retention-policy changes as documented in the getting-started guide; REST
head assertions do not compare every metadata field. Publication membership,
operation flags, column lists and row filters must remain fixed from
initialization through streaming. Restoring an altered publication does not
prove that changes were not omitted; a violation requires resynchronization.

Files written by an earlier binary with incorrect truncated statistics require
rewriting to repair their existing metadata. Newly written files omit unsafe
bounds, which may reduce file-level pruning for the affected columns while
preserving correct query results.

Detailed reviewer reports, command logs, binary hashes and live service reports
are retained locally under `target/module-review-round2/` and
`target/module-fixes/`. They are ignored run artifacts, not distributed source.
