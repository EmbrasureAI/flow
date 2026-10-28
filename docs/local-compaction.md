# Local concurrent data compaction

The live concurrency, deadline and recovery gates pass. Sustained throughput,
publication latency and reader-health qualification remain incomplete; see
[Performance](performance.md). The implementation uses local workers without
distributed coordination, HA or sort/Z-order requirements.

The existing strict rewrite remains the fallback. Optional data builds can run
while the table continues publishing CDC. Delete-only, manifest and retention
work retain their existing paths.

## Durable and public boundaries

1. While the table actor is idle, choose bounded inputs at indexed snapshot S.
   Persist artifact reservations and a nonfencing BUILD record before launching
   the worker or issuing any PUT. BUILD protects the operation's files and S's
   descendant history. It is separate
   from the normal Building/Prepared operation that fences a table.
2. The blocking worker captures an immutable RocksDB read view and its complete
   table state at S, then acknowledges capture before the actor resumes CDC.
   The read view stays scoped to that worker; no self-referential storage or
   unsafe code is needed.
3. Build selected live data and the complete original-to-replacement row mapping
   against S. Payloads, delete staging and mappings remain bounded or disk-backed.
   Mappings are staged on one more thread, in order and at most two batches
   behind the scan; the worker waits for it before sealing or returning an error.
   The worker cannot commit a catalog change or modify the live row index.
4. Once the worker finishes, use a short idle table boundary to hand preparation
   to the background. The daemon pauses admission of the next same-table CDC
   epoch while that task loads catalog head H and captures one immutable index
   snapshot, including the full table state and exact live-file counts. Require
   S ancestry, unchanged selected data identities and unchanged original delete
   inputs; additive late position deletes and unrelated appends are permitted.
5. Without an effective late position delete, retain an output row's index delta
   only if its old location still equals the current mapping. With such a delete,
   require either no current key or a newer source-LSN mapping outside the
   selected inputs. Translate the deletion to the
   new output position and omit that stale index delta. Row-version comparison
   alone is insufficient because delete/reinsert can reset a row version.
   Retain accepted mappings in the candidate's disk-backed scratch state. This
   detached phase creates no main-store operation or catalog commit. A failure
   discards the whole candidate; a new H requires fresh preparation.
6. Rebuild residual deletes for unselected surviving data using original sequence
   applicability, and add the translated output masks. New data uses S's data
   sequence; delete outputs use the maximum removed delete sequence. Reacquire
   the table actor and require the catalog identity, location, snapshot, schema
   and spec plus the complete index `TableState` to equal captured H. Atomically
   install Building and its artifact reservation, stage the filtered deltas in
   bounded batches, and seal ordinary Prepared work before POST. Existing
   ambiguous-outcome recovery applies. Never redo catch-up after an unknown
   commit outcome.
7. Remove BUILD only after Prepared is durable, or after a failed/cancelled worker
   has actually finished. Optional preparation uses a 750 ms CDC stall budget,
   then releases that gate while the same immutable candidate finishes within
   its original 30-second build age. Activation still requires exact head H;
   concurrent CDC can invalidate the candidate. If hard debt already pauses
   publication at handoff, preparation keeps that gate for the remaining build
   age. At the total age limit, a timed-out blocking worker retains BUILD, scratch and a
   worker slot while it joins and retires asynchronously. It no longer counts as
   a viable candidate for foreground pressure decisions. Dropping a blocking-task
   handle is not proof of join.
   Shutdown retains BUILD; startup discards abandoned BUILD records before actors
   resume, leaving registered artifacts for existing protected GC and grace. Startup
   also removes abandoned UUID scratch stores after durable operation recovery.

The daemon admits up to two local builds within the configured table worker
budget, leaving at least one worker for table actors and keeping each reservation
through finalization. With only one table worker, it uses the strict path.
Library callers must serialize one build per table.
A completed build receives a short table boundary for background handoff, then a
prepared candidate receives the next boundary for activation. Prepared work is
preferred over a new CDC epoch; build handoff is preferred next so sustained CDC
cannot starve a completed build's handoff or activation. Invalidated work returns
to scheduling, while hard
reader pressure can select the strict bounded fallback. Publication never bypasses
hard debt. The build wait requests cancellation after 30 seconds and still joins
the actual worker before releasing ownership. Catch-up accepts at most 256
intervening snapshots and applies the configured delete-input file, byte and row
limits before staging. The Parquet read/write admission limits also apply.

After one second of deferral, an eligible soft data-build probe may precede
another ready CDC epoch. This does not guarantee admission of soft delete
consolidation: a probe yields when the planner prefers that synchronous work,
which may wait for a CDC gap or hard pressure. Running actors are not preempted.

Catch-up also caps each mapping batch by the existing row limit and the smaller
writer batch/row-group byte budget. It charges decoded keys, locations, paths
and partition bytes; one decoded lookahead and storage buffers remain outside
that payload count. Detached preparation reads and filters the scratch mapping
at H without changing authoritative row mappings. Activation later streams that
same filtered mapping through the existing durable staging and recovery path.

Blocking maintenance workers share one lazily started, process-lifetime I/O
runtime with one driver thread. Row processing stays on the existing blocking
workers. This lifetime matches
[OpenDAL 0.57's global HTTP client](https://github.com/apache/opendal/blob/1e0fc73190afb7e221c7badda7ef43fe40592940/core/core/src/raw/http_util/client.rs#L50-L103):
[reqwest 0.13.4](https://github.com/seanmonstar/reqwest/blob/11489b34eda6d32b15ad4033e62beba2ee401350/src/async_impl/client.rs#L940)
uses [Hyper's Tokio executor](https://github.com/hyperium/hyper-util/blob/b23a13e2b7ee73e15ba008cd9b19dcd2d3861957/src/rt/tokio.rs#L105-L116),
which starts pooled connection tasks on the runtime handling the request.
Temporary worker runtimes could drop those tasks while another worker reused
the connection. The shared driver keeps worker-created connections and retry
timers alive between jobs and after main-runtime shutdown. Connections opened on
the main runtime can still require normal retry when that runtime stops. None of
this retires BUILD or substitutes for an actual worker join; bounded process
shutdown and restart recovery still apply. The pooled-HTTP regression test holds
a request across the first worker's completion and parent shutdown, then
releases the response and joins the surviving worker.

## Experimental lazy index remap

`[compaction] lazy_index_remap = true` (default `false`) makes background data
builds of keyed tables commit one remap per input file instead of one index
delta per moved row. The strict path, keyless tables and every other rewrite
keep per-row deltas; with the flag off nothing changes.

- The worker writes each input file's survivors in input order, so it records
  only runs `(old start, output file, new start, length)`. Runs break at
  positions deleted at the build base and at output-file rotation: the remap is
  O(files + deleted positions), not O(rows).
- Catch-up no longer looks up or verifies each moved row. It translates late
  position deletes through the remap into output masks and requires, per input
  file, that the live count at H equals the base survivors minus the effective
  late deletes. Flow never leaves an index entry on a deleted row, so equal
  counts mean the same row set. Output live counts are record counts minus masks.
- Activation stages the remap in the operation's delta namespace instead of
  per-row deltas. The final apply batch stores one `file_remaps` record per
  retired input, deletes the input counts, adds the output counts and checks
  that both sums agree, atomically with the table transition. Ambiguous commit,
  discard and restart recovery are unchanged; the staged remap is replayed or
  dropped with its operation.
- Primary-key and reverse entries keep their raw location. Every forward read
  (`lookup`, `lookup_many`, index snapshots, collapse binding and the apply
  compare-and-swap) translates a raw location in a retired file, following
  chained remaps, to the location per-row apply would have stored. Snapshot
  readers consult only remaps visible in their RocksDB snapshot. A later CDC
  change or per-row rewrite of a key replaces its raw entry and deletes the raw
  reverse entry.
- Reverse scans of a file do not include rows a remap moved into it. External
  rewrite reconciliation and checkpoint catch-up therefore first call
  `materialize_remaps`, which rewrites the remaining raw entries per row in
  crash-safe batches and then drops the table's remaps. A full rebuild starts a
  generation without remaps.
- Remaps are not garbage collected until materialization or a rebuild, and the
  `file_remaps` column family is new in this generation format.

## Experimental consolidation-first delete debt

`[compaction] delete_debt = "consolidate"` (default `"current"`) pays
delete-file count debt with the delete-only consolidation before any data
rewrite. Delete-file count is the most delete files applying to one data file,
or the table-wide count of shared position-delete files. With `"current"`
nothing changes.

- A table visit runs the bounded consolidation (at most 32 inputs, 32 MiB and
  one million positions, at least `min(delete_files_soft, 32)` inputs) whenever
  that count reaches `delete_files_soft`, even if some files also need delete
  reclamation. Under hard pressure CDC publishes as soon as the consolidation
  clears it; no rows move and no row-index deltas are written. A soft build
  probe runs the same consolidation instead of yielding. If hard pressure from
  delete-file count alone meets a running build, the consolidation still runs;
  a build whose delete inputs it replaced is invalidated at catch-up.
- Delete-file count no longer makes a data file a rewrite candidate. Data
  rewrites for deletes select files whose own deleted fraction reaches
  `deleted_rows_percent`, and an L1 or L2 seed admits only companions that also
  do. L0 and small-file debt keep their grouping.
- If consolidation cannot reduce the count, no other group exists and the count
  stays at `delete_files_hard`, planning falls back to the count-driven rewrite.
- Each choice emits a `maintenance_action_chosen` debug event on the
  `flow_events` target with `action`, `reason`, the path (`publication`,
  `idle`, `probe` or `build_active`) and the debt counts. Build and rewrite
  events carry the planner's `selection_reason` (`deleted_rows`,
  `delete_file_count`, `small_files`, `l0_debt` or `promotion`) in both modes.

## Bounded delete dependencies

The planner tries feasible subsets without increasing data or delete read budgets.
Position-delete files carry exact full-path bounds, and a data rewrite emits at
most eight ordered residual ranges. This avoids charging a single broad residual
to every older data file when narrower metadata can exclude it.

When no useful data group fits, a repair pass reads one individually admissible
shared delete file and counts its effective positions using disk-backed staging.
It can isolate one blocked target into at most three ordered ranges. Publication
requires that target's complete prospective delete-file, byte and row charges to
fit the existing limits. An unhelpful pass publishes nothing. A caller-owned
cursor remembers checked inputs across retries at the same table UUID and head;
new heads reset it, and I/O errors do not consume a candidate. Exhaustion waits
for changed catalog state or external maintenance without rereading failed inputs.
Ordinary delete consolidation also rejects output that would make a currently
admissible data target exceed those limits, preserving progress across restart.

## Validation

The real RocksDB, Parquet/Avro and Iceberg-reader integration gates cover:

- A captured index view remains at S through concurrent partial and completed H
  application, including reverse mappings and key moves.
- Actual Parquet/Avro and Iceberg scans verify late update, delete, key move,
  delete/reinsert, unaffected rows, sequence filtering and retained historical rows.
- Replaced selected data files or original delete files cancel safely, including
  delete consolidation that leaves every data-file identity unchanged.
  Successful commit response loss, restart and index loss recover the exact
  corrected Prepared plan. A failure after the first accepted batch leaves
  Building work unpublished; reopen, discard and a newer-H source delete verify
  that a fresh rewrite cannot reuse the partial mapping or resurrect that row.
  A held real staging write verifies that the next lookup can run concurrently
  and that its failure cannot return before staging joins. Wide-key admission
  verifies byte splits and oversized lookahead against reopened scratch data.
- BUILD protects GC/history, survives index generation replacement, and becomes
  collectible only after join/promotion or startup abandonment and the normal grace.
- Delete dependency repair covers alternate targets from one staged input,
  alternate inputs, exhaustion without repeated object reads, lost-response
  recovery and subsequent bounded data compaction. Ordinary consolidation
  preserves dependency locality before and after index reopen.

Run the [service integration suites](../tests/production/README.md) to exercise
held object reads, concurrent CDC, cancellation, restart and external reconciliation.

Iceberg's [scan-planning rules](https://iceberg.apache.org/spec/#scan-planning)
apply position deletes when the data sequence is at most the delete sequence and
the physical path/position matches. Retaining an old data sequence alone cannot
retarget position deletes after a rewrite. The translated masks and exact current
head are this protocol's additional proof. Apache's
[rewrite API](https://iceberg.apache.org/javadoc/1.4.3/org/apache/iceberg/RewriteFiles.html)
supports explicit data sequences and validates that replaced inputs remain live;
this service must additionally preserve its physical-row index and late changes.
