# OSS readiness contract

The supplied [specification](spec.md) is the acceptance contract. A working demonstration of one narrow path is insufficient. Missing operational behavior must become implementation and validation work, while intentional safety rules remain intact.

## Scope

The current goal is a production-ready PostgreSQL-to-standard-Iceberg service for one authoritative logical writer per table. It includes external compactor reconciliation. The user explicitly excludes Z-order-aware compaction, distributed compaction protocols, HA, and global autocompaction from early functionality. Those exclusions do not remove ordinary local compaction, bounded reader debt, or reconciliation of external physical rewrites.

Keep materialized acknowledgement as the safe default. Journal acknowledgement still requires independently durable storage; concurrent capture during COPY does not by itself permit earlier acknowledgement from an ordinary local disk. Preserve the source-lineage and logical-writer checks. Do not replace unknown values with placeholders or fall back to equality deletes.

V2 is the required interoperability baseline. The original specification places low-fan-out partitioning and v3 after validation of that baseline, and explicitly gates v3 work on the preceding correctness and performance phases. These features remain part of the implementation sequence, not reasons to describe the current narrow implementation as complete.

## Work and validation order

1. **Correctness and bounded resources.** Resolve source type semantics and source-contract drift; bound collapse by bytes; replace aggregate transaction frames and in-memory reference vectors with storage-backed transaction metadata. Preserve complete transaction publication and replay.
2. **Initialization and recovery.** Capture CDC concurrently with parallel snapshot workers, using separate durable bootstrap state and an acknowledgement fence. Publish initial files at L1. Recover interrupted initialization without silently resetting source identity.
3. **Schema transitions.** Serialize supported nullable additive DDL with source decoding, journal schema versions, and Iceberg publication. Preserve field IDs. Explicitly handle defaults and reject incompatible key/type changes rather than guessing their effects.
4. **Operational maintenance.** Finish protected orphan deletion, checkpoint retention and restoration, crash-safe index-generation switching, manifest maintenance, and safe external rewrite reconciliation for keyed and keyless append-only tables.
5. **Performance and observability.** Fix measured hot paths, expose actual transaction and reader-debt progress, implement adaptive scheduling and request/byte cost accounting, and verify steady-state compaction. Latency claims require complete observations and the requested source load.
6. **OSS delivery.** Run the Linux container demo and PostgreSQL/reader conformance matrix, add reproducible CI/service gates and dependency policy, and prepare reusable Iceberg extensions for upstream review.
7. **Broader format coverage.** Add low-fan-out partitioning and v3 with their own standard-reader, deletion, rewrite, and recovery gates after the v2 qualification required by the original plan.

## Evidence required for completion

Each capability needs an implemented path, an integration check that crosses its actual durable or public boundary, and an accurate support statement. Passing unit tests or documenting a limitation is not a substitute. Source, journal, catalog publication, reader observation, and physical maintenance must remain distinct measurements.

Local baseline tests already exposed unsupported unchanged TOAST, redundant durable ledger writes, oversized RocksDB index-block cache thrashing, and publication stopping after only one rewrite while hard reader debt remained. These findings drive concrete fixes. Failed or undrained workloads remain recorded as failures; they cannot qualify capacity or prove data loss without recovery checks.

The full original workload and 24-hour qualification gates remain requirements. Bounded local runs provide diagnosis and regression evidence, not a production certification.
