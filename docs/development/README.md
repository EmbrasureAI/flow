# Design and qualification records

These documents retain the implementation requirements and the evidence behind
changes. Numbered `releaseNN` records identify internal qualification builds,
not published software releases. For current user-facing information, start with
[Getting started](../getting-started.md) and [Performance](../performance.md).

- [Independent module review fixes](qualification/module-review-2026-09-06.md): correctness, lifecycle and local validation.
- [Design specification](spec.md) and [readiness requirements](oss-readiness.md).
- [Implementation plan](implementation-plan.md): current work and remaining gates.
- [Validation](validation.md): integration coverage and exact build attribution.
- [Qualification history](production-qualification.md): measured results, failures and rejected experiments.
- [Latest recorded service build](qualification/release39.md) and [source capacity](qualification/source-capacity.md).

Raw reports, database fixtures and profiler outputs live in the ignored
repository-root `target/` directory. They are local artifacts, not downloadable
files in this repository. The records retain their paths and hashes; commands
in the [service suite](../../tests/production/README.md) produce new reports.
Older results apply to their recorded binaries and do not qualify later changes.
