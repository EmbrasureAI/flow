# Positional-delete input isolation

Upstream candidate for `apache/iceberg-rust`, based on verified main commit
[`6fa8e03834dca17481441ec758afbbe12efc27f6`](https://github.com/apache/iceberg-rust/commit/6fa8e03834dca17481441ec758afbbe12efc27f6)
(checked 2026-09-04). These patches target the upstream repository layout, not
the vendored 0.10.1 crate. No upstream issue or pull request has been submitted.

The shared reader cache merges positional deletes by data-file path. When an
older delete file also names a newer data file, loading that delete for another
scan task can incorrectly delete rows from the newer file, although its own task
excludes that input. The [scan-planning scope rules](https://iceberg.apache.org/spec/#scan-planning)
require a data file's data sequence to be at most the delete's data sequence.
The cache must preserve the planner's selected inputs.

`0001-test-positional-delete-input-isolation.patch` adds an upstream-native
regression using the existing Parquet fixture and delete loader. It reads a real
shared delete in both task orders and checks exact positions, including vectors
already returned to a reader. It represents already planned tasks; it does not
test manifest planning. It also exercises the existing V3 test through the
scan-task lookup that the fix changes.

`0002-fix-positional-delete-input-isolation.patch` retains positional vectors per
input delete file and combines only the current task's inputs. Repeated lookups
with the same inputs reuse the combined vector. Borrowed bitmap union avoids
cloning each input. The existing V3 deletion-vector cache stays separate, with
Puffin tasks using that path, consistent with the specification's
[V3 precedence rule](https://iceberg.apache.org/spec/#deletion-vectors).

This is the sequence-isolation portion of the local
[`iceberg-delete-cache.patch`](../iceberg-delete-cache.patch), adapted for
upstream's [V3 reader addition, #3035](https://github.com/apache/iceberg-rust/pull/3035).
The lost-wakeup fix already merged in
[#2859](https://github.com/apache/iceberg-rust/pull/2859) is preserved. Its existing
test only gains the new completion argument. No transaction or coordinator code
is included. The original end-to-end case is in
[`Fixture::new`](../../../crates/testkit/tests/common/rewrite.rs), used by
[`delete_rewrite.rs`](../../../crates/testkit/tests/delete_rewrite.rs).

## Reproduce

Run from the Embrasure Flow repository root. The first test invocation is
expected to fail with leaked position `7`. After applying the fix, the relevant
reader tests pass, including both positional-task orderings, the existing wait
race, cache reuse, and V3 lookup and decryption.

```sh
patch_dir="$(pwd)/docs/patches/upstream"
export CARGO_TARGET_DIR="$(pwd)/target"
export CARGO_PROFILE_DEV_DEBUG=1
repro_dir="$(mktemp -d)"
git clone https://github.com/apache/iceberg-rust.git "$repro_dir/iceberg-rust"
cd "$repro_dir/iceberg-rust"
git switch --detach 6fa8e03834dca17481441ec758afbbe12efc27f6
git apply "$patch_dir/0001-test-positional-delete-input-isolation.patch"
cargo +1.97.1 test --locked -j 4 -p iceberg --lib test_positional_deletes_are_scoped_to_scan_task
git apply "$patch_dir/0002-fix-positional-delete-input-isolation.patch"
cargo +1.97.1 test --locked -j 4 -p iceberg --lib arrow::delete_filter::tests
cargo +1.97.1 test --locked -j 4 -p iceberg --lib arrow::caching_delete_file_loader::tests
```
