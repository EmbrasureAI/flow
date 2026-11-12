# Sorted layout

A table's Iceberg sort order says how rows should be ordered within each data
file. Flow writes in that order where it can do so without slowing ingestion,
and marks a file with the order (`sort_order_id`) only after checking every row
it wrote. Order is a layout property only: no reader or Flow invariant depends
on a file being sorted.

## Choosing the order

Tables Flow creates are sorted by their primary key, ascending. Keyless tables
are created unsorted. Existing tables keep their order.

To choose another order, set `sort` in the table's `[[tables]]` block, or set
the table's order from SQL (`ALTER TABLE … WRITE ORDERED BY` in Spark, the
`sorted_by` table property in Trino):

```toml
[[tables]]
target_table = "orders"
primary_key = [0]
sort = "primary_key"                       # the default for new tables
# sort = ["tenant_id", "created_at desc"]  # any configured columns
# sort = "none"                            # keep arrival order
```

A field is `column [asc|desc] [nulls first|nulls last]`. Nulls default to first
for ascending columns and last for descending ones, as in Iceberg and Spark.

| `sort` | New table | Existing table |
| --- | --- | --- |
| absent | Primary key (keyless tables: unsorted) | Its own order, which SQL engines may change |
| set | That order | Replaced at startup if it differs, with a `table_sort_order_set` event |

Use `sort` when the deployment is managed as configuration; leave it absent
when the table's order is managed from SQL. Flow reloads table metadata before
every commit and rewrite, so an order changed from SQL applies from the next
epoch. Files written in an older order keep its `sort_order_id`; they count as
not sorted in the current order.

Flow writes orders whose fields are identity transforms on any replicated
column. A table whose order uses another transform (`bucket`, `truncate`, a time
transform) is written unsorted; `embrasure-flow layout` says so.

Declaring an order also tells other writers to follow it. Spark and Trino sort
their writes, and S3 Tables' managed compaction switches from bin-packing to
sorting for tables with a declared order.

## Which files are sorted

| Files | Written | Marked sorted |
| --- | --- | --- |
| A CDC epoch collapsed in memory (up to `limits.collapse_memory_bytes`) | Sorted before writing | Always; Parquet sorting columns are declared too |
| A larger epoch collapsed on disk, and the PostgreSQL initial copy | In primary-key encoding order | Only if that order is the sort order |
| A compaction of sorted inputs whose key ranges are disjoint | Inputs in key order | When the output checks out |
| Any other compaction | Inputs in commit order | Only if the output happens to be sorted |

Sorting an epoch costs no index work: its new rows get fresh index entries in
any order. An epoch is left in collapse order if a sort column holds a TOASTed
value that is filled in from the previous row image.

Primary-key encoding order matches the sort order for a primary-key order on
non-negative integer and UUID keys, so the initial copy and disk-collapsed
epochs of such tables are marked sorted. Negative integers, variable-length
strings and other orders do not match.

Compaction keeps each input file contiguous. Inputs that overlap in key range,
such as CDC epochs that update keys across the table, are rewritten unsorted.

## Inspecting a table

`embrasure-flow layout` reads catalog metadata only and can run beside the
daemon. `--table namespace.table` selects one target and `--json` prints JSON.

```
replicated.events  (format v2)
  sort order   id ASC NULLS FIRST  (order 1)
  sorted       0.0% of rows, 0.00 of 0.06 GiB in files stamped with the order
  files        7 data files, 2646442 rows: L0 0 · L1 6 · L2 1
  overlap      on id: a file's range overlaps 7.0 files on average, 7 at most
  lookup       a point lookup on id reads at most 100.0% of rows by file bounds
  advice       100.0% of rows are in files not verified in the current order: written before it was declared, by another writer, or compacted from overlapping files.
```

This is a table under uniform updates: every epoch touches keys across the
table, so each compaction's inputs overlap and its output is unsorted.

`overlap` is measured on the leading sort column, or the first primary-key
column when the table is unsorted. A point lookup can skip files whose bounds
exclude the key; `lookup` is the resulting upper bound on rows read before
row-group and page statistics prune further.

The same numbers are exported per table, updated by periodic maintenance:
`flow_table_sort_order_id`, `flow_table_sorted_rows_ratio`,
`flow_table_unsorted_bytes` (bytes outside L0 not marked with the current order)
and `flow_table_sort_overlap_files`. `flow_epoch_sort_total{result}` counts
epochs by `already_sorted`, `sorted`, `unsortable` and `disk`.
