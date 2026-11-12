//! `embrasure-flow layout`: each target's sort order and how its live data
//! files follow it. Reads catalog metadata only, so it runs beside the daemon.

use crate::config::Config;
use anyhow::{Result, bail};
use flow_coordinator::SortLayout;
use flow_iceberg_ext::SnapshotView;
use flow_materializer::TableSort;
use iceberg::spec::{NullOrder, SortDirection, Transform};
use iceberg::table::Table;
use std::fmt::Write as _;

#[derive(serde::Serialize)]
struct Report {
    table: String,
    format_version: u8,
    /// The default sort order, such as `id ASC NULLS FIRST`; empty when unsorted.
    sort_order: Vec<String>,
    /// `sorted`, `unsorted` or `unsupported`.
    sort_support: &'static str,
    #[serde(flatten)]
    layout: SortLayout,
    sorted_rows_ratio: f64,
    /// The column `overlap` is measured on.
    overlap_column: Option<String>,
    advice: Vec<String>,
}

pub(crate) async fn run(config: &Config, only: Option<&str>, json: bool) -> Result<()> {
    let catalog = crate::services::catalog(config).await?;
    let mut reports = Vec::new();
    for configured in &config.tables {
        let name = format!(
            "{}.{}",
            configured.target_namespace.join("."),
            configured.target_table
        );
        if only.is_some_and(|only| only != name) {
            continue;
        }
        let id = crate::services::target(&configured.target_namespace, &configured.target_table)?;
        let table = catalog.load_table(&id).await?;
        let view = SnapshotView::current(&table).await?;
        let layout = flow_coordinator::sort_layout(&table, &view);
        let sort = TableSort::of_table(&configured.schema(0), table.metadata());
        reports.push(report(name, &table, layout, &sort));
    }
    if let Some(only) = only
        && reports.is_empty()
    {
        bail!("no configured target table named {only}");
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else {
        for report in &reports {
            print!("{}", text(report));
        }
    }
    Ok(())
}

fn report(name: String, table: &Table, layout: SortLayout, sort: &TableSort) -> Report {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let column = |id: i32| {
        schema
            .field_by_id(id)
            .map_or_else(|| format!("field {id}"), |field| field.name.clone())
    };
    let sort_order = metadata
        .default_sort_order()
        .fields
        .iter()
        .map(|field| {
            let source = column(field.source_id);
            let value = match field.transform {
                Transform::Identity => source,
                transform => format!("{transform}({source})"),
            };
            format!(
                "{value} {} NULLS {}",
                match field.direction {
                    SortDirection::Ascending => "ASC",
                    SortDirection::Descending => "DESC",
                },
                match field.null_order {
                    NullOrder::First => "FIRST",
                    NullOrder::Last => "LAST",
                }
            )
        })
        .collect();
    let mut advice = Vec::new();
    let sort_support = match sort {
        TableSort::Unsorted => {
            if schema.identifier_field_ids().next().is_some() {
                advice.push(
                    "No sort order: files keep arrival order. Set sort = \"primary_key\" in \
                     flow.toml, or declare an order from SQL, to sort new files."
                        .to_owned(),
                );
            }
            "unsorted"
        }
        TableSort::Supported(_) => {
            let unsorted = 1.0 - layout.sorted_rows_ratio();
            if unsorted > 0.0005 {
                advice.push(format!(
                    "{:.1}% of rows are in files not verified in the current order: written \
                     before it was declared, by another writer, or compacted from overlapping \
                     files.",
                    100.0 * unsorted
                ));
            }
            "sorted"
        }
        TableSort::Unsupported { reason, .. } => {
            advice.push(format!("Flow writes this table unsorted: {reason}."));
            "unsupported"
        }
    };
    if let Some(overlap) = &layout.overlap
        && overlap.files_without_bounds > 0
    {
        advice.push(format!(
            "{} data files have no bounds for {}, such as values longer than 64 bytes, so \
             readers cannot prune them by that column.",
            overlap.files_without_bounds,
            column(overlap.field_id)
        ));
    }
    Report {
        table: name,
        format_version: metadata.format_version() as u8,
        sort_order,
        sort_support,
        sorted_rows_ratio: layout.sorted_rows_ratio(),
        overlap_column: layout
            .overlap
            .as_ref()
            .map(|overlap| column(overlap.field_id)),
        layout,
        advice,
    }
}

fn text(report: &Report) -> String {
    let layout = &report.layout;
    let gib = |bytes: u64| bytes as f64 / f64::from(1u32 << 30);
    let mut out = String::new();
    let _ = writeln!(out, "{}  (format v{})", report.table, report.format_version);
    let order = if report.sort_order.is_empty() {
        "none".to_owned()
    } else {
        format!(
            "{}  (order {})",
            report.sort_order.join(", "),
            layout.sort_order_id
        )
    };
    let _ = writeln!(out, "  sort order   {order}");
    if report.sort_support != "unsorted" {
        let _ = writeln!(
            out,
            "  sorted       {:.1}% of rows, {:.2} of {:.2} GiB in files stamped with the order",
            100.0 * report.sorted_rows_ratio,
            gib(layout.sorted_bytes),
            gib(layout.data_bytes)
        );
    }
    let _ = writeln!(
        out,
        "  files        {} data files, {} rows: L0 {} · L1 {} · L2 {}",
        layout.data_files,
        layout.data_rows,
        layout.level_files[0],
        layout.level_files[1],
        layout.level_files[2]
    );
    if let (Some(overlap), Some(column)) = (&layout.overlap, &report.overlap_column) {
        let _ = writeln!(
            out,
            "  overlap      on {column}: a file's range overlaps {:.1} files on average, {} at most",
            overlap.mean_files, overlap.max_files
        );
        let _ = writeln!(
            out,
            "  lookup       a point lookup on {column} reads at most {:.1}% of rows by file bounds",
            100.0 * overlap.lookup_rows_ratio
        );
    }
    if report.advice.is_empty() {
        let _ = writeln!(out, "  advice       none");
    } else {
        for (index, advice) in report.advice.iter().enumerate() {
            let label = if index == 0 { "advice" } else { "" };
            let _ = writeln!(out, "  {label:<12} {advice}");
        }
    }
    out
}
