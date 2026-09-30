//! Connected, read-only source diagnostics for `check --source`. Nothing here
//! creates, alters, advances or acknowledges source state or writes local state.
use crate::config::Config;
use crate::source::{connect, validate_source_table};
use anyhow::Result;
use flow_pg_source::{fetch_relation, tokio_postgres::Client};

/// Catalog-only locks can still hold back catalog VACUUM; warn long before the
/// default 200 million transaction freeze age forces anti-wraparound work.
const CATALOG_XMIN_WARN_AGE: i64 = 100_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Level {
    Ok,
    Warn,
    Fail,
}

#[derive(Default)]
struct Report(Vec<(Level, &'static str, String)>);

impl Report {
    fn add(&mut self, level: Level, check: &'static str, detail: impl Into<String>) {
        let detail = detail.into();
        let label = match level {
            Level::Ok => "ok",
            Level::Warn => "warn",
            Level::Fail => "FAIL",
        };
        println!("[{label:>4}] {check}: {detail}");
        self.0.push((level, check, detail));
    }
    fn count(&self, level: Level) -> usize {
        self.0.iter().filter(|(found, ..)| *found == level).count()
    }
}

pub(crate) async fn check_source(config: &Config) -> Result<()> {
    let mut report = Report::default();
    run(config, &mut report).await;
    let (failed, warned) = (report.count(Level::Fail), report.count(Level::Warn));
    println!("source check: {failed} failed, {warned} warnings");
    anyhow::ensure!(failed == 0, "{failed} source checks failed");
    Ok(())
}

async fn run(config: &Config, report: &mut Report) {
    if let Some(pg) = crate::source_tls::configured(config)
        && !source_tls(&pg, report)
    {
        // The connection would fail with the same TLS configuration error.
        return;
    }
    let mut sql = match connect(config, false).await {
        Ok(sql) => sql,
        Err(error) => return report.add(Level::Fail, "connection", format!("{error:#}")),
    };
    report.add(Level::Ok, "connection", "SQL login connected");
    let initialized = config.state_dir.join("source-identity.json").exists();
    let version = match server(&sql, config, report).await {
        Ok(version) => version,
        Err(error) => {
            return report.add(Level::Fail, "server settings", format!("{error:#}"));
        }
    };
    if let Err(error) = replication_login(config, initialized, report).await {
        report.add(Level::Fail, "replication login", format!("{error:#}"));
    }
    if let Err(error) = heartbeat(&sql, version, report).await {
        report.add(Level::Fail, "heartbeat permission", format!("{error:#}"));
    }
    tables(&mut sql, config, report).await;
    if let Err(error) = slots(&sql, config, initialized, report).await {
        report.add(Level::Fail, "replication slot", format!("{error:#}"));
    }
}

/// Defaults stay libpq-compatible, so opportunistic TLS is only a warning.
/// Returns false when the TLS configuration itself is invalid.
fn source_tls(pg: &flow_pg_source::tokio_postgres::Config, report: &mut Report) -> bool {
    if let Err(error) = crate::source_tls::connector(pg) {
        report.add(Level::Fail, "source TLS", format!("{error:#}"));
        return false;
    }
    match crate::source_tls::weakness(pg) {
        Some(weakness) => report.add(
            Level::Warn,
            "source TLS",
            format!("{weakness}; {}", crate::source_tls::RECOMMENDATION),
        ),
        None => report.add(
            Level::Ok,
            "source TLS",
            "server authenticated, or the connection does not leave this host",
        ),
    }
    true
}

async fn server(sql: &Client, config: &Config, report: &mut Report) -> Result<i32> {
    let row = sql
        .query_one(
            "SELECT current_setting('server_version_num')::int,
                    current_setting('wal_level'),
                    current_setting('max_replication_slots')::int,
                    (SELECT count(*) FROM pg_catalog.pg_replication_slots)::int,
                    current_setting('max_wal_senders')::int,
                    (SELECT count(*) FROM pg_catalog.pg_stat_replication)::int,
                    (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'max_slot_wal_keep_size'),
                    current_setting('idle_replication_slot_timeout', true),
                    EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)",
            &[&config.source.slot],
        )
        .await?;
    let version: i32 = row.get(0);
    report.add(
        if (140000..190000).contains(&version) {
            Level::Ok
        } else {
            Level::Fail
        },
        "server version",
        format!("{version}; Flow supports PostgreSQL 14-18"),
    );
    let wal_level: String = row.get(1);
    if wal_level == "logical" {
        report.add(Level::Ok, "wal_level", wal_level);
    } else {
        report.add(
            Level::Fail,
            "wal_level",
            format!("{wal_level}; set wal_level = logical and restart PostgreSQL"),
        );
    }
    let slot_exists: bool = row.get(8);
    let (level, detail) = capacity(
        "replication slots",
        row.get(2),
        row.get(3),
        u32::from(!slot_exists) + 1,
    );
    report.add(level, "max_replication_slots", detail);
    let (level, detail) = capacity("WAL senders", row.get(4), row.get(5), 2);
    report.add(level, "max_wal_senders", detail);
    let (level, detail) = wal_keep(row.get(6), config.limits.wal_hard_bytes);
    report.add(level, "max_slot_wal_keep_size", detail);
    if let Some(timeout) = row.get::<_, Option<String>>(7) {
        if timeout == "0" {
            report.add(Level::Ok, "idle_replication_slot_timeout", "disabled");
        } else {
            report.add(
                Level::Warn,
                "idle_replication_slot_timeout",
                format!(
                    "{timeout}; PostgreSQL invalidates the slot if Flow stays disconnected longer, which requires resynchronization"
                ),
            );
        }
    }
    Ok(version)
}

/// Flow holds one permanent slot and sender; recovery may briefly need one more.
fn capacity(resource: &str, max: i32, used: i32, needed: u32) -> (Level, String) {
    let free = i64::from(max) - i64::from(used);
    let detail = format!("{used} of {max} {resource} in use");
    if free < 1 {
        (
            Level::Fail,
            format!("{detail}; Flow needs at least one more"),
        )
    } else if free < i64::from(needed) {
        (
            Level::Warn,
            format!("{detail}; recovery may need {needed} free (one temporary slot or sender)"),
        )
    } else {
        (Level::Ok, detail)
    }
}

fn wal_keep(megabytes: i64, wal_hard_bytes: u64) -> (Level, String) {
    if megabytes < 0 {
        return (
            Level::Warn,
            "unlimited; a stalled or abandoned slot retains WAL until the source disk fills. Cap it below the space the source volume can spare; exceeding the cap invalidates the slot and requires resynchronization".into(),
        );
    }
    let bytes = u64::try_from(megabytes)
        .unwrap_or(0)
        .saturating_mul(1 << 20);
    if bytes < wal_hard_bytes {
        (
            Level::Warn,
            format!(
                "{megabytes} MB, below limits.wal_hard_bytes ({wal_hard_bytes} bytes); the slot can be invalidated before the configured hard threshold. Flow also warns from safe_wal_size headroom"
            ),
        )
    } else {
        (Level::Ok, format!("{megabytes} MB"))
    }
}

async fn replication_login(config: &Config, initialized: bool, report: &mut Report) -> Result<()> {
    use flow_pg_source::tokio_postgres::SimpleQueryMessage;
    let replication = connect(config, true).await?;
    let response = replication.simple_query("IDENTIFY_SYSTEM").await?;
    let row = response
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("IDENTIFY_SYSTEM returned no source identity"))?;
    report.add(
        Level::Ok,
        "replication login",
        format!(
            "system {}, timeline {}, database {}",
            row.get("systemid").unwrap_or("?"),
            row.get("timeline").unwrap_or("?"),
            row.get("dbname").unwrap_or("?"),
        ),
    );
    if initialized {
        // Read-only when the proof already exists.
        crate::source::verify_source_identity(&replication, config, false).await?;
        report.add(
            Level::Ok,
            "source identity",
            "matches the initialized state directory",
        );
    }
    Ok(())
}

async fn heartbeat(sql: &Client, version: i32, report: &mut Report) -> Result<()> {
    let signature = if version >= 170000 {
        "pg_catalog.pg_logical_emit_message(boolean,text,text,boolean)"
    } else {
        "pg_catalog.pg_logical_emit_message(boolean,text,text)"
    };
    let allowed: bool = sql
        .query_one(
            "SELECT has_function_privilege($1, 'EXECUTE')",
            &[&signature],
        )
        .await?
        .get(0);
    if allowed {
        report.add(Level::Ok, "heartbeat permission", signature);
    } else {
        report.add(
            Level::Fail,
            "heartbeat permission",
            format!("grant EXECUTE on {signature}; idle sources otherwise retain WAL"),
        );
    }
    Ok(())
}

async fn tables(sql: &mut Client, config: &Config, report: &mut Report) {
    let mut schemas = Vec::new();
    for configured in &config.tables {
        let name = format!(
            "{}.{}",
            configured.source_namespace, configured.source_table
        );
        let checked = async {
            let (relation, _) = fetch_relation(
                &*sql,
                &configured.source_namespace,
                &configured.source_table,
                !configured.append_only,
            )
            .await?;
            let schema = configured.schema(relation.id);
            validate_source_table(&*sql, configured, &schema).await?;
            anyhow::Ok(schema)
        }
        .await;
        match checked {
            Ok(schema) => {
                if let Err(error) = access(sql, &name, schema.table_id.0, report).await {
                    report.add(Level::Fail, "table access", format!("{name}: {error:#}"));
                }
                schemas.push(schema);
            }
            Err(error) => report.add(Level::Fail, "table", format!("{name}: {error:#}")),
        }
    }
    if schemas.len() != config.tables.len() {
        return;
    }
    match crate::source::publication_contract(sql, config, &schemas).await {
        Ok(violations) if violations.is_empty() => report.add(
            Level::Ok,
            "publication",
            format!(
                "{} covers every configured table and operation",
                config.source.publication
            ),
        ),
        Ok(violations) => {
            for violation in violations {
                report.add(Level::Fail, "publication", violation.reason);
            }
        }
        Err(error) => report.add(Level::Fail, "publication", format!("{error:#}")),
    }
}

async fn access(sql: &Client, name: &str, oid: u32, report: &mut Report) -> Result<()> {
    let row = sql
        .query_one(
            "SELECT pg_catalog.has_table_privilege(c.oid, 'SELECT'),
                    c.relrowsecurity AND NOT (r.rolsuper OR r.rolbypassrls)
                      AND (c.relforcerowsecurity OR NOT pg_catalog.pg_has_role(c.relowner, 'USAGE')),
                    c.relhassubclass,
                    c.relreplident::text
               FROM pg_catalog.pg_class c, pg_catalog.pg_roles r
              WHERE c.oid = $1 AND r.rolname = current_user",
            &[&oid],
        )
        .await?;
    if !row.get::<_, bool>(0) {
        report.add(
            Level::Fail,
            "table access",
            format!("{name}: login lacks SELECT, which initial COPY requires"),
        );
    } else if row.get::<_, bool>(1) {
        report.add(
            Level::Fail,
            "table access",
            format!(
                "{name}: row-level security applies to this login; initial COPY runs with row_security = off and is rejected rather than copying filtered rows"
            ),
        );
    } else {
        let identity = match row.get::<_, String>(3).as_str() {
            "f" => "FULL",
            "d" => "DEFAULT",
            _ => "other",
        };
        report.add(
            Level::Ok,
            "table",
            format!("{name}: readable, replica identity {identity}"),
        );
    }
    if row.get::<_, bool>(2) {
        report.add(
            Level::Warn,
            "table inheritance",
            format!(
                "{name} has inheritance children; Flow copies and captures only rows stored in the parent itself"
            ),
        );
    }
    Ok(())
}

async fn slots(
    sql: &Client,
    config: &Config,
    initialized: bool,
    report: &mut Report,
) -> Result<()> {
    // Column sets differ across PostgreSQL 14-18; read the row as JSON.
    let rows = sql
        .query(
            "WITH head AS (
                 SELECT CASE WHEN pg_catalog.pg_is_in_recovery()
                             THEN pg_catalog.pg_last_wal_replay_lsn()
                             ELSE pg_catalog.pg_current_wal_lsn() END AS lsn)
             SELECT s.slot_name::text, s.active,
                    pg_catalog.pg_wal_lsn_diff(head.lsn, s.restart_lsn)::bigint,
                    pg_catalog.age(s.catalog_xmin)::bigint,
                    pg_catalog.to_jsonb(s)::text
               FROM pg_catalog.pg_replication_slots s, head
              ORDER BY 3 DESC NULLS LAST",
            &[],
        )
        .await?;
    let mut found = false;
    for row in &rows {
        let name: String = row.get(0);
        let active: bool = row.get(1);
        let retained = row.get::<_, Option<i64>>(2).unwrap_or(0).max(0) as u64;
        if name != config.source.slot {
            if !active && retained >= config.limits.wal_soft_bytes {
                report.add(
                    Level::Warn,
                    "other slots",
                    format!(
                        "inactive slot {name} retains {retained} bytes of WAL; drop it with pg_drop_replication_slot if it is abandoned"
                    ),
                );
            }
            continue;
        }
        found = true;
        let slot: serde_json::Value = serde_json::from_str(&row.get::<_, String>(4))?;
        let text = |key: &str| slot.get(key).and_then(|value| value.as_str());
        if !initialized {
            report.add(
                Level::Fail,
                "replication slot",
                format!(
                    "{name} already exists but this state directory is not initialized; init requires a new slot name"
                ),
            );
            continue;
        }
        if text("plugin") != Some("pgoutput") || text("slot_type") != Some("logical") {
            report.add(
                Level::Fail,
                "replication slot",
                format!("{name} is not a logical pgoutput slot"),
            );
        }
        let lost = text("wal_status") == Some("lost");
        match text("invalidation_reason") {
            Some(reason) => report.add(
                Level::Fail,
                "replication slot",
                format!("{name} was invalidated ({reason}); resynchronization is required"),
            ),
            None if lost => report.add(
                Level::Fail,
                "replication slot",
                format!("{name} lost required WAL; resynchronization is required"),
            ),
            None => {
                let (level, detail) = retention(
                    retained,
                    config.limits.wal_soft_bytes,
                    config.limits.wal_hard_bytes,
                    text("wal_status"),
                );
                report.add(level, "replication slot", format!("{name}: {detail}"));
            }
        }
        if let Some(age) = row.get::<_, Option<i64>>(3)
            && age >= CATALOG_XMIN_WARN_AGE
        {
            report.add(
                Level::Warn,
                "catalog_xmin",
                format!(
                    "{name} holds catalog_xmin {age} transactions old; catalog VACUUM cannot advance past it"
                ),
            );
        }
        if active {
            report.add(
                Level::Ok,
                "slot consumer",
                format!(
                    "{name} is active (pid {})",
                    slot.get("active_pid")
                        .map_or("?".into(), |pid| pid.to_string())
                ),
            );
        }
        spill(sql, &name, report).await?;
    }
    if !found {
        if initialized {
            report.add(
                Level::Fail,
                "replication slot",
                format!(
                    "{} is missing; resynchronization is required",
                    config.source.slot
                ),
            );
        } else {
            report.add(
                Level::Ok,
                "replication slot",
                format!("{} will be created by init", config.source.slot),
            );
        }
    }
    Ok(())
}

fn retention(retained: u64, soft: u64, hard: u64, status: Option<&str>) -> (Level, String) {
    let detail = format!(
        "retains {retained} bytes of WAL, status {}",
        status.unwrap_or("unknown")
    );
    if retained >= hard || status == Some("unreserved") {
        (
            Level::Warn,
            format!("{detail}; at or beyond the hard threshold or cap, drain publication lag"),
        )
    } else if retained >= soft {
        (
            Level::Warn,
            format!("{detail}; above limits.wal_soft_bytes"),
        )
    } else {
        (Level::Ok, detail)
    }
}

async fn spill(sql: &Client, slot: &str, report: &mut Report) -> Result<()> {
    let Some(row) = sql
        .query_opt(
            "SELECT spill_bytes::bigint, stream_bytes::bigint, total_bytes::bigint
               FROM pg_catalog.pg_stat_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
    else {
        return Ok(());
    };
    report.add(
        Level::Ok,
        "decoding",
        format!(
            "{slot}: {} bytes decoded, {} spilled to disk, {} streamed in progress; spilling large transactions is expected, frequent spill of small ones suggests raising logical_decoding_work_mem",
            row.get::<_, i64>(2),
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
        ),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_and_sender_capacity_leaves_room_for_recovery() {
        assert_eq!(capacity("slots", 10, 3, 2).0, Level::Ok);
        assert_eq!(capacity("slots", 10, 9, 2).0, Level::Warn);
        assert_eq!(capacity("slots", 10, 10, 2).0, Level::Fail);
        assert_eq!(capacity("slots", 10, 10, 1).0, Level::Fail);
    }

    #[test]
    fn unlimited_or_small_wal_caps_are_flagged() {
        assert_eq!(wal_keep(-1, 32 << 30).0, Level::Warn);
        assert_eq!(wal_keep(1024, 32 << 30).0, Level::Warn);
        assert_eq!(wal_keep(64 * 1024, 32 << 30).0, Level::Ok);
    }

    #[test]
    fn opportunistic_or_unauthenticated_source_tls_is_a_warning() {
        for (settings, level) in [
            ("host=db.example.com", Level::Warn),
            ("host=db.example.com sslmode=require", Level::Warn),
            ("host=db.example.com sslmode=disable", Level::Warn),
            ("host=db.example.com sslmode=verify-full", Level::Ok),
            ("host=/var/run/postgresql", Level::Ok),
            ("host=db.example.com sslmode=verify-ca", Level::Fail),
        ] {
            let mut report = Report::default();
            let usable = source_tls(&settings.parse().unwrap(), &mut report);
            assert_eq!(usable, level != Level::Fail, "{settings}");
            assert_eq!(report.count(level), 1, "{settings}");
            assert_eq!(report.0.len(), 1);
        }
        let mut report = Report::default();
        source_tls(&"host=db.example.com".parse().unwrap(), &mut report);
        assert!(report.0[0].2.contains("sslmode=verify-full"));
    }

    #[test]
    fn retention_matches_health_thresholds() {
        assert_eq!(retention(1, 10, 20, Some("reserved")).0, Level::Ok);
        assert_eq!(retention(10, 10, 20, Some("extended")).0, Level::Warn);
        assert_eq!(retention(1, 10, 20, Some("unreserved")).0, Level::Warn);
        assert_eq!(retention(20, 10, 20, Some("reserved")).0, Level::Warn);
    }
}
