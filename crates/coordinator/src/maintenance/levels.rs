use flow_compactor::Level;
use std::collections::HashMap;

// Level is a scheduling hint, never row-identity or commit evidence. Encoding
// it in immutable service artifact names survives snapshot expiration and index
// rebuilding. Unknown external output starts at L1; small size alone cannot
// turn an already compacted idle file back into L0 indefinitely.
pub(super) fn file_level(
    data_prefix: &str,
    path: &str,
    summary: Option<&HashMap<String, String>>,
) -> Level {
    let operation = path.strip_prefix(data_prefix).and_then(|name| {
        let (operation, ordinal) = name.rsplit_once("-data-")?;
        let ordinal = ordinal.strip_suffix(".parquet")?;
        (ordinal.len() >= 6
            && ordinal.bytes().all(|byte| byte.is_ascii_digit())
            && !operation.is_empty()
            && operation
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
        .then_some(operation)
    });
    if let Some(operation) = operation {
        for (prefix, level) in [
            ("flow-l0-", Level::L0),
            ("flow-l1-", Level::L1),
            ("flow-l2-", Level::L2),
        ] {
            if operation
                .strip_prefix(prefix)
                .is_some_and(|suffix| !suffix.is_empty())
            {
                return level;
            }
        }
    }
    match summary
        .and_then(|summary| summary.get("streaming.level"))
        .map(String::as_str)
    {
        Some("L0") => return Level::L0,
        Some("L1") => return Level::L1,
        Some("L2") => return Level::L2,
        _ => {}
    }
    if summary
        .and_then(|summary| summary.get("streaming.operation"))
        .is_some_and(|operation| operation == "ingest")
    {
        return Level::L0;
    }
    // Recognize the original service layout for existing deployments. Legacy
    // rewrite files are at least L1 even when their originating snapshot expired.
    if let Some(operation) = operation {
        if operation
            .strip_prefix("rewrite-")
            .is_some_and(|id| uuid::Uuid::try_parse(id).is_ok())
        {
            return Level::L1;
        }
        if let Some((epoch, attempt)) = operation.split_once('-')
            && epoch.len() == 64
            && epoch.bytes().all(|byte| byte.is_ascii_hexdigit())
            && uuid::Uuid::try_parse(attempt).is_ok()
        {
            return Level::L0;
        }
    }
    Level::L1
}
